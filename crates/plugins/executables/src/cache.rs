// Copyright (C) 2026, Harlan Wei
//
// This file is part of redns.

//! In-memory LRU DNS cache with lazy TTL refresh.

use async_trait::async_trait;
use hickory_proto::op::Message;
use hickory_proto::rr::RecordType;
use lru::LruCache;
use redns_core::context::MARK_CACHE_HIT;
use redns_core::plugin::PluginResult;
use redns_core::sequence::ChainWalker;
use redns_core::{Context, RecursiveExecutable};
use serde::Serialize;
use std::collections::HashSet;
use std::fmt;
use std::hash::{BuildHasher, Hash, Hasher};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex, OnceLock, Weak};
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

/// Default cache size.
const DEFAULT_CACHE_SIZE: usize = 1024;

/// Default lazy cache TTL (serve stale for this long while refreshing).
const DEFAULT_LAZY_TTL: Duration = Duration::from_secs(30);

/// Minimum capacity to enable sharding.
const SHARDING_MIN_CAPACITY: usize = 4096;

static CACHE_REGISTRY: OnceLock<StdMutex<Vec<Weak<CacheInner>>>> = OnceLock::new();
static CACHE_ID: AtomicUsize = AtomicUsize::new(1);

/// A cached DNS response entry.
struct CachedEntry {
    /// Cached parsed DNS response.
    resp: Message,
    /// Time this entry was stored.
    stored_at: Instant,
    /// Original minimum TTL of the response records.
    original_ttl: u32,
}

/// Cache key: lowercased QNAME + QTYPE.
#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
struct CacheKey {
    qname: String,
    qtype: RecordType,
}

impl fmt::Display for CacheKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.qname, self.qtype)
    }
}

impl CachedEntry {
    fn remaining_ttl(&self) -> u32 {
        let elapsed = self.stored_at.elapsed().as_secs() as u32;
        self.original_ttl.saturating_sub(elapsed)
    }

    fn is_expired(&self) -> bool {
        self.remaining_ttl() == 0
    }

    fn is_within_lazy_window(&self, lazy_ttl: Duration) -> bool {
        let elapsed = self.stored_at.elapsed();
        let expire_at = Duration::from_secs(self.original_ttl as u64);
        elapsed < expire_at + lazy_ttl
    }
}

/// Build the cache key from DNS question data.
fn cache_key(ctx: &Context) -> Option<CacheKey> {
    ctx.question().map(|q| CacheKey {
        qname: q.name().to_ascii().to_lowercase(),
        qtype: q.query_type(),
    })
}

fn host_parallelism() -> usize {
    std::thread::available_parallelism()
        .map(NonZeroUsize::get)
        .unwrap_or(1)
}

/// Extract the minimum TTL from a DNS response message.
fn min_ttl(msg: &Message) -> u32 {
    let mut min = u32::MAX;
    for rr in msg
        .answers()
        .iter()
        .chain(msg.name_servers().iter())
        .chain(msg.additionals().iter())
    {
        if rr.record_type() == hickory_proto::rr::RecordType::OPT {
            continue;
        }
        min = min.min(rr.ttl());
    }
    if min == u32::MAX { 300 } else { min }
}

/// Adjust all TTLs in a response message.
fn adjust_ttl(msg: &mut Message, remaining: u32) {
    for rr in msg.answers_mut().iter_mut() {
        rr.set_ttl(remaining);
    }
    for rr in msg.name_servers_mut().iter_mut() {
        rr.set_ttl(remaining);
    }
    for rr in msg.additionals_mut().iter_mut() {
        rr.set_ttl(remaining);
    }
}

/// In-memory LRU DNS cache.
///
/// Uses `lru::LruCache` for proper bounded eviction.
/// Sharded to reduce lock contention across threads.
#[derive(Clone)]
pub struct Cache {
    inner: Arc<CacheInner>,
}

struct CacheInner {
    id: usize,
    shard_count: usize,
    shards: Vec<Mutex<LruCache<CacheKey, CachedEntry>>>,
    shard_hasher: ahash::RandomState,
    inflight_refreshes: Mutex<HashSet<CacheKey>>,
    lazy_ttl: Duration,
    /// Captured downstream chain + server metadata for background refreshes.
    /// Set once on first `exec_recursive` call.
    refresh_chain: Mutex<Option<(ChainWalker, redns_core::context::ServerMeta)>>,
    /// Total cache hits (fresh + stale).
    hit_total: AtomicU64,
    /// Total cache misses.
    miss_total: AtomicU64,
}

#[derive(Debug, Clone, Serialize)]
pub struct CacheShardSnapshot {
    pub index: usize,
    pub entries: usize,
    pub capacity: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct CacheSnapshot {
    pub id: usize,
    pub total_entries: usize,
    pub total_capacity: usize,
    pub hit_total: u64,
    pub miss_total: u64,
    pub shards: Vec<CacheShardSnapshot>,
}

impl Cache {
    pub fn new(max_size: usize, lazy_ttl: Duration) -> Self {
        let cap = if max_size == 0 {
            DEFAULT_CACHE_SIZE
        } else {
            max_size
        };

        let shard_count = if cap < SHARDING_MIN_CAPACITY {
            1
        } else {
            host_parallelism().min(cap)
        };
        let shard_cap = std::cmp::max(1, cap.div_ceil(shard_count));
        let mut shards = Vec::with_capacity(shard_count);
        for _ in 0..shard_count {
            shards.push(Mutex::new(LruCache::new(
                NonZeroUsize::new(shard_cap).unwrap(),
            )));
        }

        let id = CACHE_ID.fetch_add(1, Ordering::Relaxed);

        let inner = Arc::new(CacheInner {
            id,
            shard_count,
            shards,
            shard_hasher: ahash::RandomState::new(),
            inflight_refreshes: Mutex::new(HashSet::new()),
            lazy_ttl,
            refresh_chain: Mutex::new(None),
            hit_total: AtomicU64::new(0),
            miss_total: AtomicU64::new(0),
        });
        register_cache(&inner);
        Self { inner }
    }

    pub fn default_cache() -> Self {
        Self::new(DEFAULT_CACHE_SIZE, DEFAULT_LAZY_TTL)
    }

    fn get_shard(&self, key: &CacheKey) -> &Mutex<LruCache<CacheKey, CachedEntry>> {
        let mut s = self.inner.shard_hasher.build_hasher();
        key.hash(&mut s);
        let hash = s.finish();
        &self.inner.shards[(hash as usize) % self.inner.shard_count]
    }
}

fn cache_registry() -> &'static StdMutex<Vec<Weak<CacheInner>>> {
    CACHE_REGISTRY.get_or_init(|| StdMutex::new(Vec::new()))
}

fn register_cache(inner: &Arc<CacheInner>) {
    let registry = cache_registry();
    let mut guard = registry.lock().unwrap();
    guard.retain(|cache| cache.upgrade().is_some());
    guard.push(Arc::downgrade(inner));
}

pub async fn cache_registry_snapshot() -> Vec<CacheSnapshot> {
    let caches: Vec<Arc<CacheInner>> = {
        let registry = cache_registry();
        let mut guard = registry.lock().unwrap();
        guard.retain(|cache| cache.upgrade().is_some());
        guard.iter().filter_map(|cache| cache.upgrade()).collect()
    };

    let mut snapshots = Vec::with_capacity(caches.len());
    for cache in caches {
        let mut total_entries = 0usize;
        let mut total_capacity = 0usize;
        let mut shards = Vec::with_capacity(cache.shards.len());

        for (index, shard) in cache.shards.iter().enumerate() {
            let store = shard.lock().await;
            let entries = store.len();
            let capacity = store.cap().get();
            total_entries += entries;
            total_capacity += capacity;
            shards.push(CacheShardSnapshot {
                index,
                entries,
                capacity,
            });
        }

        snapshots.push(CacheSnapshot {
            id: cache.id,
            total_entries,
            total_capacity,
            hit_total: cache.hit_total.load(Ordering::Relaxed),
            miss_total: cache.miss_total.load(Ordering::Relaxed),
            shards,
        });
    }

    snapshots
}

#[async_trait]
impl RecursiveExecutable for Cache {
    async fn exec_recursive(&self, ctx: &mut Context, mut next: ChainWalker) -> PluginResult<()> {
        // Capture the refresh chain on first call.
        {
            let mut chain_slot = self.inner.refresh_chain.lock().await;
            if chain_slot.is_none() {
                *chain_slot = Some((next.clone(), ctx.server_meta.clone()));
            }
        }

        let key = match cache_key(ctx) {
            Some(k) => k,
            None => return next.exec_next(ctx).await,
        };

        // Check cache.
        let mut do_optimistic_refresh = false;
        let mut cached_payload: Option<(Message, u32)> = None;
        {
            let shard = self.get_shard(&key);
            let mut store = shard.lock().await;
            if let Some(entry) = store.get_mut(&key) {
                if !entry.is_expired() {
                    // Fresh hit.
                    let ttl = entry.remaining_ttl();
                    cached_payload = Some((entry.resp.clone(), ttl));
                } else if entry.is_within_lazy_window(self.inner.lazy_ttl) {
                    // Stale but within lazy window — serve stale and refresh.
                    do_optimistic_refresh = true;
                    entry.stored_at = Instant::now();
                    cached_payload = Some((entry.resp.clone(), 1));
                }
            }
        }

        let served_from_cache = if let Some((mut resp, ttl)) = cached_payload {
            self.inner.hit_total.fetch_add(1, Ordering::Relaxed);
            resp.set_id(ctx.query().id());
            adjust_ttl(&mut resp, ttl);
            ctx.set_response(Some(resp));
            ctx.set_mark(MARK_CACHE_HIT);
            true
        } else {
            false
        };

        if do_optimistic_refresh {
            self.spawn_refresh_for_key(
                &key,
                ctx.query().clone(),
                ctx.server_meta.clone(),
                next.clone(),
            )
            .await;
        }

        if served_from_cache {
            return Ok(());
        }

        // Cache miss — execute downstream.
        self.inner.miss_total.fetch_add(1, Ordering::Relaxed);
        next.exec_next(ctx).await?;

        // Store the response in cache.
        self.store(&key, ctx).await;

        Ok(())
    }
}

impl Cache {
    async fn store(&self, key: &CacheKey, ctx: &Context) {
        use hickory_proto::op::ResponseCode;

        if let Some(resp) = ctx.response() {
            let rcode = resp.response_code();

            // Never cache REFUSED — it's a transient upstream signal (rate
            // limiting, policy, etc.) and should not poison the cache.
            if rcode == ResponseCode::Refused {
                return;
            }

            let mut ttl = min_ttl(resp);
            if rcode == ResponseCode::NXDomain {
                ttl = ttl.min(30);
            } else if rcode == ResponseCode::ServFail {
                ttl = ttl.min(5);
            }

            if ttl <= 0 {
                return;
            }

            let shard = self.get_shard(key);
            let mut store = shard.lock().await;

            store.push(
                key.clone(),
                CachedEntry {
                    resp: resp.clone(),
                    stored_at: Instant::now(),
                    original_ttl: ttl,
                },
            );
        }
    }

    /// Spawn a background refresh for a specific key, deduplicating via inflight_refreshes.
    async fn spawn_refresh_for_key(
        &self,
        key: &CacheKey,
        query: Message,
        server_meta: redns_core::context::ServerMeta,
        mut chain: ChainWalker,
    ) {
        let mut inflight = self.inner.inflight_refreshes.lock().await;
        let should_spawn = inflight.insert(key.clone());
        drop(inflight);

        if !should_spawn {
            return;
        }

        let mut refresh_ctx = Context::new(query);
        refresh_ctx.server_meta = server_meta;
        let cache_clone = self.clone();
        let refresh_key = key.clone();
        tokio::spawn(async move {
            let _ = chain.exec_next(&mut refresh_ctx).await;
            cache_clone.store(&refresh_key, &refresh_ctx).await;

            let mut inflight = cache_clone.inner.inflight_refreshes.lock().await;
            inflight.remove(&refresh_key);
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hickory_proto::op::{Message, MessageType, OpCode, Query, ResponseCode};
    use hickory_proto::rr::{Name, RData, Record, RecordType};
    use redns_core::plugin::Executable;
    use redns_core::sequence::{ChainNode, NodeExecutor, Sequence};
    use std::net::Ipv4Addr;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

    struct RespondWithTtl(u32);
    #[async_trait]
    impl Executable for RespondWithTtl {
        async fn exec(&self, ctx: &mut Context) -> PluginResult<()> {
            let q = ctx.question().unwrap().clone();
            let mut resp = Message::new();
            resp.set_id(ctx.query().id());
            resp.set_message_type(MessageType::Response);
            resp.set_response_code(ResponseCode::NoError);
            resp.add_query(q.clone());
            resp.add_answer(Record::from_rdata(
                q.name().clone(),
                self.0,
                RData::A(Ipv4Addr::new(1, 2, 3, 4).into()),
            ));
            ctx.set_response(Some(resp));
            Ok(())
        }
    }

    struct CountingDelayedResponder {
        ttl: u32,
        calls: Arc<AtomicUsize>,
        delay: Duration,
    }

    #[async_trait]
    impl Executable for CountingDelayedResponder {
        async fn exec(&self, ctx: &mut Context) -> PluginResult<()> {
            self.calls.fetch_add(1, AtomicOrdering::Relaxed);
            tokio::time::sleep(self.delay).await;

            let q = ctx.question().unwrap().clone();
            let mut resp = Message::new();
            resp.set_id(ctx.query().id());
            resp.set_message_type(MessageType::Response);
            resp.set_response_code(ResponseCode::NoError);
            resp.add_query(q.clone());
            resp.add_answer(Record::from_rdata(
                q.name().clone(),
                self.ttl,
                RData::A(Ipv4Addr::new(1, 2, 3, 4).into()),
            ));
            ctx.set_response(Some(resp));
            Ok(())
        }
    }

    fn make_query() -> Message {
        let mut msg = Message::new();
        msg.set_id(1)
            .set_message_type(MessageType::Query)
            .set_op_code(OpCode::Query);
        msg.add_query({
            let mut q = Query::new();
            q.set_name(Name::from_ascii("test.example.com.").unwrap())
                .set_query_type(RecordType::A);
            q
        });
        msg
    }

    #[tokio::test]
    async fn cache_miss_then_hit() {
        let cache = Cache::new(100, Duration::from_secs(30));
        let chain: Vec<ChainNode> = vec![
            ChainNode {
                matchers: vec![],
                executor: NodeExecutor::Recursive(Box::new(cache)),
            },
            ChainNode {
                matchers: vec![],
                executor: NodeExecutor::Simple(Box::new(RespondWithTtl(300))),
            },
        ];
        let seq = Sequence::new(chain);

        let mut ctx = Context::new(make_query());
        seq.exec(&mut ctx).await.unwrap();
        assert!(ctx.response().is_some());
        assert_eq!(ctx.response().unwrap().answers().len(), 1);
    }

    #[tokio::test]
    async fn lru_eviction_respects_capacity() {
        let cache = Cache::new(2, Duration::from_secs(30));

        // Simulate 5 different cache entries via the store directly.
        {
            let shard_key = CacheKey {
                qname: "key".to_string(),
                qtype: RecordType::A,
            };
            let shard = cache.get_shard(&shard_key);
            let mut store = shard.lock().await;
            for i in 0..5 {
                store.put(
                    CacheKey {
                        qname: format!("key{i}"),
                        qtype: RecordType::A,
                    },
                    CachedEntry {
                        resp: Message::new(),
                        stored_at: Instant::now(),
                        original_ttl: 300,
                    },
                );
            }
            // Sharding is disabled for small caches, so capacity stays exact.
            assert_eq!(store.len(), 2);
            // key0 should have been evicted (LRU).
            assert!(
                store
                    .get(&CacheKey {
                        qname: "key0".to_string(),
                        qtype: RecordType::A,
                    })
                    .is_none()
            );
            assert!(
                store
                    .get(&CacheKey {
                        qname: "key1".to_string(),
                        qtype: RecordType::A,
                    })
                    .is_none()
            );
            assert!(
                store
                    .get(&CacheKey {
                        qname: "key3".to_string(),
                        qtype: RecordType::A,
                    })
                    .is_some()
            );
            assert!(
                store
                    .get(&CacheKey {
                        qname: "key4".to_string(),
                        qtype: RecordType::A,
                    })
                    .is_some()
            );
        }
    }

    #[tokio::test]
    async fn deduplicates_inflight_refresh_for_same_key() {
        let calls = Arc::new(AtomicUsize::new(0));
        let cache = Cache::new(128, Duration::from_secs(30));
        let chain: Vec<ChainNode> = vec![
            ChainNode {
                matchers: vec![],
                executor: NodeExecutor::Recursive(Box::new(cache)),
            },
            ChainNode {
                matchers: vec![],
                executor: NodeExecutor::Simple(Box::new(CountingDelayedResponder {
                    ttl: 1,
                    calls: Arc::clone(&calls),
                    delay: Duration::from_millis(250),
                })),
            },
        ];
        let seq = Sequence::new(chain);

        // Prime cache.
        let mut first = Context::new(make_query());
        seq.exec(&mut first).await.unwrap();
        assert_eq!(calls.load(AtomicOrdering::Relaxed), 1);

        // Make entry stale but still within lazy window.
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Trigger one background refresh.
        let mut stale_1 = Context::new(make_query());
        seq.exec(&mut stale_1).await.unwrap();

        // Duplicate stale hit for same key while first refresh is still running.
        let mut stale_2 = Context::new(make_query());
        seq.exec(&mut stale_2).await.unwrap();

        // Wait for single background refresh completion.
        tokio::time::sleep(Duration::from_millis(350)).await;

        // One initial upstream call + one deduplicated refresh.
        assert_eq!(calls.load(AtomicOrdering::Relaxed), 2);
    }

    #[test]
    fn disables_sharding_below_threshold() {
        let cache = Cache::new(4095, Duration::from_secs(30));
        assert_eq!(cache.inner.shard_count, 1);
    }

    #[test]
    fn enables_sharding_at_threshold() {
        let cache = Cache::new(4096, Duration::from_secs(30));
        assert_eq!(cache.inner.shard_count, host_parallelism().min(4096));
    }

}
