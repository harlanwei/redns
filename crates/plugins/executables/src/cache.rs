// Copyright (C) 2026, Harlan Wei
//
// This file is part of redns.

//! In-memory LRU DNS cache with lazy TTL refresh.

use async_trait::async_trait;
use hickory_proto::op::{Message, MessageType, OpCode, Query};
use hickory_proto::rr::{Name, RecordType};
use lru::LruCache;
use redns_core::context::MARK_CACHE_HIT;
use redns_core::plugin::PluginResult;
use redns_core::sequence::ChainWalker;
use redns_core::upstream::global_average_latency;
use redns_core::{Context, RecursiveExecutable};
use serde::Serialize;
use std::cmp::Reverse;
use std::collections::{BTreeSet, HashSet};
use std::fmt;
use std::hash::{BuildHasher, Hash, Hasher};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex, OnceLock, Weak};
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tracing::debug;

/// Default cache size.
const DEFAULT_CACHE_SIZE: usize = 1024;

/// Default lazy cache TTL (serve stale for this long while refreshing).
const DEFAULT_LAZY_TTL: Duration = Duration::from_secs(30);

/// Default percentage of hottest cache entries eligible for proactive refresh.
pub const DEFAULT_HOTSET_PERCENT: u8 = 20;

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
    /// Number of requests observed for this cache key while entry is present.
    request_count: u64,
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
    hotset_percent: u8,
    /// Global secondary index: entries sorted by (Reverse(request_count), key).
    /// Enables O(log n) hot-set membership checks.
    hot_index: Mutex<BTreeSet<(Reverse<u64>, CacheKey)>>,
    /// Captured downstream chain + server metadata for background refreshes.
    /// Set once on first `exec_recursive` call.
    refresh_chain: Mutex<Option<(ChainWalker, redns_core::context::ServerMeta)>>,
    /// Whether the background sweep task has been spawned.
    sweep_spawned: AtomicBool,
    /// Total cache hits (fresh + stale).
    hit_total: AtomicU64,
    /// Total cache misses.
    miss_total: AtomicU64,
    /// Total hot-set proactive refreshes triggered.
    hot_refresh_total: AtomicU64,
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
    pub hot_refresh_total: u64,
    pub shards: Vec<CacheShardSnapshot>,
}

impl Cache {
    pub fn new(max_size: usize, lazy_ttl: Duration) -> Self {
        Self::new_with_hotset_percent(max_size, lazy_ttl, DEFAULT_HOTSET_PERCENT)
    }

    pub fn new_with_hotset_percent(
        max_size: usize,
        lazy_ttl: Duration,
        hotset_percent: u8,
    ) -> Self {
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
            hotset_percent: hotset_percent.min(100),
            hot_index: Mutex::new(BTreeSet::new()),
            refresh_chain: Mutex::new(None),
            sweep_spawned: AtomicBool::new(false),
            hit_total: AtomicU64::new(0),
            miss_total: AtomicU64::new(0),
            hot_refresh_total: AtomicU64::new(0),
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
            hot_refresh_total: cache.hot_refresh_total.load(Ordering::Relaxed),
            shards,
        });
    }

    snapshots
}

#[async_trait]
impl RecursiveExecutable for Cache {
    async fn exec_recursive(&self, ctx: &mut Context, mut next: ChainWalker) -> PluginResult<()> {
        // Capture the refresh chain on first call and spawn the background sweep.
        if !self.inner.sweep_spawned.load(Ordering::Relaxed) {
            let mut chain_slot = self.inner.refresh_chain.lock().await;
            if chain_slot.is_none() {
                *chain_slot = Some((next.clone(), ctx.server_meta.clone()));
                drop(chain_slot);
                if !self.inner.sweep_spawned.swap(true, Ordering::Relaxed) {
                    let inner = Arc::clone(&self.inner);
                    tokio::spawn(sweep_hot_entries(inner));
                }
            }
        }

        let key = match cache_key(ctx) {
            Some(k) => k,
            None => return next.exec_next(ctx).await,
        };

        // Check cache.
        let mut do_optimistic_refresh = false;
        let mut refresh_candidate_request_count = 0u64;
        let mut refresh_candidate_ttl = 0u32;
        let mut cached_payload: Option<(Message, u32, bool)> = None;
        {
            let shard = self.get_shard(&key);
            let mut store = shard.lock().await;
            if let Some(entry) = store.get_mut(&key) {
                let old_count = entry.request_count;
                entry.request_count = old_count.saturating_add(1);
                let new_count = entry.request_count;

                // Update the global hot index.
                {
                    let mut index = self.inner.hot_index.lock().await;
                    index.remove(&(Reverse(old_count), key.clone()));
                    index.insert((Reverse(new_count), key.clone()));
                }

                if !entry.is_expired() {
                    // Fresh hit.
                    let ttl = entry.remaining_ttl();
                    cached_payload = Some((entry.resp.clone(), ttl, false));
                    refresh_candidate_request_count = new_count;
                    refresh_candidate_ttl = ttl;
                } else if entry.is_within_lazy_window(self.inner.lazy_ttl) {
                    // Stale but within lazy window — serve stale and refresh.
                    do_optimistic_refresh = true;
                    entry.stored_at = Instant::now();
                    cached_payload = Some((entry.resp.clone(), 1, true));
                }
            }
        }

        let mut served_from_cache = false;
        if let Some((mut resp, ttl, stale_hit)) = cached_payload {
            self.inner.hit_total.fetch_add(1, Ordering::Relaxed);
            resp.set_id(ctx.query().id());
            adjust_ttl(&mut resp, ttl);
            ctx.set_response(Some(resp));
            ctx.set_mark(MARK_CACHE_HIT);
            if stale_hit {
                debug!(key = %key, "cache lazy hit (stale)");
            } else {
                debug!(key = %key, ttl = ttl, "cache hit");
            }
            served_from_cache = true;
        }

        if !do_optimistic_refresh
            && refresh_candidate_ttl > 0
            && self
                .should_proactively_refresh(
                    &key,
                    refresh_candidate_request_count,
                    refresh_candidate_ttl,
                )
                .await
        {
            do_optimistic_refresh = true;
        }

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
        self.store_entry(&key, ctx).await;

        Ok(())
    }
}

impl Cache {
    async fn should_proactively_refresh(
        &self,
        _key: &CacheKey,
        request_count: u64,
        remaining_ttl: u32,
    ) -> bool {
        let threshold = match proactive_refresh_ttl_threshold() {
            Some(v) if v > Duration::ZERO => v,
            _ => return false,
        };

        if Duration::from_secs(remaining_ttl as u64) >= threshold {
            return false;
        }

        if self.inner.hotset_percent == 0 {
            return false;
        }

        is_hot_entry(&self.inner, request_count).await
    }

    async fn store_entry(&self, key: &CacheKey, ctx: &Context) {
        if let Some(resp) = ctx.response() {
            let rcode = resp.response_code();
            let is_negative = rcode == hickory_proto::op::ResponseCode::NXDomain
                || rcode == hickory_proto::op::ResponseCode::ServFail;

            if rcode == hickory_proto::op::ResponseCode::NoError || is_negative {
                let mut ttl = min_ttl(resp);

                if rcode == hickory_proto::op::ResponseCode::NXDomain {
                    ttl = ttl.min(30);
                } else if rcode == hickory_proto::op::ResponseCode::ServFail {
                    ttl = ttl.min(5);
                }

                if ttl > 0 {
                    let shard = self.get_shard(key);
                    let mut store = shard.lock().await;

                    // Remove old index entry if this key already exists.
                    if let Some(old_entry) = store.peek(key) {
                        let mut index = self.inner.hot_index.lock().await;
                        index.remove(&(Reverse(old_entry.request_count), key.clone()));
                    }

                    // Use push() to capture evicted entries for index cleanup.
                    let evicted = store.push(
                        key.clone(),
                        CachedEntry {
                            resp: resp.clone(),
                            stored_at: Instant::now(),
                            original_ttl: ttl,
                            request_count: 1,
                        },
                    );

                    let mut index = self.inner.hot_index.lock().await;
                    // Remove evicted entry from index.
                    if let Some((evicted_key, evicted_entry)) = evicted {
                        index.remove(&(Reverse(evicted_entry.request_count), evicted_key));
                    }
                    // Insert the new entry.
                    index.insert((Reverse(1), key.clone()));
                }
            }
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
            debug!(key = %key, "refresh already in-flight; skipping duplicate");
            return;
        }

        self.inner.hot_refresh_total.fetch_add(1, Ordering::Relaxed);
        let mut refresh_ctx = Context::new(query);
        refresh_ctx.server_meta = server_meta;
        let cache_clone = self.clone();
        let refresh_key = key.clone();
        tokio::spawn(async move {
            debug!(key = %refresh_key, "background optimistic refresh triggered");
            let _ = chain.exec_next(&mut refresh_ctx).await;
            cache_clone.store_entry(&refresh_key, &refresh_ctx).await;

            let mut inflight = cache_clone.inner.inflight_refreshes.lock().await;
            inflight.remove(&refresh_key);
        });
    }
}

/// Check whether an entry with `request_count` is in the hot set.
/// Uses the global BTreeSet index — no shard locks needed.
async fn is_hot_entry(inner: &CacheInner, request_count: u64) -> bool {
    let index = inner.hot_index.lock().await;
    let total = index.len();
    if total == 0 {
        return false;
    }

    let top_n = ((total * inner.hotset_percent as usize).div_ceil(100)).max(1);

    // The BTreeSet is sorted by (Reverse(count), key), so the front has the
    // highest request counts. Count entries strictly hotter than this one.
    let mut hotter = 0usize;
    for &(Reverse(count), _) in &*index {
        if count <= request_count {
            break;
        }
        hotter += 1;
        if hotter >= top_n {
            return false;
        }
    }
    hotter < top_n
}

/// Build a DNS query Message from a CacheKey.
fn build_query_from_key(key: &CacheKey) -> Message {
    let mut msg = Message::new();
    msg.set_id(0)
        .set_message_type(MessageType::Query)
        .set_op_code(OpCode::Query);
    msg.add_query({
        let mut q = Query::new();
        // CacheKey.qname is already lowercased ASCII with trailing dot.
        q.set_name(Name::from_ascii(&key.qname).unwrap_or_default())
            .set_query_type(key.qtype);
        q
    });
    msg
}

/// Background sweep: periodically check hot entries approaching TTL expiry
/// and trigger proactive refresh, even when no queries arrive for those entries.
async fn sweep_hot_entries(inner: Arc<CacheInner>) {
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;

        let threshold = match proactive_refresh_ttl_threshold() {
            Some(v) if v > Duration::ZERO => v,
            _ => continue,
        };

        if inner.hotset_percent == 0 {
            continue;
        }

        // Collect hot keys needing refresh.
        let candidates = {
            let index = inner.hot_index.lock().await;
            let total = index.len();
            if total == 0 {
                continue;
            }

            let top_n = ((total * inner.hotset_percent as usize).div_ceil(100)).max(1);

            // Take the top_n hottest keys.
            index.iter().take(top_n).cloned().collect::<Vec<_>>()
        };

        // Determine shard hasher for key lookups.
        let cache = Cache {
            inner: Arc::clone(&inner),
        };

        for (_, key) in &candidates {
            // Check if this entry's TTL is below the threshold.
            let needs_refresh = {
                let shard = cache.get_shard(key);
                let store = shard.lock().await;
                match store.peek(key) {
                    Some(entry) => {
                        let remaining = entry.remaining_ttl();
                        remaining > 0 && Duration::from_secs(remaining as u64) < threshold
                    }
                    None => false,
                }
            };

            if needs_refresh {
                // Get the chain and server_meta for spawning a refresh.
                let chain_data = {
                    let guard = inner.refresh_chain.lock().await;
                    guard.clone()
                };
                if let Some((chain, server_meta)) = chain_data {
                    let query = build_query_from_key(key);
                    cache
                        .spawn_refresh_for_key(key, query, server_meta, chain)
                        .await;
                }
            }
        }
    }
}

fn proactive_refresh_ttl_threshold() -> Option<Duration> {
    let avg_latency = global_average_latency()?;
    let latency_bound = Duration::from_secs_f64(avg_latency.as_secs_f64() * 1.5);
    Some(std::cmp::min(Duration::from_secs(10), latency_bound))
}

#[cfg(test)]
mod tests {
    use super::*;
    use hickory_proto::op::{Message, MessageType, OpCode, Query, ResponseCode};
    use hickory_proto::rr::{Name, RData, Record, RecordType};
    use redns_core::plugin::Executable;
    use redns_core::sequence::{ChainNode, NodeExecutor, Sequence};
    use std::net::Ipv4Addr;
    use std::sync::Arc;
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
                        request_count: 0,
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

    #[tokio::test]
    async fn hot_index_tracks_entries_correctly() {
        // Cache with capacity 5 and 20% hot set → top 1 entry is "hot".
        let cache = Cache::new_with_hotset_percent(5, Duration::from_secs(30), 20);

        // Helper to build a context with a response for store_entry.
        let make_ctx_with_resp = |name: &str| {
            let mut msg = Message::new();
            msg.set_id(1)
                .set_message_type(MessageType::Query)
                .set_op_code(OpCode::Query);
            msg.add_query({
                let mut q = Query::new();
                q.set_name(Name::from_ascii(name).unwrap())
                    .set_query_type(RecordType::A);
                q
            });
            let mut ctx = Context::new(msg);

            let mut resp = Message::new();
            resp.set_message_type(MessageType::Response);
            resp.set_response_code(ResponseCode::NoError);
            resp.add_answer(Record::from_rdata(
                Name::from_ascii(name).unwrap(),
                300,
                RData::A(Ipv4Addr::new(1, 2, 3, 4).into()),
            ));
            ctx.set_response(Some(resp));
            ctx
        };

        // Insert 3 entries.
        let keys: Vec<CacheKey> = ["a.test.", "b.test.", "c.test."]
            .iter()
            .map(|n| CacheKey {
                qname: n.to_string(),
                qtype: RecordType::A,
            })
            .collect();

        for name in &["a.test.", "b.test.", "c.test."] {
            let key = CacheKey {
                qname: name.to_string(),
                qtype: RecordType::A,
            };
            let ctx = make_ctx_with_resp(name);
            cache.store_entry(&key, &ctx).await;
        }

        // All start with request_count=1, so the index should have 3 entries.
        {
            let index = cache.inner.hot_index.lock().await;
            assert_eq!(index.len(), 3);
        }

        // Bump request_count for "a.test." to 10 via direct index manipulation.
        {
            let shard = cache.get_shard(&keys[0]);
            let mut store = shard.lock().await;
            if let Some(entry) = store.get_mut(&keys[0]) {
                let old = entry.request_count;
                entry.request_count = 10;
                let mut index = cache.inner.hot_index.lock().await;
                index.remove(&(Reverse(old), keys[0].clone()));
                index.insert((Reverse(10), keys[0].clone()));
            }
        }

        // With 3 entries and 20% hot set, top_n = ceil(3*20/100) = 1.
        // "a.test." has count 10, others have 1. So "a.test." is hot.
        assert!(is_hot_entry(&cache.inner, 10).await);
        // An entry with count=1 is NOT hot (there's already 1 entry hotter).
        assert!(!is_hot_entry(&cache.inner, 1).await);

        // Now insert 3 more entries to trigger LRU eviction (capacity=5).
        for name in &["d.test.", "e.test.", "f.test."] {
            let key = CacheKey {
                qname: name.to_string(),
                qtype: RecordType::A,
            };
            let ctx = make_ctx_with_resp(name);
            cache.store_entry(&key, &ctx).await;
        }

        // After evictions, the index should track the same count as the shard.
        let mut shard_total = 0usize;
        for shard in &cache.inner.shards {
            let store = shard.lock().await;
            shard_total += store.len();
        }
        let index = cache.inner.hot_index.lock().await;
        assert_eq!(index.len(), shard_total);
    }
}
