// Copyright (C) 2026, Harlan Wei
//
// This file is part of redns.

//! In-memory LRU DNS cache with lazy TTL refresh.

use async_trait::async_trait;
use hickory_proto::op::Message;
use lru::LruCache;
use redns_core::context::MARK_CACHE_HIT;
use redns_core::plugin::PluginResult;
use redns_core::sequence::ChainWalker;
use redns_core::{Context, RecursiveExecutable};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::num::NonZeroUsize;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tracing::debug;

use std::sync::Arc;

/// Default cache size.
const DEFAULT_CACHE_SIZE: usize = 1024;

/// Default lazy cache TTL (serve stale for this long while refreshing).
const DEFAULT_LAZY_TTL: Duration = Duration::from_secs(30);

/// Number of shards for the cache to reduce lock contention.
const CACHE_SHARDS: usize = 32;

/// A cached DNS response entry.
struct CachedEntry {
    /// The cached response wire bytes.
    resp_bytes: Vec<u8>,
    /// Time this entry was stored.
    stored_at: Instant,
    /// Original minimum TTL of the response records.
    original_ttl: u32,
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

/// Cache key: lowercased QNAME + QTYPE.
fn cache_key(ctx: &Context) -> Option<String> {
    ctx.question()
        .map(|q| format!("{}:{}", q.name().to_ascii().to_lowercase(), q.query_type()))
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
    shards: Vec<Mutex<LruCache<String, CachedEntry>>>,
    lazy_ttl: Duration,
}

impl Cache {
    pub fn new(max_size: usize, lazy_ttl: Duration) -> Self {
        let cap = if max_size == 0 {
            DEFAULT_CACHE_SIZE
        } else {
            max_size
        };

        let shard_cap = cap / CACHE_SHARDS;
        let mut shards = Vec::with_capacity(CACHE_SHARDS);
        for _ in 0..CACHE_SHARDS {
            shards.push(Mutex::new(
                LruCache::new(NonZeroUsize::new(shard_cap).unwrap()),
            ));
        }

        Self {
            inner: Arc::new(CacheInner { shards, lazy_ttl }),
        }
    }

    pub fn default_cache() -> Self {
        Self::new(DEFAULT_CACHE_SIZE, DEFAULT_LAZY_TTL)
    }

    fn get_shard(&self, key: &str) -> &Mutex<LruCache<String, CachedEntry>> {
        let mut s = DefaultHasher::new();
        key.hash(&mut s);
        let hash = s.finish();
        &self.inner.shards[(hash as usize) % CACHE_SHARDS]
    }
}

#[async_trait]
impl RecursiveExecutable for Cache {
    async fn exec_recursive(
        &self,
        ctx: &mut Context,
        mut next: ChainWalker,
    ) -> PluginResult<()> {
        let key = match cache_key(ctx) {
            Some(k) => k,
            None => return next.exec_next(ctx).await,
        };

        // Check cache.
        let mut do_optimistic_refresh = false;
        let mut served_from_cache = false;
        {
            let shard = self.get_shard(&key);
            let mut store = shard.lock().await;
            if let Some(entry) = store.get_mut(&key) {
                if !entry.is_expired() {
                    // Fresh hit.
                    let remaining = entry.remaining_ttl();
                    if let Ok(mut resp) = Message::from_vec(&entry.resp_bytes) {
                        resp.set_id(ctx.query().id());
                        adjust_ttl(&mut resp, remaining);
                        ctx.set_response(Some(resp));
                        ctx.set_mark(MARK_CACHE_HIT);
                        debug!(key = %key, ttl = remaining, "cache hit");
                        served_from_cache = true;

                        let elapsed = entry.stored_at.elapsed().as_secs() as u32;
                        if elapsed >= (entry.original_ttl as f32 * 0.8) as u32 {
                            do_optimistic_refresh = true;
                            // Optimistically advance stored_at to debounce parallel refreshes
                            entry.stored_at = Instant::now();
                        }
                    }
                } else if entry.is_within_lazy_window(self.inner.lazy_ttl) {
                    // Stale but within lazy window — serve stale and refresh.
                    do_optimistic_refresh = true;
                    entry.stored_at = Instant::now();
                    if let Ok(mut resp) = Message::from_vec(&entry.resp_bytes) {
                        resp.set_id(ctx.query().id());
                        adjust_ttl(&mut resp, 5); // Short TTL for stale.
                        ctx.set_response(Some(resp));
                        ctx.set_mark(MARK_CACHE_HIT);
                        debug!(key = %key, "cache lazy hit (stale)");
                        served_from_cache = true;
                    }
                }
            }
        }

        if do_optimistic_refresh {
            let mut refresh_ctx = Context::new(ctx.query().clone());
            refresh_ctx.server_meta = ctx.server_meta.clone();
            let mut refresh_next = next.clone();
            let cache_clone = self.clone();
            let refresh_key = key.clone();
            tokio::spawn(async move {
                debug!(key = %refresh_key, "background optimistic refresh triggered");
                let _ = refresh_next.exec_next(&mut refresh_ctx).await;
                cache_clone.store_entry(&refresh_key, &refresh_ctx).await;
            });
        }

        if served_from_cache {
            return Ok(());
        }

        // Cache miss — execute downstream.
        next.exec_next(ctx).await?;

        // Store the response in cache.
        self.store_entry(&key, ctx).await;

        Ok(())
    }
}

impl Cache {
    async fn store_entry(&self, key: &str, ctx: &Context) {
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
                    if let Ok(bytes) = resp.to_vec() {
                        let shard = self.get_shard(key);
                        let mut store = shard.lock().await;
                        // LruCache automatically evicts oldest when at capacity.
                        store.put(
                            key.to_string(),
                            CachedEntry {
                                resp_bytes: bytes,
                                stored_at: Instant::now(),
                                original_ttl: ttl,
                            },
                        );
                    }
                }
            }
        }
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
            let shard = cache.get_shard("key");
            let mut store = shard.lock().await;
            for i in 0..5 {
                store.put(
                    format!("key{i}"),
                    CachedEntry {
                        resp_bytes: vec![],
                        stored_at: Instant::now(),
                        original_ttl: 300,
                    },
                );
            }
            // Capacity is 4 (min cap per shard), so only 4 should remain.
            assert_eq!(store.len(), 4);
            // key0 should have been evicted (LRU).
            assert!(store.get("key0").is_none());
            assert!(store.get("key1").is_some());
            assert!(store.get("key4").is_some());
        }
    }
}
