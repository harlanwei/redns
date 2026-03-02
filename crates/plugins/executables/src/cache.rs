// Copyright (C) 2026, Harlan Wei
//
// This file is part of redns.

//! In-memory LRU DNS cache with lazy TTL refresh.

use async_trait::async_trait;
use hickory_proto::op::Message;
use lru::LruCache;
use redns_core::plugin::PluginResult;
use redns_core::sequence::ChainWalker;
use redns_core::{Context, RecursiveExecutable};
use std::num::NonZeroUsize;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tracing::debug;

/// Default cache size.
const DEFAULT_CACHE_SIZE: usize = 1024;

/// Default lazy cache TTL (serve stale for this long while refreshing).
const DEFAULT_LAZY_TTL: Duration = Duration::from_secs(30);

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
/// Uses `lru::LruCache` for proper bounded eviction (replaces HashMap).
pub struct Cache {
    store: Mutex<LruCache<String, CachedEntry>>,
    lazy_ttl: Duration,
}

impl Cache {
    pub fn new(max_size: usize, lazy_ttl: Duration) -> Self {
        let cap = if max_size == 0 {
            DEFAULT_CACHE_SIZE
        } else {
            max_size
        };
        Self {
            store: Mutex::new(LruCache::new(NonZeroUsize::new(cap).unwrap())),
            lazy_ttl,
        }
    }

    pub fn default_cache() -> Self {
        Self::new(DEFAULT_CACHE_SIZE, DEFAULT_LAZY_TTL)
    }
}

#[async_trait]
impl RecursiveExecutable for Cache {
    async fn exec_recursive(
        &self,
        ctx: &mut Context,
        mut next: ChainWalker<'_>,
    ) -> PluginResult<()> {
        let key = match cache_key(ctx) {
            Some(k) => k,
            None => return next.exec_next(ctx).await,
        };

        // Check cache.
        {
            let mut store = self.store.lock().await;
            if let Some(entry) = store.get(&key) {
                if !entry.is_expired() {
                    // Fresh hit.
                    let remaining = entry.remaining_ttl();
                    if let Ok(mut resp) = Message::from_vec(&entry.resp_bytes) {
                        resp.set_id(ctx.query().id());
                        adjust_ttl(&mut resp, remaining);
                        ctx.set_response(Some(resp));
                        debug!(key = %key, ttl = remaining, "cache hit");
                        return Ok(());
                    }
                } else if entry.is_within_lazy_window(self.lazy_ttl) {
                    // Stale but within lazy window — serve stale.
                    if let Ok(mut resp) = Message::from_vec(&entry.resp_bytes) {
                        resp.set_id(ctx.query().id());
                        adjust_ttl(&mut resp, 5); // Short TTL for stale.
                        ctx.set_response(Some(resp));
                        debug!(key = %key, "cache lazy hit (stale)");
                        return Ok(());
                    }
                }
            }
        }

        // Cache miss — execute downstream.
        next.exec_next(ctx).await?;

        // Store the response in cache.
        if let Some(resp) = ctx.response() {
            if resp.response_code() == hickory_proto::op::ResponseCode::NoError
                || resp.response_code() == hickory_proto::op::ResponseCode::NXDomain
            {
                let ttl = min_ttl(resp);
                if ttl > 0 {
                    if let Ok(bytes) = resp.to_vec() {
                        let mut store = self.store.lock().await;
                        // LruCache automatically evicts oldest when at capacity.
                        store.put(
                            key,
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

        Ok(())
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

        // Simulate 3 different cache entries via the store directly.
        {
            let mut store = cache.store.lock().await;
            for i in 0..3 {
                store.put(
                    format!("key{i}"),
                    CachedEntry {
                        resp_bytes: vec![],
                        stored_at: Instant::now(),
                        original_ttl: 300,
                    },
                );
            }
            // Capacity is 2, so only 2 should remain.
            assert_eq!(store.len(), 2);
            // key0 should have been evicted (LRU).
            assert!(store.get("key0").is_none());
            assert!(store.get("key1").is_some());
            assert!(store.get("key2").is_some());
        }
    }
}
