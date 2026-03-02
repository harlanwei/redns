// Copyright (C) 2026, Harlan Wei
//
// This file is part of redns.

//! Per-IP token-bucket rate limiter.
//!
//! Ports `plugin/matcher/rate_limiter/rate_limiter.go`.
//!
//! Returns `true` if the request is allowed (within limits),
//! `false` if the client has exceeded their rate limit.

use dashmap::DashMap;
use redns_core::plugin::PluginResult;
use redns_core::{Context, Matcher};
use std::net::IpAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// Rate limiter configuration.
#[derive(Debug, Clone)]
pub struct RateLimiterConfig {
    /// Allowed queries per second per client.
    pub qps: f64,
    /// Burst size (max tokens).
    pub burst: u32,
    /// IPv4 subnet mask for grouping clients (default 32 = per-IP).
    pub mask4: u8,
    /// IPv6 subnet mask for grouping clients (default 48).
    pub mask6: u8,
}

impl Default for RateLimiterConfig {
    fn default() -> Self {
        Self {
            qps: 20.0,
            burst: 40,
            mask4: 32,
            mask6: 48,
        }
    }
}

impl RateLimiterConfig {
    pub fn from_str_args(s: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let s = s.trim();
        if s.is_empty() {
            return Ok(Self::default());
        }
        let fields: Vec<&str> = s.split_whitespace().collect();
        let qps: f64 = fields.first().and_then(|v| v.parse().ok()).unwrap_or(20.0);
        let burst: u32 = fields.get(1).and_then(|v| v.parse().ok()).unwrap_or(40);
        Ok(Self {
            qps,
            burst,
            ..Default::default()
        })
    }
}

/// A single token bucket for one client subnet.
struct Bucket {
    tokens: AtomicU64, // tokens * 1000 (fixed-point)
    last_refill: std::sync::Mutex<Instant>,
}

impl Bucket {
    fn new(burst: u32) -> Self {
        Self {
            tokens: AtomicU64::new((burst as u64) * 1000),
            last_refill: std::sync::Mutex::new(Instant::now()),
        }
    }
}

/// Per-IP rate limiter matcher.
pub struct RateLimiter {
    config: RateLimiterConfig,
    buckets: DashMap<IpAddr, Bucket>,
}

impl RateLimiter {
    pub fn new(config: RateLimiterConfig) -> Self {
        Self {
            config,
            buckets: DashMap::new(),
        }
    }

    /// Mask an IP to the configured subnet.
    fn mask_ip(&self, ip: IpAddr) -> IpAddr {
        match ip {
            IpAddr::V4(v4) => {
                let bits = u32::from(v4);
                let mask = if self.config.mask4 >= 32 {
                    u32::MAX
                } else {
                    u32::MAX << (32 - self.config.mask4)
                };
                IpAddr::V4((bits & mask).into())
            }
            IpAddr::V6(v6) => {
                let bits = u128::from(v6);
                let mask = if self.config.mask6 >= 128 {
                    u128::MAX
                } else {
                    u128::MAX << (128 - self.config.mask6)
                };
                IpAddr::V6((bits & mask).into())
            }
        }
    }

    /// Check if a request from the given IP is allowed.
    fn allow(&self, ip: IpAddr) -> bool {
        let masked = self.mask_ip(ip);
        let entry = self
            .buckets
            .entry(masked)
            .or_insert_with(|| Bucket::new(self.config.burst));
        let bucket = entry.value();

        // Refill tokens based on elapsed time.
        {
            let mut last = bucket.last_refill.lock().unwrap();
            let now = Instant::now();
            let elapsed = now.duration_since(*last).as_secs_f64();
            let refill = (elapsed * self.config.qps * 1000.0) as u64;
            if refill > 0 {
                let max_tokens = (self.config.burst as u64) * 1000;
                let current = bucket.tokens.load(Ordering::Relaxed);
                let new_tokens = (current + refill).min(max_tokens);
                bucket.tokens.store(new_tokens, Ordering::Relaxed);
                *last = now;
            }
        }

        // Try to consume one token.
        let current = bucket.tokens.load(Ordering::Relaxed);
        if current >= 1000 {
            bucket.tokens.fetch_sub(1000, Ordering::Relaxed);
            true
        } else {
            false
        }
    }
}

impl Matcher for RateLimiter {
    fn match_ctx(&self, ctx: &Context) -> PluginResult<bool> {
        match ctx.server_meta.client_addr {
            Some(addr) => Ok(self.allow(addr)),
            None => Ok(true), // No client addr → allow.
        }
    }
}

// RateLimiter is Send + Sync because DashMap is Send + Sync.

#[cfg(test)]
mod tests {
    use super::*;
    use hickory_proto::op::{Message, MessageType, OpCode, Query};
    use hickory_proto::rr::{Name, RecordType};

    fn make_ctx_with_client(ip: &str) -> Context {
        let mut msg = Message::new();
        msg.set_id(1)
            .set_message_type(MessageType::Query)
            .set_op_code(OpCode::Query);
        msg.add_query({
            let mut q = Query::new();
            q.set_name(Name::from_ascii("example.com.").unwrap())
                .set_query_type(RecordType::A);
            q
        });
        let mut ctx = Context::new(msg);
        ctx.server_meta.client_addr = Some(ip.parse().unwrap());
        ctx
    }

    #[test]
    fn allows_within_burst() {
        let rl = RateLimiter::new(RateLimiterConfig {
            qps: 10.0,
            burst: 5,
            ..Default::default()
        });
        let ctx = make_ctx_with_client("10.0.0.1");
        // First 5 requests should be allowed (burst).
        for _ in 0..5 {
            assert!(rl.match_ctx(&ctx).unwrap());
        }
        // 6th should be denied.
        assert!(!rl.match_ctx(&ctx).unwrap());
    }

    #[test]
    fn no_client_addr_allows() {
        let rl = RateLimiter::new(RateLimiterConfig::default());
        let mut msg = Message::new();
        msg.set_id(1)
            .set_message_type(MessageType::Query)
            .set_op_code(OpCode::Query);
        msg.add_query({
            let mut q = Query::new();
            q.set_name(Name::from_ascii("example.com.").unwrap())
                .set_query_type(RecordType::A);
            q
        });
        let ctx = Context::new(msg);
        assert!(rl.match_ctx(&ctx).unwrap());
    }

    #[test]
    fn from_str_args_defaults() {
        let cfg = RateLimiterConfig::from_str_args("").unwrap();
        assert!((cfg.qps - 20.0).abs() < 0.01);
        assert_eq!(cfg.burst, 40);
    }

    #[test]
    fn different_ips_independent() {
        let rl = RateLimiter::new(RateLimiterConfig {
            qps: 10.0,
            burst: 2,
            ..Default::default()
        });
        let ctx1 = make_ctx_with_client("10.0.0.1");
        let ctx2 = make_ctx_with_client("10.0.0.2");
        // Exhaust client 1.
        assert!(rl.match_ctx(&ctx1).unwrap());
        assert!(rl.match_ctx(&ctx1).unwrap());
        assert!(!rl.match_ctx(&ctx1).unwrap());
        // Client 2 still has tokens.
        assert!(rl.match_ctx(&ctx2).unwrap());
    }
}
