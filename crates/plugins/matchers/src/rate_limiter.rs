// Copyright (C) 2026, Harlan Wei
//
// This file is part of redns.

//! Per-IP token-bucket rate limiter.
//!
//! Returns `true` if the request is allowed (within limits),
//! `false` if the client has exceeded their rate limit.

use dashmap::DashMap;
use parking_lot::Mutex as StdMutex;
use redns_core::plugin::PluginResult;
use redns_core::{Context, Matcher};
use std::net::IpAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

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
        // Reject a non-positive or non-finite qps at config time. A zero qps
        // means the bucket never refills (every client permanently blocked
        // after the burst) and also feeds `Duration::from_secs_f64` an infinite
        // value; failing fast here surfaces the misconfiguration clearly.
        if !(qps.is_finite() && qps > 0.0) {
            return Err(format!("rate_limiter: qps must be a positive number, got '{qps}'").into());
        }
        if burst == 0 {
            return Err("rate_limiter: burst must be greater than 0".into());
        }
        Ok(Self {
            qps,
            burst,
            ..Default::default()
        })
    }
}

/// A single token bucket for one client subnet.
struct Bucket {
    tokens: AtomicU64,
    last_refill: StdMutex<Instant>,
}

impl Bucket {
    fn new(burst: u32) -> Self {
        Self {
            tokens: AtomicU64::new((burst as u64) * 1000),
            last_refill: StdMutex::new(Instant::now()),
        }
    }
}

/// How often (in `allow()` calls) to run a stale-bucket eviction sweep.
const EVICT_INTERVAL: u64 = 256;

/// Per-IP rate limiter matcher.
pub struct RateLimiter {
    config: RateLimiterConfig,
    buckets: DashMap<IpAddr, Bucket>,
    /// Monotonic counter used to trigger periodic eviction.
    call_counter: AtomicU64,
    /// Buckets idle longer than this are evicted.
    stale_threshold: Duration,
}

impl RateLimiter {
    pub fn new(config: RateLimiterConfig) -> Self {
        // A bucket refills fully in `burst / qps` seconds.
        // Consider it stale after 2× that duration (minimum 60 s).
        //
        // Guard against a non-positive or non-finite qps: `burst / 0.0` is
        // `inf`, and `Duration::from_secs_f64(inf)` panics. Fall back to a sane
        // finite staleness window so a misconfigured `qps` cannot crash the
        // process at construction time.
        let refill_secs = if config.qps > 0.0 {
            (config.burst as f64) / config.qps
        } else {
            0.0
        };
        let stale_secs = if refill_secs.is_finite() {
            (refill_secs * 2.0).max(60.0)
        } else {
            60.0
        };
        Self {
            stale_threshold: Duration::from_secs_f64(stale_secs),
            config,
            buckets: DashMap::new(),
            call_counter: AtomicU64::new(0),
        }
    }

    /// Mask an IP to the configured subnet.
    fn mask_ip(&self, ip: IpAddr) -> IpAddr {
        match ip {
            IpAddr::V4(v4) => {
                let bits = u32::from(v4);
                // Guard the shift: `mask4 == 0` would compute `u32::MAX << 32`,
                // which is a shift overflow (panics in debug). A 0 mask means
                // "group everything", i.e. mask of 0 bits.
                let mask = if self.config.mask4 >= 32 {
                    u32::MAX
                } else if self.config.mask4 == 0 {
                    0
                } else {
                    u32::MAX << (32 - self.config.mask4)
                };
                IpAddr::V4((bits & mask).into())
            }
            IpAddr::V6(v6) => {
                let bits = u128::from(v6);
                let mask = if self.config.mask6 >= 128 {
                    u128::MAX
                } else if self.config.mask6 == 0 {
                    0
                } else {
                    u128::MAX << (128 - self.config.mask6)
                };
                IpAddr::V6((bits & mask).into())
            }
        }
    }

    /// Remove buckets that have not been accessed recently.
    fn evict_stale(&self) {
        let now = Instant::now();
        self.buckets.retain(|_, bucket| {
            let last = bucket.last_refill.lock();
            now.duration_since(*last) < self.stale_threshold
        });
    }

    /// Check if a request from the given IP is allowed.
    fn allow(&self, ip: IpAddr) -> bool {
        // Periodic eviction of stale buckets.
        let n = self.call_counter.fetch_add(1, Ordering::Relaxed);
        if n % EVICT_INTERVAL == 0 {
            self.evict_stale();
        }

        let masked = self.mask_ip(ip);
        let entry = self
            .buckets
            .entry(masked)
            .or_insert_with(|| Bucket::new(self.config.burst));
        let bucket = entry.value();

        // Refill and consume are performed together under the per-bucket lock.
        // This serializes the read-modify-write so two concurrent callers cannot
        // both observe `tokens >= 1000`, both subtract, and underflow the
        // unsigned counter (which would wrap to a huge value and effectively
        // disable rate limiting for that subnet). Saturating arithmetic guards
        // against underflow regardless of how this is reached.
        let mut last = bucket.last_refill.lock();
        let now = Instant::now();
        let elapsed = now.duration_since(*last).as_secs_f64();
        let refill = (elapsed * self.config.qps * 1000.0) as u64;
        let max_tokens = (self.config.burst as u64) * 1000;

        let current = bucket.tokens.load(Ordering::Relaxed);
        let mut tokens = if refill > 0 {
            *last = now;
            current.saturating_add(refill).min(max_tokens)
        } else {
            current
        };

        // Try to consume one token.
        let allowed = if tokens >= 1000 {
            tokens -= 1000;
            true
        } else {
            false
        };

        bucket.tokens.store(tokens, Ordering::Relaxed);
        allowed
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
    fn from_str_args_rejects_non_positive_qps() {
        // qps = 0 previously produced `burst / 0.0 = inf`, and
        // `Duration::from_secs_f64(inf)` panics at construction time. The parser
        // must reject it instead of letting a bad config crash the process.
        assert!(RateLimiterConfig::from_str_args("0").is_err());
        assert!(RateLimiterConfig::from_str_args("-1").is_err());
    }

    #[test]
    fn from_str_args_rejects_zero_burst() {
        assert!(RateLimiterConfig::from_str_args("20 0").is_err());
    }

    #[test]
    fn new_does_not_panic_on_zero_qps() {
        // Even if a zero/negative qps reaches `new` directly (the fields are
        // public), construction must not panic.
        let rl = RateLimiter::new(RateLimiterConfig {
            qps: 0.0,
            burst: 40,
            ..Default::default()
        });
        // And it stays usable: the initial burst is still honored.
        assert!(rl.allow("10.0.0.1".parse().unwrap()));
    }

    #[test]
    fn mask_zero_does_not_panic() {
        // mask4/mask6 == 0 means "group everything"; the shift must not overflow.
        let rl = RateLimiter::new(RateLimiterConfig {
            qps: 10.0,
            burst: 5,
            mask4: 0,
            mask6: 0,
        });
        // Two different IPs collapse to the same (zero) masked key.
        assert_eq!(
            rl.mask_ip("10.0.0.1".parse().unwrap()),
            rl.mask_ip("192.168.1.1".parse().unwrap())
        );
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

    #[test]
    fn concurrent_consume_never_exceeds_burst() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        // A large burst with many threads hammering the same subnet must never
        // hand out more than `burst` tokens. The previous non-atomic consume
        // could underflow the token counter and grant effectively unlimited
        // tokens; this guards against that regression.
        const BURST: u32 = 1000;
        const THREADS: usize = 16;
        const PER_THREAD: usize = 500;

        let rl = Arc::new(RateLimiter::new(RateLimiterConfig {
            // Very low qps so refill during the test is negligible.
            qps: 0.001,
            burst: BURST,
            ..Default::default()
        }));
        let allowed = Arc::new(AtomicUsize::new(0));

        let ip: IpAddr = "10.0.0.1".parse().unwrap();
        let handles: Vec<_> = (0..THREADS)
            .map(|_| {
                let rl = rl.clone();
                let allowed = allowed.clone();
                std::thread::spawn(move || {
                    for _ in 0..PER_THREAD {
                        if rl.allow(ip) {
                            allowed.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }

        // We may grant a few extra tokens due to refill, but never wildly more
        // than the burst — certainly nowhere near THREADS * PER_THREAD.
        let total = allowed.load(Ordering::Relaxed);
        assert!(
            total <= BURST as usize + THREADS,
            "granted {total} tokens, expected <= {}",
            BURST as usize + THREADS
        );
    }
}
