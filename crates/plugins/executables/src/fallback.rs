// Copyright (C) 2026, Harlan Wei
//
// This file is part of redns.

//! Fallback executor — races a primary against a secondary executor.
//!
//! Behavior:
//! - Starts the primary executor immediately.
//! - If the primary fails or exceeds the `threshold` (default 500ms), the
//!   secondary is started.
//! - If `always_standby` is true, the secondary starts immediately alongside
//!   the primary but only its result is used if the primary fails/times out.
//! - Uses the first valid (non-None) response.
//! - A SERVFAIL is treated as a terminal answer, not retried against the
//!   secondary: it is how a validating upstream rejects a DNSSEC-bogus
//!   response, and re-resolving it would defeat DNSSEC and leak the qname.
//!   Only `Refused` (and a missing response) fall through to the other branch.

use hickory_proto::op::{Message, ResponseCode};
use redns_core::context::{Context, KV_SELECTED_UPSTREAM};
use redns_core::plugin::{Executable, PluginResult};
use redns_core::upstream::UpstreamWrapper;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, warn};

/// Default fallback threshold.
const DEFAULT_THRESHOLD: Duration = Duration::from_millis(500);

/// Fallback executor.
pub struct Fallback {
    primary: Arc<dyn Executable>,
    secondary: Arc<dyn Executable>,
    threshold: Duration,
    always_standby: bool,
}

/// YAML args for fallback plugin.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct FallbackArgs {
    /// Tag of primary executable.
    pub primary: String,
    /// Tag of secondary executable.
    pub secondary: String,
    /// Threshold in milliseconds before triggering secondary.
    #[serde(default)]
    pub threshold: u64,
    /// If true, secondary always runs in parallel.
    #[serde(default)]
    pub always_standby: bool,
}

impl Fallback {
    /// Create a new fallback executor with resolved primary and secondary executables.
    ///
    /// If `threshold` is zero, [`DEFAULT_THRESHOLD`] (500 ms) is used.
    pub fn new(
        primary: Arc<dyn Executable>,
        secondary: Arc<dyn Executable>,
        threshold: Duration,
        always_standby: bool,
    ) -> Self {
        let threshold = if threshold.is_zero() {
            DEFAULT_THRESHOLD
        } else {
            threshold
        };
        Self {
            primary,
            secondary,
            threshold,
            always_standby,
        }
    }
}

#[derive(Default)]
struct BranchOutcome {
    response: Option<Message>,
    response_wire: Option<Vec<u8>>,
    selected_upstream: Option<Arc<UpstreamWrapper>>,
}

fn branch_outcome_from_ctx(ctx: &Context) -> BranchOutcome {
    let response_wire = ctx.response_wire().map(ToOwned::to_owned);
    BranchOutcome {
        response: if response_wire.is_some() {
            None
        } else {
            ctx.response().cloned()
        },
        response_wire,
        selected_upstream: ctx
            .get_value::<Arc<UpstreamWrapper>>(KV_SELECTED_UPSTREAM)
            .cloned(),
    }
}

fn wire_rcode(resp_wire: &[u8]) -> Option<u16> {
    resp_wire.get(3).map(|flags| (flags & 0x0f) as u16)
}

/// Evaluates a branch's response and, if usable, writes it into `ctx`.
///
/// Returns `true` (accepted) when the response is usable and has been written
/// into `ctx`, so the fallback can stop. Returns `false` (rejected) when there
/// is no response or the response warrants trying the other branch.
///
/// A SERVFAIL is *accepted* — treated as a terminal answer, not retried against
/// the secondary. SERVFAIL is how a validating upstream rejects a DNSSEC-bogus
/// answer, and re-resolving it (via the secondary or the outer best-effort
/// system-DNS fallback) would serve a record the upstream refused to vouch for,
/// defeating DNSSEC and leaking the qname. Only `Refused` (and a missing
/// response) fall through to the other branch.
fn apply_outcome(ctx: &mut Context, outcome: BranchOutcome) -> bool {
    if let Some(resp) = outcome.response {
        let rcode = resp.response_code;
        if rcode == ResponseCode::Refused {
            return false;
        }
        ctx.set_response(Some(resp));
        if let Some(upstream) = outcome.selected_upstream {
            ctx.store_value(KV_SELECTED_UPSTREAM, upstream);
        }
        return true;
    }
    if let Some(wire) = outcome.response_wire {
        if wire_rcode(&wire) == Some(u16::from(ResponseCode::Refused)) {
            return false;
        }
        ctx.set_response_wire(Some(wire));
        if let Some(upstream) = outcome.selected_upstream {
            ctx.store_value(KV_SELECTED_UPSTREAM, upstream);
        }
        return true;
    }
    false
}

/// Own branch tasks so a completed or cancelled fallback never detaches work.
struct BranchTask {
    name: &'static str,
    handle: tokio::task::JoinHandle<BranchOutcome>,
}

impl BranchTask {
    fn spawn(name: &'static str, exec: Arc<dyn Executable>, parent: &Context) -> Self {
        // Fork, don't rebuild: Context::new would re-run ingress EDNS
        // normalization and shrink/strip the parent's already-normalized OPT
        // (advertised payload size, ECS options). fork_from preserves the
        // logical query and request settings verbatim.
        let mut ctx = Context::fork_from(parent);
        let handle = tokio::spawn(async move {
            match exec.exec(&mut ctx).await {
                Ok(()) => branch_outcome_from_ctx(&ctx),
                Err(e) => {
                    warn!(branch = name, error = %e, "fallback branch failed");
                    BranchOutcome::default()
                }
            }
        });
        Self { name, handle }
    }

    async fn join(&mut self) -> BranchOutcome {
        match (&mut self.handle).await {
            Ok(outcome) => outcome,
            Err(e) => {
                warn!(branch = self.name, error = %e, "fallback branch join failed");
                BranchOutcome::default()
            }
        }
    }
}

impl Drop for BranchTask {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

#[async_trait::async_trait]
impl Executable for Fallback {
    async fn exec(&self, ctx: &mut Context) -> PluginResult<()> {
        debug!(threshold = ?self.threshold, always_standby = self.always_standby, "fallback: starting");
        let mut primary = BranchTask::spawn("primary", self.primary.clone(), ctx);
        let mut secondary = self
            .always_standby
            .then(|| BranchTask::spawn("secondary", self.secondary.clone(), ctx));

        // Prefer the primary during the threshold window. Borrowing its handle
        // keeps it available for the race if the threshold expires.
        if let Ok(outcome) = tokio::time::timeout(self.threshold, primary.join()).await {
            if apply_outcome(ctx, outcome) {
                return Ok(());
            }
            let secondary = secondary.get_or_insert_with(|| {
                BranchTask::spawn("secondary", self.secondary.clone(), ctx)
            });
            return if apply_outcome(ctx, secondary.join().await) {
                Ok(())
            } else {
                Err("fallback: no valid response from primary or secondary".into())
            };
        }

        let mut secondary = secondary
            .unwrap_or_else(|| BranchTask::spawn("secondary", self.secondary.clone(), ctx));
        let accepted = tokio::select! {
            outcome = primary.join() => {
                apply_outcome(ctx, outcome) || apply_outcome(ctx, secondary.join().await)
            }
            outcome = secondary.join() => {
                apply_outcome(ctx, outcome) || apply_outcome(ctx, primary.join().await)
            }
        };
        if accepted {
            Ok(())
        } else {
            Err("fallback: no valid response from primary or secondary".into())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hickory_proto::op::{MessageType, OpCode, Query, ResponseCode};
    use hickory_proto::rr::{Name, RecordType};
    use redns_core::plugin::Executable;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn make_query() -> Message {
        let mut msg = Message::new(1, MessageType::Query, OpCode::Query);
        msg.add_query({
            let mut q = Query::new();
            q.set_name(Name::from_ascii("example.com.").unwrap())
                .set_query_type(RecordType::A);
            q
        });
        msg
    }

    /// Builds a response with the given rcode for `query`.
    fn resp_with_rcode(query: &Message, rcode: ResponseCode) -> Message {
        let mut resp = Message::response(query.id, OpCode::Query);
        resp.metadata.response_code = rcode;
        if let Some(q) = query.queries.first() {
            resp.add_query(q.clone());
        }
        resp
    }

    /// Executable that sets a fixed pre-built response on the context and
    /// counts how many times it ran.
    struct FixedResp {
        resp: Message,
        calls: Arc<AtomicUsize>,
    }
    #[async_trait::async_trait]
    impl Executable for FixedResp {
        async fn exec(&self, ctx: &mut Context) -> PluginResult<()> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            let mut resp = self.resp.clone();
            resp.metadata.id = ctx.query().id;
            ctx.set_response(Some(resp));
            Ok(())
        }
    }

    /// Executable that returns a NOERROR response with an A record, counting
    /// invocations.
    struct OkResp {
        calls: Arc<AtomicUsize>,
    }
    #[async_trait::async_trait]
    impl Executable for OkResp {
        async fn exec(&self, ctx: &mut Context) -> PluginResult<()> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            let q = ctx.question().unwrap().clone();
            let mut resp = Message::response(ctx.query().id, OpCode::Query);
        resp.metadata.response_code = ResponseCode::NoError;
            resp.add_query(q);
            ctx.set_response(Some(resp));
            Ok(())
        }
    }

    /// A SERVFAIL from the primary must be adopted as a terminal answer: the
    /// secondary is never consulted, and the SERVFAIL flows back to the caller
    /// (rather than the chain erroring out). SERVFAIL is how a validating
    /// upstream rejects a DNSSEC-bogus response, so re-resolving it via the
    /// secondary would defeat DNSSEC.
    #[tokio::test]
    async fn servfail_primary_is_terminal() {
        let q = make_query();
        let primary_calls = Arc::new(AtomicUsize::new(0));
        let secondary_calls = Arc::new(AtomicUsize::new(0));
        let primary: Arc<dyn Executable> = Arc::new(FixedResp {
            resp: resp_with_rcode(&q, ResponseCode::ServFail),
            calls: primary_calls.clone(),
        });
        let secondary: Arc<dyn Executable> = Arc::new(OkResp {
            calls: secondary_calls.clone(),
        });
        let fb = Fallback::new(primary, secondary, Duration::from_millis(0), false);

        let mut ctx = Context::new(q);
        // It must succeed (not return the "no valid response" error).
        fb.exec(&mut ctx).await.expect("SERVFAIL should be adopted");
        assert_eq!(primary_calls.load(Ordering::Relaxed), 1);
        assert_eq!(
            secondary_calls.load(Ordering::Relaxed),
            0,
            "secondary must not be consulted when primary is a SERVFAIL"
        );
        let resp = ctx.response().expect("response set");
        assert_eq!(resp.response_code, ResponseCode::ServFail);
    }

    /// A REFUSED from the primary is NOT terminal — the secondary is
    /// consulted, and its NOERROR is adopted.
    #[tokio::test]
    async fn refused_primary_falls_through_to_secondary() {
        let q = make_query();
        let primary_calls = Arc::new(AtomicUsize::new(0));
        let secondary_calls = Arc::new(AtomicUsize::new(0));
        let primary: Arc<dyn Executable> = Arc::new(FixedResp {
            resp: resp_with_rcode(&q, ResponseCode::Refused),
            calls: primary_calls.clone(),
        });
        let secondary: Arc<dyn Executable> = Arc::new(OkResp {
            calls: secondary_calls.clone(),
        });
        let fb = Fallback::new(primary, secondary, Duration::from_millis(0), false);

        let mut ctx = Context::new(q);
        fb.exec(&mut ctx).await.expect("secondary NOERROR adopted");
        assert_eq!(primary_calls.load(Ordering::Relaxed), 1);
        assert_eq!(
            secondary_calls.load(Ordering::Relaxed),
            1,
            "secondary must be tried after a REFUSED primary"
        );
        let resp = ctx.response().expect("response set");
        assert_eq!(resp.response_code, ResponseCode::NoError);
    }

    struct DelayedResp {
        delay: Duration,
        rcode: ResponseCode,
        dropped: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl Executable for DelayedResp {
        async fn exec(&self, ctx: &mut Context) -> PluginResult<()> {
            struct DropCounter(Arc<AtomicUsize>);
            impl Drop for DropCounter {
                fn drop(&mut self) {
                    self.0.fetch_add(1, Ordering::SeqCst);
                }
            }
            let _guard = DropCounter(self.dropped.clone());
            tokio::time::sleep(self.delay).await;
            ctx.set_response(Some(resp_with_rcode(ctx.query(), self.rcode)));
            Ok(())
        }
    }

    fn delayed(ms: u64, rcode: ResponseCode, dropped: Arc<AtomicUsize>) -> Arc<dyn Executable> {
        Arc::new(DelayedResp {
            delay: Duration::from_millis(ms),
            rcode,
            dropped,
        })
    }

    #[tokio::test(start_paused = true)]
    async fn primary_can_win_after_threshold_and_cancels_secondary() {
        let secondary_dropped = Arc::new(AtomicUsize::new(0));
        let fb = Fallback::new(
            delayed(40, ResponseCode::NoError, Arc::new(AtomicUsize::new(0))),
            delayed(150, ResponseCode::NXDomain, secondary_dropped.clone()),
            Duration::from_millis(10),
            false,
        );
        let mut ctx = Context::new(make_query());
        let start = tokio::time::Instant::now();
        fb.exec(&mut ctx).await.unwrap();
        assert_eq!(ctx.response().unwrap().response_code, ResponseCode::NoError);
        assert!(start.elapsed() < Duration::from_millis(100));
        tokio::task::yield_now().await;
        assert_eq!(secondary_dropped.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn secondary_can_win_race_and_cancels_primary() {
        let primary_dropped = Arc::new(AtomicUsize::new(0));
        let fb = Fallback::new(
            delayed(150, ResponseCode::NoError, primary_dropped.clone()),
            delayed(10, ResponseCode::NXDomain, Arc::new(AtomicUsize::new(0))),
            Duration::from_millis(10),
            false,
        );
        let mut ctx = Context::new(make_query());
        fb.exec(&mut ctx).await.unwrap();
        assert_eq!(ctx.response().unwrap().response_code, ResponseCode::NXDomain);
        tokio::task::yield_now().await;
        assert_eq!(primary_dropped.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn refused_race_winner_still_waits_for_other_branch() {
        let fb = Fallback::new(
            delayed(40, ResponseCode::NoError, Arc::new(AtomicUsize::new(0))),
            delayed(10, ResponseCode::Refused, Arc::new(AtomicUsize::new(0))),
            Duration::from_millis(10),
            false,
        );
        let mut ctx = Context::new(make_query());
        fb.exec(&mut ctx).await.unwrap();
        assert_eq!(ctx.response().unwrap().response_code, ResponseCode::NoError);
    }

    #[tokio::test(start_paused = true)]
    async fn standby_secondary_does_not_override_primary_within_threshold() {
        let fb = Fallback::new(
            delayed(40, ResponseCode::ServFail, Arc::new(AtomicUsize::new(0))),
            delayed(1, ResponseCode::NoError, Arc::new(AtomicUsize::new(0))),
            Duration::from_millis(100),
            true,
        );
        let mut ctx = Context::new(make_query());
        fb.exec(&mut ctx).await.unwrap();
        assert_eq!(ctx.response().unwrap().response_code, ResponseCode::ServFail);
    }

    #[tokio::test(start_paused = true)]
    async fn cancelling_fallback_cancels_both_branches() {
        let dropped = Arc::new(AtomicUsize::new(0));
        let fb = Fallback::new(
            delayed(1000, ResponseCode::NoError, dropped.clone()),
            delayed(1000, ResponseCode::NoError, dropped.clone()),
            Duration::from_millis(10),
            false,
        );
        let mut ctx = Context::new(make_query());
        assert!(tokio::time::timeout(Duration::from_millis(50), fb.exec(&mut ctx))
            .await
            .is_err());
        tokio::task::yield_now().await;
        assert_eq!(dropped.load(Ordering::SeqCst), 2);
    }
}
