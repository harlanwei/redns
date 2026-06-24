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

struct BranchOutcome {
    response: Option<Message>,
    selected_upstream: Option<Arc<UpstreamWrapper>>,
}

fn branch_outcome_from_ctx(ctx: &Context) -> BranchOutcome {
    BranchOutcome {
        response: ctx.response().cloned(),
        selected_upstream: ctx
            .get_value::<Arc<UpstreamWrapper>>(KV_SELECTED_UPSTREAM)
            .cloned(),
    }
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
        let rcode = resp.response_code();
        if rcode == ResponseCode::Refused {
            return false;
        }
        ctx.set_response(Some(resp));
        if let Some(upstream) = outcome.selected_upstream {
            ctx.store_value(KV_SELECTED_UPSTREAM, upstream);
        }
        return true;
    }
    false
}

#[async_trait::async_trait]
impl Executable for Fallback {
    async fn exec(&self, ctx: &mut Context) -> PluginResult<()> {
        let start = std::time::Instant::now();
        let qname = ctx
            .question()
            .map(|q| q.name().to_ascii())
            .unwrap_or_default();
        debug!(threshold = ?self.threshold, always_standby = self.always_standby, "fallback: starting");
        // Create a fresh context for each branch from the same query.
        let query = ctx.query().clone();
        let mut ctx_primary = Context::new(query.clone());
        ctx_primary.server_meta = ctx.server_meta.clone();
        let mut ctx_secondary = Context::new(query);
        ctx_secondary.server_meta = ctx.server_meta.clone();

        let primary = self.primary.clone();
        let secondary = self.secondary.clone();
        let threshold = self.threshold;
        let always_standby = self.always_standby;

        // Spawn primary task.
        let qname_primary = qname.clone();
        let primary_handle = tokio::spawn(async move {
            match primary.exec(&mut ctx_primary).await {
                Ok(()) => branch_outcome_from_ctx(&ctx_primary),
                Err(e) => {
                    warn!(error = %e, qname = %qname_primary, "fallback: primary failed");
                    BranchOutcome {
                        response: None,
                        selected_upstream: None,
                    }
                }
            }
        });

        if always_standby {
            // Start secondary immediately in parallel.
            let qname_secondary = qname.clone();
            let mut secondary_handle = tokio::spawn(async move {
                match secondary.exec(&mut ctx_secondary).await {
                    Ok(()) => branch_outcome_from_ctx(&ctx_secondary),
                    Err(e) => {
                        warn!(error = %e, qname = %qname_secondary, "fallback: secondary failed");
                        BranchOutcome {
                            response: None,
                            selected_upstream: None,
                        }
                    }
                }
            });

            // Wait for primary with threshold timeout.
            let mut primary_handle = primary_handle;
            match tokio::time::timeout(threshold, &mut primary_handle).await {
                Ok(Ok(outcome)) => {
                    // Primary responded within threshold — use it.
                    if apply_outcome(ctx, outcome) {
                        debug!(elapsed = ?start.elapsed(), "fallback: primary responded within threshold");
                        return Ok(());
                    }
                    // Primary finished but no response or panicked — use secondary.
                    debug!(elapsed = ?start.elapsed(), "fallback: primary done but no response, waiting for secondary");
                    match secondary_handle.await {
                        Ok(outcome) => {
                            if apply_outcome(ctx, outcome) {
                                return Ok(());
                            }
                        }
                        Err(e) => {
                            warn!(error = %e, qname = %qname, "fallback: secondary join failed");
                        }
                    }
                }
                Ok(Err(e)) => {
                    warn!(error = %e, qname = %qname, "fallback: primary join failed");
                    match secondary_handle.await {
                        Ok(outcome) => {
                            if apply_outcome(ctx, outcome) {
                                return Ok(());
                            }
                        }
                        Err(e) => {
                            warn!(error = %e, qname = %qname, "fallback: secondary join failed");
                        }
                    }
                }
                Err(_) => {
                    // Primary exceeded threshold — race primary vs secondary.
                    debug!(elapsed = ?start.elapsed(), "fallback: primary exceeded threshold, racing both");
                    tokio::select! {
                        result = &mut primary_handle => {
                            match result {
                                Ok(outcome) => {
                                    if apply_outcome(ctx, outcome) {
                                        debug!(elapsed = ?start.elapsed(), "fallback: primary won race");
                                        return Ok(());
                                    }
                                }
                                Err(e) => {
                                    warn!(error = %e, qname = %qname, "fallback: primary join failed");
                                }
                            }

                            match secondary_handle.await {
                                Ok(outcome) => {
                                    if apply_outcome(ctx, outcome) {
                                        return Ok(());
                                    }
                                }
                                Err(e) => {
                                    warn!(error = %e, qname = %qname, "fallback: secondary join failed");
                                }
                            }
                        }
                        result = &mut secondary_handle => {
                            match result {
                                Ok(outcome) => {
                                    if apply_outcome(ctx, outcome) {
                                        debug!(elapsed = ?start.elapsed(), "fallback: secondary won race");
                                        return Ok(());
                                    }
                                }
                                Err(e) => {
                                    warn!(error = %e, qname = %qname, "fallback: secondary join failed");
                                }
                            }

                            match primary_handle.await {
                                Ok(outcome) => {
                                    if apply_outcome(ctx, outcome) {
                                        return Ok(());
                                    }
                                }
                                Err(e) => {
                                    warn!(error = %e, qname = %qname, "fallback: primary join failed");
                                }
                            }
                        }
                    }
                }
            }
        } else {
            // Wait for primary with threshold timeout.
            let primary_result = tokio::time::timeout(threshold, primary_handle).await;
            match primary_result {
                Ok(Ok(outcome)) => {
                    if apply_outcome(ctx, outcome) {
                        debug!(elapsed = ?start.elapsed(), "fallback: primary responded within threshold");
                        return Ok(());
                    }
                }
                Ok(Err(e)) => {
                    warn!(error = %e, qname = %qname, "fallback: primary join failed");
                }
                Err(_) => {}
            }

            // Primary failed or timed out — run secondary.
            debug!(elapsed = ?start.elapsed(), "fallback: primary timed out or failed, trying secondary");
            match secondary.exec(&mut ctx_secondary).await {
                Ok(()) => {
                    if apply_outcome(ctx, branch_outcome_from_ctx(&ctx_secondary)) {
                        return Ok(());
                    }
                }
                Err(e) => {
                    warn!(error = %e, qname = %qname, "fallback: secondary failed");
                }
            }
        }

        Err("fallback: no valid response from primary or secondary".into())
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
        msg
    }

    /// Builds a response with the given rcode for `query`.
    fn resp_with_rcode(query: &Message, rcode: ResponseCode) -> Message {
        let mut resp = Message::new();
        resp.set_id(query.id());
        resp.set_message_type(MessageType::Response);
        resp.set_response_code(rcode);
        if let Some(q) = query.queries().first() {
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
            resp.set_id(ctx.query().id());
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
            let mut resp = Message::new();
            resp.set_id(ctx.query().id());
            resp.set_message_type(MessageType::Response);
            resp.set_response_code(ResponseCode::NoError);
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
        assert_eq!(resp.response_code(), ResponseCode::ServFail);
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
        assert_eq!(resp.response_code(), ResponseCode::NoError);
    }
}
