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

use redns_core::context::Context;
use redns_core::plugin::{Executable, PluginResult};
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

#[async_trait::async_trait]
impl Executable for Fallback {
    async fn exec(&self, ctx: &mut Context) -> PluginResult<()> {
        let start = std::time::Instant::now();
        debug!(threshold = ?self.threshold, always_standby = self.always_standby, "fallback: starting");
        // Create a fresh context for each branch from the same query.
        let query = ctx.query().clone();
        let mut ctx_primary = Context::new(query.clone());
        let mut ctx_secondary = Context::new(query);

        let primary = self.primary.clone();
        let secondary = self.secondary.clone();
        let threshold = self.threshold;
        let always_standby = self.always_standby;

        // Spawn primary task.
        let primary_handle = tokio::spawn(async move {
            match primary.exec(&mut ctx_primary).await {
                Ok(()) => ctx_primary.response().cloned(),
                Err(e) => {
                    warn!(error = %e, "fallback: primary failed");
                    None
                }
            }
        });

        if always_standby {
            // Start secondary immediately in parallel.
            let mut secondary_handle = tokio::spawn(async move {
                match secondary.exec(&mut ctx_secondary).await {
                    Ok(()) => ctx_secondary.response().cloned(),
                    Err(e) => {
                        warn!(error = %e, "fallback: secondary failed");
                        None
                    }
                }
            });

            // Wait for primary with threshold timeout.
            let mut primary_handle = primary_handle;
            match tokio::time::timeout(threshold, &mut primary_handle).await {
                Ok(Ok(Some(resp))) => {
                    // Primary responded within threshold — use it.
                    debug!(elapsed = ?start.elapsed(), "fallback: primary responded within threshold");
                    ctx.set_response(Some(resp));
                    return Ok(());
                }
                Ok(Ok(None)) | Ok(Err(_)) => {
                    // Primary finished but no response or panicked — use secondary.
                    debug!(elapsed = ?start.elapsed(), "fallback: primary done but no response, waiting for secondary");
                    if let Ok(Some(resp)) = secondary_handle.await {
                        ctx.set_response(Some(resp));
                        return Ok(());
                    }
                }
                Err(_) => {
                    // Primary exceeded threshold — race primary vs secondary.
                    debug!(elapsed = ?start.elapsed(), "fallback: primary exceeded threshold, racing both");
                    tokio::select! {
                        result = &mut primary_handle => {
                            if let Ok(Some(resp)) = result {
                                debug!(elapsed = ?start.elapsed(), "fallback: primary won race");
                                ctx.set_response(Some(resp));
                                return Ok(());
                            }
                            // Primary lost — wait for secondary.
                            if let Ok(Some(resp)) = secondary_handle.await {
                                ctx.set_response(Some(resp));
                                return Ok(());
                            }
                        }
                        result = &mut secondary_handle => {
                            if let Ok(Some(resp)) = result {
                                debug!(elapsed = ?start.elapsed(), "fallback: secondary won race");
                                ctx.set_response(Some(resp));
                                return Ok(());
                            }
                            // Secondary lost — wait for primary.
                            if let Ok(Some(resp)) = primary_handle.await {
                                ctx.set_response(Some(resp));
                                return Ok(());
                            }
                        }
                    }
                }
            }
        } else {
            // Wait for primary with threshold timeout.
            let primary_result = tokio::time::timeout(threshold, primary_handle).await;
            match primary_result {
                Ok(Ok(Some(resp))) => {
                    debug!(elapsed = ?start.elapsed(), "fallback: primary responded within threshold");
                    ctx.set_response(Some(resp));
                    return Ok(());
                }
                _ => {
                    // Primary failed or timed out — run secondary.
                    debug!(elapsed = ?start.elapsed(), "fallback: primary timed out or failed, trying secondary");
                    match secondary.exec(&mut ctx_secondary).await {
                        Ok(()) => {
                            if let Some(resp) = ctx_secondary.response().cloned() {
                                ctx.set_response(Some(resp));
                                return Ok(());
                            }
                        }
                        Err(e) => {
                            warn!(error = %e, "fallback: secondary failed");
                        }
                    }
                }
            }
        }

        Err("fallback: no valid response from primary or secondary".into())
    }
}
