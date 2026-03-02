// Copyright (C) 2026, Harlan Wei
//
// This file is part of redns.

//! Logs the current context state for debugging.

use async_trait::async_trait;
use redns_core::plugin::PluginResult;
use redns_core::{Context, Executable};
use tracing::debug;

/// Logs a debug summary of the current context.
#[derive(Debug, Clone, Copy, Default)]
pub struct DebugPrint;

#[async_trait]
impl Executable for DebugPrint {
    async fn exec(&self, ctx: &mut Context) -> PluginResult<()> {
        let id = ctx.id();
        let question = ctx
            .question()
            .map(|q| format!("{}", q.name()))
            .unwrap_or_default();
        let has_resp = ctx.response().is_some();
        debug!(
            ctx_id = id,
            question = %question,
            has_response = has_resp,
            elapsed = ?ctx.start_time().elapsed(),
            "debug_print"
        );
        Ok(())
    }
}
