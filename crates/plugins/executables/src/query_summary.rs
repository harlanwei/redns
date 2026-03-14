// Copyright (C) 2026, Harlan Wei
//
// This file is part of redns.

//! Logs a structured summary of the query after downstream execution.
//! This is a `RecursiveExecutable`.

use async_trait::async_trait;
use redns_core::plugin::PluginResult;
use redns_core::sequence::ChainWalker;
use redns_core::{Context, RecursiveExecutable};
use tracing::info;

#[derive(Debug, Clone)]
pub struct QuerySummary {
    msg: String,
}

impl QuerySummary {
    pub fn new(msg: impl Into<String>) -> Self {
        let msg = msg.into();
        Self {
            msg: if msg.is_empty() {
                "query summary".to_string()
            } else {
                msg
            },
        }
    }
}
impl Default for QuerySummary {
    fn default() -> Self {
        Self::new("query summary")
    }
}

#[async_trait]
impl RecursiveExecutable for QuerySummary {
    async fn exec_recursive(&self, ctx: &mut Context, mut next: ChainWalker) -> PluginResult<()> {
        let result = next.exec_next(ctx).await;
        let question = ctx
            .question()
            .map(|q| format!("{}", q.name()))
            .unwrap_or_default();
        let has_resp = ctx.response().is_some();
        let rcode = ctx.response().map(|r| format!("{}", r.response_code()));
        let elapsed = ctx.start_time().elapsed();
        match &result {
            Ok(()) => {
                info!(msg = %self.msg, id = ctx.id(), question = %question, has_response = has_resp, rcode = ?rcode, elapsed = ?elapsed)
            }
            Err(e) => {
                info!(msg = %self.msg, id = ctx.id(), question = %question, has_response = has_resp, elapsed = ?elapsed, error = %e)
            }
        }
        result
    }
}
