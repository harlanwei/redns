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
        let resp = ctx.response();
        match &result {
            Ok(()) => {
                info!(msg = %self.msg, id = ctx.id(), question = %ctx.question().map(|q| q.name().to_string()).unwrap_or_default(), has_response = resp.is_some(), rcode = ?resp.map(|r| r.response_code), elapsed = ?ctx.start_time().elapsed())
            }
            Err(e) => {
                info!(msg = %self.msg, id = ctx.id(), question = %ctx.question().map(|q| q.name().to_string()).unwrap_or_default(), has_response = resp.is_some(), elapsed = ?ctx.start_time().elapsed(), error = %e)
            }
        }
        result
    }
}
