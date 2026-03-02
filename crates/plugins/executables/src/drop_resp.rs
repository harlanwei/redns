// Copyright (C) 2026, Harlan Wei
//
// This file is part of redns.

//! Clears the response from the context.

use async_trait::async_trait;
use redns_core::plugin::PluginResult;
use redns_core::{Context, Executable};

/// Drops (clears) any existing response from the context.
#[derive(Debug, Clone, Copy, Default)]
pub struct DropResp;

#[async_trait]
impl Executable for DropResp {
    async fn exec(&self, ctx: &mut Context) -> PluginResult<()> {
        ctx.set_response(None);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hickory_proto::op::{Message, MessageType, OpCode, Query};
    use hickory_proto::rr::{Name, RecordType};

    fn make_ctx() -> Context {
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
        Context::new(msg)
    }

    #[tokio::test]
    async fn drop_resp_clears_response() {
        let mut ctx = make_ctx();
        ctx.set_response(Some(Message::new()));
        assert!(ctx.response().is_some());

        DropResp.exec(&mut ctx).await.unwrap();
        assert!(ctx.response().is_none());
    }

    #[tokio::test]
    async fn drop_resp_noop_without_response() {
        let mut ctx = make_ctx();
        assert!(ctx.response().is_none());
        DropResp.exec(&mut ctx).await.unwrap();
        assert!(ctx.response().is_none());
    }
}
