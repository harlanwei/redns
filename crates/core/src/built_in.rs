// Copyright (C) 2026, Harlan Wei
//
// This file is part of redns.
//
// redns is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// redns is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

//! Built-in sequence actions and constant matchers.

use crate::context::Context;
use crate::plugin::{Matcher, PluginResult, RecursiveExecutable};
use crate::sequence::ChainWalker;
use async_trait::async_trait;
use hickory_proto::op::{Message, MessageType, ResponseCode};

// ── Flow-control actions ──────────────────────────────────────────

/// Stops execution (drops the remaining chain walker).
pub struct ActionAccept;

#[async_trait]
impl RecursiveExecutable for ActionAccept {
    async fn exec_recursive(&self, _ctx: &mut Context, _next: ChainWalker<'_>) -> PluginResult<()> {
        Ok(()) // drop `next`
    }
}

/// Sets a REFUSED response and stops execution.
pub struct ActionReject;

#[async_trait]
impl RecursiveExecutable for ActionReject {
    async fn exec_recursive(&self, ctx: &mut Context, _next: ChainWalker<'_>) -> PluginResult<()> {
        let mut resp = Message::new();
        resp.set_id(ctx.query().id());
        resp.set_message_type(MessageType::Response);
        resp.set_response_code(ResponseCode::Refused);
        ctx.set_response(Some(resp));
        Ok(())
    }
}

/// Skips the rest of the current chain and returns to the `jump_back` point.
pub struct ActionReturn;

#[async_trait]
impl RecursiveExecutable for ActionReturn {
    async fn exec_recursive(&self, ctx: &mut Context, next: ChainWalker<'_>) -> PluginResult<()> {
        if let Some(mut jb) = next.into_jump_back() {
            return jb.exec_next(ctx).await;
        }
        Ok(())
    }
}

/// Transfers execution to a target chain, saving a return point.
pub struct ActionJump {
    pub target: &'static [crate::sequence::ChainNode],
}

#[async_trait]
impl RecursiveExecutable for ActionJump {
    async fn exec_recursive(&self, ctx: &mut Context, next: ChainWalker<'_>) -> PluginResult<()> {
        let mut walker = ChainWalker::new(self.target, Some(Box::new(next)));
        walker.exec_next(ctx).await
    }
}

/// Transfers execution to a target chain, **without** saving a return point.
pub struct ActionGoto {
    pub target: &'static [crate::sequence::ChainNode],
}

#[async_trait]
impl RecursiveExecutable for ActionGoto {
    async fn exec_recursive(&self, ctx: &mut Context, _next: ChainWalker<'_>) -> PluginResult<()> {
        let mut walker = ChainWalker::new(self.target, None);
        walker.exec_next(ctx).await
    }
}

// ── Constant matchers ─────────────────────────────────────────────

/// Always returns `true`.
pub struct MatchAlwaysTrue;
impl Matcher for MatchAlwaysTrue {
    fn match_ctx(&self, _ctx: &Context) -> PluginResult<bool> {
        Ok(true)
    }
}

/// Always returns `false`.
pub struct MatchAlwaysFalse;
impl Matcher for MatchAlwaysFalse {
    fn match_ctx(&self, _ctx: &Context) -> PluginResult<bool> {
        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::Context;
    use crate::plugin::Executable;
    use crate::sequence::{ChainNode, NodeExecutor, Sequence};
    use hickory_proto::op::{Message, MessageType, OpCode, Query};
    use hickory_proto::rr::{Name, RecordType};

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

    struct MarkSetter(u32);
    #[async_trait]
    impl Executable for MarkSetter {
        async fn exec(&self, ctx: &mut Context) -> PluginResult<()> {
            ctx.set_mark(self.0);
            Ok(())
        }
    }

    #[tokio::test]
    async fn accept_stops_chain() {
        let chain = vec![
            ChainNode {
                matchers: vec![],
                executor: NodeExecutor::Recursive(Box::new(ActionAccept)),
            },
            ChainNode {
                matchers: vec![],
                executor: NodeExecutor::Simple(Box::new(MarkSetter(1))),
            },
        ];
        let seq = Sequence::new(chain);
        let mut ctx = Context::new(make_query());
        seq.exec(&mut ctx).await.unwrap();
        assert!(!ctx.has_mark(1)); // never reached
    }

    #[tokio::test]
    async fn reject_sets_response() {
        let chain = vec![ChainNode {
            matchers: vec![],
            executor: NodeExecutor::Recursive(Box::new(ActionReject)),
        }];
        let seq = Sequence::new(chain);
        let mut ctx = Context::new(make_query());
        seq.exec(&mut ctx).await.unwrap();
        let resp = ctx.response().expect("should have response");
        assert_eq!(resp.response_code(), ResponseCode::Refused);
    }

    #[tokio::test]
    async fn always_true_and_false_matchers() {
        assert!(
            MatchAlwaysTrue
                .match_ctx(&Context::new(make_query()))
                .unwrap()
        );
        assert!(
            !MatchAlwaysFalse
                .match_ctx(&Context::new(make_query()))
                .unwrap()
        );
    }
}
