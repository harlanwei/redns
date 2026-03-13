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

//! Sequence executor — the core pipeline engine.
//!
//! A `Sequence` is a list of [`ChainNode`]s. Each node has optional matchers
//! and an executor (either [`Executable`] or [`RecursiveExecutable`]).
//! The [`ChainWalker`] walks the chain, evaluating matchers and invoking
//! executors, supporting `jump` / `return` semantics via `jump_back`.

use crate::context::Context;
use crate::plugin::{Executable, Matcher, PluginResult, RecursiveExecutable};
use std::future::Future;
use std::pin::Pin;
use tracing::debug;

use std::sync::Arc;

/// The executor stored inside a [`ChainNode`].
pub enum NodeExecutor {
    /// A simple executable.
    Simple(Box<dyn Executable>),
    /// A recursive executable that receives the downstream chain walker.
    Recursive(Box<dyn RecursiveExecutable>),
}

/// A single node in the execution chain.
///
/// Each node has zero or more matchers (all must match for the node to fire)
/// and exactly one executor.
pub struct ChainNode {
    /// Matchers that gate this node. If empty, the node always fires.
    pub matchers: Vec<Box<dyn Matcher>>,
    /// The executor to run when all matchers pass.
    pub executor: NodeExecutor,
}

/// Walks a chain of [`ChainNode`]s, evaluating matchers and invoking executors.
///
/// Supports `jump` (with return) and `goto` (without return) semantics
/// via the `jump_back` field.
#[derive(Clone)]
pub struct ChainWalker {
    pos: usize,
    chain: Arc<[ChainNode]>,
    jump_back: Option<Box<ChainWalker>>,
}

impl ChainWalker {
    /// Creates a new walker starting at position 0.
    pub fn new(chain: Arc<[ChainNode]>, jump_back: Option<Box<ChainWalker>>) -> Self {
        Self {
            pos: 0,
            chain,
            jump_back,
        }
    }

    /// Consumes this walker and returns its `jump_back` walker (if any).
    /// Used by `ActionReturn` to skip the rest of the current chain.
    pub fn into_jump_back(self) -> Option<Box<ChainWalker>> {
        self.jump_back
    }

    /// Executes the next node(s) in the chain.
    pub fn exec_next<'b>(
        &'b mut self,
        ctx: &'b mut Context,
    ) -> Pin<Box<dyn Future<Output = PluginResult<()>> + Send + 'b>> {
        Box::pin(async move {
            while self.pos < self.chain.len() {
                let idx = self.pos;
                let node = &self.chain[idx];

                // Evaluate all matchers — skip this node if any fails.
                let mut matched = true;
                for (mi, matcher) in node.matchers.iter().enumerate() {
                    let m = matcher.match_ctx(ctx)?;
                    debug!(node = idx, matcher = mi, matched = m, "matcher evaluated");
                    if !m {
                        matched = false;
                        break;
                    }
                }
                if !matched {
                    self.pos += 1;
                    continue;
                }

                // Execute.
                match &node.executor {
                    NodeExecutor::Simple(exec) => {
                        debug!(node = idx, kind = "simple", "executing node");
                        let start = std::time::Instant::now();
                        exec.exec(ctx).await?;
                        debug!(node = idx, elapsed = ?start.elapsed(), "node completed");
                        self.pos += 1;
                    }
                    NodeExecutor::Recursive(re) => {
                        debug!(node = idx, kind = "recursive", "executing node");
                        let next = ChainWalker {
                            pos: self.pos + 1,
                            chain: Arc::clone(&self.chain),
                            jump_back: self.jump_back.take(),
                        };
                        return re.exec_recursive(ctx, next).await;
                    }
                }
            }

            // End of chain — jump back if there is a saved walker.
            if let Some(mut jb) = self.jump_back.take() {
                return jb.exec_next(ctx).await;
            }

            Ok(())
        })
    }
}

/// A complete sequence that owns its chain nodes.
pub struct Sequence {
    chain: Arc<[ChainNode]>,
}

impl Sequence {
    /// Creates a new sequence from a list of chain nodes.
    pub fn new(chain: Vec<ChainNode>) -> Self {
        Self { chain: chain.into() }
    }

    /// Returns a reference to the chain for `jump`/`goto` targets.
    pub fn chain(&self) -> Arc<[ChainNode]> {
        Arc::clone(&self.chain)
    }

    /// Executes the entire sequence against the given context.
    pub async fn exec(&self, ctx: &mut Context) -> PluginResult<()> {
        let mut walker = ChainWalker::new(Arc::clone(&self.chain), None);
        walker.exec_next(ctx).await
    }
}

#[async_trait::async_trait]
impl Executable for Sequence {
    async fn exec(&self, ctx: &mut Context) -> PluginResult<()> {
        let mut walker = ChainWalker::new(Arc::clone(&self.chain), None);
        walker.exec_next(ctx).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::Context;
    use async_trait::async_trait;
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

    /// Helper: an Executable that sets a mark on the context.
    struct MarkSetter(u32);
    #[async_trait]
    impl Executable for MarkSetter {
        async fn exec(&self, ctx: &mut Context) -> PluginResult<()> {
            ctx.set_mark(self.0);
            Ok(())
        }
    }

    /// Helper: a matcher that always returns false.
    struct AlwaysFalse;
    impl Matcher for AlwaysFalse {
        fn match_ctx(&self, _ctx: &Context) -> PluginResult<bool> {
            Ok(false)
        }
    }

    /// Helper: a RecursiveExecutable that stops execution (accept).
    struct StopExec;
    #[async_trait]
    impl RecursiveExecutable for StopExec {
        async fn exec_recursive(
            &self,
            _ctx: &mut Context,
            _next: ChainWalker,
        ) -> PluginResult<()> {
            Ok(()) // drop `next` — stops chain
        }
    }

    #[tokio::test]
    async fn simple_chain_executes_all_nodes() {
        let chain = vec![
            ChainNode {
                matchers: vec![],
                executor: NodeExecutor::Simple(Box::new(MarkSetter(1))),
            },
            ChainNode {
                matchers: vec![],
                executor: NodeExecutor::Simple(Box::new(MarkSetter(2))),
            },
        ];
        let seq = Sequence::new(chain);
        let mut ctx = Context::new(make_query());
        seq.exec(&mut ctx).await.unwrap();
        assert!(ctx.has_mark(1));
        assert!(ctx.has_mark(2));
    }

    #[tokio::test]
    async fn matcher_skips_non_matching_node() {
        let chain = vec![
            ChainNode {
                matchers: vec![Box::new(AlwaysFalse)],
                executor: NodeExecutor::Simple(Box::new(MarkSetter(1))),
            },
            ChainNode {
                matchers: vec![],
                executor: NodeExecutor::Simple(Box::new(MarkSetter(2))),
            },
        ];
        let seq = Sequence::new(chain);
        let mut ctx = Context::new(make_query());
        seq.exec(&mut ctx).await.unwrap();
        assert!(!ctx.has_mark(1)); // skipped
        assert!(ctx.has_mark(2)); // executed
    }

    #[tokio::test]
    async fn recursive_exec_stops_chain() {
        let chain = vec![
            ChainNode {
                matchers: vec![],
                executor: NodeExecutor::Recursive(Box::new(StopExec)),
            },
            ChainNode {
                matchers: vec![],
                executor: NodeExecutor::Simple(Box::new(MarkSetter(99))),
            },
        ];
        let seq = Sequence::new(chain);
        let mut ctx = Context::new(make_query());
        seq.exec(&mut ctx).await.unwrap();
        assert!(!ctx.has_mark(99)); // never reached
    }

    #[tokio::test]
    async fn empty_chain_is_noop() {
        let seq = Sequence::new(vec![]);
        let mut ctx = Context::new(make_query());
        assert!(seq.exec(&mut ctx).await.is_ok());
    }
}
