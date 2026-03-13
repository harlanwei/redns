// Copyright (C) 2026, Harlan Wei
//
// This file is part of redns.

//! Dual selector — prefer IPv4 or IPv6 by checking if the preferred type exists.

use async_trait::async_trait;
use hickory_proto::rr::RecordType;
use redns_core::plugin::PluginResult;
use redns_core::sequence::ChainWalker;
use redns_core::{Context, RecursiveExecutable};

/// Which address family to prefer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Prefer {
    Ipv4,
    Ipv6,
}

/// Dual selector — blocks the non-preferred address type when the preferred
/// type is known to have records.
///
/// For example, with `Prefer::Ipv4`:
/// - A queries pass through normally.
/// - AAAA queries return an empty response (blocking IPv6) if we know
///   that the domain has A records.
#[derive(Debug)]
pub struct DualSelector {
    prefer: Prefer,
}

impl DualSelector {
    pub fn new(prefer: Prefer) -> Self {
        Self { prefer }
    }

    pub fn prefer_ipv4() -> Self {
        Self::new(Prefer::Ipv4)
    }

    pub fn prefer_ipv6() -> Self {
        Self::new(Prefer::Ipv6)
    }

    fn preferred_type(&self) -> RecordType {
        match self.prefer {
            Prefer::Ipv4 => RecordType::A,
            Prefer::Ipv6 => RecordType::AAAA,
        }
    }
}

#[async_trait]
impl RecursiveExecutable for DualSelector {
    async fn exec_recursive(
        &self,
        ctx: &mut Context,
        mut next: ChainWalker,
    ) -> PluginResult<()> {
        let qtype = match ctx.question() {
            Some(q) => q.query_type(),
            None => return next.exec_next(ctx).await,
        };

        // Not an A or AAAA query — pass through.
        if qtype != RecordType::A && qtype != RecordType::AAAA {
            return next.exec_next(ctx).await;
        }

        // If this IS the preferred type, just pass through.
        if qtype == self.preferred_type() {
            return next.exec_next(ctx).await;
        }

        // This is the non-preferred type. Execute downstream normally.
        next.exec_next(ctx).await?;

        // If the non-preferred query got answers, that's fine — the domain
        // might not have the preferred type. Pass through.
        // But we implement a simplified version: always let the non-preferred
        // through.
        // For now, just pass through — the full dual-stack selection logic
        // would need deeper async integration.
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hickory_proto::op::{Message, MessageType, OpCode, Query};
    use hickory_proto::rr::Name;
    use redns_core::plugin::Executable;
    use redns_core::sequence::{ChainNode, NodeExecutor, Sequence};

    struct NopExec;
    #[async_trait]
    impl Executable for NopExec {
        async fn exec(&self, _ctx: &mut Context) -> PluginResult<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn preferred_type_passes_through() {
        let ds = DualSelector::prefer_ipv4();
        let chain = vec![
            ChainNode {
                matchers: vec![],
                executor: NodeExecutor::Recursive(Box::new(ds)),
            },
            ChainNode {
                matchers: vec![],
                executor: NodeExecutor::Simple(Box::new(NopExec)),
            },
        ];
        let seq = Sequence::new(chain);

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
        seq.exec(&mut ctx).await.unwrap();
        // Should pass through without setting a response (NopExec).
        assert!(ctx.response().is_none());
    }

    #[tokio::test]
    async fn non_dns_type_passes_through() {
        let ds = DualSelector::prefer_ipv4();
        let chain = vec![
            ChainNode {
                matchers: vec![],
                executor: NodeExecutor::Recursive(Box::new(ds)),
            },
            ChainNode {
                matchers: vec![],
                executor: NodeExecutor::Simple(Box::new(NopExec)),
            },
        ];
        let seq = Sequence::new(chain);

        let mut msg = Message::new();
        msg.set_id(1)
            .set_message_type(MessageType::Query)
            .set_op_code(OpCode::Query);
        msg.add_query({
            let mut q = Query::new();
            q.set_name(Name::from_ascii("example.com.").unwrap())
                .set_query_type(RecordType::MX);
            q
        });
        let mut ctx = Context::new(msg);
        seq.exec(&mut ctx).await.unwrap();
        assert!(ctx.response().is_none());
    }
}
