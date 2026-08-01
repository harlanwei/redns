// Copyright (C) 2026, Harlan Wei
//
// This file is part of redns.

//! Use-answer-of plugin — answers with the records of a target qname.
//!
//! # Configuration
//!
//! ```yaml
//! plugins:
//!   - type: sequence
//!     args:
//!       - exec: $forward
//!       - matches: asn 13335
//!         exec: use-answer-of www.example.com
//!       - exec: $forward
//! ```
//!
//! When the `asn` matcher fires (or any other gating matcher), the original
//! question name is rewritten to the target qname and the rest of the chain —
//! typically a `forward` — resolves it. The response is then answered with
//! the target's records: a synthesized `CNAME <original> → <target>` is
//! prepended and the question section is restored to the original qname, so
//! the client receives a valid answer chain without ever learning the
//! rewrite happened.

use async_trait::async_trait;
use hickory_proto::rr::{Name, RData, Record};
use redns_core::plugin::PluginResult;
use redns_core::sequence::ChainWalker;
use redns_core::{Context, RecursiveExecutable};

/// Answers with the records of a single target qname.
#[derive(Debug, Clone)]
pub struct UseAnswerOf {
    target: Name,
}

impl UseAnswerOf {
    /// Parses args: the target qname (trailing dot optional).
    pub fn from_str_args(s: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let s = s.trim();
        let token = match s.split_whitespace().next() {
            Some(t) if !t.is_empty() => t,
            _ => {
                return Err(
                    "use-answer-of expects a target qname, e.g. `use-answer-of www.example.com`"
                        .into(),
                )
            }
        };
        let fqdn = if token.ends_with('.') {
            token.to_string()
        } else {
            format!("{token}.")
        };
        let target = Name::from_ascii(&fqdn)
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                format!("use-answer-of: invalid target qname '{token}': {e}").into()
            })?;
        Ok(Self { target })
    }
}

#[async_trait]
impl RecursiveExecutable for UseAnswerOf {
    async fn exec_recursive(&self, ctx: &mut Context, mut next: ChainWalker) -> PluginResult<()> {
        let question = match ctx.question() {
            Some(q) => q.clone(),
            None => return next.exec_next(ctx).await,
        };
        let original = question.name().clone();

        // Target is the same as the original question — nothing to rewrite.
        if original == self.target {
            return next.exec_next(ctx).await;
        }

        // Rewrite the question name to the target (qtype/qclass unchanged).
        ctx.query_mut().queries[0].set_name(self.target.clone());

        // Run downstream (e.g. a forward) against the target name.
        let result = next.exec_next(ctx).await;

        // Restore the original question name in the query.
        ctx.query_mut().queries[0].set_name(original.clone());

        // Fix up the response: restore the question section and link the
        // original name to the target with a CNAME so the client can follow
        // the chain. Both steps only apply when the downstream chain actually
        // resolved the target (its question section carries the target name) —
        // otherwise the response is left untouched.
        if let Some(resp) = ctx.response_mut() {
            let saw_target_question =
                resp.queries.iter().any(|q| q.name() == &self.target);

            for q in &mut resp.queries {
                if q.name() == &self.target {
                    q.set_name(original.clone());
                }
            }

            // Skip the CNAME when the upstream already produced an answer
            // chain rooted at the original name.
            if saw_target_question
                && !resp.answers.iter().any(|rr| rr.name == original)
            {
                let cname_rr = Record::from_rdata(
                    original.clone(),
                    1,
                    RData::CNAME(hickory_proto::rr::rdata::CNAME(self.target.clone())),
                );
                let mut new_answers = vec![cname_rr];
                new_answers.extend(resp.answers.iter().cloned());
                resp.answers = new_answers;
            }
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use hickory_proto::op::{Message, MessageType, OpCode, Query, ResponseCode};
    use hickory_proto::rr::RecordType;
    use redns_core::plugin::Executable;
    use redns_core::sequence::{ChainNode, NodeExecutor, Sequence};
    use std::net::Ipv4Addr;

    /// Answers the current question with an A record for its name.
    struct ResponderExec;
    #[async_trait]
    impl Executable for ResponderExec {
        async fn exec(&self, ctx: &mut Context) -> PluginResult<()> {
            let q = ctx.question().unwrap().clone();
            let mut resp = Message::response(ctx.query().id, OpCode::Query);
        resp.metadata.response_code = ResponseCode::NoError;
            resp.add_query(q.clone());
            resp.add_answer(Record::from_rdata(
                q.name().clone(),
                300,
                RData::A(Ipv4Addr::new(9, 9, 9, 9).into()),
            ));
            ctx.set_response(Some(resp));
            Ok(())
        }
    }

    fn make_query(name: &str) -> Message {
        let mut msg = Message::new(1, MessageType::Query, OpCode::Query);
        msg.add_query({
            let mut q = Query::new();
            q.set_name(Name::from_ascii(name).unwrap())
                .set_query_type(RecordType::A);
            q
        });
        msg
    }

    fn chain_with(uao: UseAnswerOf) -> Sequence {
        Sequence::new(vec![
            ChainNode {
                matchers: vec![],
                executor: NodeExecutor::Recursive(Box::new(uao)),
            },
            ChainNode {
                matchers: vec![],
                executor: NodeExecutor::Simple(Box::new(ResponderExec)),
            },
        ])
    }

    #[tokio::test]
    async fn answers_with_target_records() {
        let seq = chain_with(UseAnswerOf::from_str_args("www.example.com").unwrap());
        let mut ctx = Context::new(make_query("example.com."));
        seq.exec(&mut ctx).await.unwrap();

        // The downstream saw the target name...
        let resp = ctx.response().unwrap();
        assert_eq!(resp.answers.len(), 2);
        // ...linked by a CNAME from the original name...
        assert_eq!(resp.answers[0].record_type(), RecordType::CNAME);
        assert_eq!(resp.answers[0].name.to_ascii(), "example.com.");
        // ...to the target's own A record.
        assert_eq!(resp.answers[1].record_type(), RecordType::A);
        assert_eq!(resp.answers[1].name.to_ascii(), "www.example.com.");
        // Question section and query are restored to the original qname.
        assert_eq!(resp.queries[0].name().to_ascii(), "example.com.");
        assert_eq!(ctx.question().unwrap().name().to_ascii(), "example.com.");
    }

    #[tokio::test]
    async fn no_cname_when_target_equals_original() {
        let seq = chain_with(UseAnswerOf::from_str_args("example.com").unwrap());
        let mut ctx = Context::new(make_query("example.com."));
        seq.exec(&mut ctx).await.unwrap();

        let resp = ctx.response().unwrap();
        // No synthesized CNAME — the target is the original name.
        assert_eq!(resp.answers.len(), 1);
        assert_eq!(resp.answers[0].record_type(), RecordType::A);
    }

    #[tokio::test]
    async fn leaves_response_untouched_when_target_never_resolved() {
        // UseAnswerOf with an empty downstream: nothing resolves the target,
        // so the pre-existing response must pass through unmodified.
        let uao = UseAnswerOf::from_str_args("www.example.com").unwrap();
        let seq = Sequence::new(vec![ChainNode {
            matchers: vec![],
            executor: NodeExecutor::Recursive(Box::new(uao)),
        }]);
        let mut ctx = Context::new(make_query("example.com."));

        // A response for the original qname already exists (e.g. from an
        // earlier forward node).
        let mut resp = Message::response(1, OpCode::Query);
        resp.metadata.response_code = ResponseCode::NoError;
        resp.add_query(ctx.question().unwrap().clone());
        resp.add_answer(Record::from_rdata(
            Name::from_ascii("example.com.").unwrap(),
            300,
            RData::A(Ipv4Addr::new(1, 2, 3, 4).into()),
        ));
        ctx.set_response(Some(resp));

        seq.exec(&mut ctx).await.unwrap();

        let resp = ctx.response().unwrap();
        // Unmodified: single A record, no CNAME, question unchanged.
        assert_eq!(resp.answers.len(), 1);
        assert_eq!(resp.answers[0].record_type(), RecordType::A);
        assert_eq!(resp.answers[0].name.to_ascii(), "example.com.");
        assert_eq!(resp.queries[0].name().to_ascii(), "example.com.");
    }

    #[test]
    fn parses_target_with_and_without_trailing_dot() {
        let a = UseAnswerOf::from_str_args("www.example.com").unwrap();
        assert_eq!(a.target.to_ascii(), "www.example.com.");
        let b = UseAnswerOf::from_str_args("www.example.com.").unwrap();
        assert_eq!(b.target.to_ascii(), "www.example.com.");
    }

    #[test]
    fn rejects_empty_and_uses_first_token_of_args() {
        assert!(UseAnswerOf::from_str_args("").is_err());
        assert!(UseAnswerOf::from_str_args("   ").is_err());
        // Extra tokens are ignored; the first token is the target.
        let a = UseAnswerOf::from_str_args("www.example.com extra").unwrap();
        assert_eq!(a.target.to_ascii(), "www.example.com.");
    }
}
