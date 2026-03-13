// Copyright (C) 2026, Harlan Wei
//
// This file is part of redns.

//! Redirect plugin — rewrites QNAME and injects CNAME on response.

use async_trait::async_trait;
use hickory_proto::rr::{Name, RData, Record};
use redns_core::plugin::PluginResult;
use redns_core::sequence::ChainWalker;
use redns_core::{Context, RecursiveExecutable};
use std::collections::HashMap;

/// Redirect rules: original domain → target domain.
#[derive(Debug, Clone)]
pub struct Redirect {
    rules: HashMap<Name, Name>,
}

impl Redirect {
    pub fn new() -> Self {
        Self {
            rules: HashMap::new(),
        }
    }

    /// Adds a redirect rule: queries for `from` will be rewritten to `to`.
    pub fn add_rule(&mut self, from: Name, to: Name) {
        self.rules.insert(from, to);
    }

    /// Parse rules from lines: "from_domain to_domain".
    pub fn from_lines(lines: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let mut r = Self::new();
        for line in lines.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() != 2 {
                return Err(format!("redirect rule must have 2 fields: {line}").into());
            }
            let from = Name::from_ascii(parts[0])?;
            let to = Name::from_ascii(parts[1])?;
            r.add_rule(from, to);
        }
        Ok(r)
    }
}

impl Default for Redirect {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl RecursiveExecutable for Redirect {
    async fn exec_recursive(
        &self,
        ctx: &mut Context,
        mut next: ChainWalker,
    ) -> PluginResult<()> {
        let question = match ctx.question() {
            Some(q) => q.clone(),
            None => return next.exec_next(ctx).await,
        };
        let qname = question.name().clone();

        let target = match self.rules.get(&qname) {
            Some(t) => t.clone(),
            None => return next.exec_next(ctx).await,
        };

        // Rewrite the question name to the target.
        ctx.query_mut().queries_mut()[0].set_name(target.clone());

        // Execute downstream with rewritten name.
        let result = next.exec_next(ctx).await;

        // Restore original name in query.
        ctx.query_mut().queries_mut()[0].set_name(qname.clone());

        // If there's a response, restore question and inject CNAME.
        if let Some(resp) = ctx.response_mut() {
            // Fix question name in response.
            for q in resp.queries_mut() {
                if q.name() == &target {
                    q.set_name(qname.clone());
                }
            }

            // Insert CNAME record at the beginning of answers.
            let cname_rr = Record::from_rdata(
                qname.clone(),
                1,
                RData::CNAME(hickory_proto::rr::rdata::CNAME(target.clone())),
            );

            let mut new_answers = vec![cname_rr];
            new_answers.extend(resp.answers().iter().cloned());
            *resp.answers_mut() = new_answers;
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

    struct ResponderExec;
    #[async_trait]
    impl Executable for ResponderExec {
        async fn exec(&self, ctx: &mut Context) -> PluginResult<()> {
            let q = ctx.question().unwrap().clone();
            let mut resp = Message::new();
            resp.set_id(ctx.query().id());
            resp.set_message_type(MessageType::Response);
            resp.set_response_code(ResponseCode::NoError);
            resp.add_query(q.clone());
            resp.add_answer(Record::from_rdata(
                q.name().clone(),
                300,
                RData::A(Ipv4Addr::new(1, 2, 3, 4).into()),
            ));
            ctx.set_response(Some(resp));
            Ok(())
        }
    }

    #[tokio::test]
    async fn redirect_rewrites_and_injects_cname() {
        let mut redirect = Redirect::new();
        redirect.add_rule(
            Name::from_ascii("alias.com.").unwrap(),
            Name::from_ascii("real.com.").unwrap(),
        );

        let chain = vec![
            ChainNode {
                matchers: vec![],
                executor: NodeExecutor::Recursive(Box::new(redirect)),
            },
            ChainNode {
                matchers: vec![],
                executor: NodeExecutor::Simple(Box::new(ResponderExec)),
            },
        ];
        let seq = Sequence::new(chain);

        let mut msg = Message::new();
        msg.set_id(1)
            .set_message_type(MessageType::Query)
            .set_op_code(OpCode::Query);
        msg.add_query({
            let mut q = Query::new();
            q.set_name(Name::from_ascii("alias.com.").unwrap())
                .set_query_type(RecordType::A);
            q
        });
        let mut ctx = Context::new(msg);

        seq.exec(&mut ctx).await.unwrap();
        let resp = ctx.response().unwrap();
        // Should have CNAME + A records.
        assert!(resp.answers().len() >= 2);
        // First answer should be CNAME.
        assert_eq!(resp.answers()[0].record_type(), RecordType::CNAME);
    }

    #[tokio::test]
    async fn no_redirect_passes_through() {
        let redirect = Redirect::new();
        let chain = vec![
            ChainNode {
                matchers: vec![],
                executor: NodeExecutor::Recursive(Box::new(redirect)),
            },
            ChainNode {
                matchers: vec![],
                executor: NodeExecutor::Simple(Box::new(ResponderExec)),
            },
        ];
        let seq = Sequence::new(chain);

        let mut msg = Message::new();
        msg.set_id(1)
            .set_message_type(MessageType::Query)
            .set_op_code(OpCode::Query);
        msg.add_query({
            let mut q = Query::new();
            q.set_name(Name::from_ascii("normal.com.").unwrap())
                .set_query_type(RecordType::A);
            q
        });
        let mut ctx = Context::new(msg);
        seq.exec(&mut ctx).await.unwrap();
        assert_eq!(ctx.response().unwrap().answers().len(), 1); // Just the A record.
    }
}
