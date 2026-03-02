// Copyright (C) 2026, Harlan Wei
//
// This file is part of redns.

//! Responds with fixed IPs for A/AAAA queries.

use async_trait::async_trait;
use hickory_proto::op::{Message, MessageType, ResponseCode};
use hickory_proto::rr::{Name, RData, Record, RecordType};
use redns_core::plugin::PluginResult;
use redns_core::{Context, Executable};
use std::net::{Ipv4Addr, Ipv6Addr};

/// Returns fixed A/AAAA responses based on pre-configured IPs.
#[derive(Debug, Clone)]
pub struct BlackHole {
    ipv4: Vec<Ipv4Addr>,
    ipv6: Vec<Ipv6Addr>,
}

impl BlackHole {
    pub fn new(ipv4: Vec<Ipv4Addr>, ipv6: Vec<Ipv6Addr>) -> Self {
        Self { ipv4, ipv6 }
    }

    pub fn from_str_args(s: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let mut ipv4 = Vec::new();
        let mut ipv6 = Vec::new();
        for addr_str in s.split_whitespace() {
            if let Ok(v4) = addr_str.parse::<Ipv4Addr>() {
                ipv4.push(v4);
            } else if let Ok(v6) = addr_str.parse::<Ipv6Addr>() {
                ipv6.push(v6);
            } else {
                return Err(format!("invalid IP address: {}", addr_str).into());
            }
        }
        Ok(Self::new(ipv4, ipv6))
    }
}

#[async_trait]
impl Executable for BlackHole {
    async fn exec(&self, ctx: &mut Context) -> PluginResult<()> {
        let question = match ctx.question() {
            Some(q) => q.clone(),
            None => return Ok(()),
        };
        let qtype = question.query_type();
        let qname: Name = question.name().clone();
        let ttl = 300;

        let records: Vec<Record> = match qtype {
            RecordType::A if !self.ipv4.is_empty() => self
                .ipv4
                .iter()
                .map(|ip| Record::from_rdata(qname.clone(), ttl, RData::A((*ip).into())))
                .collect(),
            RecordType::AAAA if !self.ipv6.is_empty() => self
                .ipv6
                .iter()
                .map(|ip| Record::from_rdata(qname.clone(), ttl, RData::AAAA((*ip).into())))
                .collect(),
            _ => return Ok(()),
        };

        let mut resp = Message::new();
        resp.set_id(ctx.query().id());
        resp.set_message_type(MessageType::Response);
        resp.set_response_code(ResponseCode::NoError);
        resp.add_query(question);
        for rr in records {
            resp.add_answer(rr);
        }
        ctx.set_response(Some(resp));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hickory_proto::op::{Message, MessageType, OpCode, Query};

    fn make_ctx(rtype: RecordType) -> Context {
        let mut msg = Message::new();
        msg.set_id(1)
            .set_message_type(MessageType::Query)
            .set_op_code(OpCode::Query);
        msg.add_query({
            let mut q = Query::new();
            q.set_name(Name::from_ascii("example.com.").unwrap())
                .set_query_type(rtype);
            q
        });
        Context::new(msg)
    }

    #[tokio::test]
    async fn a_query_returns_ipv4() {
        let bh = BlackHole::new(vec!["1.2.3.4".parse().unwrap()], vec![]);
        let mut ctx = make_ctx(RecordType::A);
        bh.exec(&mut ctx).await.unwrap();
        let resp = ctx.response().unwrap();
        assert_eq!(resp.answers().len(), 1);
    }

    #[tokio::test]
    async fn aaaa_query_returns_ipv6() {
        let bh = BlackHole::new(vec![], vec!["::1".parse().unwrap()]);
        let mut ctx = make_ctx(RecordType::AAAA);
        bh.exec(&mut ctx).await.unwrap();
        assert_eq!(ctx.response().unwrap().answers().len(), 1);
    }

    #[tokio::test]
    async fn no_matching_type_is_noop() {
        let bh = BlackHole::new(vec!["1.2.3.4".parse().unwrap()], vec![]);
        let mut ctx = make_ctx(RecordType::AAAA);
        bh.exec(&mut ctx).await.unwrap();
        assert!(ctx.response().is_none());
    }

    #[test]
    fn from_str_args_parses_mixed() {
        let bh = BlackHole::from_str_args("1.2.3.4 ::1 10.0.0.1").unwrap();
        assert_eq!(bh.ipv4.len(), 2);
        assert_eq!(bh.ipv6.len(), 1);
    }
}
