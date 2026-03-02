// Copyright (C) 2026, Harlan Wei
//
// This file is part of redns.

//! Returns arbitrary pre-configured DNS records matching query names.

use async_trait::async_trait;
use hickory_proto::op::{Message, MessageType, ResponseCode};
use hickory_proto::rr::{Name, RData, Record, RecordType};
use redns_core::plugin::PluginResult;
use redns_core::{Context, Executable};
use std::collections::HashMap;
use std::net::{Ipv4Addr, Ipv6Addr};

#[derive(Debug, Clone)]
pub struct ArbitraryRecord {
    pub name: Name,
    pub ttl: u32,
    pub rdata: RData,
}

#[derive(Debug, Clone, Default)]
pub struct Arbitrary {
    records: HashMap<Name, Vec<ArbitraryRecord>>,
}

impl Arbitrary {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn add_record(&mut self, record: ArbitraryRecord) {
        self.records
            .entry(record.name.clone())
            .or_default()
            .push(record);
    }
    pub fn add_a(&mut self, name: Name, ttl: u32, ip: Ipv4Addr) {
        self.add_record(ArbitraryRecord {
            name,
            ttl,
            rdata: RData::A(ip.into()),
        });
    }
    pub fn add_aaaa(&mut self, name: Name, ttl: u32, ip: Ipv6Addr) {
        self.add_record(ArbitraryRecord {
            name,
            ttl,
            rdata: RData::AAAA(ip.into()),
        });
    }
}

fn record_type_of(rdata: &RData) -> RecordType {
    match rdata {
        RData::A(_) => RecordType::A,
        RData::AAAA(_) => RecordType::AAAA,
        RData::CNAME(_) => RecordType::CNAME,
        RData::MX(_) => RecordType::MX,
        RData::TXT(_) => RecordType::TXT,
        RData::NS(_) => RecordType::NS,
        _ => RecordType::Unknown(0),
    }
}

#[async_trait]
impl Executable for Arbitrary {
    async fn exec(&self, ctx: &mut Context) -> PluginResult<()> {
        let question = match ctx.question() {
            Some(q) => q.clone(),
            None => return Ok(()),
        };
        let qname = question.name();
        let qtype = question.query_type();
        if let Some(entries) = self.records.get(qname) {
            let matching: Vec<_> = entries
                .iter()
                .filter(|e| record_type_of(&e.rdata) == qtype)
                .collect();
            if matching.is_empty() {
                return Ok(());
            }
            let mut resp = Message::new();
            resp.set_id(ctx.query().id());
            resp.set_message_type(MessageType::Response);
            resp.set_response_code(ResponseCode::NoError);
            resp.add_query(question);
            for entry in matching {
                resp.add_answer(Record::from_rdata(
                    entry.name.clone(),
                    entry.ttl,
                    entry.rdata.clone(),
                ));
            }
            ctx.set_response(Some(resp));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hickory_proto::op::{Message, MessageType, OpCode, Query};

    fn make_ctx(name: &str, rtype: RecordType) -> Context {
        let mut msg = Message::new();
        msg.set_id(1)
            .set_message_type(MessageType::Query)
            .set_op_code(OpCode::Query);
        msg.add_query({
            let mut q = Query::new();
            q.set_name(Name::from_ascii(name).unwrap())
                .set_query_type(rtype);
            q
        });
        Context::new(msg)
    }

    #[tokio::test]
    async fn matching_record() {
        let mut arb = Arbitrary::new();
        arb.add_a(
            Name::from_ascii("test.example.com.").unwrap(),
            60,
            "10.0.0.1".parse().unwrap(),
        );
        let mut ctx = make_ctx("test.example.com.", RecordType::A);
        arb.exec(&mut ctx).await.unwrap();
        assert_eq!(ctx.response().unwrap().answers().len(), 1);
    }

    #[tokio::test]
    async fn non_matching_name() {
        let mut arb = Arbitrary::new();
        arb.add_a(
            Name::from_ascii("test.example.com.").unwrap(),
            60,
            "10.0.0.1".parse().unwrap(),
        );
        let mut ctx = make_ctx("other.example.com.", RecordType::A);
        arb.exec(&mut ctx).await.unwrap();
        assert!(ctx.response().is_none());
    }

    #[tokio::test]
    async fn non_matching_type() {
        let mut arb = Arbitrary::new();
        arb.add_a(
            Name::from_ascii("test.example.com.").unwrap(),
            60,
            "10.0.0.1".parse().unwrap(),
        );
        let mut ctx = make_ctx("test.example.com.", RecordType::AAAA);
        arb.exec(&mut ctx).await.unwrap();
        assert!(ctx.response().is_none());
    }
}
