// Copyright (C) 2026, Harlan Wei
//
// This file is part of redns.

//! Overrides min/max/fixed TTL on response records.

use async_trait::async_trait;
use redns_core::plugin::PluginResult;
use redns_core::{Context, Executable};

#[derive(Debug, Clone, Copy)]
pub struct Ttl {
    pub fix: u32,
    pub min: u32,
    pub max: u32,
}

impl Ttl {
    pub fn fixed(ttl: u32) -> Self {
        Self {
            fix: ttl,
            min: 0,
            max: 0,
        }
    }
    pub fn range(min: u32, max: u32) -> Self {
        Self { fix: 0, min, max }
    }

    pub fn from_str_args(s: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let s = s.trim();
        if let Some((lo, hi)) = s.split_once('-') {
            Ok(Self::range(lo.trim().parse()?, hi.trim().parse()?))
        } else {
            Ok(Self::fixed(s.parse()?))
        }
    }
}

#[async_trait]
impl Executable for Ttl {
    async fn exec(&self, ctx: &mut Context) -> PluginResult<()> {
        if let Some(resp) = ctx.response_mut() {
            let apply = |ttl: u32| -> u32 {
                if self.fix > 0 {
                    return self.fix;
                }
                let mut t = ttl;
                if self.min > 0 && t < self.min {
                    t = self.min;
                }
                if self.max > 0 && t > self.max {
                    t = self.max;
                }
                t
            };
            for rr in resp.answers_mut() {
                let old = rr.ttl();
                rr.set_ttl(apply(old));
            }
            for rr in resp.name_servers_mut() {
                let old = rr.ttl();
                rr.set_ttl(apply(old));
            }
            for rr in resp.additionals_mut() {
                let old = rr.ttl();
                rr.set_ttl(apply(old));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hickory_proto::op::{Message, MessageType, OpCode, Query};
    use hickory_proto::rr::{Name, RData, Record, RecordType};
    use std::net::Ipv4Addr;

    fn make_ctx_with_ttl(ttl: u32) -> Context {
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
        let mut resp = Message::new();
        resp.add_answer(Record::from_rdata(
            Name::from_ascii("example.com.").unwrap(),
            ttl,
            RData::A(Ipv4Addr::new(1, 2, 3, 4).into()),
        ));
        ctx.set_response(Some(resp));
        ctx
    }

    #[tokio::test]
    async fn fixed_ttl_overrides() {
        let t = Ttl::fixed(60);
        let mut ctx = make_ctx_with_ttl(3600);
        t.exec(&mut ctx).await.unwrap();
        assert_eq!(ctx.response().unwrap().answers()[0].ttl(), 60);
    }
    #[tokio::test]
    async fn min_clamps_up() {
        let t = Ttl::range(300, 0);
        let mut ctx = make_ctx_with_ttl(10);
        t.exec(&mut ctx).await.unwrap();
        assert_eq!(ctx.response().unwrap().answers()[0].ttl(), 300);
    }
    #[tokio::test]
    async fn max_clamps_down() {
        let t = Ttl::range(0, 600);
        let mut ctx = make_ctx_with_ttl(3600);
        t.exec(&mut ctx).await.unwrap();
        assert_eq!(ctx.response().unwrap().answers()[0].ttl(), 600);
    }
    #[tokio::test]
    async fn range_within_bounds() {
        let t = Ttl::range(100, 500);
        let mut ctx = make_ctx_with_ttl(250);
        t.exec(&mut ctx).await.unwrap();
        assert_eq!(ctx.response().unwrap().answers()[0].ttl(), 250);
    }
    #[test]
    fn from_str_fixed() {
        let t = Ttl::from_str_args("42").unwrap();
        assert_eq!(t.fix, 42);
    }
    #[test]
    fn from_str_range() {
        let t = Ttl::from_str_args("300-600").unwrap();
        assert_eq!(t.min, 300);
        assert_eq!(t.max, 600);
    }
}
