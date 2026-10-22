// Copyright (C) 2026, Harlan Wei
//
// This file is part of redns.

//! Response IP matcher — matches A/AAAA IPs in DNS response against CIDR ranges.

use crate::prefix_set::{PrefixSet, parse_cidr};
use redns_core::plugin::PluginResult;
use redns_core::{Context, Matcher};
use std::net::IpAddr;

/// Matches A/AAAA records in the DNS response against CIDR ranges.
#[derive(Debug, Clone, Default)]
pub struct RespIpMatcher {
    v4: PrefixSet<u32>,
    v6: PrefixSet<u128>,
}

impl RespIpMatcher {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_cidr(&mut self, cidr: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (addr, prefix_len) = parse_cidr(cidr)?;
        match addr {
            IpAddr::V4(a) => self.v4.insert(u32::from(a), prefix_len),
            IpAddr::V6(a) => self.v6.insert(u128::from(a), prefix_len),
        }
        Ok(())
    }

    pub fn from_str_args(s: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let mut m = Self::new();
        for part in s.split_whitespace() {
            m.add_cidr(part)?;
        }
        Ok(m)
    }

    fn matches_ip(&self, addr: IpAddr) -> bool {
        match addr {
            IpAddr::V4(a) => self.v4.contains(u32::from(a)),
            IpAddr::V6(a) => self.v6.contains(u128::from(a)),
        }
    }
}

impl Matcher for RespIpMatcher {
    fn match_ctx(&self, ctx: &Context) -> PluginResult<bool> {
        let resp = match ctx.response() {
            Some(r) => r,
            None => return Ok(false),
        };
        for rr in &resp.answers {
            let rdata = &rr.data;
            let ip: Option<IpAddr> = match rdata {
                hickory_proto::rr::RData::A(a) => Some(IpAddr::V4(a.0)),
                hickory_proto::rr::RData::AAAA(aaaa) => Some(IpAddr::V6(aaaa.0)),
                _ => None,
            };
            if let Some(ip) = ip
                && self.matches_ip(ip)
            {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hickory_proto::op::{Message, MessageType, OpCode, Query, ResponseCode};
    use hickory_proto::rr::{Name, RData, Record, RecordType};
    use std::net::Ipv4Addr;

    fn make_ctx_with_resp(ip: Ipv4Addr) -> Context {
        let mut msg = Message::new(1, MessageType::Query, OpCode::Query);
        msg.add_query({
            let mut q = Query::new();
            q.set_name(Name::from_ascii("example.com.").unwrap())
                .set_query_type(RecordType::A);
            q
        });
        let mut ctx = Context::new(msg);
        let mut resp = Message::response(1, OpCode::Query);
        resp.metadata.response_code = ResponseCode::NoError;
        resp.add_answer(Record::from_rdata(
            Name::from_ascii("example.com.").unwrap(),
            300,
            RData::A(ip.into()),
        ));
        ctx.set_response(Some(resp));
        ctx
    }

    #[test]
    fn matches_resp_ip() {
        let m = RespIpMatcher::from_str_args("10.0.0.0/8").unwrap();
        assert!(
            m.match_ctx(&make_ctx_with_resp(Ipv4Addr::new(10, 1, 2, 3)))
                .unwrap()
        );
        assert!(
            !m.match_ctx(&make_ctx_with_resp(Ipv4Addr::new(192, 168, 1, 1)))
                .unwrap()
        );
    }

    #[test]
    fn no_response_returns_false() {
        let m = RespIpMatcher::from_str_args("0.0.0.0/0").unwrap();
        let mut msg = Message::new(1, MessageType::Query, OpCode::Query);
        msg.add_query({
            let mut q = Query::new();
            q.set_name(Name::from_ascii("example.com.").unwrap())
                .set_query_type(RecordType::A);
            q
        });
        let ctx = Context::new(msg);
        assert!(!m.match_ctx(&ctx).unwrap());
    }

    #[test]
    fn rejects_prefix_len_too_large() {
        let mut m = RespIpMatcher::new();
        assert!(m.add_cidr("10.0.0.0/33").is_err());
        assert!(m.add_cidr("::1/129").is_err());
        assert!(m.add_cidr("10.0.0.0/32").is_ok());
        assert!(m.add_cidr("::1/128").is_ok());
    }
}
