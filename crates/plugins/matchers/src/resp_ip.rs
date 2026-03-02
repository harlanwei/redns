// Copyright (C) 2026, Harlan Wei
//
// This file is part of redns.

//! Response IP matcher — matches A/AAAA IPs in DNS response against CIDR ranges.

use redns_core::plugin::PluginResult;
use redns_core::{Context, Matcher};
use std::net::IpAddr;

/// CIDR range (shared logic with client_ip).
#[derive(Debug, Clone)]
struct CidrRange {
    network: IpAddr,
    prefix_len: u8,
}

impl CidrRange {
    fn parse(s: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        if let Some((addr_str, len_str)) = s.split_once('/') {
            let network: IpAddr = addr_str.parse()?;
            let prefix_len: u8 = len_str.parse()?;
            Ok(Self {
                network,
                prefix_len,
            })
        } else {
            let network: IpAddr = s.parse()?;
            let prefix_len = match network {
                IpAddr::V4(_) => 32,
                IpAddr::V6(_) => 128,
            };
            Ok(Self {
                network,
                prefix_len,
            })
        }
    }

    fn contains(&self, addr: &IpAddr) -> bool {
        match (&self.network, addr) {
            (IpAddr::V4(net), IpAddr::V4(a)) => {
                let mask = if self.prefix_len == 0 {
                    0
                } else {
                    !0u32 << (32 - self.prefix_len)
                };
                (u32::from(*net) & mask) == (u32::from(*a) & mask)
            }
            (IpAddr::V6(net), IpAddr::V6(a)) => {
                let mask = if self.prefix_len == 0 {
                    0
                } else {
                    !0u128 << (128 - self.prefix_len)
                };
                (u128::from(*net) & mask) == (u128::from(*a) & mask)
            }
            _ => false,
        }
    }
}

/// Matches A/AAAA records in the DNS response against CIDR ranges.
#[derive(Debug, Clone)]
pub struct RespIpMatcher {
    ranges: Vec<CidrRange>,
}

impl RespIpMatcher {
    pub fn new() -> Self {
        Self { ranges: Vec::new() }
    }

    pub fn add_cidr(&mut self, cidr: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.ranges.push(CidrRange::parse(cidr)?);
        Ok(())
    }

    pub fn from_str_args(s: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let mut m = Self::new();
        for part in s.split_whitespace() {
            m.add_cidr(part)?;
        }
        Ok(m)
    }
}

impl Default for RespIpMatcher {
    fn default() -> Self {
        Self::new()
    }
}

impl Matcher for RespIpMatcher {
    fn match_ctx(&self, ctx: &Context) -> PluginResult<bool> {
        let resp = match ctx.response() {
            Some(r) => r,
            None => return Ok(false),
        };
        for rr in resp.answers() {
            let rdata = rr.data();
            let ip: Option<IpAddr> = match rdata {
                hickory_proto::rr::RData::A(a) => Some(IpAddr::V4(a.0)),
                hickory_proto::rr::RData::AAAA(aaaa) => Some(IpAddr::V6(aaaa.0)),
                _ => None,
            };
            if let Some(ip) = ip {
                if self.ranges.iter().any(|r| r.contains(&ip)) {
                    return Ok(true);
                }
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
        resp.set_id(1)
            .set_message_type(MessageType::Response)
            .set_response_code(ResponseCode::NoError);
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
        let ctx = Context::new(msg);
        assert!(!m.match_ctx(&ctx).unwrap());
    }
}
