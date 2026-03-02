// Copyright (C) 2026, Harlan Wei
//
// This file is part of redns.

//! Client IP matcher — matches client address against CIDR ranges.

use redns_core::plugin::PluginResult;
use redns_core::{Context, Matcher};
use std::net::IpAddr;

/// A CIDR range.
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
            // Single IP — /32 or /128.
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
                let net_bits = u32::from(*net);
                let addr_bits = u32::from(*a);
                let mask = if self.prefix_len == 0 {
                    0
                } else {
                    !0u32 << (32 - self.prefix_len)
                };
                (net_bits & mask) == (addr_bits & mask)
            }
            (IpAddr::V6(net), IpAddr::V6(a)) => {
                let net_bits = u128::from(*net);
                let addr_bits = u128::from(*a);
                let mask = if self.prefix_len == 0 {
                    0
                } else {
                    !0u128 << (128 - self.prefix_len)
                };
                (net_bits & mask) == (addr_bits & mask)
            }
            _ => false,
        }
    }
}

/// Matches the client IP from `Context.server_meta.client_addr`
/// against a list of CIDR ranges.
#[derive(Debug, Clone)]
pub struct ClientIpMatcher {
    ranges: Vec<CidrRange>,
}

impl ClientIpMatcher {
    pub fn new() -> Self {
        Self { ranges: Vec::new() }
    }

    /// Adds a CIDR range (e.g. "192.168.0.0/16" or "10.0.0.1").
    pub fn add_cidr(&mut self, cidr: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.ranges.push(CidrRange::parse(cidr)?);
        Ok(())
    }

    /// Parse from space-separated CIDR strings.
    pub fn from_str_args(s: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let mut m = Self::new();
        for part in s.split_whitespace() {
            m.add_cidr(part)?;
        }
        Ok(m)
    }
}

impl Default for ClientIpMatcher {
    fn default() -> Self {
        Self::new()
    }
}

impl Matcher for ClientIpMatcher {
    fn match_ctx(&self, ctx: &Context) -> PluginResult<bool> {
        if let Some(client_addr) = &ctx.server_meta.client_addr {
            Ok(self.ranges.iter().any(|r| r.contains(client_addr)))
        } else {
            Ok(false)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hickory_proto::op::{Message, MessageType, OpCode, Query};
    use hickory_proto::rr::{Name, RecordType};

    fn make_ctx_with_ip(ip: &str) -> Context {
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
        ctx.server_meta.client_addr = Some(ip.parse().unwrap());
        ctx
    }

    #[test]
    fn matches_single_ip() {
        let mut m = ClientIpMatcher::new();
        m.add_cidr("10.0.0.1").unwrap();
        assert!(m.match_ctx(&make_ctx_with_ip("10.0.0.1")).unwrap());
        assert!(!m.match_ctx(&make_ctx_with_ip("10.0.0.2")).unwrap());
    }

    #[test]
    fn matches_cidr() {
        let mut m = ClientIpMatcher::new();
        m.add_cidr("192.168.0.0/16").unwrap();
        assert!(m.match_ctx(&make_ctx_with_ip("192.168.1.100")).unwrap());
        assert!(!m.match_ctx(&make_ctx_with_ip("10.0.0.1")).unwrap());
    }

    #[test]
    fn no_client_addr_returns_false() {
        let m = ClientIpMatcher::from_str_args("0.0.0.0/0").unwrap();
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

    #[test]
    fn from_str_args_works() {
        let m = ClientIpMatcher::from_str_args("10.0.0.0/8 192.168.0.0/16").unwrap();
        assert!(m.match_ctx(&make_ctx_with_ip("10.1.2.3")).unwrap());
        assert!(m.match_ctx(&make_ctx_with_ip("192.168.99.1")).unwrap());
        assert!(!m.match_ctx(&make_ctx_with_ip("172.16.0.1")).unwrap());
    }
}
