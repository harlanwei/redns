// Copyright (C) 2026, Harlan Wei
//
// This file is part of redns.

//! Client IP matcher — matches client address against CIDR ranges.

use crate::prefix_set::{PrefixSet, parse_cidr};
use redns_core::plugin::PluginResult;
use redns_core::{Context, Matcher};
use std::net::IpAddr;

/// Matches the client IP from `Context.server_meta.client_addr`
/// against a list of CIDR ranges.
#[derive(Debug, Clone, Default)]
pub struct ClientIpMatcher {
    v4: PrefixSet<u32>,
    v6: PrefixSet<u128>,
}

impl ClientIpMatcher {
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a CIDR range (e.g. "192.168.0.0/16" or "10.0.0.1").
    pub fn add_cidr(&mut self, cidr: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (addr, prefix_len) = parse_cidr(cidr)?;
        match addr {
            IpAddr::V4(a) => self.v4.insert(u32::from(a), prefix_len),
            IpAddr::V6(a) => self.v6.insert(u128::from(a), prefix_len),
        }
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

    fn matches_ip(&self, addr: IpAddr) -> bool {
        match addr {
            IpAddr::V4(a) => self.v4.contains(u32::from(a)),
            IpAddr::V6(a) => self.v6.contains(u128::from(a)),
        }
    }
}

impl Matcher for ClientIpMatcher {
    fn match_ctx(&self, ctx: &Context) -> PluginResult<bool> {
        if let Some(client_addr) = ctx.server_meta.client_addr {
            Ok(self.matches_ip(client_addr))
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
        let mut msg = Message::new(1, MessageType::Query, OpCode::Query);
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
    fn from_str_args_works() {
        let m = ClientIpMatcher::from_str_args("10.0.0.0/8 192.168.0.0/16").unwrap();
        assert!(m.match_ctx(&make_ctx_with_ip("10.1.2.3")).unwrap());
        assert!(m.match_ctx(&make_ctx_with_ip("192.168.99.1")).unwrap());
        assert!(!m.match_ctx(&make_ctx_with_ip("172.16.0.1")).unwrap());
    }

    #[test]
    fn rejects_prefix_len_too_large() {
        let mut m = ClientIpMatcher::new();
        assert!(m.add_cidr("10.0.0.0/33").is_err());
        assert!(m.add_cidr("::1/129").is_err());
        assert!(m.add_cidr("10.0.0.0/32").is_ok());
        assert!(m.add_cidr("::1/128").is_ok());
    }
}
