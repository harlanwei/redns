// Copyright (C) 2026, Harlan Wei
//
// This file is part of redns.

//! PTR IP matcher — matches IPs extracted from PTR query names against CIDRs.

use ipnet::IpNet;
use redns_core::plugin::PluginResult;
use redns_core::{Context, Matcher};
use std::net::IpAddr;

/// PTR IP matcher — extracts IP from PTR query name and matches against CIDR ranges.
pub struct PtrIpMatcher {
    ranges: Vec<IpNet>,
}

impl PtrIpMatcher {
    pub fn new(ranges: Vec<IpNet>) -> Self {
        Self { ranges }
    }

    /// Parse from string args: space-separated CIDRs.
    pub fn from_str_args(s: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let ranges: Result<Vec<IpNet>, _> = s
            .split_whitespace()
            .filter(|s| !s.is_empty())
            .map(|s| s.parse::<IpNet>())
            .collect();
        Ok(Self { ranges: ranges? })
    }
}

/// Parse a PTR name like `4.3.2.1.in-addr.arpa.` → `1.2.3.4`.
fn parse_ptr_name(name: &str) -> Option<IpAddr> {
    let lower = name.to_lowercase();
    if let Some(stripped) = lower
        .strip_suffix(".in-addr.arpa.")
        .or_else(|| lower.strip_suffix(".in-addr.arpa"))
    {
        let parts: Vec<&str> = stripped.split('.').collect();
        if parts.len() == 4 {
            let s = format!("{}.{}.{}.{}", parts[3], parts[2], parts[1], parts[0]);
            return s.parse().ok();
        }
    }
    if let Some(stripped) = lower
        .strip_suffix(".ip6.arpa.")
        .or_else(|| lower.strip_suffix(".ip6.arpa"))
    {
        let nibbles: Vec<&str> = stripped.split('.').collect();
        if nibbles.len() == 32 {
            let hex: String = nibbles.iter().rev().copied().collect();
            if hex.len() == 32 {
                let mut octets = [0u8; 16];
                for i in 0..16 {
                    octets[i] = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).ok()?;
                }
                return Some(IpAddr::V6(octets.into()));
            }
        }
    }
    None
}

impl Matcher for PtrIpMatcher {
    fn match_ctx(&self, ctx: &Context) -> PluginResult<bool> {
        use hickory_proto::rr::RecordType;
        for question in ctx.query().queries() {
            if question.query_type() == RecordType::PTR {
                let name = question.name().to_ascii();
                if let Some(ip) = parse_ptr_name(&name) {
                    if self.ranges.iter().any(|r: &IpNet| r.contains(&ip)) {
                        return Ok(true);
                    }
                }
            }
        }
        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hickory_proto::op::{Message, MessageType, OpCode, Query};
    use hickory_proto::rr::{Name, RecordType};

    fn make_ptr_ctx(ptr_name: &str) -> Context {
        let mut msg = Message::new();
        msg.set_id(1)
            .set_message_type(MessageType::Query)
            .set_op_code(OpCode::Query);
        msg.add_query({
            let mut q = Query::new();
            q.set_name(Name::from_ascii(ptr_name).unwrap())
                .set_query_type(RecordType::PTR);
            q
        });
        Context::new(msg)
    }

    #[test]
    fn matches_ptr_ipv4() {
        let m = PtrIpMatcher::from_str_args("10.0.0.0/8").unwrap();
        let ctx = make_ptr_ctx("1.0.0.10.in-addr.arpa.");
        assert!(m.match_ctx(&ctx).unwrap());
    }

    #[test]
    fn rejects_non_matching_ptr() {
        let m = PtrIpMatcher::from_str_args("10.0.0.0/8").unwrap();
        let ctx = make_ptr_ctx("1.168.192.192.in-addr.arpa.");
        assert!(!m.match_ctx(&ctx).unwrap());
    }

    #[test]
    fn non_ptr_query_returns_false() {
        let m = PtrIpMatcher::from_str_args("0.0.0.0/0").unwrap();
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
    fn parse_ptr_ipv4_works() {
        assert_eq!(
            parse_ptr_name("4.3.2.1.in-addr.arpa."),
            Some("1.2.3.4".parse().unwrap())
        );
    }
}
