// Copyright (C) 2026, Harlan Wei
//
// This file is part of redns.

//! PTR IP matcher — matches IPs extracted from PTR query names against CIDRs.

use crate::prefix_set::PrefixSet;
use ipnet::IpNet;
use redns_core::plugin::PluginResult;
use redns_core::{Context, Matcher};
use std::net::IpAddr;

/// PTR IP matcher — extracts IP from PTR query name and matches against CIDR ranges.
pub struct PtrIpMatcher {
    v4: PrefixSet<u32>,
    v6: PrefixSet<u128>,
}

impl PtrIpMatcher {
    pub fn new(ranges: Vec<IpNet>) -> Self {
        let mut m = Self {
            v4: PrefixSet::default(),
            v6: PrefixSet::default(),
        };
        for net in ranges {
            match net {
                IpNet::V4(n) => m.v4.insert(u32::from(n.network()), n.prefix_len()),
                IpNet::V6(n) => m.v6.insert(u128::from(n.network()), n.prefix_len()),
            }
        }
        m
    }

    /// Parse from string args: space-separated CIDRs.
    pub fn from_str_args(s: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let ranges: Result<Vec<IpNet>, _> = s
            .split_whitespace()
            .filter(|s| !s.is_empty())
            .map(|s| s.parse::<IpNet>())
            .collect();
        Ok(Self::new(ranges?))
    }

    fn matches_ip(&self, addr: IpAddr) -> bool {
        match addr {
            IpAddr::V4(a) => self.v4.contains(u32::from(a)),
            IpAddr::V6(a) => self.v6.contains(u128::from(a)),
        }
    }
}

/// Strip `suffix` from `s`, comparing ASCII-case-insensitively.
fn strip_suffix_ci<'a>(s: &'a str, suffix: &str) -> Option<&'a str> {
    let mid = s.len().checked_sub(suffix.len())?;
    let (head, tail) = (s.get(..mid)?, s.get(mid..)?);
    tail.eq_ignore_ascii_case(suffix).then_some(head)
}

/// Parse a dotted-quad PTR fragment like `4.3.2.1` → `1.2.3.4`
/// (octets reversed). Rejects leading zeros and out-of-range octets,
/// matching `Ipv4Addr`'s string parsing.
fn parse_ptr_v4(stripped: &str) -> Option<IpAddr> {
    let mut octets = [0u8; 4];
    let mut count = 0usize;
    for part in stripped.split('.') {
        if count == 4
            || part.is_empty()
            || part.len() > 3
            || !part.bytes().all(|b| b.is_ascii_digit())
            || (part.len() > 1 && part.starts_with('0'))
        {
            return None;
        }
        octets[3 - count] = part.parse().ok()?;
        count += 1;
    }
    if count != 4 {
        return None;
    }
    Some(IpAddr::V4(octets.into()))
}

/// Parse a 32-nibble PTR fragment (`b.a.9...` reversed nibbles) into an
/// IPv6 address without intermediate strings.
fn parse_ptr_v6(stripped: &str) -> Option<IpAddr> {
    let mut value: u128 = 0;
    let mut count = 0u32;
    for part in stripped.split('.') {
        let b = part.as_bytes();
        if count == 32 || b.len() != 1 {
            return None;
        }
        let digit = (b[0] as char).to_digit(16)?;
        value |= (digit as u128) << (4 * count);
        count += 1;
    }
    if count != 32 {
        return None;
    }
    Some(IpAddr::V6(value.into()))
}

/// Parse a PTR name like `4.3.2.1.in-addr.arpa.` → `1.2.3.4`.
fn parse_ptr_name(name: &str) -> Option<IpAddr> {
    if let Some(stripped) =
        strip_suffix_ci(name, ".in-addr.arpa.").or_else(|| strip_suffix_ci(name, ".in-addr.arpa"))
    {
        return parse_ptr_v4(stripped);
    }
    if let Some(stripped) =
        strip_suffix_ci(name, ".ip6.arpa.").or_else(|| strip_suffix_ci(name, ".ip6.arpa"))
    {
        return parse_ptr_v6(stripped);
    }
    None
}

impl Matcher for PtrIpMatcher {
    fn match_ctx(&self, ctx: &Context) -> PluginResult<bool> {
        use hickory_proto::rr::RecordType;
        for question in &ctx.query().queries {
            if question.query_type() == RecordType::PTR {
                let name = question.name().to_ascii();
                if let Some(ip) = parse_ptr_name(&name)
                    && self.matches_ip(ip)
                {
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
    use hickory_proto::op::{Message, MessageType, OpCode, Query};
    use hickory_proto::rr::{Name, RecordType};

    fn make_ptr_ctx(ptr_name: &str) -> Context {
        let mut msg = Message::new(1, MessageType::Query, OpCode::Query);
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
    fn parse_ptr_ipv4_works() {
        assert_eq!(
            parse_ptr_name("4.3.2.1.in-addr.arpa."),
            Some("1.2.3.4".parse().unwrap())
        );
    }
}
