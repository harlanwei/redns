// Copyright (C) 2026, Harlan Wei
//
// This file is part of redns.

//! IP set data provider — loads IP/CIDR lists for matching.

use ipnet::IpNet;
use redns_core::plugin::PluginResult;
use redns_core::{Context, Matcher};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::net::IpAddr;
use tracing::warn;

/// Longest-prefix-match set for a single address family.
///
/// Networks are bucketed by prefix length: `buckets[len]` holds every network
/// of that length as its masked network address. A membership test masks the
/// query address to each *present* prefix length and probes that bucket's hash
/// set, so a lookup is O(distinct prefix lengths) (≤ 33 for v4, ≤ 129 for v6)
/// regardless of how many networks are loaded — not the O(n) linear scan a
/// `Vec<IpNet>` would require for large block/allow lists.
#[derive(Default)]
struct PrefixSet<T> {
    /// prefix length → set of masked network addresses at that length.
    buckets: HashMap<u8, HashSet<T>>,
    /// Distinct prefix lengths present, kept sorted for deterministic probing.
    lengths: BTreeSet<u8>,
}

impl PrefixSet<u32> {
    fn insert(&mut self, addr: u32, prefix_len: u8) {
        let masked = mask_v4(addr, prefix_len);
        self.buckets.entry(prefix_len).or_default().insert(masked);
        self.lengths.insert(prefix_len);
    }

    fn contains(&self, addr: u32) -> bool {
        self.lengths.iter().any(|&len| {
            self.buckets
                .get(&len)
                .is_some_and(|set| set.contains(&mask_v4(addr, len)))
        })
    }
}

impl PrefixSet<u128> {
    fn insert(&mut self, addr: u128, prefix_len: u8) {
        let masked = mask_v6(addr, prefix_len);
        self.buckets.entry(prefix_len).or_default().insert(masked);
        self.lengths.insert(prefix_len);
    }

    fn contains(&self, addr: u128) -> bool {
        self.lengths.iter().any(|&len| {
            self.buckets
                .get(&len)
                .is_some_and(|set| set.contains(&mask_v6(addr, len)))
        })
    }
}

/// Mask an IPv4 address (as `u32`) to the given prefix length.
fn mask_v4(addr: u32, prefix_len: u8) -> u32 {
    if prefix_len == 0 {
        0
    } else if prefix_len >= 32 {
        addr
    } else {
        addr & (!0u32 << (32 - prefix_len))
    }
}

/// Mask an IPv6 address (as `u128`) to the given prefix length.
fn mask_v6(addr: u128, prefix_len: u8) -> u128 {
    if prefix_len == 0 {
        0
    } else if prefix_len >= 128 {
        addr
    } else {
        addr & (!0u128 << (128 - prefix_len))
    }
}

/// An IP set that matches response IPs against loaded IP/CIDR patterns.
#[derive(Default)]
pub struct IpSet {
    v4: PrefixSet<u32>,
    v6: PrefixSet<u128>,
}

/// YAML args for ip_set plugin.
#[derive(Debug, Clone, serde::Deserialize, Default)]
pub struct IpSetArgs {
    #[serde(default)]
    pub ips: Vec<String>,
    #[serde(default)]
    pub files: Vec<String>,
}

impl IpSet {
    pub fn new() -> Self {
        Self::default()
    }

    /// Parse an IP or CIDR string. Bare IPs become /32 or /128.
    fn parse_net(s: &str) -> Result<IpNet, Box<dyn std::error::Error + Send + Sync>> {
        let s = s.trim();
        if s.contains('/') {
            s.parse::<IpNet>()
                .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                    format!("invalid CIDR {}: {}", s, e).into()
                })
        } else {
            let addr: IpAddr =
                s.parse()
                    .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                        format!("invalid IP {}: {}", s, e).into()
                    })?;
            Ok(IpNet::from(addr))
        }
    }

    /// Add an IP or CIDR.
    pub fn add_ip(&mut self, s: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let s = s.trim();
        if s.is_empty() || s.starts_with('#') {
            return Ok(());
        }
        let net = Self::parse_net(s)?;
        match net {
            IpNet::V4(n) => self
                .v4
                .insert(u32::from(n.network()), n.prefix_len()),
            IpNet::V6(n) => self
                .v6
                .insert(u128::from(n.network()), n.prefix_len()),
        }
        Ok(())
    }

    /// Load IPs from a file (one per line).
    pub fn load_file(
        &mut self,
        path: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let content = std::fs::read_to_string(path).map_err(
            |e| -> Box<dyn std::error::Error + Send + Sync> {
                format!("failed to read IP file {}: {}", path, e).into()
            },
        )?;
        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            self.add_ip(line)?;
        }
        Ok(())
    }

    /// Create from a YAML string.
    pub fn from_yaml_str(s: &str) -> PluginResult<Self> {
        let args: IpSetArgs =
            serde_saphyr::from_str(s).map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                format!("ip_set: invalid args: {e}").into()
            })?;
        Self::from_args(&args)
    }

    /// Create from parsed args.
    pub fn from_args(args: &IpSetArgs) -> PluginResult<Self> {
        let mut s = Self::new();
        for ip in &args.ips {
            s.add_ip(ip)?;
        }
        for file in &args.files {
            s.load_file(file)?;
        }
        Ok(s)
    }

    /// Create from a string (YAML or space-separated).
    pub fn from_str_args(s: &str) -> PluginResult<Self> {
        // Try YAML struct deserialization first.
        if let Ok(set) = Self::from_yaml_str(s) {
            return Ok(set);
        }
        let mut set = Self::new();
        for part in s.split_whitespace() {
            if let Err(e) = set.add_ip(part) {
                warn!(error = %e, ip = part, "skipping invalid IP");
            }
        }
        Ok(set)
    }

    /// Check if an IP address matches any entry in this set.
    pub fn matches_ip(&self, addr: IpAddr) -> bool {
        match addr {
            IpAddr::V4(v4) => self.v4.contains(u32::from(v4)),
            IpAddr::V6(v6) => self.v6.contains(u128::from(v6)),
        }
    }
}

/// IpSet as a Matcher: matches if any response A/AAAA record's IP is in the set.
impl Matcher for IpSet {
    fn match_ctx(&self, ctx: &Context) -> PluginResult<bool> {
        if let Some(resp) = ctx.response() {
            for rr in resp.answers() {
                let rdata = rr.data();
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
        }
        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_cidr() {
        let mut s = IpSet::new();
        s.add_ip("10.0.0.0/8").unwrap();
        assert!(s.matches_ip("10.1.2.3".parse().unwrap()));
        assert!(!s.matches_ip("192.168.1.1".parse().unwrap()));
    }

    #[test]
    fn matches_single_ip() {
        let mut s = IpSet::new();
        s.add_ip("1.1.1.1").unwrap();
        assert!(s.matches_ip("1.1.1.1".parse().unwrap()));
        assert!(!s.matches_ip("1.1.1.2".parse().unwrap()));
    }

    #[test]
    fn from_args_works() {
        let args = IpSetArgs {
            ips: vec!["192.168.0.0/16".into(), "10.0.0.1".into()],
            files: vec![],
        };
        let s = IpSet::from_args(&args).unwrap();
        assert!(s.matches_ip("192.168.1.1".parse().unwrap()));
        assert!(s.matches_ip("10.0.0.1".parse().unwrap()));
        assert!(!s.matches_ip("8.8.8.8".parse().unwrap()));
    }

    #[test]
    fn matches_ipv6_cidr() {
        let mut s = IpSet::new();
        s.add_ip("2001:db8::/32").unwrap();
        assert!(s.matches_ip("2001:db8::1".parse().unwrap()));
        assert!(s.matches_ip("2001:db8:dead:beef::1".parse().unwrap()));
        assert!(!s.matches_ip("2001:db9::1".parse().unwrap()));
    }

    #[test]
    fn matches_single_ipv6() {
        let mut s = IpSet::new();
        s.add_ip("::1").unwrap();
        assert!(s.matches_ip("::1".parse().unwrap()));
        assert!(!s.matches_ip("::2".parse().unwrap()));
    }

    #[test]
    fn overlapping_prefix_lengths_all_match() {
        // Multiple prefix lengths covering the same address must all be probed.
        let mut s = IpSet::new();
        s.add_ip("10.0.0.0/8").unwrap();
        s.add_ip("10.1.0.0/16").unwrap();
        s.add_ip("10.1.2.3/32").unwrap();
        // Hits via the /32, the /16, and the /8 respectively.
        assert!(s.matches_ip("10.1.2.3".parse().unwrap()));
        assert!(s.matches_ip("10.1.9.9".parse().unwrap()));
        assert!(s.matches_ip("10.9.9.9".parse().unwrap()));
        assert!(!s.matches_ip("11.0.0.0".parse().unwrap()));
    }

    #[test]
    fn zero_prefix_matches_everything() {
        let mut s = IpSet::new();
        s.add_ip("0.0.0.0/0").unwrap();
        assert!(s.matches_ip("1.2.3.4".parse().unwrap()));
        assert!(s.matches_ip("255.255.255.255".parse().unwrap()));
        // v6 default route does not match v4 addresses and vice versa.
        assert!(!s.matches_ip("::1".parse().unwrap()));
    }
}
