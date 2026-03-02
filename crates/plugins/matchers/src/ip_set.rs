// Copyright (C) 2026, Harlan Wei
//
// This file is part of redns.

//! IP set data provider — loads IP/CIDR lists for matching.
//!
//! Ports `plugin/data_provider/ip_set/ip_set.go`.

use ipnet::IpNet;
use redns_core::plugin::PluginResult;
use redns_core::{Context, Matcher};
use std::net::IpAddr;
use tracing::warn;

/// An IP set that matches response IPs against loaded IP/CIDR patterns.
pub struct IpSet {
    /// Loaded IP networks (CIDRs).
    nets: Vec<IpNet>,
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
        Self { nets: Vec::new() }
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
        self.nets.push(net);
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
        self.nets.iter().any(|net| net.contains(&addr))
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
                if let Some(ip) = ip {
                    if self.matches_ip(ip) {
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
}
