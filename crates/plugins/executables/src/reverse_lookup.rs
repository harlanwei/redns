// Copyright (C) 2026, Harlan Wei
//
// This file is part of redns.

//! Reverse lookup — caches A/AAAA→domain mappings and handles PTR queries.
//!
//! After downstream resolution, saves IP→domain from A/AAAA answers.
//! When a PTR query arrives, returns cached domain if available.

use async_trait::async_trait;
use hickory_proto::op::{Message, MessageType, ResponseCode};
use hickory_proto::rr::{DNSClass, Name, RData, Record, RecordType};
use redns_core::context::MARK_CACHE_HIT;
use redns_core::plugin::PluginResult;
use redns_core::sequence::ChainWalker;
use redns_core::{Context, RecursiveExecutable};
use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::RwLock;
use std::time::{Duration, Instant};

/// Reverse lookup configuration.
#[derive(Debug, Clone)]
pub struct ReverseLookupConfig {
    /// Max entries in the cache.
    pub size: usize,
    /// Whether to handle PTR queries directly.
    pub handle_ptr: bool,
    /// TTL for cached entries (seconds).
    pub ttl: u64,
}

impl Default for ReverseLookupConfig {
    fn default() -> Self {
        Self {
            size: 64 * 1024,
            handle_ptr: true,
            ttl: 7200,
        }
    }
}

impl ReverseLookupConfig {
    pub fn from_str_args(s: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let s = s.trim();
        if s.is_empty() {
            return Ok(Self::default());
        }
        let size: usize = s.parse().unwrap_or(64 * 1024);
        Ok(Self {
            size,
            ..Default::default()
        })
    }
}

struct CacheEntry {
    domain: String,
    expires: Instant,
}

/// Reverse IP→domain cache.
pub struct ReverseLookup {
    config: ReverseLookupConfig,
    cache: RwLock<HashMap<IpAddr, CacheEntry>>,
}

impl ReverseLookup {
    pub fn new(config: ReverseLookupConfig) -> Self {
        Self {
            config,
            cache: RwLock::new(HashMap::new()),
        }
    }

    /// Look up the cached domain for an IP.
    fn lookup(&self, addr: &IpAddr) -> Option<String> {
        let cache = self.cache.read().unwrap();
        cache.get(addr).and_then(|entry| {
            if entry.expires > Instant::now() {
                Some(entry.domain.clone())
            } else {
                None
            }
        })
    }

    /// Save IP→domain mappings from A/AAAA response answers.
    fn save_ips(&self, query: &Message, response: &Message) {
        let now = Instant::now();
        let ttl = Duration::from_secs(self.config.ttl);

        // Use the query name as the canonical domain if available.
        let qname = query.queries().first().map(|q| q.name().to_ascii());

        let mut cache = self.cache.write().unwrap();
        for rr in response.answers() {
            let ip: Option<IpAddr> = match rr.data() {
                RData::A(a) => Some(IpAddr::V4(a.0)),
                RData::AAAA(aaaa) => Some(IpAddr::V6(aaaa.0)),
                _ => None,
            };
            if let Some(ip) = ip {
                let domain = qname.clone().unwrap_or_else(|| rr.name().to_ascii());
                cache.insert(
                    ip,
                    CacheEntry {
                        domain,
                        expires: now + ttl,
                    },
                );
            }
        }

        // Evict when over capacity.
        if cache.len() > self.config.size {
            // First pass: remove expired entries (cheap).
            cache.retain(|_, e| e.expires > now);

            // Second pass: if still over capacity, evict soonest-expiring entries.
            if cache.len() > self.config.size {
                let excess = cache.len() - self.config.size;
                let mut by_expiry: Vec<(IpAddr, Instant)> =
                    cache.iter().map(|(k, e)| (*k, e.expires)).collect();
                by_expiry.sort_unstable_by_key(|(_, exp)| *exp);
                for (ip, _) in by_expiry.into_iter().take(excess) {
                    cache.remove(&ip);
                }
            }
        }
    }

    /// Try to respond to a PTR query from cache.
    fn try_respond_ptr(&self, query: &Message) -> Option<Message> {
        if !self.config.handle_ptr {
            return None;
        }
        let question = query.queries().first()?;
        if question.query_type() != RecordType::PTR {
            return None;
        }

        // Parse PTR name → IP address.
        let ptr_name = question.name().to_ascii();
        let ip = parse_ptr_name(&ptr_name)?;
        let domain = self.lookup(&ip)?;

        // Build PTR response.
        let mut resp = Message::new();
        resp.set_id(query.id());
        resp.set_message_type(MessageType::Response);
        resp.set_response_code(ResponseCode::NoError);
        resp.add_query(question.clone());

        let ptr_rdata = RData::PTR(hickory_proto::rr::rdata::PTR(
            Name::from_ascii(&domain).unwrap_or_else(|_| Name::root()),
        ));
        let mut rr = Record::from_rdata(question.name().clone(), 5, ptr_rdata);
        rr.set_dns_class(DNSClass::IN);
        resp.add_answer(rr);

        Some(resp)
    }
}

/// Parse a PTR name like `1.0.168.192.in-addr.arpa.` → `192.168.0.1`.
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

#[async_trait]
impl RecursiveExecutable for ReverseLookup {
    async fn exec_recursive(&self, ctx: &mut Context, mut next: ChainWalker) -> PluginResult<()> {
        // Try to handle PTR from cache.
        if let Some(resp) = self.try_respond_ptr(ctx.query()) {
            ctx.set_response(Some(resp));
            ctx.set_mark(MARK_CACHE_HIT);
            return Ok(());
        }

        // Execute downstream.
        next.exec_next(ctx).await?;

        // Save IP→domain from response.
        if let Some(resp) = ctx.response() {
            self.save_ips(ctx.query(), resp);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hickory_proto::op::{Message, MessageType, OpCode, Query};
    use hickory_proto::rr::Name;

    fn make_a_response() -> (Message, Message) {
        let mut query = Message::new();
        query
            .set_id(1)
            .set_message_type(MessageType::Query)
            .set_op_code(OpCode::Query);
        query.add_query({
            let mut q = Query::new();
            q.set_name(Name::from_ascii("example.com.").unwrap())
                .set_query_type(RecordType::A);
            q
        });

        let mut resp = Message::new();
        resp.set_id(1);
        resp.set_message_type(MessageType::Response);
        resp.set_response_code(ResponseCode::NoError);
        resp.add_answer(Record::from_rdata(
            Name::from_ascii("example.com.").unwrap(),
            60,
            RData::A(std::net::Ipv4Addr::new(1, 2, 3, 4).into()),
        ));

        (query, resp)
    }

    #[test]
    fn parse_ptr_ipv4() {
        let ip = parse_ptr_name("4.3.2.1.in-addr.arpa.");
        assert_eq!(ip, Some(IpAddr::V4("1.2.3.4".parse().unwrap())));
    }

    #[test]
    fn save_and_lookup() {
        let rl = ReverseLookup::new(ReverseLookupConfig::default());
        let (query, resp) = make_a_response();
        rl.save_ips(&query, &resp);
        let domain = rl.lookup(&IpAddr::V4("1.2.3.4".parse().unwrap()));
        assert_eq!(domain.as_deref(), Some("example.com."));
    }

    #[test]
    fn ptr_response_from_cache() {
        let rl = ReverseLookup::new(ReverseLookupConfig::default());
        let (query, resp) = make_a_response();
        rl.save_ips(&query, &resp);

        // Build PTR query.
        let mut ptr_query = Message::new();
        ptr_query
            .set_id(2)
            .set_message_type(MessageType::Query)
            .set_op_code(OpCode::Query);
        ptr_query.add_query({
            let mut q = Query::new();
            q.set_name(Name::from_ascii("4.3.2.1.in-addr.arpa.").unwrap())
                .set_query_type(RecordType::PTR);
            q
        });

        let resp = rl.try_respond_ptr(&ptr_query);
        assert!(resp.is_some());
        assert_eq!(resp.unwrap().answers().len(), 1);
    }

    #[test]
    fn from_str_args_default() {
        let cfg = ReverseLookupConfig::from_str_args("").unwrap();
        assert_eq!(cfg.size, 64 * 1024);
        assert_eq!(cfg.ttl, 7200);
    }
}
