// Copyright (C) 2026, Harlan Wei
//
// This file is part of redns.

//! Hosts-file lookup — resolves queries from domain→IP mappings.

use async_trait::async_trait;
use hickory_proto::op::{Message, MessageType, ResponseCode};
use hickory_proto::rr::{Name, RData, Record, RecordType};
use redns_core::plugin::PluginResult;
use redns_core::{Context, Executable};
use std::collections::HashMap;
use std::net::{Ipv4Addr, Ipv6Addr};

/// A hosts entry: domain → list of IPs.
#[derive(Debug, Clone)]
struct HostEntry {
    ipv4: Vec<Ipv4Addr>,
    ipv6: Vec<Ipv6Addr>,
}

/// Hosts plugin — resolves queries from a hosts-style mapping.
#[derive(Debug, Clone)]
pub struct Hosts {
    entries: HashMap<Name, HostEntry>,
}

impl Hosts {
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    /// Adds a hosts entry: `domain ip1 [ip2 ...]`.
    pub fn add_entry(
        &mut self,
        domain: Name,
        ips: &[&str],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let entry = self.entries.entry(domain).or_insert_with(|| HostEntry {
            ipv4: Vec::new(),
            ipv6: Vec::new(),
        });
        for ip_str in ips {
            if let Ok(v4) = ip_str.parse::<Ipv4Addr>() {
                entry.ipv4.push(v4);
            } else if let Ok(v6) = ip_str.parse::<Ipv6Addr>() {
                entry.ipv6.push(v6);
            } else {
                return Err(format!("invalid IP: {}", ip_str).into());
            }
        }
        Ok(())
    }

    /// Parses hosts-file style lines: "IP domain [domain...]" or "domain IP [IP...]".
    /// We use domain-first format: "domain IP [IP...]".
    pub fn from_lines(lines: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let mut hosts = Self::new();
        for line in lines.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() < 2 {
                continue;
            }
            let domain = Name::from_ascii(parts[0]).map_err(
                |e| -> Box<dyn std::error::Error + Send + Sync> {
                    format!("invalid domain {}: {e}", parts[0]).into()
                },
            )?;
            hosts.add_entry(domain, &parts[1..])?;
        }
        Ok(hosts)
    }
}

impl Default for Hosts {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Executable for Hosts {
    async fn exec(&self, ctx: &mut Context) -> PluginResult<()> {
        let question = match ctx.question() {
            Some(q) => q.clone(),
            None => return Ok(()),
        };
        let qname = question.name();
        let qtype = question.query_type();

        if let Some(entry) = self.entries.get(qname) {
            let records: Vec<Record> = match qtype {
                RecordType::A if !entry.ipv4.is_empty() => entry
                    .ipv4
                    .iter()
                    .map(|ip| Record::from_rdata(qname.clone(), 300, RData::A((*ip).into())))
                    .collect(),
                RecordType::AAAA if !entry.ipv6.is_empty() => entry
                    .ipv6
                    .iter()
                    .map(|ip| Record::from_rdata(qname.clone(), 300, RData::AAAA((*ip).into())))
                    .collect(),
                _ => return Ok(()),
            };

            if records.is_empty() {
                return Ok(());
            }

            let mut resp = Message::new();
            resp.set_id(ctx.query().id());
            resp.set_message_type(MessageType::Response);
            resp.set_response_code(ResponseCode::NoError);
            resp.add_query(question);
            for rr in records {
                resp.add_answer(rr);
            }
            ctx.set_response(Some(resp));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hickory_proto::op::{Message, MessageType, OpCode, Query};

    fn make_ctx(name: &str, rtype: RecordType) -> Context {
        let mut msg = Message::new();
        msg.set_id(1)
            .set_message_type(MessageType::Query)
            .set_op_code(OpCode::Query);
        msg.add_query({
            let mut q = Query::new();
            q.set_name(Name::from_ascii(name).unwrap())
                .set_query_type(rtype);
            q
        });
        Context::new(msg)
    }

    #[tokio::test]
    async fn hosts_lookup_hit() {
        let mut hosts = Hosts::new();
        hosts
            .add_entry(Name::from_ascii("myhost.local.").unwrap(), &["10.0.0.1"])
            .unwrap();
        let mut ctx = make_ctx("myhost.local.", RecordType::A);
        hosts.exec(&mut ctx).await.unwrap();
        assert_eq!(ctx.response().unwrap().answers().len(), 1);
    }

    #[tokio::test]
    async fn hosts_lookup_miss() {
        let hosts = Hosts::new();
        let mut ctx = make_ctx("unknown.local.", RecordType::A);
        hosts.exec(&mut ctx).await.unwrap();
        assert!(ctx.response().is_none());
    }

    #[test]
    fn from_lines_parses() {
        let hosts =
            Hosts::from_lines("myhost.local. 10.0.0.1 ::1\n# comment\nother.local. 192.168.1.1")
                .unwrap();
        assert_eq!(hosts.entries.len(), 2);
    }
}
