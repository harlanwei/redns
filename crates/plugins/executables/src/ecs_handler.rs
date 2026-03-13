// Copyright (C) 2026, Harlan Wei
//
// This file is part of redns.

//! EDNS Client Subnet (ECS) handler.
//!
//! Adds an ECS OPT record to the query before sending upstream,
//! using either a preset address or the client address.

use async_trait::async_trait;
use hickory_proto::op::Edns;
use hickory_proto::rr::rdata::opt::ClientSubnet;
use redns_core::plugin::PluginResult;
use redns_core::sequence::ChainWalker;
use redns_core::{Context, RecursiveExecutable};
use std::net::IpAddr;

/// Default IPv4 subnet mask for ECS.
const DEFAULT_MASK4: u8 = 24;
/// Default IPv6 subnet mask for ECS.
const DEFAULT_MASK6: u8 = 48;

/// ECS handler configuration.
#[derive(Debug, Clone)]
pub struct EcsConfig {
    /// Send the client's actual IP as ECS to upstream.
    pub send: bool,
    /// Preset IP address to use as ECS source.
    pub preset: Option<IpAddr>,
    /// IPv4 source prefix length (default 24).
    pub mask4: u8,
    /// IPv6 source prefix length (default 48).
    pub mask6: u8,
}

impl Default for EcsConfig {
    fn default() -> Self {
        Self {
            send: false,
            preset: None,
            mask4: DEFAULT_MASK4,
            mask6: DEFAULT_MASK6,
        }
    }
}

impl EcsConfig {
    /// Parse from string args: `[preset_ip]` or empty for default.
    pub fn from_str_args(s: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let s = s.trim();
        if s.is_empty() {
            return Ok(Self {
                send: true,
                ..Default::default()
            });
        }
        // Strip optional /mask suffix.
        let ip_str = s.split('/').next().unwrap_or(s);
        let preset: IpAddr =
            ip_str
                .parse()
                .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                    format!("invalid ECS preset IP '{}': {}", ip_str, e).into()
                })?;
        Ok(Self {
            preset: Some(preset),
            ..Default::default()
        })
    }
}

/// ECS handler plugin — adds EDNS Client Subnet to DNS queries.
#[derive(Debug, Clone)]
pub struct EcsHandler {
    config: EcsConfig,
}

impl EcsHandler {
    pub fn new(config: EcsConfig) -> Self {
        Self { config }
    }

    /// Create a ClientSubnet for the given address.
    fn make_subnet(addr: &IpAddr, mask: u8) -> ClientSubnet {
        ClientSubnet::new(*addr, mask, 0)
    }

    /// Add ECS option to the query via the Edns API.
    fn add_ecs_to_query(msg: &mut hickory_proto::op::Message, subnet: ClientSubnet) {
        // Use the Edns builder to attach ECS.
        let mut edns = msg.extensions().as_ref().cloned().unwrap_or_else(Edns::new);
        edns.options_mut()
            .insert(hickory_proto::rr::rdata::opt::EdnsOption::Subnet(subnet));
        msg.set_edns(edns);
    }

    /// Check if the query already has an ECS option in its EDNS.
    fn has_ecs(msg: &hickory_proto::op::Message) -> bool {
        if let Some(edns) = msg.extensions().as_ref() {
            edns.options()
                .as_ref()
                .iter()
                .any(|(code, _)| *code == hickory_proto::rr::rdata::opt::EdnsCode::Subnet)
        } else {
            false
        }
    }
}

#[async_trait]
impl RecursiveExecutable for EcsHandler {
    async fn exec_recursive(
        &self,
        ctx: &mut Context,
        mut next: ChainWalker,
    ) -> PluginResult<()> {
        // Don't add ECS if query already has one.
        if Self::has_ecs(ctx.query()) {
            return next.exec_next(ctx).await;
        }

        // Determine which IP to use for ECS.
        let ecs_addr = if let Some(preset) = &self.config.preset {
            Some(*preset)
        } else if self.config.send {
            ctx.server_meta.client_addr
        } else {
            None
        };

        if let Some(addr) = ecs_addr {
            let mask = match addr {
                IpAddr::V4(_) => self.config.mask4,
                IpAddr::V6(_) => self.config.mask6,
            };
            let subnet = Self::make_subnet(&addr, mask);
            Self::add_ecs_to_query(ctx.query_mut(), subnet);
        }

        next.exec_next(ctx).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hickory_proto::op::{Message, MessageType, OpCode, Query};
    use hickory_proto::rr::{Name, RecordType};
    use redns_core::plugin::Executable;
    use redns_core::sequence::{ChainNode, NodeExecutor, Sequence};

    struct NopExec;
    #[async_trait]
    impl Executable for NopExec {
        async fn exec(&self, _ctx: &mut Context) -> PluginResult<()> {
            Ok(())
        }
    }

    fn make_query() -> Message {
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
        msg
    }

    #[tokio::test]
    async fn ecs_adds_edns() {
        let cfg = EcsConfig {
            preset: Some("1.2.3.0".parse().unwrap()),
            mask4: 24,
            ..Default::default()
        };
        let handler = EcsHandler::new(cfg);
        let chain = vec![
            ChainNode {
                matchers: vec![],
                executor: NodeExecutor::Recursive(Box::new(handler)),
            },
            ChainNode {
                matchers: vec![],
                executor: NodeExecutor::Simple(Box::new(NopExec)),
            },
        ];
        let seq = Sequence::new(chain);
        let mut ctx = Context::new(make_query());
        seq.exec(&mut ctx).await.unwrap();
        // Should now have EDNS with ECS.
        assert!(EcsHandler::has_ecs(ctx.query()));
    }

    #[test]
    fn from_str_args_preset() {
        let cfg = EcsConfig::from_str_args("1.2.3.0").unwrap();
        assert_eq!(cfg.preset, Some("1.2.3.0".parse().unwrap()));
    }

    #[test]
    fn from_str_args_empty() {
        let cfg = EcsConfig::from_str_args("").unwrap();
        assert!(cfg.preset.is_none());
        assert!(cfg.send);
    }
}
