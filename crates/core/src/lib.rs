// Copyright (C) 2026, Harlan Wei
//
// This file is part of redns.

pub mod built_in;
pub mod chain_builder;
pub mod config;
pub mod context;
pub mod doh_server;
pub mod plugin;
pub mod redns;
pub mod registry;
pub mod sequence;
pub mod server;
pub mod tcp_server;
pub mod udp_server;
pub mod upstream;

/// Ensure rustls has a process-level crypto provider installed.
///
/// This avoids runtime panics when multiple providers are compiled in.
pub fn install_rustls_crypto_provider() {
    use tokio_rustls::rustls::crypto::{CryptoProvider, ring};

    if CryptoProvider::get_default().is_none() {
        let _ = ring::default_provider().install_default();
    }
}

// Re-exports for convenience.
pub use chain_builder::ChainBuilder;
pub use config::{Config, MatchConfig, PluginConfig, RuleArgs, RuleConfig};
pub use context::Context;
pub use plugin::{
    Executable, Matcher, PluginError, PluginResult, RecursiveExecutable, ReverseMatcher,
};
pub use redns::Redns;
pub use registry::PluginRegistry;
pub use sequence::{ChainNode, ChainWalker, NodeExecutor, Sequence};
pub use server::{DnsHandler, EntryHandler, QueryMeta};
pub use upstream::{
    DohUpstream, PooledTcpUpstream, PooledTlsUpstream, TcpUpstream, TlsUpstream, UdpUpstream,
    Upstream, UpstreamMetrics, UpstreamWrapper,
};
