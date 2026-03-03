// Copyright (C) 2026, Harlan Wei
//
// This file is part of redns.

//! Forward plugin — sends queries to upstream DNS servers.

use async_trait::async_trait;
use hickory_proto::op::{Message, ResponseCode};
use redns_core::plugin::PluginResult;
use redns_core::upstream::{self, UpstreamOpts, UpstreamWrapper};
use redns_core::{Context, Executable};
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Instant;
use tracing::{debug, warn};

// ── Constants ───────────────────────────────────────────────────

const MAX_CONCURRENT_QUERIES: usize = 3;
const WEIGHT_CACHE_TTL_SECS: u64 = 5;
const NOISE_FACTOR: f64 = 0.125;
const ERROR_PENALTY_MULT: f64 = 8.0;
const DEFAULT_LATENCY: f64 = 10.0;

// ── Configuration ───────────────────────────────────────────────

/// Per-upstream configuration.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct UpstreamConfig {
    /// Upstream address (required).
    pub addr: String,
    /// Optional tag for routing.
    #[serde(default)]
    pub tag: Option<String>,
    /// Direct IP:port to connect to, bypassing DNS resolution (for DoH/DoT).
    #[serde(default)]
    pub dial_addr: Option<String>,
    /// Bootstrap DNS server for resolving the upstream hostname (for DoH/DoT).
    #[serde(default)]
    pub bootstrap: Option<String>,
}

/// Forward plugin configuration.
///
/// Can be deserialized from YAML (full config) or parsed from a string
/// (quick-setup: space-separated addresses).
#[derive(Debug, Clone, serde::Deserialize)]
pub struct ForwardConfig {
    #[serde(default)]
    pub upstreams: Vec<UpstreamConfig>,
    #[serde(default = "default_concurrent")]
    pub concurrent: usize,
}

fn default_concurrent() -> usize {
    1
}

fn default_dial_port(upstream_addr: &str) -> u16 {
    if upstream_addr.starts_with("tls://") || upstream_addr.starts_with("quic://") {
        853
    } else if upstream_addr.starts_with("https://") || upstream_addr.starts_with("h3://") {
        443
    } else {
        53
    }
}

fn parse_dial_addr(dial_addr: &str, upstream_addr: &str) -> PluginResult<SocketAddr> {
    if let Ok(addr) = dial_addr.parse::<SocketAddr>() {
        return Ok(addr);
    }

    if let Ok(ip) = dial_addr.parse::<IpAddr>() {
        return Ok(SocketAddr::new(ip, default_dial_port(upstream_addr)));
    }

    Err(format!(
        "forward: invalid dial_addr '{}': expected IP or IP:port",
        dial_addr
    )
    .into())
}

impl Default for ForwardConfig {
    fn default() -> Self {
        Self {
            upstreams: vec![],
            concurrent: 1,
        }
    }
}

impl ForwardConfig {
    /// Deserialize from a YAML string.
    pub fn from_yaml_str(s: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let cfg: ForwardConfig =
            serde_saphyr::from_str(s).map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                format!("forward: invalid YAML args: {e}").into()
            })?;
        Ok(cfg)
    }

    /// Parse from space-separated addresses (quick-setup / inline).
    pub fn from_str_args(s: &str) -> Self {
        let upstreams = s
            .split_whitespace()
            .map(|addr| UpstreamConfig {
                addr: addr.to_string(),
                tag: None,
                dial_addr: None,
                bootstrap: None,
            })
            .collect();
        ForwardConfig {
            upstreams,
            concurrent: MAX_CONCURRENT_QUERIES,
        }
    }
}

// ── Upstream Selector ───────────────────────────────────────────

struct UpstreamSelector {
    upstreams: Vec<Arc<UpstreamWrapper>>,
    cached_order: RwLock<Option<(Vec<usize>, Instant)>>,
    rr_counter: AtomicUsize,
}

impl UpstreamSelector {
    fn new(upstreams: Vec<Arc<UpstreamWrapper>>) -> Self {
        Self {
            upstreams,
            cached_order: RwLock::new(None),
            rr_counter: AtomicUsize::new(0),
        }
    }

    fn select(&self, count: usize) -> Vec<usize> {
        let n = self.upstreams.len();
        let count = count.min(n);
        if n <= count {
            return (0..n).collect();
        }

        // Check cache.
        {
            let cache = self.cached_order.read().unwrap();
            if let Some((ref order, ref ts)) = *cache {
                if ts.elapsed().as_secs() < WEIGHT_CACHE_TTL_SECS && order.len() >= count {
                    return order[..count].to_vec();
                }
            }
        }

        let scores = self.calculate_scores();
        let selected = self.weighted_sample(&scores, count);

        {
            let mut cache = self.cached_order.write().unwrap();
            *cache = Some((selected.clone(), Instant::now()));
        }
        selected
    }

    /// Select from a subset of upstream indices.
    fn select_from(&self, indices: &[usize], count: usize) -> Vec<usize> {
        let count = count.min(indices.len());
        if indices.len() <= count {
            return indices.to_vec();
        }

        let scores: Vec<(usize, f64)> = indices
            .iter()
            .map(|&i| {
                let uw = &self.upstreams[i];
                let latency = uw.ema_latency() as f64;
                let latency = if latency <= 0.0 {
                    DEFAULT_LATENCY
                } else {
                    latency
                };
                let error_rate = uw.error_rate();
                let penalty_factor = 1.0 + error_rate * ERROR_PENALTY_MULT;
                let counter = self.rr_counter.fetch_add(1, Ordering::Relaxed);
                let noise_seed = ((counter + i) % 256) as f64 / 256.0;
                let noise = (noise_seed * 2.0 - 1.0) * NOISE_FACTOR;
                let score = (1.0 / (latency * penalty_factor)) * (1.0 + noise);
                (i, score.max(0.001))
            })
            .collect();

        self.weighted_sample(&scores, count)
    }

    fn calculate_scores(&self) -> Vec<(usize, f64)> {
        self.upstreams
            .iter()
            .enumerate()
            .map(|(i, uw)| {
                let latency = uw.ema_latency() as f64;
                let latency = if latency <= 0.0 {
                    DEFAULT_LATENCY
                } else {
                    latency
                };
                let error_rate = uw.error_rate();
                let penalty_factor = 1.0 + error_rate * ERROR_PENALTY_MULT;
                let counter = self.rr_counter.fetch_add(1, Ordering::Relaxed);
                let noise_seed = ((counter + i) % 256) as f64 / 256.0;
                let noise = (noise_seed * 2.0 - 1.0) * NOISE_FACTOR;
                let score = (1.0 / (latency * penalty_factor)) * (1.0 + noise);
                (i, score.max(0.001))
            })
            .collect()
    }

    fn weighted_sample(&self, scores: &[(usize, f64)], count: usize) -> Vec<usize> {
        let mut remaining: Vec<(usize, f64)> = scores.to_vec();
        let mut selected = Vec::with_capacity(count);

        for _ in 0..count {
            if remaining.is_empty() {
                break;
            }
            let total: f64 = remaining.iter().map(|(_, s)| s).sum();
            let counter = self.rr_counter.fetch_add(1, Ordering::Relaxed);
            let point = (counter % 1000) as f64 / 1000.0 * total;
            let mut cumulative = 0.0;
            let mut pick = 0;
            for (j, (_, score)) in remaining.iter().enumerate() {
                cumulative += score;
                if point <= cumulative {
                    pick = j;
                    break;
                }
            }
            let (idx, _) = remaining.remove(pick);
            selected.push(idx);
        }
        selected
    }
}

// ── Forward Plugin ──────────────────────────────────────────────

/// Forward executable — queries upstream DNS servers with latency-aware
/// selection, tag-based routing, and rcode-aware retry.
pub struct Forward {
    upstreams: Vec<Arc<UpstreamWrapper>>,
    selector: UpstreamSelector,
    concurrent: usize,
    /// Tag → upstream indices mapping.
    tag_index: HashMap<String, Vec<usize>>,
}

impl Forward {
    pub fn new(cfg: ForwardConfig) -> PluginResult<Self> {
        if cfg.upstreams.is_empty() {
            return Err("forward: no upstreams configured".into());
        }

        let mut upstreams = Vec::new();
        let mut tag_index: HashMap<String, Vec<usize>> = HashMap::new();

        for (i, ucfg) in cfg.upstreams.iter().enumerate() {
            let name = ucfg.tag.clone().unwrap_or_else(|| ucfg.addr.clone());
            let mut opts = UpstreamOpts::default();
            if let Some(ref da) = ucfg.dial_addr {
                opts.dial_addr = Some(parse_dial_addr(da, &ucfg.addr)?);
            }
            opts.bootstrap = ucfg.bootstrap.clone();
            let uw = Arc::new(UpstreamWrapper::new(
                upstream::new_upstream(&ucfg.addr, opts)?,
                name,
            ));
            upstreams.push(uw);

            if let Some(ref tag) = ucfg.tag {
                tag_index.entry(tag.clone()).or_default().push(i);
            }
        }

        let concurrent = if cfg.concurrent == 0 {
            1
        } else {
            cfg.concurrent.min(MAX_CONCURRENT_QUERIES)
        };
        let selector = UpstreamSelector::new(upstreams.clone());

        Ok(Self {
            upstreams,
            selector,
            concurrent,
            tag_index,
        })
    }

    /// Select upstreams by tag names. If tags are empty, use all upstreams.
    pub fn select_by_tags(&self, tags: &[String]) -> Vec<usize> {
        if tags.is_empty() {
            return self.selector.select(self.concurrent);
        }

        let mut indices = Vec::new();
        for tag in tags {
            if let Some(idxs) = self.tag_index.get(tag) {
                indices.extend(idxs);
            }
        }
        indices.sort_unstable();
        indices.dedup();

        if indices.is_empty() {
            return self.selector.select(self.concurrent);
        }

        self.selector.select_from(&indices, self.concurrent)
    }

    /// Returns a reference to all upstreams for metrics collection.
    pub fn upstreams(&self) -> &[Arc<UpstreamWrapper>] {
        &self.upstreams
    }
}

#[async_trait]
impl Executable for Forward {
    async fn exec(&self, ctx: &mut Context) -> PluginResult<()> {
        if ctx.response().is_some() {
            return Ok(());
        }

        let query_bytes =
            ctx.query()
                .to_vec()
                .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                    format!("failed to serialize query: {e}").into()
                })?;

        let selected_indices = self.selector.select(self.concurrent);
        let selected: Vec<Arc<UpstreamWrapper>> = selected_indices
            .iter()
            .map(|&i| self.upstreams[i].clone())
            .collect();

        debug!(
            upstreams = ?selected.iter().map(|u| u.name()).collect::<Vec<_>>(),
            count = selected.len(),
            "forward: selected upstreams"
        );

        if selected.len() == 1 {
            let start = Instant::now();
            let resp_bytes = selected[0].exchange(&query_bytes).await?;
            debug!(upstream = %selected[0].name(), elapsed = ?start.elapsed(), "forward: upstream responded");
            let resp = Message::from_vec(&resp_bytes).map_err(
                |e| -> Box<dyn std::error::Error + Send + Sync> {
                    format!("invalid upstream response: {e}").into()
                },
            )?;
            selected[0].record_adopted();
            ctx.set_response(Some(resp));
        } else {
            let total = selected.len();
            let (tx, mut rx) = tokio::sync::mpsc::channel::<(usize, PluginResult<Vec<u8>>)>(total);

            for (sel_idx, u) in selected.iter().enumerate() {
                let tx = tx.clone();
                let qb = query_bytes.clone();
                let u = u.clone();
                tokio::spawn(async move {
                    let result = u.exchange(&qb).await;
                    let _ = tx.send((sel_idx, result)).await;
                });
            }
            drop(tx);

            let mut last_err: Option<Box<dyn std::error::Error + Send + Sync>> = None;
            let mut responses_received = 0;

            while let Some((_sel_idx, result)) = rx.recv().await {
                responses_received += 1;
                let is_last = responses_received >= total;

                match result {
                    Ok(resp_bytes) => match Message::from_vec(&resp_bytes) {
                        Ok(resp) => {
                            let rcode = resp.response_code();
                            if is_last
                                || rcode == ResponseCode::NoError
                                || rcode == ResponseCode::NXDomain
                            {
                                selected[_sel_idx].record_adopted();
                                ctx.set_response(Some(resp));
                                return Ok(());
                            }
                            debug!(rcode = ?rcode, "skipping upstream response with non-ideal rcode");
                            last_err = Some(format!("upstream returned rcode {rcode:?}").into());
                        }
                        Err(e) => {
                            warn!(error = %e, "invalid upstream response");
                            last_err = Some(format!("invalid response: {e}").into());
                        }
                    },
                    Err(e) => {
                        debug!(error = %e, "upstream exchange failed");
                        last_err = Some(e);
                    }
                }
            }

            if let Some(e) = last_err {
                return Err(e);
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn forward_config_empty_fails() {
        let cfg = ForwardConfig::default();
        assert!(Forward::new(cfg).is_err());
    }

    #[test]
    fn forward_config_valid() {
        let cfg = ForwardConfig {
            upstreams: vec![UpstreamConfig {
                addr: "udp://8.8.8.8:53".into(),
                tag: None,
                dial_addr: None,
                bootstrap: None,
            }],
            concurrent: 1,
        };
        assert!(Forward::new(cfg).is_ok());
    }

    #[test]
    fn forward_tag_index() {
        let cfg = ForwardConfig {
            upstreams: vec![
                UpstreamConfig {
                    addr: "udp://8.8.8.8:53".into(),
                    tag: Some("google".into()),
                    dial_addr: None,
                    bootstrap: None,
                },
                UpstreamConfig {
                    addr: "udp://1.1.1.1:53".into(),
                    tag: Some("cloudflare".into()),
                    dial_addr: None,
                    bootstrap: None,
                },
                UpstreamConfig {
                    addr: "udp://9.9.9.9:53".into(),
                    tag: Some("quad9".into()),
                    dial_addr: None,
                    bootstrap: None,
                },
            ],
            concurrent: 1,
        };
        let f = Forward::new(cfg).unwrap();
        assert_eq!(f.tag_index.get("google"), Some(&vec![0]));
        assert_eq!(f.tag_index.get("cloudflare"), Some(&vec![1]));
        assert_eq!(f.tag_index.get("quad9"), Some(&vec![2]));
    }

    #[test]
    fn select_by_tags_returns_tagged() {
        let cfg = ForwardConfig {
            upstreams: vec![
                UpstreamConfig {
                    addr: "udp://8.8.8.8:53".into(),
                    tag: Some("google".into()),
                    dial_addr: None,
                    bootstrap: None,
                },
                UpstreamConfig {
                    addr: "udp://1.1.1.1:53".into(),
                    tag: Some("cloudflare".into()),
                    dial_addr: None,
                    bootstrap: None,
                },
            ],
            concurrent: 1,
        };
        let f = Forward::new(cfg).unwrap();
        let selected = f.select_by_tags(&["google".into()]);
        assert_eq!(selected, vec![0]);
    }

    #[test]
    fn selector_returns_all_when_count_exceeds_upstreams() {
        use redns_core::upstream::UpstreamWrapper;

        struct MockUpstream;
        #[async_trait]
        impl redns_core::upstream::Upstream for MockUpstream {
            async fn exchange(&self, _q: &[u8]) -> PluginResult<Vec<u8>> {
                Ok(vec![])
            }
        }

        let u1 = Arc::new(UpstreamWrapper::new(Box::new(MockUpstream), "u1".into()));
        let u2 = Arc::new(UpstreamWrapper::new(Box::new(MockUpstream), "u2".into()));
        let selector = UpstreamSelector::new(vec![u1, u2]);
        let selected = selector.select(5);
        assert_eq!(selected.len(), 2);
        assert!(selected.contains(&0));
        assert!(selected.contains(&1));
    }

    #[test]
    fn parse_dial_addr_ip_without_port_uses_upstream_default_port() {
        let addr = parse_dial_addr("223.5.5.5", "h3://9999.alidns.com/dns-query").unwrap();
        assert_eq!(addr, "223.5.5.5:443".parse().unwrap());
    }

    #[test]
    fn invalid_dial_addr_fails_forward_new() {
        let cfg = ForwardConfig {
            upstreams: vec![UpstreamConfig {
                addr: "https://dns.google/dns-query".into(),
                tag: None,
                dial_addr: Some("not-an-ip".into()),
                bootstrap: None,
            }],
            concurrent: 1,
        };
        assert!(Forward::new(cfg).is_err());
    }
}
