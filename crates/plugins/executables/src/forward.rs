// Copyright (C) 2026, Harlan Wei
//
// This file is part of redns.

//! Forward plugin — sends queries to upstream DNS servers.

use async_trait::async_trait;
use hickory_proto::op::{Message, ResponseCode};
use hickory_proto::rr::{Name, RData, Record, RecordType};
use redns_core::context::KV_SELECTED_UPSTREAM;
use redns_core::plugin::PluginResult;
use redns_core::upstream::{self, UpstreamOpts, UpstreamWrapper};
use redns_core::{Context, Executable};
use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use tracing::{debug, warn};

// ── Constants ───────────────────────────────────────────────────

const MAX_CONCURRENT_QUERIES: usize = 3;
const WEIGHT_CACHE_TTL_SECS: u64 = 5;
const NOISE_FACTOR: f64 = 0.125;
const ERROR_PENALTY_MULT: f64 = 8.0;
const DEFAULT_LATENCY: f64 = 10.0;
const MAX_CNAME_FOLLOW: usize = 16;
const HEDGE_DELAY_MULT: f64 = 1.5;
const HEDGE_DELAY_MIN_MS: u64 = 15;
const HEDGE_DELAY_MAX_MS: u64 = 120;

fn hedge_delay_for(uw: &UpstreamWrapper) -> Duration {
    let base_ms = uw.ema_latency() as f64;
    let base_ms = if base_ms > 0.0 {
        base_ms
    } else {
        DEFAULT_LATENCY
    };
    let delay_ms = (base_ms * HEDGE_DELAY_MULT).round() as u64;
    Duration::from_millis(delay_ms.clamp(HEDGE_DELAY_MIN_MS, HEDGE_DELAY_MAX_MS))
}

fn dns_header_rcode(resp_wire: &[u8]) -> Option<u16> {
    if resp_wire.len() < 4 {
        return None;
    }
    Some((resp_wire[3] & 0x0f) as u16)
}

fn find_cname_for_owner(resp: &Message, owner: &Name) -> Option<(Record, Name)> {
    for rr in resp.answers() {
        if rr.record_type() != RecordType::CNAME || rr.name() != owner {
            continue;
        }
        if let RData::CNAME(cname) = rr.data() {
            return Some((rr.clone(), cname.0.clone()));
        }
    }
    None
}

fn has_answer_for_name_type(resp: &Message, name: &Name, qtype: RecordType) -> bool {
    if qtype == RecordType::CNAME {
        return true;
    }
    if qtype == RecordType::ANY {
        return !resp.answers().is_empty();
    }
    resp.answers()
        .iter()
        .any(|rr| rr.name() == name && rr.record_type() == qtype)
}

fn merge_cname_chain(mut resp: Message, original_name: &Name, cname_prefix: &[Record]) -> Message {
    if let Some(q) = resp.queries_mut().first_mut() {
        q.set_name(original_name.clone());
    }
    if !cname_prefix.is_empty() {
        let mut merged_answers = Vec::with_capacity(cname_prefix.len() + resp.answers().len());
        merged_answers.extend(cname_prefix.iter().cloned());
        merged_answers.extend(resp.answers().iter().cloned());
        *resp.answers_mut() = merged_answers;
    }
    resp
}

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
    name: String,
    upstreams: Vec<Arc<UpstreamWrapper>>,
    selector: UpstreamSelector,
    concurrent: usize,
    /// Tag → upstream indices mapping.
    tag_index: HashMap<String, Vec<usize>>,
}

impl Forward {
    pub fn new(cfg: ForwardConfig, name: &str) -> PluginResult<Self> {
        if cfg.upstreams.is_empty() {
            return Err("forward: no upstreams configured".into());
        }

        let mut upstreams = Vec::new();
        let mut tag_index: HashMap<String, Vec<usize>> = HashMap::new();

        for (i, ucfg) in cfg.upstreams.iter().enumerate() {
            let upstream_name = ucfg.tag.clone().unwrap_or_else(|| ucfg.addr.clone());
            let mut opts = UpstreamOpts::default();
            if let Some(ref da) = ucfg.dial_addr {
                opts.dial_addr = Some(parse_dial_addr(da, &ucfg.addr)?);
            }
            opts.bootstrap = ucfg.bootstrap.clone();
            let uw = Arc::new(UpstreamWrapper::new(
                upstream::new_upstream(&ucfg.addr, opts)?,
                upstream_name,
                upstream::upstream_protocol_label(&ucfg.addr).to_string(),
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
            name: name.to_string(),
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

    async fn resolve_once(
        &self,
        query_bytes: Arc<[u8]>,
    ) -> PluginResult<(Message, Arc<UpstreamWrapper>)> {
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
            return Ok((resp, selected[0].clone()));
        }

        let total = selected.len();
        let hedge_delay = hedge_delay_for(selected[0].as_ref());
        let mut tasks = tokio::task::JoinSet::new();
        let mut next_to_launch = 0usize;

        let launch_next = |tasks: &mut tokio::task::JoinSet<_>,
                           next_to_launch: &mut usize,
                           query_bytes: &Arc<[u8]>,
                           selected: &[Arc<UpstreamWrapper>]| {
            if *next_to_launch >= selected.len() {
                return false;
            }
            let sel_idx = *next_to_launch;
            *next_to_launch += 1;
            let qb = query_bytes.clone();
            let u = selected[sel_idx].clone();
            tasks.spawn(async move { (sel_idx, u.exchange(&qb).await) });
            true
        };

        launch_next(&mut tasks, &mut next_to_launch, &query_bytes, &selected);

        let mut last_err: Option<Box<dyn std::error::Error + Send + Sync>> = None;
        let mut responses_received = 0;

        while responses_received < total {
            let join_result = if next_to_launch < total {
                match tokio::time::timeout(hedge_delay, tasks.join_next()).await {
                    Ok(Some(v)) => Some(v),
                    Ok(None) => {
                        if launch_next(&mut tasks, &mut next_to_launch, &query_bytes, &selected) {
                            continue;
                        }
                        None
                    }
                    Err(_) => {
                        if launch_next(&mut tasks, &mut next_to_launch, &query_bytes, &selected) {
                            continue;
                        }
                        tasks.join_next().await
                    }
                }
            } else {
                tasks.join_next().await
            };

            let Some(join_result) = join_result else {
                break;
            };

            responses_received += 1;
            let is_last = responses_received >= total;

            let (sel_idx, result) = match join_result {
                Ok(v) => v,
                Err(e) => {
                    debug!(plugin = %self.name, error = %e, "upstream task join failed");
                    last_err = Some(format!("upstream task join failed: {e}").into());
                    continue;
                }
            };

            let upstream_name = selected[sel_idx].name();

            match result {
                Ok(resp_bytes) => {
                    let rcode = match dns_header_rcode(&resp_bytes) {
                        Some(rcode) => rcode,
                        None => {
                            warn!(plugin = %self.name, upstream = %upstream_name, "invalid upstream response (too short)");
                            last_err = Some("invalid response: short dns header".into());
                            continue;
                        }
                    };

                    let noerror = u16::from(ResponseCode::NoError);
                    let nxdomain = u16::from(ResponseCode::NXDomain);
                    let adopt = is_last || rcode == noerror || rcode == nxdomain;

                    if !adopt {
                        selected[sel_idx].record_rejected_rcode();
                        debug!(plugin = %self.name, upstream = %upstream_name, rcode, "skipping upstream response with non-ideal rcode");
                        last_err = Some(format!("upstream returned rcode {}", rcode).into());
                        continue;
                    }

                    match Message::from_vec(&resp_bytes) {
                        Ok(resp) => {
                            selected[sel_idx].record_adopted();
                            tasks.abort_all();
                            return Ok((resp, selected[sel_idx].clone()));
                        }
                        Err(e) => {
                            warn!(plugin = %self.name, upstream = %upstream_name, error = %e, "invalid upstream response");
                            last_err = Some(format!("invalid response: {e}").into());
                        }
                    }
                }
                Err(e) => {
                    debug!(plugin = %self.name, upstream = %upstream_name, error = %e, "upstream exchange failed");
                    last_err = Some(e);
                }
            }
        }

        if let Some(e) = last_err {
            return Err(e);
        }
        Err("forward: no upstream response".into())
    }

    async fn resolve_with_cname_chase(
        &self,
        query: &Message,
    ) -> PluginResult<(Message, Arc<UpstreamWrapper>)> {
        let question = query.queries().first().ok_or_else(
            || -> Box<dyn std::error::Error + Send + Sync> {
                "forward: query has no question".into()
            },
        )?;
        let original_name = question.name().clone();
        let qtype = question.query_type();

        if qtype == RecordType::CNAME {
            let query_bytes: Arc<[u8]> = query
                .to_vec()
                .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                    format!("failed to serialize query: {e}").into()
                })?
                .into();
            return self.resolve_once(query_bytes).await;
        }

        let mut chase_query = query.clone();
        let mut visited_names: HashSet<Name> = HashSet::new();
        visited_names.insert(original_name.clone());
        let mut carried_cnames: Vec<Record> = Vec::new();

        for depth in 0..=MAX_CNAME_FOLLOW {
            let current_name = chase_query
                .queries()
                .first()
                .map(|q| q.name().clone())
                .ok_or_else(|| -> Box<dyn std::error::Error + Send + Sync> {
                    "forward: query has no question".into()
                })?;

            let query_bytes: Arc<[u8]> = chase_query
                .to_vec()
                .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                    format!("failed to serialize query: {e}").into()
                })?
                .into();

            let (resp, upstream) = self.resolve_once(query_bytes).await?;

            let mut in_resp_seen: HashSet<Name> = HashSet::new();
            let mut step_cnames: Vec<Record> = Vec::new();
            let mut final_target = current_name.clone();
            in_resp_seen.insert(final_target.clone());

            while let Some((cname_rr, cname_target)) = find_cname_for_owner(&resp, &final_target) {
                if !in_resp_seen.insert(cname_target.clone()) {
                    return Err(format!(
                        "forward: cname loop detected in upstream response at '{}'",
                        cname_target.to_ascii()
                    )
                    .into());
                }
                step_cnames.push(cname_rr);
                final_target = cname_target;
            }

            if resp.response_code() != ResponseCode::NoError || step_cnames.is_empty() {
                if carried_cnames.is_empty() {
                    return Ok((resp, upstream));
                }
                return Ok((
                    merge_cname_chain(resp, &original_name, &carried_cnames),
                    upstream,
                ));
            }

            let carried_before = carried_cnames.len();
            carried_cnames.extend(step_cnames);

            if has_answer_for_name_type(&resp, &final_target, qtype) {
                if carried_before == 0 {
                    return Ok((resp, upstream));
                }
                return Ok((
                    merge_cname_chain(resp, &original_name, &carried_cnames[..carried_before]),
                    upstream,
                ));
            }

            if depth >= MAX_CNAME_FOLLOW {
                return Err(
                    format!("forward: cname chase depth exceeded {}", MAX_CNAME_FOLLOW).into(),
                );
            }

            if !visited_names.insert(final_target.clone()) {
                return Err(format!(
                    "forward: cname loop detected while chasing '{}'",
                    final_target.to_ascii()
                )
                .into());
            }

            let q = chase_query.queries_mut().first_mut().ok_or_else(
                || -> Box<dyn std::error::Error + Send + Sync> {
                    "forward: query has no question".into()
                },
            )?;
            q.set_name(final_target);
        }

        Err(format!("forward: cname chase depth exceeded {}", MAX_CNAME_FOLLOW).into())
    }
}

#[async_trait]
impl Executable for Forward {
    async fn exec(&self, ctx: &mut Context) -> PluginResult<()> {
        if ctx.response().is_some() {
            return Ok(());
        }

        let (resp, selected_upstream) = self.resolve_with_cname_chase(ctx.query()).await?;
        ctx.store_value(KV_SELECTED_UPSTREAM, selected_upstream);
        ctx.set_response(Some(resp));

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hickory_proto::op::{MessageType, OpCode, Query};
    use hickory_proto::rr::{Name, RData, Record, RecordType};
    use redns_core::upstream::{Upstream, UpstreamWrapper};
    use std::net::Ipv4Addr;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct FnUpstream {
        calls: Arc<AtomicUsize>,
        handler: Arc<dyn Fn(&Message) -> PluginResult<Message> + Send + Sync>,
    }

    #[async_trait]
    impl Upstream for FnUpstream {
        async fn exchange(&self, q: &[u8]) -> PluginResult<Vec<u8>> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            let req =
                Message::from_vec(q).map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                    format!("failed to decode query in test upstream: {e}").into()
                })?;
            let resp = (self.handler)(&req)?;
            resp.to_vec()
                .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                    format!("failed to encode response in test upstream: {e}").into()
                })
        }
    }

    fn make_forward_with_upstream(upstream: Box<dyn Upstream>) -> Forward {
        let wrapped = Arc::new(UpstreamWrapper::new(upstream, "mock".into(), "UDP".into()));
        let upstreams = vec![wrapped];
        Forward {
            name: "test-forward".into(),
            selector: UpstreamSelector::new(upstreams.clone()),
            upstreams,
            concurrent: 1,
            tag_index: HashMap::new(),
        }
    }

    fn make_query(name: &str, qtype: RecordType) -> Message {
        let mut msg = Message::new();
        msg.set_id(7)
            .set_message_type(MessageType::Query)
            .set_op_code(OpCode::Query);
        msg.add_query({
            let mut q = Query::new();
            q.set_name(Name::from_ascii(name).unwrap())
                .set_query_type(qtype);
            q
        });
        msg
    }

    fn response_with_answers(req: &Message, rcode: ResponseCode, answers: Vec<Record>) -> Message {
        let mut resp = Message::new();
        resp.set_id(req.id())
            .set_message_type(MessageType::Response)
            .set_response_code(rcode);
        for q in req.queries() {
            resp.add_query(q.clone());
        }
        for answer in answers {
            resp.add_answer(answer);
        }
        resp
    }

    #[test]
    fn forward_config_empty_fails() {
        let cfg = ForwardConfig::default();
        assert!(Forward::new(cfg, "test").is_err());
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
        assert!(Forward::new(cfg, "test").is_ok());
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
        let f = Forward::new(cfg, "test").unwrap();
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
        let f = Forward::new(cfg, "test").unwrap();
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

        let u1 = Arc::new(UpstreamWrapper::new(
            Box::new(MockUpstream),
            "u1".into(),
            "UDP".into(),
        ));
        let u2 = Arc::new(UpstreamWrapper::new(
            Box::new(MockUpstream),
            "u2".into(),
            "TCP".into(),
        ));
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
        assert!(Forward::new(cfg, "test").is_err());
    }

    #[tokio::test]
    async fn cname_only_response_is_chased() {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_for_upstream = calls.clone();
        let upstream = FnUpstream {
            calls: calls_for_upstream,
            handler: Arc::new(|req: &Message| {
                let q = req.queries().first().ok_or_else(
                    || -> Box<dyn std::error::Error + Send + Sync> { "missing question".into() },
                )?;
                let qname = q.name().to_ascii();
                if qname == "alias.com." {
                    return Ok(response_with_answers(
                        req,
                        ResponseCode::NoError,
                        vec![Record::from_rdata(
                            Name::from_ascii("alias.com.").unwrap(),
                            60,
                            RData::CNAME(hickory_proto::rr::rdata::CNAME(
                                Name::from_ascii("real.com.").unwrap(),
                            )),
                        )],
                    ));
                }
                if qname == "real.com." {
                    return Ok(response_with_answers(
                        req,
                        ResponseCode::NoError,
                        vec![Record::from_rdata(
                            Name::from_ascii("real.com.").unwrap(),
                            60,
                            RData::A(Ipv4Addr::new(1, 2, 3, 4).into()),
                        )],
                    ));
                }
                Err(format!("unexpected qname in test: {qname}").into())
            }),
        };

        let forward = make_forward_with_upstream(Box::new(upstream));
        let mut ctx = Context::new(make_query("alias.com.", RecordType::A));
        forward.exec(&mut ctx).await.unwrap();

        assert_eq!(calls.load(Ordering::Relaxed), 2);
        let resp = ctx.response().unwrap();
        assert_eq!(resp.response_code(), ResponseCode::NoError);
        assert_eq!(resp.queries()[0].name().to_ascii(), "alias.com.");
        assert_eq!(resp.answers().len(), 2);
        assert_eq!(resp.answers()[0].record_type(), RecordType::CNAME);
        assert_eq!(resp.answers()[1].record_type(), RecordType::A);
        assert_eq!(resp.answers()[1].name().to_ascii(), "real.com.");
    }

    #[tokio::test]
    async fn cname_query_type_does_not_chase() {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_for_upstream = calls.clone();
        let upstream = FnUpstream {
            calls: calls_for_upstream,
            handler: Arc::new(|req: &Message| {
                Ok(response_with_answers(
                    req,
                    ResponseCode::NoError,
                    vec![Record::from_rdata(
                        Name::from_ascii("alias.com.").unwrap(),
                        60,
                        RData::CNAME(hickory_proto::rr::rdata::CNAME(
                            Name::from_ascii("real.com.").unwrap(),
                        )),
                    )],
                ))
            }),
        };

        let forward = make_forward_with_upstream(Box::new(upstream));
        let mut ctx = Context::new(make_query("alias.com.", RecordType::CNAME));
        forward.exec(&mut ctx).await.unwrap();

        assert_eq!(calls.load(Ordering::Relaxed), 1);
        let resp = ctx.response().unwrap();
        assert_eq!(resp.answers().len(), 1);
        assert_eq!(resp.answers()[0].record_type(), RecordType::CNAME);
    }

    #[tokio::test]
    async fn response_with_cname_and_terminal_answer_does_not_extra_chase() {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_for_upstream = calls.clone();
        let upstream = FnUpstream {
            calls: calls_for_upstream,
            handler: Arc::new(|req: &Message| {
                Ok(response_with_answers(
                    req,
                    ResponseCode::NoError,
                    vec![
                        Record::from_rdata(
                            Name::from_ascii("alias.com.").unwrap(),
                            60,
                            RData::CNAME(hickory_proto::rr::rdata::CNAME(
                                Name::from_ascii("real.com.").unwrap(),
                            )),
                        ),
                        Record::from_rdata(
                            Name::from_ascii("real.com.").unwrap(),
                            60,
                            RData::A(Ipv4Addr::new(1, 2, 3, 4).into()),
                        ),
                    ],
                ))
            }),
        };

        let forward = make_forward_with_upstream(Box::new(upstream));
        let mut ctx = Context::new(make_query("alias.com.", RecordType::A));
        forward.exec(&mut ctx).await.unwrap();

        assert_eq!(calls.load(Ordering::Relaxed), 1);
        let resp = ctx.response().unwrap();
        assert_eq!(resp.answers().len(), 2);
        assert_eq!(resp.answers()[0].record_type(), RecordType::CNAME);
        assert_eq!(resp.answers()[1].record_type(), RecordType::A);
    }

    #[tokio::test]
    async fn cname_loop_returns_error() {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_for_upstream = calls.clone();
        let upstream = FnUpstream {
            calls: calls_for_upstream,
            handler: Arc::new(|req: &Message| {
                let qname = req.queries()[0].name().to_ascii();
                if qname == "alias.com." {
                    return Ok(response_with_answers(
                        req,
                        ResponseCode::NoError,
                        vec![Record::from_rdata(
                            Name::from_ascii("alias.com.").unwrap(),
                            60,
                            RData::CNAME(hickory_proto::rr::rdata::CNAME(
                                Name::from_ascii("loop.com.").unwrap(),
                            )),
                        )],
                    ));
                }
                if qname == "loop.com." {
                    return Ok(response_with_answers(
                        req,
                        ResponseCode::NoError,
                        vec![Record::from_rdata(
                            Name::from_ascii("loop.com.").unwrap(),
                            60,
                            RData::CNAME(hickory_proto::rr::rdata::CNAME(
                                Name::from_ascii("alias.com.").unwrap(),
                            )),
                        )],
                    ));
                }
                Err(format!("unexpected qname in test: {qname}").into())
            }),
        };

        let forward = make_forward_with_upstream(Box::new(upstream));
        let mut ctx = Context::new(make_query("alias.com.", RecordType::A));
        let err = forward
            .exec(&mut ctx)
            .await
            .expect_err("expected cname loop error");
        assert!(err.to_string().contains("cname loop"));
    }
}
