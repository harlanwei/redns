// Copyright (C) 2026, Harlan Wei
//
// This file is part of redns.

//! Forward plugin — sends queries to upstream DNS servers.

use async_trait::async_trait;
use hickory_proto::op::{Message, ResponseCode};
use parking_lot::RwLock;
use redns_core::context::KV_SELECTED_UPSTREAM;
use redns_core::plugin::PluginResult;
use redns_core::upstream::{self, UpstreamOpts, UpstreamWrapper};
use redns_core::{Context, Executable};
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::process::{Child, Command};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

// ── Constants ───────────────────────────────────────────────────

const MAX_CONCURRENT_QUERIES: usize = 3;
const WEIGHT_CACHE_TTL_SECS: u64 = 5;
const NOISE_FACTOR: f64 = 0.125;
const ERROR_PENALTY_MULT: f64 = 20.0;
const ERROR_PENALTY_EXP: i32 = 2;
const LATENCY_EXP: f64 = 2.0;
const STREAK_PENALTY_MULT: f64 = 1.5;
const DEFAULT_LATENCY: f64 = 10.0;
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

/// Attempt to repair a DNS response whose trailing zero bytes were trimmed by
/// the upstream (TrimEnd bug). Walks the wire format; if the message ends
/// mid-rdata, returns a zero-padded copy that satisfies the declared RDLENGTH.
fn repair_trailing_trim(wire: &[u8]) -> Option<Vec<u8>> {
    if wire.len() < 12 {
        return None;
    }
    let qdcount = u16::from_be_bytes([wire[4], wire[5]]) as usize;
    let ancount = u16::from_be_bytes([wire[6], wire[7]]) as usize;
    let nscount = u16::from_be_bytes([wire[8], wire[9]]) as usize;
    let arcount = u16::from_be_bytes([wire[10], wire[11]]) as usize;

    let mut pos = 12usize;

    // Skip question section.
    for _ in 0..qdcount {
        pos = skip_name(wire, pos)?;
        pos = pos.checked_add(4)?; // QTYPE + QCLASS
        if pos > wire.len() {
            return None;
        }
    }

    // Walk RR sections (answer + authority + additional).
    let total_rrs = ancount + nscount + arcount;
    for _ in 0..total_rrs {
        pos = skip_name(wire, pos)?;
        let fixed = pos.checked_add(10)?; // TYPE(2) + CLASS(2) + TTL(4) + RDLENGTH(2)
        if fixed > wire.len() {
            return None;
        }
        let rdlength = u16::from_be_bytes([wire[fixed - 2], wire[fixed - 1]]) as usize;
        let rdata_start = fixed;
        let rdata_end = rdata_start.checked_add(rdlength)?;

        if rdata_end > wire.len() {
            // Message ends mid-rdata — pad with zeros.
            let mut repaired = wire.to_vec();
            repaired.resize(rdata_end, 0);
            return Some(repaired);
        }
        pos = rdata_end;
    }

    None
}

/// Skip a DNS name (possibly compressed) starting at `pos`, returning the
/// offset just past the name in the outer message.
fn skip_name(wire: &[u8], mut pos: usize) -> Option<usize> {
    loop {
        if pos >= wire.len() {
            return None;
        }
        let b = wire[pos];
        if b == 0 {
            return Some(pos + 1);
        }
        if b & 0xc0 == 0xc0 {
            // Compression pointer: 2 bytes total, always terminates.
            return Some(pos + 2);
        }
        pos = pos.checked_add(1 + b as usize)?;
    }
}

fn dns_header_rcode(resp_wire: &[u8]) -> Option<u16> {
    if resp_wire.len() < 4 {
        return None;
    }
    Some((resp_wire[3] & 0x0f) as u16)
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
    /// Optional per-upstream idle pool cap.
    #[serde(default)]
    pub pool_max_idle: Option<usize>,
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
    #[serde(default)]
    pub subprocess_suffix: Option<String>,
    /// Optional default idle pool cap for upstream transports.
    #[serde(default)]
    pub pool_max_idle: Option<usize>,
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
            subprocess_suffix: None,
            pool_max_idle: None,
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
                pool_max_idle: None,
            })
            .collect();
        ForwardConfig {
            upstreams,
            concurrent: MAX_CONCURRENT_QUERIES,
            subprocess_suffix: None,
            pool_max_idle: None,
        }
    }
}

// ── Subprocess Upstream ──────────────────────────────────────────

/// Environment variable carrying the Unix socket path to the helper process.
///
/// The forward plugin spawns a second copy of the *current* executable to act
/// as an out-of-process upstream: that copy listens on a Unix socket and
/// resolves queries through the normal config chain. The helper detects this
/// mode via [`REDNS_FORWARD_SOCKET_ENV`]; [`REDNS_FORWARD_SUFFIX_ENV`] gives it
/// its process-name suffix (`redns` → `redns<suffix>`), and
/// [`REDNS_FORWARD_PARENT_PID_ENV`] lets it exit when the main process dies.
/// These constants are shared with the `redns` binary so both sides agree on
/// the contract.
pub const REDNS_FORWARD_SOCKET_ENV: &str = "REDNS_FORWARD_SOCKET";
pub const REDNS_FORWARD_SUFFIX_ENV: &str = "REDNS_FORWARD_SUFFIX";
pub const REDNS_FORWARD_PARENT_PID_ENV: &str = "REDNS_FORWARD_PARENT_PID";

const SUBPROCESS_EXCHANGE_TIMEOUT: Duration = Duration::from_secs(10);
/// How long the parent waits for the helper to bind its socket after spawn.
const SUBPROCESS_STARTUP_TIMEOUT: Duration = Duration::from_secs(5);

struct SubprocessUpstream {
    socket_path: String,
    child: Mutex<Option<Child>>,
}

impl SubprocessUpstream {
    /// Spawns the helper process.
    ///
    /// The helper is the very same executable that is currently running —
    /// there is no separate `redns<suffix>` binary. It is launched with the
    /// parent's own CLI arguments (so it loads the same config) plus the
    /// `REDNS_FORWARD_*` environment contract, and it renames itself to
    /// `redns<suffix>` so the two processes are distinguishable in `ps`.
    ///
    /// Returns once the helper's Unix socket is accepting connections.
    fn new(suffix: &str, socket_path: &str) -> PluginResult<Self> {
        let current_exe =
            std::env::current_exe().map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                format!("subprocess: failed to get current exe path: {e}").into()
            })?;

        let mut cmd = Command::new(&current_exe);
        // Replay our own argv (e.g. `start --config …`) so the helper loads
        // the same configuration; the environment carries the subprocess
        // contract instead of new CLI flags.
        cmd.args(std::env::args().skip(1))
            .env(REDNS_FORWARD_SOCKET_ENV, socket_path)
            .env(REDNS_FORWARD_SUFFIX_ENV, suffix)
            .env(REDNS_FORWARD_PARENT_PID_ENV, std::process::id().to_string());

        let mut child = cmd
            .spawn()
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                format!("subprocess: failed to spawn '{}': {e}", current_exe.display()).into()
            })?;

        info!(
            binary = %current_exe.display(),
            socket = %socket_path,
            pid = ?child.id(),
            "subprocess spawned"
        );

        // Wait until the helper has bound its socket (or died trying), so the
        // first exchange does not race the helper's startup.
        let deadline = Instant::now() + SUBPROCESS_STARTUP_TIMEOUT;
        loop {
            if std::os::unix::net::UnixStream::connect(socket_path).is_ok() {
                break;
            }
            if let Ok(Some(status)) = child.try_wait() {
                return Err(format!(
                    "subprocess exited early (status {status}) before binding '{socket_path}'"
                )
                .into());
            }
            if Instant::now() >= deadline {
                // The helper never came up; don't leave it running orphaned.
                let _ = child.kill();
                let _ = child.wait();
                return Err(format!(
                    "subprocess socket '{socket_path}' not ready within {}s",
                    SUBPROCESS_STARTUP_TIMEOUT.as_secs()
                )
                .into());
            }
            std::thread::sleep(Duration::from_millis(50));
        }

        Ok(Self {
            socket_path: socket_path.to_string(),
            child: Mutex::new(Some(child)),
        })
    }
}

#[async_trait]
impl upstream::Upstream for SubprocessUpstream {
    async fn exchange(&self, query: &[u8]) -> PluginResult<Vec<u8>> {
        let mut stream = UnixStream::connect(&self.socket_path).await.map_err(
            |e| -> Box<dyn std::error::Error + Send + Sync> {
                format!("subprocess: connect failed: {e}").into()
            },
        )?;

        let exchange = async {
            let len = query.len() as u16;
            stream.write_all(&len.to_be_bytes()).await?;
            stream.write_all(query).await?;

            let mut len_buf = [0u8; 2];
            stream.read_exact(&mut len_buf).await?;
            let resp_len = u16::from_be_bytes(len_buf) as usize;
            let mut resp_buf = vec![0u8; resp_len];
            stream.read_exact(&mut resp_buf).await?;
            Ok::<Vec<u8>, std::io::Error>(resp_buf)
        };

        match tokio::time::timeout(SUBPROCESS_EXCHANGE_TIMEOUT, exchange).await {
            Ok(Ok(resp)) => Ok(resp),
            Ok(Err(e)) => Err(format!("subprocess exchange: {e}").into()),
            Err(_) => Err("subprocess exchange timed out".into()),
        }
    }
}

impl Drop for SubprocessUpstream {
    fn drop(&mut self) {
        if let Ok(mut child) = self.child.try_lock()
            && let Some(child) = child.as_mut()
        {
            let _ = child.kill();
            // Reap the child so we don't leak a defunct process entry.
            let _ = child.wait();
        }
    }
}

// ── Upstream Selector ───────────────────────────────────────────

struct UpstreamSelector {
    upstreams: Vec<Arc<UpstreamWrapper>>,
    cached_order: RwLock<Option<(Vec<usize>, Instant)>>,
}

impl UpstreamSelector {
    fn new(upstreams: Vec<Arc<UpstreamWrapper>>) -> Self {
        Self {
            upstreams,
            cached_order: RwLock::new(None),
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
            let cache = self.cached_order.read();
            if let Some((ref order, ref ts)) = *cache
                && ts.elapsed().as_secs() < WEIGHT_CACHE_TTL_SECS
                && order.len() >= count
            {
                return order[..count].to_vec();
            }
        }

        let scores = self.calculate_scores();
        let selected = self.weighted_sample(&scores, count);

        {
            let mut cache = self.cached_order.write();
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
            .map(|&i| (i, self.score_one(&self.upstreams[i])))
            .collect();

        self.weighted_sample(&scores, count)
    }

    fn calculate_scores(&self) -> Vec<(usize, f64)> {
        self.upstreams
            .iter()
            .enumerate()
            .map(|(i, uw)| (i, self.score_one(uw)))
            .collect()
    }

    /// Compute the selection score for a single upstream.
    ///
    /// Higher scores are better. The score applies heavy penalties for latency
    /// (superlinear), error rate (quadratic), and consecutive failure streaks.
    fn score_one(&self, uw: &UpstreamWrapper) -> f64 {
        let latency = uw.ema_latency() as f64;
        let latency = if latency <= 0.0 {
            DEFAULT_LATENCY
        } else {
            latency
        };

        let latency_penalty = latency.powf(LATENCY_EXP);

        let error_rate = uw.error_rate();
        let error_penalty =
            (1.0 + error_rate * ERROR_PENALTY_MULT).powi(ERROR_PENALTY_EXP);

        let streak = uw.consecutive_failures() as f64;
        let streak_penalty = 1.0 + streak * streak * STREAK_PENALTY_MULT;

        let noise = (fastrand::f64() * 2.0 - 1.0) * NOISE_FACTOR;
        let score = (1.0 / (latency_penalty * error_penalty * streak_penalty)) * (1.0 + noise);
        score.max(0.0001)
    }

    fn weighted_sample(&self, scores: &[(usize, f64)], count: usize) -> Vec<usize> {
        let mut remaining: Vec<(usize, f64)> = scores.to_vec();
        let mut selected = Vec::with_capacity(count);

        for _ in 0..count {
            if remaining.is_empty() {
                break;
            }
            let total: f64 = remaining.iter().map(|(_, s)| s).sum();
            let point = fastrand::f64() * total;
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
        // The helper process itself is spawned with REDNS_FORWARD_SOCKET_ENV
        // set; inside it every forward instance must use its configured
        // upstreams directly and never spawn a nested helper (which would
        // recurse forever).
        let in_subprocess = std::env::var(REDNS_FORWARD_SOCKET_ENV).is_ok();
        Self::new_impl(cfg, name, in_subprocess)
    }

    fn new_impl(cfg: ForwardConfig, name: &str, in_subprocess: bool) -> PluginResult<Self> {
        if cfg.upstreams.is_empty() && (cfg.subprocess_suffix.is_none() || in_subprocess) {
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
            opts.pool_max_idle = ucfg.pool_max_idle.or(cfg.pool_max_idle);
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

        if let Some(ref suffix) = cfg.subprocess_suffix {
            if in_subprocess {
                info!(
                    suffix = %suffix,
                    "forward: subprocess mode, using configured upstreams directly"
                );
            } else {
                // Include PID in the socket path to avoid collisions between
                // multiple instances or stale sockets from crashed runs. The
                // helper is spawned with this same path.
                let safe_suffix: String = suffix
                    .chars()
                    .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
                    .collect();
                let socket_path = format!(
                    "/tmp/redns{safe_suffix}-{}.sock",
                    std::process::id()
                );
                let sub = SubprocessUpstream::new(suffix, &socket_path)?;
                let sub_name = format!("subprocess{suffix}");
                let uw = Arc::new(UpstreamWrapper::new(
                    Box::new(sub),
                    sub_name.clone(),
                    "Unix".into(),
                ));
                upstreams.push(uw);
                tag_index
                    .entry(sub_name)
                    .or_default()
                    .push(upstreams.len() - 1);
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
        query_bytes: Arc<Vec<u8>>,
        qname: &str,
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
            let resp_bytes = selected[0].exchange(query_bytes.as_slice()).await?;
            let resp = match Message::from_vec(&resp_bytes) {
                Ok(r) => r,
                Err(e) => {
                    if let Some(repaired) = repair_trailing_trim(&resp_bytes) {
                        if let Ok(r) = Message::from_vec(&repaired) {
                            r
                        } else {
                            return Err(format!("invalid upstream response: {e}").into());
                        }
                    } else {
                        return Err(format!("invalid upstream response: {e}").into());
                    }
                }
            };
            selected[0].record_adopted();
            return Ok((resp, selected[0].clone()));
        }

        let total = selected.len();
        let hedge_delay = hedge_delay_for(selected[0].as_ref());
        let mut tasks = tokio::task::JoinSet::new();
        let mut next_to_launch = 0usize;

        let launch_next = |tasks: &mut tokio::task::JoinSet<_>,
                           next_to_launch: &mut usize,
                           query_bytes: &Arc<Vec<u8>>,
                           selected: &[Arc<UpstreamWrapper>]| {
            if *next_to_launch >= selected.len() {
                return false;
            }
            let sel_idx = *next_to_launch;
            *next_to_launch += 1;
            let qb = query_bytes.clone();
            let u = selected[sel_idx].clone();
            tasks.spawn(async move { (sel_idx, u.exchange(qb.as_slice()).await) });
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
                            warn!(plugin = %self.name, upstream = %upstream_name, qname = %qname, "invalid upstream response (too short)");
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
                            if let Some(repaired) = repair_trailing_trim(&resp_bytes) {
                                if let Ok(resp) = Message::from_vec(&repaired) {
                                    selected[sel_idx].record_adopted();
                                    tasks.abort_all();
                                    return Ok((resp, selected[sel_idx].clone()));
                                }
                            }
                            warn!(plugin = %self.name, upstream = %upstream_name, qname = %qname, error = %e, "invalid upstream response");
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

    async fn resolve(
        &self,
        query: &Message,
        // Optional pre-serialized form of `query`. Must reflect the *current*
        // logical message (including EDNS rewrite / ECS / redirect). Stale
        // client-capture wire is never safe here — `Context::query_mut` clears
        // the cache so callers re-serialize after mutation.
        cached_query_wire: Option<Arc<Vec<u8>>>,
    ) -> PluginResult<(Message, Arc<UpstreamWrapper>)> {
        let qname = query
            .queries
            .first()
            .map(|q| q.name().to_ascii())
            .unwrap_or_default();
        let query_bytes = if let Some(raw) = cached_query_wire {
            raw
        } else {
            Arc::new(query.to_vec().map_err(
                |e| -> Box<dyn std::error::Error + Send + Sync> {
                    format!("failed to serialize query: {e}").into()
                },
            )?)
        };
        self.resolve_once(query_bytes, &qname).await
    }
}

#[async_trait]
impl Executable for Forward {
    async fn exec(&self, ctx: &mut Context) -> PluginResult<()> {
        if ctx.response().is_some() {
            return Ok(());
        }

        // Prefer Context's logical wire cache (set after EDNS rewrite, cleared
        // by query_mut on any subsequent mutation). Never use the raw client
        // packet from ingress.
        let (resp, selected_upstream) = self
            .resolve(ctx.query(), ctx.query_wire().cloned())
            .await?;
        ctx.store_value(KV_SELECTED_UPSTREAM, selected_upstream);
        ctx.set_response(Some(resp));

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn forward_config_empty_fails() {
        let cfg = ForwardConfig::default();
        assert!(Forward::new(cfg, "test").is_err());
    }

    #[test]
    fn forward_config_subprocess_suffix_only_ok() {
        let yaml = "subprocess_suffix: -direct\n";
        let cfg = ForwardConfig::from_yaml_str(yaml).unwrap();
        assert_eq!(cfg.subprocess_suffix.as_deref(), Some("-direct"));
        assert!(cfg.upstreams.is_empty());
    }

    #[test]
    fn forward_subprocess_mode_uses_upstreams_without_spawning() {
        // Simulates running inside the helper process: REDNS_FORWARD_SOCKET_ENV
        // is set, so the forward must use its configured upstreams directly and
        // must NOT look for a (nonexistent) `redns<suffix>` binary.
        let cfg = ForwardConfig {
            upstreams: vec![UpstreamConfig {
                addr: "udp://8.8.8.8:53".into(),
                tag: None,
                dial_addr: None,
                bootstrap: None,
                pool_max_idle: None,
            }],
            concurrent: 1,
            subprocess_suffix: Some("-direct".into()),
            pool_max_idle: None,
        };
        let f = Forward::new_impl(cfg, "test", true).unwrap();
        assert_eq!(f.upstreams().len(), 1);
        assert_eq!(f.upstreams()[0].name(), "udp://8.8.8.8:53");
    }

    #[test]
    fn forward_subprocess_mode_without_upstreams_fails() {
        // Inside the helper a suffix alone cannot work — there is nothing to
        // delegate to and spawning a nested helper would recurse forever.
        let cfg = ForwardConfig {
            upstreams: vec![],
            concurrent: 1,
            subprocess_suffix: Some("-direct".into()),
            pool_max_idle: None,
        };
        assert!(Forward::new_impl(cfg, "test", true).is_err());
    }

    #[test]
    fn forward_config_valid() {
        let cfg = ForwardConfig {
            upstreams: vec![UpstreamConfig {
                addr: "udp://8.8.8.8:53".into(),
                tag: None,
                dial_addr: None,
                bootstrap: None,
                pool_max_idle: None,
            }],
            concurrent: 1,
            subprocess_suffix: None,
            pool_max_idle: None,
        };
        assert!(Forward::new(cfg, "test").is_ok());
    }

    #[test]
    fn forward_config_pool_max_idle_yaml() {
        let yaml = r#"
pool_max_idle: 8
upstreams:
  - addr: udp://8.8.8.8:53
    pool_max_idle: 2
"#;
        let cfg = ForwardConfig::from_yaml_str(yaml).unwrap();
        assert_eq!(cfg.pool_max_idle, Some(8));
        assert_eq!(cfg.upstreams[0].pool_max_idle, Some(2));
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
                    pool_max_idle: None,
                },
                UpstreamConfig {
                    addr: "udp://1.1.1.1:53".into(),
                    tag: Some("cloudflare".into()),
                    dial_addr: None,
                    bootstrap: None,
                    pool_max_idle: None,
                },
                UpstreamConfig {
                    addr: "udp://9.9.9.9:53".into(),
                    tag: Some("quad9".into()),
                    dial_addr: None,
                    bootstrap: None,
                    pool_max_idle: None,
                },
            ],
            concurrent: 1,
            subprocess_suffix: None,
            pool_max_idle: None,
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
                    pool_max_idle: None,
                },
                UpstreamConfig {
                    addr: "udp://1.1.1.1:53".into(),
                    tag: Some("cloudflare".into()),
                    dial_addr: None,
                    bootstrap: None,
                    pool_max_idle: None,
                },
            ],
            concurrent: 1,
            subprocess_suffix: None,
            pool_max_idle: None,
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
                pool_max_idle: None,
            }],
            concurrent: 1,
            subprocess_suffix: None,
            pool_max_idle: None,
        };
        assert!(Forward::new(cfg, "test").is_err());
    }

    #[test]
    fn repair_trailing_trim_recovers_trimmed_aaaa() {
        // Real wire capture: NovaXNS Gcore trimmed trailing zeros from an AAAA
        // response for clienttoken.spotify.com. RDLENGTH=16 but only 8 bytes
        // of rdata present (2600:1901:0001:07c5 — last 8 bytes were zeros).
        #[rustfmt::skip]
        let wire: Vec<u8> = vec![
            0x1c, 0x66, 0x81, 0x80, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00,
            0x0b, 0x63, 0x6c, 0x69, 0x65, 0x6e, 0x74, 0x74, 0x6f, 0x6b, 0x65, 0x6e,
            0x07, 0x73, 0x70, 0x6f, 0x74, 0x69, 0x66, 0x79, 0x03, 0x63, 0x6f, 0x6d,
            0x00, 0x00, 0x1c, 0x00, 0x01,
            0xc0, 0x0c, 0x00, 0x1c, 0x00, 0x01, 0x00, 0x00, 0x01, 0x2c, 0x00, 0x10,
            0x26, 0x00, 0x19, 0x01, 0x00, 0x01, 0x07, 0xc5,
        ];

        // Original must fail.
        assert!(Message::from_vec(&wire).is_err());

        // Repair should pad 8 zero bytes.
        let repaired = repair_trailing_trim(&wire).expect("should detect trailing trim");
        assert_eq!(repaired.len(), wire.len() + 8);
        assert!(repaired[wire.len()..].iter().all(|&b| b == 0));

        // Repaired message must parse.
        let msg = Message::from_vec(&repaired).expect("repaired wire should parse");
        assert_eq!(msg.answers.len(), 1);
        let rdata = &msg.answers[0].data;
        if let hickory_proto::rr::RData::AAAA(ipv6) = rdata {
            assert_eq!(
                ipv6.0,
                std::net::Ipv6Addr::new(0x2600, 0x1901, 0x0001, 0x07c5, 0, 0, 0, 0)
            );
        } else {
            panic!("expected AAAA rdata");
        }
    }
}
