// Copyright (C) 2026, Harlan Wei
//
// This file is part of redns.

//! Upstream DNS transport implementations.
//!
//! This module provides the [`Upstream`] trait and concrete implementations for
//! all major DNS transport protocols. Each upstream exchanges raw DNS wire-format
//! messages with a remote DNS server.
//!
//! # Supported Protocols
//!
//! | Protocol | Scheme       | Default Port | Features                          |
//! |----------|--------------|--------------|-----------------------------------|
//! | UDP      | `udp://`     | 53           | Socket pooling, ID filtering      |
//! | TCP      | `tcp://`     | 53           | Connection pooling, auto-retry    |
//! | DoT      | `tls://`     | 853          | TLS session cache, pooling        |
//! | DoH      | `https://`   | 443          | HTTP/2, GET+POST (RFC 8484)       |
//! | DoQ      | `quic://`    | 853          | QUIC streams (RFC 9250)           |
//! | DoH3     | `h3://`      | 443          | HTTP/3 over QUIC                  |
//!
//! # Creating Upstreams
//!
//! Use [`new_upstream`] to parse a URL string and create the appropriate upstream:
//!
//! ```rust,ignore
//! use redns_core::upstream::{new_upstream, UpstreamOpts};
//!
//! // UDP (default)
//! let udp = new_upstream("8.8.8.8:53", UpstreamOpts::default())?;
//!
//! // DNS-over-TLS
//! let dot = new_upstream("tls://1.1.1.1:853", UpstreamOpts::default())?;
//!
//! // DNS-over-HTTPS
//! let doh = new_upstream("https://dns.google/dns-query", UpstreamOpts::default())?;
//! ```
//!
//! # Hostname Resolution
//!
//! Upstreams with domain names (DoH, DoT, DoQ, DoH3) require explicit resolution
//! configuration to avoid DNS bootstrapping loops. Two options:
//!
//! 1. **Static pinning** via `dial_addr` — connect directly to a fixed IP:
//!    ```rust,ignore
//!    let opts = UpstreamOpts {
//!        dial_addr: Some("8.8.8.8:443".parse().unwrap()),
//!        ..Default::default()
//!    };
//!    let doh = new_upstream("https://dns.google/dns-query", opts)?;
//!    ```
//!
//! 2. **Bootstrap resolver** — resolve via a specific DNS server with TTL caching:
//!    ```rust,ignore
//!    let opts = UpstreamOpts {
//!        bootstrap: Some("8.8.8.8:53".to_string()),
//!        ..Default::default()
//!    };
//!    let doh = new_upstream("https://dns.google/dns-query", opts)?;
//!    ```
//!
//! **Resolution precedence:** IP-based (no resolution needed) → `dial_addr` (static) → `bootstrap` (DNS) → error.
//!
//! Bootstrap upstreams must themselves be IP-based to prevent recursion.
//!
//! # Connection Pooling
//!
//! TCP and TLS upstreams pool idle connections with a 30-second TTL. Stale connections
//! are automatically detected and retried once. UDP upstreams pool sockets
//! to reuse ephemeral ports, reducing setup overhead.
//!
//! # Metrics
//!
//! Wrap an upstream with [`UpstreamWrapper`] to track per-upstream latency (EMA),
//! query counts, error rates, and adoption metrics. See [`UpstreamMetrics`] for
//! the full set of tracked statistics.

use crate::plugin::PluginResult;
use async_trait::async_trait;
use parking_lot::Mutex as StdMutex;
use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU16, AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, UdpSocket};
use tokio::sync::Mutex;
use tracing::warn;

/// Absolute maximum DNS UDP payload we will attempt to receive (EDNS upper bound).
const MAX_UDP_SIZE: usize = 65535;

/// Default UDP recv buffer when the query advertises no EDNS0 payload size.
const DEFAULT_UDP_RECV: usize = 4096;

/// Classic DNS minimum UDP payload (no EDNS).
const MIN_UDP_RECV: usize = 512;

/// Default exchange timeout.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);

/// Default idle timeout for pooled connections.
///
/// Kept generous (5 min) so secure-transport connections (DoT) stay warm
/// across the sparse query gaps typical of a home/small resolver, avoiding a
/// fresh TLS handshake (1-2 RTT) on the critical path. If the upstream has
/// closed an idle connection in the meantime, `pooled_exchange` transparently
/// reconnects and retries, so a longer window costs at most one stale retry.
const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(300);

/// Default maximum idle connections in a stream pool.
const DEFAULT_MAX_IDLE_CONNS: usize = 4;

/// Maximum retries on stale pooled connection (one retry after the first attempt).
const MAX_POOL_RETRY: usize = 1;

/// Default maximum idle UDP sockets in pool.
const DEFAULT_MAX_IDLE_UDP_SOCKETS: usize = 16;

/// User-Agent sent on DoH/DoH3 requests.
///
/// By default reqwest advertises `reqwest/x.y.z`, which some providers
/// rate-limit or deprioritize. A common browser UA blends in with ordinary
/// traffic and avoids client-based filtering.
const DOH_USER_AGENT: &str = "Mozilla/5.0 (X11; Linux x86_64; rv:153.0) Gecko/20100101 Firefox/153.0";

/// Maximum DoH GET URL length before falling back to POST (RFC 8484 §4.1).
///
/// GET carries the query as base64url in the URL; intermediaries and proxies
/// commonly cap URL length around 2 KiB. A query whose GET URL would exceed
/// this is sent via POST instead, which puts the raw wire query in the body.
const DOH_MAX_GET_URL_LEN: usize = 2048;

/// Maximum DNS response body we will accept from a DoH/DoH3/DoQ upstream.
///
/// A DNS message is at most 65535 bytes (the TCP/DoQ 2-byte length prefix caps
/// it structurally). DoH3 accumulates the HTTP/3 body chunk-by-chunk with no
/// inherent bound, so a misbehaving or hostile upstream could stream unbounded
/// data; cap it here to protect against memory exhaustion.
const MAX_DNS_RESPONSE: usize = 65535;

static GLOBAL_LATENCY_SUM_US: AtomicU64 = AtomicU64::new(0);
static GLOBAL_COMPLETED_TOTAL: AtomicU64 = AtomicU64::new(0);

/// An upstream DNS transport that can exchange raw DNS wire messages.
#[async_trait]
pub trait Upstream: Send + Sync {
    /// Sends a DNS query (wire format) and returns the response (wire format).
    async fn exchange(&self, query: &[u8]) -> PluginResult<Vec<u8>>;

    /// Eagerly initialize the upstream (e.g. probe capabilities). Default is no-op.
    async fn probe(&self) {}
}

// ── UDP ─────────────────────────────────────────────────────────

/// Simple UDP upstream transport.
pub struct UdpUpstream {
    addr: SocketAddr,
    timeout: Duration,
    max_idle_sockets: usize,
    pool: Mutex<VecDeque<UdpSocket>>,
}

impl UdpUpstream {
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            timeout: DEFAULT_TIMEOUT,
            max_idle_sockets: DEFAULT_MAX_IDLE_UDP_SOCKETS,
            pool: Mutex::new(VecDeque::new()),
        }
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn with_max_idle_sockets(mut self, max_idle_sockets: usize) -> Self {
        self.max_idle_sockets = max_idle_sockets;
        self
    }

    async fn create_socket(&self) -> PluginResult<UdpSocket> {
        // Bind to :0 gives the OS an ephemeral source port, which provides
        // per-socket randomization and reduces cache-poisoning surface. The
        // pooled sockets are reused across queries, so the ID-matching loop
        // in `exchange` is load-bearing — it discards stale datagrams that
        // could otherwise be misinterpreted as responses to later queries.
        let bind_addr: SocketAddr = if self.addr.is_ipv4() {
            "0.0.0.0:0"
                .parse()
                .expect("hardcoded IPv4 bind address is valid")
        } else {
            "[::]:0"
                .parse()
                .expect("hardcoded IPv6 bind address is valid")
        };

        let sock = UdpSocket::bind(bind_addr).await.map_err(
            |e| -> Box<dyn std::error::Error + Send + Sync> { format!("udp bind: {e}").into() },
        )?;

        sock.connect(self.addr)
            .await
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                format!("udp connect {}: {e}", self.addr).into()
            })?;

        Ok(sock)
    }

    async fn get_socket(&self) -> PluginResult<UdpSocket> {
        let mut pool = self.pool.lock().await;
        if let Some(sock) = pool.pop_front() {
            return Ok(sock);
        }
        drop(pool);
        self.create_socket().await
    }

    async fn put_socket(&self, sock: UdpSocket) {
        if self.max_idle_sockets == 0 {
            return;
        }
        let mut pool = self.pool.lock().await;
        while pool.len() >= self.max_idle_sockets {
            pool.pop_front();
        }
        pool.push_back(sock);
    }

    /// Remaining time until `deadline`, or an error if the budget is exhausted.
    fn remaining_until(deadline: Instant) -> PluginResult<Duration> {
        match deadline.checked_duration_since(Instant::now()) {
            Some(d) if !d.is_zero() => Ok(d),
            _ => Err("udp exchange timed out".into()),
        }
    }

    /// One-shot TCP exchange used when an upstream UDP answer arrives with TC=1.
    async fn tcp_fallback(&self, query: &[u8], timeout: Duration) -> PluginResult<Vec<u8>> {
        let mut stream = tcp_connect(self.addr, timeout).await?;
        stream_exchange(&mut stream, query, timeout).await
    }
}

/// DNS header TC (truncated) flag lives in byte 2, bit 1 (0x02).
fn dns_header_tc(wire: &[u8]) -> bool {
    wire.len() >= 3 && (wire[2] & 0x02) != 0
}

/// Choose a UDP recv buffer large enough for the EDNS0 payload the query
/// advertises, clamped to `[MIN_UDP_RECV, MAX_UDP_SIZE]`.
fn udp_recv_capacity(query: &[u8]) -> usize {
    use hickory_proto::op::Message;

    if let Ok(msg) = Message::from_vec(query)
        && let Some(edns) = msg.edns
    {
        return (edns.max_payload() as usize).clamp(MIN_UDP_RECV, MAX_UDP_SIZE);
    }
    DEFAULT_UDP_RECV.min(MAX_UDP_SIZE)
}

#[async_trait]
impl Upstream for UdpUpstream {
    async fn exchange(&self, query: &[u8]) -> PluginResult<Vec<u8>> {
        // The DNS message ID is the first two bytes of the wire message. Sockets
        // are pooled and reused, so a late or duplicate datagram from a previous
        // query can arrive on this socket. Match the response ID against the
        // query's and discard non-matching datagrams, reading until a matching
        // response arrives or the overall timeout elapses. Without this, a stale
        // datagram could be returned as the answer to a different query.
        let want_id = match query.get(0..2) {
            Some(id) => [id[0], id[1]],
            None => return Err("udp exchange: query too short".into()),
        };

        let sock = self.get_socket().await?;

        // Bound the whole exchange (send + recv), not just the receive side.
        // Mirrors `stream_exchange` so a stalled send cannot hang forever.
        let deadline = Instant::now() + self.timeout;

        let remaining = Self::remaining_until(deadline)?;
        match tokio::time::timeout(remaining, sock.send(query)).await {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => {
                return Err(format!("udp send: {e}").into());
            }
            Err(_) => return Err("udp exchange timed out".into()),
        }

        let mut buf = vec![0u8; udp_recv_capacity(query)];
        loop {
            let remaining = Self::remaining_until(deadline)?;

            match tokio::time::timeout(remaining, sock.recv(&mut buf)).await {
                Ok(Ok(n)) => {
                    // Ignore datagrams that are too short to carry an ID or whose
                    // ID does not match this query (a stale/duplicate response).
                    if n < 2 || buf[0..2] != want_id {
                        continue;
                    }
                    let mut resp = buf;
                    resp.truncate(n);
                    // Done with the UDP socket whether or not we fall back to TCP.
                    self.put_socket(sock).await;

                    // Transparent TCP retry on truncation (common resolver
                    // behaviour). Prefer the full TCP answer; if TCP fails,
                    // return the truncated UDP response so the client can still
                    // retry over TCP itself.
                    if dns_header_tc(&resp) {
                        match Self::remaining_until(deadline) {
                            Ok(remaining) => {
                                match self.tcp_fallback(query, remaining).await {
                                    Ok(full) => return Ok(full),
                                    Err(e) => {
                                        warn!(
                                            addr = %self.addr,
                                            error = %e,
                                            "udp TC set; TCP fallback failed, returning truncated"
                                        );
                                    }
                                }
                            }
                            Err(_) => {
                                // Budget exhausted — return the truncated answer.
                            }
                        }
                    }
                    return Ok(resp);
                }
                Ok(Err(e)) => return Err(format!("udp recv: {e}").into()),
                Err(_) => return Err("udp exchange timed out".into()),
            }
        }
    }
}

// ── Connection Pool (shared by TCP and TLS) ─────────────────────

/// An idle connection with a timestamp for expiry tracking.
struct IdleConn<S> {
    stream: S,
    idle_since: Instant,
}

/// A bounded pool of idle connections with TTL-based expiry.
///
/// Shared by the pooled TCP and TLS upstreams — both keep length-prefixed
/// byte streams alive between queries, differing only in the stream type.
struct ConnPool<S> {
    idle_timeout: Duration,
    max_idle: usize,
    conns: Mutex<VecDeque<IdleConn<S>>>,
}

impl<S> ConnPool<S> {
    fn new(idle_timeout: Duration, max_idle: usize) -> Self {
        Self {
            idle_timeout,
            max_idle,
            conns: Mutex::new(VecDeque::new()),
        }
    }

    /// Try to get a non-expired idle connection from the pool.
    async fn get_idle(&self) -> Option<S> {
        let mut pool = self.conns.lock().await;
        while let Some(idle) = pool.pop_front() {
            if idle.idle_since.elapsed() < self.idle_timeout {
                return Some(idle.stream);
            }
            // Expired — drop silently.
        }
        None
    }

    /// Return a connection to the pool, enforcing the max idle limit.
    async fn put_idle(&self, stream: S) {
        if self.max_idle == 0 {
            return;
        }
        let mut pool = self.conns.lock().await;
        while pool.len() >= self.max_idle {
            pool.pop_front();
        }
        pool.push_back(IdleConn {
            stream,
            idle_since: Instant::now(),
        });
    }
}

/// Run a length-prefixed exchange over a pooled connection, reconnecting and
/// retrying on a failed reused connection (which is usually a stale socket the
/// upstream has already closed).
async fn pooled_exchange<S, F, Fut>(
    pool: &ConnPool<S>,
    connect: F,
    query: &[u8],
    timeout: Duration,
) -> PluginResult<Vec<u8>>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = PluginResult<S>>,
{
    let mut retries = 0;
    loop {
        let (mut stream, is_reused) = match pool.get_idle().await {
            Some(s) => (s, true),
            None => (connect().await?, false),
        };

        match stream_exchange(&mut stream, query, timeout).await {
            Ok(resp) => {
                pool.put_idle(stream).await;
                return Ok(resp);
            }
            Err(e) => {
                if is_reused && retries < MAX_POOL_RETRY {
                    retries += 1;
                    continue; // Retry with a fresh connection.
                }
                return Err(e);
            }
        }
    }
}

// ── TCP Pooled ──────────────────────────────────────────────────

/// TCP upstream with connection pooling and retry on stale connections.
pub struct PooledTcpUpstream {
    addr: SocketAddr,
    timeout: Duration,
    pool: ConnPool<TcpStream>,
}

impl PooledTcpUpstream {
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            timeout: DEFAULT_TIMEOUT,
            pool: ConnPool::new(DEFAULT_IDLE_TIMEOUT, DEFAULT_MAX_IDLE_CONNS),
        }
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn with_max_idle_conns(mut self, max_idle: usize) -> Self {
        self.pool.max_idle = max_idle;
        self
    }
}

#[async_trait]
impl Upstream for PooledTcpUpstream {
    async fn exchange(&self, query: &[u8]) -> PluginResult<Vec<u8>> {
        pooled_exchange(
            &self.pool,
            || tcp_connect(self.addr, self.timeout),
            query,
            self.timeout,
        )
        .await
    }
}

// ── TLS (DoT) with Session Caching ──────────────────────────────

use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;
use tokio_rustls::rustls::pki_types::ServerName;
use tokio_rustls::rustls::{ClientConfig, RootCertStore};

/// Shared root certificate store (webpki roots), built once and cloned.
///
/// DoT, DoQ, and DoH3 each need a rustls [`RootCertStore`]. Loading the webpki
/// trust anchors once and cloning avoids re-iterating the full anchor set on
/// every upstream construction.
fn shared_root_store() -> RootCertStore {
    use std::sync::OnceLock;
    static ROOTS: OnceLock<RootCertStore> = OnceLock::new();
    ROOTS
        .get_or_init(|| {
            let mut store = RootCertStore::empty();
            store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
            store
        })
        .clone()
}

/// Build a shared `ClientConfig` with session caching.
fn build_tls_config() -> Arc<ClientConfig> {
    let config = ClientConfig::builder()
        .with_root_certificates(shared_root_store())
        .with_no_client_auth();
    // rustls enables a 256-slot session cache by default.
    Arc::new(config)
}

/// Open a TLS connection to `addr` with SNI `server_name` using `tls_config`.
async fn tls_connect(
    addr: SocketAddr,
    server_name: &str,
    tls_config: &Arc<ClientConfig>,
    timeout: Duration,
) -> PluginResult<TlsStream<TcpStream>> {
    let tcp = tcp_connect(addr, timeout).await?;
    let connector = TlsConnector::from(tls_config.clone());
    let sni = ServerName::try_from(server_name.to_string()).map_err(
        |e| -> Box<dyn std::error::Error + Send + Sync> {
            format!("invalid server name: {e}").into()
        },
    )?;
    connector
        .connect(sni, tcp)
        .await
        .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
            format!("tls handshake: {e}").into()
        })
}

// ── TLS Pooled ──────────────────────────────────────────────────

/// TLS upstream with connection pooling, session caching, and retry.
pub struct PooledTlsUpstream {
    addr: SocketAddr,
    server_name: String,
    timeout: Duration,
    tls_config: Arc<ClientConfig>,
    pool: ConnPool<TlsStream<TcpStream>>,
}

impl PooledTlsUpstream {
    pub fn new(addr: SocketAddr, server_name: String) -> Self {
        Self {
            addr,
            server_name,
            timeout: DEFAULT_TIMEOUT,
            tls_config: build_tls_config(),
            pool: ConnPool::new(DEFAULT_IDLE_TIMEOUT, DEFAULT_MAX_IDLE_CONNS),
        }
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn with_max_idle_conns(mut self, max_idle: usize) -> Self {
        self.pool.max_idle = max_idle;
        self
    }
}

#[async_trait]
impl Upstream for PooledTlsUpstream {
    async fn exchange(&self, query: &[u8]) -> PluginResult<Vec<u8>> {
        pooled_exchange(
            &self.pool,
            || tls_connect(self.addr, &self.server_name, &self.tls_config, self.timeout),
            query,
            self.timeout,
        )
        .await
    }
}

// ── Pipelined TLS (DoT) — RFC 7766 ─────────────────────────────

/// Timeout for the RFC 7766 pipelining probe.
const PIPELINE_PROBE_TIMEOUT: Duration = Duration::from_secs(3);

/// Maximum concurrent in-flight queries on a single pipelined connection.
const PIPELINE_MAX_INFLIGHT: usize = 64;

enum PipelineMode {
    Pipelined,
    Fallback,
}

/// A persistent TLS connection multiplexing multiple in-flight queries by DNS
/// transaction ID, per RFC 7766 §6.2.1.1.
struct PipelinedConn {
    writer: Mutex<Box<dyn tokio::io::AsyncWrite + Unpin + Send>>,
    pending: StdMutex<HashMap<u16, tokio::sync::oneshot::Sender<PluginResult<Vec<u8>>>>>,
    next_id: AtomicU16,
    alive: AtomicBool,
}

impl PipelinedConn {
    fn alloc_id(&self) -> u16 {
        loop {
            let id = self.next_id.fetch_add(1, Ordering::Relaxed);
            if !self.pending.lock().contains_key(&id) {
                return id;
            }
        }
    }

    fn inflight(&self) -> usize {
        self.pending.lock().len()
    }
}

fn spawn_reader<R>(mut reader: R, conn: Arc<PipelinedConn>)
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        loop {
            let mut len_buf = [0u8; 2];
            if reader.read_exact(&mut len_buf).await.is_err() {
                break;
            }
            let resp_len = u16::from_be_bytes(len_buf) as usize;
            let mut resp_buf = vec![0u8; resp_len];
            if reader.read_exact(&mut resp_buf).await.is_err() {
                break;
            }
            if resp_buf.len() < 2 {
                continue;
            }
            let id = u16::from_be_bytes([resp_buf[0], resp_buf[1]]);
            if let Some(tx) = conn.pending.lock().remove(&id) {
                let _ = tx.send(Ok(resp_buf));
            }
        }
        conn.alive.store(false, Ordering::Release);
        let drained: Vec<_> = conn.pending.lock().drain().collect();
        for (_, tx) in drained {
            let _ = tx.send(Err("pipelined connection closed".into()));
        }
    });
}

/// Build a minimal DNS query for "." type A class IN with the given transaction ID.
fn build_probe_query(id: u16) -> Vec<u8> {
    let mut q = Vec::with_capacity(17);
    q.extend_from_slice(&id.to_be_bytes()); // ID
    q.extend_from_slice(&[0x01, 0x00]); // flags: RD=1
    q.extend_from_slice(&[0x00, 0x01]); // QDCOUNT=1
    q.extend_from_slice(&[0x00, 0x00]); // ANCOUNT=0
    q.extend_from_slice(&[0x00, 0x00]); // NSCOUNT=0
    q.extend_from_slice(&[0x00, 0x00]); // ARCOUNT=0
    q.push(0x00); // root label
    q.extend_from_slice(&[0x00, 0x01]); // QTYPE=A
    q.extend_from_slice(&[0x00, 0x01]); // QCLASS=IN
    q
}

/// DoT upstream with RFC 7766 query pipelining.
///
/// On first use, probes the upstream by sending two queries back-to-back on a
/// single connection. If both responses arrive (matched by ID), pipelining is
/// enabled. Otherwise, falls back to the existing pooled exclusive-checkout model.
pub struct PipelinedTlsUpstream {
    addr: SocketAddr,
    server_name: String,
    timeout: Duration,
    tls_config: Arc<ClientConfig>,
    mode: tokio::sync::OnceCell<PipelineMode>,
    conn: Mutex<Option<Arc<PipelinedConn>>>,
    fallback_pool: ConnPool<TlsStream<TcpStream>>,
}

impl PipelinedTlsUpstream {
    pub fn new(addr: SocketAddr, server_name: String) -> Self {
        Self {
            addr,
            server_name,
            timeout: DEFAULT_TIMEOUT,
            tls_config: build_tls_config(),
            mode: tokio::sync::OnceCell::new(),
            conn: Mutex::new(None),
            fallback_pool: ConnPool::new(DEFAULT_IDLE_TIMEOUT, DEFAULT_MAX_IDLE_CONNS),
        }
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn with_max_idle_conns(mut self, max_idle: usize) -> Self {
        self.fallback_pool.max_idle = max_idle;
        self
    }

    async fn connect_tls(&self) -> PluginResult<TlsStream<TcpStream>> {
        tls_connect(self.addr, &self.server_name, &self.tls_config, self.timeout).await
    }

    /// Probe whether the upstream supports RFC 7766 pipelining.
    async fn probe_rfc7766(&self) -> PipelineMode {
        let probe = async {
            let stream = self.connect_tls().await.map_err(|_| ())?;
            let (mut reader, mut writer) = tokio::io::split(stream);

            let q1 = build_probe_query(0x0001);
            let q2 = build_probe_query(0x0002);

            // Send both queries without waiting for the first response.
            let len1 = (q1.len() as u16).to_be_bytes();
            let len2 = (q2.len() as u16).to_be_bytes();
            writer.write_all(&len1).await.map_err(|_| ())?;
            writer.write_all(&q1).await.map_err(|_| ())?;
            writer.write_all(&len2).await.map_err(|_| ())?;
            writer.write_all(&q2).await.map_err(|_| ())?;
            writer.flush().await.map_err(|_| ())?;

            // Read two responses, verify both IDs appear.
            let mut seen = [false; 2];
            for _ in 0..2 {
                let mut len_buf = [0u8; 2];
                reader.read_exact(&mut len_buf).await.map_err(|_| ())?;
                let resp_len = u16::from_be_bytes(len_buf) as usize;
                let mut resp_buf = vec![0u8; resp_len];
                reader.read_exact(&mut resp_buf).await.map_err(|_| ())?;
                if resp_buf.len() >= 2 {
                    let id = u16::from_be_bytes([resp_buf[0], resp_buf[1]]);
                    match id {
                        0x0001 => seen[0] = true,
                        0x0002 => seen[1] = true,
                        _ => {}
                    }
                }
            }

            if seen[0] && seen[1] {
                // Pipelining works — reuse this connection.
                let conn = Arc::new(PipelinedConn {
                    writer: Mutex::new(Box::new(writer)),
                    pending: StdMutex::new(HashMap::new()),
                    next_id: AtomicU16::new(0x0003),
                    alive: AtomicBool::new(true),
                });
                spawn_reader(reader, conn.clone());
                *self.conn.lock().await = Some(conn);
                Ok(())
            } else {
                Err(())
            }
        };

        match tokio::time::timeout(PIPELINE_PROBE_TIMEOUT, probe).await {
            Ok(Ok(())) => {
                tracing::info!(server = %self.server_name, addr = %self.addr, "DoT upstream supports RFC 7766 pipelining");
                PipelineMode::Pipelined
            }
            _ => {
                tracing::info!(server = %self.server_name, addr = %self.addr, "DoT upstream does not support RFC 7766 pipelining, using pooled fallback");
                PipelineMode::Fallback
            }
        }
    }

    async fn get_conn(&self) -> Option<Arc<PipelinedConn>> {
        let guard = self.conn.lock().await;
        if let Some(ref c) = *guard {
            if c.alive.load(Ordering::Acquire) {
                return Some(c.clone());
            }
        }
        drop(guard);

        // Reconnect.
        let stream = self.connect_tls().await.ok()?;
        let (reader, writer) = tokio::io::split(stream);
        let conn = Arc::new(PipelinedConn {
            writer: Mutex::new(Box::new(writer)),
            pending: StdMutex::new(HashMap::new()),
            next_id: AtomicU16::new(0),
            alive: AtomicBool::new(true),
        });
        spawn_reader(reader, conn.clone());
        *self.conn.lock().await = Some(conn.clone());
        Some(conn)
    }

    async fn pipelined_exchange(&self, query: &[u8]) -> PluginResult<Vec<u8>> {
        let conn = self
            .get_conn()
            .await
            .ok_or_else(|| -> Box<dyn std::error::Error + Send + Sync> {
                "pipelined connect failed".into()
            })?;

        if conn.inflight() >= PIPELINE_MAX_INFLIGHT {
            return Err("pipelined connection at capacity".into());
        }

        let original_id = if query.len() >= 2 {
            u16::from_be_bytes([query[0], query[1]])
        } else {
            0
        };

        let pipe_id = conn.alloc_id();
        let (tx, rx) = tokio::sync::oneshot::channel();
        conn.pending.lock().insert(pipe_id, tx);

        // Rewrite the transaction ID and send.
        let mut wire = query.to_vec();
        if wire.len() >= 2 {
            wire[0..2].copy_from_slice(&pipe_id.to_be_bytes());
        }
        let len = (wire.len() as u16).to_be_bytes();

        {
            let mut w = conn.writer.lock().await;
            if let Err(e) = w.write_all(&len).await {
                conn.pending.lock().remove(&pipe_id);
                return Err(format!("pipelined write: {e}").into());
            }
            if let Err(e) = w.write_all(&wire).await {
                conn.pending.lock().remove(&pipe_id);
                return Err(format!("pipelined write: {e}").into());
            }
            let _ = w.flush().await;
        }

        match tokio::time::timeout(self.timeout, rx).await {
            Ok(Ok(Ok(mut resp))) => {
                // Restore the caller's original transaction ID.
                if resp.len() >= 2 {
                    resp[0..2].copy_from_slice(&original_id.to_be_bytes());
                }
                Ok(resp)
            }
            Ok(Ok(Err(e))) => Err(e),
            Ok(Err(_)) => {
                conn.pending.lock().remove(&pipe_id);
                Err("pipelined connection closed".into())
            }
            Err(_) => {
                conn.pending.lock().remove(&pipe_id);
                Err("pipelined exchange timed out".into())
            }
        }
    }
}

#[async_trait]
impl Upstream for PipelinedTlsUpstream {
    async fn exchange(&self, query: &[u8]) -> PluginResult<Vec<u8>> {
        let mode = self.mode.get_or_init(|| self.probe_rfc7766()).await;

        match mode {
            PipelineMode::Fallback => {
                pooled_exchange(
                    &self.fallback_pool,
                    || tls_connect(self.addr, &self.server_name, &self.tls_config, self.timeout),
                    query,
                    self.timeout,
                )
                .await
            }
            PipelineMode::Pipelined => {
                match self.pipelined_exchange(query).await {
                    Ok(resp) => Ok(resp),
                    Err(e) => {
                        // If the connection died, retry once with a fresh connection.
                        let conn_alive = self
                            .conn
                            .lock()
                            .await
                            .as_ref()
                            .map(|c| c.alive.load(Ordering::Acquire))
                            .unwrap_or(false);
                        if !conn_alive {
                            self.pipelined_exchange(query).await
                        } else {
                            Err(e)
                        }
                    }
                }
            }
        }
    }

    async fn probe(&self) {
        self.mode.get_or_init(|| self.probe_rfc7766()).await;
    }
}

// ── Shared TCP/TLS helpers ──────────────────────────────────────

/// Connect to a TCP address with timeout.
async fn tcp_connect(addr: SocketAddr, timeout: Duration) -> PluginResult<TcpStream> {
    let stream = tokio::time::timeout(timeout, TcpStream::connect(addr))
        .await
        .map_err(|_| -> Box<dyn std::error::Error + Send + Sync> {
            "tcp connect timed out".into()
        })?
        .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
            format!("tcp connect: {e}").into()
        })?;
    let _ = stream.set_nodelay(true);
    Ok(stream)
}

/// Exchange a DNS query over any length-prefixed byte stream (TCP or TLS).
///
/// Writes the query and reads the response under a single timeout. Bounding
/// only the read would leave the write able to block indefinitely if the
/// upstream accepts the connection but stalls its receive window.
async fn stream_exchange<S>(
    stream: &mut S,
    query: &[u8],
    timeout: Duration,
) -> PluginResult<Vec<u8>>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let len = query.len() as u16;
    let exchange = tokio::time::timeout(timeout, async {
        stream.write_all(&len.to_be_bytes()).await?;
        stream.write_all(query).await?;
        let mut len_buf = [0u8; 2];
        stream.read_exact(&mut len_buf).await?;
        let resp_len = u16::from_be_bytes(len_buf) as usize;
        let mut resp_buf = vec![0u8; resp_len];
        stream.read_exact(&mut resp_buf).await?;
        Ok::<Vec<u8>, std::io::Error>(resp_buf)
    });

    match exchange.await {
        Ok(Ok(resp)) => Ok(resp),
        Ok(Err(e)) => Err(format!("stream exchange: {e}").into()),
        Err(_) => Err("stream exchange timed out".into()),
    }
}

// ── DoH Upstream (RFC 8484) ─────────────────────────────────────

/// How a DoH upstream's hostname was resolved.
enum DohResolution {
    /// Host is already an IP address — no resolution needed.
    Ip,
    /// Pinned to a static address via `dial_addr`.
    StaticAddr(SocketAddr),
    /// TTL-aware resolution via a bootstrap DNS server.
    Bootstrap(Arc<BootstrapResolver>),
}

/// DNS-over-HTTPS upstream using HTTP GET (RFC 8484 §4.1).
pub struct DohUpstream {
    endpoint: String,
    client: reqwest::Client,
    timeout: Duration,
}

impl DohUpstream {
    /// Creates a DoH upstream.
    ///
    /// `resolution` controls how the hostname in the endpoint URL is resolved:
    /// - `Ip`: host is already an IP, no special handling
    /// - `StaticAddr`: pin to a fixed address (from `dial_addr`)
    /// - `Bootstrap`: use a TTL-aware custom resolver
    ///
    /// `timeout` is baked into the reqwest client (covering connect, send, and
    /// body read) so it stays consistent with the per-exchange deadline.
    fn new(
        endpoint: String,
        resolution: DohResolution,
        pool_max_idle: usize,
        timeout: Duration,
    ) -> Self {
        // reqwest is built with the `rustls` feature so it shares the
        // aws-lc-rs backend used by DoT/DoQ/DoH3; install the process default
        // first for determinism.
        crate::install_rustls_crypto_provider();
        use reqwest::header;
        let mut headers = header::HeaderMap::new();
        // HeaderValue::from_static is infallible for valid static strings.
        headers.insert(
            header::ACCEPT,
            header::HeaderValue::from_static("application/dns-message"),
        );
        headers.insert(
            header::USER_AGENT,
            header::HeaderValue::from_static(DOH_USER_AGENT),
        );

        let mut builder = reqwest::Client::builder()
            .default_headers(headers)
            .timeout(timeout)
            // HTTP/2 tuning for a latency-sensitive, small-message DNS workload:
            // adaptive flow-control windows avoid stalls without hand-tuning, and
            // keep-alive pings (also while idle) surface a dead pooled connection
            // long before the 180s idle timeout would.
            .http2_adaptive_window(true)
            .http2_keep_alive_interval(Duration::from_secs(30))
            .http2_keep_alive_timeout(Duration::from_secs(10))
            .http2_keep_alive_while_idle(true)
            .pool_idle_timeout(Duration::from_secs(180))
            .pool_max_idle_per_host(pool_max_idle);

        match resolution {
            DohResolution::Ip => {}
            DohResolution::StaticAddr(addr) => {
                if let Ok(url) = reqwest::Url::parse(&endpoint)
                    && let Some(host) = url.host_str()
                {
                    builder = builder.resolve(host, addr);
                }
            }
            DohResolution::Bootstrap(resolver) => {
                builder = builder.dns_resolver(resolver);
            }
        }

        let client = builder.build().expect("failed to build reqwest client");

        Self {
            endpoint,
            client,
            timeout,
        }
    }
}

#[async_trait]
impl Upstream for DohUpstream {
    async fn exchange(&self, query: &[u8]) -> PluginResult<Vec<u8>> {
        use base64::Engine;
        use reqwest::header;

        // Zero the ID for HTTP cache friendliness (RFC 8484 §4.1).
        let mut wire = query.to_vec();
        if wire.len() >= 2 {
            wire[0] = 0;
            wire[1] = 0;
        }

        let encoded = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&wire);
        let url = format!("{}?dns={}", self.endpoint, encoded);

        // RFC 8484 §4.1: prefer GET, but fall back to POST when the base64url
        // query would push the URL past what intermediaries reliably accept.
        // POST carries the raw wire query in the request body instead.
        let use_post = url.len() > DOH_MAX_GET_URL_LEN;

        // Bound the *entire* exchange — send and body read — with one timeout so a
        // slow-drip response body cannot outlive the deadline.
        let bytes = tokio::time::timeout(self.timeout, async move {
            let request = if use_post {
                self.client
                    .post(&self.endpoint)
                    .header(header::CONTENT_TYPE, "application/dns-message")
                    .body(wire)
            } else {
                self.client.get(&url)
            };

            let resp =
                request
                    .send()
                    .await
                    .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                        format!("doh request: {e}").into()
                    })?;

            if !resp.status().is_success() {
                return Err(format!("doh: bad status {}", resp.status()).into());
            }

            // RFC 8484 §4.2: the response MUST be application/dns-message. A
            // captive portal or misconfigured proxy may return 200 with an HTML
            // page; reject it here with a clear error rather than letting a
            // confusing parse failure surface downstream.
            let content_type = resp
                .headers()
                .get(header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");
            if !content_type
                .to_ascii_lowercase()
                .contains("application/dns-message")
            {
                return Err(format!(
                    "doh: unexpected content-type '{content_type}' (want application/dns-message)"
                )
                .into());
            }

            // Best-effort guard against an oversized body. A DNS message is at
            // most 65535 bytes; reject anything whose advertised length exceeds
            // that. Chunked responses carry no Content-Length so this cannot
            // bound them, but a well-behaved DoH server always sends a length.
            if let Some(len) = resp.content_length()
                && len > MAX_DNS_RESPONSE as u64
            {
                return Err(format!("doh response too large: {len} bytes").into());
            }

            resp.bytes()
                .await
                .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                    format!("doh read body: {e}").into()
                })
        })
        .await
        .map_err(|_| -> Box<dyn std::error::Error + Send + Sync> { "doh request timed out".into() })??;

        let mut body = bytes.to_vec();

        // Restore original query ID.
        if body.len() >= 2 && query.len() >= 2 {
            body[0] = query[0];
            body[1] = query[1];
        }

        Ok(body)
    }
}

// ── DoQ (DNS-over-QUIC, RFC 9250) ───────────────────────────────

fn quic_bind_addr_for_target(target: SocketAddr) -> SocketAddr {
    if target.is_ipv4() {
        "0.0.0.0:0"
            .parse()
            .expect("hardcoded IPv4 bind address is valid")
    } else {
        "[::]:0"
            .parse()
            .expect("hardcoded IPv6 bind address is valid")
    }
}

/// Transport config that keeps an idle QUIC connection (DoQ/DoH3) alive.
///
/// QUIC connections are otherwise torn down by the peer (or our own
/// `max_idle_timeout`) after a short idle period, forcing a fresh handshake on
/// the next query. Sending a keepalive every 25s holds the single pooled
/// connection open across the sparse query gaps typical of a home resolver,
/// while a 5-min max idle timeout bounds how long a dead connection lingers.
fn quic_keepalive_transport() -> Arc<quinn::TransportConfig> {
    let mut transport = quinn::TransportConfig::default();
    transport.keep_alive_interval(Some(Duration::from_secs(25)));
    transport.max_idle_timeout(Some(
        quinn::IdleTimeout::try_from(Duration::from_secs(300))
            .expect("300s is a valid QUIC idle timeout"),
    ));
    Arc::new(transport)
}

/// A lazily-created, shared QUIC client endpoint per address family.
///
/// A `quinn::Endpoint` corresponds to a single UDP socket and driver task but
/// can host many connections — to different resolvers, each with its own
/// per-connection `ClientConfig` (ALPN etc.) supplied at `connect_with` time.
/// Sharing one endpoint per address family avoids spawning a fresh socket and
/// event loop for every DoQ/DoH3 upstream.
///
/// Two endpoints are kept (one bound to `0.0.0.0:0`, one to `[::]:0`) rather
/// than relying on a single dual-stack socket, so IPv4 and IPv6 targets each
/// use a socket of their own family regardless of platform dual-stack quirks.
fn shared_quic_endpoint(target: SocketAddr) -> PluginResult<quinn::Endpoint> {
    static V4: StdMutex<Option<quinn::Endpoint>> = StdMutex::new(None);
    static V6: StdMutex<Option<quinn::Endpoint>> = StdMutex::new(None);

    let cell = if target.is_ipv4() { &V4 } else { &V6 };
    let mut guard = cell.lock();
    if let Some(ep) = guard.as_ref() {
        return Ok(ep.clone());
    }
    let bind_addr = quic_bind_addr_for_target(target);
    // Do not set a default client config: each connection passes its own via
    // `connect_with`, letting one endpoint serve both DoQ ("doq") and DoH3
    // ("h3") connections. On error the cache is left empty so a later upstream
    // can retry the bind instead of inheriting a cached failure.
    let endpoint = quinn::Endpoint::client(bind_addr).map_err(
        |e| -> Box<dyn std::error::Error + Send + Sync> {
            format!("quic endpoint bind: {e}").into()
        },
    )?;
    *guard = Some(endpoint.clone());
    Ok(endpoint)
}

type H3SendRequest = h3::client::SendRequest<h3_quinn::OpenStreams, bytes::Bytes>;

struct Doh3Session {
    quinn_conn: quinn::Connection,
    send_request: H3SendRequest,
    driver: tokio::task::JoinHandle<()>,
}

impl Doh3Session {
    fn is_healthy(&self) -> bool {
        !self.driver.is_finished() && self.quinn_conn.close_reason().is_none()
    }
}

/// DNS-over-QUIC upstream using `quinn` (RFC 9250).
///
/// Per RFC 9250 §4.2: open a bi-directional QUIC stream, send a
/// 2-byte length prefix + DNS wire query, read the response in the
/// same length-prefixed format.
pub struct DoqUpstream {
    addr: SocketAddr,
    server_name: String,
    timeout: Duration,
    endpoint: quinn::Endpoint,
    client_config: quinn::ClientConfig,
    conn: Mutex<Option<quinn::Connection>>,
}

impl DoqUpstream {
    fn new(addr: SocketAddr, server_name: String) -> PluginResult<Self> {
        let client_config = Self::build_client_config();
        let endpoint = shared_quic_endpoint(addr)?;
        Ok(Self {
            addr,
            server_name,
            timeout: DEFAULT_TIMEOUT,
            endpoint,
            client_config,
            conn: Mutex::new(None),
        })
    }

    fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    fn build_client_config() -> quinn::ClientConfig {
        let mut tls_config = tokio_rustls::rustls::ClientConfig::builder()
            .with_root_certificates(shared_root_store())
            .with_no_client_auth();
        tls_config.alpn_protocols = vec![b"doq".to_vec()];
        let quic_config = quinn::crypto::rustls::QuicClientConfig::try_from(tls_config)
            .expect("failed to create QUIC client config");
        let mut client_config = quinn::ClientConfig::new(Arc::new(quic_config));
        client_config.transport_config(quic_keepalive_transport());
        client_config
    }

    async fn connect(&self) -> PluginResult<quinn::Connection> {
        let connecting = self
            .endpoint
            .connect_with(self.client_config.clone(), self.addr, &self.server_name)
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                format!("doq connect: {e}").into()
            })?;

        tokio::time::timeout(self.timeout, connecting)
            .await
            .map_err(|_| -> Box<dyn std::error::Error + Send + Sync> {
                "doq connect timed out".into()
            })?
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                format!("doq connection: {e}").into()
            })
    }

    async fn get_connection(&self) -> PluginResult<quinn::Connection> {
        let mut guard = self.conn.lock().await;
        if let Some(conn) = guard.as_ref() {
            if conn.close_reason().is_none() {
                return Ok(conn.clone());
            }
            *guard = None;
        }

        let conn = self.connect().await?;
        *guard = Some(conn.clone());
        Ok(conn)
    }

    /// Drop the cached connection only if it is the one identified by `stable_id`
    /// *and* it is actually dead (the QUIC layer recorded a close reason).
    ///
    /// A single failed or timed-out stream does not imply the connection is
    /// gone — QUIC multiplexes every concurrent query onto it. Force-closing on
    /// any per-stream error would abort all sibling in-flight queries, so a live
    /// connection is deliberately left intact and only the failing query retries.
    async fn discard_dead_connection(&self, stable_id: usize) {
        let mut guard = self.conn.lock().await;
        if let Some(conn) = guard.as_ref()
            && conn.stable_id() == stable_id
            && conn.close_reason().is_some()
        {
            *guard = None;
        }
    }
}

#[async_trait]
impl Upstream for DoqUpstream {
    async fn exchange(&self, query: &[u8]) -> PluginResult<Vec<u8>> {
        // RFC 9250 §4.2.1: when sending over QUIC the DNS Message ID MUST be set
        // to 0 — the stream mapping (one query/response per bidirectional stream)
        // already correlates them, and strict servers reject a non-zero ID with a
        // stream reset. Zero it on the wire and restore the caller's original ID
        // on the response, matching what the UDP/TCP transports preserve.
        let mut wire = query.to_vec();
        if wire.len() >= 2 {
            wire[0] = 0;
            wire[1] = 0;
        }

        let mut attempt = 0;
        loop {
            let conn = self.get_connection().await?;
            let stable_id = conn.stable_id();

            let exchange = async {
                let (mut send, mut recv) = conn.open_bi().await.map_err(
                    |e| -> Box<dyn std::error::Error + Send + Sync> {
                        format!("doq open stream: {e}").into()
                    },
                )?;

                let len = wire.len() as u16;
                send.write_all(&len.to_be_bytes()).await.map_err(
                    |e| -> Box<dyn std::error::Error + Send + Sync> {
                        format!("doq write: {e}").into()
                    },
                )?;
                send.write_all(&wire).await.map_err(
                    |e| -> Box<dyn std::error::Error + Send + Sync> {
                        format!("doq write: {e}").into()
                    },
                )?;
                send.finish()
                    .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                        format!("doq finish: {e}").into()
                    })?;

                let mut len_buf = [0u8; 2];
                recv.read_exact(&mut len_buf).await.map_err(
                    |e| -> Box<dyn std::error::Error + Send + Sync> {
                        format!("doq read len: {e}").into()
                    },
                )?;
                let resp_len = u16::from_be_bytes(len_buf) as usize;
                let mut resp_buf = vec![0u8; resp_len];
                recv.read_exact(&mut resp_buf).await.map_err(
                    |e| -> Box<dyn std::error::Error + Send + Sync> {
                        format!("doq read body: {e}").into()
                    },
                )?;

                Ok::<Vec<u8>, Box<dyn std::error::Error + Send + Sync>>(resp_buf)
            };

            match tokio::time::timeout(self.timeout, exchange).await {
                Ok(Ok(mut resp)) => {
                    // Restore the caller's original transaction ID.
                    if resp.len() >= 2 && query.len() >= 2 {
                        resp[0] = query[0];
                        resp[1] = query[1];
                    }
                    return Ok(resp);
                }
                Ok(Err(e)) => {
                    self.discard_dead_connection(stable_id).await;
                    if attempt < MAX_POOL_RETRY {
                        attempt += 1;
                        continue;
                    }
                    return Err(e);
                }
                Err(_) => {
                    self.discard_dead_connection(stable_id).await;
                    if attempt < MAX_POOL_RETRY {
                        attempt += 1;
                        continue;
                    }
                    return Err("doq exchange timed out".into());
                }
            }
        }
    }
}

// ── DoH3 (DNS-over-HTTPS/3) ────────────────────────────────────

/// DNS-over-HTTPS/3 upstream using `h3` + `h3-quinn`.
///
/// Sends HTTP/3 GET requests with base64url-encoded DNS queries,
/// same as DoH (RFC 8484) but over HTTP/3 instead of HTTP/2 or HTTP/1.1.
pub struct Doh3Upstream {
    addr: SocketAddr,
    server_name: String,
    authority: String,
    path_prefix: String,
    /// Request path (with any original query string) used for POST, without the
    /// `?dns=` GET parameter.
    post_path: String,
    timeout: Duration,
    endpoint: quinn::Endpoint,
    client_config: quinn::ClientConfig,
    session: Mutex<Option<Arc<Doh3Session>>>,
}

impl Doh3Upstream {
    fn new(endpoint_url: String, addr: SocketAddr, server_name: String) -> PluginResult<Self> {
        let client_config = Self::build_client_config();
        let endpoint = shared_quic_endpoint(addr)?;

        let url = reqwest::Url::parse(&endpoint_url).map_err(
            |e| -> Box<dyn std::error::Error + Send + Sync> {
                format!("invalid h3 endpoint url '{endpoint_url}': {e}").into()
            },
        )?;
        let authority = if let Some(port) = url.port() {
            if port == 443 {
                server_name.clone()
            } else {
                format!("{}:{}", server_name, port)
            }
        } else {
            server_name.clone()
        };

        let mut path_prefix = url.path().to_string();
        if let Some(q) = url.query() {
            if q.is_empty() {
                path_prefix.push('?');
            } else {
                path_prefix.push('?');
                path_prefix.push_str(q);
                path_prefix.push('&');
            }
        } else {
            path_prefix.push('?');
        }

        // POST targets the bare path plus any original (non-`dns`) query string.
        let post_path = match url.query() {
            Some(q) if !q.is_empty() => format!("{}?{}", url.path(), q),
            _ => url.path().to_string(),
        };

        Ok(Self {
            addr,
            server_name,
            authority,
            path_prefix,
            post_path,
            timeout: DEFAULT_TIMEOUT,
            endpoint,
            client_config,
            session: Mutex::new(None),
        })
    }

    fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    fn build_client_config() -> quinn::ClientConfig {
        let mut tls_config = tokio_rustls::rustls::ClientConfig::builder()
            .with_root_certificates(shared_root_store())
            .with_no_client_auth();
        tls_config.alpn_protocols = vec![b"h3".to_vec()];
        let quic_config = quinn::crypto::rustls::QuicClientConfig::try_from(tls_config)
            .expect("failed to create QUIC client config");
        let mut client_config = quinn::ClientConfig::new(Arc::new(quic_config));
        client_config.transport_config(quic_keepalive_transport());
        client_config
    }

    async fn connect_quic(&self) -> PluginResult<quinn::Connection> {
        let connecting = self
            .endpoint
            .connect_with(self.client_config.clone(), self.addr, &self.server_name)
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                format!("h3 connect: {e}").into()
            })?;

        tokio::time::timeout(self.timeout, connecting)
            .await
            .map_err(|_| -> Box<dyn std::error::Error + Send + Sync> {
                "h3 connect timed out".into()
            })?
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                format!("h3 connection: {e}").into()
            })
    }

    async fn create_session(&self) -> PluginResult<Arc<Doh3Session>> {
        let quinn_conn = self.connect_quic().await?;
        let h3_conn = h3_quinn::Connection::new(quinn_conn.clone());
        let (mut driver, send_request) = h3::client::new(h3_conn).await.map_err(
            |e| -> Box<dyn std::error::Error + Send + Sync> {
                format!("h3 client init: {e}").into()
            },
        )?;

        let driver = tokio::spawn(async move {
            let _ = std::future::poll_fn(|cx| driver.poll_close(cx)).await;
        });

        Ok(Arc::new(Doh3Session {
            quinn_conn,
            send_request,
            driver,
        }))
    }

    fn clear_session_locked(session: &mut Option<Arc<Doh3Session>>) {
        if let Some(old) = session.take() {
            old.quinn_conn
                .close(quinn::VarInt::from_u32(0), b"reconnect");
            old.driver.abort();
        }
    }

    async fn get_session(&self) -> PluginResult<Arc<Doh3Session>> {
        let mut guard = self.session.lock().await;
        if let Some(session) = guard.as_ref()
            && session.is_healthy()
        {
            return Ok(session.clone());
        }
        Self::clear_session_locked(&mut guard);

        let session = self.create_session().await?;
        *guard = Some(session.clone());
        Ok(session)
    }

    /// Clear the cached session only if it is the one identified by `stable_id`
    /// *and* it is no longer healthy (its driver finished or the QUIC connection
    /// recorded a close reason). A single failed request — an `RST_STREAM`, a
    /// per-request timeout — must not tear down the shared H3 connection that
    /// sibling in-flight requests are multiplexed on; only a genuinely dead
    /// connection is replaced.
    async fn discard_dead_session(&self, stable_id: usize) {
        let mut guard = self.session.lock().await;
        if let Some(session) = guard.as_ref()
            && session.quinn_conn.stable_id() == stable_id
            && !session.is_healthy()
        {
            Self::clear_session_locked(&mut guard);
        }
    }
}

#[async_trait]
impl Upstream for Doh3Upstream {
    async fn exchange(&self, query: &[u8]) -> PluginResult<Vec<u8>> {
        use base64::Engine;

        // Zero the ID for HTTP cache friendliness (RFC 8484 §4.1).
        let mut wire = query.to_vec();
        if wire.len() >= 2 {
            wire[0] = 0;
            wire[1] = 0;
        }

        let encoded = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&wire);
        let get_url = format!(
            "https://{}{}dns={}",
            self.authority, self.path_prefix, encoded
        );

        // RFC 8484 §4.1: prefer GET, but fall back to POST when the base64url
        // query would push the URL past what intermediaries reliably accept.
        // POST carries the raw wire query in the request body instead (parity
        // with the HTTP/2 DoH transport).
        let use_post = get_url.len() > DOH_MAX_GET_URL_LEN;
        let request_url = if use_post {
            format!("https://{}{}", self.authority, self.post_path)
        } else {
            get_url
        };
        let body = bytes::Bytes::from(wire);

        let mut attempt = 0;
        loop {
            let session = self.get_session().await?;
            let stable_id = session.quinn_conn.stable_id();
            let mut send_request = session.send_request.clone();

            let builder = if use_post {
                http::Request::post(request_url.as_str())
                    .header("content-type", "application/dns-message")
            } else {
                http::Request::get(request_url.as_str())
            };
            let req = builder
                .header("accept", "application/dns-message")
                .header("user-agent", DOH_USER_AGENT)
                .body(())
                .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                    format!("h3 build request: {e}").into()
                })?;

            let mut resp_stream =
                match tokio::time::timeout(self.timeout, send_request.send_request(req)).await {
                    Ok(Ok(stream)) => stream,
                    Ok(Err(e)) => {
                        self.discard_dead_session(stable_id).await;
                        if attempt < MAX_POOL_RETRY {
                            attempt += 1;
                            continue;
                        }
                        return Err(format!("h3 send request: {e}").into());
                    }
                    Err(_) => {
                        self.discard_dead_session(stable_id).await;
                        if attempt < MAX_POOL_RETRY {
                            attempt += 1;
                            continue;
                        }
                        return Err("h3 request timed out".into());
                    }
                };

            // h3 does not finalize the send side implicitly: send the POST body
            // (if any) and then `finish()` to signal request completion. Without
            // the finish the server may wait indefinitely for more request data.
            let send_body = async {
                if use_post {
                    resp_stream.send_data(body.clone()).await?;
                }
                resp_stream.finish().await
            };
            match tokio::time::timeout(self.timeout, send_body).await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    self.discard_dead_session(stable_id).await;
                    if attempt < MAX_POOL_RETRY {
                        attempt += 1;
                        continue;
                    }
                    return Err(format!("h3 send body: {e}").into());
                }
                Err(_) => {
                    self.discard_dead_session(stable_id).await;
                    if attempt < MAX_POOL_RETRY {
                        attempt += 1;
                        continue;
                    }
                    return Err("h3 send body timed out".into());
                }
            }

            let resp = match tokio::time::timeout(self.timeout, resp_stream.recv_response()).await {
                Ok(Ok(resp)) => resp,
                Ok(Err(e)) => {
                    self.discard_dead_session(stable_id).await;
                    if attempt < MAX_POOL_RETRY {
                        attempt += 1;
                        continue;
                    }
                    return Err(format!("h3 recv response: {e}").into());
                }
                Err(_) => {
                    self.discard_dead_session(stable_id).await;
                    if attempt < MAX_POOL_RETRY {
                        attempt += 1;
                        continue;
                    }
                    return Err("h3 response timed out".into());
                }
            };

            if !resp.status().is_success() {
                return Err(format!("h3: bad status {}", resp.status()).into());
            }

            // RFC 8484 §4.2: the response MUST be application/dns-message.
            let content_type = resp
                .headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");
            if !content_type
                .to_ascii_lowercase()
                .contains("application/dns-message")
            {
                return Err(format!(
                    "h3: unexpected content-type '{content_type}' (want application/dns-message)"
                )
                .into());
            }

            let mut body_bytes = Vec::new();
            let mut read_failed = None;
            loop {
                match tokio::time::timeout(self.timeout, resp_stream.recv_data()).await {
                    Ok(Ok(Some(mut chunk))) => {
                        // `recv_data` yields an opaque `Buf`; copy *all* remaining
                        // bytes (not just the first contiguous slice) so a
                        // multi-chunk buffer is not silently truncated.
                        let remaining = bytes::Buf::remaining(&chunk);
                        body_bytes
                            .extend_from_slice(&bytes::Buf::copy_to_bytes(&mut chunk, remaining));
                        if body_bytes.len() > MAX_DNS_RESPONSE {
                            read_failed =
                                Some(format!("h3 response exceeds {MAX_DNS_RESPONSE} bytes").into());
                            break;
                        }
                    }
                    Ok(Ok(None)) => break,
                    Ok(Err(e)) => {
                        read_failed = Some(format!("h3 recv body: {e}").into());
                        break;
                    }
                    Err(_) => {
                        read_failed = Some("h3 recv body timed out".into());
                        break;
                    }
                }
            }

            if let Some(err) = read_failed {
                self.discard_dead_session(stable_id).await;
                if attempt < MAX_POOL_RETRY {
                    attempt += 1;
                    continue;
                }
                return Err(err);
            }

            if body_bytes.len() >= 2 && query.len() >= 2 {
                body_bytes[0] = query[0];
                body_bytes[1] = query[1];
            }

            return Ok(body_bytes);
        }
    }
}

// ── Upstream Wrapper with Latency/Error Tracking ────────────────

/// A point-in-time snapshot of per-upstream metrics.
#[derive(Debug, Clone, serde::Serialize)]
pub struct UpstreamMetrics {
    pub name: String,
    pub protocol: String,
    pub query_total: u64,
    pub completed_total: u64,
    pub inflight_total: u64,
    pub canceled_total: u64,
    pub adopted_total: u64,
    pub final_selected_total: u64,
    pub rejected_rcode_total: u64,
    pub error_total: u64,
    pub avg_latency_ms: f64,
}

/// Wraps an upstream transport with per-upstream latency and error tracking.
pub struct UpstreamWrapper {
    inner: Box<dyn Upstream>,
    name: String,
    protocol: String,
    ema_latency_ms: AtomicI64,
    query_count: AtomicU64,
    inflight_count: AtomicU64,
    completed_count: AtomicU64,
    error_count: AtomicU64,
    consecutive_failures: AtomicU32,
    adopted_count: AtomicU64,
    final_selected_count: AtomicU64,
    rejected_rcode_count: AtomicU64,
    latency_sum_us: AtomicU64,
}

/// Returns the global average upstream latency across all completed exchanges.
pub fn global_average_latency() -> Option<Duration> {
    let completed = GLOBAL_COMPLETED_TOTAL.load(Ordering::Relaxed);
    if completed == 0 {
        return None;
    }
    let sum_us = GLOBAL_LATENCY_SUM_US.load(Ordering::Relaxed);
    Some(Duration::from_micros(sum_us / completed))
}

impl UpstreamWrapper {
    /// EMA smoothing factor.
    const ALPHA: f64 = 0.3;

    pub fn new(inner: Box<dyn Upstream>, name: String, protocol: String) -> Self {
        Self {
            inner,
            name,
            protocol,
            ema_latency_ms: AtomicI64::new(0),
            query_count: AtomicU64::new(0),
            inflight_count: AtomicU64::new(0),
            completed_count: AtomicU64::new(0),
            error_count: AtomicU64::new(0),
            consecutive_failures: AtomicU32::new(0),
            adopted_count: AtomicU64::new(0),
            final_selected_count: AtomicU64::new(0),
            rejected_rcode_count: AtomicU64::new(0),
            latency_sum_us: AtomicU64::new(0),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn protocol(&self) -> &str {
        &self.protocol
    }
    pub fn ema_latency(&self) -> i64 {
        self.ema_latency_ms.load(Ordering::Relaxed)
    }
    pub fn query_count(&self) -> u64 {
        self.query_count.load(Ordering::Relaxed)
    }
    pub fn completed_count(&self) -> u64 {
        self.completed_count.load(Ordering::Relaxed)
    }
    pub fn inflight_count(&self) -> u64 {
        self.inflight_count.load(Ordering::Relaxed)
    }
    pub fn error_count(&self) -> u64 {
        self.error_count.load(Ordering::Relaxed)
    }
    pub fn consecutive_failures(&self) -> u32 {
        self.consecutive_failures.load(Ordering::Relaxed)
    }

    pub fn error_rate(&self) -> f64 {
        let q = self.completed_count();
        if q == 0 {
            return 0.0;
        }
        self.error_count() as f64 / q as f64
    }

    /// Record that this upstream's response was adopted by the Forward plugin.
    pub fn record_adopted(&self) {
        self.adopted_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Record that this upstream ended up being returned to the client.
    pub fn record_final_selected(&self) {
        self.final_selected_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Record that this upstream responded but was skipped due to RCODE policy.
    pub fn record_rejected_rcode(&self) {
        self.rejected_rcode_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Returns a point-in-time snapshot of this upstream's metrics.
    pub fn snapshot(&self) -> UpstreamMetrics {
        let query_total = self.query_count.load(Ordering::Relaxed);
        let completed_total = self.completed_count.load(Ordering::Relaxed);
        let inflight_total = self.inflight_count.load(Ordering::Relaxed);
        let latency_sum = self.latency_sum_us.load(Ordering::Relaxed);
        UpstreamMetrics {
            name: self.name.clone(),
            protocol: self.protocol.clone(),
            query_total,
            completed_total,
            inflight_total,
            canceled_total: query_total
                .saturating_sub(completed_total.saturating_add(inflight_total)),
            adopted_total: self.adopted_count.load(Ordering::Relaxed),
            final_selected_total: self.final_selected_count.load(Ordering::Relaxed),
            rejected_rcode_total: self.rejected_rcode_count.load(Ordering::Relaxed),
            error_total: self.error_count.load(Ordering::Relaxed),
            avg_latency_ms: if completed_total > 0 {
                (latency_sum as f64 / completed_total as f64) / 1000.0
            } else {
                0.0
            },
        }
    }

    pub async fn probe(&self) {
        self.inner.probe().await;
    }

    pub async fn exchange(&self, query: &[u8]) -> PluginResult<Vec<u8>> {
        self.query_count.fetch_add(1, Ordering::Relaxed);
        self.inflight_count.fetch_add(1, Ordering::Relaxed);
        struct InflightGuard<'a>(&'a AtomicU64);
        impl Drop for InflightGuard<'_> {
            fn drop(&mut self) {
                self.0.fetch_sub(1, Ordering::Relaxed);
            }
        }
        let _inflight_guard = InflightGuard(&self.inflight_count);

        let start = std::time::Instant::now();
        let result = self.inner.exchange(query).await;
        let elapsed = start.elapsed();
        let elapsed_ms = elapsed.as_millis() as i64;
        self.completed_count.fetch_add(1, Ordering::Relaxed);
        self.latency_sum_us
            .fetch_add(elapsed.as_micros() as u64, Ordering::Relaxed);
        GLOBAL_COMPLETED_TOTAL.fetch_add(1, Ordering::Relaxed);
        GLOBAL_LATENCY_SUM_US.fetch_add(elapsed.as_micros() as u64, Ordering::Relaxed);

        // Feed the EMA latency on both success and failure. A failure — most
        // importantly a timeout — burns real wall-clock time, so folding it into
        // the latency signal lets the selector's score penalize a chronically
        // slow/failing upstream on latency too, not just via `error_rate`.
        // Without this, a timing-out upstream keeps a stale, misleadingly-low
        // EMA (it only ever recorded its fast successes) and keeps getting picked.
        self.update_ema_latency(elapsed_ms);
        if result.is_err() {
            self.error_count.fetch_add(1, Ordering::Relaxed);
            self.consecutive_failures.fetch_add(1, Ordering::Relaxed);
            warn!(upstream = %self.name, error = %result.as_ref().unwrap_err(), "upstream exchange failed");
        } else {
            self.consecutive_failures.store(0, Ordering::Relaxed);
        }
        result
    }

    fn update_ema_latency(&self, latency_ms: i64) {
        let current = self.ema_latency_ms.load(Ordering::Relaxed);
        if current == 0 {
            self.ema_latency_ms.store(latency_ms, Ordering::Relaxed);
        } else {
            let new_val =
                ((current as f64) * (1.0 - Self::ALPHA) + (latency_ms as f64) * Self::ALPHA) as i64;
            self.ema_latency_ms.store(new_val, Ordering::Relaxed);
        }
    }
}

// ── Factory ─────────────────────────────────────────────────────

/// Upstream configuration options.
pub struct UpstreamOpts {
    pub timeout: Duration,
    /// Direct IP:port to connect to, bypassing DNS resolution.
    pub dial_addr: Option<SocketAddr>,
    /// Bootstrap DNS server address for resolving the upstream hostname.
    pub bootstrap: Option<String>,
    /// Maximum idle transport resources to retain per upstream.
    pub pool_max_idle: Option<usize>,
}

impl Default for UpstreamOpts {
    fn default() -> Self {
        Self {
            timeout: DEFAULT_TIMEOUT,
            dial_addr: None,
            bootstrap: None,
            pool_max_idle: None,
        }
    }
}

/// Parses a URL string and creates the appropriate upstream.
///
/// Supported schemes:
/// - `udp://host:port` — UDP
/// - `tcp://host:port` — TCP with connection pooling
/// - `tls://host:port` — DNS-over-TLS with connection pooling + session cache
/// - `https://host/path` — DNS-over-HTTPS (RFC 8484)
/// - `host:port` — defaults to UDP
///
/// Normalize an address string by appending the default port if missing.
/// Handles bare IPs like "202.96.128.86" → "202.96.128.86:53",
/// IPv6 like "[::1]" → "[::1]:53", and already-ported "1.1.1.1:53" → unchanged.
fn normalize_addr(addr: &str, default_port: u16) -> String {
    // If it parses as a SocketAddr already, it has a port.
    if addr.parse::<SocketAddr>().is_ok() {
        return addr.to_string();
    }
    // If it parses as a bare IP, append default port.
    if let Ok(ip) = addr.parse::<std::net::IpAddr>() {
        return format!("{}:{}", ip, default_port);
    }
    // Host:port or hostname — check if last colon is followed by digits.
    if let Some((_, port_str)) = addr.rsplit_once(':')
        && port_str.parse::<u16>().is_ok()
    {
        return addr.to_string(); // Already has port.
    }
    // Append default port.
    format!("{}:{}", addr, default_port)
}

// ── Bootstrap Resolver (TTL-aware) ──────────────────────────────

/// Cached bootstrap resolution: resolved addresses and their shared expiry.
type BootstrapCache = Arc<StdMutex<Option<(Vec<SocketAddr>, Instant)>>>;

/// A custom DNS resolver for reqwest that resolves a specific hostname
/// via a bootstrap DNS server, caching the result according to DNS TTL.
///
/// This resolver is used for DoH upstreams when the user specifies a `bootstrap`
/// server. It prevents recursion by requiring the bootstrap itself to be IP-based,
/// and respects DNS TTL for cache freshness (clamped to 60–3600 seconds).
///
/// For hostnames other than the target, falls back to the system resolver.
struct BootstrapResolver {
    target_host: String,
    bootstrap: String,
    cache: BootstrapCache,
    /// Serializes cache refreshes so a burst of concurrent misses collapses into
    /// a single bootstrap query (single-flight) instead of one query per request.
    refresh_lock: Arc<Mutex<()>>,
    port: u16,
}

impl BootstrapResolver {
    fn new(target_host: String, bootstrap: String, port: u16) -> Self {
        Self {
            target_host,
            bootstrap,
            cache: Arc::new(StdMutex::new(None)),
            refresh_lock: Arc::new(Mutex::new(())),
            port,
        }
    }
}

impl reqwest::dns::Resolve for BootstrapResolver {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        let name_str = name.as_str().to_string();

        // For names other than our target, fall back to system resolver.
        if name_str != self.target_host {
            return Box::pin(async move {
                use std::net::ToSocketAddrs;
                let addrs: Vec<SocketAddr> = format!("{}:0", name_str)
                    .to_socket_addrs()
                    .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                        format!("resolve {}: {}", name_str, e).into()
                    })?
                    .collect();
                Ok(Box::new(addrs.into_iter()) as Box<dyn Iterator<Item = SocketAddr> + Send>)
            });
        }

        // Fast path: serve every cached address while the entry is fresh.
        {
            let guard = self.cache.lock();
            if let Some((addrs, expiry)) = &*guard
                && Instant::now() < *expiry
            {
                let addrs = addrs.clone();
                return Box::pin(async move {
                    Ok(Box::new(addrs.into_iter()) as Box<dyn Iterator<Item = SocketAddr> + Send>)
                });
            }
        }

        let target_host = self.target_host.clone();
        let bootstrap = self.bootstrap.clone();
        let port = self.port;
        // Clone the Arcs so the cache and refresh lock outlive this future
        // independently of the resolver. reqwest stores the resolver behind an Arc
        // and may poll this future after dropping its handle, so capturing a
        // borrow would be unsound; owned Arcs are both safe and cheap.
        let cache = Arc::clone(&self.cache);
        let refresh_lock = Arc::clone(&self.refresh_lock);

        Box::pin(async move {
            // Single-flight: only the lock holder resolves; concurrent misses
            // wait here and then observe the cache the winner just populated.
            let _refresh = refresh_lock.lock().await;

            // Re-check under the refresh lock — another task may have refreshed
            // while we waited.
            {
                let guard = cache.lock();
                if let Some((addrs, expiry)) = &*guard
                    && Instant::now() < *expiry
                {
                    let addrs = addrs.clone();
                    return Ok(
                        Box::new(addrs.into_iter()) as Box<dyn Iterator<Item = SocketAddr> + Send>
                    );
                }
            }

            let (ips, ttl) = bootstrap_resolve(&target_host, &bootstrap).await?;
            let addrs: Vec<SocketAddr> =
                ips.iter().map(|ip| SocketAddr::new(*ip, port)).collect();

            {
                let mut guard = cache.lock();
                *guard = Some((addrs.clone(), Instant::now() + ttl));
            }

            Ok(Box::new(addrs.into_iter()) as Box<dyn Iterator<Item = SocketAddr> + Send>)
        })
    }
}

// ── Upstream Host Resolution (DoT, DoQ, etc.) ───────────────────

/// Resolves a hostname for socket-based upstreams (DoT, DoQ).
///
/// ## Resolution Precedence
///
/// 1. **IP address** — If `host` is already an IP, no resolution needed.
/// 2. **`dial_addr`** — Static pinning to a fixed address (highest precedence).
/// 3. **`bootstrap`** — Resolve via a specific DNS server.
/// 4. **Error** — Unresolved domains without dial_addr/bootstrap are rejected.
///
/// ## Examples
///
/// ```rust,ignore
/// // IP-based — no resolution needed
/// resolve_upstream_host("1.1.1.1", 853, &opts)?; // → 1.1.1.1:853
///
/// // Domain with dial_addr — static pin
/// let opts = UpstreamOpts {
///     dial_addr: Some("1.1.1.1:853".parse().unwrap()),
///     ..Default::default()
/// };
/// resolve_upstream_host("one.one.one.one", 853, &opts)?; // → 1.1.1.1:853
///
/// // Domain with bootstrap — DNS resolution
/// let opts = UpstreamOpts {
///     bootstrap: Some("8.8.8.8:53".to_string()),
///     ..Default::default()
/// };
/// resolve_upstream_host("dns.google", 853, &opts)?; // → resolves via 8.8.8.8
/// ```
fn resolve_upstream_host(host: &str, port: u16, opts: &UpstreamOpts) -> PluginResult<SocketAddr> {
    // If host is already an IP, no resolution needed.
    if let Ok(ip) = host.parse::<std::net::IpAddr>() {
        return Ok(SocketAddr::new(ip, port));
    }

    // 1. dial_addr takes highest precedence.
    if let Some(addr) = opts.dial_addr {
        return Ok(addr);
    }

    // 2. Bootstrap — resolve via specific DNS.
    if let Some(ref bootstrap) = opts.bootstrap {
        let (ips, ttl) = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(bootstrap_resolve(host, bootstrap))
        })?;
        // Socket upstreams connect to a single address; use the first resolved IP.
        // `bootstrap_resolve` guarantees at least one on success.
        let ip = ips[0];
        tracing::info!(
            host = %host, resolved = %ip, ttl = ?ttl, bootstrap = %bootstrap,
            "upstream host resolved via bootstrap"
        );
        return Ok(SocketAddr::new(ip, port));
    }

    // 3. Neither — error.
    Err(format!(
        "upstream host '{}' is an unresolved domain: \
         set 'dial_addr' (direct IP) or 'bootstrap' (DNS server for resolution)",
        host
    )
    .into())
}

// ── DoH Host Resolution ─────────────────────────────────────────

/// Resolves a DoH endpoint hostname.
///
/// ## Resolution Precedence
///
/// 1. **IP address** — If the host in the URL is already an IP, no resolution needed.
/// 2. **`dial_addr`** — Static pinning to a fixed address.
/// 3. **`bootstrap`** — TTL-aware DNS resolver with caching.
/// 4. **Error** — Unresolved domains without dial_addr/bootstrap are rejected.
///
/// The bootstrap resolver performs an initial resolution at startup to fail fast,
/// then caches results according to DNS TTL (clamped to 60–3600 seconds).
fn resolve_doh_host(endpoint: &str, opts: &UpstreamOpts) -> PluginResult<DohResolution> {
    let url =
        reqwest::Url::parse(endpoint).map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
            format!("invalid DoH endpoint URL '{}': {}", endpoint, e).into()
        })?;
    let host = url
        .host_str()
        .ok_or_else(|| -> Box<dyn std::error::Error + Send + Sync> {
            format!("DoH URL '{}' has no host", endpoint).into()
        })?;
    let port = url.port_or_known_default().unwrap_or(443);

    // If the host is already an IP, no resolution needed.
    if host.parse::<std::net::IpAddr>().is_ok() {
        return Ok(DohResolution::Ip);
    }

    // 1. dial_addr takes highest precedence — static pin.
    if let Some(addr) = opts.dial_addr {
        return Ok(DohResolution::StaticAddr(addr));
    }

    // 2. Bootstrap — TTL-aware resolver.
    if let Some(ref bootstrap) = opts.bootstrap {
        // Do an initial resolve to fail fast at startup.
        let (ips, ttl) = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(bootstrap_resolve(host, bootstrap))
        })?;
        tracing::info!(
            host = %host, resolved = ?ips, ttl = ?ttl, bootstrap = %bootstrap,
            "DoH upstream resolved via bootstrap"
        );

        let resolver = Arc::new(BootstrapResolver::new(
            host.to_string(),
            bootstrap.to_string(),
            port,
        ));
        {
            let addrs: Vec<SocketAddr> =
                ips.iter().map(|ip| SocketAddr::new(*ip, port)).collect();
            let mut cache = resolver.cache.lock();
            *cache = Some((addrs, Instant::now() + ttl));
        }

        return Ok(DohResolution::Bootstrap(resolver));
    }

    // 3. Neither dial_addr nor bootstrap — error.
    Err(format!(
        "DoH upstream '{}' has an unresolved domain '{}': \
         set 'dial_addr' (direct IP) or 'bootstrap' (DNS server for resolution)",
        endpoint, host
    )
    .into())
}

/// Validates that a bootstrap upstream URL is IP-based (no unresolved domain).
///
/// Prevents recursion: a bootstrap target must not itself need DNS resolution.
fn validate_bootstrap_is_ip_based(bootstrap: &str) -> PluginResult<()> {
    // Extract the host from the bootstrap URL.
    let host = if bootstrap.contains("://") {
        // URL-style: https://1.1.1.1/dns-query, tls://1.1.1.1:853, etc.
        if let Ok(url) = reqwest::Url::parse(bootstrap) {
            url.host_str().unwrap_or("").to_string()
        } else {
            // Try host:port parsing for non-URL formats like tls://host:port
            let after_scheme = bootstrap.split("://").last().unwrap_or(bootstrap);
            let host_part = after_scheme.split(':').next().unwrap_or(after_scheme);
            host_part.to_string()
        }
    } else {
        // Bare address: 8.8.8.8 or 8.8.8.8:53
        bootstrap.split(':').next().unwrap_or(bootstrap).to_string()
    };

    if host.parse::<std::net::IpAddr>().is_ok() {
        Ok(())
    } else {
        Err(format!(
            "bootstrap '{}' contains an unresolved domain '{}': \
             bootstrap must use an IP address to avoid DNS loops",
            bootstrap, host
        )
        .into())
    }
}

/// Resolves a hostname using a bootstrap upstream.
///
/// Creates a temporary upstream from the bootstrap URL, builds a DNS A query,
/// sends it via `exchange()`, and extracts *all* A/AAAA records with a single
/// clamped TTL (the minimum across records). Returning every address lets the
/// caller fail over to another IP if the first is unreachable.
/// Supports any protocol: UDP, TCP, TLS, DoH, DoQ, DoH3.
async fn bootstrap_resolve(
    hostname: &str,
    bootstrap: &str,
) -> Result<(Vec<std::net::IpAddr>, Duration), Box<dyn std::error::Error + Send + Sync>> {
    use hickory_proto::op::{Message, MessageType, OpCode, Query};
    use hickory_proto::rr::{Name, RData, RecordType};

    // Validate bootstrap is IP-based to prevent recursion.
    validate_bootstrap_is_ip_based(bootstrap)?;

    // Create a temporary upstream with no dial_addr/bootstrap (IP-based, no recursion).
    let upstream = new_upstream(bootstrap, UpstreamOpts::default())?;

    let name =
        Name::from_ascii(hostname).map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
            format!("invalid hostname '{}': {}", hostname, e).into()
        })?;

    let mut query_msg = Message::new(
        std::process::id() as u16,
        MessageType::Query,
        OpCode::Query,
    );
    query_msg.metadata.recursion_desired = true;
    let mut q = Query::new();
    q.set_name(name).set_query_type(RecordType::A);
    query_msg.add_query(q);

    let wire = query_msg
        .to_vec()
        .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
            format!("failed to serialize DNS query: {}", e).into()
        })?;

    let resp_wire = upstream.exchange(&wire).await?;

    let resp =
        Message::from_vec(&resp_wire).map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
            format!("invalid bootstrap DNS response: {}", e).into()
        })?;

    let mut ips = Vec::new();
    let mut min_ttl = u32::MAX;
    for answer in &resp.answers {
        match answer.data {
            RData::A(a) => {
                ips.push(std::net::IpAddr::V4(a.0));
                min_ttl = min_ttl.min(answer.ttl);
            }
            RData::AAAA(aaaa) => {
                ips.push(std::net::IpAddr::V6(aaaa.0));
                min_ttl = min_ttl.min(answer.ttl);
            }
            _ => continue,
        }
    }

    if ips.is_empty() {
        return Err(format!(
            "bootstrap DNS returned no A/AAAA records for '{}'",
            hostname
        )
        .into());
    }

    // Clamp TTL: min 60s, max 3600s.
    let ttl = Duration::from_secs(min_ttl as u64)
        .max(Duration::from_secs(60))
        .min(Duration::from_secs(3600));

    Ok((ips, ttl))
}

pub fn new_upstream(addr: &str, opts: UpstreamOpts) -> PluginResult<Box<dyn Upstream>> {
    let udp_pool_max_idle = opts.pool_max_idle.unwrap_or(DEFAULT_MAX_IDLE_UDP_SOCKETS);
    let stream_pool_max_idle = opts.pool_max_idle.unwrap_or(DEFAULT_MAX_IDLE_CONNS);

    if let Some(rest) = addr.strip_prefix("udp://") {
        let normalized = normalize_addr(rest, 53);
        let socket_addr: SocketAddr =
            normalized
                .parse()
                .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                    format!("invalid address {rest}: {e}").into()
                })?;
        Ok(Box::new(
            UdpUpstream::new(socket_addr)
                .with_timeout(opts.timeout)
                .with_max_idle_sockets(udp_pool_max_idle),
        ))
    } else if let Some(rest) = addr.strip_prefix("tcp://") {
        let normalized = normalize_addr(rest, 53);
        let socket_addr: SocketAddr =
            normalized
                .parse()
                .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                    format!("invalid address {rest}: {e}").into()
                })?;
        Ok(Box::new(
            PooledTcpUpstream::new(socket_addr)
                .with_timeout(opts.timeout)
                .with_max_idle_conns(stream_pool_max_idle),
        ))
    } else if let Some(rest) = addr.strip_prefix("tls://") {
        let normalized = normalize_addr(rest, 853);
        let (host, port_str) = normalized.rsplit_once(':').ok_or_else(
            || -> Box<dyn std::error::Error + Send + Sync> {
                format!("tls address must be host:port, got: {rest}").into()
            },
        )?;
        let port: u16 =
            port_str
                .parse()
                .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                    format!("invalid port: {e}").into()
                })?;

        let socket_addr = resolve_upstream_host(host, port, &opts)?;

        Ok(Box::new(
            PipelinedTlsUpstream::new(socket_addr, host.to_string())
                .with_timeout(opts.timeout)
                .with_max_idle_conns(stream_pool_max_idle),
        ))
    } else if addr.starts_with("https://") {
        let resolution = resolve_doh_host(addr, &opts)?;
        Ok(Box::new(DohUpstream::new(
            addr.to_string(),
            resolution,
            stream_pool_max_idle,
            opts.timeout,
        )))
    } else if let Some(rest) = addr.strip_prefix("quic://") {
        let normalized = normalize_addr(rest, 853);
        let (host, port_str) = normalized.rsplit_once(':').ok_or_else(
            || -> Box<dyn std::error::Error + Send + Sync> {
                format!("quic address must be host:port, got: {rest}").into()
            },
        )?;
        let port: u16 =
            port_str
                .parse()
                .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                    format!("invalid port: {e}").into()
                })?;

        let socket_addr = resolve_upstream_host(host, port, &opts)?;

        Ok(Box::new(
            DoqUpstream::new(socket_addr, host.to_string())?.with_timeout(opts.timeout),
        ))
    } else if let Some(rest) = addr.strip_prefix("h3://") {
        // h3://host/path → uses HTTPS URL internally but HTTP/3 transport
        let https_url = format!("https://{}", rest);
        let url = reqwest::Url::parse(&https_url).map_err(
            |e| -> Box<dyn std::error::Error + Send + Sync> {
                format!("invalid h3 URL '{}': {}", addr, e).into()
            },
        )?;
        let host = url
            .host_str()
            .ok_or_else(|| -> Box<dyn std::error::Error + Send + Sync> {
                format!("h3 URL '{}' has no host", addr).into()
            })?;
        let port = url.port_or_known_default().unwrap_or(443);

        let socket_addr = resolve_upstream_host(host, port, &opts)?;

        Ok(Box::new(
            Doh3Upstream::new(https_url, socket_addr, host.to_string())?
                .with_timeout(opts.timeout),
        ))
    } else {
        // Default to UDP.
        let normalized = normalize_addr(addr, 53);
        let socket_addr: SocketAddr =
            normalized
                .parse()
                .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                    format!("invalid address {addr}: {e}").into()
                })?;
        Ok(Box::new(
            UdpUpstream::new(socket_addr)
                .with_timeout(opts.timeout)
                .with_max_idle_sockets(udp_pool_max_idle),
        ))
    }
}

pub fn upstream_protocol_label(addr: &str) -> &'static str {
    if addr.strip_prefix("udp://").is_some() {
        "UDP"
    } else if addr.strip_prefix("tcp://").is_some() {
        "TCP"
    } else if addr.strip_prefix("tls://").is_some() {
        "DoT"
    } else if addr.starts_with("https://") {
        "DoH"
    } else if addr.strip_prefix("quic://").is_some() {
        "DoQ"
    } else if addr.strip_prefix("h3://").is_some() {
        "DoH3"
    } else {
        "UDP"
    }
}

/// Creates an `UpstreamWrapper` from an address string.
pub fn new_wrapped_upstream(addr: &str, opts: UpstreamOpts) -> PluginResult<UpstreamWrapper> {
    let inner = new_upstream(addr, opts)?;
    let name = addr.to_string();
    Ok(UpstreamWrapper::new(
        inner,
        name,
        upstream_protocol_label(addr).to_string(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_udp_upstream() {
        let u = new_upstream("udp://8.8.8.8:53", UpstreamOpts::default());
        assert!(u.is_ok());
    }

    #[test]
    fn parse_tcp_upstream() {
        let u = new_upstream("tcp://8.8.8.8:53", UpstreamOpts::default());
        assert!(u.is_ok());
    }

    #[test]
    fn parse_bare_addr_defaults_udp() {
        let u = new_upstream("8.8.8.8:53", UpstreamOpts::default());
        assert!(u.is_ok());
    }

    #[test]
    fn upstream_protocol_labels_match_supported_transports() {
        assert_eq!(upstream_protocol_label("udp://8.8.8.8:53"), "UDP");
        assert_eq!(upstream_protocol_label("tcp://8.8.8.8:53"), "TCP");
        assert_eq!(upstream_protocol_label("tls://1.1.1.1:853"), "DoT");
        assert_eq!(
            upstream_protocol_label("https://dns.google/dns-query"),
            "DoH"
        );
        assert_eq!(
            upstream_protocol_label("quic://dns.adguard-dns.com:853"),
            "DoQ"
        );
        assert_eq!(
            upstream_protocol_label("h3://dns.example/dns-query"),
            "DoH3"
        );
        assert_eq!(upstream_protocol_label("8.8.8.8:53"), "UDP");
    }

    #[test]
    fn parse_doh_upstream_ip_url() {
        // IP-based DoH URL needs no dial_addr/bootstrap.
        let u = new_upstream("https://1.1.1.1/dns-query", UpstreamOpts::default());
        assert!(u.is_ok());
    }

    #[test]
    fn parse_doh_upstream_domain_requires_config() {
        // Domain-based DoH URL without dial_addr/bootstrap must error.
        let u = new_upstream("https://dns.google/dns-query", UpstreamOpts::default());
        assert!(u.is_err());
    }

    #[test]
    fn parse_doh_upstream_with_dial_addr() {
        let opts = UpstreamOpts {
            dial_addr: Some("8.8.8.8:443".parse().unwrap()),
            ..Default::default()
        };
        let u = new_upstream("https://dns.google/dns-query", opts);
        assert!(u.is_ok());
    }

    #[test]
    fn pooled_tcp_upstream_creates() {
        let u = PooledTcpUpstream::new("8.8.8.8:53".parse().unwrap());
        assert_eq!(u.addr, "8.8.8.8:53".parse::<SocketAddr>().unwrap());
    }

    #[test]
    fn tls_config_reuse() {
        // Install crypto provider for rustls 0.23+.
        let _ = tokio_rustls::rustls::crypto::aws_lc_rs::default_provider().install_default();
        // Verify the pooled DoT upstream can be constructed with its own
        // ClientConfig (which backs session resumption within the pooled
        // connection lifecycle).
        let u1 = PooledTlsUpstream::new("1.1.1.1:853".parse().unwrap(), "one.one.one.one".into());
        let u2 = PooledTlsUpstream::new("8.8.8.8:853".parse().unwrap(), "dns.google".into());
        assert!(Arc::strong_count(&u1.tls_config) == 1);
        assert!(Arc::strong_count(&u2.tls_config) == 1);
    }

    #[test]
    fn udp_recv_capacity_uses_edns_payload() {
        use hickory_proto::op::{Edns, Message, MessageType, OpCode, Query};
        use hickory_proto::rr::{Name, RecordType};

        let mut msg = Message::new(0x1111, MessageType::Query, OpCode::Query);
        msg.metadata.op_code = OpCode::Query;
        msg.metadata.recursion_desired = true;
        msg.add_query({
            let mut q = Query::new();
            q.set_name(Name::from_ascii("example.com.").unwrap())
                .set_query_type(RecordType::A);
            q
        });
        let mut edns = Edns::new();
        edns.set_max_payload(1232);
        msg.set_edns(edns);
        let wire = msg.to_vec().unwrap();
        assert_eq!(udp_recv_capacity(&wire), 1232);

        // No EDNS → default buffer.
        let mut bare = Message::new(1, MessageType::Query, OpCode::Query);
        bare.add_query({
            let mut q = Query::new();
            q.set_name(Name::from_ascii("example.com.").unwrap())
                .set_query_type(RecordType::A);
            q
        });
        assert_eq!(udp_recv_capacity(&bare.to_vec().unwrap()), DEFAULT_UDP_RECV);
    }

    /// A stale datagram with a mismatched DNS message ID (e.g. a late response
    /// from a prior query on a reused, pooled socket) must be discarded rather
    /// than returned as the answer to a different query.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn udp_exchange_skips_mismatched_id() {
        // Fake upstream: on receiving a query, first reply with a wrong-ID
        // datagram, then the correctly-IDed response.
        let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let server_addr = server.local_addr().unwrap();

        tokio::spawn(async move {
            let mut buf = [0u8; MAX_UDP_SIZE];
            let (n, peer) = server.recv_from(&mut buf).await.unwrap();
            let query = &buf[..n];

            // Stale datagram: copy the query but flip the ID so it cannot match.
            let mut stale = query.to_vec();
            stale[0] ^= 0xFF;
            stale[1] ^= 0xFF;
            // Tag the body so we can tell the two responses apart.
            stale.push(0xAA);
            server.send_to(&stale, peer).await.unwrap();

            // Correct datagram: keep the query's ID, tag the body differently.
            let mut good = query.to_vec();
            good.push(0xBB);
            server.send_to(&good, peer).await.unwrap();
        });

        let upstream = UdpUpstream::new(server_addr).with_timeout(Duration::from_secs(2));
        // Query wire: 2-byte ID followed by a minimal body.
        let query = vec![0x12, 0x34, 0x00, 0x00, 0x00, 0x00];
        let resp = upstream.exchange(&query).await.unwrap();

        // We must get the correctly-IDed response (tagged 0xBB), not the stale
        // one (tagged 0xAA).
        assert_eq!(&resp[0..2], &query[0..2], "response ID must match query ID");
        assert_eq!(*resp.last().unwrap(), 0xBB, "must skip the stale datagram");
    }

    /// When the UDP answer has TC=1, the upstream must transparently re-query
    /// over TCP and return the full answer.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn udp_exchange_retries_over_tcp_on_tc() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        let udp_server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let server_addr = udp_server.local_addr().unwrap();
        let tcp_listener = TcpListener::bind(server_addr).await.unwrap();

        let query = vec![0xAB, 0xCD, 0x01, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];

        // UDP: reply with TC=1, same ID.
        let udp_query = query.clone();
        tokio::spawn(async move {
            let mut buf = [0u8; 512];
            let (n, peer) = udp_server.recv_from(&mut buf).await.unwrap();
            assert_eq!(&buf[..n], udp_query.as_slice());
            // Header: ID + flags with QR=1, TC=1, RA=1; zero counts.
            let mut truncated = vec![0xAB, 0xCD, 0x82, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
            // Tag so we can tell UDP vs TCP answers apart.
            truncated.push(0x11);
            udp_server.send_to(&truncated, peer).await.unwrap();
        });

        // TCP: reply with a full (non-truncated) answer, length-prefixed.
        let tcp_query = query.clone();
        tokio::spawn(async move {
            let (mut stream, _) = tcp_listener.accept().await.unwrap();
            let mut len_buf = [0u8; 2];
            stream.read_exact(&mut len_buf).await.unwrap();
            let qlen = u16::from_be_bytes(len_buf) as usize;
            let mut qbuf = vec![0u8; qlen];
            stream.read_exact(&mut qbuf).await.unwrap();
            assert_eq!(qbuf, tcp_query);

            let mut full = vec![0xAB, 0xCD, 0x80, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
            full.push(0x22); // full-answer tag
            let len = (full.len() as u16).to_be_bytes();
            stream.write_all(&len).await.unwrap();
            stream.write_all(&full).await.unwrap();
        });

        let upstream = UdpUpstream::new(server_addr).with_timeout(Duration::from_secs(2));
        let resp = upstream.exchange(&query).await.unwrap();
        assert_eq!(&resp[0..2], &query[0..2]);
        assert!(!dns_header_tc(&resp), "TCP fallback must clear truncation");
        assert_eq!(*resp.last().unwrap(), 0x22, "must return the full TCP answer");
    }

    // ── RFC 7766 pipelining tests ─────────────────────────────────

    #[test]
    fn probe_query_wire_format() {
        let q = build_probe_query(0xABCD);
        assert_eq!(q.len(), 17);
        assert_eq!(&q[0..2], &[0xAB, 0xCD]); // transaction ID
        assert_eq!(&q[2..4], &[0x01, 0x00]); // RD=1
        assert_eq!(&q[4..6], &[0x00, 0x01]); // QDCOUNT=1
        assert_eq!(q[12], 0x00); // root label
        assert_eq!(&q[13..15], &[0x00, 0x01]); // QTYPE=A
        assert_eq!(&q[15..17], &[0x00, 0x01]); // QCLASS=IN
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn pipelined_conn_demux_by_id() {
        use tokio::io::duplex;

        let (client, mut server) = duplex(4096);
        let (reader, writer) = tokio::io::split(client);

        let conn = Arc::new(PipelinedConn {
            writer: Mutex::new(Box::new(writer)),
            pending: StdMutex::new(HashMap::new()),
            next_id: AtomicU16::new(0),
            alive: AtomicBool::new(true),
        });
        spawn_reader(reader, conn.clone());

        // Register two pending queries with IDs 10 and 20.
        let (tx1, rx1) = tokio::sync::oneshot::channel();
        let (tx2, rx2) = tokio::sync::oneshot::channel();
        conn.pending.lock().insert(10, tx1);
        conn.pending.lock().insert(20, tx2);

        // Server sends response for ID 20 first (out of order), then ID 10.
        let mut resp20 = vec![0u8; 12];
        resp20[0..2].copy_from_slice(&20u16.to_be_bytes());
        resp20[5] = 0xBB;
        let len20 = (resp20.len() as u16).to_be_bytes();
        server.write_all(&len20).await.unwrap();
        server.write_all(&resp20).await.unwrap();

        let mut resp10 = vec![0u8; 12];
        resp10[0..2].copy_from_slice(&10u16.to_be_bytes());
        resp10[5] = 0xAA;
        let len10 = (resp10.len() as u16).to_be_bytes();
        server.write_all(&len10).await.unwrap();
        server.write_all(&resp10).await.unwrap();
        server.flush().await.unwrap();

        let r1 = rx1.await.unwrap().unwrap();
        let r2 = rx2.await.unwrap().unwrap();
        assert_eq!(r1[5], 0xAA);
        assert_eq!(r2[5], 0xBB);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn pipelined_conn_drains_on_close() {
        use tokio::io::duplex;

        let (client, server) = duplex(4096);
        let (reader, writer) = tokio::io::split(client);

        let conn = Arc::new(PipelinedConn {
            writer: Mutex::new(Box::new(writer)),
            pending: StdMutex::new(HashMap::new()),
            next_id: AtomicU16::new(0),
            alive: AtomicBool::new(true),
        });
        spawn_reader(reader, conn.clone());

        let (tx, rx) = tokio::sync::oneshot::channel();
        conn.pending.lock().insert(42, tx);

        // Drop the server side to simulate connection close.
        drop(server);

        let result = rx.await.unwrap();
        assert!(result.is_err());
        assert!(!conn.alive.load(Ordering::Acquire));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn pipelined_conn_alloc_id_skips_inflight() {
        use tokio::io::duplex;

        let (client, _server) = duplex(4096);
        let (reader, writer) = tokio::io::split(client);

        let conn = Arc::new(PipelinedConn {
            writer: Mutex::new(Box::new(writer)),
            pending: StdMutex::new(HashMap::new()),
            next_id: AtomicU16::new(0),
            alive: AtomicBool::new(true),
        });
        spawn_reader(reader, conn.clone());

        // Occupy IDs 0, 1, 2.
        let (tx0, _rx0) = tokio::sync::oneshot::channel();
        let (tx1, _rx1) = tokio::sync::oneshot::channel();
        let (tx2, _rx2) = tokio::sync::oneshot::channel();
        conn.pending.lock().insert(0, tx0);
        conn.pending.lock().insert(1, tx1);
        conn.pending.lock().insert(2, tx2);

        // next_id starts at 0, so alloc_id should skip 0, 1, 2 and return 3.
        let id = conn.alloc_id();
        assert_eq!(id, 3);
    }
}
