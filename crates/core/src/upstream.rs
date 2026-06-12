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
//! | DoH      | `https://`   | 443          | HTTP/2, GET method (RFC 8484)     |
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
//! are automatically detected and retried (up to 2 attempts). UDP upstreams pool sockets
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
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, UdpSocket};
use tokio::sync::Mutex;
use tracing::warn;

/// Maximum DNS UDP payload.
const MAX_UDP_SIZE: usize = 4096;

/// Default exchange timeout.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);

/// Default idle timeout for pooled connections.
const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

/// Maximum idle connections in pool.
const MAX_IDLE_CONNS: usize = 4;

/// Maximum retries on stale pooled connection.
const MAX_POOL_RETRY: usize = 2;

/// Maximum idle UDP sockets in pool.
const MAX_IDLE_UDP_SOCKETS: usize = 16;

static GLOBAL_LATENCY_SUM_US: AtomicU64 = AtomicU64::new(0);
static GLOBAL_COMPLETED_TOTAL: AtomicU64 = AtomicU64::new(0);

/// An upstream DNS transport that can exchange raw DNS wire messages.
#[async_trait]
pub trait Upstream: Send + Sync {
    /// Sends a DNS query (wire format) and returns the response (wire format).
    async fn exchange(&self, query: &[u8]) -> PluginResult<Vec<u8>>;
}

// ── UDP ─────────────────────────────────────────────────────────

/// Simple UDP upstream transport.
pub struct UdpUpstream {
    addr: SocketAddr,
    timeout: Duration,
    pool: Mutex<VecDeque<UdpSocket>>,
}

impl UdpUpstream {
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            timeout: DEFAULT_TIMEOUT,
            pool: Mutex::new(VecDeque::new()),
        }
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
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
        let mut pool = self.pool.lock().await;
        while pool.len() >= MAX_IDLE_UDP_SOCKETS {
            pool.pop_front();
        }
        pool.push_back(sock);
    }
}

#[async_trait]
impl Upstream for UdpUpstream {
    async fn exchange(&self, query: &[u8]) -> PluginResult<Vec<u8>> {
        let sock = self.get_socket().await?;

        sock.send(query)
            .await
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                format!("udp send: {e}").into()
            })?;

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

        let deadline = Instant::now() + self.timeout;
        let mut buf = vec![0u8; MAX_UDP_SIZE];
        loop {
            let remaining = match deadline.checked_duration_since(Instant::now()) {
                Some(d) if !d.is_zero() => d,
                _ => return Err("udp exchange timed out".into()),
            };

            match tokio::time::timeout(remaining, sock.recv(&mut buf)).await {
                Ok(Ok(n)) => {
                    // Ignore datagrams that are too short to carry an ID or whose
                    // ID does not match this query (a stale/duplicate response).
                    if n < 2 || buf[0..2] != want_id {
                        continue;
                    }
                    let mut resp = buf;
                    resp.truncate(n);
                    self.put_socket(sock).await;
                    return Ok(resp);
                }
                Ok(Err(e)) => return Err(format!("udp recv: {e}").into()),
                Err(_) => return Err("udp exchange timed out".into()),
            }
        }
    }
}

// ── TCP (Simple, one-conn-per-query) ────────────────────────────

/// Simple TCP upstream transport (one connection per query, kept for direct use).
pub struct TcpUpstream {
    addr: SocketAddr,
    timeout: Duration,
}

impl TcpUpstream {
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            timeout: DEFAULT_TIMEOUT,
        }
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }
}

#[async_trait]
impl Upstream for TcpUpstream {
    async fn exchange(&self, query: &[u8]) -> PluginResult<Vec<u8>> {
        let mut stream = tcp_connect(self.addr, self.timeout).await?;
        stream_exchange(&mut stream, query, self.timeout).await
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
    conns: Mutex<VecDeque<IdleConn<S>>>,
}

impl<S> ConnPool<S> {
    fn new(idle_timeout: Duration) -> Self {
        Self {
            idle_timeout,
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
        let mut pool = self.conns.lock().await;
        while pool.len() >= MAX_IDLE_CONNS {
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
            pool: ConnPool::new(DEFAULT_IDLE_TIMEOUT),
        }
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
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

/// Build a shared `ClientConfig` with session caching.
fn build_tls_config() -> Arc<ClientConfig> {
    let mut root_store = RootCertStore::empty();
    root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let config = ClientConfig::builder()
        .with_root_certificates(root_store)
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
    connector.connect(sni, tcp).await.map_err(
        |e| -> Box<dyn std::error::Error + Send + Sync> {
            format!("tls handshake: {e}").into()
        },
    )
}

/// TLS (DNS-over-TLS) upstream transport with session caching.
///
/// Phase 3: The `ClientConfig` is built once and reused, enabling
/// TLS session resumption via rustls's built-in LRU session cache.
pub struct TlsUpstream {
    addr: SocketAddr,
    server_name: String,
    timeout: Duration,
    tls_config: Arc<ClientConfig>,
}

impl TlsUpstream {
    pub fn new(addr: SocketAddr, server_name: String) -> Self {
        Self {
            addr,
            server_name,
            timeout: DEFAULT_TIMEOUT,
            tls_config: build_tls_config(),
        }
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }
}

#[async_trait]
impl Upstream for TlsUpstream {
    async fn exchange(&self, query: &[u8]) -> PluginResult<Vec<u8>> {
        let mut tls =
            tls_connect(self.addr, &self.server_name, &self.tls_config, self.timeout).await?;
        stream_exchange(&mut tls, query, self.timeout).await
    }
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
            pool: ConnPool::new(DEFAULT_IDLE_TIMEOUT),
        }
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
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

// ── Shared TCP/TLS helpers ──────────────────────────────────────

/// Connect to a TCP address with timeout.
async fn tcp_connect(addr: SocketAddr, timeout: Duration) -> PluginResult<TcpStream> {
    tokio::time::timeout(timeout, TcpStream::connect(addr))
        .await
        .map_err(|_| -> Box<dyn std::error::Error + Send + Sync> {
            "tcp connect timed out".into()
        })?
        .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
            format!("tcp connect: {e}").into()
        })
}

/// Exchange a DNS query over any length-prefixed byte stream (TCP or TLS).
///
/// Writes the query and reads the response under a single timeout. Bounding
/// only the read would leave the write able to block indefinitely if the
/// upstream accepts the connection but stalls its receive window.
async fn stream_exchange<S>(stream: &mut S, query: &[u8], timeout: Duration) -> PluginResult<Vec<u8>>
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
    fn new(endpoint: String, resolution: DohResolution) -> Self {
        use reqwest::header;
        let mut headers = header::HeaderMap::new();
        // HeaderValue::from_static is infallible for valid static strings.
        headers.insert(
            header::ACCEPT,
            header::HeaderValue::from_static("application/dns-message"),
        );

        let mut builder = reqwest::Client::builder()
            .default_headers(headers)
            .timeout(Duration::from_secs(10))
            .pool_idle_timeout(Duration::from_secs(180))
            .pool_max_idle_per_host(4);

        match resolution {
            DohResolution::Ip => {}
            DohResolution::StaticAddr(addr) => {
                if let Ok(url) = reqwest::Url::parse(&endpoint) {
                    if let Some(host) = url.host_str() {
                        builder = builder.resolve(host, addr);
                    }
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
            timeout: Duration::from_secs(10),
        }
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }
}

#[async_trait]
impl Upstream for DohUpstream {
    async fn exchange(&self, query: &[u8]) -> PluginResult<Vec<u8>> {
        use base64::Engine;

        // Zero the ID for HTTP cache friendliness (RFC 8484 §4.1).
        let mut wire = query.to_vec();
        if wire.len() >= 2 {
            wire[0] = 0;
            wire[1] = 0;
        }

        let encoded = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&wire);
        let url = format!("{}?dns={}", self.endpoint, encoded);

        let resp = tokio::time::timeout(self.timeout, self.client.get(&url).send())
            .await
            .map_err(|_| -> Box<dyn std::error::Error + Send + Sync> {
                "doh request timed out".into()
            })?
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                format!("doh request: {e}").into()
            })?;

        if !resp.status().is_success() {
            return Err(format!("doh: bad status {}", resp.status()).into());
        }

        let mut body = resp
            .bytes()
            .await
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                format!("doh read body: {e}").into()
            })?
            .to_vec();

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

fn build_quic_endpoint(
    target: SocketAddr,
    client_config: quinn::ClientConfig,
    protocol: &str,
) -> PluginResult<quinn::Endpoint> {
    let bind_addr = quic_bind_addr_for_target(target);
    let mut endpoint = quinn::Endpoint::client(bind_addr).map_err(
        |e| -> Box<dyn std::error::Error + Send + Sync> {
            format!("{protocol} endpoint bind: {e}").into()
        },
    )?;
    endpoint.set_default_client_config(client_config);
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
    conn: Mutex<Option<quinn::Connection>>,
}

impl DoqUpstream {
    fn new(addr: SocketAddr, server_name: String) -> Self {
        let client_config = Self::build_client_config();
        let endpoint =
            build_quic_endpoint(addr, client_config, "doq").expect("failed to create doq endpoint");
        Self {
            addr,
            server_name,
            timeout: DEFAULT_TIMEOUT,
            endpoint,
            conn: Mutex::new(None),
        }
    }

    fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    fn build_client_config() -> quinn::ClientConfig {
        let mut root_store = RootCertStore::empty();
        root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        let mut tls_config = tokio_rustls::rustls::ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth();
        tls_config.alpn_protocols = vec![b"doq".to_vec()];
        let quic_config = quinn::crypto::rustls::QuicClientConfig::try_from(tls_config)
            .expect("failed to create QUIC client config");
        quinn::ClientConfig::new(Arc::new(quic_config))
    }

    async fn connect(&self) -> PluginResult<quinn::Connection> {
        let connecting = self
            .endpoint
            .connect(self.addr, &self.server_name)
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

    async fn invalidate_connection(&self, stable_id: usize) {
        let mut guard = self.conn.lock().await;
        if let Some(conn) = guard.as_ref() {
            if conn.stable_id() == stable_id {
                if let Some(conn) = guard.take() {
                    conn.close(quinn::VarInt::from_u32(0), b"reconnect");
                }
            }
        }
    }
}

#[async_trait]
impl Upstream for DoqUpstream {
    async fn exchange(&self, query: &[u8]) -> PluginResult<Vec<u8>> {
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

                let len = query.len() as u16;
                send.write_all(&len.to_be_bytes()).await.map_err(
                    |e| -> Box<dyn std::error::Error + Send + Sync> {
                        format!("doq write: {e}").into()
                    },
                )?;
                send.write_all(query).await.map_err(
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
                Ok(Ok(resp)) => return Ok(resp),
                Ok(Err(e)) => {
                    self.invalidate_connection(stable_id).await;
                    if attempt < MAX_POOL_RETRY {
                        attempt += 1;
                        continue;
                    }
                    return Err(e);
                }
                Err(_) => {
                    self.invalidate_connection(stable_id).await;
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
    timeout: Duration,
    endpoint: quinn::Endpoint,
    session: Mutex<Option<Arc<Doh3Session>>>,
}

impl Doh3Upstream {
    fn new(endpoint_url: String, addr: SocketAddr, server_name: String) -> Self {
        let client_config = Self::build_client_config();
        let endpoint =
            build_quic_endpoint(addr, client_config, "h3").expect("failed to create h3 endpoint");

        let url = reqwest::Url::parse(&endpoint_url).expect("invalid h3 endpoint url");
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

        Self {
            addr,
            server_name,
            authority,
            path_prefix,
            timeout: DEFAULT_TIMEOUT,
            endpoint,
            session: Mutex::new(None),
        }
    }

    fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    fn build_client_config() -> quinn::ClientConfig {
        let mut root_store = RootCertStore::empty();
        root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        let mut tls_config = tokio_rustls::rustls::ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth();
        tls_config.alpn_protocols = vec![b"h3".to_vec()];
        let quic_config = quinn::crypto::rustls::QuicClientConfig::try_from(tls_config)
            .expect("failed to create QUIC client config");
        quinn::ClientConfig::new(Arc::new(quic_config))
    }

    async fn connect_quic(&self) -> PluginResult<quinn::Connection> {
        let connecting = self
            .endpoint
            .connect(self.addr, &self.server_name)
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
        if let Some(session) = guard.as_ref() {
            if session.is_healthy() {
                return Ok(session.clone());
            }
        }
        Self::clear_session_locked(&mut guard);

        let session = self.create_session().await?;
        *guard = Some(session.clone());
        Ok(session)
    }

    async fn invalidate_session(&self, stable_id: usize) {
        let mut guard = self.session.lock().await;
        if let Some(session) = guard.as_ref() {
            if session.quinn_conn.stable_id() == stable_id {
                Self::clear_session_locked(&mut guard);
            }
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

        let mut attempt = 0;
        loop {
            let session = self.get_session().await?;
            let stable_id = session.quinn_conn.stable_id();
            let mut send_request = session.send_request.clone();

            let req = http::Request::get(format!(
                "https://{}{}dns={}",
                self.authority, self.path_prefix, encoded
            ))
            .header("accept", "application/dns-message")
            .body(())
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                format!("h3 build request: {e}").into()
            })?;

            let mut resp_stream =
                match tokio::time::timeout(self.timeout, send_request.send_request(req)).await {
                    Ok(Ok(stream)) => stream,
                    Ok(Err(e)) => {
                        self.invalidate_session(stable_id).await;
                        if attempt < MAX_POOL_RETRY {
                            attempt += 1;
                            continue;
                        }
                        return Err(format!("h3 send request: {e}").into());
                    }
                    Err(_) => {
                        self.invalidate_session(stable_id).await;
                        if attempt < MAX_POOL_RETRY {
                            attempt += 1;
                            continue;
                        }
                        return Err("h3 request timed out".into());
                    }
                };

            let resp = match tokio::time::timeout(self.timeout, resp_stream.recv_response()).await {
                Ok(Ok(resp)) => resp,
                Ok(Err(e)) => {
                    self.invalidate_session(stable_id).await;
                    if attempt < MAX_POOL_RETRY {
                        attempt += 1;
                        continue;
                    }
                    return Err(format!("h3 recv response: {e}").into());
                }
                Err(_) => {
                    self.invalidate_session(stable_id).await;
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

            let mut body_bytes = Vec::new();
            let mut read_failed = None;
            loop {
                match tokio::time::timeout(self.timeout, resp_stream.recv_data()).await {
                    Ok(Ok(Some(chunk))) => body_bytes.extend_from_slice(bytes::Buf::chunk(&chunk)),
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
                self.invalidate_session(stable_id).await;
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

        match &result {
            Ok(_) => self.update_ema_latency(elapsed_ms),
            Err(e) => {
                self.error_count.fetch_add(1, Ordering::Relaxed);
                warn!(upstream = %self.name, error = %e, "upstream exchange failed");
            }
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
}

impl Default for UpstreamOpts {
    fn default() -> Self {
        Self {
            timeout: DEFAULT_TIMEOUT,
            dial_addr: None,
            bootstrap: None,
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
    if let Some((_, port_str)) = addr.rsplit_once(':') {
        if port_str.parse::<u16>().is_ok() {
            return addr.to_string(); // Already has port.
        }
    }
    // Append default port.
    format!("{}:{}", addr, default_port)
}

// ── Bootstrap Resolver (TTL-aware) ──────────────────────────────

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
    cache: Arc<StdMutex<Option<(SocketAddr, Instant)>>>,
    port: u16,
}

impl BootstrapResolver {
    fn new(target_host: String, bootstrap: String, port: u16) -> Self {
        Self {
            target_host,
            bootstrap,
            cache: Arc::new(StdMutex::new(None)),
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

        let guard = self.cache.lock();
        if let Some((addr, expiry)) = &*guard {
            if Instant::now() < *expiry {
                let addrs = vec![*addr];
                return Box::pin(async move {
                    Ok(Box::new(addrs.into_iter()) as Box<dyn Iterator<Item = SocketAddr> + Send>)
                });
            }
        }

        let target_host = self.target_host.clone();
        let bootstrap = self.bootstrap.clone();
        let port = self.port;
        // Clone the Arc so the cache outlives this future independently of the
        // resolver. reqwest stores the resolver behind an Arc and may poll this
        // future after dropping its handle, so capturing a borrow would be
        // unsound; an owned Arc is both safe and cheap.
        let cache = Arc::clone(&self.cache);

        Box::pin(async move {
            let result = bootstrap_resolve(&target_host, &bootstrap).await?;

            let (ip, ttl) = result;
            let addr = SocketAddr::new(ip, port);

            {
                let mut guard = cache.lock();
                *guard = Some((addr, Instant::now() + ttl));
            }

            let addrs = vec![addr];
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
        let (ip, ttl) = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(bootstrap_resolve(host, bootstrap))
        })?;
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
        let (ip, ttl) = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(bootstrap_resolve(host, bootstrap))
        })?;
        tracing::info!(
            host = %host, resolved = %ip, ttl = ?ttl, bootstrap = %bootstrap,
            "DoH upstream resolved via bootstrap"
        );

        let resolver = Arc::new(BootstrapResolver::new(
            host.to_string(),
            bootstrap.to_string(),
            port,
        ));
        {
            let mut cache = resolver.cache.lock();
            *cache = Some((SocketAddr::new(ip, port), Instant::now() + ttl));
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
/// sends it via `exchange()`, and extracts the first A record with its TTL.
/// Supports any protocol: UDP, TCP, TLS, DoH, DoQ, DoH3.
async fn bootstrap_resolve(
    hostname: &str,
    bootstrap: &str,
) -> Result<(std::net::IpAddr, Duration), Box<dyn std::error::Error + Send + Sync>> {
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

    let mut query_msg = Message::new();
    query_msg
        .set_id(std::process::id() as u16)
        .set_message_type(MessageType::Query)
        .set_op_code(OpCode::Query)
        .set_recursion_desired(true);
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

    for answer in resp.answers() {
        match answer.data() {
            RData::A(a) => {
                let ttl = Duration::from_secs(answer.ttl() as u64);
                // Clamp TTL: min 60s, max 3600s.
                let ttl = ttl
                    .max(Duration::from_secs(60))
                    .min(Duration::from_secs(3600));
                return Ok((std::net::IpAddr::V4(a.0), ttl));
            }
            RData::AAAA(aaaa) => {
                let ttl = Duration::from_secs(answer.ttl() as u64);
                let ttl = ttl
                    .max(Duration::from_secs(60))
                    .min(Duration::from_secs(3600));
                return Ok((std::net::IpAddr::V6(aaaa.0), ttl));
            }
            _ => continue,
        }
    }

    Err(format!("bootstrap DNS returned no A/AAAA records for '{}'", hostname).into())
}

pub fn new_upstream(addr: &str, opts: UpstreamOpts) -> PluginResult<Box<dyn Upstream>> {
    if let Some(rest) = addr.strip_prefix("udp://") {
        let normalized = normalize_addr(rest, 53);
        let socket_addr: SocketAddr =
            normalized
                .parse()
                .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                    format!("invalid address {rest}: {e}").into()
                })?;
        Ok(Box::new(
            UdpUpstream::new(socket_addr).with_timeout(opts.timeout),
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
            PooledTcpUpstream::new(socket_addr).with_timeout(opts.timeout),
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
            PooledTlsUpstream::new(socket_addr, host.to_string()).with_timeout(opts.timeout),
        ))
    } else if addr.starts_with("https://") {
        let resolution = resolve_doh_host(addr, &opts)?;
        Ok(Box::new(
            DohUpstream::new(addr.to_string(), resolution).with_timeout(opts.timeout),
        ))
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
            DoqUpstream::new(socket_addr, host.to_string()).with_timeout(opts.timeout),
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
            Doh3Upstream::new(https_url, socket_addr, host.to_string()).with_timeout(opts.timeout),
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
            UdpUpstream::new(socket_addr).with_timeout(opts.timeout),
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
        let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();
        // Verify TLS config is created once with session cache.
        let u1 = TlsUpstream::new("1.1.1.1:853".parse().unwrap(), "one.one.one.one".into());
        let u2 = TlsUpstream::new("8.8.8.8:853".parse().unwrap(), "dns.google".into());
        // Both should have their own Arc<ClientConfig>.
        assert!(Arc::strong_count(&u1.tls_config) == 1);
        assert!(Arc::strong_count(&u2.tls_config) == 1);
    }

    /// A stale datagram with a mismatched DNS message ID (e.g. a late response
    /// from a prior query on a reused, pooled socket) must be discarded rather
    /// than returned as the answer to the current query.
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

        let upstream =
            UdpUpstream::new(server_addr).with_timeout(Duration::from_secs(2));
        // Query wire: 2-byte ID followed by a minimal body.
        let query = vec![0x12, 0x34, 0x00, 0x00, 0x00, 0x00];
        let resp = upstream.exchange(&query).await.unwrap();

        // We must get the correctly-IDed response (tagged 0xBB), not the stale
        // one (tagged 0xAA).
        assert_eq!(&resp[0..2], &query[0..2], "response ID must match query ID");
        assert_eq!(*resp.last().unwrap(), 0xBB, "must skip the stale datagram");
    }
}
