// Copyright (C) 2026, Harlan Wei
//
// This file is part of redns.

//! Upstream DNS transport implementations.

use crate::plugin::PluginResult;
use async_trait::async_trait;
use std::collections::VecDeque;
use tracing::warn;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, UdpSocket};
use tokio::sync::Mutex;

/// Maximum DNS UDP payload.
const MAX_UDP_SIZE: usize = 4096;

/// Default exchange timeout.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);

/// Default idle timeout for pooled connections.
const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(10);

/// Maximum idle connections in pool.
const MAX_IDLE_CONNS: usize = 4;

/// Maximum retries on stale pooled connection.
const MAX_POOL_RETRY: usize = 2;

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
}

impl UdpUpstream {
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
impl Upstream for UdpUpstream {
    async fn exchange(&self, query: &[u8]) -> PluginResult<Vec<u8>> {
        let bind_addr: SocketAddr = if self.addr.is_ipv4() {
            "0.0.0.0:0".parse().unwrap()
        } else {
            "[::]:0".parse().unwrap()
        };
        let sock = UdpSocket::bind(bind_addr).await.map_err(
            |e| -> Box<dyn std::error::Error + Send + Sync> { format!("udp bind: {e}").into() },
        )?;
        sock.send_to(query, self.addr).await.map_err(
            |e| -> Box<dyn std::error::Error + Send + Sync> { format!("udp send: {e}").into() },
        )?;

        let mut buf = vec![0u8; MAX_UDP_SIZE];
        let result = tokio::time::timeout(self.timeout, sock.recv_from(&mut buf)).await;
        match result {
            Ok(Ok((n, _))) => {
                buf.truncate(n);
                Ok(buf)
            }
            Ok(Err(e)) => Err(format!("udp recv: {e}").into()),
            Err(_) => Err("udp exchange timed out".into()),
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
        tcp_exchange(&mut stream, query, self.timeout).await
    }
}

// ── TCP Pooled ──────────────────────────────────────────────────

/// An idle connection with a timestamp for expiry tracking.
struct IdleConn<S> {
    stream: S,
    idle_since: Instant,
}

/// TCP upstream with connection pooling and retry on stale connections.
pub struct PooledTcpUpstream {
    addr: SocketAddr,
    timeout: Duration,
    idle_timeout: Duration,
    pool: Mutex<VecDeque<IdleConn<TcpStream>>>,
}

impl PooledTcpUpstream {
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            timeout: DEFAULT_TIMEOUT,
            idle_timeout: DEFAULT_IDLE_TIMEOUT,
            pool: Mutex::new(VecDeque::new()),
        }
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Try to get a non-expired idle connection from the pool.
    async fn get_idle(&self) -> Option<TcpStream> {
        let mut pool = self.pool.lock().await;
        while let Some(idle) = pool.pop_front() {
            if idle.idle_since.elapsed() < self.idle_timeout {
                return Some(idle.stream);
            }
            // Expired — drop silently.
        }
        None
    }

    /// Return a connection to the pool.
    async fn put_idle(&self, stream: TcpStream) {
        let mut pool = self.pool.lock().await;
        // Enforce max idle limit.
        while pool.len() >= MAX_IDLE_CONNS {
            pool.pop_front();
        }
        pool.push_back(IdleConn {
            stream,
            idle_since: Instant::now(),
        });
    }
}

#[async_trait]
impl Upstream for PooledTcpUpstream {
    async fn exchange(&self, query: &[u8]) -> PluginResult<Vec<u8>> {
        let mut retries = 0;
        loop {
            let (mut stream, is_reused) = match self.get_idle().await {
                Some(s) => (s, true),
                None => (tcp_connect(self.addr, self.timeout).await?, false),
            };

            match tcp_exchange(&mut stream, query, self.timeout).await {
                Ok(resp) => {
                    // Return connection to pool for reuse.
                    self.put_idle(stream).await;
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

    async fn tls_connect(&self) -> PluginResult<TlsStream<TcpStream>> {
        let tcp = tcp_connect(self.addr, self.timeout).await?;
        let connector = TlsConnector::from(self.tls_config.clone());
        let sni = ServerName::try_from(self.server_name.clone()).map_err(
            |e| -> Box<dyn std::error::Error + Send + Sync> {
                format!("invalid server name: {e}").into()
            },
        )?;
        let tls = connector.connect(sni, tcp).await.map_err(
            |e| -> Box<dyn std::error::Error + Send + Sync> {
                format!("tls handshake: {e}").into()
            },
        )?;
        Ok(tls)
    }
}

#[async_trait]
impl Upstream for TlsUpstream {
    async fn exchange(&self, query: &[u8]) -> PluginResult<Vec<u8>> {
        let mut tls = self.tls_connect().await?;
        tls_exchange(&mut tls, query, self.timeout).await
    }
}

// ── TLS Pooled ──────────────────────────────────────────────────

/// TLS upstream with connection pooling, session caching, and retry.
pub struct PooledTlsUpstream {
    addr: SocketAddr,
    server_name: String,
    timeout: Duration,
    idle_timeout: Duration,
    tls_config: Arc<ClientConfig>,
    pool: Mutex<VecDeque<IdleConn<TlsStream<TcpStream>>>>,
}

impl PooledTlsUpstream {
    pub fn new(addr: SocketAddr, server_name: String) -> Self {
        Self {
            addr,
            server_name,
            timeout: DEFAULT_TIMEOUT,
            idle_timeout: DEFAULT_IDLE_TIMEOUT,
            tls_config: build_tls_config(),
            pool: Mutex::new(VecDeque::new()),
        }
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    async fn get_idle(&self) -> Option<TlsStream<TcpStream>> {
        let mut pool = self.pool.lock().await;
        while let Some(idle) = pool.pop_front() {
            if idle.idle_since.elapsed() < self.idle_timeout {
                return Some(idle.stream);
            }
        }
        None
    }

    async fn put_idle(&self, stream: TlsStream<TcpStream>) {
        let mut pool = self.pool.lock().await;
        while pool.len() >= MAX_IDLE_CONNS {
            pool.pop_front();
        }
        pool.push_back(IdleConn {
            stream,
            idle_since: Instant::now(),
        });
    }

    async fn tls_connect(&self) -> PluginResult<TlsStream<TcpStream>> {
        let tcp = tcp_connect(self.addr, self.timeout).await?;
        let connector = TlsConnector::from(self.tls_config.clone());
        let sni = ServerName::try_from(self.server_name.clone()).map_err(
            |e| -> Box<dyn std::error::Error + Send + Sync> {
                format!("invalid server name: {e}").into()
            },
        )?;
        let tls = connector.connect(sni, tcp).await.map_err(
            |e| -> Box<dyn std::error::Error + Send + Sync> {
                format!("tls handshake: {e}").into()
            },
        )?;
        Ok(tls)
    }
}

#[async_trait]
impl Upstream for PooledTlsUpstream {
    async fn exchange(&self, query: &[u8]) -> PluginResult<Vec<u8>> {
        let mut retries = 0;
        loop {
            let (mut stream, is_reused) = match self.get_idle().await {
                Some(s) => (s, true),
                None => (self.tls_connect().await?, false),
            };

            match tls_exchange(&mut stream, query, self.timeout).await {
                Ok(resp) => {
                    self.put_idle(stream).await;
                    return Ok(resp);
                }
                Err(e) => {
                    if is_reused && retries < MAX_POOL_RETRY {
                        retries += 1;
                        continue;
                    }
                    return Err(e);
                }
            }
        }
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

/// Exchange a DNS query over a length-prefixed TCP stream.
async fn tcp_exchange(
    stream: &mut TcpStream,
    query: &[u8],
    timeout: Duration,
) -> PluginResult<Vec<u8>> {
    // Write length-prefixed query.
    let len = query.len() as u16;
    stream.write_all(&len.to_be_bytes()).await.map_err(
        |e| -> Box<dyn std::error::Error + Send + Sync> { format!("tcp write: {e}").into() },
    )?;
    stream
        .write_all(query)
        .await
        .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
            format!("tcp write: {e}").into()
        })?;

    // Read length-prefixed response.
    let read = tokio::time::timeout(timeout, async {
        let mut len_buf = [0u8; 2];
        stream.read_exact(&mut len_buf).await?;
        let resp_len = u16::from_be_bytes(len_buf) as usize;
        let mut resp_buf = vec![0u8; resp_len];
        stream.read_exact(&mut resp_buf).await?;
        Ok::<Vec<u8>, std::io::Error>(resp_buf)
    });

    match read.await {
        Ok(Ok(resp)) => Ok(resp),
        Ok(Err(e)) => Err(format!("tcp read: {e}").into()),
        Err(_) => Err("tcp exchange timed out".into()),
    }
}

/// Exchange a DNS query over a TLS stream (length-prefixed).
async fn tls_exchange(
    stream: &mut TlsStream<TcpStream>,
    query: &[u8],
    timeout: Duration,
) -> PluginResult<Vec<u8>> {
    let len = query.len() as u16;
    stream.write_all(&len.to_be_bytes()).await.map_err(
        |e| -> Box<dyn std::error::Error + Send + Sync> { format!("tls write: {e}").into() },
    )?;
    stream
        .write_all(query)
        .await
        .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
            format!("tls write: {e}").into()
        })?;

    let read = tokio::time::timeout(timeout, async {
        let mut len_buf = [0u8; 2];
        stream.read_exact(&mut len_buf).await?;
        let resp_len = u16::from_be_bytes(len_buf) as usize;
        let mut resp_buf = vec![0u8; resp_len];
        stream.read_exact(&mut resp_buf).await?;
        Ok::<Vec<u8>, std::io::Error>(resp_buf)
    });

    match read.await {
        Ok(Ok(resp)) => Ok(resp),
        Ok(Err(e)) => Err(format!("tls read: {e}").into()),
        Err(_) => Err("tls exchange timed out".into()),
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
        headers.insert(header::ACCEPT, "application/dns-message".parse().unwrap());

        let mut builder = reqwest::Client::builder()
            .default_headers(headers)
            .timeout(Duration::from_secs(10))
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

/// DNS-over-QUIC upstream using `quinn` (RFC 9250).
///
/// Per RFC 9250 §4.2: open a bi-directional QUIC stream, send a
/// 2-byte length prefix + DNS wire query, read the response in the
/// same length-prefixed format.
pub struct DoqUpstream {
    addr: SocketAddr,
    server_name: String,
    timeout: Duration,
    client_config: quinn::ClientConfig,
}

impl DoqUpstream {
    fn new(addr: SocketAddr, server_name: String) -> Self {
        let client_config = Self::build_client_config();
        Self {
            addr,
            server_name,
            timeout: DEFAULT_TIMEOUT,
            client_config,
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
}

#[async_trait]
impl Upstream for DoqUpstream {
    async fn exchange(&self, query: &[u8]) -> PluginResult<Vec<u8>> {
        let bind_addr: SocketAddr = if self.addr.is_ipv4() {
            "0.0.0.0:0".parse().unwrap()
        } else {
            "[::]:0".parse().unwrap()
        };

        let mut endpoint = quinn::Endpoint::client(bind_addr).map_err(
            |e| -> Box<dyn std::error::Error + Send + Sync> {
                format!("doq endpoint bind: {e}").into()
            },
        )?;
        endpoint.set_default_client_config(self.client_config.clone());

        let connecting = endpoint.connect(self.addr, &self.server_name).map_err(
            |e| -> Box<dyn std::error::Error + Send + Sync> { format!("doq connect: {e}").into() },
        )?;

        let connection = tokio::time::timeout(self.timeout, connecting)
            .await
            .map_err(|_| -> Box<dyn std::error::Error + Send + Sync> {
                "doq connect timed out".into()
            })?
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                format!("doq connection: {e}").into()
            })?;

        let (mut send, mut recv) = connection.open_bi().await.map_err(
            |e| -> Box<dyn std::error::Error + Send + Sync> {
                format!("doq open stream: {e}").into()
            },
        )?;

        // Send length-prefixed DNS query (RFC 9250 §4.2).
        let len = query.len() as u16;
        send.write_all(&len.to_be_bytes()).await.map_err(
            |e| -> Box<dyn std::error::Error + Send + Sync> { format!("doq write: {e}").into() },
        )?;
        send.write_all(query)
            .await
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                format!("doq write: {e}").into()
            })?;
        send.finish()
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                format!("doq finish: {e}").into()
            })?;

        // Read length-prefixed response.
        let resp = tokio::time::timeout(self.timeout, async {
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
        })
        .await
        .map_err(|_| -> Box<dyn std::error::Error + Send + Sync> {
            "doq exchange timed out".into()
        })??;

        // Gracefully close the connection.
        connection.close(quinn::VarInt::from_u32(0), b"done");

        Ok(resp)
    }
}

// ── DoH3 (DNS-over-HTTPS/3) ────────────────────────────────────

/// DNS-over-HTTPS/3 upstream using `h3` + `h3-quinn`.
///
/// Sends HTTP/3 GET requests with base64url-encoded DNS queries,
/// same as DoH (RFC 8484) but over HTTP/3 instead of HTTP/2 or HTTP/1.1.
pub struct Doh3Upstream {
    endpoint_url: String,
    addr: SocketAddr,
    server_name: String,
    timeout: Duration,
    client_config: quinn::ClientConfig,
}

impl Doh3Upstream {
    fn new(endpoint_url: String, addr: SocketAddr, server_name: String) -> Self {
        let client_config = Self::build_client_config();
        Self {
            endpoint_url,
            addr,
            server_name,
            timeout: DEFAULT_TIMEOUT,
            client_config,
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
        let path = {
            let url = reqwest::Url::parse(&self.endpoint_url).map_err(
                |e| -> Box<dyn std::error::Error + Send + Sync> {
                    format!("invalid h3 endpoint: {e}").into()
                },
            )?;
            format!("{}?dns={}", url.path(), encoded)
        };

        let bind_addr: SocketAddr = if self.addr.is_ipv4() {
            "0.0.0.0:0".parse().unwrap()
        } else {
            "[::]:0".parse().unwrap()
        };

        let mut endpoint = quinn::Endpoint::client(bind_addr).map_err(
            |e| -> Box<dyn std::error::Error + Send + Sync> {
                format!("h3 endpoint bind: {e}").into()
            },
        )?;
        endpoint.set_default_client_config(self.client_config.clone());

        let connecting = endpoint.connect(self.addr, &self.server_name).map_err(
            |e| -> Box<dyn std::error::Error + Send + Sync> { format!("h3 connect: {e}").into() },
        )?;

        let quinn_conn = tokio::time::timeout(self.timeout, connecting)
            .await
            .map_err(|_| -> Box<dyn std::error::Error + Send + Sync> {
                "h3 connect timed out".into()
            })?
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                format!("h3 connection: {e}").into()
            })?;

        let h3_conn = h3_quinn::Connection::new(quinn_conn.clone());
        let (mut driver, mut send_request) = h3::client::new(h3_conn).await.map_err(
            |e| -> Box<dyn std::error::Error + Send + Sync> {
                format!("h3 client init: {e}").into()
            },
        )?;

        // Drive the H3 connection in the background.
        let drive_fut = tokio::spawn(async move {
            let _ = futures_util::future::poll_fn(|cx| driver.poll_close(cx)).await;
        });

        // Build HTTP/3 request.
        let req = http::Request::get(format!("https://{}{}", self.server_name, path))
            .header("accept", "application/dns-message")
            .body(())
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                format!("h3 build request: {e}").into()
            })?;

        let mut resp_stream = tokio::time::timeout(self.timeout, send_request.send_request(req))
            .await
            .map_err(|_| -> Box<dyn std::error::Error + Send + Sync> {
                "h3 request timed out".into()
            })?
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                format!("h3 send request: {e}").into()
            })?;

        let resp = resp_stream.recv_response().await.map_err(
            |e| -> Box<dyn std::error::Error + Send + Sync> {
                format!("h3 recv response: {e}").into()
            },
        )?;

        if !resp.status().is_success() {
            return Err(format!("h3: bad status {}", resp.status()).into());
        }

        // Read the response body.
        let mut body_bytes = Vec::new();
        while let Some(chunk) = resp_stream.recv_data().await.map_err(
            |e| -> Box<dyn std::error::Error + Send + Sync> { format!("h3 recv body: {e}").into() },
        )? {
            body_bytes.extend_from_slice(bytes::Buf::chunk(&chunk));
        }

        // Clean up.
        quinn_conn.close(quinn::VarInt::from_u32(0), b"done");
        drive_fut.abort();

        // Restore original query ID.
        if body_bytes.len() >= 2 && query.len() >= 2 {
            body_bytes[0] = query[0];
            body_bytes[1] = query[1];
        }

        Ok(body_bytes)
    }
}

// ── Upstream Wrapper with Latency/Error Tracking ────────────────

/// A point-in-time snapshot of per-upstream metrics.
#[derive(Debug, Clone, serde::Serialize)]
pub struct UpstreamMetrics {
    pub name: String,
    pub query_total: u64,
    pub adopted_total: u64,
    pub error_total: u64,
    pub avg_latency_ms: f64,
}

/// Wraps an upstream transport with per-upstream latency and error tracking.
pub struct UpstreamWrapper {
    inner: Box<dyn Upstream>,
    name: String,
    ema_latency_ms: AtomicI64,
    query_count: AtomicU64,
    error_count: AtomicU64,
    adopted_count: AtomicU64,
    latency_sum_us: AtomicU64,
}

impl UpstreamWrapper {
    /// EMA smoothing factor.
    const ALPHA: f64 = 0.3;

    pub fn new(inner: Box<dyn Upstream>, name: String) -> Self {
        Self {
            inner,
            name,
            ema_latency_ms: AtomicI64::new(0),
            query_count: AtomicU64::new(0),
            error_count: AtomicU64::new(0),
            adopted_count: AtomicU64::new(0),
            latency_sum_us: AtomicU64::new(0),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn ema_latency(&self) -> i64 {
        self.ema_latency_ms.load(Ordering::Relaxed)
    }
    pub fn query_count(&self) -> u64 {
        self.query_count.load(Ordering::Relaxed)
    }
    pub fn error_count(&self) -> u64 {
        self.error_count.load(Ordering::Relaxed)
    }

    pub fn error_rate(&self) -> f64 {
        let q = self.query_count();
        if q == 0 {
            return 0.0;
        }
        self.error_count() as f64 / q as f64
    }

    /// Record that this upstream's response was adopted by the Forward plugin.
    pub fn record_adopted(&self) {
        self.adopted_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Returns a point-in-time snapshot of this upstream's metrics.
    pub fn snapshot(&self) -> UpstreamMetrics {
        let query_total = self.query_count.load(Ordering::Relaxed);
        let latency_sum = self.latency_sum_us.load(Ordering::Relaxed);
        UpstreamMetrics {
            name: self.name.clone(),
            query_total,
            adopted_total: self.adopted_count.load(Ordering::Relaxed),
            error_total: self.error_count.load(Ordering::Relaxed),
            avg_latency_ms: if query_total > 0 {
                (latency_sum as f64 / query_total as f64) / 1000.0
            } else {
                0.0
            },
        }
    }

    pub async fn exchange(&self, query: &[u8]) -> PluginResult<Vec<u8>> {
        self.query_count.fetch_add(1, Ordering::Relaxed);
        let start = std::time::Instant::now();
        let result = self.inner.exchange(query).await;
        let elapsed = start.elapsed();
        let elapsed_ms = elapsed.as_millis() as i64;
        self.latency_sum_us
            .fetch_add(elapsed.as_micros() as u64, Ordering::Relaxed);

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
struct BootstrapResolver {
    /// The hostname this resolver is responsible for.
    target_host: String,
    /// Bootstrap upstream URL (e.g., "8.8.8.8", "tls://1.1.1.1", "https://1.1.1.1/dns-query").
    bootstrap: String,
    /// Cached resolution: (resolved address, expiry time).
    cache: std::sync::Mutex<Option<(SocketAddr, Instant)>>,
    /// Port to use for the resolved address.
    port: u16,
}

impl BootstrapResolver {
    fn new(target_host: String, bootstrap: String, port: u16) -> Self {
        Self {
            target_host,
            bootstrap,
            cache: std::sync::Mutex::new(None),
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

        // Check cache (sync Mutex, fast path).
        {
            if let Ok(guard) = self.cache.lock() {
                if let Some((addr, expiry)) = *guard {
                    if Instant::now() < expiry {
                        let addrs = vec![addr];
                        return Box::pin(async move {
                            Ok(Box::new(addrs.into_iter())
                                as Box<dyn Iterator<Item = SocketAddr> + Send>)
                        });
                    }
                }
            }
        }

        let target_host = self.target_host.clone();
        let bootstrap = self.bootstrap.clone();
        let port = self.port;
        // Clone the Arc to the cache mutex for use in the async block.
        // We need a way to update cache from the async block. Since self is behind
        // Arc (reqwest stores the resolver in Arc), we can't directly capture &self.
        // Instead, wrap cache update in a pointer — safe because Arc keeps us alive.
        let cache_ptr = &self.cache as *const std::sync::Mutex<Option<(SocketAddr, Instant)>>;
        let cache_raw = cache_ptr as usize; // Convert to raw for Send

        Box::pin(async move {
            let result = bootstrap_resolve(&target_host, &bootstrap).await?;

            let (ip, ttl) = result;
            let addr = SocketAddr::new(ip, port);

            // Update cache.
            // SAFETY: BootstrapResolver is stored in Arc inside reqwest::Client,
            // so it outlives all resolve calls.
            let cache_ref =
                unsafe { &*(cache_raw as *const std::sync::Mutex<Option<(SocketAddr, Instant)>>) };
            if let Ok(mut guard) = cache_ref.lock() {
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
/// Priority: `dial_addr` (direct) → `bootstrap` (via specific DNS) → error if unresolved domain.
/// IP-based hosts resolve directly without needing dial_addr/bootstrap.
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
/// Priority: `dial_addr` (static pin) → `bootstrap` (TTL-aware resolver).
/// If the host is already an IP, no resolution is needed.
/// **Errors** if the host is a domain and neither `dial_addr` nor `bootstrap` is set.
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
        // Seed the cache with the initial resolution.
        if let Ok(mut cache) = resolver.cache.lock() {
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
        if let RData::A(a) = answer.data() {
            let ttl = Duration::from_secs(answer.ttl() as u64);
            // Clamp TTL: min 60s, max 3600s.
            let ttl = ttl
                .max(Duration::from_secs(60))
                .min(Duration::from_secs(3600));
            return Ok((std::net::IpAddr::V4(a.0), ttl));
        }
    }

    Err(format!("bootstrap DNS returned no A records for '{}'", hostname).into())
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

/// Creates an `UpstreamWrapper` from an address string.
pub fn new_wrapped_upstream(addr: &str, opts: UpstreamOpts) -> PluginResult<UpstreamWrapper> {
    let inner = new_upstream(addr, opts)?;
    let name = addr.to_string();
    Ok(UpstreamWrapper::new(inner, name))
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
}
