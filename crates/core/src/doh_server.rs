// Copyright (C) 2026, Harlan Wei
//
// This file is part of redns.

//! DNS-over-HTTPS (RFC 8484) server handler.
//!
//! Supports:
//! - HTTP GET with `?dns=` base64url parameter
//! - HTTP POST with `application/dns-message` body
//! - X-Forwarded-For header for client IP extraction

use crate::server::DnsHandler;
use base64::Engine;
use hickory_proto::op::Message;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tracing::{debug, error, warn};

/// Maximum DNS message size for DoH.
const MAX_DOH_MSG_SIZE: usize = 65535;

/// DoH server configuration.
pub struct DohServerConfig {
    /// Header to read client IP from (e.g., "X-Forwarded-For").
    pub src_ip_header: Option<String>,
}

impl Default for DohServerConfig {
    fn default() -> Self {
        Self {
            src_ip_header: None,
        }
    }
}

/// Starts a simple DNS-over-HTTPS server.
///
/// This implements a minimal HTTP/1.1 server that handles RFC 8484
/// GET and POST requests. For production use, consider using a
/// reverse proxy (nginx, caddy) for TLS termination.
pub async fn serve_doh(
    listener: TcpListener,
    handler: Arc<dyn DnsHandler>,
    config: DohServerConfig,
    cancel: tokio_util::sync::CancellationToken,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let config = Arc::new(config);
    loop {
        tokio::select! {
            accept = listener.accept() => {
                let (stream, peer) = accept?;
                let handler = handler.clone();
                let config = config.clone();
                let cancel = cancel.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_doh_connection(stream, peer, handler, config, cancel).await {
                        debug!(error = %e, peer = %peer, "DoH connection error");
                    }
                });
            }
            _ = cancel.cancelled() => {
                debug!("DoH server shutting down");
                return Ok(());
            }
        }
    }
}

/// Handle a single HTTP connection (simplified HTTP/1.1 parser).
async fn handle_doh_connection(
    mut stream: tokio::net::TcpStream,
    peer: SocketAddr,
    handler: Arc<dyn DnsHandler>,
    config: Arc<DohServerConfig>,
    cancel: tokio_util::sync::CancellationToken,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let timeout = std::time::Duration::from_secs(10);

    loop {
        // Read HTTP request headers.
        let mut header_buf = vec![0u8; 8192];
        let n = tokio::select! {
            result = tokio::time::timeout(timeout, stream.read(&mut header_buf)) => {
                match result {
                    Ok(Ok(0)) | Err(_) => return Ok(()), // Connection closed or timeout.
                    Ok(Ok(n)) => n,
                    Ok(Err(e)) => return Err(format!("read error: {e}").into()),
                }
            }
            _ = cancel.cancelled() => return Ok(()),
        };

        let request_data = &header_buf[..n];
        let request_str = String::from_utf8_lossy(request_data);

        // Parse first line: METHOD PATH HTTP/1.1
        let first_line = match request_str.lines().next() {
            Some(l) => l,
            None => {
                send_http_error(&mut stream, 400, "Bad Request").await?;
                return Ok(());
            }
        };

        let parts: Vec<&str> = first_line.split_whitespace().collect();
        if parts.len() < 3 {
            send_http_error(&mut stream, 400, "Bad Request").await?;
            return Ok(());
        }

        let method = parts[0];
        let path = parts[1];

        // Extract client IP from XFF header if configured.
        let _client_ip = extract_client_ip(&request_str, peer.ip(), &config.src_ip_header);

        // Parse DNS query from request.
        let dns_query = match method {
            "GET" => {
                // Extract ?dns= parameter.
                if let Some(query_string) = path.split('?').nth(1) {
                    let dns_param = query_string
                        .split('&')
                        .find(|p| p.starts_with("dns="))
                        .and_then(|p| p.strip_prefix("dns="));

                    match dns_param {
                        Some(encoded) => {
                            match base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(encoded) {
                                Ok(bytes) => match Message::from_vec(&bytes) {
                                    Ok(msg) => Some(msg),
                                    Err(e) => {
                                        warn!(error = %e, "invalid DNS query in GET");
                                        send_http_error(&mut stream, 400, "Invalid DNS message")
                                            .await?;
                                        return Ok(());
                                    }
                                },
                                Err(e) => {
                                    warn!(error = %e, "invalid base64 in GET");
                                    send_http_error(&mut stream, 400, "Invalid base64").await?;
                                    return Ok(());
                                }
                            }
                        }
                        None => {
                            send_http_error(&mut stream, 400, "Missing dns parameter").await?;
                            return Ok(());
                        }
                    }
                } else {
                    send_http_error(&mut stream, 400, "Missing query string").await?;
                    return Ok(());
                }
            }
            "POST" => {
                // Read Content-Length and body.
                let content_length = extract_header(&request_str, "Content-Length")
                    .and_then(|v| v.parse::<usize>().ok())
                    .unwrap_or(0);

                if content_length == 0 || content_length > MAX_DOH_MSG_SIZE {
                    send_http_error(&mut stream, 400, "Invalid content length").await?;
                    return Ok(());
                }

                // Check if body is already in the header buffer.
                let header_end = request_str.find("\r\n\r\n").map(|p| p + 4).unwrap_or(n);

                let body_in_header = n.saturating_sub(header_end);
                let mut body = Vec::with_capacity(content_length);

                if body_in_header > 0 && header_end < n {
                    body.extend_from_slice(&request_data[header_end..n]);
                }

                // Read remaining body.
                while body.len() < content_length {
                    let mut buf = vec![0u8; content_length - body.len()];
                    let br = stream.read(&mut buf).await.map_err(
                        |e| -> Box<dyn std::error::Error + Send + Sync> {
                            format!("body read: {e}").into()
                        },
                    )?;
                    if br == 0 {
                        break;
                    }
                    body.extend_from_slice(&buf[..br]);
                }

                match Message::from_vec(&body) {
                    Ok(msg) => Some(msg),
                    Err(e) => {
                        warn!(error = %e, "invalid DNS query in POST body");
                        send_http_error(&mut stream, 400, "Invalid DNS message").await?;
                        return Ok(());
                    }
                }
            }
            _ => {
                send_http_error(&mut stream, 405, "Method Not Allowed").await?;
                return Ok(());
            }
        };

        if let Some(query) = dns_query {
            match handler.handle(query).await {
                Ok(resp) => match resp.to_vec() {
                    Ok(resp_bytes) => {
                        send_http_dns_response(&mut stream, &resp_bytes).await?;
                    }
                    Err(e) => {
                        error!(error = %e, "failed to serialize DNS response");
                        send_http_error(&mut stream, 500, "Internal Server Error").await?;
                    }
                },
                Err(e) => {
                    error!(error = %e, "handler error");
                    send_http_error(&mut stream, 500, "Internal Server Error").await?;
                }
            }
        }

        // HTTP/1.0 style: close after one request for simplicity.
        return Ok(());
    }
}

fn extract_header<'a>(request: &'a str, name: &str) -> Option<&'a str> {
    for line in request.lines() {
        if let Some(rest) = line.strip_prefix(name) {
            if let Some(value) = rest.strip_prefix(':') {
                return Some(value.trim());
            }
            // Case-insensitive check.
            if rest.starts_with(':') {
                return Some(rest[1..].trim());
            }
        }
        // Case-insensitive.
        let lower = line.to_ascii_lowercase();
        let name_lower = name.to_ascii_lowercase();
        if lower.starts_with(&name_lower) {
            if let Some(rest) = line.get(name.len()..) {
                if let Some(value) = rest.strip_prefix(':') {
                    return Some(value.trim());
                }
            }
        }
    }
    None
}

fn extract_client_ip(request: &str, peer_ip: IpAddr, src_ip_header: &Option<String>) -> IpAddr {
    if let Some(header) = src_ip_header {
        if let Some(value) = extract_header(request, header) {
            // XFF: take the first IP.
            let first = value.split(',').next().unwrap_or("").trim();
            if let Ok(ip) = first.parse::<IpAddr>() {
                return ip;
            }
        }
    }
    peer_ip
}

async fn send_http_error(
    stream: &mut tokio::net::TcpStream,
    status: u16,
    msg: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let response = format!(
        "HTTP/1.1 {} {}\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        status,
        msg,
        msg.len(),
        msg
    );
    stream.write_all(response.as_bytes()).await.map_err(
        |e| -> Box<dyn std::error::Error + Send + Sync> { format!("write error: {e}").into() },
    )?;
    Ok(())
}

async fn send_http_dns_response(
    stream: &mut tokio::net::TcpStream,
    body: &[u8],
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let header = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/dns-message\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(header.as_bytes()).await.map_err(
        |e| -> Box<dyn std::error::Error + Send + Sync> { format!("write header: {e}").into() },
    )?;
    stream
        .write_all(body)
        .await
        .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
            format!("write body: {e}").into()
        })?;
    Ok(())
}
