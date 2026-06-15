// Copyright (C) 2026, Harlan Wei
//
// This file is part of redns.

//! Async TCP DNS server with graceful shutdown.

use crate::server::{DnsHandler, QueryMeta};
use hickory_proto::op::Message;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, warn};

/// Default TCP idle timeout.
const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(10);

/// Default first read timeout.
const FIRST_READ_TIMEOUT: Duration = Duration::from_secs(2);

/// Timeout for writing a response back to the client. Bounds the per-connection
/// task so a client that stops reading (a full receive window) cannot pin the
/// task and its `MAX_CONNECTIONS` permit indefinitely (slowloris-on-write).
const WRITE_TIMEOUT: Duration = Duration::from_secs(10);

/// Maximum number of concurrent in-flight TCP connections. Bounds task and
/// memory growth under a connection flood; further connections wait in the OS
/// accept backlog until a slot frees rather than spawning unbounded tasks.
const MAX_CONNECTIONS: usize = 1024;

/// Backoff applied after an accept failure caused by fd exhaustion, to avoid
/// busy-spinning while descriptors are unavailable.
const ACCEPT_BACKOFF: Duration = Duration::from_millis(100);

/// Returns true if the accept error indicates file-descriptor exhaustion
/// (`EMFILE`/`ENFILE`), where backing off helps rather than retrying hot.
fn is_fd_exhaustion(e: &std::io::Error) -> bool {
    matches!(
        e.raw_os_error(),
        Some(libc::EMFILE) | Some(libc::ENFILE)
    )
}

/// Starts an async TCP DNS server on the given listener.
///
/// Runs until the cancellation token is triggered or the listener errors.
pub async fn serve_tcp(
    listener: TcpListener,
    handler: Arc<dyn DnsHandler>,
    cancel: CancellationToken,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Bound concurrent connections. A permit is acquired before spawning the
    // per-connection task and held until that task ends, so a connection flood
    // cannot grow tasks and memory without limit.
    let conn_limit = Arc::new(Semaphore::new(MAX_CONNECTIONS));
    loop {
        tokio::select! {
            result = listener.accept() => {
                let (stream, peer) = match result {
                    Ok(v) => v,
                    Err(e) => {
                        // A failed accept must never tear down the listener.
                        // Back off briefly on fd exhaustion so we don't spin at
                        // 100% CPU while descriptors are unavailable.
                        if is_fd_exhaustion(&e) {
                            warn!(error = %e, "TCP accept failed (fd exhaustion), backing off");
                            tokio::time::sleep(ACCEPT_BACKOFF).await;
                        } else {
                            warn!(error = %e, "TCP accept failed");
                        }
                        continue;
                    }
                };

                // Wait for a free connection slot. Cancellation still wins so
                // shutdown is not blocked by a saturated pool.
                let permit = tokio::select! {
                    p = conn_limit.clone().acquire_owned() => match p {
                        Ok(p) => p,
                        Err(_) => return Ok(()), // Semaphore closed; shutting down.
                    },
                    _ = cancel.cancelled() => {
                        debug!("TCP server shutting down");
                        return Ok(());
                    }
                };
                let handler = handler.clone();
                let cancel = cancel.clone();

                tokio::spawn(async move {
                    let _permit = permit;
                    debug!(peer = %peer, "new TCP connection");
                    let mut stream = stream;
                    let mut first_read = true;

                    loop {
                        // Check cancellation.
                        if cancel.is_cancelled() { return; }

                        let timeout = if first_read {
                            first_read = false;
                            FIRST_READ_TIMEOUT
                        } else {
                            DEFAULT_IDLE_TIMEOUT
                        };

                        // Read 2-byte length prefix.
                        let mut len_buf = [0u8; 2];
                        match tokio::time::timeout(timeout, stream.read_exact(&mut len_buf)).await {
                            Ok(Ok(_)) => {}
                            _ => return,
                        }

                        let msg_len = u16::from_be_bytes(len_buf) as usize;
                        if msg_len == 0 { return; }

                        let mut msg_buf = vec![0u8; msg_len];
                        match tokio::time::timeout(timeout, stream.read_exact(&mut msg_buf)).await {
                            Ok(Ok(_)) => {}
                            _ => return,
                        }

                        let query = match Message::from_vec(&msg_buf) {
                            Ok(q) => q,
                            Err(e) => {
                                debug!(error = %e, "invalid TCP DNS query");
                                return;
                            }
                        };

                        let meta = QueryMeta {
                            protocol: Some("tcp".to_string()),
                            from_udp: false,
                            client_addr: Some(peer.ip()),
                            url_path: None,
                            server_name: None,
                            selected_upstreams: None,
                            query_wire: Some(Arc::new(msg_buf)),
                        };

                        match handler.handle(query, meta).await {
                            Ok(resp) => {
                                match resp.to_vec() {
                                    Ok(resp_bytes) => {
                                        let len = (resp_bytes.len() as u16).to_be_bytes();
                                        let write = async {
                                            stream.write_all(&len).await?;
                                            stream.write_all(&resp_bytes).await
                                        };
                                        match tokio::time::timeout(WRITE_TIMEOUT, write).await {
                                            Ok(Ok(())) => {}
                                            _ => return,
                                        }
                                    }
                                    Err(e) => {
                                        warn!(error = %e, "failed to serialize TCP response");
                                        return;
                                    }
                                }
                            }
                            Err(e) => {
                                error!(error = %e, "TCP handler error");
                                return;
                            }
                        }
                    }
                });
            }
            _ = cancel.cancelled() => {
                debug!("TCP server shutting down");
                return Ok(());
            }
        }
    }
}
