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
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, warn};

/// Default TCP idle timeout.
const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(10);

/// Default first read timeout.
const FIRST_READ_TIMEOUT: Duration = Duration::from_secs(2);

/// Starts an async TCP DNS server on the given listener.
///
/// Runs until the cancellation token is triggered or the listener errors.
pub async fn serve_tcp(
    listener: TcpListener,
    handler: Arc<dyn DnsHandler>,
    cancel: CancellationToken,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    loop {
        tokio::select! {
            result = listener.accept() => {
                let (stream, peer) = result?;
                let handler = handler.clone();
                let cancel = cancel.clone();

                tokio::spawn(async move {
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
                        if msg_len == 0 || msg_len > 65535 { return; }

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
                                        if stream.write_all(&len).await.is_err() { return; }
                                        if stream.write_all(&resp_bytes).await.is_err() { return; }
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
