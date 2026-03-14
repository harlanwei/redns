// Copyright (C) 2026, Harlan Wei
//
// This file is part of redns.

//! Async UDP DNS server with graceful shutdown.

use crate::server::{DnsHandler, QueryMeta};
use hickory_proto::op::Message;
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, warn};

/// Maximum DNS UDP message size.
const MAX_UDP_SIZE: usize = 4096;

/// Upper bound for UDP worker tasks.
const MAX_UDP_WORKERS: usize = 32;

fn default_udp_workers() -> usize {
    let parallelism = std::thread::available_parallelism()
        .map(std::num::NonZeroUsize::get)
        .unwrap_or(1);
    // A small multiple of CPU parallelism handles socket bursts without
    // unbounded task creation on every packet.
    (parallelism * 2).clamp(1, MAX_UDP_WORKERS)
}

async fn udp_worker(
    socket: Arc<UdpSocket>,
    handler: Arc<dyn DnsHandler>,
    cancel: CancellationToken,
) {
    let mut buf = vec![0u8; MAX_UDP_SIZE];

    loop {
        tokio::select! {
            result = socket.recv_from(&mut buf) => {
                let (n, peer) = match result {
                    Ok(v) => v,
                    Err(e) => {
                        warn!(error = %e, "UDP receive failed");
                        continue;
                    }
                };

                let query = match Message::from_vec(&buf[..n]) {
                    Ok(q) => q,
                    Err(e) => {
                        debug!(error = %e, "invalid UDP DNS query");
                        continue;
                    }
                };

                let meta = QueryMeta {
                    protocol: Some("udp".to_string()),
                    from_udp: true,
                    client_addr: Some(peer.ip()),
                    url_path: None,
                    server_name: None,
                    selected_upstreams: None,
                    query_wire: None,
                };

                match handler.handle(query, meta).await {
                    Ok(resp) => {
                        match resp.to_vec() {
                            Ok(resp_bytes) => {
                                if let Err(e) = socket.send_to(&resp_bytes, peer).await {
                                    warn!(error = %e, peer = %peer, "failed to send UDP response");
                                }
                            }
                            Err(e) => {
                                warn!(error = %e, "failed to serialize response");
                            }
                        }
                    }
                    Err(e) => {
                        error!(error = %e, "handler error");
                    }
                }
            }
            _ = cancel.cancelled() => {
                return;
            }
        }
    }
}

/// Starts an async UDP DNS server on the given socket.
///
/// Runs until the cancellation token is triggered or the socket errors.
pub async fn serve_udp(
    socket: Arc<UdpSocket>,
    handler: Arc<dyn DnsHandler>,
    cancel: CancellationToken,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let worker_count = default_udp_workers();
    let mut workers = JoinSet::new();

    for _ in 0..worker_count {
        workers.spawn(udp_worker(socket.clone(), handler.clone(), cancel.clone()));
    }

    cancel.cancelled().await;
    debug!(workers = worker_count, "UDP server shutting down");
    workers.abort_all();
    while workers.join_next().await.is_some() {}

    Ok(())
}
