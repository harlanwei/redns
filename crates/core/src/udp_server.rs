// Copyright (C) 2026, Harlan Wei
//
// This file is part of redns.

//! Async UDP DNS server with graceful shutdown.

use crate::server::DnsHandler;
use hickory_proto::op::Message;
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, warn};

/// Maximum DNS UDP message size.
const MAX_UDP_SIZE: usize = 4096;

/// Starts an async UDP DNS server on the given socket.
///
/// Runs until the cancellation token is triggered or the socket errors.
pub async fn serve_udp(
    socket: Arc<UdpSocket>,
    handler: Arc<dyn DnsHandler>,
    cancel: CancellationToken,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut buf = vec![0u8; MAX_UDP_SIZE];
    loop {
        tokio::select! {
            result = socket.recv_from(&mut buf) => {
                let (n, peer) = result?;
                let data = buf[..n].to_vec();
                let handler = handler.clone();
                let socket = socket.clone();

                tokio::spawn(async move {
                    let query = match Message::from_vec(&data) {
                        Ok(q) => q,
                        Err(e) => {
                            debug!(error = %e, "invalid UDP DNS query");
                            return;
                        }
                    };

                    match handler.handle(query).await {
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
                });
            }
            _ = cancel.cancelled() => {
                debug!("UDP server shutting down");
                return Ok(());
            }
        }
    }
}
