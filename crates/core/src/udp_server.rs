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
    serve_udp_with_workers(socket, handler, cancel, default_udp_workers()).await
}

/// Worker-pool core of [`serve_udp`], parameterized on the pool size for tests.
async fn serve_udp_with_workers(
    socket: Arc<UdpSocket>,
    handler: Arc<dyn DnsHandler>,
    cancel: CancellationToken,
    worker_count: usize,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut workers = JoinSet::new();

    for _ in 0..worker_count {
        workers.spawn(udp_worker(socket.clone(), handler.clone(), cancel.clone()));
    }

    // Supervise the pool. The handler runs inline in each worker, so a panic in
    // a single query unwinds that worker task. Without supervision the fixed
    // pool would erode one panic at a time until UDP stops serving entirely.
    // Respawn workers that exit before shutdown so the pool stays at capacity.
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            joined = workers.join_next() => {
                if cancel.is_cancelled() {
                    break;
                }
                // A worker exited while still serving. A clean return is not
                // expected here (workers only return on cancellation, handled
                // above); a JoinError means it panicked or was aborted. Either
                // way, refill the pool back to capacity.
                if let Some(Err(e)) = &joined {
                    warn!(error = %e, "UDP worker terminated abnormally, respawning");
                }
                while workers.len() < worker_count {
                    workers.spawn(udp_worker(socket.clone(), handler.clone(), cancel.clone()));
                }
            }
        }
    }

    debug!(workers = worker_count, "UDP server shutting down");
    workers.abort_all();
    while workers.join_next().await.is_some() {}

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plugin::PluginResult;
    use async_trait::async_trait;
    use hickory_proto::op::{Message, MessageType, OpCode, Query};
    use hickory_proto::rr::{Name, RecordType};
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Handler that panics on the first N queries, then succeeds. Used to prove
    /// the worker pool recovers from handler panics instead of eroding.
    struct PanicThenOk {
        calls: Arc<AtomicUsize>,
        panic_until: usize,
    }

    #[async_trait]
    impl DnsHandler for PanicThenOk {
        async fn handle(&self, query: Message, _meta: QueryMeta) -> PluginResult<Message> {
            let n = self.calls.fetch_add(1, Ordering::SeqCst);
            if n < self.panic_until {
                panic!("simulated handler panic #{n}");
            }
            let mut resp = Message::new();
            resp.set_id(query.id());
            resp.set_message_type(MessageType::Response);
            if let Some(q) = query.queries().first() {
                resp.add_query(q.clone());
            }
            Ok(resp)
        }
    }

    fn make_query() -> Vec<u8> {
        let mut msg = Message::new();
        msg.set_id(1)
            .set_message_type(MessageType::Query)
            .set_op_code(OpCode::Query);
        msg.add_query({
            let mut q = Query::new();
            q.set_name(Name::from_ascii("example.com.").unwrap())
                .set_query_type(RecordType::A);
            q
        });
        msg.to_vec().unwrap()
    }

    /// A handler panic must not permanently shrink the worker pool. With a single
    /// worker, the first query panics and unwinds that worker; the supervisor must
    /// respawn it so a subsequent query is still served.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn handler_panic_respawns_worker() {
        let server_sock = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let server_addr = server_sock.local_addr().unwrap();
        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();

        let calls = Arc::new(AtomicUsize::new(0));
        let handler: Arc<dyn DnsHandler> = Arc::new(PanicThenOk {
            calls: calls.clone(),
            panic_until: 1,
        });
        let cancel = CancellationToken::new();

        // Single worker so the first (panicking) query takes down the only worker;
        // recovery is then unambiguous.
        let server = tokio::spawn(serve_udp_with_workers(
            server_sock,
            handler,
            cancel.clone(),
            1,
        ));

        let query = make_query();

        // First query: triggers the panic. No response expected.
        client.send_to(&query, server_addr).await.unwrap();
        let mut buf = [0u8; 4096];
        let first = tokio::time::timeout(
            std::time::Duration::from_millis(300),
            client.recv_from(&mut buf),
        )
        .await;
        assert!(first.is_err(), "panicking query should not produce a reply");

        // Second query: the pool must have respawned the worker and served it.
        // Retry briefly to absorb the respawn race.
        let mut served = false;
        for _ in 0..20 {
            client.send_to(&query, server_addr).await.unwrap();
            if tokio::time::timeout(
                std::time::Duration::from_millis(150),
                client.recv_from(&mut buf),
            )
            .await
            .is_ok()
            {
                served = true;
                break;
            }
        }
        assert!(served, "worker pool failed to recover after a handler panic");

        cancel.cancel();
        let _ = server.await;
    }
}
