// Copyright (C) 2026, Harlan Wei
//
// This file is part of redns.

//! Async UDP DNS server with graceful shutdown.

use crate::server::{DnsHandler, QueryMeta};
use hickory_proto::op::Message;
use std::net::IpAddr;
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, warn};

/// Maximum DNS UDP message size.
const MAX_UDP_SIZE: usize = 4096;

/// Upper bound for UDP worker tasks.
const MAX_UDP_WORKERS: usize = 32;

/// Upper bound on concurrently in-flight handler tasks across all UDP workers.
///
/// Each received datagram is dispatched to its own spawned task so a worker's
/// receive loop never blocks on slow upstream resolution. Previously the handler
/// ran inline in the receive loop, which capped in-flight work at the worker
/// count (≤ [`MAX_UDP_WORKERS`]): under load every worker would sit `await`ing an
/// upstream while incoming datagrams piled up in the kernel socket buffer until
/// it overflowed and the kernel silently dropped them — making the server appear
/// slow to respond. Bounding the spawned tasks keeps a flood from spawning
/// handlers (and the upstream/memory load behind them) without limit; at the cap
/// further datagrams are dropped, which is acceptable for UDP and applies
/// backpressure to the flood rather than the server. Mirrors the io_uring
/// backend's `MAX_INFLIGHT_HANDLERS`.
const MAX_INFLIGHT_HANDLERS: usize = 2048;

fn default_udp_workers() -> usize {
    let parallelism = std::thread::available_parallelism()
        .map(std::num::NonZeroUsize::get)
        .unwrap_or(1);
    // A small multiple of CPU parallelism handles socket bursts without
    // unbounded task creation on every packet.
    (parallelism * 2).clamp(1, MAX_UDP_WORKERS)
}

fn build_udp_query(
    query_wire: Arc<Vec<u8>>,
    client_ip: IpAddr,
) -> Result<(Message, QueryMeta), hickory_proto::ProtoError> {
    let query = Message::from_vec(query_wire.as_slice())?;
    let meta = QueryMeta {
        protocol: Some("udp".to_string()),
        from_udp: true,
        client_addr: Some(client_ip),
        url_path: None,
        server_name: None,
        selected_upstreams: None,
        query_wire: Some(query_wire),
    };
    Ok((query, meta))
}

async fn udp_worker(
    socket: Arc<UdpSocket>,
    handler: Arc<dyn DnsHandler>,
    cancel: CancellationToken,
    inflight: Arc<Semaphore>,
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

                // Own the received wire bytes once so downstream handlers can
                // reuse them instead of re-serializing the query later.
                let query_wire = Arc::new(buf[..n].to_vec());
                let (query, meta) = match build_udp_query(query_wire, peer.ip()) {
                    Ok(v) => v,
                    Err(e) => {
                        debug!(error = %e, "invalid UDP DNS query");
                        continue;
                    }
                };

                // Bound the number of in-flight handlers. The permit is acquired
                // here, in the receive loop, with `try_acquire` (never an await):
                // the whole point is to keep draining the socket while earlier
                // queries are still resolving, so we must not block the loop
                // waiting for capacity. At the cap we drop this datagram (the
                // client retries) instead of stalling the worker — stalling is
                // exactly what made the server slow under load when handling ran
                // inline.
                let permit = match inflight.clone().try_acquire_owned() {
                    Ok(p) => p,
                    Err(_) => {
                        debug!(peer = %peer, "UDP in-flight limit reached, dropping query");
                        continue;
                    }
                };

                // Handle the query in its own task so the receive loop is free to
                // accept the next datagram immediately. A panic here unwinds only
                // this task, not the worker.
                let handler = handler.clone();
                let socket = socket.clone();
                tokio::spawn(async move {
                    // Hold the permit for the handler's lifetime; released on drop.
                    let _permit = permit;
                    match handler.handle_udp(query, meta).await {
                        Ok(resp_bytes) => {
                            if let Err(e) = socket.send_to(&resp_bytes, peer).await {
                                warn!(error = %e, peer = %peer, "failed to send UDP response");
                            }
                        }
                        Err(e) => {
                            error!(error = %e, "handler error");
                        }
                    }
                });
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
    let inflight = Arc::new(Semaphore::new(MAX_INFLIGHT_HANDLERS));
    let mut workers = JoinSet::new();

    for _ in 0..worker_count {
        workers.spawn(udp_worker(
            socket.clone(),
            handler.clone(),
            cancel.clone(),
            inflight.clone(),
        ));
    }

    // Supervise the pool. Query handling is spawned into its own task, so a panic
    // in a handler unwinds only that task — not the worker. This supervisor still
    // guards the rarer case of a panic in the receive loop itself: respawn any
    // worker that exits before shutdown so the pool stays at capacity.
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
                    workers.spawn(udp_worker(
                        socket.clone(),
                        handler.clone(),
                        cancel.clone(),
                        inflight.clone(),
                    ));
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

    /// Handler that panics on the first N queries, then succeeds. Used to prove a
    /// panicking handler is isolated to its own spawned task and does not stop the
    /// server from serving subsequent queries.
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

    /// A handler panic must not take the server down. Handlers run in their own
    /// spawned tasks, so a panic unwinds only that task — the receive loop keeps
    /// running and later queries are still served. (The worker pool is also
    /// supervised, so even a panic in the receive loop itself is recovered.)
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn handler_panic_does_not_disrupt_serving() {
        let server_sock = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let server_addr = server_sock.local_addr().unwrap();
        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();

        let calls = Arc::new(AtomicUsize::new(0));
        let handler: Arc<dyn DnsHandler> = Arc::new(PanicThenOk {
            calls: calls.clone(),
            panic_until: 1,
        });
        let cancel = CancellationToken::new();

        // Single worker so behavior is unambiguous: the first (panicking) query
        // must not stop this worker from receiving and serving the next one.
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

        // Second query: the worker kept running past the isolated panic and serves
        // it. Retry briefly to absorb scheduling.
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
        assert!(served, "server stopped serving after an isolated handler panic");

        cancel.cancel();
        let _ = server.await;
    }

    #[test]
    fn build_udp_query_preserves_raw_wire() {
        let query = make_query();
        let expected_wire = Arc::new(query.clone());
        let client_ip: IpAddr = "127.0.0.1".parse().unwrap();

        let (parsed, meta) = build_udp_query(expected_wire.clone(), client_ip).unwrap();

        assert_eq!(parsed.id(), 1);
        assert_eq!(meta.protocol.as_deref(), Some("udp"));
        assert!(meta.from_udp);
        assert_eq!(meta.client_addr, Some(client_ip));
        assert_eq!(meta.query_wire.unwrap().as_slice(), expected_wire.as_slice());
    }

    /// A slow handler must not stall the receive loop. While one query is awaiting
    /// (e.g. a slow upstream), the worker must keep receiving and dispatching
    /// others. With the old "handle inline in the recv loop" design and a single
    /// worker, peak concurrency would be 1; decoupling the handler into a spawned
    /// task lets multiple run at once.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn slow_handler_does_not_block_receive_loop() {
        use tokio::sync::Notify;

        struct GatedHandler {
            in_flight: Arc<AtomicUsize>,
            peak: Arc<AtomicUsize>,
            release: Arc<Notify>,
        }

        #[async_trait]
        impl DnsHandler for GatedHandler {
            async fn handle(&self, query: Message, _meta: QueryMeta) -> PluginResult<Message> {
                let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                self.peak.fetch_max(now, Ordering::SeqCst);
                // Hold the request open until the test releases it, simulating a
                // slow upstream.
                self.release.notified().await;
                self.in_flight.fetch_sub(1, Ordering::SeqCst);

                let mut resp = Message::new();
                resp.set_id(query.id());
                resp.set_message_type(MessageType::Response);
                if let Some(q) = query.queries().first() {
                    resp.add_query(q.clone());
                }
                Ok(resp)
            }
        }

        let server_sock = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let server_addr = server_sock.local_addr().unwrap();
        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();

        let in_flight = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(Notify::new());
        let handler: Arc<dyn DnsHandler> = Arc::new(GatedHandler {
            in_flight: in_flight.clone(),
            peak: peak.clone(),
            release: release.clone(),
        });
        let cancel = CancellationToken::new();

        // A single worker: with the old inline handling this caps concurrency at 1.
        let server = tokio::spawn(serve_udp_with_workers(
            server_sock,
            handler,
            cancel.clone(),
            1,
        ));

        let query = make_query();
        for _ in 0..4 {
            client.send_to(&query, server_addr).await.unwrap();
        }

        // Wait for the queries to pile into handle() concurrently.
        let mut observed = 0;
        for _ in 0..50 {
            observed = peak.load(Ordering::SeqCst);
            if observed >= 2 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }

        assert!(
            observed >= 2,
            "receive loop stalled on a slow handler (peak in-flight = {observed})"
        );

        // Release the gated handlers and shut down.
        release.notify_waiters();
        cancel.cancel();
        let _ = server.await;
    }
}
