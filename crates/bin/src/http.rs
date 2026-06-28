// Copyright (C) 2026, Harlan Wei
//
// This file is part of redns.

//! Minimal helpers shared by the hand-rolled HTTP/1.1 endpoints (the metrics
//! API and the dashboard).
//!
//! These servers expose a small, fixed set of routes, so they parse requests by
//! hand rather than pulling in a full HTTP stack. The one part that genuinely
//! needs care is *reading* the request: a single `read()` can return only part
//! of the request (TCP segmentation) or — for a slow or hostile client — never
//! deliver the end of the headers at all. [`read_request_head`] handles those
//! cases so each handler can work with a complete header block.

use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt};

type DynError = Box<dyn std::error::Error + Send + Sync>;

/// Maximum size of an HTTP request head (request line + headers). Requests whose
/// headers exceed this are rejected rather than buffered without bound.
pub const MAX_REQUEST_HEAD: usize = 64 * 1024;

/// Maximum time spent reading a single request's head before giving up, so a
/// stalled connection cannot pin the per-connection task indefinitely.
pub const REQUEST_READ_TIMEOUT: Duration = Duration::from_secs(15);

/// Reads an HTTP/1.1 request head — everything up to and including the blank
/// line (`\r\n\r\n`) that terminates the headers.
///
/// Unlike a bare `stream.read()`, this:
/// - loops until the header terminator is seen, so a request split across
///   multiple TCP segments is reassembled correctly;
/// - caps the buffered head at [`MAX_REQUEST_HEAD`] so a client that never sends
///   the terminator can't drive unbounded memory growth;
/// - bounds the whole read with [`REQUEST_READ_TIMEOUT`].
///
/// Returns `Ok(None)` when the peer closes the connection without sending
/// anything (e.g. a bare TCP health probe). On success returns the raw head
/// bytes together with any bytes already read past the terminator — the start of
/// the request body, if one was pipelined into the same segment.
pub async fn read_request_head<R>(stream: &mut R) -> Result<Option<(Vec<u8>, Vec<u8>)>, DynError>
where
    R: AsyncRead + Unpin,
{
    let mut buf: Vec<u8> = Vec::with_capacity(2048);
    let mut chunk = [0u8; 4096];

    loop {
        if let Some(pos) = find_header_end(&buf) {
            // Split the body bytes (if any) off the head; keep the head without
            // the trailing terminator-free remainder.
            let body = buf.split_off(pos + 4);
            return Ok(Some((buf, body)));
        }

        if buf.len() > MAX_REQUEST_HEAD {
            return Err("HTTP request head exceeds maximum size".into());
        }

        let n = match tokio::time::timeout(REQUEST_READ_TIMEOUT, stream.read(&mut chunk)).await {
            Ok(Ok(0)) => {
                // EOF. Nothing buffered → the peer simply hung up; treat as an
                // empty request. A partial head means the request was truncated.
                if buf.is_empty() {
                    return Ok(None);
                }
                return Err("connection closed before HTTP headers completed".into());
            }
            Ok(Ok(n)) => n,
            Ok(Err(e)) => return Err(Box::new(e)),
            Err(_) => return Err("timed out reading HTTP request head".into()),
        };
        buf.extend_from_slice(&chunk[..n]);
    }
}

/// Returns the offset of the `\r\n\r\n` header terminator within `buf`, if present.
fn find_header_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;

    #[tokio::test]
    async fn reads_complete_request_in_one_segment() {
        let (mut client, mut server) = tokio::io::duplex(4096);
        client
            .write_all(b"GET /api/cache HTTP/1.1\r\nHost: x\r\n\r\n")
            .await
            .unwrap();
        drop(client);

        let (head, body) = read_request_head(&mut server).await.unwrap().unwrap();
        assert!(head.starts_with(b"GET /api/cache HTTP/1.1\r\n"));
        assert!(head.ends_with(b"\r\n\r\n"));
        assert!(body.is_empty());
    }

    #[tokio::test]
    async fn reassembles_request_split_across_segments() {
        let (mut client, mut server) = tokio::io::duplex(4096);
        let writer = tokio::spawn(async move {
            client.write_all(b"GET /api/lo").await.unwrap();
            client.flush().await.unwrap();
            // Force the reader to loop on a partial head before the rest arrives.
            tokio::time::sleep(Duration::from_millis(20)).await;
            client.write_all(b"gs HTTP/1.1\r\nHost: x\r\n\r\n").await.unwrap();
            drop(client);
        });

        let (head, _body) = read_request_head(&mut server).await.unwrap().unwrap();
        assert!(head.starts_with(b"GET /api/logs HTTP/1.1\r\n"));
        writer.await.unwrap();
    }

    #[tokio::test]
    async fn returns_body_bytes_pipelined_with_head() {
        let (mut client, mut server) = tokio::io::duplex(4096);
        client
            .write_all(b"POST /api/logs/clear HTTP/1.1\r\nContent-Length: 3\r\n\r\nabc")
            .await
            .unwrap();
        drop(client);

        let (head, body) = read_request_head(&mut server).await.unwrap().unwrap();
        assert!(head.starts_with(b"POST /api/logs/clear"));
        assert_eq!(body, b"abc");
    }

    #[tokio::test]
    async fn empty_connection_returns_none() {
        let (client, mut server) = tokio::io::duplex(64);
        drop(client); // peer hangs up without sending anything
        let result = read_request_head(&mut server).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn oversized_head_is_rejected() {
        let (mut client, mut server) = tokio::io::duplex(256 * 1024);
        // Send more than MAX_REQUEST_HEAD bytes with no header terminator.
        let junk = vec![b'a'; MAX_REQUEST_HEAD + 4096];
        let writer = tokio::spawn(async move {
            let _ = client.write_all(&junk).await;
            // Hold the connection open; the reader must error on the size cap,
            // not wait for EOF.
            tokio::time::sleep(Duration::from_millis(200)).await;
            drop(client);
        });

        let err = read_request_head(&mut server)
            .await
            .expect_err("oversized head must be rejected");
        assert!(err.to_string().contains("maximum size"));
        writer.await.unwrap();
    }
}
