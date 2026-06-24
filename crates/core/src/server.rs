// Copyright (C) 2026, Harlan Wei
//
// This file is part of redns.
//
// redns is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// redns is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

//! DNS server handler trait and entry handler.

use crate::context::{Context, KV_SELECTED_UPSTREAM, MARK_CACHE_HIT};
use crate::plugin::{Executable, PluginResult};
use crate::system_dns::system_fallback_resolve;
use crate::upstream::UpstreamWrapper;
use async_trait::async_trait;
use hickory_proto::op::{Message, MessageType, ResponseCode};
use parking_lot::Mutex;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, warn};

/// Default query timeout.
pub const DEFAULT_QUERY_TIMEOUT: Duration = Duration::from_secs(5);

/// Metadata about an incoming DNS query.
#[derive(Debug, Clone, Default)]
pub struct QueryMeta {
    /// Transport protocol label (e.g. "udp", "tcp", "doh").
    pub protocol: Option<String>,
    /// Whether the query was received over UDP.
    pub from_udp: bool,
    /// The client's IP address.
    pub client_addr: Option<IpAddr>,
    /// The URL path (for DoH requests).
    pub url_path: Option<String>,
    /// The TLS server name (SNI, for DoT/DoH).
    pub server_name: Option<String>,
    /// Optional selected upstream collector for query observers.
    pub selected_upstreams: Option<Arc<Mutex<Vec<String>>>>,
    /// Optional original DNS query wire bytes.
    pub query_wire: Option<Arc<Vec<u8>>>,
}

/// A DNS handler that processes a query and returns a response.
#[async_trait]
pub trait DnsHandler: Send + Sync {
    /// Handle a DNS query. Returns the response message.
    async fn handle(&self, query: Message, meta: QueryMeta) -> PluginResult<Message>;
}

/// Wraps a sequence [`Executable`] as a [`DnsHandler`].
///
/// - If the executable returns an error → SERVFAIL response.
/// - If the executable returns Ok but sets no response → NOERROR response.
/// - The response's `RecursionAvailable` flag is always set (forwarder assumption).
/// - When `best_effort` is enabled, the query is retried against the
///   WAN-assigned system DNS as a last resort — but only when every upstream
///   either returned SERVFAIL or could not be reached at all. Any other result
///   (NOERROR, NXDOMAIN, REFUSED, …) is a real answer and is never second-guessed.
pub struct EntryHandler {
    entry: Arc<dyn Executable>,
    best_effort: bool,
}

impl EntryHandler {
    /// Creates a new entry handler wrapping the given executable.
    pub fn new(entry: Arc<dyn Executable>) -> Self {
        Self {
            entry,
            best_effort: false,
        }
    }

    /// Creates a new entry handler with best-effort system DNS fallback.
    pub fn with_best_effort(entry: Arc<dyn Executable>, best_effort: bool) -> Self {
        Self {
            entry,
            best_effort,
        }
    }
}

#[async_trait]
impl DnsHandler for EntryHandler {
    async fn handle(&self, query: Message, meta: QueryMeta) -> PluginResult<Message> {
        // Basic query validation.
        if query.message_type() == MessageType::Response || query.queries().len() != 1 {
            return Ok(servfail_response(&query));
        }

        // Safe: validated above that exactly one question exists. Binding
        // `qname` as a `&Name` (not a String) keeps it allocation-free; `query`
        // is never mutated, so this borrow lives for the whole handler. The
        // `tracing` macros only evaluate field expressions when the level is
        // enabled, so no `to_ascii`/`format!` allocation happens on the hot
        // path when debug logging is off.
        let question = &query.queries()[0];
        let qname = question.name();
        debug!(
            qname = %qname,
            qtype = ?question.query_type(),
            id = query.id(),
            "handling query"
        );

        let start = std::time::Instant::now();
        let mut ctx = Context::new(query.clone());
        ctx.server_meta.from_udp = meta.from_udp;
        ctx.server_meta.client_addr = meta.client_addr;
        ctx.server_meta.url_path = meta.url_path;
        ctx.server_meta.server_name = meta.server_name;
        ctx.set_query_wire(meta.query_wire.clone());
        let result = self.entry.exec(&mut ctx).await;
        let elapsed = start.elapsed();
        let selected_upstream = if result.is_ok() && ctx.response().is_some() {
            ctx.get_value::<Arc<UpstreamWrapper>>(KV_SELECTED_UPSTREAM)
                .cloned()
        } else {
            None
        };
        let served_from_cache =
            result.is_ok() && ctx.response().is_some() && ctx.has_mark(MARK_CACHE_HIT);

        // Best-effort system DNS is a *last resort*: it fires only when the
        // entire upstream strategy produced nothing usable — every upstream
        // either answered SERVFAIL or could not be reached at all.
        //
        // - A chain `Err` means no response came back from any upstream
        //   (timeouts / transport failures across the board, e.g. the
        //   "no valid response from primary or secondary" exhaustion). That is
        //   the "connection problems with all upstreams" case.
        // - An `Ok(())` carrying a SERVFAIL rcode means the upstreams were
        //   reached but all of them returned SERVFAIL.
        //
        // Any other outcome — NOERROR, NXDOMAIN, REFUSED, etc. — is a real
        // answer from an upstream and must never be second-guessed against the
        // system (ISP) resolver, otherwise queries would leak out of the
        // encrypted upstreams on ordinary results.
        let all_upstreams_failed = match &result {
            Err(_) => true,
            Ok(()) => ctx
                .response()
                .map(|r| r.response_code() == ResponseCode::ServFail)
                .unwrap_or(false),
        };

        let mut resp = match result {
            Ok(()) => ctx
                .response()
                .cloned()
                .unwrap_or_else(|| noerror_response(&query)),
            Err(e) => {
                warn!(error = %e, qname = %qname, elapsed = ?elapsed, "entry handler error");
                servfail_response(&query)
            }
        };

        if all_upstreams_failed && self.best_effort {
            match tokio::time::timeout(
                DEFAULT_QUERY_TIMEOUT,
                system_fallback_resolve(&query),
            )
            .await
            {
                Ok(Ok(Some(system_resp))) => {
                    debug!(qname = %qname, "using system DNS fallback response");
                    resp = system_resp;
                }
                Ok(Ok(None)) => {
                    debug!(qname = %qname, "system DNS fallback unavailable");
                }
                Ok(Err(e)) => {
                    debug!(error = %e, qname = %qname, "system DNS fallback error");
                }
                Err(_) => {
                    debug!(qname = %qname, "system DNS fallback timed out");
                }
            }
        }

        if let Some(upstream) = selected_upstream {
            if let Some(selected_upstreams) = meta.selected_upstreams.as_ref() {
                let mut selected = selected_upstreams.lock();
                let upstream_name = upstream.name().to_string();
                if !selected.iter().any(|name| name == &upstream_name) {
                    selected.push(upstream_name);
                }
            }
            upstream.record_final_selected();
        }

        if served_from_cache
            && let Some(selected_upstreams) = meta.selected_upstreams.as_ref()
        {
            let mut selected = selected_upstreams.lock();
            if !selected.iter().any(|name| name == "__C__") {
                selected.push("__C__".to_string());
            }
        }

        debug!(
            qname = %qname,
            rcode = ?resp.response_code(),
            elapsed = ?elapsed,
            "query completed"
        );

        // Forwarder: always set RA.
        resp.set_recursion_available(true);

        // For UDP, ensure the response fits the client's advertised buffer. If
        // it doesn't, return a truncated response (TC bit set, answer sections
        // dropped) so the client retries over TCP (RFC 1035 §4.2.1). TCP/DoH
        // are length-prefixed/streamed and need no truncation.
        if meta.from_udp {
            resp = truncate_for_udp(resp, &query);
        }

        Ok(resp)
    }
}

/// Maximum UDP response size assumed when the client advertises no EDNS0 buffer.
const MIN_UDP_RESPONSE_SIZE: usize = 512;

/// Ensures a UDP response fits the client's advertised buffer.
///
/// The limit is the client's EDNS0 `max_payload` (read from the original
/// query), floored at the classic 512-byte DNS limit. If the wire form exceeds
/// it, the response is truncated: the TC bit is set and answer sections are
/// dropped, signalling the client to retry over TCP. The `RecursionAvailable`
/// flag is re-applied since [`Message::truncate`] rebuilds the message.
fn truncate_for_udp(resp: Message, query: &Message) -> Message {
    let max_payload = query
        .extensions()
        .as_ref()
        .map(|e| e.max_payload() as usize)
        .filter(|&p| p >= MIN_UDP_RESPONSE_SIZE)
        .unwrap_or(MIN_UDP_RESPONSE_SIZE);

    match resp.to_vec() {
        Ok(wire) if wire.len() > max_payload => {
            let mut truncated = resp.truncate();
            truncated.set_recursion_available(true);
            truncated
        }
        _ => resp,
    }
}

fn empty_response(query: &Message) -> Message {
    let mut resp = Message::new();
    resp.set_id(query.id());
    resp.set_message_type(MessageType::Response);
    if let Some(q) = query.queries().first() {
        resp.add_query(q.clone());
    }
    resp
}

fn noerror_response(query: &Message) -> Message {
    let mut resp = empty_response(query);
    resp.set_response_code(ResponseCode::NoError);
    resp
}

fn servfail_response(query: &Message) -> Message {
    let mut resp = empty_response(query);
    resp.set_response_code(ResponseCode::ServFail);
    resp
}

#[cfg(test)]
mod tests {
    use super::*;
    use hickory_proto::op::{Message, MessageType, OpCode, Query};
    use hickory_proto::rr::{Name, RecordType};

    fn make_query() -> Message {
        let mut msg = Message::new();
        msg.set_id(42)
            .set_message_type(MessageType::Query)
            .set_op_code(OpCode::Query);
        msg.add_query({
            let mut q = Query::new();
            q.set_name(Name::from_ascii("example.com.").unwrap())
                .set_query_type(RecordType::A);
            q
        });
        msg
    }

    struct NopExec;
    #[async_trait]
    impl Executable for NopExec {
        async fn exec(&self, _ctx: &mut Context) -> PluginResult<()> {
            Ok(())
        }
    }

    struct FailExec;
    #[async_trait]
    impl Executable for FailExec {
        async fn exec(&self, _ctx: &mut Context) -> PluginResult<()> {
            Err("test error".into())
        }
    }

    struct SetResponseExec;
    #[async_trait]
    impl Executable for SetResponseExec {
        async fn exec(&self, ctx: &mut Context) -> PluginResult<()> {
            let mut resp = Message::new();
            resp.set_id(ctx.query().id());
            resp.set_message_type(MessageType::Response);
            resp.set_response_code(ResponseCode::NoError);
            ctx.set_response(Some(resp));
            Ok(())
        }
    }

    #[tokio::test]
    async fn no_response_returns_noerror() {
        let handler = EntryHandler::new(Arc::new(NopExec));
        let resp = handler
            .handle(make_query(), QueryMeta::default())
            .await
            .unwrap();
        assert_eq!(resp.response_code(), ResponseCode::NoError);
        assert!(resp.recursion_available());
    }

    #[tokio::test]
    async fn error_returns_servfail() {
        let handler = EntryHandler::new(Arc::new(FailExec));
        let resp = handler
            .handle(make_query(), QueryMeta::default())
            .await
            .unwrap();
        assert_eq!(resp.response_code(), ResponseCode::ServFail);
        assert!(resp.recursion_available());
    }

    #[tokio::test]
    async fn success_returns_actual_response() {
        let handler = EntryHandler::new(Arc::new(SetResponseExec));
        let resp = handler
            .handle(make_query(), QueryMeta::default())
            .await
            .unwrap();
        assert_eq!(resp.response_code(), ResponseCode::NoError);
        assert!(resp.recursion_available());
    }

    /// Executable that stuffs the response with enough answer records to blow
    /// past the 512-byte classic UDP limit.
    struct BigResponseExec;
    #[async_trait]
    impl Executable for BigResponseExec {
        async fn exec(&self, ctx: &mut Context) -> PluginResult<()> {
            use hickory_proto::rr::{rdata::TXT, Name, RData, Record};
            let mut resp = Message::new();
            resp.set_id(ctx.query().id());
            resp.set_message_type(MessageType::Response);
            resp.set_response_code(ResponseCode::NoError);
            if let Some(q) = ctx.query().queries().first() {
                resp.add_query(q.clone());
            }
            let name = Name::from_ascii("example.com.").unwrap();
            for _ in 0..40 {
                resp.add_answer(Record::from_rdata(
                    name.clone(),
                    300,
                    RData::TXT(TXT::new(vec!["x".repeat(60)])),
                ));
            }
            ctx.set_response(Some(resp));
            Ok(())
        }
    }

    #[tokio::test]
    async fn oversized_udp_response_is_truncated() {
        let handler = EntryHandler::new(Arc::new(BigResponseExec));
        let meta = QueryMeta {
            from_udp: true,
            ..Default::default()
        };
        let resp = handler.handle(make_query(), meta).await.unwrap();
        assert!(resp.truncated(), "TC bit should be set");
        assert!(resp.answers().is_empty(), "answers should be dropped");
        assert!(resp.recursion_available());
    }

    #[tokio::test]
    async fn oversized_tcp_response_is_not_truncated() {
        let handler = EntryHandler::new(Arc::new(BigResponseExec));
        // from_udp defaults to false → TCP path, no truncation.
        let resp = handler
            .handle(make_query(), QueryMeta::default())
            .await
            .unwrap();
        assert!(!resp.truncated(), "TCP response must not be truncated");
        assert_eq!(resp.answers().len(), 40);
    }
}
