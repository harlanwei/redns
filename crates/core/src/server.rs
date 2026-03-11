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
use crate::upstream::UpstreamWrapper;
use async_trait::async_trait;
use hickory_proto::op::{Message, MessageType, ResponseCode};
use std::net::IpAddr;
use std::sync::{Arc, Mutex};
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
/// - If the executable returns Ok but no response is set → REFUSED response.
/// - The response's `RecursionAvailable` flag is always set (forwarder assumption).
pub struct EntryHandler {
    entry: Arc<dyn Executable>,
}

impl EntryHandler {
    /// Creates a new entry handler wrapping the given executable.
    pub fn new(entry: Arc<dyn Executable>) -> Self {
        Self { entry }
    }
}

#[async_trait]
impl DnsHandler for EntryHandler {
    async fn handle(&self, query: Message, meta: QueryMeta) -> PluginResult<Message> {
        // Basic query validation.
        if query.message_type() == MessageType::Response || query.queries().len() != 1 {
            return Ok(refused_response(&query));
        }

        let qname = query
            .queries()
            .first()
            .map(|q| q.name().to_ascii())
            .unwrap_or_default();
        let qtype = query
            .queries()
            .first()
            .map(|q| format!("{:?}", q.query_type()))
            .unwrap_or_default();
        debug!(qname = %qname, qtype = %qtype, id = query.id(), "handling query");

        let start = std::time::Instant::now();
        let mut ctx = Context::new(query.clone());
        ctx.server_meta.from_udp = meta.from_udp;
        ctx.server_meta.client_addr = meta.client_addr;
        ctx.server_meta.url_path = meta.url_path;
        ctx.server_meta.server_name = meta.server_name;
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

        let mut resp = match result {
            Ok(()) => ctx
                .response()
                .cloned()
                .unwrap_or_else(|| refused_response(&query)),
            Err(e) => {
                warn!(error = %e, qname = %qname, elapsed = ?elapsed, "entry handler error");
                servfail_response(&query)
            }
        };

        if let Some(upstream) = selected_upstream {
            if let Some(selected_upstreams) = meta.selected_upstreams.as_ref()
                && let Ok(mut selected) = selected_upstreams.lock()
            {
                let upstream_name = upstream.name().to_string();
                if !selected.iter().any(|name| name == &upstream_name) {
                    selected.push(upstream_name);
                }
            }
            upstream.record_final_selected();
        }

        if served_from_cache
            && let Some(selected_upstreams) = meta.selected_upstreams.as_ref()
            && let Ok(mut selected) = selected_upstreams.lock()
            && !selected.iter().any(|name| name == "__C__")
        {
            selected.push("__C__".to_string());
        }

        debug!(
            qname = %qname,
            rcode = ?resp.response_code(),
            elapsed = ?elapsed,
            "query completed"
        );

        // Forwarder: always set RA.
        resp.set_recursion_available(true);
        Ok(resp)
    }
}

fn refused_response(query: &Message) -> Message {
    let mut resp = Message::new();
    resp.set_id(query.id());
    resp.set_message_type(MessageType::Response);
    resp.set_response_code(ResponseCode::Refused);
    if let Some(q) = query.queries().first() {
        resp.add_query(q.clone());
    }
    resp
}

fn servfail_response(query: &Message) -> Message {
    let mut resp = Message::new();
    resp.set_id(query.id());
    resp.set_message_type(MessageType::Response);
    resp.set_response_code(ResponseCode::ServFail);
    if let Some(q) = query.queries().first() {
        resp.add_query(q.clone());
    }
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
    async fn no_response_returns_refused() {
        let handler = EntryHandler::new(Arc::new(NopExec));
        let resp = handler
            .handle(make_query(), QueryMeta::default())
            .await
            .unwrap();
        assert_eq!(resp.response_code(), ResponseCode::Refused);
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
}
