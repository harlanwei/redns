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

use hickory_proto::op::{Edns, Message, Query};
use std::collections::{HashMap, HashSet};
use std::net::IpAddr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Instant;

/// Default EDNS0 UDP payload size.
const EDNS0_SIZE: u16 = 1200;

/// Context KV key for the upstream selected by a forward stage.
pub const KV_SELECTED_UPSTREAM: u32 = 0x5244_4e53;

/// Context mark indicating the response was served from cache.
pub const MARK_CACHE_HIT: u32 = 0x4341_4348;

/// Global monotonically increasing context ID.
static CONTEXT_UID: AtomicU32 = AtomicU32::new(0);

/// Metadata from the server that received the query.
/// Read-only once set.
#[derive(Debug, Clone, Default)]
pub struct ServerMeta {
    /// Whether the query was received over UDP.
    pub from_udp: bool,
    /// Optional client address.
    pub client_addr: Option<IpAddr>,
    /// Optional server name (e.g. for DoH/DoT).
    pub server_name: Option<String>,
    /// Optional URL path (for DoH).
    pub url_path: Option<String>,
}

/// A query context that passes through the plugin pipeline.
///
/// All methods assume single-threaded access — the context is **not**
/// `Sync`. Pass it through the pipeline by value or behind `&mut`.
#[derive(Debug)]
pub struct Context {
    id: u32,
    start_time: Instant,

    /// Metadata from the server that accepted this query.
    pub server_meta: ServerMeta,

    /// The DNS query message. Always contains at least one question and
    /// has an OPT pseudo-record for EDNS0.
    query: Message,

    /// The original EDNS0 OPT sent by the client (if any).
    client_edns: Option<Edns>,

    /// Optional original query wire bytes as received by the server.
    query_wire: Option<std::sync::Arc<Vec<u8>>>,

    /// The DNS response (may be `None` until a plugin sets it).
    response: Option<Message>,

    /// A key-value store for passing arbitrary data between plugins.
    kv: HashMap<u32, Box<dyn std::any::Any + Send + Sync>>,

    /// A set of boolean marks for fast flag checks.
    marks: HashSet<u32>,
}

impl Context {
    /// Creates a new `Context` that takes ownership of the given query message.
    ///
    /// If the incoming message contains an existing EDNS0 OPT record, it is
    /// saved as `client_edns` and replaced with a fresh OPT record. If none
    /// is present a new one is appended.
    pub fn new(mut query: Message) -> Self {
        let id = CONTEXT_UID.fetch_add(1, Ordering::Relaxed) + 1;

        // Extract client EDNS and replace with our own.
        let client_edns: Option<Edns> = query.extensions().as_ref().cloned();
        let mut new_edns = Edns::new();
        new_edns.set_max_payload(EDNS0_SIZE);

        // Copy the DO bit from the client (RFC 3225 §3).
        if let Some(ref ce) = client_edns {
            if ce.flags().dnssec_ok {
                new_edns.set_dnssec_ok(true);
            }
        }
        query.set_edns(new_edns);

        Context {
            id,
            start_time: Instant::now(),
            server_meta: ServerMeta::default(),
            query,
            client_edns,
            query_wire: None,
            response: None,
            kv: HashMap::new(),
            marks: HashSet::new(),
        }
    }

    // ── Accessors ────────────────────────────────────────────────

    /// Returns the unique context id (not the DNS message id).
    pub fn id(&self) -> u32 {
        self.id
    }

    /// Returns the time this context was created.
    pub fn start_time(&self) -> Instant {
        self.start_time
    }

    /// Returns a reference to the query message.
    pub fn query(&self) -> &Message {
        &self.query
    }

    /// Returns a mutable reference to the query message.
    pub fn query_mut(&mut self) -> &mut Message {
        &mut self.query
    }

    /// Returns the first query question, if any.
    pub fn question(&self) -> Option<&Query> {
        self.query.queries().first()
    }

    /// Returns the EDNS0 OPT sent by the client (may be `None`).
    pub fn client_edns(&self) -> Option<&Edns> {
        self.client_edns.as_ref()
    }

    /// Sets optional original query wire bytes captured by the ingress server.
    pub fn set_query_wire(&mut self, wire: Option<std::sync::Arc<Vec<u8>>>) {
        self.query_wire = wire;
    }

    /// Returns original query wire bytes when available.
    pub fn query_wire(&self) -> Option<&std::sync::Arc<Vec<u8>>> {
        self.query_wire.as_ref()
    }

    // ── Response ─────────────────────────────────────────────────

    /// Sets the response message. Pass `None` to clear any existing response.
    pub fn set_response(&mut self, resp: Option<Message>) {
        self.response = resp;
    }

    /// Returns a reference to the current response, if set.
    pub fn response(&self) -> Option<&Message> {
        self.response.as_ref()
    }

    /// Returns a mutable reference to the current response, if set.
    pub fn response_mut(&mut self) -> Option<&mut Message> {
        self.response.as_mut()
    }

    // ── Key-Value Store ──────────────────────────────────────────

    /// Stores an arbitrary value under key `k`.
    pub fn store_value<V: 'static + Send + Sync>(&mut self, k: u32, v: V) {
        self.kv.insert(k, Box::new(v));
    }

    /// Retrieves a reference to the value stored under key `k`.
    pub fn get_value<V: 'static>(&self, k: u32) -> Option<&V> {
        self.kv.get(&k).and_then(|b| b.downcast_ref::<V>())
    }

    /// Removes the value stored under key `k`.
    pub fn delete_value(&mut self, k: u32) {
        self.kv.remove(&k);
    }

    // ── Marks ────────────────────────────────────────────────────

    /// Sets a boolean mark `m` on this context.
    pub fn set_mark(&mut self, m: u32) {
        self.marks.insert(m);
    }

    /// Returns `true` if mark `m` has been set.
    pub fn has_mark(&self, m: u32) -> bool {
        self.marks.contains(&m)
    }

    /// Removes mark `m`.
    pub fn delete_mark(&mut self, m: u32) {
        self.marks.remove(&m);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hickory_proto::op::{Message, MessageType, OpCode};
    use hickory_proto::rr::{Name, RecordType};

    fn make_query() -> Message {
        let mut msg = Message::new();
        msg.set_id(1234)
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

    #[test]
    fn context_created_with_id_and_time() {
        let ctx = Context::new(make_query());
        assert!(ctx.id() > 0);
        assert!(ctx.start_time().elapsed().as_secs() < 1);
    }

    #[test]
    fn context_query_preserved() {
        let ctx = Context::new(make_query());
        let q = ctx.question().expect("should have a question");
        assert_eq!(q.name().to_ascii(), "example.com.");
        assert_eq!(q.query_type(), RecordType::A);
    }

    #[test]
    fn context_response_lifecycle() {
        let mut ctx = Context::new(make_query());
        assert!(ctx.response().is_none());

        let resp = Message::new();
        ctx.set_response(Some(resp));
        assert!(ctx.response().is_some());

        ctx.set_response(None);
        assert!(ctx.response().is_none());
    }

    #[test]
    fn context_kv_store() {
        let mut ctx = Context::new(make_query());
        ctx.store_value(1, String::from("hello"));
        assert_eq!(ctx.get_value::<String>(1), Some(&String::from("hello")));

        ctx.delete_value(1);
        assert_eq!(ctx.get_value::<String>(1), None);
    }

    #[test]
    fn context_marks() {
        let mut ctx = Context::new(make_query());
        assert!(!ctx.has_mark(42));

        ctx.set_mark(42);
        assert!(ctx.has_mark(42));

        ctx.delete_mark(42);
        assert!(!ctx.has_mark(42));
    }
}
