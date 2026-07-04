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

use crate::plugin::PluginResult;
use hickory_proto::op::{Edns, Message, Query};
use std::collections::{HashMap, HashSet};
use std::net::IpAddr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Instant;

/// Default EDNS0 UDP payload size.
///
/// This is the value used when no explicit `edns_udp_size` is set in the config.
/// 1232 bytes is a safe default that avoids IPv6 fragmentation on typical MTUs
/// while providing more space than the classic 512-byte limit. Configurable via
/// the `edns_udp_size` field in the top-level config.
pub const DEFAULT_EDNS0_SIZE: u16 = 1232;

/// Context KV key for the upstream selected by a forward stage.
pub const KV_SELECTED_UPSTREAM: u32 = 0x5244_4e53;

/// Context mark indicating the response was served from cache.
pub const MARK_CACHE_HIT: u32 = 0x4341_4348;

/// Global monotonically increasing context ID.
static CONTEXT_UID: AtomicU32 = AtomicU32::new(0);

/// Maximum number of chain nodes a single query may execute.
///
/// `jump`/`goto` can transfer control between chains, so a misconfigured or
/// hostile config (e.g. chain A `goto`s B and B `goto`s A) could otherwise
/// recurse forever — overflowing the stack and growing `jump_back` without
/// bound. This budget is charged per node visited across the whole query and
/// caps that work. The limit is generous relative to any sane config.
pub const MAX_CHAIN_STEPS: u32 = 10_000;

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

    /// The parsed DNS response (may be empty until a plugin sets it or a
    /// wire response is decoded on demand).
    response: OnceLock<Message>,

    /// Optional pre-serialized DNS response wire bytes.
    response_wire: Option<Vec<u8>>,

    /// Hot-path storage for the selected upstream. Keeping this out of the
    /// generic KV map avoids allocating a HashMap for the common forward path.
    selected_upstream: Option<Arc<crate::upstream::UpstreamWrapper>>,

    /// A key-value store for passing arbitrary data between plugins.
    kv: Option<HashMap<u32, Box<dyn std::any::Any + Send + Sync>>>,

    /// A set of boolean marks for fast flag checks.
    marks: Option<HashSet<u32>>,

    /// Hot-path storage for cache-hit marking.
    cache_hit: bool,

    /// Remaining chain-node execution budget for this query (see
    /// [`MAX_CHAIN_STEPS`]). Decremented as the chain walker visits nodes.
    steps_remaining: u32,

    /// EDNS0 UDP payload size to use when sending queries to upstreams.
    edns_udp_size: u16,
}

impl Context {
    /// Creates a new `Context` with the default EDNS UDP size.
    ///
    /// If the incoming message contains an existing EDNS0 OPT record, it is
    /// saved as `client_edns` and replaced with a fresh OPT record. If none
    /// is present a new one is appended.
    ///
    /// The context `id` is a monotonically increasing `u32` that wraps after
    /// ~4B queries. It is unique within a session until wrap, but not globally
    /// unique across restarts or forever.
    pub fn new(query: Message) -> Self {
        Self::with_edns_size(query, DEFAULT_EDNS0_SIZE)
    }

    /// Creates a new `Context` with a custom EDNS UDP payload size.
    ///
    /// The `edns_udp_size` is the buffer size advertised to upstream DNS servers
    /// via the EDNS0 OPT record. Larger values allow bigger responses but may
    /// trigger fragmentation on some networks.
    pub fn with_edns_size(mut query: Message, edns_udp_size: u16) -> Self {
        let id = CONTEXT_UID.fetch_add(1, Ordering::Relaxed) + 1;

        // Extract client EDNS and replace with our own.
        let client_edns: Option<Edns> = query.extensions().as_ref().cloned();
        let mut new_edns = Edns::new();
        new_edns.set_max_payload(edns_udp_size);

        // Copy the DO bit from the client (RFC 3225 §3).
        if let Some(ref ce) = client_edns
            && ce.flags().dnssec_ok
        {
            new_edns.set_dnssec_ok(true);
        }
        query.set_edns(new_edns);

        Context {
            id,
            start_time: Instant::now(),
            server_meta: ServerMeta::default(),
            query,
            client_edns,
            query_wire: None,
            response: OnceLock::new(),
            response_wire: None,
            selected_upstream: None,
            kv: None,
            marks: None,
            cache_hit: false,
            steps_remaining: MAX_CHAIN_STEPS,
            edns_udp_size,
        }
    }

    /// Charges one chain-node execution against this query's step budget.
    ///
    /// Returns `Err` once the budget is exhausted, which aborts the query
    /// rather than letting a cyclic `jump`/`goto` config recurse forever. The
    /// chain walker calls this before visiting each node.
    pub fn charge_step(&mut self) -> PluginResult<()> {
        match self.steps_remaining.checked_sub(1) {
            Some(rem) => {
                self.steps_remaining = rem;
                Ok(())
            }
            None => Err(format!(
                "chain execution exceeded {MAX_CHAIN_STEPS} steps (possible jump/goto cycle)"
            )
            .into()),
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

    /// Returns the configured EDNS0 UDP payload size for this context.
    pub fn edns_udp_size(&self) -> u16 {
        self.edns_udp_size
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
        self.response = OnceLock::new();
        if let Some(resp) = resp {
            let _ = self.response.set(resp);
        }
        self.response_wire = None;
    }

    /// Returns a reference to the current response, if set.
    pub fn response(&self) -> Option<&Message> {
        if let Some(resp) = self.response.get() {
            return Some(resp);
        }

        let wire = self.response_wire.as_deref()?;
        let resp = Message::from_vec(wire).ok()?;
        let _ = self.response.set(resp);
        self.response.get()
    }

    /// Returns a mutable reference to the current response, if set.
    pub fn response_mut(&mut self) -> Option<&mut Message> {
        if self.response.get().is_none()
            && let Some(wire) = self.response_wire.as_deref()
        {
            let resp = Message::from_vec(wire).ok()?;
            let _ = self.response.set(resp);
        }

        let resp = self.response.get_mut();
        if resp.is_some() {
            self.response_wire = None;
        }
        resp
    }

    /// Sets a pre-serialized response. Pass `None` to clear it.
    pub fn set_response_wire(&mut self, wire: Option<Vec<u8>>) {
        self.response_wire = wire;
        self.response = OnceLock::new();
    }

    /// Returns the pre-serialized response wire, if set.
    pub fn response_wire(&self) -> Option<&[u8]> {
        self.response_wire.as_deref()
    }

    /// Takes the pre-serialized response wire, if set.
    pub fn take_response_wire(&mut self) -> Option<Vec<u8>> {
        self.response_wire.take()
    }

    /// Returns true when either response representation is present.
    pub fn has_response_output(&self) -> bool {
        self.response.get().is_some() || self.response_wire.is_some()
    }

    // ── Key-Value Store ──────────────────────────────────────────

    /// Stores an arbitrary value under key `k`.
    pub fn store_value<V: 'static + Send + Sync>(&mut self, k: u32, v: V) {
        let boxed: Box<dyn std::any::Any + Send + Sync> = Box::new(v);
        if k == KV_SELECTED_UPSTREAM {
            match boxed.downcast::<Arc<crate::upstream::UpstreamWrapper>>() {
                Ok(upstream) => {
                    self.selected_upstream = Some(*upstream);
                }
                Err(boxed) => {
                    self.kv.get_or_insert_with(HashMap::new).insert(k, boxed);
                }
            }
        } else {
            self.kv.get_or_insert_with(HashMap::new).insert(k, boxed);
        }
    }

    /// Retrieves a reference to the value stored under key `k`.
    pub fn get_value<V: 'static>(&self, k: u32) -> Option<&V> {
        if k == KV_SELECTED_UPSTREAM
            && let Some(upstream) = self.selected_upstream.as_ref()
        {
            let any = upstream as &dyn std::any::Any;
            if let Some(value) = any.downcast_ref::<V>() {
                return Some(value);
            }
        }
        self.kv
            .as_ref()
            .and_then(|kv| kv.get(&k))
            .and_then(|b| b.downcast_ref::<V>())
    }

    /// Removes the value stored under key `k`.
    pub fn delete_value(&mut self, k: u32) {
        if k == KV_SELECTED_UPSTREAM {
            self.selected_upstream = None;
        }
        if let Some(kv) = self.kv.as_mut() {
            kv.remove(&k);
            if kv.is_empty() {
                self.kv = None;
            }
        }
    }

    // ── Marks ────────────────────────────────────────────────────

    /// Sets a boolean mark `m` on this context.
    pub fn set_mark(&mut self, m: u32) {
        if m == MARK_CACHE_HIT {
            self.cache_hit = true;
        } else {
            self.marks.get_or_insert_with(HashSet::new).insert(m);
        }
    }

    /// Returns `true` if mark `m` has been set.
    pub fn has_mark(&self, m: u32) -> bool {
        if m == MARK_CACHE_HIT {
            self.cache_hit
        } else {
            self.marks.as_ref().is_some_and(|marks| marks.contains(&m))
        }
    }

    /// Removes mark `m`.
    pub fn delete_mark(&mut self, m: u32) {
        if m == MARK_CACHE_HIT {
            self.cache_hit = false;
            return;
        }
        if let Some(marks) = self.marks.as_mut() {
            marks.remove(&m);
            if marks.is_empty() {
                self.marks = None;
            }
        }
    }
}

impl std::fmt::Debug for Context {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Context")
            .field("id", &self.id)
            .field("start_time", &self.start_time)
            .field("server_meta", &self.server_meta)
            .field("query", &self.query)
            .field("client_edns", &self.client_edns)
            .field("query_wire_len", &self.query_wire.as_ref().map(|w| w.len()))
            .field("response", &self.response.get())
            .field(
                "response_wire_len",
                &self.response_wire.as_ref().map(Vec::len),
            )
            .field(
                "selected_upstream",
                &self.selected_upstream.as_ref().map(|u| u.name()),
            )
            .field("kv_len", &self.kv.as_ref().map_or(0, HashMap::len))
            .field("marks_len", &self.marks.as_ref().map_or(0, HashSet::len))
            .field("cache_hit", &self.cache_hit)
            .field("steps_remaining", &self.steps_remaining)
            .field("edns_udp_size", &self.edns_udp_size)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hickory_proto::op::{Message, MessageType, OpCode, ResponseCode};
    use hickory_proto::rr::{Name, RData, Record, RecordType};
    use std::net::Ipv4Addr;

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

    fn make_response(ttl: u32) -> Message {
        let mut resp = Message::new();
        resp.set_id(1234)
            .set_message_type(MessageType::Response)
            .set_op_code(OpCode::Query)
            .set_response_code(ResponseCode::NoError);
        resp.add_query({
            let mut q = Query::new();
            q.set_name(Name::from_ascii("example.com.").unwrap())
                .set_query_type(RecordType::A);
            q
        });
        resp.add_answer(Record::from_rdata(
            Name::from_ascii("example.com.").unwrap(),
            ttl,
            RData::A(Ipv4Addr::new(1, 2, 3, 4).into()),
        ));
        resp
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
    fn response_decodes_wire_on_read_without_consuming_wire() {
        let mut ctx = Context::new(make_query());
        let wire = make_response(300).to_vec().unwrap();

        ctx.set_response_wire(Some(wire.clone()));

        let resp = ctx.response().expect("wire response should decode");
        assert_eq!(resp.id(), 1234);
        assert_eq!(resp.answers()[0].ttl(), 300);
        assert_eq!(ctx.response_wire(), Some(wire.as_slice()));
    }

    #[test]
    fn response_mut_decodes_wire_and_clears_stale_wire() {
        let mut ctx = Context::new(make_query());
        ctx.set_response_wire(Some(make_response(300).to_vec().unwrap()));

        let resp = ctx.response_mut().expect("wire response should decode");
        resp.answers_mut()[0].set_ttl(60);

        assert!(ctx.response_wire().is_none());
        assert_eq!(ctx.response().unwrap().answers()[0].ttl(), 60);
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

    #[test]
    fn charge_step_exhausts_budget_and_then_errors() {
        let mut ctx = Context::new(make_query());
        // The budget allows exactly MAX_CHAIN_STEPS successful charges; the
        // next one fails, which is what aborts a cyclic jump/goto config
        // instead of letting the walker recurse until the stack overflows.
        for _ in 0..MAX_CHAIN_STEPS {
            assert!(ctx.charge_step().is_ok());
        }
        assert!(ctx.charge_step().is_err());
        // Remains exhausted on subsequent calls (no wraparound).
        assert!(ctx.charge_step().is_err());
    }
}
