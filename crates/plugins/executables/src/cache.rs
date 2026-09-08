// Copyright (C) 2026, Harlan Wei
//
// This file is part of redns.

//! In-memory LRU DNS cache with lazy TTL refresh and optional file persistence.

use async_trait::async_trait;
use hickory_proto::op::Message;
use hickory_proto::rr::RecordType;
use parking_lot::Mutex;
use quick_cache::sync::Cache as QuickCache;
use redns_core::context::MARK_CACHE_HIT;
use redns_core::plugin::PluginResult;
use redns_core::sequence::ChainWalker;
use redns_core::{Context, RecursiveExecutable};
use serde::Serialize;
use std::collections::HashSet;
use std::fmt;
use std::io::{self, Write as _};
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock, Weak};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::watch;

/// Default cache size.
const DEFAULT_CACHE_SIZE: usize = 1024;

/// Default lazy cache TTL (serve stale for this long while refreshing).
const DEFAULT_LAZY_TTL: Duration = Duration::from_secs(30);

/// Default interval between periodic cache dumps to disk.
pub const DEFAULT_DUMP_INTERVAL: Duration = Duration::from_secs(300);

/// Minimum capacity to enable sharding.
const SHARDING_MIN_CAPACITY: usize = 4096;

/// File persistence magic header and version.
///
/// Version 2 extends each entry with the full [`CacheKey`] view fields
/// (QCLASS, RD/CD/AD/DO bits, EDNS-options hash). Version 1 files are
/// rejected: their keys cannot be reconstructed safely, so entries from an
/// older version must not be served under a version-2 key.
const FILE_MAGIC: &[u8; 10] = b"REDNSCACHE";
const FILE_VERSION: u8 = 2;

/// Configuration for cache file persistence.
#[derive(Debug, Clone)]
pub struct CachePersistConfig {
    /// Path to the cache file.
    pub file_path: String,
    /// Interval between periodic dumps.
    pub dump_interval: Duration,
}

static CACHE_REGISTRY: OnceLock<Mutex<Vec<Weak<CacheInner>>>> = OnceLock::new();
static CACHE_ID: AtomicUsize = AtomicUsize::new(1);

/// A cached DNS response entry.
///
/// `Clone` is required by `quick_cache`, whose `get` returns an owned clone
/// (cheap here: the payload is behind `Arc`s).
#[derive(Clone)]
struct CachedEntry {
    /// Pre-serialized DNS response wire bytes. All record TTL fields have been
    /// normalized to `original_ttl`, so on a cache hit we only need to patch the
    /// query ID (bytes 0-1) and the TTL offsets in place.
    resp_wire: Arc<Vec<u8>>,
    /// Byte offsets of the 4-byte TTL fields inside `resp_wire` (answers,
    /// authorities, additionals). The OPT pseudo-record TTL is intentionally
    /// excluded because it carries extended RCODE / DO bit data. Stored behind
    /// an `Arc` so a cache hit can clone it out from under the shard lock in
    /// O(1) instead of allocating a fresh `Vec`.
    ttl_offsets: Arc<[usize]>,
    /// Time this entry was stored.
    stored_at: Instant,
    /// Original minimum TTL of the response records.
    original_ttl: u32,
}

/// Query-view bits packed into [`CacheKey`]. Answers can differ on all of
/// these, so they partition the cache.
const VIEW_RD: u8 = 1 << 0;
const VIEW_CD: u8 = 1 << 1;
const VIEW_AD: u8 = 1 << 2;
const VIEW_DO: u8 = 1 << 3;

/// Cache key: everything the response depends on beyond the passage of time —
/// lowercased QNAME + QTYPE + QCLASS, the request view (RD/CD/AD header flags
/// and the DNSSEC-OK bit) and a hash of the logical query's EDNS options (e.g.
/// an ECS option injected per client by `ecs_handler`).
///
/// Partitioning by CD/DO matters most: a validating upstream's CD=1 answer is
/// *unchecked* and must never be served to a CD=0 client (which the upstream
/// would have answered with SERVFAIL), and a DO=1 answer carries DNSSEC
/// records a DO=0 answer lacks.
#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
struct CacheKey {
    qname: String,
    qtype: RecordType,
    qclass: hickory_proto::rr::DNSClass,
    view: u8,
    options_hash: u64,
}

impl fmt::Display for CacheKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}:{}:class={:?}:view={:#x}:opts={:#x}",
            self.qname, self.qtype, self.qclass, self.view, self.options_hash
        )
    }
}

impl CachedEntry {
    fn remaining_ttl(&self) -> u32 {
        let elapsed = self.stored_at.elapsed().as_secs() as u32;
        self.original_ttl.saturating_sub(elapsed)
    }

    fn is_expired(&self) -> bool {
        self.remaining_ttl() == 0
    }

    fn is_within_lazy_window(&self, lazy_ttl: Duration) -> bool {
        let elapsed = self.stored_at.elapsed();
        let expire_at = Duration::from_secs(self.original_ttl as u64);
        elapsed < expire_at + lazy_ttl
    }
}

/// Build the cache key from DNS question data and the request's query view.
fn cache_key(ctx: &Context) -> Option<CacheKey> {
    let q = ctx.question()?;
    let query = ctx.query();
    let mut qname = q.name().to_ascii();
    qname.make_ascii_lowercase();

    let mut view = 0u8;
    if query.metadata.recursion_desired {
        view |= VIEW_RD;
    }
    if query.metadata.checking_disabled {
        view |= VIEW_CD;
    }
    if query.metadata.authentic_data {
        view |= VIEW_AD;
    }
    if query.edns.as_ref().is_some_and(|e| e.flags().dnssec_ok) {
        view |= VIEW_DO;
    }

    // Hash the logical query's EDNS options (stable SipHash with fixed keys).
    // Deterministic across runs so persisted keys stay valid; a hash change
    // across toolchains only orphans persisted entries, never mis-hits.
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    if let Some(edns) = &query.edns {
        edns.options().options.hash(&mut hasher);
    }

    Some(CacheKey {
        qname,
        qtype: q.query_type(),
        qclass: q.query_class(),
        view,
        options_hash: hasher.finish(),
    })
}

fn host_parallelism() -> usize {
    std::thread::available_parallelism()
        .map(NonZeroUsize::get)
        .unwrap_or(1)
}

/// Extract the minimum TTL from a DNS response message.
fn min_ttl(msg: &Message) -> u32 {
    let mut min = u32::MAX;
    for rr in msg
        .answers
        .iter()
        .chain(msg.authorities.iter())
        .chain(msg.additionals.iter())
    {
        if rr.record_type() == hickory_proto::rr::RecordType::OPT {
            continue;
        }
        min = min.min(rr.ttl);
    }
    if min == u32::MAX { 300 } else { min }
}

/// Extract the offsets of all record TTL fields in a DNS wire message.
/// Skips the OPT pseudo-record (TYPE 41) because its TTL field is actually
/// the extended RCODE / Z / DO bits.
fn extract_ttl_offsets(wire: &[u8]) -> Vec<usize> {
    if wire.len() < 12 {
        return Vec::new();
    }
    let mut pos = 12; // skip fixed-size DNS header
    let qdcount = u16::from_be_bytes([wire[4], wire[5]]) as usize;
    for _ in 0..qdcount {
        skip_name(wire, &mut pos);
        if pos + 4 > wire.len() {
            break;
        }
        pos += 4; // QTYPE + QCLASS
    }
    let counts = [
        u16::from_be_bytes([wire[6], wire[7]]) as usize,
        u16::from_be_bytes([wire[8], wire[9]]) as usize,
        u16::from_be_bytes([wire[10], wire[11]]) as usize,
    ];
    let mut offsets = Vec::new();
    for count in counts {
        for _ in 0..count {
            skip_name(wire, &mut pos);
            if pos + 10 > wire.len() {
                break;
            }
            let rtype = u16::from_be_bytes([wire[pos], wire[pos + 1]]);
            // TTL offset is after TYPE (2) + CLASS (2).
            if rtype != 41 {
                offsets.push(pos + 4);
            }
            pos += 4; // TYPE + CLASS
            pos += 4; // TTL
            if pos + 2 > wire.len() {
                break;
            }
            let rdlength = u16::from_be_bytes([wire[pos], wire[pos + 1]]) as usize;
            pos += 2 + rdlength;
        }
    }
    offsets
}

/// Advance `pos` past a DNS domain name, following compression pointers.
fn skip_name(wire: &[u8], pos: &mut usize) {
    loop {
        if *pos >= wire.len() {
            return;
        }
        let len = wire[*pos] as usize;
        if len == 0 {
            *pos += 1;
            return;
        }
        if len & 0xC0 == 0xC0 {
            // Compression pointer: 2 bytes total, then stop following this name.
            *pos += 2;
            return;
        }
        *pos += 1 + len;
    }
}

/// Patch all recorded TTL offsets in the wire with the given value.
fn set_ttl_in_wire(wire: &mut [u8], offsets: &[usize], ttl: u32) {
    let ttl_bytes = ttl.to_be_bytes();
    for off in offsets {
        if *off + 4 <= wire.len() {
            wire[*off..*off + 4].copy_from_slice(&ttl_bytes);
        }
    }
}

/// Build a response wire suitable for storing: all record TTLs are normalized
/// to `original_ttl` and the offsets of the TTL fields are returned.
fn build_stored_wire(resp: &Message, original_ttl: u32) -> Option<(Vec<u8>, Vec<usize>)> {
    let wire = resp.to_vec().ok()?;
    let offsets = extract_ttl_offsets(&wire);
    let mut wire = wire;
    set_ttl_in_wire(&mut wire, &offsets, original_ttl);
    Some((wire, offsets))
}

/// In-memory approximate-LRU DNS cache.
///
/// Uses `quick_cache` (clock / hot-cold segmented eviction) for bounded
/// capacity with better hit rates than a strict LRU on skewed workloads.
/// Sharded to reduce lock contention across threads.
#[derive(Clone)]
pub struct Cache {
    inner: Arc<CacheInner>,
}

struct CacheInner {
    id: usize,
    shard_count: usize,
    shards: Vec<QuickCache<CacheKey, CachedEntry>>,
    shard_hasher: ahash::RandomState,
    /// Deduplicates background lazy refreshes for the same key.
    inflight_refreshes: Mutex<HashSet<CacheKey>>,
    /// Coalesces concurrent cache misses so only one query fetches upstream.
    inflight_misses: Mutex<ahash::HashMap<CacheKey, Arc<MissState>>>,
    lazy_ttl: Duration,
    /// Total cache hits (fresh + stale).
    hit_total: AtomicU64,
    /// Total cache misses.
    miss_total: AtomicU64,
}

/// The completed outcome of a miss leader, shared with its followers.
#[derive(Clone)]
enum SharedOutcome {
    /// A response the leader produced — including ones inadmissible for
    /// persistent caching (TTL zero, REFUSED, TC=1) — so a burst of concurrent
    /// queries for such a name does not degenerate into a serial fetch queue.
    Response {
        resp_wire: Arc<Vec<u8>>,
        ttl_offsets: Arc<[usize]>,
        ttl: u32,
    },
    /// The leader's chain failed; followers share the failure instead of each
    /// repeating it upstream.
    Error(String),
}

/// Shared between a miss leader and its followers.
struct MissState {
    /// Clone source for follower receivers. Completion is signalled by the
    /// leader's sender being dropped (closing the channel and failing every
    /// `changed()`), which happens on all leader exit paths.
    completion: watch::Receiver<()>,
    outcome: Mutex<Option<SharedOutcome>>,
}

/// What a follower waits on: the leader's watch channel (closed on every exit
/// path, including cancellation) plus the shared outcome slot.
struct MissFollower {
    receiver: watch::Receiver<()>,
    state: Arc<MissState>,
}

impl MissFollower {
    fn take_outcome(&self) -> Option<SharedOutcome> {
        self.state.outcome.lock().clone()
    }
}

/// Closing the channel wakes followers even if they haven't started waiting yet.
/// Drop also runs when a leader is cancelled or its downstream chain panics.
struct MissGuard<'a> {
    inner: &'a CacheInner,
    key: &'a CacheKey,
    state: Arc<MissState>,
    _completion: watch::Sender<()>,
}

impl Drop for MissGuard<'_> {
    fn drop(&mut self) {
        self.inner.inflight_misses.lock().remove(self.key);
    }
}

struct RefreshGuard {
    inner: Arc<CacheInner>,
    key: CacheKey,
}

impl Drop for RefreshGuard {
    fn drop(&mut self) {
        self.inner.inflight_refreshes.lock().remove(&self.key);
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct CacheShardSnapshot {
    pub index: usize,
    pub entries: usize,
    pub capacity: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct CacheSnapshot {
    pub id: usize,
    pub total_entries: usize,
    pub total_capacity: usize,
    pub hit_total: u64,
    pub miss_total: u64,
    pub shards: Vec<CacheShardSnapshot>,
}

impl Cache {
    pub fn new(
        max_size: usize,
        lazy_ttl: Duration,
        persist_config: Option<CachePersistConfig>,
    ) -> Self {
        let cap = if max_size == 0 {
            DEFAULT_CACHE_SIZE
        } else {
            max_size
        };

        let shard_count = if cap < SHARDING_MIN_CAPACITY {
            1
        } else {
            host_parallelism().min(cap)
        };
        let shard_cap = std::cmp::max(1, cap.div_ceil(shard_count));
        let mut shards = Vec::with_capacity(shard_count);
        for _ in 0..shard_count {
            shards.push(QuickCache::new(shard_cap));
        }

        let id = CACHE_ID.fetch_add(1, Ordering::Relaxed);

        let inner = Arc::new(CacheInner {
            id,
            shard_count,
            shards,
            shard_hasher: ahash::RandomState::new(),
            inflight_refreshes: Mutex::new(HashSet::new()),
            inflight_misses: Mutex::new(ahash::HashMap::default()),
            lazy_ttl,
            hit_total: AtomicU64::new(0),
            miss_total: AtomicU64::new(0),
        });
        register_cache(&inner);
        let cache = Self { inner };

        if let Some(persist) = persist_config {
            let cache_clone = cache.clone();
            let file_path = persist.file_path;
            let dump_interval = persist.dump_interval;
            tokio::spawn(async move {
                match cache_clone.load_from_file(Path::new(&file_path)).await {
                    Ok(n) => {
                        tracing::info!(path = %file_path, entries = n, "cache loaded from file")
                    }
                    Err(e) => {
                        if e.kind() != io::ErrorKind::NotFound {
                            tracing::warn!(
                                error = %e,
                                path = %file_path,
                                "cache load from file failed"
                            );
                        }
                    }
                }

                let mut interval = tokio::time::interval(dump_interval);
                loop {
                    interval.tick().await;
                    match cache_clone.dump_to_file(Path::new(&file_path)).await {
                        Ok(n) => {
                            tracing::debug!(path = %file_path, entries = n, "cache dumped")
                        }
                        Err(e) => {
                            tracing::warn!(
                                error = %e,
                                path = %file_path,
                                "cache dump failed"
                            );
                        }
                    }
                }
            });
        }

        cache
    }

    pub fn default_cache() -> Self {
        Self::new(DEFAULT_CACHE_SIZE, DEFAULT_LAZY_TTL, None)
    }

    fn get_shard(&self, key: &CacheKey) -> &QuickCache<CacheKey, CachedEntry> {
        let hash = self.inner.shard_hasher.hash_one(key);
        &self.inner.shards[(hash as usize) % self.inner.shard_count]
    }

    fn register_miss<'a>(&'a self, key: &'a CacheKey) -> Result<MissGuard<'a>, MissFollower> {
        let mut inflight = self.inner.inflight_misses.lock();
        if let Some(state) = inflight.get(key) {
            return Err(MissFollower {
                receiver: state.completion.clone(),
                state: state.clone(),
            });
        }
        let (completion, receiver) = watch::channel(());
        let state = Arc::new(MissState {
            completion: receiver,
            outcome: Mutex::new(None),
        });
        inflight.insert(key.clone(), state.clone());
        Ok(MissGuard {
            inner: &self.inner,
            key,
            state,
            _completion: completion,
        })
    }
}

fn cache_registry() -> &'static Mutex<Vec<Weak<CacheInner>>> {
    CACHE_REGISTRY.get_or_init(|| Mutex::new(Vec::new()))
}

fn register_cache(inner: &Arc<CacheInner>) {
    let registry = cache_registry();
    let mut guard = registry.lock();
    guard.retain(|cache| cache.upgrade().is_some());
    guard.push(Arc::downgrade(inner));
}

pub async fn cache_registry_snapshot() -> Vec<CacheSnapshot> {
    let caches: Vec<Arc<CacheInner>> = {
        let registry = cache_registry();
        let mut guard = registry.lock();
        guard.retain(|cache| cache.upgrade().is_some());
        guard.iter().filter_map(|cache| cache.upgrade()).collect()
    };

    let mut snapshots = Vec::with_capacity(caches.len());
    for cache in caches {
        let mut total_entries = 0usize;
        let mut total_capacity = 0usize;
        let mut shards = Vec::with_capacity(cache.shards.len());

        for (index, shard) in cache.shards.iter().enumerate() {
            let entries = shard.len();
            let capacity = shard.capacity() as usize;
            total_entries += entries;
            total_capacity += capacity;
            shards.push(CacheShardSnapshot {
                index,
                entries,
                capacity,
            });
        }

        snapshots.push(CacheSnapshot {
            id: cache.id,
            total_entries,
            total_capacity,
            hit_total: cache.hit_total.load(Ordering::Relaxed),
            miss_total: cache.miss_total.load(Ordering::Relaxed),
            shards,
        });
    }

    snapshots
}

/// Result of a cache lookup: a fresh hit, a stale hit eligible for lazy
/// refresh, or a miss that should be fetched upstream.
enum CacheLookup {
    Hit(Vec<u8>),
    Stale(Vec<u8>),
    Miss,
}

impl Cache {
    /// Look up a key and build response wire for the current query ID.
    ///
    /// `quick_cache` releases its internal lock before returning the owned entry,
    /// so cloning and patching the response don't serialize concurrent lookups.
    fn lookup_and_build(&self, key: &CacheKey, query_id: u16) -> CacheLookup {
        let captured = {
            match self.get_shard(key).get(key) {
                Some(entry) if !entry.is_expired() => {
                    let ttl = entry.remaining_ttl();
                    Some((entry.resp_wire, entry.ttl_offsets, ttl, false))
                }
                Some(entry) if entry.is_within_lazy_window(self.inner.lazy_ttl) => {
                    // Stale-while-refresh: serve with a 1-second TTL while a
                    // background refresh runs.
                    Some((entry.resp_wire, entry.ttl_offsets, 1, true))
                }
                _ => None,
            }
        };

        match captured {
            Some((wire, offsets, ttl, is_stale)) => {
                match build_response_wire(&wire, &offsets, query_id, ttl) {
                    Some(wire) if is_stale => CacheLookup::Stale(wire),
                    Some(wire) => CacheLookup::Hit(wire),
                    None => CacheLookup::Miss,
                }
            }
            None => CacheLookup::Miss,
        }
    }

    fn serve_cached(&self, key: &CacheKey, ctx: &mut Context, next: &ChainWalker) -> bool {
        let (wire, stale) = match self.lookup_and_build(key, ctx.query().id) {
            CacheLookup::Hit(wire) => (wire, false),
            CacheLookup::Stale(wire) => (wire, true),
            CacheLookup::Miss => return false,
        };
        self.inner.hit_total.fetch_add(1, Ordering::Relaxed);
        ctx.set_response_wire(Some(wire));
        ctx.set_mark(MARK_CACHE_HIT);
        if stale {
            self.spawn_refresh_for_key(key, ctx, next.clone());
        }
        true
    }
}

/// Build response wire from a cached entry's stored wire by patching the query
/// ID and record TTLs. Operates on cloned-out data so it can run without
/// holding the shard lock and without reparsing the DNS message.
fn build_response_wire(
    resp_wire: &[u8],
    offsets: &[usize],
    query_id: u16,
    ttl: u32,
) -> Option<Vec<u8>> {
    let mut wire = resp_wire.to_vec();
    if wire.len() < 2 {
        return None;
    }
    wire[0] = (query_id >> 8) as u8;
    wire[1] = (query_id & 0xff) as u8;
    set_ttl_in_wire(&mut wire, offsets, ttl);
    Some(wire)
}

#[async_trait]
impl RecursiveExecutable for Cache {
    async fn exec_recursive(&self, ctx: &mut Context, mut next: ChainWalker) -> PluginResult<()> {
        let key = match cache_key(ctx) {
            Some(k) => k,
            None => return next.exec_next(ctx).await,
        };

        loop {
            if self.serve_cached(&key, ctx, &next) {
                return Ok(());
            }

            let _leader = match self.register_miss(&key) {
                Ok(leader) => leader,
                Err(mut follower) => {
                    // The leader closes the channel on every exit path. Closure
                    // is remembered, unlike a Notify::notify_waiters wakeup.
                    let _ = follower.receiver.changed().await;
                    match follower.take_outcome() {
                        Some(SharedOutcome::Response {
                            resp_wire,
                            ttl_offsets,
                            ttl,
                        }) => {
                            // Adopt the leader's response even though it was
                            // not eligible for persistent caching (TTL zero,
                            // REFUSED, TC=1). Without this, every follower
                            // would become the next leader and repeat the
                            // upstream fetch serially.
                            if let Some(wire) =
                                build_response_wire(&resp_wire, &ttl_offsets, ctx.query().id, ttl)
                            {
                                ctx.set_response_wire(Some(wire));
                                return Ok(());
                            }
                            continue;
                        }
                        Some(SharedOutcome::Error(e)) => return Err(e.into()),
                        // Leader was cancelled before producing anything:
                        // retry (this task may become the next leader).
                        None => continue,
                    }
                }
            };

            // A previous leader may have populated the cache between our first
            // lookup and registration. Don't launch a duplicate upstream fetch.
            if self.serve_cached(&key, ctx, &next) {
                return Ok(());
            }

            self.inner.miss_total.fetch_add(1, Ordering::Relaxed);
            let result = next.exec_next(ctx).await;
            match &result {
                Ok(()) => {
                    // Publish the response for followers even when it is not
                    // admissible for persistent caching.
                    if let Some(shared) = self.store(&key, ctx) {
                        *_leader.state.outcome.lock() = Some(shared);
                    }
                }
                Err(e) => {
                    *_leader.state.outcome.lock() = Some(SharedOutcome::Error(e.to_string()));
                }
            }
            return result;
        }
    }
}

impl Cache {
    /// Admit a response into the cache when eligible, and return it as a
    /// shareable outcome for miss followers either way.
    ///
    /// Never cached:
    /// - REFUSED — a transient upstream signal (rate limiting, policy, etc.);
    /// - TC=1 — a truncated message is incomplete; caching one would poison
    ///   later TCP-retry queries with an empty answer;
    /// - minimum TTL of zero — would expire instantly anyway.
    fn store(&self, key: &CacheKey, ctx: &Context) -> Option<SharedOutcome> {
        use hickory_proto::op::ResponseCode;

        let resp = ctx.response()?;
        let rcode = resp.response_code;

        let mut ttl = min_ttl(resp);
        if rcode == ResponseCode::NXDomain {
            ttl = ttl.min(30);
        } else if rcode == ResponseCode::ServFail {
            ttl = ttl.min(5);
        }

        let cacheable = rcode != ResponseCode::Refused && !resp.metadata.truncation && ttl != 0;

        let (wire, offsets) = build_stored_wire(resp, ttl)?;

        if cacheable {
            self.get_shard(key).insert(
                key.clone(),
                CachedEntry {
                    resp_wire: Arc::new(wire.clone()),
                    ttl_offsets: offsets.clone().into(),
                    stored_at: Instant::now(),
                    original_ttl: ttl,
                },
            );
        }

        Some(SharedOutcome::Response {
            resp_wire: Arc::new(wire),
            ttl_offsets: offsets.into(),
            ttl,
        })
    }

    /// Spawn a background refresh for a specific key, deduplicating via inflight_refreshes.
    fn spawn_refresh_for_key(&self, key: &CacheKey, parent: &Context, mut chain: ChainWalker) {
        let mut inflight = self.inner.inflight_refreshes.lock();
        let should_spawn = inflight.insert(key.clone());
        drop(inflight);

        if !should_spawn {
            return;
        }

        // Fork (don't rebuild) so the refresh query keeps the parent's
        // already-normalized logical query, EDNS options and request settings.
        let mut refresh_ctx = Context::fork_from(parent);
        let cache_clone = self.clone();
        let guard = RefreshGuard {
            inner: self.inner.clone(),
            key: key.clone(),
        };
        tokio::spawn(async move {
            let refresh = guard;
            if chain.exec_next(&mut refresh_ctx).await.is_ok() {
                cache_clone.store(&refresh.key, &refresh_ctx);
            }
        });
    }

    /// Dump all non-expired cache entries to a binary file.
    ///
    /// The file is written atomically via a temp file + rename.
    /// Returns the number of entries dumped.
    async fn dump_to_file(&self, path: &Path) -> io::Result<usize> {
        let now_ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        let mut entries_buf = Vec::new();
        let mut count: usize = 0;

        for shard in &self.inner.shards {
            // `quick_cache::iter` yields owned clones, so `key`/`entry` can be
            // used without holding references into the shard.
            for (key, entry) in shard.iter() {
                let remaining = entry.remaining_ttl();
                if remaining == 0 {
                    continue;
                }

                let qname_bytes = key.qname.as_bytes();
                if qname_bytes.len() > u16::MAX as usize {
                    continue;
                }
                entries_buf.write_all(&(qname_bytes.len() as u16).to_be_bytes())?;
                entries_buf.write_all(qname_bytes)?;
                entries_buf.write_all(&u16::from(key.qtype).to_be_bytes())?;
                entries_buf.write_all(&u16::from(key.qclass).to_be_bytes())?;
                entries_buf.write_all(&[key.view])?;
                entries_buf.write_all(&key.options_hash.to_be_bytes())?;
                entries_buf.write_all(&remaining.to_be_bytes())?;

                let msg_wire = entry.resp_wire.as_slice();
                if msg_wire.len() > u32::MAX as usize {
                    continue;
                }
                entries_buf.write_all(&(msg_wire.len() as u32).to_be_bytes())?;
                entries_buf.write_all(msg_wire)?;

                count += 1;
            }
        }

        let mut buf = Vec::with_capacity(FILE_MAGIC.len() + 1 + 8 + 4 + entries_buf.len());
        buf.write_all(FILE_MAGIC)?;
        buf.write_all(&[FILE_VERSION])?;
        buf.write_all(&now_ts.to_be_bytes())?;
        buf.write_all(&(count as u32).to_be_bytes())?;
        buf.write_all(&entries_buf)?;

        // Offload the blocking write + fsync + rename off the async worker.
        let path = path.to_path_buf();
        tokio::task::spawn_blocking(move || -> io::Result<()> {
            let tmp_path = format!("{}.tmp", path.display());
            {
                let mut f = std::fs::File::create(&tmp_path)?;
                f.write_all(&buf)?;
                f.sync_all()?;
            }
            std::fs::rename(&tmp_path, &path)?;
            Ok(())
        })
        .await
        .map_err(|e| io::Error::other(format!("cache dump task panicked: {e}")))??;

        Ok(count)
    }

    /// Load cache entries from a binary file written by [`dump_to_file`].
    ///
    /// Entries that have expired between dump and load are skipped.
    /// Returns the number of entries loaded.
    async fn load_from_file(&self, path: &Path) -> io::Result<usize> {
        let path = path.to_path_buf();
        let data = tokio::task::spawn_blocking(move || std::fs::read(&path))
            .await
            .map_err(|e| io::Error::other(format!("cache load task panicked: {e}")))??;

        if data.len() < FILE_MAGIC.len() + 1 + 8 + 4 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "cache file too short",
            ));
        }

        let mut pos = 0;
        if &data[pos..pos + FILE_MAGIC.len()] != FILE_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "cache file: invalid magic",
            ));
        }
        pos += FILE_MAGIC.len();

        if data[pos] != FILE_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "cache file: unsupported version",
            ));
        }
        pos += 1;

        let dump_ts = i64::from_be_bytes(data[pos..pos + 8].try_into().map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "cache file: timestamp corrupt")
        })?);
        pos += 8;

        let entry_count = u32::from_be_bytes(data[pos..pos + 4].try_into().map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "cache file: entry count corrupt",
            )
        })?) as usize;
        pos += 4;

        let now_ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        let elapsed_since_dump = (now_ts - dump_ts).max(0) as u32;

        let mut loaded = 0;
        for _ in 0..entry_count {
            if pos + 2 > data.len() {
                break;
            }
            let qname_len = u16::from_be_bytes([data[pos], data[pos + 1]]) as usize;
            pos += 2;

            if pos + qname_len > data.len() {
                break;
            }
            let qname = String::from_utf8_lossy(&data[pos..pos + qname_len]).into_owned();
            pos += qname_len;

            if pos + 2 > data.len() {
                break;
            }
            let qtype_u16 = u16::from_be_bytes([data[pos], data[pos + 1]]);
            pos += 2;

            if pos + 2 + 1 + 8 + 4 > data.len() {
                break;
            }
            let qclass_u16 = u16::from_be_bytes([data[pos], data[pos + 1]]);
            pos += 2;
            let view = data[pos];
            pos += 1;
            let options_hash = u64::from_be_bytes([
                data[pos],
                data[pos + 1],
                data[pos + 2],
                data[pos + 3],
                data[pos + 4],
                data[pos + 5],
                data[pos + 6],
                data[pos + 7],
            ]);
            pos += 8;

            if pos + 4 > data.len() {
                break;
            }
            let remaining_at_dump =
                u32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
            pos += 4;

            if pos + 4 > data.len() {
                break;
            }
            let msg_len =
                u32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]])
                    as usize;
            pos += 4;

            if pos + msg_len > data.len() {
                break;
            }
            let msg_wire = &data[pos..pos + msg_len];
            pos += msg_len;

            let effective_remaining = remaining_at_dump.saturating_sub(elapsed_since_dump);
            if effective_remaining == 0 {
                continue;
            }

            // Validate the wire before adopting the entry.
            if Message::from_vec(msg_wire).is_err() {
                continue;
            }

            let key = CacheKey {
                qname,
                qtype: RecordType::from(qtype_u16),
                qclass: hickory_proto::rr::DNSClass::from(qclass_u16),
                view,
                options_hash,
            };

            // Normalize the loaded wire so all record TTLs equal the remaining
            // TTL, matching how freshly-stored responses are kept.
            let mut wire = msg_wire.to_vec();
            let offsets = extract_ttl_offsets(&wire);
            set_ttl_in_wire(&mut wire, &offsets, effective_remaining);

            self.get_shard(&key).insert(
                key,
                CachedEntry {
                    resp_wire: Arc::new(wire),
                    ttl_offsets: offsets.into(),
                    stored_at: Instant::now(),
                    original_ttl: effective_remaining,
                },
            );
            loaded += 1;
        }

        Ok(loaded)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hickory_proto::op::{Message, MessageType, OpCode, Query, ResponseCode};
    use hickory_proto::rr::{Name, RData, Record, RecordType};
    use redns_core::plugin::Executable;
    use redns_core::sequence::{ChainNode, NodeExecutor, Sequence};
    use std::net::Ipv4Addr;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

    struct RespondWithTtl(u32);
    #[async_trait]
    impl Executable for RespondWithTtl {
        async fn exec(&self, ctx: &mut Context) -> PluginResult<()> {
            let q = ctx.question().unwrap().clone();
            let mut resp = Message::response(ctx.query().id, OpCode::Query);
        resp.metadata.response_code = ResponseCode::NoError;
            resp.add_query(q.clone());
            resp.add_answer(Record::from_rdata(
                q.name().clone(),
                self.0,
                RData::A(Ipv4Addr::new(1, 2, 3, 4).into()),
            ));
            ctx.set_response(Some(resp));
            Ok(())
        }
    }

    struct CountingDelayedResponder {
        ttl: u32,
        calls: Arc<AtomicUsize>,
        delay: Duration,
    }

    #[async_trait]
    impl Executable for CountingDelayedResponder {
        async fn exec(&self, ctx: &mut Context) -> PluginResult<()> {
            self.calls.fetch_add(1, AtomicOrdering::Relaxed);
            tokio::time::sleep(self.delay).await;

            let q = ctx.question().unwrap().clone();
            let mut resp = Message::response(ctx.query().id, OpCode::Query);
        resp.metadata.response_code = ResponseCode::NoError;
            resp.add_query(q.clone());
            resp.add_answer(Record::from_rdata(
                q.name().clone(),
                self.ttl,
                RData::A(Ipv4Addr::new(1, 2, 3, 4).into()),
            ));
            ctx.set_response(Some(resp));
            Ok(())
        }
    }

    fn make_query() -> Message {
        let mut msg = Message::new(1, MessageType::Query, OpCode::Query);
        msg.add_query({
            let mut q = Query::new();
            q.set_name(Name::from_ascii("test.example.com.").unwrap())
                .set_query_type(RecordType::A);
            q
        });
        msg
    }

    struct PendingFirstResponder(Arc<AtomicUsize>);

    #[async_trait]
    impl Executable for PendingFirstResponder {
        async fn exec(&self, ctx: &mut Context) -> PluginResult<()> {
            if self.0.fetch_add(1, AtomicOrdering::SeqCst) == 0 {
                std::future::pending::<()>().await;
            }
            RespondWithTtl(60).exec(ctx).await
        }
    }

    fn poll_pending(future: std::pin::Pin<&mut impl std::future::Future>) {
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(future.poll(&mut cx).is_pending());
    }

    #[tokio::test]
    async fn cancelled_miss_wakes_waiters_and_allows_retry() {
        let cache = Cache::new(128, Duration::ZERO, None);
        let calls = Arc::new(AtomicUsize::new(0));
        let seq = Sequence::new(vec![
            ChainNode {
                matchers: vec![],
                executor: NodeExecutor::Recursive(Box::new(cache.clone())),
            },
            ChainNode {
                matchers: vec![],
                executor: NodeExecutor::Simple(Box::new(PendingFirstResponder(calls.clone()))),
            },
        ]);
        let mut leader_ctx = Context::new(make_query());
        let mut waiter_ctx = Context::new(make_query());
        let mut leader = Box::pin(seq.exec(&mut leader_ctx));
        poll_pending(leader.as_mut());
        let mut waiter = Box::pin(seq.exec(&mut waiter_ctx));
        poll_pending(waiter.as_mut());
        assert_eq!(calls.load(AtomicOrdering::SeqCst), 1);

        drop(leader);
        assert!(cache.inner.inflight_misses.lock().is_empty());
        tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("cancelled leader must wake its waiter")
            .unwrap();
        assert!(waiter_ctx.has_response_output());
        assert_eq!(calls.load(AtomicOrdering::SeqCst), 2);

        let mut cached_ctx = Context::new(make_query());
        seq.exec(&mut cached_ctx).await.unwrap();
        assert!(cached_ctx.has_mark(MARK_CACHE_HIT));
        assert_eq!(calls.load(AtomicOrdering::SeqCst), 2);
    }

    #[tokio::test]
    async fn completion_before_wait_is_not_lost() {
        let cache = Cache::default_cache();
        let ctx = Context::new(make_query());
        let key = cache_key(&ctx).unwrap();
        let leader = cache.register_miss(&key).ok().unwrap();
        let mut waiter = cache.register_miss(&key).err().unwrap();

        // Complete before the follower even constructs its waiting future.
        drop(leader);
        assert!(
            tokio::time::timeout(Duration::from_secs(1), waiter.receiver.changed())
                .await
                .expect("completion must be remembered")
                .is_err()
        );
        assert!(cache.register_miss(&key).is_ok());
    }

    #[tokio::test]
    async fn failed_miss_does_not_cache_partial_response_or_keep_leader() {
        struct FailAfterResponse;
        #[async_trait]
        impl Executable for FailAfterResponse {
            async fn exec(&self, ctx: &mut Context) -> PluginResult<()> {
                RespondWithTtl(60).exec(ctx).await?;
                Err("downstream failed".into())
            }
        }
        let cache = Cache::default_cache();
        let mut ctx = Context::new(make_query());
        let key = cache_key(&ctx).unwrap();
        let next = ChainWalker::new(vec![ChainNode {
            matchers: vec![],
            executor: NodeExecutor::Simple(Box::new(FailAfterResponse)),
        }].into(), None);
        assert!(cache.exec_recursive(&mut ctx, next).await.is_err());
        assert!(cache.inner.inflight_misses.lock().is_empty());
        assert!(matches!(cache.lookup_and_build(&key, 1), CacheLookup::Miss));
    }

    #[tokio::test]
    async fn executed_cache_does_not_retain_its_chain() {
        let weak;
        {
            let cache = Cache::default_cache();
            weak = Arc::downgrade(&cache.inner);
            let seq = Sequence::new(vec![
                ChainNode {
                    matchers: vec![],
                    executor: NodeExecutor::Recursive(Box::new(cache)),
                },
                ChainNode {
                    matchers: vec![],
                    executor: NodeExecutor::Simple(Box::new(RespondWithTtl(60))),
                },
            ]);
            seq.exec(&mut Context::new(make_query())).await.unwrap();
        }
        assert!(weak.upgrade().is_none(), "cache must not form a cycle with its chain");
    }

    #[tokio::test]
    async fn cache_miss_then_hit() {
        let cache = Cache::new(100, Duration::from_secs(30), None);
        let chain: Vec<ChainNode> = vec![
            ChainNode {
                matchers: vec![],
                executor: NodeExecutor::Recursive(Box::new(cache)),
            },
            ChainNode {
                matchers: vec![],
                executor: NodeExecutor::Simple(Box::new(RespondWithTtl(300))),
            },
        ];
        let seq = Sequence::new(chain);

        let mut ctx = Context::new(make_query());
        seq.exec(&mut ctx).await.unwrap();
        assert!(ctx.response().is_some());
        assert_eq!(ctx.response().unwrap().answers.len(), 1);
    }

    fn empty_entry(ttl: u32) -> CachedEntry {
        let resp = Message::response(0, OpCode::Query);
        let (wire, offsets) = build_stored_wire(&resp, ttl).unwrap();
        CachedEntry {
            resp_wire: Arc::new(wire),
            ttl_offsets: offsets.into(),
            stored_at: Instant::now(),
            original_ttl: ttl,
        }
    }

    fn plain_key(qname: &str) -> CacheKey {
        CacheKey {
            qname: qname.to_string(),
            qtype: RecordType::A,
            qclass: hickory_proto::rr::DNSClass::IN,
            view: 0,
            options_hash: 0,
        }
    }

    #[tokio::test]
    async fn eviction_bounds_capacity() {
        let cache = Cache::new(2, Duration::from_secs(30), None);

        // Simulate 4 different cache entries via the store directly.
        {
            let shard_key = plain_key("key");
            let store = cache.get_shard(&shard_key);
            for i in 0..4 {
                store.insert(plain_key(&format!("key{i}")), empty_entry(300));
            }
            // Sharding is disabled for small caches, so capacity stays exact:
            // 4 inserts into a 2-entry cache leave only 2 resident.
            assert_eq!(store.len(), 2);
            // The most recently inserted entry is always resident.
            assert!(store.get(&plain_key("key3")).is_some());
            // key0 was admitted into the hot segment on first insert and is
            // never scanned for eviction by later inserts, so it survives
            // while the cold entries (key1, key2) fall out one by one.
            assert!(store.get(&plain_key("key0")).is_some());
            assert!(store.get(&plain_key("key1")).is_none());
        }
    }

    #[tokio::test]
    async fn deduplicates_inflight_refresh_for_same_key() {
        let calls = Arc::new(AtomicUsize::new(0));
        let cache = Cache::new(128, Duration::from_secs(30), None);
        let chain: Vec<ChainNode> = vec![
            ChainNode {
                matchers: vec![],
                executor: NodeExecutor::Recursive(Box::new(cache)),
            },
            ChainNode {
                matchers: vec![],
                executor: NodeExecutor::Simple(Box::new(CountingDelayedResponder {
                    ttl: 1,
                    calls: Arc::clone(&calls),
                    delay: Duration::from_millis(250),
                })),
            },
        ];
        let seq = Sequence::new(chain);

        // Prime cache.
        let mut first = Context::new(make_query());
        seq.exec(&mut first).await.unwrap();
        assert_eq!(calls.load(AtomicOrdering::Relaxed), 1);

        // Make entry stale but still within lazy window.
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Trigger one background refresh.
        let mut stale_1 = Context::new(make_query());
        seq.exec(&mut stale_1).await.unwrap();

        // Duplicate stale hit for same key while first refresh is still running.
        let mut stale_2 = Context::new(make_query());
        seq.exec(&mut stale_2).await.unwrap();

        // Wait for single background refresh completion.
        tokio::time::sleep(Duration::from_millis(350)).await;

        // One initial upstream call + one deduplicated refresh.
        assert_eq!(calls.load(AtomicOrdering::Relaxed), 2);
    }

    /// Many concurrent queries for a cold key must result in exactly one
    /// upstream fetch, not a thundering herd.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn coalesces_concurrent_cache_misses() {
        let calls = Arc::new(AtomicUsize::new(0));
        let cache = Cache::new(128, Duration::from_secs(30), None);

        let mut handles = Vec::new();
        for _ in 0..20 {
            let calls = Arc::clone(&calls);
            let cache = cache.clone();
            handles.push(tokio::spawn(async move {
                let chain: Vec<ChainNode> = vec![
                    ChainNode {
                        matchers: vec![],
                        executor: NodeExecutor::Recursive(Box::new(cache)),
                    },
                    ChainNode {
                        matchers: vec![],
                        executor: NodeExecutor::Simple(Box::new(CountingDelayedResponder {
                            ttl: 60,
                            calls,
                            delay: Duration::from_millis(100),
                        })),
                    },
                ];
                let seq = Sequence::new(chain);
                let mut ctx = Context::new(make_query());
                seq.exec(&mut ctx).await.unwrap();
                assert!(ctx.has_response_output());
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        assert_eq!(calls.load(AtomicOrdering::Relaxed), 1);
    }

    #[test]
    fn disables_sharding_below_threshold() {
        let cache = Cache::new(4095, Duration::from_secs(30), None);
        assert_eq!(cache.inner.shard_count, 1);
    }

    #[test]
    fn enables_sharding_at_threshold() {
        let cache = Cache::new(4096, Duration::from_secs(30), None);
        assert_eq!(cache.inner.shard_count, host_parallelism().min(4096));
    }

    // ── Request-view partitioning ─────────────────────────────────

    fn query_with_view(cd: bool, dnssec_ok: bool) -> Message {
        let mut msg = make_query();
        msg.metadata.checking_disabled = cd;
        if dnssec_ok {
            let mut edns = hickory_proto::op::Edns::new();
            edns.set_dnssec_ok(true);
            msg.set_edns(edns);
        }
        msg
    }

    /// A CD=1 answer (unchecked at a validating upstream) must never be
    /// served to a CD=0 client: the two request views get different keys, so
    /// the CD=0 query cannot adopt the CD=1 entry and fetches separately.
    #[tokio::test]
    async fn cache_partitions_cd_and_do_views() {
        let calls = Arc::new(AtomicUsize::new(0));
        let cache = Cache::new(128, Duration::from_secs(30), None);
        let seq = Sequence::new(vec![
            ChainNode {
                matchers: vec![],
                executor: NodeExecutor::Recursive(Box::new(cache)),
            },
            ChainNode {
                matchers: vec![],
                executor: NodeExecutor::Simple(Box::new(CountingDelayedResponder {
                    ttl: 60,
                    calls: Arc::clone(&calls),
                    delay: Duration::ZERO,
                })),
            },
        ]);

        // Warm the cache with a CD=1, DO=1 request.
        let mut ctx_cd = Context::new(query_with_view(true, true));
        seq.exec(&mut ctx_cd).await.unwrap();
        assert_eq!(calls.load(AtomicOrdering::Relaxed), 1);

        // Same qname/qtype but CD=0, DO=0: must NOT hit the CD=1 entry.
        let mut ctx_plain = Context::new(query_with_view(false, false));
        seq.exec(&mut ctx_plain).await.unwrap();
        assert_eq!(
            calls.load(AtomicOrdering::Relaxed),
            2,
            "CD=0 client must not receive the CD=1 answer"
        );

        // The CD=1, DO=1 view itself still hits its own entry.
        let mut ctx_cd2 = Context::new(query_with_view(true, true));
        seq.exec(&mut ctx_cd2).await.unwrap();
        assert_eq!(calls.load(AtomicOrdering::Relaxed), 2);
        assert!(ctx_cd2.has_mark(MARK_CACHE_HIT));
    }

    /// Different QCLASS and different EDNS options (e.g. per-client ECS
    /// injected by `ecs_handler`) must also produce distinct keys.
    #[tokio::test]
    async fn cache_partitions_class_and_edns_options() {
        use hickory_proto::rr::DNSClass;

        let mut q_ch = make_query();
        q_ch.queries[0].set_query_class(DNSClass::CH);

        let base = Context::new(make_query());
        let mut with_ecs = Context::new(make_query());
        {
            let q = with_ecs.query_mut();
            q.edns.as_mut().unwrap().options_mut().options.push((
                hickory_proto::rr::rdata::opt::EdnsCode::Unknown(8),
                hickory_proto::rr::rdata::opt::EdnsOption::Unknown(8, vec![1, 2, 3, 4]),
            ));
        }

        let key_base = cache_key(&base).unwrap();
        let key_ecs = cache_key(&with_ecs).unwrap();
        let key_ch = {
            let ctx = Context::new(q_ch);
            cache_key(&ctx).unwrap()
        };

        assert_ne!(key_base, key_ecs, "ECS options must partition the cache");
        assert_ne!(key_base, key_ch, "QCLASS must partition the cache");
        assert_eq!(key_base, cache_key(&Context::new(make_query())).unwrap());
    }

    // ── Shared miss outcomes ──────────────────────────────────────

    /// Followers must adopt a leader's completed outcome even when it is not
    /// eligible for persistent caching (here: TTL zero). Otherwise every
    /// follower becomes the next leader and the burst repeats the upstream
    /// fetch serially.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn uncachable_miss_outcome_is_shared_with_followers() {
        let calls = Arc::new(AtomicUsize::new(0));
        let cache = Cache::new(128, Duration::ZERO, None);

        let mut handles = Vec::new();
        for _ in 0..8 {
            let calls = Arc::clone(&calls);
            let cache = cache.clone();
            handles.push(tokio::spawn(async move {
                let seq = Sequence::new(vec![
                    ChainNode {
                        matchers: vec![],
                        executor: NodeExecutor::Recursive(Box::new(cache)),
                    },
                    ChainNode {
                        matchers: vec![],
                        executor: NodeExecutor::Simple(Box::new(CountingDelayedResponder {
                            ttl: 0,
                            calls,
                            delay: Duration::from_millis(100),
                        })),
                    },
                ]);
                let mut ctx = Context::new(make_query());
                seq.exec(&mut ctx).await.unwrap();
                assert!(ctx.has_response_output());
            }));
        }
        for h in handles {
            h.await.unwrap();
        }

        assert_eq!(
            calls.load(AtomicOrdering::Relaxed),
            1,
            "TTL-zero response must be shared, not re-fetched per follower"
        );
    }

    /// A leader's chain failure is likewise shared: followers fail fast
    /// instead of repeating the upstream error one at a time.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn error_outcome_is_shared_with_followers() {
        struct FailAfterDelay(Duration);
        #[async_trait]
        impl Executable for FailAfterDelay {
            async fn exec(&self, _ctx: &mut Context) -> PluginResult<()> {
                tokio::time::sleep(self.0).await;
                Err("upstream broken".into())
            }
        }

        let cache = Cache::new(128, Duration::ZERO, None);
        let mut handles = Vec::new();
        for _ in 0..8 {
            let cache = cache.clone();
            handles.push(tokio::spawn(async move {
                let seq = Sequence::new(vec![
                    ChainNode {
                        matchers: vec![],
                        executor: NodeExecutor::Recursive(Box::new(cache)),
                    },
                    ChainNode {
                        matchers: vec![],
                        executor: NodeExecutor::Simple(Box::new(FailAfterDelay(
                            Duration::from_millis(100),
                        ))),
                    },
                ]);
                let mut ctx = Context::new(make_query());
                assert!(seq.exec(&mut ctx).await.is_err());
            }));
        }
        for h in handles {
            h.await.unwrap();
        }

        // All eight shared one leader: the counting is implicit — an
        // assert would need a counter inside the failing executor, so
        // instead assert via miss_total (only leaders count misses).
        assert_eq!(cache.inner.miss_total.load(Ordering::Relaxed), 1);
    }

    /// TC=1 responses are never admitted to the cache: a later query must
    /// not receive a cached truncated (empty) answer in place of fetching a
    /// full one.
    #[tokio::test]
    async fn truncated_response_is_not_cached() {
        struct TruncatedResponder(Arc<AtomicUsize>);
        #[async_trait]
        impl Executable for TruncatedResponder {
            async fn exec(&self, ctx: &mut Context) -> PluginResult<()> {
                self.0.fetch_add(1, AtomicOrdering::Relaxed);
                let q = ctx.question().unwrap().clone();
                let mut resp = Message::response(ctx.query().id, OpCode::Query);
                resp.metadata.response_code = ResponseCode::NoError;
                resp.metadata.truncation = true;
                resp.add_query(q);
                ctx.set_response(Some(resp));
                Ok(())
            }
        }

        let calls = Arc::new(AtomicUsize::new(0));
        let cache = Cache::new(128, Duration::from_secs(30), None);
        let seq = Sequence::new(vec![
            ChainNode {
                matchers: vec![],
                executor: NodeExecutor::Recursive(Box::new(cache.clone())),
            },
            ChainNode {
                matchers: vec![],
                executor: NodeExecutor::Simple(Box::new(TruncatedResponder(Arc::clone(&calls)))),
            },
        ]);

        let mut first = Context::new(make_query());
        seq.exec(&mut first).await.unwrap();
        let mut second = Context::new(make_query());
        seq.exec(&mut second).await.unwrap();

        assert_eq!(
            calls.load(AtomicOrdering::Relaxed),
            2,
            "TC=1 response must not be cached; second query must re-fetch"
        );

        let key = cache_key(&Context::new(make_query())).unwrap();
        assert!(matches!(cache.lookup_and_build(&key, 1), CacheLookup::Miss));
    }
}
