// Copyright (C) 2026, Harlan Wei
//
// This file is part of redns.

//! io_uring-based UDP DNS server for Linux.
//!
//! Two receive paths:
//! - **Multishot RecvMsg** (kernel ≥ 6.0): single SQE generates one CQE per
//!   datagram; buffers come from a kernel-managed provided-buffer ring.
//! - **Single-shot RecvMsg** (kernel ≥ 5.1): one SQE per datagram, 128
//!   pre-submitted at a time.
//!
//! Falls back gracefully if io_uring is unavailable.

#[cfg(target_os = "linux")]
use crate::server::{DnsHandler, QueryMeta};
#[cfg(target_os = "linux")]
use hickory_proto::op::Message;
#[cfg(target_os = "linux")]
use io_uring::{cqueue, opcode, squeue, types::Fd, IoUring};
#[cfg(target_os = "linux")]
use std::collections::VecDeque;
#[cfg(target_os = "linux")]
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
#[cfg(target_os = "linux")]
use std::os::fd::AsRawFd;
#[cfg(target_os = "linux")]
use std::sync::Arc;
#[cfg(target_os = "linux")]
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(target_os = "linux")]
use tokio::sync::{mpsc, Semaphore};
#[cfg(target_os = "linux")]
use tokio_util::sync::CancellationToken;
#[cfg(target_os = "linux")]
use tracing::{debug, error, info, warn};

/// Maximum DNS UDP message size.
const MAX_UDP_SIZE: usize = 4096;

/// io_uring queue depth (must be power of 2).
const RING_DEPTH: u32 = 256;

/// Number of pre-allocated recv buffers (single-shot) / ring entries (multishot).
const NUM_BUFFERS: usize = 128;

/// Provided-buffer group id for multishot mode.
const BUF_GROUP_ID: u16 = 0;

/// Capacity for the handler-to-eventloop response channel.
const SEND_CHANNEL_CAPACITY: usize = 1024;

/// Upper bound on concurrently in-flight handler tasks. Every received datagram
/// spawns a handler task; without a cap a UDP flood would spawn tasks (and the
/// upstream/memory load behind them) without bound. At capacity, further
/// datagrams are dropped rather than queued, which is acceptable for UDP and
/// applies backpressure to the flood instead of the server.
const MAX_INFLIGHT_HANDLERS: usize = 2048;

/// Backoff applied after a persistent io_uring submit error, so the event loop
/// does not spin at 100% CPU while the ring is in a transient failure state.
const SUBMIT_ERROR_BACKOFF: std::time::Duration = std::time::Duration::from_millis(10);

/// User data encoding (single-shot mode only):
/// - High bit set  = recv completion, low bits = buffer index.
/// - High bit clear = send completion, value = send_ctxs index.
const RECV_MARKER: u64 = 1 << 63;

fn encode_recv_user_data(buf_idx: usize) -> u64 {
    RECV_MARKER | (buf_idx as u64)
}

fn is_recv_completion(user_data: u64) -> bool {
    user_data & RECV_MARKER != 0
}

fn recv_buf_idx(user_data: u64) -> usize {
    (user_data & !RECV_MARKER) as usize
}

/// Send completions carry `user_data == slab_index + 1` so the value is always
/// non-zero (it never collides with the multishot recv's `user_data == 0`).
/// Returns the slab index, or `None` if `user_data` is 0 (not a send).
fn send_slab_idx(user_data: u64) -> Option<usize> {
    (user_data != 0).then(|| (user_data - 1) as usize)
}

// ---------------------------------------------------------------------------
// Kernel version detection
// ---------------------------------------------------------------------------

/// Parse the running kernel version from `uname -r`.
#[cfg(target_os = "linux")]
fn kernel_version() -> Option<(u32, u32)> {
    let mut uts: libc::utsname = unsafe { std::mem::zeroed() };
    if unsafe { libc::uname(&mut uts) } != 0 {
        return None;
    }
    let release = unsafe { std::ffi::CStr::from_ptr(uts.release.as_ptr()) }
        .to_str()
        .ok()?;
    let mut parts = release.split('.');
    let major: u32 = parts.next()?.parse().ok()?;
    let minor: u32 = parts.next()?.parse().ok()?;
    Some((major, minor))
}

/// Multishot RecvMsgMulti requires kernel ≥ 6.0.
#[cfg(target_os = "linux")]
fn supports_multishot_recvmsg() -> bool {
    match kernel_version() {
        Some((major, _minor)) => major >= 6,
        None => false,
    }
}

// ---------------------------------------------------------------------------
// Provided-buffer ring (multishot mode)
// ---------------------------------------------------------------------------

/// Manages a kernel-provided buffer ring for multishot recvmsg.
///
/// Each entry points to a 4 KB buffer.  The kernel picks buffers from the
/// head of the ring; the application returns them by advancing the tail
/// after processing each completion.
#[cfg(target_os = "linux")]
struct BufRing {
    /// Ring entries (page-aligned allocation).
    entries: std::ptr::NonNull<io_uring::types::BufRingEntry>,
    /// Layout used for deallocation.
    layout: std::alloc::Layout,
    /// Actual data buffers.
    buffers: Vec<Box<[u8; MAX_UDP_SIZE]>>,
}

#[cfg(target_os = "linux")]
unsafe impl Send for BufRing {}

#[cfg(target_os = "linux")]
impl BufRing {
    fn new(
        ring: &IoUring,
        bgid: u16,
        num_entries: usize,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let entry_size = std::mem::size_of::<io_uring::types::BufRingEntry>();
        let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as usize;
        let align = entry_size.max(page_size);
        let layout =
            std::alloc::Layout::from_size_align(entry_size * (num_entries + 1), align)?;

        let ptr = unsafe { std::alloc::alloc_zeroed(layout) };
        let entries = std::ptr::NonNull::new(ptr.cast::<io_uring::types::BufRingEntry>())
            .ok_or("buf ring alloc failed")?;

        // Allocate the data buffers and populate ring entries.
        let buffers: Vec<Box<[u8; MAX_UDP_SIZE]>> =
            (0..num_entries).map(|_| Box::new([0u8; MAX_UDP_SIZE])).collect();

        for (i, buf) in buffers.iter().enumerate() {
            let entry = unsafe { &mut *entries.as_ptr().add(i) };
            entry.set_addr(buf.as_ptr() as u64);
            entry.set_len(MAX_UDP_SIZE as u32);
            entry.set_bid(i as u16);
        }

        // Tail pointer sits just past the last entry (kernel reads it there).
        let tail_ptr = unsafe { io_uring::types::BufRingEntry::tail(entries.as_ptr()) };
        unsafe { std::ptr::write_volatile(tail_ptr as *mut u16, 0) };

        // Register with the kernel.
        let ring_addr = entries.as_ptr() as u64;
        unsafe {
            ring.submitter()
                .register_buf_ring_with_flags(ring_addr, num_entries as u16, bgid, 0)?;
        }

        Ok(Self {
            entries,
            layout,
            buffers,
        })
    }

    /// Recycle a buffer by advancing the ring tail.
    fn recycle(&self, _bid: u16, tail: u16) {
        let tail_ptr = unsafe {
            io_uring::types::BufRingEntry::tail(self.entries.as_ptr()) as *mut u16
        };
        // The entry at position `tail` already has addr/len/bid set from
        // initialisation – just bump the tail so the kernel can reuse it.
        unsafe { std::ptr::write_volatile(tail_ptr, tail.wrapping_add(1)) }
    }
}

#[cfg(target_os = "linux")]
impl Drop for BufRing {
    fn drop(&mut self) {
        // Best-effort unregister.  Ignore errors – the kernel cleans up
        // when the ring fd is closed anyway.
        if let Ok(r) = IoUring::new(1) {
            let _ = r.submitter().unregister_buf_ring(BUF_GROUP_ID);
        }
        unsafe {
            std::alloc::dealloc(self.entries.as_ptr().cast::<u8>(), self.layout);
        }
    }
}

// ---------------------------------------------------------------------------
// Single-shot receive context
// ---------------------------------------------------------------------------

/// Per-buffer receive context (single-shot mode).  Owns the pinned C types
/// that the kernel writes into during RecvMsg, including the peer address.
///
/// # Safety
/// The raw pointers are self-referential (point into the same struct) and
/// are only accessed from the single io_uring event-loop thread.  The
/// struct is moved into `spawn_blocking` but never shared.
#[cfg(target_os = "linux")]
struct RecvCtx {
    addr_storage: libc::sockaddr_storage,
    msghdr: libc::msghdr,
    iovec: libc::iovec,
    buffer: Box<[u8; MAX_UDP_SIZE]>,
}

#[cfg(target_os = "linux")]
unsafe impl Send for RecvCtx {}

#[cfg(target_os = "linux")]
impl RecvCtx {
    fn new() -> Self {
        let mut buffer = Box::new([0u8; MAX_UDP_SIZE]);
        let buf_ptr = buffer.as_mut_ptr();

        let mut ctx = Self {
            addr_storage: unsafe { std::mem::zeroed() },
            msghdr: unsafe { std::mem::zeroed() },
            iovec: unsafe { std::mem::zeroed() },
            buffer,
        };

        ctx.iovec.iov_base = buf_ptr as *mut libc::c_void;
        ctx.iovec.iov_len = MAX_UDP_SIZE;

        ctx.msghdr.msg_name = &mut ctx.addr_storage as *mut _ as *mut libc::c_void;
        ctx.msghdr.msg_namelen = std::mem::size_of::<libc::sockaddr_storage>() as u32;
        ctx.msghdr.msg_iov = &mut ctx.iovec as *mut _;
        ctx.msghdr.msg_iovlen = 1;

        ctx
    }

    /// Extract the peer address populated by the kernel after RecvMsg.
    fn peer_addr(&self) -> Option<SocketAddr> {
        sockaddr_to_socket_addr(&self.addr_storage, self.msghdr.msg_namelen)
    }
}

// ---------------------------------------------------------------------------
// Multishot template context
// ---------------------------------------------------------------------------

/// Backing storage for the multishot `RecvMsgMulti` template `msghdr`.
///
/// The kernel keeps a reference to the `iovec` for the lifetime of the
/// multishot request, so the `iovec` itself (not just the buffer it points
/// to) must live at a stable address. Boxing the whole template guarantees
/// that: the `msghdr.msg_iov` pointer wired in `setup_multishot` stays valid
/// until the server is dropped.
#[cfg(target_os = "linux")]
struct MultishotTemplate {
    iovec: libc::iovec,
    _iov_buf: Box<[u8; MAX_UDP_SIZE]>,
}

#[cfg(target_os = "linux")]
unsafe impl Send for MultishotTemplate {}

// ---------------------------------------------------------------------------
// Send context
// ---------------------------------------------------------------------------

/// Send context.  Owns the response bytes and address so the kernel can
/// read them during SendMsg.  Kept alive until the completion arrives.
#[cfg(target_os = "linux")]
struct SendCtx {
    _response: Vec<u8>,
    _addr: libc::sockaddr_storage,
    _addr_len: libc::socklen_t,
    iovec: libc::iovec,
    msghdr: libc::msghdr,
}

#[cfg(target_os = "linux")]
unsafe impl Send for SendCtx {}

#[cfg(target_os = "linux")]
impl SendCtx {
    /// Allocates a `SendCtx` on the heap and wires its `msghdr`/`iovec`
    /// self-pointers to the box's final address.
    ///
    /// # Safety contract
    /// The `msghdr` points into the box's own fields (`_addr`, `iovec`), so the
    /// box must never be moved out of or relocated while an in-flight `SendMsg`
    /// SQE references it. Callers keep it boxed in a stable slot (never
    /// `swap_remove`d) until the matching completion arrives.
    fn new_boxed(
        response: Vec<u8>,
        addr: libc::sockaddr_storage,
        addr_len: libc::socklen_t,
    ) -> Box<Self> {
        let mut ctx = Box::new(Self {
            _response: response,
            _addr: addr,
            _addr_len: addr_len,
            iovec: unsafe { std::mem::zeroed() },
            msghdr: unsafe { std::mem::zeroed() },
        });

        // Wire self-pointers now that the box has its final, stable address.
        // `_response` owns its heap buffer, so its data pointer is stable
        // regardless of where the box lives.
        ctx.iovec.iov_base = ctx._response.as_ptr() as *mut libc::c_void;
        ctx.iovec.iov_len = ctx._response.len();

        ctx.msghdr.msg_name = &mut ctx._addr as *mut _ as *mut libc::c_void;
        ctx.msghdr.msg_namelen = ctx._addr_len;
        ctx.msghdr.msg_iov = &mut ctx.iovec as *mut _;
        ctx.msghdr.msg_iovlen = 1;

        ctx
    }
}

// ---------------------------------------------------------------------------
// Send context slab (stable-address storage)
// ---------------------------------------------------------------------------

/// Stable-slot storage for in-flight `SendCtx`s.
///
/// Each boxed `SendCtx` lives at a fixed index for its whole lifetime: an
/// in-flight `SendMsg` SQE carries that index in its `user_data`, and the
/// matching completion frees exactly that slot. We never reindex live entries
/// (no `swap_remove`), so the address the kernel reads through stays valid.
/// Freed indices are recycled via a free list to bound growth.
#[cfg(target_os = "linux")]
#[derive(Default)]
struct SendSlab {
    slots: Vec<Option<Box<SendCtx>>>,
    free: Vec<usize>,
}

#[cfg(target_os = "linux")]
impl SendSlab {
    /// Inserts a context and returns its stable index.
    fn insert(&mut self, ctx: Box<SendCtx>) -> usize {
        if let Some(idx) = self.free.pop() {
            self.slots[idx] = Some(ctx);
            idx
        } else {
            self.slots.push(Some(ctx));
            self.slots.len() - 1
        }
    }

    /// Frees the slot at `idx` (if occupied), recycling it for reuse.
    fn remove(&mut self, idx: usize) {
        if let Some(slot) = self.slots.get_mut(idx) {
            if slot.take().is_some() {
                self.free.push(idx);
            }
        }
    }

    /// Raw pointer to the boxed context's `msghdr`, stable across slab growth.
    fn msghdr_ptr(&self, idx: usize) -> Option<*const libc::msghdr> {
        self.slots
            .get(idx)
            .and_then(|s| s.as_ref())
            .map(|ctx| &ctx.msghdr as *const libc::msghdr)
    }
}

// ---------------------------------------------------------------------------
// Channel message
// ---------------------------------------------------------------------------

/// Message passed from handler tasks to the io_uring event loop.
#[cfg(target_os = "linux")]
struct UringSend {
    response: Vec<u8>,
    peer: SocketAddr,
}

// ---------------------------------------------------------------------------
// sockaddr conversion helpers
// ---------------------------------------------------------------------------

#[cfg(target_os = "linux")]
fn socket_addr_to_sockaddr(addr: &SocketAddr) -> (libc::sockaddr_storage, libc::socklen_t) {
    let mut storage: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
    let len;

    match addr {
        SocketAddr::V4(v4) => {
            let sin = unsafe { &mut *(&mut storage as *mut _ as *mut libc::sockaddr_in) };
            sin.sin_family = libc::AF_INET as libc::sa_family_t;
            sin.sin_port = v4.port().to_be();
            sin.sin_addr = libc::in_addr {
                s_addr: u32::from_ne_bytes(v4.ip().octets()),
            };
            len = std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t;
        }
        SocketAddr::V6(v6) => {
            let sin6 = unsafe { &mut *(&mut storage as *mut _ as *mut libc::sockaddr_in6) };
            sin6.sin6_family = libc::AF_INET6 as libc::sa_family_t;
            sin6.sin6_port = v6.port().to_be();
            sin6.sin6_flowinfo = v6.flowinfo();
            sin6.sin6_addr = libc::in6_addr {
                s6_addr: v6.ip().octets(),
            };
            sin6.sin6_scope_id = v6.scope_id();
            len = std::mem::size_of::<libc::sockaddr_in6>() as libc::socklen_t;
        }
    }

    (storage, len)
}

#[cfg(target_os = "linux")]
fn sockaddr_to_socket_addr(
    storage: &libc::sockaddr_storage,
    len: libc::socklen_t,
) -> Option<SocketAddr> {
    if len == 0 {
        return None;
    }

    match storage.ss_family as libc::c_int {
        libc::AF_INET => {
            let sin = unsafe { &*(storage as *const _ as *const libc::sockaddr_in) };
            let ip = Ipv4Addr::from(u32::from_be(sin.sin_addr.s_addr));
            let port = u16::from_be(sin.sin_port);
            Some(SocketAddr::new(IpAddr::V4(ip), port))
        }
        libc::AF_INET6 => {
            let sin6 = unsafe { &*(storage as *const _ as *const libc::sockaddr_in6) };
            let ip = Ipv6Addr::from(sin6.sin6_addr.s6_addr);
            let port = u16::from_be(sin6.sin6_port);
            Some(SocketAddr::new(IpAddr::V6(ip), port))
        }
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------

/// io_uring UDP server instance.
#[cfg(target_os = "linux")]
pub struct UringUdpServer {
    ring: IoUring<squeue::Entry, cqueue::Entry>,
    socket_fd: std::os::raw::c_int,
    /// Single-shot: per-buffer receive contexts.
    recv_ctxs: Vec<RecvCtx>,
    /// Shared send context slab (both modes). Stable-index storage so an
    /// in-flight `SendMsg` SQE's `user_data` always maps back to the same box.
    send_ctxs: SendSlab,
    pending_sends: VecDeque<UringSend>,
    shutdown: Arc<AtomicBool>,
    /// Multishot: provided-buffer ring.
    buf_ring: Option<BufRing>,
    /// Multishot: template msghdr submitted with RecvMsgMulti.
    msghdr_template: Option<libc::msghdr>,
    /// Multishot: heap-allocated backing storage (`iovec` + dummy buffer)
    /// that `msghdr_template.msg_iov` points into. Kept alive for the
    /// lifetime of the server so the kernel never sees a dangling pointer.
    multishot_template: Option<Box<MultishotTemplate>>,
    /// Multishot: whether the multishot SQE is still active (has MORE pending).
    multishot_active: bool,
    /// Running tail counter for buffer recycling.
    buf_tail: u16,
    /// Enable multishot RecvMsgMulti (requires kernel ≥ 6.0).
    multishot: bool,
    /// Bounds concurrently in-flight handler tasks so a UDP flood cannot spawn
    /// tasks without limit. Permits are acquired before spawning and released
    /// when the handler finishes.
    inflight: Arc<Semaphore>,
}

// The raw pointers inside msghdr / iovec are self-referential and only
// accessed from the single io_uring thread; safe to move into spawn_blocking.
#[cfg(target_os = "linux")]
unsafe impl Send for UringUdpServer {}

#[cfg(target_os = "linux")]
impl UringUdpServer {
    /// Create a new io_uring UDP server from an existing UDP socket.
    pub fn new(
        socket: &tokio::net::UdpSocket,
        multishot: bool,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let socket_fd = socket.as_raw_fd();

        let ring = IoUring::builder()
            .setup_single_issuer()
            .build(RING_DEPTH)?;

        Ok(Self {
            ring,
            socket_fd,
            recv_ctxs: (0..NUM_BUFFERS).map(|_| RecvCtx::new()).collect(),
            send_ctxs: SendSlab::default(),
            pending_sends: VecDeque::new(),
            shutdown: Arc::new(AtomicBool::new(false)),
            buf_ring: None,
            msghdr_template: None,
            multishot_template: None,
            multishot_active: false,
            buf_tail: 0,
            multishot,
            inflight: Arc::new(Semaphore::new(MAX_INFLIGHT_HANDLERS)),
        })
    }

    // -----------------------------------------------------------------------
    // Multishot setup
    // -----------------------------------------------------------------------

    fn setup_multishot(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let buf_ring = BufRing::new(&self.ring, BUF_GROUP_ID, NUM_BUFFERS)?;

        // Heap-allocate the template so the `iovec` (which `msghdr.msg_iov`
        // will point at) gets a stable address for the server's lifetime.
        // The kernel writes incoming datagrams into the provided buffer ring,
        // not into `_iov_buf`; the buffer exists only so `iovec.iov_base` is
        // a valid (writable) pointer if the kernel ever consults it.
        let mut template = Box::new(MultishotTemplate {
            iovec: libc::iovec {
                iov_base: std::ptr::null_mut(),
                iov_len: MAX_UDP_SIZE,
            },
            _iov_buf: Box::new([0u8; MAX_UDP_SIZE]),
        });
        template.iovec.iov_base = template._iov_buf.as_mut_ptr() as *mut libc::c_void;

        let mut msghdr: libc::msghdr = unsafe { std::mem::zeroed() };
        msghdr.msg_namelen = std::mem::size_of::<libc::sockaddr_storage>() as u32;
        msghdr.msg_iov = &mut template.iovec as *mut _;
        msghdr.msg_iovlen = 1;

        self.buf_ring = Some(buf_ring);
        self.msghdr_template = Some(msghdr);
        self.multishot_template = Some(template);
        self.multishot_active = false;
        self.buf_tail = 0;

        self.submit_multishot_recv()?;

        info!(
            num_buffers = NUM_BUFFERS,
            buf_group = BUF_GROUP_ID,
            "io_uring multishot recvmsg active"
        );
        Ok(())
    }

    fn submit_multishot_recv(
        &mut self,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let msghdr = self
            .msghdr_template
            .as_mut()
            .ok_or("multishot msghdr missing")?;

        let sqe = opcode::RecvMsgMulti::new(
            Fd(self.socket_fd),
            msghdr as *mut libc::msghdr,
            BUF_GROUP_ID,
        )
        .build();

        unsafe {
            self.ring.submission().push(&sqe)?;
        }
        self.multishot_active = true;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Shared helpers
    // -----------------------------------------------------------------------

    /// Submit pending sends from the handler channel via io_uring SendMsg.
    fn drain_and_submit_sends(&mut self, send_rx: &mut mpsc::Receiver<UringSend>) {
        while let Ok(send) = send_rx.try_recv() {
            self.pending_sends.push_back(send);
        }

        while let Some(send) = self.pending_sends.pop_front() {
            let (storage, addr_len) = socket_addr_to_sockaddr(&send.peer);
            let ctx = SendCtx::new_boxed(send.response, storage, addr_len);
            let idx = self.send_ctxs.insert(ctx);

            // `user_data` encodes the slot as `idx + 1` so it is always non-zero
            // and never collides with the multishot recv completion (user_data
            // == 0). The matching completion frees exactly this slot.
            let msghdr_ptr = match self.send_ctxs.msghdr_ptr(idx) {
                Some(p) => p,
                None => continue,
            };
            let sqe = opcode::SendMsg::new(Fd(self.socket_fd), msghdr_ptr)
                .build()
                .user_data((idx as u64) + 1);

            if unsafe { self.ring.submission().push(&sqe) }.is_err() {
                // SQ is full; reclaim the slot and stop draining. The response
                // is dropped (acceptable for UDP); remaining sends stay queued.
                self.send_ctxs.remove(idx);
                break;
            }
        }
    }

    /// Spawn the DNS handler for a received query.
    ///
    /// Takes the query bytes **by value**. This is load-bearing: in multishot
    /// mode the bytes live in a kernel-provided buffer-ring slot that is
    /// recycled the moment `process_multishot_cqe` returns. Taking ownership
    /// forces the caller to copy the payload out of that slot *before*
    /// recycling (and before this method returns), so the spawned handler task
    /// can never read alias buffer memory while the kernel reuses it. It also
    /// lets `query_wire` share the single owned allocation via `Arc` instead of
    /// re-copying it.
    fn dispatch_query(
        &self,
        query_data: Vec<u8>,
        peer: Option<SocketAddr>,
        handler: &Arc<dyn DnsHandler>,
        send_tx: &mpsc::Sender<UringSend>,
    ) {
        if let Ok(query) = Message::from_vec(&query_data) {
            let meta = QueryMeta {
                protocol: Some("udp".to_string()),
                from_udp: true,
                client_addr: peer.map(|a| a.ip()),
                url_path: None,
                server_name: None,
                selected_upstreams: None,
                query_wire: Some(Arc::new(query_data)),
            };

            // Bound the number of concurrently in-flight handlers. A permit is
            // held for the lifetime of the spawned task; if none is available
            // we drop the datagram rather than spawn unboundedly under a flood.
            let permit = match self.inflight.clone().try_acquire_owned() {
                Ok(p) => p,
                Err(_) => {
                    debug!("io_uring UDP: handler concurrency limit reached, dropping query");
                    return;
                }
            };

            let handler = handler.clone();
            let tx = send_tx.clone();

            tokio::runtime::Handle::current().spawn(async move {
                let _permit = permit;
                match handler.handle(query, meta).await {
                    Ok(resp) => {
                        if let (Ok(resp_bytes), Some(peer)) = (resp.to_vec(), peer) {
                            let _ = tx.send(UringSend { response: resp_bytes, peer }).await;
                        }
                    }
                    Err(e) => {
                        error!(error = %e, "handler error");
                    }
                }
            });
        }
    }

    // -----------------------------------------------------------------------
    // Event loop
    // -----------------------------------------------------------------------

    fn run(
        mut self,
        handler: Arc<dyn DnsHandler>,
        send_tx: mpsc::Sender<UringSend>,
        mut send_rx: mpsc::Receiver<UringSend>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.multishot {
            self.setup_multishot()?;
            info!(
                ring_depth = RING_DEPTH,
                num_buffers = NUM_BUFFERS,
                mode = "multishot",
                "io_uring UDP server starting"
            );
        } else {
            for i in 0..NUM_BUFFERS {
                self.submit_recv(i)?;
            }
            info!(
                ring_depth = RING_DEPTH,
                num_buffers = NUM_BUFFERS,
                mode = "single-shot",
                "io_uring UDP server starting"
            );
        }

        loop {
            if self.shutdown.load(Ordering::Acquire) {
                debug!("io_uring UDP server: shutdown requested");
                break;
            }

            self.drain_and_submit_sends(&mut send_rx);

            // Submit & wait. A persistently failing submit/submit_and_wait would
            // otherwise spin this loop at 100% CPU; back off briefly so a
            // transient error does not turn into a busy-loop.
            if let Err(e) = self.ring.submit() {
                warn!(error = %e, "io_uring submit failed");
                std::thread::sleep(SUBMIT_ERROR_BACKOFF);
                continue;
            }
            match self.ring.submit_and_wait(1) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => {
                    warn!(error = %e, "io_uring submit_and_wait failed");
                    std::thread::sleep(SUBMIT_ERROR_BACKOFF);
                    continue;
                }
            }

            // Collect all completions.
            let completions: Vec<(u64, i32, u32)> = self
                .ring
                .completion()
                .map(|cqe| (cqe.user_data(), cqe.result(), cqe.flags()))
                .collect();

            for (user_data, result, flags) in completions {
                if result < 0 {
                    warn!(result = result, "io_uring operation failed");
                    if self.multishot {
                        if user_data == 0 {
                            // The multishot recv itself errored. An error CQE
                            // terminates the multishot operation just like a
                            // non-MORE completion, so without re-arming here the
                            // server would never receive another datagram on
                            // this socket. Unlike process_multishot_cqe there is
                            // no provided-buffer to recycle (the kernel did not
                            // hand one back on an error), so just submit a fresh
                            // RecvMsgMulti.
                            self.multishot_active = false;
                            if let Err(e) = self.submit_multishot_recv() {
                                error!(error = %e, "failed to re-arm multishot recvmsg after error");
                            }
                        } else if let Some(idx) = send_slab_idx(user_data) {
                            self.send_ctxs.remove(idx);
                        }
                    } else if is_recv_completion(user_data) {
                        let _ = self.submit_recv(recv_buf_idx(user_data));
                    } else if let Some(idx) = send_slab_idx(user_data) {
                        self.send_ctxs.remove(idx);
                    }
                    continue;
                }

                if self.multishot {
                    // user_data == 0 → multishot recv, else → send completion
                    if user_data == 0 {
                        self.process_multishot_cqe(
                            result,
                            flags,
                            &handler,
                            &send_tx,
                        );
                    } else if let Some(idx) = send_slab_idx(user_data) {
                        self.send_ctxs.remove(idx);
                    }
                } else if is_recv_completion(user_data) {
                    self.process_singleshot_recv(
                        result,
                        recv_buf_idx(user_data),
                        &handler,
                        &send_tx,
                    );
                } else if let Some(idx) = send_slab_idx(user_data) {
                    self.send_ctxs.remove(idx);
                }
            }
        }

        debug!("io_uring UDP server: event loop exiting");
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Single-shot recv
    // -----------------------------------------------------------------------

    fn process_singleshot_recv(
        &mut self,
        result: i32,
        buf_idx: usize,
        handler: &Arc<dyn DnsHandler>,
        send_tx: &mpsc::Sender<UringSend>,
    ) {
        let bytes = result as usize;
        if bytes == 0 {
            let _ = self.submit_recv(buf_idx);
            return;
        }

        let peer = self.recv_ctxs[buf_idx].peer_addr();
        let data = self.recv_ctxs[buf_idx].buffer[..bytes].to_vec();
        self.dispatch_query(data, peer, handler, send_tx);
        let _ = self.submit_recv(buf_idx);
    }

    fn submit_recv(
        &mut self,
        buf_idx: usize,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.recv_ctxs[buf_idx].msghdr.msg_namelen =
            std::mem::size_of::<libc::sockaddr_storage>() as u32;

        let sqe = opcode::RecvMsg::new(
            Fd(self.socket_fd),
            &mut self.recv_ctxs[buf_idx].msghdr as *mut libc::msghdr,
        )
        .build()
        .user_data(encode_recv_user_data(buf_idx));

        unsafe {
            self.ring.submission().push(&sqe)?;
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Multishot recv
    // -----------------------------------------------------------------------

    fn process_multishot_cqe(
        &mut self,
        result: i32,
        flags: u32,
        handler: &Arc<dyn DnsHandler>,
        send_tx: &mpsc::Sender<UringSend>,
    ) {
        let bid = match cqueue::buffer_select(flags) {
            Some(b) => b,
            None => return,
        };

        let buf_ring = match &self.buf_ring {
            Some(br) => br,
            None => return,
        };
        let msghdr = match &self.msghdr_template {
            Some(m) => m,
            None => return,
        };

        if result > 0 {
            // `bid` and `result` are supplied by the kernel via the CQE. Validate
            // both before indexing so a malformed/unexpected completion cannot
            // panic the single event-loop thread (which would kill the server).
            let bid_idx = bid as usize;
            let len = result as usize;
            match buf_ring.buffers.get(bid_idx) {
                Some(b) if len <= b.len() => {
                    let raw = &b[..len];
                    if let Ok(out) = io_uring::types::RecvMsgOut::parse(raw, msghdr) {
                        let peer = sockaddr_to_socket_addr(
                            // name_data() is a &[u8] slice over the sockaddr bytes in
                            // the buffer. Reinterpret as sockaddr_storage for our helper.
                            unsafe {
                                &*(out.name_data().as_ptr() as *const libc::sockaddr_storage)
                            },
                            out.incoming_name_len(),
                        );
                        // Copy the payload out of the provided buffer BEFORE
                        // dispatch and BEFORE the slot is recycled below. The
                        // spawned handler runs asynchronously after this method
                        // returns; if we passed `payload_data()` (a borrow into
                        // the buffer-ring slot) instead, the kernel could reuse
                        // that slot for the next datagram while the handler was
                        // still reading it — clobbering the query bytes. Taking
                        // ownership here guarantees the handler reads a stable
                        // snapshot regardless of buffer-ring turnover.
                        let payload = out.payload_data().to_vec();
                        self.dispatch_query(payload, peer, handler, send_tx);
                    }
                }
                _ => {
                    warn!(bid = bid, result = result, "io_uring recv: invalid buffer id/len");
                }
            }
        }

        // Recycle the buffer (always, even on an invalid completion, so the
        // provided-buffer ring is not starved).
        let tail = self.buf_tail;
        buf_ring.recycle(bid, tail);
        self.buf_tail = tail.wrapping_add(1);

        // Re-arm if the multishot operation ended.
        if !cqueue::more(flags) {
            self.multishot_active = false;
            if let Err(e) = self.submit_multishot_recv() {
                error!(error = %e, "failed to re-arm multishot recvmsg");
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Start io_uring UDP server.  Spawns a blocking task and returns immediately.
#[cfg(target_os = "linux")]
pub async fn serve_udp_uring(
    socket: Arc<tokio::net::UdpSocket>,
    handler: Arc<dyn DnsHandler>,
    cancel: CancellationToken,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let multishot = supports_multishot_recvmsg();
    if multishot {
        info!("kernel ≥ 6.0 detected, using multishot recvmsg");
    } else {
        info!("kernel < 6.0 or undetected, using single-shot recvmsg");
    }

    let (send_tx, send_rx) = mpsc::channel::<UringSend>(SEND_CHANNEL_CAPACITY);

    let server = UringUdpServer::new(&socket, multishot)?;
    let shutdown = server.shutdown.clone();

    let handle = tokio::task::spawn_blocking(move || {
        if let Err(e) = server.run(handler, send_tx, send_rx) {
            error!(error = %e, "io_uring UDP server error");
        }
    });

    cancel.cancelled().await;
    debug!("io_uring UDP server: cancellation requested");

    // Signal the event loop to stop.
    shutdown.store(true, Ordering::Release);

    // Wait for the blocking task to finish.
    let _ = handle.await;

    Ok(())
}

/// Check if io_uring is available and functional on this system.
#[cfg(target_os = "linux")]
pub fn is_uring_available() -> bool {
    match IoUring::new(8) {
        Ok(_) => {
            debug!("io_uring is available");
            true
        }
        Err(e) => {
            debug!(error = %e, "io_uring is not available");
            false
        }
    }
}
