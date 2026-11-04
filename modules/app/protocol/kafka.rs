//! kafka — Kafka binary-protocol codec.
//!
//! Reassembles size-delimited requests, decodes the supported API
//! surface into session proposals, and encodes responses back onto the
//! wire. Holds per-connection reassembly state and the advertised
//! host/port used in Metadata responses.
//!
//! ## Response order
//!
//! A connection's responses leave in the order its requests arrived, as
//! the protocol requires (`kafka_order_core.rs`): this codec answers
//! ApiVersions, Metadata, FindCoordinator and InitProducerId itself and
//! `session_processor` answers the rest, some at once and some once they
//! are durable, so an answer ready ahead of its turn waits in a slot
//! until the answers before it are written. At most `kafka_order::DEPTH`
//! requests are in flight per connection, and a request beyond that, or
//! one no slot is free for, stays in the connection's reassembly buffer
//! until there is room; the raw record that would overflow that buffer
//! is held and no raw bytes are taken while it is (`step`), which is the
//! backpressure. A request that will never be answered — malformed, an
//! API this broker does not serve, one `session_processor` marks with
//! `wire::KAFKA_API_CLOSE`, or one whose answer is overdue — closes the
//! connection in its turn: no later answer may pass it.
//!
//! ## Per-step bound
//!
//! `RX_QUOTA` records off `in_raw` and `RESP_QUOTA` off `in_responses`;
//! `step` is O(`KCONNS` × `kafka_order::DEPTH`).

use super::abi::SyscallTable;
use super::kafka_order as order;
use super::{dev_channel_port, dev_log, dev_millis, wire};

/// Per-connection reassembly capacity. Bounds the largest single Kafka
/// request we accept; a request whose `size` field exceeds this closes
/// the conn's reassembly state (fail-closed, client reconnects). Sized
/// so a max request still fits the 8 KiB channel envelope after the
/// session-proposal header is prepended.
const RASM: usize = 8192;
/// Largest accepted value of the Kafka `size` field. Leaves room for
/// the session envelope (`SESSION_HDR` bytes, conn_id included) +
/// tagged-proposal (18 bytes) + wire envelope (3 bytes) downstream of
/// the 8 KiB channel cap.
const MAX_KREQ: usize = 8100;
/// Concurrent Kafka connections per node.
///
/// Each slot carries an 8 KiB reassembly buffer for its whole life —
/// a Kafka connection is long-lived and effectively always mid-stream —
/// so this is `KCONNS * RASM` of resident state (256 -> 2 MiB), paid
/// whether or not the connections exist.
///
/// That is why this stays a static array rather than moving to the
/// per-module heap: `heap_alloc`'s arena is reserved from STATE_ARENA
/// at instantiation, so a heap of the same worst-case size costs the
/// same. The heap wins only where a buffer's lifetime is SHORTER than
/// its slot's (wave's websocket fragments; clustor's pending-apply
/// entries), which is not the case here.
///
/// Raising this further is a memory decision, not a code change.
const KCONNS: usize = 256;
/// Frame/scratch buffer size (responses, outbound envelopes).
const OUT_BUF: usize = 8192;
/// Per-tick drain quotas. Ingress must outrun the router component's 64
/// records/tick; response encode matches the session side's burst.
const RX_QUOTA: usize = 64;
const RESP_QUOTA: usize = 64;
/// Bytes of conn-id prefix on every client record.
const CID: usize = 2;
/// Session envelope header:
/// `[conn_id:u16 LE][proto=1][api_key:i16 LE][api_ver:i16 LE][corr_id:i32 LE]`.
const SESSION_HDR: usize = 11;

const API_PRODUCE: i16 = 0;
const API_FETCH: i16 = 1;
const API_LIST_OFFSETS: i16 = 2;
const API_METADATA: i16 = 3;
const API_OFFSET_COMMIT: i16 = 8;
const API_OFFSET_FETCH: i16 = 9;
const API_FIND_COORDINATOR: i16 = 10;
const API_JOIN_GROUP: i16 = 11;
const API_HEARTBEAT: i16 = 12;
const API_LEAVE_GROUP: i16 = 13;
const API_SYNC_GROUP: i16 = 14;
const API_API_VERSIONS: i16 = 18;
const API_INIT_PRODUCER_ID: i16 = 22;

/// A request whose answer has not come this long after it became its
/// connection's oldest closes the connection. Every request is answered
/// or marked never to be — `session_processor` answers its own expiry
/// (8 s) — so this is the backstop for an answer lost on the way, which
/// would otherwise hold every later answer on the connection for ever.
/// Java's `request.timeout.ms` defaults to 30 s, so the client has not
/// given up on it first.
const HEAD_TIMEOUT_MS: u64 = 15_000;
/// How often the overdue scan runs.
const OVERDUE_SCAN_MS: u64 = 1_000;

/// `KConn::closing`: open.
const OPEN: u8 = 0;
/// A request will never be answered: nothing more is taken, and the
/// connection is closed once the answers ahead of it are written.
const CLOSING: u8 = 1;
/// The close was asked of the transport; what the client still sends is
/// dropped until `MSG_CONN_CLOSED`.
const CLOSED: u8 = 2;

const MAX_TOPICS_SEEN: usize = 8;
const MAX_TOPIC_LEN: usize = 64;
const MAX_HOST_LEN: usize = 48;

#[repr(C)]
#[derive(Clone, Copy)]
struct KConn {
    conn_id: wire::ConnId,
    active: u8,
    /// A complete request is in `buf` that could not be taken yet (no
    /// room in flight, or the session edge refused it).
    stalled: u8,
    /// `OPEN`, `CLOSING` or `CLOSED`.
    closing: u8,
    len: u16,
    /// The requests in flight, in arrival order.
    seq: order::Seq,
    buf: [u8; RASM],
}

impl KConn {
    const fn zero() -> Self {
        Self {
            conn_id: 0,
            active: 0,
            stalled: 0,
            closing: OPEN,
            len: 0,
            seq: order::Seq::zero(),
            buf: [0; RASM],
        }
    }
}

#[repr(C)]
pub struct Kafka {
    /// The composite's anchor: every proposal goes through it
    /// ([`forward`]), which is what binds the connection to a session
    /// worker and holds it across a handoff.
    pub anchor: *mut super::anchor::Anchor,
    pub out_frames: i32,

    // Params
    advertised_host: [u8; MAX_HOST_LEN],
    advertised_host_len: u8,
    pub advertised_port: u16,
    pub partitions: u16,

    requests_decoded: u32,
    responses_encoded: u32,
    parse_errors: u32,
    resync_resets: u32,
    unsupported_dropped: u32,
    /// InitProducerId issuance counter (monotonic; broker-local).
    next_producer_id: u64,
    /// This node's id (param 4), forming the high bits of issued
    /// producer ids so two nodes cannot hand out the same one.
    pub node_id: u16,
    /// Cluster size and per-node client ports, for Metadata.
    pub peer_count: u8,
    pub peer_ports: [u16; super::kafka_metadata::MAX_BROKERS],
    /// Per-node advertised host. A peer with no host configured falls
    /// back to this node's, which is right only when peers share an
    /// address (co-located). Advertising a WRONG host is worse than
    /// advertising one broker — a client would connect somewhere that
    /// does not serve the partition — so each peer gets its own.
    pub peer_hosts: [[u8; MAX_HOST_LEN]; super::kafka_metadata::MAX_BROKERS],
    pub peer_host_lens: [u8; super::kafka_metadata::MAX_BROKERS],
    /// in: `MSG_LEADER_HINT`. `-1` when unwired.
    pub in_leader_state: i32,
    /// Believed raft leader, `HINT_UNKNOWN` until the first hint.
    pub leader_hint: u8,

    topics_seen: [[u8; MAX_TOPIC_LEN]; MAX_TOPICS_SEEN],
    topics_seen_len: [u8; MAX_TOPICS_SEEN],
    topics_seen_count: u8,

    conns: [KConn; KCONNS],
    /// Answers waiting for their turn (`kafka_order_core.rs`).
    pub pool: order::Pool,
    /// A raw record that would have overflowed a stalled connection's
    /// reassembly buffer, held whole until the buffer has room for it;
    /// no raw record is taken while one is held. Length 0 = none.
    rx_held_len: u16,
    rx_held: [u8; OUT_BUF],
    /// Answers kept for their turn, connections closed for a request
    /// never answered, of them for an overdue one, answers for no request
    /// in flight (dropped), requests that waited for room.
    order_held: u32,
    order_closed: u32,
    order_timeouts: u32,
    order_unmatched: u32,
    order_stalls: u32,
    last_scan_ms: u64,
    frame: [u8; OUT_BUF],
    scratch: [u8; OUT_BUF],
}

/// Set the broker address advertised in Metadata responses. Must be the
/// address CLIENTS can reach (e.g. the rig's LAN IP), not the bind
/// address. Bounded and non-empty only; oversize or empty is ignored so
/// the default survives.
///
/// # Safety
///
/// `d` must be valid for `len` reads (the param-macro contract).
/// Record peer `i`'s advertised host.
///
/// # Safety
/// `d` must point to `len` readable bytes, or be null.
pub unsafe fn set_peer_host(s: &mut Kafka, i: usize, d: *const u8, len: usize) {
    if i >= super::kafka_metadata::MAX_BROKERS || d.is_null() || len == 0 || len > MAX_HOST_LEN {
        return;
    }
    core::ptr::copy_nonoverlapping(d, s.peer_hosts[i].as_mut_ptr(), len);
    s.peer_host_lens[i] = len as u8;
}

pub unsafe fn set_advertised_host(s: &mut Kafka, d: *const u8, len: usize) {
    if d.is_null() || len == 0 || len > MAX_HOST_LEN {
        return;
    }
    // SAFETY: caller guarantees `d` is valid for `len` reads, and `len`
    // is bounded by MAX_HOST_LEN above.
    unsafe {
        core::ptr::copy_nonoverlapping(d, s.advertised_host.as_mut_ptr(), len);
    }
    s.advertised_host_len = len as u8;
}

/// Component defaults. Channel handles are assigned by the
/// composite after this returns.
/// Hand a staged envelope to the anchor, which forwards it to the
/// worker the connection is bound to or holds it across a handoff.
/// `false` only when the live worker's channel refused it.
///
/// # Safety
///
/// `sys` must point at a live kernel syscall table.
#[inline]
unsafe fn forward(
    anchor: *mut super::anchor::Anchor,
    sys: &SyscallTable,
    mt: u8,
    payload: &[u8],
) -> bool {
    !anchor.is_null()
        && super::anchor::forward_env(&mut *anchor, sys, super::dev_millis(sys), mt, payload)
}

/// Hand a coordinator call (a group API or a committed-offset request,
/// `anchor::kafka_coordinator_api`) to the anchor, which sends it to the
/// worker coordinating for the pair whichever worker the connection is
/// bound to — with one worker, to that one. `false` only when that
/// channel refused it.
///
/// # Safety
///
/// `sys` must point at a live kernel syscall table.
#[inline]
unsafe fn forward_coord(
    anchor: *mut super::anchor::Anchor,
    sys: &SyscallTable,
    mt: u8,
    payload: &[u8],
) -> bool {
    !anchor.is_null()
        && super::anchor::forward_coord(&mut *anchor, sys, super::dev_millis(sys), mt, payload)
}

pub fn init(s: &mut Kafka) {
    for c in s.conns.iter_mut() {
        *c = KConn::zero();
    }
    order::pool_init(&mut s.pool);
    s.rx_held_len = 0;
    s.topics_seen_count = 0;
    s.next_producer_id = 0;
    s.node_id = 0;
    s.peer_count = 1;
    s.peer_ports = [0; super::kafka_metadata::MAX_BROKERS];
    s.peer_hosts = [[0; MAX_HOST_LEN]; super::kafka_metadata::MAX_BROKERS];
    s.peer_host_lens = [0; super::kafka_metadata::MAX_BROKERS];
    s.in_leader_state = -1;
    s.leader_hint = super::kafka_metadata::HINT_UNKNOWN;
    s.advertised_host_len = 0;
    s.advertised_port = 9090;
    s.partitions = 1;
}

/// Apply the advertised-host default if the graph supplied none. Called
/// by the composite after param parsing.
pub fn finish_init(s: &mut Kafka) {
    if s.advertised_host_len == 0 {
        let def = b"127.0.0.1";
        s.advertised_host[..def.len()].copy_from_slice(def);
        s.advertised_host_len = def.len() as u8;
    }
}

/// Emit a `[conn_id:u16 LE][bytes]` payload as a `MSG_CLIENT_FRAME`
/// envelope toward peer_router. See `mqtt::write_conn_frame` for the
/// rationale.
/// # Safety
unsafe fn write_conn(sys: &SyscallTable, chan: i32, conn_id: wire::ConnId, bytes: &[u8]) -> bool {
    write_conn_mtype(sys, chan, conn_id, wire::MSG_CLIENT_FRAME, bytes)
}

/// [`write_conn`] with the envelope type named: `MSG_CONN_CLOSE_REQUEST`
/// with no bytes asks the transport to close the connection.
/// # Safety
unsafe fn write_conn_mtype(
    sys: &SyscallTable,
    chan: i32,
    conn_id: wire::ConnId,
    mtype: u8,
    bytes: &[u8],
) -> bool {
    if chan < 0 {
        return false;
    }
    let total = CID + bytes.len();
    if total > OUT_BUF {
        return false;
    }
    let mut out = [0u8; OUT_BUF];
    out[..CID].copy_from_slice(&conn_id.to_le_bytes());
    out[CID..total].copy_from_slice(bytes);
    let w = wire::channel_write_msg(sys, chan, mtype, &out[..total]);
    w > 0
}

/// The slot connection `conn_id` holds, if it holds one.
fn lookup_conn(s: &Kafka, conn_id: wire::ConnId) -> Option<usize> {
    (0..KCONNS).find(|&i| s.conns[i].active == 1 && s.conns[i].conn_id == conn_id)
}

/// Free slot `ci`: its requests in flight are forgotten (their answers,
/// should any come, match nothing and are dropped) and its slots given
/// back.
fn release_conn(s: &mut Kafka, ci: usize) {
    order::reset(&mut s.conns[ci].seq, &mut s.pool);
    s.conns[ci].active = 0;
    s.conns[ci].stalled = 0;
    s.conns[ci].closing = OPEN;
    s.conns[ci].len = 0;
}

fn find_conn(s: &mut Kafka, conn_id: wire::ConnId) -> usize {
    if let Some(i) = lookup_conn(s, conn_id) {
        return i;
    }
    // Table full: evict slot 0. Only a fraction of peer_router's client
    // conns speak Kafka; the bench profile stays well inside 32.
    let i = (0..KCONNS).find(|&i| s.conns[i].active == 0).unwrap_or(0);
    release_conn(s, i);
    s.conns[i].conn_id = conn_id;
    s.conns[i].active = 1;
    i
}

fn remember_topic(s: &mut Kafka, name: &[u8]) {
    if name.is_empty() || name.len() > MAX_TOPIC_LEN {
        return;
    }
    let n = s.topics_seen_count as usize;
    for i in 0..n {
        if &s.topics_seen[i][..s.topics_seen_len[i] as usize] == name {
            return;
        }
    }
    if n < MAX_TOPICS_SEEN {
        s.topics_seen[n][..name.len()].copy_from_slice(name);
        s.topics_seen_len[n] = name.len() as u8;
        s.topics_seen_count += 1;
    }
}

// ── Response builders ───────────────────────────────────────────────────────

/// Frame `[size][corr][body]` into `s.frame` from the body in
/// `s.scratch[..body_len]`. Returns the frame's length, or `None` when
/// no codec frame can carry it.
fn frame_response(s: &mut Kafka, corr: i32, body_len: usize) -> Option<usize> {
    let total = 8 + body_len;
    if total + CID > OUT_BUF || body_len > s.scratch.len() {
        return None;
    }
    s.frame[0..4].copy_from_slice(&((4 + body_len) as i32).to_be_bytes());
    s.frame[4..8].copy_from_slice(&corr.to_be_bytes());
    s.frame[8..total].copy_from_slice(&s.scratch[..body_len]);
    Some(total)
}

/// A request on connection `ci` will never be answered: nothing more is
/// taken from it, what it buffered is dropped, and it is closed once the
/// answers ahead are written ([`settle`]).
fn fail_conn(s: &mut Kafka, ci: usize) {
    if s.conns[ci].closing == OPEN {
        s.conns[ci].closing = CLOSING;
    }
    s.conns[ci].len = 0;
    s.conns[ci].stalled = 0;
}

/// Write connection `ci`'s answers that are in order, then close it if
/// its turn to close has come: a request marked never to be answered is
/// at the front, or the connection is closing and nothing is in flight.
///
/// # Safety
/// `sys` must point at a live kernel syscall table.
unsafe fn settle(s: &mut Kafka, sys: &SyscallTable, ci: usize, now: u64) {
    let conn_id = s.conns[ci].conn_id;
    let out = s.out_frames;
    let encoded = &mut s.responses_encoded;
    let front = order::flush(&mut s.conns[ci].seq, &mut s.pool, now, |f| {
        // SAFETY: caller guarantees `sys` is live.
        let w = unsafe { write_conn(sys, out, conn_id, f) };
        if w {
            *encoded = encoded.wrapping_add(1);
        }
        w
    });
    if front == order::Front::Close {
        // Nothing after it may be written: the client is told by the
        // close, and resends what it still wants on a new connection.
        order::reset(&mut s.conns[ci].seq, &mut s.pool);
        fail_conn(s, ci);
    }
    if s.conns[ci].closing == CLOSING && order::is_idle(&s.conns[ci].seq) {
        // SAFETY: caller guarantees `sys` is live.
        if unsafe { write_conn_mtype(sys, out, conn_id, wire::MSG_CONN_CLOSE_REQUEST, &[]) } {
            s.conns[ci].closing = CLOSED;
            s.order_closed = s.order_closed.wrapping_add(1);
        }
    }
}

/// ApiVersions response body into `s.scratch`. Returns body length.
///
/// v0-v2 non-flexible: [error:i16][api_keys: i32 count, per key
/// i16×3][throttle:i32 (v1+)]. For v3+ requests we answer the KIP-511
/// downgrade: a v0-shaped body with error 35 and the full key list.
fn build_api_versions(s: &mut Kafka, api_ver: i16) -> usize {
    let downgrade = api_ver >= 3;
    let error: i16 = if downgrade { 35 } else { 0 };
    const KEYS: [(i16, i16, i16); 13] = [
        (API_PRODUCE, 2, 8),
        (API_FETCH, 0, 5),
        (API_LIST_OFFSETS, 0, 2),
        (API_METADATA, 0, 7),
        (API_OFFSET_COMMIT, 0, 2),
        (API_OFFSET_FETCH, 0, 3),
        (API_FIND_COORDINATOR, 0, 1),
        (API_JOIN_GROUP, 0, 2),
        (API_HEARTBEAT, 0, 1),
        (API_LEAVE_GROUP, 0, 1),
        (API_SYNC_GROUP, 0, 1),
        (API_API_VERSIONS, 0, 2),
        (API_INIT_PRODUCER_ID, 0, 1),
    ];
    let mut p = 0usize;
    s.scratch[p..p + 2].copy_from_slice(&error.to_be_bytes());
    p += 2;
    s.scratch[p..p + 4].copy_from_slice(&(KEYS.len() as i32).to_be_bytes());
    p += 4;
    for &(k, mn, mx) in KEYS.iter() {
        s.scratch[p..p + 2].copy_from_slice(&k.to_be_bytes());
        p += 2;
        s.scratch[p..p + 2].copy_from_slice(&mn.to_be_bytes());
        p += 2;
        s.scratch[p..p + 2].copy_from_slice(&mx.to_be_bytes());
        p += 2;
    }
    if !downgrade && api_ver >= 1 {
        s.scratch[p..p + 4].copy_from_slice(&0i32.to_be_bytes());
        p += 4;
    }
    p
}

/// Append one topic block (error 0, `partitions` partitions, leader and
/// replica set from `view`) to the Metadata response being built in
/// `s.scratch` at offset `p`.
/// Returns the new offset. Capacity is guaranteed by the caller: with
/// `partitions` clamped to 16 and MAX_TOPICS_SEEN = 8, the worst-case
/// response (8 × (77 + 16×34) = 4.9 KiB) fits OUT_BUF.
fn put_metadata_topic(
    s: &mut Kafka,
    mut p: usize,
    name_idx: usize,
    v: i16,
    view: &super::kafka_metadata::MetadataView,
) -> usize {
    let nlen = s.topics_seen_len[name_idx] as usize;
    let parts = s.partitions.clamp(1, 16) as usize;
    s.scratch[p..p + 2].copy_from_slice(&0i16.to_be_bytes());
    p += 2; // error
    s.scratch[p..p + 2].copy_from_slice(&(nlen as i16).to_be_bytes());
    p += 2;
    let name = s.topics_seen[name_idx];
    s.scratch[p..p + nlen].copy_from_slice(&name[..nlen]);
    p += nlen;
    if v >= 1 {
        s.scratch[p] = 0;
        p += 1;
    } // is_internal = false
    s.scratch[p..p + 4].copy_from_slice(&(parts as i32).to_be_bytes());
    p += 4;
    for part in 0..parts {
        s.scratch[p..p + 2].copy_from_slice(&0i16.to_be_bytes());
        p += 2; // error
        s.scratch[p..p + 4].copy_from_slice(&(part as i32).to_be_bytes());
        p += 4;
        // Leader: the raft leader, not always node 0. One Raft group
        // serves every partition today, so its leader IS each
        // partition's leader — true now, and the call site does not
        // change when placement starts answering per shard.
        s.scratch[p..p + 4].copy_from_slice(&view.leader_for(part as u32).to_be_bytes());
        p += 4;
        if v >= 7 {
            s.scratch[p..p + 4].copy_from_slice(&0i32.to_be_bytes());
            p += 4; // leader_epoch
        }
        // Replicas and ISR span every node hosting the group, not [0]:
        // a client told there is one replica believes the partition has
        // no redundancy.
        let nrep = view.replica_count();
        for _ in 0..2 {
            s.scratch[p..p + 4].copy_from_slice(&(nrep as i32).to_be_bytes());
            p += 4;
            for r in 0..nrep {
                s.scratch[p..p + 4].copy_from_slice(&(r as i32).to_be_bytes());
                p += 4;
            }
        }
        if v >= 5 {
            s.scratch[p..p + 4].copy_from_slice(&0i32.to_be_bytes());
            p += 4; // offline: []
        }
    }
    p
}

/// Metadata response body (non-flexible v0-v7) into `s.scratch`.
/// `req` is the request body after client_id. Returns body length.
impl Kafka {
    /// Snapshot the cluster shape for a Metadata answer.
    ///
    /// A peer with no configured port falls back to this node's, so a
    /// single-node graph — which configures none — still advertises a
    /// reachable broker rather than port 0.
    pub fn metadata_view(&self) -> super::kafka_metadata::MetadataView {
        let mut ports = self.peer_ports;
        let me = (self.node_id as usize).min(super::kafka_metadata::MAX_BROKERS - 1);
        if ports[me] == 0 {
            ports[me] = self.advertised_port;
        }
        super::kafka_metadata::MetadataView {
            self_id: self.node_id as u8,
            peer_count: self.peer_count.max(1),
            peer_ports: ports,
            leader_hint: self.leader_hint,
        }
    }
}

fn build_metadata(s: &mut Kafka, v: i16, req: &[u8]) -> usize {
    // Parse requested topic names first (also refreshes topics_seen).
    // Request: [topics: i32 count][string...]  (count -1 = all, v1+)
    let mut want: [usize; MAX_TOPICS_SEEN] = [0; MAX_TOPICS_SEEN];
    let mut want_n = 0usize;
    let mut all = true;
    if req.len() >= 4 {
        let count = i32::from_be_bytes([req[0], req[1], req[2], req[3]]);
        if count > 0 {
            all = false;
            let mut off = 4usize;
            for _ in 0..count.min(MAX_TOPICS_SEEN as i32) {
                if off + 2 > req.len() {
                    break;
                }
                let nlen = i16::from_be_bytes([req[off], req[off + 1]]) as usize;
                off += 2;
                if off + nlen > req.len() || nlen == 0 || nlen > MAX_TOPIC_LEN {
                    break;
                }
                let mut name = [0u8; MAX_TOPIC_LEN];
                name[..nlen].copy_from_slice(&req[off..off + nlen]);
                off += nlen;
                remember_topic(s, &name[..nlen]);
                // Locate index in topics_seen (remember_topic guarantees
                // presence unless the table is full).
                for i in 0..s.topics_seen_count as usize {
                    if s.topics_seen[i][..s.topics_seen_len[i] as usize] == name[..nlen]
                        && want_n < MAX_TOPICS_SEEN
                    {
                        want[want_n] = i;
                        want_n += 1;
                        break;
                    }
                }
            }
        } else if count == 0 {
            // v0: empty array means "all topics"; keep `all`.
        }
    }
    if all {
        want_n = s.topics_seen_count as usize;
        for (i, w) in want.iter_mut().enumerate().take(want_n) {
            *w = i;
        }
    }

    let mut p = 0usize;
    if v >= 3 {
        s.scratch[p..p + 4].copy_from_slice(&0i32.to_be_bytes());
        p += 4; // throttle
    }
    // Brokers: every node in the cluster, not just this one. A response
    // naming one broker tells the client the cluster is a single
    // machine, and no client can then distribute load however well the
    // substrate partitions.
    //
    // Each broker is advertised on ITS OWN host when the graph supplies
    // one (`peer{N}_host`), falling back to this node's address only for
    // a peer that shares it. Advertising a wrong host is worse than
    // advertising one broker: the client connects somewhere that does
    // not serve the partition.
    let view = s.metadata_view();
    let nbrokers = view.broker_count();
    s.scratch[p..p + 4].copy_from_slice(&(nbrokers as i32).to_be_bytes());
    p += 4;
    for bi in 0..nbrokers {
        let Some(b) = view.broker(bi) else { break };
        // Peer's own host when the graph gave one; this node's only as a
        // fallback for a co-located peer.
        let (host, hlen) = if s.peer_host_lens[bi] != 0 {
            (s.peer_hosts[bi], s.peer_host_lens[bi] as usize)
        } else {
            (s.advertised_host, s.advertised_host_len as usize)
        };
        s.scratch[p..p + 4].copy_from_slice(&b.node_id.to_be_bytes());
        p += 4;
        s.scratch[p..p + 2].copy_from_slice(&(hlen as i16).to_be_bytes());
        p += 2;
        s.scratch[p..p + hlen].copy_from_slice(&host[..hlen]);
        p += hlen;
        s.scratch[p..p + 4].copy_from_slice(&(b.port as i32).to_be_bytes());
        p += 4;
        if v >= 1 {
            s.scratch[p..p + 2].copy_from_slice(&(-1i16).to_be_bytes());
            p += 2; // rack null
        }
    }
    if v >= 2 {
        s.scratch[p..p + 2].copy_from_slice(&(-1i16).to_be_bytes());
        p += 2; // cluster_id null
    }
    if v >= 1 {
        // Controller: the raft leader, which is the node that can serve
        // an admin request. Naming node 0 unconditionally sent every
        // client's admin traffic at one node regardless of who led.
        s.scratch[p..p + 4].copy_from_slice(&view.leader_for(0).to_be_bytes());
        p += 4;
    }
    s.scratch[p..p + 4].copy_from_slice(&(want_n as i32).to_be_bytes());
    p += 4;
    for w in want.iter().take(want_n) {
        p = put_metadata_topic(s, p, *w, v, &view);
    }
    p
}

// ── Step ────────────────────────────────────────────────────────────────────

/// Process one complete Kafka request sitting at `conn.buf[off..off+total]`.
/// `false` when it cannot be taken yet — no room in flight on its
/// connection, no slot for its answer, or the edge refused it: it stays
/// where it is and is offered again ([`step`]). A request that can never
/// be answered fails the connection ([`fail_conn`]) and is taken.
/// # Safety
unsafe fn handle_request(
    s: &mut Kafka,
    sys: &SyscallTable,
    conn_id: wire::ConnId,
    ci: usize,
    off: usize,
    total: usize,
    now: u64,
) -> bool {
    let req = &s.conns[ci].buf[off..off + total];
    if total < 12 {
        s.parse_errors = s.parse_errors.wrapping_add(1);
        fail_conn(s, ci);
        return true;
    }
    let api_key = i16::from_be_bytes([req[4], req[5]]);
    let api_ver = i16::from_be_bytes([req[6], req[7]]);
    let corr = i32::from_be_bytes([req[8], req[9], req[10], req[11]]);
    // client_id: nullable string right after the header (all non-flexible
    // request header versions we advertise).
    let mut body_off = 12usize;
    if total >= body_off + 2 {
        let cl = i16::from_be_bytes([req[body_off], req[body_off + 1]]);
        body_off += 2;
        if cl > 0 {
            body_off += cl as usize;
            if body_off > total {
                s.parse_errors = s.parse_errors.wrapping_add(1);
                fail_conn(s, ci);
                return true;
            }
        }
    } else {
        s.parse_errors = s.parse_errors.wrapping_add(1);
        fail_conn(s, ci);
        return true;
    }
    let local = matches!(
        api_key,
        API_API_VERSIONS | API_METADATA | API_FIND_COORDINATOR | API_INIT_PRODUCER_ID
    );
    let forwarded = matches!(
        api_key,
        API_PRODUCE
            | API_FETCH
            | API_LIST_OFFSETS
            | API_OFFSET_COMMIT
            | API_OFFSET_FETCH
            | API_JOIN_GROUP
            | API_HEARTBEAT
            | API_LEAVE_GROUP
            | API_SYNC_GROUP
    );
    if !local && !forwarded {
        // An API this broker does not serve has no answer here, and a
        // request left unanswered would hold every later answer: the
        // connection closes in its turn, as a Kafka broker closes one on
        // an unsupported request.
        s.unsupported_dropped = s.unsupported_dropped.wrapping_add(1);
        fail_conn(s, ci);
        return true;
    }
    let answered = order::answered(api_key, api_ver, &req[body_off..]);
    // Its place in the connection's order, and the slot its answer
    // waits in if it is not the oldest.
    let slot = if answered {
        match order::reserve(&s.conns[ci].seq, &mut s.pool, order::class_for(api_key)) {
            Some(slot) => slot,
            None => return false,
        }
    } else {
        order::NO_SLOT
    };

    if local {
        let blen = match api_key {
            API_API_VERSIONS => build_api_versions(s, api_ver),
            API_METADATA => {
                // Copy the request body into `frame` (as scratch input) so
                // build_metadata can borrow it while writing s.scratch.
                let body_len = total - body_off;
                if body_len > OUT_BUF {
                    order::unreserve(&mut s.pool, slot);
                    s.parse_errors = s.parse_errors.wrapping_add(1);
                    fail_conn(s, ci);
                    return true;
                }
                core::ptr::copy_nonoverlapping(
                    s.conns[ci].buf.as_ptr().add(off + body_off),
                    s.frame.as_mut_ptr(),
                    body_len,
                );
                // SAFETY: build_metadata writes only s.scratch; req_copy
                // aliases s.frame, which build_metadata never touches.
                let req_copy: &[u8] = core::slice::from_raw_parts(s.frame.as_ptr(), body_len);
                build_metadata(s, api_ver, req_copy)
            }
            API_FIND_COORDINATOR => {
                // Single logical broker: the coordinator is always us.
                // v0: [err][node_id][host][port]
                // v1: [throttle][err][error_message null][node_id][host][port]
                let hlen = s.advertised_host_len as usize;
                let host = s.advertised_host;
                let port = s.advertised_port as i32;
                let mut p = 0usize;
                if api_ver >= 1 {
                    s.scratch[p..p + 4].copy_from_slice(&0i32.to_be_bytes());
                    p += 4;
                }
                s.scratch[p..p + 2].copy_from_slice(&0i16.to_be_bytes());
                p += 2;
                if api_ver >= 1 {
                    s.scratch[p..p + 2].copy_from_slice(&(-1i16).to_be_bytes());
                    p += 2;
                }
                s.scratch[p..p + 4].copy_from_slice(&0i32.to_be_bytes());
                p += 4; // node 0
                s.scratch[p..p + 2].copy_from_slice(&(hlen as i16).to_be_bytes());
                p += 2;
                s.scratch[p..p + hlen].copy_from_slice(&host[..hlen]);
                p += hlen;
                s.scratch[p..p + 4].copy_from_slice(&port.to_be_bytes());
                p += 4;
                p
            }
            // API_INIT_PRODUCER_ID, the one local API left.
            _ => {
                // Producer id issuance. Sequence enforcement IS implemented:
                // `session_processor` checks `(producer_id, epoch, sequence)`
                // on the APPLY side, so a retried batch is recognised and not
                // appended twice, and a gap answers
                // OUT_OF_ORDER_SEQUENCE_NUMBER. See
                // `modules/common/cores/kafka_idem_core.rs`.
                //
                // Ids are issued from a per-connection counter, NOT a
                // cluster-wide one: two nodes will hand out the same id, so
                // this is single-node-correct only. A cluster needs the id
                // allocated through the control plane.
                // v0/v1: [throttle][err][producer_id i64][producer_epoch i16]
                // `[node_id:16][counter:40]`, so ids from different nodes can
                // never collide. 40 bits is ~1.1e12 producers per node, and
                // the whole value stays well inside a positive i64 — which
                // matters because a non-positive producer id means "not
                // idempotent" to the apply-side check.
                //
                // KNOWN GAP: the counter restarts at 1 when a node restarts,
                // so a node CAN reissue an id it previously handed out. A
                // producer holding the old one would then share sequence
                // state with the new holder. Closing that needs the id
                // allocated through the control plane (or persisted), which
                // is control-plane work this does not pretend to do.
                s.next_producer_id = s.next_producer_id.wrapping_add(1);
                let pid =
                    (((s.node_id as u64) << 40) | (s.next_producer_id & 0x00FF_FFFF_FFFF)) as i64;
                let mut p = 0usize;
                s.scratch[p..p + 4].copy_from_slice(&0i32.to_be_bytes());
                p += 4;
                s.scratch[p..p + 2].copy_from_slice(&0i16.to_be_bytes());
                p += 2;
                s.scratch[p..p + 8].copy_from_slice(&pid.to_be_bytes());
                p += 8;
                s.scratch[p..p + 2].copy_from_slice(&0i16.to_be_bytes());
                p += 2;
                p
            }
        };
        let Some(n) = frame_response(s, corr, blen) else {
            order::unreserve(&mut s.pool, slot);
            s.parse_errors = s.parse_errors.wrapping_add(1);
            fail_conn(s, ci);
            return true;
        };
        if slot == order::NO_SLOT {
            // Nothing in flight ahead of it: answered now, or offered
            // again once the edge takes it.
            let out = s.out_frames;
            if !write_conn(sys, out, conn_id, &s.frame[..n]) {
                return false;
            }
            s.responses_encoded = s.responses_encoded.wrapping_add(1);
        } else if order::push_answered(
            &mut s.conns[ci].seq,
            &mut s.pool,
            corr,
            slot,
            &s.frame[..n],
            now,
        ) {
            s.order_held = s.order_held.wrapping_add(1);
        } else {
            // The answer outgrew its slot class (unreachable by
            // `class_for`): unanswerable, so the connection closes.
            fail_conn(s, ci);
        }
        s.requests_decoded = s.requests_decoded.wrapping_add(1);
        return true;
    }

    // Envelope: [conn_id:u16 LE][proto=1][api_key LE][api_ver LE]
    //           [corr LE][body]
    let body_len = total - body_off;
    let env_len = SESSION_HDR + body_len;
    if env_len > OUT_BUF {
        order::unreserve(&mut s.pool, slot);
        s.parse_errors = s.parse_errors.wrapping_add(1);
        fail_conn(s, ci);
        return true;
    }
    s.scratch[0..2].copy_from_slice(&conn_id.to_le_bytes());
    s.scratch[2] = 1; // PROTO_KAFKA
    s.scratch[3..5].copy_from_slice(&api_key.to_le_bytes());
    s.scratch[5..7].copy_from_slice(&api_ver.to_le_bytes());
    s.scratch[7..11].copy_from_slice(&corr.to_le_bytes());
    core::ptr::copy_nonoverlapping(
        s.conns[ci].buf.as_ptr().add(off + body_off),
        s.scratch.as_mut_ptr().add(SESSION_HDR),
        body_len,
    );
    // Group membership and committed offsets are one worker's for the
    // pair (the coordinator); the log the others read is every worker's.
    let taken = if super::anchor::kafka_coordinator_api(api_key) {
        forward_coord(
            s.anchor,
            sys,
            wire::MSG_SESSION_PROPOSAL,
            &s.scratch[..env_len],
        )
    } else {
        forward(
            s.anchor,
            sys,
            wire::MSG_SESSION_PROPOSAL,
            &s.scratch[..env_len],
        )
    };
    if !taken {
        // codec_in saturated: the request stays in the buffer and is
        // offered again, rather than dropped and left unanswered.
        order::unreserve(&mut s.pool, slot);
        return false;
    }
    if answered {
        order::push(&mut s.conns[ci].seq, corr, slot, now);
    }
    s.requests_decoded = s.requests_decoded.wrapping_add(1);
    true
}

/// Drain raft leader hints so Metadata can name the real leader.
///
/// Monotone in nothing — the leader legitimately changes — so the
/// latest hint simply wins. An unwired port leaves `leader_hint` at
/// `HINT_UNKNOWN`, and Metadata answers self, which is the honest
/// answer from a node that is serving the request.
///
/// # Safety
///
/// `sys` must point at a live kernel syscall table.
pub unsafe fn drain_leader_state(s: &mut Kafka, sys: &SyscallTable) {
    if s.in_leader_state < 0 {
        return;
    }
    let mut buf = [0u8; 16];
    for _ in 0..4 {
        let (msg_type, plen) = wire::channel_read_msg(sys, s.in_leader_state, &mut buf);
        if msg_type == 0 && plen == 0 {
            break;
        }
        if msg_type == wire::MSG_LEADER_HINT && (plen as usize) >= 1 {
            s.leader_hint = buf[0];
        }
    }
}

/// Handle one client record: `[conn_id:u16 LE][tcp chunk]` framed as
/// MSG_CLIENT_FRAME, or MSG_CONN_CLOSED carrying just the conn id.
/// Appends to the connection's reassembly buffer and takes every
/// complete size-delimited request from it that there is room for.
/// `true` when the record is HELD: it would overflow a connection whose
/// requests are waiting for room, so the caller must take no more raw
/// records until [`step`] lands it.
///
/// # Safety
///
/// `sys` must point at a live kernel syscall table.
pub unsafe fn on_frame(s: &mut Kafka, sys: &SyscallTable, mtype: u8, payload: &[u8]) -> bool {
    // Stage the record exactly as the channel read did:
    // `frame` holds `[conn_id][tcp chunk]`.
    let n = payload.len();
    if n > s.frame.len() {
        return false;
    }
    s.frame[..n].copy_from_slice(payload);
    // SAFETY: caller guarantees `sys` is live.
    unsafe { on_staged(s, sys, mtype, n) }
}

/// [`on_frame`] for the record staged in `s.frame[..n]`.
///
/// # Safety
///
/// `sys` must point at a live kernel syscall table.
unsafe fn on_staged(s: &mut Kafka, sys: &SyscallTable, mtype: u8, n: usize) -> bool {
    // SAFETY: caller guarantees `sys` is live.
    unsafe {
        if n < CID {
            return false;
        }
        let conn_id = u16::from_le_bytes([s.frame[0], s.frame[1]]);
        let now = dev_millis(sys);

        // Connection closed: release this conn's reassembly slot and
        // tell session_processor to drop conn-keyed state (groups).
        if mtype == wire::MSG_CONN_CLOSED {
            if let Some(ci) = lookup_conn(s, conn_id) {
                release_conn(s, ci);
            }
            // Forward as MSG_CONN_CLOSED (NOT MSG_SESSION_DISCONNECT —
            // that type already means "MQTT DISCONNECT packet" on this
            // fan-in lane and is claimed by session_processor's MQTT
            // path). session_processor releases conn-keyed state on it.
            let cb = conn_id.to_le_bytes();
            forward(s.anchor, sys, wire::MSG_CONN_CLOSED, &cb);
            return false;
        }
        if n <= CID {
            return false;
        }
        let ci = find_conn(s, conn_id);
        if s.conns[ci].closing != OPEN {
            // Nothing more is taken from a connection that is closing.
            return false;
        }

        // Append to the conn's reassembly buffer as far as it has room,
        // taking the requests that completes; a chunk larger than the
        // room left goes in as the requests ahead of it make room.
        let mut src = CID;
        while src < n {
            let have = s.conns[ci].len as usize;
            let take = (RASM - have).min(n - src);
            core::ptr::copy_nonoverlapping(
                s.frame.as_ptr().add(src),
                s.conns[ci].buf.as_mut_ptr().add(have),
                take,
            );
            s.conns[ci].len = (have + take) as u16;
            src += take;
            drain_conn(s, sys, ci, now);
            settle(s, sys, ci, now);
            if src == n || s.conns[ci].closing != OPEN {
                break;
            }
            if usize::from(s.conns[ci].len) < RASM {
                continue;
            }
            if s.conns[ci].stalled != 0 {
                // Its requests are waiting for room: hold the rest of the
                // record, and with it every later one, until they are
                // taken.
                let rest = n - src;
                s.rx_held[..CID].copy_from_slice(&conn_id.to_le_bytes());
                s.rx_held[CID..CID + rest].copy_from_slice(&s.frame[src..n]);
                s.rx_held_len = (CID + rest) as u16;
                return true;
            }
            // A full buffer holding no complete request: a size field
            // past what this buffer can carry got through, so the stream
            // cannot be trusted. The connection closes once the answers
            // ahead are written.
            s.resync_resets = s.resync_resets.wrapping_add(1);
            fail_conn(s, ci);
            settle(s, sys, ci, now);
            break;
        }
        false
    }
}

/// Take every complete request at the front of connection `ci`'s
/// reassembly buffer that there is room for, leaving the rest there:
/// `stalled` says whether a complete one is left.
///
/// # Safety
///
/// `sys` must point at a live kernel syscall table.
unsafe fn drain_conn(s: &mut Kafka, sys: &SyscallTable, ci: usize, now: u64) {
    // SAFETY: caller guarantees `sys` is live.
    unsafe {
        let conn_id = s.conns[ci].conn_id;
        s.conns[ci].stalled = 0;
        let mut off = 0usize;
        let mut avail = s.conns[ci].len as usize;
        loop {
            if avail < 4 || s.conns[ci].closing != OPEN {
                break;
            }
            let size = i32::from_be_bytes([
                s.conns[ci].buf[off],
                s.conns[ci].buf[off + 1],
                s.conns[ci].buf[off + 2],
                s.conns[ci].buf[off + 3],
            ]);
            if size <= 0 || size as usize > MAX_KREQ {
                // Unrecoverable desync for a stream protocol.
                s.resync_resets = s.resync_resets.wrapping_add(1);
                fail_conn(s, ci);
                break;
            }
            let total = 4 + size as usize;
            if avail < total {
                break;
            }
            if !handle_request(s, sys, conn_id, ci, off, total, now) {
                s.conns[ci].stalled = 1;
                s.order_stalls = s.order_stalls.wrapping_add(1);
                break;
            }
            off += total;
            avail -= total;
        }
        if s.conns[ci].closing != OPEN {
            // `fail_conn` dropped the buffer.
            return;
        }
        // Compact leftover fragment to the front.
        if off > 0 && avail > 0 {
            core::ptr::copy(
                s.conns[ci].buf.as_ptr().add(off),
                s.conns[ci].buf.as_mut_ptr(),
                avail,
            );
        }
        if off > 0 {
            s.conns[ci].len = avail as u16;
        }
    }
}

/// Once per step, ahead of the raw read: write the answers whose turn
/// came, take the requests that were waiting for room, close the
/// connections whose turn to close came or whose oldest answer is
/// overdue, then land the held raw record. `true` when the caller may
/// take raw records; `false` while the held one still has no room.
///
/// # Safety
///
/// `sys` must point at a live kernel syscall table.
pub unsafe fn step(s: &mut Kafka, sys: &SyscallTable) -> bool {
    // SAFETY: caller guarantees `sys` is live.
    unsafe {
        let now = dev_millis(sys);
        let scan = now.wrapping_sub(s.last_scan_ms) >= OVERDUE_SCAN_MS;
        if scan {
            s.last_scan_ms = now;
        }
        for ci in 0..KCONNS {
            if s.conns[ci].active == 0 {
                continue;
            }
            if scan && order::overdue(&s.conns[ci].seq, now, HEAD_TIMEOUT_MS) {
                s.order_timeouts = s.order_timeouts.wrapping_add(1);
                order::reset(&mut s.conns[ci].seq, &mut s.pool);
                fail_conn(s, ci);
            }
            let c = &s.conns[ci];
            if c.stalled == 0 && c.closing != CLOSING && !order::has_work(&c.seq) {
                continue;
            }
            settle(s, sys, ci, now);
            if s.conns[ci].stalled != 0 {
                drain_conn(s, sys, ci, now);
                settle(s, sys, ci, now);
            }
        }
        let n = s.rx_held_len as usize;
        if n == 0 {
            return true;
        }
        s.rx_held_len = 0;
        s.frame[..n].copy_from_slice(&s.rx_held[..n]);
        !on_staged(s, sys, wire::MSG_CLIENT_FRAME, n)
    }
}

/// Encode one session response into a Kafka response frame and write it
/// in its turn.
///
/// Payload is the session envelope
/// `[conn_id:u16 LE][proto=1][api_key:i16 LE][api_ver:i16 LE][corr_id:i32 LE][body]`
/// (see session_processor's `emit_kafka_response`). The body is already a
/// complete, correctly-versioned response body for `api_key`/`api_ver`;
/// this reframes it as the wire form `[size:i32 BE][corr:i32 BE][body]`,
/// writes it to the originating connection if it answers the oldest
/// request in flight there, and otherwise keeps it until it does. An
/// `api_key` of `wire::KAFKA_API_CLOSE` answers nothing: the request
/// will never be answered, and the connection closes in its turn.
/// `false` when the response answers the oldest request and the frame
/// edge refused it: nothing changed, and it must be offered again.
///
/// # Safety
///
/// `sys` must point at a live kernel syscall table.
pub unsafe fn on_response(s: &mut Kafka, sys: &SyscallTable, payload: &[u8]) -> bool {
    let plen = payload.len();
    // conn(2) + proto(1) + api_key(2) + api_ver(2) + corr(4)
    if plen < SESSION_HDR {
        return true;
    }
    let conn_id = u16::from_le_bytes([payload[0], payload[1]]);
    // payload[2] is the PROTO_KAFKA discriminator; the dispatch table has
    // already matched it.
    let api_key = i16::from_le_bytes([payload[3], payload[4]]);
    let corr = i32::from_le_bytes([payload[7], payload[8], payload[9], payload[10]]);
    let Some(ci) = lookup_conn(s, conn_id) else {
        // The connection is gone: nothing to answer.
        return true;
    };
    if s.conns[ci].closing == CLOSED {
        return true;
    }
    let body_len = plen - SESSION_HDR;
    let framed = if api_key == wire::KAFKA_API_CLOSE || body_len > s.scratch.len() {
        if api_key != wire::KAFKA_API_CLOSE {
            s.parse_errors = s.parse_errors.wrapping_add(1);
        }
        None
    } else {
        s.scratch[..body_len].copy_from_slice(&payload[SESSION_HDR..plen]);
        let f = frame_response(s, corr, body_len);
        if f.is_none() {
            s.parse_errors = s.parse_errors.wrapping_add(1);
        }
        f
    };
    // SAFETY: caller guarantees `sys` is live.
    unsafe {
        let now = dev_millis(sys);
        let out = s.out_frames;
        let encoded = &mut s.responses_encoded;
        let r = order::answer(
            &mut s.conns[ci].seq,
            &mut s.pool,
            corr,
            framed.map(|n| &s.frame[..n]),
            now,
            |f| {
                let w = write_conn(sys, out, conn_id, f);
                if w {
                    *encoded = encoded.wrapping_add(1);
                }
                w
            },
        );
        match r {
            order::Answered::Refused => return false,
            order::Answered::Unmatched => {
                // Writing it would put it out of order (or answer a
                // connection that never asked): dropped, counted.
                s.order_unmatched = s.order_unmatched.wrapping_add(1);
                if s.order_unmatched & 0x3F == 1 {
                    dev_log(sys, 2, b"[kafka] response for no request".as_ptr(), 31);
                }
                return true;
            }
            order::Answered::Overflow => {
                s.parse_errors = s.parse_errors.wrapping_add(1);
            }
            order::Answered::Kept => {
                if framed.is_some() {
                    s.order_held = s.order_held.wrapping_add(1);
                }
            }
            order::Answered::Written => {}
        }
        settle(s, sys, ci, now);
        if s.conns[ci].stalled != 0 {
            drain_conn(s, sys, ci, now);
            settle(s, sys, ci, now);
        }
    }
    true
}

/// Fill the component's metric payload. Returns the byte count:
/// `[requests_decoded][responses_encoded][parse_errors][answers held for
/// their turn][connections closed for an unanswered request][of them for
/// an overdue answer]`, each `u32 LE`.
pub fn metrics(s: &Kafka, m: &mut [u8; 24]) -> usize {
    m[0..4].copy_from_slice(&s.requests_decoded.to_le_bytes());
    m[4..8].copy_from_slice(&s.responses_encoded.to_le_bytes());
    m[8..12].copy_from_slice(&s.parse_errors.to_le_bytes());
    m[12..16].copy_from_slice(&s.order_held.to_le_bytes());
    m[16..20].copy_from_slice(&s.order_closed.to_le_bytes());
    m[20..24].copy_from_slice(&s.order_timeouts.to_le_bytes());
    24
}
