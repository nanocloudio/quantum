//! sessions — the per-session state machine table.
//!
//! Owns `Session[MAX_SESSIONS]` and each session's QoS `Inflight`
//! sub-table: identity (tenant, stream hash, epoch), connection binding,
//! keep-alive and expiry deadlines, will-message state, and the
//! in-flight QoS 1/2 packets awaiting their next protocol step.
//!
//! ## Per-step bound
//!
//! The table is indexed, so the per-packet entry points — `find_by_conn`,
//! `find_by_stream`, `allocate`, `unbind_other_conns`, `conn_flags` and
//! every per-slot mutator — are O(1) expected over fixed tables and
//! return without blocking. Two intrusive chained hash indexes hang off
//! the slot array: one keyed by `(tenant, stream_hash)` over slots with
//! `present == 1 || transient == 1`, one keyed by `conn_id` over slots
//! with `conn_bound == 1`. Chains are `slot + 1` links with 0 as the end
//! marker; bucket counts are powers of two at twice `MAX_SESSIONS`. A
//! free stack backs allocation. `reindex` is the single maintenance
//! point: every mutator that can change a key or a membership flag ends
//! by calling it, and it reconciles the slot's chain memberships and
//! free-stack presence against the recorded copy of its keys.
//!
//! Only `init` and `release_foreign` walk the whole table; the
//! per-second sweeps in the parent module do their own linear walk.

use super::{
    SessionGeneration, StreamHash, TenantId, MAX_INFLIGHT_PER_SESSION, MAX_SESSIONS,
    MAX_WILL_PAYLOAD, MAX_WILL_TOPIC, PROTO_UNKNOWN, QOS2_PUBLISH,
};

#[repr(C)]
#[derive(Clone, Copy)]
pub struct Inflight {
    packet_id: u16,
    qos: u8,
    phase: u8,
    /// Session epoch the flow opened under. The dedupe key that names
    /// this flow's durable record is keyed by it, and a reconnect bumps
    /// the session's epoch, so a QoS 2 transaction that spans one must
    /// carry the epoch it started with rather than read the session's
    /// current value. 0 for slots opened before any publish named one.
    session_generation: SessionGeneration,
    wal_index: u64,
    /// Raft correlation_id for the proposal that owns this slot (QoS 1+
    /// only; 0 for QoS 0 and subscriber-side slots). Used to look up the
    /// stashed topic-publish envelope on durability and to re-emit the
    /// proposal on MSG_ACK_REDELIVER.
    correlation_id: u64,
    /// Raft partition the publish was assigned to, with `wal_index`:
    /// the pair `flow` keys its ledger by, so an imported inflight can
    /// be re-registered exactly.
    partition_id: u16,
    direction: u8,
    /// QoS 2 publisher flow: the PUBREC has been emitted, so a further
    /// durability completion for the PUBLISH owes the client nothing.
    rec_sent: u8,
    active: u8,
}

impl Inflight {
    pub const fn zero() -> Self {
        Self {
            packet_id: 0,
            qos: 0,
            phase: 0,
            session_generation: 0,
            wal_index: 0,
            correlation_id: 0,
            partition_id: 0,
            direction: 0,
            rec_sent: 0,
            active: 0,
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct Session {
    tenant: TenantId,
    stream_hash: StreamHash,
    session_generation: SessionGeneration,
    protocol: u8,
    conn_id: u16,
    /// 1 once the durable session record exists on this node, set by
    /// every node when `QOP_CONNECT` applies. `active` is narrower: it
    /// means a client socket is attached HERE, which a follower never
    /// has. Keeping them apart is what lets a follower hold the same
    /// durable record as the leader and still be found by
    /// `find_by_stream`.
    present: u8,
    active: u8,
    /// 1 if this slot still owns subscription / inflight / prefetch state
    /// after a disconnect (clean_start=false). Distinct from `active` so
    /// reconnect lookup can find it.
    persisted: u8,
    /// 1 if the most recent CONNECT carried clean_start=true.
    clean_start: u8,
    /// 1 between CONNECT receipt and the matching `QOP_CONNECT` commit.
    /// Propose-side admission may need to route follow-up packets from
    /// the same `conn_id` before the durable session record exists; the
    /// transient flag lets `find_session_by_conn` find the slot during
    /// that window. Cleared on apply when the slot becomes durable. See
    /// docs/architecture/apply_path.md §Optimistic CONNACK.
    transient: u8,
    last_activity_ms: u64,
    keep_alive_ms: u32,
    next_msg_id: u32,
    inflight: [Inflight; MAX_INFLIGHT_PER_SESSION],
    /// MQTT Will-message state (3.1.1 §3.1.2.5, MQTT 5 §3.1.2.5).
    /// `will_present == 1` when the most recent CONNECT carried a Will
    /// section; `apply_qop_disconnect` fires this publish when the
    /// disconnect reason is anything other than CLEAN.
    will_present: u8,
    will_qos: u8,
    will_retain: u8,
    will_delay_ms: u32,
    will_topic_len: u16,
    will_topic: [u8; MAX_WILL_TOPIC],
    will_payload_len: u16,
    will_payload: [u8; MAX_WILL_PAYLOAD],
    /// MQTT 5 §3.1.2.11.2 SessionExpiryInterval (seconds). MQTT 3.1.1
    /// has no equivalent property; propose-side normalises
    /// clean_session=0 connections to `u32::MAX` so they're treated as
    /// "never expire" by the sweep. Special values:
    ///   * `0` — expire immediately on disconnect (no persisted slot
    ///     survives the disconnect, even with clean_start=false).
    ///   * `u32::MAX`    — never expire (sweep skips the slot).
    ///   * anything else — purge at `disconnected_at_ms + session_expiry_s * 1000`.
    session_expiry_s: u32,
    /// Wall-clock millis when the slot last transitioned to
    /// `persisted == 1`. Used by the expiry sweep; meaningful only
    /// while `persisted == 1`.
    disconnected_at_ms: u64,
    /// MQTT 5 §3.1.2.11.3 ReceiveMaximum — the client's cap on
    /// concurrent unacked QoS 1+2 publishes the broker may have in
    /// flight to it. `0` means "absent" / no cap (the spec default
    /// is 65535 but we treat both as unlimited for the per-slot
    /// gate). MQTT 3.1.1 connections never set this and stay at 0.
    /// The effective limit in `try_deliver` is
    /// `min(prefetch_credit, receive_maximum)` when both are > 0.
    receive_maximum: u16,
    /// 1 while `conn_id` names a live connection.
    ///
    /// `conn_id` alone cannot express "unbound": it is a `u16` and 0 is a
    /// VALID connection id, so a slot cleared to 0 is indistinguishable
    /// from one legitimately bound to connection 0 — and `find_by_conn`
    /// would hand a new client on that recycled id the previous
    /// client's session. A separate flag removes the ambiguity outright
    /// rather than reserving a sentinel that a large enough connection
    /// table would eventually collide with.
    conn_bound: u8,
    /// Wall-clock millis when a deferred Will-message publish becomes
    /// due. Set on `apply_qop_disconnect` with `reason != CLEAN` when
    /// the stored Will has a non-zero `will_delay_ms`. Cleared by
    /// either:
    ///   * the metrics-tick sweep firing the Will, or
    ///   * a fresh CONNECT on the same `(tenant, stream_hash)` that
    ///     resurrects the session (MQTT 5 §3.1.3.2.2 — "if the Session
    ///     is taken over by another connection, the Will Message
    ///     publication MUST NOT be sent").
    ///
    /// `0` means no pending Will fire.
    pending_will_fire_at_ms: u64,
}

impl Session {
    pub const fn zero() -> Self {
        Self {
            tenant: 0,
            stream_hash: 0,
            session_generation: 0,
            protocol: PROTO_UNKNOWN,
            conn_id: 0,
            present: 0,
            active: 0,
            persisted: 0,
            clean_start: 0,
            transient: 0,
            last_activity_ms: 0,
            keep_alive_ms: 0,
            next_msg_id: 1,
            inflight: [Inflight::zero(); MAX_INFLIGHT_PER_SESSION],
            will_present: 0,
            will_qos: 0,
            will_retain: 0,
            will_delay_ms: 0,
            will_topic_len: 0,
            will_topic: [0; MAX_WILL_TOPIC],
            will_payload_len: 0,
            will_payload: [0; MAX_WILL_PAYLOAD],
            session_expiry_s: 0,
            disconnected_at_ms: 0,
            receive_maximum: 0,
            conn_bound: 0,
            pending_will_fire_at_ms: 0,
        }
    }

    pub fn allocate_inflight(&mut self, packet_id: u16, qos: u8, direction: u8) -> Option<usize> {
        for i in 0..MAX_INFLIGHT_PER_SESSION {
            if self.inflight[i].active == 0 {
                self.inflight[i] = Inflight {
                    packet_id,
                    qos,
                    phase: if qos == 2 { QOS2_PUBLISH } else { 0 },
                    session_generation: 0,
                    wal_index: 0,
                    correlation_id: 0,
                    partition_id: 0,
                    direction,
                    rec_sent: 0,
                    active: 1,
                };
                return Some(i);
            }
        }
        None
    }

    pub fn find_inflight_dir(&self, packet_id: u16, direction: u8) -> Option<usize> {
        for i in 0..MAX_INFLIGHT_PER_SESSION {
            let e = &self.inflight[i];
            if e.active == 1 && e.packet_id == packet_id && e.direction == direction {
                return Some(i);
            }
        }
        None
    }
}

/// Bucket counts for the two chained hash indexes: powers of two at
/// twice the slot count, so a full table averages half a slot per
/// bucket and the bucket mask is a single AND.
const STREAM_BUCKETS: usize = MAX_SESSIONS * 2;
const CONN_BUCKETS: usize = MAX_SESSIONS * 2;
const _: () = assert!(STREAM_BUCKETS.is_power_of_two() && CONN_BUCKETS.is_power_of_two());
const _: () = assert!(MAX_SESSIONS < u16::MAX as usize);

/// Chain link encoding: `slot + 1`, with `NIL` (0) as the end marker,
/// so a zero-initialised head array reads as "every bucket empty".
const NIL: u16 = 0;

/// Component state. Owned exclusively by this subtree.
#[repr(C)]
pub struct Sessions {
    slots: [Session; MAX_SESSIONS],
    /// Per-session subscriber flow control: the credit window the
    /// prefetch controller grants, and how many deliveries are
    /// outstanding against it. Kept beside the session table because
    /// both are cleared on exactly the same transitions.
    prefetch_credit: [u32; MAX_SESSIONS],
    sub_outstanding: [u32; MAX_SESSIONS],

    // ── Stream index: (tenant, stream_hash) → slot ──
    //
    // Membership: `present == 1 || transient == 1`. The chain holds
    // every slot `find_by_stream` may answer with; the lookup still
    // applies the full predicate, so a bucket collision costs a compare
    // and never a wrong answer.
    stream_head: [u16; STREAM_BUCKETS],
    stream_next: [u16; MAX_SESSIONS],
    /// The key each slot is linked under, so `reindex` can tell a key
    /// change from a no-op and unlink from the bucket the slot is
    /// actually in rather than the one its current key names.
    idx_stream_tenant: [TenantId; MAX_SESSIONS],
    idx_stream_hash: [StreamHash; MAX_SESSIONS],
    idx_in_stream: [u8; MAX_SESSIONS],

    // ── Connection index: conn_id → slot ──
    //
    // Membership: `conn_bound == 1`. `find_by_conn` narrows further to
    // active-or-transient; a parked (persisted) slot keeps its binding
    // and stays in the chain until `unbind_conn` drops it.
    conn_head: [u16; CONN_BUCKETS],
    conn_next: [u16; MAX_SESSIONS],
    idx_conn: [u16; MAX_SESSIONS],
    idx_in_conn: [u8; MAX_SESSIONS],

    // ── Free stack ──
    //
    // A slot is free when `present == 0 && transient == 0`. `in_free`
    // says whether the slot has an entry on the stack; the entry is
    // left in place when the slot is occupied and discarded as stale
    // when popped, so a slot never holds more than one entry and the
    // stack never exceeds `MAX_SESSIONS`.
    free_stack: [u16; MAX_SESSIONS],
    free_len: u32,
    in_free: [u8; MAX_SESSIONS],

    // ── Worker slot ownership ──
    //
    // Two session workers sharing one node (an anchor-preserved
    // handoff pair, docs/architecture/session_continuity.md) share the
    // slot NUMBER SPACE — topic_engine, flow and messaging key by slot
    // — so each allocates new sessions only from its own range. A slot
    // index is therefore globally unique across the pair, and a
    // session keeps its index when it moves: the importer places it
    // at the same index, which is free there because the ranges are
    // disjoint.
    //
    // `lent` marks a slot in THIS worker's range whose session was
    // handed to the other worker. It stays off the free stack until the
    // session comes back (`unlend` on import), so the index is never
    // handed to a second client while the first still lives elsewhere.
    alloc_lo: u16,
    /// Exclusive upper bound; 0 means `MAX_SESSIONS`.
    alloc_hi: u16,
    lent: [u8; MAX_SESSIONS],
}

/// Restrict new-session allocation to `[lo, hi)` (`hi == 0` means the
/// whole table). Must be set before the table is built, since the free
/// stack is populated as slots are cleared.
pub fn set_alloc_range(s: &mut Sessions, lo: u16, hi: u16) {
    s.alloc_lo = lo;
    s.alloc_hi = hi;
}

#[inline]
fn alloc_hi(s: &Sessions) -> usize {
    if s.alloc_hi == 0 {
        MAX_SESSIONS
    } else {
        usize::from(s.alloc_hi).min(MAX_SESSIONS)
    }
}

/// True when `si` is one this worker allocates from.
#[inline]
pub fn in_alloc_range(s: &Sessions, si: usize) -> bool {
    si >= usize::from(s.alloc_lo) && si < alloc_hi(s)
}

/// True when the slot's session lives on another worker.
#[inline]
pub fn is_lent(s: &Sessions, si: usize) -> bool {
    si < MAX_SESSIONS && s.lent[si] == 1
}

/// True when this worker holds a session at `si` in any state.
#[inline]
pub fn holds(s: &Sessions, si: usize) -> bool {
    si < MAX_SESSIONS && {
        let x = &s.slots[si];
        x.present == 1 || x.transient == 1 || x.active == 1 || x.persisted == 1
    }
}

/// Hand the session at `si` to another worker: the record goes, the
/// index stays reserved. The caller has already exported it.
pub fn lend(s: &mut Sessions, si: usize) {
    if si >= MAX_SESSIONS {
        return;
    }
    s.lent[si] = 1;
    s.slots[si] = Session::zero();
    s.prefetch_credit[si] = 0;
    s.sub_outstanding[si] = 0;
    reindex(s, si);
}

/// The session at `si` is (back) on this worker; `import_record` fills
/// the slot next.
pub fn unlend(s: &mut Sessions, si: usize) {
    if si < MAX_SESSIONS {
        s.lent[si] = 0;
    }
}

pub fn init(s: &mut Sessions) {
    for x in s.slots.iter_mut() {
        *x = Session::zero();
    }
    for i in 0..MAX_SESSIONS {
        s.prefetch_credit[i] = 0;
        s.sub_outstanding[i] = 0;
    }
    rebuild_index(s);
}

// ── Index maintenance ───────────────────────────────────────────────

#[inline]
fn stream_bucket(tenant: TenantId, stream_hash: StreamHash) -> usize {
    // `stream_hash` is FNV-1a output, so its low bits are already well
    // mixed; folding the high half in and stirring the tenant keeps two
    // tenants sharing a client id apart.
    let mut k = stream_hash ^ (u64::from(tenant)).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    k ^= k >> 32;
    k ^= k >> 17;
    (k as usize) & (STREAM_BUCKETS - 1)
}

#[inline]
fn conn_bucket(conn_id: u16) -> usize {
    usize::from(conn_id) & (CONN_BUCKETS - 1)
}

/// Unlink `si` from the chain rooted at `head[bucket]`. A chain is
/// singly linked, so the predecessor is found by walking from the head;
/// chains average under one entry, so this is O(1) expected.
fn chain_unlink(head: &mut [u16], next: &mut [u16; MAX_SESSIONS], bucket: usize, si: usize) {
    let target = (si + 1) as u16;
    let mut cur = head[bucket];
    if cur == target {
        head[bucket] = next[si];
        next[si] = NIL;
        return;
    }
    while cur != NIL {
        let ci = usize::from(cur - 1);
        if next[ci] == target {
            next[ci] = next[si];
            next[si] = NIL;
            return;
        }
        cur = next[ci];
    }
}

#[inline]
fn chain_link(head: &mut [u16], next: &mut [u16; MAX_SESSIONS], bucket: usize, si: usize) {
    next[si] = head[bucket];
    head[bucket] = (si + 1) as u16;
}

/// Reconcile slot `si`'s index memberships with its current fields.
///
/// The one maintenance point: every function that writes `tenant`,
/// `stream_hash`, `present`, `transient`, `conn_id`, `conn_bound` or
/// `active` ends by calling this. It compares the slot's keys and flags
/// against the copy recorded at its most recent link, unlinks
/// from a chain whose key or membership no longer holds, links into
/// the chain the current fields name, and pushes the slot onto the
/// free stack when it has become free and holds no stack entry.
fn reindex(s: &mut Sessions, si: usize) {
    if si >= MAX_SESSIONS {
        return;
    }
    let x = &s.slots[si];
    let tenant = x.tenant;
    let stream_hash = x.stream_hash;
    let conn_id = x.conn_id;
    let want_stream = x.present == 1 || x.transient == 1;
    let want_conn = x.conn_bound == 1;

    if s.idx_in_stream[si] == 1
        && (!want_stream
            || s.idx_stream_tenant[si] != tenant
            || s.idx_stream_hash[si] != stream_hash)
    {
        let b = stream_bucket(s.idx_stream_tenant[si], s.idx_stream_hash[si]);
        chain_unlink(&mut s.stream_head, &mut s.stream_next, b, si);
        s.idx_in_stream[si] = 0;
    }
    if want_stream && s.idx_in_stream[si] == 0 {
        let b = stream_bucket(tenant, stream_hash);
        chain_link(&mut s.stream_head, &mut s.stream_next, b, si);
        s.idx_stream_tenant[si] = tenant;
        s.idx_stream_hash[si] = stream_hash;
        s.idx_in_stream[si] = 1;
    }

    if s.idx_in_conn[si] == 1 && (!want_conn || s.idx_conn[si] != conn_id) {
        let b = conn_bucket(s.idx_conn[si]);
        chain_unlink(&mut s.conn_head, &mut s.conn_next, b, si);
        s.idx_in_conn[si] = 0;
    }
    if want_conn && s.idx_in_conn[si] == 0 {
        let b = conn_bucket(conn_id);
        chain_link(&mut s.conn_head, &mut s.conn_next, b, si);
        s.idx_conn[si] = conn_id;
        s.idx_in_conn[si] = 1;
    }

    if !want_stream
        && s.in_free[si] == 0
        && s.lent[si] == 0
        && in_alloc_range(s, si)
        && (s.free_len as usize) < MAX_SESSIONS
    {
        s.free_stack[s.free_len as usize] = si as u16;
        s.free_len += 1;
        s.in_free[si] = 1;
    }
}

/// Rebuild both indexes and the free stack from the slot array. Slots
/// are pushed highest-first so `allocate` hands out the lowest index
/// first on a fresh table.
fn rebuild_index(s: &mut Sessions) {
    s.stream_head = [NIL; STREAM_BUCKETS];
    s.conn_head = [NIL; CONN_BUCKETS];
    s.free_len = 0;
    for i in 0..MAX_SESSIONS {
        s.stream_next[i] = NIL;
        s.conn_next[i] = NIL;
        s.idx_in_stream[i] = 0;
        s.idx_in_conn[i] = 0;
        s.in_free[i] = 0;
    }
    for i in (0..MAX_SESSIONS).rev() {
        reindex(s, i);
    }
}

// ── Subscriber flow control ─────────────────────────────────────────

pub fn prefetch_credit(s: &Sessions, si: usize) -> u32 {
    if si < MAX_SESSIONS {
        s.prefetch_credit[si]
    } else {
        0
    }
}

pub fn set_prefetch_credit(s: &mut Sessions, si: usize, credit: u32) {
    if si < MAX_SESSIONS {
        s.prefetch_credit[si] = credit;
    }
}

pub fn sub_outstanding(s: &Sessions, si: usize) -> u32 {
    if si < MAX_SESSIONS {
        s.sub_outstanding[si]
    } else {
        0
    }
}

pub fn note_delivery(s: &mut Sessions, si: usize) -> u32 {
    if si >= MAX_SESSIONS {
        return 0;
    }
    s.sub_outstanding[si] = s.sub_outstanding[si].wrapping_add(1);
    s.sub_outstanding[si]
}

/// Release one outstanding delivery as its ack lands, saturating at 0.
pub fn note_ack(s: &mut Sessions, si: usize) {
    if si < MAX_SESSIONS {
        s.sub_outstanding[si] = s.sub_outstanding[si].saturating_sub(1);
    }
}

pub fn set_sub_outstanding(s: &mut Sessions, si: usize, n: u32) {
    if si < MAX_SESSIONS {
        s.sub_outstanding[si] = n;
    }
}

/// Clear the flow-control window — done on every transition that also
/// resets delivery state.
pub fn clear_flow(s: &mut Sessions, si: usize) {
    if si < MAX_SESSIONS {
        s.prefetch_credit[si] = 0;
        s.sub_outstanding[si] = 0;
    }
}

/// Unbind `conn_id` from every session slot EXCEPT `keep`. Returns how
/// many stale bindings were evicted.
///
/// INVARIANT: at most one session may be bound to a given `conn_id`.
/// `conn_id` is a transport-layer slot index and the transport RECYCLES
/// it, so without this a client that closed abruptly — no DISCONNECT,
/// which is the common case — leaves its session still claiming the id.
/// `find_by_conn` returns whichever match heads the chain, so the NEXT
/// client to land on that recycled id resolves to the previous client's
/// session: its SUBSCRIBE is anchored there, and its publishes are
/// attributed there. That is cross-client state corruption, not merely
/// a stale lookup.
///
/// Unbinding does not destroy the session. A persisted one stays
/// resurrectable by `(tenant, stream_hash)`, which is the identity a
/// reconnect is supposed to use.
pub fn unbind_other_conns(s: &mut Sessions, keep: usize, conn_id: u16) -> u32 {
    let mut n = 0u32;
    // Each unbind unlinks the slot from the chain being walked, so the
    // walk restarts from the head after every eviction. Bounded: every
    // pass removes one member or ends.
    loop {
        let mut cur = s.conn_head[conn_bucket(conn_id)];
        let mut victim = None;
        while cur != NIL {
            let i = usize::from(cur - 1);
            let x = &s.slots[i];
            if i != keep
                && x.conn_bound == 1
                && x.conn_id == conn_id
                && (x.active == 1 || x.transient == 1)
            {
                victim = Some(i);
                break;
            }
            cur = s.conn_next[i];
        }
        let Some(i) = victim else {
            return n;
        };
        unbind_conn(s, i);
        n += 1;
    }
}

/// Diagnostic: the state bits of the first slot in `conn_id`'s chain
/// carrying it — `1` active, `2` transient, `4` present, `8`
/// conn_bound, `16` persisted — or `32` when no slot carries it. Says
/// why `find_by_conn` came back empty.
///
/// Reads the connection index, so it sees the slots `find_by_conn`
/// could have matched: those with `conn_bound == 1`. A slot whose
/// binding `unbind_conn` dropped has `conn_id == 0` and is
/// out of every chain, so nothing is hidden by the index.
pub fn conn_flags(s: &Sessions, conn_id: u16) -> u32 {
    let mut cur = s.conn_head[conn_bucket(conn_id)];
    while cur != NIL {
        let x = &s.slots[usize::from(cur - 1)];
        if x.conn_id == conn_id && (x.conn_bound == 1 || x.active == 1 || x.transient == 1) {
            return u32::from(x.active)
                | u32::from(x.transient) << 1
                | u32::from(x.present) << 2
                | u32::from(x.conn_bound) << 3
                | u32::from(x.persisted) << 4;
        }
        cur = s.conn_next[usize::from(cur - 1)];
    }
    32
}

/// Find the session bound to `conn_id`.
///
/// Matches `active` OR `transient` slots: between CONNECT receipt and
/// the QOP_CONNECT commit the slot is transient, and follow-up packets
/// from the same connection must still route to it for admission
/// control.
pub fn find_by_conn(s: &Sessions, conn_id: u16) -> Option<usize> {
    let mut cur = s.conn_head[conn_bucket(conn_id)];
    while cur != NIL {
        let i = usize::from(cur - 1);
        let x = &s.slots[i];
        if (x.active == 1 || x.transient == 1) && x.conn_bound == 1 && x.conn_id == conn_id {
            return Some(i);
        }
        cur = s.conn_next[i];
    }
    None
}

/// Find any session matching `(tenant, stream_hash)`, including
/// `persisted` (disconnected, clean_start=false) and `transient`
/// (CONNECT received, apply pending) slots. Reconnect uses this to
/// resurrect the prior session per MQTT 3.1.1 §3.1.2.4, and apply-side
/// QOP_CONNECT to reconcile a transient record with its durable
/// counterpart.
pub fn find_by_stream(s: &Sessions, tenant: TenantId, stream_hash: StreamHash) -> Option<usize> {
    let mut cur = s.stream_head[stream_bucket(tenant, stream_hash)];
    while cur != NIL {
        let i = usize::from(cur - 1);
        let x = &s.slots[i];
        if (x.present == 1 || x.transient == 1)
            && x.tenant == tenant
            && x.stream_hash == stream_hash
        {
            return Some(i);
        }
        cur = s.stream_next[i];
    }
    None
}

/// Allocate a free slot — skips active, persisted and transient
/// sessions. Pops the free stack, discarding entries whose slot has
/// been occupied since their push; None when the table is full.
///
/// The popped slot leaves the stack without being marked occupied, so
/// the caller is expected to occupy it (`open_transient` or
/// `commit_connect`) before the next `allocate`; both call sites do.
pub fn allocate(s: &mut Sessions) -> Option<usize> {
    while s.free_len > 0 {
        s.free_len -= 1;
        let i = usize::from(s.free_stack[s.free_len as usize]);
        if i >= MAX_SESSIONS {
            continue;
        }
        s.in_free[i] = 0;
        let x = &s.slots[i];
        if x.present == 0 && x.transient == 0 {
            return Some(i);
        }
    }
    None
}

pub fn is_active(s: &Sessions, si: usize) -> bool {
    si < MAX_SESSIONS && s.slots[si].active == 1
}

// ── Identity and state ──────────────────────────────────────────────

/// The hot read path: everything a handler needs about a session
/// without touching its record.
#[derive(Clone, Copy)]
pub struct SessionView {
    pub tenant: TenantId,
    pub stream_hash: StreamHash,
    pub session_generation: SessionGeneration,
    pub protocol: u8,
    pub conn_id: u16,
    pub active: bool,
    pub persisted: bool,
    pub transient: bool,
    pub clean_start: bool,
    pub keep_alive_ms: u32,
    pub session_expiry_s: u32,
    pub receive_maximum: u16,
    pub last_activity_ms: u64,
    pub disconnected_at_ms: u64,
}

pub fn view(s: &Sessions, si: usize) -> Option<SessionView> {
    if si >= MAX_SESSIONS {
        return None;
    }
    let x = &s.slots[si];
    if x.active == 0 && x.persisted == 0 && x.transient == 0 {
        return None;
    }
    Some(SessionView {
        tenant: x.tenant,
        stream_hash: x.stream_hash,
        session_generation: x.session_generation,
        protocol: x.protocol,
        conn_id: x.conn_id,
        active: x.active == 1,
        persisted: x.persisted == 1,
        transient: x.transient == 1,
        clean_start: x.clean_start == 1,
        keep_alive_ms: x.keep_alive_ms,
        session_expiry_s: x.session_expiry_s,
        receive_maximum: x.receive_maximum,
        last_activity_ms: x.last_activity_ms,
        disconnected_at_ms: x.disconnected_at_ms,
    })
}

pub fn is_persisted(s: &Sessions, si: usize) -> bool {
    si < MAX_SESSIONS && s.slots[si].persisted == 1
}

pub fn is_transient(s: &Sessions, si: usize) -> bool {
    si < MAX_SESSIONS && s.slots[si].transient == 1
}

pub fn conn_id(s: &Sessions, si: usize) -> u16 {
    if si < MAX_SESSIONS {
        s.slots[si].conn_id
    } else {
        0
    }
}

pub fn stream_hash(s: &Sessions, si: usize) -> StreamHash {
    if si < MAX_SESSIONS {
        s.slots[si].stream_hash
    } else {
        0
    }
}

pub fn tenant(s: &Sessions, si: usize) -> TenantId {
    if si < MAX_SESSIONS {
        s.slots[si].tenant
    } else {
        0
    }
}

pub fn generation(s: &Sessions, si: usize) -> SessionGeneration {
    if si < MAX_SESSIONS {
        s.slots[si].session_generation
    } else {
        0
    }
}

pub fn protocol(s: &Sessions, si: usize) -> u8 {
    if si < MAX_SESSIONS {
        s.slots[si].protocol
    } else {
        PROTO_UNKNOWN
    }
}

/// Record client activity, which defers the keep-alive deadline.
pub fn touch(s: &mut Sessions, si: usize, now: u64) {
    if si < MAX_SESSIONS {
        s.slots[si].last_activity_ms = now;
    }
}

/// True when the client has missed its keep-alive window. MQTT 3.1.1
/// §3.1.2.10 gives a 1.5× grace on the negotiated interval.
pub fn keepalive_expired(s: &Sessions, si: usize, now: u64) -> bool {
    if si >= MAX_SESSIONS || s.slots[si].active != 1 {
        return false;
    }
    let ka = s.slots[si].keep_alive_ms;
    ka != 0 && now.wrapping_sub(s.slots[si].last_activity_ms) > (ka as u64) * 3 / 2
}

/// True when a disconnected-but-persisted session has outlived its
/// expiry interval and its slot may be reclaimed.
pub fn session_expired(s: &Sessions, si: usize, now: u64) -> bool {
    if si >= MAX_SESSIONS || s.slots[si].persisted != 1 {
        return false;
    }
    let secs = s.slots[si].session_expiry_s;
    secs != 0
        && s.slots[si].disconnected_at_ms != 0
        && now.wrapping_sub(s.slots[si].disconnected_at_ms) > (secs as u64) * 1000
}

/// Take the next subscriber-delivery packet id. MQTT reserves id 0, so
/// a wrap lands on 1.
pub fn next_sub_packet_id(s: &mut Sessions, si: usize) -> u16 {
    if si >= MAX_SESSIONS {
        return 1;
    }
    let id_full = s.slots[si].next_msg_id;
    s.slots[si].next_msg_id = id_full.wrapping_add(1);
    if (id_full as u16) == 0 {
        1
    } else {
        id_full as u16
    }
}

/// Take the next packet id for a broker-originated publish, wrapping
/// through the 1..=65535 range MQTT reserves (id 0 is illegal).
pub fn next_packet_id(s: &mut Sessions, si: usize) -> u16 {
    if si >= MAX_SESSIONS {
        return 1;
    }
    let x = &mut s.slots[si];
    x.next_msg_id = x.next_msg_id.wrapping_add(1);
    if x.next_msg_id == 0 || x.next_msg_id > 0xFFFF {
        x.next_msg_id = 1;
    }
    x.next_msg_id as u16
}

// ── Lifecycle ───────────────────────────────────────────────────────

/// Wipe the per-session delivery state a fresh (non-resumed) session
/// must not inherit: QoS inflight and the packet-id counter.
pub fn reset_delivery_state(s: &mut Sessions, si: usize) {
    if si >= MAX_SESSIONS {
        return;
    }
    s.slots[si].inflight = [Inflight::zero(); MAX_INFLIGHT_PER_SESSION];
    s.slots[si].next_msg_id = 1;
}

/// Bind a freshly-admitted connection to an existing slot on the
/// propose side. `mark_transient` then flags it pending apply.
pub fn rebind(
    s: &mut Sessions,
    si: usize,
    conn_id: u16,
    protocol: u8,
    clean_start: bool,
    keep_alive_ms: u32,
) {
    if si >= MAX_SESSIONS {
        return;
    }
    let x = &mut s.slots[si];
    x.conn_id = conn_id;
    x.conn_bound = 1;
    x.protocol = protocol;
    x.clean_start = u8::from(clean_start);
    x.keep_alive_ms = keep_alive_ms;
    reindex(s, si);
}

/// Occupy a free slot for a CONNECT that has no prior session. `active`
/// flips at QOP_CONNECT apply; until then the slot is transient.
pub fn open_transient(s: &mut Sessions, si: usize, p: ConnectParams, conn_id: u16, now: u64) {
    if si >= MAX_SESSIONS {
        return;
    }
    s.slots[si] = Session::zero();
    let x = &mut s.slots[si];
    x.tenant = p.tenant;
    x.stream_hash = p.stream_hash;
    x.protocol = p.protocol;
    x.conn_id = conn_id;
    x.conn_bound = 1;
    x.transient = 1;
    x.clean_start = u8::from(p.clean_start);
    x.keep_alive_ms = p.keep_alive_ms;
    x.last_activity_ms = now;
    x.next_msg_id = 1;
    reindex(s, si);
}

/// Identity and negotiated parameters carried by CONNECT.
#[derive(Clone, Copy)]
pub struct ConnectParams {
    pub tenant: TenantId,
    pub stream_hash: StreamHash,
    pub protocol: u8,
    pub clean_start: bool,
    pub keep_alive_ms: u32,
}

/// Apply a committed CONNECT. Bumps the session generation — the
/// fencing token every in-flight round-trip is validated against —
/// and clears `persisted`, since the session is no longer parked.
pub fn commit_connect(s: &mut Sessions, si: usize, p: ConnectParams) {
    if si >= MAX_SESSIONS {
        return;
    }
    let x = &mut s.slots[si];
    x.tenant = p.tenant;
    x.stream_hash = p.stream_hash;
    x.session_generation = x.session_generation.wrapping_add(1);
    x.protocol = p.protocol;
    x.clean_start = u8::from(p.clean_start);
    x.keep_alive_ms = p.keep_alive_ms;
    x.present = 1;
    x.persisted = 0;
    reindex(s, si);
}

/// Leader path: the propose-side admitted this session, apply now makes
/// it durable.
pub fn mark_active(s: &mut Sessions, si: usize) {
    if si < MAX_SESSIONS {
        s.slots[si].active = 1;
        s.slots[si].transient = 0;
        reindex(s, si);
    }
}

/// Follower / replay path under `!clean_start`: park the durable record
/// so a later reconnect on this node can resurrect it.
pub fn mark_persisted(s: &mut Sessions, si: usize) {
    if si < MAX_SESSIONS {
        s.slots[si].persisted = 1;
        reindex(s, si);
    }
}

pub fn set_session_expiry(s: &mut Sessions, si: usize, secs: u32) {
    if si < MAX_SESSIONS {
        s.slots[si].session_expiry_s = secs;
    }
}

pub fn session_expiry_s(s: &Sessions, si: usize) -> u32 {
    if si < MAX_SESSIONS {
        s.slots[si].session_expiry_s
    } else {
        0
    }
}

pub fn set_receive_maximum(s: &mut Sessions, si: usize, n: u16) {
    if si < MAX_SESSIONS {
        s.slots[si].receive_maximum = n;
    }
}

pub fn set_conn(s: &mut Sessions, si: usize, conn_id: u16, protocol: u8) {
    if si < MAX_SESSIONS {
        s.slots[si].conn_id = conn_id;
        s.slots[si].conn_bound = 1;
        s.slots[si].protocol = protocol;
        reindex(s, si);
    }
}

pub fn mark_transient(s: &mut Sessions, si: usize) {
    if si < MAX_SESSIONS {
        s.slots[si].transient = 1;
        reindex(s, si);
    }
}

/// Take the session out of service. `persisted` decides whether the
/// slot is parked for a matching reconnect (stamping the disconnect
/// time so the expiry sweep can purge it) or dropped outright.
pub fn close(s: &mut Sessions, si: usize, persisted: bool, now: u64) {
    if si >= MAX_SESSIONS {
        return;
    }
    let x = &mut s.slots[si];
    x.active = 0;
    x.transient = 0;
    if persisted {
        x.persisted = 1;
        x.disconnected_at_ms = now;
    } else {
        // Clean disconnect: the durable record ends here, so the slot
        // stops being findable and returns to the free pool.
        x.persisted = 0;
        x.present = 0;
    }
    reindex(s, si);
}

/// Unbind the connection without ending the session — the slot keeps
/// its identity for a reconnect.
pub fn unbind_conn(s: &mut Sessions, si: usize) {
    if si < MAX_SESSIONS {
        s.slots[si].conn_id = 0;
        s.slots[si].conn_bound = 0;
        reindex(s, si);
    }
}

pub fn receive_maximum(s: &Sessions, si: usize) -> u16 {
    if si < MAX_SESSIONS {
        s.slots[si].receive_maximum
    } else {
        0
    }
}

pub fn clean_start(s: &Sessions, si: usize) -> bool {
    si < MAX_SESSIONS && s.slots[si].clean_start != 0
}

/// Reclaim an expired persisted slot: identity and delivery state go,
/// the slot returns to the free pool.
pub fn reclaim(s: &mut Sessions, si: usize) {
    if si < MAX_SESSIONS {
        s.slots[si] = Session::zero();
        reindex(s, si);
    }
}

/// Free the slot entirely.
pub fn clear(s: &mut Sessions, si: usize) {
    if si < MAX_SESSIONS {
        s.slots[si] = Session::zero();
        reindex(s, si);
    }
}

/// Release every session whose shard is no longer this node's, and
/// report how many went. `is_local` decides: it is supplied by the
/// caller so this module keeps no placement view of its own and there
/// is still exactly one shard->owner derivation in the tree.
///
/// Called ONLY when placement actually reassigns a shard — never on a
/// fence. A fenced shard is still ours: the migration can abort, and an
/// abort leaves ownership exactly where it was, so dropping the state
/// would destroy sessions the transfer was supposed to leave untouched.
///
/// Releasing matters because the alternative is worse than a leak: a
/// node that keeps a session for a shard it no longer owns will keep
/// answering `find_by_stream` for it, so a client reconnecting here is
/// handed stale state while the new owner is already diverging from it.
///
/// Empty slots are skipped rather than cleared: `present == 0` already
/// means "no session", and `Session::zero()` on an untouched slot would
/// be a write for no reason across the whole table.
/// `on_released` is invoked with each freed slot BEFORE it is zeroed,
/// so the caller can tear down state held for that slot elsewhere — the
/// offline queue in `messaging` above all, which is keyed by slot index
/// and would otherwise be drained to whichever session is allocated the
/// slot next.
///
/// It also receives the slot's live connection, if any, as
/// `Some(conn_id)`. Releasing the SESSION is not enough: the client's
/// socket stays open, and a client left connected to a node that no
/// longer owns it receives nothing and has no reason to reconnect — so
/// it never reaches the CONNECT redirect that would send it to the new
/// owner. The caller needs the id to tell it to go.
pub fn release_foreign(
    s: &mut Sessions,
    is_local: impl Fn(u32) -> bool,
    mut on_released: impl FnMut(usize, Option<u16>),
) -> u32 {
    let mut released = 0u32;
    for si in 0..MAX_SESSIONS {
        if s.slots[si].present == 0 && s.slots[si].active == 0 && s.slots[si].transient == 0 {
            continue;
        }
        let shard = super::wire::shard_mqtt_stream(s.slots[si].tenant, s.slots[si].stream_hash);
        if is_local(shard) {
            continue;
        }
        let bound = if s.slots[si].conn_bound == 1 {
            Some(s.slots[si].conn_id)
        } else {
            None
        };
        on_released(si, bound);
        s.slots[si] = Session::zero();
        s.prefetch_credit[si] = 0;
        s.sub_outstanding[si] = 0;
        reindex(s, si);
        released = released.wrapping_add(1);
    }
    released
}

// ── Handoff record ──────────────────────────────────────────────────
//
// The per-session state a worker move carries. Everything here is what
// the committed log does NOT determine for the importing worker — or
// what it determines but the importer has not applied, which the import
// reconciles against (`session_identity_core::reconcile_generation`).
// Subscriptions are deliberately absent: they live in `topic_engine`,
// keyed by the slot index the session keeps across the move.
//
// Layout (all LE):
//   [magic "QSR1":4][tenant:4][stream_hash:8][generation:4][slot:4]
//   [protocol:1][clean_start:1][persisted:1][keep_alive_ms:4]
//   [next_msg_id:4][session_expiry_s:4][receive_maximum:2]
//   [last_activity_age_ms:4]  — `now - last_activity`, rebased on import
//   [prefetch_credit:4][sub_outstanding:4]
//   [will_present:1][will_qos:1][will_retain:1][will_delay_ms:4]
//   [will_topic_len:2][topic][will_payload_len:2][payload]
//   [inflight_count:1] then per entry
//     [packet_id:2][qos:1][phase:1][direction:1][generation:4]
//     [wal_index:8][correlation_id:8][partition_id:2][rec_sent:1]
//
// `RECORD_MAX` bounds the blob; the worker's import buffer is sized to
// it, so an oversize record is refused at EXPORT_BEGIN rather than
// truncated.

const RECORD_MAGIC: [u8; 4] = *b"QSR1";
const RECORD_FIXED: usize =
    4 + 4 + 8 + 4 + 4 + 1 + 1 + 1 + 4 + 4 + 4 + 2 + 4 + 4 + 4 + 1 + 1 + 1 + 4 + 2 + 2 + 1;
const INFLIGHT_RECORD: usize = 2 + 1 + 1 + 1 + 4 + 8 + 8 + 2 + 1;
/// Largest handoff record a session can export.
pub const RECORD_MAX: usize =
    RECORD_FIXED + MAX_WILL_TOPIC + MAX_WILL_PAYLOAD + MAX_INFLIGHT_PER_SESSION * INFLIGHT_RECORD;

/// Serialise the session at `si` into `out`. Returns the byte count, or
/// 0 if the slot holds nothing or `out` is too short.
pub fn export_record(s: &Sessions, si: usize, now: u64, out: &mut [u8]) -> usize {
    if !holds(s, si) || out.len() < RECORD_MAX {
        return 0;
    }
    let x = &s.slots[si];
    let mut p = 0usize;
    let mut put = |b: &[u8], p: &mut usize| {
        out[*p..*p + b.len()].copy_from_slice(b);
        *p += b.len();
    };
    put(&RECORD_MAGIC, &mut p);
    put(&x.tenant.to_le_bytes(), &mut p);
    put(&x.stream_hash.to_le_bytes(), &mut p);
    put(&x.session_generation.to_le_bytes(), &mut p);
    put(&(si as u32).to_le_bytes(), &mut p);
    put(&[x.protocol, x.clean_start, x.persisted], &mut p);
    put(&x.keep_alive_ms.to_le_bytes(), &mut p);
    put(&x.next_msg_id.to_le_bytes(), &mut p);
    put(&x.session_expiry_s.to_le_bytes(), &mut p);
    put(&x.receive_maximum.to_le_bytes(), &mut p);
    let age = now
        .saturating_sub(x.last_activity_ms)
        .min(u64::from(u32::MAX)) as u32;
    put(&age.to_le_bytes(), &mut p);
    put(&s.prefetch_credit[si].to_le_bytes(), &mut p);
    put(&s.sub_outstanding[si].to_le_bytes(), &mut p);
    put(&[x.will_present, x.will_qos, x.will_retain], &mut p);
    put(&x.will_delay_ms.to_le_bytes(), &mut p);
    let wt = usize::from(x.will_topic_len).min(MAX_WILL_TOPIC);
    put(&(wt as u16).to_le_bytes(), &mut p);
    put(&x.will_topic[..wt], &mut p);
    let wp = usize::from(x.will_payload_len).min(MAX_WILL_PAYLOAD);
    put(&(wp as u16).to_le_bytes(), &mut p);
    put(&x.will_payload[..wp], &mut p);
    let count_at = p;
    p += 1;
    let mut n = 0u8;
    for e in x.inflight.iter() {
        if e.active == 0 {
            continue;
        }
        put(&e.packet_id.to_le_bytes(), &mut p);
        put(&[e.qos, e.phase, e.direction], &mut p);
        put(&e.session_generation.to_le_bytes(), &mut p);
        put(&e.wal_index.to_le_bytes(), &mut p);
        put(&e.correlation_id.to_le_bytes(), &mut p);
        put(&e.partition_id.to_le_bytes(), &mut p);
        put(&[e.rec_sent], &mut p);
        n += 1;
    }
    out[count_at] = n;
    p
}

/// The identity a record names, read without importing it:
/// `(tenant, stream_hash, generation, slot)`. `None` if the record is
/// not one.
pub fn record_identity(blob: &[u8]) -> Option<(TenantId, StreamHash, SessionGeneration, u32)> {
    if blob.len() < RECORD_FIXED || blob[..4] != RECORD_MAGIC {
        return None;
    }
    let tenant = u32::from_le_bytes([blob[4], blob[5], blob[6], blob[7]]);
    let mut h = [0u8; 8];
    h.copy_from_slice(&blob[8..16]);
    let generation = u32::from_le_bytes([blob[16], blob[17], blob[18], blob[19]]);
    let slot = u32::from_le_bytes([blob[20], blob[21], blob[22], blob[23]]);
    Some((tenant, u64::from_le_bytes(h), generation, slot))
}

/// Place the record in `blob` at slot `si` as a live, connection-bound
/// session. `false` if the record is malformed or the slot is taken by
/// a different session. `generation` is the reconciled value the
/// importer resolved (see `session_identity_core`).
pub fn import_record(
    s: &mut Sessions,
    si: usize,
    blob: &[u8],
    conn_id: u16,
    generation: SessionGeneration,
    now: u64,
) -> bool {
    let Some((tenant, stream_hash, _, _)) = record_identity(blob) else {
        return false;
    };
    if si >= MAX_SESSIONS {
        return false;
    }
    if holds(s, si) && (s.slots[si].tenant != tenant || s.slots[si].stream_hash != stream_hash) {
        return false;
    }
    let mut p = 24usize;
    macro_rules! rd {
        ($n:expr) => {{
            if p + $n > blob.len() {
                return false;
            }
            let at = p;
            p += $n;
            at
        }};
    }
    let mut x = Session::zero();
    x.tenant = tenant;
    x.stream_hash = stream_hash;
    x.session_generation = generation;
    let at = rd!(3);
    x.protocol = blob[at];
    x.clean_start = blob[at + 1];
    let at = rd!(4);
    x.keep_alive_ms = u32::from_le_bytes([blob[at], blob[at + 1], blob[at + 2], blob[at + 3]]);
    let at = rd!(4);
    x.next_msg_id = u32::from_le_bytes([blob[at], blob[at + 1], blob[at + 2], blob[at + 3]]);
    let at = rd!(4);
    x.session_expiry_s = u32::from_le_bytes([blob[at], blob[at + 1], blob[at + 2], blob[at + 3]]);
    let at = rd!(2);
    x.receive_maximum = u16::from_le_bytes([blob[at], blob[at + 1]]);
    let at = rd!(4);
    let age = u32::from_le_bytes([blob[at], blob[at + 1], blob[at + 2], blob[at + 3]]);
    x.last_activity_ms = now.saturating_sub(u64::from(age));
    let at = rd!(4);
    let prefetch = u32::from_le_bytes([blob[at], blob[at + 1], blob[at + 2], blob[at + 3]]);
    let at = rd!(4);
    let outstanding = u32::from_le_bytes([blob[at], blob[at + 1], blob[at + 2], blob[at + 3]]);
    let at = rd!(3);
    x.will_present = blob[at];
    x.will_qos = blob[at + 1];
    x.will_retain = blob[at + 2];
    let at = rd!(4);
    x.will_delay_ms = u32::from_le_bytes([blob[at], blob[at + 1], blob[at + 2], blob[at + 3]]);
    let at = rd!(2);
    let wt = usize::from(u16::from_le_bytes([blob[at], blob[at + 1]]));
    if wt > MAX_WILL_TOPIC {
        return false;
    }
    let at = rd!(wt);
    x.will_topic[..wt].copy_from_slice(&blob[at..at + wt]);
    x.will_topic_len = wt as u16;
    let at = rd!(2);
    let wp = usize::from(u16::from_le_bytes([blob[at], blob[at + 1]]));
    if wp > MAX_WILL_PAYLOAD {
        return false;
    }
    let at = rd!(wp);
    x.will_payload[..wp].copy_from_slice(&blob[at..at + wp]);
    x.will_payload_len = wp as u16;
    let at = rd!(1);
    let n = usize::from(blob[at]);
    if n > MAX_INFLIGHT_PER_SESSION {
        return false;
    }
    for i in 0..n {
        let at = rd!(INFLIGHT_RECORD);
        let b = &blob[at..at + INFLIGHT_RECORD];
        let mut w = [0u8; 8];
        w.copy_from_slice(&b[9..17]);
        let mut c = [0u8; 8];
        c.copy_from_slice(&b[17..25]);
        let partition_id = u16::from_le_bytes([b[25], b[26]]);
        let rec_sent = b[27];
        x.inflight[i] = Inflight {
            packet_id: u16::from_le_bytes([b[0], b[1]]),
            qos: b[2],
            phase: b[3],
            direction: b[4],
            session_generation: u32::from_le_bytes([b[5], b[6], b[7], b[8]]),
            wal_index: u64::from_le_bytes(w),
            correlation_id: u64::from_le_bytes(c),
            partition_id,
            rec_sent,
            active: 1,
        };
    }
    // Bound and live on this worker: the anchor kept the connection.
    x.conn_id = conn_id;
    x.conn_bound = 1;
    x.present = 1;
    x.active = 1;
    x.persisted = 0;
    x.transient = 0;
    s.lent[si] = 0;
    s.slots[si] = x;
    s.prefetch_credit[si] = prefetch;
    s.sub_outstanding[si] = outstanding;
    reindex(s, si);
    true
}

/// Every active inflight entry at `si`, for re-registration after an
/// import: `(index, packet_id, wal_index, correlation_id, generation,
/// qos, phase, direction)`. Returns how many were written into `out`.
pub fn inflight_entries(
    s: &Sessions,
    si: usize,
    out: &mut [InflightView; MAX_INFLIGHT_PER_SESSION],
) -> usize {
    if si >= MAX_SESSIONS {
        return 0;
    }
    let mut n = 0usize;
    for (ii, e) in s.slots[si].inflight.iter().enumerate() {
        if e.active == 1 {
            out[n] = InflightView {
                index: ii,
                packet_id: e.packet_id,
                qos: e.qos,
                phase: e.phase,
                session_generation: e.session_generation,
                wal_index: e.wal_index,
                correlation_id: e.correlation_id,
                partition_id: e.partition_id,
                rec_sent: e.rec_sent == 1,
                direction: e.direction,
            };
            n += 1;
        }
    }
    n
}

// ── Will messages ───────────────────────────────────────────────────

/// The stored Will, as the publish path needs it.
#[derive(Clone, Copy)]
pub struct WillView {
    pub qos: u8,
    pub retain: bool,
    pub topic_len: usize,
    pub payload_len: usize,
}

pub fn will_present(s: &Sessions, si: usize) -> bool {
    si < MAX_SESSIONS && s.slots[si].will_present == 1
}

pub fn will_view(s: &Sessions, si: usize) -> Option<WillView> {
    if !will_present(s, si) {
        return None;
    }
    let x = &s.slots[si];
    Some(WillView {
        qos: x.will_qos & 0x03,
        retain: x.will_retain != 0,
        topic_len: x.will_topic_len as usize,
        payload_len: x.will_payload_len as usize,
    })
}

pub fn will_topic_into(s: &Sessions, si: usize, dst: &mut [u8]) -> usize {
    if si >= MAX_SESSIONS {
        return 0;
    }
    let n = (s.slots[si].will_topic_len as usize).min(dst.len());
    dst[..n].copy_from_slice(&s.slots[si].will_topic[..n]);
    n
}

pub fn will_payload_into(s: &Sessions, si: usize, dst: &mut [u8]) -> usize {
    if si >= MAX_SESSIONS {
        return 0;
    }
    let n = (s.slots[si].will_payload_len as usize).min(dst.len());
    dst[..n].copy_from_slice(&s.slots[si].will_payload[..n]);
    n
}

/// Store the Will carried by CONNECT. A CONNECT always *replaces* the
/// prior Will — including replacing it with nothing, which is the
/// correct behaviour per MQTT 3.1.1 §3.1.2.5, so `clear_will` is the
/// no-Will branch rather than a no-op.
pub fn set_will(
    s: &mut Sessions,
    si: usize,
    qos: u8,
    retain: bool,
    delay_ms: u32,
    topic: &[u8],
    payload: &[u8],
) {
    if si >= MAX_SESSIONS {
        return;
    }
    let x = &mut s.slots[si];
    x.will_present = 1;
    x.will_qos = qos & 0x03;
    x.will_retain = u8::from(retain);
    x.will_delay_ms = delay_ms;
    let tn = topic.len().min(MAX_WILL_TOPIC);
    let pn = payload.len().min(MAX_WILL_PAYLOAD);
    x.will_topic_len = tn as u16;
    x.will_payload_len = pn as u16;
    x.will_topic[..tn].copy_from_slice(&topic[..tn]);
    x.will_payload[..pn].copy_from_slice(&payload[..pn]);
}

pub fn clear_will(s: &mut Sessions, si: usize) {
    if si >= MAX_SESSIONS {
        return;
    }
    let x = &mut s.slots[si];
    x.will_present = 0;
    x.will_topic_len = 0;
    x.will_payload_len = 0;
}

/// Arm the delayed-Will timer, or disarm it with `at_ms == 0`.
pub fn set_will_deadline(s: &mut Sessions, si: usize, at_ms: u64) {
    if si < MAX_SESSIONS {
        s.slots[si].pending_will_fire_at_ms = at_ms;
    }
}

/// True when an armed delayed Will has come due.
pub fn will_due(s: &Sessions, si: usize, now: u64) -> bool {
    si < MAX_SESSIONS
        && s.slots[si].pending_will_fire_at_ms != 0
        && now >= s.slots[si].pending_will_fire_at_ms
}

pub fn will_deadline(s: &Sessions, si: usize) -> u64 {
    if si < MAX_SESSIONS {
        s.slots[si].pending_will_fire_at_ms
    } else {
        0
    }
}

pub fn will_delay_ms(s: &Sessions, si: usize) -> u32 {
    if si < MAX_SESSIONS {
        s.slots[si].will_delay_ms
    } else {
        0
    }
}

// ── QoS inflight ────────────────────────────────────────────────────

/// What an in-flight entry carries for the QoS state machine.
#[derive(Clone, Copy)]
pub struct InflightView {
    pub index: usize,
    pub packet_id: u16,
    pub qos: u8,
    pub phase: u8,
    pub direction: u8,
    pub partition_id: u16,
    pub rec_sent: bool,
    pub session_generation: SessionGeneration,
    pub correlation_id: u64,
    pub wal_index: u64,
}

impl InflightView {
    pub const fn zero() -> Self {
        Self {
            index: 0,
            packet_id: 0,
            qos: 0,
            phase: 0,
            direction: 0,
            partition_id: 0,
            rec_sent: false,
            session_generation: 0,
            correlation_id: 0,
            wal_index: 0,
        }
    }
}

pub fn inflight_view(s: &Sessions, si: usize, ii: usize) -> Option<InflightView> {
    if si >= MAX_SESSIONS || ii >= MAX_INFLIGHT_PER_SESSION {
        return None;
    }
    let e = &s.slots[si].inflight[ii];
    if e.active != 1 {
        return None;
    }
    Some(InflightView {
        index: ii,
        packet_id: e.packet_id,
        direction: e.direction,
        partition_id: e.partition_id,
        rec_sent: e.rec_sent == 1,
        qos: e.qos,
        phase: e.phase,
        session_generation: e.session_generation,
        correlation_id: e.correlation_id,
        wal_index: e.wal_index,
    })
}

/// Reserve an in-flight slot for `packet_id` in `direction`.
pub fn inflight_add(
    s: &mut Sessions,
    si: usize,
    packet_id: u16,
    qos: u8,
    direction: u8,
) -> Option<usize> {
    if si >= MAX_SESSIONS {
        return None;
    }
    s.slots[si].allocate_inflight(packet_id, qos, direction)
}

pub fn inflight_release(s: &mut Sessions, si: usize, ii: usize) {
    if si < MAX_SESSIONS && ii < MAX_INFLIGHT_PER_SESSION {
        s.slots[si].inflight[ii].active = 0;
    }
}

/// Advance a QoS 2 entry to the PUBREL phase.
/// The PUBREC for this QoS 2 flow went out.
pub fn inflight_mark_rec_sent(s: &mut Sessions, si: usize, ii: usize) {
    if si < MAX_SESSIONS && ii < MAX_INFLIGHT_PER_SESSION {
        s.slots[si].inflight[ii].rec_sent = 1;
    }
}

pub fn inflight_set_phase(s: &mut Sessions, si: usize, ii: usize, phase: u8) {
    if si < MAX_SESSIONS && ii < MAX_INFLIGHT_PER_SESSION {
        s.slots[si].inflight[ii].phase = phase;
    }
}

/// Stamp the session epoch the flow opened under. Set once, when the
/// publish that opened the flow applies.
pub fn inflight_set_generation(s: &mut Sessions, si: usize, ii: usize, epoch: SessionGeneration) {
    if si < MAX_SESSIONS && ii < MAX_INFLIGHT_PER_SESSION {
        s.slots[si].inflight[ii].session_generation = epoch;
    }
}

pub fn inflight_set_correlation(s: &mut Sessions, si: usize, ii: usize, cid: u64) {
    if si < MAX_SESSIONS && ii < MAX_INFLIGHT_PER_SESSION {
        s.slots[si].inflight[ii].correlation_id = cid;
    }
}

pub fn inflight_set_wal_index(
    s: &mut Sessions,
    si: usize,
    ii: usize,
    partition_id: u16,
    wal_index: u64,
) {
    if si < MAX_SESSIONS && ii < MAX_INFLIGHT_PER_SESSION {
        s.slots[si].inflight[ii].partition_id = partition_id;
        s.slots[si].inflight[ii].wal_index = wal_index;
    }
}

/// Find an in-flight QoS packet by id and direction.
/// Release every SUBSCRIBER-side inflight slot on this session and
/// reset its outstanding count. Returns how many were released.
///
/// Called when a session is resumed on a new connection. MQTT 3.1.1
/// §4.4 says the server must RE-SEND unacknowledged QoS 1+ publishes on
/// resume — but a subscriber-side slot records only `(packet_id, qos,
/// phase)`; `correlation_id` is 0 for these by construction and no
/// reference to the message body is kept, so the broker no longer knows
/// what to send. Those messages are already unrecoverable.
///
/// Given that, holding the slots is strictly worse than dropping them:
/// they keep counting toward `ReceiveMaximum`, so after enough resumed
/// sessions the outstanding count reaches the cap and the subscriber
/// can NEVER receive again — the messages stay lost AND the session is
/// dead. Releasing them loses nothing further and keeps the session
/// usable. The caller counts the release so the loss is visible rather
/// than silent.
///
/// Real redelivery would need the delivered envelope (or a durable
/// pointer to it) retained per inflight slot, which no slot carries.
pub fn release_sub_inflight(s: &mut Sessions, si: usize, direction: u8) -> u32 {
    if si >= MAX_SESSIONS {
        return 0;
    }
    let mut n = 0u32;
    for ii in 0..MAX_INFLIGHT_PER_SESSION {
        let e = &mut s.slots[si].inflight[ii];
        if e.active == 1 && e.direction == direction {
            *e = Inflight::zero();
            n += 1;
        }
    }
    s.sub_outstanding[si] = 0;
    n
}

pub fn inflight_find(s: &Sessions, si: usize, packet_id: u16, direction: u8) -> Option<usize> {
    if si >= MAX_SESSIONS {
        return None;
    }
    s.slots[si].find_inflight_dir(packet_id, direction)
}
