//! correlate — the propose→apply correlation table and the
//! commit-gating stashes.
//!
//! Three pieces of state that all answer the same question — "what was
//! this in-flight operation, and may it proceed yet?":
//!
//!   - `pending` — tagged Raft proposals awaiting their
//!     `proposal_assigned` round-trip, so a correlation id can be bound
//!     back to `(session_slot, packet_id, op)`. A reply that never
//!     arrives (leadership churn, a dropped assignment) would leak its
//!     slot, so entries age out on a sweep.
//!   - `dlv` — delivery envelopes held until both dedup and durability
//!     resolve. The commit-gating contract requires the envelope to
//!     survive that long, so a full stash means the publish must be
//!     dropped rather than delivered early.
//!   - `ack` — ack registrations held for the same reason.
//!
//! ## Per-step bound
//!
//! Every entry point is O(slots) over fixed tables and returns without
//! blocking. The stash drains are driven by the dispatch table at its
//! own per-step budget.

use super::wire;
use super::{
    MAX_PENDING_CORRELATIONS, MAX_PENDING_DLV, MAX_STASH_ENV, PENDING_ACK_SLOTS, PENDING_DLV_SLOTS,
    STASH_DEDUP_PENDING, STASH_SLOTS,
};

/// Pending correlation: a tagged Raft proposal waiting for the
/// `proposal_assigned` round-trip so we can bind correlation_id →
/// (session_slot, packet_id, op). After the binding we register with
/// the ack component and free the slot.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PendingCorrelation {
    correlation_id: u64,
    session_slot: u32,
    packet_id: u16,
    op: u8,
    active: u8,
    /// Allocation time. A correlation whose MSG_PROPOSAL_ASSIGNED reply is
    /// never delivered (leadership churn, raft drop, assigned-queue
    /// overflow) would otherwise leak its slot forever; the 1 Hz sweep
    /// reclaims entries older than CORRELATION_TIMEOUT_MS so a slow leak
    /// can't exhaust the table and stall acks across ALL protocols.
    ts_ms: u64,
}

impl PendingCorrelation {
    pub const fn zero() -> Self {
        Self {
            correlation_id: 0,
            session_slot: 0,
            packet_id: 0,
            op: 0,
            active: 0,
            ts_ms: 0,
        }
    }
}

/// Component state. Owned exclusively by this subtree.
#[repr(C)]
pub struct Correlate {
    pending: [PendingCorrelation; MAX_PENDING_CORRELATIONS],
    next_correlation_id: u64,
    dropped: u32,
    /// Live correlations, and ack registrations parked because the
    /// edge to the ack component was full. Their sum is what
    /// `allocate` reserves against: a correlation that exists is a
    /// promise that its ack registration will have somewhere to go.
    pending_active: u32,
    ack_parked: u32,

    dlv_active: [u8; PENDING_DLV_SLOTS],
    dlv_len: [u16; PENDING_DLV_SLOTS],
    dlv_env: [[u8; MAX_PENDING_DLV]; PENDING_DLV_SLOTS],

    ack_active: [u8; PENDING_ACK_SLOTS],
    ack_buf: [[u8; wire::ACK_REGISTER_LEN]; PENDING_ACK_SLOTS],

    /// Commit-gating stash: a publish is released only once BOTH its
    /// dedup verdict and its durability have landed, which arrive
    /// independently and in either order. The envelope must survive
    /// until then, so a full stash means the publish is dropped rather
    /// than released early.
    /// Slot occupancy, held separately from `stash_correlation` so a
    /// stash can carry correlation id 0. Apply-side stashes on a
    /// follower have no client correlation to name.
    stash_active: [u8; STASH_SLOTS],
    stash_correlation: [u64; STASH_SLOTS],
    stash_dedup_key: [[u8; 20]; STASH_SLOTS],
    stash_dedup_state: [u8; STASH_SLOTS],
    stash_durable: [u8; STASH_SLOTS],
    stash_env_len: [u16; STASH_SLOTS],
    stash_env: [[u8; MAX_STASH_ENV]; STASH_SLOTS],
}

// ── Commit-gating stash ─────────────────────────────────────────────

/// Reserve a stash slot for a publish awaiting dedup + durability.
pub fn stash_alloc(s: &mut Correlate, correlation_id: u64, dedup_key: &[u8]) -> Option<usize> {
    for i in 0..STASH_SLOTS {
        if s.stash_active[i] == 0 {
            s.stash_active[i] = 1;
            s.stash_correlation[i] = correlation_id;
            let k = dedup_key.len().min(20);
            s.stash_dedup_key[i][..k].copy_from_slice(&dedup_key[..k]);
            for j in k..20 {
                s.stash_dedup_key[i][j] = 0;
            }
            s.stash_dedup_state[i] = STASH_DEDUP_PENDING;
            s.stash_durable[i] = 0;
            s.stash_env_len[i] = 0;
            return Some(i);
        }
    }
    None
}

pub fn stash_by_correlation(s: &Correlate, cid: u64) -> Option<usize> {
    (0..STASH_SLOTS).find(|&i| s.stash_active[i] == 1 && s.stash_correlation[i] == cid)
}

pub fn stash_by_dedup_key(s: &Correlate, key: &[u8]) -> Option<usize> {
    if key.len() < 20 {
        return None;
    }
    (0..STASH_SLOTS).find(|&i| s.stash_active[i] == 1 && s.stash_dedup_key[i][..] == key[..20])
}

pub fn stash_release(s: &mut Correlate, slot: usize) {
    if slot >= STASH_SLOTS {
        return;
    }
    s.stash_active[slot] = 0;
    s.stash_correlation[slot] = 0;
    s.stash_dedup_state[slot] = STASH_DEDUP_PENDING;
    s.stash_durable[slot] = 0;
    s.stash_env_len[slot] = 0;
}

pub fn stash_occupied(s: &Correlate, slot: usize) -> bool {
    slot < STASH_SLOTS && s.stash_active[slot] == 1
}

pub fn stash_dedup_state(s: &Correlate, slot: usize) -> u8 {
    if slot < STASH_SLOTS {
        s.stash_dedup_state[slot]
    } else {
        STASH_DEDUP_PENDING
    }
}

pub fn stash_set_dedup_state(s: &mut Correlate, slot: usize, state: u8) {
    if slot < STASH_SLOTS {
        s.stash_dedup_state[slot] = state;
    }
}

pub fn stash_is_durable(s: &Correlate, slot: usize) -> bool {
    slot < STASH_SLOTS && s.stash_durable[slot] != 0
}

pub fn stash_mark_durable(s: &mut Correlate, slot: usize) {
    if slot < STASH_SLOTS {
        s.stash_durable[slot] = 1;
    }
}

pub fn stash_env_len(s: &Correlate, slot: usize) -> usize {
    if slot < STASH_SLOTS {
        s.stash_env_len[slot] as usize
    } else {
        0
    }
}

/// Store the publish envelope. False if it does not fit.
pub fn stash_set_env(s: &mut Correlate, slot: usize, env: &[u8]) -> bool {
    if slot >= STASH_SLOTS || env.len() > MAX_STASH_ENV {
        return false;
    }
    for (i, b) in env.iter().enumerate() {
        s.stash_env[slot][i] = *b;
    }
    s.stash_env_len[slot] = env.len() as u16;
    true
}

/// Borrow the stashed envelope.
pub fn stash_env(s: &Correlate, slot: usize) -> &[u8] {
    if slot >= STASH_SLOTS {
        return &[];
    }
    &s.stash_env[slot][..s.stash_env_len[slot] as usize]
}

pub fn init(s: &mut Correlate) {
    for p in s.pending.iter_mut() {
        *p = PendingCorrelation::zero();
    }
    s.next_correlation_id = 0;
    s.dropped = 0;
    s.pending_active = 0;
    s.ack_parked = 0;
    for i in 0..PENDING_DLV_SLOTS {
        s.dlv_active[i] = 0;
        s.dlv_len[i] = 0;
    }
    for i in 0..PENDING_ACK_SLOTS {
        s.ack_active[i] = 0;
    }
    for i in 0..STASH_SLOTS {
        s.stash_active[i] = 0;
        s.stash_correlation[i] = 0;
        s.stash_dedup_key[i] = [0u8; 20];
        s.stash_dedup_state[i] = STASH_DEDUP_PENDING;
        s.stash_durable[i] = 0;
        s.stash_env_len[i] = 0;
    }
}

/// Wipe every correlation and stash. Apply-derived state does not
/// survive a reset; in-flight operations are abandoned and their
/// clients time out.
pub fn reset(s: &mut Correlate) {
    init(s);
}

// ── Correlations ────────────────────────────────────────────────────

/// Bind a new correlation and return its id, or None when there is no
/// capacity (the caller must then refuse the operation rather than
/// propose something it can never match back).
///
/// Capacity is the correlation table AND a parking slot for the ack
/// registration this correlation will produce. Both are reserved here,
/// before the work is accepted, so the registration can never be
/// discarded later for want of room: the operation is refused while it
/// is still the client's to retry, never after the protocol has
/// acknowledged it.
pub fn allocate(
    s: &mut Correlate,
    session_slot: u32,
    packet_id: u16,
    op: u8,
    now: u64,
) -> Option<u64> {
    if s.pending_active + s.ack_parked >= PENDING_ACK_SLOTS as u32 {
        return None;
    }
    for i in 0..MAX_PENDING_CORRELATIONS {
        if s.pending[i].active == 0 {
            s.next_correlation_id = s.next_correlation_id.wrapping_add(1);
            if s.next_correlation_id == 0 {
                s.next_correlation_id = 1;
            }
            let cid = s.next_correlation_id;
            s.pending[i] = PendingCorrelation {
                correlation_id: cid,
                session_slot,
                packet_id,
                op,
                active: 1,
                ts_ms: now,
            };
            s.pending_active = s.pending_active.saturating_add(1);
            return Some(cid);
        }
    }
    None
}

/// Issue time of a pending correlation, for age accounting before it
/// is taken.
pub fn issued_ms(s: &Correlate, cid: u64) -> Option<u64> {
    (0..MAX_PENDING_CORRELATIONS)
        .find(|&i| s.pending[i].active == 1 && s.pending[i].correlation_id == cid)
        .map(|i| s.pending[i].ts_ms)
}

/// Resolve and free a correlation. Returns `(session_slot, packet_id,
/// op)` if it was still outstanding.
pub fn take(s: &mut Correlate, cid: u64) -> Option<(u32, u16, u8)> {
    for i in 0..MAX_PENDING_CORRELATIONS {
        if s.pending[i].active == 1 && s.pending[i].correlation_id == cid {
            let r = (
                s.pending[i].session_slot,
                s.pending[i].packet_id,
                s.pending[i].op,
            );
            s.pending[i] = PendingCorrelation::zero();
            s.pending_active = s.pending_active.saturating_sub(1);
            return Some(r);
        }
    }
    None
}

/// Count correlations older than `timeout_ms` without touching them.
///
/// A live correlation names a proposal the substrate accepted, and the
/// substrate emits `proposal_assigned` exactly once for every accepted
/// tagged proposal. The assignment is therefore always still in flight,
/// however late it is, and reclaiming the slot would consume it with
/// nowhere to bind it — the publish would be accepted and then never
/// acknowledged. Capacity is bounded at the other end instead:
/// `allocate` refuses while the publish is still the client's to retry.
///
/// The count is a leak detector. A non-zero value means an assignment
/// went missing, which is a substrate contract violation, not something
/// this table can correct.
pub fn count_overdue(s: &Correlate, now: u64, timeout_ms: u64) -> u32 {
    let mut n = 0;
    for i in 0..MAX_PENDING_CORRELATIONS {
        if s.pending[i].active == 1 && now.wrapping_sub(s.pending[i].ts_ms) > timeout_ms {
            n += 1;
        }
    }
    n
}

/// Account one correlation abandoned by the caller — a publish refused
/// after its slot was bound, a throttled operation, a session torn down
/// mid-flight. Kept here so the drop count is the component's, not a
/// tally spread across every call site.
/// True while any tagged proposal of `session_slot` awaits its Raft
/// assignment — the session is mid-transaction and not exportable.
pub fn pending_for_slot(s: &Correlate, session_slot: u32) -> bool {
    s.pending
        .iter()
        .any(|p| p.active == 1 && p.session_slot == session_slot)
}

/// True while a delivery for `session_slot` is parked on the defer
/// queue (delivery envelopes carry the slot in their first four bytes).
pub fn dlv_parked_for_slot(s: &Correlate, session_slot: u32) -> bool {
    let want = session_slot.to_le_bytes();
    (0..PENDING_DLV_SLOTS)
        .any(|i| s.dlv_active[i] == 1 && s.dlv_len[i] >= 4 && s.dlv_env[i][..4] == want)
}

/// True while an ack registration for `session_slot` is parked.
pub fn ack_parked_for_slot(s: &Correlate, session_slot: u32) -> bool {
    let want = session_slot.to_le_bytes();
    (0..PENDING_ACK_SLOTS).any(|i| s.ack_active[i] == 1 && s.ack_buf[i][..4] == want)
}

pub fn note_dropped(s: &mut Correlate) {
    s.dropped = s.dropped.wrapping_add(1);
}

pub fn dropped(s: &Correlate) -> u32 {
    s.dropped
}

// ── Delivery stash ──────────────────────────────────────────────────

/// Reserve a stash slot for a commit-gated delivery envelope. None when
/// full — the caller must drop the publish, because the commit-gating
/// contract requires the envelope to survive until both dedup and
/// durability resolve.
pub fn dlv_alloc(s: &Correlate) -> Option<usize> {
    (0..PENDING_DLV_SLOTS).find(|&i| s.dlv_active[i] == 0)
}

/// Park a backpressured envelope: reserve a slot and store it in one
/// step. False when the stash is full or the envelope does not fit, in
/// which case the caller must account the drop.
pub fn dlv_park(s: &mut Correlate, env: &[u8]) -> bool {
    match dlv_alloc(s) {
        Some(slot) => dlv_store(s, slot, env),
        None => false,
    }
}

/// Store an envelope in a reserved slot. Returns false if it does not
/// fit, leaving the slot free.
pub fn dlv_store(s: &mut Correlate, slot: usize, env: &[u8]) -> bool {
    if slot >= PENDING_DLV_SLOTS || env.len() > MAX_PENDING_DLV {
        return false;
    }
    for (i, b) in env.iter().enumerate() {
        s.dlv_env[slot][i] = *b;
    }
    s.dlv_len[slot] = env.len() as u16;
    s.dlv_active[slot] = 1;
    true
}

pub fn dlv_is_active(s: &Correlate, slot: usize) -> bool {
    slot < PENDING_DLV_SLOTS && s.dlv_active[slot] == 1
}

pub fn dlv_len(s: &Correlate, slot: usize) -> usize {
    if slot < PENDING_DLV_SLOTS {
        s.dlv_len[slot] as usize
    } else {
        0
    }
}

/// Copy a stashed envelope out into `dst`. False if the slot is empty or
/// `dst` is too small.
pub fn dlv_copy_out(s: &Correlate, slot: usize, dst: &mut [u8]) -> bool {
    if !dlv_is_active(s, slot) {
        return false;
    }
    let n = s.dlv_len[slot] as usize;
    if n > dst.len() {
        return false;
    }
    dst[..n].copy_from_slice(&s.dlv_env[slot][..n]);
    true
}

pub fn dlv_free(s: &mut Correlate, slot: usize) {
    if slot < PENDING_DLV_SLOTS {
        s.dlv_active[slot] = 0;
        s.dlv_len[slot] = 0;
    }
}

// ── Ack stash ───────────────────────────────────────────────────────

/// Park an 18-byte ack registration whose emission was backpressured.
/// The slot was reserved when the correlation was allocated, so this
/// succeeds for every registration the broker owes; false means the
/// reservation invariant was violated and the caller must say so
/// rather than account the work dropped.
pub fn ack_stash(s: &mut Correlate, reg: [u8; wire::ACK_REGISTER_LEN]) -> bool {
    for i in 0..PENDING_ACK_SLOTS {
        if s.ack_active[i] == 0 {
            s.ack_buf[i] = reg;
            s.ack_active[i] = 1;
            s.ack_parked = s.ack_parked.saturating_add(1);
            return true;
        }
    }
    false
}

/// How many registrations are parked. Zero lets the drain skip its
/// sweep entirely, which is the ordinary case.
pub fn ack_parked(s: &Correlate) -> u32 {
    s.ack_parked
}

pub fn ack_is_active(s: &Correlate, slot: usize) -> bool {
    slot < PENDING_ACK_SLOTS && s.ack_active[slot] == 1
}

pub fn ack_get(s: &Correlate, slot: usize) -> Option<[u8; wire::ACK_REGISTER_LEN]> {
    if ack_is_active(s, slot) {
        Some(s.ack_buf[slot])
    } else {
        None
    }
}

pub fn ack_free(s: &mut Correlate, slot: usize) {
    if slot < PENDING_ACK_SLOTS && s.ack_active[slot] == 1 {
        s.ack_active[slot] = 0;
        s.ack_parked = s.ack_parked.saturating_sub(1);
    }
}
