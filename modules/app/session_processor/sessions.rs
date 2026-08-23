//! sessions — the per-session state machine table.
//!
//! Owns `Session[MAX_SESSIONS]` and each session's QoS `Inflight`
//! sub-table: identity (tenant, stream hash, epoch), connection binding,
//! keep-alive and expiry deadlines, will-message state, and the
//! in-flight QoS 1/2 packets awaiting their next protocol step.
//!
//! ## Per-step bound
//!
//! Every entry point is O(MAX_SESSIONS) or O(MAX_INFLIGHT_PER_SESSION)
//! over fixed tables and returns without blocking.

use super::{
    SessionEpoch, StreamHash, TenantId, MAX_INFLIGHT_PER_SESSION, MAX_SESSIONS, MAX_WILL_PAYLOAD,
    MAX_WILL_TOPIC, PROTO_UNKNOWN, QOS2_PUBLISH,
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
    session_epoch: SessionEpoch,
    wal_index: u64,
    /// Raft correlation_id for the proposal that owns this slot (QoS 1+
    /// only; 0 for QoS 0 and subscriber-side slots). Used to look up the
    /// stashed topic-publish envelope on durability and to re-emit the
    /// proposal on MSG_ACK_REDELIVER.
    correlation_id: u64,
    direction: u8,
    active: u8,
}

impl Inflight {
    pub const fn zero() -> Self {
        Self {
            packet_id: 0,
            qos: 0,
            phase: 0,
            session_epoch: 0,
            wal_index: 0,
            correlation_id: 0,
            direction: 0,
            active: 0,
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct Session {
    tenant: TenantId,
    stream_hash: StreamHash,
    session_epoch: SessionEpoch,
    protocol: u8,
    conn_id: u8,
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
            session_epoch: 0,
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
                    session_epoch: 0,
                    wal_index: 0,
                    correlation_id: 0,
                    direction,
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
}

pub fn init(s: &mut Sessions) {
    for x in s.slots.iter_mut() {
        *x = Session::zero();
    }
    for i in 0..MAX_SESSIONS {
        s.prefetch_credit[i] = 0;
        s.sub_outstanding[i] = 0;
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

/// Find the session bound to `conn_id`.
///
/// Matches `active` OR `transient` slots: between CONNECT receipt and
/// the QOP_CONNECT commit the slot is transient, and follow-up packets
/// from the same connection must still route to it for admission
/// control.
pub fn find_by_conn(s: &Sessions, conn_id: u8) -> Option<usize> {
    (0..MAX_SESSIONS).find(|&i| {
        let x = &s.slots[i];
        (x.active == 1 || x.transient == 1) && x.conn_id == conn_id
    })
}

/// Find any session matching `(tenant, stream_hash)`, including
/// `persisted` (disconnected, clean_start=false) and `transient`
/// (CONNECT received, apply pending) slots. Reconnect uses this to
/// resurrect the prior session per MQTT 3.1.1 §3.1.2.4, and apply-side
/// QOP_CONNECT to reconcile a transient record with its durable
/// counterpart.
pub fn find_by_stream(s: &Sessions, tenant: TenantId, stream_hash: StreamHash) -> Option<usize> {
    (0..MAX_SESSIONS).find(|&i| {
        let x = &s.slots[i];
        (x.present == 1 || x.transient == 1) && x.tenant == tenant && x.stream_hash == stream_hash
    })
}

/// Allocate a free slot — skips active, persisted and transient sessions.
pub fn allocate(s: &Sessions) -> Option<usize> {
    (0..MAX_SESSIONS).find(|&i| {
        let x = &s.slots[i];
        x.present == 0 && x.transient == 0
    })
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
    pub session_epoch: SessionEpoch,
    pub protocol: u8,
    pub conn_id: u8,
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
        session_epoch: x.session_epoch,
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

pub fn conn_id(s: &Sessions, si: usize) -> u8 {
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

pub fn session_epoch(s: &Sessions, si: usize) -> SessionEpoch {
    if si < MAX_SESSIONS {
        s.slots[si].session_epoch
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
    conn_id: u8,
    protocol: u8,
    clean_start: bool,
    keep_alive_ms: u32,
) {
    if si >= MAX_SESSIONS {
        return;
    }
    let x = &mut s.slots[si];
    x.conn_id = conn_id;
    x.protocol = protocol;
    x.clean_start = u8::from(clean_start);
    x.keep_alive_ms = keep_alive_ms;
}

/// Occupy a free slot for a CONNECT that has no prior session. `active`
/// flips at QOP_CONNECT apply; until then the slot is transient.
pub fn open_transient(s: &mut Sessions, si: usize, p: ConnectParams, conn_id: u8, now: u64) {
    if si >= MAX_SESSIONS {
        return;
    }
    s.slots[si] = Session::zero();
    let x = &mut s.slots[si];
    x.tenant = p.tenant;
    x.stream_hash = p.stream_hash;
    x.protocol = p.protocol;
    x.conn_id = conn_id;
    x.transient = 1;
    x.clean_start = u8::from(p.clean_start);
    x.keep_alive_ms = p.keep_alive_ms;
    x.last_activity_ms = now;
    x.next_msg_id = 1;
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

/// Apply a committed CONNECT. Bumps the session epoch — the fencing
/// token every in-flight round-trip is validated against — and clears
/// `persisted`, since the session is no longer parked.
pub fn commit_connect(s: &mut Sessions, si: usize, p: ConnectParams) {
    if si >= MAX_SESSIONS {
        return;
    }
    let x = &mut s.slots[si];
    x.tenant = p.tenant;
    x.stream_hash = p.stream_hash;
    x.session_epoch = x.session_epoch.wrapping_add(1);
    x.protocol = p.protocol;
    x.clean_start = u8::from(p.clean_start);
    x.keep_alive_ms = p.keep_alive_ms;
    x.present = 1;
    x.persisted = 0;
}

/// Leader path: the propose-side admitted this session, apply now makes
/// it durable.
pub fn mark_active(s: &mut Sessions, si: usize) {
    if si < MAX_SESSIONS {
        s.slots[si].active = 1;
        s.slots[si].transient = 0;
    }
}

/// Follower / replay path under `!clean_start`: park the durable record
/// so a later reconnect on this node can resurrect it.
pub fn mark_persisted(s: &mut Sessions, si: usize) {
    if si < MAX_SESSIONS {
        s.slots[si].persisted = 1;
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

pub fn set_conn(s: &mut Sessions, si: usize, conn_id: u8, protocol: u8) {
    if si < MAX_SESSIONS {
        s.slots[si].conn_id = conn_id;
        s.slots[si].protocol = protocol;
    }
}

pub fn mark_transient(s: &mut Sessions, si: usize) {
    if si < MAX_SESSIONS {
        s.slots[si].transient = 1;
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
}

/// Unbind the connection without ending the session — the slot keeps
/// its identity for a reconnect.
pub fn unbind_conn(s: &mut Sessions, si: usize) {
    if si < MAX_SESSIONS {
        s.slots[si].conn_id = 0;
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
    }
}

/// Free the slot entirely.
pub fn clear(s: &mut Sessions, si: usize) {
    if si < MAX_SESSIONS {
        s.slots[si] = Session::zero();
    }
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
    pub qos: u8,
    pub phase: u8,
    pub session_epoch: SessionEpoch,
    pub correlation_id: u64,
    pub wal_index: u64,
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
        qos: e.qos,
        phase: e.phase,
        session_epoch: e.session_epoch,
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
pub fn inflight_set_phase(s: &mut Sessions, si: usize, ii: usize, phase: u8) {
    if si < MAX_SESSIONS && ii < MAX_INFLIGHT_PER_SESSION {
        s.slots[si].inflight[ii].phase = phase;
    }
}

/// Stamp the session epoch the flow opened under. Set once, when the
/// publish that opened the flow applies.
pub fn inflight_set_epoch(s: &mut Sessions, si: usize, ii: usize, epoch: SessionEpoch) {
    if si < MAX_SESSIONS && ii < MAX_INFLIGHT_PER_SESSION {
        s.slots[si].inflight[ii].session_epoch = epoch;
    }
}

pub fn inflight_set_correlation(s: &mut Sessions, si: usize, ii: usize, cid: u64) {
    if si < MAX_SESSIONS && ii < MAX_INFLIGHT_PER_SESSION {
        s.slots[si].inflight[ii].correlation_id = cid;
    }
}

pub fn inflight_set_wal_index(s: &mut Sessions, si: usize, ii: usize, wal_index: u64) {
    if si < MAX_SESSIONS && ii < MAX_INFLIGHT_PER_SESSION {
        s.slots[si].inflight[ii].wal_index = wal_index;
    }
}

/// Find an in-flight QoS packet by id and direction.
pub fn inflight_find(s: &Sessions, si: usize, packet_id: u16, direction: u8) -> Option<usize> {
    if si >= MAX_SESSIONS {
        return None;
    }
    s.slots[si].find_inflight_dir(packet_id, direction)
}
