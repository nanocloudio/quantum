//! Session Processor — Unified session state machine for MQTT/Kafka/AMQP.
//!
//! The Raft apply callback for Quantum. Uses multiplexed channels to stay
//! within the 8-port-per-direction Fluxor wire limit.
//!
//! Ingress envelope (codec → session): `[conn_id:u8][mtype:u8][proto][pkt_type][flags][fields]`.
//! Egress envelope (session → codec):  `[conn_id:u8][mtype:u8][proto][pkt_type][flags][body]`.
//!
//! Per-session state tracks conn_id so responses go back to the correct
//! client connection. Session → conn_id is set on CONNECT and cleared on
//! DISCONNECT.
//!
//! Ports (7 in, 7 out):
//!   in[0] codec_in      — protocol proposals
//!   in[1] committed_in  — applied entries from apply_pipeline
//!   in[2] flow_in       — PID credits + backpressure + prefetch
//!   in[3] ack_in        — ACK emit + redeliver from ack_tracker
//!   in[4] cp_in         — capabilities + epoch + disconnect
//!   in[5] messaging_in  — dedup/offline/retained/group/txn results
//!   in[6] deliver_in    — topic deliveries
//!   out[0] proposals    — to raft_engine (fluxor wire envelope)
//!   out[1] codec_out    — [conn_id][mtype][envelope] responses to codecs
//!   out[2] topic_out    — publish + subscribe + unsubscribe (wire envelope)
//!   out[3] messaging_out — dedup/offline/retained/group/txn ops (wire envelope)
//!   out[4] forward_out  — forward + inflight register (wire envelope)
//!   out[5] audit_out    — audit events (wire envelope)
//!   out[6] metrics_out  — metrics (wire envelope)

#![no_std]
#![allow(
    unused_imports,
    dead_code,
    reason = "the fluxor SDK is include!'d wholesale and each module consumes only a subset; pending upstream allow attributes in target/fluxor/fluxor-abi/sdk/"
)]

use core::ffi::c_void;

#[allow(
    unused_imports,
    dead_code,
    reason = "see file-level allow: SDK surface is shared across modules"
)]
#[path = "../../../target/fluxor/fluxor-abi/sdk/abi.rs"]
mod abi;
use abi::SyscallTable;

include!("../../../target/fluxor/fluxor-abi/sdk/runtime.rs");
include!("../../../target/fluxor/fluxor-abi/sdk/params.rs");

#[path = "../../common/wire.rs"]
mod wire;

#[path = "../../common/types.rs"]
mod types;

use types::*;

const MAX_SESSIONS: usize = 1024;
const MAX_INFLIGHT_PER_SESSION: usize = 16;
/// Read/write buffer cap for codec ↔ session_processor and
/// session_processor ↔ topic_engine / forward_coordinator /
/// messaging-fan-out traffic. Sized to the wire-channel per-message
/// capacity (`fluxor-abi::CHANNEL_BUFFER_SIZE` = 8192) so a worst-case
/// MQTT packet (`mqtt_codec::MAX_PACKET` = 4096) plus routing headers
/// (`MSG_TOPIC_PUBLISH` topic prefix, `MSG_TOPIC_DELIVER` session-slot
/// prefix, etc.) fits without hitting the `channel_read_msg` discard
/// path.
const BUF_SIZE: usize = 8192;
const MAX_PENDING_CORRELATIONS: usize = 1024;

/// Stash slots for QoS 1+ topic-publish envelopes that are commit-gated.
/// Each slot retains the dedup_key plus the topic-publish envelope (up to
/// `MAX_STASH_ENV` bytes) and tracks dedup + durability resolution; the
/// fan-out to topic_engine only happens once both have landed and the
/// publish was not flagged as duplicate. Bounds the worst-case memory
/// cost of deferred topic emission to a fixed allocation in module state.
const STASH_SLOTS: usize = 32;
const MAX_STASH_ENV: usize = 2048;
/// Bigger QoS 1+ publishes (rare in MQTT control-plane traffic) overflow
/// the stash and fall back to the legacy fan-out semantics — they are
/// dropped to preserve the commit-gating invariant for the in-budget path.
const STASH_DEDUP_PENDING: u8 = 0;
const STASH_DEDUP_OK: u8 = 1;
const STASH_DEDUP_DUPLICATE: u8 = 2;

/// Defer queue for subscriber deliveries that hit backpressure
/// (prefetch_credit cap, PID exhaustion, or inflight-slot exhaustion).
/// Drained ahead of fresh deliveries on each tick so slow consumers don't
/// silently lose messages while credit refreshes catch up. Sized to admit
/// a MAX_PACKET-class MQTT publish envelope (4096-byte body + small
/// session-envelope overhead).
const PENDING_DLV_SLOTS: usize = 8;
const MAX_PENDING_DLV: usize = 4112;

/// Defer queue for `MSG_ACK_REGISTER` emissions that hit `out_forward`
/// backpressure during `MSG_PROPOSAL_ASSIGNED` processing. If the register
/// is lost, ack_tracker never learns about the inflight entry and the
/// publisher's PUBACK never fires. Each entry is the fixed-size 18-byte
/// register payload (see PROPOSAL_ASSIGNED handler).
const PENDING_ACK_SLOTS: usize = 64;

// MQTT packet types (mirrored in mqtt_codec)
const PKT_CONNECT: u8 = 1;
const PKT_CONNACK: u8 = 2;
const PKT_PUBLISH: u8 = 3;
const PKT_PUBACK: u8 = 4;
const PKT_PUBREC: u8 = 5;
const PKT_PUBREL: u8 = 6;
const PKT_PUBCOMP: u8 = 7;
const PKT_SUBSCRIBE: u8 = 8;
const PKT_SUBACK: u8 = 9;
const PKT_UNSUBSCRIBE: u8 = 10;
const PKT_UNSUBACK: u8 = 11;

/// Direction tag for an inflight slot. Publisher and subscriber packet_id
/// spaces are assigned independently (one by the remote client, one by us),
/// so the same numeric id can name two distinct logical entries. Tagging
/// each slot prevents a PUBACK on one direction from clearing the other.
const INFLIGHT_PUB: u8 = 0;
const INFLIGHT_SUB: u8 = 1;

#[repr(C)]
#[derive(Clone, Copy)]
struct Inflight {
    packet_id: u16,
    qos: u8,
    phase: u8,
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
    const fn zero() -> Self {
        Self {
            packet_id: 0, qos: 0, phase: 0, wal_index: 0,
            correlation_id: 0, direction: 0, active: 0,
        }
    }
}

/// A `Session` slot can be in three logical states:
///   - free      : `active == 0 && persisted == 0` — no client uses this slot.
///   - connected : `active == 1` — a client is online; conn_id is current.
///   - persisted : `active == 0 && persisted == 1` — client disconnected but
///                 clean_start was false, so subscriptions, inflight, and
///                 prefetch state must survive until either reconnect or
///                 explicit purge.
///
/// Per MQTT 3.1.1 §3.1.2.4, sessions with clean_start=false MUST persist
/// across reconnect; the existing slot is reactivated rather than allocated
/// fresh. The previous implementation dropped state on disconnect and made
/// offline_queue + subscription resume unreachable.
/// Per-session Will-message storage. Cap chosen to keep the per-slot
/// memory bounded while covering typical MQTT IoT payloads (status,
/// last-known-value strings). Larger Wills are rejected at propose
/// time so the apply-side slot never has to deal with a partial Will.
const MAX_WILL_TOPIC: usize = 256;
const MAX_WILL_PAYLOAD: usize = 512;

/// MQTT 5 User Property caps. The codec parses inbound publishes and
/// drops entries exceeding any cap so the QOP_PUBLISH V2 body — and
/// every downstream envelope — stays bounded. Mirrors the constants in
/// `mqtt_codec`.
const MAX_USER_PROPS_COUNT: usize = 4;
const MAX_USER_PROP_KEY_LEN: usize = 64;
const MAX_USER_PROP_VAL_LEN: usize = 128;
/// Worst-case bytes occupied by a serialised user_props block —
/// 1 count byte + N * (2 + max_key + 2 + max_val).
const MAX_USER_PROPS_BYTES: usize =
    1 + MAX_USER_PROPS_COUNT * (2 + MAX_USER_PROP_KEY_LEN + 2 + MAX_USER_PROP_VAL_LEN);

/// Parse a self-delimiting user_props block (matches the wire format
/// in `wire::QOP_PUBLISH` docstring and the codec envelope):
///   `[count:u8][per prop: key_len:u16 BE, key, val_len:u16 BE, val]`
/// Returns the number of bytes the block occupies in `buf`, or 0 if
/// the bytes are malformed. The first byte is treated as `count`; a
/// truncated entry or a body too short for the claimed sizes is
/// reported as malformed via a `None`-equivalent return.
fn user_props_block_len(buf: &[u8]) -> Option<usize> {
    if buf.is_empty() { return None; }
    let count = buf[0] as usize;
    let mut off = 1usize;
    for _ in 0..count {
        if off + 2 > buf.len() { return None; }
        let klen = u16::from_be_bytes([buf[off], buf[off + 1]]) as usize;
        off += 2 + klen;
        if off + 2 > buf.len() { return None; }
        let vlen = u16::from_be_bytes([buf[off], buf[off + 1]]) as usize;
        off += 2 + vlen;
        if off > buf.len() { return None; }
    }
    Some(off)
}

#[repr(C)]
#[derive(Clone, Copy)]
struct Session {
    tenant: TenantId,
    stream_hash: StreamHash,
    session_epoch: SessionEpoch,
    protocol: u8,
    conn_id: u8,
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
    /// docs/apply_side_state_machine.md §Phase 2 (CONNECT specifically).
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
    ///   * `0`           — expire immediately on disconnect (no persisted
    ///                      slot survives the disconnect, even with
    ///                      clean_start=false).
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
    /// `0` means no pending Will fire.
    pending_will_fire_at_ms: u64,
}

impl Session {
    const fn zero() -> Self {
        Self {
            tenant: 0, stream_hash: 0, session_epoch: 0,
            protocol: PROTO_UNKNOWN, conn_id: 0, active: 0,
            persisted: 0, clean_start: 0, transient: 0,
            last_activity_ms: 0, keep_alive_ms: 0, next_msg_id: 1,
            inflight: [Inflight::zero(); MAX_INFLIGHT_PER_SESSION],
            will_present: 0, will_qos: 0, will_retain: 0, will_delay_ms: 0,
            will_topic_len: 0, will_topic: [0; MAX_WILL_TOPIC],
            will_payload_len: 0, will_payload: [0; MAX_WILL_PAYLOAD],
            session_expiry_s: 0, disconnected_at_ms: 0,
            receive_maximum: 0,
            pending_will_fire_at_ms: 0,
        }
    }

    fn allocate_inflight(
        &mut self, packet_id: u16, qos: u8, direction: u8,
    ) -> Option<usize> {
        for i in 0..MAX_INFLIGHT_PER_SESSION {
            if self.inflight[i].active == 0 {
                self.inflight[i] = Inflight {
                    packet_id, qos,
                    phase: if qos == 2 { QOS2_PUBLISH } else { 0 },
                    wal_index: 0, correlation_id: 0,
                    direction, active: 1,
                };
                return Some(i);
            }
        }
        None
    }

    fn find_inflight_dir(&self, packet_id: u16, direction: u8) -> Option<usize> {
        for i in 0..MAX_INFLIGHT_PER_SESSION {
            let e = &self.inflight[i];
            if e.active == 1 && e.packet_id == packet_id && e.direction == direction {
                return Some(i);
            }
        }
        None
    }
}

/// Operation identifier for tagged Raft proposals, so ack_tracker's
/// `MSG_ACK_EMIT` can route to the right MQTT response (PUBACK / PUBREC /
/// PUBCOMP) per the QoS state machine.
const OP_PUBLISH: u8 = 0;
const OP_PUBREL: u8 = 1;

/// Pending correlation: a tagged Raft proposal waiting for the
/// `proposal_assigned` round-trip so we can bind correlation_id →
/// (session_slot, packet_id, op). After the binding we register with
/// ack_tracker and free the slot.
#[repr(C)]
#[derive(Clone, Copy)]
struct PendingCorrelation {
    correlation_id: u64,
    session_slot: u32,
    packet_id: u16,
    op: u8,
    active: u8,
}

impl PendingCorrelation {
    const fn zero() -> Self {
        Self { correlation_id: 0, session_slot: 0, packet_id: 0, op: 0, active: 0 }
    }
}

#[repr(C)]
struct ModuleState {
    syscalls: *const SyscallTable,
    in_codec: i32, in_committed: i32, in_flow: i32, in_ack: i32,
    in_cp: i32, in_messaging: i32, in_deliver: i32, in_assigned: i32,
    out_proposals: i32, out_codec: i32, out_topic: i32,
    out_messaging: i32, out_forward: i32, out_audit: i32, out_metrics: i32,
    out_proposals_tagged: i32,

    credit_base_window: u32,
    keep_alive_factor_pct: u32,
    max_inflight_default: u16,
    pid_entry_credits: i32,
    pid_byte_credits: i32,
    follower_factor_q16: i32,

    sessions: [Session; MAX_SESSIONS],
    /// Per-subscriber prefetch credit set by prefetch_controller.
    /// Populated from MSG_PREFETCH_CREDIT on flow_in; 0 = unset / no cap.
    /// Enforced on the MSG_TOPIC_DELIVER push path.
    prefetch_credit: [u32; MAX_SESSIONS],
    /// QoS 1+ deliveries pushed to each subscriber but not yet acked by
    /// that subscriber. Incremented on successful delivery; decremented
    /// on inbound PUBACK. Drives the subscriber-backlog MSG_LAG_SIGNAL
    /// feedback loop without resetting on every credit refresh.
    sub_outstanding: [u32; MAX_SESSIONS],
    pending: [PendingCorrelation; MAX_PENDING_CORRELATIONS],
    next_correlation_id: u64,

    // ── Stash for commit-gated QoS 1+ topic fan-out ──
    //
    // Each slot pairs a tagged Raft correlation_id with the
    // MSG_TOPIC_PUBLISH envelope we'll emit only once durability and
    // dedup both resolve. A slot is free when stash_correlation == 0.
    stash_correlation: [u64; STASH_SLOTS],
    stash_dedup_key: [[u8; 20]; STASH_SLOTS],
    stash_dedup_state: [u8; STASH_SLOTS],
    stash_durable: [u8; STASH_SLOTS],
    stash_env_len: [u16; STASH_SLOTS],
    stash_env: [[u8; MAX_STASH_ENV]; STASH_SLOTS],

    // ── Defer queue for backpressured deliveries ──
    //
    // When a MSG_TOPIC_DELIVER would be dropped (prefetch, PID, or
    // inflight exhaustion), the raw envelope is parked here and drained
    // ahead of fresh deliveries on subsequent ticks.
    pending_dlv_active: [u8; PENDING_DLV_SLOTS],
    pending_dlv_len: [u16; PENDING_DLV_SLOTS],
    pending_dlv_env: [[u8; MAX_PENDING_DLV]; PENDING_DLV_SLOTS],

    // ── Defer queue for failed ACK_REGISTER emissions ──
    pending_ack_active: [u8; PENDING_ACK_SLOTS],
    pending_ack_buf: [[u8; 18]; PENDING_ACK_SLOTS],

    session_count: u32,
    proposals_emitted: u32,
    connects: u32,
    disconnects: u32,
    publishes: u32,
    applied: u32,
    acks_emitted: u32,
    qos2_rec: u32,
    qos2_rel: u32,
    qos2_comp: u32,
    correlations_dropped: u32,
    publishes_throttled: u32,
    deliveries_throttled: u32,
    redeliver_signals: u32,
    /// Count of per-entry MSG_COMMITTED_ENTRY notices observed on
    /// `in_entries`. The apply-side state machine is staged across
    /// future turns; this surfaces the substrate wiring so reviewers can
    /// confirm the stream is actually flowing.
    committed_entries_observed: u32,
    /// Highest WAL index applied via `apply_committed_op`. Used to
    /// detect forward-jumps in the `MSG_COMMITTED_ENTRY` index
    /// sequence (snapshot install / leader log truncation) — see
    /// `docs/apply_side_state_machine.md` §Phase 4. Reset to the
    /// new index after `apply_pipeline_reset` clears state.
    apply_index: u64,
    /// Count of apply-side resets observed since boot. Surfaced in
    /// metrics so operators can correlate reset events with
    /// snapshot installs / leader transfers.
    apply_resets: u32,
    last_metrics_ms: u64,

    in_buf: [u8; BUF_SIZE],
    out_buf: [u8; BUF_SIZE],
}

impl ModuleState {
    fn find_session_by_conn(&self, conn_id: u8) -> Option<usize> {
        for i in 0..MAX_SESSIONS {
            let s = &self.sessions[i];
            // Match active OR transient slots: between CONNECT receipt and
            // the QOP_CONNECT commit, the slot is transient and the
            // follow-up packets from the same conn_id must still route to
            // it for admission control.
            if (s.active == 1 || s.transient == 1) && s.conn_id == conn_id {
                return Some(i);
            }
        }
        None
    }

    /// Find any session matching `(tenant, stream_hash)`, including
    /// `persisted` (disconnected, clean_start=false) and `transient`
    /// (CONNECT received, apply pending) slots. Used by reconnect to
    /// resurrect the prior session per MQTT 3.1.1 §3.1.2.4 and by
    /// apply-side QOP_CONNECT to reconcile a transient record with its
    /// durable counterpart.
    fn find_session(&self, tenant: TenantId, stream_hash: StreamHash) -> Option<usize> {
        for i in 0..MAX_SESSIONS {
            let s = &self.sessions[i];
            if (s.active == 1 || s.persisted == 1 || s.transient == 1)
                && s.tenant == tenant
                && s.stream_hash == stream_hash
            {
                return Some(i);
            }
        }
        None
    }

    /// Allocate a free slot — skips active, persisted, and transient sessions.
    fn allocate_session(&self) -> Option<usize> {
        for i in 0..MAX_SESSIONS {
            let s = &self.sessions[i];
            if s.active == 0 && s.persisted == 0 && s.transient == 0 { return Some(i); }
        }
        None
    }

    fn allocate_correlation(
        &mut self, session_slot: u32, packet_id: u16, op: u8,
    ) -> Option<u64> {
        for i in 0..MAX_PENDING_CORRELATIONS {
            if self.pending[i].active == 0 {
                self.next_correlation_id = self.next_correlation_id.wrapping_add(1);
                if self.next_correlation_id == 0 { self.next_correlation_id = 1; }
                let cid = self.next_correlation_id;
                self.pending[i] = PendingCorrelation {
                    correlation_id: cid, session_slot, packet_id, op, active: 1,
                };
                return Some(cid);
            }
        }
        None
    }

    fn take_correlation(&mut self, cid: u64) -> Option<(u32, u16, u8)> {
        for i in 0..MAX_PENDING_CORRELATIONS {
            if self.pending[i].active == 1 && self.pending[i].correlation_id == cid {
                let r = (
                    self.pending[i].session_slot,
                    self.pending[i].packet_id,
                    self.pending[i].op,
                );
                self.pending[i] = PendingCorrelation::zero();
                return Some(r);
            }
        }
        None
    }

    /// Reserve a stash slot for a commit-gated topic-publish envelope.
    /// Returns the slot index or None when the stash is full (caller must
    /// then drop the publish, since the commit-gating contract requires
    /// the envelope to survive until both dedup and durability resolve).
    fn allocate_stash(&mut self, correlation_id: u64, dedup_key: &[u8]) -> Option<usize> {
        for i in 0..STASH_SLOTS {
            if self.stash_correlation[i] == 0 {
                self.stash_correlation[i] = correlation_id;
                let k = dedup_key.len().min(20);
                self.stash_dedup_key[i][..k].copy_from_slice(&dedup_key[..k]);
                if k < 20 { self.stash_dedup_key[i][k..].fill(0); }
                self.stash_dedup_state[i] = STASH_DEDUP_PENDING;
                self.stash_durable[i] = 0;
                self.stash_env_len[i] = 0;
                return Some(i);
            }
        }
        None
    }

    fn find_stash_by_correlation(&self, cid: u64) -> Option<usize> {
        for i in 0..STASH_SLOTS {
            if self.stash_correlation[i] == cid { return Some(i); }
        }
        None
    }

    fn find_stash_by_dedup_key(&self, key: &[u8]) -> Option<usize> {
        if key.len() < 20 { return None; }
        for i in 0..STASH_SLOTS {
            if self.stash_correlation[i] != 0 && &self.stash_dedup_key[i][..] == &key[..20] {
                return Some(i);
            }
        }
        None
    }

    fn release_stash(&mut self, slot: usize) {
        self.stash_correlation[slot] = 0;
        self.stash_dedup_state[slot] = STASH_DEDUP_PENDING;
        self.stash_durable[slot] = 0;
        self.stash_env_len[slot] = 0;
    }

    fn allocate_pending_delivery(&mut self) -> Option<usize> {
        for i in 0..PENDING_DLV_SLOTS {
            if self.pending_dlv_active[i] == 0 { return Some(i); }
        }
        None
    }
}

#[no_mangle]
#[link_section = ".text.module_state_size"]
pub extern "C" fn module_state_size() -> u32 { core::mem::size_of::<ModuleState>() as u32 }

#[no_mangle]
#[link_section = ".text.module_init"]
pub extern "C" fn module_init(_syscalls: *const c_void) {}

#[no_mangle]
#[link_section = ".text.module_new"]
pub extern "C" fn module_new(
    in_chan: i32, out_chan: i32, _ctrl_chan: i32,
    _params: *const u8, _params_len: usize,
    state: *mut u8, state_size: usize, syscalls: *const c_void,
) -> i32 {
    unsafe {
        if syscalls.is_null() || state.is_null() { return -1; }
        if state_size < core::mem::size_of::<ModuleState>() { return -2; }
        let s = &mut *(state as *mut ModuleState);
        let sys = &*(syscalls as *const SyscallTable);
        s.syscalls = sys;
        s.in_codec = in_chan;
        s.out_proposals = out_chan;
        s.in_committed = dev_channel_port(sys, 0, 1);
        s.in_flow = dev_channel_port(sys, 0, 2);
        s.in_ack = dev_channel_port(sys, 0, 3);
        s.in_cp = dev_channel_port(sys, 0, 4);
        s.in_messaging = dev_channel_port(sys, 0, 5);
        s.in_deliver = dev_channel_port(sys, 0, 6);
        s.in_assigned = dev_channel_port(sys, 0, 7);
        s.out_codec = dev_channel_port(sys, 1, 1);
        s.out_topic = dev_channel_port(sys, 1, 2);
        s.out_messaging = dev_channel_port(sys, 1, 3);
        s.out_forward = dev_channel_port(sys, 1, 4);
        s.out_proposals_tagged = dev_channel_port(sys, 1, 7);
        s.out_audit = dev_channel_port(sys, 1, 5);
        s.out_metrics = dev_channel_port(sys, 1, 6);
        s.credit_base_window = 10;
        s.keep_alive_factor_pct = 150;
        s.max_inflight_default = 10;
        s.pid_entry_credits = 4096;
        s.pid_byte_credits = 64 * 1024;
        s.follower_factor_q16 = 65536;
        s.next_correlation_id = 0;
        s.apply_index = 0;
        s.apply_resets = 0;
        for i in 0..MAX_SESSIONS {
            s.sessions[i] = Session::zero();
            s.prefetch_credit[i] = 0;
            s.sub_outstanding[i] = 0;
        }
        for i in 0..MAX_PENDING_CORRELATIONS { s.pending[i] = PendingCorrelation::zero(); }
        for i in 0..STASH_SLOTS {
            s.stash_correlation[i] = 0;
            s.stash_dedup_key[i] = [0u8; 20];
            s.stash_dedup_state[i] = STASH_DEDUP_PENDING;
            s.stash_durable[i] = 0;
            s.stash_env_len[i] = 0;
        }
        for i in 0..PENDING_DLV_SLOTS {
            s.pending_dlv_active[i] = 0;
            s.pending_dlv_len[i] = 0;
        }
        for i in 0..PENDING_ACK_SLOTS {
            s.pending_ack_active[i] = 0;
        }
        dev_log(sys, 3, b"[sess] init v6".as_ptr(), 14);
        0
    }
}

/// Emit a `MSG_SESSION_RESPONSE` to out_codec via wire-envelope framing.
///
/// Channel payload (post-envelope-strip) is `[conn_id][proto][pkt_type]
/// [flags][body]`. Envelope framing is required because the codec read
/// loop drains the channel multiple messages per tick — without per-
/// message length prefixes, back-to-back writes coalesce on the byte
/// FIFO and the consumer mis-parses everything after the first.
///
/// # Safety
unsafe fn emit_codec_response(
    sys: &SyscallTable, chan: i32,
    conn_id: u8, proto: u8, pkt_type: u8, flags: u8, body: &[u8],
) -> bool {
    if chan < 0 { return false; }
    let total = 1 + 3 + body.len();
    if total > BUF_SIZE { return false; }
    let mut out = [0u8; BUF_SIZE];
    out[0] = conn_id;
    out[1] = proto;
    out[2] = pkt_type;
    out[3] = flags;
    out[4..4 + body.len()].copy_from_slice(body);
    let w = wire::channel_write_msg(sys, chan, wire::MSG_SESSION_RESPONSE, &out[..total]);
    w > 0
}

/// Wire-envelope emit (fluxor `[mtype][len][payload]` format) for non-codec
/// outputs like raft_engine, topic_engine, dedup_engine, etc. Returns
/// `true` iff the full envelope was written; returns `false` on
/// `CHAN_EAGAIN`, on an oversize payload that exceeds
/// `CHANNEL_BUFFER_SIZE`, or on any other write failure surfaced by
/// `channel_write_msg`. Callers must propagate the failure (retry next
/// tick, log, etc.) rather than treating emission as always-successful
/// — see the buffer-capacity discussion in `modules/common/wire.rs`.
///
/// # Safety
unsafe fn try_emit(sys: &SyscallTable, chan: i32, msg_type: u8, payload: &[u8]) -> bool {
    if chan < 0 { return false; }
    // channel_write_msg writes the envelope atomically and returns the
    // total written count, or a negative value (CHAN_EAGAIN, oversize,
    // EINVAL, ...) on failure. Anything ≤ 0 means the payload did not
    // reach the channel; the caller decides what to do.
    let total = (wire::ENVELOPE_HDR + payload.len()) as i32;
    let w = wire::channel_write_msg(sys, chan, msg_type, payload);
    w == total
}

/// Resolve a stash slot. Returns `true` when the slot is now free; `false`
/// if the topic emit failed under backpressure and the slot must be
/// retried on a subsequent tick. The retry scan in `module_step` Phase 5
/// drives those re-attempts.
///
/// If dedup said duplicate, the topic envelope is dropped (the publisher's
/// PUBACK still goes out — MQTT 3.1.1 §3.3.1.3 makes deduplication a
/// broker concern; clients always see ack).
///
/// # Safety
unsafe fn finalise_stash(s: &mut ModuleState, sys: &SyscallTable, stash_idx: usize) -> bool {
    let env_len = s.stash_env_len[stash_idx] as usize;
    match s.stash_dedup_state[stash_idx] {
        STASH_DEDUP_OK if env_len >= 18 => {
            // Stash holds the QOP_PUBLISH V2 op-body:
            //   [pub_qos:u8][packet_id:u16 BE][stream_hash:u64 LE]
            //   [session_epoch:u32 LE][retain:u8][topic_len:u16 BE]
            //   [topic][user_props_count:u8][per prop ...][payload]
            // Build the MSG_TOPIC_PUBLISH envelope topic_engine expects:
            //   [tenant:u32 LE][qos:u8][_pad:u8][topic_len:u16 LE][topic]
            //   [user_props_count:u8][per prop ...][payload]
            // tenant is hardcoded 0 for now (single-tenant first ship); when
            // multi-tenancy lands the tenant will be stashed alongside.
            let pub_qos = s.stash_env[stash_idx][0];
            let topic_len = u16::from_be_bytes([
                s.stash_env[stash_idx][16],
                s.stash_env[stash_idx][17],
            ]) as usize;
            if 18 + topic_len + 1 > env_len {
                s.release_stash(stash_idx);
                return true;
            }
            let up_off = 18 + topic_len;
            let stash_slice = &s.stash_env[stash_idx][..env_len];
            let Some(up_len) = user_props_block_len(&stash_slice[up_off..env_len]) else {
                s.release_stash(stash_idx);
                return true;
            };
            let payload_off = up_off + up_len;
            if payload_off > env_len {
                s.release_stash(stash_idx);
                return true;
            }
            let payload_len = env_len - payload_off;
            let topic_pub_len = 4 + 1 + 1 + 2 + topic_len + up_len + payload_len;
            if topic_pub_len > s.out_buf.len() {
                s.release_stash(stash_idx);
                return true;
            }
            let tenant: TenantId = 0;
            s.out_buf[0..4].copy_from_slice(&tenant.to_le_bytes());
            s.out_buf[4] = pub_qos;
            s.out_buf[5] = 0;
            s.out_buf[6..8].copy_from_slice(&(topic_len as u16).to_le_bytes());
            // topic + user_props + payload sit contiguous in the stash
            // starting at offset 18; a single copy reproduces the
            // downstream layout (offset 8 of out_buf).
            let src = s.stash_env[stash_idx].as_ptr().add(18);
            let dst = s.out_buf.as_mut_ptr().add(8);
            core::ptr::copy_nonoverlapping(src, dst, topic_len + up_len + payload_len);
            if try_emit(
                sys, s.out_topic, wire::MSG_TOPIC_PUBLISH,
                &s.out_buf[..topic_pub_len],
            ) {
                s.release_stash(stash_idx);
                true
            } else {
                // topic_engine.op_in saturated. Keep parked; a later tick
                // retries via finalise_durable_stashes().
                false
            }
        }
        STASH_DEDUP_DUPLICATE => {
            s.release_stash(stash_idx);
            true
        }
        _ => {
            // Pending state shouldn't normally reach finalise. Treat as
            // released to avoid leaking the slot.
            s.release_stash(stash_idx);
            true
        }
    }
}

/// Periodic retry sweep for stash slots whose topic emit was deferred by
/// channel backpressure. Called once per tick (Phase 5-pre).
///
/// # Safety
unsafe fn finalise_durable_stashes(s: &mut ModuleState, sys: &SyscallTable) {
    for i in 0..STASH_SLOTS {
        if s.stash_correlation[i] == 0 { continue; }
        if s.stash_durable[i] == 0 { continue; }
        if s.stash_dedup_state[i] == STASH_DEDUP_PENDING { continue; }
        finalise_stash(s, sys, i);
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum DeliverResult {
    Delivered,
    /// Backpressure (prefetch cap, PID exhaustion, inflight-slot exhaustion,
    /// or a transient codec EAGAIN). The caller should park the envelope
    /// on the defer queue and retry next tick.
    Backpressured,
    /// Malformed or session-stale — cannot ever succeed; caller drops.
    Dropped,
}

/// Push a MSG_TOPIC_DELIVER envelope into offline_queue for replay on the
/// subscriber's next reconnect. Body shape: `[session:u32][env_len:u16][env]`.
/// On out_messaging backpressure the enqueue is dropped — offline_queue
/// metrics will reflect the loss, and the publisher's durability ack still
/// fired so the system contract holds at the publisher edge.
///
/// # Safety
unsafe fn enqueue_offline(
    s: &mut ModuleState, sys: &SyscallTable, session_slot: u32, env: &[u8],
) {
    let env_len = env.len();
    let total = 6 + env_len;
    if total > s.out_buf.len() { return; }
    s.out_buf[0..4].copy_from_slice(&session_slot.to_le_bytes());
    s.out_buf[4..6].copy_from_slice(&(env_len as u16).to_le_bytes());
    let dst = s.out_buf.as_mut_ptr().add(6);
    let src = env.as_ptr();
    core::ptr::copy_nonoverlapping(src, dst, env_len);
    try_emit(sys, s.out_messaging, wire::MSG_OFFLINE_ENQUEUE, &s.out_buf[..total]);
}

/// Attempt one delivery of a single MSG_TOPIC_DELIVER envelope. Used both
/// for fresh inbound deliveries and for the defer-queue drain so the
/// backpressure path stays consistent.
///
/// # Safety
unsafe fn try_deliver(
    s: &mut ModuleState, sys: &SyscallTable, env: &[u8],
) -> DeliverResult {
    let plen = env.len();
    if plen < 14 { return DeliverResult::Dropped; }

    let session_slot = u32::from_le_bytes([env[0], env[1], env[2], env[3]]) as usize;
    if session_slot >= MAX_SESSIONS { return DeliverResult::Dropped; }

    // Persisted session (clean_start=false, currently offline) → enqueue
    // for replay on reconnect. Treated as Delivered from the caller's
    // perspective: the message is now offline_queue's responsibility.
    if s.sessions[session_slot].active != 1 {
        if s.sessions[session_slot].persisted == 1 {
            enqueue_offline(s, sys, session_slot as u32, env);
            return DeliverResult::Delivered;
        }
        return DeliverResult::Dropped;
    }
    let conn_id = s.sessions[session_slot].conn_id;
    let sub_qos = env[4] & 0x03;

    let topic_len = u16::from_le_bytes([env[12], env[13]]) as usize;
    if 14 + topic_len > plen { return DeliverResult::Dropped; }
    // user_props block sits immediately after the topic (item 6 wire
    // contract — topic_engine forwards bytes between topic and payload
    // untouched). Apply-side always writes a single zero count byte
    // when there are no properties, so we expect at least one byte.
    let up_off_env = 14 + topic_len;
    let up_len = if up_off_env >= plen {
        0
    } else {
        user_props_block_len(&env[up_off_env..plen]).unwrap_or(0)
    };
    let payload_start = up_off_env + up_len;
    if payload_start > plen { return DeliverResult::Dropped; }
    let payload_len = plen - payload_start;

    let pid_bytes = if sub_qos > 0 { 2 } else { 0 };
    let up_bytes = if up_len > 0 { up_len } else { 1 }; // placeholder for empty block
    let body_len = 2 + topic_len + pid_bytes + up_bytes + payload_len;
    if body_len > s.out_buf.len() { return DeliverResult::Dropped; }
    let body_cost = body_len as i32;

    if sub_qos > 0 {
        let credit = s.prefetch_credit[session_slot];
        if credit > 0 && s.sub_outstanding[session_slot] >= credit {
            return DeliverResult::Backpressured;
        }
        // MQTT 5 §3.3.4 ReceiveMaximum — the client's hard cap on
        // concurrent unacked QoS 1+2 publishes. Separate from
        // prefetch_credit (which is operator-set / dynamic); the
        // effective cap is the tighter of the two when both are > 0.
        // The pending_dlv park path handles the retry on PUBACK.
        let rxmax = s.sessions[session_slot].receive_maximum as u32;
        if rxmax > 0 && s.sub_outstanding[session_slot] >= rxmax {
            return DeliverResult::Backpressured;
        }
    }
    if s.pid_entry_credits <= 0 || s.pid_byte_credits < body_cost {
        return DeliverResult::Backpressured;
    }

    let mut sub_packet_id: u16 = 0;
    if sub_qos > 0 {
        let id_full = s.sessions[session_slot].next_msg_id;
        s.sessions[session_slot].next_msg_id = id_full.wrapping_add(1);
        sub_packet_id = if (id_full as u16) == 0 { 1 } else { id_full as u16 };
        if s.sessions[session_slot]
            .allocate_inflight(sub_packet_id, sub_qos, INFLIGHT_SUB)
            .is_none()
        {
            return DeliverResult::Backpressured;
        }
    }

    // emit_codec_response payload shape (item 6 — PUBLISH only):
    //   [topic_len:u16 BE][topic][packet_id:u16 BE if qos>0]
    //   [user_props_count:u8][per prop ...][payload]
    // The codec encoder either drops the user_props block (MQTT
    // 3.1.1 subscriber) or emits it as an MQTT 5 PUBLISH properties
    // block. `up_len == 0` (V1 replay path) is treated downstream as
    // an empty block.
    s.out_buf[0..2].copy_from_slice(&(topic_len as u16).to_be_bytes());
    core::ptr::copy_nonoverlapping(
        env.as_ptr().add(14),
        s.out_buf.as_mut_ptr().add(2),
        topic_len,
    );
    let mut cursor = 2 + topic_len;
    if sub_qos > 0 {
        s.out_buf[cursor..cursor + 2].copy_from_slice(&sub_packet_id.to_be_bytes());
        cursor += 2;
    }
    if up_len > 0 {
        core::ptr::copy_nonoverlapping(
            env.as_ptr().add(up_off_env),
            s.out_buf.as_mut_ptr().add(cursor),
            up_len,
        );
        cursor += up_len;
    } else {
        s.out_buf[cursor] = 0; // empty user_props block
        cursor += 1;
    }
    core::ptr::copy_nonoverlapping(
        env.as_ptr().add(payload_start),
        s.out_buf.as_mut_ptr().add(cursor),
        payload_len,
    );

    let flags = sub_qos << 1;
    let emitted = emit_codec_response(
        sys, s.out_codec, conn_id, PROTO_MQTT, PKT_PUBLISH, flags,
        &s.out_buf[..body_len],
    );
    if emitted {
        s.pid_entry_credits = s.pid_entry_credits.saturating_sub(1);
        s.pid_byte_credits = s.pid_byte_credits.saturating_sub(body_cost);
        if sub_qos > 0 {
            s.sub_outstanding[session_slot] =
                s.sub_outstanding[session_slot].wrapping_add(1);
            let mut lag_msg = [0u8; 8];
            lag_msg[0..4].copy_from_slice(&(session_slot as u32).to_le_bytes());
            lag_msg[4..8].copy_from_slice(&s.sub_outstanding[session_slot].to_le_bytes());
            try_emit(sys, s.out_forward, wire::MSG_LAG_SIGNAL, &lag_msg);
        }
        DeliverResult::Delivered
    } else {
        if sub_qos > 0 {
            if let Some(ii) = s.sessions[session_slot]
                .find_inflight_dir(sub_packet_id, INFLIGHT_SUB)
            {
                s.sessions[session_slot].inflight[ii].active = 0;
            }
        }
        DeliverResult::Backpressured
    }
}

/// Apply-pipeline reset (Phase 4 of `docs/apply_side_state_machine.md`).
/// Wipes every apply-derived arena in `session_processor`, fast-forwards
/// `apply_index` to `reset_index`, and emits `MSG_APPLY_RESET_FANOUT` on
/// each downstream bus so topic_engine, dedup_engine, retained_store, and
/// offline_queue clear their own state. Until Phase 5 snapshots ship,
/// state is rebuilt purely from `MSG_COMMITTED_ENTRY` events at or above
/// `reset_index`.
///
/// # Safety
/// Caller must hold an exclusive `&mut ModuleState` and supply a valid
/// `&SyscallTable`.
unsafe fn apply_pipeline_reset(s: &mut ModuleState, sys: &SyscallTable, reset_index: u64) {
    s.apply_resets = s.apply_resets.wrapping_add(1);

    // Clear apply-derived session state. Propose-side transient slots
    // (admission state held between CONNECT receipt and QOP_CONNECT
    // apply) are also wiped — after a reset, the leader-local view
    // matches the substrate's view, which means any pending CONNECT
    // is lost. Clients reconnect; that's the documented failover
    // semantic.
    for i in 0..MAX_SESSIONS {
        s.sessions[i] = Session::zero();
        s.prefetch_credit[i] = 0;
        s.sub_outstanding[i] = 0;
    }
    s.session_count = 0;
    for i in 0..MAX_PENDING_CORRELATIONS {
        s.pending[i] = PendingCorrelation::zero();
    }
    for i in 0..STASH_SLOTS {
        s.stash_correlation[i] = 0;
        s.stash_dedup_key[i] = [0u8; 20];
        s.stash_dedup_state[i] = STASH_DEDUP_PENDING;
        s.stash_durable[i] = 0;
        s.stash_env_len[i] = 0;
    }
    for i in 0..PENDING_DLV_SLOTS {
        s.pending_dlv_active[i] = 0;
        s.pending_dlv_len[i] = 0;
    }
    for i in 0..PENDING_ACK_SLOTS {
        s.pending_ack_active[i] = 0;
    }
    s.apply_index = reset_index;

    // Fan the reset out to downstream modules. `out_topic` covers
    // topic_engine; `out_messaging` covers dedup_engine, retained_store,
    // and offline_queue (they share the messaging bus, demuxing by
    // msg_type). Each downstream handler is "wipe everything" until
    // Phase 5 snapshots land.
    let body = reset_index.to_le_bytes();
    try_emit(sys, s.out_topic, wire::MSG_APPLY_RESET_FANOUT, &body);
    try_emit(sys, s.out_messaging, wire::MSG_APPLY_RESET_FANOUT, &body);
}

// ── Apply-side dispatcher (Phase 3 of docs/apply_side_state_machine.md) ─────
//
// Routes peeled canonical Quantum proposals to per-op handlers. Each
// handler owns the durable mutations and downstream emissions for its
// op; this dispatcher exists only to demultiplex. Until every op is
// converted, only those listed in the match arm have apply-side
// handlers; the rest are silently skipped because their propose-side
// path still mutates state inline. See `docs/apply_side_state_machine.md`
// §"Phasing summary" for the migration order.

/// # Safety
/// Caller must hold an exclusive `&mut ModuleState`, supply a valid
/// `&SyscallTable`, and guarantee `body_start <= body_end <= s.in_buf.len()`.
unsafe fn apply_committed_op(
    s: &mut ModuleState,
    sys: &SyscallTable,
    p: &wire::PeeledQProp,
    body_start: usize,
    body_end: usize,
    now: u64,
) {
    match p.op {
        wire::QOP_CONNECT => apply_qop_connect(s, sys, p.tenant, body_start, body_end, now),
        wire::QOP_DISCONNECT => apply_qop_disconnect(s, sys, p.tenant, body_start, body_end, now),
        wire::QOP_SUBSCRIBE => apply_qop_subscribe(s, sys, p.tenant, body_start, body_end),
        wire::QOP_UNSUBSCRIBE => apply_qop_unsubscribe(s, sys, p.tenant, body_start, body_end),
        wire::QOP_PUBREL => apply_qop_pubrel(s, p.tenant, body_start, body_end),
        wire::QOP_PUBLISH => apply_qop_publish(s, sys, p.tenant, body_start, body_end, p.version),
        // QOP_RETAINED_CLEAR has no propose-side emitter yet.
        _ => {}
    }
}

/// Apply-side handler for QOP_CONNECT. Promotes a transient slot
/// (leader path) or builds a fresh durable record (follower / replay
/// path), bumps `session_epoch`, and fans out the
/// `MSG_SESSION_DROP` / `MSG_OFFLINE_RECONNECT` notices to downstream
/// modules so every node observes the same downstream effects the
/// leader's propose-side used to emit inline.
///
/// # Safety
/// Caller must hold an exclusive `&mut ModuleState`, supply a valid
/// `&SyscallTable`, and guarantee `body_start <= body_end <= s.in_buf.len()`.
unsafe fn apply_qop_connect(
    s: &mut ModuleState,
    sys: &SyscallTable,
    tenant: TenantId,
    body_start: usize,
    body_end: usize,
    _now: u64,
) {
    if body_end < body_start { return; }
    let body_len = body_end - body_start;
    // Minimum: clean_start(1) + keep_alive(2) + stream_hash(8) + cid_len(2) + protocol(1) = 14
    if body_len < 14 { return; }
    let clean_start = s.in_buf[body_start] != 0;
    let keep_alive_s = u16::from_be_bytes([
        s.in_buf[body_start + 1],
        s.in_buf[body_start + 2],
    ]) as u32;
    let stream_hash = u64::from_le_bytes([
        s.in_buf[body_start + 3],  s.in_buf[body_start + 4],
        s.in_buf[body_start + 5],  s.in_buf[body_start + 6],
        s.in_buf[body_start + 7],  s.in_buf[body_start + 8],
        s.in_buf[body_start + 9],  s.in_buf[body_start + 10],
    ]);
    let cid_len = u16::from_be_bytes([
        s.in_buf[body_start + 11],
        s.in_buf[body_start + 12],
    ]) as usize;
    if body_len < 14 + cid_len { return; }
    let protocol = s.in_buf[body_start + 13 + cid_len];

    // Will section (additive trailer; older proposals omit the
    // will_flag byte entirely and we treat that as `will_present == 0`).
    let will_flag_off = body_start + 14 + cid_len;
    let mut will_present = false;
    let mut will_qos: u8 = 0;
    let mut will_retain: u8 = 0;
    let mut will_delay_s: u32 = 0;
    let mut will_topic_off = 0usize;
    let mut will_topic_len = 0usize;
    let mut will_payload_off = 0usize;
    let mut will_payload_len = 0usize;
    // Tail-of-Will offset (== first byte after the Will section, or
    // immediately after `will_flag` when no Will is present). The
    // session_expiry trailer reads from here.
    let mut post_will_off = will_flag_off + 1;
    if body_end > will_flag_off && s.in_buf[will_flag_off] != 0 {
        // 1 (qos) + 1 (retain) + 4 (delay_s) + 2 (topic_len) = 8, then topic
        // bytes + 2 (payload_len) + payload bytes.
        let q_off = will_flag_off + 1;
        if q_off + 8 <= body_end {
            will_qos = s.in_buf[q_off];
            will_retain = s.in_buf[q_off + 1];
            will_delay_s = u32::from_le_bytes([
                s.in_buf[q_off + 2], s.in_buf[q_off + 3],
                s.in_buf[q_off + 4], s.in_buf[q_off + 5],
            ]);
            let wt_len_off = q_off + 6;
            will_topic_len = u16::from_be_bytes([
                s.in_buf[wt_len_off], s.in_buf[wt_len_off + 1],
            ]) as usize;
            will_topic_off = wt_len_off + 2;
            let wp_len_off = will_topic_off + will_topic_len;
            if wp_len_off + 2 <= body_end
                && will_topic_len <= MAX_WILL_TOPIC
            {
                will_payload_len = u16::from_be_bytes([
                    s.in_buf[wp_len_off], s.in_buf[wp_len_off + 1],
                ]) as usize;
                will_payload_off = wp_len_off + 2;
                if will_payload_off + will_payload_len <= body_end
                    && will_payload_len <= MAX_WILL_PAYLOAD
                {
                    will_present = true;
                    post_will_off = will_payload_off + will_payload_len;
                }
            }
        }
    }

    // SessionExpiryInterval trailer (additive; older proposals stop
    // before it and the field defaults to 0 → "expire on disconnect").
    let session_expiry_s = if post_will_off + 4 <= body_end {
        u32::from_le_bytes([
            s.in_buf[post_will_off],
            s.in_buf[post_will_off + 1],
            s.in_buf[post_will_off + 2],
            s.in_buf[post_will_off + 3],
        ])
    } else { 0 };

    // ReceiveMaximum trailer (additive after session_expiry_s; older
    // proposals stop here and the field defaults to 0 → "no cap").
    let post_expiry_off = post_will_off + 4;
    let receive_maximum = if post_expiry_off + 2 <= body_end {
        u16::from_le_bytes([
            s.in_buf[post_expiry_off],
            s.in_buf[post_expiry_off + 1],
        ])
    } else { 0 };

    // Look up by durable identity. On the leader, the propose-side
    // will have parked a transient=1 slot for this stream_hash; on a
    // follower or post-restart replay, no slot exists yet.
    let prior = s.find_session(tenant, stream_hash);
    let was_transient = match prior {
        Some(i) => s.sessions[i].transient == 1,
        None => false,
    };
    let was_persisted = match prior {
        Some(i) => s.sessions[i].persisted == 1,
        None => false,
    };
    let was_active = match prior {
        Some(i) => s.sessions[i].active == 1,
        None => false,
    };
    let session_idx = if let Some(i) = prior {
        Some(i)
    } else {
        s.allocate_session()
    };
    let Some(i) = session_idx else { return; };

    // clean_start with a prior slot wipes subscription / inflight /
    // prefetch state. Emit MSG_SESSION_DROP unconditionally on
    // clean_start so topic_engine's view stays consistent across
    // leader and followers (topic_engine tolerates spurious drops).
    if clean_start && prior.is_some() {
        let drop_body = (i as u32).to_le_bytes();
        try_emit(sys, s.out_topic, wire::MSG_SESSION_DROP, &drop_body);
        // Only wipe local operational state when taking over a genuinely
        // pre-existing session. When `prior` is this same CONNECT's
        // propose-side transient slot (the leader path — propose-side
        // already performed this reset at admission), the inflight
        // accumulated since belongs to QoS 1+ publishes admitted after
        // the optimistic CONNACK but before this QOP_CONNECT committed.
        // Wiping those here strands their PUBACKs: the durability ack
        // returns from ack_tracker but find_inflight_dir misses, so the
        // publisher hangs forever. This races in only under concurrent
        // first-publish bursts, which is why single-client load never hit
        // it. See docs/apply_side_state_machine.md.
        if !was_transient {
            s.sessions[i].inflight = [Inflight::zero(); MAX_INFLIGHT_PER_SESSION];
            s.sub_outstanding[i] = 0;
            s.prefetch_credit[i] = 0;
            s.sessions[i].next_msg_id = 1;
        }
    }

    // MQTT 5 §3.1.3.2.2: a fresh CONNECT taking over an existing
    // Session cancels any pending Will-Delay publication. Clear
    // regardless of clean_start — the new connection inherits or
    // replaces the prior Will, so the deferred fire from the
    // previous disconnect must not run.
    if prior.is_some() {
        s.sessions[i].pending_will_fire_at_ms = 0;
    }

    // Durable identity + parameters (idempotent on the leader where
    // propose-side already set most of these on the transient slot).
    s.sessions[i].tenant = tenant;
    s.sessions[i].stream_hash = stream_hash;
    s.sessions[i].session_epoch = s.sessions[i].session_epoch.wrapping_add(1);
    s.sessions[i].protocol = protocol;
    s.sessions[i].clean_start = clean_start as u8;
    s.sessions[i].keep_alive_ms = keep_alive_s.saturating_mul(1000);
    s.sessions[i].persisted = 0;

    if was_transient {
        // Leader path: propose-side admitted, apply now makes it durable.
        if !was_active {
            s.session_count = s.session_count.wrapping_add(1);
        }
        s.sessions[i].active = 1;
        s.sessions[i].transient = 0;
    } else if !was_active && !clean_start {
        // Follower / replay path under !clean_start: park the durable
        // record so a future reconnect on this node can resurrect it.
        s.sessions[i].persisted = 1;
    }

    // Resurrecting a persisted session under !clean_start: drain the
    // offline queue. Every node fires this signal — leader's drain
    // becomes try_deliver → codec, follower's drain hits an inactive
    // slot in try_deliver and re-parks via the persisted path. No
    // message loss because the offline_queue state is replicated.
    if !clean_start && was_persisted {
        let body_reconnect = (i as u32).to_le_bytes();
        try_emit(
            sys, s.out_messaging,
            wire::MSG_OFFLINE_RECONNECT, &body_reconnect,
        );
    }

    // Will-message state (MQTT 3.1.1 §3.1.2.5). A fresh CONNECT
    // *replaces* the prior Will (even if the new CONNECT has no Will
    // — clearing is the correct behaviour per §3.1.2.5). `apply_qop_
    // disconnect` fires this publish when the reason isn't CLEAN.
    if will_present {
        s.sessions[i].will_present = 1;
        s.sessions[i].will_qos = will_qos & 0x03;
        s.sessions[i].will_retain = if will_retain != 0 { 1 } else { 0 };
        s.sessions[i].will_delay_ms = will_delay_s.saturating_mul(1000);
        s.sessions[i].will_topic_len = will_topic_len as u16;
        core::ptr::copy_nonoverlapping(
            s.in_buf.as_ptr().add(will_topic_off),
            s.sessions[i].will_topic.as_mut_ptr(),
            will_topic_len,
        );
        s.sessions[i].will_payload_len = will_payload_len as u16;
        core::ptr::copy_nonoverlapping(
            s.in_buf.as_ptr().add(will_payload_off),
            s.sessions[i].will_payload.as_mut_ptr(),
            will_payload_len,
        );
    } else {
        s.sessions[i].will_present = 0;
        s.sessions[i].will_topic_len = 0;
        s.sessions[i].will_payload_len = 0;
    }

    // Session expiry policy stays attached to the slot for the duration
    // of this CONNECT. The apply-side disconnect path consults it; the
    // metrics-tick sweep purges persisted slots whose deadline elapses.
    s.sessions[i].session_expiry_s = session_expiry_s;
    // ReceiveMaximum caps how many concurrent unacked QoS 1+ deliveries
    // try_deliver will push to this subscriber. 0 means "no cap".
    s.sessions[i].receive_maximum = receive_maximum;

    s.applied = s.applied.wrapping_add(1);
}

/// Emit the stored Will-message for `tenant`'s slot `i` as an
/// apply-side `MSG_TOPIC_PUBLISH` (and `MSG_RETAINED_WRITE` if the
/// retain flag is set). Used by both the immediate-fire path in
/// `apply_qop_disconnect` (`will_delay_ms == 0`) and the deferred-fire
/// sweep (`pending_will_fire_at_ms` due). Doesn't clear the Will
/// fields — callers handle slot bookkeeping.
///
/// # Safety
/// Caller must hold an exclusive `&mut ModuleState`, supply a valid
/// `&SyscallTable`, and ensure slot `i < MAX_SESSIONS` is populated.
unsafe fn fire_will(
    s: &mut ModuleState,
    sys: &SyscallTable,
    tenant: TenantId,
    i: usize,
) {
    let topic_len = s.sessions[i].will_topic_len as usize;
    let payload_len = s.sessions[i].will_payload_len as usize;
    let pub_qos = s.sessions[i].will_qos & 0x03;
    let retain = s.sessions[i].will_retain != 0;
    // MSG_TOPIC_PUBLISH envelope shape (matches apply_qop_publish QoS 0
    // path so topic_engine fan-out is identical):
    //   [tenant:u32 LE][pub_qos:u8][_pad:u8][topic_len:u16 LE]
    //   [topic][user_props_count:u8][payload]
    // Will-fired publishes carry no user properties (the Will section
    // in CONNECT has its own properties block but they aren't stored
    // on the Session for now), so the user_props block is a single
    // zero byte.
    let dlv_total = 4 + 1 + 1 + 2 + topic_len + 1 + payload_len;
    if dlv_total <= s.out_buf.len() {
        s.out_buf[0..4].copy_from_slice(&tenant.to_le_bytes());
        s.out_buf[4] = pub_qos;
        s.out_buf[5] = 0;
        s.out_buf[6..8].copy_from_slice(&(topic_len as u16).to_le_bytes());
        core::ptr::copy_nonoverlapping(
            s.sessions[i].will_topic.as_ptr(),
            s.out_buf.as_mut_ptr().add(8),
            topic_len,
        );
        s.out_buf[8 + topic_len] = 0;
        core::ptr::copy_nonoverlapping(
            s.sessions[i].will_payload.as_ptr(),
            s.out_buf.as_mut_ptr().add(8 + topic_len + 1),
            payload_len,
        );
        try_emit(sys, s.out_topic, wire::MSG_TOPIC_PUBLISH, &s.out_buf[..dlv_total]);
    }
    // Retain=1 Will latches in retained_store the same way as an
    // ordinary retained PUBLISH (item 2's wire format).
    if retain {
        let topic_hash = wire::fnv1a_64(
            &s.sessions[i].will_topic[..topic_len],
        );
        let rwrite_len = 4 + 8 + 2 + topic_len + 4 + payload_len;
        if rwrite_len <= s.out_buf.len() {
            s.out_buf[0..4].copy_from_slice(&tenant.to_le_bytes());
            s.out_buf[4..12].copy_from_slice(&topic_hash.to_le_bytes());
            s.out_buf[12..14].copy_from_slice(&(topic_len as u16).to_le_bytes());
            core::ptr::copy_nonoverlapping(
                s.sessions[i].will_topic.as_ptr(),
                s.out_buf.as_mut_ptr().add(14),
                topic_len,
            );
            let pl_off = 14 + topic_len;
            s.out_buf[pl_off..pl_off + 4]
                .copy_from_slice(&(payload_len as u32).to_le_bytes());
            core::ptr::copy_nonoverlapping(
                s.sessions[i].will_payload.as_ptr(),
                s.out_buf.as_mut_ptr().add(pl_off + 4),
                payload_len,
            );
            try_emit(sys, s.out_messaging, wire::MSG_RETAINED_WRITE, &s.out_buf[..rwrite_len]);
        }
    }
}

/// Apply-side handler for QOP_DISCONNECT. Locates the slot via
/// `(tenant, stream_hash)`, deactivates it, and either parks the
/// durable record (`clean_start=false` session) or drops the slot
/// entirely (`clean_start=true`). Followers run the same logic so
/// their session table stays in lockstep with the leader's.
///
/// # Safety
/// Caller must hold an exclusive `&mut ModuleState`, supply a valid
/// `&SyscallTable`, and guarantee `body_start <= body_end <= s.in_buf.len()`.
unsafe fn apply_qop_disconnect(
    s: &mut ModuleState,
    sys: &SyscallTable,
    tenant: TenantId,
    body_start: usize,
    body_end: usize,
    now: u64,
) {
    if body_end < body_start { return; }
    let body_len = body_end - body_start;
    if body_len < 9 { return; }
    let reason = s.in_buf[body_start];
    let stream_hash = u64::from_le_bytes([
        s.in_buf[body_start + 1], s.in_buf[body_start + 2],
        s.in_buf[body_start + 3], s.in_buf[body_start + 4],
        s.in_buf[body_start + 5], s.in_buf[body_start + 6],
        s.in_buf[body_start + 7], s.in_buf[body_start + 8],
    ]);
    let Some(i) = s.find_session(tenant, stream_hash) else { return; };

    // Will-message handling (MQTT 3.1.1 §3.1.2.5 / MQTT 5 §3.1.3.2):
    // any non-CLEAN reason — keep-alive timeout, forced admin
    // teardown, in future TCP-reset detection — fires the stored Will
    // on behalf of the client. A clean DISCONNECT packet discards the
    // Will without publishing. MQTT 5 adds a Will Delay Interval
    // (§3.1.3.2.2): the broker MUST wait `will_delay_ms` before
    // publishing, OR until the Session ends, whichever comes first.
    // A reconnect within the delay window cancels the Will entirely.
    if reason != wire::QDISC_REASON_CLEAN && s.sessions[i].will_present == 1 {
        let will_delay_ms = s.sessions[i].will_delay_ms;
        if will_delay_ms == 0 {
            // No delay — fire now and consume the slot's Will state.
            fire_will(s, sys, tenant, i);
            s.sessions[i].will_present = 0;
            s.sessions[i].will_topic_len = 0;
            s.sessions[i].will_payload_len = 0;
        } else {
            // Schedule a deferred fire. The sweep walks
            // `pending_will_fire_at_ms` once per second. Cap the
            // deadline at the session-end deadline so an in-flight
            // session_expiry purge can't outlast the Will: §3.1.3.2.2
            // says publish OR end-of-session, whichever first.
            let fire_at = now.saturating_add(will_delay_ms as u64);
            let expiry_s = s.sessions[i].session_expiry_s;
            let session_end = if expiry_s == u32::MAX {
                u64::MAX
            } else if expiry_s == 0 {
                // session_expiry=0 means the session ends at network
                // disconnect; the Will must publish before the slot
                // is dropped. Cap at `now` so the sweep fires it on
                // the next tick (effectively immediate) — clamps to
                // the spec's "whichever first" guarantee.
                now
            } else {
                now.saturating_add((expiry_s as u64).saturating_mul(1000))
            };
            s.sessions[i].pending_will_fire_at_ms = fire_at.min(session_end);
            // DO NOT clear will_* fields — the sweep needs them.
        }
    } else {
        // CLEAN disconnect (or no Will): the Will is consumed without
        // publishing per §3.1.2.5.
        s.sessions[i].will_present = 0;
        s.sessions[i].will_topic_len = 0;
        s.sessions[i].will_payload_len = 0;
        s.sessions[i].pending_will_fire_at_ms = 0;
    }

    let was_active = s.sessions[i].active == 1;
    let was_clean = s.sessions[i].clean_start != 0;
    // MQTT 5 §3.1.2.11.2: session_expiry_s == 0 means the session
    // ends at network disconnect even when clean_start=false. Treat
    // it the same as a clean session here so we don't pay the cost
    // of carrying a slot that the sweep would purge on the next tick
    // anyway.
    let expire_now = s.sessions[i].session_expiry_s == 0;
    s.sessions[i].active = 0;
    // Drop the transient marker — propose-side admission is over.
    s.sessions[i].transient = 0;
    if was_active {
        s.session_count = s.session_count.saturating_sub(1);
    }
    if was_clean || expire_now {
        // Clean session OR session_expiry==0: drop everything so the
        // slot is fully free.
        s.sub_outstanding[i] = 0;
        s.prefetch_credit[i] = 0;
        s.sessions[i].persisted = 0;
        let drop_body = (i as u32).to_le_bytes();
        try_emit(sys, s.out_topic, wire::MSG_SESSION_DROP, &drop_body);
    } else {
        // Persistent session: park the slot for matching reconnect.
        // Stamp `disconnected_at_ms` so the expiry sweep can purge
        // the slot once `now > disconnected_at + session_expiry`.
        // `session_expiry_s == u32::MAX` makes the sweep skip the
        // slot — that's the propose-side normalisation for MQTT 3.1.1
        // clean_session=0.
        s.sessions[i].persisted = 1;
        s.sessions[i].disconnected_at_ms = now;
    }
    s.applied = s.applied.wrapping_add(1);
}

/// Apply-side handler for QOP_SUBSCRIBE. Looks up the local session
/// slot by `(tenant, stream_hash)`, emits `MSG_TOPIC_SUBSCRIBE` to
/// topic_engine keyed to the local slot index, and triggers retained
/// delivery for the matching topic.
///
/// # Safety
/// Caller must hold an exclusive `&mut ModuleState`, supply a valid
/// `&SyscallTable`, and guarantee `body_start <= body_end <= s.in_buf.len()`.
unsafe fn apply_qop_subscribe(
    s: &mut ModuleState,
    sys: &SyscallTable,
    tenant: TenantId,
    body_start: usize,
    body_end: usize,
) {
    if body_end < body_start { return; }
    let body_len = body_end - body_start;
    // [req_qos:u8][stream_hash:u64 LE][topic_len:u16 BE][topic]
    if body_len < 1 + 8 + 2 { return; }
    let req_qos = s.in_buf[body_start];
    let stream_hash = u64::from_le_bytes([
        s.in_buf[body_start + 1], s.in_buf[body_start + 2],
        s.in_buf[body_start + 3], s.in_buf[body_start + 4],
        s.in_buf[body_start + 5], s.in_buf[body_start + 6],
        s.in_buf[body_start + 7], s.in_buf[body_start + 8],
    ]);
    let topic_len = u16::from_be_bytes([
        s.in_buf[body_start + 9], s.in_buf[body_start + 10],
    ]) as usize;
    if body_len < 11 + topic_len { return; }
    let topic_off = body_start + 11;
    let Some(i) = s.find_session(tenant, stream_hash) else { return; };
    let session_slot = i as u32;

    // MSG_TOPIC_SUBSCRIBE body (matches the legacy propose-side shape):
    //   [tenant:u32 LE][session_slot:u32 LE][req_qos:u8][_pad:u8]
    //   [topic_len:u16 LE][topic]
    let sub_len = 4 + 4 + 1 + 1 + 2 + topic_len;
    if sub_len <= s.out_buf.len() {
        s.out_buf[0..4].copy_from_slice(&tenant.to_le_bytes());
        s.out_buf[4..8].copy_from_slice(&session_slot.to_le_bytes());
        s.out_buf[8] = req_qos;
        s.out_buf[9] = 0;
        s.out_buf[10..12].copy_from_slice(&(topic_len as u16).to_le_bytes());
        let src = s.in_buf.as_ptr().add(topic_off);
        let dst = s.out_buf.as_mut_ptr().add(12);
        core::ptr::copy_nonoverlapping(src, dst, topic_len);
        try_emit(sys, s.out_topic, wire::MSG_TOPIC_SUBSCRIBE, &s.out_buf[..sub_len]);
    }

    // Retained delivery: emit MSG_RETAINED_READ for the subscription
    // pattern. `retained_store` iterates its entries and runs
    // `wire::mqtt_topic_match(pattern, entry_topic)` against each one,
    // emitting a response per hit. Request body (item: wildcard
    // retained matching):
    //   [tenant:u32 LE][session_slot:u32 LE][sub_qos:u8]
    //   [pattern_len:u16 LE][pattern_bytes]
    // The response shape stays identical to the exact-match path
    // (echoes `(session_slot, sub_qos)` and carries the matched
    // entry's topic + payload), so the messaging_in handler doesn't
    // change. A wildcard subscription naturally produces N responses,
    // one per matching retained entry; the handler runs each through
    // `try_deliver` with its own delivery envelope.
    let rreq_len = 4 + 4 + 1 + 2 + topic_len;
    if rreq_len <= s.out_buf.len() {
        s.out_buf[0..4].copy_from_slice(&tenant.to_le_bytes());
        s.out_buf[4..8].copy_from_slice(&session_slot.to_le_bytes());
        s.out_buf[8] = req_qos;
        s.out_buf[9..11].copy_from_slice(&(topic_len as u16).to_le_bytes());
        core::ptr::copy_nonoverlapping(
            s.in_buf.as_ptr().add(topic_off),
            s.out_buf.as_mut_ptr().add(11),
            topic_len,
        );
        try_emit(sys, s.out_messaging, wire::MSG_RETAINED_READ, &s.out_buf[..rreq_len]);
    }

    s.applied = s.applied.wrapping_add(1);
}

/// Apply-side handler for QOP_UNSUBSCRIBE. Emits `MSG_TOPIC_UNSUBSCRIBE`
/// to topic_engine keyed to the local slot index.
///
/// # Safety
/// Caller must hold an exclusive `&mut ModuleState`, supply a valid
/// `&SyscallTable`, and guarantee `body_start <= body_end <= s.in_buf.len()`.
unsafe fn apply_qop_unsubscribe(
    s: &mut ModuleState,
    sys: &SyscallTable,
    tenant: TenantId,
    body_start: usize,
    body_end: usize,
) {
    if body_end < body_start { return; }
    let body_len = body_end - body_start;
    // [stream_hash:u64 LE][topic_len:u16 BE][topic]
    if body_len < 8 + 2 { return; }
    let stream_hash = u64::from_le_bytes([
        s.in_buf[body_start],     s.in_buf[body_start + 1],
        s.in_buf[body_start + 2], s.in_buf[body_start + 3],
        s.in_buf[body_start + 4], s.in_buf[body_start + 5],
        s.in_buf[body_start + 6], s.in_buf[body_start + 7],
    ]);
    let topic_len = u16::from_be_bytes([
        s.in_buf[body_start + 8], s.in_buf[body_start + 9],
    ]) as usize;
    if body_len < 10 + topic_len { return; }
    let topic_off = body_start + 10;
    let Some(i) = s.find_session(tenant, stream_hash) else { return; };
    let session_slot = i as u32;

    // MSG_TOPIC_UNSUBSCRIBE body (matches the legacy shape):
    //   [tenant:u32 LE][session_slot:u32 LE][topic_len:u16 LE][topic]
    let unsub_len = 4 + 4 + 2 + topic_len;
    if unsub_len <= s.out_buf.len() {
        s.out_buf[0..4].copy_from_slice(&tenant.to_le_bytes());
        s.out_buf[4..8].copy_from_slice(&session_slot.to_le_bytes());
        s.out_buf[8..10].copy_from_slice(&(topic_len as u16).to_le_bytes());
        let src = s.in_buf.as_ptr().add(topic_off);
        let dst = s.out_buf.as_mut_ptr().add(10);
        core::ptr::copy_nonoverlapping(src, dst, topic_len);
        try_emit(sys, s.out_topic, wire::MSG_TOPIC_UNSUBSCRIBE, &s.out_buf[..unsub_len]);
    }
    s.applied = s.applied.wrapping_add(1);
}

/// Apply-side handler for QOP_PUBREL. Records the durable QoS 2
/// phase transition on the publisher inflight. The propose-side
/// already set the inflight phase to QOS2_PUBREL before proposing;
/// apply runs the same mutation on every node so follower state
/// converges (the inflight slot is the durable record of an
/// in-progress QoS 2 transaction). MSG_ACK_EMIT then fires PUBCOMP
/// once durability lands.
///
/// # Safety
/// Caller must hold an exclusive `&mut ModuleState` and guarantee
/// `body_start <= body_end <= s.in_buf.len()`.
unsafe fn apply_qop_pubrel(
    s: &mut ModuleState,
    tenant: TenantId,
    body_start: usize,
    body_end: usize,
) {
    if body_end < body_start { return; }
    let body_len = body_end - body_start;
    // [packet_id:u16 BE][stream_hash:u64 LE]
    if body_len < 10 { return; }
    let packet_id = u16::from_be_bytes([
        s.in_buf[body_start], s.in_buf[body_start + 1],
    ]);
    let stream_hash = u64::from_le_bytes([
        s.in_buf[body_start + 2], s.in_buf[body_start + 3],
        s.in_buf[body_start + 4], s.in_buf[body_start + 5],
        s.in_buf[body_start + 6], s.in_buf[body_start + 7],
        s.in_buf[body_start + 8], s.in_buf[body_start + 9],
    ]);
    let Some(i) = s.find_session(tenant, stream_hash) else { return; };
    if let Some(ii) = s.sessions[i].find_inflight_dir(packet_id, INFLIGHT_PUB) {
        s.sessions[i].inflight[ii].phase = QOS2_PUBREL;
    }
    s.applied = s.applied.wrapping_add(1);
}

/// Apply-side handler for QOP_PUBLISH. For QoS 0, builds
/// `MSG_TOPIC_PUBLISH` directly and emits it. For QoS 1+, allocates a
/// stash slot keyed by `correlation_id`, stores the op-body verbatim
/// (the redeliver path repropose-wraps it), marks the stash durable
/// immediately — apply runs post-commit so durability is implicit —
/// and emits `MSG_DEDUP_CHECK` to gate the topic fan-out on the dedup
/// engine's verdict. On the leader the existing `MSG_ACK_EMIT` path
/// re-marks durable (idempotent) and fires PUBACK; on followers the
/// apply-side marking is the only durability signal.
///
/// `MSG_RETAINED_WRITE` is emitted directly from apply when the retain
/// flag is set, independent of the stash gating.
///
/// # Safety
/// Caller must hold an exclusive `&mut ModuleState`, supply a valid
/// `&SyscallTable`, and guarantee `body_start <= body_end <= s.in_buf.len()`.
unsafe fn apply_qop_publish(
    s: &mut ModuleState,
    sys: &SyscallTable,
    tenant: TenantId,
    body_start: usize,
    body_end: usize,
    version: u8,
) {
    if body_end < body_start { return; }
    let body_len = body_end - body_start;
    // QOP_PUBLISH op-body: 18-byte fixed header + topic [+ V2 user_props block] + payload.
    if body_len < 18 { return; }
    let pub_qos = s.in_buf[body_start] & 0x03;
    let packet_id = u16::from_be_bytes([
        s.in_buf[body_start + 1], s.in_buf[body_start + 2],
    ]);
    let stream_hash = u64::from_le_bytes([
        s.in_buf[body_start + 3],  s.in_buf[body_start + 4],
        s.in_buf[body_start + 5],  s.in_buf[body_start + 6],
        s.in_buf[body_start + 7],  s.in_buf[body_start + 8],
        s.in_buf[body_start + 9],  s.in_buf[body_start + 10],
    ]);
    let session_epoch = u32::from_le_bytes([
        s.in_buf[body_start + 11], s.in_buf[body_start + 12],
        s.in_buf[body_start + 13], s.in_buf[body_start + 14],
    ]);
    let retain = s.in_buf[body_start + 15] != 0;
    let topic_len = u16::from_be_bytes([
        s.in_buf[body_start + 16], s.in_buf[body_start + 17],
    ]) as usize;
    if body_len < 18 + topic_len { return; }
    let topic_off = body_start + 18;

    // V2 interleaves a user_props block between topic and payload.
    // V1 (replayed from older WALs) has payload immediately after
    // topic, equivalent to "user_props_count == 0".
    let (up_off, up_len) = if version == wire::QPROP_VERSION_V2 {
        let up_off = topic_off + topic_len;
        let slice_end = body_start + body_len;
        if up_off > slice_end { return; }
        let n = user_props_block_len(&s.in_buf[up_off..slice_end]).unwrap_or(0);
        if n == 0 || up_off + n > slice_end { return; }
        (up_off, n)
    } else {
        (topic_off + topic_len, 0)
    };
    let payload_off = up_off + up_len;
    if payload_off > body_start + body_len { return; }
    let payload_len = body_len - 18 - topic_len - up_len;

    // topic_hash is a u64 value — no s.in_buf borrow held after this
    // call, so subsequent mutations to s.out_buf / s.stash_env are safe.
    let topic_hash = wire::fnv1a_64(&s.in_buf[topic_off..topic_off + topic_len]);

    // For QoS 1+, look up correlation_id from the leader's publisher
    // inflight (propose-side allocates correlation + inflight together).
    // Followers have no inflight here, so correlation_id stays 0 and
    // QoS 1+ falls back to the direct emit path — leader fan-out is
    // still gated through the stash via the correlation_id !=0 branch.
    let correlation_id: u64 = if pub_qos > 0 {
        match s.find_session(tenant, stream_hash) {
            Some(i) => match s.sessions[i].find_inflight_dir(packet_id, INFLIGHT_PUB) {
                Some(ii) => s.sessions[i].inflight[ii].correlation_id,
                None => 0,
            },
            None => 0,
        }
    } else {
        0
    };

    if pub_qos == 0 || correlation_id == 0 {
        // QoS 0, or QoS 1+ on a follower (no inflight, no stash needed):
        // emit MSG_TOPIC_PUBLISH directly. Subscribers tolerate the
        // duplicate-on-DUP-retry case at the protocol layer.
        // MSG_TOPIC_PUBLISH wire shape (matches what topic_engine
        // reads):
        //   [tenant:u32 LE][pub_qos:u8][_pad:u8][topic_len:u16 LE]
        //   [topic][user_props_count:u8][per prop ...][payload]
        // For V1 entries `up_len == 0` and we emit a single zero byte
        // for the count — topic_engine and the down-stream
        // MSG_TOPIC_DELIVER consumer always see a present (possibly
        // empty) user_props block.
        let placeholder_up_len = if up_len > 0 { up_len } else { 1 };
        let topic_pub_len = 4 + 1 + 1 + 2 + topic_len + placeholder_up_len + payload_len;
        if topic_pub_len <= s.out_buf.len() {
            s.out_buf[0..4].copy_from_slice(&tenant.to_le_bytes());
            s.out_buf[4] = pub_qos;
            s.out_buf[5] = 0;
            s.out_buf[6..8].copy_from_slice(&(topic_len as u16).to_le_bytes());
            let topic_dst = 8;
            core::ptr::copy_nonoverlapping(
                s.in_buf.as_ptr().add(topic_off),
                s.out_buf.as_mut_ptr().add(topic_dst),
                topic_len,
            );
            let up_dst = topic_dst + topic_len;
            if up_len > 0 {
                core::ptr::copy_nonoverlapping(
                    s.in_buf.as_ptr().add(up_off),
                    s.out_buf.as_mut_ptr().add(up_dst),
                    up_len,
                );
            } else {
                s.out_buf[up_dst] = 0;
            }
            let payload_dst = up_dst + placeholder_up_len;
            core::ptr::copy_nonoverlapping(
                s.in_buf.as_ptr().add(payload_off),
                s.out_buf.as_mut_ptr().add(payload_dst),
                payload_len,
            );
            try_emit(sys, s.out_topic, wire::MSG_TOPIC_PUBLISH, &s.out_buf[..topic_pub_len]);
        }
    } else {
        // QoS 1+ on the leader: stash op-body, mark durable, emit
        // dedup_check. finalise_stash builds MSG_TOPIC_PUBLISH on
        // dedup_result.
        let mut dkey = [0u8; 20];
        wire::encode_dedup_key(
            &mut dkey, tenant, stream_hash, session_epoch, packet_id as u32,
        );
        if body_len <= MAX_STASH_ENV {
            if let Some(stash_idx) = s.allocate_stash(correlation_id, &dkey) {
                let src = s.in_buf.as_ptr().add(body_start);
                let dst = s.stash_env[stash_idx].as_mut_ptr();
                core::ptr::copy_nonoverlapping(src, dst, body_len);
                s.stash_env_len[stash_idx] = body_len as u16;
                s.stash_durable[stash_idx] = 1;
                try_emit(sys, s.out_messaging, wire::MSG_DEDUP_CHECK, &dkey);
            }
            // If allocate_stash fails (stash table full), the publish
            // is dropped from the fan-out path; ack_tracker still fires
            // PUBACK so the publisher's protocol state advances.
        }
    }

    if retain {
        // MSG_RETAINED_WRITE body (extended for inline payload storage):
        //   [tenant:u32 LE][topic_hash:u64 LE][topic_len:u16 LE][topic_bytes]
        //   [payload_len:u32 LE][payload_bytes]
        // retained_store stores topic + payload inline so the
        // read-back path can echo them straight to session_processor's
        // MSG_TOPIC_DELIVER builder without a separate content store.
        let rwrite_len = 4 + 8 + 2 + topic_len + 4 + payload_len;
        if rwrite_len <= s.out_buf.len() {
            s.out_buf[0..4].copy_from_slice(&tenant.to_le_bytes());
            s.out_buf[4..12].copy_from_slice(&topic_hash.to_le_bytes());
            s.out_buf[12..14].copy_from_slice(&(topic_len as u16).to_le_bytes());
            core::ptr::copy_nonoverlapping(
                s.in_buf.as_ptr().add(topic_off),
                s.out_buf.as_mut_ptr().add(14),
                topic_len,
            );
            let payload_len_off = 14 + topic_len;
            s.out_buf[payload_len_off..payload_len_off + 4]
                .copy_from_slice(&(payload_len as u32).to_le_bytes());
            core::ptr::copy_nonoverlapping(
                s.in_buf.as_ptr().add(payload_off),
                s.out_buf.as_mut_ptr().add(payload_len_off + 4),
                payload_len,
            );
            try_emit(sys, s.out_messaging, wire::MSG_RETAINED_WRITE, &s.out_buf[..rwrite_len]);
        }
    }

    s.applied = s.applied.wrapping_add(1);
}

#[no_mangle]
#[link_section = ".text.module_step"]
pub extern "C" fn module_step(state: *mut u8) -> i32 {
    unsafe {
        let s = &mut *(state as *mut ModuleState);
        let sys = &*s.syscalls;
        let now = dev_millis(sys);

        // ── Phase 1: drain flow signals (wire envelope) ──────────
        // flow_in is shared by flow_controller (MSG_THROTTLE_CREDITS),
        // backpressure_propagator (MSG_BP_SIGNAL), and prefetch_controller
        // (MSG_PREFETCH_CREDIT). Demultiplex by msg_type.
        if s.in_flow >= 0 {
            loop {
                let poll = (sys.channel_poll)(s.in_flow, 0x01);
                if poll <= 0 || (poll as u32 & 0x01) == 0 { break; }
                let (mt, plen) = wire::channel_read_msg(sys, s.in_flow, &mut s.in_buf);
                match mt {
                    wire::MSG_THROTTLE_CREDITS if plen >= 8 => {
                        s.pid_entry_credits = i32::from_le_bytes([s.in_buf[0], s.in_buf[1], s.in_buf[2], s.in_buf[3]]);
                        s.pid_byte_credits = i32::from_le_bytes([s.in_buf[4], s.in_buf[5], s.in_buf[6], s.in_buf[7]]);
                    }
                    wire::MSG_BP_SIGNAL if plen >= 9 => {
                        // [reason:u8][entry_credits:i32 LE][byte_credits:i32 LE]
                        // Treat as a backpressure-flavoured credit update.
                        s.pid_entry_credits = i32::from_le_bytes([s.in_buf[1], s.in_buf[2], s.in_buf[3], s.in_buf[4]]);
                        s.pid_byte_credits = i32::from_le_bytes([s.in_buf[5], s.in_buf[6], s.in_buf[7], s.in_buf[8]]);
                    }
                    wire::MSG_PREFETCH_CREDIT if plen >= 8 => {
                        // [session_slot:u32 LE][credit:u32 LE]
                        // credit is an absolute cap on outstanding (unacked)
                        // deliveries to this subscriber. Do not reset
                        // sub_outstanding here — that would defeat the
                        // backlog measurement; the consumer drains it via
                        // PUBACK as deliveries clear.
                        let slot = u32::from_le_bytes([s.in_buf[0], s.in_buf[1], s.in_buf[2], s.in_buf[3]]) as usize;
                        let credit = u32::from_le_bytes([s.in_buf[4], s.in_buf[5], s.in_buf[6], s.in_buf[7]]);
                        if slot < MAX_SESSIONS {
                            s.prefetch_credit[slot] = credit;
                        }
                    }
                    _ => {}
                }
            }
        }

        // ── Phase 2: process codec proposals (envelope-framed) ──
        //
        // Wire format on codec_in: wire envelope `[mtype:u8][len:u16 LE][payload]`
        // where `payload = [conn_id:u8][proto:u8][pkt_type:u8][flags:u8][body]`.
        // Codecs (mqtt, amqp, kafka) write via `channel_write_msg`; the
        // envelope length prefix is what lets the consumer demarcate each
        // message when several writers fan in or one writer bursts multiple
        // packets within a tick.
        if s.in_codec >= 0 {
            for _ in 0..8 {
                let poll = (sys.channel_poll)(s.in_codec, 0x01);
                if poll <= 0 || (poll as u32 & 0x01) == 0 { break; }

                let (mt, plen) = wire::channel_read_msg(sys, s.in_codec, &mut s.in_buf);
                if plen < 4 { continue; } // conn + proto + pkt + flags

                let conn_id = s.in_buf[0];
                let proto = s.in_buf[1];
                let pkt_type = s.in_buf[2];
                let flags = s.in_buf[3];
                let body_ptr = s.in_buf.as_ptr().add(4);
                let body_len = plen as usize - 4;
                let body = core::slice::from_raw_parts(body_ptr, body_len);

                // Keep-alive bookkeeping: every codec packet from this
                // conn_id resets the inactivity timer. The periodic scan
                // at the tail of module_step uses last_activity_ms to
                // detect dropped sockets that peer_router cleared without
                // notifying us.
                if let Some(si) = s.find_session_by_conn(conn_id) {
                    s.sessions[si].last_activity_ms = now;
                }

                match mt {
                    wire::MSG_SESSION_CONNECT => {
                        s.connects = s.connects.wrapping_add(1);
                        // MQTT CONNECT envelope from mqtt_codec:
                        //   [proto_ver:u8][clean_start:u8][keep_alive:u16 BE]
                        //   [session_expiry:u32 LE][recv_max:u16 LE]
                        //   [cid_len:u16 BE][cid bytes][...]
                        //
                        // Phase 2+3 of docs/apply_side_state_machine.md splits
                        // CONNECT handling:
                        //   • Propose-side (here): admission routing — bind
                        //     conn_id to a *transient* slot, send CONNACK
                        //     optimistically, encode QOP_CONNECT, propose.
                        //   • Apply-side (committed_in dispatcher): make the
                        //     slot durable, emit MSG_SESSION_DROP /
                        //     MSG_OFFLINE_RECONNECT to downstream modules.
                        //
                        // Operational mutations (inflight reset on clean_start,
                        // last_activity_ms tracking, keep_alive_ms) stay
                        // propose-side because they gate the post-CONNACK
                        // packet burst before QOP_CONNECT can apply.
                        let clean_start = if body.len() >= 2 { body[1] != 0 } else { true };
                        let cid_len = if body.len() >= 12 {
                            u16::from_be_bytes([body[10], body[11]]) as usize
                        } else { 0 };
                        let cid_present = cid_len > 0 && body.len() >= 12 + cid_len;
                        let stream_hash = if cid_present {
                            wire::fnv1a_64(&body[12..12 + cid_len])
                        } else {
                            // Empty client_id (allowed under clean_start) — derive a
                            // unique stream_hash from the conn_id so two anonymous
                            // clients don't collapse to the same session.
                            wire::fnv1a_64(&[conn_id])
                        };

                        // MQTT keep_alive (seconds) is parsed by mqtt_codec
                        // into bytes 2..4 BE. 0 means "no keep-alive" per
                        // MQTT 3.1.1 §3.1.2.10.
                        let keep_alive_s = if body.len() >= 4 {
                            u16::from_be_bytes([body[2], body[3]]) as u32
                        } else { 0 };
                        let tenant: TenantId = 0;
                        let prior = s.find_session(tenant, stream_hash);
                        // Was the matched slot a persisted session that we're
                        // about to resurrect, vs an already-active reconnect
                        // (e.g. same client_id re-CONNECT before DISCONNECT)?
                        // Only the persisted path sets session_present=1 in
                        // the optimistic CONNACK.
                        let resurrecting_persisted = match prior {
                            Some(i) => s.sessions[i].persisted == 1 && !clean_start,
                            None => false,
                        };
                        let session_idx = if let Some(i) = prior {
                            if clean_start {
                                // Operational reset so the post-CONNACK
                                // packet burst doesn't pick up stale inflight.
                                // Durable counterpart (MSG_SESSION_DROP on
                                // out_topic) lands at QOP_CONNECT apply.
                                s.sessions[i].inflight = [Inflight::zero(); MAX_INFLIGHT_PER_SESSION];
                                s.sub_outstanding[i] = 0;
                                s.prefetch_credit[i] = 0;
                                s.sessions[i].next_msg_id = 1;
                            }
                            s.sessions[i].last_activity_ms = now;
                            s.sessions[i].conn_id = conn_id;
                            s.sessions[i].protocol = proto;
                            s.sessions[i].clean_start = clean_start as u8;
                            s.sessions[i].keep_alive_ms =
                                keep_alive_s.saturating_mul(1000);
                            // Propose-side admission marker; apply flips
                            // transient→active and bumps session_epoch.
                            s.sessions[i].transient = 1;
                            Some(i)
                        } else if let Some(i) = s.allocate_session() {
                            s.sessions[i] = Session {
                                tenant, stream_hash,
                                protocol: proto, conn_id,
                                // active flips at QOP_CONNECT apply.
                                transient: 1,
                                clean_start: clean_start as u8,
                                last_activity_ms: now,
                                keep_alive_ms: keep_alive_s.saturating_mul(1000),
                                ..Session::zero()
                            };
                            // session_count is the durable count of active
                            // sessions; apply-side increments it.
                            Some(i)
                        } else { None };

                        // CONNACK body = [session_present:u8][reason_code:u8].
                        // MQTT 3.1.1 §3.2.2.2 requires session_present=1
                        // when the broker is resurrecting a stored session.
                        // Optimistic — see docs/apply_side_state_machine.md
                        // §Phase 2 (CONNECT specifically).
                        if proto == PROTO_MQTT {
                            let connack_body =
                                [if resurrecting_persisted { 1u8 } else { 0u8 }, 0u8];
                            emit_codec_response(sys, s.out_codec, conn_id, PROTO_MQTT, PKT_CONNACK, 0, &connack_body);
                        }

                        // Locate the Will section in the codec envelope. After
                        // the cid the layout continues with `[un_len:u16 BE]
                        // [un_bytes][will_flag:u8][...]`. The Will fields are
                        // present iff `will_flag == 1`, in which case they
                        // continue with `[qos][retain][delay_s LE][topic_len BE]
                        // [topic][payload_len BE][payload]` (see
                        // modules/app/mqtt_codec/mod.rs PKT_CONNECT case).
                        // Reject Will sizes that wouldn't fit in the Session
                        // slot at propose time — apply-side must never see a
                        // Will it can't store.
                        let un_len_off = 12 + cid_len;
                        let un_len = if body.len() >= un_len_off + 2 {
                            u16::from_be_bytes([body[un_len_off], body[un_len_off + 1]]) as usize
                        } else { 0 };
                        let will_flag_off = un_len_off + 2 + un_len;
                        let will_present = body.len() > will_flag_off && body[will_flag_off] != 0;
                        let mut will_qos: u8 = 0;
                        let mut will_retain: u8 = 0;
                        let mut will_delay_s: u32 = 0;
                        let mut will_topic_off: usize = 0;
                        let mut will_topic_len: usize = 0;
                        let mut will_payload_off: usize = 0;
                        let mut will_payload_len: usize = 0;
                        let mut will_accept = will_present;
                        if will_present {
                            let q_off = will_flag_off + 1;
                            if body.len() < q_off + 1 + 1 + 4 + 2 { will_accept = false; }
                            else {
                                will_qos = body[q_off];
                                will_retain = body[q_off + 1];
                                will_delay_s = u32::from_le_bytes([
                                    body[q_off + 2], body[q_off + 3],
                                    body[q_off + 4], body[q_off + 5],
                                ]);
                                let wt_len_off = q_off + 6;
                                will_topic_len = u16::from_be_bytes([
                                    body[wt_len_off], body[wt_len_off + 1],
                                ]) as usize;
                                will_topic_off = wt_len_off + 2;
                                let wp_len_off = will_topic_off + will_topic_len;
                                if body.len() < wp_len_off + 2 { will_accept = false; }
                                else if will_topic_len > MAX_WILL_TOPIC { will_accept = false; }
                                else {
                                    will_payload_len = u16::from_be_bytes([
                                        body[wp_len_off], body[wp_len_off + 1],
                                    ]) as usize;
                                    will_payload_off = wp_len_off + 2;
                                    if body.len() < will_payload_off + will_payload_len
                                        || will_payload_len > MAX_WILL_PAYLOAD
                                    {
                                        will_accept = false;
                                    }
                                }
                            }
                        }
                        let encode_will = will_present && will_accept;

                        // SessionExpiryInterval (MQTT 5 §3.1.2.11.2). The
                        // codec stashes the parsed property value at
                        // body[4..8] LE; MQTT 3.1.1 has no such property
                        // so the field is 0. Normalise: MQTT 3.1.1 with
                        // clean_session=0 means "persist indefinitely"
                        // (§3.1.2.4), which we map to u32::MAX (sweep
                        // skip). clean_start=1 → expiry is irrelevant
                        // because apply_qop_disconnect drops the slot
                        // outright on the clean path; pass through as-is.
                        let session_expiry_s = if body.len() >= 8 {
                            u32::from_le_bytes([body[4], body[5], body[6], body[7]])
                        } else { 0 };
                        let session_expiry_s = if proto == PROTO_MQTT
                            && body.first().copied().unwrap_or(0) < 5
                            && !clean_start
                            && session_expiry_s == 0
                        {
                            u32::MAX
                        } else {
                            session_expiry_s
                        };

                        // ReceiveMaximum (MQTT 5 §3.1.2.11.3). Stashed
                        // by mqtt_codec at body[8..10] LE (default 0 for
                        // MQTT 3.1.1 / absent property). 0 means "no
                        // cap" in our slot semantics; >0 caps concurrent
                        // unacked QoS 1+ deliveries to the subscriber.
                        let receive_maximum_s = if body.len() >= 10 {
                            u16::from_le_bytes([body[8], body[9]])
                        } else { 0 };

                        // Encode QOP_CONNECT canonical envelope and propose.
                        // Body shape (from wire::QOP_CONNECT docstring):
                        //   [clean_start:u8][keep_alive_s:u16 BE]
                        //   [stream_hash:u64 LE][cid_len:u16 BE]
                        //   [cid bytes][protocol:u8][will_flag:u8]
                        //   [if will: qos][retain][delay_s LE][topic_len BE][topic][payload_len BE][payload]
                        //   [session_expiry_s:u32 LE][receive_maximum:u16 LE]
                        let session_slot = session_idx.unwrap_or(0) as u32;
                        let will_extra = if encode_will {
                            1 + 1 + 4 + 2 + will_topic_len + 2 + will_payload_len
                        } else { 0 };
                        let qbody_len = 1 + 2 + 8 + 2 + cid_len + 1 + 1 + will_extra + 4 + 2;
                        let prop_total = wire::QPROP_UNTAGGED_HDR_LEN + qbody_len;
                        if prop_total <= s.out_buf.len() {
                            wire::encode_qprop_header(
                                &mut s.out_buf[0..wire::QPROP_HEADER_LEN],
                                wire::QOP_CONNECT, tenant, session_slot,
                            );
                            let off = wire::QPROP_UNTAGGED_HDR_LEN;
                            s.out_buf[off] = clean_start as u8;
                            s.out_buf[off + 1..off + 3]
                                .copy_from_slice(&(keep_alive_s as u16).to_be_bytes());
                            s.out_buf[off + 3..off + 11]
                                .copy_from_slice(&stream_hash.to_le_bytes());
                            s.out_buf[off + 11..off + 13]
                                .copy_from_slice(&(cid_len as u16).to_be_bytes());
                            if cid_present {
                                core::ptr::copy_nonoverlapping(
                                    body.as_ptr().add(12),
                                    s.out_buf.as_mut_ptr().add(off + 13),
                                    cid_len,
                                );
                            }
                            s.out_buf[off + 13 + cid_len] = proto;
                            let mut wpos = off + 14 + cid_len;
                            s.out_buf[wpos] = if encode_will { 1 } else { 0 };
                            wpos += 1;
                            if encode_will {
                                s.out_buf[wpos] = will_qos;
                                s.out_buf[wpos + 1] = will_retain;
                                s.out_buf[wpos + 2..wpos + 6]
                                    .copy_from_slice(&will_delay_s.to_le_bytes());
                                s.out_buf[wpos + 6..wpos + 8]
                                    .copy_from_slice(&(will_topic_len as u16).to_be_bytes());
                                core::ptr::copy_nonoverlapping(
                                    body.as_ptr().add(will_topic_off),
                                    s.out_buf.as_mut_ptr().add(wpos + 8),
                                    will_topic_len,
                                );
                                let wp_len_pos = wpos + 8 + will_topic_len;
                                s.out_buf[wp_len_pos..wp_len_pos + 2]
                                    .copy_from_slice(&(will_payload_len as u16).to_be_bytes());
                                core::ptr::copy_nonoverlapping(
                                    body.as_ptr().add(will_payload_off),
                                    s.out_buf.as_mut_ptr().add(wp_len_pos + 2),
                                    will_payload_len,
                                );
                                wpos = wp_len_pos + 2 + will_payload_len;
                            }
                            s.out_buf[wpos..wpos + 4]
                                .copy_from_slice(&session_expiry_s.to_le_bytes());
                            s.out_buf[wpos + 4..wpos + 6]
                                .copy_from_slice(&receive_maximum_s.to_le_bytes());
                            if try_emit(
                                sys, s.out_proposals,
                                wire::MSG_CLIENT_PROPOSAL,
                                &s.out_buf[..prop_total],
                            ) {
                                s.proposals_emitted = s.proposals_emitted.wrapping_add(1);
                            }
                        }

                        // Audit (operational, propose-side).
                        let mut ev = [0u8; 2];
                        ev[0] = AUDIT_CONNECT;
                        ev[1] = proto;
                        try_emit(sys, s.out_audit, wire::MSG_AUDIT_EVENT, &ev);

                        let _ = session_idx;
                        let _ = flags;
                        let _ = pkt_type;
                    }

                    wire::MSG_SESSION_DISCONNECT => {
                        // Propose QOP_DISCONNECT; apply-side does the durable
                        // state mutation (clear active, persist or drop, fan
                        // out MSG_SESSION_DROP on clean).
                        s.disconnects = s.disconnects.wrapping_add(1);
                        let tenant: TenantId = 0;
                        if let Some(i) = s.find_session_by_conn(conn_id) {
                            let stream_hash = s.sessions[i].stream_hash;
                            let session_slot = i as u32;
                            // Clear conn_id locally so subsequent packets on
                            // this conn don't re-route to a slot whose
                            // disconnect is in flight.
                            s.sessions[i].conn_id = 0;

                            // QOP_DISCONNECT body: [reason:u8][stream_hash:u64 LE]
                            let qbody_len = 1 + 8;
                            let prop_total = wire::QPROP_UNTAGGED_HDR_LEN + qbody_len;
                            if prop_total <= s.out_buf.len() {
                                wire::encode_qprop_header(
                                    &mut s.out_buf[0..wire::QPROP_HEADER_LEN],
                                    wire::QOP_DISCONNECT, tenant, session_slot,
                                );
                                let off = wire::QPROP_UNTAGGED_HDR_LEN;
                                s.out_buf[off] = wire::QDISC_REASON_CLEAN;
                                s.out_buf[off + 1..off + 9]
                                    .copy_from_slice(&stream_hash.to_le_bytes());
                                if try_emit(
                                    sys, s.out_proposals,
                                    wire::MSG_CLIENT_PROPOSAL,
                                    &s.out_buf[..prop_total],
                                ) {
                                    s.proposals_emitted =
                                        s.proposals_emitted.wrapping_add(1);
                                }
                            }
                        }
                        let mut ev = [0u8; 2];
                        ev[0] = AUDIT_DISCONNECT;
                        ev[1] = proto;
                        try_emit(sys, s.out_audit, wire::MSG_AUDIT_EVENT, &ev);
                    }

                    wire::MSG_SESSION_PROPOSAL if proto == PROTO_MQTT => {
                        match pkt_type {
                            PKT_PUBLISH => {
                                // Propose QOP_PUBLISH; apply-side handles
                                // dedup_check, retained_write, MSG_TOPIC_PUBLISH
                                // (direct for QoS 0, stash-gated for QoS 1+).
                                // Propose-side keeps only admission concerns:
                                // PID credit accounting, publisher inflight
                                // allocation (QoS 1+ — needed so PUBACK fires
                                // back on the right packet_id), and
                                // correlation allocation for ack_tracker.
                                s.publishes = s.publishes.wrapping_add(1);
                                let qos = (flags >> 1) & 0x03;
                                let retain = flags & 0x01 != 0;

                                // Body shape from mqtt_codec:
                                //   [packet_id BE][topic_len BE][topic]
                                //   [user_props_count:u8][per prop ...]
                                //   [payload]
                                // The user_props block is always present even
                                // for MQTT 3.1.1 (count = 0). Item 6
                                // bumps QOP_PUBLISH to V2 to carry it.
                                if body.len() < 4 { continue; }
                                let packet_id = u16::from_be_bytes([body[0], body[1]]);
                                let topic_len = u16::from_be_bytes([body[2], body[3]]) as usize;
                                if body.len() < 4 + topic_len + 1 { continue; }
                                let up_off = 4 + topic_len;
                                let Some(up_len) = user_props_block_len(&body[up_off..]) else {
                                    s.publishes_throttled =
                                        s.publishes_throttled.wrapping_add(1);
                                    continue;
                                };
                                if up_len > MAX_USER_PROPS_BYTES {
                                    s.publishes_throttled =
                                        s.publishes_throttled.wrapping_add(1);
                                    continue;
                                }
                                let payload_off = up_off + up_len;
                                let payload_len = body.len() - payload_off;

                                let tenant: TenantId = 0;
                                let session_idx = s.find_session_by_conn(conn_id);

                                // PID admission.
                                let body_cost = body.len() as i32;
                                if s.pid_entry_credits <= 0 || s.pid_byte_credits < body_cost {
                                    s.publishes_throttled = s.publishes_throttled.wrapping_add(1);
                                    continue;
                                }

                                // QoS 1+: allocate publisher inflight FIRST and
                                // fail-fast if the slot table is full. Per
                                // MQTT 3.1.1 §4.4, the broker must track every
                                // QoS 1+ delivery until it is acknowledged;
                                // continuing to route + propose without an
                                // inflight slot would silently downgrade the
                                // contract to QoS 0 and break PUBACK matching.
                                if qos > 0 {
                                    let Some(si) = session_idx else {
                                        s.publishes_throttled =
                                            s.publishes_throttled.wrapping_add(1);
                                        continue;
                                    };
                                    if s.sessions[si]
                                        .allocate_inflight(packet_id, qos, INFLIGHT_PUB)
                                        .is_none()
                                    {
                                        s.publishes_throttled =
                                            s.publishes_throttled.wrapping_add(1);
                                        continue;
                                    }
                                }

                                // Admission succeeded — draw down PID credit.
                                s.pid_entry_credits = s.pid_entry_credits.saturating_sub(1);
                                s.pid_byte_credits = s.pid_byte_credits.saturating_sub(body_cost);

                                let stream_hash = session_idx
                                    .map(|si| s.sessions[si].stream_hash)
                                    .unwrap_or(0);
                                let session_epoch = session_idx
                                    .map(|si| s.sessions[si].session_epoch)
                                    .unwrap_or(0);
                                let session_slot = session_idx.unwrap_or(0) as u32;

                                // QOP_PUBLISH V2 op-body shape (see wire::QOP_PUBLISH):
                                //   [pub_qos:u8][packet_id:u16 BE]
                                //   [stream_hash:u64 LE][session_epoch:u32 LE]
                                //   [retain:u8][topic_len:u16 BE][topic]
                                //   [user_props_count:u8][per prop ...]
                                //   [payload]
                                let qbody_len = 1 + 2 + 8 + 4 + 1 + 2 + topic_len + up_len + payload_len;
                                let prop_total = if qos > 0 {
                                    wire::QPROP_TAGGED_HDR_LEN + qbody_len
                                } else {
                                    wire::QPROP_UNTAGGED_HDR_LEN + qbody_len
                                };
                                // Apply-side stashes QoS 1+ envelopes in
                                // stash_env (MAX_STASH_ENV bytes). Reject
                                // here if the op-body wouldn't fit; QoS 0
                                // skips the stash so only the proposal-size
                                // bound applies.
                                let stash_oversize = qos > 0 && qbody_len > MAX_STASH_ENV;
                                if prop_total > s.out_buf.len() || stash_oversize {
                                    if qos > 0 {
                                        if let Some(si) = session_idx {
                                            if let Some(ii) = s.sessions[si]
                                                .find_inflight_dir(packet_id, INFLIGHT_PUB)
                                            {
                                                s.sessions[si].inflight[ii].active = 0;
                                            }
                                        }
                                    }
                                    s.publishes_throttled = s.publishes_throttled.wrapping_add(1);
                                    continue;
                                }

                                let cid: u64 = if qos > 0 {
                                    let Some(cid) = s.allocate_correlation(
                                        session_slot, packet_id, OP_PUBLISH,
                                    ) else {
                                        if let Some(si) = session_idx {
                                            if let Some(ii) = s.sessions[si]
                                                .find_inflight_dir(packet_id, INFLIGHT_PUB)
                                            {
                                                s.sessions[si].inflight[ii].active = 0;
                                            }
                                        }
                                        s.correlations_dropped =
                                            s.correlations_dropped.wrapping_add(1);
                                        s.publishes_throttled =
                                            s.publishes_throttled.wrapping_add(1);
                                        continue;
                                    };
                                    // Bind the correlation to the inflight slot
                                    // so MSG_ACK_REDELIVER (apply-side reconstruct
                                    // from stash) and MSG_ACK_EMIT (PUBACK fire)
                                    // can locate this publish.
                                    if let Some(si) = session_idx {
                                        if let Some(ii) = s.sessions[si]
                                            .find_inflight_dir(packet_id, INFLIGHT_PUB)
                                        {
                                            s.sessions[si].inflight[ii].correlation_id = cid;
                                        }
                                    }
                                    cid
                                } else {
                                    0
                                };

                                // Build the proposal envelope. Layout:
                                //   Untagged: [disc=0x01][canonical hdr][op-body]
                                //   Tagged:   [disc=0x02][correlation_id:8][canonical hdr][op-body]
                                // QOP_PUBLISH uses V2 because the op-body
                                // shape has changed (user_props block
                                // interleaved between topic and payload).
                                let hdr_end;
                                if qos > 0 {
                                    s.out_buf[0..8].copy_from_slice(&cid.to_le_bytes());
                                    wire::encode_qprop_header_v(
                                        &mut s.out_buf[8..8 + wire::QPROP_HEADER_LEN],
                                        wire::QPROP_VERSION_V2,
                                        wire::QOP_PUBLISH, tenant, session_slot,
                                    );
                                    hdr_end = wire::QPROP_TAGGED_HDR_LEN;
                                } else {
                                    wire::encode_qprop_header_v(
                                        &mut s.out_buf[0..wire::QPROP_HEADER_LEN],
                                        wire::QPROP_VERSION_V2,
                                        wire::QOP_PUBLISH, tenant, session_slot,
                                    );
                                    hdr_end = wire::QPROP_UNTAGGED_HDR_LEN;
                                }
                                let off = hdr_end;
                                s.out_buf[off] = qos;
                                s.out_buf[off + 1..off + 3]
                                    .copy_from_slice(&packet_id.to_be_bytes());
                                s.out_buf[off + 3..off + 11]
                                    .copy_from_slice(&stream_hash.to_le_bytes());
                                s.out_buf[off + 11..off + 15]
                                    .copy_from_slice(&session_epoch.to_le_bytes());
                                s.out_buf[off + 15] = retain as u8;
                                s.out_buf[off + 16..off + 18]
                                    .copy_from_slice(&(topic_len as u16).to_be_bytes());
                                // Topic, user_props block, and payload are
                                // dynamic length; use copy_nonoverlapping
                                // so bare-metal builds don't link the
                                // copy_from_slice panic path.
                                core::ptr::copy_nonoverlapping(
                                    body.as_ptr().add(4),
                                    s.out_buf.as_mut_ptr().add(off + 18),
                                    topic_len,
                                );
                                core::ptr::copy_nonoverlapping(
                                    body.as_ptr().add(up_off),
                                    s.out_buf.as_mut_ptr().add(off + 18 + topic_len),
                                    up_len,
                                );
                                core::ptr::copy_nonoverlapping(
                                    body.as_ptr().add(payload_off),
                                    s.out_buf.as_mut_ptr().add(off + 18 + topic_len + up_len),
                                    payload_len,
                                );

                                let chan = if qos > 0 {
                                    s.out_proposals_tagged
                                } else {
                                    s.out_proposals
                                };
                                let emit_ok = try_emit(
                                    sys, chan,
                                    wire::MSG_CLIENT_PROPOSAL,
                                    &s.out_buf[..prop_total],
                                );
                                if emit_ok {
                                    s.proposals_emitted =
                                        s.proposals_emitted.wrapping_add(1);
                                } else {
                                    // Proposal didn't reach raft (channel full /
                                    // overload). For QoS 1+ roll back the
                                    // correlation + inflight so the publisher
                                    // times out and DUP-retries cleanly; keeping
                                    // half-state would wedge the slot forever.
                                    if qos > 0 {
                                        let _ = s.take_correlation(cid);
                                        if let Some(si) = session_idx {
                                            if let Some(ii) = s.sessions[si]
                                                .find_inflight_dir(packet_id, INFLIGHT_PUB)
                                            {
                                                s.sessions[si].inflight[ii].active = 0;
                                            }
                                        }
                                    }
                                    // Count the drop for ALL QoS levels so
                                    // offered != accepted stays visible. QoS 0
                                    // was previously dropped silently (no
                                    // accounting) — the brief's "no silent
                                    // drops" invariant.
                                    s.publishes_throttled =
                                        s.publishes_throttled.wrapping_add(1);
                                }
                            }

                            PKT_SUBSCRIBE => {
                                // Propose QOP_SUBSCRIBE; apply-side emits
                                // MSG_TOPIC_SUBSCRIBE to topic_engine and
                                // triggers retained delivery. SUBACK is sent
                                // optimistically — MQTT 3.1.1 §3.9.3 grants
                                // a per-topic-filter reason code immediately;
                                // the actual subscription becomes effective
                                // when topic_engine consumes the apply-side
                                // emission, which is bounded by Raft commit
                                // latency (typically sub-ms in single-node).
                                dev_log(sys, 3, b"[sess] sub".as_ptr(), 10);
                                if body.len() < 5 { continue; }
                                let packet_id = u16::from_be_bytes([body[0], body[1]]);
                                let req_qos = body[2];
                                let topic_len = u16::from_be_bytes([body[3], body[4]]) as usize;
                                if body.len() < 5 + topic_len { continue; }

                                let tenant: TenantId = 0;
                                let session_idx = s.find_session_by_conn(conn_id);
                                let session_slot = session_idx.unwrap_or(0) as u32;
                                let stream_hash = session_idx
                                    .map(|i| s.sessions[i].stream_hash)
                                    .unwrap_or(0);

                                // QOP_SUBSCRIBE body:
                                //   [req_qos:u8][stream_hash:u64 LE]
                                //   [topic_len:u16 BE][topic]
                                let qbody_len = 1 + 8 + 2 + topic_len;
                                let prop_total = wire::QPROP_UNTAGGED_HDR_LEN + qbody_len;
                                if prop_total <= s.out_buf.len() {
                                    wire::encode_qprop_header(
                                        &mut s.out_buf[0..wire::QPROP_HEADER_LEN],
                                        wire::QOP_SUBSCRIBE, tenant, session_slot,
                                    );
                                    let off = wire::QPROP_UNTAGGED_HDR_LEN;
                                    s.out_buf[off] = req_qos;
                                    s.out_buf[off + 1..off + 9]
                                        .copy_from_slice(&stream_hash.to_le_bytes());
                                    s.out_buf[off + 9..off + 11]
                                        .copy_from_slice(&(topic_len as u16).to_be_bytes());
                                    core::ptr::copy_nonoverlapping(
                                        body.as_ptr().add(5),
                                        s.out_buf.as_mut_ptr().add(off + 11),
                                        topic_len,
                                    );
                                    if try_emit(
                                        sys, s.out_proposals,
                                        wire::MSG_CLIENT_PROPOSAL,
                                        &s.out_buf[..prop_total],
                                    ) {
                                        s.proposals_emitted =
                                            s.proposals_emitted.wrapping_add(1);
                                    }
                                }

                                // SUBACK optimistic: [packet_id BE][req_qos]
                                let mut suback = [0u8; 3];
                                suback[0..2].copy_from_slice(&packet_id.to_be_bytes());
                                suback[2] = req_qos;
                                emit_codec_response(sys, s.out_codec, conn_id, PROTO_MQTT, PKT_SUBACK, 0, &suback);
                            }

                            PKT_UNSUBSCRIBE => {
                                if body.len() < 5 { continue; }
                                let packet_id = u16::from_be_bytes([body[0], body[1]]);
                                let topic_len = u16::from_be_bytes([body[3], body[4]]) as usize;
                                if body.len() < 5 + topic_len { continue; }

                                let tenant: TenantId = 0;
                                let session_idx = s.find_session_by_conn(conn_id);
                                let session_slot = session_idx.unwrap_or(0) as u32;
                                let stream_hash = session_idx
                                    .map(|i| s.sessions[i].stream_hash)
                                    .unwrap_or(0);

                                // QOP_UNSUBSCRIBE body:
                                //   [stream_hash:u64 LE][topic_len:u16 BE][topic]
                                let qbody_len = 8 + 2 + topic_len;
                                let prop_total = wire::QPROP_UNTAGGED_HDR_LEN + qbody_len;
                                if prop_total <= s.out_buf.len() {
                                    wire::encode_qprop_header(
                                        &mut s.out_buf[0..wire::QPROP_HEADER_LEN],
                                        wire::QOP_UNSUBSCRIBE, tenant, session_slot,
                                    );
                                    let off = wire::QPROP_UNTAGGED_HDR_LEN;
                                    s.out_buf[off..off + 8]
                                        .copy_from_slice(&stream_hash.to_le_bytes());
                                    s.out_buf[off + 8..off + 10]
                                        .copy_from_slice(&(topic_len as u16).to_be_bytes());
                                    core::ptr::copy_nonoverlapping(
                                        body.as_ptr().add(5),
                                        s.out_buf.as_mut_ptr().add(off + 10),
                                        topic_len,
                                    );
                                    if try_emit(
                                        sys, s.out_proposals,
                                        wire::MSG_CLIENT_PROPOSAL,
                                        &s.out_buf[..prop_total],
                                    ) {
                                        s.proposals_emitted =
                                            s.proposals_emitted.wrapping_add(1);
                                    }
                                }

                                let mut unsuback = [0u8; 2];
                                unsuback.copy_from_slice(&packet_id.to_be_bytes());
                                emit_codec_response(sys, s.out_codec, conn_id, PROTO_MQTT, PKT_UNSUBACK, 0, &unsuback);
                            }

                            PKT_PUBREC => {
                                // Inbound PUBREC is the subscriber's QoS 2
                                // phase-1 ack of a PUBLISH we delivered to them.
                                if body.len() < 2 { continue; }
                                let packet_id = u16::from_be_bytes([body[0], body[1]]);
                                if let Some(si) = s.find_session_by_conn(conn_id) {
                                    if let Some(ii) = s.sessions[si].find_inflight_dir(packet_id, INFLIGHT_SUB) {
                                        if s.sessions[si].inflight[ii].phase == QOS2_PUBLISH {
                                            s.sessions[si].inflight[ii].phase = QOS2_PUBREL;
                                            s.qos2_rec = s.qos2_rec.wrapping_add(1);
                                            let mut pubrel = [0u8; 2];
                                            pubrel.copy_from_slice(&packet_id.to_be_bytes());
                                            emit_codec_response(sys, s.out_codec, conn_id, PROTO_MQTT, PKT_PUBREL, 0x02, &pubrel);
                                        }
                                    }
                                }
                            }

                            PKT_PUBREL => {
                                // Publisher's QoS 2 phase-3 message. Per
                                // MQTT 3.1.1 §4.3.3 + docs/messaging_model.md
                                // the PUBCOMP must only be sent after the
                                // phase-transition is durable. Propose the
                                // PUBREL through Raft as a tagged QOP_PUBREL
                                // entry; the ack_tracker → MSG_ACK_EMIT path
                                // then emits PUBCOMP and frees the publisher
                                // inflight. Apply-side records the phase
                                // transition durably (see apply_qop_pubrel).
                                if body.len() < 2 { continue; }
                                let packet_id = u16::from_be_bytes([body[0], body[1]]);
                                s.qos2_rel = s.qos2_rel.wrapping_add(1);

                                let Some(si) = s.find_session_by_conn(conn_id) else { continue; };
                                // Mark publisher inflight as awaiting PUBREL
                                // durability so a stray PUBCOMP attempt
                                // doesn't fire twice.
                                let inflight_idx = s.sessions[si]
                                    .find_inflight_dir(packet_id, INFLIGHT_PUB);
                                if let Some(ii) = inflight_idx {
                                    s.sessions[si].inflight[ii].phase = QOS2_PUBREL;
                                }

                                let tenant: TenantId = 0;
                                let session_slot = si as u32;
                                let stream_hash = s.sessions[si].stream_hash;
                                let Some(cid) = s.allocate_correlation(
                                    session_slot, packet_id, OP_PUBREL,
                                ) else {
                                    s.correlations_dropped =
                                        s.correlations_dropped.wrapping_add(1);
                                    continue;
                                };

                                // Bind the new correlation to the inflight
                                // slot so MSG_ACK_REDELIVER can reconstruct
                                // a PUBREL retry without consulting the
                                // (already-released) PUBLISH stash.
                                if let Some(ii) = inflight_idx {
                                    s.sessions[si].inflight[ii].correlation_id = cid;
                                }

                                // QOP_PUBREL body:
                                //   [packet_id:u16 BE][stream_hash:u64 LE]
                                let qbody_len = 2 + 8;
                                let prop_total = wire::QPROP_TAGGED_HDR_LEN + qbody_len;
                                let mut emit_ok = false;
                                if prop_total <= s.out_buf.len() {
                                    s.out_buf[0..8].copy_from_slice(&cid.to_le_bytes());
                                    wire::encode_qprop_header(
                                        &mut s.out_buf[8..8 + wire::QPROP_HEADER_LEN],
                                        wire::QOP_PUBREL, tenant, session_slot,
                                    );
                                    let off = wire::QPROP_TAGGED_HDR_LEN;
                                    s.out_buf[off..off + 2]
                                        .copy_from_slice(&packet_id.to_be_bytes());
                                    s.out_buf[off + 2..off + 10]
                                        .copy_from_slice(&stream_hash.to_le_bytes());
                                    if try_emit(
                                        sys, s.out_proposals_tagged,
                                        wire::MSG_CLIENT_PROPOSAL,
                                        &s.out_buf[..prop_total],
                                    ) {
                                        s.proposals_emitted =
                                            s.proposals_emitted.wrapping_add(1);
                                        emit_ok = true;
                                    }
                                }
                                if !emit_ok {
                                    // Release the correlation so retry can
                                    // allocate fresh; the inflight stays in
                                    // QOS2_PUBREL phase so the publisher's
                                    // backoff-timer DUP retry re-enters
                                    // this branch.
                                    let _ = s.take_correlation(cid);
                                    if let Some(ii) = inflight_idx {
                                        s.sessions[si].inflight[ii].correlation_id = 0;
                                    }
                                    s.correlations_dropped =
                                        s.correlations_dropped.wrapping_add(1);
                                }
                                continue;
                            }

                            PKT_PUBCOMP => {
                                // Inbound PUBCOMP completes a QoS 2 delivery
                                // we pushed to the subscriber. Release both
                                // the inflight slot and the subscriber's
                                // prefetch credit so further deliveries can
                                // flow.
                                if body.len() < 2 { continue; }
                                let packet_id = u16::from_be_bytes([body[0], body[1]]);
                                s.qos2_comp = s.qos2_comp.wrapping_add(1);
                                if let Some(si) = s.find_session_by_conn(conn_id) {
                                    if let Some(ii) = s.sessions[si].find_inflight_dir(packet_id, INFLIGHT_SUB) {
                                        s.sessions[si].inflight[ii].active = 0;
                                        if s.sub_outstanding[si] > 0 {
                                            s.sub_outstanding[si] -= 1;
                                            let mut lag_msg = [0u8; 8];
                                            lag_msg[0..4].copy_from_slice(&(si as u32).to_le_bytes());
                                            lag_msg[4..8].copy_from_slice(&s.sub_outstanding[si].to_le_bytes());
                                            try_emit(sys, s.out_forward, wire::MSG_LAG_SIGNAL, &lag_msg);
                                        }
                                    }
                                }
                            }

                            PKT_PUBACK => {
                                // Inbound PUBACK is the subscriber draining
                                // a QoS 1 delivery we pushed.
                                if body.len() < 2 { continue; }
                                let packet_id = u16::from_be_bytes([body[0], body[1]]);
                                if let Some(si) = s.find_session_by_conn(conn_id) {
                                    if let Some(ii) = s.sessions[si].find_inflight_dir(packet_id, INFLIGHT_SUB) {
                                        s.sessions[si].inflight[ii].active = 0;
                                        s.acks_emitted = s.acks_emitted.wrapping_add(1);
                                        if s.sub_outstanding[si] > 0 {
                                            s.sub_outstanding[si] -= 1;
                                            let mut lag_msg = [0u8; 8];
                                            lag_msg[0..4].copy_from_slice(&(si as u32).to_le_bytes());
                                            lag_msg[4..8].copy_from_slice(&s.sub_outstanding[si].to_le_bytes());
                                            try_emit(sys, s.out_forward, wire::MSG_LAG_SIGNAL, &lag_msg);
                                        }
                                    }
                                }
                            }

                            _ => {
                                try_emit(sys, s.out_proposals, wire::MSG_CLIENT_PROPOSAL, body);
                                s.proposals_emitted = s.proposals_emitted.wrapping_add(1);
                            }
                        }
                    }

                    wire::MSG_SESSION_PROPOSAL => {
                        // Kafka/AMQP or unknown — forward to raft
                        try_emit(sys, s.out_proposals, wire::MSG_CLIENT_PROPOSAL, body);
                        s.proposals_emitted = s.proposals_emitted.wrapping_add(1);
                    }

                    _ => {}
                }
            }
        }

        // ── Phase 3: apply committed entries (wire envelope) ──
        //
        // `in_committed` is fanned in from both:
        //   - `apply_pipeline.applied` — MSG_CLIENT_RESPONSE per-batch
        //     notifications (legacy, body-less), bumps `applied`.
        //   - `apply_pipeline.committed_entries` — MSG_COMMITTED_ENTRY
        //     per-entry stream `[term:u64][index:u64][body]`.
        //
        // Per-entry bodies that carry a canonical Quantum envelope
        // (`wire::peel_qprop` succeeds) drive durable state mutation
        // through the apply-side dispatcher below. Entries that don't
        // peel (e.g. raw MQTT bodies from ops still on the legacy
        // propose-side path) advance `applied` without dispatching;
        // those ops keep their existing inline mutations until their
        // QOP_* conversion lands. See docs/apply_side_state_machine.md.
        if s.in_committed >= 0 {
            for _ in 0..16 {
                let poll = (sys.channel_poll)(s.in_committed, 0x01);
                if poll <= 0 || (poll as u32 & 0x01) == 0 { break; }
                let (mt, plen) = wire::channel_read_msg(sys, s.in_committed, &mut s.in_buf);
                if mt == wire::MSG_COMMITTED_ENTRY && plen >= 16 {
                    s.committed_entries_observed =
                        s.committed_entries_observed.wrapping_add(1);
                    let entry_end = plen as usize;
                    // MSG_COMMITTED_ENTRY body: [term:u64][index:u64][entry_body].
                    // Strip the 16-byte prefix and peel the Quantum
                    // canonical envelope (apply_pipeline forwards the
                    // proposer's body verbatim — clustor's tagged-
                    // proposal handler strips any leading correlation_id
                    // upstream).
                    let entry_index = u64::from_le_bytes([
                        s.in_buf[8],  s.in_buf[9],  s.in_buf[10], s.in_buf[11],
                        s.in_buf[12], s.in_buf[13], s.in_buf[14], s.in_buf[15],
                    ]);

                    // Index sequencing (Phase 4 of docs/apply_side_state_machine.md):
                    //   • entry_index <= apply_index  →  duplicate/replay; skip.
                    //   • entry_index == apply_index + 1  →  contiguous; apply.
                    //   • entry_index >  apply_index + 1  →  forward jump (snapshot
                    //     install / leader log truncation). Wipe apply-derived
                    //     state, fan the reset out to downstream modules,
                    //     fast-forward apply_index, then apply the new entry.
                    //
                    // The first entry seen after boot has apply_index==0, so the
                    // gap branch fires unless the entry happens to be at
                    // index 1. That's intentional and harmless: state is
                    // already empty on a fresh boot, so the reset is a no-op
                    // beyond resyncing apply_index.
                    if entry_index <= s.apply_index { continue; }
                    if entry_index > s.apply_index + 1 && s.apply_index > 0 {
                        apply_pipeline_reset(s, sys, entry_index);
                    }
                    if entry_end > 16 {
                        if let Some(p) = wire::peel_qprop(&s.in_buf[16..entry_end]) {
                            let body_start = 16 + p.op_body_offset;
                            apply_committed_op(s, sys, &p, body_start, entry_end, now);
                        }
                    }
                    s.apply_index = entry_index;
                } else if plen > 0 {
                    s.applied = s.applied.wrapping_add(1);
                }
            }
        }

        // ── Phase 3b: drain proposal-assigned events from raft_engine ──
        //
        // Each MSG_PROPOSAL_ASSIGNED carries
        //   [correlation_id:u64 LE][partition_id:u16 LE][wal_index:u64 LE]
        // (18 bytes). We forward `(partition_id, wal_index)` to
        // ack_tracker via MSG_ACK_REGISTER so it can disambiguate the
        // same wal_index arriving from different partitions —
        // ack-on-durability matches by `(partition_id, wal_index)`,
        // not just `wal_index`. Replaces the legacy `wal_index = 0`
        // placeholder + ack_tracker heuristic.
        if s.in_assigned >= 0 {
            for _ in 0..16 {
                let poll = (sys.channel_poll)(s.in_assigned, 0x01);
                if poll <= 0 || (poll as u32 & 0x01) == 0 { break; }
                let (mt, plen) = wire::channel_read_msg(sys, s.in_assigned, &mut s.in_buf);
                if mt != wire::MSG_PROPOSAL_ASSIGNED || (plen as usize) < wire::PROPOSAL_ASSIGNED_LEN {
                    continue;
                }
                let (cid, partition_id, wal_index) =
                    wire::decode_proposal_assigned(&s.in_buf[..wire::PROPOSAL_ASSIGNED_LEN]);
                if let Some((session_slot, packet_id, _op)) = s.take_correlation(cid) {
                    // MSG_ACK_REGISTER (18 bytes):
                    //   [session_slot:u32 LE][packet_id:u32 LE]
                    //   [partition_id:u16 LE][wal_index:u64 LE]
                    // Both OP_PUBLISH and OP_PUBREL register with the same
                    // payload shape; the publisher inflight's `phase` field
                    // distinguishes them when MSG_ACK_EMIT later fires for
                    // the same (session_slot, packet_id) tuple.
                    let mut reg = [0u8; 18];
                    reg[0..4].copy_from_slice(&session_slot.to_le_bytes());
                    reg[4..8].copy_from_slice(&(packet_id as u32).to_le_bytes());
                    reg[8..10].copy_from_slice(&partition_id.to_le_bytes());
                    reg[10..18].copy_from_slice(&wal_index.to_le_bytes());

                    // Stamp the wal_index onto the inflight slot regardless
                    // of register success — the publisher state machine
                    // benefits from this even if the broker→ack_tracker
                    // edge is momentarily saturated.
                    let ssize = session_slot as usize;
                    if ssize < MAX_SESSIONS {
                        if let Some(ii) = s.sessions[ssize]
                            .find_inflight_dir(packet_id, INFLIGHT_PUB)
                        {
                            s.sessions[ssize].inflight[ii].wal_index = wal_index;
                        }
                    }

                    // Try once; on failure park in the retry queue. Phase 5-pre
                    // drains it next tick. Dropping the register would mean
                    // ack_tracker never learns of this inflight entry and the
                    // publisher's PUBACK never fires.
                    if !try_emit(sys, s.out_forward, wire::MSG_ACK_REGISTER, &reg) {
                        let mut parked = false;
                        for j in 0..PENDING_ACK_SLOTS {
                            if s.pending_ack_active[j] == 0 {
                                s.pending_ack_buf[j] = reg;
                                s.pending_ack_active[j] = 1;
                                parked = true;
                                break;
                            }
                        }
                        if !parked {
                            // Pending-ack queue full: the publisher will
                            // eventually time out and retry with DUP set;
                            // dedup_engine catches the duplicate.
                            s.correlations_dropped =
                                s.correlations_dropped.wrapping_add(1);
                        }
                    }
                }
            }
        }

        // ── Phase 4: drain ACK feedback (wire envelope) ──
        //
        // ack_tracker emits MSG_ACK_EMIT once quorum durability lands; only
        // that triggers PUBACK/PUBREC/PUBCOMP to the publisher per the
        // ACK-DURABILITY contract in `docs/messaging_model.md`, AND only at
        // that point is the commit-gated topic_publish stash released to
        // topic_engine. The publisher inflight `phase` distinguishes the
        // QoS 2 PUBLISH-commit and PUBREL-commit acks for the same
        // (session_slot, packet_id) tuple — see `PKT_PUBREL`.
        //
        // MSG_ACK_REDELIVER signals "still waiting for durability, retry
        // backoff window fired" and triggers a fresh re-emission of the
        // tagged proposal (reconstructed from the stashed topic envelope)
        // so a proposal lost between session_processor and raft_engine has
        // a chance to be picked up.
        if s.in_ack >= 0 {
            for _ in 0..8 {
                let poll = (sys.channel_poll)(s.in_ack, 0x01);
                if poll <= 0 || (poll as u32 & 0x01) == 0 { break; }
                let (mt, plen) = wire::channel_read_msg(sys, s.in_ack, &mut s.in_buf);
                if plen < 8 { continue; }
                let session_slot = u32::from_le_bytes([s.in_buf[0], s.in_buf[1], s.in_buf[2], s.in_buf[3]]) as usize;
                let packet_id = u32::from_le_bytes([s.in_buf[4], s.in_buf[5], s.in_buf[6], s.in_buf[7]]) as u16;
                if session_slot >= MAX_SESSIONS { continue; }

                match mt {
                    wire::MSG_ACK_EMIT => {
                        // Accept both active and transient slots. A QoS 1+
                        // PUBLISH proposed immediately after CONNECT can have
                        // its durability proof land before the QOP_CONNECT
                        // apply has flipped the slot to active=1; the leader's
                        // PUBACK still needs to fire optimistically (the
                        // CONNACK already went out) so the publisher's
                        // protocol state machine advances.
                        if s.sessions[session_slot].active != 1
                            && s.sessions[session_slot].transient != 1
                        {
                            continue;
                        }
                        let conn_id = s.sessions[session_slot].conn_id;
                        let Some(ii) = s.sessions[session_slot]
                            .find_inflight_dir(packet_id, INFLIGHT_PUB)
                        else { continue; };
                        let qos = s.sessions[session_slot].inflight[ii].qos;
                        let phase = s.sessions[session_slot].inflight[ii].phase;
                        let correlation_id = s.sessions[session_slot].inflight[ii].correlation_id;

                        // Stash bookkeeping for PUBLISH ops — mark durable
                        // and finalise if dedup already resolved.
                        if correlation_id != 0 {
                            if let Some(stash_idx) = s.find_stash_by_correlation(correlation_id) {
                                s.stash_durable[stash_idx] = 1;
                                if s.stash_dedup_state[stash_idx] != STASH_DEDUP_PENDING {
                                    finalise_stash(s, sys, stash_idx);
                                }
                            }
                        }

                        let body = packet_id.to_be_bytes();
                        if qos == 1 {
                            s.sessions[session_slot].inflight[ii].active = 0;
                            emit_codec_response(sys, s.out_codec, conn_id, PROTO_MQTT, PKT_PUBACK, 0, &body);
                        } else if qos == 2 {
                            if phase == QOS2_PUBLISH {
                                // PUBLISH commit landed; emit PUBREC and
                                // keep inflight alive to await PUBREL.
                                emit_codec_response(sys, s.out_codec, conn_id, PROTO_MQTT, PKT_PUBREC, 0, &body);
                            } else if phase == QOS2_PUBREL {
                                // PUBREL commit landed; emit PUBCOMP and
                                // release the publisher inflight.
                                s.sessions[session_slot].inflight[ii].active = 0;
                                emit_codec_response(sys, s.out_codec, conn_id, PROTO_MQTT, PKT_PUBCOMP, 0, &body);
                                s.qos2_comp = s.qos2_comp.wrapping_add(1);
                            }
                        }
                        s.acks_emitted = s.acks_emitted.wrapping_add(1);
                    }
                    wire::MSG_ACK_REDELIVER => {
                        s.redeliver_signals = s.redeliver_signals.wrapping_add(1);
                        if s.sessions[session_slot].active != 1 { continue; }
                        let Some(ii) = s.sessions[session_slot]
                            .find_inflight_dir(packet_id, INFLIGHT_PUB)
                        else { continue; };
                        let correlation_id = s.sessions[session_slot].inflight[ii].correlation_id;
                        if correlation_id == 0 { continue; }
                        let phase = s.sessions[session_slot].inflight[ii].phase;

                        // For PUBREL-phase inflight, the stash has already
                        // been released after the PUBLISH commit. Resend a
                        // QOP_PUBREL marker carrying the same correlation_id
                        // so ack_tracker can drive PUBCOMP when this round
                        // commits.
                        if phase == QOS2_PUBREL {
                            let tenant: TenantId = 0;
                            let stream_hash = s.sessions[session_slot].stream_hash;
                            let qbody_len = 2 + 8;
                            let prop_total = wire::QPROP_TAGGED_HDR_LEN + qbody_len;
                            if prop_total <= s.out_buf.len() {
                                s.out_buf[0..8]
                                    .copy_from_slice(&correlation_id.to_le_bytes());
                                wire::encode_qprop_header(
                                    &mut s.out_buf[8..8 + wire::QPROP_HEADER_LEN],
                                    wire::QOP_PUBREL, tenant, session_slot as u32,
                                );
                                let off = wire::QPROP_TAGGED_HDR_LEN;
                                s.out_buf[off..off + 2]
                                    .copy_from_slice(&packet_id.to_be_bytes());
                                s.out_buf[off + 2..off + 10]
                                    .copy_from_slice(&stream_hash.to_le_bytes());
                                if try_emit(
                                    sys, s.out_proposals_tagged,
                                    wire::MSG_CLIENT_PROPOSAL,
                                    &s.out_buf[..prop_total],
                                ) {
                                    s.proposals_emitted =
                                        s.proposals_emitted.wrapping_add(1);
                                }
                            }
                            continue;
                        }

                        // PUBLISH phase — reconstruct the tagged QOP_PUBLISH
                        // envelope from the still-active stash. The stash
                        // holds the QOP_PUBLISH op-body verbatim; wrap it in
                        // a fresh tagged envelope and repropose.
                        let Some(stash_idx) = s.find_stash_by_correlation(correlation_id) else { continue; };
                        let env_len = s.stash_env_len[stash_idx] as usize;
                        if env_len < 18 || env_len > MAX_STASH_ENV { continue; }
                        let tenant: TenantId = 0;
                        let prop_total = wire::QPROP_TAGGED_HDR_LEN + env_len;
                        if prop_total > s.out_buf.len() { continue; }
                        s.out_buf[0..8].copy_from_slice(&correlation_id.to_le_bytes());
                        wire::encode_qprop_header(
                            &mut s.out_buf[8..8 + wire::QPROP_HEADER_LEN],
                            wire::QOP_PUBLISH, tenant, session_slot as u32,
                        );
                        let off = wire::QPROP_TAGGED_HDR_LEN;
                        let stash_ptr = s.stash_env[stash_idx].as_ptr();
                        let out_ptr = s.out_buf.as_mut_ptr();
                        core::ptr::copy_nonoverlapping(
                            stash_ptr,
                            out_ptr.add(off),
                            env_len,
                        );
                        if try_emit(
                            sys, s.out_proposals_tagged,
                            wire::MSG_CLIENT_PROPOSAL,
                            &s.out_buf[..prop_total],
                        ) {
                            s.proposals_emitted = s.proposals_emitted.wrapping_add(1);
                        }
                    }
                    _ => {}
                }
            }
        }

        // ── Phase 5-pre: retry any stash slot whose topic emit was
        // deferred under backpressure on a prior tick. Without this,
        // out_topic saturation would silently drop already-durable
        // publishes — the post-durability tail of the fan-out path.
        finalise_durable_stashes(s, sys);

        // Phase 5-pre also drains pending ACK_REGISTERs that were
        // deferred by out_forward backpressure (see PROPOSAL_ASSIGNED
        // handler).
        for i in 0..PENDING_ACK_SLOTS {
            if s.pending_ack_active[i] == 0 { continue; }
            let payload = s.pending_ack_buf[i];
            if try_emit(sys, s.out_forward, wire::MSG_ACK_REGISTER, &payload) {
                s.pending_ack_active[i] = 0;
            }
        }

        // ── Phase 5a: topic deliveries → encode PUBLISH to subscriber
        //
        // topic_engine emits MSG_TOPIC_DELIVER with payload:
        //   [session_slot:u32][sub_qos:u8][_pad:u8;3][tenant:u32]
        //   [topic_len:u16][topic][payload]
        //
        // Drain order (per P1.B defer-not-drop): first retry every
        // backpressured envelope parked in pending_dlv from prior ticks,
        // then accept fresh deliveries. Anything still backpressured stays
        // parked; overflow goes to deliveries_throttled.

        // 5a-pre: drain deferred deliveries.
        for s_idx in 0..PENDING_DLV_SLOTS {
            if s.pending_dlv_active[s_idx] == 0 { continue; }
            let env_len = s.pending_dlv_len[s_idx] as usize;
            let mut buf = [0u8; MAX_PENDING_DLV];
            if env_len > buf.len() {
                s.pending_dlv_active[s_idx] = 0;
                s.pending_dlv_len[s_idx] = 0;
                continue;
            }
            buf[..env_len].copy_from_slice(&s.pending_dlv_env[s_idx][..env_len]);
            match try_deliver(s, sys, &buf[..env_len]) {
                DeliverResult::Delivered | DeliverResult::Dropped => {
                    s.pending_dlv_active[s_idx] = 0;
                    s.pending_dlv_len[s_idx] = 0;
                }
                DeliverResult::Backpressured => {
                    // Still backpressured — keep parked for next tick.
                }
            }
        }

        // 5a-new: accept fresh deliveries.
        if s.in_deliver >= 0 {
            for _ in 0..8 {
                let poll = (sys.channel_poll)(s.in_deliver, 0x01);
                if poll <= 0 || (poll as u32 & 0x01) == 0 { break; }
                let (mt, plen) = wire::channel_read_msg(sys, s.in_deliver, &mut s.in_buf);
                dev_log(sys, 3, b"[sess] deliver rx".as_ptr(), 17);
                if mt != wire::MSG_TOPIC_DELIVER || plen < 14 { continue; }
                let plen = plen as usize;

                let mut buf = [0u8; MAX_PENDING_DLV];
                if plen > buf.len() {
                    // Oversize for both delivery and defer paths; drop.
                    s.deliveries_throttled = s.deliveries_throttled.wrapping_add(1);
                    continue;
                }
                buf[..plen].copy_from_slice(&s.in_buf[..plen]);

                match try_deliver(s, sys, &buf[..plen]) {
                    DeliverResult::Delivered | DeliverResult::Dropped => {
                        dev_log(sys, 3, b"[sess] deliver -> codec".as_ptr(), 23);
                    }
                    DeliverResult::Backpressured => {
                        // Park rather than drop.
                        if let Some(slot) = s.allocate_pending_delivery() {
                            s.pending_dlv_len[slot] = plen as u16;
                            s.pending_dlv_env[slot][..plen].copy_from_slice(&buf[..plen]);
                            s.pending_dlv_active[slot] = 1;
                        } else {
                            s.deliveries_throttled =
                                s.deliveries_throttled.wrapping_add(1);
                        }
                    }
                }
            }
        }

        // ── Phase 5b: drain dedup / retained / offline / group / txn results.
        // MSG_DEDUP_RESULT carries `[dedup_key:[u8;20]][duplicate:u8]`. For
        // QoS 1+ stashed publishes, this is the dedup half of the
        // commit-gating contract; once both this and durability resolve
        // OK, the stashed envelope is emitted to topic_engine.
        if s.in_messaging >= 0 {
            for _ in 0..16 {
                let poll = (sys.channel_poll)(s.in_messaging, 0x01);
                if poll <= 0 || (poll as u32 & 0x01) == 0 { break; }
                let (mt, plen) = wire::channel_read_msg(sys, s.in_messaging, &mut s.in_buf);
                let plen = plen as usize;
                match mt {
                    wire::MSG_DEDUP_RESULT if plen >= 21 => {
                        let mut key = [0u8; 20];
                        key.copy_from_slice(&s.in_buf[..20]);
                        let duplicate = s.in_buf[20];
                        if let Some(stash_idx) = s.find_stash_by_dedup_key(&key) {
                            s.stash_dedup_state[stash_idx] = if duplicate != 0 {
                                STASH_DEDUP_DUPLICATE
                            } else {
                                STASH_DEDUP_OK
                            };
                            if s.stash_durable[stash_idx] != 0 {
                                finalise_stash(s, sys, stash_idx);
                            }
                        }
                    }
                    wire::MSG_RETAINED_READ if plen >= 23 => {
                        // Response body from retained_store (hit only):
                        //   [tenant:u32 LE][topic_hash:u64 LE]
                        //   [session_slot:u32 LE][sub_qos:u8]
                        //   [topic_len:u16 LE][topic_bytes]
                        //   [payload_len:u32 LE][payload_bytes]
                        // Build the MSG_TOPIC_DELIVER envelope this
                        // session's try_deliver expects:
                        //   [session_slot:u32][sub_qos:u8][_pad:u8;3]
                        //   [tenant:u32][topic_len:u16 LE][topic][payload]
                        let tenant = u32::from_le_bytes([
                            s.in_buf[0], s.in_buf[1], s.in_buf[2], s.in_buf[3],
                        ]);
                        // bytes 4..12 are topic_hash — not needed for delivery.
                        let session_slot = u32::from_le_bytes([
                            s.in_buf[12], s.in_buf[13], s.in_buf[14], s.in_buf[15],
                        ]);
                        let sub_qos = s.in_buf[16];
                        let topic_len = u16::from_le_bytes([
                            s.in_buf[17], s.in_buf[18],
                        ]) as usize;
                        if 19 + topic_len + 4 > plen { continue; }
                        let payload_len_off = 19 + topic_len;
                        let payload_len = u32::from_le_bytes([
                            s.in_buf[payload_len_off],
                            s.in_buf[payload_len_off + 1],
                            s.in_buf[payload_len_off + 2],
                            s.in_buf[payload_len_off + 3],
                        ]) as usize;
                        let payload_off = payload_len_off + 4;
                        if payload_off + payload_len > plen { continue; }

                        // MSG_TOPIC_DELIVER includes a (possibly empty)
                        // user_props block between topic and payload —
                        // item 6 contract. Retained deliveries carry
                        // no user properties (retained_store doesn't
                        // store them today), so the block is a single
                        // zero-count byte.
                        let dlv_total = 14 + topic_len + 1 + payload_len;
                        if dlv_total > MAX_PENDING_DLV { continue; }
                        let mut buf = [0u8; MAX_PENDING_DLV];
                        buf[0..4].copy_from_slice(&session_slot.to_le_bytes());
                        buf[4] = sub_qos;
                        buf[5..8].fill(0);
                        buf[8..12].copy_from_slice(&tenant.to_le_bytes());
                        buf[12..14].copy_from_slice(&(topic_len as u16).to_le_bytes());
                        // Copy topic + zero user_props_count + payload
                        // via raw pointers to avoid overlapping borrows
                        // on s.in_buf.
                        let src_topic = s.in_buf.as_ptr().add(19);
                        let dst_topic = buf.as_mut_ptr().add(14);
                        core::ptr::copy_nonoverlapping(src_topic, dst_topic, topic_len);
                        buf[14 + topic_len] = 0;
                        let src_payload = s.in_buf.as_ptr().add(payload_off);
                        let dst_payload = buf.as_mut_ptr().add(14 + topic_len + 1);
                        core::ptr::copy_nonoverlapping(src_payload, dst_payload, payload_len);

                        match try_deliver(s, sys, &buf[..dlv_total]) {
                            DeliverResult::Delivered | DeliverResult::Dropped => {}
                            DeliverResult::Backpressured => {
                                if let Some(slot) = s.allocate_pending_delivery() {
                                    s.pending_dlv_len[slot] = dlv_total as u16;
                                    s.pending_dlv_env[slot][..dlv_total]
                                        .copy_from_slice(&buf[..dlv_total]);
                                    s.pending_dlv_active[slot] = 1;
                                } else {
                                    s.deliveries_throttled =
                                        s.deliveries_throttled.wrapping_add(1);
                                }
                            }
                        }
                    }
                    wire::MSG_OFFLINE_DRAIN if plen >= 6 => {
                        // Body: [session:u32][env_len:u16][env]. Route the
                        // replayed envelope through the standard delivery
                        // path so it picks up flow control + the codec
                        // emit path. Failure parks back in pending_dlv as
                        // with any other backpressured delivery.
                        let env_len = u16::from_le_bytes([
                            s.in_buf[4], s.in_buf[5],
                        ]) as usize;
                        if 6 + env_len > plen { continue; }
                        let mut buf = [0u8; MAX_PENDING_DLV];
                        if env_len > buf.len() { continue; }
                        // Copy the embedded envelope (no session/env_len
                        // prefix) into a local buffer so try_deliver can
                        // operate without holding a borrow on s.in_buf.
                        let src = s.in_buf.as_ptr().add(6);
                        let dst = buf.as_mut_ptr();
                        core::ptr::copy_nonoverlapping(src, dst, env_len);
                        match try_deliver(s, sys, &buf[..env_len]) {
                            DeliverResult::Delivered | DeliverResult::Dropped => {}
                            DeliverResult::Backpressured => {
                                if let Some(slot) = s.allocate_pending_delivery() {
                                    s.pending_dlv_len[slot] = env_len as u16;
                                    s.pending_dlv_env[slot][..env_len]
                                        .copy_from_slice(&buf[..env_len]);
                                    s.pending_dlv_active[slot] = 1;
                                } else {
                                    s.deliveries_throttled =
                                        s.deliveries_throttled.wrapping_add(1);
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
        // The other inputs (in_cp, etc.) are still pure no-ops.
        if s.in_cp >= 0 {
            for _ in 0..8 {
                let poll = (sys.channel_poll)(s.in_cp, 0x01);
                if poll <= 0 || (poll as u32 & 0x01) == 0 { break; }
                let (_, _) = wire::channel_read_msg(sys, s.in_cp, &mut s.in_buf);
            }
        }

        // ── Phase 5c: keep-alive timeout sweep.
        //
        // MQTT 3.1.1 §3.1.2.10 mandates the server disconnect a client
        // that hasn't transmitted within 1.5 × keep_alive. peer_router
        // clears its own connection slot on NMSG_CLOSED but doesn't
        // notify the session layer, so abrupt TCP closes would otherwise
        // leave sessions wedged active. The check runs once per
        // metrics-tick (~1 Hz) so the scan cost is bounded.
        //
        // Propose-then-apply: the sweep proposes QOP_DISCONNECT with
        // REASON_KEEPALIVE; the durable state mutation lands at
        // apply-side. We also clear the local conn_id so further
        // packets on the orphaned socket don't re-route to a slot
        // whose disconnect is in flight.
        if now.wrapping_sub(s.last_metrics_ms) >= 1000 {
            for i in 0..MAX_SESSIONS {
                if s.sessions[i].active != 1 { continue; }
                let kalive = s.sessions[i].keep_alive_ms;
                if kalive == 0 { continue; }
                let deadline = (kalive as u64).saturating_mul(3) / 2;
                let age = now.wrapping_sub(s.sessions[i].last_activity_ms);
                if age <= deadline { continue; }

                s.disconnects = s.disconnects.wrapping_add(1);
                let tenant = s.sessions[i].tenant;
                let stream_hash = s.sessions[i].stream_hash;
                let protocol = s.sessions[i].protocol;
                let session_slot = i as u32;
                s.sessions[i].conn_id = 0;

                let qbody_len = 1 + 8;
                let prop_total = wire::QPROP_UNTAGGED_HDR_LEN + qbody_len;
                if prop_total <= s.out_buf.len() {
                    wire::encode_qprop_header(
                        &mut s.out_buf[0..wire::QPROP_HEADER_LEN],
                        wire::QOP_DISCONNECT, tenant, session_slot,
                    );
                    let off = wire::QPROP_UNTAGGED_HDR_LEN;
                    s.out_buf[off] = wire::QDISC_REASON_KEEPALIVE;
                    s.out_buf[off + 1..off + 9]
                        .copy_from_slice(&stream_hash.to_le_bytes());
                    if try_emit(
                        sys, s.out_proposals,
                        wire::MSG_CLIENT_PROPOSAL,
                        &s.out_buf[..prop_total],
                    ) {
                        s.proposals_emitted = s.proposals_emitted.wrapping_add(1);
                    }
                }

                let mut ev = [0u8; 2];
                ev[0] = AUDIT_DISCONNECT;
                ev[1] = protocol;
                try_emit(sys, s.out_audit, wire::MSG_AUDIT_EVENT, &ev);
            }

            // ── Phase 5d: MQTT 5 session-expiry sweep.
            //
            // Walk persisted (disconnected, non-active) slots and purge
            // any whose `disconnected_at + session_expiry_s * 1000`
            // deadline has elapsed. The purge is local-only: subscription
            // state lives in topic_engine and gets cleared via
            // MSG_SESSION_DROP; offline_queue clears via the same fan-out
            // pattern keyed on session_slot. session_expiry_s == 0 is
            // already short-circuited to a fast-drop in
            // apply_qop_disconnect so the persisted path never carries
            // a "0 means immediate" slot. u32::MAX is the "never expire"
            // sentinel (also covers MQTT 3.1.1 clean_session=0 thanks
            // to the propose-side normalisation).
            for i in 0..MAX_SESSIONS {
                if s.sessions[i].persisted != 1 { continue; }
                if s.sessions[i].active == 1 { continue; }
                let expiry_s = s.sessions[i].session_expiry_s;
                if expiry_s == 0 || expiry_s == u32::MAX { continue; }
                let deadline_ms = (expiry_s as u64).saturating_mul(1000);
                let elapsed = now.wrapping_sub(s.sessions[i].disconnected_at_ms);
                if elapsed < deadline_ms { continue; }

                // Purge.
                s.sub_outstanding[i] = 0;
                s.prefetch_credit[i] = 0;
                s.sessions[i].persisted = 0;
                s.sessions[i].session_expiry_s = 0;
                s.sessions[i].disconnected_at_ms = 0;
                s.sessions[i].inflight = [Inflight::zero(); MAX_INFLIGHT_PER_SESSION];
                let drop_body = (i as u32).to_le_bytes();
                try_emit(sys, s.out_topic, wire::MSG_SESSION_DROP, &drop_body);
                s.sessions[i] = Session::zero();
            }

            // ── Phase 5e: deferred Will-message fire sweep.
            //
            // MQTT 5 §3.1.3.2.2 — Will Delay Interval. When
            // apply_qop_disconnect handles a non-CLEAN disconnect on
            // a Will-bearing session with `will_delay_ms > 0`, the
            // Will fire is deferred to `now + will_delay_ms`,
            // recorded as `pending_will_fire_at_ms`. The slot keeps
            // its Will fields populated. A fresh CONNECT in the
            // meantime cancels the deferred fire (apply_qop_connect
            // clears `pending_will_fire_at_ms`); otherwise this
            // sweep runs the fire when the deadline elapses.
            for i in 0..MAX_SESSIONS {
                let fire_at = s.sessions[i].pending_will_fire_at_ms;
                if fire_at == 0 { continue; }
                if now < fire_at { continue; }
                if s.sessions[i].will_present == 1 {
                    let t = s.sessions[i].tenant;
                    fire_will(s, sys, t, i);
                }
                s.sessions[i].pending_will_fire_at_ms = 0;
                s.sessions[i].will_present = 0;
                s.sessions[i].will_topic_len = 0;
                s.sessions[i].will_payload_len = 0;
            }
        }

        // ── Phase 6: metrics (wire envelope) ──
        if now.wrapping_sub(s.last_metrics_ms) >= 1000 && s.out_metrics >= 0 {
            s.last_metrics_ms = now;
            let mut m = [0u8; 40];
            m[0..4].copy_from_slice(&s.session_count.to_le_bytes());
            m[4..8].copy_from_slice(&s.connects.to_le_bytes());
            m[8..12].copy_from_slice(&s.disconnects.to_le_bytes());
            m[12..16].copy_from_slice(&s.publishes.to_le_bytes());
            m[16..20].copy_from_slice(&s.applied.to_le_bytes());
            m[20..24].copy_from_slice(&s.proposals_emitted.to_le_bytes());
            m[24..28].copy_from_slice(&s.acks_emitted.to_le_bytes());
            m[28..32].copy_from_slice(&s.qos2_rec.to_le_bytes());
            m[32..36].copy_from_slice(&s.qos2_rel.to_le_bytes());
            m[36..40].copy_from_slice(&s.qos2_comp.to_le_bytes());
            try_emit(sys, s.out_metrics, wire::MSG_METRICS, &m);
        }

        0
    }
}
