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
//!   in[1] committed_in  — applied entries from consensus
//!   in[2] flow_in       — PID credits + backpressure + prefetch
//!   in[3] ack_in        — ACK emit + redeliver from `flow::ack`
//!   in[4] cp_in         — capabilities + epoch + disconnect
//!   in[5] messaging_in  — dedup/offline/retained/group/txn results
//!   in[6] deliver_in    — topic deliveries
//!   out[0] proposals    — to consensus (fluxor wire envelope)
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
include!("../../../target/fluxor/fluxor-abi/sdk/runtime/params.rs");

#[path = "../../common/wire.rs"]
mod wire;

mod consumers;
mod correlate;
mod sessions;
mod store;

/// Kernel step ABI: 0=Continue, 1=Done, 2=Burst, 3=Ready. Returning
/// Burst re-runs the domain's exec rotation within the same tick (up to
/// the kernel's pass cap), so a record consumed here reaches its
/// consumer in this tick instead of the next. We burst only when a
/// record was actually consumed: an idle graph reports no burst and the
/// tick costs exactly one pass, as before.
const STEP_BURST: i32 = 2;

#[path = "../../common/types.rs"]
mod types;

use types::*;

const MAX_SESSIONS: usize = 1024;
const MAX_INFLIGHT_PER_SESSION: usize = 16;
/// Read/write buffer cap for codec ↔ session_processor and
/// session_processor ↔ topic_engine / forward_coordinator /
/// messaging-fan-out traffic. Sized to the wire-channel per-message
/// capacity (`fluxor-abi::CHANNEL_BUFFER_SIZE` = 8192) so a worst-case
/// MQTT packet (`protocol::mqtt`'s `MAX_PACKET` = 4096) plus routing headers
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
/// the stash and are dropped, preserving the commit-gating invariant for
/// the in-budget path.
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
/// is lost, the ack component never learns about the inflight entry and the
/// publisher's PUBACK never fires. Each entry is the fixed-size 18-byte
/// register payload (see PROPOSAL_ASSIGNED handler).
const PENDING_ACK_SLOTS: usize = 64;

// MQTT packet types (mirrored in `protocol::mqtt`)
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
/// the offline queue + subscription resume unreachable.
/// Per-session Will-message storage. Cap chosen to keep the per-slot
/// memory bounded while covering typical MQTT IoT payloads (status,
/// last-known-value strings). Larger Wills are rejected at propose
/// time so the apply-side slot never has to deal with a partial Will.
const MAX_WILL_TOPIC: usize = 256;
const MAX_WILL_PAYLOAD: usize = 512;

/// MQTT 5 User Property caps. The codec parses inbound publishes and
/// drops entries exceeding any cap so the QOP_PUBLISH V2 body — and
/// every downstream envelope — stays bounded. Mirrors the constants in
/// `protocol::mqtt`.
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



/// Operation identifier for tagged Raft proposals, so the ack component's
/// `MSG_ACK_EMIT` can route to the right MQTT response (PUBACK / PUBREC /
/// PUBCOMP) per the QoS state machine.
const OP_PUBLISH: u8 = 0;
const OP_PUBREL: u8 = 1;
/// Kafka Produce awaiting quorum durability (ProduceResponse gating).
const OP_KPRODUCE: u8 = 2;

// ── Durable-publish inflight (generic; shared by Kafka + AMQP) ───────────────
//
// A protocol-neutral "publish this opaque body and complete once it reaches
// the configured durability level" primitive. Kafka `acks`, AMQP
// publisher-confirms, and (eventually) MQTT QoS 1/2 are the SAME primitive
// at different QoS levels: a slot holds the completion context, a tagged
// proposal carries it through raft, and the durability round-trip
// (MSG_PROPOSAL_ASSIGNED → ack → MSG_ACK_EMIT) fires the completion.
//
// The round-trip reuses the ack component unchanged by encoding the slot into a
// reserved `session_slot` namespace that the ack component treats as opaque and
// the MQTT bounds check rejects before the Kafka/AMQP path claims it:
//
//   session_slot = KAFKA_SLOT_BASE | (epoch << KIN_EPOCH_SHIFT) | ki
//
// EPOCH TAGGING (the correctness spine): every allocation bumps the slot's
// epoch, and the epoch travels in `session_slot`. A stale
// MSG_PROPOSAL_ASSIGNED / MSG_ACK_EMIT — one whose slot was freed by the
// expiry sweep and REUSED by a different request (possibly a different
// protocol) before durability landed — arrives carrying the OLD epoch, so
// the `slot.epoch == decoded_epoch` check fails and it is dropped instead
// of resolving against the new occupant. Without this, an 8-second
// durability stall (exactly what triggers the sweep) could fire a Kafka
// produce's durability as a spurious AMQP Basic.Ack, or report a foreign
// WAL index as a base_offset. See docs/architecture/flow_control.md.

/// Reserved session_slot namespace for durable-publish inflights. Bit 30
/// set, far above MAX_SESSIONS (1024) so the two spaces can never collide.
const KAFKA_SLOT_BASE: u32 = 0x4000_0000;
const KAFKA_INFLIGHT: usize = 256;
/// Slot index occupies the low 8 bits (KAFKA_INFLIGHT ≤ 256).
const KIN_KI_MASK: u32 = 0xFF;
/// Epoch occupies bits 8..30 (22 bits) — below KAFKA_SLOT_BASE's bit 30 so
/// the `>= KAFKA_SLOT_BASE` routing check is unaffected. Wraps every ~4M
/// allocations per slot; a collision needs the SAME slot reused exactly
/// 2^22 times within one stalled proposal's lifetime — impossible.
const KIN_EPOCH_SHIFT: u32 = 8;
const KIN_EPOCH_MASK: u32 = 0x3F_FFFF;


/// Bounds accepted topic names on the produce path (Metadata-side cap in
/// `protocol::kafka` uses the same value).
const KAFKA_MAX_TOPIC: usize = 64;
/// Drop produce inflights that never saw a durability ack (leadership
/// churn, lost proposal). The producer's own request timeout re-sends.
const KAFKA_INFLIGHT_TIMEOUT_MS: u64 = 8000;

/// Kafka error codes surfaced on the produce path.
const KERR_NONE: i16 = 0;
const KERR_REQUEST_TIMED_OUT: i16 = 7;
const KERR_MESSAGE_TOO_LARGE: i16 = 10;
const KERR_INVALID_REQUEST: i16 = 42;

/// Largest record batch accepted per Produce request. The clustor
/// substrate pins log entries at ~2 KiB end to end (raft
/// PROPOSAL_BATCH_CAP, wal MEMORY_ENTRY_BODY_CAP, and the replay
/// validator all use 2048); a proposal that can't fit is dropped after
/// the ack path is armed, which reads as silent loss. Enforce the
/// ceiling here with MESSAGE_TOO_LARGE so producers fail fast and
/// size their batches down (the protocol's designed-for response).
/// 1900 = 2048 − tagged header (18) − op-body framing (4) − max topic
/// (64) − slack.
const KAFKA_MAX_RECORDS_BYTES: usize = 1900;

/// Which protocol a durability-gated publish inflight belongs to.
/// Dispatches the MSG_ACK_EMIT handling: Kafka → ProduceResponse,
/// AMQP → Basic.Ack (publisher confirm).
const KIN_PROTO_KAFKA: u8 = 1;
const KIN_PROTO_AMQP: u8 = 2;

/// Max partitions serviced in ONE Produce request (one topic). The
/// partition index is packed into the correlation `packet_id` high
/// byte (`ki | part_idx << 8`), so each partition's durability ack
/// resolves independently and the ProduceResponse fires once all land.
const KIN_MAX_PARTS: usize = 16;



// ── Kafka consumer groups + committed offsets ──────────────────────────────
//
// Broker-side group membership is deliberately thin: the ASSIGNMENT is
// computed client-side by the group leader (standard Kafka protocol —
// JoinGroup returns the member list to the leader, the leader pushes
// per-member assignments via SyncGroup, the broker stores and echoes
// them). The broker's job is member identity, generation numbering, and
// staleness signaling (ILLEGAL_GENERATION / REBALANCE_IN_PROGRESS).
// Group state is leader-local (rebuilt by clients rejoining after
// failover); committed offsets are durable via QOP_KAFKA_OFFSET.

const KGROUPS: usize = 8;
const KGROUP_MEMBERS: usize = 8;
const KG_NAME: usize = 48;
const KG_META: usize = 192;
const KOFFSETS: usize = 64;

/// Kafka error codes used by the group APIs.
const KERR_ILLEGAL_GENERATION: i16 = 22;
const KERR_UNKNOWN_MEMBER_ID: i16 = 25;
const KERR_REBALANCE_IN_PROGRESS: i16 = 27;







// ── AMQP push consumers (Basic.Consume) ─────────────────────────────────────
//
// Registered by `protocol::amqp` op=3; the delivery pump in module_step pushes
// raw store entries as op=3 responses (Basic.Deliver) whenever a
// consumer has prefetch credit. Manual-ack consumers hold credit until
// the client's Basic.Ack (op=5) releases it; no_ack consumers never
// hold credit. No redelivery-on-nack in v1 (Nack/Reject release credit
// and count).

const ACONSUMERS: usize = 16;
const AMQP_TAG_MAX: usize = 48;
/// Deliveries pushed per consumer per tick (pump pacing).
const AMQP_DELIVER_QUOTA: usize = 4;

















// ── Apply-side message store (Kafka Fetch / AMQP Basic.Get) ────────────────
//
// Committed QOP_KAFKA_PRODUCE / QOP_AMQP_PUBLISH bodies land here, keyed
// by (topic, partition). Offsets are CONTIGUOUS LOGICAL offsets assigned
// at apply time (per-partition counter), deterministic across replay
// because apply order is the log order — NOT the sparse WAL indexes the
// ProduceResponse reports (that divergence is documented; consumers
// position via ListOffsets, not produce responses).
//
// Storage is a bounded per-partition byte ring of entries:
//   [len:u16 LE][flags:u8][offset:u64 LE][nrec:u32 LE][data ...]
// flags bit0: 1 = Kafka record batch v2 (baseOffset already patched to
// the logical offset), 0 = raw AMQP payload. Kafka Fetch serves only
// batch entries; AMQP Basic.Get serves only raw entries — the namespaces
// share a topic table without corrupting each other. Oldest entries are
// evicted when the ring fills; `log_start` tracks the oldest offset
// still resident. Purely apply-derived state: wiped on
// MSG_APPLY_PIPELINE_RESET and rebuilt by replay.

// One topic advertised at the max 16 partitions must fit without
// exhausting the store, so KSTORE_PARTS covers a full max-partition
// topic plus headroom. Ring size keeps total store state bounded at
// 24 × 16 KiB = 384 KiB.
const KSTORE_PARTS: usize = 24;
const KSTORE_BYTES: usize = 16384;
const KENTRY_HDR: usize = 2 + 1 + 8 + 4;
const KFLAG_BATCH: u8 = 1;
/// `len` sentinel marking "wrap to ring start" (never a real entry len).
const KWRAP: u16 = 0xFFFF;





/// A correlation whose assignment never returns is reclaimed after this
/// long. Held well above the durability-round-trip tail (fsync stalls,
/// leadership churn) so a legitimately-slow assignment is never dropped.
const CORRELATION_TIMEOUT_MS: u64 = 30_000;

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

    sessions: sessions::Sessions,
    /// Per-subscriber prefetch credit set by the prefetch component.
    /// Populated from MSG_PREFETCH_CREDIT on flow_in; 0 = unset / no cap.
    /// Enforced on the MSG_TOPIC_DELIVER push path.
    /// QoS 1+ deliveries pushed to each subscriber but not yet acked by
    /// that subscriber. Incremented on successful delivery; decremented
    /// on inbound PUBACK. Drives the subscriber-backlog MSG_LAG_SIGNAL
    /// feedback loop without resetting on every credit refresh.
    correlate: correlate::Correlate,

    // ── Stash for commit-gated QoS 1+ topic fan-out ──
    //
    // Each slot pairs a tagged Raft correlation_id with the
    // MSG_TOPIC_PUBLISH envelope we'll emit only once durability and
    // dedup both resolve. A slot is free when stash_correlation == 0.

    // ── Defer queue for backpressured deliveries ──
    //
    // When a MSG_TOPIC_DELIVER would be dropped (prefetch, PID, or
    // inflight exhaustion), the raw envelope is parked here and drained
    // ahead of fresh deliveries on subsequent ticks.

    // ── Defer queue for failed ACK_REGISTER emissions ──

    // ── Kafka produce inflight table (see KAFKA_SLOT_BASE) ──
    kafka_produce_rx: u32,
    kafka_produce_acked: u32,
    kafka_produce_errors: u32,

    // ── Apply-side message store + AMQP counters ──
    store: store::Store,
    kafka_fetch_rx: u32,
    amqp_publish_rx: u32,
    amqp_publish_acked: u32,
    /// Confirm-mode publishes rejected (unroutable/oversize) — each
    /// sends the client a Basic.Nack rather than black-holing the
    /// confirm.
    amqp_publish_errors: u32,
    amqp_get_rx: u32,

    // ── Kafka groups / offsets + AMQP consumers ──
    consumers: consumers::Consumers,
    kafka_group_ops: u32,
    kafka_offset_commits: u32,
    amqp_delivers: u32,
    amqp_consumer_acks: u32,

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
    /// `docs/architecture/apply_path.md §Apply-pipeline reset. Reset to the
    /// new index after `apply_reset` clears state.
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
        s.apply_index = 0;
        s.apply_resets = 0;
        for i in 0..MAX_SESSIONS {
            sessions::clear(&mut s.sessions, i);
        sessions::clear_flow(&mut s.sessions, i);
        }
        correlate::init(&mut s.correlate);
        store::init(&mut s.store);
        s.kafka_produce_rx = 0;
        s.kafka_produce_acked = 0;
        s.kafka_produce_errors = 0;

        s.kafka_fetch_rx = 0;
        s.store.full = 0;
        s.amqp_publish_rx = 0;
        s.amqp_publish_acked = 0;
        s.amqp_publish_errors = 0;
        s.amqp_get_rx = 0;
        consumers::init(&mut s.consumers);
        s.kafka_group_ops = 0;
        s.kafka_offset_commits = 0;
        s.amqp_delivers = 0;
        s.amqp_consumer_acks = 0;
        dev_log(sys, 3, b"[sess] init v9".as_ptr(), 14);
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

#[cfg(feature = "kafka")]
/// Emit a Kafka `MSG_SESSION_RESPONSE` to out_codec. Kafka responses use
/// a wider header than the MQTT `[conn][proto][pkt][flags]` form so the
/// codec can reframe by correlation id without any session state:
///   `[conn_id:u8][proto=1][api_key:i16 LE][api_ver:i16 LE]
///    [corr_id:i32 LE][body...]`
///
/// # Safety
unsafe fn emit_kafka_response(
    sys: &SyscallTable, chan: i32,
    conn_id: u8, api_key: i16, api_ver: i16, kafka_corr: i32, body: &[u8],
) -> bool {
    if chan < 0 { return false; }
    let total = 10 + body.len();
    if total > BUF_SIZE { return false; }
    let mut out = [0u8; 256];
    if total > out.len() { return false; }
    out[0] = conn_id;
    out[1] = 1; // PROTO_KAFKA envelope discriminator
    out[2..4].copy_from_slice(&api_key.to_le_bytes());
    out[4..6].copy_from_slice(&api_ver.to_le_bytes());
    out[6..10].copy_from_slice(&kafka_corr.to_le_bytes());
    out[10..total].copy_from_slice(body);
    let w = wire::channel_write_msg(sys, chan, wire::MSG_SESSION_RESPONSE, &out[..total]);
    w > 0
}

#[cfg(feature = "kafka")]
/// Build + emit a ProduceResponse for one (topic, partition). Non-flexible
/// encoding, valid for api_version 2..=8:
///   [topics:i32=1][topic:str][partitions:i32=1]
///     [partition:i32][error:i16][base_offset:i64]
///     [log_append_time:i64 = -1]           (v2+)
///     [log_start_offset:i64 = 0]           (v5+)
///   [throttle_time_ms:i32 = 0]             (v1+, trailing)
///
/// # Safety
unsafe fn emit_kafka_produce_response(
    sys: &SyscallTable, chan: i32,
    conn_id: u8, api_ver: i16, kafka_corr: i32,
    topic: &[u8], partition: i32, error: i16, base_offset: i64,
) -> bool {
    if topic.len() > KAFKA_MAX_TOPIC { return false; }
    let mut body = [0u8; 128];
    let mut p = 0usize;
    body[p..p + 4].copy_from_slice(&1i32.to_be_bytes()); p += 4;
    body[p..p + 2].copy_from_slice(&(topic.len() as i16).to_be_bytes()); p += 2;
    body[p..p + topic.len()].copy_from_slice(topic); p += topic.len();
    body[p..p + 4].copy_from_slice(&1i32.to_be_bytes()); p += 4;
    body[p..p + 4].copy_from_slice(&partition.to_be_bytes()); p += 4;
    body[p..p + 2].copy_from_slice(&error.to_be_bytes()); p += 2;
    body[p..p + 8].copy_from_slice(&base_offset.to_be_bytes()); p += 8;
    if api_ver >= 2 {
        body[p..p + 8].copy_from_slice(&(-1i64).to_be_bytes()); p += 8;
    }
    if api_ver >= 5 {
        body[p..p + 8].copy_from_slice(&0i64.to_be_bytes()); p += 8;
    }
    if api_ver >= 1 {
        body[p..p + 4].copy_from_slice(&0i32.to_be_bytes()); p += 4;
    }
    emit_kafka_response(sys, chan, conn_id, 0, api_ver, kafka_corr, &body[..p])
}

#[cfg(feature = "kafka")]
/// Handle a Kafka Produce request forwarded by `protocol::kafka`. Payload in
/// `s.in_buf[..plen]`:
///   `[conn_id][proto=1][api_key:i16 LE][api_ver:i16 LE][corr:i32 LE]
///    [request body after client_id]`
///
/// One topic per request, up to KIN_MAX_PARTS partitions (each becomes
/// its own tagged proposal; the ProduceResponse aggregates and fires
/// once every partition's quorum durability lands). Multi-topic
/// requests get INVALID_REQUEST — clients then retry per topic.
///
/// acks=0  → untagged fire-and-forget proposals, no response (protocol
///           forbids one).
/// acks!=0 → tagged proposals; base_offset = each entry's WAL index.
///
/// # Safety
unsafe fn handle_kafka_produce(
    s: &mut ModuleState, sys: &SyscallTable, now: u64, plen: usize,
) {
    let conn_id = s.in_buf[0];
    let api_ver = i16::from_le_bytes([s.in_buf[4], s.in_buf[5]]);
    let kafka_corr = i32::from_le_bytes([
        s.in_buf[6], s.in_buf[7], s.in_buf[8], s.in_buf[9],
    ]);
    let req_end = plen;
    let mut off = 10usize;
    let mut parse_ok = true;
    macro_rules! need {
        ($n:expr) => {
            if off + $n > req_end { parse_ok = false; }
        };
    }

    // transactional_id (nullable string, v3+)
    if api_ver >= 3 {
        need!(2);
        if parse_ok {
            let tl = i16::from_be_bytes([s.in_buf[off], s.in_buf[off + 1]]);
            off += 2;
            if tl > 0 {
                off += tl as usize;
                if off > req_end { parse_ok = false; }
            }
        }
    }
    let mut acks: i16 = -1;
    if parse_ok {
        need!(6);
        if parse_ok {
            acks = i16::from_be_bytes([s.in_buf[off], s.in_buf[off + 1]]);
            off += 2 + 4; // acks + timeout_ms
        }
    }
    let mut topic_count: i32 = 0;
    if parse_ok {
        need!(4);
        if parse_ok {
            topic_count = i32::from_be_bytes([
                s.in_buf[off], s.in_buf[off + 1], s.in_buf[off + 2], s.in_buf[off + 3],
            ]);
            off += 4;
        }
    }
    let mut topic = [0u8; KAFKA_MAX_TOPIC];
    let mut topic_len = 0usize;
    let mut part_count: i32 = 0;
    let mut p_ids = [0i32; KIN_MAX_PARTS];
    let mut p_offs = [0usize; KIN_MAX_PARTS]; // records byte offset in in_buf
    let mut p_lens = [0usize; KIN_MAX_PARTS];
    let mut p_errs = [0i16; KIN_MAX_PARTS];
    if parse_ok && topic_count >= 1 {
        need!(2);
        if parse_ok {
            let tl = i16::from_be_bytes([s.in_buf[off], s.in_buf[off + 1]]) as usize;
            off += 2;
            if tl == 0 || tl > KAFKA_MAX_TOPIC || off + tl > req_end {
                parse_ok = false;
            } else {
                topic[..tl].copy_from_slice(&s.in_buf[off..off + tl]);
                topic_len = tl;
                off += tl;
            }
        }
        need!(4);
        if parse_ok {
            part_count = i32::from_be_bytes([
                s.in_buf[off], s.in_buf[off + 1], s.in_buf[off + 2], s.in_buf[off + 3],
            ]);
            off += 4;
        }
        if parse_ok && (1..=KIN_MAX_PARTS as i32).contains(&part_count) {
            for pi in 0..part_count as usize {
                need!(8);
                if !parse_ok { break; }
                p_ids[pi] = i32::from_be_bytes([
                    s.in_buf[off], s.in_buf[off + 1], s.in_buf[off + 2], s.in_buf[off + 3],
                ]);
                off += 4;
                let rlen = i32::from_be_bytes([
                    s.in_buf[off], s.in_buf[off + 1], s.in_buf[off + 2], s.in_buf[off + 3],
                ]);
                off += 4;
                if rlen <= 0 || off + rlen as usize > req_end {
                    parse_ok = false;
                    break;
                }
                p_offs[pi] = off;
                p_lens[pi] = rlen as usize;
                off += rlen as usize;
                if p_ids[pi] < 0 {
                    p_errs[pi] = KERR_INVALID_REQUEST;
                } else if p_lens[pi] > KAFKA_MAX_RECORDS_BYTES {
                    p_errs[pi] = KERR_MESSAGE_TOO_LARGE;
                }
            }
        } else if parse_ok {
            parse_ok = false;
        }
    } else if parse_ok {
        parse_ok = false;
    }

    s.kafka_produce_rx = s.kafka_produce_rx.wrapping_add(1);

    if !parse_ok || topic_count != 1 {
        s.kafka_produce_errors = s.kafka_produce_errors.wrapping_add(1);
        if topic_len > 0 && acks != 0 {
            emit_kafka_produce_response(
                sys, s.out_codec, conn_id, api_ver, kafka_corr,
                &topic[..topic_len], if part_count >= 1 { p_ids[0] } else { 0 },
                KERR_INVALID_REQUEST, -1,
            );
        }
        return;
    }
    let n_parts = part_count as usize;

    // Emit one proposal per healthy partition; per-partition failures
    // stay in p_errs and surface in the aggregated response.
    if acks == 0 {
        for pi in 0..n_parts {
            if p_errs[pi] != 0 { continue; }
            let op_body_len = 2 + 2 + topic_len + p_lens[pi];
            let hdr = wire::QPROP_HEADER_LEN;
            if hdr + op_body_len > s.out_buf.len() { continue; }
            wire::encode_qprop_header(
                &mut s.out_buf[..hdr], wire::QOP_KAFKA_PRODUCE, 0, 0,
            );
            s.out_buf[hdr..hdr + 2].copy_from_slice(&(p_ids[pi] as u16).to_le_bytes());
            s.out_buf[hdr + 2..hdr + 4].copy_from_slice(&(topic_len as u16).to_le_bytes());
            s.out_buf[hdr + 4..hdr + 4 + topic_len].copy_from_slice(&topic[..topic_len]);
            core::ptr::copy_nonoverlapping(
                s.in_buf.as_ptr().add(p_offs[pi]),
                s.out_buf.as_mut_ptr().add(hdr + 4 + topic_len),
                p_lens[pi],
            );
            if try_emit(
                sys, s.out_proposals, wire::MSG_CLIENT_PROPOSAL,
                &s.out_buf[..hdr + op_body_len],
            ) {
                s.proposals_emitted = s.proposals_emitted.wrapping_add(1);
            }
        }
        return;
    }

    // Ack-gated path: one inflight slot aggregates all partitions.
    let Some((ki, slot)) = store::inflight_alloc(&mut s.store) else {
        s.kafka_produce_errors = s.kafka_produce_errors.wrapping_add(1);
        emit_kafka_produce_response(
            sys, s.out_codec, conn_id, api_ver, kafka_corr,
            &topic[..topic_len], p_ids[0], KERR_REQUEST_TIMED_OUT, -1,
        );
        return;
    };

    let mut pending = 0u8;
    for pi in 0..n_parts {
        if p_errs[pi] != 0 { continue; }
        let packet_id = (ki as u16) | ((pi as u16) << 8);
        let Some(cid) = correlate::allocate(&mut s.correlate, slot, packet_id, OP_KPRODUCE, now) else {
            p_errs[pi] = KERR_REQUEST_TIMED_OUT;
            continue;
        };
        let op_body_len = 2 + 2 + topic_len + p_lens[pi];
        let prop_total = wire::QPROP_TAGGED_HDR_LEN + op_body_len;
        if prop_total > s.out_buf.len() {
            let _ = correlate::take(&mut s.correlate, cid);
            p_errs[pi] = KERR_MESSAGE_TOO_LARGE;
            continue;
        }
        s.out_buf[0..8].copy_from_slice(&cid.to_le_bytes());
        wire::encode_qprop_header(
            &mut s.out_buf[8..8 + wire::QPROP_HEADER_LEN],
            wire::QOP_KAFKA_PRODUCE, 0, slot,
        );
        let ob = wire::QPROP_TAGGED_HDR_LEN;
        s.out_buf[ob..ob + 2].copy_from_slice(&(p_ids[pi] as u16).to_le_bytes());
        s.out_buf[ob + 2..ob + 4].copy_from_slice(&(topic_len as u16).to_le_bytes());
        s.out_buf[ob + 4..ob + 4 + topic_len].copy_from_slice(&topic[..topic_len]);
        core::ptr::copy_nonoverlapping(
            s.in_buf.as_ptr().add(p_offs[pi]),
            s.out_buf.as_mut_ptr().add(ob + 4 + topic_len),
            p_lens[pi],
        );
        if try_emit(
            sys, s.out_proposals_tagged, wire::MSG_CLIENT_PROPOSAL,
            &s.out_buf[..prop_total],
        ) {
            s.proposals_emitted = s.proposals_emitted.wrapping_add(1);
            pending += 1;
        } else {
            let _ = correlate::take(&mut s.correlate, cid);
            p_errs[pi] = KERR_REQUEST_TIMED_OUT;
        }
    }

    store::inflight_open(
        &mut s.store,
        ki,
        store::InflightOpen {
            conn_id,
            proto: KIN_PROTO_KAFKA,
            topic_len: topic_len as u8,
            n_parts: n_parts as u8,
            n_done: n_parts as u8 - pending,
            api_ver,
            acks,
            kafka_corr,
            channel: 0,
            delivery_tag: 0,
            ts_ms: now,
        },
        &topic[..topic_len],
    );
    for pi in 0..n_parts {
        store::inflight_set_part(&mut s.store, ki, pi, p_ids[pi], p_errs[pi]);
    }
    if pending == 0 {
        // Every partition failed before proposing — respond now.
        emit_kafka_produce_response_multi(s, sys, ki);
        store::inflight_free(&mut s.store, ki);
    }
}

#[cfg(feature = "kafka")]
/// Build + emit the aggregated ProduceResponse for inflight `ki` (all
/// partitions resolved). Non-flexible encoding, api_version 2..=8.
///
/// # Safety
unsafe fn emit_kafka_produce_response_multi(
    s: &mut ModuleState, sys: &SyscallTable, ki: usize,
) -> bool {
    let Some(e) = store::inflight_get(&s.store, ki) else { return false; };
    let tl = e.topic_len as usize;
    let n = e.n_parts as usize;
    let mut p = 0usize;
    s.out_buf[p..p + 4].copy_from_slice(&1i32.to_be_bytes()); p += 4;
    s.out_buf[p..p + 2].copy_from_slice(&(tl as i16).to_be_bytes()); p += 2;
    s.out_buf[p..p + tl].copy_from_slice(&e.topic[..tl]); p += tl;
    s.out_buf[p..p + 4].copy_from_slice(&(n as i32).to_be_bytes()); p += 4;
    for pi in 0..n {
        s.out_buf[p..p + 4].copy_from_slice(&e.part_ids[pi].to_be_bytes()); p += 4;
        s.out_buf[p..p + 2].copy_from_slice(&e.part_errs[pi].to_be_bytes()); p += 2;
        s.out_buf[p..p + 8].copy_from_slice(&e.part_offs[pi].to_be_bytes()); p += 8;
        if e.api_ver >= 2 {
            s.out_buf[p..p + 8].copy_from_slice(&(-1i64).to_be_bytes()); p += 8;
        }
        if e.api_ver >= 5 {
            s.out_buf[p..p + 8].copy_from_slice(&0i64.to_be_bytes()); p += 8;
        }
    }
    if e.api_ver >= 1 {
        s.out_buf[p..p + 4].copy_from_slice(&0i32.to_be_bytes()); p += 4;
    }
    emit_kafka_response_outbuf(s, sys, e.conn_id, 0, e.api_ver, e.kafka_corr, p)
}

// ── Apply-side message store ────────────────────────────────────────────────







#[cfg(feature = "kafka")]
/// Apply-side QOP_KAFKA_PRODUCE: store the committed record batches and
/// patch each baseOffset to the partition's contiguous logical offset
/// (bytes 0..8 of a v2 batch sit OUTSIDE the batch CRC, so the patch is
/// safe). Op-body: [partition:u16 LE][topic_len:u16 LE][topic][records].
///
/// `records` may hold SEVERAL v2 batches concatenated (a producer that
/// accumulates multiple record batches for one partition sends them
/// back-to-back); each is stored as its own entry with its own
/// contiguous baseOffset so consumer offset accounting stays monotonic.
fn apply_kafka_produce(s: &mut ModuleState, body: &[u8]) {
    if body.len() < 4 { return; }
    let partition = u16::from_le_bytes([body[0], body[1]]);
    let tl = u16::from_le_bytes([body[2], body[3]]) as usize;
    if tl == 0 || tl > KAFKA_MAX_TOPIC || 4 + tl >= body.len() { return; }
    let mut topic = [0u8; KAFKA_MAX_TOPIC];
    topic[..tl].copy_from_slice(&body[4..4 + tl]);
    let records = &body[4 + tl..];
    let Some(pi) = store::find_or_create(&mut s.store, &topic[..tl], partition) else {
        s.store.full = s.store.full.wrapping_add(1);
        return;
    };

    // Walk concatenated v2 batches: baseOffset(8) batchLength(4)
    // [leaderEpoch(4) magic(1) crc(4) attrs(2) lastOffsetDelta(4 @23)...].
    // A batch spans 12 + batchLength bytes.
    let mut bo = 0usize;
    let mut guard = 0u32;
    while bo + 61 <= records.len() && guard < 256 {
        guard += 1;
        let batch_len = i32::from_be_bytes([
            records[bo + 8], records[bo + 9], records[bo + 10], records[bo + 11],
        ]);
        if batch_len < 49 { break; }
        let total = 12 + batch_len as usize;
        if bo + total > records.len() { break; }
        let nrec_minus1 = i32::from_be_bytes([
            records[bo + 23], records[bo + 24], records[bo + 25], records[bo + 26],
        ]);
        if !(0..=65535).contains(&nrec_minus1) { break; }
        let nrec = (nrec_minus1 + 1) as u32;
        // `records` aliases s.in_buf; store::push writes only the store.
        let rec = unsafe { core::slice::from_raw_parts(records.as_ptr().add(bo), total) };
        if let Some((offset, data_pos)) = store::push(&mut s.store, pi, KFLAG_BATCH, nrec, rec) {
            store::stamp_base_offset(&mut s.store, pi, data_pos, offset);
            s.store.batches_applied = s.store.batches_applied.wrapping_add(1);
        }
        bo += total;
    }
}

#[cfg(feature = "amqp")]
/// Apply-side QOP_AMQP_PUBLISH: store the raw message body on the
/// routing key's log (partition 0, raw-flagged — invisible to Kafka
/// Fetch). Op-body: [rk_len:u16 LE][routing_key][payload].
fn apply_amqp_publish(s: &mut ModuleState, body: &[u8]) {
    if body.len() < 2 { return; }
    let rl = u16::from_le_bytes([body[0], body[1]]) as usize;
    if rl == 0 || rl > KAFKA_MAX_TOPIC || 2 + rl > body.len() { return; }
    let mut rk = [0u8; KAFKA_MAX_TOPIC];
    rk[..rl].copy_from_slice(&body[2..2 + rl]);
    let payload = &body[2 + rl..];
    let Some(pi) = store::find_or_create(&mut s.store, &rk[..rl], 0) else { return; };
    let pl = payload.len();
    let pp = payload.as_ptr();
    let pay = unsafe { core::slice::from_raw_parts(pp, pl) };
    let _ = store::push(&mut s.store, pi, 0, 1, pay);
}

// ── Kafka Fetch / ListOffsets ───────────────────────────────────────────────

#[cfg(feature = "kafka")]
/// Emit a Kafka MSG_SESSION_RESPONSE whose body was built in
/// `s.out_buf[..body_len]` (the 256-byte stack path in
/// `emit_kafka_response` is too small for Fetch payloads).
///
/// # Safety
unsafe fn emit_kafka_response_outbuf(
    s: &mut ModuleState, sys: &SyscallTable,
    conn_id: u8, api_key: i16, api_ver: i16, kafka_corr: i32, body_len: usize,
) -> bool {
    let total = 10 + body_len;
    if total > BUF_SIZE { return false; }
    // Shift the body up to make room for the 10-byte header (memmove).
    core::ptr::copy(s.out_buf.as_ptr(), s.out_buf.as_mut_ptr().add(10), body_len);
    s.out_buf[0] = conn_id;
    s.out_buf[1] = 1; // PROTO_KAFKA envelope discriminator
    s.out_buf[2..4].copy_from_slice(&api_key.to_le_bytes());
    s.out_buf[4..6].copy_from_slice(&api_ver.to_le_bytes());
    s.out_buf[6..10].copy_from_slice(&kafka_corr.to_le_bytes());
    let w = wire::channel_write_msg(
        sys, s.out_codec, wire::MSG_SESSION_RESPONSE, &s.out_buf[..total],
    );
    w > 0
}


/// Bounds on how much of a Fetch/ListOffsets request we service in one
/// response. A real assignment is a handful of topics × ≤16 partitions;
/// excess entries are parsed-and-skipped, not answered (documented gap).
const KFETCH_MAX_TOPICS: usize = 8;
const KFETCH_MAX_PARTS: usize = 16;

#[cfg(feature = "kafka")]
/// Handle a Kafka Fetch request (api_key 1, v0-v5 non-flexible). Serves
/// stored batches for EVERY requested topic-partition up to the response
/// budget; no long-poll (max_wait ignored — an empty response returns
/// immediately and the client re-polls).
///
/// # Safety
unsafe fn handle_kafka_fetch(s: &mut ModuleState, sys: &SyscallTable, plen: usize) {
    let conn_id = s.in_buf[0];
    let api_ver = i16::from_le_bytes([s.in_buf[4], s.in_buf[5]]);
    let kafka_corr = i32::from_le_bytes([s.in_buf[6], s.in_buf[7], s.in_buf[8], s.in_buf[9]]);
    let end = plen;
    s.kafka_fetch_rx = s.kafka_fetch_rx.wrapping_add(1);

    // replica_id(4) max_wait(4) min_bytes(4) [v3+: max_bytes(4)]
    // [v4+: isolation(1)] topics(4) ...
    let mut off = 10 + 12;
    if api_ver >= 3 { off += 4; }
    if api_ver >= 4 { off += 1; }
    if off + 4 > end { return; }
    let topic_count = i32::from_be_bytes([
        s.in_buf[off], s.in_buf[off + 1], s.in_buf[off + 2], s.in_buf[off + 3],
    ]);
    off += 4;
    if topic_count < 1 { return; }

    let budget_end = 7600usize.min(BUF_SIZE - 16);
    let mut p = 0usize;
    if api_ver >= 1 {
        s.out_buf[p..p + 4].copy_from_slice(&0i32.to_be_bytes()); p += 4; // throttle
    }
    let resp_topic_count = topic_count.min(KFETCH_MAX_TOPICS as i32);
    s.out_buf[p..p + 4].copy_from_slice(&resp_topic_count.to_be_bytes()); p += 4;

    for _ in 0..resp_topic_count {
        if off + 2 > end { break; }
        let tl = i16::from_be_bytes([s.in_buf[off], s.in_buf[off + 1]]) as usize;
        off += 2;
        if tl == 0 || tl > KAFKA_MAX_TOPIC || off + tl > end { return; }
        let mut topic = [0u8; KAFKA_MAX_TOPIC];
        topic[..tl].copy_from_slice(&s.in_buf[off..off + tl]);
        off += tl;
        if off + 4 > end { return; }
        let part_count = i32::from_be_bytes([
            s.in_buf[off], s.in_buf[off + 1], s.in_buf[off + 2], s.in_buf[off + 3],
        ]);
        off += 4;
        let resp_part_count = part_count.clamp(0, KFETCH_MAX_PARTS as i32);

        if p + 2 + tl + 8 > BUF_SIZE { return; }
        s.out_buf[p..p + 2].copy_from_slice(&(tl as i16).to_be_bytes()); p += 2;
        s.out_buf[p..p + tl].copy_from_slice(&topic[..tl]); p += tl;
        s.out_buf[p..p + 4].copy_from_slice(&resp_part_count.to_be_bytes()); p += 4;

        for _ in 0..part_count {
            // partition(4) fetch_offset(8) [v5+: log_start(8)] max_bytes(4)
            let mut need = 16;
            if api_ver >= 5 { need += 8; }
            if off + need > end { return; }
            let partition = i32::from_be_bytes([
                s.in_buf[off], s.in_buf[off + 1], s.in_buf[off + 2], s.in_buf[off + 3],
            ]);
            let fetch_offset = i64::from_be_bytes([
                s.in_buf[off + 4], s.in_buf[off + 5], s.in_buf[off + 6], s.in_buf[off + 7],
                s.in_buf[off + 8], s.in_buf[off + 9], s.in_buf[off + 10], s.in_buf[off + 11],
            ]);
            off += need;

            // Only the first resp_part_count partitions are answered; the
            // rest are parsed (to stay frame-aligned) but skipped.
            if p >= budget_end { continue; }

            let (hw, log_start, pi) = match store::find(&s.store, &topic[..tl], partition as u16) {
                Some(i) => (
                    store::next_offset(&s.store, i) as i64,
                    store::log_start(&s.store, i) as i64,
                    Some(i),
                ),
                None => (0i64, 0i64, None),
            };
            // partition header: index, error, hw, [LSO v4], [logStart v5],
            // [aborted v4], records_len
            let fixed = 4 + 2 + 8
                + if api_ver >= 4 { 8 } else { 0 }
                + if api_ver >= 5 { 8 } else { 0 }
                + if api_ver >= 4 { 4 } else { 0 }
                + 4;
            if p + fixed > BUF_SIZE { return; }
            s.out_buf[p..p + 4].copy_from_slice(&partition.to_be_bytes()); p += 4;
            s.out_buf[p..p + 2].copy_from_slice(&0i16.to_be_bytes()); p += 2;
            s.out_buf[p..p + 8].copy_from_slice(&hw.to_be_bytes()); p += 8;
            if api_ver >= 4 {
                s.out_buf[p..p + 8].copy_from_slice(&hw.to_be_bytes()); p += 8; // LSO
            }
            if api_ver >= 5 {
                s.out_buf[p..p + 8].copy_from_slice(&log_start.to_be_bytes()); p += 8;
            }
            if api_ver >= 4 {
                s.out_buf[p..p + 4].copy_from_slice(&0i32.to_be_bytes()); p += 4; // aborted
            }
            let records_len_pos = p;
            p += 4;
            let rb = match pi {
                Some(pi) => store::fetch_into(&s.store, pi, fetch_offset, &mut s.out_buf, p, budget_end),
                None => 0,
            };
            s.out_buf[records_len_pos..records_len_pos + 4]
                .copy_from_slice(&(rb as i32).to_be_bytes());
            p += rb;
        }
    }
    emit_kafka_response_outbuf(s, sys, conn_id, 1, api_ver, kafka_corr, p);
}

#[cfg(feature = "kafka")]
/// Handle a Kafka ListOffsets request (api_key 2, v0-v2 non-flexible).
/// timestamp -1 (latest) → high watermark; -2 (earliest) → log start.
///
/// # Safety
unsafe fn handle_kafka_list_offsets(s: &mut ModuleState, sys: &SyscallTable, plen: usize) {
    let conn_id = s.in_buf[0];
    let api_ver = i16::from_le_bytes([s.in_buf[4], s.in_buf[5]]);
    let kafka_corr = i32::from_le_bytes([s.in_buf[6], s.in_buf[7], s.in_buf[8], s.in_buf[9]]);
    let end = plen;
    // replica_id(4) [v2+: isolation(1)] topics(4) [name partitions(4)
    //   [partition(4) timestamp(8) [v0: max_num_offsets(4)]]]
    let mut off = 10 + 4;
    if api_ver >= 2 { off += 1; }
    if off + 4 > end { return; }
    let topic_count = i32::from_be_bytes([
        s.in_buf[off], s.in_buf[off + 1], s.in_buf[off + 2], s.in_buf[off + 3],
    ]);
    off += 4;
    if topic_count < 1 { return; }

    let mut p = 0usize;
    if api_ver >= 2 {
        s.out_buf[p..p + 4].copy_from_slice(&0i32.to_be_bytes()); p += 4; // throttle
    }
    let resp_topics = topic_count.min(KFETCH_MAX_TOPICS as i32);
    s.out_buf[p..p + 4].copy_from_slice(&resp_topics.to_be_bytes()); p += 4;

    for _ in 0..resp_topics {
        if off + 2 > end { return; }
        let tl = i16::from_be_bytes([s.in_buf[off], s.in_buf[off + 1]]) as usize;
        off += 2;
        if tl == 0 || tl > KAFKA_MAX_TOPIC || off + tl > end { return; }
        let mut topic = [0u8; KAFKA_MAX_TOPIC];
        topic[..tl].copy_from_slice(&s.in_buf[off..off + tl]);
        off += tl;
        if off + 4 > end { return; }
        let part_count = i32::from_be_bytes([
            s.in_buf[off], s.in_buf[off + 1], s.in_buf[off + 2], s.in_buf[off + 3],
        ]);
        off += 4;
        let resp_parts = part_count.clamp(0, KFETCH_MAX_PARTS as i32);

        if p + 2 + tl + 4 > BUF_SIZE { return; }
        s.out_buf[p..p + 2].copy_from_slice(&(tl as i16).to_be_bytes()); p += 2;
        s.out_buf[p..p + tl].copy_from_slice(&topic[..tl]); p += tl;
        s.out_buf[p..p + 4].copy_from_slice(&resp_parts.to_be_bytes()); p += 4;

        for pi_idx in 0..part_count {
            let need = 12 + if api_ver == 0 { 4 } else { 0 };
            if off + need > end { return; }
            let partition = i32::from_be_bytes([
                s.in_buf[off], s.in_buf[off + 1], s.in_buf[off + 2], s.in_buf[off + 3],
            ]);
            let timestamp = i64::from_be_bytes([
                s.in_buf[off + 4], s.in_buf[off + 5], s.in_buf[off + 6], s.in_buf[off + 7],
                s.in_buf[off + 8], s.in_buf[off + 9], s.in_buf[off + 10], s.in_buf[off + 11],
            ]);
            off += need;
            if pi_idx >= resp_parts { continue; }

            let (hw, log_start) = match store::find(&s.store, &topic[..tl], partition as u16) {
                Some(i) => (store::next_offset(&s.store, i) as i64, store::log_start(&s.store, i) as i64),
                None => (0i64, 0i64),
            };
            let offset = if timestamp == -2 { log_start } else { hw };

            let fixed = 4 + 2 + if api_ver == 0 { 4 + 8 } else { 8 + 8 };
            if p + fixed > BUF_SIZE { return; }
            s.out_buf[p..p + 4].copy_from_slice(&partition.to_be_bytes()); p += 4;
            s.out_buf[p..p + 2].copy_from_slice(&0i16.to_be_bytes()); p += 2;
            if api_ver == 0 {
                s.out_buf[p..p + 4].copy_from_slice(&1i32.to_be_bytes()); p += 4; // 1 offset
                s.out_buf[p..p + 8].copy_from_slice(&offset.to_be_bytes()); p += 8;
            } else {
                s.out_buf[p..p + 8].copy_from_slice(&(-1i64).to_be_bytes()); p += 8; // ts
                s.out_buf[p..p + 8].copy_from_slice(&offset.to_be_bytes()); p += 8;
            }
        }
    }
    emit_kafka_response_outbuf(s, sys, conn_id, 2, api_ver, kafka_corr, p);
}

// ── AMQP publish / Basic.Get ────────────────────────────────────────────────

#[cfg(feature = "amqp")]
/// Emit an AMQP MSG_SESSION_RESPONSE. Payload:
///   [conn_id][proto=2][op][channel:u16 LE][rest...]
///
/// # Safety
unsafe fn emit_amqp_response(
    sys: &SyscallTable, chan: i32, conn_id: u8, op: u8, channel: u16, rest: &[u8],
) -> bool {
    if chan < 0 { return false; }
    let total = 5 + rest.len();
    let mut out = [0u8; 2200];
    if total > out.len() { return false; }
    out[0] = conn_id;
    out[1] = 2; // PROTO_AMQP envelope discriminator
    out[2] = op;
    out[3..5].copy_from_slice(&channel.to_le_bytes());
    out[5..total].copy_from_slice(rest);
    wire::channel_write_msg(sys, chan, wire::MSG_SESSION_RESPONSE, &out[..total]) > 0
}

#[cfg(feature = "amqp")]
/// AMQP publish (op 1) from `protocol::amqp`:
///   [conn][2][1][channel:u16 LE][delivery_tag:u64 LE][rk_len:u16 LE][rk][body]
/// delivery_tag != 0 → confirm mode: tagged proposal, Basic.Ack gated on
/// quorum durability. delivery_tag == 0 → untagged fire-and-forget
/// (still durably logged; no confirm to route).
///
/// # Safety
unsafe fn handle_amqp_publish(
    s: &mut ModuleState, sys: &SyscallTable, now: u64, plen: usize,
) {
    if plen < 15 { return; }
    let conn_id = s.in_buf[0];
    let channel = u16::from_le_bytes([s.in_buf[3], s.in_buf[4]]);
    let delivery_tag = u64::from_le_bytes([
        s.in_buf[5], s.in_buf[6], s.in_buf[7], s.in_buf[8],
        s.in_buf[9], s.in_buf[10], s.in_buf[11], s.in_buf[12],
    ]);
    let rl = u16::from_le_bytes([s.in_buf[13], s.in_buf[14]]) as usize;
    s.amqp_publish_rx = s.amqp_publish_rx.wrapping_add(1);

    // Nack a confirm-mode publish (or silently drop a non-confirm one).
    // Defined up front so EVERY rejection path — including an
    // unroutable/empty routing key — sends the confirm the publisher is
    // waiting on. A silent return here would black-hole the confirm and
    // wedge the publisher.
    let nack = |s: &mut ModuleState, sys: &SyscallTable| {
        if delivery_tag != 0 {
            let mut rest = [0u8; 9];
            rest[0..8].copy_from_slice(&delivery_tag.to_le_bytes());
            rest[8] = 1;
            emit_amqp_response(sys, s.out_codec, conn_id, 1, channel, &rest);
        }
    };

    // An empty or oversdized routing key can't be stored (the queue log
    // is keyed by routing key). Reject with a Nack instead of a silent
    // black-hole so a confirm-mode publisher unblocks.
    if rl == 0 || rl > KAFKA_MAX_TOPIC || 15 + rl > plen {
        s.amqp_publish_errors = s.amqp_publish_errors.wrapping_add(1);
        nack(s, sys);
        return;
    }
    let body_len = plen - 15 - rl;

    let op_body_len = 2 + rl + body_len;
    if op_body_len + 2 > KAFKA_MAX_RECORDS_BYTES + 4 {
        s.amqp_publish_errors = s.amqp_publish_errors.wrapping_add(1);
        nack(s, sys);
        return;
    }

    if delivery_tag == 0 {
        let hdr = wire::QPROP_HEADER_LEN;
        wire::encode_qprop_header(&mut s.out_buf[..hdr], wire::QOP_AMQP_PUBLISH, 0, 0);
        s.out_buf[hdr..hdr + 2].copy_from_slice(&(rl as u16).to_le_bytes());
        core::ptr::copy_nonoverlapping(
            s.in_buf.as_ptr().add(15),
            s.out_buf.as_mut_ptr().add(hdr + 2),
            rl + body_len,
        );
        if try_emit(
            sys, s.out_proposals, wire::MSG_CLIENT_PROPOSAL,
            &s.out_buf[..hdr + op_body_len],
        ) {
            s.proposals_emitted = s.proposals_emitted.wrapping_add(1);
        }
        return;
    }

    // Confirm mode: inflight slot + correlation + tagged proposal.
    let Some((ki, slot)) = store::inflight_alloc(&mut s.store) else { nack(s, sys); return; };
    let Some(cid) = correlate::allocate(&mut s.correlate, slot, ki as u16, OP_KPRODUCE, now) else {
        nack(s, sys);
        return;
    };
    s.out_buf[0..8].copy_from_slice(&cid.to_le_bytes());
    wire::encode_qprop_header(
        &mut s.out_buf[8..8 + wire::QPROP_HEADER_LEN],
        wire::QOP_AMQP_PUBLISH, 0, slot,
    );
    let ob = wire::QPROP_TAGGED_HDR_LEN;
    s.out_buf[ob..ob + 2].copy_from_slice(&(rl as u16).to_le_bytes());
    core::ptr::copy_nonoverlapping(
        s.in_buf.as_ptr().add(15),
        s.out_buf.as_mut_ptr().add(ob + 2),
        rl + body_len,
    );
    if !try_emit(
        sys, s.out_proposals_tagged, wire::MSG_CLIENT_PROPOSAL,
        &s.out_buf[..ob + op_body_len],
    ) {
        let _ = correlate::take(&mut s.correlate, cid);
        nack(s, sys);
        return;
    }
    s.proposals_emitted = s.proposals_emitted.wrapping_add(1);
    store::inflight_open(
        &mut s.store,
        ki,
        store::InflightOpen {
            conn_id,
            proto: KIN_PROTO_AMQP,
            topic_len: 0,
            n_parts: 1,
            n_done: 0,
            api_ver: 0,
            acks: -1,
            kafka_corr: 0,
            channel,
            delivery_tag,
            ts_ms: now,
        },
        &[],
    );
}

#[cfg(feature = "amqp")]
/// AMQP Basic.Get (op 2):
///   [conn][2][2][channel:u16 LE][dt:u64=0][q_len:u16 LE][queue]
/// Pops the next raw entry at or past the queue's get_cursor.
/// Single-consumer, auto-ack semantics (v1).
///
/// # Safety
unsafe fn handle_amqp_get(s: &mut ModuleState, sys: &SyscallTable, plen: usize) {
    if plen < 15 { return; }
    let conn_id = s.in_buf[0];
    let channel = u16::from_le_bytes([s.in_buf[3], s.in_buf[4]]);
    let ql = u16::from_le_bytes([s.in_buf[13], s.in_buf[14]]) as usize;
    if ql == 0 || ql > KAFKA_MAX_TOPIC || 15 + ql > plen { return; }
    let mut queue = [0u8; KAFKA_MAX_TOPIC];
    queue[..ql].copy_from_slice(&s.in_buf[15..15 + ql]);
    s.amqp_get_rx = s.amqp_get_rx.wrapping_add(1);

    // Empty result still carries the full [result][offset][remaining]
    // shape — the codec parses one fixed layout for both outcomes.
    let empty = |s: &mut ModuleState, sys: &SyscallTable| {
        let rest = [1u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        emit_amqp_response(sys, s.out_codec, conn_id, 2, channel, &rest);
    };

    let Some(pi) = store::find(&s.store, &queue[..ql], 0) else { empty(s, sys); return; };
    if store::is_empty(&s.store, pi) { empty(s, sys); return; }

    // Walk oldest→newest for the first raw entry at/past the cursor,
    // counting the raw entries behind it (message-count for GetOk).
    let cursor = store::get_cursor(&s.store, pi);
    let Some((data_pos, len, offset, remaining_after)) =
        store::next_raw_entry(&s.store, pi, cursor)
    else {
        empty(s, sys);
        return;
    };
    store::set_get_cursor(&mut s.store, pi, offset + 1);

    let mut rest = [0u8; 2048];
    let l = len as usize;
    if 13 + l > rest.len() { empty(s, sys); return; }
    rest[0] = 0; // result: message
    // Delivery tag = offset + 1: AMQP reserves tag 0 for the client's
    // "all messages" ack, so the server MUST NOT assign it (the first
    // message on a fresh queue has offset 0). GetOk is auto-ack here, so
    // the tag is informational, but a zero tag makes a client's ack
    // ambiguous with ack-all.
    rest[1..9].copy_from_slice(&(offset + 1).to_le_bytes());
    rest[9..13].copy_from_slice(&remaining_after.to_le_bytes());
    store::copy_entry_into(&s.store, pi, data_pos as usize, l, &mut rest[13..13 + l]);
    emit_amqp_response(sys, s.out_codec, conn_id, 2, channel, &rest[..13 + l]);
}

// ── Kafka consumer-group + offset APIs ──────────────────────────────────────

#[cfg(feature = "kafka")]
/// Read a Kafka STRING (i16 BE len + bytes) out of `buf`. Returns
/// `(next_off, start, len)`; len 0 for null (-1) strings.
fn kstr(buf: &[u8], off: usize, end: usize) -> Option<(usize, usize, usize)> {
    if off + 2 > end { return None; }
    let l = i16::from_be_bytes([buf[off], buf[off + 1]]);
    if l < 0 { return Some((off + 2, off + 2, 0)); }
    let l = l as usize;
    if off + 2 + l > end { return None; }
    Some((off + 2 + l, off + 2, l))
}







#[cfg(feature = "kafka")]
/// JoinGroup (api_key 11, v0-v2). Assigns/refreshes member identity,
/// bumps the generation on membership change, and returns the member
/// list to the leader so it can compute assignments client-side.
///
/// # Safety
unsafe fn handle_kafka_join_group(s: &mut ModuleState, sys: &SyscallTable, plen: usize) {
    let conn_id = s.in_buf[0];
    let v = i16::from_le_bytes([s.in_buf[4], s.in_buf[5]]);
    let corr = i32::from_le_bytes([s.in_buf[6], s.in_buf[7], s.in_buf[8], s.in_buf[9]]);
    let end = plen;
    s.kafka_group_ops = s.kafka_group_ops.wrapping_add(1);

    let Some((mut off, gs, gl)) = kstr(&s.in_buf, 10, end) else { return; };
    if off + 4 > end { return; }
    off += 4; // session_timeout
    if v >= 1 {
        if off + 4 > end { return; }
        off += 4; // rebalance_timeout
    }
    let Some((off2, ms, ml)) = kstr(&s.in_buf, off, end) else { return; };
    off = off2;
    let Some((off3, _pts, _ptl)) = kstr(&s.in_buf, off, end) else { return; };
    off = off3;
    if off + 4 > end { return; }
    let proto_count = i32::from_be_bytes([
        s.in_buf[off], s.in_buf[off + 1], s.in_buf[off + 2], s.in_buf[off + 3],
    ]);
    off += 4;
    // First protocol: name + metadata (echoed back in SyncGroup flows).
    let mut pname = [0u8; 16];
    let mut pname_len = 0usize;
    let mut meta = [0u8; KG_META];
    let mut meta_len = 0usize;
    if proto_count >= 1 {
        let Some((off4, ps, pl)) = kstr(&s.in_buf, off, end) else { return; };
        pname_len = pl.min(16);
        pname[..pname_len].copy_from_slice(&s.in_buf[ps..ps + pname_len]);
        off = off4;
        if off + 4 > end { return; }
        let bl = i32::from_be_bytes([
            s.in_buf[off], s.in_buf[off + 1], s.in_buf[off + 2], s.in_buf[off + 3],
        ]);
        off += 4;
        if bl > 0 {
            if off + bl as usize > end { return; }
            meta_len = (bl as usize).min(KG_META);
            meta[..meta_len].copy_from_slice(&s.in_buf[off..off + meta_len]);
        }
    }

    let mut group = [0u8; KG_NAME];
    let gl = gl.min(KG_NAME);
    group[..gl].copy_from_slice(&s.in_buf[gs..gs + gl]);
    let mut member = [0u8; KG_NAME];
    let ml_in = ml.min(KG_NAME);
    member[..ml_in].copy_from_slice(&s.in_buf[ms..ms + ml_in]);

    let Some(gi) = consumers::group_find_or_create(&mut s.consumers, &group[..gl]) else {
        // Table full: GROUP_AUTHORIZATION_FAILED would mislead; use
        // COORDINATOR_NOT_AVAILABLE (15) so the client retries.
        emit_kafka_group_error(s, sys, conn_id, 11, v, corr, 15);
        return;
    };

    // Resolve / create the member.
    let mut member_len = ml_in;
    if member_len == 0 {
        member_len = consumers::next_member_id(&mut s.consumers, gi, &mut member);
    }
    let Some(mi) = consumers::member_join(
        &mut s.consumers, gi, &member[..member_len], conn_id,
    ) else {
        emit_kafka_group_error(s, sys, conn_id, 11, v, corr, 15);
        return;
    };
    consumers::member_set_meta(
        &mut s.consumers, gi, mi, &meta[..meta_len], &pname[..pname_len],
    );

    // Response.
    let leader = consumers::group_leader(&s.consumers, gi).unwrap_or(mi);
    let is_leader = leader == mi;
    let generation = consumers::group_generation(&s.consumers, gi);
    let mut proto = [0u8; 16];
    let proto_len = consumers::group_proto_into(&s.consumers, gi, &mut proto);
    let mut leader_id = [0u8; KG_NAME];
    let leader_len = consumers::member_id_into(&s.consumers, gi, leader, &mut leader_id);

    let mut p = 0usize;
    if v >= 2 {
        s.out_buf[p..p + 4].copy_from_slice(&0i32.to_be_bytes()); p += 4;
    }
    s.out_buf[p..p + 2].copy_from_slice(&0i16.to_be_bytes()); p += 2;
    s.out_buf[p..p + 4].copy_from_slice(&generation.to_be_bytes()); p += 4;
    s.out_buf[p..p + 2].copy_from_slice(&(proto_len as i16).to_be_bytes()); p += 2;
    s.out_buf[p..p + proto_len].copy_from_slice(&proto[..proto_len]); p += proto_len;
    s.out_buf[p..p + 2].copy_from_slice(&(leader_len as i16).to_be_bytes()); p += 2;
    s.out_buf[p..p + leader_len].copy_from_slice(&leader_id[..leader_len]); p += leader_len;
    s.out_buf[p..p + 2].copy_from_slice(&(member_len as i16).to_be_bytes()); p += 2;
    s.out_buf[p..p + member_len].copy_from_slice(&member[..member_len]); p += member_len;
    if is_leader {
        let count = (0..KGROUP_MEMBERS)
            .filter(|&i| consumers::member_active(&s.consumers, gi, i))
            .count() as i32;
        s.out_buf[p..p + 4].copy_from_slice(&count.to_be_bytes()); p += 4;
        for i in 0..KGROUP_MEMBERS {
            if !consumers::member_active(&s.consumers, gi, i) { continue; }
            let mut mid = [0u8; KG_NAME];
            let mut mmeta = [0u8; KG_META];
            let il = consumers::member_id_into(&s.consumers, gi, i, &mut mid);
            let mel = consumers::member_meta_into(&s.consumers, gi, i, &mut mmeta);
            if p + 2 + il + 4 + mel > BUF_SIZE - 32 { break; }
            s.out_buf[p..p + 2].copy_from_slice(&(il as i16).to_be_bytes()); p += 2;
            s.out_buf[p..p + il].copy_from_slice(&mid[..il]); p += il;
            s.out_buf[p..p + 4].copy_from_slice(&(mel as i32).to_be_bytes()); p += 4;
            s.out_buf[p..p + mel].copy_from_slice(&mmeta[..mel]); p += mel;
        }
    } else {
        s.out_buf[p..p + 4].copy_from_slice(&0i32.to_be_bytes()); p += 4;
    }
    emit_kafka_response_outbuf(s, sys, conn_id, 11, v, corr, p);
}

#[cfg(feature = "kafka")]
/// Small-body group error response (JoinGroup shape degenerates fine:
/// clients read error_code first and bail).
///
/// # Safety
unsafe fn emit_kafka_group_error(
    s: &mut ModuleState, sys: &SyscallTable,
    conn_id: u8, api_key: i16, v: i16, corr: i32, err: i16,
) {
    let mut p = 0usize;
    if (api_key == 11 && v >= 2) || (api_key != 11 && v >= 1) {
        s.out_buf[p..p + 4].copy_from_slice(&0i32.to_be_bytes()); p += 4;
    }
    s.out_buf[p..p + 2].copy_from_slice(&err.to_be_bytes()); p += 2;
    if api_key == 11 {
        // generation, protocol "", leader "", member_id "", members []
        s.out_buf[p..p + 4].copy_from_slice(&(-1i32).to_be_bytes()); p += 4;
        for _ in 0..3 {
            s.out_buf[p..p + 2].copy_from_slice(&0i16.to_be_bytes()); p += 2;
        }
        s.out_buf[p..p + 4].copy_from_slice(&0i32.to_be_bytes()); p += 4;
    } else if api_key == 14 {
        s.out_buf[p..p + 4].copy_from_slice(&0i32.to_be_bytes()); p += 4; // empty assignment
    }
    emit_kafka_response_outbuf(s, sys, conn_id, api_key, v, corr, p);
}

#[cfg(feature = "kafka")]
/// SyncGroup (api_key 14, v0-v1): the leader supplies per-member
/// assignments; everyone gets their stored assignment back.
///
/// # Safety
unsafe fn handle_kafka_sync_group(s: &mut ModuleState, sys: &SyscallTable, plen: usize) {
    let conn_id = s.in_buf[0];
    let v = i16::from_le_bytes([s.in_buf[4], s.in_buf[5]]);
    let corr = i32::from_le_bytes([s.in_buf[6], s.in_buf[7], s.in_buf[8], s.in_buf[9]]);
    let end = plen;
    s.kafka_group_ops = s.kafka_group_ops.wrapping_add(1);

    let Some((mut off, gs, gl)) = kstr(&s.in_buf, 10, end) else { return; };
    if off + 4 > end { return; }
    let generation = i32::from_be_bytes([
        s.in_buf[off], s.in_buf[off + 1], s.in_buf[off + 2], s.in_buf[off + 3],
    ]);
    off += 4;
    let Some((off2, ms, ml)) = kstr(&s.in_buf, off, end) else { return; };
    off = off2;
    let mut group = [0u8; KG_NAME];
    let gl = gl.min(KG_NAME);
    group[..gl].copy_from_slice(&s.in_buf[gs..gs + gl]);
    let mut member = [0u8; KG_NAME];
    let ml = ml.min(KG_NAME);
    member[..ml].copy_from_slice(&s.in_buf[ms..ms + ml]);

    let Some(gi) = consumers::group_find(&s.consumers, &group[..gl]) else {
        emit_kafka_group_error(s, sys, conn_id, 14, v, corr, KERR_UNKNOWN_MEMBER_ID);
        return;
    };
    if consumers::group_generation(&s.consumers, gi) != generation {
        emit_kafka_group_error(s, sys, conn_id, 14, v, corr, KERR_ILLEGAL_GENERATION);
        return;
    }
    let Some(mi) = consumers::member_find(&s.consumers, gi, &member[..ml]) else {
        emit_kafka_group_error(s, sys, conn_id, 14, v, corr, KERR_UNKNOWN_MEMBER_ID);
        return;
    };

    // Store any supplied assignments (leader path).
    if off + 4 <= end {
        let n = i32::from_be_bytes([
            s.in_buf[off], s.in_buf[off + 1], s.in_buf[off + 2], s.in_buf[off + 3],
        ]);
        off += 4;
        for _ in 0..n.clamp(0, KGROUP_MEMBERS as i32 * 2) {
            let Some((o2, ids, idl)) = kstr(&s.in_buf, off, end) else { break; };
            off = o2;
            if off + 4 > end { break; }
            let bl = i32::from_be_bytes([
                s.in_buf[off], s.in_buf[off + 1], s.in_buf[off + 2], s.in_buf[off + 3],
            ]);
            off += 4;
            if bl < 0 { continue; }
            let bl = bl as usize;
            if off + bl > end { break; }
            let mut mid = [0u8; KG_NAME];
            let idl = idl.min(KG_NAME);
            mid[..idl].copy_from_slice(&s.in_buf[ids..ids + idl]);
            let al = bl.min(KG_META);
            let mut abuf = [0u8; KG_META];
            core::ptr::copy_nonoverlapping(
                s.in_buf.as_ptr().add(off), abuf.as_mut_ptr(), al,
            );
            consumers::member_set_assignment(
                &mut s.consumers, gi, &mid[..idl], &abuf[..al],
            );
            off += bl;
        }
    }

    let mut assign = [0u8; KG_META];
    let al = consumers::member_assignment_into(&s.consumers, gi, mi, &mut assign);
    let mut p = 0usize;
    if v >= 1 {
        s.out_buf[p..p + 4].copy_from_slice(&0i32.to_be_bytes()); p += 4;
    }
    s.out_buf[p..p + 2].copy_from_slice(&0i16.to_be_bytes()); p += 2;
    s.out_buf[p..p + 4].copy_from_slice(&(al as i32).to_be_bytes()); p += 4;
    s.out_buf[p..p + al].copy_from_slice(&assign[..al]); p += al;
    emit_kafka_response_outbuf(s, sys, conn_id, 14, v, corr, p);
}

#[cfg(feature = "kafka")]
/// Heartbeat (12) / LeaveGroup (13), v0-v1 — same request prefix.
///
/// # Safety
unsafe fn handle_kafka_heartbeat_leave(
    s: &mut ModuleState, sys: &SyscallTable, plen: usize, api_key: i16,
) {
    let conn_id = s.in_buf[0];
    let v = i16::from_le_bytes([s.in_buf[4], s.in_buf[5]]);
    let corr = i32::from_le_bytes([s.in_buf[6], s.in_buf[7], s.in_buf[8], s.in_buf[9]]);
    let end = plen;
    s.kafka_group_ops = s.kafka_group_ops.wrapping_add(1);

    let Some((mut off, gs, gl)) = kstr(&s.in_buf, 10, end) else { return; };
    let mut generation = 0i32;
    if api_key == 12 {
        if off + 4 > end { return; }
        generation = i32::from_be_bytes([
            s.in_buf[off], s.in_buf[off + 1], s.in_buf[off + 2], s.in_buf[off + 3],
        ]);
        off += 4;
    }
    let Some((_o, ms, ml)) = kstr(&s.in_buf, off, end) else { return; };
    let mut group = [0u8; KG_NAME];
    let gl = gl.min(KG_NAME);
    group[..gl].copy_from_slice(&s.in_buf[gs..gs + gl]);
    let mut member = [0u8; KG_NAME];
    let ml = ml.min(KG_NAME);
    member[..ml].copy_from_slice(&s.in_buf[ms..ms + ml]);

    let mut err: i16 = 0;
    match consumers::group_find(&s.consumers, &group[..gl]) {
        None => err = KERR_UNKNOWN_MEMBER_ID,
        Some(gi) => match consumers::member_find(&s.consumers, gi, &member[..ml]) {
            None => err = KERR_UNKNOWN_MEMBER_ID,
            Some(mi) => {
                if api_key == 12 {
                    if consumers::group_generation(&s.consumers, gi) != generation {
                        err = KERR_REBALANCE_IN_PROGRESS;
                    }
                } else {
                    consumers::member_leave(&mut s.consumers, gi, mi);
                }
            }
        },
    }
    let mut p = 0usize;
    if v >= 1 {
        s.out_buf[p..p + 4].copy_from_slice(&0i32.to_be_bytes()); p += 4;
    }
    s.out_buf[p..p + 2].copy_from_slice(&err.to_be_bytes()); p += 2;
    emit_kafka_response_outbuf(s, sys, conn_id, api_key, v, corr, p);
}

#[cfg(feature = "kafka")]
/// OffsetCommit (8, v0-v2). Stores every (topic, partition, offset) in
/// the request, mirrors the structure back with per-partition error 0,
/// and emits an untagged QOP_KAFKA_OFFSET per commit for durability
/// (response is not durability-gated — see wire.rs).
///
/// # Safety
unsafe fn handle_kafka_offset_commit(s: &mut ModuleState, sys: &SyscallTable, plen: usize) {
    let conn_id = s.in_buf[0];
    let v = i16::from_le_bytes([s.in_buf[4], s.in_buf[5]]);
    let corr = i32::from_le_bytes([s.in_buf[6], s.in_buf[7], s.in_buf[8], s.in_buf[9]]);
    let end = plen;
    s.kafka_offset_commits = s.kafka_offset_commits.wrapping_add(1);

    let Some((mut off, gs, gl)) = kstr(&s.in_buf, 10, end) else { return; };
    let mut group = [0u8; KG_NAME];
    let gl = gl.min(KG_NAME);
    group[..gl].copy_from_slice(&s.in_buf[gs..gs + gl]);
    if v >= 1 {
        if off + 4 > end { return; }
        off += 4; // generation
        let Some((o2, _ms, _ml)) = kstr(&s.in_buf, off, end) else { return; };
        off = o2;
    }
    if v >= 2 {
        if off + 8 > end { return; }
        off += 8; // retention_time
    }
    if off + 4 > end { return; }
    let topic_count = i32::from_be_bytes([
        s.in_buf[off], s.in_buf[off + 1], s.in_buf[off + 2], s.in_buf[off + 3],
    ]).clamp(0, 4);
    off += 4;

    // Response built as we parse (structure mirrors the request).
    let mut p = 0usize;
    s.out_buf[p..p + 4].copy_from_slice(&topic_count.to_be_bytes()); p += 4;
    for _ in 0..topic_count {
        let Some((o2, ts, tl)) = kstr(&s.in_buf, off, end) else { return; };
        off = o2;
        let tl = tl.min(KAFKA_MAX_TOPIC);
        let mut topic = [0u8; KAFKA_MAX_TOPIC];
        topic[..tl].copy_from_slice(&s.in_buf[ts..ts + tl]);
        if off + 4 > end { return; }
        let pc = i32::from_be_bytes([
            s.in_buf[off], s.in_buf[off + 1], s.in_buf[off + 2], s.in_buf[off + 3],
        ]).clamp(0, 8);
        off += 4;
        s.out_buf[p..p + 2].copy_from_slice(&(tl as i16).to_be_bytes()); p += 2;
        s.out_buf[p..p + tl].copy_from_slice(&topic[..tl]); p += tl;
        s.out_buf[p..p + 4].copy_from_slice(&pc.to_be_bytes()); p += 4;
        for _ in 0..pc {
            if off + 12 > end { return; }
            let part = i32::from_be_bytes([
                s.in_buf[off], s.in_buf[off + 1], s.in_buf[off + 2], s.in_buf[off + 3],
            ]);
            let o = i64::from_be_bytes([
                s.in_buf[off + 4], s.in_buf[off + 5], s.in_buf[off + 6], s.in_buf[off + 7],
                s.in_buf[off + 8], s.in_buf[off + 9], s.in_buf[off + 10], s.in_buf[off + 11],
            ]);
            off += 12;
            if v == 1 {
                if off + 8 > end { return; }
                off += 8; // timestamp
            }
            let Some((o3, _mds, _mdl)) = kstr(&s.in_buf, off, end) else { return; };
            off = o3;
            consumers::offset_store(&mut s.consumers, &group[..gl], &topic[..tl], part as u16, o);
            s.out_buf[p..p + 4].copy_from_slice(&part.to_be_bytes()); p += 4;
            s.out_buf[p..p + 2].copy_from_slice(&0i16.to_be_bytes()); p += 2;
        }
        // Durability: one QOP_KAFKA_OFFSET per (topic) high-water — emit
        // per partition would multiply proposals; commit the LAST
        // partition's offset per topic is wrong, so emit per partition
        // above instead: build here from the stored values.
        for i in 0..consumers::OFFSET_SLOTS {
            let Some((ofs_partition, ofs_offset)) =
                consumers::offset_at(&s.consumers, i, &group[..gl], &topic[..tl])
            else {
                continue;
            };
            let hdr = wire::QPROP_HEADER_LEN;
            let body_len = 2 + gl + 2 + tl + 2 + 8;
            if hdr + body_len > s.out_buf.len() { continue; }
            // Build the proposal in a stack buffer — out_buf holds the
            // in-progress response.
            let mut prop = [0u8; 200];
            wire::encode_qprop_header(&mut prop[..hdr], wire::QOP_KAFKA_OFFSET, 0, 0);
            let mut q = hdr;
            prop[q..q + 2].copy_from_slice(&(gl as u16).to_le_bytes()); q += 2;
            prop[q..q + gl].copy_from_slice(&group[..gl]); q += gl;
            prop[q..q + 2].copy_from_slice(&(tl as u16).to_le_bytes()); q += 2;
            prop[q..q + tl].copy_from_slice(&topic[..tl]); q += tl;
            prop[q..q + 2].copy_from_slice(&ofs_partition.to_le_bytes()); q += 2;
            prop[q..q + 8].copy_from_slice(&ofs_offset.to_le_bytes()); q += 8;
            if try_emit(sys, s.out_proposals, wire::MSG_CLIENT_PROPOSAL, &prop[..q]) {
                s.proposals_emitted = s.proposals_emitted.wrapping_add(1);
            }
        }
    }
    emit_kafka_response_outbuf(s, sys, conn_id, 8, v, corr, p);
}

#[cfg(feature = "kafka")]
/// OffsetFetch (9, v0-v3).
///
/// # Safety
unsafe fn handle_kafka_offset_fetch(s: &mut ModuleState, sys: &SyscallTable, plen: usize) {
    let conn_id = s.in_buf[0];
    let v = i16::from_le_bytes([s.in_buf[4], s.in_buf[5]]);
    let corr = i32::from_le_bytes([s.in_buf[6], s.in_buf[7], s.in_buf[8], s.in_buf[9]]);
    let end = plen;

    let Some((mut off, gs, gl)) = kstr(&s.in_buf, 10, end) else { return; };
    let mut group = [0u8; KG_NAME];
    let gl = gl.min(KG_NAME);
    group[..gl].copy_from_slice(&s.in_buf[gs..gs + gl]);
    if off + 4 > end { return; }
    let topic_count = i32::from_be_bytes([
        s.in_buf[off], s.in_buf[off + 1], s.in_buf[off + 2], s.in_buf[off + 3],
    ]).clamp(0, 4);
    off += 4;

    let mut p = 0usize;
    if v >= 3 {
        s.out_buf[p..p + 4].copy_from_slice(&0i32.to_be_bytes()); p += 4;
    }
    s.out_buf[p..p + 4].copy_from_slice(&topic_count.to_be_bytes()); p += 4;
    for _ in 0..topic_count {
        let Some((o2, ts, tl)) = kstr(&s.in_buf, off, end) else { return; };
        off = o2;
        let tl = tl.min(KAFKA_MAX_TOPIC);
        let mut topic = [0u8; KAFKA_MAX_TOPIC];
        topic[..tl].copy_from_slice(&s.in_buf[ts..ts + tl]);
        if off + 4 > end { return; }
        let pc = i32::from_be_bytes([
            s.in_buf[off], s.in_buf[off + 1], s.in_buf[off + 2], s.in_buf[off + 3],
        ]).clamp(0, 8);
        off += 4;
        s.out_buf[p..p + 2].copy_from_slice(&(tl as i16).to_be_bytes()); p += 2;
        s.out_buf[p..p + tl].copy_from_slice(&topic[..tl]); p += tl;
        s.out_buf[p..p + 4].copy_from_slice(&pc.to_be_bytes()); p += 4;
        for _ in 0..pc {
            if off + 4 > end { return; }
            let part = i32::from_be_bytes([
                s.in_buf[off], s.in_buf[off + 1], s.in_buf[off + 2], s.in_buf[off + 3],
            ]);
            off += 4;
            let o = consumers::offset_get(&s.consumers, &group[..gl], &topic[..tl], part as u16);
            s.out_buf[p..p + 4].copy_from_slice(&part.to_be_bytes()); p += 4;
            s.out_buf[p..p + 8].copy_from_slice(&o.to_be_bytes()); p += 8;
            s.out_buf[p..p + 2].copy_from_slice(&(-1i16).to_be_bytes()); p += 2; // metadata null
            s.out_buf[p..p + 2].copy_from_slice(&0i16.to_be_bytes()); p += 2;
        }
    }
    if v >= 2 {
        s.out_buf[p..p + 2].copy_from_slice(&0i16.to_be_bytes()); p += 2; // top-level err
    }
    emit_kafka_response_outbuf(s, sys, conn_id, 9, v, corr, p);
}

// ── Generic connection teardown ─────────────────────────────────────────────

/// Release every piece of state keyed to a now-closed connection. Driven
/// by MSG_SESSION_DISCONNECT (peer_router socket close → codec → here).
/// Protocol-neutral by construction: it walks the conn-keyed tables and
/// drops matching entries. MQTT sessions are intentionally NOT touched —
/// their teardown (will firing, session-expiry) runs on the apply-side
/// keep-alive path so the durable semantics are preserved.
fn handle_conn_disconnect(s: &mut ModuleState, conn_id: u8) {
    // Releases both this conn's AMQP push consumers and its Kafka group
    // memberships. The former stops the delivery pump emitting
    // Basic.Deliver to a conn_id a different client may now own
    // (cross-client delivery + confidentiality leak); the latter stops a
    // crashed consumer lingering as a ghost (leadership stuck on a dead
    // member, table exhaustion). Generation bumps and empty-group reaping
    // are the component's own invariants.
    consumers::release_conn(&mut s.consumers, conn_id);
}

// ── AMQP push consumers ─────────────────────────────────────────────────────

#[cfg(feature = "amqp")]
/// op=3 consume-start from `protocol::amqp`:
///   [13-byte prefix][flags:u8 (bit0 no_ack)][prefetch:u16 LE]
///   [tag_len:u16 LE][tag][q_len:u16 LE][queue]
///
/// # Safety
unsafe fn handle_amqp_consume(s: &mut ModuleState, sys: &SyscallTable, plen: usize) {
    if plen < 18 { return; }
    let conn_id = s.in_buf[0];
    let channel = u16::from_le_bytes([s.in_buf[3], s.in_buf[4]]);
    let flags = s.in_buf[13];
    let prefetch = u16::from_le_bytes([s.in_buf[14], s.in_buf[15]]);
    let tl = u16::from_le_bytes([s.in_buf[16], s.in_buf[17]]) as usize;
    if tl == 0 || tl > AMQP_TAG_MAX || 18 + tl + 2 > plen { return; }
    let toff = 18;
    let ql = u16::from_le_bytes([s.in_buf[18 + tl], s.in_buf[19 + tl]]) as usize;
    if ql == 0 || ql > KAFKA_MAX_TOPIC || 20 + tl + ql > plen { return; }
    let qoff = 20 + tl;

    // Replaces any existing consumer on this (conn, channel) — AMQP
    // allows one per channel here, so a re-Consume replaces rather than
    // duplicates.
    let Some(slot) = consumers::consumer_slot(&s.consumers, conn_id, channel) else {
        // Table full: broker-initiated cancel so the client knows.
        let mut rest = [0u8; 2 + AMQP_TAG_MAX];
        rest[0..2].copy_from_slice(&(tl as u16).to_le_bytes());
        rest[2..2 + tl].copy_from_slice(&s.in_buf[toff..toff + tl]);
        emit_amqp_response(sys, s.out_codec, conn_id, 4, channel, &rest[..2 + tl]);
        return;
    };
    let mut tag = [0u8; AMQP_TAG_MAX];
    tag[..tl].copy_from_slice(&s.in_buf[toff..toff + tl]);
    let mut queue = [0u8; KAFKA_MAX_TOPIC];
    queue[..ql].copy_from_slice(&s.in_buf[qoff..qoff + ql]);
    // Start from the queue's current Get cursor if the store exists so
    // Get-consumed messages aren't redelivered; else from log start.
    let cursor = match store::find(&s.store, &queue[..ql], 0) {
        Some(pi) => store::get_cursor(&s.store, pi).max(store::log_start(&s.store, pi)),
        None => 0,
    };
    consumers::consumer_register(
        &mut s.consumers, slot, conn_id, channel, flags & 1 == 1, prefetch,
        &tag[..tl], &queue[..ql], cursor,
    );
}

#[cfg(feature = "amqp")]
/// op=4 consume-cancel: [13-byte prefix][tag_len:u16 LE][tag]
///
/// # Safety
unsafe fn handle_amqp_cancel(s: &mut ModuleState, plen: usize) {
    if plen < 15 { return; }
    let conn_id = s.in_buf[0];
    let channel = u16::from_le_bytes([s.in_buf[3], s.in_buf[4]]);
    let tl = u16::from_le_bytes([s.in_buf[13], s.in_buf[14]]) as usize;
    if tl == 0 || tl > AMQP_TAG_MAX || 15 + tl > plen { return; }
    let mut tag = [0u8; AMQP_TAG_MAX];
    tag[..tl].copy_from_slice(&s.in_buf[15..15 + tl]);
    if let Some(ci) = consumers::consumer_by_tag(&s.consumers, conn_id, channel, &tag[..tl]) {
        consumers::consumer_release(&mut s.consumers, ci);
    }
}

#[cfg(feature = "amqp")]
/// op=5 client ack/nack: [13-byte prefix, dt = delivery-tag][flags:u8]
/// flags bit0 = multiple. Releases prefetch credit tag-accurately.
///
/// Outstanding delivery tags for a consumer are the contiguous range
/// `(last_acked_dtag, next_dtag)`; this lets every ack form resolve
/// exactly:
///   - single ack of `dt`  → releases one credit iff `dt` is in range;
///     advances `last_acked_dtag` when `dt` is the range's low end (the
///     normal in-order case).
///   - `multiple` ack of `dt` → releases every outstanding tag ≤ `dt`.
///   - `dt == 0, multiple`   → AMQP "acknowledge all outstanding", the
///     common `basic_ack(0, multiple=True)` idiom; releases every
///     outstanding tag, so a manual-ack consumer never wedges at its
///     prefetch limit.
/// Nack/Reject release credit without redelivery (documented v1 gap).
///
/// # Safety
unsafe fn handle_amqp_client_ack(s: &mut ModuleState, plen: usize) {
    if plen < 14 { return; }
    let conn_id = s.in_buf[0];
    let channel = u16::from_le_bytes([s.in_buf[3], s.in_buf[4]]);
    let dt = u64::from_le_bytes([
        s.in_buf[5], s.in_buf[6], s.in_buf[7], s.in_buf[8],
        s.in_buf[9], s.in_buf[10], s.in_buf[11], s.in_buf[12],
    ]);
    let multiple = s.in_buf[13] & 1 != 0;
    s.amqp_consumer_acks = s.amqp_consumer_acks.wrapping_add(1);
    if let Some(ci) = consumers::consumer_on_channel(&s.consumers, conn_id, channel) {
        consumers::consumer_ack(&mut s.consumers, ci, dt, multiple);
    }
}

#[cfg(feature = "amqp")]
/// Delivery pump: push raw store entries to registered consumers with
/// available prefetch credit. Called once per module_step.
///
/// # Safety
unsafe fn amqp_delivery_pump(s: &mut ModuleState, sys: &SyscallTable) {
    for ci in 0..ACONSUMERS {
        if !consumers::consumer_active(&s.consumers, ci) { continue; }
        let mut queue = [0u8; KAFKA_MAX_TOPIC];
        let ql = consumers::consumer_queue_into(&s.consumers, ci, &mut queue);
        let Some(pi) = store::find(&s.store, &queue[..ql], 0) else { continue; };
        for _ in 0..AMQP_DELIVER_QUOTA {
            if !consumers::consumer_in_credit(&s.consumers, ci) { break; }
            let Some(view) = consumers::consumer_view(&s.consumers, ci) else { break; };
            let cursor = view.cursor;
            // Find the next raw entry at/past the cursor.
            if store::is_empty(&s.store, pi) || cursor >= store::next_offset(&s.store, pi) {
                break;
            }
            let Some((data_pos, len, offset, _rest_after)) =
                store::next_raw_entry(&s.store, pi, cursor)
            else {
                break;
            };

            // Deliver: [dtag u64 LE][redelivered u8][tag_len u16 LE][tag][payload]
            let mut tag = [0u8; AMQP_TAG_MAX];
            let tl = consumers::consumer_tag_into(&s.consumers, ci, &mut tag);
            let l = len as usize;
            let mut rest = [0u8; 2048 + 64];
            if 11 + tl + l > rest.len() { break; }
            rest[0..8].copy_from_slice(&view.next_dtag.to_le_bytes());
            rest[8] = 0;
            rest[9..11].copy_from_slice(&(tl as u16).to_le_bytes());
            rest[11..11 + tl].copy_from_slice(&tag[..tl]);
            store::copy_entry_into(
                &s.store, pi, data_pos as usize, l, &mut rest[11 + tl..11 + tl + l],
            );
            if !emit_amqp_response(
                sys, s.out_codec, view.conn_id, 3, view.channel, &rest[..11 + tl + l],
            ) {
                // Backpressure: retry next tick from the same cursor.
                break;
            }
            consumers::consumer_delivered(&mut s.consumers, ci, offset);
            s.amqp_delivers = s.amqp_delivers.wrapping_add(1);
        }
    }
}

/// Wire-envelope emit (fluxor `[mtype][len][payload]` format) for non-codec
/// outputs like consensus, topic_engine, messaging, etc. Returns
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
    let env_len = correlate::stash_env_len(&s.correlate, stash_idx);
    match correlate::stash_dedup_state(&s.correlate, stash_idx) {
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
            let stash_env = correlate::stash_env(&s.correlate, stash_idx);
            let pub_qos = stash_env[0];
            let topic_len = u16::from_be_bytes([
                stash_env[16],
                stash_env[17],
            ]) as usize;
            if 18 + topic_len + 1 > env_len {
                correlate::stash_release(&mut s.correlate, stash_idx);
                return true;
            }
            let up_off = 18 + topic_len;
            let stash_slice = &stash_env[..env_len];
            let Some(up_len) = user_props_block_len(&stash_slice[up_off..env_len]) else {
                correlate::stash_release(&mut s.correlate, stash_idx);
                return true;
            };
            let payload_off = up_off + up_len;
            if payload_off > env_len {
                correlate::stash_release(&mut s.correlate, stash_idx);
                return true;
            }
            let payload_len = env_len - payload_off;
            let topic_pub_len = 4 + 1 + 1 + 2 + topic_len + up_len + payload_len;
            if topic_pub_len > s.out_buf.len() {
                correlate::stash_release(&mut s.correlate, stash_idx);
                return true;
            }
            // TENANCY GAP: the stash is keyed by correlation, not session,
            // so no per-session tenant is reachable here. See
            // docs/architecture/multi_tenancy.md.
            let tenant: TenantId = 0;
            s.out_buf[0..4].copy_from_slice(&tenant.to_le_bytes());
            s.out_buf[4] = pub_qos;
            s.out_buf[5] = 0;
            s.out_buf[6..8].copy_from_slice(&(topic_len as u16).to_le_bytes());
            // topic + user_props + payload sit contiguous in the stash
            // starting at offset 18; a single copy reproduces the
            // downstream layout (offset 8 of out_buf).
            let src = correlate::stash_env(&s.correlate, stash_idx).as_ptr().add(18);
            let dst = s.out_buf.as_mut_ptr().add(8);
            core::ptr::copy_nonoverlapping(src, dst, topic_len + up_len + payload_len);
            if try_emit(
                sys, s.out_topic, wire::MSG_TOPIC_PUBLISH,
                &s.out_buf[..topic_pub_len],
            ) {
                correlate::stash_release(&mut s.correlate, stash_idx);
                true
            } else {
                // topic_engine.op_in saturated. Keep parked; a later tick
                // retries via finalise_durable_stashes().
                false
            }
        }
        STASH_DEDUP_DUPLICATE => {
            correlate::stash_release(&mut s.correlate, stash_idx);
            true
        }
        _ => {
            // Pending state shouldn't normally reach finalise. Treat as
            // released to avoid leaking the slot.
            correlate::stash_release(&mut s.correlate, stash_idx);
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
        if !correlate::stash_occupied(&s.correlate, i) { continue; }
        if !correlate::stash_is_durable(&s.correlate, i) { continue; }
        if correlate::stash_dedup_state(&s.correlate, i) == STASH_DEDUP_PENDING { continue; }
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

/// Push a MSG_TOPIC_DELIVER envelope into the offline queue for replay on the
/// subscriber's next reconnect. Body shape: `[session:u32][env_len:u16][env]`.
/// On out_messaging backpressure the enqueue is dropped — the offline queue
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
    // perspective: the message is now the offline queue's responsibility.
    if !sessions::is_active(&s.sessions, session_slot) {
        if sessions::is_persisted(&s.sessions, session_slot) {
            enqueue_offline(s, sys, session_slot as u32, env);
            return DeliverResult::Delivered;
        }
        return DeliverResult::Dropped;
    }
    let conn_id = sessions::conn_id(&s.sessions, session_slot);
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
        let credit = sessions::prefetch_credit(&s.sessions, session_slot);
        if credit > 0 && sessions::sub_outstanding(&s.sessions, session_slot) >= credit {
            return DeliverResult::Backpressured;
        }
        // MQTT 5 §3.3.4 ReceiveMaximum — the client's hard cap on
        // concurrent unacked QoS 1+2 publishes. Separate from
        // prefetch_credit (which is operator-set / dynamic); the
        // effective cap is the tighter of the two when both are > 0.
        // The pending_dlv park path handles the retry on PUBACK.
        let rxmax = sessions::receive_maximum(&s.sessions, session_slot) as u32;
        if rxmax > 0 && sessions::sub_outstanding(&s.sessions, session_slot) >= rxmax {
            return DeliverResult::Backpressured;
        }
    }
    if s.pid_entry_credits <= 0 || s.pid_byte_credits < body_cost {
        return DeliverResult::Backpressured;
    }

    let mut sub_packet_id: u16 = 0;
    if sub_qos > 0 {
        sub_packet_id = sessions::next_sub_packet_id(&mut s.sessions, session_slot);
        if sessions::inflight_add(&mut s.sessions, session_slot, sub_packet_id, sub_qos, INFLIGHT_SUB)
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
            sessions::note_delivery(&mut s.sessions, session_slot);
            let mut lag_msg = [0u8; 8];
            lag_msg[0..4].copy_from_slice(&(session_slot as u32).to_le_bytes());
            lag_msg[4..8].copy_from_slice(&sessions::sub_outstanding(&s.sessions, session_slot).to_le_bytes());
            try_emit(sys, s.out_forward, wire::MSG_LAG_SIGNAL, &lag_msg);
        }
        DeliverResult::Delivered
    } else {
        if sub_qos > 0 {
            if let Some(ii) = sessions::inflight_find(&s.sessions, session_slot, sub_packet_id, INFLIGHT_SUB)
            {
                sessions::inflight_release(&mut s.sessions, session_slot, ii);
            }
        }
        DeliverResult::Backpressured
    }
}

/// Apply-pipeline reset (docs/architecture/apply_path.md §Apply-pipeline reset).
/// Wipes every apply-derived arena in `session_processor`, fast-forwards
/// `apply_index` to `reset_index`, and emits `MSG_APPLY_RESET_FANOUT` on
/// each downstream bus so topic_engine and messaging's dedup / retained /
/// offline components clear their own state. Until snapshot install
/// ships, state is rebuilt purely from `MSG_COMMITTED_ENTRY` events at
/// or above `reset_index`.
///
/// # Safety
/// Caller must hold an exclusive `&mut ModuleState` and supply a valid
/// `&SyscallTable`.
unsafe fn apply_reset(s: &mut ModuleState, sys: &SyscallTable, reset_index: u64) {
    s.apply_resets = s.apply_resets.wrapping_add(1);

    // The message store is purely apply-derived: wipe and let replay
    // repopulate (logical offsets are deterministic in log order).
    store::reset_parts(&mut s.store);

    // Clear apply-derived session state. Propose-side transient slots
    // (admission state held between CONNECT receipt and QOP_CONNECT
    // apply) are also wiped — after a reset, the leader-local view
    // matches the substrate's view, which means any pending CONNECT
    // is lost. Clients reconnect; that's the documented failover
    // semantic.
    for i in 0..MAX_SESSIONS {
        sessions::clear(&mut s.sessions, i);
        sessions::clear_flow(&mut s.sessions, i);
    }
    s.session_count = 0;
    correlate::reset(&mut s.correlate);
    s.apply_index = reset_index;

    // Fan the reset out to downstream modules. `out_topic` covers
    // topic_engine; `out_messaging` covers messaging's dedup, retained and
    // offline components (they share the messaging bus, demuxing by
    // msg_type). Each downstream handler is "wipe everything" until
    // snapshot install lands.
    let body = reset_index.to_le_bytes();
    try_emit(sys, s.out_topic, wire::MSG_APPLY_RESET_FANOUT, &body);
    try_emit(sys, s.out_messaging, wire::MSG_APPLY_RESET_FANOUT, &body);
}

// ── Apply-side dispatcher (docs/architecture/apply_path.md §Apply-side dispatch) ─────
//
// Routes peeled canonical Quantum proposals to per-op handlers. Each
// handler owns the durable mutations and downstream emissions for its
// op; this dispatcher exists only to demultiplex. Until every op is
// converted, only those listed in the match arm have apply-side
// handlers; the rest are silently skipped because their propose-side
// path still mutates state inline. See `docs/architecture/apply_path.md`
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
        // Committed Kafka batches / AMQP messages land in the apply-side
        // message store, which Kafka Fetch and AMQP Basic.Get serve from.
        #[cfg(feature = "kafka")]
        wire::QOP_KAFKA_PRODUCE => {
            let body = core::slice::from_raw_parts(
                s.in_buf.as_ptr().add(body_start), body_end - body_start,
            );
            apply_kafka_produce(s, body);
        }
        #[cfg(feature = "amqp")]
        wire::QOP_AMQP_PUBLISH => {
            let body = core::slice::from_raw_parts(
                s.in_buf.as_ptr().add(body_start), body_end - body_start,
            );
            apply_amqp_publish(s, body);
        }
        // Durable consumer-group offset commit: replay repopulates the
        // offsets table (leader stores at propose time too — idempotent).
        #[cfg(feature = "kafka")]
        wire::QOP_KAFKA_OFFSET => {
            let b = core::slice::from_raw_parts(
                s.in_buf.as_ptr().add(body_start), body_end - body_start,
            );
            if b.len() >= 2 {
                let gl = u16::from_le_bytes([b[0], b[1]]) as usize;
                if gl > 0 && gl <= KG_NAME && 2 + gl + 2 <= b.len() {
                    let tl = u16::from_le_bytes([b[2 + gl], b[3 + gl]]) as usize;
                    if tl > 0 && tl <= KAFKA_MAX_TOPIC && 4 + gl + tl + 10 <= b.len() {
                        let mut group = [0u8; KG_NAME];
                        group[..gl].copy_from_slice(&b[2..2 + gl]);
                        let mut topic = [0u8; KAFKA_MAX_TOPIC];
                        topic[..tl].copy_from_slice(&b[4 + gl..4 + gl + tl]);
                        let po = 4 + gl + tl;
                        let part = u16::from_le_bytes([b[po], b[po + 1]]);
                        let off = i64::from_le_bytes([
                            b[po + 2], b[po + 3], b[po + 4], b[po + 5],
                            b[po + 6], b[po + 7], b[po + 8], b[po + 9],
                        ]);
                        consumers::offset_store(&mut s.consumers, &group[..gl], &topic[..tl], part, off);
                    }
                }
            }
        }
        // QOP_RETAINED_CLEAR has no propose-side emitter yet.
        _ => {}
    }
}

/// Apply-side handler for QOP_CONNECT. Promotes a transient slot
/// (leader path) or builds a fresh durable record (follower / replay
/// path), bumps `session_epoch`, and fans out the
/// `MSG_SESSION_DROP` / `MSG_OFFLINE_RECONNECT` notices to downstream
/// modules, so every node observes the same downstream effects.
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
    let prior = sessions::find_by_stream(&s.sessions, tenant, stream_hash);
    let was_transient = match prior {
        Some(i) => sessions::is_transient(&s.sessions, i),
        None => false,
    };
    let was_persisted = match prior {
        Some(i) => sessions::is_persisted(&s.sessions, i),
        None => false,
    };
    let was_active = match prior {
        Some(i) => sessions::is_active(&s.sessions, i),
        None => false,
    };
    let session_idx = if let Some(i) = prior {
        Some(i)
    } else {
        sessions::allocate(&s.sessions)
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
        // returns from the ack component but find_inflight_dir misses, so the
        // publisher hangs forever. This races in only under concurrent
        // first-publish bursts, which is why single-client load never hit
        // it. See docs/architecture/apply_path.md.
        if !was_transient {
            sessions::reset_delivery_state(&mut s.sessions, i);
        sessions::clear_flow(&mut s.sessions, i);
        }
    }

    // MQTT 5 §3.1.3.2.2: a fresh CONNECT taking over an existing
    // Session cancels any pending Will-Delay publication. Clear
    // regardless of clean_start — the new connection inherits or
    // replaces the prior Will, so the deferred fire from the
    // previous disconnect must not run.
    if prior.is_some() {
        sessions::set_will_deadline(&mut s.sessions, i, 0);
    }

    // Durable identity + parameters (idempotent on the leader where
    // propose-side already set most of these on the transient slot).
    sessions::commit_connect(
        &mut s.sessions, i,
        sessions::ConnectParams {
            tenant,
            stream_hash,
            protocol,
            clean_start,
            keep_alive_ms: keep_alive_s.saturating_mul(1000),
        },
    );

    if was_transient {
        // Leader path: propose-side admitted, apply now makes it durable.
        if !was_active {
            s.session_count = s.session_count.wrapping_add(1);
        }
        sessions::mark_active(&mut s.sessions, i);
    } else if !was_active && !clean_start {
        // Follower / replay path under !clean_start: park the durable
        // record so a future reconnect on this node can resurrect it.
        sessions::mark_persisted(&mut s.sessions, i);
    }

    // Resurrecting a persisted session under !clean_start: drain the
    // offline queue. Every node fires this signal — leader's drain
    // becomes try_deliver → codec, follower's drain hits an inactive
    // slot in try_deliver and re-parks via the persisted path. No
    // message loss because the offline-queue state is replicated.
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
        let mut wt = [0u8; MAX_WILL_TOPIC];
        let mut wp = [0u8; MAX_WILL_PAYLOAD];
        core::ptr::copy_nonoverlapping(
            s.in_buf.as_ptr().add(will_topic_off), wt.as_mut_ptr(), will_topic_len,
        );
        core::ptr::copy_nonoverlapping(
            s.in_buf.as_ptr().add(will_payload_off), wp.as_mut_ptr(), will_payload_len,
        );
        sessions::set_will(
            &mut s.sessions, i, will_qos, will_retain != 0,
            will_delay_s.saturating_mul(1000),
            &wt[..will_topic_len], &wp[..will_payload_len],
        );
    } else {
        sessions::clear_will(&mut s.sessions, i);
    }

    // Session expiry policy stays attached to the slot for the duration
    // of this CONNECT. The apply-side disconnect path consults it; the
    // metrics-tick sweep purges persisted slots whose deadline elapses.
    sessions::set_session_expiry(&mut s.sessions, i, session_expiry_s);
    // ReceiveMaximum caps how many concurrent unacked QoS 1+ deliveries
    // try_deliver will push to this subscriber. 0 means "no cap".
    sessions::set_receive_maximum(&mut s.sessions, i, receive_maximum);

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
    let Some(will) = sessions::will_view(&s.sessions, i) else { return; };
    let (topic_len, payload_len) = (will.topic_len, will.payload_len);
    let (pub_qos, retain) = (will.qos, will.retain);
    let mut will_topic = [0u8; MAX_WILL_TOPIC];
    let mut will_payload = [0u8; MAX_WILL_PAYLOAD];
    sessions::will_topic_into(&s.sessions, i, &mut will_topic);
    sessions::will_payload_into(&s.sessions, i, &mut will_payload);
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
            will_topic.as_ptr(),
            s.out_buf.as_mut_ptr().add(8),
            topic_len,
        );
        s.out_buf[8 + topic_len] = 0;
        core::ptr::copy_nonoverlapping(
            will_payload.as_ptr(),
            s.out_buf.as_mut_ptr().add(8 + topic_len + 1),
            payload_len,
        );
        try_emit(sys, s.out_topic, wire::MSG_TOPIC_PUBLISH, &s.out_buf[..dlv_total]);
    }
    // Retain=1 Will latches in the retained store the same way as an
    // ordinary retained PUBLISH (item 2's wire format).
    if retain {
        let topic_hash = wire::fnv1a_64(
            &will_topic[..topic_len],
        );
        let rwrite_len = 4 + 8 + 2 + topic_len + 4 + payload_len;
        if rwrite_len <= s.out_buf.len() {
            s.out_buf[0..4].copy_from_slice(&tenant.to_le_bytes());
            s.out_buf[4..12].copy_from_slice(&topic_hash.to_le_bytes());
            s.out_buf[12..14].copy_from_slice(&(topic_len as u16).to_le_bytes());
            core::ptr::copy_nonoverlapping(
                will_topic.as_ptr(),
                s.out_buf.as_mut_ptr().add(14),
                topic_len,
            );
            let pl_off = 14 + topic_len;
            s.out_buf[pl_off..pl_off + 4]
                .copy_from_slice(&(payload_len as u32).to_le_bytes());
            core::ptr::copy_nonoverlapping(
                will_payload.as_ptr(),
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
    let Some(i) = sessions::find_by_stream(&s.sessions, tenant, stream_hash) else { return; };

    // Will-message handling (MQTT 3.1.1 §3.1.2.5 / MQTT 5 §3.1.3.2):
    // any non-CLEAN reason — keep-alive timeout, forced admin
    // teardown, in future TCP-reset detection — fires the stored Will
    // on behalf of the client. A clean DISCONNECT packet discards the
    // Will without publishing. MQTT 5 adds a Will Delay Interval
    // (§3.1.3.2.2): the broker MUST wait `will_delay_ms` before
    // publishing, OR until the Session ends, whichever comes first.
    // A reconnect within the delay window cancels the Will entirely.
    if reason != wire::QDISC_REASON_CLEAN && sessions::will_present(&s.sessions, i) {
        let will_delay_ms = sessions::will_delay_ms(&s.sessions, i);
        if will_delay_ms == 0 {
            // No delay — fire now and consume the slot's Will state.
            fire_will(s, sys, tenant, i);
            sessions::clear_will(&mut s.sessions, i);
            sessions::clear_will(&mut s.sessions, i);
        } else {
            // Schedule a deferred fire. The sweep walks
            // `pending_will_fire_at_ms` once per second. Cap the
            // deadline at the session-end deadline so an in-flight
            // session_expiry purge can't outlast the Will: §3.1.3.2.2
            // says publish OR end-of-session, whichever first.
            let fire_at = now.saturating_add(will_delay_ms as u64);
            let expiry_s = sessions::session_expiry_s(&s.sessions, i);
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
            sessions::set_will_deadline(&mut s.sessions, i, fire_at.min(session_end));
            // DO NOT clear will_* fields — the sweep needs them.
        }
    } else {
        // CLEAN disconnect (or no Will): the Will is consumed without
        // publishing per §3.1.2.5.
        sessions::clear_will(&mut s.sessions, i);
        sessions::clear_will(&mut s.sessions, i);
        sessions::set_will_deadline(&mut s.sessions, i, 0);
    }

    let was_active = sessions::is_active(&s.sessions, i);
    let was_clean = sessions::clean_start(&s.sessions, i);
    // MQTT 5 §3.1.2.11.2: session_expiry_s == 0 means the session
    // ends at network disconnect even when clean_start=false. Treat
    // it the same as a clean session here so we don't pay the cost
    // of carrying a slot that the sweep would purge on the next tick
    // anyway.
    let expire_now = sessions::session_expiry_s(&s.sessions, i) == 0;
    if was_active {
        s.session_count = s.session_count.saturating_sub(1);
    }
    if was_clean || expire_now {
        // Clean session OR session_expiry==0: drop everything so the
        // slot is fully free.
        sessions::clear_flow(&mut s.sessions, i);
        sessions::close(&mut s.sessions, i, false, now);
        let drop_body = (i as u32).to_le_bytes();
        try_emit(sys, s.out_topic, wire::MSG_SESSION_DROP, &drop_body);
    } else {
        // Persistent session: park the slot for matching reconnect.
        // Stamp `disconnected_at_ms` so the expiry sweep can purge
        // the slot once `now > disconnected_at + session_expiry`.
        // `session_expiry_s == u32::MAX` makes the sweep skip the
        // slot — that's the propose-side normalisation for MQTT 3.1.1
        // clean_session=0.
        sessions::close(&mut s.sessions, i, true, now);
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
    let Some(i) = sessions::find_by_stream(&s.sessions, tenant, stream_hash) else { return; };
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
    // pattern. The retained component iterates its entries and runs
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
    let Some(i) = sessions::find_by_stream(&s.sessions, tenant, stream_hash) else { return; };
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
    let Some(i) = sessions::find_by_stream(&s.sessions, tenant, stream_hash) else { return; };
    if let Some(ii) = sessions::inflight_find(&s.sessions, i, packet_id, INFLIGHT_PUB) {
        sessions::inflight_set_phase(&mut s.sessions, i, ii, QOS2_PUBREL);
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
    // call, so subsequent mutations to s.out_buf / the stash are safe.
    let topic_hash = wire::fnv1a_64(&s.in_buf[topic_off..topic_off + topic_len]);

    // For QoS 1+, look up correlation_id from the leader's publisher
    // inflight (propose-side allocates correlation + inflight together).
    // Followers have no inflight here, so correlation_id stays 0 and
    // QoS 1+ falls back to the direct emit path — leader fan-out is
    // still gated through the stash via the correlation_id !=0 branch.
    let correlation_id: u64 = if pub_qos > 0 {
        match sessions::find_by_stream(&s.sessions, tenant, stream_hash) {
            Some(i) => match sessions::inflight_find(&s.sessions, i, packet_id, INFLIGHT_PUB) {
                Some(ii) => sessions::inflight_view(&s.sessions, i, ii)
                    .map(|v| v.correlation_id)
                    .unwrap_or(0),
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
            if let Some(stash_idx) = correlate::stash_alloc(&mut s.correlate, correlation_id, &dkey) {
                let env = core::slice::from_raw_parts(
                    s.in_buf.as_ptr().add(body_start), body_len,
                );
                correlate::stash_set_env(&mut s.correlate, stash_idx, env);
                correlate::stash_mark_durable(&mut s.correlate, stash_idx);
                try_emit(sys, s.out_messaging, wire::MSG_DEDUP_CHECK, &dkey);
            }
            // If allocate_stash fails (stash table full), the publish
            // is dropped from the fan-out path; the ack component still fires
            // PUBACK so the publisher's protocol state advances.
        }
    }

    if retain {
        // MSG_RETAINED_WRITE body (extended for inline payload storage):
        //   [tenant:u32 LE][topic_hash:u64 LE][topic_len:u16 LE][topic_bytes]
        //   [payload_len:u32 LE][payload_bytes]
        // the retained store keeps topic + payload inline so the
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
        let mut worked = 0u32;
        let now = dev_millis(sys);

        // ── Phase 1: drain flow signals (wire envelope) ──────────
        // flow_in is shared by admission (MSG_THROTTLE_CREDITS),
        // flow's backpressure (MSG_BP_SIGNAL) and prefetch
        // (MSG_PREFETCH_CREDIT). Demultiplex by msg_type.
        if s.in_flow >= 0 {
            loop {
                let poll = (sys.channel_poll)(s.in_flow, 0x01);
                if poll <= 0 || (poll as u32 & 0x01) == 0 { break; }
                let (mt, plen) = { worked += 1; wire::channel_read_msg(sys, s.in_flow, &mut s.in_buf) };
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
                            sessions::set_prefetch_credit(&mut s.sessions, slot, credit);
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
            // 32 packets/tick: at the apply domain's 250 µs tick this admits
            // ~128k requests/s before the codec_in drain — not the session
            // engine — becomes the pipeline's rate limiter. A smaller quota
            // caps pipelined Kafka produce bursts below the WAL's batch
            // capacity.
            for _ in 0..32 {
                let poll = (sys.channel_poll)(s.in_codec, 0x01);
                if poll <= 0 || (poll as u32 & 0x01) == 0 { break; }

                let (mt, plen) = { worked += 1; wire::channel_read_msg(sys, s.in_codec, &mut s.in_buf) };

                // Transport connection-closed notice (payload `[conn_id]`,
                // forwarded by any codec on socket close). Distinct from
                // MSG_SESSION_DISCONNECT, which is the MQTT DISCONNECT
                // *packet* and must reach the MQTT handler below. Release
                // conn-keyed state for the non-MQTT protocols; MQTT
                // sessions keep their apply-side keep-alive / will-fire
                // lifecycle.
                if mt == wire::MSG_CONN_CLOSED && plen >= 1 {
                    handle_conn_disconnect(s, s.in_buf[0]);
                    continue;
                }
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
                if let Some(si) = sessions::find_by_conn(&s.sessions, conn_id) {
                    sessions::touch(&mut s.sessions, si, now);
                }

                match mt {
                    wire::MSG_SESSION_CONNECT => {
                        s.connects = s.connects.wrapping_add(1);
                        // MQTT CONNECT envelope from `protocol::mqtt`:
                        //   [proto_ver:u8][clean_start:u8][keep_alive:u16 BE]
                        //   [session_expiry:u32 LE][recv_max:u16 LE]
                        //   [cid_len:u16 BE][cid bytes][...]
                        //
                        // docs/architecture/apply_path.md splits
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

                        // MQTT keep_alive (seconds) is parsed by `protocol::mqtt`
                        // into bytes 2..4 BE. 0 means "no keep-alive" per
                        // MQTT 3.1.1 §3.1.2.10.
                        let keep_alive_s = if body.len() >= 4 {
                            u16::from_be_bytes([body[2], body[3]]) as u32
                        } else { 0 };
                        // TENANCY GAP: CONNECT arrival — no session exists
                        // yet, so the tenant would have to come from the
                        // control plane. See docs/architecture/multi_tenancy.md.
                        let tenant: TenantId = 0;
                        let prior = sessions::find_by_stream(&s.sessions, tenant, stream_hash);
                        // Was the matched slot a persisted session that we're
                        // about to resurrect, vs an already-active reconnect
                        // (e.g. same client_id re-CONNECT before DISCONNECT)?
                        // Only the persisted path sets session_present=1 in
                        // the optimistic CONNACK.
                        let resurrecting_persisted = match prior {
                            Some(i) => sessions::is_persisted(&s.sessions, i) && !clean_start,
                            None => false,
                        };
                        let session_idx = if let Some(i) = prior {
                            if clean_start {
                                // Operational reset so the post-CONNACK
                                // packet burst doesn't pick up stale inflight.
                                // Durable counterpart (MSG_SESSION_DROP on
                                // out_topic) lands at QOP_CONNECT apply.
                                sessions::reset_delivery_state(&mut s.sessions, i);
        sessions::clear_flow(&mut s.sessions, i);
                            }
                            sessions::touch(&mut s.sessions, i, now);
                            sessions::rebind(
                                &mut s.sessions, i, conn_id, proto, clean_start,
                                keep_alive_s.saturating_mul(1000),
                            );
                            // Propose-side admission marker; apply flips
                            // transient→active and bumps session_epoch.
                            sessions::mark_transient(&mut s.sessions, i);
                            Some(i)
                        } else if let Some(i) = sessions::allocate(&s.sessions) {
                            // `active` flips at QOP_CONNECT apply; until
                            // then the slot is transient.
                            sessions::open_transient(
                                &mut s.sessions, i, tenant, stream_hash, proto,
                                conn_id, clean_start,
                                keep_alive_s.saturating_mul(1000), now,
                            );
                            // session_count is the durable count of active
                            // sessions; apply-side increments it.
                            Some(i)
                        } else { None };

                        // CONNACK body = [session_present:u8][reason_code:u8].
                        // MQTT 3.1.1 §3.2.2.2 requires session_present=1
                        // when the broker is resurrecting a stored session.
                        // Optimistic — see docs/architecture/apply_path.md
                        // §Optimistic CONNACK.
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
                        // modules/app/protocol/mqtt.rs PKT_CONNECT case).
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
                        // by `protocol::mqtt` at body[8..10] LE (default 0 for
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
                        if let Some(i) = sessions::find_by_conn(&s.sessions, conn_id) {
                            let tenant = sessions::tenant(&s.sessions, i);
                            let stream_hash = sessions::stream_hash(&s.sessions, i);
                            let session_slot = i as u32;
                            // Clear conn_id locally so subsequent packets on
                            // this conn don't re-route to a slot whose
                            // disconnect is in flight.
                            sessions::unbind_conn(&mut s.sessions, i);

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
                                // correlation allocation for the ack component.
                                s.publishes = s.publishes.wrapping_add(1);
                                let qos = (flags >> 1) & 0x03;
                                let retain = flags & 0x01 != 0;

                                // Body shape from `protocol::mqtt`:
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

                                let session_idx = sessions::find_by_conn(&s.sessions, conn_id);
                                let tenant = session_idx
                                    .map(|i| sessions::tenant(&s.sessions, i))
                                    .unwrap_or(0);

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
                                    if sessions::inflight_add(&mut s.sessions, si, packet_id, qos, INFLIGHT_PUB)
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
                                    .map(|si| sessions::stream_hash(&s.sessions, si))
                                    .unwrap_or(0);
                                let session_epoch = session_idx
                                    .map(|si| sessions::session_epoch(&s.sessions, si))
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
                                            if let Some(ii) = sessions::inflight_find(&s.sessions, si, packet_id, INFLIGHT_PUB)
                                            {
                                                sessions::inflight_release(&mut s.sessions, si, ii);
                                            }
                                        }
                                    }
                                    s.publishes_throttled = s.publishes_throttled.wrapping_add(1);
                                    continue;
                                }

                                let cid: u64 = if qos > 0 {
                                    let Some(cid) = correlate::allocate(&mut s.correlate, 
                                        session_slot, packet_id, OP_PUBLISH, now,
                                    ) else {
                                        if let Some(si) = session_idx {
                                            if let Some(ii) = sessions::inflight_find(&s.sessions, si, packet_id, INFLIGHT_PUB)
                                            {
                                                sessions::inflight_release(&mut s.sessions, si, ii);
                                            }
                                        }
                                        correlate::note_dropped(&mut s.correlate);
                                        s.publishes_throttled =
                                            s.publishes_throttled.wrapping_add(1);
                                        continue;
                                    };
                                    // Bind the correlation to the inflight slot
                                    // so MSG_ACK_REDELIVER (apply-side reconstruct
                                    // from stash) and MSG_ACK_EMIT (PUBACK fire)
                                    // can locate this publish.
                                    if let Some(si) = session_idx {
                                        if let Some(ii) = sessions::inflight_find(&s.sessions, si, packet_id, INFLIGHT_PUB)
                                        {
                                            sessions::inflight_set_correlation(&mut s.sessions, si, ii, cid);
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
                                        let _ = correlate::take(&mut s.correlate, cid);
                                        if let Some(si) = session_idx {
                                            if let Some(ii) = sessions::inflight_find(&s.sessions, si, packet_id, INFLIGHT_PUB)
                                            {
                                                sessions::inflight_release(&mut s.sessions, si, ii);
                                            }
                                        }
                                    }
                                    // Count the drop for ALL QoS levels,
                                    // including QoS 0, so offered != accepted
                                    // stays visible — the "no silent drops"
                                    // invariant.
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

                                let session_idx = sessions::find_by_conn(&s.sessions, conn_id);
                                let tenant = session_idx
                                    .map(|i| sessions::tenant(&s.sessions, i))
                                    .unwrap_or(0);
                                let session_slot = session_idx.unwrap_or(0) as u32;
                                let stream_hash = session_idx
                                    .map(|i| sessions::stream_hash(&s.sessions, i))
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

                                let session_idx = sessions::find_by_conn(&s.sessions, conn_id);
                                let tenant = session_idx
                                    .map(|i| sessions::tenant(&s.sessions, i))
                                    .unwrap_or(0);
                                let session_slot = session_idx.unwrap_or(0) as u32;
                                let stream_hash = session_idx
                                    .map(|i| sessions::stream_hash(&s.sessions, i))
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
                                if let Some(si) = sessions::find_by_conn(&s.sessions, conn_id) {
                                    if let Some(ii) = sessions::inflight_find(&s.sessions, si, packet_id, INFLIGHT_SUB) {
                                        if sessions::inflight_view(&s.sessions, si, ii)
                                            .is_some_and(|v| v.phase == QOS2_PUBLISH)
                                        {
                                            sessions::inflight_set_phase(&mut s.sessions, si, ii, QOS2_PUBREL);
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
                                // entry; the ack component → MSG_ACK_EMIT path
                                // then emits PUBCOMP and frees the publisher
                                // inflight. Apply-side records the phase
                                // transition durably (see apply_qop_pubrel).
                                if body.len() < 2 { continue; }
                                let packet_id = u16::from_be_bytes([body[0], body[1]]);
                                s.qos2_rel = s.qos2_rel.wrapping_add(1);

                                let Some(si) = sessions::find_by_conn(&s.sessions, conn_id) else { continue; };
                                // Mark publisher inflight as awaiting PUBREL
                                // durability so a stray PUBCOMP attempt
                                // doesn't fire twice.
                                let inflight_idx = sessions::inflight_find(&s.sessions, si, packet_id, INFLIGHT_PUB);
                                if let Some(ii) = inflight_idx {
                                    sessions::inflight_set_phase(&mut s.sessions, si, ii, QOS2_PUBREL);
                                }

                                let tenant = sessions::tenant(&s.sessions, si);
                                let session_slot = si as u32;
                                let stream_hash = sessions::stream_hash(&s.sessions, si);
                                let Some(cid) = correlate::allocate(&mut s.correlate, 
                                    session_slot, packet_id, OP_PUBREL, now,
                                ) else {
                                    correlate::note_dropped(&mut s.correlate);
                                    continue;
                                };

                                // Bind the new correlation to the inflight
                                // slot so MSG_ACK_REDELIVER can reconstruct
                                // a PUBREL retry without consulting the
                                // (already-released) PUBLISH stash.
                                if let Some(ii) = inflight_idx {
                                    sessions::inflight_set_correlation(&mut s.sessions, si, ii, cid);
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
                                    let _ = correlate::take(&mut s.correlate, cid);
                                    if let Some(ii) = inflight_idx {
                                        sessions::inflight_set_correlation(&mut s.sessions, si, ii, 0);
                                    }
                                    correlate::note_dropped(&mut s.correlate);
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
                                if let Some(si) = sessions::find_by_conn(&s.sessions, conn_id) {
                                    if let Some(ii) = sessions::inflight_find(&s.sessions, si, packet_id, INFLIGHT_SUB) {
                                        sessions::inflight_release(&mut s.sessions, si, ii);
                                        if sessions::sub_outstanding(&s.sessions, si) > 0 {
                                            sessions::note_ack(&mut s.sessions, si);
                                            let mut lag_msg = [0u8; 8];
                                            lag_msg[0..4].copy_from_slice(&(si as u32).to_le_bytes());
                                            lag_msg[4..8].copy_from_slice(&sessions::sub_outstanding(&s.sessions, si).to_le_bytes());
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
                                if let Some(si) = sessions::find_by_conn(&s.sessions, conn_id) {
                                    if let Some(ii) = sessions::inflight_find(&s.sessions, si, packet_id, INFLIGHT_SUB) {
                                        sessions::inflight_release(&mut s.sessions, si, ii);
                                        s.acks_emitted = s.acks_emitted.wrapping_add(1);
                                        if sessions::sub_outstanding(&s.sessions, si) > 0 {
                                            sessions::note_ack(&mut s.sessions, si);
                                            let mut lag_msg = [0u8; 8];
                                            lag_msg[0..4].copy_from_slice(&(si as u32).to_le_bytes());
                                            lag_msg[4..8].copy_from_slice(&sessions::sub_outstanding(&s.sessions, si).to_le_bytes());
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

                    #[cfg(feature = "kafka")]
                    wire::MSG_SESSION_PROPOSAL if proto == PROTO_KAFKA => {
                        // Kafka envelope (see `protocol::kafka`):
                        //   [conn_id][proto=1][api_key:i16 LE][api_ver:i16 LE]
                        //   [corr:i32 LE][request body after client_id]
                        // ApiVersions/Metadata are answered inline by the
                        // codec; the durable/data-path APIs land here.
                        if plen as usize >= 10 {
                            let api_key = i16::from_le_bytes([s.in_buf[2], s.in_buf[3]]);
                            match api_key {
                                0 => handle_kafka_produce(s, sys, now, plen as usize),
                                1 => handle_kafka_fetch(s, sys, plen as usize),
                                2 => handle_kafka_list_offsets(s, sys, plen as usize),
                                8 => handle_kafka_offset_commit(s, sys, plen as usize),
                                9 => handle_kafka_offset_fetch(s, sys, plen as usize),
                                11 => handle_kafka_join_group(s, sys, plen as usize),
                                12 | 13 => handle_kafka_heartbeat_leave(
                                    s, sys, plen as usize, api_key,
                                ),
                                14 => handle_kafka_sync_group(s, sys, plen as usize),
                                _ => {}
                            }
                        }
                    }

                    #[cfg(feature = "amqp")]
                    wire::MSG_SESSION_PROPOSAL if proto == PROTO_AMQP => {
                        // AMQP envelope (see `protocol::amqp`):
                        //   [conn_id][proto=2][op:u8][channel:u16 LE][rest]
                        // op 1 = assembled Basic.Publish (confirm-gated when
                        // delivery_tag != 0), op 2 = Basic.Get.
                        if plen as usize >= 5 {
                            match s.in_buf[2] {
                                1 => handle_amqp_publish(s, sys, now, plen as usize),
                                2 => handle_amqp_get(s, sys, plen as usize),
                                3 => handle_amqp_consume(s, sys, plen as usize),
                                4 => handle_amqp_cancel(s, plen as usize),
                                5 => handle_amqp_client_ack(s, plen as usize),
                                _ => {}
                            }
                        }
                    }

                    wire::MSG_SESSION_PROPOSAL => {
                        // Unknown protocol — forward to raft (legacy path).
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
        //   - `consensus.applied` — MSG_CLIENT_RESPONSE per-batch
        //     notifications (legacy, body-less), bumps `applied`.
        //   - `consensus.committed_entries` — MSG_COMMITTED_ENTRY
        //     per-entry stream `[term:u64][index:u64][body]`.
        //
        // Per-entry bodies that carry a canonical Quantum envelope
        // (`wire::peel_qprop` succeeds) drive durable state mutation
        // through the apply-side dispatcher below. Entries that don't
        // peel (e.g. raw MQTT bodies from ops still on the legacy
        // propose-side path) advance `applied` without dispatching;
        // those ops keep their existing inline mutations until their
        // QOP_* conversion lands. See docs/architecture/apply_path.md.
        if s.in_committed >= 0 {
            for _ in 0..16 {
                let poll = (sys.channel_poll)(s.in_committed, 0x01);
                if poll <= 0 || (poll as u32 & 0x01) == 0 { break; }
                let (mt, plen) = { worked += 1; wire::channel_read_msg(sys, s.in_committed, &mut s.in_buf) };
                if mt == wire::MSG_COMMITTED_ENTRY && plen >= 16 {
                    s.committed_entries_observed =
                        s.committed_entries_observed.wrapping_add(1);
                    let entry_end = plen as usize;
                    // MSG_COMMITTED_ENTRY body: [term:u64][index:u64][entry_body].
                    // Strip the 16-byte prefix and peel the Quantum
                    // canonical envelope (the apply path forwards the
                    // proposer's body verbatim — clustor's tagged-
                    // proposal handler strips any leading correlation_id
                    // upstream).
                    let entry_index = u64::from_le_bytes([
                        s.in_buf[8],  s.in_buf[9],  s.in_buf[10], s.in_buf[11],
                        s.in_buf[12], s.in_buf[13], s.in_buf[14], s.in_buf[15],
                    ]);

                    // Index sequencing (docs/architecture/apply_path.md §Apply-pipeline reset):
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
                        apply_reset(s, sys, entry_index);
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

        // ── Phase 3b: drain proposal-assigned events from consensus ──
        //
        // Each MSG_PROPOSAL_ASSIGNED carries
        //   [correlation_id:u64 LE][partition_id:u16 LE][wal_index:u64 LE]
        // (18 bytes). We forward `(partition_id, wal_index)` to
        // the ack component via MSG_ACK_REGISTER so it can disambiguate the
        // same wal_index arriving from different partitions —
        // ack-on-durability matches by `(partition_id, wal_index)`,
        // not just `wal_index`. Replaces the legacy `wal_index = 0`
        // placeholder + the ack component heuristic.
        if s.in_assigned >= 0 {
            // 32/tick, matching the proposal-side burst capacity — every
            // tagged proposal produces exactly one assignment.
            for _ in 0..32 {
                let poll = (sys.channel_poll)(s.in_assigned, 0x01);
                if poll <= 0 || (poll as u32 & 0x01) == 0 { break; }
                let (mt, plen) = { worked += 1; wire::channel_read_msg(sys, s.in_assigned, &mut s.in_buf) };
                if mt != wire::MSG_PROPOSAL_ASSIGNED || (plen as usize) < wire::PROPOSAL_ASSIGNED_LEN {
                    continue;
                }
                let (cid, partition_id, wal_index) =
                    wire::decode_proposal_assigned(&s.in_buf[..wire::PROPOSAL_ASSIGNED_LEN]);
                if let Some((session_slot, packet_id, _op)) = correlate::take(&mut s.correlate, cid) {
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
                    // benefits from this even if the broker→ack
                    // edge is momentarily saturated.
                    let ssize = session_slot as usize;
                    if ssize < MAX_SESSIONS {
                        if let Some(ii) = sessions::inflight_find(&s.sessions, ssize, packet_id, INFLIGHT_PUB)
                        {
                            sessions::inflight_set_wal_index(&mut s.sessions, ssize, ii, wal_index);
                        }
                    } else if session_slot >= KAFKA_SLOT_BASE {
                        // Durable-publish inflight. `session_slot` carries
                        // the slot index AND its epoch; `packet_id` high
                        // byte carries the partition index. Drop the
                        // assignment if the slot's epoch no longer matches
                        // — the slot was freed and reused (EPOCH TAGGING),
                        // so stamping the new occupant would report a
                        // foreign WAL index as its base_offset.
                        let (ki, epoch) = store::kin_decode_slot(session_slot);
                        let pidx = (packet_id >> 8) as usize;
                        if ki < KAFKA_INFLIGHT
                            && pidx < KIN_MAX_PARTS
                            && store::inflight_valid(&s.store, ki, epoch)
                        {
                            store::inflight_set_part_offset(&mut s.store, ki, pidx, wal_index as i64);
                        }
                    }

                    // Try once; on failure park in the retry queue. Phase 5-pre
                    // drains it next tick. Dropping the register would mean
                    // the ack component never learns of this inflight entry and the
                    // publisher's PUBACK never fires.
                    if !try_emit(sys, s.out_forward, wire::MSG_ACK_REGISTER, &reg) {
                        let mut parked = false;
                        for _ in 0..PENDING_ACK_SLOTS {
                            if correlate::ack_stash(&mut s.correlate, reg) {
                                parked = true;
                                break;
                            }
                        }
                        if !parked {
                            // Pending-ack queue full: the publisher will
                            // eventually time out and retry with DUP set;
                            // the dedup component catches the duplicate.
                            correlate::note_dropped(&mut s.correlate);
                        }
                    }
                }
            }
        }

        // ── Phase 4: drain ACK feedback (wire envelope) ──
        //
        // the ack component emits MSG_ACK_EMIT once quorum durability lands; only
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
        // so a proposal lost between session_processor and consensus has
        // a chance to be picked up.
        if s.in_ack >= 0 {
            // 32/tick: group-fsync durability resolves in bursts (one fence
            // covers up to group_max_pending entries), so ack emission must
            // drain a whole batch in the tick it lands or ack latency grows
            // by a tick per 8 acks.
            for _ in 0..32 {
                let poll = (sys.channel_poll)(s.in_ack, 0x01);
                if poll <= 0 || (poll as u32 & 0x01) == 0 { break; }
                let (mt, plen) = { worked += 1; wire::channel_read_msg(sys, s.in_ack, &mut s.in_buf) };
                if plen < 8 { continue; }
                let session_slot = u32::from_le_bytes([s.in_buf[0], s.in_buf[1], s.in_buf[2], s.in_buf[3]]) as usize;
                let packet_id = u32::from_le_bytes([s.in_buf[4], s.in_buf[5], s.in_buf[6], s.in_buf[7]]) as u16;

                // Kafka produce inflights live in a reserved session_slot
                // namespace (see KAFKA_SLOT_BASE). Quorum durability landed
                // → emit the ProduceResponse with base_offset = wal_index.
                // MSG_ACK_REDELIVER is ignored for Kafka: the producer's own
                // request timeout drives the retry; the stale-entry sweep in
                // the metrics tick reclaims the slot.
                if session_slot >= KAFKA_SLOT_BASE as usize {
                    if mt != wire::MSG_ACK_EMIT { continue; }
                    // Decode slot + epoch from session_slot; partition from
                    // packet_id. An epoch mismatch means this durability
                    // landed for a slot that has since been freed and
                    // reused — drop it (EPOCH TAGGING) rather than
                    // completing an unrelated request's publish.
                    let (ki, epoch) = store::kin_decode_slot(session_slot as u32);
                    let pidx = (packet_id >> 8) as usize;
                    if ki >= KAFKA_INFLIGHT
                        || pidx >= KIN_MAX_PARTS
                        || !store::inflight_valid(&s.store, ki, epoch)
                    {
                        continue;
                    }
                    // Which response shape to emit is the durable-publish
                    // protocol's business, so each arm is gated with its
                    // protocol. A variant carrying neither compiles this
                    // whole resolution away — nothing can populate the
                    // inflight table in that build.
                    #[cfg(feature = "amqp")]
                    if store::inflight_proto(&s.store, ki) == KIN_PROTO_AMQP {
                        // Publisher confirm: Basic.Ack gated on quorum
                        // durability, mirroring the Kafka contract.
                        if let Some(e) = store::inflight_get(&s.store, ki) {
                            let mut rest = [0u8; 9];
                            rest[0..8].copy_from_slice(&e.delivery_tag.to_le_bytes());
                            rest[8] = 0;
                            if emit_amqp_response(
                                sys, s.out_codec, e.conn_id, 1, e.channel, &rest,
                            ) {
                                s.amqp_publish_acked =
                                    s.amqp_publish_acked.wrapping_add(1);
                                store::inflight_free(&mut s.store, ki);
                                s.acks_emitted = s.acks_emitted.wrapping_add(1);
                            }
                            // On emit failure the entry stays active; the
                            // sweep expires it (Nack) and the publisher
                            // retries.
                        }
                        continue;
                    }
                    #[cfg(feature = "kafka")]
                    {
                            // One partition of the request reached quorum
                            // durability. Respond once ALL have resolved.
                            if store::inflight_complete_part(&mut s.store, ki)
                                && emit_kafka_produce_response_multi(s, sys, ki)
                            {
                                s.kafka_produce_acked =
                                    s.kafka_produce_acked.wrapping_add(1);
                                store::inflight_free(&mut s.store, ki);
                                s.acks_emitted = s.acks_emitted.wrapping_add(1);
                            }
                    }
                    continue;
                }
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
                        if !sessions::is_active(&s.sessions, session_slot)
                            && !sessions::is_transient(&s.sessions, session_slot)
                        {
                            continue;
                        }
                        let conn_id = sessions::conn_id(&s.sessions, session_slot);
                        let Some(ii) = sessions::inflight_find(&s.sessions, session_slot, packet_id, INFLIGHT_PUB)
                        else { continue; };
                        let iv = sessions::inflight_view(&s.sessions, session_slot, ii);
                        let qos = iv.map(|v| v.qos).unwrap_or(0);
                        let phase = iv.map(|v| v.phase).unwrap_or(0);
                        let correlation_id = iv.map(|v| v.correlation_id).unwrap_or(0);

                        // Stash bookkeeping for PUBLISH ops — mark durable
                        // and finalise if dedup already resolved.
                        if correlation_id != 0 {
                            if let Some(stash_idx) = correlate::stash_by_correlation(&s.correlate, correlation_id) {
                                correlate::stash_mark_durable(&mut s.correlate, stash_idx);
                                if correlate::stash_dedup_state(&s.correlate, stash_idx) != STASH_DEDUP_PENDING {
                                    finalise_stash(s, sys, stash_idx);
                                }
                            }
                        }

                        let body = packet_id.to_be_bytes();
                        if qos == 1 {
                            sessions::inflight_release(&mut s.sessions, session_slot, ii);
                            emit_codec_response(sys, s.out_codec, conn_id, PROTO_MQTT, PKT_PUBACK, 0, &body);
                        } else if qos == 2 {
                            if phase == QOS2_PUBLISH {
                                // PUBLISH commit landed; emit PUBREC and
                                // keep inflight alive to await PUBREL.
                                emit_codec_response(sys, s.out_codec, conn_id, PROTO_MQTT, PKT_PUBREC, 0, &body);
                            } else if phase == QOS2_PUBREL {
                                // PUBREL commit landed; emit PUBCOMP and
                                // release the publisher inflight.
                                sessions::inflight_release(&mut s.sessions, session_slot, ii);
                                emit_codec_response(sys, s.out_codec, conn_id, PROTO_MQTT, PKT_PUBCOMP, 0, &body);
                                s.qos2_comp = s.qos2_comp.wrapping_add(1);
                            }
                        }
                        s.acks_emitted = s.acks_emitted.wrapping_add(1);
                    }
                    wire::MSG_ACK_REDELIVER => {
                        s.redeliver_signals = s.redeliver_signals.wrapping_add(1);
                        if !sessions::is_active(&s.sessions, session_slot) { continue; }
                        let Some(ii) = sessions::inflight_find(&s.sessions, session_slot, packet_id, INFLIGHT_PUB)
                        else { continue; };
                        let correlation_id = sessions::inflight_view(&s.sessions, session_slot, ii)
                            .map(|v| v.correlation_id).unwrap_or(0);
                        if correlation_id == 0 { continue; }
                        let phase = sessions::inflight_view(&s.sessions, session_slot, ii)
                            .map(|v| v.phase).unwrap_or(0);

                        // For PUBREL-phase inflight, the stash has already
                        // been released after the PUBLISH commit. Resend a
                        // QOP_PUBREL marker carrying the same correlation_id
                        // so the ack component can drive PUBCOMP when this round
                        // commits.
                        if phase == QOS2_PUBREL {
                            let tenant = sessions::tenant(&s.sessions, session_slot);
                            let stream_hash = sessions::stream_hash(&s.sessions, session_slot);
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
                        let Some(stash_idx) = correlate::stash_by_correlation(&s.correlate, correlation_id) else { continue; };
                        let env_len = correlate::stash_env_len(&s.correlate, stash_idx);
                        if env_len < 18 || env_len > MAX_STASH_ENV { continue; }
                        // TENANCY GAP: keyed by correlation, not session.
                        // See docs/architecture/multi_tenancy.md.
                        let tenant: TenantId = 0;
                        let prop_total = wire::QPROP_TAGGED_HDR_LEN + env_len;
                        if prop_total > s.out_buf.len() { continue; }
                        s.out_buf[0..8].copy_from_slice(&correlation_id.to_le_bytes());
                        wire::encode_qprop_header(
                            &mut s.out_buf[8..8 + wire::QPROP_HEADER_LEN],
                            wire::QOP_PUBLISH, tenant, session_slot as u32,
                        );
                        let off = wire::QPROP_TAGGED_HDR_LEN;
                        let stash_ptr = correlate::stash_env(&s.correlate, stash_idx).as_ptr();
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

        // AMQP push delivery: drain the message store to registered
        // consumers with available prefetch credit.
        #[cfg(feature = "amqp")]
        amqp_delivery_pump(s, sys);

        // Phase 5-pre also drains pending ACK_REGISTERs that were
        // deferred by out_forward backpressure (see PROPOSAL_ASSIGNED
        // handler).
        for i in 0..PENDING_ACK_SLOTS {
            let Some(payload) = correlate::ack_get(&s.correlate, i) else { continue; };
            if try_emit(sys, s.out_forward, wire::MSG_ACK_REGISTER, &payload) {
                correlate::ack_free(&mut s.correlate, i);
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
            if !correlate::dlv_is_active(&s.correlate, s_idx) { continue; }
            let env_len = correlate::dlv_len(&s.correlate, s_idx);
            let mut buf = [0u8; MAX_PENDING_DLV];
            if !correlate::dlv_copy_out(&s.correlate, s_idx, &mut buf) {
                correlate::dlv_free(&mut s.correlate, s_idx);
                continue;
            }
            match try_deliver(s, sys, &buf[..env_len]) {
                DeliverResult::Delivered | DeliverResult::Dropped => {
                    correlate::dlv_free(&mut s.correlate, s_idx);
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
                let (mt, plen) = { worked += 1; wire::channel_read_msg(sys, s.in_deliver, &mut s.in_buf) };
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
                        if !correlate::dlv_park(&mut s.correlate, &buf[..plen]) {
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
                let (mt, plen) = { worked += 1; wire::channel_read_msg(sys, s.in_messaging, &mut s.in_buf) };
                let plen = plen as usize;
                match mt {
                    wire::MSG_DEDUP_RESULT if plen >= 21 => {
                        let mut key = [0u8; 20];
                        key.copy_from_slice(&s.in_buf[..20]);
                        let duplicate = s.in_buf[20];
                        if let Some(stash_idx) = correlate::stash_by_dedup_key(&s.correlate, &key) {
                            correlate::stash_set_dedup_state(
                                &mut s.correlate,
                                stash_idx,
                                if duplicate != 0 { STASH_DEDUP_DUPLICATE } else { STASH_DEDUP_OK },
                            );
                            if correlate::stash_is_durable(&s.correlate, stash_idx) {
                                finalise_stash(s, sys, stash_idx);
                            }
                        }
                    }
                    wire::MSG_RETAINED_READ if plen >= 23 => {
                        // Response body from the retained store (hit only):
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
                        // no user properties (the retained store doesn't
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
                                if !correlate::dlv_park(&mut s.correlate, &buf[..dlv_total]) {
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
                                if !correlate::dlv_park(&mut s.correlate, &buf[..env_len]) {
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
                let (_, _) = { worked += 1; wire::channel_read_msg(sys, s.in_cp, &mut s.in_buf) };
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
            // Publish inflights that never resolved (lost proposal,
            // leadership churn, codec_out saturation at ack time): reclaim
            // the slot. Kafka producers retry on their own request timeout;
            // AMQP confirm-mode publishers wait indefinitely, so they get
            // an explicit Basic.Nack.
            for i in 0..KAFKA_INFLIGHT {
                if !store::inflight_expired(&s.store, i, now, KAFKA_INFLIGHT_TIMEOUT_MS) {
                    continue;
                }
                if let Some(e) = store::inflight_get(&s.store, i) {
                    // Only the AMQP arm below reads the entry.
                    let _ = &e;
                    // Kafka producers retry on their own request timeout;
                    // AMQP confirm-mode publishers wait indefinitely, so
                    // they get an explicit Nack.
                    #[cfg(feature = "amqp")]
                    if e.proto == KIN_PROTO_AMQP {
                        let mut rest = [0u8; 9];
                        rest[0..8].copy_from_slice(&e.delivery_tag.to_le_bytes());
                        rest[8] = 1;
                        emit_amqp_response(
                            sys, s.out_codec, e.conn_id, 1, e.channel, &rest,
                        );
                    }
                    // Free the slot. The NEXT allocation bumps its epoch,
                    // so any durability round-trip still outstanding for
                    // this occupant is invalidated on arrival — no need to
                    // hunt down its correlations or cancel the ack component
                    // registration (that entry ages out via the ack component's
                    // own max_attempts).
                    store::inflight_free(&mut s.store, i);
                    s.kafka_produce_errors = s.kafka_produce_errors.wrapping_add(1);
                }
            }
            // Reclaim correlations whose MSG_PROPOSAL_ASSIGNED never
            // arrived (see PendingCorrelation.ts_ms). Without this a slow
            // leak eventually fills the shared table and stalls acks for
            // MQTT, Kafka, AND AMQP at once.
            correlate::expire(&mut s.correlate, now, CORRELATION_TIMEOUT_MS);
            for i in 0..MAX_SESSIONS {
                if !sessions::is_active(&s.sessions, i) { continue; }
                let kalive = sessions::view(&s.sessions, i).map(|v| v.keep_alive_ms).unwrap_or(0);
                if kalive == 0 { continue; }
                let deadline = (kalive as u64).saturating_mul(3) / 2;
                let age = now.wrapping_sub(sessions::view(&s.sessions, i).map(|v| v.last_activity_ms).unwrap_or(0));
                if age <= deadline { continue; }

                s.disconnects = s.disconnects.wrapping_add(1);
                let tenant = sessions::tenant(&s.sessions, i);
                let stream_hash = sessions::stream_hash(&s.sessions, i);
                let protocol = sessions::protocol(&s.sessions, i);
                let session_slot = i as u32;
                sessions::unbind_conn(&mut s.sessions, i);

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
            // MSG_SESSION_DROP; the offline queue clears via the same fan-out
            // pattern keyed on session_slot. session_expiry_s == 0 is
            // already short-circuited to a fast-drop in
            // apply_qop_disconnect so the persisted path never carries
            // a "0 means immediate" slot. u32::MAX is the "never expire"
            // sentinel (also covers MQTT 3.1.1 clean_session=0 thanks
            // to the propose-side normalisation).
            for i in 0..MAX_SESSIONS {
                if !sessions::is_persisted(&s.sessions, i) { continue; }
                if sessions::is_active(&s.sessions, i) { continue; }
                let expiry_s = sessions::session_expiry_s(&s.sessions, i);
                if expiry_s == 0 || expiry_s == u32::MAX { continue; }
                let deadline_ms = (expiry_s as u64).saturating_mul(1000);
                let elapsed = now.wrapping_sub(sessions::view(&s.sessions, i).map(|v| v.disconnected_at_ms).unwrap_or(0));
                if elapsed < deadline_ms { continue; }

                // Purge.
        sessions::clear_flow(&mut s.sessions, i);
                let drop_body = (i as u32).to_le_bytes();
                try_emit(sys, s.out_topic, wire::MSG_SESSION_DROP, &drop_body);
                sessions::clear(&mut s.sessions, i);
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
                let fire_at = sessions::will_deadline(&s.sessions, i);
                if fire_at == 0 { continue; }
                if now < fire_at { continue; }
                if sessions::will_present(&s.sessions, i) {
                    let t = sessions::tenant(&s.sessions, i);
                    fire_will(s, sys, t, i);
                }
                sessions::set_will_deadline(&mut s.sessions, i, 0);
                sessions::clear_will(&mut s.sessions, i);
                sessions::clear_will(&mut s.sessions, i);
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

        if worked > 0 { STEP_BURST } else { 0 }
    }
}
