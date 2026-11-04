//! Session Processor — Unified session state machine for MQTT/Kafka/AMQP.
//!
//! The Raft apply callback for Quantum. Related signals share a channel
//! and are demuxed by MSG_* type byte, so the module stays inside the
//! 16-port-per-direction Fluxor wire limit.
//!
//! Ingress envelope (codec → session): `[conn_id:u16 LE][proto][pkt_type][flags][fields]`.
//! Egress envelope (session → codec):  `[conn_id:u16 LE][proto][pkt_type][flags][body]`.
//!
//! Per-session state tracks conn_id so responses go back to the correct
//! client connection. Session → conn_id is set on CONNECT and cleared on
//! DISCONNECT.
//!
//! Ports. The index is the ABI — a graph binds by position — so a port
//! is only ever appended, never inserted or reordered.
//!
//!   in[0]  codec_in       — protocol proposals
//!   in[1]  committed_in   — applied entries from consensus
//!   in[2]  flow_in        — PID credits + backpressure + prefetch
//!   in[3]  ack_in         — ACK emit + redeliver from `flow::ack`
//!   in[4]  cp_in          — capabilities + placement + epoch + disconnect
//!   in[5]  messaging_in   — dedup/offline/retained/group/txn results
//!   in[6]  deliver_in     — topic deliveries
//!   in[7]  assigned_in    — MSG_PROPOSAL_ASSIGNED (correlation → wal index)
//!   in[8]  wal_reply      — WAL records for a cold Kafka Fetch
//!   in[9]  ctrl_in        — SessionCtrlV1 commands from the anchor
//!   in[10] front_in       — session envelopes from the in-graph front
//!   out[0] proposals      — to consensus (fluxor wire envelope)
//!   out[1] codec_out      — [conn_id:u16 LE][proto][pkt_type][flags][body] responses to codecs
//!   out[2] topic_out      — publish + subscribe + unsubscribe (wire envelope)
//!   out[3] messaging_out  — dedup/offline/retained/group/txn ops (wire envelope)
//!   out[4] forward_out    — inflight register + lag signal to `flow`
//!   out[5] audit_out      — audit events (wire envelope)
//!   out[6] metrics_out    — metrics (wire envelope)
//!   out[7] proposals_tagged — correlation-prefixed proposals for QoS 1+
//!   out[8] wal_request    — cold-read request for a Kafka Fetch
//!   out[9] retention_floor — lowest raft index this broker still needs
//!   out[10] cp_out        — committed entries the control plane owns
//!   out[11] ctrl_out      — SessionCtrlV1 replies and export frames
//!   out[12] front_out     — responses to the in-graph front's sessions
//!
//! The in-graph front (`broker_front`) speaks the codec envelope on its own
//! pair, `front_in` / `front_out`, with connection ids from
//! `wire::FRONT_CONN_BASE` up. Its envelopes take the codec path, and a
//! response for one of its sessions leaves on `front_out`.
//!
//! No client response is lost to a full output (`reply_hold`): one the bus
//! refuses is held, per output, and written in order as it has room; an
//! input that answers a client and can wait is read only while the hold
//! has room for its answers, and one that cannot wait keeps its operation
//! with the answer owed (an inflight slot, a cold-fetch slot, an owed
//! DISCONNECT) and answers it on a later step.

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

define_params! {
    ModuleState;

    // This node's replica id, stamped into every proposal this node
    // ORIGINATES so the apply side — which runs on every replica — can
    // tell its own requests from a peer's.
    //
    // Apply is where a produce's logical offset and its idempotence
    // verdict become known, but the client waiting for them is parked
    // in an inflight slot that exists only on the accepting node.
    // Clustor strips the tagged correlation before storing the entry,
    // and the replicated slot index alone is not safe to match on: a
    // follower can hold a live slot at the same index and generation.
    // The origin id makes the match structural instead of probabilistic
    // — a replica that did not originate the proposal skips it outright.
    1, self_id, u8, 0
        => |s, d, len| { s.self_id = p_u8(d, len, 0, 0); };

    // Kafka time retention, in milliseconds. 0 (the default) means
    // "retain until the ring fills", which is the behaviour every
    // existing graph has today — a broker must not start deleting data
    // because a config was upgraded under it.
    //
    // Retention is what makes the log's footprint a function of the
    // POLICY rather than of the traffic: without it a low-rate topic
    // keeps records for ever while a high-rate one keeps minutes, and
    // neither is what was asked for.
    2, kafka_retention_ms, u32, 0
        => |s, d, len| { s.kafka_retention_ms = p_u32(d, len, 0, 0); };

    // Key compaction, off by default. Kafka models this as a per-topic
    // `cleanup.policy`; there is no per-topic config surface here yet,
    // so it is a broker-wide switch. Off is the safe default for the
    // same reason retention's is: a config upgrade must not start
    // deleting data.
    //
    // Compaction and time retention are INDEPENDENT here — Kafka treats
    // them as alternative policies, but a compacted topic that also ages
    // out is a coherent thing to want and neither sweep can drop a
    // record the other would have kept.
    3, kafka_compact, u8, 0
        => |s, d, len| { s.kafka_compact = p_u8(d, len, 0, 0); };

    // ── Session-worker identity (docs/architecture/session_continuity.md) ──
    //
    // Two session_processor instances can share one node as an
    // anchor-preserved handoff pair. They share topic_engine, flow and
    // messaging, all keyed by session slot, so each allocates NEW
    // sessions from its own slot range and applies only the committed
    // entries of sessions it holds. A node with one worker leaves all
    // four at their defaults: worker 0, no peers, the whole table.
    4, worker_id, u8, 0
        => |s, d, len| { s.worker_id = p_u8(d, len, 0, 0); };
    // Bitmask of OTHER worker ids on this node (bit n = worker n).
    // Non-zero turns on the apply-side ownership filter.
    5, co_located_workers, u8, 0
        => |s, d, len| { s.co_located_workers = p_u8(d, len, 0, 0); };
    // Slot range this worker allocates from: `[slot_base, slot_base +
    // slot_count)`; `slot_count = 0` runs to the end of the table.
    6, slot_base, u16, 0
        => |s, d, len| { s.slot_base = p_u16(d, len, 0, 0); };
    7, slot_count, u16, 0
        => |s, d, len| { s.slot_count = p_u16(d, len, 0, 0); };
    // Fault injection for the cursor gate: bytes ADDED to the inbound
    // cursor every export carries, so the anchor's admission check
    // sees a blob that accounts for bytes it never forwarded and must
    // refuse the handoff. 0 in every deployment.
    8, export_cursor_skew, u32, 0
        => |s, d, len| { s.worker.cursor_skew = p_u32(d, len, 0, 0); };
    // Refuse to emit an UNKEYED proposal (`MSG_CLIENT_PROPOSAL` with no
    // shard id). On, the default, every proposal this module originates
    // names its virtual shard and an op with no derivable key is counted
    // (`unkeyed=` on the `[sess] hb` line) and dropped instead of taking
    // clustor `partition_router`'s body-hash placement. Off (0) passes
    // such an op through to that placement. The router accepts
    // body-hashed proposals from other producers either way
    // (docs/architecture/partitioning.md, "Unkeyed proposals").
    9, keyed_only, u8, 1
        => |s, d, len| { s.keyed_only = p_u8(d, len, 0, 1); };
    // Log every QoS 1/2 delivery to a subscriber (`QOP_DELIVER`) and its
    // progress (`QOP_DELIVER_ACK`), so what a session is owed is
    // reconstructible from the log after a worker crash or a node
    // restart and resent on resume. Costs two proposals per QoS 1
    // delivery and three per QoS 2; 0 turns it off, and what a session
    // is owed then lives only in the memory of the worker serving it
    // (docs/architecture/session_continuity.md, "Exact replay").
    10, log_deliveries, u8, 1
        => |s, d, len| { s.log_deliveries = p_u8(d, len, 0, 1); };
    // Test-only fault injection for the worker-loss gate: on the first
    // liveness mark (a client's PINGREQ) the worker goes dark — it drains
    // every input and discards it, and emits nothing, as a crashed worker
    // whose channels were torn down would. Draining (rather than
    // stopping) keeps the shared fan-outs it hangs off flowing for the
    // workers that are not lost. 0 in every deployment.
    11, fault_stop_on_ping, u8, 0
        => |s, d, len| { s.fault_stop_on_ping = p_u8(d, len, 0, 0); };
    // Checkpoint period (0 = none): every period, each session this
    // worker serves whose consumed delivery high-water moved is sent, at
    // a coherent point, through the anchor to the standby — what a loss
    // of this worker is adopted from (docs/architecture/mqtt_anchor.md).
    12, checkpoint_ms, u32, 0
        => |s, d, len| { s.worker.checkpoint_ms = p_u32(d, len, 0, 0); };
}

/// Kafka idempotent-producer sequence bookkeeping. Enforced on the
/// APPLY side (see `apply_kafka_produce`), because apply is the
/// replicated state machine: every replica reaches the same verdict
/// from the same committed log, so the decision survives failover. The
/// propose side cannot do this — any node may accept a produce, and its
/// table is neither shared nor durable.
#[cfg(feature = "kafka")]
#[path = "../../common/cores/kafka_idem_core.rs"]
mod kafka_idem;

/// Kafka partition-log semantics — here for the retention decision and
/// the RecordBatch timestamp it reads. Shared with the host test bed so
/// "has this record aged out?" has one definition.
#[cfg(feature = "kafka")]
#[allow(
    dead_code,
    reason = "the core is compiled verbatim into every module that shares it; \
              this module uses the retention half, the host tests the whole \
              surface. Trimming it per consumer would fork the one definition \
              the core exists to provide"
)]
#[path = "../../common/cores/kafka_log_core.rs"]
mod kafka_log;

/// Placement: which PRG owns a shard, whether it is this node's, and
/// whether it is mid-fence. The same core `topic_engine` decides
/// against, so both modules resolve a shard to the same owner.
#[allow(
    dead_code,
    reason = "the core is compiled verbatim into every module that shares it \
              and into the host test bed; this module uses the placement half \
              (EdgeView + apply_placement_update), while topic_engine uses the \
              routing half. Trimming it per consumer would fork the one \
              definition the core exists to provide"
)]
#[path = "../../common/cores/edge_routing_core.rs"]
mod edge;

mod consumers;
mod correlate;
/// Exact replay across a handoff: the QSR2 tail, the import cursor rule
/// and the outbound delivery ledger (`cores/session_replay_core.rs`).
mod replay {
    include!("../../common/cores/session_replay_core.rs");
}
/// Where a delivery to a session goes so that it receives its deliveries
/// in order: offered, parked, or queued.
mod order {
    include!("../../common/cores/delivery_order_core.rs");
}
/// When a session's next client packet, or a delivery to it, may be taken.
mod gate {
    include!("../../common/cores/inflight_gate_core.rs");
}
/// What Clustor's refusal of a tagged proposal means for the operation
/// waiting on it.
mod refusal {
    include!("../../common/cores/proposal_refusal_core.rs");
}
/// Whether the entry a durable publish was assigned is the one committed
/// at its index: the registration and completion bodies `flow` reads, and
/// the applied term runs this module reports to it.
mod term {
    include!("../../common/cores/ack_term_core.rs");
}
const _: () = assert!(term::REGISTER_LEN == wire::ACK_REGISTER_LEN);
const _: () = assert!(term::APPLIED_LEN == wire::ACK_APPLIED_LEN);
const _: () = assert!(term::EMIT_LEN == wire::ACK_EMIT_LEN);
// A completion's failure status is a refusal status: one mapping fails both.
const _: () = assert!(term::STATUS_SUPERSEDED == refusal::REFUSED_NOT_LEADER);
const _: () = assert!(term::STATUS_UNKNOWN == refusal::REFUSED_OUTCOME_UNKNOWN);
/// How often the open applied term runs are reported to `flow`. A run that
/// closes (the committed log changed term) is reported at once; the open
/// run only settles registrations the commit horizon's term cannot — those
/// that span a leader change — so a few milliseconds' delay costs nothing
/// in the steady state and bounds the notices to one per partition per
/// interval.
const APPLIED_NOTICE_MS: u64 = 10;
/// Credit for the deliveries `messaging`'s retained reads and offline
/// drains bring, and which sessions' fresh deliveries join their offline
/// queue while it drains.
#[path = "../../common/cores/messaging_flow_core.rs"]
mod mflow;
/// The ordered queue toward `messaging`: nothing asked of it is dropped
/// on a full op edge.
#[path = "../../common/cores/messaging_outbox_core.rs"]
mod outbox;
mod reply_hold;
mod sessions;
mod site;
mod store;
mod worker;
/// The site record (`QSS1`) and the cursors it carries, mounted beside the
/// replay and identity cores it names by bare name.
mod site_core {
    include!("../../common/cores/session_replay_core.rs");
    include!("../../common/cores/session_identity_core.rs");
    include!("../../common/cores/session_site_core.rs");
}

/// Kernel step ABI: 0=Continue, 1=Done, 2=Burst, 3=Ready. Returning
/// Burst re-runs the domain's exec rotation within the same tick (up to
/// the kernel's pass cap), so a record consumed here reaches its
/// consumer in this tick instead of the next. We burst only when a
/// record was actually consumed: an idle graph reports no burst and the
/// tick costs exactly one pass.
const STEP_BURST: i32 = 2;

#[path = "../../common/types.rs"]
mod types;

use types::*;

const MAX_SESSIONS: usize = 16384;
// The site book tracks every slot's subscriptions individually.
const _: () = assert!(MAX_SESSIONS <= site_core::SITE_BOOK_SLOTS);

/// Apply cursors, one per hosted raft partition. Must be at least
/// clustor's `consensus` `K_MAX` (64); a partition id at or above this
/// shares cursor 0 rather than indexing out of bounds, which costs that
/// partition the per-partition sequencing but never memory safety.
const MAX_APPLY_PARTITIONS: usize = 64;
const MAX_INFLIGHT_PER_SESSION: usize = 16;
/// The tenant every proposal this module originates is stamped with.
///
/// The codec envelope on `codec_in` carries no listener or authenticated
/// identity, so the tenant a CONNECT belongs to cannot be resolved here;
/// `docs/architecture/multi_tenancy.md` names the gap and the
/// control-plane record (`MSG_TENANT_RECORD`) that closes it. Every
/// session, and therefore every placement key, is tenant 0 until then.
/// Named so the sites are greppable rather than literal zeros that read
/// as "no tenant" and "tenant zero" alike.
const TENANT_UNRESOLVED: TenantId = 0;

/// The placement of an MQTT session: its client id's shard, or, for a
/// client that presented none, its stream hash's. A pure function of the
/// CONNECT, run where the CONNECT is proposed and where it is applied.
fn session_shard(tenant: TenantId, client_id: &[u8], stream_hash: StreamHash) -> u32 {
    if client_id.is_empty() {
        wire::shard_mqtt_stream(tenant, stream_hash)
    } else {
        wire::shard_mqtt_session(tenant, client_id)
    }
}

/// Imports at once whose replay floor is still filtering entries. A full
/// table refuses the import `STATUS_NOT_READY` (the anchor keeps the
/// session where it is) rather than admit one that would apply twice.
const REPLAY_FLOORS: usize = 64;

#[derive(Clone, Copy)]
struct ReplayFloor {
    slot: u32,
    live: u8,
    tail: replay::HandoffTail,
}
/// Proposal credit the codec drain requires before it takes a packet
/// off `codec_in`: the substrate's entry cap, so no admitted packet can
/// find the credit gone when it reaches admission.
const ADMISSION_GATE_BYTES: i32 = 2048;
/// Read/write buffer cap for codec ↔ session_processor and
/// session_processor ↔ topic_engine / messaging-fan-out traffic. Sized to the wire-channel per-message
/// capacity (`fluxor-abi::CHANNEL_BUFFER_SIZE` = 8192) so a worst-case
/// MQTT packet (`protocol::mqtt`'s `MAX_PACKET` = 4096) plus routing headers
/// (`MSG_TOPIC_PUBLISH` topic prefix, `MSG_TOPIC_DELIVER` session-slot
/// prefix, etc.) fits without hitting the `channel_read_msg` discard
/// path.
const BUF_SIZE: usize = 8192;
/// Fresh deliveries taken off `deliver_in` per step. `topic_engine`
/// holds a fan-out while the edge is full, so this paces throughput,
/// never loses a delivery.
const DELIVER_BUDGET: usize = 32;
/// Codec inputs: the transport's `codec_in`, the second anchor seam's
/// `codec2_in` and the front's `front_in`.
const CODEC_INPUTS: usize = 3;
/// Codec envelope header, both directions:
/// `[conn_id:u16 LE][proto:u8][pkt_type:u8][flags:u8]`; the body follows.
const CODEC_ENV_HDR: usize = 5;
/// Kafka codec envelope header, both directions:
/// `[conn_id:u16 LE][proto=1][api_key:i16 LE][api_ver:i16 LE][corr:i32 LE]`.
const KAFKA_ENV_HDR: usize = 11;
/// AMQP codec envelope header, both directions:
/// `[conn_id:u16 LE][proto=2][op:u8][channel:u16 LE]`.
const AMQP_ENV_HDR: usize = 6;
/// AMQP ingress fixed prefix: the envelope header plus the
/// `delivery_tag:u64 LE` every op carries (0 where it has no meaning).
const AMQP_ENV_PREFIX: usize = AMQP_ENV_HDR + 8;

/// Read the connection id off a codec envelope.
#[inline]
fn env_conn_id(buf: &[u8]) -> u16 {
    u16::from_le_bytes([buf[0], buf[1]])
}

/// Read `(conn_id, api_ver, corr)` off a Kafka codec envelope. `buf`
/// must hold at least `KAFKA_ENV_HDR` bytes.
#[cfg(feature = "kafka")]
#[inline]
fn kafka_env_header(buf: &[u8]) -> (u16, i16, i32) {
    (
        env_conn_id(buf),
        i16::from_le_bytes([buf[5], buf[6]]),
        i32::from_le_bytes([buf[7], buf[8], buf[9], buf[10]]),
    )
}

/// Read `(conn_id, channel)` off an AMQP codec envelope. `buf` must
/// hold at least `AMQP_ENV_HDR` bytes.
#[cfg(feature = "amqp")]
#[inline]
fn amqp_env_header(buf: &[u8]) -> (u16, u16) {
    (env_conn_id(buf), u16::from_le_bytes([buf[4], buf[5]]))
}

/// Read the `delivery_tag:u64 LE` that follows the AMQP envelope
/// header. `buf` must hold at least `AMQP_ENV_PREFIX` bytes.
#[cfg(feature = "amqp")]
#[inline]
fn amqp_env_delivery_tag(buf: &[u8]) -> u64 {
    let mut b = [0u8; 8];
    b.copy_from_slice(&buf[AMQP_ENV_HDR..AMQP_ENV_PREFIX]);
    u64::from_le_bytes(b)
}

/// Write the Kafka codec envelope header into `out[..KAFKA_ENV_HDR]`.
#[cfg(feature = "kafka")]
#[inline]
fn write_kafka_env_header(out: &mut [u8], conn_id: u16, api_key: i16, api_ver: i16, corr: i32) {
    out[0..2].copy_from_slice(&conn_id.to_le_bytes());
    out[2] = 1; // PROTO_KAFKA envelope discriminator
    out[3..5].copy_from_slice(&api_key.to_le_bytes());
    out[5..7].copy_from_slice(&api_ver.to_le_bytes());
    out[7..11].copy_from_slice(&corr.to_le_bytes());
}
const MAX_PENDING_CORRELATIONS: usize = 1024;

/// Stash slots for QoS 1+ topic-publish envelopes that are commit-gated.
/// Each slot retains the dedup_key plus the topic-publish envelope (up to
/// `MAX_STASH_ENV` bytes) and tracks dedup + durability resolution; the
/// fan-out to topic_engine only happens once both have landed and the
/// publish was not flagged as duplicate. Bounds the worst-case memory
/// cost of deferred topic emission to a fixed allocation in module state.
const STASH_SLOTS: usize = 32;
/// A QoS 1+ publish whose op body is past it is refused before it is
/// proposed (`wire::publish_fits`, Packet too large), so every one proposed
/// fits its stash slot.
const MAX_STASH_ENV: usize = wire::QOS1_OP_MAX;
// A publish `wire::publish_fits` admits is built whole in `out_buf`.
const _: () = assert!(wire::RAFT_ENTRY_MAX <= BUF_SIZE);
const STASH_DEDUP_PENDING: u8 = 0;
const STASH_DEDUP_OK: u8 = 1;
const STASH_DEDUP_DUPLICATE: u8 = 2;
/// The dedup component had no room to record this key, so no
/// duplicate-suppression exists for it. The publish is dropped without
/// a client-facing ack, leaving the retry with the client, and is
/// counted as throttled.
const STASH_DEDUP_REFUSED: u8 = 3;

/// Defer queue for subscriber deliveries that hit backpressure
/// (prefetch_credit cap, PID exhaustion, or inflight-slot exhaustion).
/// Drained ahead of fresh deliveries on each tick so slow consumers don't
/// silently lose messages while credit refreshes catch up. Sized to admit
/// a MAX_PACKET-class MQTT publish envelope (4096-byte body + small
/// session-envelope overhead).
const PENDING_DLV_SLOTS: usize = 8;
const MAX_PENDING_DLV: usize = wire::DELIVERY_ENVELOPE_MAX;

/// Defer queue for `MSG_ACK_REGISTER` emissions that hit `out_forward`
/// backpressure during `MSG_PROPOSAL_ASSIGNED` processing. If the register
/// is lost, the ack component never learns about the inflight entry and the
/// publisher's PUBACK never fires. Each entry is the fixed-size 18-byte
/// register payload (see PROPOSAL_ASSIGNED handler).
///
/// One slot per correlation: a tagged proposal produces exactly one
/// registration, and `correlate::allocate` reserves a slot for it
/// before the proposal is accepted. That makes the queue large enough
/// for every registration the broker can owe, so a saturated edge
/// parks work instead of discarding it.
const PENDING_ACK_SLOTS: usize = MAX_PENDING_CORRELATIONS;

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
/// PINGRESP: answered here only for a seated session (anchor continuity),
/// where every byte the client reads is a production.
const PKT_PINGRESP: u8 = 13;
/// Server-initiated DISCONNECT. v5 only — the codec drops it for 3.1.1,
/// which has no such packet.
const PKT_DISCONNECT: u8 = 14;

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
///     clean_start was false, so subscriptions, inflight, and prefetch state
///     must survive until either reconnect or explicit purge.
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
    if buf.is_empty() {
        return None;
    }
    let count = buf[0] as usize;
    let mut off = 1usize;
    for _ in 0..count {
        if off + 2 > buf.len() {
            return None;
        }
        let klen = u16::from_be_bytes([buf[off], buf[off + 1]]) as usize;
        off += 2 + klen;
        if off + 2 > buf.len() {
            return None;
        }
        let vlen = u16::from_be_bytes([buf[off], buf[off + 1]]) as usize;
        off += 2 + vlen;
        if off > buf.len() {
            return None;
        }
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
//
// The epoch's top bits name the WORKER that allocated the slot
// (`store::KIN_EPOCH_WORKER_SHIFT`): flow's completions reach every worker
// on the node, and each worker numbers its own slots, so without it worker
// 0 resolved worker 1's produce at the same index and reuse count.

/// Reserved session_slot namespace for durable-publish inflights. Bit 30
/// set, far above MAX_SESSIONS (16384) so the two spaces can never collide.
const KAFKA_SLOT_BASE: u32 = 0x4000_0000;
const KAFKA_INFLIGHT: usize = 256;
/// Slot index occupies the low 8 bits (KAFKA_INFLIGHT ≤ 256).
const KIN_KI_MASK: u32 = 0xFF;
/// Epoch occupies bits 8..30 (22 bits) — below KAFKA_SLOT_BASE's bit 30 so
/// the `>= KAFKA_SLOT_BASE` routing check is unaffected. Its top 3 bits are
/// the allocating worker's id and its low 19 the reuse count, which wraps
/// every 2^19 allocations per slot; a collision needs the SAME slot reused
/// exactly 2^19 times within one stalled proposal's lifetime — impossible.
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
/// Kafka `OFFSET_OUT_OF_RANGE`. The client's response is to apply its
/// `auto.offset.reset` policy — which it can only do if we TELL it.
const KERR_OFFSET_OUT_OF_RANGE: i16 = 1;

/// Largest record batch accepted per Produce request. The clustor
/// substrate pins log entries at ~2 KiB end to end (raft
/// PROPOSAL_BATCH_CAP, wal MEMORY_ENTRY_BODY_CAP, and the replay
/// validator all use 2048); a proposal that can't fit is dropped after
/// the ack path is armed, which reads as silent loss. Enforce the
/// ceiling here with MESSAGE_TOO_LARGE so producers fail fast and
/// size their batches down (the protocol's designed-for response).
/// 1900 = 2048 − tagged header (18) − op-body framing
/// (`wire::PRODUCE_BODY_HDR`, 6) − max topic (64) − slack.
const KAFKA_MAX_RECORDS_BYTES: usize = 1900;

/// Which protocol a durability-gated publish inflight belongs to.
/// Dispatches the MSG_ACK_EMIT handling: Kafka → ProduceResponse,
/// AMQP → Basic.Ack (publisher confirm).
const KIN_PROTO_KAFKA: u8 = 1;
const KIN_PROTO_AMQP: u8 = 2;
/// A Kafka OffsetCommit whose response is parked until its proposals
/// are quorum-durable. Distinguished from `KIN_PROTO_KAFKA` because the
/// completion emits a stored body rather than rebuilding a produce
/// response from per-partition offsets.
const KIN_PROTO_KAFKA_OFFSET: u8 = 3;
/// OffsetCommit op tag for `correlate`.
const OP_KOFFSET: u8 = 3;

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
/// Ring entry header: `len(2) flags(1) offset(8) nrec(4) appended_ms(8)`.
///
/// `appended_ms` is the broker's MONOTONIC clock at append, and it is
/// what time retention measures age from — the producer's timestamp is
/// on a different clock entirely (see `kafka_log_core::retention_expired`).
///
/// Extending this header is safe precisely because the ring is never
/// persisted: it is rebuilt by applying the raft log, so there is no
/// stored layout to stay compatible with. The consequence, stated
/// plainly, is that a restart resets every record's age — retention
/// after a restart runs a fresh window rather than resuming the old one.
const KENTRY_HDR: usize = 2 + 1 + 8 + 4 + 8;
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
    in_codec: i32,
    /// The second anchor seam's session envelopes (in[11]) and their
    /// responses (out[14]); -1 when the graph wires one anchor.
    in_codec2: i32,
    out_codec2: i32,
    /// The in-graph front's session envelopes (in[10]) and their
    /// responses (out[12]); -1 when the graph wires no front.
    in_front: i32,
    out_front: i32,
    /// Which input the codec drain offers first, rotated per packet so no
    /// busy input starves the others.
    codec_turn: u8,
    /// Envelopes dropped for naming a connection outside their input's
    /// namespace.
    codec_namespace_drops: u32,
    /// The answer being written is a coordinator answer
    /// (`wire::MSG_COORD_RESPONSE`, counted in no cursor): set for a
    /// `MSG_COORD_CALL` while it is handled, and for a later answer to one
    /// (an OffsetCommit's slot, a coordinated consumer's delivery) while
    /// that is written. Read by [`respond`].
    coord_reply: bool,
    /// Coordinator calls taken (`MSG_COORD_CALL`).
    coord_calls: u32,
    in_committed: i32,
    in_flow: i32,
    in_ack: i32,
    in_cp: i32,
    /// Placement view learned from the control plane over `cp_in`.
    ///
    /// Read-only here: it answers which shards this node owns
    /// (`shard_owned_locally`) for the stores and the ownership gates.
    /// Moving per-shard state when ownership changes is the control
    /// plane's migration path, not this view's job.
    view: edge::EdgeMap,
    /// Placement updates applied.
    placement_updates: u32,
    /// Shard-map (ownership override) updates applied and refused.
    shard_map_updates: u32,
    shard_map_stale: u32,
    /// Sessions dropped because their shard was reassigned away.
    /// Non-zero means this node handed shards over; it should track the
    /// migrations that moved them.
    sessions_released_foreign: u32,
    in_messaging: i32,
    in_deliver: i32,
    in_assigned: i32,
    out_proposals: i32,
    /// This node's replica id (param 1). Stamped into proposals this
    /// node originates; see the `define_params!` block.
    self_id: u8,
    /// Kafka time-retention window; 0 disables it.
    kafka_retention_ms: u32,
    /// Non-zero enables key compaction.
    kafka_compact: u8,
    /// This instance's worker id and the ids sharing the node (params
    /// 4–7). See `entry_is_mine`.
    worker_id: u8,
    co_located_workers: u8,
    slot_base: u16,
    slot_count: u16,
    /// Param 9: refuse unkeyed proposals (see `try_emit_unkeyed`).
    keyed_only: u8,
    /// Param 10: log outbound QoS 1/2 deliveries (`log_delivery`).
    log_deliveries: u8,
    /// Param 11 (test only) and whether it has fired: the worker is dark.
    fault_stop_on_ping: u8,
    faulted: u8,
    /// Deliveries the log says each session is owed, applied on every
    /// worker whatever slot the entry names (`replay::DeliveryLedger`).
    ledger: replay::DeliveryLedger,
    /// Per slot, the most recent committed entry this worker skipped as
    /// another worker's: `(partition, index)`, index 0 = none. The import
    /// cursor rule reads it (`replay::cursor_verdict`).
    skip_part: [u16; MAX_SESSIONS],
    skip_index: [u64; MAX_SESSIONS],
    /// Replay floors of sessions imported while this worker's apply was
    /// behind the record: their entries at or below the record's cursor
    /// are already in it and are skipped until the cursor passes.
    floors: [ReplayFloor; REPLAY_FLOORS],
    /// Committed entries a floor filtered, imports refused for a cursor
    /// the importer had passed, deliveries that could not be logged, and
    /// deliveries redelivered from the ledger.
    replay_floor_skipped: u32,
    replay_cursor_refused: u32,
    deliver_logged: u32,
    deliver_unlogged: u32,
    redelivered: u32,
    /// Proposals refused by the keyed-only gate: ops with no derivable
    /// routing key that would otherwise have taken the router's
    /// body-hash placement. Steady state 0 on a graph whose clients
    /// speak only the ops this module keys.
    proposals_unkeyed_refused: u32,
    /// QoS 2 PUBLISH retransmissions answered from the in-flight ledger
    /// instead of starting a second flow.
    qos2_dup_answered: u32,
    /// Committed publishes the dedupe table filed as duplicates: acked,
    /// not fanned out.
    dedup_duplicates: u32,
    /// Records dropped by time retention.
    #[cfg(feature = "kafka")]
    kafka_retention_evictions: u32,
    out_codec: i32,
    /// `site_out` (out[13]): `QSS1` records for the cross-site carrier.
    out_site: i32,
    /// The worker's book of its sessions' subscriptions (site.rs).
    sub_book: site_core::SiteSubBook,
    site_rec: site_core::SiteSession,
    site_buf: [u8; site_core::SITE_RECORD_MAX],
    /// A session's book section, assembled for its handoff record.
    book_buf: [u8; site_core::SITE_BOOK_SECTION_MAX],
    site_exported: u32,
    site_refused: u32,
    out_topic: i32,
    out_messaging: i32,
    out_forward: i32,
    out_audit: i32,
    out_metrics: i32,
    /// Cold-read request/reply to `durability`, for a Fetch whose offset
    /// has aged out of the in-memory ring.
    #[cfg(feature = "kafka")]
    out_wal_request: i32,
    /// out[9]: `MSG_COMPACTION_FLOOR` to `durability.retention_floor`.
    out_retention_floor: i32,
    /// out[10]: committed entries that are not Quantum operations,
    /// forwarded verbatim for `control_plane` (see the manifest).
    out_cp: i32,
    /// Records forwarded on `out_cp`, and the writes it refused.
    cp_forwarded: u32,
    cp_refused: u32,
    /// Last floor published per raft partition, so an unchanged floor
    /// is not re-sent every interval; `floor_sent_mask` says which
    /// entries have been published at all, because a floor of 0 is a
    /// real value ("keep everything"), not an unset one.
    last_floor_sent: [u64; MAX_APPLY_PARTITIONS],
    floor_sent_mask: u64,
    floor_emitted: u32,
    /// Publishes whose proposal channel refused the write.
    proposals_refused: u32,
    /// Raft partition of the entry being applied right now.
    applying_partition: u16,
    /// One sampled QoS 1 publish traced from proposal to PUBACK:
    /// `(session_slot, packet_id)` armed at assignment, its issue time,
    /// and the ms the assignment took. Logged as `[sess] qos1 age` at
    /// the PUBACK so the follower-relay path can be read off the log.
    trace_slot: u32,
    trace_packet: u16,
    trace_armed: bool,
    trace_issued_ms: u64,
    trace_assign_ms: u32,
    #[cfg(feature = "kafka")]
    in_wal_reply: i32,
    #[cfg(feature = "kafka")]
    cold: [ColdFetch; COLD_FETCH_SLOTS],
    #[cfg(feature = "kafka")]
    next_cold_id: u32,
    #[cfg(feature = "kafka")]
    cold_served: u32,
    #[cfg(feature = "kafka")]
    cold_refused: u32,
    out_proposals_tagged: i32,

    credit_base_window: u32,
    keep_alive_factor_pct: u32,
    max_inflight_default: u16,
    pid_entry_credits: i32,
    /// Steps the codec drain sat out for want of proposal credit.
    codec_gated: u32,
    /// A publish proposal the router edge refused, kept whole with its
    /// channel and shard and offered again ahead of the next codec
    /// drain (length 0 = none). The packet is already off `codec_in`
    /// and its correlation and inflight slot are allocated; dropping it
    /// here is a QoS 1 publish the client never hears about.
    prop_hold_len: u16,
    prop_hold_chan: i32,
    prop_hold_shard: u32,
    prop_hold: [u8; BUF_SIZE],
    /// Proposals held on a refused edge (retried, never dropped).
    proposals_held: u32,
    /// Per codec input (`codec_in`, `codec2_in`, `front_in`), an
    /// envelope read that has to wait (`codec_waits`), kept whole as it
    /// came off the channel — nothing about it counted, admitted or
    /// allocated yet (length 0 = none). That input is not read again
    /// until it goes, as if it had stayed in the channel; the others go
    /// on. So a client's pipelined packets wait for the commits and the
    /// horizon ahead of them instead of being dropped, and back pressure
    /// reaches its TCP window.
    codec_hold_len: [u16; CODEC_INPUTS],
    codec_hold_mt: [u8; CODEC_INPUTS],
    codec_hold: [[u8; BUF_SIZE]; CODEC_INPUTS],
    /// Envelopes held on a codec input (each counted once).
    codec_held: u32,
    /// Everything asked of `messaging`, in order (`to_messaging`).
    mbox: outbox::Outbox,
    /// Read and drain credit, and the sessions whose fresh deliveries
    /// join their offline queue (`mflow`).
    mflow: mflow::Flow,
    /// Set while a delivery that goes ahead of its session's offline queue
    /// is being delivered — `messaging`'s answer, or one parked before
    /// the queue took the session's deliveries — so it does not join the
    /// queue behind itself.
    ahead_of_backlog: bool,
    /// Deliveries that found neither the session nor a park slot free and
    /// joined the session's offline queue instead.
    deliveries_spilled: u32,
    /// Retained reads that could not be filed: a filter longer than a
    /// read carries (`mflow::FLOW_PATTERN_MAX`).
    retained_reads_refused: u32,
    /// Committed publishes fanned out although the dedup table could
    /// not file them: the entry is committed and acknowledged, so a
    /// verdict the table cannot give is not a reason to lose it.
    dedup_refused_delivered: u32,
    /// Per-second accounting for the `[sess] hb` line: packets taken
    /// off `codec_in`, publishes admitted, proposals written,
    /// assignments received, ack registrations written, committed
    /// entries applied. Reset when the line goes out.
    hb_codec: u32,
    hb_pub: u32,
    hb_prop: u32,
    hb_assigned: u32,
    hb_reg: u32,
    hb_commit: u32,
    last_hb_ms: u64,
    /// CONNECT applies that found no slot for their stream and
    /// allocated one: on the proposing node this means the transient
    /// slot the CONNACK stood on was not the one the apply reconciled.
    apply_connect_no_prior: u32,
    pid_byte_credits: i32,
    follower_factor_q16: i32,

    sessions: sessions::Sessions,
    /// SessionCtrlV1 session-worker role (`worker.rs`).
    worker: worker::Worker,
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
    /// The Kafka request being handled has its answer: written or held,
    /// owed in an inflight or cold-fetch slot, or none due (acks = 0).
    /// Cleared before each request; one left unset is answered with the
    /// close marker (`wire::KAFKA_API_CLOSE`), since the codec writes a
    /// connection's answers in request order and an unanswered request
    /// would hold every later one.
    #[cfg(feature = "kafka")]
    kafka_settled: bool,
    /// Kafka requests answered with the close marker.
    #[cfg(feature = "kafka")]
    kafka_unanswered: u32,
    /// Per-`(producer_id, partition)` sequence state, maintained from
    /// the committed log so a producer's retry after a lost response is
    /// recognised instead of appended twice.
    #[cfg(feature = "kafka")]
    idem: kafka_idem::IdemTable,
    /// Batches not appended because they repeated an already-applied
    /// sequence — i.e. duplicates this feature suppressed.
    #[cfg(feature = "kafka")]
    kafka_idem_duplicates: u32,
    /// Batches not appended because their sequence was out of order or
    /// their producer epoch was fenced. Non-zero means a producer is
    /// losing batches; it is a signal, not routine.
    #[cfg(feature = "kafka")]
    kafka_idem_rejected: u32,
    /// Group-generation records that could not be applied because the
    /// group table is full. Non-zero means a coordinator may reissue a
    /// generation after failover — the exact hazard the record exists
    /// to close — so it is counted rather than ignored.
    #[cfg(feature = "kafka")]
    kafka_group_gen_dropped: u32,
    /// Group-generation records proposed.
    #[cfg(feature = "kafka")]
    kafka_group_gen_proposed: u32,
    /// Member records proposed.
    #[cfg(feature = "kafka")]
    kafka_group_members_proposed: u32,
    /// Highest generation already proposed per group slot, so the
    /// reconciliation sweep proposes a change once rather than every
    /// step.
    #[cfg(feature = "kafka")]
    gen_proposed: [i32; KGROUPS],

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
    /// Fetches refused as OFFSET_OUT_OF_RANGE. Non-zero means consumers
    /// are falling behind the store's retention — the signal that used
    /// to be invisible because the answer looked like "caught up".
    kafka_fetch_out_of_range: u32,
    /// Fetches answered with a placement error because the partition's
    /// shard is not served here. Rising means clients are reaching the
    /// wrong broker — normal briefly after a migration, persistent
    /// means Metadata is telling them the wrong thing.
    #[cfg(feature = "kafka")]
    kafka_fetch_not_owned: u32,
    /// OffsetCommits answered WITHOUT durability gating — no inflight
    /// slot or correlation was free, or the response was too large to
    /// park. Non-zero means some commits were acknowledged before they
    /// were durable.
    kafka_offset_ungated: u32,
    amqp_delivers: u32,
    amqp_consumer_acks: u32,

    session_count: u32,
    proposals_emitted: u32,
    connects: u32,
    disconnects: u32,
    publishes: u32,
    applied: u32,
    acks_emitted: u32,
    /// Subscriber-side inflight slots released on session resume
    /// because they can never be redelivered. Non-zero means a resumed
    /// subscriber lost messages it had not acknowledged; the count is
    /// what makes that loss visible rather than silent.
    sub_inflight_dropped_on_resume: u32,
    /// Stale `conn_id` bindings evicted when a new CONNECT claimed the
    /// same recycled connection slot. Non-zero means clients are
    /// closing abruptly — normal — and that this guard is doing work
    /// that would otherwise have been cross-client corruption.
    conn_bindings_evicted: u32,
    /// CONNECTs refused because the session's shard is served elsewhere.
    connects_redirected: u32,
    /// MQTT completions dropped because the session generation they
    /// were registered under no longer occupies the slot.
    acks_stale_epoch: u32,
    /// Correlations still awaiting an assignment past
    /// `CORRELATION_TIMEOUT_MS`. Non-zero means the substrate owes an
    /// assignment it has not delivered.
    correlations_overdue: u32,
    /// Tagged proposals Clustor refused (MSG_PROPOSAL_REFUSED) whose
    /// correlation was still waiting: freed at once and the operation
    /// failed in its protocol's terms. A refusal for a correlation no
    /// longer held is not counted.
    proposals_refused_raft: u32,
    /// Steps whose committed-entry drain stopped because an apply
    /// output edge was full. Rising values mean commits are backing up
    /// behind a saturated messaging or topic bus.
    apply_output_stalls: u32,
    qos2_rec: u32,
    qos2_rel: u32,
    qos2_comp: u32,
    publishes_throttled: u32,
    /// PUBLISHes refused for their size (`wire::publish_fits`): their Raft
    /// entry past what Clustor commits, so the publisher's connection was
    /// ended with Packet too large rather than left without an answer.
    publishes_too_large: u32,
    deliveries_throttled: u32,
    redeliver_signals: u32,
    /// Count of per-entry MSG_COMMITTED_ENTRY notices observed on
    /// `in_entries`. Surfaces the substrate wiring: a zero here while
    /// commits are happening means the stream is not reaching apply.
    committed_entries_observed: u32,
    /// Highest WAL index applied via `apply_committed_op`. Used to
    /// detect forward-jumps in the `MSG_COMMITTED_ENTRY` index
    /// sequence (snapshot install / leader log truncation) — see
    /// `docs/architecture/apply_path.md §Apply-pipeline reset. Reset to the
    /// new index after `apply_reset` clears state.
    /// Last COMPLETED apply index, PER PARTITION.
    ///
    /// One engine hosts K raft groups whose logs each number from 1, and
    /// all of them feed this one `committed_in` stream. A single cursor
    /// therefore saw partition 1's index 1 arrive after partition 0's
    /// and skipped it as a duplicate — or, when a partition ran ahead,
    /// read the jump as a gap and wiped all apply-derived state. At K=4
    /// only ~1/4 of topics ever delivered, the same ones every run.
    /// The ack path keys on `(partition_id, wal_index)` for exactly
    /// this reason; the apply cursor must be keyed the same way.
    apply_index: [u64; MAX_APPLY_PARTITIONS],
    /// Per partition, the committed log's term run the applied entries are
    /// extending, and how much of it `flow` has been told
    /// (MSG_ACK_APPLIED): what settles a registration whose term the
    /// commit horizon cannot.
    applied_runs: [term::RunTracker; MAX_APPLY_PARTITIONS],
    /// Closed runs not yet reported, held while the forward edge is full.
    applied_spill: term::Spill,
    applied_notice_ms: u64,
    /// Closed runs dropped because the spill was full: `flow` resolves the
    /// indices they covered as unknown, failing (not acking) what waits on
    /// them. Must stay 0.
    applied_notice_drops: u32,
    /// Completions `flow` answered as not committed — another term's entry
    /// took the index, or its term is unknown here — whose operation was
    /// failed for the client to retry.
    acks_superseded: u32,
    /// Raft index of the entry currently being applied. See the
    /// assignment site: `apply_index` is the last COMPLETED entry.
    applying_index: u64,
    /// Count of apply-side resets observed since boot. Surfaced in
    /// metrics so operators can correlate reset events with
    /// snapshot installs / leader transfers.
    apply_resets: u32,
    last_metrics_ms: u64,

    /// Client responses the codec bus refused, per output, written in
    /// order as it takes them (`reply_hold`): a response is never lost
    /// to a full edge.
    hold: reply_hold::Hold,
    /// Connections owed a DISCONNECT the bus has not taken yet: a
    /// placement update's "server moved" for every session it released,
    /// and a failed flow's that no ring had room for.
    disc_owed: reply_hold::Owed,
    /// Where the next pass over `disc_owed` starts, so every owed
    /// connection has its turn.
    disc_owed_from: u16,
    /// Kafka / AMQP answers the bus had no room for when they were
    /// decided, owed in their inflight slot and answered on a later step.
    answers_owed: u32,

    in_buf: [u8; BUF_SIZE],
    out_buf: [u8; BUF_SIZE],
}

impl ModuleState {}

#[no_mangle]
#[link_section = ".text.module_state_size"]
pub extern "C" fn module_state_size() -> u32 {
    core::mem::size_of::<ModuleState>() as u32
}

/// PIC module ABI entry: one-time process-wide init, before any instance
/// exists.
///
/// # Safety
/// `syscalls` is a kernel-owned table whose function pointers reach live
/// kernel routines for the lifetime of the process.
#[no_mangle]
#[link_section = ".text.module_init"]
pub unsafe extern "C" fn module_init(_syscalls: *const c_void) {}

/// PIC module ABI entry: construct module state in `state` (kernel-allocated
/// from the manifest-declared `state_size`).
///
/// # Safety
/// `state` / `params` / `syscalls` are kernel-owned buffers passed across the
/// module ABI. The kernel guarantees `state` is at least `state_size` bytes,
/// `params` is at least `params_len` bytes, and `state` is zero-initialised.
#[no_mangle]
#[link_section = ".text.module_new"]
pub unsafe extern "C" fn module_new(
    in_chan: i32,
    out_chan: i32,
    _ctrl_chan: i32,
    params: *const u8,
    params_len: usize,
    state: *mut u8,
    state_size: usize,
    syscalls: *const c_void,
) -> i32 {
    unsafe {
        if syscalls.is_null() || state.is_null() {
            return -1;
        }
        if state_size < core::mem::size_of::<ModuleState>() {
            return -2;
        }
        let s = &mut *(state as *mut ModuleState);
        let sys = &*(syscalls as *const SyscallTable);
        s.syscalls = sys;
        s.in_codec = in_chan;
        s.out_proposals = out_chan;
        set_defaults(s);
        if !params.is_null() && params_len >= 4 {
            parse_tlv(s, params, params_len);
        }
        s.in_committed = dev_channel_port(sys, 0, 1);
        s.in_flow = dev_channel_port(sys, 0, 2);
        s.in_ack = dev_channel_port(sys, 0, 3);
        s.in_cp = dev_channel_port(sys, 0, 4);
        // Single PRG, epoch 0, until the control plane speaks: a node
        // that has heard no placement owns everything it is asked
        // about, which is exactly the single-node behaviour.
        s.view = edge::EdgeMap::new(0, 0);
        s.placement_updates = 0;
        s.sessions_released_foreign = 0;
        s.in_messaging = dev_channel_port(sys, 0, 5);
        s.in_deliver = dev_channel_port(sys, 0, 6);
        s.in_assigned = dev_channel_port(sys, 0, 7);
        s.out_codec = dev_channel_port(sys, 1, 1);
        // Declared after ctrl_in / ctrl_out: in[10] / out[12].
        s.in_front = dev_channel_port(sys, 0, 10);
        s.out_front = dev_channel_port(sys, 1, 12);
        s.out_site = dev_channel_port(sys, 1, 13);
        // The second anchor seam, declared last: in[11] / in[12] and
        // out[14] / out[15].
        s.in_codec2 = dev_channel_port(sys, 0, 11);
        s.out_codec2 = dev_channel_port(sys, 1, 14);
        s.sub_book.lost = [0; site_core::SITE_BOOK_SLOTS / 64];
        s.sub_book.lost_beyond = false;
        s.site_exported = 0;
        s.site_refused = 0;
        s.codec_turn = 0;
        s.codec_namespace_drops = 0;
        s.coord_reply = false;
        s.coord_calls = 0;
        s.proposals_unkeyed_refused = 0;
        s.replay_floor_skipped = 0;
        s.faulted = 0;
        s.replay_cursor_refused = 0;
        s.deliver_logged = 0;
        s.deliver_unlogged = 0;
        s.redelivered = 0;
        s.ledger.refused = 0;
        for f in s.floors.iter_mut() {
            f.live = 0;
        }
        s.out_topic = dev_channel_port(sys, 1, 2);
        s.out_messaging = dev_channel_port(sys, 1, 3);
        s.out_forward = dev_channel_port(sys, 1, 4);
        s.out_proposals_tagged = dev_channel_port(sys, 1, 7);
        s.out_audit = dev_channel_port(sys, 1, 5);
        s.out_metrics = dev_channel_port(sys, 1, 6);
        // Declared LAST in the manifest, so these take the highest
        // indices and nothing above shifted.
        // retention_floor and cp_out are not Kafka's: every variant
        // forwards the control plane's entries and publishes its floor.
        // Left at zero in the mqtt variant they named channel 0.
        s.out_retention_floor = dev_channel_port(sys, 1, 9);
        s.out_cp = dev_channel_port(sys, 1, 10);
        s.cp_forwarded = 0;
        s.cp_refused = 0;
        #[cfg(feature = "kafka")]
        {
            s.out_wal_request = dev_channel_port(sys, 1, 8);
            s.in_wal_reply = dev_channel_port(sys, 0, 8);
            s.cold = [ColdFetch::zero(); COLD_FETCH_SLOTS];
            s.next_cold_id = cold_id_base(s.worker_id) | 1;
        }
        s.credit_base_window = 10;
        s.keep_alive_factor_pct = 150;
        s.max_inflight_default = 10;
        s.pid_entry_credits = 4096;
        s.codec_gated = 0;
        s.prop_hold_len = 0;
        s.prop_hold_chan = -1;
        s.prop_hold_shard = 0;
        s.proposals_held = 0;
        s.codec_hold_len = [0; CODEC_INPUTS];
        s.codec_hold_mt = [0; CODEC_INPUTS];
        s.codec_held = 0;
        s.mbox.init();
        s.mflow.init();
        s.ahead_of_backlog = false;
        s.deliveries_spilled = 0;
        s.retained_reads_refused = 0;
        s.dedup_refused_delivered = 0;
        s.hb_codec = 0;
        s.hb_pub = 0;
        s.hb_prop = 0;
        s.hb_assigned = 0;
        s.hb_reg = 0;
        s.hb_commit = 0;
        s.last_hb_ms = 0;
        s.apply_connect_no_prior = 0;
        s.pid_byte_credits = 64 * 1024;
        s.follower_factor_q16 = 65536;
        s.apply_index = [0; MAX_APPLY_PARTITIONS];
        s.applied_runs = [term::RunTracker::new(); MAX_APPLY_PARTITIONS];
        s.applied_spill = term::Spill::new();
        s.applied_notice_ms = 0;
        s.applied_notice_drops = 0;
        s.acks_superseded = 0;
        s.applying_index = 0;
        s.apply_resets = 0;
        // The allocation range must be in place before the table is
        // built: `clear` pushes each free slot in range onto the stack.
        let hi = if s.slot_count == 0 {
            0
        } else {
            s.slot_base.saturating_add(s.slot_count)
        };
        sessions::set_alloc_range(&mut s.sessions, s.slot_base, hi);
        for i in 0..MAX_SESSIONS {
            sessions::clear(&mut s.sessions, i);
            s.sub_book.clear(i as u32);
            sessions::clear_flow(&mut s.sessions, i);
        }
        correlate::init(&mut s.correlate);
        correlate::set_namespace(&mut s.correlate, s.worker_id);
        worker::init(&mut s.worker, s.worker_id);
        // Declared last in the manifest: in[9] / out[11].
        s.worker.in_ctrl = dev_channel_port(sys, 0, 9);
        s.worker.out_ctrl = dev_channel_port(sys, 1, 11);
        s.worker.in_ctrl2 = dev_channel_port(sys, 0, 12);
        s.worker.out_ctrl2 = dev_channel_port(sys, 1, 15);
        store::init(&mut s.store);
        store::set_worker(&mut s.store, s.worker_id);
        reply_hold::init(&mut s.hold);
        reply_hold::owed_init(&mut s.disc_owed);
        s.disc_owed_from = 0;
        s.answers_owed = 0;
        s.sub_inflight_dropped_on_resume = 0;
        s.conn_bindings_evicted = 0;
        s.kafka_produce_rx = 0;
        s.kafka_produce_acked = 0;
        s.kafka_produce_errors = 0;
        #[cfg(feature = "kafka")]
        {
            s.idem = kafka_idem::IdemTable::new();
            s.kafka_idem_duplicates = 0;
            s.kafka_idem_rejected = 0;
            s.kafka_group_gen_dropped = 0;
            s.kafka_group_gen_proposed = 0;
            s.kafka_group_members_proposed = 0;
            s.gen_proposed = [0; KGROUPS];
        }

        s.kafka_fetch_rx = 0;
        s.store.full = 0;
        s.amqp_publish_rx = 0;
        s.amqp_publish_acked = 0;
        s.amqp_publish_errors = 0;
        s.amqp_get_rx = 0;
        consumers::init(&mut s.consumers);
        s.kafka_group_ops = 0;
        s.kafka_offset_commits = 0;
        s.kafka_fetch_out_of_range = 0;
        #[cfg(feature = "kafka")]
        {
            s.kafka_fetch_not_owned = 0;
        }
        s.kafka_offset_ungated = 0;
        s.amqp_delivers = 0;
        s.amqp_consumer_acks = 0;
        dev_log(sys, 3, b"[sess] init v9".as_ptr(), 14);
        0
    }
}

use reply_hold::Reply;

/// Emit a `MSG_SESSION_RESPONSE` to the connection's output via
/// wire-envelope framing; one the bus refuses is held and written in
/// order ([`Reply::Must`]).
///
/// Channel payload (post-envelope-strip) is `[conn_id:u16 LE][proto]
/// [pkt_type][flags][body]`. Envelope framing is required because the codec read
/// loop drains the channel multiple messages per tick — without per-
/// message length prefixes, back-to-back writes coalesce on the byte
/// FIFO and the consumer mis-parses everything after the first.
///
/// # Safety
/// `sys` must point at a live kernel syscall table.
unsafe fn emit_codec_response(
    s: &mut ModuleState,
    sys: &SyscallTable,
    conn_id: u16,
    proto: u8,
    pkt_type: u8,
    flags: u8,
    body: &[u8],
) -> bool {
    emit_codec_reply(s, sys, conn_id, proto, pkt_type, flags, body, Reply::Must)
}

/// [`emit_codec_response`] for work the caller keeps and offers again
/// ([`Reply::Offer`]): false when the output has no room for it now.
///
/// # Safety
/// `sys` must point at a live kernel syscall table.
unsafe fn offer_codec_response(
    s: &mut ModuleState,
    sys: &SyscallTable,
    conn_id: u16,
    proto: u8,
    pkt_type: u8,
    flags: u8,
    body: &[u8],
) -> bool {
    emit_codec_reply(s, sys, conn_id, proto, pkt_type, flags, body, Reply::Offer)
}

/// # Safety
/// `sys` must point at a live kernel syscall table.
#[allow(
    clippy::too_many_arguments,
    reason = "the envelope's fields and how a refusal is met; the two callers above are the API"
)]
unsafe fn emit_codec_reply(
    s: &mut ModuleState,
    sys: &SyscallTable,
    conn_id: u16,
    proto: u8,
    pkt_type: u8,
    flags: u8,
    body: &[u8],
    reply: Reply,
) -> bool {
    let chan = codec_chan(s, conn_id);
    if chan < 0 {
        return false;
    }
    // A session frozen mid-handoff emits nothing: the importing worker
    // answers from the state it received (worker.rs §Frozen sessions).
    if worker::is_frozen(&s.worker, conn_id) {
        s.worker.frozen_drops = s.worker.frozen_drops.wrapping_add(1);
        return true;
    }
    // A connection owed a DISCONNECT is told nothing more: nothing may go
    // ahead of the DISCONNECT, and after it the codec closes the
    // connection. What a delivery or a resend carried stays owed in the
    // log for the session's next connection.
    if reply_hold::owed(&s.disc_owed, conn_id).is_some() {
        return true;
    }
    // A DISCONNECT that finds no room is owed (`disc_owed`), one per
    // connection, rather than held: refusals and superseded verdicts fail
    // flows from inputs that cannot wait, more of them than a ring holds.
    let disconnect = proto == PROTO_MQTT && pkt_type == PKT_DISCONNECT;
    // A session attached as transport-migratable answers in productions:
    // numbered and retained until the anchor's horizon covers them
    // (docs/architecture/anchor_continuity.md). A full ring holds the
    // response back, as a full channel does; one the channel refuses, and
    // any numbered while it is owed, stays in the ring and `seat_step`
    // writes it in its turn.
    if s.worker.seats.seat_of(conn_id).is_some() {
        let total = CODEC_ENV_HDR + body.len();
        if total > BUF_SIZE {
            return false;
        }
        let mut inner = [0u8; BUF_SIZE];
        inner[0..2].copy_from_slice(&conn_id.to_le_bytes());
        inner[2] = proto;
        inner[3] = pkt_type;
        inner[4] = flags;
        inner[CODEC_ENV_HDR..total].copy_from_slice(body);
        let poll = (sys.channel_poll)(chan, 0x02);
        let writable = poll > 0 && (poll as u32 & 0x02) != 0;
        let mut out = [0u8; BUF_SIZE + worker::anchor_core::PRODUCTION_INSERT];
        let ok = match worker::produce(&mut s.worker, conn_id, &inner[..total], &mut out) {
            worker::anchor_core::Produced::Wrapped { id, len } => {
                let w = if writable {
                    wire::channel_write_msg(sys, chan, wire::MSG_ANCHOR_PRODUCTION, &out[..len])
                } else {
                    0
                };
                if w > 0 {
                    worker::note_out(&mut s.worker, conn_id, wire::ENVELOPE_HDR + len);
                    true
                } else if s.worker.seats.defer(conn_id, id) {
                    // The channel has no room for it: kept, and written
                    // from the seat's step.
                    true
                } else {
                    // Numbered without its bytes: the id is taken back and
                    // the response held, so the anchor never sees a gap.
                    s.worker.seats.unproduce(conn_id, id);
                    false
                }
            }
            worker::anchor_core::Produced::Queued { .. } => true,
            worker::anchor_core::Produced::Full => false,
            worker::anchor_core::Produced::Unseated => false,
            // Unreachable for a frame built above; dropped and counted.
            worker::anchor_core::Produced::Malformed => {
                s.worker.seats.full = s.worker.seats.full.wrapping_add(1);
                true
            }
        };
        if !ok && disconnect {
            reply_hold::owe(
                &mut s.disc_owed,
                conn_id,
                body.first().copied().unwrap_or(0),
            );
            return true;
        }
        return ok;
    }
    let total = CODEC_ENV_HDR + body.len();
    if total > BUF_SIZE {
        return false;
    }
    let mut out = [0u8; BUF_SIZE];
    out[0..2].copy_from_slice(&conn_id.to_le_bytes());
    out[2] = proto;
    out[3] = pkt_type;
    out[4] = flags;
    out[CODEC_ENV_HDR..total].copy_from_slice(body);
    let reply = if disconnect { Reply::Kept } else { reply };
    let taken = respond(s, sys, chan, conn_id, &out[..total], reply, true).taken();
    if !taken && disconnect {
        reply_hold::owe(
            &mut s.disc_owed,
            conn_id,
            body.first().copied().unwrap_or(0),
        );
        return true;
    }
    taken
}

/// The hold ring output `chan` is held on: `codec_out`, `codec2_out`,
/// `front_out` in that order.
fn hold_output(s: &ModuleState, chan: i32) -> Option<usize> {
    if chan < 0 {
        return None;
    }
    [s.out_codec, s.out_codec2, s.out_front]
        .iter()
        .position(|&c| c == chan)
}

/// Write a `MSG_SESSION_RESPONSE` `payload` for `conn` to `chan`: at once
/// when nothing is held ahead of it on that output and the bus takes it;
/// otherwise met as `reply` says (`reply_hold::respond`) — held behind
/// what is held and written in order by [`flush_holds`], or refused to a
/// caller that keeps the work. `counted` writes count against the
/// connection's delivery cursor (`worker::note_out`) when they are made.
/// A [`Reply::Must`] meeting a full ring is the one loss (`hold.overflow`,
/// `rlost=` on the heartbeat); the input gates make it unreachable.
///
/// # Safety
/// `sys` must point at a live kernel syscall table.
unsafe fn respond(
    s: &mut ModuleState,
    sys: &SyscallTable,
    chan: i32,
    conn: u16,
    payload: &[u8],
    reply: Reply,
    counted: bool,
) -> reply_hold::Sent {
    let Some(k) = hold_output(s, chan) else {
        return reply_hold::Sent::Refused;
    };
    // A coordinator answer is the pair's, not the session's: its own type,
    // admitted by the anchor from the coordinator whatever the binding,
    // and in no delivery cursor.
    let (msg_type, counted) = if s.coord_reply {
        (wire::MSG_COORD_RESPONSE, false)
    } else {
        (wire::MSG_SESSION_RESPONSE, counted)
    };
    let w = &mut s.worker;
    let sent = reply_hold::respond(
        &mut s.hold.rings[k],
        sys,
        chan,
        conn,
        msg_type,
        payload,
        reply,
        counted,
        |c, bytes, counted| {
            if counted {
                worker::note_out(w, c, bytes);
            }
        },
    );
    match sent {
        reply_hold::Sent::Held => s.hold.held = s.hold.held.wrapping_add(1),
        reply_hold::Sent::Full => {
            s.hold.overflow = s.hold.overflow.wrapping_add(1);
            if s.hold.overflow & 0x3F == 1 {
                dev_log(
                    sys,
                    1,
                    b"[sess] response hold full: response lost".as_ptr(),
                    40,
                );
            }
        }
        _ => {}
    }
    sent
}

/// Write output `k`'s held responses, oldest first, as far as the bus
/// takes them.
///
/// # Safety
/// `sys` must point at a live kernel syscall table.
unsafe fn flush_hold(s: &mut ModuleState, sys: &SyscallTable, k: usize) {
    let chan = [s.out_codec, s.out_codec2, s.out_front][k];
    let w = &mut s.worker;
    reply_hold::flush(&mut s.hold.rings[k], sys, chan, |conn, bytes, counted| {
        if counted {
            worker::note_out(w, conn, bytes);
        }
    });
}

/// Every output's held responses, then the DISCONNECTs owed
/// (`disc_owed`) — each only once its output holds nothing for its
/// connection, up to 64 a step, in turn. Run first in every step, so each
/// input gate sees what the bus took.
///
/// # Safety
/// `sys` must point at a live kernel syscall table.
unsafe fn flush_holds(s: &mut ModuleState, sys: &SyscallTable) {
    for k in 0..reply_hold::OUTPUTS {
        if !reply_hold::is_empty(&s.hold.rings[k]) {
            flush_hold(s, sys, k);
        }
    }
    if s.disc_owed.count == 0 {
        return;
    }
    let mut conns = [0u16; 64];
    let n = reply_hold::owed_conns(&s.disc_owed, s.disc_owed_from, &mut conns);
    if n > 0 {
        s.disc_owed_from = conns[n - 1].wrapping_add(1);
    }
    for &conn in &conns[..n] {
        let Some(reason) = reply_hold::owed(&s.disc_owed, conn) else {
            continue;
        };
        let chan = codec_chan(s, conn);
        let Some(k) = hold_output(s, chan) else {
            reply_hold::settle(&mut s.disc_owed, conn);
            continue;
        };
        // Behind whatever its output still holds for the connection.
        if reply_hold::ring_holds_for(&s.hold.rings[k], conn) {
            continue;
        }
        let mut out = [0u8; CODEC_ENV_HDR + 1];
        out[0..2].copy_from_slice(&conn.to_le_bytes());
        out[2] = PROTO_MQTT;
        out[3] = PKT_DISCONNECT;
        out[CODEC_ENV_HDR] = reason;
        if wire::channel_write_msg(sys, chan, wire::MSG_SESSION_RESPONSE, &out) > 0 {
            worker::note_out(&mut s.worker, conn, wire::ENVELOPE_HDR + out.len());
            reply_hold::settle(&mut s.disc_owed, conn);
        }
    }
}

/// Whether every output has room for the most one input can answer
/// (`reply_hold::has_reserve`): the gate on reading `flow`'s completions,
/// which `flow` holds while they are not read. Inputs that cannot wait —
/// committed entries, Clustor's assignments and refusals, cold reads —
/// are never gated on a client; their answers are kept instead
/// ([`Reply::Kept`], [`answer_inflight`], the cold-fetch slot's `owed`).
fn hold_ready(s: &ModuleState) -> bool {
    [s.out_codec, s.out_codec2, s.out_front]
        .iter()
        .zip(s.hold.rings.iter())
        .all(|(&c, r)| c < 0 || reply_hold::has_reserve(r))
}

/// Whether the output a codec envelope from `conn` (protocol `proto`,
/// where it names one) is answered on has room for the most it can
/// answer: the connection's own output for MQTT, `codec_out` — where
/// every Kafka and AMQP answer goes — for the others. Only that output
/// holds the envelope back, so a full output stops the work of the
/// sessions it serves and no other.
fn hold_ready_for(s: &ModuleState, conn: u16, proto: Option<u8>) -> bool {
    let k = match proto {
        Some(PROTO_KAFKA | PROTO_AMQP) => Some(0),
        _ => hold_output(s, codec_chan(s, conn)),
    };
    k.is_none_or(|k| reply_hold::has_reserve(&s.hold.rings[k]))
}

/// Whether a seated session (`conn`) has a ring slot free beyond those
/// its publishes still being committed will need for their answers: the
/// room it may spend on anything else, so an answer always finds a slot
/// (docs/architecture/anchor_continuity.md). A session without a seat
/// always has room.
fn seat_has_room(s: &ModuleState, conn: u16) -> bool {
    if s.worker.seats.seat_of(conn).is_none() {
        return true;
    }
    let mut flows = [gate::GateFlow::default(); MAX_INFLIGHT_PER_SESSION];
    let n = sessions::find_by_conn(&s.sessions, conn).map_or(0, |si| {
        sessions::gate_flows(&s.sessions, si, INFLIGHT_PUB, &mut flows)
    });
    gate::seat_may_take(s.worker.seats.room(conn), &flows[..n])
}

/// Whether the codec envelope `buf` of type `mt` must wait before it is
/// taken: its session is seated and has no room for the answer it may
/// cause ([`seat_has_room`]), or it is a QoS 1/2 PUBLISH whose session
/// must wait for an in-flight slot one of its commits will free
/// (`gate::publish_waits`). A delivery envelope is read
/// through to the frame it carries. Read only: nothing is admitted,
/// counted or allocated. Each condition clears without anything more
/// from the client — a commit, or the anchor's horizon — so waiting
/// never waits on a packet behind this one.
fn codec_waits(s: &ModuleState, mt: u8, buf: &[u8]) -> bool {
    if buf.len() < 2 {
        return false;
    }
    let conn_id = u16::from_le_bytes([buf[0], buf[1]]);
    if !seat_has_room(s, conn_id) {
        return true;
    }
    let (inner_mt, at) = if mt == wire::MSG_ANCHOR_DELIVERY {
        match worker::anchor_core::decode_delivery(buf) {
            Some(h) => (
                h.inner_msg_type,
                worker::anchor_core::CONN_ID_LEN + worker::anchor_core::DELIVERY_INSERT,
            ),
            None => return false,
        }
    } else if mt == wire::MSG_COORD_CALL {
        // `[conn:2][inner_mt][rest]`.
        match buf.get(2) {
            Some(&inner) => (inner, 3),
            None => return false,
        }
    } else {
        (mt, 2)
    };
    // An envelope may be answered only with room in the response hold for
    // its answers; it waits, and so does its input, until the bus has
    // taken what is held. A connection-closed notice answers nothing.
    if inner_mt != wire::MSG_CONN_CLOSED && !hold_ready_for(s, conn_id, buf.get(at).copied()) {
        return true;
    }
    // [proto][pkt_type][flags][packet_id:2 BE]...
    if inner_mt != wire::MSG_SESSION_PROPOSAL || buf.len() < at + 5 {
        return false;
    }
    let (proto, pkt_type, flags) = (buf[at], buf[at + 1], buf[at + 2]);
    let qos = (flags >> 1) & 0x03;
    if proto != PROTO_MQTT || pkt_type != PKT_PUBLISH || qos == 0 {
        return false;
    }
    let Some(si) = sessions::find_by_conn(&s.sessions, conn_id) else {
        return false;
    };
    // A QoS 2 retransmission of a flow already in flight needs no slot,
    // and goes on to be answered.
    let packet_id = u16::from_be_bytes([buf[at + 3], buf[at + 4]]);
    if qos == 2 && sessions::inflight_find(&s.sessions, si, packet_id, INFLIGHT_PUB).is_some() {
        return false;
    }
    let mut flows = [gate::GateFlow::default(); MAX_INFLIGHT_PER_SESSION];
    let n = sessions::gate_flows(&s.sessions, si, INFLIGHT_PUB, &mut flows);
    gate::publish_waits(&flows[..n], MAX_INFLIGHT_PER_SESSION)
}

/// The index of codec input `chan` among [`CODEC_INPUTS`].
fn codec_input(s: &ModuleState, chan: i32) -> usize {
    if chan == s.in_codec {
        0
    } else if chan == s.in_codec2 {
        1
    } else {
        2
    }
}

/// The next codec input with an envelope waiting: the transport's
/// `codec_in`, the second anchor seam's `codec2_in` and the front's
/// `front_in`, each offered first in turn, passing over one whose held
/// envelope still waits.
///
/// # Safety
/// `sys` is the kernel's live syscall table.
unsafe fn next_codec_input(s: &mut ModuleState, sys: &SyscallTable) -> Option<i32> {
    let order = match s.codec_turn {
        0 => [s.in_codec, s.in_codec2, s.in_front],
        1 => [s.in_codec2, s.in_front, s.in_codec],
        _ => [s.in_front, s.in_codec, s.in_codec2],
    };
    s.codec_turn = (s.codec_turn + 1) % 3;
    for chan in order {
        if chan < 0 || s.codec_hold_len[codec_input(s, chan)] > 0 {
            continue;
        }
        let poll = (sys.channel_poll)(chan, 0x01);
        if poll > 0 && (poll as u32 & 0x01) != 0 {
            return Some(chan);
        }
    }
    None
}

/// The channel a response to `conn_id` leaves on: `front_out` for an
/// in-graph front session, `codec2_out` for a transport connection
/// attached through the second anchor seam, `codec_out` for any other.
#[inline]
fn codec_chan(s: &ModuleState, conn_id: u16) -> i32 {
    out_chan(
        s.out_codec,
        s.out_codec2,
        s.out_front,
        worker::seam(&s.worker, conn_id),
        conn_id,
    )
}

/// [`codec_chan`] over copied-out channels.
#[inline]
fn out_chan(out_codec: i32, out_codec2: i32, out_front: i32, seam: u8, conn_id: u16) -> i32 {
    if wire::is_front_conn(conn_id) {
        out_front
    } else if seam == 1 {
        out_codec2
    } else {
        out_codec
    }
}

/// Offer the productions `conn`'s seat owes a replacement anchor again,
/// under their original ids, oldest first, for as long as the channel
/// takes them; the rest go on a later pass. The replacement's gate
/// answers what its transport already holds without sending it twice and
/// sends the rest.
///
/// # Safety
/// `sys` is the kernel's live syscall table.
pub(crate) unsafe fn reoffer_productions(s: &mut ModuleState, sys: &SyscallTable, conn: u16) {
    let chan = codec_chan(s, conn);
    if chan < 0 {
        return;
    }
    let mut copy = [0u8; worker::anchor_core::RETAIN_BYTES_MAX];
    let mut out =
        [0u8; worker::anchor_core::RETAIN_BYTES_MAX + worker::anchor_core::PRODUCTION_INSERT];
    while let Some((id, inner)) = s.worker.seats.next_reoffer(conn) {
        let len = inner.len();
        copy[..len].copy_from_slice(inner);
        let n = s
            .worker
            .seats
            .rewrap(conn, wire::MSG_SESSION_RESPONSE, id, &copy[..len], &mut out);
        if n == 0 || wire::channel_write_msg(sys, chan, wire::MSG_ANCHOR_PRODUCTION, &out[..n]) <= 0
        {
            return;
        }
        worker::note_out(&mut s.worker, conn, wire::ENVELOPE_HDR + n);
        s.worker.seats.reoffered(conn, id);
    }
}

/// Per-step work for the seated sessions: re-offers a replacement anchor
/// is still owed, then a PINGRESP a full ring held back, then how far the
/// session has consumed.
///
/// # Safety
/// `sys` is the kernel's live syscall table.
pub(crate) unsafe fn seat_step(s: &mut ModuleState, sys: &SyscallTable) {
    for i in 0..worker::anchor_core::SEAT_COUNT {
        let seat = &s.worker.seats.seats[i];
        if !seat.used {
            continue;
        }
        let conn = seat.conn;
        if seat.reoffer_next != 0 {
            reoffer_productions(s, sys, conn);
        }
        if s.worker.seats.seats[i].pingresp_owed
            && emit_codec_response(s, sys, conn, PROTO_MQTT, PKT_PINGRESP, 0, &[])
        {
            s.worker.seats.seats[i].pingresp_owed = false;
        }
        worker::report_consumed(&mut s.worker, sys, i);
    }
}

/// The dark worker of the fault-injection gate: every input is read and
/// discarded, nothing is emitted. Returns the step outcome.
///
/// # Safety
/// `sys` must be the live kernel syscall table.
unsafe fn fault_drain(s: &mut ModuleState, sys: &SyscallTable) -> i32 {
    #[cfg(feature = "kafka")]
    let wal_reply = s.in_wal_reply;
    #[cfg(not(feature = "kafka"))]
    let wal_reply = -1;
    let chans = [
        s.in_codec,
        s.in_codec2,
        s.in_committed,
        s.in_flow,
        s.in_ack,
        s.in_cp,
        s.in_messaging,
        s.in_deliver,
        s.in_assigned,
        wal_reply,
        s.worker.in_ctrl,
        s.worker.in_ctrl2,
        s.in_front,
    ];
    let mut worked = 0u32;
    for ch in chans {
        if ch < 0 {
            continue;
        }
        for _ in 0..64 {
            let poll = (sys.channel_poll)(ch, 0x01);
            if poll <= 0 || (poll as u32 & 0x01) == 0 {
                break;
            }
            let _ = wire::channel_read_msg(sys, ch, &mut s.in_buf);
            worked += 1;
        }
    }
    if worked > 0 {
        STEP_BURST
    } else {
        0
    }
}

// ── Exact replay (session_replay_core) ──────────────────────────────────────

/// The most recent entry for `slot` this worker skipped as another
/// worker's, if any.
fn replay_last_skip(s: &ModuleState, slot: usize) -> Option<(u16, u64)> {
    if slot < MAX_SESSIONS && s.skip_index[slot] != 0 {
        Some((s.skip_part[slot], s.skip_index[slot]))
    } else {
        None
    }
}

/// Install a replay floor for an imported `slot`. `false` when every
/// floor is in use.
fn replay_install_floor(s: &mut ModuleState, slot: u32, tail: &replay::HandoffTail) -> bool {
    let Some(f) = s.floors.iter_mut().find(|f| f.live == 0 || f.slot == slot) else {
        return false;
    };
    f.slot = slot;
    f.tail = *tail;
    f.live = 1;
    true
}

/// True when a free floor exists (or `slot` already has one).
fn replay_floor_available(s: &ModuleState, slot: u32) -> bool {
    s.floors.iter().any(|f| f.live == 0 || f.slot == slot)
}

/// The import of `slot` is committed: what this worker skipped for it
/// while it was the other worker's no longer matters.
fn replay_clear_skip(s: &mut ModuleState, slot: usize) {
    if slot < MAX_SESSIONS {
        s.skip_index[slot] = 0;
        s.skip_part[slot] = 0;
    }
}

/// True when the entry at `(partition, index)` for `slot` is already in
/// the record the slot was imported with.
fn replay_floor_filters(s: &ModuleState, slot: u32, partition: u16, index: u64) -> bool {
    s.floors
        .iter()
        .any(|f| f.live != 0 && f.slot == slot && replay::below_floor(&f.tail, partition, index))
}

/// Release every floor this worker's apply cursor has passed.
fn replay_release_floors(s: &mut ModuleState) {
    let apply = s.apply_index;
    for f in s.floors.iter_mut() {
        if f.live != 0 && replay::floor_passed(&f.tail, &apply) {
            f.live = 0;
        }
    }
}

/// The ledger's half of apply, run on EVERY worker whatever slot the
/// entry names: it has no side effects, so a worker that may one day
/// adopt the session holds what it is owed. `QOP_DELIVER` records an
/// owed delivery, `QOP_DELIVER_ACK` releases or completes one, and a
/// clean-start `QOP_CONNECT` forgets what the session was owed.
fn replay_apply_ledger(s: &mut ModuleState, p: &wire::PeeledQProp, start: usize, end: usize) {
    if end <= start || end > s.in_buf.len() {
        return;
    }
    match p.op {
        wire::QOP_DELIVER => {
            if let Some(d) = replay::decode_deliver(&s.in_buf[start..end]) {
                if !s.ledger.record(p.tenant, &d) {
                    // What the session is owed is no longer whole here: a
                    // site export of it would leave this delivery out.
                    if let Some(si) = sessions::find_by_stream(&s.sessions, p.tenant, d.stream_hash)
                    {
                        s.sub_book.lose(si as u32);
                    }
                }
            }
        }
        wire::QOP_DELIVER_ACK => {
            if let Some(a) = replay::decode_deliver_ack(&s.in_buf[start..end]) {
                s.ledger.ack(p.tenant, &a);
            }
        }
        wire::QOP_CONNECT => {
            // `[clean_start][keep_alive:2][stream_hash:8]...`
            if end - start >= 11 && s.in_buf[start] != 0 {
                let mut h = [0u8; 8];
                h.copy_from_slice(&s.in_buf[start + 3..start + 11]);
                s.ledger.drop_stream(p.tenant, u64::from_le_bytes(h));
            }
        }
        _ => {}
    }
}

/// Log a QoS 1/2 delivery just handed to the codec for session `si`
/// under `packet_id` (`QOP_DELIVER`), keyed to the session's shard.
/// A delivery that cannot be logged — past the ledger's body bound, or
/// refused by the proposal edge — is counted: it is not reconstructible.
///
/// # Safety
/// `sys` must be the live kernel syscall table.
unsafe fn log_delivery(
    s: &mut ModuleState,
    sys: &SyscallTable,
    si: usize,
    packet_id: u16,
    qos: u8,
    body: &[u8],
) {
    if s.log_deliveries == 0 {
        return;
    }
    let mut buf =
        [0u8; wire::QPROP_UNTAGGED_HDR_LEN + replay::DELIVER_FIXED + replay::DELIVER_BODY_MAX];
    let tenant = sessions::tenant(&s.sessions, si);
    wire::encode_qprop_header(
        &mut buf[..wire::QPROP_HEADER_LEN],
        wire::QOP_DELIVER,
        tenant,
        si as u32,
    );
    let n = replay::encode_deliver(
        &mut buf[wire::QPROP_UNTAGGED_HDR_LEN..],
        &replay::Deliver {
            stream_hash: sessions::stream_hash(&s.sessions, si),
            generation: sessions::generation(&s.sessions, si),
            packet_id,
            qos,
            body,
        },
    );
    let total = wire::QPROP_UNTAGGED_HDR_LEN + n;
    let shard = sessions::shard(&s.sessions, si);
    if n != 0 && try_emit_keyed(sys, s.out_proposals, &s.view, shard, &buf[..total]) {
        s.deliver_logged = s.deliver_logged.wrapping_add(1);
        s.proposals_emitted = s.proposals_emitted.wrapping_add(1);
    } else {
        s.deliver_unlogged = s.deliver_unlogged.wrapping_add(1);
    }
}

/// Log the progress of the delivery session `si` owes under
/// `packet_id` (`QOP_DELIVER_ACK`): `replay::ACK_RELEASED` when a QoS 2
/// subscriber's PUBREC moved it to PUBREL, `replay::ACK_COMPLETE` at the
/// client's PUBACK or PUBCOMP.
///
/// # Safety
/// `sys` must be the live kernel syscall table.
unsafe fn log_deliver_ack(
    s: &mut ModuleState,
    sys: &SyscallTable,
    si: usize,
    packet_id: u16,
    stage: u8,
) {
    if s.log_deliveries == 0 {
        return;
    }
    let mut buf = [0u8; wire::QPROP_UNTAGGED_HDR_LEN + replay::DELIVER_ACK_LEN];
    wire::encode_qprop_header(
        &mut buf[..wire::QPROP_HEADER_LEN],
        wire::QOP_DELIVER_ACK,
        sessions::tenant(&s.sessions, si),
        si as u32,
    );
    replay::encode_deliver_ack(
        &mut buf[wire::QPROP_UNTAGGED_HDR_LEN..],
        &replay::DeliverAck {
            stream_hash: sessions::stream_hash(&s.sessions, si),
            generation: sessions::generation(&s.sessions, si),
            packet_id,
            stage,
        },
    );
    let shard = sessions::shard(&s.sessions, si);
    if try_emit_keyed(sys, s.out_proposals, &s.view, shard, &buf) {
        s.proposals_emitted = s.proposals_emitted.wrapping_add(1);
    }
}

/// Resend, in the order first sent, everything the log says session `si`
/// is still owed: on a resumed CONNECT, and when a worker adopts the
/// session after its previous worker was lost (MQTT 3.1.1 §4.4, 5.0
/// §4.4: unacknowledged PUBLISH and PUBREL packets are re-sent under
/// their original packet ids).
///
/// A delivery the subscriber has not answered goes out again as a DUP
/// PUBLISH. A QoS 2 delivery whose PUBREC arrived — the ledger says
/// released, or the session's own in-flight entry is past PUBLISH — goes
/// out as its PUBREL: the subscriber may already have handed the message
/// on and forgotten the packet id, so a PUBLISH now would be a second
/// copy. Each is registered in the session's outbound in-flight table
/// under its packet id, so the client's PUBACK / PUBREC / PUBCOMP
/// completes it as usual; an entry the session already held (a resume on
/// the same worker, or one carried in a handoff record) is reused and not
/// counted against the flow window again. A seated session's resends stop
/// where its ring has no slot left beyond those its publishes being
/// committed will need ([`seat_has_room`]); the rest stay owed in the log.
///
/// # Safety
/// `sys` must be the live kernel syscall table.
unsafe fn redeliver_owed(s: &mut ModuleState, sys: &SyscallTable, si: usize) -> u32 {
    let tenant = sessions::tenant(&s.sessions, si);
    let stream = sessions::stream_hash(&s.sessions, si);
    let conn_id = sessions::conn_id(&s.sessions, si);
    let mut owed = [0usize; MAX_INFLIGHT_PER_SESSION];
    let n = s.ledger.owed(tenant, stream, &mut owed);
    let mut sent = 0u32;
    for &i in owed.iter().take(n) {
        if !seat_has_room(s, conn_id) {
            break;
        }
        let e = s.ledger.entries[i];
        sessions::reserve_sub_packet_id(&mut s.sessions, si, e.packet_id);
        let (ii, added) = match sessions::inflight_find(&s.sessions, si, e.packet_id, INFLIGHT_SUB)
        {
            Some(ii) => (ii, false),
            None => {
                match sessions::inflight_add(&mut s.sessions, si, e.packet_id, e.qos, INFLIGHT_SUB)
                {
                    Some(ii) => (ii, true),
                    None => continue,
                }
            }
        };
        let in_rel =
            sessions::inflight_view(&s.sessions, si, ii).is_some_and(|v| v.phase == QOS2_PUBREL);
        let ok = if e.qos == 2 && (e.released != 0 || in_rel) {
            sessions::inflight_set_phase(&mut s.sessions, si, ii, QOS2_PUBREL);
            let pubrel = e.packet_id.to_be_bytes();
            emit_codec_reply(
                s,
                sys,
                conn_id,
                PROTO_MQTT,
                PKT_PUBREL,
                0x02,
                &pubrel,
                Reply::Spare,
            )
        } else {
            let flags = (e.qos << 1) | 0x08; // DUP
            let body = e.body();
            emit_codec_reply(
                s,
                sys,
                conn_id,
                PROTO_MQTT,
                PKT_PUBLISH,
                flags,
                body,
                Reply::Spare,
            )
        };
        if !ok {
            // The rest stay owed, in order: none is sent ahead of this one.
            break;
        }
        if added {
            sessions::note_delivery(&mut s.sessions, si);
        }
        sent += 1;
    }
    s.redelivered = s.redelivered.wrapping_add(sent);
    sent
}

/// Emit a proposal that names NO routing key — the one path onto clustor
/// `partition_router`'s body-hash placement — or refuse it when
/// `keyed_only` (param 9, default on) says every proposal from this
/// module must be keyed.
///
/// Refusal is counted (`proposals_unkeyed_refused`, `unkeyed=` on the
/// `[sess] hb` line) and said once per 32 so the drop is never silent:
/// a client whose op reaches here is sending something this module
/// cannot place, and placing it by a hash of its bytes would scatter
/// one session's ops over the partitions.
///
/// # Safety
/// Caller must supply a valid `&SyscallTable` per the module ABI.
unsafe fn try_emit_unkeyed(s: &mut ModuleState, sys: &SyscallTable, body: &[u8]) -> bool {
    if s.keyed_only != 0 {
        s.proposals_unkeyed_refused = s.proposals_unkeyed_refused.wrapping_add(1);
        if s.proposals_unkeyed_refused & 0x1F == 1 {
            dev_log(sys, 2, b"[sess] unkeyed proposal refused".as_ptr(), 31);
        }
        return false;
    }
    try_emit(sys, s.out_proposals, wire::MSG_CLIENT_PROPOSAL, body)
}

#[cfg(feature = "kafka")]
/// Emit a Kafka `MSG_SESSION_RESPONSE` to out_codec. Kafka responses use
/// a wider header than the MQTT `[conn][proto][pkt][flags]` form so the
/// codec can reframe by correlation id without any session state:
///   `[conn_id:u16 LE][proto=1][api_key:i16 LE][api_ver:i16 LE]
///    [corr_id:i32 LE][body...]`
///
/// # Safety
unsafe fn emit_kafka_response(
    s: &mut ModuleState,
    sys: &SyscallTable,
    conn_id: u16,
    api_key: i16,
    api_ver: i16,
    kafka_corr: i32,
    body: &[u8],
) -> bool {
    let total = KAFKA_ENV_HDR + body.len();
    if total > BUF_SIZE {
        return false;
    }
    let mut out = [0u8; 256];
    if total > out.len() {
        return false;
    }
    write_kafka_env_header(&mut out, conn_id, api_key, api_ver, kafka_corr);
    out[KAFKA_ENV_HDR..total].copy_from_slice(body);
    let chan = s.out_codec;
    // Counted: the anchor counts every response it relays for a bound
    // connection, and a handoff admits the two counts only when equal.
    let taken = respond(s, sys, chan, conn_id, &out[..total], Reply::Must, true).taken();
    s.kafka_settled |= taken;
    taken
}

/// One partition's ProduceResponse outcome: the request correlation it
/// answers and the per-partition result.
#[cfg(feature = "kafka")]
#[derive(Clone, Copy)]
struct KafkaProduceAck<'a> {
    api_ver: i16,
    kafka_corr: i32,
    topic: &'a [u8],
    partition: i32,
    error: i16,
    base_offset: i64,
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
    s: &mut ModuleState,
    sys: &SyscallTable,
    conn_id: u16,
    ack: KafkaProduceAck<'_>,
) -> bool {
    let KafkaProduceAck {
        api_ver,
        kafka_corr,
        topic,
        partition,
        error,
        base_offset,
    } = ack;
    if topic.len() > KAFKA_MAX_TOPIC {
        return false;
    }
    let mut body = [0u8; 128];
    let mut p = 0usize;
    body[p..p + 4].copy_from_slice(&1i32.to_be_bytes());
    p += 4;
    body[p..p + 2].copy_from_slice(&(topic.len() as i16).to_be_bytes());
    p += 2;
    body[p..p + topic.len()].copy_from_slice(topic);
    p += topic.len();
    body[p..p + 4].copy_from_slice(&1i32.to_be_bytes());
    p += 4;
    body[p..p + 4].copy_from_slice(&partition.to_be_bytes());
    p += 4;
    body[p..p + 2].copy_from_slice(&error.to_be_bytes());
    p += 2;
    body[p..p + 8].copy_from_slice(&base_offset.to_be_bytes());
    p += 8;
    if api_ver >= 2 {
        body[p..p + 8].copy_from_slice(&(-1i64).to_be_bytes());
        p += 8;
    }
    if api_ver >= 5 {
        body[p..p + 8].copy_from_slice(&0i64.to_be_bytes());
        p += 8;
    }
    if api_ver >= 1 {
        body[p..p + 4].copy_from_slice(&0i32.to_be_bytes());
        p += 4;
    }
    emit_kafka_response(s, sys, conn_id, 0, api_ver, kafka_corr, &body[..p])
}

#[cfg(feature = "kafka")]
/// Handle a Kafka Produce request forwarded by `protocol::kafka`. Payload in
/// `s.in_buf[..plen]`:
///   `[conn_id:u16 LE][proto=1][api_key:i16 LE][api_ver:i16 LE][corr:i32 LE]
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
unsafe fn handle_kafka_produce(s: &mut ModuleState, sys: &SyscallTable, now: u64, plen: usize) {
    let (conn_id, api_ver, kafka_corr) = kafka_env_header(&s.in_buf);
    let req_end = plen;
    let mut off = KAFKA_ENV_HDR;
    let mut parse_ok = true;
    macro_rules! need {
        ($n:expr) => {
            if off + $n > req_end {
                parse_ok = false;
            }
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
                if off > req_end {
                    parse_ok = false;
                }
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
                s.in_buf[off],
                s.in_buf[off + 1],
                s.in_buf[off + 2],
                s.in_buf[off + 3],
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
                s.in_buf[off],
                s.in_buf[off + 1],
                s.in_buf[off + 2],
                s.in_buf[off + 3],
            ]);
            off += 4;
        }
        if parse_ok && (1..=KIN_MAX_PARTS as i32).contains(&part_count) {
            for pi in 0..part_count as usize {
                need!(8);
                if !parse_ok {
                    break;
                }
                p_ids[pi] = i32::from_be_bytes([
                    s.in_buf[off],
                    s.in_buf[off + 1],
                    s.in_buf[off + 2],
                    s.in_buf[off + 3],
                ]);
                off += 4;
                let rlen = i32::from_be_bytes([
                    s.in_buf[off],
                    s.in_buf[off + 1],
                    s.in_buf[off + 2],
                    s.in_buf[off + 3],
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
    // acks = 0: the protocol forbids an answer, and the codec, reading
    // the same field, expects none.
    s.kafka_settled |= acks == 0;

    if !parse_ok || topic_count != 1 {
        s.kafka_produce_errors = s.kafka_produce_errors.wrapping_add(1);
        if topic_len > 0 && acks != 0 {
            emit_kafka_produce_response(
                s,
                sys,
                conn_id,
                KafkaProduceAck {
                    api_ver,
                    kafka_corr,
                    topic: &topic[..topic_len],
                    partition: if part_count >= 1 { p_ids[0] } else { 0 },
                    error: KERR_INVALID_REQUEST,
                    base_offset: -1,
                },
            );
        }
        return;
    }
    let n_parts = part_count as usize;

    // Emit one proposal per healthy partition; per-partition failures
    // stay in p_errs and surface in the aggregated response.
    if acks == 0 {
        for pi in 0..n_parts {
            if p_errs[pi] != 0 {
                continue;
            }
            let op_body_len = wire::PRODUCE_BODY_HDR + topic_len + p_lens[pi];
            let hdr = wire::QPROP_HEADER_LEN;
            if hdr + op_body_len > s.out_buf.len() {
                continue;
            }
            wire::encode_qprop_header(&mut s.out_buf[..hdr], wire::QOP_KAFKA_PRODUCE, 0, 0);
            // No client is waiting on an acks=0 produce, so no origin.
            let rec = wire::encode_produce_body_hdr(
                &mut s.out_buf[hdr..],
                wire::PRODUCE_ORIGIN_NONE,
                wire::PRODUCE_ORIGIN_NONE,
                p_ids[pi] as u16,
                &topic[..topic_len],
            );
            core::ptr::copy_nonoverlapping(
                s.in_buf.as_ptr().add(p_offs[pi]),
                s.out_buf.as_mut_ptr().add(hdr + rec),
                p_lens[pi],
            );
            // A Kafka partition is the unit of placement: its log lives
            // on one shard so offsets stay contiguous and ordered.
            // Tenant is the placeholder 0 until multi-tenancy is wired.
            let shard = wire::shard_kafka(0, &topic[..topic_len], p_ids[pi] as u32);
            if try_emit_keyed(
                sys,
                s.out_proposals,
                &s.view,
                shard,
                &s.out_buf[..hdr + op_body_len],
            ) {
                s.proposals_emitted = s.proposals_emitted.wrapping_add(1);
                s.hb_prop = s.hb_prop.wrapping_add(1);
            }
        }
        return;
    }

    // Ack-gated path: one inflight slot aggregates all partitions.
    let Some((ki, slot)) = store::inflight_alloc(&mut s.store) else {
        s.kafka_produce_errors = s.kafka_produce_errors.wrapping_add(1);
        emit_kafka_produce_response(
            s,
            sys,
            conn_id,
            KafkaProduceAck {
                api_ver,
                kafka_corr,
                topic: &topic[..topic_len],
                partition: p_ids[0],
                error: KERR_REQUEST_TIMED_OUT,
                base_offset: -1,
            },
        );
        return;
    };

    let mut pending = 0u8;
    for pi in 0..n_parts {
        if p_errs[pi] != 0 {
            continue;
        }
        let packet_id = (ki as u16) | ((pi as u16) << 8);
        let Some(cid) = correlate::allocate(&mut s.correlate, slot, packet_id, OP_KPRODUCE, now)
        else {
            p_errs[pi] = KERR_REQUEST_TIMED_OUT;
            continue;
        };
        let op_body_len = wire::PRODUCE_BODY_HDR + topic_len + p_lens[pi];
        let prop_total = wire::QPROP_TAGGED_HDR_LEN + op_body_len;
        if prop_total > s.out_buf.len() {
            let _ = correlate::take(&mut s.correlate, cid);
            p_errs[pi] = KERR_MESSAGE_TOO_LARGE;
            continue;
        }
        s.out_buf[0..8].copy_from_slice(&cid.to_le_bytes());
        wire::encode_qprop_header(
            &mut s.out_buf[8..8 + wire::QPROP_HEADER_LEN],
            wire::QOP_KAFKA_PRODUCE,
            0,
            slot,
        );
        let ob = wire::QPROP_TAGGED_HDR_LEN;
        // This worker of this node accepted the produce, so apply on it —
        // and only on it — may resolve the waiting inflight slot: every
        // worker on every node applies the entry.
        let rec = wire::encode_produce_body_hdr(
            &mut s.out_buf[ob..],
            s.self_id,
            s.worker_id,
            p_ids[pi] as u16,
            &topic[..topic_len],
        );
        core::ptr::copy_nonoverlapping(
            s.in_buf.as_ptr().add(p_offs[pi]),
            s.out_buf.as_mut_ptr().add(ob + rec),
            p_lens[pi],
        );
        let shard = wire::shard_kafka(0, &topic[..topic_len], p_ids[pi] as u32);
        // Ownership is decided BEFORE the emit so the client is told
        // WHY. `try_emit_keyed` refuses the same work, but its `false`
        // is indistinguishable from backpressure, and answering a moved
        // partition with `REQUEST_TIMED_OUT` makes the client retry this
        // broker until it gives up instead of refreshing metadata and
        // going to the right one.
        if let Some(e) = kafka_shard_error(&s.view, shard) {
            let _ = correlate::take(&mut s.correlate, cid);
            p_errs[pi] = e;
            continue;
        }
        if try_emit_keyed(
            sys,
            s.out_proposals_tagged,
            &s.view,
            shard,
            &s.out_buf[..prop_total],
        ) {
            s.proposals_emitted = s.proposals_emitted.wrapping_add(1);
            s.hb_prop = s.hb_prop.wrapping_add(1);
            pending += 1;
        } else {
            // Reached only under genuine backpressure now.
            let _ = correlate::take(&mut s.correlate, cid);
            p_errs[pi] = KERR_REQUEST_TIMED_OUT;
        }
    }

    // Answered from the slot, now or once every partition resolves.
    s.kafka_settled = true;
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
        answer_inflight(s, sys, ki, store::ANSWER_PRODUCE);
    }
}

/// MSG_PROPOSAL_REFUSED: Clustor will never assign this tagged proposal
/// (it is in no log, or for `OUTCOME_UNKNOWN` may be in one). The
/// correlation is freed now — waiting on it would hold its slot and the
/// client until a timeout, and for MQTT none comes — the refusal is
/// counted, and the operation is failed in its protocol's terms
/// (`proposal_refusal_core.rs` says how each status maps):
///
/// - an MQTT PUBLISH: its inflight is released and the connection closed
///   with the reason (DISCONNECT to MQTT 5, a close for MQTT 3.1.1); the
///   client resends it on reconnect under the same packet id. The publish
///   stash is left to its verdict (`fail_publisher_flow` says why);
/// - an MQTT PUBREL: the flow keeps its QoS 2 PUBREL phase with no
///   correlation, and the connection is closed the same way, so the
///   resent PUBREL proposes afresh;
/// - a Kafka Produce partition: answered with the mapped error, and the
///   response fires once every partition has resolved;
/// - an AMQP confirm-mode publish: Basic.Nack;
/// - a Kafka OffsetCommit: its parked response is answered now with every
///   partition failed (`REQUEST_TIMED_OUT`).
///
/// # Safety
/// `sys` must point at a live kernel syscall table.
unsafe fn on_proposal_refused(s: &mut ModuleState, sys: &SyscallTable, r: refusal::Refusal) {
    let Some((session_slot, packet_id, op)) = correlate::take(&mut s.correlate, r.correlation_id)
    else {
        return;
    };
    s.proposals_refused_raft = s.proposals_refused_raft.wrapping_add(1);
    if s.proposals_refused_raft & 0x3F == 1 {
        let mut line = [0u8; 64];
        let mut pos = 0usize;
        for &b in b"[sess] proposal refused by raft status=" {
            line[pos] = b;
            pos += 1;
        }
        pos += fmt_u32_raw(line.as_mut_ptr().add(pos), r.status as u32);
        for &b in b" n=" {
            line[pos] = b;
            pos += 1;
        }
        pos += fmt_u32_raw(line.as_mut_ptr().add(pos), s.proposals_refused_raft);
        dev_log(sys, 2, line.as_ptr(), pos);
    }
    match op {
        OP_PUBLISH | OP_PUBREL => {
            let si = session_slot as usize;
            if si >= MAX_SESSIONS {
                return;
            }
            // Only the flow this correlation was bound to: a packet id is
            // the client's to reuse once its flow is gone.
            let Some(ii) = sessions::inflight_find(&s.sessions, si, packet_id, INFLIGHT_PUB) else {
                return;
            };
            let bound = sessions::inflight_view(&s.sessions, si, ii)
                .is_some_and(|v| v.correlation_id == r.correlation_id);
            if !bound {
                return;
            }
            if op == OP_PUBLISH {
                s.publishes_throttled = s.publishes_throttled.wrapping_add(1);
            }
            fail_publisher_flow(s, sys, si, ii, op == OP_PUBREL, r.status);
        }
        #[cfg(any(feature = "kafka", feature = "amqp"))]
        OP_KPRODUCE | OP_KOFFSET => fail_kin(s, sys, session_slot, packet_id, r.status),
        _ => {}
    }
}

/// Fail an MQTT publisher flow whose proposal will not commit — refused by
/// Clustor, or superseded at its index — with a refusal `status`. A
/// PUBLISH's inflight is released; a PUBREL's flow keeps its QoS 2 PUBREL
/// phase with no correlation and no assignment, so the resent PUBREL
/// proposes afresh and no late completion reads as its PUBCOMP. MQTT has no
/// negative acknowledgement that keeps the packet id, so the connection is
/// closed with the mapped reason and the client resends on reconnect. The
/// DISCONNECT is never lost to a full codec bus: one with no room is owed
/// to the connection (`disc_owed`) and written once its output holds
/// nothing ahead of it.
///
/// The publish stash is not the flow's to release. A stash is opened only
/// by `apply_qop_publish`, for an entry that COMMITTED, under the
/// correlation the flow's inflight held when it applied — so a proposal
/// that failed never has one of its own. The only stash this flow's
/// correlation can name is a committed entry of the same flow: an
/// `OUTCOME_UNKNOWN` (refused or completed) whose entry did commit, or an
/// earlier attempt of the publish committing under the resend's
/// correlation. Its dedup key is already filed, so releasing it would lose
/// the message — the resend reads DUPLICATE and is never fanned out. It
/// frees on its dedup verdict and the topic bus, as every stash does
/// (`finalise_stash`), and the failed flow's correlation is never reused.
///
/// # Safety
/// `sys` must point at a live kernel syscall table.
unsafe fn fail_publisher_flow(
    s: &mut ModuleState,
    sys: &SyscallTable,
    si: usize,
    ii: usize,
    pubrel: bool,
    status: u8,
) {
    if pubrel {
        sessions::inflight_set_correlation(&mut s.sessions, si, ii, 0);
        sessions::inflight_set_wal_index(&mut s.sessions, si, ii, 0, 0, 0);
    } else {
        sessions::inflight_release(&mut s.sessions, si, ii);
    }
    if sessions::is_active(&s.sessions, si) || sessions::is_transient(&s.sessions, si) {
        let conn_id = sessions::conn_id(&s.sessions, si);
        let reason = refusal::mqtt_disconnect_reason(status);
        emit_codec_response(s, sys, conn_id, PROTO_MQTT, PKT_DISCONNECT, 0, &[reason]);
    }
}

/// Fail one partition of a Kafka Produce, a Kafka OffsetCommit or an AMQP
/// confirm-mode publish (the inflight `session_slot` / `packet_id` name)
/// whose proposal will not commit, with a refusal `status`: the produce
/// partition answers the mapped error and the response fires once every
/// partition has resolved; the OffsetCommit answers every partition with
/// its error now; the AMQP publish is Basic.Nack'd.
///
/// # Safety
/// `sys` must point at a live kernel syscall table.
#[cfg(any(feature = "kafka", feature = "amqp"))]
#[cfg_attr(
    not(feature = "kafka"),
    allow(
        unused_variables,
        reason = "the status maps only Kafka's errors and the packet id names only a Kafka produce partition; a Basic.Nack carries neither"
    )
)]
unsafe fn fail_kin(
    s: &mut ModuleState,
    sys: &SyscallTable,
    session_slot: u32,
    packet_id: u16,
    status: u8,
) {
    if session_slot < KAFKA_SLOT_BASE {
        return;
    }
    let (ki, epoch) = store::kin_decode_slot(session_slot);
    let pidx = (packet_id >> 8) as usize;
    if ki >= KAFKA_INFLIGHT || !store::inflight_valid(&s.store, ki, epoch) {
        return;
    }
    // An answer already decided and owed stands.
    if store::inflight_owed(&s.store, ki) != store::ANSWER_NONE {
        return;
    }
    let Some(e) = store::inflight_get(&s.store, ki) else {
        return;
    };
    match e.proto {
        #[cfg(feature = "amqp")]
        KIN_PROTO_AMQP => {
            // A confirm-mode publisher waits on this tag indefinitely, so
            // a Nack the codec bus has no room for keeps the slot, owed,
            // and is answered on a later step.
            answer_inflight(s, sys, ki, store::ANSWER_NACK);
        }
        #[cfg(feature = "kafka")]
        KIN_PROTO_KAFKA_OFFSET => {
            // Later answers for its other offsets find the slot
            // freed (its epoch moves on) and are dropped.
            let err = refusal::kafka_offset_error(status);
            let n = store::inflight_response(&s.store, ki).len();
            s.kafka_produce_errors = s.kafka_produce_errors.wrapping_add(1);
            if n > 0 && n <= s.out_buf.len() {
                s.out_buf[..n].copy_from_slice(store::inflight_response(&s.store, ki));
                if refusal::offset_commit_fail(&mut s.out_buf[..n], err)
                    && store::inflight_set_response(&mut s.store, ki, &s.out_buf[..n])
                {
                    // The failed body is the answer now, owed if the bus
                    // has no room for it.
                    answer_inflight(s, sys, ki, store::ANSWER_OFFSET);
                    return;
                }
            }
            store::inflight_free(&mut s.store, ki);
        }
        #[cfg(feature = "kafka")]
        KIN_PROTO_KAFKA => {
            // This partition is resolved — with an error — on both
            // counts the response waits for. Its apply is counted once
            // however it lands: an `OUTCOME_UNKNOWN` entry may still
            // commit and apply after this, or have applied before it.
            if pidx >= KIN_MAX_PARTS {
                return;
            }
            let err = refusal::kafka_produce_error(status);
            store::inflight_set_part_err(&mut s.store, ki, pidx, err);
            store::inflight_complete_part(&mut s.store, ki);
            store::inflight_apply_part(&mut s.store, ki, pidx);
            s.kafka_produce_errors = s.kafka_produce_errors.wrapping_add(1);
            if store::inflight_ready(&s.store, ki) {
                answer_inflight(s, sys, ki, store::ANSWER_PRODUCE);
            }
        }
        _ => {}
    }
}

/// Answer inflight slot `ki` with `answer` (`store::ANSWER_*`) and free
/// it. The answer is decided where the input cannot wait for room — a
/// committed entry, Clustor's refusal, the expiry sweep — so it is met as
/// [`Reply::Kept`]: when the codec bus and the hold have no room, the slot
/// is kept with the answer owed, and [`answer_owed_inflights`] answers it
/// on a later step. Returns whether it was answered now.
///
/// # Safety
/// `sys` must point at a live kernel syscall table.
#[cfg(any(feature = "kafka", feature = "amqp"))]
unsafe fn answer_inflight(s: &mut ModuleState, sys: &SyscallTable, ki: usize, answer: u8) -> bool {
    let Some(e) = store::inflight_get(&s.store, ki) else {
        return false;
    };
    // Only the arms a variant carries read the entry.
    let _ = &e;
    // A coordinator call's slot is answered as one, whatever input this
    // answer is decided on.
    let outer_coord = s.coord_reply;
    s.coord_reply = e.coord == 1;
    let taken = match answer {
        #[cfg(feature = "kafka")]
        store::ANSWER_PRODUCE => emit_kafka_produce_response_multi(s, sys, ki),
        #[cfg(feature = "kafka")]
        store::ANSWER_OFFSET => {
            let n = store::inflight_response(&s.store, ki).len();
            if n == 0 || n > s.out_buf.len() {
                // Nothing parked to answer with: freed, as it always was.
                true
            } else {
                s.out_buf[..n].copy_from_slice(store::inflight_response(&s.store, ki));
                kafka_outbuf_reply(
                    s,
                    sys,
                    e.conn_id,
                    8,
                    e.api_ver,
                    e.kafka_corr,
                    n,
                    Reply::Kept,
                )
            }
        }
        #[cfg(feature = "amqp")]
        store::ANSWER_ACK | store::ANSWER_NACK => {
            let mut rest = [0u8; 9];
            rest[0..8].copy_from_slice(&e.delivery_tag.to_le_bytes());
            rest[8] = u8::from(answer == store::ANSWER_NACK);
            emit_amqp_reply(s, sys, e.conn_id, 1, e.channel, &rest, Reply::Kept)
        }
        _ => true,
    };
    s.coord_reply = outer_coord;
    if taken {
        store::inflight_free(&mut s.store, ki);
    } else {
        store::inflight_owe(&mut s.store, ki, answer);
        s.answers_owed = s.answers_owed.wrapping_add(1);
    }
    taken
}

/// Answer the inflight slots whose answer is owed, lowest first, until
/// the bus has no room for the next.
///
/// # Safety
/// `sys` must point at a live kernel syscall table.
#[cfg(any(feature = "kafka", feature = "amqp"))]
unsafe fn answer_owed_inflights(s: &mut ModuleState, sys: &SyscallTable) {
    for ki in 0..KAFKA_INFLIGHT {
        let answer = store::inflight_owed(&s.store, ki);
        if answer != store::ANSWER_NONE && !answer_inflight(s, sys, ki, answer) {
            return;
        }
    }
}

/// Answer Kafka inflight `ki` (a copy `e` of its record), expired before
/// all its proposals resolved, as timed out: a produce partition still
/// unresolved answers `REQUEST_TIMED_OUT`, an OffsetCommit's parked
/// answer has every partition failed with it. Answered as
/// [`answer_inflight`] answers (owed when the bus has no room). `false`
/// when `ki` is not a Kafka inflight.
///
/// # Safety
/// `sys` must point at a live kernel syscall table.
#[cfg(feature = "kafka")]
unsafe fn expire_kafka_inflight(
    s: &mut ModuleState,
    sys: &SyscallTable,
    ki: usize,
    e: store::KafkaInflight,
) -> bool {
    match e.proto {
        KIN_PROTO_KAFKA => {
            for pi in 0..usize::from(e.n_parts).min(KIN_MAX_PARTS) {
                if e.part_errs[pi] == KERR_NONE && e.part_offs[pi] < 0 {
                    store::inflight_set_part_err(&mut s.store, ki, pi, KERR_REQUEST_TIMED_OUT);
                }
            }
            s.kafka_produce_errors = s.kafka_produce_errors.wrapping_add(1);
            answer_inflight(s, sys, ki, store::ANSWER_PRODUCE);
            true
        }
        KIN_PROTO_KAFKA_OFFSET => {
            s.kafka_produce_errors = s.kafka_produce_errors.wrapping_add(1);
            let n = store::inflight_response(&s.store, ki).len();
            if n > 0 && n <= s.out_buf.len() {
                s.out_buf[..n].copy_from_slice(store::inflight_response(&s.store, ki));
                if refusal::offset_commit_fail(&mut s.out_buf[..n], KERR_REQUEST_TIMED_OUT)
                    && store::inflight_set_response(&mut s.store, ki, &s.out_buf[..n])
                {
                    answer_inflight(s, sys, ki, store::ANSWER_OFFSET);
                    return true;
                }
            }
            // No parked answer to fail: the request has none, so the
            // codec is told, and closes the connection in its turn.
            store::inflight_free(&mut s.store, ki);
            s.kafka_unanswered = s.kafka_unanswered.wrapping_add(1);
            let outer_coord = s.coord_reply;
            s.coord_reply = e.coord == 1;
            emit_kafka_response(
                s,
                sys,
                e.conn_id,
                wire::KAFKA_API_CLOSE,
                e.api_ver,
                e.kafka_corr,
                &[],
            );
            s.coord_reply = outer_coord;
            true
        }
        _ => false,
    }
}

#[cfg(feature = "kafka")]
/// Build + emit the aggregated ProduceResponse for inflight `ki` (all
/// partitions resolved). Non-flexible encoding, api_version 2..=8.
///
/// # Safety
unsafe fn emit_kafka_produce_response_multi(
    s: &mut ModuleState,
    sys: &SyscallTable,
    ki: usize,
) -> bool {
    let Some(e) = store::inflight_get(&s.store, ki) else {
        return false;
    };
    let tl = e.topic_len as usize;
    let n = e.n_parts as usize;
    let mut p = 0usize;
    s.out_buf[p..p + 4].copy_from_slice(&1i32.to_be_bytes());
    p += 4;
    s.out_buf[p..p + 2].copy_from_slice(&(tl as i16).to_be_bytes());
    p += 2;
    s.out_buf[p..p + tl].copy_from_slice(&e.topic[..tl]);
    p += tl;
    s.out_buf[p..p + 4].copy_from_slice(&(n as i32).to_be_bytes());
    p += 4;
    for pi in 0..n {
        s.out_buf[p..p + 4].copy_from_slice(&e.part_ids[pi].to_be_bytes());
        p += 4;
        s.out_buf[p..p + 2].copy_from_slice(&e.part_errs[pi].to_be_bytes());
        p += 2;
        s.out_buf[p..p + 8].copy_from_slice(&e.part_offs[pi].to_be_bytes());
        p += 8;
        if e.api_ver >= 2 {
            s.out_buf[p..p + 8].copy_from_slice(&(-1i64).to_be_bytes());
            p += 8;
        }
        if e.api_ver >= 5 {
            s.out_buf[p..p + 8].copy_from_slice(&0i64.to_be_bytes());
            p += 8;
        }
    }
    if e.api_ver >= 1 {
        s.out_buf[p..p + 4].copy_from_slice(&0i32.to_be_bytes());
        p += 4;
    }
    kafka_outbuf_reply(
        s,
        sys,
        e.conn_id,
        0,
        e.api_ver,
        e.kafka_corr,
        p,
        Reply::Kept,
    )
}

// ── Apply-side message store ────────────────────────────────────────────────

#[cfg(feature = "kafka")]
/// Replicate one member's identity and assignment so a coordinator
/// change does not cost a rebalance.
///
/// Best-effort: a dropped proposal costs the group a rebalance after
/// the next failover, which is what happens today anyway, so it is
/// counted rather than retried. It must never block SyncGroup.
///
/// # Safety
/// Caller must hold an exclusive `&mut ModuleState` and supply a valid
/// `&SyscallTable`.
unsafe fn propose_group_member(
    s: &mut ModuleState,
    sys: &SyscallTable,
    gi: usize,
    member_id: &[u8],
    assign: &[u8],
) {
    let mut name = [0u8; KG_NAME];
    let Some((nl, _)) = consumers::group_snapshot(&s.consumers, gi, &mut name) else {
        return;
    };
    let ml = member_id.len();
    let al = assign.len();
    if ml == 0 || ml > KG_NAME || al > KG_META {
        return;
    }
    let body_len = 2 + nl + 1 + ml + 2 + al;
    let total = wire::QPROP_UNTAGGED_HDR_LEN + body_len;
    if total > s.out_buf.len() {
        return;
    }
    wire::encode_qprop_header(
        &mut s.out_buf[..wire::QPROP_HEADER_LEN],
        wire::QOP_KAFKA_GROUP_MEMBER,
        0,
        0,
    );
    let mut o = wire::QPROP_UNTAGGED_HDR_LEN;
    s.out_buf[o..o + 2].copy_from_slice(&(nl as u16).to_le_bytes());
    o += 2;
    s.out_buf[o..o + nl].copy_from_slice(&name[..nl]);
    o += nl;
    s.out_buf[o] = ml as u8;
    o += 1;
    s.out_buf[o..o + ml].copy_from_slice(member_id);
    o += ml;
    s.out_buf[o..o + 2].copy_from_slice(&(al as u16).to_le_bytes());
    o += 2;
    s.out_buf[o..o + al].copy_from_slice(assign);
    // Coordinator state rides the group's shard, like the offsets it
    // commits (`handle_kafka_offset_commit`), so a group's records stay
    // on one partition. Routed, not ownership-gated: the member joined
    // HERE, and the record must land whatever this node's placement is.
    let shard = wire::shard_kafka_group(TENANT_UNRESOLVED, &name[..nl]);
    if emit_keyed_unowned(sys, s.out_proposals, &s.view, shard, &s.out_buf[..total]) {
        s.kafka_group_members_proposed = s.kafka_group_members_proposed.wrapping_add(1);
        s.proposals_emitted = s.proposals_emitted.wrapping_add(1);
        s.hb_prop = s.hb_prop.wrapping_add(1);
    }
}

#[cfg(feature = "kafka")]
/// Replicate any group generation that has advanced since we last
/// proposed it.
///
/// A SWEEP rather than an emission at each bump site, because the
/// generation moves on three separate paths — join, leave, and the bulk
/// `release_conn` that reclaims members of a dropped connection — and
/// one of them lives inside `consumers` where there is no syscall table
/// to propose from. Reconciling in one place cannot miss a path that a
/// future fourth mutation site would also have to remember.
///
/// Safe to repeat: apply raises the generation to a MAXIMUM, so a
/// duplicate or out-of-order record converges on the same value.
///
/// # Safety
/// Caller must hold an exclusive `&mut ModuleState` and supply a valid
/// `&SyscallTable`.
unsafe fn reconcile_group_generations(s: &mut ModuleState, sys: &SyscallTable) {
    for gi in 0..KGROUPS {
        let mut name = [0u8; KG_NAME];
        let Some((nl, generation)) = consumers::group_snapshot(&s.consumers, gi, &mut name) else {
            continue;
        };
        if generation <= s.gen_proposed[gi] {
            continue;
        }
        let body_len = 2 + nl + 4;
        let total = wire::QPROP_UNTAGGED_HDR_LEN + body_len;
        if total > s.out_buf.len() {
            continue;
        }
        wire::encode_qprop_header(
            &mut s.out_buf[..wire::QPROP_HEADER_LEN],
            wire::QOP_KAFKA_GROUP_GEN,
            0,
            0,
        );
        let ob = wire::QPROP_UNTAGGED_HDR_LEN;
        s.out_buf[ob..ob + 2].copy_from_slice(&(nl as u16).to_le_bytes());
        s.out_buf[ob + 2..ob + 2 + nl].copy_from_slice(&name[..nl]);
        s.out_buf[ob + 2 + nl..ob + 2 + nl + 4].copy_from_slice(&generation.to_le_bytes());
        // Keyed to the group's shard, as every other group record is.
        let shard = wire::shard_kafka_group(TENANT_UNRESOLVED, &name[..nl]);
        if emit_keyed_unowned(sys, s.out_proposals, &s.view, shard, &s.out_buf[..total]) {
            // Record only on a successful write: a dropped proposal must
            // be retried, or the generation this coordinator issued is
            // never replicated and a successor could reissue it.
            s.gen_proposed[gi] = generation;
            s.kafka_group_gen_proposed = s.kafka_group_gen_proposed.wrapping_add(1);
            s.proposals_emitted = s.proposals_emitted.wrapping_add(1);
            s.hb_prop = s.hb_prop.wrapping_add(1);
        }
    }
}

/// Apply-side QOP_KAFKA_PRODUCE: store the committed record batches and
/// patch each baseOffset to the partition's contiguous logical offset
/// (bytes 0..8 of a v2 batch sit OUTSIDE the batch CRC, so the patch is
/// safe). Op-body: `wire::QOP_KAFKA_PRODUCE`'s
/// ([`wire::decode_produce_body_hdr`]), then the records.
///
/// `records` may hold SEVERAL v2 batches concatenated (a producer that
/// accumulates multiple record batches for one partition sends them
/// back-to-back); each is stored as its own entry with its own
/// contiguous baseOffset so consumer offset accounting stays monotonic.
#[cfg(feature = "kafka")]
unsafe fn apply_kafka_produce(
    s: &mut ModuleState,
    sys: &SyscallTable,
    body: &[u8],
    session_slot: u32,
) {
    let Some(h) = wire::decode_produce_body_hdr(body) else {
        return;
    };
    let (origin, origin_worker, partition, tl) =
        (h.origin, h.origin_worker, h.partition, h.topic_len);
    if tl == 0 || tl > KAFKA_MAX_TOPIC || h.records_at >= body.len() {
        return;
    }
    let mut topic = [0u8; KAFKA_MAX_TOPIC];
    topic[..tl].copy_from_slice(&body[wire::PRODUCE_BODY_HDR..h.records_at]);
    let records = &body[h.records_at..];

    // Resolve the client waiting on this batch, if it is ours. The
    // origin check is what makes this safe on a follower and on a
    // co-located worker, both of which apply the entry: a replica — or a
    // worker — that did not accept the produce never touches an inflight
    // slot, so it cannot stamp an offset onto an unrelated client's
    // request. `inflight_valid` then rejects a slot that has been freed
    // and reused since (EPOCH TAGGING, the epoch naming the worker too),
    // and the topic/partition check rejects a slot whose occupant is a
    // different request.
    let mut waiting: Option<(usize, usize)> = None;
    if origin != wire::PRODUCE_ORIGIN_NONE && origin == s.self_id && origin_worker == s.worker_id {
        let (ki, epoch) = store::kin_decode_slot(session_slot);
        if ki < KAFKA_INFLIGHT && store::inflight_valid(&s.store, ki, epoch) {
            if let Some(e) = store::inflight_get(&s.store, ki) {
                if e.topic_len as usize == tl && e.topic[..tl] == topic[..tl] {
                    let n = e.n_parts as usize;
                    for pidx in 0..n.min(KIN_MAX_PARTS) {
                        if e.part_ids[pidx] == partition as i32 {
                            waiting = Some((ki, pidx));
                            break;
                        }
                    }
                }
            }
        }
    }
    let Some(pi) = store::find_or_create(&mut s.store, &topic[..tl], partition) else {
        s.store.full = s.store.full.wrapping_add(1);
        return;
    };

    // Walk concatenated v2 batches: baseOffset(8) batchLength(4)
    // [leaderEpoch(4) magic(1) crc(4) attrs(2) lastOffsetDelta(4 @23)...].
    // A batch spans 12 + batchLength bytes.
    let mut bo = 0usize;
    let mut guard = 0u32;
    // `base_offset` is where the REQUEST's data begins, so only the
    // first accepted batch of this op-body sets it.
    let mut stamped = false;
    while bo + 61 <= records.len() && guard < 256 {
        guard += 1;
        let batch_len = i32::from_be_bytes([
            records[bo + 8],
            records[bo + 9],
            records[bo + 10],
            records[bo + 11],
        ]);
        if batch_len < 49 {
            break;
        }
        let total = 12 + batch_len as usize;
        if bo + total > records.len() {
            break;
        }
        let nrec_minus1 = i32::from_be_bytes([
            records[bo + 23],
            records[bo + 24],
            records[bo + 25],
            records[bo + 26],
        ]);
        if !(0..=65535).contains(&nrec_minus1) {
            break;
        }
        let nrec = (nrec_minus1 + 1) as u32;

        // Idempotence gate. A v2 batch carries the producer identity and
        // the sequence range it covers:
        //   producerId @43 (i64), producerEpoch @51 (i16),
        //   baseSequence @53 (i32); lastOffsetDelta @23 spans the range.
        // Deciding here — on the committed entry, on every replica —
        // is what makes the verdict survive failover: the propose side
        // would have to guess, because any node may accept a produce.
        let producer_id = i64::from_be_bytes([
            records[bo + 43],
            records[bo + 44],
            records[bo + 45],
            records[bo + 46],
            records[bo + 47],
            records[bo + 48],
            records[bo + 49],
            records[bo + 50],
        ]);
        let producer_epoch = i16::from_be_bytes([records[bo + 51], records[bo + 52]]);
        let base_seq = i32::from_be_bytes([
            records[bo + 53],
            records[bo + 54],
            records[bo + 55],
            records[bo + 56],
        ]);
        let last_seq = base_seq.saturating_add(nrec_minus1);
        let verdict = s.idem.classify(
            producer_id,
            producer_epoch,
            partition as u32,
            base_seq,
            last_seq,
        );
        match verdict {
            kafka_idem::IdemVerdict::Duplicate { base_offset } => {
                // The producer retried a batch we already have. NOT
                // appending it is the entire point: this is the case
                // that silently duplicated data before. Acknowledge it
                // with the offset the ORIGINAL landed at — answering a
                // different one would make a retrying producer record
                // the wrong position.
                s.kafka_idem_duplicates = s.kafka_idem_duplicates.wrapping_add(1);
                if let Some((ki, pidx)) = waiting {
                    if !stamped {
                        store::inflight_set_part_offset(&mut s.store, ki, pidx, base_offset);
                        stamped = true;
                    }
                }
                bo += total;
                continue;
            }
            kafka_idem::IdemVerdict::Reject(code) => {
                // A gap or a fenced epoch. Appending would either leave
                // an unfillable hole or accept a zombie's writes. Tell
                // the producer WHICH, so its driver can act: an
                // OUT_OF_ORDER_SEQUENCE_NUMBER is retriable after a
                // reset, an INVALID_PRODUCER_EPOCH is fatal for that
                // producer instance. Reporting success here would let it
                // believe data landed that never did.
                s.kafka_idem_rejected = s.kafka_idem_rejected.wrapping_add(1);
                if let Some((ki, pidx)) = waiting {
                    store::inflight_set_part_err(&mut s.store, ki, pidx, code);
                }
                bo += total;
                continue;
            }
            kafka_idem::IdemVerdict::Accept => {}
        }

        // `records` aliases s.in_buf; store::push writes only the store.
        let rec = unsafe { core::slice::from_raw_parts(records.as_ptr().add(bo), total) };
        if let Some((offset, data_pos)) =
            store::push(&mut s.store, pi, KFLAG_BATCH, nrec, rec, dev_millis(sys))
        {
            store::stamp_base_offset(&mut s.store, pi, data_pos, offset);
            // Anchor this offset to the raft entry carrying it, so a
            // Fetch that has fallen behind the in-memory ring can still
            // be located in the WAL — the ring is a cache of the raft
            // log, not the log itself. Sparse: the index ignores most
            // calls, so this is cheap on every append.
            // The anchor names the raft partition too: indexes from
            // different partitions are NOT comparable — each log numbers
            // from 1 — so a cold read must ask that partition's WAL and
            // a retention floor must be published to that partition.
            store::note_offset_anchor(
                &mut s.store,
                pi,
                offset,
                s.applying_index,
                s.applying_partition,
            );
            s.store.batches_applied = s.store.batches_applied.wrapping_add(1);
            // Report the partition's LOGICAL offset, which is only
            // known here. The propose side can only see the raft WAL
            // index, which is unrelated to the offset a consumer will
            // fetch. Stamp the FIRST batch's offset: `base_offset` names where the request's
            // data starts, so later batches in the same request must
            // not overwrite it.
            if let Some((ki, pidx)) = waiting {
                if !stamped {
                    store::inflight_set_part_offset(&mut s.store, ki, pidx, offset as i64);
                    stamped = true;
                }
            }
            // Record ONLY after the append landed. Recording an append
            // that failed would make the producer's retry look like a
            // duplicate and be dropped without ever being stored.
            s.idem.commit(
                producer_id,
                producer_epoch,
                partition as u32,
                last_seq,
                offset as i64,
            );
        }
        bo += total;
    }

    // This partition's apply has landed. Durability and apply are
    // independent post-commit signals with no ordering between them —
    // measured: the ack routinely arrives FIRST — so whichever completes
    // last releases the response. Gating on durability alone reported an
    // offset apply had not yet assigned. A partition already failed
    // (refused or superseded, `fail_kin`) counted its apply then; an
    // `OUTCOME_UNKNOWN` one may still commit and land here, and counts
    // nothing twice.
    if let Some((ki, pidx)) = waiting {
        store::inflight_apply_part(&mut s.store, ki, pidx);
        if store::inflight_ready(&s.store, ki)
            && store::inflight_owed(&s.store, ki) == store::ANSWER_NONE
            && answer_inflight(s, sys, ki, store::ANSWER_PRODUCE)
        {
            s.kafka_produce_acked = s.kafka_produce_acked.wrapping_add(1);
            s.acks_emitted = s.acks_emitted.wrapping_add(1);
        }
    }
}

#[cfg(feature = "amqp")]
/// Apply-side QOP_AMQP_PUBLISH: store the raw message body on the
/// routing key's log (partition 0, raw-flagged — invisible to Kafka
/// Fetch). Op-body: [rk_len:u16 LE][routing_key][payload].
fn apply_amqp_publish(s: &mut ModuleState, body: &[u8], now_ms: u64) {
    if body.len() < 2 {
        return;
    }
    let rl = u16::from_le_bytes([body[0], body[1]]) as usize;
    if rl == 0 || rl > KAFKA_MAX_TOPIC || 2 + rl > body.len() {
        return;
    }
    let mut rk = [0u8; KAFKA_MAX_TOPIC];
    rk[..rl].copy_from_slice(&body[2..2 + rl]);
    let payload = &body[2 + rl..];
    let Some(pi) = store::find_or_create(&mut s.store, &rk[..rl], 0) else {
        return;
    };
    let pl = payload.len();
    let pp = payload.as_ptr();
    let pay = unsafe { core::slice::from_raw_parts(pp, pl) };
    let _ = store::push(&mut s.store, pi, 0, 1, pay, now_ms);
}

// ── Kafka Fetch / ListOffsets ───────────────────────────────────────────────

#[cfg(feature = "kafka")]
/// Emit a Kafka MSG_SESSION_RESPONSE whose body was built in
/// `s.out_buf[..body_len]` (the 256-byte stack path in
/// `emit_kafka_response` is too small for Fetch payloads).
///
/// # Safety
unsafe fn emit_kafka_response_outbuf(
    s: &mut ModuleState,
    sys: &SyscallTable,
    conn_id: u16,
    api_key: i16,
    api_ver: i16,
    kafka_corr: i32,
    body_len: usize,
) -> bool {
    kafka_outbuf_reply(
        s,
        sys,
        conn_id,
        api_key,
        api_ver,
        kafka_corr,
        body_len,
        Reply::Must,
    )
}

#[cfg(feature = "kafka")]
/// [`emit_kafka_response_outbuf`], met as `reply` says when the bus has
/// no room for it.
///
/// # Safety
/// `sys` must point at a live kernel syscall table.
#[allow(
    clippy::too_many_arguments,
    reason = "the envelope's fields and how a refusal is met"
)]
unsafe fn kafka_outbuf_reply(
    s: &mut ModuleState,
    sys: &SyscallTable,
    conn_id: u16,
    api_key: i16,
    api_ver: i16,
    kafka_corr: i32,
    body_len: usize,
    reply: Reply,
) -> bool {
    let total = KAFKA_ENV_HDR + body_len;
    if total > BUF_SIZE {
        return false;
    }
    // Shift the body up to make room for the header (memmove).
    core::ptr::copy(
        s.out_buf.as_ptr(),
        s.out_buf.as_mut_ptr().add(KAFKA_ENV_HDR),
        body_len,
    );
    write_kafka_env_header(&mut s.out_buf, conn_id, api_key, api_ver, kafka_corr);
    let mut out = [0u8; BUF_SIZE];
    out[..total].copy_from_slice(&s.out_buf[..total]);
    let chan = s.out_codec;
    // Counted, as every response the anchor relays (`emit_kafka_response`).
    let taken = respond(s, sys, chan, conn_id, &out[..total], reply, true).taken();
    s.kafka_settled |= taken;
    taken
}

/// Try to serve a Fetch from the WAL because its offset has aged out of
/// the in-memory ring. Returns true when the request has been PARKED —
/// the caller must then emit nothing, because the answer arrives on a
/// later step.
///
/// Restricted to a single topic and a single partition. A Fetch can
/// cover many partitions and the response is built as one frame, so
/// parking one partition would park the whole reply; the simple
/// consumer case is one partition, and every other shape keeps exactly
/// today's behaviour.
///
/// The Kafka log IS the raft log, so a record evicted from the ring is
/// still durable — what was missing was the way back from a Kafka
/// OFFSET to a raft INDEX, which `store::seek_offset` now provides.
#[cfg(feature = "kafka")]
#[allow(
    clippy::too_many_arguments,
    reason = "the parked request IS this argument list — conn, api version, \
              correlation, topic, partition and offset must all be carried to \
              a later step to answer on. Bundling them into a struct would \
              just move the same fields behind a name the caller has to build"
)]
unsafe fn try_park_cold_fetch(
    s: &mut ModuleState,
    sys: &SyscallTable,
    conn_id: u16,
    api_ver: i16,
    corr: i32,
    topic: &[u8],
    partition: i32,
    fetch_offset: i64,
    pi: usize,
    now: u64,
) -> bool {
    if s.out_wal_request < 0 || s.in_wal_reply < 0 || fetch_offset < 0 {
        return false;
    }
    // Never serve below the RETENTION floor. An offset can be under
    // `log_start` for two opposite reasons: evicted for capacity (still
    // on disk, fair game) or deleted by a retention/compaction policy
    // (gone, and a broker that serves it has not deleted it). Only the
    // first is a cold read.
    if (fetch_offset as u64) < store::retention_floor(&s.store, pi) {
        return false;
    }
    // Only worth a WAL read if the index can actually locate it. Below
    // the index floor the answer is still OFFSET_OUT_OF_RANGE.
    let Some((raft_partition, raft_index)) = store::seek_offset(&s.store, pi, fetch_offset as u64)
    else {
        return false;
    };
    let Some(slot) = (0..COLD_FETCH_SLOTS).find(|&i| s.cold[i].active == 0) else {
        // All slots busy: answer from the ring's rules rather than
        // queueing. A cold read is the slow path and must not become an
        // unbounded backlog.
        return false;
    };
    let request_id = s.next_cold_id;
    s.next_cold_id =
        cold_id_base(s.worker_id) | (s.next_cold_id.wrapping_add(1) & COLD_ID_MASK).max(1);

    let mut buf = [0u8; wire::WAL_ENTRY_REQUEST_LEN];
    wire::encode_wal_entry_request(&mut buf, request_id, raft_index);
    // Partitioned envelope: one durability instance hosts every raft
    // partition's WAL, and only the partition the anchor names holds
    // this index.
    if wire::channel_write_partitioned(
        sys,
        s.out_wal_request,
        raft_partition,
        wire::MSG_WAL_ENTRY_REQUEST,
        &buf,
    ) <= 0
    {
        return false;
    }

    let tl = topic.len().min(KAFKA_MAX_TOPIC);
    let c = &mut s.cold[slot];
    *c = ColdFetch::zero();
    c.active = 1;
    c.conn_id = conn_id;
    c.api_ver = api_ver;
    c.corr = corr;
    c.partition = partition;
    c.fetch_offset = fetch_offset;
    c.request_id = request_id;
    c.issued_ms = now;
    c.topic_len = tl as u8;
    c.part_idx = pi as u8;
    c.topic[..tl].copy_from_slice(&topic[..tl]);
    // Answered from the cold slot, by the WAL's reply or its timeout.
    s.kafka_settled = true;
    true
}

/// Emit a single-partition Fetch response. Used by the cold path, whose
/// answer is built on a later step than the request.
#[cfg(feature = "kafka")]
unsafe fn emit_cold_fetch_response(
    s: &mut ModuleState,
    sys: &SyscallTable,
    slot: usize,
    error: i16,
    hw: i64,
    log_start: i64,
    records: &[u8],
) -> bool {
    let c = s.cold[slot];
    let tl = c.topic_len as usize;
    let mut p = 0usize;
    if c.api_ver >= 1 {
        s.out_buf[p..p + 4].copy_from_slice(&0i32.to_be_bytes());
        p += 4; // throttle
    }
    s.out_buf[p..p + 4].copy_from_slice(&1i32.to_be_bytes());
    p += 4; // one topic
    s.out_buf[p..p + 2].copy_from_slice(&(tl as i16).to_be_bytes());
    p += 2;
    s.out_buf[p..p + tl].copy_from_slice(&c.topic[..tl]);
    p += tl;
    s.out_buf[p..p + 4].copy_from_slice(&1i32.to_be_bytes());
    p += 4; // one partition
    s.out_buf[p..p + 4].copy_from_slice(&c.partition.to_be_bytes());
    p += 4;
    s.out_buf[p..p + 2].copy_from_slice(&error.to_be_bytes());
    p += 2;
    s.out_buf[p..p + 8].copy_from_slice(&hw.to_be_bytes());
    p += 8;
    if c.api_ver >= 4 {
        s.out_buf[p..p + 8].copy_from_slice(&hw.to_be_bytes());
        p += 8; // LSO
    }
    if c.api_ver >= 5 {
        s.out_buf[p..p + 8].copy_from_slice(&log_start.to_be_bytes());
        p += 8;
    }
    if c.api_ver >= 4 {
        s.out_buf[p..p + 4].copy_from_slice(&0i32.to_be_bytes());
        p += 4; // aborted
    }
    let n = records.len().min(s.out_buf.len().saturating_sub(p + 4));
    s.out_buf[p..p + 4].copy_from_slice(&(n as i32).to_be_bytes());
    p += 4;
    if n > 0 {
        s.out_buf[p..p + n].copy_from_slice(&records[..n]);
        p += n;
    }
    // Answered from a reply that cannot wait: held while there is room,
    // else the slot is kept with its answer owed.
    if !kafka_outbuf_reply(s, sys, c.conn_id, 1, c.api_ver, c.corr, p, Reply::Kept) {
        return false;
    }
    s.cold[slot].active = 0;
    true
}

/// Bounds on how much of a Fetch/ListOffsets request we service in one
/// response. A real assignment is a handful of topics × ≤16 partitions;
/// excess entries are parsed-and-skipped, not answered (documented gap).
/// `session_slot` in a `MSG_TOPIC_SUBSCRIBE` record when the subscriber's
/// session lives on ANOTHER PRG. The topic's owner still records the
/// subscription — it is the node that matches publishes — and addresses
/// the subscriber by `stream_hash` instead.
const SESSION_SLOT_REMOTE: u32 = u32::MAX;

const KFETCH_MAX_TOPICS: usize = 8;
const KFETCH_MAX_PARTS: usize = 16;

#[cfg(feature = "kafka")]
/// Handle a Kafka Fetch request (api_key 1, v0-v5 non-flexible). Serves
/// stored batches for EVERY requested topic-partition up to the response
/// budget; no long-poll (max_wait ignored — an empty response returns
/// immediately and the client re-polls).
///
/// # Safety
/// Caller must hold an exclusive `&mut ModuleState` and supply a valid
/// `&SyscallTable` per the module ABI.
unsafe fn handle_kafka_fetch(s: &mut ModuleState, sys: &SyscallTable, plen: usize) {
    let (conn_id, api_ver, kafka_corr) = kafka_env_header(&s.in_buf);
    let end = plen;
    s.kafka_fetch_rx = s.kafka_fetch_rx.wrapping_add(1);

    // replica_id(4) max_wait(4) min_bytes(4) [v3+: max_bytes(4)]
    // [v4+: isolation(1)] topics(4) ...
    let mut off = KAFKA_ENV_HDR + 12;
    if api_ver >= 3 {
        off += 4;
    }
    if api_ver >= 4 {
        off += 1;
    }
    if off + 4 > end {
        return;
    }
    let topic_count = i32::from_be_bytes([
        s.in_buf[off],
        s.in_buf[off + 1],
        s.in_buf[off + 2],
        s.in_buf[off + 3],
    ]);
    off += 4;
    if topic_count < 1 {
        return;
    }

    let budget_end = 7600usize.min(BUF_SIZE - 16);
    let mut p = 0usize;
    if api_ver >= 1 {
        s.out_buf[p..p + 4].copy_from_slice(&0i32.to_be_bytes());
        p += 4; // throttle
    }
    let resp_topic_count = topic_count.min(KFETCH_MAX_TOPICS as i32);
    s.out_buf[p..p + 4].copy_from_slice(&resp_topic_count.to_be_bytes());
    p += 4;

    for _ in 0..resp_topic_count {
        if off + 2 > end {
            break;
        }
        let tl = i16::from_be_bytes([s.in_buf[off], s.in_buf[off + 1]]) as usize;
        off += 2;
        if tl == 0 || tl > KAFKA_MAX_TOPIC || off + tl > end {
            return;
        }
        let mut topic = [0u8; KAFKA_MAX_TOPIC];
        topic[..tl].copy_from_slice(&s.in_buf[off..off + tl]);
        off += tl;
        if off + 4 > end {
            return;
        }
        let part_count = i32::from_be_bytes([
            s.in_buf[off],
            s.in_buf[off + 1],
            s.in_buf[off + 2],
            s.in_buf[off + 3],
        ]);
        off += 4;
        let resp_part_count = part_count.clamp(0, KFETCH_MAX_PARTS as i32);

        if p + 2 + tl + 8 > BUF_SIZE {
            return;
        }
        s.out_buf[p..p + 2].copy_from_slice(&(tl as i16).to_be_bytes());
        p += 2;
        s.out_buf[p..p + tl].copy_from_slice(&topic[..tl]);
        p += tl;
        s.out_buf[p..p + 4].copy_from_slice(&resp_part_count.to_be_bytes());
        p += 4;

        for _ in 0..part_count {
            // partition(4) fetch_offset(8) [v5+: log_start(8)] max_bytes(4)
            let mut need = 16;
            if api_ver >= 5 {
                need += 8;
            }
            if off + need > end {
                return;
            }
            let partition = i32::from_be_bytes([
                s.in_buf[off],
                s.in_buf[off + 1],
                s.in_buf[off + 2],
                s.in_buf[off + 3],
            ]);
            let fetch_offset = i64::from_be_bytes([
                s.in_buf[off + 4],
                s.in_buf[off + 5],
                s.in_buf[off + 6],
                s.in_buf[off + 7],
                s.in_buf[off + 8],
                s.in_buf[off + 9],
                s.in_buf[off + 10],
                s.in_buf[off + 11],
            ]);
            off += need;

            // Only the first resp_part_count partitions are answered; the
            // rest are parsed (to stay frame-aligned) but skipped.
            if p >= budget_end {
                continue;
            }

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
            let fixed = 4
                + 2
                + 8
                + if api_ver >= 4 { 8 } else { 0 }
                + if api_ver >= 5 { 8 } else { 0 }
                + if api_ver >= 4 { 4 } else { 0 }
                + 4;
            if p + fixed > BUF_SIZE {
                return;
            }
            // Offset validity, BEFORE claiming success.
            //
            // The apply-side store is a bounded ring that evicts its
            // oldest entries when full, so `log_start` advances past
            // offsets a slow consumer has not read yet. Answering
            // `error = 0` with no records tells that consumer it is
            // CAUGHT UP: it skips the evicted range silently and never
            // learns it lost data. Kafka's contract for this is
            // OFFSET_OUT_OF_RANGE, which is what drives the client's
            // `auto.offset.reset`.
            //
            // Same test above the high watermark: an offset past the
            // end is out of range, not "nothing yet".
            // (`kafka_log_core::Watermarks::readable_at` is the same
            // predicate, stated once for the durable log.)
            // Ownership FIRST, before the watermarks are trusted.
            //
            // This node's `hw` and `log_start` describe the log it
            // holds. For a partition whose shard has moved away, that
            // state has been released, so both read 0 and every arm
            // below concludes `KERR_NONE` with no records — which tells
            // the consumer it is CAUGHT UP on a partition it has not
            // read. That is the same silent-skip failure the
            // out-of-range test exists to prevent, except the consumer
            // skips the whole partition rather than an evicted range.
            // Answering with the placement disposition instead sends it
            // to the broker that has the data.
            let shard = wire::shard_kafka(0, &topic[..tl], partition as u32);
            let partition_error = if let Some(e) = kafka_shard_error(&s.view, shard) {
                s.kafka_fetch_not_owned = s.kafka_fetch_not_owned.wrapping_add(1);
                e
            } else if pi.is_none() {
                KERR_NONE // unknown topic/partition answers empty
            } else if fetch_offset < log_start {
                // Below the ring — but the record may still be in the
                // WAL, which is the same log. Park the request and
                // answer from disk if the offset index can locate it;
                // only a single-partition Fetch qualifies, because the
                // response is one frame.
                if topic_count == 1
                    && part_count == 1
                    && try_park_cold_fetch(
                        s,
                        sys,
                        conn_id,
                        api_ver,
                        kafka_corr,
                        &topic[..tl],
                        partition,
                        fetch_offset,
                        pi.unwrap_or(0),
                        dev_millis(sys),
                    )
                {
                    return;
                }
                s.kafka_fetch_out_of_range = s.kafka_fetch_out_of_range.wrapping_add(1);
                KERR_OFFSET_OUT_OF_RANGE
            } else if fetch_offset > hw {
                s.kafka_fetch_out_of_range = s.kafka_fetch_out_of_range.wrapping_add(1);
                KERR_OFFSET_OUT_OF_RANGE
            } else {
                KERR_NONE
            };
            s.out_buf[p..p + 4].copy_from_slice(&partition.to_be_bytes());
            p += 4;
            s.out_buf[p..p + 2].copy_from_slice(&partition_error.to_be_bytes());
            p += 2;
            s.out_buf[p..p + 8].copy_from_slice(&hw.to_be_bytes());
            p += 8;
            if api_ver >= 4 {
                s.out_buf[p..p + 8].copy_from_slice(&hw.to_be_bytes());
                p += 8; // LSO
            }
            if api_ver >= 5 {
                s.out_buf[p..p + 8].copy_from_slice(&log_start.to_be_bytes());
                p += 8;
            }
            if api_ver >= 4 {
                s.out_buf[p..p + 4].copy_from_slice(&0i32.to_be_bytes());
                p += 4; // aborted
            }
            let records_len_pos = p;
            p += 4;
            // An out-of-range fetch returns no records — the error is
            // the answer. Serving from `log_start` instead would hand
            // the consumer a silent jump over the evicted range.
            let rb = match pi {
                Some(pi) if partition_error == KERR_NONE => {
                    store::fetch_into(&s.store, pi, fetch_offset, &mut s.out_buf, p, budget_end)
                }
                _ => 0,
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
    let (conn_id, api_ver, kafka_corr) = kafka_env_header(&s.in_buf);
    let end = plen;
    // replica_id(4) [v2+: isolation(1)] topics(4) [name partitions(4)
    //   [partition(4) timestamp(8) [v0: max_num_offsets(4)]]]
    let mut off = KAFKA_ENV_HDR + 4;
    if api_ver >= 2 {
        off += 1;
    }
    if off + 4 > end {
        return;
    }
    let topic_count = i32::from_be_bytes([
        s.in_buf[off],
        s.in_buf[off + 1],
        s.in_buf[off + 2],
        s.in_buf[off + 3],
    ]);
    off += 4;
    if topic_count < 1 {
        return;
    }

    let mut p = 0usize;
    if api_ver >= 2 {
        s.out_buf[p..p + 4].copy_from_slice(&0i32.to_be_bytes());
        p += 4; // throttle
    }
    let resp_topics = topic_count.min(KFETCH_MAX_TOPICS as i32);
    s.out_buf[p..p + 4].copy_from_slice(&resp_topics.to_be_bytes());
    p += 4;

    for _ in 0..resp_topics {
        if off + 2 > end {
            return;
        }
        let tl = i16::from_be_bytes([s.in_buf[off], s.in_buf[off + 1]]) as usize;
        off += 2;
        if tl == 0 || tl > KAFKA_MAX_TOPIC || off + tl > end {
            return;
        }
        let mut topic = [0u8; KAFKA_MAX_TOPIC];
        topic[..tl].copy_from_slice(&s.in_buf[off..off + tl]);
        off += tl;
        if off + 4 > end {
            return;
        }
        let part_count = i32::from_be_bytes([
            s.in_buf[off],
            s.in_buf[off + 1],
            s.in_buf[off + 2],
            s.in_buf[off + 3],
        ]);
        off += 4;
        let resp_parts = part_count.clamp(0, KFETCH_MAX_PARTS as i32);

        if p + 2 + tl + 4 > BUF_SIZE {
            return;
        }
        s.out_buf[p..p + 2].copy_from_slice(&(tl as i16).to_be_bytes());
        p += 2;
        s.out_buf[p..p + tl].copy_from_slice(&topic[..tl]);
        p += tl;
        s.out_buf[p..p + 4].copy_from_slice(&resp_parts.to_be_bytes());
        p += 4;

        for pi_idx in 0..part_count {
            let need = 12 + if api_ver == 0 { 4 } else { 0 };
            if off + need > end {
                return;
            }
            let partition = i32::from_be_bytes([
                s.in_buf[off],
                s.in_buf[off + 1],
                s.in_buf[off + 2],
                s.in_buf[off + 3],
            ]);
            let timestamp = i64::from_be_bytes([
                s.in_buf[off + 4],
                s.in_buf[off + 5],
                s.in_buf[off + 6],
                s.in_buf[off + 7],
                s.in_buf[off + 8],
                s.in_buf[off + 9],
                s.in_buf[off + 10],
                s.in_buf[off + 11],
            ]);
            off += need;
            if pi_idx >= resp_parts {
                continue;
            }

            let (hw, log_start) = match store::find(&s.store, &topic[..tl], partition as u16) {
                Some(i) => (
                    store::next_offset(&s.store, i) as i64,
                    store::log_start(&s.store, i) as i64,
                ),
                None => (0i64, 0i64),
            };
            let offset = if timestamp == -2 { log_start } else { hw };

            let fixed = 4 + 2 + if api_ver == 0 { 4 + 8 } else { 8 + 8 };
            if p + fixed > BUF_SIZE {
                return;
            }
            s.out_buf[p..p + 4].copy_from_slice(&partition.to_be_bytes());
            p += 4;
            s.out_buf[p..p + 2].copy_from_slice(&0i16.to_be_bytes());
            p += 2;
            if api_ver == 0 {
                s.out_buf[p..p + 4].copy_from_slice(&1i32.to_be_bytes());
                p += 4; // 1 offset
                s.out_buf[p..p + 8].copy_from_slice(&offset.to_be_bytes());
                p += 8;
            } else {
                s.out_buf[p..p + 8].copy_from_slice(&(-1i64).to_be_bytes());
                p += 8; // ts
                s.out_buf[p..p + 8].copy_from_slice(&offset.to_be_bytes());
                p += 8;
            }
        }
    }
    emit_kafka_response_outbuf(s, sys, conn_id, 2, api_ver, kafka_corr, p);
}

// ── AMQP publish / Basic.Get ────────────────────────────────────────────────

#[cfg(feature = "amqp")]
/// Emit an AMQP MSG_SESSION_RESPONSE. Payload:
///   [conn_id:u16 LE][proto=2][op][channel:u16 LE][rest...]
///
/// # Safety
unsafe fn emit_amqp_response(
    s: &mut ModuleState,
    sys: &SyscallTable,
    conn_id: u16,
    op: u8,
    channel: u16,
    rest: &[u8],
) -> bool {
    emit_amqp_reply(s, sys, conn_id, op, channel, rest, Reply::Must)
}

/// [`emit_amqp_response`] for work the caller keeps and offers again
/// ([`Reply::Offer`]).
///
/// # Safety
/// `sys` must point at a live kernel syscall table.
#[cfg(feature = "amqp")]
unsafe fn offer_amqp_response(
    s: &mut ModuleState,
    sys: &SyscallTable,
    conn_id: u16,
    op: u8,
    channel: u16,
    rest: &[u8],
) -> bool {
    emit_amqp_reply(s, sys, conn_id, op, channel, rest, Reply::Offer)
}

/// # Safety
/// `sys` must point at a live kernel syscall table.
#[allow(
    clippy::too_many_arguments,
    reason = "the envelope's fields and how a refusal is met; the two callers above are the API"
)]
unsafe fn emit_amqp_reply(
    s: &mut ModuleState,
    sys: &SyscallTable,
    conn_id: u16,
    op: u8,
    channel: u16,
    rest: &[u8],
    reply: Reply,
) -> bool {
    let chan = s.out_codec;
    if chan < 0 {
        return false;
    }
    let total = AMQP_ENV_HDR + rest.len();
    let mut out = [0u8; 2200];
    if total > out.len() {
        return false;
    }
    out[0..2].copy_from_slice(&conn_id.to_le_bytes());
    out[2] = 2; // PROTO_AMQP envelope discriminator
    out[3] = op;
    out[4..6].copy_from_slice(&channel.to_le_bytes());
    out[AMQP_ENV_HDR..total].copy_from_slice(rest);
    // Counted, as every response the anchor relays (`emit_kafka_response`).
    respond(s, sys, chan, conn_id, &out[..total], reply, true).taken()
}

#[cfg(feature = "amqp")]
/// AMQP publish (op 1) from `protocol::amqp`:
///   [conn:u16 LE][2][1][channel:u16 LE][delivery_tag:u64 LE][rk_len:u16 LE][rk][body]
/// delivery_tag != 0 → confirm mode: tagged proposal, Basic.Ack gated on
/// quorum durability. delivery_tag == 0 → untagged fire-and-forget
/// (still durably logged; no confirm to route).
///
/// # Safety
unsafe fn handle_amqp_publish(s: &mut ModuleState, sys: &SyscallTable, now: u64, plen: usize) {
    const RK: usize = AMQP_ENV_PREFIX + 2;
    if plen < RK {
        return;
    }
    let (conn_id, channel) = amqp_env_header(&s.in_buf);
    let delivery_tag = amqp_env_delivery_tag(&s.in_buf);
    let rl =
        u16::from_le_bytes([s.in_buf[AMQP_ENV_PREFIX], s.in_buf[AMQP_ENV_PREFIX + 1]]) as usize;
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
            emit_amqp_response(s, sys, conn_id, 1, channel, &rest);
        }
    };

    // An empty or oversdized routing key can't be stored (the queue log
    // is keyed by routing key). Reject with a Nack instead of a silent
    // black-hole so a confirm-mode publisher unblocks.
    if rl == 0 || rl > KAFKA_MAX_TOPIC || RK + rl > plen {
        s.amqp_publish_errors = s.amqp_publish_errors.wrapping_add(1);
        nack(s, sys);
        return;
    }
    let body_len = plen - RK - rl;

    let op_body_len = 2 + rl + body_len;
    if op_body_len + 2 > KAFKA_MAX_RECORDS_BYTES + 4 {
        s.amqp_publish_errors = s.amqp_publish_errors.wrapping_add(1);
        nack(s, sys);
        return;
    }

    // Placement. AMQP 0-9-1 has no redirect — `Connection.Redirect`
    // existed in 0-8 and was removed — so unlike Kafka and MQTT there
    // is nothing to point the client AT. What the protocol does have is
    // a negative confirm, and the honest answer for a queue served by
    // another node is to use it: a confirm-mode publisher then knows
    // its message was not stored instead of blocking on a confirm that
    // will never come. A publisher NOT in confirm mode gets nothing,
    // which is inherent to the protocol rather than a choice here.
    {
        let shard = wire::shard_amqp_queue(0, &s.in_buf[RK..RK + rl]);
        if !s.view.owns_shard(shard) {
            s.amqp_publish_errors = s.amqp_publish_errors.wrapping_add(1);
            nack(s, sys);
            return;
        }
    }

    if delivery_tag == 0 {
        let hdr = wire::QPROP_HEADER_LEN;
        wire::encode_qprop_header(&mut s.out_buf[..hdr], wire::QOP_AMQP_PUBLISH, 0, 0);
        s.out_buf[hdr..hdr + 2].copy_from_slice(&(rl as u16).to_le_bytes());
        core::ptr::copy_nonoverlapping(
            s.in_buf.as_ptr().add(RK),
            s.out_buf.as_mut_ptr().add(hdr + 2),
            rl + body_len,
        );
        // AMQP routing key names the queue, which owns the messages.
        let shard = wire::shard_amqp_queue(0, &s.in_buf[RK..RK + rl]);
        if try_emit_keyed(
            sys,
            s.out_proposals,
            &s.view,
            shard,
            &s.out_buf[..hdr + op_body_len],
        ) {
            s.proposals_emitted = s.proposals_emitted.wrapping_add(1);
            s.hb_prop = s.hb_prop.wrapping_add(1);
        }
        return;
    }

    // Confirm mode: inflight slot + correlation + tagged proposal.
    let Some((ki, slot)) = store::inflight_alloc(&mut s.store) else {
        nack(s, sys);
        return;
    };
    let Some(cid) = correlate::allocate(&mut s.correlate, slot, ki as u16, OP_KPRODUCE, now) else {
        nack(s, sys);
        return;
    };
    s.out_buf[0..8].copy_from_slice(&cid.to_le_bytes());
    wire::encode_qprop_header(
        &mut s.out_buf[8..8 + wire::QPROP_HEADER_LEN],
        wire::QOP_AMQP_PUBLISH,
        0,
        slot,
    );
    let ob = wire::QPROP_TAGGED_HDR_LEN;
    s.out_buf[ob..ob + 2].copy_from_slice(&(rl as u16).to_le_bytes());
    core::ptr::copy_nonoverlapping(
        s.in_buf.as_ptr().add(RK),
        s.out_buf.as_mut_ptr().add(ob + 2),
        rl + body_len,
    );
    let shard = wire::shard_amqp_queue(0, &s.in_buf[RK..RK + rl]);
    if !try_emit_keyed(
        sys,
        s.out_proposals_tagged,
        &s.view,
        shard,
        &s.out_buf[..ob + op_body_len],
    ) {
        let _ = correlate::take(&mut s.correlate, cid);
        nack(s, sys);
        return;
    }
    s.proposals_emitted = s.proposals_emitted.wrapping_add(1);
    s.hb_prop = s.hb_prop.wrapping_add(1);
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
///   [conn:u16 LE][2][2][channel:u16 LE][dt:u64=0][q_len:u16 LE][queue]
/// Pops the next raw entry at or past the queue's get_cursor.
/// Single-consumer, auto-ack semantics (v1).
///
/// # Safety
unsafe fn handle_amqp_get(s: &mut ModuleState, sys: &SyscallTable, plen: usize) {
    const Q: usize = AMQP_ENV_PREFIX + 2;
    if plen < Q {
        return;
    }
    let (conn_id, channel) = amqp_env_header(&s.in_buf);
    let ql =
        u16::from_le_bytes([s.in_buf[AMQP_ENV_PREFIX], s.in_buf[AMQP_ENV_PREFIX + 1]]) as usize;
    if ql == 0 || ql > KAFKA_MAX_TOPIC || Q + ql > plen {
        return;
    }
    let mut queue = [0u8; KAFKA_MAX_TOPIC];
    queue[..ql].copy_from_slice(&s.in_buf[Q..Q + ql]);
    s.amqp_get_rx = s.amqp_get_rx.wrapping_add(1);

    // Empty result still carries the full [result][offset][remaining]
    // shape — the codec parses one fixed layout for both outcomes.
    let empty = |s: &mut ModuleState, sys: &SyscallTable| {
        let rest = [1u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        emit_amqp_response(s, sys, conn_id, 2, channel, &rest);
    };

    let Some(pi) = store::find(&s.store, &queue[..ql], 0) else {
        empty(s, sys);
        return;
    };
    if store::is_empty(&s.store, pi) {
        empty(s, sys);
        return;
    }

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
    if 13 + l > rest.len() {
        empty(s, sys);
        return;
    }
    rest[0] = 0; // result: message
                 // Delivery tag = offset + 1: AMQP reserves tag 0 for the client's
                 // "all messages" ack, so the server MUST NOT assign it (the first
                 // message on a fresh queue has offset 0). GetOk is auto-ack here, so
                 // the tag is informational, but a zero tag makes a client's ack
                 // ambiguous with ack-all.
    rest[1..9].copy_from_slice(&(offset + 1).to_le_bytes());
    rest[9..13].copy_from_slice(&remaining_after.to_le_bytes());
    store::copy_entry_into(&s.store, pi, data_pos as usize, l, &mut rest[13..13 + l]);
    emit_amqp_response(s, sys, conn_id, 2, channel, &rest[..13 + l]);
}

// ── Kafka consumer-group + offset APIs ──────────────────────────────────────

#[cfg(feature = "kafka")]
/// Read a Kafka STRING (i16 BE len + bytes) out of `buf`. Returns
/// `(next_off, start, len)`; len 0 for null (-1) strings.
fn kstr(buf: &[u8], off: usize, end: usize) -> Option<(usize, usize, usize)> {
    if off + 2 > end {
        return None;
    }
    let l = i16::from_be_bytes([buf[off], buf[off + 1]]);
    if l < 0 {
        return Some((off + 2, off + 2, 0));
    }
    let l = l as usize;
    if off + 2 + l > end {
        return None;
    }
    Some((off + 2 + l, off + 2, l))
}

#[cfg(feature = "kafka")]
/// JoinGroup (api_key 11, v0-v2). Assigns/refreshes member identity,
/// bumps the generation on membership change, and returns the member
/// list to the leader so it can compute assignments client-side.
///
/// # Safety
unsafe fn handle_kafka_join_group(s: &mut ModuleState, sys: &SyscallTable, plen: usize) {
    let (conn_id, v, corr) = kafka_env_header(&s.in_buf);
    let end = plen;
    s.kafka_group_ops = s.kafka_group_ops.wrapping_add(1);

    let Some((mut off, gs, gl)) = kstr(&s.in_buf, KAFKA_ENV_HDR, end) else {
        return;
    };
    if off + 4 > end {
        return;
    }
    off += 4; // session_timeout
    if v >= 1 {
        if off + 4 > end {
            return;
        }
        off += 4; // rebalance_timeout
    }
    let Some((off2, ms, ml)) = kstr(&s.in_buf, off, end) else {
        return;
    };
    off = off2;
    let Some((off3, _pts, _ptl)) = kstr(&s.in_buf, off, end) else {
        return;
    };
    off = off3;
    if off + 4 > end {
        return;
    }
    let proto_count = i32::from_be_bytes([
        s.in_buf[off],
        s.in_buf[off + 1],
        s.in_buf[off + 2],
        s.in_buf[off + 3],
    ]);
    off += 4;
    // First protocol: name + metadata (echoed back in SyncGroup flows).
    let mut pname = [0u8; 16];
    let mut pname_len = 0usize;
    let mut meta = [0u8; KG_META];
    let mut meta_len = 0usize;
    if proto_count >= 1 {
        let Some((off4, ps, pl)) = kstr(&s.in_buf, off, end) else {
            return;
        };
        pname_len = pl.min(16);
        pname[..pname_len].copy_from_slice(&s.in_buf[ps..ps + pname_len]);
        off = off4;
        if off + 4 > end {
            return;
        }
        let bl = i32::from_be_bytes([
            s.in_buf[off],
            s.in_buf[off + 1],
            s.in_buf[off + 2],
            s.in_buf[off + 3],
        ]);
        off += 4;
        if bl > 0 {
            if off + bl as usize > end {
                return;
            }
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
    let Some(mi) = consumers::member_join(&mut s.consumers, gi, &member[..member_len], conn_id)
    else {
        emit_kafka_group_error(s, sys, conn_id, 11, v, corr, 15);
        return;
    };
    consumers::member_set_meta(
        &mut s.consumers,
        gi,
        mi,
        &meta[..meta_len],
        &pname[..pname_len],
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
        s.out_buf[p..p + 4].copy_from_slice(&0i32.to_be_bytes());
        p += 4;
    }
    s.out_buf[p..p + 2].copy_from_slice(&0i16.to_be_bytes());
    p += 2;
    s.out_buf[p..p + 4].copy_from_slice(&generation.to_be_bytes());
    p += 4;
    s.out_buf[p..p + 2].copy_from_slice(&(proto_len as i16).to_be_bytes());
    p += 2;
    s.out_buf[p..p + proto_len].copy_from_slice(&proto[..proto_len]);
    p += proto_len;
    s.out_buf[p..p + 2].copy_from_slice(&(leader_len as i16).to_be_bytes());
    p += 2;
    s.out_buf[p..p + leader_len].copy_from_slice(&leader_id[..leader_len]);
    p += leader_len;
    s.out_buf[p..p + 2].copy_from_slice(&(member_len as i16).to_be_bytes());
    p += 2;
    s.out_buf[p..p + member_len].copy_from_slice(&member[..member_len]);
    p += member_len;
    if is_leader {
        let count = (0..KGROUP_MEMBERS)
            .filter(|&i| consumers::member_active(&s.consumers, gi, i))
            .count() as i32;
        s.out_buf[p..p + 4].copy_from_slice(&count.to_be_bytes());
        p += 4;
        for i in 0..KGROUP_MEMBERS {
            if !consumers::member_active(&s.consumers, gi, i) {
                continue;
            }
            let mut mid = [0u8; KG_NAME];
            let mut mmeta = [0u8; KG_META];
            let il = consumers::member_id_into(&s.consumers, gi, i, &mut mid);
            let mel = consumers::member_meta_into(&s.consumers, gi, i, &mut mmeta);
            if p + 2 + il + 4 + mel > BUF_SIZE - 32 {
                break;
            }
            s.out_buf[p..p + 2].copy_from_slice(&(il as i16).to_be_bytes());
            p += 2;
            s.out_buf[p..p + il].copy_from_slice(&mid[..il]);
            p += il;
            s.out_buf[p..p + 4].copy_from_slice(&(mel as i32).to_be_bytes());
            p += 4;
            s.out_buf[p..p + mel].copy_from_slice(&mmeta[..mel]);
            p += mel;
        }
    } else {
        s.out_buf[p..p + 4].copy_from_slice(&0i32.to_be_bytes());
        p += 4;
    }
    emit_kafka_response_outbuf(s, sys, conn_id, 11, v, corr, p);
}

#[cfg(feature = "kafka")]
/// Small-body group error response (JoinGroup shape degenerates fine:
/// clients read error_code first and bail).
///
/// # Safety
unsafe fn emit_kafka_group_error(
    s: &mut ModuleState,
    sys: &SyscallTable,
    conn_id: u16,
    api_key: i16,
    v: i16,
    corr: i32,
    err: i16,
) {
    let mut p = 0usize;
    if (api_key == 11 && v >= 2) || (api_key != 11 && v >= 1) {
        s.out_buf[p..p + 4].copy_from_slice(&0i32.to_be_bytes());
        p += 4;
    }
    s.out_buf[p..p + 2].copy_from_slice(&err.to_be_bytes());
    p += 2;
    if api_key == 11 {
        // generation, protocol "", leader "", member_id "", members []
        s.out_buf[p..p + 4].copy_from_slice(&(-1i32).to_be_bytes());
        p += 4;
        for _ in 0..3 {
            s.out_buf[p..p + 2].copy_from_slice(&0i16.to_be_bytes());
            p += 2;
        }
        s.out_buf[p..p + 4].copy_from_slice(&0i32.to_be_bytes());
        p += 4;
    } else if api_key == 14 {
        s.out_buf[p..p + 4].copy_from_slice(&0i32.to_be_bytes());
        p += 4; // empty assignment
    }
    emit_kafka_response_outbuf(s, sys, conn_id, api_key, v, corr, p);
}

#[cfg(feature = "kafka")]
/// SyncGroup (api_key 14, v0-v1): the leader supplies per-member
/// assignments; everyone gets their stored assignment back.
///
/// # Safety
unsafe fn handle_kafka_sync_group(s: &mut ModuleState, sys: &SyscallTable, plen: usize) {
    let (conn_id, v, corr) = kafka_env_header(&s.in_buf);
    let end = plen;
    s.kafka_group_ops = s.kafka_group_ops.wrapping_add(1);

    let Some((mut off, gs, gl)) = kstr(&s.in_buf, KAFKA_ENV_HDR, end) else {
        return;
    };
    if off + 4 > end {
        return;
    }
    let generation = i32::from_be_bytes([
        s.in_buf[off],
        s.in_buf[off + 1],
        s.in_buf[off + 2],
        s.in_buf[off + 3],
    ]);
    off += 4;
    let Some((off2, ms, ml)) = kstr(&s.in_buf, off, end) else {
        return;
    };
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
            s.in_buf[off],
            s.in_buf[off + 1],
            s.in_buf[off + 2],
            s.in_buf[off + 3],
        ]);
        off += 4;
        for _ in 0..n.clamp(0, KGROUP_MEMBERS as i32 * 2) {
            let Some((o2, ids, idl)) = kstr(&s.in_buf, off, end) else {
                break;
            };
            off = o2;
            if off + 4 > end {
                break;
            }
            let bl = i32::from_be_bytes([
                s.in_buf[off],
                s.in_buf[off + 1],
                s.in_buf[off + 2],
                s.in_buf[off + 3],
            ]);
            off += 4;
            if bl < 0 {
                continue;
            }
            let bl = bl as usize;
            if off + bl > end {
                break;
            }
            let mut mid = [0u8; KG_NAME];
            let idl = idl.min(KG_NAME);
            mid[..idl].copy_from_slice(&s.in_buf[ids..ids + idl]);
            let al = bl.min(KG_META);
            let mut abuf = [0u8; KG_META];
            core::ptr::copy_nonoverlapping(s.in_buf.as_ptr().add(off), abuf.as_mut_ptr(), al);
            consumers::member_set_assignment(&mut s.consumers, gi, &mid[..idl], &abuf[..al]);
            // Replicate the stable membership. Emitted here, at the one
            // site where an assignment is decided, rather than by a
            // sweep: unlike the generation (which moves on join, leave
            // AND the bulk conn release), an assignment changes in
            // exactly this loop.
            propose_group_member(s, sys, gi, &mid[..idl], &abuf[..al]);
            off += bl;
        }
    }

    let mut assign = [0u8; KG_META];
    let al = consumers::member_assignment_into(&s.consumers, gi, mi, &mut assign);
    let mut p = 0usize;
    if v >= 1 {
        s.out_buf[p..p + 4].copy_from_slice(&0i32.to_be_bytes());
        p += 4;
    }
    s.out_buf[p..p + 2].copy_from_slice(&0i16.to_be_bytes());
    p += 2;
    s.out_buf[p..p + 4].copy_from_slice(&(al as i32).to_be_bytes());
    p += 4;
    s.out_buf[p..p + al].copy_from_slice(&assign[..al]);
    p += al;
    emit_kafka_response_outbuf(s, sys, conn_id, 14, v, corr, p);
}

#[cfg(feature = "kafka")]
/// Heartbeat (12) / LeaveGroup (13), v0-v1 — same request prefix.
///
/// # Safety
unsafe fn handle_kafka_heartbeat_leave(
    s: &mut ModuleState,
    sys: &SyscallTable,
    plen: usize,
    api_key: i16,
) {
    let (conn_id, v, corr) = kafka_env_header(&s.in_buf);
    let end = plen;
    s.kafka_group_ops = s.kafka_group_ops.wrapping_add(1);

    let Some((mut off, gs, gl)) = kstr(&s.in_buf, KAFKA_ENV_HDR, end) else {
        return;
    };
    let mut generation = 0i32;
    if api_key == 12 {
        if off + 4 > end {
            return;
        }
        generation = i32::from_be_bytes([
            s.in_buf[off],
            s.in_buf[off + 1],
            s.in_buf[off + 2],
            s.in_buf[off + 3],
        ]);
        off += 4;
    }
    let Some((_o, ms, ml)) = kstr(&s.in_buf, off, end) else {
        return;
    };
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
        s.out_buf[p..p + 4].copy_from_slice(&0i32.to_be_bytes());
        p += 4;
    }
    s.out_buf[p..p + 2].copy_from_slice(&err.to_be_bytes());
    p += 2;
    emit_kafka_response_outbuf(s, sys, conn_id, api_key, v, corr, p);
}

#[cfg(feature = "kafka")]
/// OffsetCommit (8, v0-v2). Stores every (topic, partition, offset) in
/// the request, mirrors the structure back with per-partition error 0,
/// and emits an untagged QOP_KAFKA_OFFSET per commit for durability
/// (response is not durability-gated — see wire.rs).
///
/// # Safety
unsafe fn handle_kafka_offset_commit(
    s: &mut ModuleState,
    sys: &SyscallTable,
    now: u64,
    plen: usize,
) {
    let (conn_id, v, corr) = kafka_env_header(&s.in_buf);
    let end = plen;
    s.kafka_offset_commits = s.kafka_offset_commits.wrapping_add(1);

    let Some((mut off, gs, gl)) = kstr(&s.in_buf, KAFKA_ENV_HDR, end) else {
        return;
    };
    let mut group = [0u8; KG_NAME];
    let gl = gl.min(KG_NAME);
    group[..gl].copy_from_slice(&s.in_buf[gs..gs + gl]);
    if v >= 1 {
        if off + 4 > end {
            return;
        }
        off += 4; // generation
        let Some((o2, _ms, _ml)) = kstr(&s.in_buf, off, end) else {
            return;
        };
        off = o2;
    }
    if v >= 2 {
        if off + 8 > end {
            return;
        }
        off += 8; // retention_time
    }

    // DURABILITY GATING. The offset is proposed to raft; the response
    // must not claim success until that proposal is quorum-durable.
    // Acking early is not data loss — after a crash the consumer
    // re-reads from an older offset — but it IS a broken promise: a
    // client told "committed" will not commit that offset again.
    //
    // The response is parked in an inflight slot and fired from the
    // same MSG_ACK_EMIT path that gates PUBACK and the produce
    // response, so it inherits their counting, lifecycle and expiry
    // sweep rather than adding a second mechanism beside them.
    let gate = store::inflight_alloc(&mut s.store);
    let mut gated_parts = 0u8;
    if off + 4 > end {
        return;
    }
    let topic_count = i32::from_be_bytes([
        s.in_buf[off],
        s.in_buf[off + 1],
        s.in_buf[off + 2],
        s.in_buf[off + 3],
    ])
    .clamp(0, 4);
    off += 4;

    // Response built as we parse (structure mirrors the request).
    let mut p = 0usize;
    s.out_buf[p..p + 4].copy_from_slice(&topic_count.to_be_bytes());
    p += 4;
    for _ in 0..topic_count {
        let Some((o2, ts, tl)) = kstr(&s.in_buf, off, end) else {
            return;
        };
        off = o2;
        let tl = tl.min(KAFKA_MAX_TOPIC);
        let mut topic = [0u8; KAFKA_MAX_TOPIC];
        topic[..tl].copy_from_slice(&s.in_buf[ts..ts + tl]);
        if off + 4 > end {
            return;
        }
        let pc = i32::from_be_bytes([
            s.in_buf[off],
            s.in_buf[off + 1],
            s.in_buf[off + 2],
            s.in_buf[off + 3],
        ])
        .clamp(0, 8);
        off += 4;
        s.out_buf[p..p + 2].copy_from_slice(&(tl as i16).to_be_bytes());
        p += 2;
        s.out_buf[p..p + tl].copy_from_slice(&topic[..tl]);
        p += tl;
        s.out_buf[p..p + 4].copy_from_slice(&pc.to_be_bytes());
        p += 4;
        for _ in 0..pc {
            if off + 12 > end {
                return;
            }
            let part = i32::from_be_bytes([
                s.in_buf[off],
                s.in_buf[off + 1],
                s.in_buf[off + 2],
                s.in_buf[off + 3],
            ]);
            let o = i64::from_be_bytes([
                s.in_buf[off + 4],
                s.in_buf[off + 5],
                s.in_buf[off + 6],
                s.in_buf[off + 7],
                s.in_buf[off + 8],
                s.in_buf[off + 9],
                s.in_buf[off + 10],
                s.in_buf[off + 11],
            ]);
            off += 12;
            if v == 1 {
                if off + 8 > end {
                    return;
                }
                off += 8; // timestamp
            }
            let Some((o3, _mds, _mdl)) = kstr(&s.in_buf, off, end) else {
                return;
            };
            off = o3;
            consumers::offset_store(&mut s.consumers, &group[..gl], &topic[..tl], part as u16, o);
            s.out_buf[p..p + 4].copy_from_slice(&part.to_be_bytes());
            p += 4;
            s.out_buf[p..p + 2].copy_from_slice(&0i16.to_be_bytes());
            p += 2;
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
            if hdr + body_len > s.out_buf.len() {
                continue;
            }
            // Build the proposal in a stack buffer — out_buf holds the
            // in-progress response.
            let mut prop = [0u8; 200];
            wire::encode_qprop_header(&mut prop[..hdr], wire::QOP_KAFKA_OFFSET, 0, 0);
            let mut q = hdr;
            prop[q..q + 2].copy_from_slice(&(gl as u16).to_le_bytes());
            q += 2;
            prop[q..q + gl].copy_from_slice(&group[..gl]);
            q += gl;
            prop[q..q + 2].copy_from_slice(&(tl as u16).to_le_bytes());
            q += 2;
            prop[q..q + tl].copy_from_slice(&topic[..tl]);
            q += tl;
            prop[q..q + 2].copy_from_slice(&ofs_partition.to_le_bytes());
            q += 2;
            prop[q..q + 8].copy_from_slice(&ofs_offset.to_le_bytes());
            q += 8;
            // Committed offsets belong to the group coordinator's
            // shard, so a group's offsets stay on one partition.
            let shard = wire::shard_kafka_group(TENANT_UNRESOLVED, &group[..gl]);
            let mut emitted_gated = false;
            if let Some((ki, slot)) = gate {
                if gated_parts < u8::MAX {
                    // Tagged, so durability comes back as
                    // MSG_PROPOSAL_ASSIGNED -> ack registration ->
                    // MSG_ACK_EMIT, which is what releases the response.
                    let packet_id = (ki as u16) | ((gated_parts as u16) << 8);
                    if let Some(cid) =
                        correlate::allocate(&mut s.correlate, slot, packet_id, OP_KOFFSET, now)
                    {
                        let mut tagged = [0u8; 208];
                        tagged[0..8].copy_from_slice(&cid.to_le_bytes());
                        tagged[8..8 + q].copy_from_slice(&prop[..q]);
                        if try_emit_keyed(
                            sys,
                            s.out_proposals_tagged,
                            &s.view,
                            shard,
                            &tagged[..8 + q],
                        ) {
                            s.proposals_emitted = s.proposals_emitted.wrapping_add(1);
                            s.hb_prop = s.hb_prop.wrapping_add(1);
                            gated_parts += 1;
                            emitted_gated = true;
                        } else {
                            let _ = correlate::take(&mut s.correlate, cid);
                        }
                    }
                }
            }
            if !emitted_gated {
                // No slot or no correlation free: fall back to the
                // previous ungated emit rather than dropping the
                // commit, and count it — the guarantee does not hold
                // for these and that must be visible.
                if try_emit_keyed(sys, s.out_proposals, &s.view, shard, &prop[..q]) {
                    s.proposals_emitted = s.proposals_emitted.wrapping_add(1);
                    s.hb_prop = s.hb_prop.wrapping_add(1);
                    s.kafka_offset_ungated = s.kafka_offset_ungated.wrapping_add(1);
                }
            }
        }
    }
    // Park the response when anything was gated; otherwise answer now.
    let parked = match gate {
        Some((ki, _)) if gated_parts > 0 => {
            if store::inflight_set_response(&mut s.store, ki, &s.out_buf[..p]) {
                // Answered from the slot once its offsets are durable.
                s.kafka_settled = true;
                store::inflight_open(
                    &mut s.store,
                    ki,
                    store::InflightOpen {
                        conn_id,
                        proto: KIN_PROTO_KAFKA_OFFSET,
                        topic_len: 0,
                        n_parts: gated_parts,
                        n_done: 0,
                        api_ver: v,
                        acks: 1,
                        kafka_corr: corr,
                        channel: 0,
                        delivery_tag: 0,
                        ts_ms: now,
                    },
                    &[],
                );
                // Committed through the coordinator: answered through it.
                if s.coord_reply {
                    store::inflight_set_coord(&mut s.store, ki);
                }
                true
            } else {
                // Larger than a slot can park. Answer now and count it.
                store::inflight_free(&mut s.store, ki);
                s.kafka_offset_ungated = s.kafka_offset_ungated.wrapping_add(1);
                false
            }
        }
        other => {
            if let Some((ki, _)) = other {
                store::inflight_free(&mut s.store, ki);
            }
            false
        }
    };
    if !parked {
        emit_kafka_response_outbuf(s, sys, conn_id, 8, v, corr, p);
    }
}

#[cfg(feature = "kafka")]
/// OffsetFetch (9, v0-v3).
///
/// # Safety
unsafe fn handle_kafka_offset_fetch(s: &mut ModuleState, sys: &SyscallTable, plen: usize) {
    let (conn_id, v, corr) = kafka_env_header(&s.in_buf);
    let end = plen;

    let Some((mut off, gs, gl)) = kstr(&s.in_buf, KAFKA_ENV_HDR, end) else {
        return;
    };
    let mut group = [0u8; KG_NAME];
    let gl = gl.min(KG_NAME);
    group[..gl].copy_from_slice(&s.in_buf[gs..gs + gl]);
    if off + 4 > end {
        return;
    }
    let topic_count = i32::from_be_bytes([
        s.in_buf[off],
        s.in_buf[off + 1],
        s.in_buf[off + 2],
        s.in_buf[off + 3],
    ])
    .clamp(0, 4);
    off += 4;

    let mut p = 0usize;
    if v >= 3 {
        s.out_buf[p..p + 4].copy_from_slice(&0i32.to_be_bytes());
        p += 4;
    }
    s.out_buf[p..p + 4].copy_from_slice(&topic_count.to_be_bytes());
    p += 4;
    for _ in 0..topic_count {
        let Some((o2, ts, tl)) = kstr(&s.in_buf, off, end) else {
            return;
        };
        off = o2;
        let tl = tl.min(KAFKA_MAX_TOPIC);
        let mut topic = [0u8; KAFKA_MAX_TOPIC];
        topic[..tl].copy_from_slice(&s.in_buf[ts..ts + tl]);
        if off + 4 > end {
            return;
        }
        let pc = i32::from_be_bytes([
            s.in_buf[off],
            s.in_buf[off + 1],
            s.in_buf[off + 2],
            s.in_buf[off + 3],
        ])
        .clamp(0, 8);
        off += 4;
        s.out_buf[p..p + 2].copy_from_slice(&(tl as i16).to_be_bytes());
        p += 2;
        s.out_buf[p..p + tl].copy_from_slice(&topic[..tl]);
        p += tl;
        s.out_buf[p..p + 4].copy_from_slice(&pc.to_be_bytes());
        p += 4;
        for _ in 0..pc {
            if off + 4 > end {
                return;
            }
            let part = i32::from_be_bytes([
                s.in_buf[off],
                s.in_buf[off + 1],
                s.in_buf[off + 2],
                s.in_buf[off + 3],
            ]);
            off += 4;
            let o = consumers::offset_get(&s.consumers, &group[..gl], &topic[..tl], part as u16);
            s.out_buf[p..p + 4].copy_from_slice(&part.to_be_bytes());
            p += 4;
            s.out_buf[p..p + 8].copy_from_slice(&o.to_be_bytes());
            p += 8;
            s.out_buf[p..p + 2].copy_from_slice(&(-1i16).to_be_bytes());
            p += 2; // metadata null
            s.out_buf[p..p + 2].copy_from_slice(&0i16.to_be_bytes());
            p += 2;
        }
    }
    if v >= 2 {
        s.out_buf[p..p + 2].copy_from_slice(&0i16.to_be_bytes());
        p += 2; // top-level err
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
fn handle_conn_disconnect(s: &mut ModuleState, conn_id: u16) {
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
///   [AMQP_ENV_PREFIX bytes][flags:u8 (bit0 no_ack)][prefetch:u16 LE]
///   [tag_len:u16 LE][tag][q_len:u16 LE][queue]
///
/// # Safety
unsafe fn handle_amqp_consume(s: &mut ModuleState, sys: &SyscallTable, plen: usize) {
    const FLAGS: usize = AMQP_ENV_PREFIX;
    const PREFETCH: usize = FLAGS + 1;
    const TAG_LEN: usize = PREFETCH + 2;
    const TAG: usize = TAG_LEN + 2;
    if plen < TAG {
        return;
    }
    let (conn_id, channel) = amqp_env_header(&s.in_buf);
    let flags = s.in_buf[FLAGS];
    let prefetch = u16::from_le_bytes([s.in_buf[PREFETCH], s.in_buf[PREFETCH + 1]]);
    let tl = u16::from_le_bytes([s.in_buf[TAG_LEN], s.in_buf[TAG_LEN + 1]]) as usize;
    if tl == 0 || tl > AMQP_TAG_MAX || TAG + tl + 2 > plen {
        return;
    }
    let toff = TAG;
    let ql = u16::from_le_bytes([s.in_buf[TAG + tl], s.in_buf[TAG + tl + 1]]) as usize;
    if ql == 0 || ql > KAFKA_MAX_TOPIC || TAG + 2 + tl + ql > plen {
        return;
    }
    let qoff = TAG + 2 + tl;

    // Replaces any existing consumer on this (conn, channel) — AMQP
    // allows one per channel here, so a re-Consume replaces rather than
    // duplicates.
    let Some(slot) = consumers::consumer_slot(&s.consumers, conn_id, channel) else {
        // Table full: broker-initiated cancel so the client knows.
        let mut rest = [0u8; 2 + AMQP_TAG_MAX];
        rest[0..2].copy_from_slice(&(tl as u16).to_le_bytes());
        rest[2..2 + tl].copy_from_slice(&s.in_buf[toff..toff + tl]);
        emit_amqp_response(s, sys, conn_id, 4, channel, &rest[..2 + tl]);
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
        &mut s.consumers,
        slot,
        conn_id,
        channel,
        flags & 1 == 1,
        prefetch,
        &tag[..tl],
        &queue[..ql],
        cursor,
    );
    if s.coord_reply {
        consumers::consumer_set_coord(&mut s.consumers, slot);
    }
}

#[cfg(feature = "amqp")]
/// op=4 consume-cancel: [AMQP_ENV_PREFIX bytes][tag_len:u16 LE][tag]
///
/// # Safety
unsafe fn handle_amqp_cancel(s: &mut ModuleState, plen: usize) {
    const TAG: usize = AMQP_ENV_PREFIX + 2;
    if plen < TAG {
        return;
    }
    let (conn_id, channel) = amqp_env_header(&s.in_buf);
    let tl =
        u16::from_le_bytes([s.in_buf[AMQP_ENV_PREFIX], s.in_buf[AMQP_ENV_PREFIX + 1]]) as usize;
    if tl == 0 || tl > AMQP_TAG_MAX || TAG + tl > plen {
        return;
    }
    let mut tag = [0u8; AMQP_TAG_MAX];
    tag[..tl].copy_from_slice(&s.in_buf[TAG..TAG + tl]);
    if let Some(ci) = consumers::consumer_by_tag(&s.consumers, conn_id, channel, &tag[..tl]) {
        consumers::consumer_release(&mut s.consumers, ci);
    }
}

#[cfg(feature = "amqp")]
/// op=5 client ack/nack: [AMQP_ENV_PREFIX bytes, dt = delivery-tag][flags:u8]
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
///
/// Nack/Reject release credit without redelivery (documented v1 gap).
///
/// # Safety
unsafe fn handle_amqp_client_ack(s: &mut ModuleState, plen: usize) {
    if plen < AMQP_ENV_PREFIX + 1 {
        return;
    }
    let (conn_id, channel) = amqp_env_header(&s.in_buf);
    let dt = amqp_env_delivery_tag(&s.in_buf);
    let multiple = s.in_buf[AMQP_ENV_PREFIX] & 1 != 0;
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
        if !consumers::consumer_active(&s.consumers, ci) {
            continue;
        }
        let mut queue = [0u8; KAFKA_MAX_TOPIC];
        let ql = consumers::consumer_queue_into(&s.consumers, ci, &mut queue);
        let Some(pi) = store::find(&s.store, &queue[..ql], 0) else {
            continue;
        };
        for _ in 0..AMQP_DELIVER_QUOTA {
            if !consumers::consumer_in_credit(&s.consumers, ci) {
                break;
            }
            let Some(view) = consumers::consumer_view(&s.consumers, ci) else {
                break;
            };
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
            if 11 + tl + l > rest.len() {
                break;
            }
            rest[0..8].copy_from_slice(&view.next_dtag.to_le_bytes());
            rest[8] = 0;
            rest[9..11].copy_from_slice(&(tl as u16).to_le_bytes());
            rest[11..11 + tl].copy_from_slice(&tag[..tl]);
            store::copy_entry_into(
                &s.store,
                pi,
                data_pos as usize,
                l,
                &mut rest[11 + tl..11 + tl + l],
            );
            // A consumer registered through the coordinator is delivered
            // to as one: wherever its connection is bound.
            s.coord_reply = view.coord;
            let offered =
                offer_amqp_response(s, sys, view.conn_id, 3, view.channel, &rest[..11 + tl + l]);
            s.coord_reply = false;
            if !offered {
                // Backpressure: retry next tick from the same cursor.
                break;
            }
            consumers::consumer_delivered(&mut s.consumers, ci, offset);
            s.amqp_delivers = s.amqp_delivers.wrapping_add(1);
        }
    }
}

/// True when every output an apply handler can write to has room.
///
/// The apply path asks `messaging` for dedup checks, retained writes and
/// offline ops — through the outbox, which must have room for an entry's
/// worth — files a SUBSCRIBE's retained read, which needs a free read
/// slot, and writes the topic bus (fan-out) and the control-plane bus
/// directly; a handler has no way to defer half its effects. Checking all
/// before the entry is consumed is what keeps apply-derived state and the
/// committed log in step: a module advances only on a committed output,
/// which is fluxor's standing rule for every module in the graph.
///
/// # Safety
/// `sys` must point at a live kernel syscall table.
unsafe fn outputs_ready_for_apply(s: &ModuleState, sys: &SyscallTable) -> bool {
    // SAFETY: caller guarantees `sys` is live.
    unsafe {
        for chan in [s.out_topic, s.out_cp] {
            if chan < 0 {
                continue;
            }
            let poll = (sys.channel_poll)(chan, 0x02);
            if poll <= 0 || (poll as u32 & 0x02) == 0 {
                return false;
            }
        }
    }
    // What it asks of `messaging` waits in the outbox, which must have
    // room for an entry's worth; a SUBSCRIBE files a retained read, and a
    // QoS 1/2 PUBLISH a stash, which must never be refused: a stash frees
    // on its verdict and the topic bus, never on a client.
    messaging_room(s, APPLY_ROOM)
        && s.mflow.reads_free() > 0
        && correlate::stash_free(&s.correlate) > 0
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
/// `sys` must point at a live kernel syscall table.
unsafe fn try_emit(sys: &SyscallTable, chan: i32, msg_type: u8, payload: &[u8]) -> bool {
    if chan < 0 {
        return false;
    }
    // channel_write_msg writes the envelope atomically and returns the
    // total written count, or a negative value (CHAN_EAGAIN, oversize,
    // EINVAL, ...) on failure. Anything ≤ 0 means the payload did not
    // reach the channel; the caller decides what to do.
    let total = (wire::ENVELOPE_HDR + payload.len()) as i32;
    let w = wire::channel_write_msg(sys, chan, msg_type, payload);
    w == total
}

/// Ask `messaging` for `(msg_type, payload)`: written now when nothing
/// waits ahead of it in the outbox and the op edge takes it, else queued
/// behind what waits and written in order as the edge has room. Every
/// input that files requests is read only with room for what its next
/// record can file ([`messaging_room`]), so this never meets a full
/// outbox (`mbox.refused` would count it).
///
/// # Safety
/// `sys` must point at a live kernel syscall table.
unsafe fn to_messaging(s: &mut ModuleState, sys: &SyscallTable, msg_type: u8, payload: &[u8]) {
    send_messaging(&mut s.mbox, sys, s.out_messaging, msg_type, payload);
}

/// [`to_messaging`] of the first `len` bytes of `out_buf`.
///
/// # Safety
/// `sys` must point at a live kernel syscall table.
unsafe fn out_buf_to_messaging(s: &mut ModuleState, sys: &SyscallTable, msg_type: u8, len: usize) {
    send_messaging(
        &mut s.mbox,
        sys,
        s.out_messaging,
        msg_type,
        &s.out_buf[..len],
    );
}

/// [`to_messaging`] on the outbox and edge alone, for a caller holding
/// other parts of the state.
///
/// # Safety
/// `sys` must point at a live kernel syscall table.
unsafe fn send_messaging(
    mbox: &mut outbox::Outbox,
    sys: &SyscallTable,
    chan: i32,
    msg_type: u8,
    payload: &[u8],
) {
    if chan < 0 {
        return;
    }
    // SAFETY: caller guarantees `sys` is live.
    mbox.send(
        |mt, p| unsafe { try_emit(sys, chan, mt, p) },
        msg_type,
        payload,
    );
}

/// Write what waits in the outbox toward `messaging`, as far as the op
/// edge takes it.
///
/// # Safety
/// `sys` must point at a live kernel syscall table.
unsafe fn flush_messaging(s: &mut ModuleState, sys: &SyscallTable) {
    let chan = s.out_messaging;
    if chan >= 0 {
        // SAFETY: caller guarantees `sys` is live.
        s.mbox.flush(|mt, p| unsafe { try_emit(sys, chan, mt, p) });
    }
}

/// The outbox room one offline enqueue takes: the largest delivery
/// envelope behind its `[session:u32][env_len:u16]`.
const ENQUEUE_ROOM: usize = outbox::Outbox::cost(6 + MAX_PENDING_DLV);
/// The outbox room one delivery joining its session's queue takes, with
/// everything its session has parked moved ahead of it.
const SPILL_ROOM: usize = PENDING_DLV_SLOTS * ENQUEUE_ROOM;
/// Room kept for what the deliveries `messaging` answers with may file:
/// a retained read's may join an offline queue, each of its credit
/// (`mflow::READ_CREDIT`), moving parked ones ahead of it (each parked
/// one moves once), and a drain's whose session left meanwhile is queued
/// again, each of its park slots.
const REPLY_ROOM: usize = (mflow::READ_CREDIT as usize + 2 * PENDING_DLV_SLOTS) * ENQUEUE_ROOM;
/// Park slots never credited to drains: a drain's answer may wait on its
/// client, and these keep room for deliveries that wait only on
/// something shared, so `deliver_in` is always read again without any
/// client.
const EDGE_PARK_RESERVE: u32 = 2;
/// What one committed entry's apply may file: a dedup check, a phase, a
/// retained write and a reset, none larger than `out_buf`.
const APPLY_ROOM: usize = 4 * outbox::Outbox::cost(BUF_SIZE);
/// What one placement update may file: a queue release for every session
/// it moves away, and the update itself, forwarded.
const PLACEMENT_ROOM: usize = MAX_SESSIONS * outbox::Outbox::cost(4) + outbox::Outbox::cost(16);
/// One retained read or drain request.
const REQUEST_ROOM: usize = outbox::Outbox::cost(11 + mflow::FLOW_PATTERN_MAX + 6);
// Every input that files requests can be read with the outbox empty, and
// every frame it files fits one.
const _: () = {
    let mut most = if APPLY_ROOM > PLACEMENT_ROOM {
        APPLY_ROOM
    } else {
        PLACEMENT_ROOM
    };
    if SPILL_ROOM > most {
        most = SPILL_ROOM;
    }
    assert!(most + REPLY_ROOM <= outbox::OUTBOX_BYTES);
    assert!(BUF_SIZE <= outbox::FRAME_MAX && 6 + MAX_PENDING_DLV <= outbox::FRAME_MAX);
    assert!(mflow::FLOW_READ_DONE == wire::RETAINED_READ_DONE);
};

/// Whether the outbox has room for `need` beyond what the deliveries
/// already credited may file — the condition for reading an input whose
/// next record may file up to `need`. `messaging_in` is never held to it:
/// it carries the dedup verdicts committed publishes wait on.
fn messaging_room(s: &ModuleState, need: usize) -> bool {
    s.mbox.free() >= need + REPLY_ROOM
}

/// Delivery park slots free beyond those credited to drains still being
/// answered.
fn park_room(s: &ModuleState) -> u32 {
    (correlate::dlv_free_slots(&s.correlate) as u32).saturating_sub(s.mflow.drain_credit())
}

/// Park slots a new drain may be credited: those free beyond the ones
/// already credited and the [`EDGE_PARK_RESERVE`], less the slots drains'
/// answers already hold.
fn drain_room(s: &ModuleState) -> u32 {
    let held = correlate::dlv_count_kind(&s.correlate, park_code(order::From::Drained)) as u32;
    let drains_at_most = PENDING_DLV_SLOTS as u32 - EDGE_PARK_RESERVE;
    park_room(s).min(drains_at_most.saturating_sub(held + s.mflow.drain_credit()))
}

/// Drain session `slot`'s offline queue, in turns its credit allows; until
/// it is empty the session's fresh deliveries join it (`mflow`). Without
/// `messaging` wired there is no queue.
fn want_drain(s: &mut ModuleState, slot: u32) {
    if s.out_messaging >= 0 && (slot as usize) < MAX_SESSIONS {
        s.mflow.want_drain(slot);
    }
}

/// Send the retained reads their credit allows and the drains the park has
/// room to credit.
///
/// # Safety
/// `sys` must point at a live kernel syscall table.
unsafe fn request_messaging(s: &mut ModuleState, sys: &SyscallTable) {
    if s.out_messaging < 0 {
        return;
    }
    while messaging_room(s, REQUEST_ROOM) {
        let room = drain_room(s);
        let sessions = &s.sessions;
        let stream_of = |slot: u32| {
            let si = slot as usize;
            sessions::is_active(sessions, si).then(|| sessions::stream_hash(sessions, si))
        };
        let Some(req) = s.mflow.next_request(room, stream_of) else {
            return;
        };
        let mut p = [0u8; 11 + mflow::FLOW_PATTERN_MAX + 6];
        let (mt, n) = match req {
            mflow::Request::Read { read, max } => {
                let r = s.mflow.read(read);
                let pat = r.pattern();
                p[0..4].copy_from_slice(&r.tenant.to_le_bytes());
                p[4..8].copy_from_slice(&r.session.to_le_bytes());
                p[8] = r.qos;
                p[9..11].copy_from_slice(&(pat.len() as u16).to_le_bytes());
                p[11..11 + pat.len()].copy_from_slice(pat);
                let tail = 11 + pat.len();
                p[tail..tail + 4].copy_from_slice(&r.next.to_le_bytes());
                p[tail + 4..tail + 6].copy_from_slice(&max.to_le_bytes());
                (wire::MSG_RETAINED_READ, tail + 6)
            }
            mflow::Request::Drain { session, max } => {
                p[0..4].copy_from_slice(&session.to_le_bytes());
                p[4..6].copy_from_slice(&max.to_le_bytes());
                (wire::MSG_OFFLINE_RECONNECT, 6)
            }
        };
        let before = s.mbox.refused;
        to_messaging(s, sys, mt, &p[..n]);
        if s.mbox.refused != before {
            s.mflow.unsent(req);
            return;
        }
    }
}

/// Emit a keyed proposal: the same single atomic channel write as
/// [`try_emit`], with the proposer's virtual-shard id prefixed so
/// `partition_router` places it by routing key rather than by hashing
/// the body.
///
/// `payload` is exactly what would have gone to [`try_emit`] under
/// `MSG_CLIENT_PROPOSAL` — a bare op body on `out_proposals`, or
/// `[correlation_id:u64 LE][op body]` on `out_proposals_tagged`. The
/// router strips the shard prefix, so the bytes that reach the WAL are
/// unchanged and the substrate's opaque-command contract holds.
///
/// Fetches parked awaiting a WAL read-back. Small on purpose: a cold
/// read is the slow path, and letting many of them queue would turn a
/// lagging consumer into a memory cost on the broker.
#[cfg(feature = "kafka")]
const COLD_FETCH_SLOTS: usize = 4;

/// A Fetch waiting on `durability` to return the raft entry that carries
/// its offset.
#[cfg(feature = "kafka")]
#[repr(C)]
#[derive(Clone, Copy)]
struct ColdFetch {
    active: u8,
    conn_id: u16,
    topic_len: u8,
    /// Store slot, so the reply can report the CURRENT high watermark
    /// and log start. Reporting 0 would tell a consumer that has just
    /// been served from disk that the log is empty, and it is exactly a
    /// lagging consumer that needs to know how far behind it still is.
    part_idx: u8,
    api_ver: i16,
    corr: i32,
    partition: i32,
    fetch_offset: i64,
    request_id: u32,
    issued_ms: u64,
    /// The answer is decided and the codec bus had no room for it
    /// (`COLD_OWED_*`): kept, and answered on a later step.
    owed: u8,
    topic: [u8; KAFKA_MAX_TOPIC],
}

#[cfg(feature = "kafka")]
impl ColdFetch {
    const fn zero() -> Self {
        Self {
            active: 0,
            conn_id: 0,
            topic_len: 0,
            part_idx: 0,
            api_ver: 0,
            corr: 0,
            partition: 0,
            fetch_offset: 0,
            request_id: 0,
            issued_ms: 0,
            owed: COLD_OWED_NONE,
            topic: [0; KAFKA_MAX_TOPIC],
        }
    }
}

/// No cold-fetch answer is owed.
#[cfg(feature = "kafka")]
const COLD_OWED_NONE: u8 = 0;
/// An empty Fetch is owed: the records read back had no room on the bus,
/// so the client is told "nothing yet" and fetches the offset again.
#[cfg(feature = "kafka")]
const COLD_OWED_EMPTY: u8 = 1;
/// The timeout's OFFSET_OUT_OF_RANGE is owed.
#[cfg(feature = "kafka")]
const COLD_OWED_TIMEOUT: u8 = 2;

/// Whether this worker still owes `conn` an answer it gives later than
/// the request: an inflight slot (Kafka Produce / OffsetCommit awaiting
/// durability or apply, an AMQP confirm, or any of those owed for want
/// of bus room) or a parked cold Fetch. Such an answer exists only here
/// — its correlation, its place in the connection's request order and
/// the partition outcome it will report — and a handoff that exported
/// the connection now would leave it to a frozen, fenced worker: the
/// client's request would never be answered. `worker::quiescent` waits
/// for it, as it waits for an MQTT flow still in Raft.
fn answer_deferred_for(s: &ModuleState, conn: u16) -> bool {
    if store::inflight_for_conn(&s.store, conn) {
        return true;
    }
    #[cfg(feature = "kafka")]
    if s.cold.iter().any(|c| c.active == 1 && c.conn_id == conn) {
        return true;
    }
    false
}

/// A cold read's request id: the worker's id in the top 3 bits, a counter
/// below. Durability answers on one `cold_read_reply` the co-located
/// workers share, and each takes only the ids it issued.
#[cfg(feature = "kafka")]
const COLD_ID_SHIFT: u32 = 29;
#[cfg(feature = "kafka")]
const COLD_ID_MASK: u32 = (1 << COLD_ID_SHIFT) - 1;

/// The request-id namespace of worker `worker` (`COLD_ID_SHIFT`).
#[cfg(feature = "kafka")]
const fn cold_id_base(worker: u8) -> u32 {
    ((worker & 0x07) as u32) << COLD_ID_SHIFT
}

/// How long a parked cold fetch waits before it is answered
/// OFFSET_OUT_OF_RANGE and its slot reclaimed. A WAL read is a disk
/// read; a client that waited this long is better served an answer it
/// can act on than a slot held for ever.
#[cfg(feature = "kafka")]
const COLD_FETCH_TIMEOUT_MS: u64 = 3_000;

/// The Kafka error a client must be told when this node cannot serve a
/// shard, or `None` when it can.
///
/// The propose-side gate in [`try_emit_keyed`] REFUSES such work, but a
/// refusal alone is not an answer: a Kafka client that gets silence, or
/// gets `REQUEST_TIMED_OUT`, retries the same broker until its own
/// timeout and never learns that the partition moved. `kafka_error_for`
/// turns the placement disposition into the code the protocol already
/// has for this — `NOT_LEADER_OR_FOLLOWER` for a settled elsewhere-owner
/// (refresh metadata, go to the right broker) and `LEADER_NOT_AVAILABLE`
/// for the transient states (fenced, or our view is behind), which the
/// client retries.
///
/// So the Kafka side of a redirect is an error code and not a proxy:
/// the protocol already has a redirect, and forwarding the request
/// would reinvent one while pinning the client to the wrong broker for
/// ever.
///
/// `sender_epoch` is 0 — a plain client states no epoch, which
/// `classify` never treats as stale.
#[cfg(feature = "kafka")]
fn kafka_shard_error(view: &edge::EdgeMap, shard: u32) -> Option<i16> {
    edge::kafka_error_for(edge::classify(&view.view, view.owner_prg(shard), 0))
}

/// ## The ownership gate (EDGE-FENCE)
///
/// Every shard-keyed proposal this module makes passes through here,
/// which is why the gate lives at this one chokepoint rather than at
/// the sixteen call sites: a gate that can be forgotten at a new call
/// site is not a fence.
///
/// A proposal for a shard this node does not currently own is REFUSED.
/// While a shard is mid-transfer the losing PRG has stopped accepting
/// and the gaining one has not started; accepting at either end is
/// exactly how two groups come to own one shard.
///
/// **Gated at PROPOSE, never at APPLY** — and note this is the INVERSE
/// of the Kafka idempotence rule, deliberately. Idempotence is a
/// state-machine decision, so it must be taken at apply where every
/// replica reaches the same verdict from the same log. Ownership is an
/// ADMISSION decision: it is about whether to accept new work, and the
/// answer legitimately differs per node and per epoch. Applying is
/// replaying the log, and a replica must apply every committed entry
/// unconditionally — gating apply on a live placement view would make
/// the state machine diverge between nodes that learned a placement at
/// different times, which is a far worse failure than the one being
/// prevented.
///
/// # Safety
/// Caller must supply a valid `&SyscallTable` per the module ABI.
unsafe fn try_emit_keyed(
    sys: &SyscallTable,
    chan: i32,
    view: &edge::EdgeMap,
    shard_id: u32,
    payload: &[u8],
) -> bool {
    if chan < 0 {
        return false;
    }
    if !view.owns_shard(shard_id) {
        return false;
    }
    emit_keyed_unowned(sys, chan, view, shard_id, payload)
}

/// Emit a keyed proposal WITHOUT the ownership gate.
///
/// The gate exists to stop two PRGs accepting for one shard, and that is
/// the right rule for STATE — a session, a retained value, an offline
/// queue. A PUBLISH is not state this node owns; it is a message to be
/// ROUTED, and the shard key says where it must land, not who may accept
/// it. `partition_router` puts it on the topic's partition and exactly
/// one node applies and delivers it, so nothing is split.
///
/// Gating publishes on ownership was over-applying the rule, and it did
/// not fail loudly: a client publishing to a topic owned by another PRG
/// had the proposal REFUSED at propose, so the message never entered the
/// log and no node could deliver it. With QoS 0 there is no ack to
/// carry the refusal either — the publish simply vanished. Measured at
/// `prg_count = 3`: a subscriber received nothing even when its session
/// and the publisher shared a node.
unsafe fn emit_keyed_unowned(
    sys: &SyscallTable,
    chan: i32,
    view: &edge::EdgeMap,
    shard_id: u32,
    payload: &[u8],
) -> bool {
    if chan < 0 {
        return false;
    }
    // Ownership does not gate a routed message, but the FENCE does. A
    // fenced shard is mid-transfer: the losing owner has stopped
    // accepting and the gaining one has not started, so accepting at
    // EITHER end is the split ownership the fence exists to prevent.
    // Dropping this check let a publish through mid-migration —
    // `fence_gate.sh` caught it immediately.
    if view.is_shard_fenced(shard_id) {
        return false;
    }
    let total = (wire::ENVELOPE_HDR + wire::KEYED_PROPOSAL_HDR + payload.len()) as i32;
    wire::channel_write_keyed_proposal(sys, chan, shard_id, payload) == total
}

/// Resolve a stash slot. Returns `true` when the slot is now free; `false`
/// if the topic emit failed under backpressure and the slot waits, with
/// every stash ready after it, for [`release_ready_stashes`] to offer it
/// again.
///
/// If dedup said duplicate, the topic envelope is dropped (the publisher's
/// PUBACK still goes out — MQTT 3.1.1 §3.3.1.3 makes deduplication a
/// broker concern; clients always see ack).
///
/// # Safety
unsafe fn finalise_stash(s: &mut ModuleState, sys: &SyscallTable, stash_idx: usize) -> bool {
    let env_len = correlate::stash_env_len(&s.correlate, stash_idx);
    let state = correlate::stash_dedup_state(&s.correlate, stash_idx);
    if state == STASH_DEDUP_REFUSED {
        // The table could not file the key, so it cannot call this a
        // duplicate either. The entry is committed and the publisher
        // acknowledged; it fans out like a fresh one, counted.
        s.dedup_refused_delivered = s.dedup_refused_delivered.wrapping_add(1);
    }
    match state {
        STASH_DEDUP_OK | STASH_DEDUP_REFUSED if env_len >= 18 => {
            // Stash holds the QOP_PUBLISH V2 op-body:
            //   [pub_qos:u8][packet_id:u16 BE][stream_hash:u64 LE]
            //   [session_generation:u32 LE][retain:u8][topic_len:u16 BE]
            //   [topic][user_props_count:u8][per prop ...][payload]
            // Build the MSG_TOPIC_PUBLISH envelope topic_engine expects:
            //   [tenant:u32 LE][qos:u8][_pad:u8][topic_len:u16 LE][topic]
            //   [user_props_count:u8][per prop ...][payload]
            // The tenant is `TENANT_UNRESOLVED` (below).
            let stash_env = correlate::stash_env(&s.correlate, stash_idx);
            let pub_qos = stash_env[0];
            let topic_len = u16::from_be_bytes([stash_env[16], stash_env[17]]) as usize;
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
            // `TENANT_UNRESOLVED`.
            let tenant: TenantId = TENANT_UNRESOLVED;
            s.out_buf[0..4].copy_from_slice(&tenant.to_le_bytes());
            s.out_buf[4] = pub_qos;
            s.out_buf[5] = 0;
            s.out_buf[6..8].copy_from_slice(&(topic_len as u16).to_le_bytes());
            // topic + user_props + payload sit contiguous in the stash
            // starting at offset 18; a single copy reproduces the
            // downstream layout (offset 8 of out_buf).
            let src = correlate::stash_env(&s.correlate, stash_idx)
                .as_ptr()
                .add(18);
            let dst = s.out_buf.as_mut_ptr().add(8);
            core::ptr::copy_nonoverlapping(src, dst, topic_len + up_len + payload_len);
            if try_emit(
                sys,
                s.out_topic,
                wire::MSG_TOPIC_PUBLISH,
                &s.out_buf[..topic_pub_len],
            ) {
                correlate::stash_release(&mut s.correlate, stash_idx);
                true
            } else {
                // topic_engine.op_in saturated: kept, offered again in
                // its turn (`release_ready_stashes`).
                false
            }
        }
        STASH_DEDUP_DUPLICATE => {
            s.dedup_duplicates = s.dedup_duplicates.wrapping_add(1);
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

/// Release the stashes whose verdict and durability are both in, in the
/// order they became ready, until one finds the topic bus full: that one
/// waits, and so does every stash behind it, so publishes reach
/// `topic_engine` in commit order. Called whenever a stash becomes ready
/// and once a step for those the bus refused.
///
/// # Safety
/// `sys` must be the live kernel syscall table.
unsafe fn release_ready_stashes(s: &mut ModuleState, sys: &SyscallTable) {
    while let Some(i) = correlate::stash_oldest_ready(&s.correlate) {
        if !finalise_stash(s, sys, i) {
            return;
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum DeliverResult {
    Delivered,
    /// It cannot go yet, for the reason given: on something shared that
    /// clears without any client (the codec edge, admission credit, the
    /// seat ring's horizon), or on its session's own client (the flow
    /// credit, the receive maximum, its half of the in-flight table).
    Waits(order::Waits),
    /// Malformed or session-stale — cannot ever succeed; caller drops.
    Dropped,
}

/// Push a MSG_TOPIC_DELIVER envelope into the offline queue for replay on the
/// subscriber's next reconnect. Body shape: `[session:u32][env_len:u16][env]`,
/// through the outbox (`to_messaging`).
///
/// # Safety
unsafe fn enqueue_offline(s: &mut ModuleState, sys: &SyscallTable, session_slot: u32, env: &[u8]) {
    let env_len = env.len();
    if env_len > MAX_PENDING_DLV {
        return;
    }
    let total = 6 + env_len;
    let mut body = [0u8; 6 + MAX_PENDING_DLV];
    body[0..4].copy_from_slice(&session_slot.to_le_bytes());
    body[4..6].copy_from_slice(&(env_len as u16).to_le_bytes());
    body[6..total].copy_from_slice(env);
    to_messaging(s, sys, wire::MSG_OFFLINE_ENQUEUE, &body[..total]);
}

/// `try_deliver` for a messaging REPLY (an offline drain, a retained
/// read). Replies reach every worker on the node, so one for a session
/// whose attachment is frozen here — exported and not yet detached, or
/// imported and not yet resumed — belongs to the other worker and is
/// dropped rather than parked a second time. A topic delivery is
/// different: it is routed to exactly one worker, so a frozen one parks
/// it (see `try_deliver`).
///
/// # Safety
unsafe fn try_deliver_reply(s: &mut ModuleState, sys: &SyscallTable, env: &[u8]) -> DeliverResult {
    if env.len() >= 4 {
        let slot = u32::from_le_bytes([env[0], env[1], env[2], env[3]]) as usize;
        if slot < MAX_SESSIONS
            && sessions::is_active(&s.sessions, slot)
            && worker::is_frozen(&s.worker, sessions::conn_id(&s.sessions, slot))
        {
            s.worker.frozen_drops = s.worker.frozen_drops.wrapping_add(1);
            return DeliverResult::Dropped;
        }
    }
    try_deliver(s, sys, env)
}

/// A delivery's origin as the park records it.
const fn park_code(from: order::From) -> u8 {
    match from {
        order::From::Fresh => 0,
        order::From::Retained => 1,
        order::From::Drained => 2,
    }
}

/// The origin of the delivery parked in slot `k`.
fn parked_from(s: &ModuleState, k: usize) -> order::From {
    match correlate::dlv_kind(&s.correlate, k) {
        1 => order::From::Retained,
        2 => order::From::Drained,
        _ => order::From::Fresh,
    }
}

/// Offer `env`, one delivery of `from`, to its session: `ahead` of the
/// session's offline queue (a reply, or one parked before the queue took
/// the session's deliveries), or else behind it while it drains.
///
/// # Safety
/// `sys` must be the live kernel syscall table.
unsafe fn offer(
    s: &mut ModuleState,
    sys: &SyscallTable,
    env: &[u8],
    from: order::From,
    ahead: bool,
) -> DeliverResult {
    let reply = from != order::From::Fresh;
    s.ahead_of_backlog = ahead;
    let r = if reply {
        try_deliver_reply(s, sys, env)
    } else {
        try_deliver(s, sys, env)
    };
    s.ahead_of_backlog = false;
    r
}

/// Deliver `env` (a `MSG_TOPIC_DELIVER` payload, its session slot first)
/// in its session's order (`order`): a fresh one to a session whose
/// offline queue is draining joins the queue; any other goes behind what
/// its session has parked. One that waits on something shared parks; one
/// that waits on its own client joins its session's offline queue, with
/// what the session has parked ahead of it ([`spill_session`]). A drain's
/// answer always parks, in the slot its credit reserved. `false` only
/// when it could go nowhere: no park slot and no `messaging` to queue it.
///
/// # Safety
/// `sys` must be the live kernel syscall table.
unsafe fn deliver_in_order(
    s: &mut ModuleState,
    sys: &SyscallTable,
    env: &[u8],
    from: order::From,
) -> bool {
    if env.len() < 4 {
        return true;
    }
    let slot = u32::from_le_bytes([env[0], env[1], env[2], env[3]]);
    let parked = correlate::dlv_parked_for_slot(&s.correlate, slot);
    let r = match order::arrive(from, s.mflow.backlog(slot), parked) {
        order::Arrive::Offer { ahead } => offer(s, sys, env, from, ahead),
        // Behind its session's parked ones, whatever they wait on.
        order::Arrive::BehindParked => DeliverResult::Waits(order::Waits::Edge),
    };
    match r {
        DeliverResult::Delivered | DeliverResult::Dropped => true,
        DeliverResult::Waits(why) => match order::place(from, why, park_room(s) > 0) {
            order::Place::Park => {
                correlate::dlv_park(&mut s.correlate, env, park_code(from))
                    || spill_session(s, sys, slot, Some(env))
            }
            order::Place::Queue => spill_session(s, sys, slot, Some(env)),
        },
    }
}

/// Queue session `slot`'s deliveries that wait on its client on its
/// offline queue — first what it has parked, fresh or retained, oldest
/// first, then `env` — and drain the queue to it in turn: its later
/// deliveries join the queue behind them (`mflow`). A slot with no active
/// session takes `env` down the inactive path of [`try_deliver`]. `false`
/// with no `messaging` to queue it.
///
/// # Safety
/// `sys` must be the live kernel syscall table.
unsafe fn spill_session(
    s: &mut ModuleState,
    sys: &SyscallTable,
    slot: u32,
    env: Option<&[u8]>,
) -> bool {
    if s.out_messaging < 0 {
        return false;
    }
    if !sessions::is_active(&s.sessions, slot as usize) {
        return env.is_none_or(|e| !matches!(try_deliver(s, sys, e), DeliverResult::Waits(_)));
    }
    let mut order = [0usize; PENDING_DLV_SLOTS];
    let parked = correlate::dlv_order(&s.correlate, &mut order);
    let mut buf = [0u8; MAX_PENDING_DLV];
    for &k in &order[..parked] {
        if parked_from(s, k) == order::From::Drained {
            continue;
        }
        let n = correlate::dlv_len(&s.correlate, k);
        if n < 4 || !correlate::dlv_copy_out(&s.correlate, k, &mut buf) {
            continue;
        }
        if u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) == slot {
            enqueue_offline(s, sys, slot, &buf[..n]);
            correlate::dlv_release(&mut s.correlate, k);
            s.deliveries_spilled = s.deliveries_spilled.wrapping_add(1);
        }
    }
    if let Some(e) = env {
        enqueue_offline(s, sys, slot, e);
        s.deliveries_spilled = s.deliveries_spilled.wrapping_add(1);
    }
    want_drain(s, slot);
    true
}

/// Attempt one delivery of a single MSG_TOPIC_DELIVER envelope. Used both
/// for fresh inbound deliveries and for the defer-queue drain so the
/// backpressure path stays consistent.
///
/// # Safety
unsafe fn try_deliver(s: &mut ModuleState, sys: &SyscallTable, env: &[u8]) -> DeliverResult {
    let plen = env.len();
    if plen < 14 {
        return DeliverResult::Dropped;
    }

    let session_slot = u32::from_le_bytes([env[0], env[1], env[2], env[3]]) as usize;
    if session_slot >= MAX_SESSIONS {
        return DeliverResult::Dropped;
    }

    // Persisted session (clean_start=false, currently offline) → enqueue
    // for replay on reconnect. Treated as Delivered from the caller's
    // perspective: the message is now the offline queue's responsibility.
    if !sessions::is_active(&s.sessions, session_slot) {
        // Persisted (offline) — or lent to the other worker while its
        // subscriptions still point here: park it by slot, which the
        // holder's settle drain replays. Dropping it would lose a
        // committed publish to a live subscriber.
        if sessions::is_persisted(&s.sessions, session_slot)
            || sessions::is_lent(&s.sessions, session_slot)
        {
            enqueue_offline(s, sys, session_slot as u32, env);
            return DeliverResult::Delivered;
        }
        return DeliverResult::Dropped;
    }
    let conn_id = sessions::conn_id(&s.sessions, session_slot);
    // Exported and not yet detached — or just resumed and settling:
    // park it, keyed by the slot the session keeps; the importing
    // worker's settle drain replays the queue in order.
    if worker::is_frozen(&s.worker, conn_id) || worker::is_settling(&s.worker, conn_id) {
        enqueue_offline(s, sys, session_slot as u32, env);
        s.worker.frozen_parked = s.worker.frozen_parked.wrapping_add(1);
        return DeliverResult::Delivered;
    }
    // Its offline queue is draining: a fresh delivery joins it, behind
    // what is there, so the session receives them in order (`mflow`).
    if !s.ahead_of_backlog && s.mflow.backlog(session_slot as u32) {
        enqueue_offline(s, sys, session_slot as u32, env);
        s.mflow.joined(session_slot as u32);
        return DeliverResult::Delivered;
    }
    let sub_qos = env[4] & 0x03;

    let topic_len = u16::from_le_bytes([env[12], env[13]]) as usize;
    if 14 + topic_len > plen {
        return DeliverResult::Dropped;
    }
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
    if payload_start > plen {
        return DeliverResult::Dropped;
    }
    let payload_len = plen - payload_start;

    let pid_bytes = if sub_qos > 0 { 2 } else { 0 };
    let up_bytes = if up_len > 0 { up_len } else { 1 }; // placeholder for empty block
    let body_len = 2 + topic_len + pid_bytes + up_bytes + payload_len;
    if body_len > s.out_buf.len() {
        return DeliverResult::Dropped;
    }
    let body_cost = body_len as i32;

    if sub_qos > 0 {
        let credit = sessions::prefetch_credit(&s.sessions, session_slot);
        if credit > 0 && sessions::sub_outstanding(&s.sessions, session_slot) >= credit {
            return DeliverResult::Waits(order::Waits::Client);
        }
        // MQTT 5 §3.3.4 ReceiveMaximum — the client's hard cap on
        // concurrent unacked QoS 1+2 publishes. Separate from
        // prefetch_credit (which is operator-set / dynamic); the
        // effective cap is the tighter of the two when both are > 0.
        // The pending_dlv park path handles the retry on PUBACK.
        let rxmax = sessions::receive_maximum(&s.sessions, session_slot) as u32;
        if rxmax > 0 && sessions::sub_outstanding(&s.sessions, session_slot) >= rxmax {
            return DeliverResult::Waits(order::Waits::Client);
        }
    }
    if s.pid_entry_credits <= 0 || s.pid_byte_credits < body_cost {
        return DeliverResult::Waits(order::Waits::Edge);
    }
    // A session's deliveries leave half its in-flight table to its own
    // publishes, so a publish of the client's always finds a slot or one
    // that a commit will free (`codec_waits`), never only slots that wait
    // on acknowledgements still behind it in its stream.
    let mut flows = [gate::GateFlow::default(); MAX_INFLIGHT_PER_SESSION];
    let n = sessions::gate_flows(&s.sessions, session_slot, INFLIGHT_PUB, &mut flows);
    if !gate::delivery_may_take(sub_qos, &flows[..n], MAX_INFLIGHT_PER_SESSION) {
        return DeliverResult::Waits(order::Waits::Client);
    }
    // A seated subscriber takes a ring slot only when one is left beyond
    // those the answers to its publishes still being committed will need.
    if !seat_has_room(s, conn_id) {
        return DeliverResult::Waits(order::Waits::Edge);
    }

    let mut sub_packet_id: u16 = 0;
    if sub_qos > 0 {
        sub_packet_id = sessions::next_sub_packet_id(&mut s.sessions, session_slot);
        if sessions::inflight_add(
            &mut s.sessions,
            session_slot,
            sub_packet_id,
            sub_qos,
            INFLIGHT_SUB,
        )
        .is_none()
        {
            return DeliverResult::Waits(order::Waits::Client);
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
    let mut body = [0u8; BUF_SIZE];
    body[..body_len].copy_from_slice(&s.out_buf[..body_len]);
    let emitted = offer_codec_response(
        s,
        sys,
        conn_id,
        PROTO_MQTT,
        PKT_PUBLISH,
        flags,
        &body[..body_len],
    );
    if emitted {
        s.pid_entry_credits = s.pid_entry_credits.saturating_sub(1);
        s.pid_byte_credits = s.pid_byte_credits.saturating_sub(body_cost);
        if sub_qos > 0 {
            // What the subscriber is now owed goes into the log, so it
            // survives the worker that sent it.
            log_delivery(
                s,
                sys,
                session_slot,
                sub_packet_id,
                sub_qos,
                &body[..body_len],
            );
            sessions::note_delivery(&mut s.sessions, session_slot);
            let mut lag_msg = [0u8; 8];
            lag_msg[0..4].copy_from_slice(&(session_slot as u32).to_le_bytes());
            lag_msg[4..8].copy_from_slice(
                &sessions::sub_outstanding(&s.sessions, session_slot).to_le_bytes(),
            );
            try_emit(sys, s.out_forward, wire::MSG_LAG_SIGNAL, &lag_msg);
        }
        DeliverResult::Delivered
    } else {
        if sub_qos > 0 {
            if let Some(ii) =
                sessions::inflight_find(&s.sessions, session_slot, sub_packet_id, INFLIGHT_SUB)
            {
                sessions::inflight_release(&mut s.sessions, session_slot, ii);
            }
        }
        DeliverResult::Waits(order::Waits::Edge)
    }
}

/// Note one applied committed entry in its partition's term run. A run
/// the entry closes (the committed log changed term, or the index jumped)
/// is queued for `flow` at once. Partitions past the tracked range are not
/// reported: `flow` keeps no view of them either.
fn note_applied_entry(s: &mut ModuleState, partition_id: u16, index: u64, entry_term: u64) {
    let pi = partition_id as usize;
    if pi >= MAX_APPLY_PARTITIONS {
        return;
    }
    if let Some(closed) = s.applied_runs[pi].note(index, entry_term) {
        if !s.applied_spill.push(partition_id, closed) {
            s.applied_notice_drops = s.applied_notice_drops.wrapping_add(1);
        }
    }
}

/// Report the applied term runs to `flow` (MSG_ACK_APPLIED on the forward
/// bus): every closed run held in the spill, oldest first, whenever there
/// are any; each partition's open run once per [`APPLIED_NOTICE_MS`]. A
/// notice the edge refuses stays owed for the next step.
///
/// # Safety
/// `sys` must point at a live kernel syscall table.
unsafe fn flush_applied_runs(s: &mut ModuleState, sys: &SyscallTable, now: u64) {
    if s.out_forward < 0 {
        return;
    }
    while let Some((pid, run)) = s.applied_spill.front() {
        let body = term::encode_applied(pid, run);
        if !try_emit(sys, s.out_forward, wire::MSG_ACK_APPLIED, &body) {
            return;
        }
        s.applied_spill.pop_front();
    }
    if now.wrapping_sub(s.applied_notice_ms) < APPLIED_NOTICE_MS {
        return;
    }
    s.applied_notice_ms = now;
    for pi in 0..MAX_APPLY_PARTITIONS {
        let Some(run) = s.applied_runs[pi].owed() else {
            continue;
        };
        let body = term::encode_applied(pi as u16, run);
        if !try_emit(sys, s.out_forward, wire::MSG_ACK_APPLIED, &body) {
            // The edge is full: the rest stay owed, retried next step.
            s.applied_notice_ms = now.wrapping_sub(APPLIED_NOTICE_MS);
            return;
        }
        s.applied_runs[pi].mark_sent(run.last);
    }
}

/// Cursor slot for a partition id, saturating rather than panicking:
/// a `no_std` PIC module cannot unwind, so an out-of-range id must
/// degrade, not abort.
#[inline]
fn apply_slot(partition_id: u16) -> usize {
    (partition_id as usize).min(MAX_APPLY_PARTITIONS - 1)
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
unsafe fn apply_reset(
    s: &mut ModuleState,
    sys: &SyscallTable,
    partition_id: u16,
    reset_index: u64,
) {
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
        s.sub_book.clear(i as u32);
        sessions::clear_flow(&mut s.sessions, i);
    }
    s.session_count = 0;
    correlate::reset(&mut s.correlate);
    // The wipe is global — every arena above is shared across partitions —
    // so every cursor must restart too, or a partition whose cursor
    // survived would skip the entries needed to rebuild what was just
    // cleared. Only the partition that triggered the reset fast-forwards.
    s.apply_index = [0; MAX_APPLY_PARTITIONS];
    s.apply_index[apply_slot(partition_id)] = reset_index;

    // Fan the reset out to downstream modules. `out_topic` covers
    // topic_engine; `out_messaging` covers messaging's dedup, retained and
    // offline components (they share the messaging bus, demuxing by
    // msg_type). Each downstream handler is "wipe everything" until
    // snapshot install lands.
    let body = reset_index.to_le_bytes();
    try_emit(sys, s.out_topic, wire::MSG_APPLY_RESET_FANOUT, &body);
    to_messaging(s, sys, wire::MSG_APPLY_RESET_FANOUT, &body);
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
/// Whether committed op `op` names no session: a Kafka record batch, a
/// committed offset, a group's generation or member, an AMQP message.
/// Their state is the log's, not a session's, and every worker applies
/// them (`entry_is_mine`) — whatever `session_slot` the proposal carries,
/// which for these is a durable-publish inflight (`KAFKA_SLOT_BASE`) or 0.
fn op_names_no_session(op: u8) -> bool {
    matches!(
        op,
        wire::QOP_KAFKA_PRODUCE
            | wire::QOP_KAFKA_OFFSET
            | wire::QOP_KAFKA_GROUP_GEN
            | wire::QOP_KAFKA_GROUP_MEMBER
            | wire::QOP_AMQP_PUBLISH
    )
}

/// Whether this worker applies a committed entry of op `op` naming
/// `slot`.
///
/// One worker per node (the default, `co_located_workers == 0`) applies
/// everything — it must hold every session's durable record, as a Raft
/// follower does. When two workers share a node, each applies only the
/// entries of sessions it holds, or of slots it allocates and has not
/// lent out: an entry proposed by the other worker for a session it
/// owns must not create a second, persisted copy here, whose
/// subscriptions and offline queue would then shadow the live one.
///
/// An op that names no session (`op_names_no_session`: the Kafka log,
/// committed offsets, group records, AMQP messages) is applied by EVERY
/// worker, so a Kafka or AMQP connection is served by whichever worker
/// holds it: each one reads the same log, numbers it with the same
/// offsets and answers its own clients — a produce's origin names the
/// node AND the worker (`wire::QOP_KAFKA_PRODUCE`), and an inflight
/// slot's epoch names the worker (`store::inflight_alloc`), so only the
/// worker that accepted a request resolves it. Remote subscriptions
/// (`SESSION_SLOT_REMOTE`) and the slots past the session table still
/// belong to worker 0.
fn entry_is_mine(s: &ModuleState, op: u8, slot: u32) -> bool {
    if s.co_located_workers == 0 || op_names_no_session(op) {
        return true;
    }
    if slot == SESSION_SLOT_REMOTE || slot as usize >= MAX_SESSIONS {
        return s.worker_id == 0;
    }
    let si = slot as usize;
    sessions::holds(&s.sessions, si)
        || (sessions::in_alloc_range(&s.sessions, si) && !sessions::is_lent(&s.sessions, si))
}

unsafe fn apply_committed_op(
    s: &mut ModuleState,
    sys: &SyscallTable,
    p: &wire::PeeledQProp,
    body_start: usize,
    body_end: usize,
    now: u64,
) {
    match p.op {
        wire::QOP_CONNECT => {
            apply_qop_connect(s, sys, p.tenant, body_start, body_end, now, p.session_slot)
        }
        wire::QOP_DISCONNECT => apply_qop_disconnect(s, sys, p.tenant, body_start, body_end, now),
        wire::QOP_SUBSCRIBE => apply_qop_subscribe(s, sys, p.tenant, body_start, body_end),
        wire::QOP_UNSUBSCRIBE => apply_qop_unsubscribe(s, sys, p.tenant, body_start, body_end),
        wire::QOP_PUBREL => apply_qop_pubrel(s, sys, p.tenant, body_start, body_end),
        wire::QOP_PUBLISH => apply_qop_publish(s, sys, p.tenant, body_start, body_end, p.version),
        // Committed Kafka batches / AMQP messages land in the apply-side
        // message store, which Kafka Fetch and AMQP Basic.Get serve from.
        #[cfg(feature = "kafka")]
        wire::QOP_KAFKA_PRODUCE => {
            let body = core::slice::from_raw_parts(
                s.in_buf.as_ptr().add(body_start),
                body_end - body_start,
            );
            apply_kafka_produce(s, sys, body, p.session_slot);
        }
        #[cfg(feature = "amqp")]
        wire::QOP_AMQP_PUBLISH => {
            let body = core::slice::from_raw_parts(
                s.in_buf.as_ptr().add(body_start),
                body_end - body_start,
            );
            apply_amqp_publish(s, body, dev_millis(sys));
        }
        // Durable consumer-group offset commit: replay repopulates the
        // offsets table (leader stores at propose time too — idempotent).
        #[cfg(feature = "kafka")]
        wire::QOP_KAFKA_OFFSET => {
            let b = core::slice::from_raw_parts(
                s.in_buf.as_ptr().add(body_start),
                body_end - body_start,
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
                            b[po + 2],
                            b[po + 3],
                            b[po + 4],
                            b[po + 5],
                            b[po + 6],
                            b[po + 7],
                            b[po + 8],
                            b[po + 9],
                        ]);
                        consumers::offset_store(
                            &mut s.consumers,
                            &group[..gl],
                            &topic[..tl],
                            part,
                            off,
                        );
                    }
                }
            }
        }
        // Group membership, replicated so a coordinator change does not
        // force every group it hosted through a rebalance.
        #[cfg(feature = "kafka")]
        wire::QOP_KAFKA_GROUP_MEMBER => {
            let b = core::slice::from_raw_parts(
                s.in_buf.as_ptr().add(body_start),
                body_end - body_start,
            );
            if b.len() >= 3 {
                let gl = u16::from_le_bytes([b[0], b[1]]) as usize;
                if gl > 0 && gl <= KG_NAME && 2 + gl < b.len() {
                    let ml = b[2 + gl] as usize;
                    let mo = 3 + gl;
                    if ml > 0 && ml <= KG_NAME && mo + ml + 2 <= b.len() {
                        let ao = mo + ml;
                        let al = u16::from_le_bytes([b[ao], b[ao + 1]]) as usize;
                        if al <= KG_META && ao + 2 + al <= b.len() {
                            let mut group = [0u8; KG_NAME];
                            group[..gl].copy_from_slice(&b[2..2 + gl]);
                            let mut mid = [0u8; KG_NAME];
                            mid[..ml].copy_from_slice(&b[mo..mo + ml]);
                            let mut asg = [0u8; KG_META];
                            asg[..al].copy_from_slice(&b[ao + 2..ao + 2 + al]);
                            if !consumers::member_restore(
                                &mut s.consumers,
                                &group[..gl],
                                &mid[..ml],
                                &asg[..al],
                            ) {
                                s.kafka_group_gen_dropped =
                                    s.kafka_group_gen_dropped.wrapping_add(1);
                            }
                        }
                    }
                }
            }
        }
        // Group generation, replicated so a coordinator change cannot
        // reissue a generation a zombie consumer still holds.
        #[cfg(feature = "kafka")]
        wire::QOP_KAFKA_GROUP_GEN => {
            let b = core::slice::from_raw_parts(
                s.in_buf.as_ptr().add(body_start),
                body_end - body_start,
            );
            if b.len() >= 2 {
                let gl = u16::from_le_bytes([b[0], b[1]]) as usize;
                if gl > 0 && gl <= KG_NAME && 2 + gl + 4 <= b.len() {
                    let mut group = [0u8; KG_NAME];
                    group[..gl].copy_from_slice(&b[2..2 + gl]);
                    let go = 2 + gl;
                    let generation = i32::from_le_bytes([b[go], b[go + 1], b[go + 2], b[go + 3]]);
                    if !consumers::group_generation_raise(
                        &mut s.consumers,
                        &group[..gl],
                        generation,
                    ) {
                        s.kafka_group_gen_dropped = s.kafka_group_gen_dropped.wrapping_add(1);
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
/// path), bumps `session_generation`, and fans out the
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
    // Slot the PROPOSE side bound this connection to. Leader-local, so
    // it cannot be used for slot resolution on a follower or replay —
    // which is exactly why the apply path re-derives the slot from
    // `stream_hash` instead of trusting this one.
    _propose_slot: u32,
) {
    if body_end < body_start {
        return;
    }
    let body_len = body_end - body_start;
    // Minimum: clean_start(1) + keep_alive(2) + stream_hash(8) + cid_len(2) + protocol(1) = 14
    if body_len < 14 {
        return;
    }
    let clean_start = s.in_buf[body_start] != 0;
    let keep_alive_s =
        u16::from_be_bytes([s.in_buf[body_start + 1], s.in_buf[body_start + 2]]) as u32;
    let stream_hash = u64::from_le_bytes([
        s.in_buf[body_start + 3],
        s.in_buf[body_start + 4],
        s.in_buf[body_start + 5],
        s.in_buf[body_start + 6],
        s.in_buf[body_start + 7],
        s.in_buf[body_start + 8],
        s.in_buf[body_start + 9],
        s.in_buf[body_start + 10],
    ]);
    let cid_len =
        u16::from_be_bytes([s.in_buf[body_start + 11], s.in_buf[body_start + 12]]) as usize;
    if body_len < 14 + cid_len {
        return;
    }
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
                s.in_buf[q_off + 2],
                s.in_buf[q_off + 3],
                s.in_buf[q_off + 4],
                s.in_buf[q_off + 5],
            ]);
            let wt_len_off = q_off + 6;
            will_topic_len =
                u16::from_be_bytes([s.in_buf[wt_len_off], s.in_buf[wt_len_off + 1]]) as usize;
            will_topic_off = wt_len_off + 2;
            let wp_len_off = will_topic_off + will_topic_len;
            if wp_len_off + 2 <= body_end && will_topic_len <= MAX_WILL_TOPIC {
                will_payload_len =
                    u16::from_be_bytes([s.in_buf[wp_len_off], s.in_buf[wp_len_off + 1]]) as usize;
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
    } else {
        0
    };

    // ReceiveMaximum trailer (additive after session_expiry_s; older
    // proposals stop here and the field defaults to 0 → "no cap").
    let post_expiry_off = post_will_off + 4;
    let receive_maximum = if post_expiry_off + 2 <= body_end {
        u16::from_le_bytes([s.in_buf[post_expiry_off], s.in_buf[post_expiry_off + 1]])
    } else {
        0
    };

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
        s.apply_connect_no_prior = s.apply_connect_no_prior.wrapping_add(1);
        sessions::allocate(&mut s.sessions)
    };
    let Some(i) = session_idx else {
        return;
    };

    // clean_start with a prior slot wipes subscription / inflight /
    // prefetch state. Emit MSG_SESSION_DROP unconditionally on
    // clean_start so topic_engine's view stays consistent across
    // leader and followers (topic_engine tolerates spurious drops).
    if clean_start && prior.is_some() {
        let drop_body = (i as u32).to_le_bytes();
        try_emit(sys, s.out_topic, wire::MSG_SESSION_DROP, &drop_body);
        s.sub_book.clear(i as u32);
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
        &mut s.sessions,
        i,
        sessions::ConnectParams {
            tenant,
            stream_hash,
            shard: session_shard(
                tenant,
                &s.in_buf[body_start + 13..body_start + 13 + cid_len],
                stream_hash,
            ),
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
        // `commit_connect` has already made the record present, which is
        // what a clean-start follower relies on — it has no socket to be
        // active for and nothing to park, but its QoS 2 flows still need
        // to find the session.
        sessions::mark_persisted(&mut s.sessions, i);
    }

    // Resurrecting a persisted session under !clean_start: drain the
    // offline queue, on the node the client connected to (where the
    // session is active). A follower's slot is persisted, and a drain
    // there would only re-park every envelope where it was.
    if !clean_start && was_persisted {
        // Subscriber-side inflight cannot be redelivered — the slot
        // records no reference to the message — so releasing
        // it is strictly better than holding it: held, it keeps
        // counting toward ReceiveMaximum until the cap is reached and
        // the subscriber can never receive again. The messages are
        // already lost either way; this at least keeps the session
        // alive. Counted, so the loss is visible rather than silent.
        let dropped = sessions::release_sub_inflight(&mut s.sessions, i, INFLIGHT_SUB);
        if dropped > 0 {
            s.sub_inflight_dropped_on_resume =
                s.sub_inflight_dropped_on_resume.wrapping_add(dropped);
        }
        if sessions::is_active(&s.sessions, i) {
            want_drain(s, i as u32);
        }
    }

    // Will-message state (MQTT 3.1.1 §3.1.2.5). A fresh CONNECT
    // *replaces* the prior Will (even if the new CONNECT has no Will
    // — clearing is the correct behaviour per §3.1.2.5). `apply_qop_
    // disconnect` fires this publish when the reason isn't CLEAN.
    if will_present {
        let mut wt = [0u8; MAX_WILL_TOPIC];
        let mut wp = [0u8; MAX_WILL_PAYLOAD];
        core::ptr::copy_nonoverlapping(
            s.in_buf.as_ptr().add(will_topic_off),
            wt.as_mut_ptr(),
            will_topic_len,
        );
        core::ptr::copy_nonoverlapping(
            s.in_buf.as_ptr().add(will_payload_off),
            wp.as_mut_ptr(),
            will_payload_len,
        );
        sessions::set_will(
            &mut s.sessions,
            i,
            will_qos,
            will_retain != 0,
            will_delay_s.saturating_mul(1000),
            &wt[..will_topic_len],
            &wp[..will_payload_len],
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
unsafe fn fire_will(s: &mut ModuleState, sys: &SyscallTable, tenant: TenantId, i: usize) {
    let Some(will) = sessions::will_view(&s.sessions, i) else {
        return;
    };
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
    // Will-fired publishes carry no user properties (the Will
    // properties in CONNECT are not stored on the Session), so the
    // user_props block is a single zero byte.
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
        try_emit(
            sys,
            s.out_topic,
            wire::MSG_TOPIC_PUBLISH,
            &s.out_buf[..dlv_total],
        );
    }
    // Retain=1 Will latches in the retained store the same way as an
    // ordinary retained PUBLISH (item 2's wire format).
    if retain {
        let topic_hash = wire::fnv1a_64(&will_topic[..topic_len]);
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
            s.out_buf[pl_off..pl_off + 4].copy_from_slice(&(payload_len as u32).to_le_bytes());
            core::ptr::copy_nonoverlapping(
                will_payload.as_ptr(),
                s.out_buf.as_mut_ptr().add(pl_off + 4),
                payload_len,
            );
            out_buf_to_messaging(s, sys, wire::MSG_RETAINED_WRITE, rwrite_len);
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
    if body_end < body_start {
        return;
    }
    let body_len = body_end - body_start;
    if body_len < 9 {
        return;
    }
    let reason = s.in_buf[body_start];
    let stream_hash = u64::from_le_bytes([
        s.in_buf[body_start + 1],
        s.in_buf[body_start + 2],
        s.in_buf[body_start + 3],
        s.in_buf[body_start + 4],
        s.in_buf[body_start + 5],
        s.in_buf[body_start + 6],
        s.in_buf[body_start + 7],
        s.in_buf[body_start + 8],
    ]);
    let Some(i) = sessions::find_by_stream(&s.sessions, tenant, stream_hash) else {
        return;
    };
    // The slot has been taken over: a CONNECT for the same client id
    // was admitted after this disconnect was proposed and is waiting
    // for its own commit. The connection this entry ends is already
    // gone from the slot; closing it now would end the NEW
    // connection's session under it — a client that disconnects and
    // reconnects at once (one `mosquitto_pub` per message) then
    // publishes into nothing. The takeover's own CONNECT commit
    // carries the session forward.
    if sessions::is_transient(&s.sessions, i) {
        s.applied = s.applied.wrapping_add(1);
        return;
    }

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
        s.sub_book.clear(i as u32);
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
    if body_end < body_start {
        return;
    }
    let body_len = body_end - body_start;
    // [req_qos:u8][stream_hash:u64 LE][topic_len:u16 BE][topic]
    if body_len < 1 + 8 + 2 {
        return;
    }
    let req_qos = s.in_buf[body_start];
    let stream_hash = u64::from_le_bytes([
        s.in_buf[body_start + 1],
        s.in_buf[body_start + 2],
        s.in_buf[body_start + 3],
        s.in_buf[body_start + 4],
        s.in_buf[body_start + 5],
        s.in_buf[body_start + 6],
        s.in_buf[body_start + 7],
        s.in_buf[body_start + 8],
    ]);
    let topic_len =
        u16::from_be_bytes([s.in_buf[body_start + 9], s.in_buf[body_start + 10]]) as usize;
    if body_len < 11 + topic_len {
        return;
    }
    let topic_off = body_start + 11;

    // A subscription is recorded on the TOPIC's PRG as well as the
    // session's.
    //
    // Publishes route by topic, so the node that must MATCH a
    // subscription against a publish is the topic's owner. Recording
    // only where the session lives leaves that node with nothing to
    // match, and at `prg_count > 1` the two are usually different
    // nodes — so the subscriber receives nothing.
    //
    // This handler is apply-side and runs on every replica, so the
    // topic's owner reaches this point too; it simply has no local
    // session slot. `session_slot` is therefore `SESSION_SLOT_REMOTE`
    // there, and `stream_hash` — which the proposal already carries — is
    // what names the subscriber across nodes.
    let local_session = sessions::find_by_stream(&s.sessions, tenant, stream_hash);
    let owns_topic = s.view.owns_shard(wire::shard_mqtt_topic(
        tenant,
        &s.in_buf[topic_off..topic_off + topic_len],
    ));
    // Recorded on BOTH the topic's owner and the session's owner, and
    // nowhere else. Each needs it for a different half of the delivery:
    // the topic's owner is where publishes arrive and must MATCH, and
    // the session's owner is the only node that can actually write to
    // the subscriber's socket. At `prg_count == 1` they are the same
    // node and the two conditions collapse into one.
    if !owns_topic && local_session.is_none() {
        return;
    }
    let session_slot = match local_session {
        Some(i) => i as u32,
        None => SESSION_SLOT_REMOTE,
    };
    if let Some(i) = local_session {
        // The worker's own book of the subscription, for a site export.
        let filter = core::slice::from_raw_parts(s.in_buf.as_ptr().add(topic_off), topic_len);
        s.sub_book.subscribe(i as u32, req_qos, filter);
    }

    // MSG_TOPIC_SUBSCRIBE body:
    //   [tenant:u32 LE][session_slot:u32 LE][req_qos:u8][worker:u8]
    //   [topic_len:u16 LE][topic][stream_hash:u64 LE]
    // `stream_hash` is appended LAST so the prefix is byte-identical to
    // the shape topic_engine already parsed.
    let sub_len = 4 + 4 + 1 + 1 + 2 + topic_len + 8;
    if sub_len <= s.out_buf.len() {
        s.out_buf[0..4].copy_from_slice(&tenant.to_le_bytes());
        s.out_buf[4..8].copy_from_slice(&session_slot.to_le_bytes());
        s.out_buf[8] = req_qos;
        // The worker holding the session: topic_engine delivers to its
        // port (MSG_TOPIC_MOVE re-points it when the session moves).
        s.out_buf[9] = s.worker_id;
        s.out_buf[10..12].copy_from_slice(&(topic_len as u16).to_le_bytes());
        let src = s.in_buf.as_ptr().add(topic_off);
        s.out_buf[12 + topic_len..12 + topic_len + 8].copy_from_slice(&stream_hash.to_le_bytes());
        let dst = s.out_buf.as_mut_ptr().add(12);
        core::ptr::copy_nonoverlapping(src, dst, topic_len);
        try_emit(
            sys,
            s.out_topic,
            wire::MSG_TOPIC_SUBSCRIBE,
            &s.out_buf[..sub_len],
        );
    }

    // Retained delivery, for a subscriber this worker serves: its retained
    // read is filed (`mflow`) and asked of `messaging` with the credit the
    // park can give it, as many times as it takes — `messaging` answers at
    // most that many matching entries and says where its scan stopped. Each
    // answer is run through `try_deliver` with its own delivery envelope.
    if let Some(i) = local_session.filter(|_| s.out_messaging >= 0) {
        if sessions::is_active(&s.sessions, i) {
            let pattern = core::slice::from_raw_parts(s.in_buf.as_ptr().add(topic_off), topic_len);
            if !s
                .mflow
                .read_add(i as u32, stream_hash, tenant, req_qos, pattern)
            {
                s.retained_reads_refused = s.retained_reads_refused.wrapping_add(1);
            }
        }
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
    if body_end < body_start {
        return;
    }
    let body_len = body_end - body_start;
    // [stream_hash:u64 LE][topic_len:u16 BE][topic]
    if body_len < 8 + 2 {
        return;
    }
    let stream_hash = u64::from_le_bytes([
        s.in_buf[body_start],
        s.in_buf[body_start + 1],
        s.in_buf[body_start + 2],
        s.in_buf[body_start + 3],
        s.in_buf[body_start + 4],
        s.in_buf[body_start + 5],
        s.in_buf[body_start + 6],
        s.in_buf[body_start + 7],
    ]);
    let topic_len =
        u16::from_be_bytes([s.in_buf[body_start + 8], s.in_buf[body_start + 9]]) as usize;
    if body_len < 10 + topic_len {
        return;
    }
    let topic_off = body_start + 10;
    let Some(i) = sessions::find_by_stream(&s.sessions, tenant, stream_hash) else {
        return;
    };
    let session_slot = i as u32;
    {
        let filter = core::slice::from_raw_parts(s.in_buf.as_ptr().add(topic_off), topic_len);
        s.sub_book.unsubscribe(session_slot, filter);
    }

    // MSG_TOPIC_UNSUBSCRIBE body:
    //   [tenant:u32 LE][session_slot:u32 LE][topic_len:u16 LE][topic]
    let unsub_len = 4 + 4 + 2 + topic_len;
    if unsub_len <= s.out_buf.len() {
        s.out_buf[0..4].copy_from_slice(&tenant.to_le_bytes());
        s.out_buf[4..8].copy_from_slice(&session_slot.to_le_bytes());
        s.out_buf[8..10].copy_from_slice(&(topic_len as u16).to_le_bytes());
        let src = s.in_buf.as_ptr().add(topic_off);
        let dst = s.out_buf.as_mut_ptr().add(10);
        core::ptr::copy_nonoverlapping(src, dst, topic_len);
        try_emit(
            sys,
            s.out_topic,
            wire::MSG_TOPIC_UNSUBSCRIBE,
            &s.out_buf[..unsub_len],
        );
    }
    s.applied = s.applied.wrapping_add(1);
}

/// Apply-side handler for QOP_PUBREL. Records the committed QoS 2
/// phase transition on both durable records: the publisher inflight
/// the state machine reads, and the identity-keyed dedupe entry that
/// outlives it. The leader's propose side already set the inflight to
/// QOS2_PUBREL; a follower or a post-restart replay reaches the same
/// phase from this entry alone, creating the inflight if the flow
/// reached it without one. MSG_ACK_EMIT then fires PUBCOMP once
/// durability lands.
///
/// Both mutations are idempotent: re-applying a committed PUBREL —
/// replay, a client retry, a duplicate after the slot was released and
/// reused — leaves the phase where it already was.
///
/// # Safety
/// Caller must hold an exclusive `&mut ModuleState`, supply a valid
/// `&SyscallTable`, and guarantee
/// `body_start <= body_end <= s.in_buf.len()`.
unsafe fn apply_qop_pubrel(
    s: &mut ModuleState,
    sys: &SyscallTable,
    tenant: TenantId,
    body_start: usize,
    body_end: usize,
) {
    if body_end < body_start {
        return;
    }
    let body_len = body_end - body_start;
    // [packet_id:u16 BE][stream_hash:u64 LE][session_generation:u32 LE]
    //
    // The epoch is a trailing field. Entries logged before it existed
    // are 10 bytes and replay with `logged_epoch == 0`, which falls back
    // to the session's current epoch exactly as they did when written.
    if body_len < 10 {
        return;
    }
    let packet_id = u16::from_be_bytes([s.in_buf[body_start], s.in_buf[body_start + 1]]);
    let stream_hash = u64::from_le_bytes([
        s.in_buf[body_start + 2],
        s.in_buf[body_start + 3],
        s.in_buf[body_start + 4],
        s.in_buf[body_start + 5],
        s.in_buf[body_start + 6],
        s.in_buf[body_start + 7],
        s.in_buf[body_start + 8],
        s.in_buf[body_start + 9],
    ]);
    let logged_epoch = if body_len >= 14 {
        u32::from_le_bytes([
            s.in_buf[body_start + 10],
            s.in_buf[body_start + 11],
            s.in_buf[body_start + 12],
            s.in_buf[body_start + 13],
        ])
    } else {
        0
    };
    let Some(i) = sessions::find_by_stream(&s.sessions, tenant, stream_hash) else {
        return;
    };
    let ii = match sessions::inflight_find(&s.sessions, i, packet_id, INFLIGHT_PUB) {
        Some(ii) => Some(ii),
        None => sessions::inflight_add(&mut s.sessions, i, packet_id, QOS_2, INFLIGHT_PUB),
    };
    // The epoch the flow opened under names its dedupe record. The
    // logged value is authoritative: it is what the PUBLISH keyed on,
    // and it survives promotion, where this node's own inflight table
    // and current session epoch may both differ from the leader's.
    let mut flow_epoch = if logged_epoch != 0 {
        logged_epoch
    } else {
        sessions::generation(&s.sessions, i)
    };
    if let Some(ii) = ii {
        sessions::inflight_set_phase(&mut s.sessions, i, ii, QOS2_PUBREL);
        // Only consulted for entries logged without an epoch: this
        // node's inflight table is local state a promoted follower may
        // not share.
        if logged_epoch == 0 {
            if let Some(v) = sessions::inflight_view(&s.sessions, i, ii) {
                if v.session_generation != 0 {
                    flow_epoch = v.session_generation;
                }
            }
        }
    }

    let mut ph = [0u8; wire::DEDUP_KEY_LEN + 1];
    wire::encode_dedup_key(
        &mut ph[..wire::DEDUP_KEY_LEN],
        tenant,
        stream_hash,
        flow_epoch,
        packet_id as u32,
    );
    ph[wire::DEDUP_KEY_LEN] = QOS2_PUBREL;
    to_messaging(s, sys, wire::MSG_DEDUP_PHASE, &ph);

    s.applied = s.applied.wrapping_add(1);
}

/// Tell `durability` the lowest raft index this broker still needs.
///
/// Kafka retention and raft compaction are two lifetimes over ONE set of
/// bytes: the Kafka log IS the raft WAL, and compaction is driven by
/// snapshots, which know nothing about a consumer's retention window.
/// Without this a snapshot retires segments a consumer still inside its
/// retention window can ask for, and the cold-read path then has
/// nothing to read — the failure is silent, because an empty fetch and
/// a caught-up fetch look identical.
///
/// OPT-IN, and deliberately so. With no retention policy configured the
/// broker needs every record it has ever applied, so an honest floor
/// would be "compact nothing" — which would grow the WAL without bound
/// on every deployment that never asked for retention. No policy, no
/// floor: `retention_floor_allows` permits any snapshot when the floor
/// set is empty, which is exactly today's behaviour.
///
/// One floor per raft partition. Raft indexes are per-log — each
/// partition numbers from 1 — and every anchor records the partition
/// that carried it, so the floor for a partition is computed over that
/// partition's anchors alone and published on the partitioned envelope
/// to exactly that partition's WAL.
///
/// # Safety
/// Caller must supply a valid `&SyscallTable` per the module ABI.
#[cfg(feature = "kafka")]
unsafe fn publish_retention_floor(s: &mut ModuleState, sys: &SyscallTable) {
    if s.out_retention_floor < 0 || s.kafka_retention_ms == 0 {
        return;
    }
    let mut seen = store::raft_partitions_seen(&s.store);
    while seen != 0 {
        let partition = seen.trailing_zeros() as u16;
        seen &= seen - 1;
        let slot = apply_slot(partition);
        let Some(oldest) = store::oldest_needed_raft_index(&s.store, partition) else {
            continue;
        };
        // The highest index safe to compact at is one BELOW the oldest
        // still needed. An off-by-one here deletes the very entry a
        // cold read is about to want.
        let floor = oldest.saturating_sub(1);
        // A floor of 0 is NOT "nothing to say" — it is "I need everything
        // from index 1, so nothing may be retired". Hence a sent mask
        // rather than reading an initial 0 as already-published.
        if s.floor_sent_mask & (1u64 << slot) != 0 && floor == s.last_floor_sent[slot] {
            continue;
        }
        let poll = (sys.channel_poll)(s.out_retention_floor, 0x02);
        if poll <= 0 || (poll as u32 & 0x02) == 0 {
            return;
        }
        let mut buf = [0u8; 10];
        buf[0..2].copy_from_slice(&partition.to_le_bytes());
        buf[2..10].copy_from_slice(&floor.to_le_bytes());
        if wire::channel_write_partitioned(
            sys,
            s.out_retention_floor,
            partition,
            wire::MSG_COMPACTION_FLOOR,
            &buf,
        ) > 0
        {
            s.last_floor_sent[slot] = floor;
            s.floor_sent_mask |= 1u64 << slot;
            s.floor_emitted = s.floor_emitted.wrapping_add(1);
        }
    }
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
    if body_end < body_start {
        return;
    }
    let body_len = body_end - body_start;
    // QOP_PUBLISH op-body: 18-byte fixed header + topic [+ V2 user_props block] + payload.
    if body_len < 18 {
        return;
    }
    let pub_qos = s.in_buf[body_start] & 0x03;
    let packet_id = u16::from_be_bytes([s.in_buf[body_start + 1], s.in_buf[body_start + 2]]);
    let stream_hash = u64::from_le_bytes([
        s.in_buf[body_start + 3],
        s.in_buf[body_start + 4],
        s.in_buf[body_start + 5],
        s.in_buf[body_start + 6],
        s.in_buf[body_start + 7],
        s.in_buf[body_start + 8],
        s.in_buf[body_start + 9],
        s.in_buf[body_start + 10],
    ]);
    let session_generation = u32::from_le_bytes([
        s.in_buf[body_start + 11],
        s.in_buf[body_start + 12],
        s.in_buf[body_start + 13],
        s.in_buf[body_start + 14],
    ]);
    let retain = s.in_buf[body_start + 15] != 0;
    let topic_len =
        u16::from_be_bytes([s.in_buf[body_start + 16], s.in_buf[body_start + 17]]) as usize;
    if body_len < 18 + topic_len {
        return;
    }
    let topic_off = body_start + 18;

    // V2 interleaves a user_props block between topic and payload.
    // V1 (replayed from older WALs) has payload immediately after
    // topic, equivalent to "user_props_count == 0".
    let (up_off, up_len) = if version == wire::QPROP_VERSION_V2 {
        let up_off = topic_off + topic_len;
        let slice_end = body_start + body_len;
        if up_off > slice_end {
            return;
        }
        let n = user_props_block_len(&s.in_buf[up_off..slice_end]).unwrap_or(0);
        if n == 0 || up_off + n > slice_end {
            return;
        }
        (up_off, n)
    } else {
        (topic_off + topic_len, 0)
    };
    let payload_off = up_off + up_len;
    if payload_off > body_start + body_len {
        return;
    }
    let payload_len = body_len - 18 - topic_len - up_len;

    // topic_hash is a u64 value — no s.in_buf borrow held after this
    // call, so subsequent mutations to s.out_buf / the stash are safe.
    let topic_hash = wire::fnv1a_64(&s.in_buf[topic_off..topic_off + topic_len]);

    // QoS 2 owes the publisher a PUBREC now and a PUBCOMP after its
    // PUBREL commits, so the transaction is live from this entry until
    // that PUBREL applies. The inflight slot is where the QoS 2 state
    // machine reads its phase, so every node builds one here: the
    // leader already holds the slot its propose side opened, while a
    // follower or a post-restart replay creates it in the PUBLISH
    // phase. Without that, a promoted follower — or this node after a
    // restart — has no record that the transaction is open and cannot
    // answer the PUBREL the publisher still owes.
    if pub_qos == 2 {
        if let Some(i) = sessions::find_by_stream(&s.sessions, tenant, stream_hash) {
            let ii = match sessions::inflight_find(&s.sessions, i, packet_id, INFLIGHT_PUB) {
                Some(ii) => Some(ii),
                None => {
                    sessions::inflight_add(&mut s.sessions, i, packet_id, pub_qos, INFLIGHT_PUB)
                }
            };
            if let Some(ii) = ii {
                // The epoch this publish named is the one its dedupe
                // record is keyed by, and a reconnect mid-transaction
                // moves the session's own epoch on.
                sessions::inflight_set_generation(&mut s.sessions, i, ii, session_generation);
            }
        }
    }

    // For QoS 1+, look up correlation_id from the leader's publisher
    // inflight (propose-side allocates correlation + inflight together).
    // A follower's reconstructed inflight carries no correlation, so
    // correlation_id stays 0 and QoS 1+ falls back to the direct emit
    // path — leader fan-out is still gated through the stash via the
    // correlation_id != 0 branch.
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

    // The dedupe entry is the identity-keyed durable record of the
    // flow: it outlives the inflight slot and survives its reuse, and
    // for QoS 2 it carries the phase. Both the leader and the nodes
    // that only apply record it, so the entry a promoted follower
    // holds is the same one the leader had.
    let mut dkey = [0u8; wire::DEDUP_KEY_LEN];
    if pub_qos > 0 {
        wire::encode_dedup_key(
            &mut dkey,
            tenant,
            stream_hash,
            session_generation,
            packet_id as u32,
        );
    }

    // The check record carries the committed QoS 2 phase, so filing the
    // entry and recording its phase are one write rather than two
    // independently-droppable ones. Recording is monotone, so replay and
    // retries land on the same phase.
    let mut check = [0u8; wire::DEDUP_KEY_LEN + 1];
    check[..wire::DEDUP_KEY_LEN].copy_from_slice(&dkey);
    check[wire::DEDUP_KEY_LEN] = if pub_qos == 2 {
        QOS2_PUBLISH
    } else {
        wire::DEDUP_PHASE_NONE
    };

    if pub_qos == 0 {
        // QoS 0 carries no duplicate contract, so it fans out directly
        // with no dedupe round-trip.
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
            try_emit(
                sys,
                s.out_topic,
                wire::MSG_TOPIC_PUBLISH,
                &s.out_buf[..topic_pub_len],
            );
        }
        // QoS 1+ reaching this branch is a node that only applies:
        // record the dedupe entry it would otherwise never see, so its
        // duplicate verdict — and, for QoS 2, its phase — matches the
        // leader's if this node is promoted.
        if pub_qos > 0 {
            to_messaging(s, sys, wire::MSG_DEDUP_CHECK, &check);
        }
    } else {
        // QoS 1+ takes the same route on every node: stash the op-body,
        // mark it durable — apply means committed — and file the check.
        // finalise_stash fans out on the OK verdict and drops on
        // DUPLICATE. A node applying without a client correlation
        // stashes under correlation id 0; the verdict is looked up by
        // dedupe key, so nothing here needs one. Routing followers
        // through the same decision is what makes the duplicate
        // suppression broker-wide rather than leader-only.
        if body_len <= MAX_STASH_ENV {
            if let Some(stash_idx) = correlate::stash_alloc(&mut s.correlate, correlation_id, &dkey)
            {
                let env = core::slice::from_raw_parts(s.in_buf.as_ptr().add(body_start), body_len);
                correlate::stash_set_env(&mut s.correlate, stash_idx, env);
                correlate::stash_mark_durable(&mut s.correlate, stash_idx);
                to_messaging(s, sys, wire::MSG_DEDUP_CHECK, &check);
            }
            // The apply path reads an entry only with a stash slot free
            // (`outputs_ready_for_apply`), so this always finds one.
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
            out_buf_to_messaging(s, sys, wire::MSG_RETAINED_WRITE, rwrite_len);
        }
    }

    s.applied = s.applied.wrapping_add(1);
}

/// PIC module ABI entry: run one scheduler step against this instance.
///
/// # Safety
/// `state` is the kernel-owned buffer a prior `module_new` initialised, and is
/// exclusively borrowed for the duration of the call.
#[no_mangle]
#[link_section = ".text.module_step"]
pub unsafe extern "C" fn module_step(state: *mut u8) -> i32 {
    unsafe {
        let s = &mut *(state as *mut ModuleState);
        let sys = &*s.syscalls;
        let mut worked = 0u32;
        let now = dev_millis(sys);
        if s.faulted != 0 {
            return fault_drain(s, sys);
        }
        // The outbox toward `messaging` drains first, so every room check
        // this step sees what the op edge took; so do the held client
        // responses, so every input gate sees what the codec bus took.
        flush_messaging(s, sys);
        flush_holds(s, sys);
        #[cfg(any(feature = "kafka", feature = "amqp"))]
        answer_owed_inflights(s, sys);

        // ── Phase 1: drain flow signals (wire envelope) ──────────
        // flow_in is shared by admission (MSG_THROTTLE_CREDITS),
        // flow's backpressure (MSG_BP_SIGNAL) and prefetch
        // (MSG_PREFETCH_CREDIT). Demultiplex by msg_type.
        if s.in_flow >= 0 {
            loop {
                let poll = (sys.channel_poll)(s.in_flow, 0x01);
                if poll <= 0 || (poll as u32 & 0x01) == 0 {
                    break;
                }
                let (mt, plen) = {
                    worked += 1;
                    wire::channel_read_msg(sys, s.in_flow, &mut s.in_buf)
                };
                match mt {
                    wire::MSG_THROTTLE_CREDITS if plen >= 8 => {
                        s.pid_entry_credits = i32::from_le_bytes([
                            s.in_buf[0],
                            s.in_buf[1],
                            s.in_buf[2],
                            s.in_buf[3],
                        ]);
                        s.pid_byte_credits = i32::from_le_bytes([
                            s.in_buf[4],
                            s.in_buf[5],
                            s.in_buf[6],
                            s.in_buf[7],
                        ]);
                    }
                    wire::MSG_BP_SIGNAL if plen >= 9 => {
                        // [reason:u8][entry_credits:i32 LE][byte_credits:i32 LE]
                        // Treat as a backpressure-flavoured credit update.
                        s.pid_entry_credits = i32::from_le_bytes([
                            s.in_buf[1],
                            s.in_buf[2],
                            s.in_buf[3],
                            s.in_buf[4],
                        ]);
                        s.pid_byte_credits = i32::from_le_bytes([
                            s.in_buf[5],
                            s.in_buf[6],
                            s.in_buf[7],
                            s.in_buf[8],
                        ]);
                    }
                    wire::MSG_PREFETCH_CREDIT if plen >= 8 => {
                        // [session_slot:u32 LE][credit:u32 LE]
                        // credit is an absolute cap on outstanding (unacked)
                        // deliveries to this subscriber. Do not reset
                        // sub_outstanding here — that would defeat the
                        // backlog measurement; the consumer drains it via
                        // PUBACK as deliveries clear.
                        let slot = u32::from_le_bytes([
                            s.in_buf[0],
                            s.in_buf[1],
                            s.in_buf[2],
                            s.in_buf[3],
                        ]) as usize;
                        let credit = u32::from_le_bytes([
                            s.in_buf[4],
                            s.in_buf[5],
                            s.in_buf[6],
                            s.in_buf[7],
                        ]);
                        if slot < MAX_SESSIONS {
                            sessions::set_prefetch_credit(&mut s.sessions, slot, credit);
                        }
                    }
                    _ => {}
                }
            }
        }

        // ── Phase 1b: SessionCtrlV1 from the anchor ──
        worked += worker::handle_ctrl(s, sys, now);

        // ── Phase 2: process codec proposals (envelope-framed) ──
        //
        // Wire format on codec_in: wire envelope `[mtype:u8][len:u16 LE][payload]`
        // where `payload = [conn_id:u16 LE][proto:u8][pkt_type:u8][flags:u8][body]`.
        // Codecs (mqtt, amqp, kafka) write via `channel_write_msg`; the
        // envelope length prefix is what lets the consumer demarcate each
        // message when several writers fan in or one writer bursts multiple
        // packets within a tick.
        if s.in_codec >= 0 || s.in_codec2 >= 0 || s.in_front >= 0 {
            // 32 packets/tick: at the apply domain's 250 µs tick this admits
            // ~128k requests/s before the codec_in drain — not the session
            // engine — becomes the pipeline's rate limiter. A smaller quota
            // caps pipelined Kafka produce bursts below the WAL's batch
            // capacity.
            // A held proposal goes first, and no packet is taken off the
            // channel until it lands: the client's next packet may be
            // the same session's, and order on a session is what the
            // ack path keys by.
            if s.prop_hold_len > 0 {
                let len = s.prop_hold_len as usize;
                let mut held = [0u8; BUF_SIZE];
                held[..len].copy_from_slice(&s.prop_hold[..len]);
                if emit_keyed_unowned(
                    sys,
                    s.prop_hold_chan,
                    &s.view,
                    s.prop_hold_shard,
                    &held[..len],
                ) {
                    s.prop_hold_len = 0;
                    s.proposals_emitted = s.proposals_emitted.wrapping_add(1);
                    s.hb_prop = s.hb_prop.wrapping_add(1);
                }
            }
            let codec_budget = if s.prop_hold_len > 0 { 0 } else { 32 };
            for _ in 0..codec_budget {
                // Set below for the one envelope that is a coordinator
                // call, read by every answer it makes.
                s.coord_reply = false;
                // Admission gate BEFORE the read. A packet taken off the
                // channel with no credit to propose it has nowhere to go
                // but the floor, and a QoS 1 PUBLISH on the floor is one
                // the client never hears back about. Leaving it in the
                // channel instead lets the codec, the router and finally
                // the client's TCP window carry the pressure until
                // `admission` refills the credit. The gate is the
                // substrate's entry cap, the most any one packet can
                // cost.
                if s.pid_entry_credits <= 0 || s.pid_byte_credits < ADMISSION_GATE_BYTES {
                    s.codec_gated = s.codec_gated.wrapping_add(1);
                    if s.codec_gated & 0x3FF == 1 {
                        dev_log(sys, 2, b"[sess] codec gated: no credit".as_ptr(), 28);
                    }
                    break;
                }
                // A held envelope whose wait is over goes first, ahead of
                // anything else from its input; an input whose held one
                // still waits is not read.
                let released = (0..CODEC_INPUTS).find(|&k| {
                    let n = usize::from(s.codec_hold_len[k]);
                    n > 0 && !codec_waits(s, s.codec_hold_mt[k], &s.codec_hold[k][..n])
                });
                let (chan, mut mt, mut plen) = if let Some(k) = released {
                    let n = usize::from(s.codec_hold_len[k]);
                    s.in_buf[..n].copy_from_slice(&s.codec_hold[k][..n]);
                    s.codec_hold_len[k] = 0;
                    worked += 1;
                    let chan = [s.in_codec, s.in_codec2, s.in_front][k];
                    (chan, s.codec_hold_mt[k], n as u16)
                } else {
                    let Some(chan) = next_codec_input(s, sys) else {
                        break;
                    };
                    worked += 1;
                    s.hb_codec = s.hb_codec.wrapping_add(1);
                    let (mt, plen) = wire::channel_read_msg(sys, chan, &mut s.in_buf);
                    let n = usize::from(plen);
                    if n <= BUF_SIZE && codec_waits(s, mt, &s.in_buf[..n]) {
                        let k = codec_input(s, chan);
                        s.codec_hold[k][..n].copy_from_slice(&s.in_buf[..n]);
                        s.codec_hold_len[k] = plen;
                        s.codec_hold_mt[k] = mt;
                        s.codec_held = s.codec_held.wrapping_add(1);
                        continue;
                    }
                    (chan, mt, plen)
                };
                // Each input speaks for its own namespace only: a front
                // envelope naming a transport connection, or a codec
                // envelope naming a front session, would act on a session
                // that is not its sender's.
                if plen >= 2 && wire::is_front_conn(env_conn_id(&s.in_buf)) != (chan == s.in_front)
                {
                    s.codec_namespace_drops = s.codec_namespace_drops.wrapping_add(1);
                    continue;
                }
                // A coordinator call (`wire::MSG_COORD_CALL`): state this
                // worker owns for the pair — a Kafka group and its offsets,
                // an AMQP queue's cursor and consumers — whichever worker
                // the connection is bound to. It is not the session's: no
                // binding admits it, no cursor counts it, a frozen
                // attachment does not drop it, and every answer it makes
                // goes back as a coordinator answer (`coord_reply`).
                if mt == wire::MSG_COORD_CALL {
                    match worker::anchor_core::unwrap_coord_call_in_place(
                        &mut s.in_buf,
                        plen as usize,
                    ) {
                        Some((inner, n)) => {
                            mt = inner;
                            plen = n as u16;
                            s.coord_reply = true;
                            s.coord_calls = s.coord_calls.wrapping_add(1);
                        }
                        None => continue,
                    }
                    // The connection's close: what this worker coordinates
                    // for it is released — its group memberships and its
                    // queue consumers. Its binding, if this worker holds
                    // one, ends on the session's own notice.
                    if mt == wire::MSG_CONN_CLOSED && plen >= 2 {
                        let c = env_conn_id(&s.in_buf);
                        reply_hold::forget_coord(&mut s.hold, c);
                        handle_conn_disconnect(s, c);
                        continue;
                    }
                }
                // Delivery cursor: every envelope for the attachment,
                // header included, whatever it carries. A frozen
                // attachment (exported, not yet detached) consumes
                // nothing — the anchor holds its records, so one that
                // still arrives is a stale one and is dropped.
                if plen >= 2 && !s.coord_reply {
                    let c = env_conn_id(&s.in_buf);
                    if worker::is_frozen(&s.worker, c) {
                        s.worker.frozen_drops = s.worker.frozen_drops.wrapping_add(1);
                        continue;
                    }
                    let bytes = wire::ENVELOPE_HDR + plen as usize;
                    if mt == wire::MSG_ANCHOR_DELIVERY {
                        // Counted only at the binding's epoch: the anchor
                        // that wrote it counted it under that epoch, and
                        // one a session has left counts for nobody.
                        worker::note_in_delivery(&mut s.worker, &s.in_buf[..plen as usize], bytes);
                    } else {
                        worker::note_in(&mut s.worker, c, bytes);
                    }
                }
                // An anchored frame names the session, the epoch it was
                // decided under and its delivery id: admit it against the
                // binding (old epochs and retried ids are refused, counted)
                // and consume the frame inside as if it had come bare.
                // The cursor above counts the envelope as written, which
                // is the unit the anchor counts too.
                if mt == wire::MSG_ANCHOR_DELIVERY {
                    match worker::admit(&mut s.worker, &mut s.in_buf, plen as usize) {
                        Some((inner, n)) => {
                            mt = inner;
                            plen = n as u16;
                        }
                        None => continue,
                    }
                }

                // Transport connection-closed notice (payload `[conn_id:u16 LE]`,
                // forwarded by any codec on socket close). Distinct from
                // MSG_SESSION_DISCONNECT, which is the MQTT DISCONNECT
                // *packet* and must reach the MQTT handler below. Release
                // conn-keyed state for the non-MQTT protocols; MQTT
                // sessions keep their apply-side keep-alive / will-fire
                // lifecycle.
                if mt == wire::MSG_CONN_CLOSED && plen >= 2 {
                    // From an anchor it ends the binding as well, in order
                    // behind the client's last frames (`worker::client_gone`).
                    let c = env_conn_id(&s.in_buf);
                    // Nothing held for the closed connection is written:
                    // its id is the next connection's.
                    reply_hold::forget_conn(&mut s.hold, c);
                    reply_hold::settle(&mut s.disc_owed, c);
                    if !worker::client_gone(s, sys, c) {
                        handle_conn_disconnect(s, c);
                    }
                    continue;
                }
                if (plen as usize) < CODEC_ENV_HDR {
                    continue;
                } // conn + proto + pkt + flags

                let conn_id = env_conn_id(&s.in_buf);
                let proto = s.in_buf[2];
                let pkt_type = s.in_buf[3];
                let flags = s.in_buf[4];
                let body_ptr = s.in_buf.as_ptr().add(CODEC_ENV_HDR);
                let body_len = plen as usize - CODEC_ENV_HDR;
                let body = core::slice::from_raw_parts(body_ptr, body_len);

                // Keep-alive bookkeeping: every codec packet from this
                // conn_id resets the inactivity timer. The periodic scan
                // at the tail of module_step uses last_activity_ms to
                // detect dropped sockets that peer_router cleared without
                // notifying us.
                //
                // That includes `MSG_SESSION_LIVENESS`: the codec answers
                // PINGREQ itself and forwards only this mark, so without
                // it a client that does nothing but ping — an idle
                // subscriber — would read as silent here and be torn down
                // at 1.5x its keep-alive while alive and pinging.
                if let Some(si) = sessions::find_by_conn(&s.sessions, conn_id) {
                    sessions::touch(&mut s.sessions, si, now);
                }

                match mt {
                    // Activity only: the touch above is all it carries —
                    // unless the worker-loss gate's fault fires on it.
                    wire::MSG_SESSION_LIVENESS => {
                        if s.fault_stop_on_ping != 0 {
                            s.faulted = 1;
                            dev_log(sys, 1, b"[sess] fault injection: worker dark".as_ptr(), 35);
                            break;
                        }
                        // A seated session's anchor forwards the PINGREQ
                        // instead of answering it: the PINGRESP is a
                        // production, placed in the connection's output
                        // like every other response.
                        if let Some(i) = s.worker.seats.seat_of(conn_id) {
                            if !emit_codec_response(
                                s,
                                sys,
                                conn_id,
                                PROTO_MQTT,
                                PKT_PINGRESP,
                                0,
                                &[],
                            ) {
                                // Owed, and answered by `seat_step`.
                                s.worker.seats.seats[i].pingresp_owed = true;
                                s.worker.pingresp_refused =
                                    s.worker.pingresp_refused.wrapping_add(1);
                            }
                        }
                    }
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
                        } else {
                            0
                        };
                        let cid_present = cid_len > 0 && body.len() >= 12 + cid_len;
                        let stream_hash = if cid_present {
                            wire::fnv1a_64(&body[12..12 + cid_len])
                        } else {
                            // Empty client_id (allowed under clean_start) — derive a
                            // unique stream_hash from the conn_id so two anonymous
                            // clients don't collapse to the same session.
                            wire::fnv1a_64(&conn_id.to_le_bytes())
                        };

                        // MQTT keep_alive (seconds) is parsed by `protocol::mqtt`
                        // into bytes 2..4 BE. 0 means "no keep-alive" per
                        // MQTT 3.1.1 §3.1.2.10.
                        let keep_alive_s = if body.len() >= 4 {
                            u16::from_be_bytes([body[2], body[3]]) as u32
                        } else {
                            0
                        };
                        // TENANCY GAP: CONNECT arrival — no session exists
                        // yet, and the codec envelope carries no listener
                        // or authenticated identity for the tenant to come
                        // from. See `TENANT_UNRESOLVED`.
                        let tenant: TenantId = TENANT_UNRESOLVED;
                        // The session's placement, by `(tenant, client_id)`.
                        // The same derivation runs where the CONNECT is
                        // applied, so every node keys the session alike.
                        let session_shard = session_shard(
                            tenant,
                            if cid_present {
                                &body[12..12 + cid_len]
                            } else {
                                &[]
                            },
                            stream_hash,
                        );

                        // Placement, BEFORE the optimistic CONNACK.
                        //
                        // A session's state belongs to the PRG owning
                        // its shard. If that is not this node, the
                        // proposal would be refused by the ownership
                        // gate — but the optimistic CONNACK has already
                        // told the client it is connected by then, so
                        // the client proceeds to SUBSCRIBE against a
                        // session that will never exist. Silently
                        // accepting a connection whose state cannot be
                        // stored is the worst of the options.
                        //
                        // MQTT has its own redirect, so this needs no
                        // proxy for the same reason Kafka's half did
                        // not: v5 reason code 0x9D "Server moved"
                        // (0x9C "Use another server" for a transient
                        // fence), and v3.1.1 CONNACK 0x03 "Server
                        // unavailable", which is the only refusal that
                        // version can express. Redirecting rather than
                        // proxying also keeps EDGE-NOAFFINITY: the
                        // client ends up talking to the node that owns
                        // its session instead of being pinned to
                        // whichever node it happened to reach first.
                        if proto == PROTO_MQTT {
                            let action =
                                edge::classify(&s.view.view, s.view.owner_prg(session_shard), 0);
                            if action != edge::EdgeAction::Local {
                                // Version-agnostic: the codec spells it.
                                // A fence or a stale view is transient
                                // (come back), a reassignment is settled
                                // (go elsewhere and stay).
                                let reason = match action {
                                    edge::EdgeAction::Fenced
                                    | edge::EdgeAction::RefreshThenRetry => {
                                        wire::MQTT_REASON_TRY_ELSEWHERE
                                    }
                                    _ => wire::MQTT_REASON_MOVED,
                                };
                                emit_codec_response(
                                    s,
                                    sys,
                                    conn_id,
                                    PROTO_MQTT,
                                    PKT_CONNACK,
                                    0,
                                    &[0u8, reason],
                                );
                                s.connects_redirected = s.connects_redirected.wrapping_add(1);
                                continue;
                            }
                        }

                        let prior = sessions::find_by_stream(&s.sessions, tenant, stream_hash);
                        // Was the matched slot a persisted session that we're
                        // about to resurrect, vs an already-active reconnect
                        // (e.g. same client_id re-CONNECT before DISCONNECT)?
                        // Only the persisted path sets session_present=1 in
                        // the optimistic CONNACK.
                        //
                        // NOTE: this predicate is too NARROW — a client that
                        // reconnects before its DISCONNECT has applied is told
                        // `session_present = 0` and throws away subscriptions
                        // and inflight state the broker still holds; the same
                        // client one second later is told 1. Broadening it to
                        // `prior.is_some() && !clean_start` fixes that case and
                        // BREAKS a worse one: the expiry sweep only purges
                        // `persisted` slots, so a slot that lingers un-persisted
                        // is never purged and would then report 1 for ever —
                        // erring toward "your state is here" when it is not,
                        // which makes a client skip resubscribing. The real fix
                        // is in the session lifecycle, not this predicate.
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
                                &mut s.sessions,
                                i,
                                conn_id,
                                proto,
                                clean_start,
                                keep_alive_s.saturating_mul(1000),
                            );
                            // Propose-side admission marker; apply flips
                            // transient→active and bumps session_generation.
                            sessions::mark_transient(&mut s.sessions, i);
                            Some(i)
                        } else if let Some(i) = sessions::allocate(&mut s.sessions) {
                            // `active` flips at QOP_CONNECT apply; until
                            // then the slot is transient.
                            sessions::open_transient(
                                &mut s.sessions,
                                i,
                                sessions::ConnectParams {
                                    tenant,
                                    stream_hash,
                                    shard: session_shard,
                                    protocol: proto,
                                    clean_start,
                                    keep_alive_ms: keep_alive_s.saturating_mul(1000),
                                },
                                conn_id,
                                now,
                            );
                            // session_count is the durable count of active
                            // sessions; apply-side increments it.
                            Some(i)
                        } else {
                            None
                        };

                        // Enforce "one session per conn_id" BEFORE anything
                        // on this connection is routed. `conn_id` is a
                        // recycled transport slot, and a client that closed
                        // abruptly leaves its session still claiming the id —
                        // so without this the new client's SUBSCRIBE and
                        // publishes resolve to the PREVIOUS client's session
                        // (`find_by_conn` returns the first match by slot
                        // index). Measured: a subscriber bound to slot 1 had
                        // its SUBSCRIBE anchored to slot 0 and was then flow-
                        // controlled by that session's ReceiveMaximum.
                        if let Some(i) = session_idx {
                            let evicted = sessions::unbind_other_conns(&mut s.sessions, i, conn_id);
                            if evicted > 0 {
                                s.conn_bindings_evicted =
                                    s.conn_bindings_evicted.wrapping_add(evicted);
                            }
                        }

                        // CONNACK body = [session_present:u8][reason_code:u8].
                        // MQTT 3.1.1 §3.2.2.2 requires session_present=1
                        // when the broker is resurrecting a stored session.
                        // Optimistic — see docs/architecture/apply_path.md
                        // §Optimistic CONNACK.
                        if proto == PROTO_MQTT {
                            let connack_body =
                                [if resurrecting_persisted { 1u8 } else { 0u8 }, 0u8];
                            emit_codec_response(
                                s,
                                sys,
                                conn_id,
                                PROTO_MQTT,
                                PKT_CONNACK,
                                0,
                                &connack_body,
                            );
                            // A resumed session is owed what the log says it
                            // was sent and never acknowledged — including
                            // deliveries a crashed worker or a restarted node
                            // no longer holds in memory.
                            if resurrecting_persisted {
                                if let Some(i) = session_idx {
                                    redeliver_owed(s, sys, i);
                                }
                            }
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
                        } else {
                            0
                        };
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
                            if body.len() < q_off + 1 + 1 + 4 + 2 {
                                will_accept = false;
                            } else {
                                will_qos = body[q_off];
                                will_retain = body[q_off + 1];
                                will_delay_s = u32::from_le_bytes([
                                    body[q_off + 2],
                                    body[q_off + 3],
                                    body[q_off + 4],
                                    body[q_off + 5],
                                ]);
                                let wt_len_off = q_off + 6;
                                will_topic_len =
                                    u16::from_be_bytes([body[wt_len_off], body[wt_len_off + 1]])
                                        as usize;
                                will_topic_off = wt_len_off + 2;
                                let wp_len_off = will_topic_off + will_topic_len;
                                if body.len() < wp_len_off + 2 || will_topic_len > MAX_WILL_TOPIC {
                                    will_accept = false;
                                } else {
                                    will_payload_len = u16::from_be_bytes([
                                        body[wp_len_off],
                                        body[wp_len_off + 1],
                                    ])
                                        as usize;
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
                        } else {
                            0
                        };
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
                        } else {
                            0
                        };
                        // Apply the cap to the slot NOW, not only at
                        // QOP_CONNECT apply. The CONNACK is optimistic, so
                        // the client may SUBSCRIBE and start receiving before
                        // the CONNECT has committed — and until then the slot
                        // still carried the previous occupant's cap (0 = no
                        // cap on a fresh slot). Deliveries in that window
                        // ignored the ReceiveMaximum the client had just
                        // asked for, which is the flow control it uses to
                        // protect itself. Apply sets it again; the write is
                        // idempotent, and a CONNECT that never commits takes
                        // its transient slot with it.
                        if let Some(i) = session_idx {
                            sessions::set_receive_maximum(&mut s.sessions, i, receive_maximum_s);
                        }

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
                        } else {
                            0
                        };
                        let qbody_len = 1 + 2 + 8 + 2 + cid_len + 1 + 1 + will_extra + 4 + 2;
                        let prop_total = wire::QPROP_UNTAGGED_HDR_LEN + qbody_len;
                        if prop_total <= s.out_buf.len() {
                            wire::encode_qprop_header(
                                &mut s.out_buf[0..wire::QPROP_HEADER_LEN],
                                wire::QOP_CONNECT,
                                tenant,
                                session_slot,
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
                            // Session state is owned by the session
                            // shard — the one the placement gate above
                            // admitted this CONNECT on.
                            let shard = session_shard;
                            if try_emit_keyed(
                                sys,
                                s.out_proposals,
                                &s.view,
                                shard,
                                &s.out_buf[..prop_total],
                            ) {
                                s.proposals_emitted = s.proposals_emitted.wrapping_add(1);
                                s.hb_prop = s.hb_prop.wrapping_add(1);
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
                                    wire::QOP_DISCONNECT,
                                    tenant,
                                    session_slot,
                                );
                                let off = wire::QPROP_UNTAGGED_HDR_LEN;
                                s.out_buf[off] = wire::QDISC_REASON_CLEAN;
                                s.out_buf[off + 1..off + 9]
                                    .copy_from_slice(&stream_hash.to_le_bytes());
                                let shard = sessions::shard(&s.sessions, i);
                                if try_emit_keyed(
                                    sys,
                                    s.out_proposals,
                                    &s.view,
                                    shard,
                                    &s.out_buf[..prop_total],
                                ) {
                                    s.proposals_emitted = s.proposals_emitted.wrapping_add(1);
                                    s.hb_prop = s.hb_prop.wrapping_add(1);
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
                                if body.len() < 4 {
                                    continue;
                                }
                                let packet_id = u16::from_be_bytes([body[0], body[1]]);
                                let topic_len = u16::from_be_bytes([body[2], body[3]]) as usize;
                                if body.len() < 4 + topic_len + 1 {
                                    continue;
                                }
                                let up_off = 4 + topic_len;
                                let Some(up_len) = user_props_block_len(&body[up_off..]) else {
                                    s.publishes_throttled = s.publishes_throttled.wrapping_add(1);
                                    dev_log(sys, 2, b"[sess] pub props unparsable".as_ptr(), 27);
                                    continue;
                                };
                                if up_len > MAX_USER_PROPS_BYTES {
                                    s.publishes_throttled = s.publishes_throttled.wrapping_add(1);
                                    dev_log(sys, 2, b"[sess] pub props oversize".as_ptr(), 25);
                                    continue;
                                }
                                let payload_off = up_off + up_len;
                                let payload_len = body.len() - payload_off;

                                // A publish Clustor would never commit is
                                // refused before anything is spent on it:
                                // proposed, it would be dropped on the way to
                                // the log, and its publisher would hear
                                // nothing — no PUBACK, no error. MQTT says
                                // so with Packet too large (0x95), on a
                                // DISCONNECT: the codec sends it to an MQTT 5
                                // client, and closes an MQTT 3.1.1 one, which
                                // has no server DISCONNECT.
                                if !wire::publish_fits(qos, topic_len, up_len, payload_len) {
                                    s.publishes_too_large = s.publishes_too_large.wrapping_add(1);
                                    if s.publishes_too_large & 0x1F == 1 {
                                        dev_log(
                                            sys,
                                            1,
                                            b"[sess] publish past the raft entry - packet too large".as_ptr(),
                                            53,
                                        );
                                    }
                                    emit_codec_response(
                                        s,
                                        sys,
                                        conn_id,
                                        PROTO_MQTT,
                                        PKT_DISCONNECT,
                                        0,
                                        &[wire::MQTT_REASON_PACKET_TOO_LARGE],
                                    );
                                    continue;
                                }

                                let session_idx = sessions::find_by_conn(&s.sessions, conn_id);
                                let tenant = session_idx
                                    .map(|i| sessions::tenant(&s.sessions, i))
                                    .unwrap_or(0);

                                // PID admission. The gate at the top of
                                // the drain keeps this from firing for
                                // any packet within the entry cap; it
                                // remains for a body past the gate, and
                                // says so.
                                let body_cost = body.len() as i32;
                                if s.pid_entry_credits <= 0 || s.pid_byte_credits < body_cost {
                                    s.publishes_throttled = s.publishes_throttled.wrapping_add(1);
                                    dev_log(sys, 2, b"[sess] publish throttled".as_ptr(), 24);
                                    continue;
                                }

                                // QoS 1+: allocate publisher inflight FIRST and
                                // fail-fast if the slot table is full. Per
                                // MQTT 3.1.1 §4.4, the broker must track every
                                // QoS 1+ delivery until it is acknowledged;
                                // continuing to route + propose without an
                                // inflight slot would silently downgrade the
                                // contract to QoS 0 and break PUBACK matching.
                                // A QoS 2 retransmission of a flow this
                                // session already has in flight (MQTT
                                // 3.1.1 §4.3.3): the receiver answers
                                // PUBREC again and must not start a
                                // second flow — a second proposal would
                                // be a second inflight entry and a second
                                // PUBCOMP. Before the first is durable
                                // there is nothing to answer yet; the
                                // original's PUBREC will cover both.
                                if qos == 2 {
                                    if let Some(si) = session_idx {
                                        if let Some(ii) = sessions::inflight_find(
                                            &s.sessions,
                                            si,
                                            packet_id,
                                            INFLIGHT_PUB,
                                        ) {
                                            let durable =
                                                sessions::inflight_view(&s.sessions, si, ii)
                                                    .is_some_and(|v| v.wal_index != 0);
                                            if durable {
                                                emit_codec_response(
                                                    s,
                                                    sys,
                                                    conn_id,
                                                    PROTO_MQTT,
                                                    PKT_PUBREC,
                                                    0,
                                                    &packet_id.to_be_bytes(),
                                                );
                                            }
                                            s.qos2_dup_answered =
                                                s.qos2_dup_answered.wrapping_add(1);
                                            continue;
                                        }
                                    }
                                }
                                if qos > 0 {
                                    let Some(si) = session_idx else {
                                        // No session is bound to this
                                        // connection: the publish has no
                                        // owner to ack it. Said with the
                                        // connection so the binding that
                                        // went missing can be traced.
                                        s.publishes_throttled =
                                            s.publishes_throttled.wrapping_add(1);
                                        let mut line = [0u8; 40];
                                        let mut pos = 0usize;
                                        for &b in b"[sess] pub no session c=" {
                                            line[pos] = b;
                                            pos += 1;
                                        }
                                        pos += fmt_u32_raw(
                                            line.as_mut_ptr().add(pos),
                                            u32::from(conn_id),
                                        );
                                        for &b in b" f=" {
                                            line[pos] = b;
                                            pos += 1;
                                        }
                                        pos += fmt_u32_raw(
                                            line.as_mut_ptr().add(pos),
                                            sessions::conn_flags(&s.sessions, conn_id),
                                        );
                                        dev_log(sys, 2, line.as_ptr(), pos);
                                        continue;
                                    };
                                    if sessions::inflight_add(
                                        &mut s.sessions,
                                        si,
                                        packet_id,
                                        qos,
                                        INFLIGHT_PUB,
                                    )
                                    .is_none()
                                    {
                                        // The client has more unacked
                                        // publishes than this broker
                                        // tracks per session
                                        // (`MAX_INFLIGHT_PER_SESSION`).
                                        // 3.1.1 has no way to say so;
                                        // the drop is at least visible.
                                        s.publishes_throttled =
                                            s.publishes_throttled.wrapping_add(1);
                                        if s.publishes_throttled & 0x1F == 1 {
                                            dev_log(sys, 2, b"[sess] inflight full".as_ptr(), 20);
                                        }
                                        continue;
                                    }
                                }

                                // Admission succeeded — draw down PID credit.
                                s.pid_entry_credits = s.pid_entry_credits.saturating_sub(1);
                                s.pid_byte_credits = s.pid_byte_credits.saturating_sub(body_cost);
                                s.hb_pub = s.hb_pub.wrapping_add(1);

                                let stream_hash = session_idx
                                    .map(|si| sessions::stream_hash(&s.sessions, si))
                                    .unwrap_or(0);
                                let session_generation = session_idx
                                    .map(|si| sessions::generation(&s.sessions, si))
                                    .unwrap_or(0);
                                let session_slot = session_idx.unwrap_or(0) as u32;

                                // QOP_PUBLISH V2 op-body shape (see wire::QOP_PUBLISH):
                                //   [pub_qos:u8][packet_id:u16 BE]
                                //   [stream_hash:u64 LE][session_generation:u32 LE]
                                //   [retain:u8][topic_len:u16 BE][topic]
                                //   [user_props_count:u8][per prop ...]
                                //   [payload]
                                let qbody_len =
                                    1 + 2 + 8 + 4 + 1 + 2 + topic_len + up_len + payload_len;
                                let prop_total = if qos > 0 {
                                    wire::QPROP_TAGGED_HDR_LEN + qbody_len
                                } else {
                                    wire::QPROP_UNTAGGED_HDR_LEN + qbody_len
                                };
                                // `publish_fits` held above: the proposal is
                                // within the Raft entry, so within `out_buf`
                                // (`BUF_SIZE`), and a QoS 1+ op body within
                                // the apply side's stash (MAX_STASH_ENV).

                                let cid: u64 = if qos > 0 {
                                    let Some(cid) = correlate::allocate(
                                        &mut s.correlate,
                                        session_slot,
                                        packet_id,
                                        OP_PUBLISH,
                                        now,
                                    ) else {
                                        if let Some(si) = session_idx {
                                            if let Some(ii) = sessions::inflight_find(
                                                &s.sessions,
                                                si,
                                                packet_id,
                                                INFLIGHT_PUB,
                                            ) {
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
                                        if let Some(ii) = sessions::inflight_find(
                                            &s.sessions,
                                            si,
                                            packet_id,
                                            INFLIGHT_PUB,
                                        ) {
                                            sessions::inflight_set_correlation(
                                                &mut s.sessions,
                                                si,
                                                ii,
                                                cid,
                                            );
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
                                let hdr_end = if qos > 0 {
                                    s.out_buf[0..8].copy_from_slice(&cid.to_le_bytes());
                                    wire::encode_qprop_header_v(
                                        &mut s.out_buf[8..8 + wire::QPROP_HEADER_LEN],
                                        wire::QPROP_VERSION_V2,
                                        wire::QOP_PUBLISH,
                                        tenant,
                                        session_slot,
                                    );
                                    wire::QPROP_TAGGED_HDR_LEN
                                } else {
                                    wire::encode_qprop_header_v(
                                        &mut s.out_buf[0..wire::QPROP_HEADER_LEN],
                                        wire::QPROP_VERSION_V2,
                                        wire::QOP_PUBLISH,
                                        tenant,
                                        session_slot,
                                    );
                                    wire::QPROP_UNTAGGED_HDR_LEN
                                };
                                let off = hdr_end;
                                s.out_buf[off] = qos;
                                s.out_buf[off + 1..off + 3]
                                    .copy_from_slice(&packet_id.to_be_bytes());
                                s.out_buf[off + 3..off + 11]
                                    .copy_from_slice(&stream_hash.to_le_bytes());
                                s.out_buf[off + 11..off + 15]
                                    .copy_from_slice(&session_generation.to_le_bytes());
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
                                // A publish mutates topic state
                                // (retained value, subscriber fan-out),
                                // so it is placed on the topic shard —
                                // which also keeps one topic's publishes
                                // ordered on one partition.
                                let shard = wire::shard_mqtt_topic(tenant, &body[4..4 + topic_len]);
                                // Routed, not owned — see
                                // `emit_keyed_unowned`. The shard says
                                // WHERE the publish lands, not whether
                                // this node may accept it; gating here
                                // dropped every publish for a topic
                                // owned by another PRG.
                                let emit_ok = emit_keyed_unowned(
                                    sys,
                                    chan,
                                    &s.view,
                                    shard,
                                    &s.out_buf[..prop_total],
                                );
                                if emit_ok {
                                    s.proposals_emitted = s.proposals_emitted.wrapping_add(1);
                                    s.hb_prop = s.hb_prop.wrapping_add(1);
                                } else if prop_total <= s.prop_hold.len() {
                                    // Refused: held whole, correlation and
                                    // inflight intact, and the codec drain
                                    // stops until it lands (see the top of
                                    // the drain). Counted as a refusal so an
                                    // undersized edge still shows.
                                    s.proposals_refused = s.proposals_refused.wrapping_add(1);
                                    s.proposals_held = s.proposals_held.wrapping_add(1);
                                    if s.proposals_refused & 0x3F == 1 {
                                        let mut line = [0u8; 48];
                                        let mut pos = 0usize;
                                        for &b in b"[sess] proposal held n=" {
                                            line[pos] = b;
                                            pos += 1;
                                        }
                                        pos += fmt_u32_raw(
                                            line.as_mut_ptr().add(pos),
                                            s.proposals_refused,
                                        );
                                        dev_log(sys, 2, line.as_ptr(), pos);
                                    }
                                    s.prop_hold[..prop_total]
                                        .copy_from_slice(&s.out_buf[..prop_total]);
                                    s.prop_hold_len = prop_total as u16;
                                    s.prop_hold_chan = chan;
                                    s.prop_hold_shard = shard;
                                    break;
                                } else {
                                    // Wider than the hold can carry: the
                                    // proposal-size bound above makes this
                                    // unreachable, but a drop is never
                                    // silent.
                                    s.proposals_refused = s.proposals_refused.wrapping_add(1);
                                    dev_log(
                                        sys,
                                        2,
                                        b"[sess] proposal dropped oversize".as_ptr(),
                                        32,
                                    );
                                    if qos > 0 {
                                        let _ = correlate::take(&mut s.correlate, cid);
                                        if let Some(si) = session_idx {
                                            if let Some(ii) = sessions::inflight_find(
                                                &s.sessions,
                                                si,
                                                packet_id,
                                                INFLIGHT_PUB,
                                            ) {
                                                sessions::inflight_release(&mut s.sessions, si, ii);
                                            }
                                        }
                                    }
                                    s.publishes_throttled = s.publishes_throttled.wrapping_add(1);
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
                                if body.len() < 5 {
                                    continue;
                                }
                                let packet_id = u16::from_be_bytes([body[0], body[1]]);
                                let req_qos = body[2];
                                let topic_len = u16::from_be_bytes([body[3], body[4]]) as usize;
                                if body.len() < 5 + topic_len {
                                    continue;
                                }

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
                                        wire::QOP_SUBSCRIBE,
                                        tenant,
                                        session_slot,
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
                                    // Applied by `find_by_stream` against
                                    // session state, so it rides the session
                                    // shard. The topic-side fan-out travels
                                    // separately as MSG_TOPIC_SUBSCRIBE.
                                    let shard = session_idx
                                        .map(|i| sessions::shard(&s.sessions, i))
                                        .unwrap_or_else(|| {
                                            wire::shard_mqtt_stream(tenant, stream_hash)
                                        });
                                    if try_emit_keyed(
                                        sys,
                                        s.out_proposals,
                                        &s.view,
                                        shard,
                                        &s.out_buf[..prop_total],
                                    ) {
                                        s.proposals_emitted = s.proposals_emitted.wrapping_add(1);
                                        s.hb_prop = s.hb_prop.wrapping_add(1);
                                    }
                                }

                                // SUBACK optimistic: [packet_id BE][req_qos]
                                let mut suback = [0u8; 3];
                                suback[0..2].copy_from_slice(&packet_id.to_be_bytes());
                                suback[2] = req_qos;
                                emit_codec_response(
                                    s, sys, conn_id, PROTO_MQTT, PKT_SUBACK, 0, &suback,
                                );
                            }

                            PKT_UNSUBSCRIBE => {
                                if body.len() < 5 {
                                    continue;
                                }
                                let packet_id = u16::from_be_bytes([body[0], body[1]]);
                                let topic_len = u16::from_be_bytes([body[3], body[4]]) as usize;
                                if body.len() < 5 + topic_len {
                                    continue;
                                }

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
                                        wire::QOP_UNSUBSCRIBE,
                                        tenant,
                                        session_slot,
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
                                    // Applied by `find_by_stream` against
                                    // session state, so it rides the session
                                    // shard. The topic-side fan-out travels
                                    // separately as MSG_TOPIC_SUBSCRIBE.
                                    let shard = session_idx
                                        .map(|i| sessions::shard(&s.sessions, i))
                                        .unwrap_or_else(|| {
                                            wire::shard_mqtt_stream(tenant, stream_hash)
                                        });
                                    if try_emit_keyed(
                                        sys,
                                        s.out_proposals,
                                        &s.view,
                                        shard,
                                        &s.out_buf[..prop_total],
                                    ) {
                                        s.proposals_emitted = s.proposals_emitted.wrapping_add(1);
                                        s.hb_prop = s.hb_prop.wrapping_add(1);
                                    }
                                }

                                let mut unsuback = [0u8; 2];
                                unsuback.copy_from_slice(&packet_id.to_be_bytes());
                                emit_codec_response(
                                    s,
                                    sys,
                                    conn_id,
                                    PROTO_MQTT,
                                    PKT_UNSUBACK,
                                    0,
                                    &unsuback,
                                );
                            }

                            PKT_PUBREC => {
                                // Inbound PUBREC is the subscriber's QoS 2
                                // phase-1 ack of a PUBLISH we delivered to them.
                                if body.len() < 2 {
                                    continue;
                                }
                                let packet_id = u16::from_be_bytes([body[0], body[1]]);
                                if let Some(si) = sessions::find_by_conn(&s.sessions, conn_id) {
                                    if let Some(ii) = sessions::inflight_find(
                                        &s.sessions,
                                        si,
                                        packet_id,
                                        INFLIGHT_SUB,
                                    ) {
                                        if sessions::inflight_view(&s.sessions, si, ii)
                                            .is_some_and(|v| v.phase == QOS2_PUBLISH)
                                        {
                                            sessions::inflight_set_phase(
                                                &mut s.sessions,
                                                si,
                                                ii,
                                                QOS2_PUBREL,
                                            );
                                            log_deliver_ack(
                                                s,
                                                sys,
                                                si,
                                                packet_id,
                                                replay::ACK_RELEASED,
                                            );
                                            s.qos2_rec = s.qos2_rec.wrapping_add(1);
                                            let mut pubrel = [0u8; 2];
                                            pubrel.copy_from_slice(&packet_id.to_be_bytes());
                                            emit_codec_response(
                                                s, sys, conn_id, PROTO_MQTT, PKT_PUBREL, 0x02,
                                                &pubrel,
                                            );
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
                                if body.len() < 2 {
                                    continue;
                                }
                                let packet_id = u16::from_be_bytes([body[0], body[1]]);
                                s.qos2_rel = s.qos2_rel.wrapping_add(1);

                                let Some(si) = sessions::find_by_conn(&s.sessions, conn_id) else {
                                    continue;
                                };
                                // Mark publisher inflight as awaiting PUBREL
                                // durability so a stray PUBCOMP attempt
                                // doesn't fire twice. A PUBREL whose flow
                                // has no live slot — a duplicate arriving
                                // after completion released it, or a
                                // transaction whose slot this node rebuilt
                                // from a later entry — opens one, because
                                // the PUBCOMP that answers it is emitted
                                // from the slot. Refuse before proposing
                                // when the table is full: the publisher
                                // retries a PUBREL it has not been
                                // released from.
                                let inflight_idx = match sessions::inflight_find(
                                    &s.sessions,
                                    si,
                                    packet_id,
                                    INFLIGHT_PUB,
                                ) {
                                    Some(ii) => Some(ii),
                                    None => sessions::inflight_add(
                                        &mut s.sessions,
                                        si,
                                        packet_id,
                                        QOS_2,
                                        INFLIGHT_PUB,
                                    ),
                                };
                                let Some(inflight_ii) = inflight_idx else {
                                    s.publishes_throttled = s.publishes_throttled.wrapping_add(1);
                                    continue;
                                };
                                sessions::inflight_set_phase(
                                    &mut s.sessions,
                                    si,
                                    inflight_ii,
                                    QOS2_PUBREL,
                                );
                                // The PUBREL's own registration names the
                                // flow from here: until it is assigned,
                                // no completion — the PUBLISH's late one
                                // above all — may read as the PUBCOMP.
                                sessions::inflight_set_wal_index(
                                    &mut s.sessions,
                                    si,
                                    inflight_ii,
                                    0,
                                    0,
                                    0,
                                );
                                let inflight_idx = Some(inflight_ii);

                                let tenant = sessions::tenant(&s.sessions, si);
                                let session_slot = si as u32;
                                let stream_hash = sessions::stream_hash(&s.sessions, si);
                                let Some(cid) = correlate::allocate(
                                    &mut s.correlate,
                                    session_slot,
                                    packet_id,
                                    OP_PUBREL,
                                    now,
                                ) else {
                                    correlate::note_dropped(&mut s.correlate);
                                    continue;
                                };

                                // Bind the new correlation to the inflight
                                // slot so MSG_ACK_REDELIVER can reconstruct
                                // a PUBREL retry without consulting the
                                // (already-released) PUBLISH stash.
                                if let Some(ii) = inflight_idx {
                                    sessions::inflight_set_correlation(
                                        &mut s.sessions,
                                        si,
                                        ii,
                                        cid,
                                    );
                                }

                                // QOP_PUBREL body:
                                //   [packet_id:u16 BE][stream_hash:u64 LE]
                                //   [session_generation:u32 LE]
                                let qbody_len = 2 + 8 + 4;
                                let prop_total = wire::QPROP_TAGGED_HDR_LEN + qbody_len;
                                let mut emit_ok = false;
                                if prop_total <= s.out_buf.len() {
                                    s.out_buf[0..8].copy_from_slice(&cid.to_le_bytes());
                                    wire::encode_qprop_header(
                                        &mut s.out_buf[8..8 + wire::QPROP_HEADER_LEN],
                                        wire::QOP_PUBREL,
                                        tenant,
                                        session_slot,
                                    );
                                    let off = wire::QPROP_TAGGED_HDR_LEN;
                                    s.out_buf[off..off + 2]
                                        .copy_from_slice(&packet_id.to_be_bytes());
                                    s.out_buf[off + 2..off + 10]
                                        .copy_from_slice(&stream_hash.to_le_bytes());
                                    // The epoch the flow opened under. The
                                    // dedupe key is epoch-derived, so without
                                    // it a replayed PUBREL cannot name the
                                    // entry its own PUBLISH created.
                                    let flow_epoch = sessions::inflight_view(
                                        &s.sessions,
                                        session_slot as usize,
                                        inflight_ii,
                                    )
                                    .map(|v| v.session_generation)
                                    .filter(|e| *e != 0)
                                    .unwrap_or_else(|| {
                                        sessions::generation(&s.sessions, session_slot as usize)
                                    });
                                    s.out_buf[off + 10..off + 14]
                                        .copy_from_slice(&flow_epoch.to_le_bytes());
                                    let shard = sessions::shard(&s.sessions, session_slot as usize);
                                    if try_emit_keyed(
                                        sys,
                                        s.out_proposals_tagged,
                                        &s.view,
                                        shard,
                                        &s.out_buf[..prop_total],
                                    ) {
                                        s.proposals_emitted = s.proposals_emitted.wrapping_add(1);
                                        s.hb_prop = s.hb_prop.wrapping_add(1);
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
                                        sessions::inflight_set_correlation(
                                            &mut s.sessions,
                                            si,
                                            ii,
                                            0,
                                        );
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
                                if body.len() < 2 {
                                    continue;
                                }
                                let packet_id = u16::from_be_bytes([body[0], body[1]]);
                                s.qos2_comp = s.qos2_comp.wrapping_add(1);
                                if let Some(si) = sessions::find_by_conn(&s.sessions, conn_id) {
                                    if let Some(ii) = sessions::inflight_find(
                                        &s.sessions,
                                        si,
                                        packet_id,
                                        INFLIGHT_SUB,
                                    ) {
                                        sessions::inflight_release(&mut s.sessions, si, ii);
                                        log_deliver_ack(
                                            s,
                                            sys,
                                            si,
                                            packet_id,
                                            replay::ACK_COMPLETE,
                                        );
                                        if sessions::sub_outstanding(&s.sessions, si) > 0 {
                                            sessions::note_ack(&mut s.sessions, si);
                                            let mut lag_msg = [0u8; 8];
                                            lag_msg[0..4]
                                                .copy_from_slice(&(si as u32).to_le_bytes());
                                            lag_msg[4..8].copy_from_slice(
                                                &sessions::sub_outstanding(&s.sessions, si)
                                                    .to_le_bytes(),
                                            );
                                            try_emit(
                                                sys,
                                                s.out_forward,
                                                wire::MSG_LAG_SIGNAL,
                                                &lag_msg,
                                            );
                                        }
                                    }
                                }
                            }

                            PKT_PUBACK => {
                                // Inbound PUBACK is the subscriber draining
                                // a QoS 1 delivery we pushed.
                                if body.len() < 2 {
                                    continue;
                                }
                                let packet_id = u16::from_be_bytes([body[0], body[1]]);
                                if let Some(si) = sessions::find_by_conn(&s.sessions, conn_id) {
                                    if let Some(ii) = sessions::inflight_find(
                                        &s.sessions,
                                        si,
                                        packet_id,
                                        INFLIGHT_SUB,
                                    ) {
                                        sessions::inflight_release(&mut s.sessions, si, ii);
                                        log_deliver_ack(
                                            s,
                                            sys,
                                            si,
                                            packet_id,
                                            replay::ACK_COMPLETE,
                                        );
                                        s.acks_emitted = s.acks_emitted.wrapping_add(1);
                                        if sessions::sub_outstanding(&s.sessions, si) > 0 {
                                            sessions::note_ack(&mut s.sessions, si);
                                            let mut lag_msg = [0u8; 8];
                                            lag_msg[0..4]
                                                .copy_from_slice(&(si as u32).to_le_bytes());
                                            lag_msg[4..8].copy_from_slice(
                                                &sessions::sub_outstanding(&s.sessions, si)
                                                    .to_le_bytes(),
                                            );
                                            try_emit(
                                                sys,
                                                s.out_forward,
                                                wire::MSG_LAG_SIGNAL,
                                                &lag_msg,
                                            );
                                        }
                                    }
                                }
                            }

                            _ => {
                                // An MQTT packet type this module has no
                                // handler for (AUTH, or a type the codec
                                // let through): the body is not a Quantum
                                // op and names no routing key. Under
                                // `keyed_only` it is refused and counted;
                                // with the gate off it takes the router's
                                // body-hash placement.
                                if try_emit_unkeyed(s, sys, body) {
                                    s.proposals_emitted = s.proposals_emitted.wrapping_add(1);
                                    s.hb_prop = s.hb_prop.wrapping_add(1);
                                }
                            }
                        }
                    }

                    #[cfg(feature = "kafka")]
                    wire::MSG_SESSION_PROPOSAL if proto == PROTO_KAFKA => {
                        // Kafka envelope (see `protocol::kafka`):
                        //   [conn_id:u16 LE][proto=1][api_key:i16 LE][api_ver:i16 LE]
                        //   [corr:i32 LE][request body after client_id]
                        // ApiVersions/Metadata are answered inline by the
                        // codec; the durable/data-path APIs land here.
                        if plen as usize >= KAFKA_ENV_HDR {
                            let api_key = i16::from_le_bytes([s.in_buf[3], s.in_buf[4]]);
                            let (k_conn, k_ver, k_corr) = kafka_env_header(&s.in_buf);
                            s.kafka_settled = false;
                            match api_key {
                                0 => handle_kafka_produce(s, sys, now, plen as usize),
                                1 => handle_kafka_fetch(s, sys, plen as usize),
                                2 => handle_kafka_list_offsets(s, sys, plen as usize),
                                8 => handle_kafka_offset_commit(s, sys, now, plen as usize),
                                9 => handle_kafka_offset_fetch(s, sys, plen as usize),
                                11 => handle_kafka_join_group(s, sys, plen as usize),
                                12 | 13 => {
                                    handle_kafka_heartbeat_leave(s, sys, plen as usize, api_key)
                                }
                                14 => handle_kafka_sync_group(s, sys, plen as usize),
                                _ => {}
                            }
                            if !s.kafka_settled {
                                // No answer was made and none is owed (a
                                // malformed request, a shape no response
                                // fits, an API not served here): the codec
                                // writes this connection's answers in
                                // request order, so it is told this one
                                // has none and closes the connection in
                                // its turn rather than hold every later
                                // answer behind it.
                                s.kafka_unanswered = s.kafka_unanswered.wrapping_add(1);
                                emit_kafka_response(
                                    s,
                                    sys,
                                    k_conn,
                                    wire::KAFKA_API_CLOSE,
                                    k_ver,
                                    k_corr,
                                    &[],
                                );
                            }
                        }
                    }

                    #[cfg(feature = "amqp")]
                    wire::MSG_SESSION_PROPOSAL if proto == PROTO_AMQP => {
                        // AMQP envelope (see `protocol::amqp`):
                        //   [conn_id:u16 LE][proto=2][op:u8][channel:u16 LE][rest]
                        // op 1 = assembled Basic.Publish (confirm-gated when
                        // delivery_tag != 0), op 2 = Basic.Get.
                        if plen as usize >= AMQP_ENV_HDR {
                            match s.in_buf[3] {
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
                        // Unknown protocol. No routing key is derivable
                        // from a body this module cannot parse, so under
                        // `keyed_only` it is refused and counted; with the
                        // gate off it takes the router's body-hash
                        // placement.
                        if try_emit_unkeyed(s, sys, body) {
                            s.proposals_emitted = s.proposals_emitted.wrapping_add(1);
                            s.hb_prop = s.hb_prop.wrapping_add(1);
                        }
                    }

                    _ => {}
                }
            }
        }

        // ── Phase 3: apply committed entries (wire envelope) ──
        //
        // `in_committed` is fanned in from both:
        //   - `consensus.applied` — MSG_CLIENT_RESPONSE per-batch
        //     notifications (body-less), bumps `applied`.
        //   - `consensus.committed_entries` — MSG_COMMITTED_ENTRY
        //     per-entry stream `[term:u64][index:u64][body]`.
        //
        // Per-entry bodies that carry a canonical Quantum envelope
        // (`wire::peel_qprop` succeeds) drive durable state mutation
        // through the apply-side dispatcher below. Entries that don't
        // peel (a body proposed unframed, such as an unknown protocol's)
        // advance `applied` without dispatching. See
        // docs/architecture/apply_path.md.
        if s.in_committed >= 0 {
            for _ in 0..16 {
                // Reserve the outputs before consuming the entry. A
                // committed entry is delivered once and never re-sent, so
                // an apply whose side effects cannot be written would
                // advance past state this node then never records —
                // silently diverging from the leader. Requiring room in
                // the outbox toward `messaging` and on the topic and
                // control-plane edges first leaves the entry in the
                // channel for a later step instead.
                if !outputs_ready_for_apply(s, sys) {
                    s.apply_output_stalls = s.apply_output_stalls.wrapping_add(1);
                    break;
                }
                let poll = (sys.channel_poll)(s.in_committed, 0x01);
                if poll <= 0 || (poll as u32 & 0x01) == 0 {
                    break;
                }
                let (mt, plen) = {
                    worked += 1;
                    wire::channel_read_msg(sys, s.in_committed, &mut s.in_buf)
                };
                if mt == wire::MSG_COMMITTED_ENTRY && (plen as usize) >= wire::COMMITTED_ENTRY_HDR {
                    s.committed_entries_observed = s.committed_entries_observed.wrapping_add(1);
                    s.hb_commit = s.hb_commit.wrapping_add(1);
                    let entry_end = plen as usize;
                    // MSG_COMMITTED_ENTRY body:
                    //   [partition_id:u16][term:u64][index:u64][entry_body].
                    // Strip the prefix and peel the Quantum canonical
                    // envelope (the apply path forwards the proposer's
                    // body verbatim — clustor's tagged-proposal handler
                    // strips any leading correlation_id upstream).
                    let entry_partition = u16::from_le_bytes([s.in_buf[0], s.in_buf[1]]);
                    let entry_term = u64::from_le_bytes([
                        s.in_buf[2],
                        s.in_buf[3],
                        s.in_buf[4],
                        s.in_buf[5],
                        s.in_buf[6],
                        s.in_buf[7],
                        s.in_buf[8],
                        s.in_buf[9],
                    ]);
                    let entry_index = u64::from_le_bytes([
                        s.in_buf[10],
                        s.in_buf[11],
                        s.in_buf[12],
                        s.in_buf[13],
                        s.in_buf[14],
                        s.in_buf[15],
                        s.in_buf[16],
                        s.in_buf[17],
                    ]);
                    let cur = apply_slot(entry_partition);

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
                    if entry_index <= s.apply_index[cur] {
                        continue;
                    }
                    if entry_index > s.apply_index[cur] + 1 && s.apply_index[cur] > 0 {
                        // A jump in one partition's committed stream reads
                        // as a snapshot install and resets EVERY apply-side
                        // arena on this node, sessions included. Said at
                        // warning level with the partition and both
                        // indices: a jump the substrate did not intend
                        // (a mis-framed entry, a dropped one) is otherwise
                        // indistinguishable from a quiet restart.
                        let mut line = [0u8; 72];
                        let mut pos = 0usize;
                        for &b in b"[sess] apply gap p=" {
                            line[pos] = b;
                            pos += 1;
                        }
                        pos += fmt_u32_raw(line.as_mut_ptr().add(pos), u32::from(entry_partition));
                        for &b in b" from=" {
                            line[pos] = b;
                            pos += 1;
                        }
                        pos += fmt_u32_raw(
                            line.as_mut_ptr().add(pos),
                            s.apply_index[cur].min(u32::MAX as u64) as u32,
                        );
                        for &b in b" to=" {
                            line[pos] = b;
                            pos += 1;
                        }
                        pos += fmt_u32_raw(
                            line.as_mut_ptr().add(pos),
                            entry_index.min(u32::MAX as u64) as u32,
                        );
                        dev_log(sys, 2, line.as_ptr(), pos);
                        apply_reset(s, sys, entry_partition, entry_index);
                    }
                    if entry_end > wire::COMMITTED_ENTRY_HDR {
                        let peeled =
                            wire::peel_qprop(&s.in_buf[wire::COMMITTED_ENTRY_HDR..entry_end]);
                        if peeled.is_none() {
                            // An operator swap record (`QASW`): forwarded
                            // to the anchor, sequenced by its log position.
                            // It still reaches the control plane below.
                            let mut rec = [0u8; wire::QA_SWAP_RECORD_LEN];
                            let n = (entry_end - wire::COMMITTED_ENTRY_HDR).min(rec.len());
                            rec[..n].copy_from_slice(
                                &s.in_buf[wire::COMMITTED_ENTRY_HDR..wire::COMMITTED_ENTRY_HDR + n],
                            );
                            worker::forward_swap_record(
                                &mut s.worker,
                                sys,
                                &rec[..n],
                                entry_partition,
                                entry_index,
                            );
                        }
                        if peeled.is_none() && s.out_cp >= 0 {
                            // Not a Quantum operation: an epoch or
                            // migration record, an admin or config
                            // entry. The control plane reads those from
                            // this module, not from a second consumer
                            // on the committed stream (manifest,
                            // `cp_out`). `outputs_ready_for_apply`
                            // checked the edge for room before this
                            // entry was taken; a refusal past that
                            // check is counted and said.
                            if try_emit(
                                sys,
                                s.out_cp,
                                wire::MSG_COMMITTED_ENTRY,
                                &s.in_buf[..entry_end],
                            ) {
                                s.cp_forwarded = s.cp_forwarded.wrapping_add(1);
                            } else {
                                s.cp_refused = s.cp_refused.wrapping_add(1);
                                dev_log(sys, 2, b"[sess] cp forward refused".as_ptr(), 25);
                            }
                        }
                        // The ledger applies on every worker; everything else
                        // only on the worker that holds the slot, and not if
                        // the slot's import record already contains it.
                        let mine = match peeled {
                            Some(p) => {
                                let body_start = wire::COMMITTED_ENTRY_HDR + p.op_body_offset;
                                replay_apply_ledger(s, &p, body_start, entry_end);
                                if matches!(p.op, wire::QOP_DELIVER | wire::QOP_DELIVER_ACK) {
                                    None
                                } else if op_names_no_session(p.op) {
                                    // The log's, on every worker: no slot
                                    // owns it, so no skip mark or import
                                    // floor applies (a session imported at
                                    // the slot such an entry happens to
                                    // carry must not filter the log).
                                    Some(p)
                                } else if !entry_is_mine(s, p.op, p.session_slot) {
                                    let slot = p.session_slot as usize;
                                    if slot < MAX_SESSIONS {
                                        s.skip_part[slot] = entry_partition;
                                        s.skip_index[slot] = entry_index;
                                    }
                                    None
                                } else if replay_floor_filters(
                                    s,
                                    p.session_slot,
                                    entry_partition,
                                    entry_index,
                                ) {
                                    s.replay_floor_skipped = s.replay_floor_skipped.wrapping_add(1);
                                    None
                                } else {
                                    Some(p)
                                }
                            }
                            None => None,
                        };
                        if let Some(p) = mine {
                            let body_start = wire::COMMITTED_ENTRY_HDR + p.op_body_offset;
                            // The index of the entry being applied RIGHT
                            // NOW. `apply_index` is the last COMPLETED
                            // one and is only advanced below, so a
                            // handler that needs to record where its
                            // record lives in the raft log cannot read
                            // it from there.
                            s.applying_index = entry_index;
                            s.applying_partition = entry_partition;
                            apply_committed_op(s, sys, &p, body_start, entry_end, now);
                        }
                    }
                    s.apply_index[cur] = entry_index;
                    note_applied_entry(s, entry_partition, entry_index, entry_term);
                    replay_release_floors(s);
                } else if plen > 0 {
                    s.applied = s.applied.wrapping_add(1);
                }
            }
        }

        // ── Phase 3a: report the applied term runs to `flow` ──
        flush_applied_runs(s, sys, now);

        // ── Phase 3b: drain proposal-assigned events from consensus ──
        //
        // Each MSG_PROPOSAL_ASSIGNED carries
        //   [correlation_id:u64 LE][partition_id:u16 LE][wal_index:u64 LE]
        //   [term:u64 LE]
        // (26 bytes). We forward `(partition_id, wal_index, term)` to
        // the ack component via MSG_ACK_REGISTER: the partition id
        // disambiguates the same wal_index arriving from different
        // partitions, and the term names WHICH entry at that index — a
        // leader change can commit another one there, which must fail the
        // operation rather than acknowledge it.
        if s.in_assigned >= 0 {
            // 32/tick, matching the proposal-side burst capacity — every
            // tagged proposal produces exactly one assignment.
            for _ in 0..32 {
                let poll = (sys.channel_poll)(s.in_assigned, 0x01);
                if poll <= 0 || (poll as u32 & 0x01) == 0 {
                    break;
                }
                let (mt, plen) = {
                    worked += 1;
                    wire::channel_read_msg(sys, s.in_assigned, &mut s.in_buf)
                };
                if mt == wire::MSG_PROPOSAL_REFUSED {
                    // No assignment follows a refusal: free the
                    // correlation now and fail what waits on it.
                    if let Some(r) = refusal::decode_refused(&s.in_buf[..plen as usize]) {
                        on_proposal_refused(s, sys, r);
                    }
                    continue;
                }
                if mt != wire::MSG_PROPOSAL_ASSIGNED
                    || (plen as usize) < wire::PROPOSAL_ASSIGNED_LEN
                {
                    continue;
                }
                let (cid, partition_id, wal_index, wal_term) =
                    wire::decode_proposal_assigned(&s.in_buf[..wire::PROPOSAL_ASSIGNED_LEN]);
                s.hb_assigned = s.hb_assigned.wrapping_add(1);
                let issued = correlate::issued_ms(&s.correlate, cid);
                if let Some((session_slot, packet_id, _op)) = correlate::take(&mut s.correlate, cid)
                {
                    if !s.trace_armed && s.proposals_emitted & 31 == 0 {
                        if let Some(t) = issued {
                            s.trace_armed = true;
                            s.trace_slot = session_slot;
                            s.trace_packet = packet_id;
                            s.trace_issued_ms = t;
                            s.trace_assign_ms =
                                dev_millis(sys).wrapping_sub(t).min(u32::MAX as u64) as u32;
                        }
                    }
                    // MSG_ACK_REGISTER — shape in `wire::ACK_REGISTER_LEN`.
                    // Both OP_PUBLISH and OP_PUBREL register with the same
                    // payload shape; the publisher inflight's `phase` field
                    // distinguishes them when MSG_ACK_EMIT later fires for
                    // the same (session_slot, packet_id) tuple. The epoch
                    // pins the completion to this session generation.
                    let reg_epoch = sessions::generation(&s.sessions, session_slot as usize);
                    let reg = term::encode_register(&term::Registration {
                        session_slot,
                        message_id: packet_id as u32,
                        partition_id,
                        wal_index,
                        session_generation: reg_epoch,
                        term: wal_term,
                    });

                    // Stamp the wal_index onto the inflight slot regardless
                    // of register success — the publisher state machine
                    // benefits from this even if the broker→ack
                    // edge is momentarily saturated.
                    let ssize = session_slot as usize;
                    if ssize < MAX_SESSIONS {
                        if let Some(ii) =
                            sessions::inflight_find(&s.sessions, ssize, packet_id, INFLIGHT_PUB)
                        {
                            sessions::inflight_set_wal_index(
                                &mut s.sessions,
                                ssize,
                                ii,
                                partition_id,
                                wal_index,
                                wal_term,
                            );
                        }
                    }
                    // NOTE: `wal_index` is NOT the produce's
                    // `base_offset`. It counts raft entries, so stamping
                    // it here would tell a producer an offset unrelated
                    // to the one a consumer fetches — a fresh topic
                    // would answer 2 where Kafka requires 0. The real
                    // logical offset is assigned in
                    // `apply_kafka_produce`, which stamps it there. If
                    // apply never stamps — the batch was refused, or
                    // this node did not originate the produce — the slot
                    // keeps its initial -1, which is Kafka's "unknown"
                    // and is honest.

                    // Try once; on failure park in the retry queue. Phase 5-pre
                    // drains it next tick. Dropping the register would mean
                    // the ack component never learns of this inflight entry and the
                    // publisher's PUBACK never fires.
                    //
                    // The parking slot was reserved when this proposal's
                    // correlation was allocated — before the publish was
                    // accepted — so parking succeeds. A failure here is a
                    // broken reservation invariant, not a capacity
                    // decision: say so rather than account accepted work
                    // as dropped.
                    s.hb_reg = s.hb_reg.wrapping_add(1);
                    if !try_emit(sys, s.out_forward, wire::MSG_ACK_REGISTER, &reg)
                        && !correlate::ack_stash(&mut s.correlate, reg)
                    {
                        dev_log(sys, 1, b"[sess] ack register unparked".as_ptr(), 28);
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
                // A completion is answered to the client at once.
                if !hold_ready(s) {
                    break;
                }
                let poll = (sys.channel_poll)(s.in_ack, 0x01);
                if poll <= 0 || (poll as u32 & 0x01) == 0 {
                    break;
                }
                let (mt, plen) = {
                    worked += 1;
                    wire::channel_read_msg(sys, s.in_ack, &mut s.in_buf)
                };
                if plen < 8 {
                    continue;
                }
                // A completion names its verdict; one too short to carry
                // it answers nothing.
                let status = if mt == wire::MSG_ACK_EMIT {
                    match term::emit_status(&s.in_buf[..plen as usize]) {
                        Some(st) => st,
                        None => continue,
                    }
                } else {
                    term::STATUS_COMMITTED
                };
                let session_slot =
                    u32::from_le_bytes([s.in_buf[0], s.in_buf[1], s.in_buf[2], s.in_buf[3]])
                        as usize;
                let packet_id =
                    u32::from_le_bytes([s.in_buf[4], s.in_buf[5], s.in_buf[6], s.in_buf[7]]) as u16;

                // Kafka produce inflights live in a reserved session_slot
                // namespace (see KAFKA_SLOT_BASE). Quorum durability landed
                // → emit the ProduceResponse with base_offset = wal_index.
                // MSG_ACK_REDELIVER is ignored for Kafka: the producer's own
                // request timeout drives the retry; the stale-entry sweep in
                // the metrics tick reclaims the slot.
                if session_slot >= KAFKA_SLOT_BASE as usize {
                    if mt != wire::MSG_ACK_EMIT {
                        continue;
                    }
                    // Decode slot + epoch from session_slot. An epoch
                    // mismatch means this durability landed for a slot
                    // that has since been freed and reused — drop it
                    // (EPOCH TAGGING) rather than completing an unrelated
                    // request's publish. The packet id's high byte is not
                    // bounded here: a produce's names one of its
                    // `KIN_MAX_PARTS` partitions, but an OffsetCommit's
                    // counts its gated proposals, up to 254, and every one
                    // must land for the parked response to go out.
                    let (ki, epoch) = store::kin_decode_slot(session_slot as u32);
                    if ki >= KAFKA_INFLIGHT || !store::inflight_valid(&s.store, ki, epoch) {
                        continue;
                    }
                    // An answer already decided and owed stands.
                    if store::inflight_owed(&s.store, ki) != store::ANSWER_NONE {
                        continue;
                    }
                    if status != term::STATUS_COMMITTED {
                        // Not the entry committed at its index: fail the
                        // partition / commit / confirm as a refusal with
                        // that status would, so the client retries.
                        s.acks_superseded = s.acks_superseded.wrapping_add(1);
                        #[cfg(any(feature = "kafka", feature = "amqp"))]
                        fail_kin(s, sys, session_slot as u32, packet_id, status);
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
                        if answer_inflight(s, sys, ki, store::ANSWER_ACK) {
                            s.amqp_publish_acked = s.amqp_publish_acked.wrapping_add(1);
                            s.acks_emitted = s.acks_emitted.wrapping_add(1);
                        }
                        // With no room on the bus the Ack is owed in the
                        // slot and answered on a later step.
                        continue;
                    }
                    #[cfg(feature = "kafka")]
                    if store::inflight_proto(&s.store, ki) == KIN_PROTO_KAFKA_OFFSET {
                        // An OffsetCommit whose response was parked
                        // until its proposals were durable. Fire the
                        // stored body once every one has landed — the
                        // client is told "committed" only now.
                        if store::inflight_complete_part(&mut s.store, ki)
                            && answer_inflight(s, sys, ki, store::ANSWER_OFFSET)
                        {
                            s.acks_emitted = s.acks_emitted.wrapping_add(1);
                        }
                        continue;
                    }
                    #[cfg(feature = "kafka")]
                    {
                        // One partition of the request reached quorum
                        // durability. Respond once ALL have resolved.
                        store::inflight_complete_part(&mut s.store, ki);
                        if store::inflight_ready(&s.store, ki)
                            && answer_inflight(s, sys, ki, store::ANSWER_PRODUCE)
                        {
                            s.kafka_produce_acked = s.kafka_produce_acked.wrapping_add(1);
                            s.acks_emitted = s.acks_emitted.wrapping_add(1);
                        }
                    }
                    continue;
                }
                if session_slot >= MAX_SESSIONS {
                    continue;
                }
                // MQTT completions carry the session generation the
                // publish was accepted under. MQTT packet ids are
                // client-chosen, so `(session_slot, packet_id)` alone
                // matches a delayed or duplicate completion against
                // whichever client now holds the slot. A mismatch means
                // the flow this answers is gone; drop it. Epoch 0 is a
                // registration made before the session had one and is
                // not matched against.
                let mut ack_wal: u64 = 0;
                if (plen as usize) >= wire::ACK_EMIT_LEN {
                    let emit_epoch =
                        u32::from_le_bytes([s.in_buf[8], s.in_buf[9], s.in_buf[10], s.in_buf[11]]);
                    let live_epoch = sessions::generation(&s.sessions, session_slot);
                    if emit_epoch != 0 && emit_epoch != live_epoch {
                        s.acks_stale_epoch = s.acks_stale_epoch.wrapping_add(1);
                        continue;
                    }
                    let mut w = [0u8; 8];
                    w.copy_from_slice(&s.in_buf[12..20]);
                    ack_wal = u64::from_le_bytes(w);
                }

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
                        // Completions reach every worker on the node. One
                        // for a session this worker has imported but not
                        // resumed belongs to the exporter's copy of the
                        // flow: touching the imported ledger here would
                        // release an inflight whose completion the
                        // client never saw (it is re-registered on RESUME).
                        if worker::phase(&s.worker, conn_id) == worker::Phase::Imported {
                            continue;
                        }
                        let Some(ii) = sessions::inflight_find(
                            &s.sessions,
                            session_slot,
                            packet_id,
                            INFLIGHT_PUB,
                        ) else {
                            continue;
                        };
                        let iv = sessions::inflight_view(&s.sessions, session_slot, ii);
                        let qos = iv.map(|v| v.qos).unwrap_or(0);
                        let phase = iv.map(|v| v.phase).unwrap_or(0);
                        let correlation_id = iv.map(|v| v.correlation_id).unwrap_or(0);
                        let inflight_wal = iv.map(|v| v.wal_index).unwrap_or(0);
                        // The completion must answer THIS registration:
                        // a QoS 2 flow's PUBLISH completion landing after
                        // its PUBREL was proposed is not the PUBREL's.
                        let awaiting_pubrel_assignment =
                            qos == 2 && phase == QOS2_PUBREL && inflight_wal == 0;
                        if ack_wal != 0 && (ack_wal != inflight_wal || awaiting_pubrel_assignment) {
                            s.acks_stale_epoch = s.acks_stale_epoch.wrapping_add(1);
                            continue;
                        }

                        if status != term::STATUS_COMMITTED {
                            // Another entry was committed at this flow's
                            // index (or its term is unknown here): the
                            // client must resend — DISCONNECT, as for a
                            // refusal, and like a refusal it leaves the
                            // publish stash alone (`fail_publisher_flow`).
                            s.acks_superseded = s.acks_superseded.wrapping_add(1);
                            let pubrel = qos == 2 && phase == QOS2_PUBREL;
                            // An exported session's flow is the importer's,
                            // which re-registers it and hears its own verdict.
                            if worker::is_frozen(&s.worker, conn_id) {
                                continue;
                            }
                            fail_publisher_flow(s, sys, session_slot, ii, pubrel, status);
                            continue;
                        }

                        // Stash bookkeeping for PUBLISH ops — mark durable
                        // and finalise if dedup already resolved.
                        if correlation_id != 0 {
                            if let Some(stash_idx) =
                                correlate::stash_by_correlation(&s.correlate, correlation_id)
                            {
                                correlate::stash_mark_durable(&mut s.correlate, stash_idx);
                                release_ready_stashes(s, sys);
                            }
                        }

                        if s.trace_armed
                            && s.trace_slot as usize == session_slot
                            && s.trace_packet == packet_id
                        {
                            s.trace_armed = false;
                            let total = dev_millis(sys).wrapping_sub(s.trace_issued_ms);
                            let mut line = [0u8; 64];
                            let mut pos = 0usize;
                            for &b in b"[sess] qos1 age assign_ms=" {
                                line[pos] = b;
                                pos += 1;
                            }
                            pos += fmt_u32_raw(line.as_mut_ptr().add(pos), s.trace_assign_ms);
                            for &b in b" ack_ms=" {
                                line[pos] = b;
                                pos += 1;
                            }
                            pos += fmt_u32_raw(
                                line.as_mut_ptr().add(pos),
                                total.min(u32::MAX as u64) as u32,
                            );
                            dev_log(sys, 3, line.as_ptr(), pos);
                        }
                        // An exported session's copy of the flow: the
                        // stash above was this worker's to finalise, the
                        // client-facing ledger is now the importer's.
                        if worker::is_frozen(&s.worker, conn_id) {
                            continue;
                        }
                        let body = packet_id.to_be_bytes();
                        if qos == 1 {
                            sessions::inflight_release(&mut s.sessions, session_slot, ii);
                            emit_codec_response(s, sys, conn_id, PROTO_MQTT, PKT_PUBACK, 0, &body);
                        } else if qos == 2 {
                            if phase == QOS2_PUBLISH {
                                // PUBLISH commit landed; emit PUBREC and
                                // keep inflight alive to await PUBREL.
                                emit_codec_response(
                                    s, sys, conn_id, PROTO_MQTT, PKT_PUBREC, 0, &body,
                                );
                            } else if phase == QOS2_PUBREL {
                                // PUBREL commit landed; emit PUBCOMP and
                                // release the publisher inflight.
                                sessions::inflight_release(&mut s.sessions, session_slot, ii);
                                emit_codec_response(
                                    s,
                                    sys,
                                    conn_id,
                                    PROTO_MQTT,
                                    PKT_PUBCOMP,
                                    0,
                                    &body,
                                );
                                s.qos2_comp = s.qos2_comp.wrapping_add(1);
                            }
                        }
                        s.acks_emitted = s.acks_emitted.wrapping_add(1);
                    }
                    wire::MSG_ACK_REDELIVER => {
                        s.redeliver_signals = s.redeliver_signals.wrapping_add(1);
                        if !sessions::is_active(&s.sessions, session_slot) {
                            continue;
                        }
                        let Some(ii) = sessions::inflight_find(
                            &s.sessions,
                            session_slot,
                            packet_id,
                            INFLIGHT_PUB,
                        ) else {
                            continue;
                        };
                        let correlation_id = sessions::inflight_view(&s.sessions, session_slot, ii)
                            .map(|v| v.correlation_id)
                            .unwrap_or(0);
                        if correlation_id == 0 {
                            continue;
                        }
                        let phase = sessions::inflight_view(&s.sessions, session_slot, ii)
                            .map(|v| v.phase)
                            .unwrap_or(0);

                        // For PUBREL-phase inflight, the stash has already
                        // been released after the PUBLISH commit. Resend a
                        // QOP_PUBREL marker carrying the same correlation_id
                        // so the ack component can drive PUBCOMP when this round
                        // commits.
                        if phase == QOS2_PUBREL {
                            let tenant = sessions::tenant(&s.sessions, session_slot);
                            let stream_hash = sessions::stream_hash(&s.sessions, session_slot);
                            let qbody_len = 2 + 8 + 4;
                            let prop_total = wire::QPROP_TAGGED_HDR_LEN + qbody_len;
                            if prop_total <= s.out_buf.len() {
                                s.out_buf[0..8].copy_from_slice(&correlation_id.to_le_bytes());
                                wire::encode_qprop_header(
                                    &mut s.out_buf[8..8 + wire::QPROP_HEADER_LEN],
                                    wire::QOP_PUBREL,
                                    tenant,
                                    session_slot as u32,
                                );
                                let off = wire::QPROP_TAGGED_HDR_LEN;
                                s.out_buf[off..off + 2].copy_from_slice(&packet_id.to_be_bytes());
                                s.out_buf[off + 2..off + 10]
                                    .copy_from_slice(&stream_hash.to_le_bytes());
                                let flow_epoch =
                                    sessions::inflight_view(&s.sessions, session_slot, ii)
                                        .map(|v| v.session_generation)
                                        .filter(|e| *e != 0)
                                        .unwrap_or_else(|| {
                                            sessions::generation(&s.sessions, session_slot)
                                        });
                                s.out_buf[off + 10..off + 14]
                                    .copy_from_slice(&flow_epoch.to_le_bytes());
                                let shard = sessions::shard(&s.sessions, session_slot);
                                if try_emit_keyed(
                                    sys,
                                    s.out_proposals_tagged,
                                    &s.view,
                                    shard,
                                    &s.out_buf[..prop_total],
                                ) {
                                    s.proposals_emitted = s.proposals_emitted.wrapping_add(1);
                                    s.hb_prop = s.hb_prop.wrapping_add(1);
                                }
                            }
                            continue;
                        }

                        // PUBLISH phase — reconstruct the tagged QOP_PUBLISH
                        // envelope from the still-active stash. The stash
                        // holds the QOP_PUBLISH op-body verbatim; wrap it in
                        // a fresh tagged envelope and repropose.
                        let Some(stash_idx) =
                            correlate::stash_by_correlation(&s.correlate, correlation_id)
                        else {
                            continue;
                        };
                        let env_len = correlate::stash_env_len(&s.correlate, stash_idx);
                        if !(18..=MAX_STASH_ENV).contains(&env_len) {
                            continue;
                        }
                        // TENANCY GAP: keyed by correlation, not session.
                        // See `TENANT_UNRESOLVED`.
                        let tenant: TenantId = TENANT_UNRESOLVED;
                        let prop_total = wire::QPROP_TAGGED_HDR_LEN + env_len;
                        if prop_total > s.out_buf.len() {
                            continue;
                        }
                        s.out_buf[0..8].copy_from_slice(&correlation_id.to_le_bytes());
                        wire::encode_qprop_header(
                            &mut s.out_buf[8..8 + wire::QPROP_HEADER_LEN],
                            wire::QOP_PUBLISH,
                            tenant,
                            session_slot as u32,
                        );
                        let off = wire::QPROP_TAGGED_HDR_LEN;
                        let stash_env = correlate::stash_env(&s.correlate, stash_idx);
                        let stash_ptr = stash_env.as_ptr();
                        let out_ptr = s.out_buf.as_mut_ptr();
                        core::ptr::copy_nonoverlapping(stash_ptr, out_ptr.add(off), env_len);
                        // Re-derive the topic shard from the stashed op body
                        // so the repropose lands on the SAME partition as
                        // the original publish. The stash holds the
                        // QOP_PUBLISH body verbatim: topic_len is a BE u16
                        // at offset 16, the topic follows at 18.
                        let stash_topic_len = if env_len >= 18 {
                            u16::from_be_bytes([stash_env[16], stash_env[17]]) as usize
                        } else {
                            0
                        };
                        let shard = if stash_topic_len > 0 && 18 + stash_topic_len <= env_len {
                            wire::shard_mqtt_topic(tenant, &stash_env[18..18 + stash_topic_len])
                        } else {
                            // Malformed stash: fall back to the session
                            // shard rather than guessing a topic.
                            wire::shard_mqtt_stream(tenant, 0)
                        };
                        if emit_keyed_unowned(
                            sys,
                            s.out_proposals_tagged,
                            &s.view,
                            shard,
                            &s.out_buf[..prop_total],
                        ) {
                            s.proposals_emitted = s.proposals_emitted.wrapping_add(1);
                            s.hb_prop = s.hb_prop.wrapping_add(1);
                        }
                    }
                    _ => {}
                }
            }
        }

        // ── Phase 5-pre: offer again the ready stashes the topic bus
        // refused, oldest-ready first.
        release_ready_stashes(s, sys);

        // AMQP push delivery: drain the message store to registered
        // consumers with available prefetch credit.
        #[cfg(feature = "amqp")]
        amqp_delivery_pump(s, sys);

        // Phase 5-pre also drains pending ACK_REGISTERs that were
        // deferred by out_forward backpressure (see PROPOSAL_ASSIGNED
        // handler).
        if correlate::ack_parked(&s.correlate) > 0 {
            for i in 0..PENDING_ACK_SLOTS {
                let Some(payload) = correlate::ack_get(&s.correlate, i) else {
                    continue;
                };
                if try_emit(sys, s.out_forward, wire::MSG_ACK_REGISTER, &payload) {
                    correlate::ack_free(&mut s.correlate, i);
                }
            }
        }

        // ── Phase 5a: topic deliveries → encode PUBLISH to subscriber
        //
        // topic_engine emits MSG_TOPIC_DELIVER with payload:
        //   [session_slot:u32][sub_qos:u8][_pad:u8;3][tenant:u32]
        //   [topic_len:u16][topic][payload]
        //
        // Drain order: first retry the backpressured envelopes parked in
        // pending_dlv, oldest first — a session's later ones wait while an
        // earlier one is still backpressured — then accept fresh
        // deliveries, each parked behind any of its session's, or, with
        // the park full, joining its session's offline queue
        // (`deliver_in_order`). `deliver_in` is read whenever the outbox
        // has room for that, which never waits on a client.

        // 5a-pre: drain deferred deliveries.
        let mut order = [0usize; PENDING_DLV_SLOTS];
        let parked = correlate::dlv_order(&s.correlate, &mut order);
        let mut blocked = [0u32; PENDING_DLV_SLOTS];
        let mut nblocked = 0usize;
        for &s_idx in &order[..parked] {
            // A retry may move its session's parked ones to its queue.
            if !messaging_room(s, SPILL_ROOM) {
                break;
            }
            if !correlate::dlv_is_active(&s.correlate, s_idx) {
                // Moved to its queue with an earlier one of its session's.
                continue;
            }
            let env_len = correlate::dlv_len(&s.correlate, s_idx);
            let mut buf = [0u8; MAX_PENDING_DLV];
            if !correlate::dlv_copy_out(&s.correlate, s_idx, &mut buf) || env_len < 4 {
                correlate::dlv_release(&mut s.correlate, s_idx);
                continue;
            }
            let slot = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
            if blocked[..nblocked].contains(&slot) {
                continue;
            }
            // A parked delivery is older than anything its session's
            // offline queue took meanwhile, so it goes ahead of it.
            let from = parked_from(s, s_idx);
            match offer(s, sys, &buf[..env_len], from, true) {
                DeliverResult::Delivered | DeliverResult::Dropped => {
                    correlate::dlv_release(&mut s.correlate, s_idx);
                }
                DeliverResult::Waits(why) => match order::replace(from, why) {
                    order::Place::Queue => {
                        // Now waiting on its client: it, and its session's
                        // later parked ones, join the session's queue.
                        spill_session(s, sys, slot, None);
                    }
                    order::Place::Park => {
                        // Kept, and the session's later ones wait behind it.
                        blocked[nblocked] = slot;
                        nblocked += 1;
                    }
                },
            }
        }

        // 5a-new: accept fresh deliveries.
        if s.in_deliver >= 0 {
            for _ in 0..DELIVER_BUDGET {
                // A park slot for one that waits on something shared, and
                // room to queue one that waits on its client with what its
                // session has parked.
                if park_room(s) == 0 || !messaging_room(s, SPILL_ROOM) {
                    break;
                }
                let poll = (sys.channel_poll)(s.in_deliver, 0x01);
                if poll <= 0 || (poll as u32 & 0x01) == 0 {
                    break;
                }
                let (mt, plen) = {
                    worked += 1;
                    wire::channel_read_msg(sys, s.in_deliver, &mut s.in_buf)
                };
                dev_log(sys, 3, b"[sess] deliver rx".as_ptr(), 17);
                if mt != wire::MSG_TOPIC_DELIVER || plen < 14 {
                    continue;
                }
                let plen = plen as usize;

                let mut buf = [0u8; MAX_PENDING_DLV];
                if plen > buf.len() {
                    // Oversize for both delivery and defer paths; drop.
                    s.deliveries_throttled = s.deliveries_throttled.wrapping_add(1);
                    continue;
                }
                buf[..plen].copy_from_slice(&s.in_buf[..plen]);

                if !deliver_in_order(s, sys, &buf[..plen], order::From::Fresh) {
                    s.deliveries_throttled = s.deliveries_throttled.wrapping_add(1);
                }
            }
        }

        // ── Phase 5b: drain dedup / retained / offline / group / txn results.
        // MSG_DEDUP_RESULT carries
        // `[dedup_key:[u8;20]][duplicate:u8][phase:u8]`. For
        // QoS 1+ stashed publishes, this is the dedup half of the
        // commit-gating contract; once both this and durability resolve
        // OK, the stashed envelope is emitted to topic_engine.
        if s.in_messaging >= 0 {
            for _ in 0..16 {
                let poll = (sys.channel_poll)(s.in_messaging, 0x01);
                if poll <= 0 || (poll as u32 & 0x01) == 0 {
                    break;
                }
                let (mt, plen) = {
                    worked += 1;
                    wire::channel_read_msg(sys, s.in_messaging, &mut s.in_buf)
                };
                let plen = plen as usize;
                match mt {
                    wire::MSG_DEDUP_RESULT if plen >= 21 => {
                        let mut key = [0u8; 20];
                        key.copy_from_slice(&s.in_buf[..20]);
                        // Verdict byte: 0 new, 1 duplicate, 2 refused
                        // (dedup shard full — see `dedup::DEDUP_VERDICT_*`).
                        let verdict = match s.in_buf[20] {
                            0 => STASH_DEDUP_OK,
                            1 => STASH_DEDUP_DUPLICATE,
                            _ => STASH_DEDUP_REFUSED,
                        };
                        if let Some(stash_idx) = correlate::stash_by_dedup_key(&s.correlate, &key) {
                            correlate::stash_set_dedup_state(&mut s.correlate, stash_idx, verdict);
                            release_ready_stashes(s, sys);
                        }
                    }
                    wire::MSG_RETAINED_READ if plen >= 23 => {
                        // Each answer spends a credit the park slot was
                        // reserved for; one for a session that went away,
                        // or whose slot another session now holds, is
                        // dropped, its credit returned.
                        let answered = u32::from_le_bytes([
                            s.in_buf[12],
                            s.in_buf[13],
                            s.in_buf[14],
                            s.in_buf[15],
                        ]);
                        let stream = sessions::stream_hash(&s.sessions, answered as usize);
                        if s.mflow.read_answer(answered, stream) == mflow::Answer::Drop {
                            continue;
                        }
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
                            s.in_buf[0],
                            s.in_buf[1],
                            s.in_buf[2],
                            s.in_buf[3],
                        ]);
                        // bytes 4..12 are topic_hash — not needed for delivery.
                        let session_slot = u32::from_le_bytes([
                            s.in_buf[12],
                            s.in_buf[13],
                            s.in_buf[14],
                            s.in_buf[15],
                        ]);
                        let sub_qos = s.in_buf[16];
                        let topic_len = u16::from_le_bytes([s.in_buf[17], s.in_buf[18]]) as usize;
                        if 19 + topic_len + 4 > plen {
                            continue;
                        }
                        let payload_len_off = 19 + topic_len;
                        let payload_len = u32::from_le_bytes([
                            s.in_buf[payload_len_off],
                            s.in_buf[payload_len_off + 1],
                            s.in_buf[payload_len_off + 2],
                            s.in_buf[payload_len_off + 3],
                        ]) as usize;
                        let payload_off = payload_len_off + 4;
                        if payload_off + payload_len > plen {
                            continue;
                        }

                        // MSG_TOPIC_DELIVER includes a (possibly empty)
                        // user_props block between topic and payload —
                        // item 6 contract. Retained deliveries carry
                        // no user properties (the retained store doesn't
                        // store them today), so the block is a single
                        // zero-count byte.
                        let dlv_total = 14 + topic_len + 1 + payload_len;
                        if dlv_total > MAX_PENDING_DLV {
                            continue;
                        }
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

                        if !deliver_in_order(s, sys, &buf[..dlv_total], order::From::Retained) {
                            s.deliveries_throttled = s.deliveries_throttled.wrapping_add(1);
                        }
                    }
                    wire::MSG_RETAINED_READ_END if plen >= 8 => {
                        let session = u32::from_le_bytes([
                            s.in_buf[0],
                            s.in_buf[1],
                            s.in_buf[2],
                            s.in_buf[3],
                        ]);
                        let next = u32::from_le_bytes([
                            s.in_buf[4],
                            s.in_buf[5],
                            s.in_buf[6],
                            s.in_buf[7],
                        ]);
                        s.mflow.read_end(session, next);
                    }
                    wire::MSG_OFFLINE_DRAINED if plen >= 8 => {
                        let session = u32::from_le_bytes([
                            s.in_buf[0],
                            s.in_buf[1],
                            s.in_buf[2],
                            s.in_buf[3],
                        ]);
                        let remaining = u32::from_le_bytes([
                            s.in_buf[4],
                            s.in_buf[5],
                            s.in_buf[6],
                            s.in_buf[7],
                        ]);
                        s.mflow.drained(session, remaining);
                    }
                    wire::MSG_OFFLINE_DRAIN if plen >= 6 => {
                        // Body: [session:u32][env_len:u16][env]. It spends
                        // the credit its drain was granted — a park slot
                        // reserved for it — and goes through the standard
                        // delivery path, parked when backpressured. One
                        // for a session that went away, or whose slot
                        // another session now holds, is dropped.
                        let drained = u32::from_le_bytes([
                            s.in_buf[0],
                            s.in_buf[1],
                            s.in_buf[2],
                            s.in_buf[3],
                        ]);
                        let stream = sessions::stream_hash(&s.sessions, drained as usize);
                        if s.mflow.drain_answer(drained, stream) == mflow::Answer::Drop {
                            continue;
                        }
                        let env_len = u16::from_le_bytes([s.in_buf[4], s.in_buf[5]]) as usize;
                        if 6 + env_len > plen {
                            continue;
                        }
                        let mut buf = [0u8; MAX_PENDING_DLV];
                        if env_len > buf.len() {
                            continue;
                        }
                        // Copy the embedded envelope (no session/env_len
                        // prefix) into a local buffer so try_deliver can
                        // operate without holding a borrow on s.in_buf.
                        let src = s.in_buf.as_ptr().add(6);
                        let dst = buf.as_mut_ptr();
                        core::ptr::copy_nonoverlapping(src, dst, env_len);
                        if !deliver_in_order(s, sys, &buf[..env_len], order::From::Drained) {
                            s.deliveries_throttled = s.deliveries_throttled.wrapping_add(1);
                        }
                    }
                    _ => {}
                }
            }
        }
        // What waits for `messaging` goes first, then the retained reads
        // and drains the park has room to credit.
        flush_messaging(s, sys);
        request_messaging(s, sys);

        // `cp_in` carries the control plane's placement: which shards
        // this node serves, and the fence while one is moving. Parsing
        // is the shared core's, so this module and `topic_engine`
        // cannot read one frame two ways.
        if s.in_cp >= 0 {
            for _ in 0..8 {
                // A placement update may release every session's queue.
                if !messaging_room(s, PLACEMENT_ROOM) {
                    break;
                }
                let poll = (sys.channel_poll)(s.in_cp, 0x01);
                if poll <= 0 || (poll as u32 & 0x01) == 0 {
                    break;
                }
                worked += 1;
                let (msg_type, plen) = wire::channel_read_msg(sys, s.in_cp, &mut s.in_buf);
                // The shard map rides the SAME channel as the placement
                // update because it answers the same question — who owns
                // this shard — and splitting it onto its own edge would
                // let the two arrive out of order, so a node could
                // resolve ownership against a new prg_count and an old
                // override set. One channel, one order.
                if msg_type == wire::MSG_SHARD_MAP_UPDATE {
                    match edge::apply_shard_map_update(
                        &mut s.view.overrides,
                        &s.in_buf[..plen as usize],
                    ) {
                        edge::PlacementUpdate::Applied => {
                            s.shard_map_updates = s.shard_map_updates.wrapping_add(1);
                        }
                        edge::PlacementUpdate::Stale => {
                            s.shard_map_stale = s.shard_map_stale.wrapping_add(1);
                        }
                        edge::PlacementUpdate::Malformed => {}
                    }
                    continue;
                }
                if msg_type != wire::MSG_PLACEMENT_UPDATE {
                    continue;
                }
                match edge::apply_placement_update(&mut s.view.view, &s.in_buf[..plen as usize]) {
                    edge::PlacementUpdate::Applied => {
                        // One line the FIRST time this module learns a
                        // placement, and never again: it is the only
                        // external evidence that `control_plane.routing`
                        // actually reaches this module, and a wiring gap
                        // here is silent — the module simply keeps its
                        // single-PRG default and believes it owns
                        // everything. `multi_node.sh` asserts on it.
                        // Bounded to one line so placement churn cannot
                        // turn it into a log flood.
                        if s.placement_updates == 0 {
                            dev_log(sys, 3, b"[sp] placement".as_ptr(), 14);
                        }
                        s.placement_updates = s.placement_updates.wrapping_add(1);
                        // Placement moved, so shards may have been
                        // reassigned away from this node. Release the
                        // sessions that went with them.
                        //
                        // `is_local`, NOT `owns_shard`: a fenced shard
                        // is still ours and its state must survive,
                        // because an aborted migration lifts the fence
                        // and leaves ownership where it was. Only a
                        // genuine reassignment releases.
                        //
                        // Swept on the update rather than continuously:
                        // placement changes are rare, and a per-step
                        // scan of the session table would cost the
                        // steady state to serve an event that happens
                        // during a migration.
                        // Borrowed, not copied: `EdgeMap` owns the
                        // override table and is deliberately not `Copy`.
                        let view = &s.view;
                        // The offline queue lives in `messaging` and is
                        // keyed by slot index, so a released slot must
                        // have its queue dropped or the next session
                        // allocated that slot inherits it. Emitted from
                        // inside the sweep rather than buffered: a fixed
                        // buffer would silently drop the tail once more
                        // sessions moved than it held, and "leaks only
                        // under a large migration" is exactly the bug
                        // that would never show up in a small test.
                        // The outbox and flow are borrowed apart from
                        // `s.sessions`, which the sweep holds mutably; the
                        // update is read only with room for a release per
                        // session (`PLACEMENT_ROOM`).
                        let out_messaging = s.out_messaging;
                        let mbox = &mut s.mbox;
                        let flow = &mut s.mflow;
                        let moved = &mut s.disc_owed;
                        let released = sessions::release_foreign(
                            &mut s.sessions,
                            |sh| view.is_local(sh),
                            |si, conn| {
                                send_messaging(
                                    mbox,
                                    sys,
                                    out_messaging,
                                    wire::MSG_OFFLINE_RELEASE,
                                    &(si as u32).to_le_bytes(),
                                );
                                flow.release(si as u32);
                                // Tell a still-connected client to go.
                                // Its session now lives elsewhere; left
                                // alone it would sit here receiving
                                // nothing, never reaching the CONNECT
                                // redirect. The codec spells the reason
                                // per version and drops it for 3.1.1,
                                // which has no server DISCONNECT. Owed, and
                                // written once its output holds nothing
                                // ahead of it (`flush_holds`): one update can
                                // release more sessions than a bus takes.
                                if let Some(conn_id) = conn {
                                    reply_hold::owe(moved, conn_id, wire::MQTT_REASON_MOVED);
                                }
                            },
                        );
                        s.sessions_released_foreign =
                            s.sessions_released_foreign.wrapping_add(released);
                        // `messaging` holds the retained store and the
                        // offline queues, which are per-shard state too,
                        // but it has no control-plane port. Rather than
                        // add one and an edge to every graph, the
                        // placement travels the op bus this module
                        // already drives — and it travels as the
                        // UNMODIFIED `MSG_PLACEMENT_UPDATE` frame, so
                        // `messaging` parses it with the same core call
                        // and there is still one wire format and one
                        // shard->owner derivation.
                        let pl = plen as usize;
                        let mut fwd = [0u8; 16];
                        if pl <= fwd.len() {
                            fwd[..pl].copy_from_slice(&s.in_buf[..pl]);
                            to_messaging(s, sys, wire::MSG_PLACEMENT_UPDATE, &fwd[..pl]);
                        }
                    }
                    // Stale and re-delivered updates are dropped. They
                    // are counted where that count is actionable —
                    // `topic_engine.routing_stale`, on the same frames
                    // from the same sender — rather than kept here as a
                    // second copy nothing reads.
                    edge::PlacementUpdate::Stale => {}
                    edge::PlacementUpdate::Malformed => {}
                }
            }
        }

        // ── Cold-read replies: a WAL entry that carries a Fetch's
        //    offset, arriving on a later step than the request.
        #[cfg(feature = "kafka")]
        if s.in_wal_reply >= 0 {
            for _ in 0..4 {
                // Durability replies on a PARTITIONED channel, so this
                // must read the 5-byte envelope. The plain reader takes
                // the low byte of `partition_id` as the message type,
                // which is why the reply appeared never to arrive.
                let (_pid, msg_type, plen) =
                    wire::channel_read_partitioned(sys, s.in_wal_reply, &mut s.in_buf);
                if msg_type == 0 && plen == 0 {
                    break;
                }
                if msg_type != wire::MSG_WAL_ENTRY_REPLY {
                    continue;
                }
                let pl = plen as usize;
                let Some((request_id, _term, _index, _prev)) =
                    wire::decode_wal_entry_reply(&s.in_buf[..pl])
                else {
                    continue;
                };
                let Some(slot) = (0..COLD_FETCH_SLOTS)
                    .find(|&i| s.cold[i].active == 1 && s.cold[i].request_id == request_id)
                else {
                    continue; // not ours, or already timed out
                };

                // An empty body is durability's NOT FOUND: the entry has
                // aged out of the location ring. Answer the error the
                // client would have had anyway.
                let body = &s.in_buf[wire::WAL_ENTRY_REPLY_HDR..pl];
                let mut records_off = 0usize;
                let mut records_len = 0usize;
                if body.len() > wire::QPROP_HEADER_LEN {
                    // The raft entry body is the client proposal: the
                    // qprop header, then the produce op-body
                    // (`wire::QOP_KAFKA_PRODUCE`).
                    let h = wire::QPROP_HEADER_LEN;
                    if let Some(p) = wire::decode_produce_body_hdr(&body[h..]) {
                        let start = h + p.records_at;
                        if start <= body.len() {
                            records_off = wire::WAL_ENTRY_REPLY_HDR + start;
                            records_len = body.len() - start;
                        }
                    }
                }

                let pidx = s.cold[slot].part_idx as usize;
                let hw = store::next_offset(&s.store, pidx) as i64;
                let ls = store::log_start(&s.store, pidx) as i64;
                let err = if records_len == 0 {
                    s.cold_refused = s.cold_refused.wrapping_add(1);
                    KERR_OFFSET_OUT_OF_RANGE
                } else {
                    s.cold_served = s.cold_served.wrapping_add(1);
                    KERR_NONE
                };
                // `in_buf` holds the records and `emit_cold_fetch_response`
                // writes `out_buf`, so the copy below cannot alias.
                let mut recs = [0u8; KAFKA_MAX_RECORDS_BYTES];
                let n = records_len.min(recs.len());
                if n > 0 {
                    recs[..n].copy_from_slice(&s.in_buf[records_off..records_off + n]);
                }
                if !emit_cold_fetch_response(s, sys, slot, err, hw, ls, &recs[..n]) {
                    // No room for the records: the client is owed an
                    // empty answer and fetches the offset again.
                    s.cold[slot].owed = if err == KERR_NONE {
                        COLD_OWED_EMPTY
                    } else {
                        COLD_OWED_TIMEOUT
                    };
                }
            }
        }

        // Reclaim parked cold fetches that never got an answer. A WAL
        // read is a disk read; a client that waited this long is better
        // served an error it can act on than a slot held for ever.
        #[cfg(feature = "kafka")]
        {
            let now_ms = dev_millis(sys);
            for i in 0..COLD_FETCH_SLOTS {
                if s.cold[i].active != 1 {
                    continue;
                }
                // An owed answer first; then the timeout's, owed in turn
                // when the bus has no room for it.
                let owed = s.cold[i].owed;
                let timed_out = now_ms.wrapping_sub(s.cold[i].issued_ms) > COLD_FETCH_TIMEOUT_MS;
                if owed == COLD_OWED_NONE && !timed_out {
                    continue;
                }
                let err = if owed == COLD_OWED_EMPTY {
                    KERR_NONE
                } else {
                    KERR_OFFSET_OUT_OF_RANGE
                };
                let pidx = s.cold[i].part_idx as usize;
                let hw = store::next_offset(&s.store, pidx) as i64;
                let ls = store::log_start(&s.store, pidx) as i64;
                if owed == COLD_OWED_NONE {
                    s.cold_refused = s.cold_refused.wrapping_add(1);
                }
                if !emit_cold_fetch_response(s, sys, i, err, hw, ls, &[]) {
                    s.cold[i].owed = if owed == COLD_OWED_NONE {
                        COLD_OWED_TIMEOUT
                    } else {
                        owed
                    };
                }
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
        // Replicate any group generation that advanced this tick. Run
        // on the metrics tick rather than every step: generations move
        // only on membership change, so a per-step scan would be pure
        // overhead, and a sub-second lag before the record is proposed
        // is bounded by the same window an unreplicated coordinator
        // failover already has.
        #[cfg(feature = "kafka")]
        if now.wrapping_sub(s.last_metrics_ms) >= 1000 {
            reconcile_group_generations(s, sys);
            publish_retention_floor(s, sys);

            // Time retention, swept on the 1 Hz tick rather than on
            // append: retention is a wall-clock policy, so records
            // expire while a partition is IDLE, and a sweep driven by
            // appends would keep a silent topic's records for ever —
            // exactly the topic whose retention an operator is most
            // likely to be relying on.
            //
            // Budgeted per partition per tick. Expiring a whole
            // partition in one step would be an unbounded walk inside a
            // step every client is waiting on; at this budget a backlog
            // drains steadily instead.
            if s.kafka_retention_ms > 0 {
                const EXPIRE_BUDGET: u32 = 16;
                let retention = s.kafka_retention_ms as u64;
                for pi in 0..KSTORE_PARTS {
                    let n =
                        store::expire_partition(&mut s.store, pi, now, retention, EXPIRE_BUDGET);
                    s.kafka_retention_evictions = s.kafka_retention_evictions.wrapping_add(n);
                }
            }

            // Key compaction, on the same tick and with the same
            // per-partition budget discipline: the scan is bounded and a
            // long droppable run drains over several ticks rather than
            // inside one step.
            if s.kafka_compact != 0 {
                const COMPACT_BUDGET: u32 = 16;
                for pi in 0..KSTORE_PARTS {
                    store::compact_partition(&mut s.store, pi, COMPACT_BUDGET);
                }
            }
        }

        // One accounting line per second so the stage that swallows
        // a publish names itself: what came in from the codec, what was
        // admitted, proposed, assigned, registered for an ack, and
        // applied — plus the refusals and holds along the way.
        if now.wrapping_sub(s.last_hb_ms) >= 1000 {
            s.last_hb_ms = now;
            // Every label below with ten digits after it.
            let mut line = [0u8; 1024];
            let mut pos = 0usize;
            for (label, v) in [
                (&b"[sess] hb codec="[..], s.hb_codec),
                (&b" pub="[..], s.hb_pub),
                (&b" prop="[..], s.hb_prop),
                (&b" assigned="[..], s.hb_assigned),
                (&b" reg="[..], s.hb_reg),
                (&b" commit="[..], s.hb_commit),
                (&b" refused="[..], s.proposals_refused),
                (&b" rrefused="[..], s.proposals_refused_raft),
                (&b" superseded="[..], s.acks_superseded),
                (&b" throttled="[..], s.publishes_throttled),
                (&b" cheld="[..], s.codec_held),
                (&b" mbox="[..], s.mbox.used() as u32),
                (&b" mwait="[..], s.mbox.waited),
                (&b" mref="[..], s.mbox.refused),
                (&b" munasked="[..], s.mflow.unasked),
                (&b" rref="[..], s.retained_reads_refused),
                (&b" spill="[..], s.deliveries_spilled),
                (&b" toolarge="[..], s.publishes_too_large),
                (&b" gated="[..], s.codec_gated),
                (&b" ostall="[..], s.apply_output_stalls),
                (&b" conn="[..], s.connects),
                (&b" redir="[..], s.connects_redirected),
                (&b" disc="[..], s.disconnects),
                (&b" evict="[..], s.conn_bindings_evicted),
                (&b" foreign="[..], s.sessions_released_foreign),
                (&b" noprior="[..], s.apply_connect_no_prior),
                (&b" resets="[..], s.apply_resets),
                (&b" cp="[..], s.cp_forwarded),
                (&b" cpref="[..], s.cp_refused),
                (&b" pheld="[..], s.proposals_held),
                (&b" dedupref="[..], s.dedup_refused_delivered),
                (&b" dedupdup="[..], s.dedup_duplicates),
                (&b" hexp="[..], s.worker.exported),
                (&b" himp="[..], s.worker.imported),
                (&b" hres="[..], s.worker.resumed),
                (&b" href="[..], s.worker.refused),
                (&b" hfull="[..], s.worker.busy_overflow),
                (&b" nsdrop="[..], s.codec_namespace_drops),
                (&b" unkeyed="[..], s.proposals_unkeyed_refused),
                (&b" dstale="[..], s.worker.delivery_stale),
                (&b" ddup="[..], s.worker.delivery_dup),
                (&b" dunk="[..], s.worker.delivery_unknown),
                (&b" swfwd="[..], s.worker.swaps_forwarded),
                (&b" dlog="[..], s.deliver_logged),
                (&b" dunlog="[..], s.deliver_unlogged),
                (&b" ledger="[..], s.ledger.live() as u32),
                (&b" redlv="[..], s.redelivered),
                (&b" floorskip="[..], s.replay_floor_skipped),
                (&b" curref="[..], s.replay_cursor_refused),
                (&b" cksent="[..], s.worker.ckpt_sent),
                (&b" ckkept="[..], s.worker.ckpt_kept),
                (&b" adopted="[..], s.worker.adopted),
                (&b" adoptref="[..], s.worker.adopt_refused),
            ] {
                for &b in label {
                    line[pos] = b;
                    pos += 1;
                }
                pos += fmt_u32_raw(line.as_mut_ptr().add(pos), v);
            }
            dev_log(sys, 3, line.as_ptr(), pos);
            // Anchor continuity and site export, once any of it ran:
            // seats refused, productions held on a full ring, numbered
            // without bytes, released; horizon reports ignored; re-anchors
            // answered and refused; PINGRESPs held; site records out and
            // refused; consumption reports to the anchor; client responses
            // held for a full codec bus, and lost to a full hold (must stay 0).
            let seats = &s.worker.seats;
            let cont = [
                (&b"[sess] cont seatref="[..], seats.refused),
                (&b" full="[..], seats.full),
                (&b" unret="[..], seats.unretained),
                (&b" rel="[..], seats.released),
                (&b" hzstale="[..], seats.stale_reports),
                (&b" reanch="[..], seats.reanchors),
                (&b" reref="[..], seats.reanchor_refused),
                (&b" pongheld="[..], s.worker.pingresp_refused),
                (&b" siteout="[..], s.site_exported),
                (&b" siteref="[..], s.site_refused),
                (&b" consumed="[..], s.worker.consumed_reports),
                (&b" rheld="[..], s.hold.held),
                (&b" rlost="[..], s.hold.overflow),
            ];
            if cont.iter().any(|&(_, v)| v != 0) {
                let mut pos = 0usize;
                for (label, v) in cont {
                    for &b in label {
                        line[pos] = b;
                        pos += 1;
                    }
                    pos += fmt_u32_raw(line.as_mut_ptr().add(pos), v);
                }
                dev_log(sys, 3, line.as_ptr(), pos);
            }
            // Kafka and AMQP, once any ran on this worker, named by worker
            // id: produces taken and answered, fetches, group calls and
            // offset commits, AMQP publishes taken and confirmed, Basic.Get
            // calls, consumer deliveries, coordinator calls taken, and
            // requests answered with the close marker. Cumulative.
            let wire_counts = [
                (&b"[sess] kq w="[..], u32::from(s.worker_id)),
                (&b" kprod="[..], s.kafka_produce_rx),
                (&b" kacked="[..], s.kafka_produce_acked),
                (&b" kfetch="[..], s.kafka_fetch_rx),
                (&b" kgroup="[..], s.kafka_group_ops),
                (&b" koffc="[..], s.kafka_offset_commits),
                (&b" apub="[..], s.amqp_publish_rx),
                (&b" aacked="[..], s.amqp_publish_acked),
                (&b" aget="[..], s.amqp_get_rx),
                (&b" adlv="[..], s.amqp_delivers),
                (&b" coord="[..], s.coord_calls),
            ];
            if wire_counts.iter().skip(1).any(|&(_, v)| v != 0) {
                let mut pos = 0usize;
                for (label, v) in wire_counts {
                    for &b in label {
                        line[pos] = b;
                        pos += 1;
                    }
                    pos += fmt_u32_raw(line.as_mut_ptr().add(pos), v);
                }
                dev_log(sys, 3, line.as_ptr(), pos);
            }
            s.hb_codec = 0;
            s.hb_pub = 0;
            s.hb_prop = 0;
            s.hb_assigned = 0;
            s.hb_reg = 0;
            s.hb_commit = 0;
        }

        if now.wrapping_sub(s.last_metrics_ms) >= 1000 {
            // Publish inflights that never resolved (lost proposal,
            // leadership churn, codec_out saturation at ack time): answer
            // and reclaim the slot. A Kafka request answers
            // REQUEST_TIMED_OUT (the codec writes a connection's answers
            // in request order, so one left unanswered would hold every
            // later one); AMQP confirm-mode publishers wait indefinitely,
            // so they get an explicit Basic.Nack.
            for i in 0..KAFKA_INFLIGHT {
                if !store::inflight_expired(&s.store, i, now, KAFKA_INFLIGHT_TIMEOUT_MS) {
                    continue;
                }
                // An owed answer is decided: it is answered, not expired.
                if store::inflight_owed(&s.store, i) != store::ANSWER_NONE {
                    continue;
                }
                if let Some(e) = store::inflight_get(&s.store, i) {
                    // Only the AMQP and Kafka arms below read the entry.
                    let _ = &e;
                    #[cfg(feature = "amqp")]
                    if e.proto == KIN_PROTO_AMQP {
                        // The Nack is the publisher's only answer: freed
                        // with the slot when written, else owed in it.
                        s.kafka_produce_errors = s.kafka_produce_errors.wrapping_add(1);
                        answer_inflight(s, sys, i, store::ANSWER_NACK);
                        continue;
                    }
                    // A Kafka request is answered too: the codec writes a
                    // connection's answers in request order, so one never
                    // answered would hold every later answer on it. A
                    // produce answers REQUEST_TIMED_OUT for each partition
                    // whose durability never came (Kafka's own "may or
                    // may not have been written": the producer retries,
                    // an idempotent one exactly), an OffsetCommit fails
                    // its parked answer the same way. Freed with the slot
                    // when written, else owed in it.
                    #[cfg(feature = "kafka")]
                    if expire_kafka_inflight(s, sys, i, e) {
                        continue;
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
            // Surface correlations whose MSG_PROPOSAL_ASSIGNED is overdue.
            // The substrate answers every tagged proposal once (an
            // assignment, or a refusal that frees it at once), so an
            // overdue correlation is a contract violation to alert on, not
            // a slot to reclaim: freeing it would strand the assignment
            // still in flight and leave the publish unacknowledged.
            s.correlations_overdue =
                correlate::count_overdue(&s.correlate, now, CORRELATION_TIMEOUT_MS);
            for i in 0..MAX_SESSIONS {
                if !sessions::is_active(&s.sessions, i) {
                    continue;
                }
                // A session mid-handoff is exempt: the silence is the
                // anchor's hold, not the client's.
                if worker::is_held(&s.worker, sessions::conn_id(&s.sessions, i)) {
                    continue;
                }
                let kalive = sessions::view(&s.sessions, i)
                    .map(|v| v.keep_alive_ms)
                    .unwrap_or(0);
                if kalive == 0 {
                    continue;
                }
                let deadline = (kalive as u64).saturating_mul(3) / 2;
                let age = now.wrapping_sub(
                    sessions::view(&s.sessions, i)
                        .map(|v| v.last_activity_ms)
                        .unwrap_or(0),
                );
                if age <= deadline {
                    continue;
                }

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
                        wire::QOP_DISCONNECT,
                        tenant,
                        session_slot,
                    );
                    let off = wire::QPROP_UNTAGGED_HDR_LEN;
                    s.out_buf[off] = wire::QDISC_REASON_KEEPALIVE;
                    s.out_buf[off + 1..off + 9].copy_from_slice(&stream_hash.to_le_bytes());
                    let shard = sessions::shard(&s.sessions, i);
                    if try_emit_keyed(
                        sys,
                        s.out_proposals,
                        &s.view,
                        shard,
                        &s.out_buf[..prop_total],
                    ) {
                        s.proposals_emitted = s.proposals_emitted.wrapping_add(1);
                        s.hb_prop = s.hb_prop.wrapping_add(1);
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
                if !sessions::is_persisted(&s.sessions, i) {
                    continue;
                }
                if sessions::is_active(&s.sessions, i) {
                    continue;
                }
                let expiry_s = sessions::session_expiry_s(&s.sessions, i);
                if expiry_s == 0 || expiry_s == u32::MAX {
                    continue;
                }
                let deadline_ms = (expiry_s as u64).saturating_mul(1000);
                let elapsed = now.wrapping_sub(
                    sessions::view(&s.sessions, i)
                        .map(|v| v.disconnected_at_ms)
                        .unwrap_or(0),
                );
                if elapsed < deadline_ms {
                    continue;
                }

                // Purge.
                sessions::clear_flow(&mut s.sessions, i);
                let drop_body = (i as u32).to_le_bytes();
                try_emit(sys, s.out_topic, wire::MSG_SESSION_DROP, &drop_body);
                sessions::clear(&mut s.sessions, i);
                s.sub_book.clear(i as u32);
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
                if fire_at == 0 {
                    continue;
                }
                if now < fire_at {
                    continue;
                }
                if sessions::will_present(&s.sessions, i) {
                    let t = sessions::tenant(&s.sessions, i);
                    fire_will(s, sys, t, i);
                }
                sessions::set_will_deadline(&mut s.sessions, i, 0);
                sessions::clear_will(&mut s.sessions, i);
            }
        }

        // ── Phase 5f: handoff drains — declare DRAINED and export the
        // attachments that reached quiescence this step.
        worker::step(s, sys, now);

        // ── Phase 6: metrics (wire envelope) ──
        if now.wrapping_sub(s.last_metrics_ms) >= 1000 && s.out_metrics >= 0 {
            s.last_metrics_ms = now;
            let mut m = [0u8; 72];
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
            // Saturation and fencing signals. Each is an invariant an
            // operator needs to see rising before it becomes an outage:
            // apply backing up behind a full output edge, completions
            // arriving for a slot that has moved on, and assignments the
            // substrate owes but has not delivered.
            m[40..44].copy_from_slice(&s.apply_output_stalls.to_le_bytes());
            m[44..48].copy_from_slice(&s.acks_stale_epoch.to_le_bytes());
            m[48..52].copy_from_slice(&s.correlations_overdue.to_le_bytes());
            // Tagged proposals Clustor refused, their operations failed
            // at once rather than left to a timeout.
            m[60..64].copy_from_slice(&s.proposals_refused_raft.to_le_bytes());
            // Completions answered "not the committed entry" (its operation
            // failed for a retry), and applied-run notices lost to a full
            // spill (must stay 0: their indices resolve as unknown).
            m[64..68].copy_from_slice(&s.acks_superseded.to_le_bytes());
            m[68..72].copy_from_slice(&s.applied_notice_drops.to_le_bytes());
            // Idempotence outcomes. `duplicates` rising is the feature
            // working — producer retries that would otherwise have been
            // appended twice. `rejected` rising means a producer is
            // losing batches to a sequence gap or a fenced epoch, which
            // is never routine.
            #[cfg(feature = "kafka")]
            {
                m[52..56].copy_from_slice(&s.kafka_idem_duplicates.to_le_bytes());
                m[56..60].copy_from_slice(&s.kafka_idem_rejected.to_le_bytes());
            }
            try_emit(sys, s.out_metrics, wire::MSG_METRICS, &m);
        }

        if worked > 0 {
            STEP_BURST
        } else {
            0
        }
    }
}
