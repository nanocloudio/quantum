//! Wire format helpers for Quantum module channels.
//!
//! Reuses Clustor's 3-byte envelope format:
//!   [msg_type: u8] [len: u16 LE] [payload: len bytes]
//!
//! Quantum-specific message types occupy the 0x90-0xEF range, leaving
//! 0x00-0x8F for Clustor substrate types (shared with clustor/modules/common/wire.rs).

#![allow(
    dead_code,
    reason = "shared wire helpers; individual modules use different subsets of constants and codecs"
)]

// ── Clustor substrate types (duplicated for consistency) ────────────────────

pub const MSG_CLIENT_PROPOSAL: u8 = 0x10;
pub const MSG_CLIENT_RESPONSE: u8 = 0x11;
pub const MSG_ADMIN_COMMAND: u8 = 0x12;
pub const MSG_ADMIN_RESPONSE: u8 = 0x13;
pub const MSG_PROPOSAL_ASSIGNED: u8 = 0x14;

pub const MSG_WAL_ENTRY: u8 = 0x20;
pub const MSG_DURABILITY_PROOF: u8 = 0x22;
pub const MSG_COMMITTED_BATCH: u8 = 0x23;
/// Per-entry body stream from `consensus.committed_entries`.
/// Body: `[term:u64 LE][index:u64 LE][entry_body...]`. Quantum's
/// session_processor consumes this on `entries_in` to drive the
/// apply-side state machine (P0 of `docs/partitioning.md`).
pub const MSG_COMMITTED_ENTRY: u8 = 0x24;

/// Apply-pipeline reset notice (mirrors Clustor's value). Emitted by
/// the substrate when the apply index has rewound — snapshot install,
/// leader-driven log truncation, or any other event that invalidates
/// already-applied state. Body: `[term:u64 LE][index:u64 LE]`.
///
/// Every Quantum module that derives state from `MSG_COMMITTED_ENTRY`
/// must honour this signal by discarding state above the reset index;
/// see clustor's `docs/architecture/substrate_capability_surface.md`
/// for the contract and `docs/architecture/apply_path.md`
/// §Apply-pipeline reset for per-module policy.
pub const MSG_APPLY_PIPELINE_RESET: u8 = 0x2B;

pub const MSG_CP_PROOF: u8 = 0x30;
pub const MSG_CACHE_STATE: u8 = 0x31;

pub const MSG_THROTTLE_CREDITS: u8 = 0x40;
pub const MSG_THROTTLE_ENVELOPE: u8 = 0x41;
pub const MSG_LAG_SIGNAL: u8 = 0x42;

pub const MSG_METRICS: u8 = 0x70;
pub const MSG_PLACEMENT_UPDATE: u8 = 0x80;

// ── Quantum-specific message types ──────────────────────────────────────────

// Protocol transport
pub const MSG_PROTOCOL_MQTT: u8 = 0x90;
pub const MSG_PROTOCOL_KAFKA: u8 = 0x91;
pub const MSG_PROTOCOL_AMQP: u8 = 0x92;

// Codec-to-session
pub const MSG_SESSION_PROPOSAL: u8 = 0x98;
pub const MSG_SESSION_RESPONSE: u8 = 0x99;
pub const MSG_SESSION_CONNECT: u8 = 0x9A;
pub const MSG_SESSION_DISCONNECT: u8 = 0x9B;

// Dedupe
//
// MSG_DEDUP_CHECK   `[dedup_key(20)]` or `[dedup_key(20)][phase:u8]`
// MSG_DEDUP_RESULT  `[dedup_key(20)][duplicate:u8][phase:u8]`
//
// The phase byte carries the QoS 2 protocol state the durable entry
// holds (`DEDUP_PHASE_NONE` for flows that have none, otherwise a
// `QOS2_*` value). It is a trailing field: a check written without it
// is read as `DEDUP_PHASE_NONE` and leaves the stored phase untouched,
// so entries recorded by either shape stay readable.
// MSG_DEDUP_PHASE   `[dedup_key(20)][phase:u8]`
//
// Record-only: it advances the phase of an entry the apply path has
// already checked in, and emits no result. Recording is separate from
// MSG_DEDUP_CHECK because a check is also the duplicate verdict that
// gates fan-out; a phase transition must never be able to answer one.
pub const MSG_DEDUP_CHECK: u8 = 0xA0;
pub const MSG_DEDUP_RESULT: u8 = 0xA1;
pub const MSG_DEDUP_PHASE: u8 = 0xA2;

/// Length of the bare dedupe key, and the offset of the trailing phase
/// byte in a check payload that carries one.
pub const DEDUP_KEY_LEN: usize = 20;

/// "This flow has no QoS 2 phase" — the value a check without a phase
/// byte decodes to, and the value a result reports for an entry whose
/// phase was never recorded. Distinct from every `QOS2_*` state.
pub const DEDUP_PHASE_NONE: u8 = 0xFF;

// Topic routing
pub const MSG_TOPIC_PUBLISH: u8 = 0xA8;
pub const MSG_TOPIC_SUBSCRIBE: u8 = 0xA9;
pub const MSG_TOPIC_UNSUBSCRIBE: u8 = 0xAC;
pub const MSG_TOPIC_DELIVER: u8 = 0xAA;
pub const MSG_TOPIC_FORWARD: u8 = 0xAB;
/// Session teardown notice. Body: `[session_slot:u32 LE]`. Sent by
/// session_processor on DISCONNECT so topic_engine can purge any
/// subscriptions still keyed to the now-defunct slot; otherwise a future
/// client reusing the slot would inherit them.
pub const MSG_SESSION_DROP: u8 = 0xAD;

// Messaging infrastructure
pub const MSG_OFFLINE_ENQUEUE: u8 = 0xB0;
pub const MSG_OFFLINE_DRAIN: u8 = 0xB1;
pub const MSG_RETAINED_WRITE: u8 = 0xB2;
pub const MSG_RETAINED_READ: u8 = 0xB3;
/// Reconnect notice from session_processor → the messaging module's
/// offline component.
/// Body: `[session_slot:u32 LE]`. Triggers a drain of every queued
/// envelope for that slot back to the messaging bus so the now-reconnected
/// subscriber receives the messages it would have seen had it stayed
/// online. Routed on the same messaging bus as the other infrastructure
/// ops; the offline component filters by msg_type.
pub const MSG_OFFLINE_RECONNECT: u8 = 0xB4;

/// Apply-pipeline reset fan-out from `session_processor` to every
/// apply-derived module. Body: `[reset_index:u64 LE]`. Fires when the
/// substrate signals a snapshot install or leader-driven log
/// truncation (detected by `session_processor` as a forward jump in
/// the `MSG_COMMITTED_ENTRY` index sequence) — see
/// `docs/architecture/apply_path.md` §Apply-pipeline reset. Downstream
/// handlers (topic_engine, and messaging's dedup / retained / offline
/// components) clear all apply-derived state; snapshot install
/// repopulates it.
pub const MSG_APPLY_RESET_FANOUT: u8 = 0xB5;

// Consumer groups & transactions
pub const MSG_GROUP_OP: u8 = 0xB8;
pub const MSG_GROUP_ASSIGN: u8 = 0xB9;
pub const MSG_TXN_OP: u8 = 0xBA;
pub const MSG_TXN_RESULT: u8 = 0xBB;

// Flow control (Quantum-specific)
pub const MSG_ACK_REGISTER: u8 = 0xC0;
pub const MSG_ACK_EMIT: u8 = 0xC1;
pub const MSG_ACK_REDELIVER: u8 = 0xC2;
pub const MSG_BP_SIGNAL: u8 = 0xC3;
pub const MSG_PREFETCH_CREDIT: u8 = 0xC4;
pub const MSG_DELIVERY_LAG: u8 = 0xC5;

/// MSG_ACK_REGISTER body:
/// `[session_slot:u32 LE][message_id:u32 LE][partition_id:u16 LE]
///  [wal_index:u64 LE][session_epoch:u32 LE]`.
///
/// The epoch names the session generation the publish was accepted
/// under. It travels back on MSG_ACK_EMIT so a completion arriving
/// after the slot has been reused for a different client is discarded
/// rather than matched on `(session_slot, message_id)` alone — MQTT
/// packet ids are client-chosen and collide freely across sessions.
pub const ACK_REGISTER_LEN: usize = 22;

/// MSG_ACK_EMIT body:
/// `[session_slot:u32 LE][message_id:u32 LE][session_epoch:u32 LE]`.
pub const ACK_EMIT_LEN: usize = 12;

// Control plane (Quantum-specific)
pub const MSG_TENANT_RECORD: u8 = 0xD0;
pub const MSG_TENANT_QUOTA: u8 = 0xD1;
pub const MSG_TENANT_DISCONNECT: u8 = 0xD2;
pub const MSG_CAPABILITIES: u8 = 0xD3;
pub const MSG_EPOCH_EVENT: u8 = 0xD4;

// Forward coordination
pub const MSG_FORWARD_REQUEST: u8 = 0xD8;
pub const MSG_FORWARD_ACK: u8 = 0xD9;

// Operations
pub const MSG_AUDIT_EVENT: u8 = 0xE0;
pub const MSG_DR_SNAPSHOT_REQ: u8 = 0xE1;
pub const MSG_DR_SNAPSHOT_RESP: u8 = 0xE2;
pub const MSG_DR_PROMOTE: u8 = 0xE3;
pub const MSG_METRICS_ROLLUP: u8 = 0xE8;

/// HTTP request parsed off the shared client port for the admin
/// surface (clustor `operations`' `request` port; values shared with
/// clustor's wire vocabulary). Payload:
///   `[conn_id:u8][method:u8 (G=0x47, P=0x50, ...)][path_len:u8][path bytes][body...]`
/// The method byte is the verb's first character so the consumer
/// dispatches without re-parsing.
pub const MSG_HTTP_REQUEST: u8 = 0x74;

/// HTTP response from the admin surface back to the module that owns
/// the client port, which frames the wire-level HTTP/1.1 response
/// itself. Payload:
///   `[conn_id:u8][status:u16 LE][body_len:u16 LE][body bytes]`
pub const MSG_HTTP_RESPONSE: u8 = 0x75;

/// Client-facing frame on the `codec → protocol → peer_router.client_resp`
/// chain. Payload is `[conn_id:u8][protocol bytes]`. Carries an
/// envelope so back-to-back writes don't coalesce on the merge
/// module's byte FIFO — same rationale as the `codec_in` fan-in fix.
/// `peer_router` ignores the msg_type and just unwraps the conn_id
/// prefix; `protocol` forwards the envelope verbatim.
pub const MSG_CLIENT_FRAME: u8 = 0xEA;

/// Transport-level connection-closed notice on the same
/// `peer_router.cleartext → protocol::router → codec` chain as
/// MSG_CLIENT_FRAME. Payload is `[conn_id:u8]`. peer_router emits it when
/// a CLIENT socket closes (NMSG_CLOSED, replica_id < 0); `protocol::router`
/// clears its sniff state and forwards it to the conn's codec, which
/// clears per-conn reassembly and forwards MSG_SESSION_DISCONNECT to
/// session_processor. This is the generic "connection closed" verb that
/// lets every protocol release conn-keyed state (AMQP consumers, Kafka
/// group members) deterministically instead of leaking until a timeout —
/// and closes the conn_id-reuse cross-delivery hazard.
pub const MSG_CONN_CLOSED: u8 = 0xEB;

// ── Envelope primitives ─────────────────────────────────────────────────────

pub const ENVELOPE_HDR: usize = 3;
pub const MAX_PAYLOAD: usize = 0xFFFF;

#[inline]
pub fn encode_header(buf: &mut [u8], msg_type: u8, payload_len: u16) -> i32 {
    if buf.len() < ENVELOPE_HDR {
        return -1;
    }
    buf[0] = msg_type;
    let lb = payload_len.to_le_bytes();
    buf[1] = lb[0];
    buf[2] = lb[1];
    ENVELOPE_HDR as i32
}

#[inline]
pub fn decode_header(buf: &[u8]) -> (u8, u16) {
    let msg_type = buf[0];
    let payload_len = u16::from_le_bytes([buf[1], buf[2]]);
    (msg_type, payload_len)
}

/// # Safety
/// `sys` must point to a valid SyscallTable. `chan` must be a valid handle.
///
/// Composes the envelope header and payload into a single stack buffer
/// and emits one `channel_write`, so the channel lock spans the whole
/// message. Two separate writes would release the lock between them and
/// allow a reader (e.g. a fan-in merge bridge on multi-producer ports)
/// to observe a partial message and interleave bytes from another
/// producer. The buffer is sized to `CHANNEL_BUFFER_SIZE` from
/// `fluxor-abi`'s `kernel_abi.rs` so the cap tracks the underlying
/// channel slot size; messages above the cap cannot fit in the channel
/// anyway and are rejected here rather than silently truncated.
#[inline]
pub unsafe fn channel_write_msg(
    sys: &crate::abi::SyscallTable,
    chan: i32,
    msg_type: u8,
    payload: &[u8],
) -> i32 {
    const MAX_MSG: usize = crate::abi::CHANNEL_BUFFER_SIZE;
    let total = ENVELOPE_HDR + payload.len();
    if total > MAX_MSG {
        return -1;
    }
    let mut buf = [0u8; MAX_MSG];
    encode_header(&mut buf[..ENVELOPE_HDR], msg_type, payload.len() as u16);
    if !payload.is_empty() {
        buf[ENVELOPE_HDR..total].copy_from_slice(payload);
    }
    let w = (sys.channel_write)(chan, buf.as_ptr(), total);
    if w < total as i32 {
        return -1;
    }
    total as i32
}

/// # Safety
/// `sys` must point to a valid SyscallTable.
#[inline]
pub unsafe fn channel_read_msg(
    sys: &crate::abi::SyscallTable,
    chan: i32,
    buf: &mut [u8],
) -> (u8, u16) {
    let mut hdr = [0u8; ENVELOPE_HDR];
    let n = (sys.channel_read)(chan, hdr.as_mut_ptr(), ENVELOPE_HDR);
    if n < ENVELOPE_HDR as i32 {
        return (0, 0);
    }
    let (msg_type, payload_len) = decode_header(&hdr);
    let plen = payload_len as usize;
    if plen == 0 {
        return (msg_type, 0);
    }
    if plen > buf.len() {
        let mut discard = [0u8; 256];
        let mut remaining = plen;
        while remaining > 0 {
            let chunk = remaining.min(256);
            let r = (sys.channel_read)(chan, discard.as_mut_ptr(), chunk);
            if r <= 0 {
                break;
            }
            remaining -= r as usize;
        }
        return (0, 0);
    }
    let n2 = (sys.channel_read)(chan, buf.as_mut_ptr(), plen);
    if (n2 as usize) < plen {
        return (0, 0);
    }
    (msg_type, payload_len)
}

// ── Common Quantum payload helpers ──────────────────────────────────────────

/// Tagged proposal envelope header size — 8 bytes for the correlation_id.
pub const TAGGED_PROPOSAL_HDR: usize = 8;

/// Build a tagged proposal payload `[correlation_id:u64 LE][body]` into `dst`.
/// Returns total bytes written or -1 if `dst` is too small.
pub fn encode_tagged_proposal(dst: &mut [u8], correlation_id: u64, body: &[u8]) -> i32 {
    let total = TAGGED_PROPOSAL_HDR + body.len();
    if dst.len() < total {
        return -1;
    }
    dst[0..8].copy_from_slice(&correlation_id.to_le_bytes());
    dst[8..total].copy_from_slice(body);
    total as i32
}

/// MSG_PROPOSAL_ASSIGNED payload size (18 bytes):
///   correlation_id(8 LE) + partition_id(2 LE) + wal_index(8 LE)
pub const PROPOSAL_ASSIGNED_LEN: usize = 18;

/// Decode a MSG_PROPOSAL_ASSIGNED payload (18 bytes).
/// Returns `(correlation_id, partition_id, wal_index)`. The
/// `partition_id` is needed by `flow::ack` to disambiguate the
/// same `wal_index` arriving from different partitions when QoS 1
/// PUBLISHes are routed through `partition_router`.
pub fn decode_proposal_assigned(buf: &[u8]) -> (u64, u16, u64) {
    let cid = u64::from_le_bytes([
        buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
    ]);
    let pid = u16::from_le_bytes([buf[8], buf[9]]);
    let wi = u64::from_le_bytes([
        buf[10], buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17],
    ]);
    (cid, pid, wi)
}

/// MSG_DURABILITY_PROOF payload size (19 bytes):
///   partition_id(2 LE) + term(8 LE) + index(8 LE) + replica_id(1)
pub const DURABILITY_PROOF_LEN: usize = 19;

/// Decode a MSG_DURABILITY_PROOF payload (19 bytes).
/// Returns `(partition_id, term, index, replica)`.
#[inline]
pub fn decode_durability_proof(buf: &[u8]) -> (u16, u64, u64, u8) {
    let partition_id = u16::from_le_bytes([buf[0], buf[1]]);
    let term = u64::from_le_bytes([
        buf[2], buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9],
    ]);
    let index = u64::from_le_bytes([
        buf[10], buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17],
    ]);
    let replica = buf[18];
    (partition_id, term, index, replica)
}

/// DedupKey: (tenant_id: u32, stream_hash: u64, session_epoch: u32, message_id: u32) = 20 bytes.
#[inline]
pub fn encode_dedup_key(buf: &mut [u8], tenant: u32, stream_hash: u64, epoch: u32, msg_id: u32) {
    buf[0..4].copy_from_slice(&tenant.to_le_bytes());
    buf[4..12].copy_from_slice(&stream_hash.to_le_bytes());
    buf[12..16].copy_from_slice(&epoch.to_le_bytes());
    buf[16..20].copy_from_slice(&msg_id.to_le_bytes());
}

#[inline]
pub fn decode_dedup_key(buf: &[u8]) -> (u32, u64, u32, u32) {
    let tenant = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
    let stream_hash = u64::from_le_bytes([
        buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10], buf[11],
    ]);
    let epoch = u32::from_le_bytes([buf[12], buf[13], buf[14], buf[15]]);
    let msg_id = u32::from_le_bytes([buf[16], buf[17], buf[18], buf[19]]);
    (tenant, stream_hash, epoch, msg_id)
}

/// Read the trailing QoS 2 phase byte of a MSG_DEDUP_CHECK payload.
/// Payloads written without the field report `DEDUP_PHASE_NONE`.
#[inline]
pub fn decode_dedup_phase(buf: &[u8]) -> u8 {
    if buf.len() > DEDUP_KEY_LEN {
        buf[DEDUP_KEY_LEN]
    } else {
        DEDUP_PHASE_NONE
    }
}

/// TopicKey: (tenant_id: u32, topic_hash: u64) = 12 bytes.
#[inline]
pub fn encode_topic_key(buf: &mut [u8], tenant: u32, topic_hash: u64) {
    buf[0..4].copy_from_slice(&tenant.to_le_bytes());
    buf[4..12].copy_from_slice(&topic_hash.to_le_bytes());
}

#[inline]
pub fn decode_topic_key(buf: &[u8]) -> (u32, u64) {
    let tenant = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
    let topic_hash = u64::from_le_bytes([
        buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10], buf[11],
    ]);
    (tenant, topic_hash)
}

/// FNV-1a 64-bit hash for topic matching and stream IDs.
#[inline]
pub fn fnv1a_64(data: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for &b in data {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

/// MQTT topic match: supports `+` (single-level) and `#` (multi-level) wildcards.
/// Returns true if `pattern` matches `topic`.
pub fn mqtt_topic_match(pattern: &[u8], topic: &[u8]) -> bool {
    let mut pi = 0usize;
    let mut ti = 0usize;
    while pi < pattern.len() {
        let pc = pattern[pi];
        if pc == b'#' {
            // Matches rest of topic (must be at end of pattern, or after `/`)
            return pi == pattern.len() - 1 && (pi == 0 || pattern[pi - 1] == b'/');
        } else if pc == b'+' {
            // Matches single topic level
            while ti < topic.len() && topic[ti] != b'/' {
                ti += 1;
            }
            pi += 1;
            if pi < pattern.len() && pi < pattern.len() && pattern[pi] == b'/' {
                if ti >= topic.len() || topic[ti] != b'/' {
                    return false;
                }
                ti += 1;
                pi += 1;
            }
        } else {
            if ti >= topic.len() || topic[ti] != pc {
                return false;
            }
            ti += 1;
            pi += 1;
        }
    }
    ti == topic.len()
}

// ── Canonical proposal envelope (Quantum apply-side state machine) ──────────
//
// Every Quantum proposal that mutates durable state goes on the wire
// as a self-describing envelope so that follower replicas and
// post-restart WAL replay can drive the apply-side state machine
// without consulting propose-side context.
//
//   Untagged (fire-and-forget ops) sent on `proposals`:
//     wire body = [canonical envelope]
//
//   Tagged (publisher-ack-gated ops) sent on `proposals_tagged`:
//     wire body = [correlation_id:u64 LE][canonical envelope]
//     Clustor's `decode_tagged_proposal` strips the 8-byte
//     `correlation_id` prefix before storing the entry, so the WAL
//     body — and the `MSG_COMMITTED_ENTRY` body forwarded by
//     the apply path — is byte-identical to the untagged form.
//
//   canonical envelope = [version:u8][op:u8][tenant:u32 LE]
//                        [session_slot:u32 LE][op-body]
//
// Apply-side reads the canonical envelope at the start of every
// `MSG_COMMITTED_ENTRY` body (after stripping the 16-byte
// `[term][index]` prefix). The `correlation_id` for tagged ops is
// not carried through the apply stream; the leader recovers it from
// the matching publisher inflight (`Session.inflight[i].correlation_id`).
//
// See `docs/architecture/apply_path.md` §Canonical proposal format for
// the design rationale and the per-op body shapes; see clustor's
// `docs/architecture/substrate_capability_surface.md` for the substrate
// contract that delivers these bodies on `committed_entries`.

/// Canonical envelope body version. Increment for breaking shape
/// changes; never decrement — replay must understand every shipped
/// version forever.
pub const QPROP_VERSION_V1: u8 = 1;

/// V2 bumps the QOP_PUBLISH body shape to interleave a `user_props`
/// block between `topic` and `payload` (MQTT 5 §3.3.2.3.7 User
/// Property propagation). All other op-bodies are bit-identical
/// between V1 and V2; the version is per-proposal so callers can
/// keep emitting V1 for ops that haven't changed shape. Replay
/// support: apply-side parsers MUST honour both V1 and V2 forever —
/// any new V3 must keep this rule. See
/// `docs/architecture/apply_path.md` §Canonical proposal format.
pub const QPROP_VERSION_V2: u8 = 2;

/// Size of the canonical envelope header
/// (`version + op + tenant + session_slot`) = 10 bytes.
pub const QPROP_HEADER_LEN: usize = 1 + 1 + 4 + 4;

/// Header size of an untagged proposal body. Identical to
/// `QPROP_HEADER_LEN` — kept as a named alias so propose-side call
/// sites read symmetrically with the tagged variant.
pub const QPROP_UNTAGGED_HDR_LEN: usize = QPROP_HEADER_LEN;

/// Header size of a tagged proposal body — 8 bytes for the
/// `correlation_id` prefix that Clustor's tagged-proposal handler
/// strips before storing the entry, plus the canonical envelope
/// header. Used by the propose-side encoder; apply-side reads
/// `QPROP_HEADER_LEN` only.
pub const QPROP_TAGGED_HDR_LEN: usize = TAGGED_PROPOSAL_HDR + QPROP_HEADER_LEN;

// ── Canonical opcodes ───────────────────────────────────────────────────────
//
// Reserved range: `0x01 — 0x0F`. Additions are non-breaking; reuse of
// retired opcodes requires a `QPROP_VERSION` bump.

/// CONNECT: allocate / resurrect / refresh a session slot.
///
/// `stream_hash` is computed propose-side (from `cid` for normal
/// clients, from `conn_id` for anonymous-cid MQTT clients) and shipped
/// verbatim so followers and post-restart replay arrive at the same
/// session identity without needing the leader's transient routing
/// state.
///
/// Op-body: `[clean_start:u8][keep_alive_s:u16 BE][stream_hash:u64 LE]
///           [cid_len:u16 BE][cid bytes][protocol:u8][will_flag:u8]`.
///
/// When `will_flag == 1`, the body continues with:
///   `[will_qos:u8][will_retain:u8][will_delay_s:u32 LE]
///    [will_topic_len:u16 BE][will_topic][will_payload_len:u16 BE][will_payload]`
///
/// After the Will section (or after the bare `will_flag == 0` byte
/// when no Will is present), the trailer continues with the MQTT 5
/// SessionExpiryInterval and ReceiveMaximum:
///   `[session_expiry_s:u32 LE][receive_maximum:u16 LE]`
/// MQTT 3.1.1 connections are normalised propose-side to
/// `session_expiry_s = u32::MAX` for clean_session=0 (treated as
/// "never expire" — the sweep skips them) and `0` for clean_session=1
/// (irrelevant; clean drops the slot at disconnect anyway).
/// `receive_maximum == 0` means "no cap" (also the MQTT 3.1.1
/// default); >0 caps concurrent unacked QoS 1+ deliveries to the
/// subscriber per MQTT 5 §3.3.4.
///
/// Additive: a parser written against the pre-Will / pre-expiry shape
/// stops at the last field it understands and treats the missing
/// fields as defaults (no Will / `session_expiry == 0`). No
/// `QPROP_VERSION` bump per `docs/architecture/apply_path.md` rules
/// (additive only).
pub const QOP_CONNECT: u8 = 0x01;

/// DISCONNECT: clean-disconnect or keep-alive-driven teardown.
///
/// `stream_hash` identifies the durable session record. The
/// `session_slot` field in the canonical envelope header is the
/// leader's local slot index and not portable across nodes; apply-side
/// handlers look up by `(tenant, stream_hash)` instead.
///
/// Op-body: `[reason:u8][stream_hash:u64 LE]` (see `QDISC_REASON_*`).
pub const QOP_DISCONNECT: u8 = 0x02;

/// PUBLISH: protocol-neutral publish. The `stream_hash + session_epoch`
/// fields key the apply-side dedup decision without needing the
/// propose-side session table to exist on the follower.
///
/// V1 op-body: `[pub_qos:u8][packet_id:u16 BE][stream_hash:u64 LE]
///              [session_epoch:u32 LE][retain:u8][topic_len:u16 BE]
///              [topic][payload]`.
///
/// V2 op-body adds an MQTT 5 User Property block between `topic` and
/// `payload` (the body is parsed left-to-right with `payload`
/// extending to end, so the block must sit before payload):
///
///   `[pub_qos:u8][packet_id:u16 BE][stream_hash:u64 LE]
///    [session_epoch:u32 LE][retain:u8][topic_len:u16 BE][topic]
///    [user_props_count:u8]
///    [for each prop: key_len:u16 BE, key, val_len:u16 BE, val]
///    [payload]`
///
/// `user_props_count == 0` represents "no user properties" — the
/// block degenerates to a single zero byte, so V2 is non-zero-cost
/// even for publishers that don't send any. Bounded:
/// `MAX_USER_PROPS_COUNT`, `MAX_USER_PROP_KEY_LEN`,
/// `MAX_USER_PROP_VAL_LEN` (defined in `session_processor`) reject
/// oversized inputs at propose time so apply / topic_engine never
/// see runaway sizes.
pub const QOP_PUBLISH: u8 = 0x03;

/// SUBSCRIBE: add a subscription for the session identified by
/// `stream_hash` to `topic`. Apply-side handlers look up the local
/// session_slot by `(tenant, stream_hash)`.
///
/// Op-body: `[req_qos:u8][stream_hash:u64 LE][topic_len:u16 BE][topic]`.
pub const QOP_SUBSCRIBE: u8 = 0x04;

/// UNSUBSCRIBE: remove `topic` from the subscriptions of the session
/// identified by `stream_hash`.
///
/// Op-body: `[stream_hash:u64 LE][topic_len:u16 BE][topic]`.
pub const QOP_UNSUBSCRIBE: u8 = 0x05;

/// PUBREL: drive the QoS 2 phase transition for the publisher
/// inflight identified by `(stream_hash, packet_id)`. Tagged so the
/// publisher's PUBCOMP is gated on PUBREL durability.
///
/// Op-body: `[packet_id:u16 BE][stream_hash:u64 LE]`.
pub const QOP_PUBREL: u8 = 0x06;

/// RETAINED_CLEAR: explicit clear of `topic`'s retained payload.
/// Distinct from `QOP_PUBLISH` with empty payload so replay never has
/// to disambiguate "clear" from "empty publish".
///
/// Op-body: `[topic_len:u16 BE][topic]`.
pub const QOP_RETAINED_CLEAR: u8 = 0x07;

/// KAFKA_PRODUCE: one Kafka record batch appended to a topic-partition.
/// The `base_offset` returned to the producer is the WAL index assigned
/// to this entry (offsets are WAL indexes interpreted through the
/// topic's view of the WAL — see docs/architecture/kafka_adapter.md
/// §Retention). Tagged when `acks != 0` so the ProduceResponse is
/// gated on quorum durability; untagged (fire-and-forget) for
/// `acks == 0`.
///
/// Op-body: `[partition:u16 LE][topic_len:u16 LE][topic]
///           [records: remaining bytes — the verbatim Kafka record
///            batch (magic v2) from the ProduceRequest]`.
pub const QOP_KAFKA_PRODUCE: u8 = 0x08;

/// AMQP_PUBLISH: one AMQP 0-9-1 Basic.Publish body appended to the
/// routing key's message log (shared apply-side store with Kafka —
/// entries are flagged raw, so Kafka Fetch skips them and AMQP
/// Basic.Get returns them verbatim). Tagged when the channel is in
/// confirm mode so the Basic.Ack is gated on quorum durability
/// (publisher-confirms == ACK-DURABILITY); untagged otherwise.
///
/// Op-body: `[rk_len:u16 LE][routing_key][payload: remaining bytes]`.
pub const QOP_AMQP_PUBLISH: u8 = 0x09;

/// KAFKA_OFFSET: one consumer-group offset commit. Durable (offset
/// commits survive restart exactly like session records, per
/// docs/architecture/kafka_adapter.md §Consumer groups); the
/// OffsetCommitResponse is NOT gated on durability — a lost commit
/// re-consumes from the previous offset, which the at-least-once
/// contract already admits.
///
/// Op-body: `[group_len:u16 LE][group][topic_len:u16 LE][topic]
///           [partition:u16 LE][offset:i64 LE]`.
pub const QOP_KAFKA_OFFSET: u8 = 0x0A;

// ── Disconnect reasons (QOP_DISCONNECT body) ────────────────────────────────

/// Client closed the session via DISCONNECT packet (MQTT) or equivalent.
pub const QDISC_REASON_CLEAN: u8 = 0;

/// Keep-alive deadline elapsed; the broker tore down the slot.
pub const QDISC_REASON_KEEPALIVE: u8 = 1;

/// Operator / admin-driven forced disconnect.
pub const QDISC_REASON_FORCED: u8 = 2;

// ── Encoders / decoders ─────────────────────────────────────────────────────

/// Write the canonical envelope header (without the discriminator) into `dst`.
/// Returns `QPROP_HEADER_LEN` on success or `-1` if `dst` is too small.
/// Wrapper around `encode_qprop_header_v` that hardcodes V1 for ops
/// whose body shape hasn't bumped.
#[inline]
pub fn encode_qprop_header(dst: &mut [u8], op: u8, tenant: u32, session_slot: u32) -> i32 {
    encode_qprop_header_v(dst, QPROP_VERSION_V1, op, tenant, session_slot)
}

/// Version-aware variant of `encode_qprop_header`. Use this when
/// emitting an op whose body shape requires a non-V1 version
/// (currently only QOP_PUBLISH V2, see `QPROP_VERSION_V2`).
#[inline]
pub fn encode_qprop_header_v(
    dst: &mut [u8],
    version: u8,
    op: u8,
    tenant: u32,
    session_slot: u32,
) -> i32 {
    if dst.len() < QPROP_HEADER_LEN {
        return -1;
    }
    dst[0] = version;
    dst[1] = op;
    dst[2..6].copy_from_slice(&tenant.to_le_bytes());
    dst[6..10].copy_from_slice(&session_slot.to_le_bytes());
    QPROP_HEADER_LEN as i32
}

/// Decode the canonical envelope header. Returns
/// `(version, op, tenant, session_slot)` if `buf` is long enough and
/// the version is recognised; `None` otherwise.
#[inline]
pub fn decode_qprop_header(buf: &[u8]) -> Option<(u8, u8, u32, u32)> {
    if buf.len() < QPROP_HEADER_LEN {
        return None;
    }
    let version = buf[0];
    if version != QPROP_VERSION_V1 && version != QPROP_VERSION_V2 {
        return None;
    }
    let op = buf[1];
    let tenant = u32::from_le_bytes([buf[2], buf[3], buf[4], buf[5]]);
    let session_slot = u32::from_le_bytes([buf[6], buf[7], buf[8], buf[9]]);
    Some((version, op, tenant, session_slot))
}

/// Result of peeling a Quantum proposal body received on
/// `MSG_COMMITTED_ENTRY`. `op_body_offset` points into the original
/// buffer at the first byte of the op-specific body (after the
/// canonical header).
#[derive(Clone, Copy, Debug)]
pub struct PeeledQProp {
    pub version: u8,
    pub op: u8,
    pub tenant: u32,
    pub session_slot: u32,
    pub op_body_offset: usize,
}

/// Peel a Quantum proposal body. Returns `None` if the canonical
/// header is truncated or the version is unsupported. Call this on
/// the body half of a `MSG_COMMITTED_ENTRY` envelope (after the
/// `[term:u64][index:u64]` prefix is stripped) — Clustor's tagged-
/// proposal handler strips any `correlation_id` upstream so the body
/// at this point is the canonical envelope for both tagged and
/// untagged proposals.
pub fn peel_qprop(buf: &[u8]) -> Option<PeeledQProp> {
    let (version, op, tenant, session_slot) = decode_qprop_header(buf)?;
    Some(PeeledQProp {
        version,
        op,
        tenant,
        session_slot,
        op_body_offset: QPROP_HEADER_LEN,
    })
}
