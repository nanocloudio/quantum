# Apply Pipeline

Quantum drives every durable mutation through the Raft apply side, so
that follower replicas and post-restart replay converge on the same
state as the leader. This document defines the propose/apply seam: the
canonical proposal format, the apply-side dispatch, reset handling, the
snapshot payload, and the substrate primitives the seam rests on. The
invariants it protects are stated in
[messaging_model.md](messaging_model.md); the surrounding cascade
(propose → WAL → durable → ACK) is in
[../architecture.md](../architecture.md#durability-model).

## The seam

Propose-side handling is admission control, encoding, and proposing —
nothing durable. All durable state (session records, dedupe shards,
retained store, offline queues, topic subscriptions, forward counters)
mutates only when a committed entry arrives on
`session_processor.committed_in`. This is what makes the state machine
replicable: a follower that never sees a client proposal still
reconstructs identical state from the committed-entry stream, and a
restarted node rebuilds from the WAL.

`session_processor` is the sole apply-side translator. Downstream
consumers (`messaging`'s dedup / retained / offline components,
`topic_engine`, `forward_coordinator`) do not subscribe to committed
entries; they consume the apply-side emissions `session_processor`
re-broadcasts on their existing input ports. Centralising the
committed-entry framing in one module keeps the framing knowledge in one
place, preserves every other module's port budget, and narrows the
apply-pipeline contract to a single consumer.

## Canonical proposal format

Every proposal that mutates Quantum state is encoded as:

```
[version:u8]
[op:u8]
[tenant:u32 LE]
[session_slot:u32 LE]
[op-specific body]
```

The leading `version` byte is fixed from day one: once entries are
logged in a given format, replay must understand them for the life of
the WAL, and the version byte is what makes format evolution safe.
Two versions exist: V1, and V2, which changes only the `QOP_PUBLISH`
body (an MQTT 5 user-property block between `topic` and `payload`).
The version is per-proposal, so callers keep emitting V1 for ops
whose shape has not changed, and apply-side parsers honour every
shipped version forever. Only additive body changes are permitted
without a version bump.

Opcodes are defined in `modules/common/wire.rs` (values reserved in the
`0x01`–`0x0F` range, distinct from `msg_type` constants); the docstrings
there are authoritative for body shape.

| Opcode | Name | Body |
|---|---|---|
| 0x01 | `QOP_CONNECT` | `[clean_start:u8][keep_alive_s:u16 BE][stream_hash:u64 LE][cid_len:u16 BE][cid][protocol:u8][will_flag:u8]` then, when `will_flag == 1`, the Will section `[will_qos:u8][will_retain:u8][will_delay_s:u32 LE][will_topic_len:u16 BE][will_topic][will_payload_len:u16 BE][will_payload]`; the trailer is `[session_expiry_s:u32 LE][receive_maximum:u16 LE]` |
| 0x02 | `QOP_DISCONNECT` | `[reason:u8][stream_hash:u64 LE]` |
| 0x03 | `QOP_PUBLISH` | V1: `[pub_qos:u8][packet_id:u16 BE][stream_hash:u64 LE][session_epoch:u32 LE][retain:u8][topic_len:u16 BE][topic][payload]`. V2 inserts `[user_props_count:u8][{key_len:u16 BE, key, val_len:u16 BE, val}…]` between `topic` and `payload` |
| 0x04 | `QOP_SUBSCRIBE` | `[req_qos:u8][stream_hash:u64 LE][topic_len:u16 BE][topic]` |
| 0x05 | `QOP_UNSUBSCRIBE` | `[stream_hash:u64 LE][topic_len:u16 BE][topic]` |
| 0x06 | `QOP_PUBREL` | `[packet_id:u16 BE][stream_hash:u64 LE][session_epoch:u32 LE]`. The epoch is a trailing field: entries logged without it replay against the session's current epoch, as they did when written |
| 0x07 | `QOP_RETAINED_CLEAR` | `[topic_len:u16 BE][topic]` |
| 0x08 | `QOP_KAFKA_PRODUCE` | `[partition:u16 LE][topic_len:u16 LE][topic][records — the verbatim Kafka record batch]`; tagged when `acks != 0`, untagged for `acks == 0` |
| 0x09 | `QOP_AMQP_PUBLISH` | `[rk_len:u16 LE][routing_key][payload]`; tagged when the channel is in confirm mode |
| 0x0A | `QOP_KAFKA_OFFSET` | `[group_len:u16 LE][group][topic_len:u16 LE][topic][partition:u16 LE][offset:i64 LE]` |

`QOP_CONNECT`'s Will section and expiry trailer are additive: a
parser written against the shorter shape stops at the last field it
understands and treats the missing fields as defaults, so no version
bump was needed. MQTT 3.1.1 connections are normalised propose-side
(`session_expiry_s = u32::MAX` for `clean_session=0`, `0` for
`clean_session=1`; `receive_maximum == 0` means no cap).

`stream_hash` is carried in every session-targeted body so that
followers and replay can locate the durable session record without the
`session_slot` field — that field is the leader's local slot index and
is not portable across nodes.

Tagged proposals (QoS 1+ publishes and `QOP_PUBREL`, whose publisher ACK
is gated on durability) arrive wrapped in `[correlation_id:u64 LE][body]`.
The substrate strips the `correlation_id` before storing the entry, so
the committed body is byte-identical to the untagged form. On the leader,
the `correlation_id` is recovered from the matching publisher inflight;
followers carry no inflight, so `correlation_id` stays 0 and QoS 1+
emits `MSG_TOPIC_PUBLISH` directly.

## Apply-side dispatch

`session_processor.committed_in` peels the discriminator and (for tagged
ops) the `correlation_id`, then dispatches on the canonical op:

| Op | Apply action |
|---|---|
| `QOP_CONNECT` | Allocate/resurrect/refresh the session slot; reconcile any propose-side transient record for this `(tenant, stream_hash)`; set `session_epoch`, `keep_alive_ms`, `protocol`. |
| `QOP_DISCONNECT` | Deactivate the slot. On `QDISC_REASON_CLEAN`, emit `MSG_SESSION_DROP` so `topic_engine` purges subscriptions; otherwise park the slot `persisted = 1` per MQTT 3.1.1 §3.1.2.4. |
| `QOP_PUBLISH` | Emit `MSG_DEDUP_CHECK`, `MSG_RETAINED_WRITE` (if `retain`) on `out_messaging` and `MSG_TOPIC_PUBLISH` on `out_topic`. On the leader the publisher's PUBACK follows from `flow`'s ack component; on a follower the stash slot is allocated and released without a client-facing emit. For QoS 2, every node also opens the publisher's in-flight slot in the PUBLISH phase and records that phase on the dedupe entry, so the transaction is known to be live wherever it is replayed. |
| `QOP_SUBSCRIBE` / `QOP_UNSUBSCRIBE` | Emit `MSG_TOPIC_SUBSCRIBE` / `MSG_TOPIC_UNSUBSCRIBE` keyed to the apply-side slot. |
| `QOP_PUBREL` | Drive the QoS 2 phase transition on both the in-flight slot and the dedupe entry, opening the slot if the flow has none; on the leader the durability proof then fires PUBCOMP and releases it, on a follower the transition applies with no client-facing emit. Advancing is monotone, so a replayed or repeated PUBREL is a no-op on state and still answerable. |
| `QOP_RETAINED_CLEAR` | Emit an explicit clear op on `out_messaging` keyed to `topic_hash`. |
| `QOP_KAFKA_PRODUCE` | Append the record batch to the topic-partition's message log; the assigned WAL index becomes the batch's base offset. |
| `QOP_AMQP_PUBLISH` | Append the body to the routing key's message log (shared store with Kafka; entries are flagged raw so Kafka Fetch skips them and Basic.Get returns them verbatim). |
| `QOP_KAFKA_OFFSET` | Record the consumer-group offset commit. |

Apply-side handlers stay bounded per tick (a fixed inner-loop cap); the
substrate paces committed-entry emission, so replaying a large WAL is the
substrate's concern, not the handler's.

### Optimistic CONNACK

CONNACK is sent on the propose side, before `QOP_CONNECT` commits — a
Raft round-trip per connect would overwhelm an IoT workload where clients
reconnect aggressively. The authoritative session record is created on
apply. Between CONNECT receipt and apply, the propose side holds a
transient session record (local-only, non-durable, keyed to `conn_id`)
for admission control so a follow-up PUBLISH can find its slot; the
`QOP_CONNECT` apply reconciles or the keep-alive sweep purges it.
`session_present` in the CONNACK is best-effort optimistic, set from the
propose-side view of `(tenant, stream_hash)`.

Keep-alive and clean-DISCONNECT both propose a `QOP_DISCONNECT` and let
the apply side perform the state mutation; the socket may drop
immediately (a non-durable, peer-router-level concern), but the source
of truth is the WAL entry.

## Apply-pipeline reset

When the substrate signals `MSG_APPLY_PIPELINE_RESET` (leader-driven
truncation or reset), every apply-derived arena zeroes and rebuilds.
`session_processor` is the sole subscriber: it wipes its own arenas
(sessions, message store, correlation tables), fast-forwards
`apply_index` to the reset index, and fans `MSG_APPLY_RESET_FANOUT`
out on the topic and messaging buses so `topic_engine` and
`messaging`'s dedup, retained and offline components clear their own
state. Propose-side transient slots are wiped too: a CONNECT pending
at reset time is lost and the client reconnects — that is the
failover semantic. State then rebuilds from committed entries at or
above the reset index.

## Snapshots

**Status: design target, not wired.** Apply-state snapshot export and
install are not implemented; today a reset rebuilds from WAL replay
alone. The target design: Quantum owns its snapshot payload, and the
substrate calls an export hook at snapshot time and an install hook
on cold-start / learner-catchup without interpreting the bytes. Each
sub-section is self-delimiting and forward-compatible — an unknown
trailing sub-section is ignored, so additive changes need no version
bump.

```
[magic:u32 = "QSNP"][version:u32 = 1]
[session_count:u32][session entries...]
[dedup_shard_count:u32][shard entries...]
[retained_count:u32][retained entries...]
[offline_queue_count:u32][queue entries...]
[forward_counter_count:u32][counter entries...]
[checksum:u64]
```

| Component | Content |
|-----------|---------|
| Session records | `{tenant_id, stream_id, session_epoch, connected_at, keep_alive, protocol_state}` |
| Subscription index | Active subscriptions, QoS levels, shared-group assignments |
| Dedupe shards | 16-shard `DedupeKey → DedupeState` with expiry |
| Offline queues | Per-session FIFO entries |
| Retained messages | Topic-indexed payloads |
| Consumer groups | Group metadata, member assignments, committed offsets |
| Forward sequences | Per-PRG `forward_seq` counters |
| Routing epoch | Current epoch for placement validation |

Encoding is bare-metal-safe: fixed-size `#[repr(C)]` state, stack
buffers, and bounds-checked copies — no heap, no panic paths.

## Substrate primitives

Every durable Quantum op lands on the substrate via exactly one
primitive below. The substrate contract is clustor's; operational
signals (PID credits, lag, prefetch) are not durable and are not in
this surface.

| Substrate primitive | Quantum op(s) | Consumer |
|---|---|---|
| `proposals` (untagged) | `QOP_CONNECT`, `QOP_DISCONNECT`, `QOP_SUBSCRIBE`, `QOP_UNSUBSCRIBE`, `QOP_RETAINED_CLEAR`, QoS 0 `QOP_PUBLISH`, `acks=0` `QOP_KAFKA_PRODUCE`, non-confirm `QOP_AMQP_PUBLISH`, `QOP_KAFKA_OFFSET` | `session_processor` |
| `proposals_tagged` | QoS 1+ `QOP_PUBLISH`, `QOP_PUBREL`, `acks!=0` `QOP_KAFKA_PRODUCE`, confirm-mode `QOP_AMQP_PUBLISH` | `session_processor` |
| `proposal_assigned` (echo) | Binds `correlation_id → (session_slot, packet_id, op)`; emits `MSG_ACK_REGISTER` | `session_processor` → `flow`'s ack component |
| `committed_entries` | Sole apply-side translation of session / dedup / retained / offline / topic state | `session_processor.committed_in` |
| `quorum_durable` (proof) | Drives `MSG_ACK_EMIT` → PUBACK / PUBREC / PUBCOMP | `flow`'s ack component |
| `MSG_APPLY_PIPELINE_RESET` | Clears apply-derived arenas and fans out the reset | `session_processor` |
| Snapshot install / export | Quantum-owned payload (design target, above) | `session_processor` + apply-state modules |

### Multi-PRG proof keying

A single Quantum process hosts multiple PRGs (see
[partitioning.md](partitioning.md)), and `wal_index` is per-PRG — two
proposals on different PRGs can share a `wal_index`. `flow`'s ack component
therefore matches durability proofs by the `(partition_id, wal_index)`
tuple. Both `MSG_PROPOSAL_ASSIGNED` and `MSG_DURABILITY_PROOF` carry the
partition id for this reason; it is the one place Quantum's contract is
strictly wider than the single-PRG primitive description.
