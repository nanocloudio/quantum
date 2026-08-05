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
[version:u8 = 1]
[op:u8]
[tenant:u32 LE]
[session_slot:u32 LE]
[op-specific body]
```

The leading `version` byte is fixed from day one: once entries are
logged in a given format, replay must understand them for the life of
the WAL, and the version byte is what makes format evolution safe. Only
additive body changes are permitted without a version bump.

Opcodes are defined in `modules/common/wire.rs` (values reserved in the
`0x01`–`0x0F` range, distinct from `msg_type` constants); the docstrings
there are authoritative for body shape.

| Opcode | Name | Body |
|---|---|---|
| 0x01 | `QOP_CONNECT` | `[clean_start:u8][keep_alive_s:u16 BE][stream_hash:u64 LE][cid_len:u16 BE][cid][protocol:u8]` |
| 0x02 | `QOP_DISCONNECT` | `[reason:u8][stream_hash:u64 LE]` |
| 0x03 | `QOP_PUBLISH` | `[pub_qos:u8][packet_id:u16 BE][stream_hash:u64 LE][session_epoch:u32 LE][retain:u8][topic_len:u16 BE][topic][payload]` |
| 0x04 | `QOP_SUBSCRIBE` | `[req_qos:u8][stream_hash:u64 LE][topic_len:u16 BE][topic]` |
| 0x05 | `QOP_UNSUBSCRIBE` | `[stream_hash:u64 LE][topic_len:u16 BE][topic]` |
| 0x06 | `QOP_PUBREL` | `[packet_id:u16 BE][stream_hash:u64 LE]` |
| 0x07 | `QOP_RETAINED_CLEAR` | `[topic_len:u16 BE][topic]` |

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
| `QOP_DISCONNECT` | Deactivate the slot. On `REASON_CLEAN_START`, emit `MSG_SESSION_DROP` so `topic_engine` purges subscriptions; otherwise park the slot `persisted = 1` per MQTT 3.1.1 §3.1.2.4. |
| `QOP_PUBLISH` | Emit `MSG_DEDUP_CHECK`, `MSG_RETAINED_WRITE` (if `retain`) on `out_messaging` and `MSG_TOPIC_PUBLISH` on `out_topic`. On the leader the publisher's PUBACK follows from `flow`'s ack component; on a follower the stash slot is allocated and released without a client-facing emit. |
| `QOP_SUBSCRIBE` / `QOP_UNSUBSCRIBE` | Emit `MSG_TOPIC_SUBSCRIBE` / `MSG_TOPIC_UNSUBSCRIBE` keyed to the apply-side slot. |
| `QOP_PUBREL` | Drive the QoS 2 phase transition; on the leader this releases the inflight and fires PUBCOMP, on a follower it applies the transition with no emit. |
| `QOP_RETAINED_CLEAR` | Emit an explicit clear op on `out_messaging` keyed to `topic_hash`. |

Apply-side handlers stay bounded per tick (a fixed inner-loop cap); the
substrate paces committed-entry emission, so replaying a large WAL is the
substrate's concern, not the handler's.

### Optimistic CONNACK

CONNACK is sent on the propose side, before `QOP_CONNECT` commits — a
Raft round-trip per connect would melt an IoT workload where clients
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

When the substrate signals `MSG_APPLY_PIPELINE_RESET` (snapshot install
or leader-driven truncation), every apply-derived arena zeroes and
rebuilds. `session_processor` is the sole subscriber; it clears its own
arenas and fans the reset out to downstream modules over their existing
buses so they clear before receiving the snapshot re-emissions.

| Module | On reset |
|---|---|
| `session_processor` | Clear sessions, prefetch credits, outstanding subs, pending/stash pools; re-emit `MSG_SESSION_DROP` for any slot the snapshot does not reinstate. |
| `messaging` / dedup | Clear all shards; reload from snapshot. |
| `messaging` / retained | Clear retained map; reload from snapshot. |
| `messaging` / offline | Clear queues; reload from snapshot; emit `MSG_OFFLINE_DRAIN` for reconnected slots that lost queued state. |
| `topic_engine` | Clear subscriptions; reload from snapshot. |
| `forward_coordinator` | Clear `(ingress, egress, epoch) → seq` counters; reload from snapshot. |

## Snapshots

Quantum owns its snapshot payload; the substrate calls an export hook at
snapshot time and an install hook on cold-start / learner-catchup and
does not interpret the bytes. Each sub-section is self-delimiting and
forward-compatible — an unknown trailing sub-section is ignored, so
additive changes need no version bump.

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
| Session records | `{tenant_id, stream_id, session_epoch, auth_chain_digest, connected_at, keep_alive, protocol_state}` |
| Subscription index | Active subscriptions, QoS levels, shared-group assignments |
| Dedupe shards | 16-shard `DedupeKey → DedupeState` with expiry |
| Offline queues | Per-session FIFO entries with content-addressed payload refs |
| Retained messages | Topic-indexed payloads with content-addressed refs |
| Consumer groups | Group metadata, member assignments, committed offsets |
| Transaction state | In-progress transactions, involved PRGs, markers |
| Forward sequences | Per-PRG `forward_seq` counters |
| Routing epoch | Current epoch for placement validation |

Encoding is bare-metal-safe: fixed-size `#[repr(C)]` state, stack
buffers, and `core::ptr::copy_nonoverlapping` with caller-verified
bounds — no heap, no `copy_from_slice` panic paths.

## Substrate primitives

Every durable Quantum op lands on the substrate via exactly one
primitive below. The substrate contract is defined in
[Clustor's substrate capability surface](../../../clustor/docs/architecture/substrate_capability_surface.md);
operational signals (PID credits, lag, prefetch) are not durable and are
not in this surface.

| Substrate primitive | Quantum op(s) | Consumer |
|---|---|---|
| `proposals` (untagged) | `QOP_CONNECT`, `QOP_DISCONNECT`, `QOP_SUBSCRIBE`, `QOP_UNSUBSCRIBE`, `QOP_RETAINED_CLEAR`, QoS 0 `QOP_PUBLISH` | `session_processor` |
| `proposals_tagged` | QoS 1+ `QOP_PUBLISH`, `QOP_PUBREL` | `session_processor` |
| `proposal_assigned` (echo) | Binds `correlation_id → (session_slot, packet_id, op)`; emits `MSG_ACK_REGISTER` | `session_processor` → `flow`'s ack component |
| `committed_entries` | Sole apply-side translation of session / dedup / retained / offline / topic state | `session_processor.committed_in` |
| `quorum_durable` (proof) | Drives `MSG_ACK_EMIT` → PUBACK / PUBREC / PUBCOMP | `flow`'s ack component |
| `MSG_APPLY_PIPELINE_RESET` | Clears apply-derived arenas and fans out the reset | `session_processor` |
| Snapshot install / export | Quantum-owned payload (above) | `session_processor` + apply-state modules |

### Multi-PRG proof keying

A single Quantum process hosts multiple PRGs (see
[partitioning.md](partitioning.md)), and `wal_index` is per-PRG — two
proposals on different PRGs can share a `wal_index`. `flow`'s ack component
therefore matches durability proofs by the `(partition_id, wal_index)`
tuple. Both `MSG_PROPOSAL_ASSIGNED` and `MSG_DURABILITY_PROOF` carry the
partition id for this reason; it is the one place Quantum's contract is
strictly wider than the single-PRG primitive description.

## Verification

The single-node integration suite (`make test-mqtt-suite`) exercises
every propose-then-apply op and proves the seam is load-bearing:

| Test | What it exercises |
|---|---|
| `module_graph_mqtt.sh` | CONNECT → `QOP_CONNECT` (slot active) → QoS 1 PUBLISH → `QOP_PUBLISH` → `MSG_ACK_EMIT` → PUBACK. PUBACK fires only after the entry commits, so apply runs first. |
| `module_graph_pubsub.sh` | `QOP_SUBSCRIBE` installs in `topic_engine` via apply; QoS 0 PUBLISH fans out via apply to the subscriber. |
| `module_graph_resume.sh` | Persistent-session lifecycle: clean disconnect drops the slot; non-clean parks `persisted=1`; reconnect resurrects with `session_present=1` and the surviving subscription still delivers. |
| `module_graph_load.sh` | Sustained QoS 1 load; validates no leak in the inflight / correlation / stash pools across many cycles. |

Replay and follower-convergence tests extend this to multi-node
guarantees: a restarted node converges to the same state, and a follower
that never received propose-side traffic ends with the same session,
dedup, retained, and offline state as the leader.
