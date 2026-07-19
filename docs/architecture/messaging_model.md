# Messaging Model

The canonical reference for Quantum's messaging entities and the
invariants that govern them. Every adapter — MQTT, Kafka, AMQP —
maps its protocol-specific concepts onto the entities defined here,
and every persistence and routing concern in the rest of the
architecture docs builds on the invariants below.

Clustor is authoritative for consensus, WAL framing, durability
ledgers, read gates, strict fallback, storage layout, and ledger
ordering. This document specifies the messaging-overlay shape that
sits on top.

## Terminology

| Term | Definition |
|---|---|
| **PRG** | Partition Raft Group — a tenant-scoped Clustor partition (3 voters by default) that owns a slice of session and topic state. |
| **CP-Raft** | Control Plane Raft cluster holding tenant manifests, routing epochs, quotas, PKI bundles, feature gates, and durability proofs. |
| **Protocol adapter** | Module that maps a wire protocol onto session-processor APIs and Clustor durability primitives. Quantum ships three: `mqtt_codec`, `kafka_codec`, `amqp_codec`. |
| **Session Processor** | Messaging state machine (the `session_processor` module). Handles connection binding, QoS / ack handshakes, inflight replay, offline queues, protocol reason codes. Plugs into Clustor's `apply_pipeline`. |
| **session_epoch** | Monotone counter per `(tenant_id, stream_id)` fencing session state; increments on clean reconnect or fenced takeover. |
| **stream_id** | Protocol-defined logical stream identifier (MQTT `client_id`, Kafka producer ID, AMQP container ID). |
| **dedupe entry** | `(tenant_id, stream_id, session_epoch, message_id)` map rejecting duplicates until `dedupe_ttl` expires. Owned by `dedup_engine`. |
| **offline_queue** | Persistent FIFO storing durable deliveries for disconnected or throttled sessions. Owned by `offline_queue` module. |
| **forward_seq** | Idempotence key for cross-PRG forwarding; monotonically increasing per `(ingress PRG, egress PRG, routing_epoch)` and persisted so replay fences duplicates. Owned by `forward_coordinator`. |
| **dirty_epoch** | Routing-epoch mismatch condition; adapters map it to protocol-specific outcomes (reject, disconnect, retry). |

## Default timers

All timers are operator-configurable in the graph YAML; tenants may
override within operator-policy bounds.

| Timer | Default | Maximum |
|---|---|---|
| `session_ttl_default_ms` | 259 200 000 (72h) | 604 800 000 (7d) |
| `dedupe_ttl_default_ms` | 259 200 000 (72h) | 604 800 000 (7d) |
| `offline_queue_ttl_default_ms` | 259 200 000 (72h) | 604 800 000 (7d) |

The 72h / 7d window covers the longest realistic disconnect-and-resume
scenarios while keeping dedupe and offline-queue state arenas bounded.

## Entities

### Session record

```
session_record {
    tenant_id
    stream_id
    session_epoch
    auth_chain_digest
    connected_at
    keep_alive_ms?
    protocol_state          // adapter-specific
}
```

Persisted by `session_processor` through WAL frames committed via
`raft_engine` → `wal` → `durability_ledger`. Survives node restart and
leader transfer.

### Routing record

```
routing_record {
    subject                 // topic / queue / partition
    subscribers[]
    forward_seq             // per (ingress, egress, epoch)
    last_emit_index         // last WAL index emitted by this PRG for this subject
}
```

Owned by `topic_engine` and `forward_coordinator`. `last_emit_index`
is the floor below which WAL compaction must not truncate forwards
that haven't been acknowledged by the destination PRG.

### Retained record

```
retained_record {
    subject
    payload_ref             // content-addressed
    updated_at
}
```

Owned by `retained_store`. Payload references are content-addressed
so identical retained payloads across topics share storage.

### Dedupe entry

```
DedupeKey → DedupeState {
    phase                   // adapter-specific (e.g. MQTT QoS 2 four-phase)
    publish_index
    ack_index
    expiry_at
}
```

Owned by `dedup_engine` (16-shard partitioned map). Entries expire
after `dedupe_ttl_default_ms`. The earliest non-expired index per PRG
(`earliest_dedupe_index`) is the WAL compaction floor for dedupe
state.

### Offline queue entry

```
offline_entry {
    sequence
    payload_ref             // content-addressed
    expiry_at
}
```

Owned by `offline_queue`. The earliest non-expired index per PRG
(`earliest_offline_queue_index`) is the WAL compaction floor for
queued state. Bounded by per-tenant quota policy.

## Invariants

The four invariants below are the load-bearing contracts the
messaging overlay rests on. Any adapter implementation, snapshot
format, or compaction policy that violates one is broken.

### WAL-SOURCE

Every observable messaging effect stems from WAL entries or signed
snapshots. There are no parallel durable channels — no separate
per-node caches, no out-of-band ack stores, no shadow ledgers. If it
isn't in the WAL (or a snapshot derived from it), it didn't happen.

### DETERMINISTIC-REPLAY

Replay from WAL + snapshot deterministically reconstructs all durable
messaging state. Compaction must not truncate WAL entries required to
reconstruct state at or above `earliest_dedupe_index` or
`earliest_offline_queue_index`. Two nodes that replay the same prefix
produce byte-identical PRG state.

### ACK-DURABILITY

Protocol-level ACKs emit only after WAL entries reach quorum
durability. `ack_tracker` consumes proofs from
`durability_ledger.quorum_durable` directly; there is no path from
"WAL written locally" to "PUBACK sent" that skips quorum.

This is the contract that gives MQTT QoS 1/2, Kafka `acks=all`, and
AMQP `settled=false` their no-loss guarantee. Adapters must not
advertise stronger semantics than what this contract delivers.

### ROUTING-EPOCH

State transitions carry the CP-Raft routing epoch; mismatches result
in `dirty_epoch` rejection. Adapters map `dirty_epoch` to a
protocol-specific outcome:

- MQTT: `0x95` (Topic name invalid) or `0x9C` (Use another server) depending on context.
- Kafka: `NOT_LEADER_OR_FOLLOWER` + epoch fence.
- AMQP: `link-detach` with `amqp:link:detach-forced`.

Epoch changes propagate through `cp_bridge` →
`placement_router.epoch_events` → `session_processor`, which fences
in-flight state before accepting traffic on the new epoch.

## Crash model

Per Clustor §§6.2 and 10.5. Quantum adds no new crash-model
assumptions beyond what the substrate guarantees: write-path ordering
is fsync-on-quorum, in-flight non-acked messages may be retried by
the client, and replay reproduces all durable state.

## Why these invariants, in this order

The four invariants compose:

1. **WAL-SOURCE** establishes that the WAL is the truth.
2. **DETERMINISTIC-REPLAY** establishes that replay of that truth reconstructs identical state on every node.
3. **ACK-DURABILITY** establishes that the truth has been replicated before clients observe it.
4. **ROUTING-EPOCH** establishes that two writers can't disagree about who owns which subject.

Together they bound the failure modes Quantum can produce: at-least-once
delivery during in-flight retries (which clients already expect from
QoS 1/2), bounded duplicate windows during DR promotion (see
[disaster_recovery.md](disaster_recovery.md)), and no silent data loss
or double-ack of the same publish. Every other architectural decision
flows from preserving these four.
