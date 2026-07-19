# Substrate adapter notes (Quantum)

Quantum consumes the substrate contract defined at
[`../../clustor/docs/architecture/substrate_capability_surface.md`](../../clustor/docs/architecture/substrate_capability_surface.md).
This document covers the Quantum-specific mapping and notes — which
Quantum op uses which primitive, which Quantum module owns the
consumer-side wiring, and where Quantum's apply-state shape needs
extra context beyond the generic substrate expectation.

The canonical reference for Quantum's apply-side state machine is
[apply_side_state_machine.md](apply_side_state_machine.md). Read that
first for the design rationale; this document is the cross-walk
between Quantum's ops and the substrate's primitives.

## Per-primitive mapping

Every Quantum op that mutates durable state lands on the substrate via
exactly one of the primitives below. Operational signals (PID
credits, lag, prefetch) are not on this list — see "What is NOT in the
surface" in the clustor doc.

| Substrate primitive | Quantum op(s) that use it |
|---|---|
| `proposals` (untagged) | `QOP_CONNECT`, `QOP_DISCONNECT`, `QOP_SUBSCRIBE`, `QOP_UNSUBSCRIBE`, `QOP_RETAINED_CLEAR`, QoS 0 `QOP_PUBLISH` |
| `proposals_tagged` | QoS 1+ `QOP_PUBLISH`, `QOP_PUBREL` (publisher ack gated on durability) |
| `proposal_assigned` (echo) | `session_processor` binds `correlation_id → (session_slot, packet_id, op)` and emits `MSG_ACK_REGISTER` to `ack_tracker` |
| `committed_entries` (per-entry stream) | `session_processor.committed_in` — sole apply-side translator for session / dedup / retained / offline / topic state |
| `quorum_durable` (durability proof) | `ack_tracker` — drives `MSG_ACK_EMIT` which produces PUBACK / PUBREC / PUBCOMP |
| Snapshot install / export hooks | Quantum-owned payload covering session table, dedup shards, retained store, offline queue, forward counters (Phase 5) |
| `MSG_APPLY_PIPELINE_RESET` | `session_processor` clears apply-derived arenas and fans the reset out to downstream modules (Phase 4) |

## Quantum-specific notes

### PRG keying via `(partition_id, wal_index)`

A single Quantum process hosts multiple PRGs (one per tenant slice
per node — see [architecture/partitioning.md](architecture/partitioning.md)).
The substrate's `wal_index` is per-PRG, so two distinct proposals on
different PRGs can carry the same `wal_index`. `ack_tracker` matches
durability proofs by the `(partition_id, wal_index)` tuple, not by
`wal_index` alone. Both `MSG_PROPOSAL_ASSIGNED` (18 bytes; see
`wire::PROPOSAL_ASSIGNED_LEN`) and `MSG_DURABILITY_PROOF` (19 bytes;
see `wire::DURABILITY_PROOF_LEN`) carry the partition id for this
reason.

This is the one place Quantum's contract is strictly wider than the
generic primitive description: the substrate need not expose
`partition_id` to a single-PRG consumer, but a multi-PRG consumer
(like Quantum) does need it.

### `session_processor` as sole apply-side translator

Downstream modules (`dedup_engine`, `retained_store`, `offline_queue`,
`topic_engine`, `forward_coordinator`) do **not** subscribe to
`committed_entries` directly. They consume apply-side emissions
re-broadcast by `session_processor` on their existing input ports.

This is a deliberate Quantum-side choice (rationale in
[apply_side_state_machine.md](apply_side_state_machine.md) §Phase 3):
it keeps `MSG_COMMITTED_ENTRY` framing knowledge in one place,
preserves port-budget headroom in every other module, and narrows
the apply-pipeline contract.

### Optimistic CONNACK

CONNACK is sent on the propose side, before `QOP_CONNECT` commits.
This is required for an IoT workload — a Raft round-trip per
connect would melt the cluster. The authoritative session record is
created on apply. Between CONNECT receipt and CONNECT apply, the
propose-side holds a *transient* session record (local-only,
non-durable) for admission control only. Rationale and reconciliation
rules: [apply_side_state_machine.md](apply_side_state_machine.md) §Phase 2.

### Apply-pipeline reset wiring (current gap)

Quantum's `wire.rs` now defines `MSG_APPLY_PIPELINE_RESET = 0x2B`
mirroring clustor's value (added in Phase 1). The consumer wiring is
still pending: `session_processor.committed_in` does not yet dispatch
on the reset envelope, and the per-module fan-out
([apply_side_state_machine.md](apply_side_state_machine.md) §Phase 4)
is not yet implemented. Until Phase 4 lands, follower-side state can
diverge across a snapshot install or leader-driven truncation.

### Snapshots (current gap)

Quantum does not yet implement the snapshot export / install hooks.
Until Phase 5 ships, replay starts from WAL index 0 on cold start.
Acceptable for the first ship; bounded by disk read bandwidth.
Operator-facing implication is captured in
[apply_side_state_machine.md](apply_side_state_machine.md) §Phase 5.

## Consumer pointers

Each consumer of a substrate signal carries a
`// see docs/clustor_capability_surface.md` comment at the use site so
a reader chasing one signal's contract lands here. The table below is
the inverse view — where to look when chasing a primitive.

| Primitive | Quantum consumer | Use site |
|---|---|---|
| `proposals` | `session_processor` | `out_proposals` emit sites |
| `proposals_tagged` | `session_processor` | `out_proposals_tagged` emit sites |
| `proposal_assigned` | `session_processor` | Phase 3b in `module_step` |
| `committed_entries` | `session_processor` | Phase 3 in `module_step` (`committed_in`) |
| `quorum_durable` | `ack_tracker` | (see ack_tracker module) |
| Snapshot hooks | TBD — wire-up in Phase 5 | — |
| `MSG_APPLY_PIPELINE_RESET` | `session_processor` (dispatches into downstream modules via existing buses) | Phase 4 in [apply_side_state_machine.md](apply_side_state_machine.md) |
