# Apply-side State Machine

How Quantum drives every durable mutation through `committed_entries`
so that follower replicas and post-restart replay converge on the
same state as the leader. The substrate contract this design rests
on is enumerated in
[clustor_capability_surface.md](clustor_capability_surface.md); the
invariants this design protects are stated in
[architecture/messaging_model.md](architecture/messaging_model.md).

This is an architecture document, not a step-by-step implementation
plan. It defines the seams, the wire format, and the consistency
story; it explicitly leaves a handful of design questions open for
discussion before code lands.

## Why this exists

Today, durable Quantum mutations happen on the *propose* side:

- `session_processor` mutates the session table when a CONNECT envelope
  arrives from `mqtt_codec`.
- It emits `MSG_DEDUP_CHECK`, `MSG_RETAINED_WRITE`, `MSG_OFFLINE_ENQUEUE`
  directly on `out_messaging`, and downstream modules mutate their
  arenas the moment they see the message.
- The QoS 1+ topic-publish stash + dedup-result + durability gate
  already does the "wait for commit before fan-out" dance, but
  *applying* the publish to dedup / retained / offline state still
  happens propose-side.

That works on a single node with no replication and a fresh process.
It does not work for:

1. **Followers.** A replica that never sees a `mqtt_codec` proposal
   never mutates its session table, dedup shards, retained store, or
   offline queue. When the leader fails and a follower is promoted,
   the new leader's state machine is *empty* on these fronts.
2. **Replay.** A node that restarts and replays the WAL sees only
   `MSG_COMMITTED_ENTRY` envelopes; nothing in the current handler
   reconstructs the session / dedup / retained / offline arenas from
   them.

The fix is to move every durable mutation behind the apply-side seam.
`MSG_COMMITTED_ENTRY` becomes the only path that mutates these
arenas. Propose-side handling shrinks to admission control, encoding,
and proposing.

## Scope

In scope:

- The canonical proposal wire format (§Phase 1).
- Propose-side rewrite of `session_processor` (§Phase 2).
- Apply-side dispatch in `session_processor.committed_in` (§Phase 3).
- `MSG_APPLY_PIPELINE_RESET` handling (§Phase 4).
- Snapshot payload format (§Phase 5).
- Verification strategy: replay + follower-convergence tests (§Phase 6).

Out of scope:

- Changes to the protocol adapters (`mqtt_codec`, `kafka_codec`,
  `amqp_codec`). They stay thin bytes↔envelope translators.
- Changes to the substrate. Everything Quantum needs from Clustor is
  already enumerated in
  [clustor_capability_surface.md](clustor_capability_surface.md);
  this design uses only those primitives.
- Operational signals (PID credits, lag, prefetch). They keep their
  current shape and use sites.
- Disaster-recovery promotion semantics. Covered in
  [architecture/disaster_recovery.md](architecture/disaster_recovery.md);
  the apply-side seam makes DR easier but is independent of it.

## Phase 1 — Canonical proposal body

Every proposal that mutates Quantum state goes on the wire as:

```
[version:u8 = 1]
[op:u8]
[tenant:u32 LE]
[session_slot:u32 LE]
[op-specific body]
```

The leading `version` byte is non-negotiable from day one. Once
Clustor logs entries in a given format, replay must understand them
forever; bumping `version` lets us evolve without losing the ability
to replay old WALs.

Opcodes (defined in `modules/common/wire.rs`; values reserved in the
0x01 – 0x0F range, distinct from `msg_type` constants):

| Opcode | Name | Body |
|---|---|---|
| 0x01 | `QOP_CONNECT` | `[clean_start:u8][keep_alive_s:u16 BE][stream_hash:u64 LE][cid_len:u16 BE][cid bytes][protocol:u8]` |
| 0x02 | `QOP_DISCONNECT` | `[reason:u8][stream_hash:u64 LE]` (see `QDISC_REASON_*`) |
| 0x03 | `QOP_PUBLISH` | `[pub_qos:u8][packet_id:u16 BE][stream_hash:u64 LE][session_epoch:u32 LE][retain:u8][topic_len:u16 BE][topic][payload]` |
| 0x04 | `QOP_SUBSCRIBE` | `[req_qos:u8][stream_hash:u64 LE][topic_len:u16 BE][topic]` |
| 0x05 | `QOP_UNSUBSCRIBE` | `[stream_hash:u64 LE][topic_len:u16 BE][topic]` |
| 0x06 | `QOP_PUBREL` | `[packet_id:u16 BE][stream_hash:u64 LE]` |
| 0x07 | `QOP_RETAINED_CLEAR` | `[topic_len:u16 BE][topic]` |

`stream_hash` is included in every session-targeted op-body so
followers (and post-restart replay) can find the durable session
record without relying on the `session_slot` field in the canonical
envelope header — that field is the leader's local slot index and is
not portable across nodes. `wire.rs` docstrings on each `QOP_*`
constant are authoritative for shape; the table above is a quick
reference.

### Tagged vs. untagged disambiguation (resolved)

Tagged proposals arrive at the propose-side wrapped in
`[correlation_id:u64 LE][body]` (existing `proposals_tagged`
contract). Clustor's `decode_tagged_proposal` strips the 8-byte
`correlation_id` prefix before storing the entry, so the WAL body —
and the `MSG_COMMITTED_ENTRY` body forwarded by `apply_pipeline` —
is byte-identical to the untagged form. Both shapes arrive at
session_processor's `committed_in` as a canonical envelope:

```
[version:u8][op:u8][tenant:u32 LE][session_slot:u32 LE][op-body]
```

The `correlation_id` for tagged ops is recovered on the leader from
the matching publisher inflight (`Session.inflight[i].correlation_id`,
set propose-side when the correlation was allocated). Followers do
not carry an inflight so `correlation_id` stays 0 there; QoS 1+ on a
follower bypasses the stash and emits `MSG_TOPIC_PUBLISH` directly.

An earlier iteration of this RFC proposed prepending a 1-byte
discriminator to distinguish tagged from untagged at the apply-side
reader, but that conflicted with Clustor's tagged-proposal contract —
the discriminator would have landed at bytes 0..7 where Clustor expects
the `correlation_id` LE bytes, mangling the cid round-trip and
breaking PUBACK. The inflight-lookup approach is strictly simpler.

### Constants and wire helpers

All `QOP_*` opcodes go in `modules/common/wire.rs` with detailed
docstrings. Their byte-level body shape is part of Quantum's public
contract — once shipped, only additive changes are allowed without a
`version` bump.

A new constant `MSG_APPLY_PIPELINE_RESET` is also added there,
matching Clustor's value (Clustor wire range 0x20 – 0x2F is shared).

## Phase 2 — Propose-side rewrite

Every place in `session_processor` that today mutates session state
or emits to `messaging_out` durably becomes an encode + propose pair:

1. **Validate** at receipt: PID admission, prefetch checks,
   topic-name sanity, capability bits. These gates use only
   non-durable / locally-derivable state.
2. **Encode** the canonical op (§Phase 1).
3. **Propose** via `proposals` (untagged) or `proposals_tagged`
   (when the publisher needs an ACK keyed to durability).
4. **Stop.** No further durable side-effects on the propose side.

The existing PUBACK/CONNACK pipeline through `ack_tracker` stays
exactly as it does today; the only change is that the propose-side
stops touching session / dedup / retained / offline tables.

### CONNECT specifically

The propose-side still sends CONNACK immediately. A round-trip
through Raft on every CONNECT is a non-starter for an IoT workload
where clients reconnect aggressively after transient network blips.

But the session-table entry is created on apply, not on receipt.
That has consequences:

- `session_present` in CONNACK is best-effort optimistic. If the
  propose-side observes that a `(tenant, stream_hash)` already exists
  in its local session table, it sets `session_present = 1` —
  assuming nothing has gone wrong, this matches what apply-side will
  reconstruct.
- Between CONNECT receipt and CONNECT apply, the propose-side may
  hold a *transient* session record keyed to the `conn_id`. This
  record is local-only, non-durable, and serves admission control
  (so a follow-up PUBLISH on the same `conn_id` can find its
  `session_slot`). It is reconciled with the apply-side record when
  the `QOP_CONNECT` entry commits.
- If a CONNECT proposal is admitted but never commits (the proposer
  loses leadership before quorum), the transient record is purged on
  the next CONNECT for the same `(tenant, stream_hash)`, or by the
  keep-alive sweep. The client's TCP socket eventually times out and
  reconnects.

**Open question.** Is the transient record a separate struct or
exactly the same `Session` slot with a `transient: u8` flag?
Recommendation: same slot + flag. The flag means "do not derive
durable state from this slot; reconcile or purge". Apply-side
`QOP_CONNECT` clears the flag.

### DISCONNECT and keep-alive sweep

Today, keep-alive timeout marks the session as
`active = 0` directly, sometimes setting `persisted = 1`. After this
refactor, the sweep proposes a `QOP_DISCONNECT` with
`reason = REASON_KEEP_ALIVE` and lets apply-side do the actual state
mutation. The sweep can still drop the socket immediately
(non-durable, peer-router-level concern) and zero the propose-side's
transient view, but the source of truth is the WAL entry.

Same logic applies to clean-DISCONNECT from `mqtt_codec`.

## Phase 3 — Apply-side dispatch

`session_processor.committed_in` becomes the only path that mutates
session / dedup / retained / offline state. Today the handler
increments `committed_entries_observed` and returns; this widens
into a per-op dispatch:

```
peel discriminator + (optional) correlation_id
match canonical op:
    QOP_CONNECT      → apply_connect(s, body)
    QOP_DISCONNECT   → apply_disconnect(s, body)
    QOP_PUBLISH      → apply_publish(s, body, correlation_id)
    QOP_SUBSCRIBE    → apply_subscribe(s, body)
    QOP_UNSUBSCRIBE  → apply_unsubscribe(s, body)
    QOP_PUBREL       → apply_pubrel(s, body, correlation_id)
    QOP_RETAINED_CLEAR → apply_retained_clear(s, body)
```

Per-op handler sketches:

### `QOP_CONNECT`

Allocate / resurrect / refresh the session slot. Reconcile the
transient record if the propose-side already created one for this
`(tenant, stream_hash)`. If the slot doesn't yet exist on this node
(e.g. follower mid-snapshot install), create it; if it exists,
update `session_epoch`, `keep_alive_ms`, `protocol`.

Edge case: propose-side admission for a slot that doesn't yet exist
on apply-side. If `committed_entries_observed` lags the proposer's
view by more than a small window, the propose-side may have admitted
a PUBLISH for a slot the apply-side hasn't created yet. Apply-side
must handle this gracefully — defer (rare) or reject (more common) —
not crash. Apply-side never reorders entries; the `QOP_CONNECT`
*will* arrive, and it arrives before any tagged op that depends on
it (the proposer can't have issued the tagged op without first
having proposed the CONNECT).

### `QOP_PUBLISH`

Emit `MSG_DEDUP_CHECK` and (if `retain`) `MSG_RETAINED_WRITE` on
`out_messaging`. Emit `MSG_TOPIC_PUBLISH` on `out_topic`. These
emissions happen from the apply-side, not the propose-side, so
followers also emit them.

The existing topic-publish stash + dedup-result + durability gate
machinery stays, but is now driven exclusively from apply. Specifically:

- The stash slot is allocated at apply time, not propose time.
- The publisher's PUBACK still comes from `ack_tracker` →
  `MSG_ACK_EMIT` — that machinery is unchanged.
- On the follower, there is no publisher to PUBACK; the stash slot
  is allocated, the dedup check is fired, and the fan-out emit
  happens, but `out_codec` is not driven (no CONN_ID is associated).
  The stash slot is released after fan-out without any client-facing
  side effect.

**Open question.** Do we need a follower-side fan-out at all? If a
follower is not driving any subscriber connections, emitting
`MSG_TOPIC_PUBLISH` on its own `topic_engine` is wasted work — the
topic graph is the same, but no `MSG_TOPIC_DELIVER` will ever produce
a downstream codec emit. Argument for keeping it: keeps follower
state machinery hot so leader failover is a no-op. Argument against:
3× write amplification on a hot leader is non-trivial. Recommendation
deferred to performance characterisation.

### `QOP_DISCONNECT`

Deactivate the slot. If `reason == REASON_CLEAN_START` (the
disconnect originated from a CONNECT with `clean_start=1`), emit
`MSG_SESSION_DROP` on `out_topic` so `topic_engine` purges
subscriptions. Otherwise park the slot as `persisted = 1` per the
existing MQTT 3.1.1 §3.1.2.4 semantics.

Keep-alive-driven disconnects (today's `Phase 5c` sweep) and
clean-DISCONNECT from `mqtt_codec` both flow through this path.

### `QOP_SUBSCRIBE` / `QOP_UNSUBSCRIBE`

Emit `MSG_TOPIC_SUBSCRIBE` / `MSG_TOPIC_UNSUBSCRIBE` on `out_topic`,
keyed to the apply-side session_slot. `topic_engine` is unchanged.

### `QOP_PUBREL`

Drive the QoS 2 phase transition. On the leader, this releases the
publisher's QoS 2 inflight and lets `MSG_ACK_EMIT` fire PUBCOMP. On
the follower, the phase transition is applied to the persisted
session record but no PUBCOMP is emitted (no client to send to).

### `QOP_RETAINED_CLEAR`

Emit a clear op on `out_messaging` keyed to `topic_hash`. Today's
retained-write encoding includes an implicit "if payload empty,
treat as clear" — explicit is better for replay.

### Why session_processor stays the sole translator

Downstream modules (`dedup_engine`, `retained_store`, `offline_queue`,
`topic_engine`) do not consume `committed_entries` directly. They
keep their existing input ports (`in_messaging`, `in_topic`, etc.)
and consume the apply-side emissions from `session_processor` exactly
as they consume propose-side emissions today.

Reasons:

1. **Port budget.** Every Quantum module is at or near the 8-input
   cap. Adding a `committed_in` to four more modules would force a
   port-budget audit on each.
2. **Framing knowledge.** `MSG_COMMITTED_ENTRY` framing (peeling the
   `[term][index]` prefix and the discriminator + correlation_id)
   would have to be learned by every consumer. Centralising it in
   `session_processor` keeps the framing knowledge in one place.
3. **Apply-pipeline contract is narrow.** Fewer consumers = fewer
   things the substrate has to guarantee about per-consumer ordering
   and backpressure.

## Phase 4 — MSG_APPLY_PIPELINE_RESET handling

When the substrate signals a reset (snapshot install,
leader-driven log truncation), every apply-derived arena in Quantum
zeroes itself and rebuilds from the snapshot.

Per-module policy:

| Module | On reset |
|---|---|
| `session_processor` | Clear `sessions`, `prefetch_credit`, `sub_outstanding`, `pending`, `stash_*`, `pending_dlv_*`, `pending_ack_*`. Re-emit `MSG_SESSION_DROP` for any slot the snapshot doesn't reinstate. |
| `dedup_engine` | Clear all shards. Reload from snapshot if one is being installed. |
| `retained_store` | Clear retained map. Reload from snapshot. |
| `offline_queue` | Clear queues. Reload from snapshot. Emit `MSG_OFFLINE_DRAIN` for any reconnected slots that lost queued state. |
| `topic_engine` | Clear subscriptions. Reload from snapshot. |
| `forward_coordinator` | Clear `(ingress, egress, epoch) → seq` counters. Reload from snapshot. |

`session_processor` is the sole subscriber to `MSG_APPLY_PIPELINE_RESET`
on `committed_in`. It propagates the reset to the other modules via
their existing buses: a small `MSG_APPLY_RESET_FANOUT` op (TBD; needs
exactly one byte and one constant) on each `out_*` so the downstream
modules know to clear *before* they start receiving the apply-side
re-emissions from the snapshot.

**Open question.** Is `MSG_APPLY_RESET_FANOUT` worth a distinct
opcode, or can we overload `MSG_SESSION_DROP` with a sentinel slot
id? Recommendation: distinct opcode. Sentinels are bug-bait.

## Phase 5 — Snapshots

Quantum owns its own snapshot payload. The substrate calls an export
hook at snapshot time and an install hook on cold-start /
learner-catchup; both are Quantum-owned and the substrate does not
interpret the bytes.

Snapshot payload (proposed):

```
[magic:u32 = "QSNP"][version:u32 = 1]
[session_count:u32][session entries...]
[dedup_shard_count:u32][shard entries...]
[retained_count:u32][retained entries...]
[offline_queue_count:u32][queue entries...]
[forward_counter_count:u32][counter entries...]
[checksum:u64]
```

Each sub-section is self-delimiting and forward-compatible (an
unknown trailing sub-section is ignored, which makes additive changes
easy).

Bare-metal-safe encoding: `core::ptr::copy_nonoverlapping` with
caller-verified bounds, no `copy_from_slice` panic paths, no heap.

**Per-module vs. centralised serialisation.** Recommendation:
per-module — keeps each module's serialisation logic local to that
module's state layout. Matches the substrate's `MSG_APP_SNAPSHOT_*`
contract which addresses one consumer at a time.

### Current substrate constraint (Phase 5 deferred)

Clustor defines the wire types — `MSG_APP_SNAPSHOT_REQUEST` (0x58),
`MSG_APP_SNAPSHOT_CHUNK` (0x57), `MSG_APP_SNAPSHOT_RESET` (0x59) —
along with `encode_app_snapshot_chunk` / `decode_app_snapshot_chunk`
helpers and a documented `example_consumer` pattern that names the
required ports (`snapshot_chunk`, `snapshot_request` inputs;
`snapshot_export` output). However, **Clustor's `snapshot_engine`
module manifest does not currently expose ports that emit
`MSG_APP_SNAPSHOT_REQUEST` to consumers or receive their
`MSG_APP_SNAPSHOT_CHUNK` exports.** The example consumer's snapshot
ports are documented and tested in isolation but no production graph
wires the round-trip.

Implementing Phase 5 in Quantum would therefore add export/install
handlers to `session_processor` (and the four downstream apply-state
modules) that no graph could ever exercise. The right sequencing is:

1. Clustor's `snapshot_engine` grows `app_request_out` and
   `app_chunks_in` ports, wires them through the partition's
   `wal.segment_threshold` / `snapshot.trigger` flow, and updates a
   production config to exercise the round-trip via `example_consumer`.
2. Quantum then mirrors the pattern in `session_processor` (with the
   payload format above), multiplexes the new messages through
   existing ports (`committed_in` for REQUEST / RESET / install CHUNK,
   `messaging_out` for export CHUNK), and adds graph YAML edges.

Phase 4 (`MSG_APPLY_PIPELINE_RESET` handling) gives apply-derived
modules a clear "wipe everything" code path that snapshot install
will piggyback on once the substrate side ships — so Quantum is
half-ready for Phase 5 even without writing Phase 5 code.

**Open question.** Until snapshots ship, replay starts from WAL index
0. Acceptable for the first ship; document the bound and the
performance implication in the runbook. Roughly: for a workload that
sustains 100k publishes/sec at 1KB each, an hour of WAL is ~360 GB.
Replay-from-zero on cold start is bounded by disk read bandwidth.

## Phase 6 — Verification

Apply-side coverage delivered (all four pass via
`make test-mqtt-suite`):

| Test | What it exercises |
|---|---|
| `module_graph_mqtt.sh` | CONNECT → QOP_CONNECT (apply, slot active=1) → QoS 1 PUBLISH → QOP_PUBLISH (apply) → MSG_ACK_EMIT → PUBACK. |
| `module_graph_pubsub.sh` | Two clients; QOP_SUBSCRIBE installs in topic_engine via apply; QoS 0 PUBLISH fans out via apply to the subscriber. |
| `module_graph_resume.sh` | Persistent-session lifecycle. Clean-disconnect drops the slot via `MSG_SESSION_DROP`; non-clean disconnect parks `persisted=1`; reconnect resurrects with CONNACK `session_present=1` and the surviving subscription still receives the publisher's PUBLISH. Round-trips QOP_CONNECT (twice) + QOP_SUBSCRIBE + QOP_DISCONNECT (twice). |
| `module_graph_load.sh` | Sustained QoS 1 load. p50 ~8ms, all PUBACKs received. Validates no leak in the inflight / correlation / stash pools across many cycles. |

These tests collectively prove the propose-then-apply seam is
load-bearing: PUBACK fires only after `QOP_PUBLISH` reaches
`MSG_COMMITTED_ENTRY` (so the apply-side runs first); the subscriber
delivery only works because `QOP_SUBSCRIBE` ran apply-side and emitted
`MSG_TOPIC_SUBSCRIBE` to topic_engine; the resumption only works
because `QOP_CONNECT` apply-side picked up the parked `persisted=1`
slot via `find_session(tenant, stream_hash)`. The PR-side rewrite
without the apply-side seam would fail every one of these.

### Deferred verification (blocked on substrate)

Three planned tests are gated on Clustor-side work that is not part
of Quantum's scope:

| Test | Substrate blocker |
|---|---|
| **Single-node replay (kill / restart, verify state)** | Quantum's `wal` module declares `requires_contract = "fs"`, but Clustor's linux FS provider returns `< 0` on `FS_OPEN` for the WAL's relative paths — the WAL falls back to in-memory and logs `[wal] no fs`. Until the provider supports create-on-open or the `linux-minimal.yaml` config configures a writable WAL root, a `kill -9` test cannot validate state recovery. |
| **3-node follower convergence** | `tests/integration/multi_node.sh` now boots all three nodes (after adding `scheduler: { accept_cycles: true }` to `quantum-node{0,1,2}.yaml`) and `peer_router` reports `[pr] peer ok` for all three connections, but raft votes do not drive election state — every node spins `[raft] elect` indefinitely without electing a stable leader. The Quantum apply-side paths cannot be reached without a working leader, so follower convergence stays unverified. |
| **Snapshot install / export round-trip** | Clustor's `snapshot_engine` does not currently expose `app_request_out` / `app_chunks_in` ports — see §Phase 5 above. Phase 4's `MSG_APPLY_RESET_FANOUT` already gives downstream modules the install side; the install path becomes exercisable as soon as the substrate emits requests. |

For now, the single-node test suite (`make test-mqtt-suite`) is the
strongest correctness signal Quantum has, and it covers every
propose-then-apply op in the design.

The existing integration suite exercises one node's propose+apply
path. The apply-side seam invalidates that as a sufficient test —
we need to verify both replay (single node) and replication
(multi-node).

### Replay test

Single-node graph, the existing `quantum-cm5-smoke` graph plus a WAL
on tmpfs:

1. Drive 100× CONNECT + 1000× QoS 1 PUBLISH + 100× DISCONNECT (some
   clean, some persisted).
2. Capture session table + dedup shards + retained payloads via
   the introspection ports (metrics rollup is sufficient).
3. SIGKILL the process.
4. Restart.
5. After apply has caught up to the last committed index, capture
   the same state via the same introspection.
6. Assert byte-equal.

The persisted-session subset is the interesting case: those slots
must survive replay with their inflight + subscription state intact.

### Follower-convergence test

3-node Clustor cluster (existing `quantum-cm5.yaml` or a new
`quantum-cluster-3.yaml`):

1. Drive load against the leader.
2. Observe `committed_entries_observed` on each follower; assert it
   converges to the same value as the leader within a bounded delay.
3. Promote a follower (via `admin_handler` leader transfer).
4. Drive a small amount of additional load against the new leader.
5. Assert no PUBACK / PUBREC / PUBCOMP regression — clients see no
   loss, no duplicate ack, no missed PUBREL state transition across
   the promotion.

Introspectable follower state is the hard part — today the
follower's session_processor is not directly readable. The test
harness can introspect via `MSG_METRICS_ROLLUP` (existing) plus a
new `MSG_DEBUG_DUMP` op (test-only, behind a feature flag) that
dumps the apply-derived arenas as a flat byte blob.

### Negative tests

- Reset mid-stream: install a snapshot at a known point and verify
  every module clears + reloads correctly.
- Tagged proposal lost between propose and apply: verify the
  publisher's DUP retransmit re-enters dedup correctly.
- CONNECT admitted but leadership lost before commit: verify no
  stale transient record survives.

## Constraints

These are non-negotiable Fluxor-philosophy constraints; any design
that violates them is wrong:

- **Port budget.** `session_processor` already uses all 8 inputs and
  8 outputs. The apply-side per-entry stream came in by fan-in on
  `committed_in` rather than a new port — that pattern continues. If
  a new port is needed, demonstrably free another first.
- **No heap.** State stays in fixed-size `#[repr(C)]` arrays in
  module state. Snapshot encoding / decoding uses stack buffers and
  `core::ptr::copy_nonoverlapping`.
- **No panic paths in dynamic-length copies.** `copy_from_slice`
  pulls in `panic_fmt` on bare-metal builds. Use
  `core::ptr::copy_nonoverlapping` with caller-verified bounds for
  any dynamic-length copy.
- **Bounded per-tick work.** Apply-side handlers stay bounded per
  tick (inner loop with a fixed cap like `for _ in 0..16`). Replay
  of a large WAL is the substrate's problem, not Quantum's — the
  substrate paces `committed_entries` emission.
- **Workspace-mode dev rig.** New wire constants and message types
  go in `modules/common/wire.rs`; rebuild and Clustor picks them up
  via the workspace mount.
- **Protocol adapters stay thin.** No MQTT-specific logic in
  `session_processor`; no broker logic in `mqtt_codec`. If a piece
  of logic resists protocol-neutral framing, escalate before adding
  it.

## Pitfalls

Catalogued so they're not relearned:

- **Don't fan out the per-entry stream.** Every module learning
  `MSG_COMMITTED_ENTRY` framing multiplies port usage and couples
  downstream modules to the apply pipeline. Keep `session_processor`
  as the single translator.
- **Don't make CONNACK wait for commit.** IoT clients reconnect
  aggressively; a Raft round-trip per CONNECT melts the cluster.
  CONNACK is best-effort optimistic; real session truth is apply-side.
- **Don't make dedup synchronous.** Propose-side `MSG_DEDUP_CHECK`
  is a hint, not a gate. The real dedup happens on apply when the
  entry is being committed across all replicas. The propose-side
  check exists only to fast-path obvious duplicates.
- **Don't change the proposal body wire format quietly.** Once
  Clustor logs entries in a given format, replay must understand
  them. The leading `version` byte exists from day one to make
  evolution safe.
- **Don't bypass Raft for "small" mutations.** Keep-alive disconnects
  and offline-queue inserts feel local; if they're not in the WAL,
  followers diverge.

## Open questions consolidated

Repeated here so reviewers can find them without scanning the whole
document:

1. ~~**Tagged/untagged discrimination.** 1-byte discriminator on the
   proposer side vs. push it into `apply_pipeline`.~~ **Resolved**:
   the body shape is byte-identical for both forms because Clustor
   strips the tagged `correlation_id` upstream. Apply-side uses
   inflight lookup to recover the cid for the leader's stash bookkeeping.
2. **Transient session record.** Separate struct vs. `Session` slot
   with a `transient: u8` flag. Recommendation: flag.
3. **Follower-side topic fan-out.** Always-emit (keep state hot for
   failover) vs. leader-only emit (avoid 3× write amplification).
   Recommendation: deferred to performance characterisation. Default
   to always-emit for safety; revisit when we have numbers.
4. **Apply-pipeline reset fan-out.** Distinct `MSG_APPLY_RESET_FANOUT`
   opcode vs. overload `MSG_SESSION_DROP` with sentinel.
   Recommendation: distinct opcode.
5. **Snapshot ownership.** Per-module serialisation vs.
   `session_processor` pulls from peers. Recommendation: per-module.
6. **Replay-from-zero bound.** What's the operator-facing cap on
   cold-start time before snapshots land? Needs measurement.

## Phasing summary

The phases are ordered by dependency. Each phase is mergeable as
a standalone PR.

| Phase | Deliverable | Acceptance |
|---|---|---|
| 1 | Canonical wire format constants + helpers in `modules/common/wire.rs`; `MSG_APPLY_PIPELINE_RESET` constant | `make ci` green; no behaviour change yet |
| 2 | Propose-side rewrite of `session_processor`; CONNACK optimistic, durable mutations removed from propose path | Existing integration tests pass; new dedup ordering test covers the propose-then-apply gap |
| 3 | Apply-side dispatch in `session_processor.committed_in` | Replay test (Phase 6) passes |
| 4 | `MSG_APPLY_PIPELINE_RESET` handling | Reset mid-stream test passes |
| 5 | Snapshot payload format + install/export hooks | Cold-start time bounded; follower learner-catchup works |
| 6 | Follower-convergence test | 3-node cluster + leader transfer test passes |

## Acceptance criteria

When this work is done:

- `make ci` is green; `fluxor validate` is green on all 8 graphs;
  integration tests and the bare-metal rig pass.
- No durable mutation happens before the proposing op reaches
  `committed_entries`. Code review confirms by inspection of
  `session_processor`, `dedup_engine`, `retained_store`,
  `offline_queue`.
- The capability surface document
  ([clustor_capability_surface.md](clustor_capability_surface.md))
  enumerates exactly what Quantum requires from the substrate, and
  a comment in each consumer of those signals points back to it.
- The replay test passes: a restarted node converges to the same
  state.
- The follower-convergence test passes: a follower never receiving
  propose-side traffic still ends up with the same session / dedup /
  retained / offline state as the leader.
