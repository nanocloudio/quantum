# Session Processor Decomposition

`session_processor` is the one Quantum module that is not yet a composite
in the sense [standards/fluxor-modules.md](../../../standards/fluxor-modules.md)
§8 means: 5,906 lines in a single `mod.rs`, one `ModuleState` with 64
fields, and free functions that reach any field they need. Rules 1
(one state struct per component), 2 (no cross-component field access)
and 3 (re-extractability) are unmet. Rule 4 is closer than it looks —
the step function already runs a documented Phase 1…Phase 6 sequence.

This document specifies the target decomposition and the order to reach
it. It exists because the work is larger than one sitting and the
sequencing matters: done in the wrong order it destabilises the apply
path, which is the correctness-critical part of the broker.

## Why decompose

Three payoffs, in descending order of value:

1. **Protocol variants.** `protocol` already ships `full`/`mqtt`/`kafka`/
   `amqp` artifacts (the kafka-only codec is 13 KB against 61 KB for the
   full set). The session module carries the matching per-protocol state
   and is the larger half of the win — but only once the state is
   separable along a seam that a `#[cfg]` can cut.
2. **Multi-partition.** [partitioning.md](partitioning.md) states the
   apply loop is per-PRG, and `quantum-linux-2p.yaml` runs two
   consensus/durability pairs. But `module_new` takes no params
   (`_params`, `_params_len` are both ignored) and tenant is hardcoded
   (`let tenant: TenantId = 0;`). Per-instance state keyed by
   `partition_id` is §8 rule 6, and the documented story is not real
   until it lands.
3. **Observability granularity.** §8 rule 8 wants per-component metric
   identity. The other composites now stamp a `metric_id`; this module
   emits one undifferentiated set.

## The seam: state ownership, not protocol

The obvious split — one component per protocol — **does not work**, and
this is the single most important constraint in this document.

Two state structures are explicitly shared between Kafka and AMQP:

- `KafkaInflight` (`mod.rs:327`, "Durable-publish inflight (generic;
  shared by Kafka + AMQP)"), with `KIN_PROTO_KAFKA` / `KIN_PROTO_AMQP`
  discriminators. AMQP publish allocates from the same table as Kafka
  produce.
- `KPart` / `kstore` (`mod.rs:608`, "Apply-side message store (Kafka
  Fetch / AMQP Basic.Get)"). AMQP reads it for Basic.Get, Cancel and the
  delivery pump; Kafka reads it for Fetch and ListOffsets.

A `kafka` component and an `amqp` component would both need direct
access to these arrays — a rule 2 violation — and neither could be
lifted back out without taking the other's state with it, failing rule
3. It also kills the variant story it was supposed to enable: you cannot
compile out `kafka` without removing `kstore`, which AMQP needs.

Split by **who owns the state** instead:

| component | owns | source |
|---|---|---|
| `sessions` | `Session[1024]`, prefetch credit, subscription outstanding, per-session QoS inflight, keep-alive / expiry / will sweeps, `try_deliver` | `:201-320`, `:3098-3248`, `:5732-5886` |
| `store` | `KPart kstore[24]` + `KafkaInflight[256]` — the protocol-neutral durable-publish and log primitive | `:327-455`, `:608-675`, `:1424-1632` |
| `consumers` | `KGroup` / `KOffset` / `AmqpConsumer` — group membership, committed offsets, push consumers | `:457-607`, `:2110-2687`, `:2729-2951` |
| `correlate` | `PendingCorrelation[1024]`, the commit-gating stash, the pending delivery/ack slots | `:676-704`, `:735-745`, `:2973-3096` |


Protocol framing (`handle_kafka_*`, `handle_amqp_*`) becomes a set of
thin **adapters** over `store` and `consumers` — not components. The
`#[cfg]` seam then gates *adapters*, which is clean, rather than
components, which is not.

## Where the coupling actually is, measured

Counting direct field touches by enclosing function (post-phase-1, so
the `store` column reads entry-point calls rather than field access):

```
store                              correlate
  13  module_step                    34  module_step
   6  handle_kafka_produce            5  take_correlation
   6  apply_kafka_produce             4  module_new
   6  handle_amqp_get                 4  apply_reset
   5  amqp_delivery_pump              2  allocate_correlation
   4  handle_kafka_fetch              1  allocate_pending_delivery
```

Both components concentrate in **`module_step`** — the dispatch
function — not in the apply path. That is the opposite of what it looks
like from a casual read, and it makes the work far more tractable than
feared: the correlate state is dispatch-owned (the commit-gating stash
is drained by the step loop), and the store's callers are the protocol
handlers, which are exactly the adapters this design wants them to be.

`apply_qop_publish` — the 168-line function on the publish apply path —
touches **neither** component. An earlier draft of this document claimed
it held 18 store and 35 correlate touches and called that "the true cost
of this work". That was wrong: the measurement attributed impl-block
methods and `pub extern "C" fn module_step` to the nearest preceding
top-level `fn`. Recorded here because the erroneous figure was the
stated reason for sequencing phases 1 and 2 apart.

The phases are still worth keeping separate — a regression in the
commit-gating stash should be bisectable from one in the message store —
but the justification is ordinary caution, not a hot-spot that both
phases must fight over.

A second complication: `in_buf` and `out_buf` are shared scratch used by
every handler. Components cannot share them (rule 1). Either each
component gets its own scratch — costing state — or the dispatch table
owns the buffer and passes borrowed payloads, as the `protocol`,
`messaging` and `flow` composites already do. Prefer the latter; it is
the pattern the rest of the tree now follows.

## Order of work

Each phase ends green on the integration suites; none is started before
the previous is verified.

1. **`store`** — DONE (2026-07-26). `store.rs` holds `KPart`,
   `KafkaInflight` and the ring/inflight logic behind 28 entry points
   (`push`, `fetch_into`, `find_or_create`, `inflight_alloc`/`_open`/
   `_get`/`_valid`/`_free`/`_complete_part`/`_expired`, the log
   accessors, `copy_entry_into`, `stamp_base_offset`, `reset_parts`).
   `mod.rs` 5906 → 5601 lines. **Rule 2 is met: `mod.rs` contains zero
   direct reads or writes of the component's state** — every
   interaction crosses an entry point. Gate suites green:
   `kafka_produce_consume`, `kafka_multipart`, `kafka_group_smoke`,
   `amqp_smoke`, `amqp_ack`, `amqp_consume_smoke`, plus the MQTT,
   pubsub, resume and load suites.

   Two entry points exist because the boundary revealed a leak rather
   than to satisfy the rule mechanically:

   - `stamp_base_offset` — the Kafka apply path was reaching into the
     ring to write a batch's base offset back after `push`. Assigning
     that offset is the store's own business.
   - `next_raw_entry` — AMQP Basic.Get and the delivery pump each
     carried their own copy of a 20-line ring walk over `tail`/`used`.
     One entry point now serves both; Basic.Get uses the
     `remaining_after` count as queue depth, the pump ignores it.

   Also note `copy_entry_into` takes a destination rather than lending a
   slice: a borrowed-slice accessor makes the caller's
   `copy_from_slice` emit a length-mismatch panic path the bare-metal
   PIC link cannot resolve. Any future accessor that hands out a
   variable-length slice will hit the same wall.

   Phase 1 did **not** restructure `apply_qop_publish`, and does not
   need to: that function touches neither component (see "Where the
   coupling actually is").
2. **`correlate`** — DONE (2026-07-26). `correlate.rs` holds
   `PendingCorrelation`, the commit-gating delivery stash and the
   pending-ack slots behind 18 entry points (`allocate`, `take`,
   `expire`, `note_dropped`, `dlv_alloc`/`_store`/`_park`/`_copy_out`/
   `_free`/`_is_active`/`_len`, `ack_stash`/`_get`/`_free`/`_is_active`,
   `init`, `reset`, `dropped`). **Rule 2 met: zero direct access from
   `mod.rs`.** Gate suites green: the full MQTT suite (incl. QoS-1 and
   QoS-2), pubsub, resume, load, and all six Kafka/AMQP suites.

   The correlation-expiry sweep, which `module_step` open-coded, is now
   `correlate::expire`. `dlv_park` exists because three separate call
   sites had each written the same reserve-then-store-else-account-drop
   sequence by hand.
3. **`consumers`** — DONE (2026-07-26, second attempt). `consumers.rs`
   holds `KGroup`/`KGroupMember`/`KOffset`/`AmqpConsumer` behind 33
   entry points. **Rule 2 met: zero direct access from `mod.rs`**, and
   the record types stay *private* to the component — callers cannot
   obtain a `&mut` to one. Gate suites green: `kafka_group_smoke`,
   `amqp_consume_smoke`, `amqp_ack`, `amqp_smoke`, plus the Kafka,
   MQTT, resume, pubsub and load suites.

   The first attempt was reverted at the visibility wall (17 uses of
   `KGroup::members`, 9 of `generation`, mostly via `&mut` bindings).
   The fix was to stop mirroring fields and write the operations
   instead, with reads returning `Copy` views:

   - `member_join` / `member_leave` / `release_conn` own the generation
     bump and the empty-group reap, so no caller can change membership
     without them. `handle_conn_disconnect` collapsed from two
     hand-written loops to one `release_conn` call.
   - `next_member_id` owns the `qm-<seq>` allocation and its sequence
     counter.
   - `consumer_ack` owns the delivery-tag arithmetic — the contiguous
     `(last_acked, next_dtag)` range that makes single acks, `multiple`
     acks and the `dt == 0` "ack everything" idiom exact, including
     the out-of-order hole a later `multiple` closes.
   - `consumer_in_credit` / `consumer_delivered` own prefetch credit,
     so the delivery pump can no longer skew `unacked`.
   - reads go through `member_id_into` / `member_meta_into` /
     `member_assignment_into` / `group_proto_into` / `consumer_view` /
     `offset_at`, which copy out rather than lend.

   This is why the phase was worth doing rather than relocating: the
   invariants are now enforced where the state lives, not by every
   caller remembering to bump a counter.


4. **`sessions`** — DONE (2026-07-26). `sessions.rs` holds
   `Session[MAX_SESSIONS]` and the per-session QoS `Inflight` sub-table
   behind 45 entry points. **Rule 2 met: zero direct access from
   `mod.rs`**, and both records are private — only the `SessionView` /
   `WillView` / `InflightView` / `ConnectParams` shapes cross the
   boundary. All 12 suites green.

   152 sites, the largest phase. It was converted **incrementally, with
   the tree compiling and testable at every step**, precisely because a
   conversion this size cannot be safely done as one edit:

   1. relocate the records and the array (green, not yet compliant)
   2. lookups + `inflight_find` → 152 → 115
   3. will group → 115 → 98
   4. QoS inflight group → 98 → 63
   5. identity / state / sweep predicates → 63 → 40
   6. connect + disconnect lifecycle → 40 → 9
   7. the multi-line `find_inflight_dir` pattern → 9 → 0
   8. drop the relocation-step `pub` from the records

   Anyone continuing this work should use the same ladder. The
   alternative — one edit, then debug — leaves a non-compiling tree with
   no way back.

   Operations that now own an invariant rather than exposing a field:
   `commit_connect` (bumps the session epoch, the fencing token every
   in-flight round-trip validates against), `close` (parks vs drops, and
   stamps the disconnect time the expiry sweep reads), `open_transient`
   / `rebind` / `mark_transient` (the transient→active handshake between
   propose and apply), `next_sub_packet_id` (wraps to 1, since MQTT
   reserves id 0), `keepalive_expired` (the 1.5× grace of MQTT 3.1.1
   §3.1.2.10), `session_expired`, and the will set/clear/deadline group
   (a CONNECT always *replaces* the prior Will, including with nothing —
   §3.1.2.5).


5. **`apply`** — RESOLVED (2026-07-26): **there is no `apply`
   component, and there should not be one.**

   The original plan listed `apply` as the fifth component, owning
   `apply_committed_op`, the `apply_qop_*` arms and `apply_reset`. Once
   the other four were extracted it became clear those are *functions
   with no state of their own* — they read and mutate the session,
   store, correlate and consumers tables and nothing else. §8 rule 1
   says the module's state is a struct of component state structs; a
   component with no state struct is not a component. The apply
   dispatcher is the module's own ordered dispatch (rule 4), which is
   where it already lives.

   What phase 5 turned out to be was **finishing the placement of state
   that belonged to components already built**:

   - the commit-gating stash (`stash_*`, 30 sites) → `correlate`. The
     spec always said correlate owned it; phase 2 had moved only the
     delivery stash. It gates a publish on dedup AND durability, which
     arrive independently and in either order.
   - `prefetch_credit` / `sub_outstanding` (25 sites) → `sessions`.
     Per-session arrays cleared on exactly the same transitions as the
     session record, so they belong beside it. `note_delivery` /
     `note_ack` / `clear_flow` now own the window arithmetic.
   - three dead counter fields (`kstore_evictions`, `kstore_full`,
     `kafka_batches_applied`) left behind when phase 1 moved their
     counters into `store`.

   `ModuleState` went 64 → 43 fields and is now exactly the shape rule 1
   describes: channels, params, the four component states, module-level
   telemetry counters, and the shared read/write buffers the dispatch
   table owns.


6. **Rule 6 + variants — DONE (2026-07-26).**

   *What was wrong:* the plan called for a `partition_id` param. That
   turned out to be the wrong diagnosis. The partition-scoped state is
   already keyed per instance — `store`'s log is keyed by
   `(topic, partition)`, `consumers`' offsets by
   `(group, topic, partition)` — and `quantum-linux-2p.yaml` runs one
   `session_processor` across two partitions deliberately, so the module
   does not want a partition identity of its own.

   The actual rule-6 violation was **`let tenant: TenantId = 0;`
   repeated at 9 sites** — a module constant standing in for
   per-instance state. Six now read the tenant from the session
   (`sessions::tenant`). The other three are marked `TENANCY GAP` in
   the source because no session is reachable there: CONNECT arrival
   (no session yet), and `finalise_stash` / the PROPOSAL_ASSIGNED
   handler (both keyed by correlation, not session).

   Those three cannot be closed inside this module — they need
   `control_plane.tenant_records` wired and a decision about how a
   CONNECT maps to a tenant. Recorded in
   [multi_tenancy.md](multi_tenancy.md) "Implementation status", which
   also states plainly that every shipped graph currently produces
   exactly one tenant.

   *Variants — DONE (2026-07-26).* `session_processor` now ships
   `full` (default) / `mqtt` / `kafka` / `amqp`, matching `protocol`'s.
   The per-protocol handlers, their apply arms and the durable-publish
   resolution are `#[cfg]`-gated; MQTT is the base every variant
   carries, declared with a marker feature (clustor's `durability`
   `disk` pattern) since the tool requires a variant to name a feature
   set.

   Measured artifacts, and the pair a deployment actually loads:

   | variant | protocol | session | total |
   |---|---:|---:|---:|
   | full | 52,180 | 76,616 | **128,796** |
   | mqtt | 22,044 | 38,744 | **60,788** |
   | kafka | 13,300 | 67,120 | **80,420** |
   | amqp | 24,860 | 50,088 | **74,948** |

   An MQTT-only deployment loads **53% less code**. Verified
   functionally, not just by build: `quantum-linux-2p.yaml` (both
   modules at `variant: mqtt`) boots and passes the pubsub round-trip
   with Kafka and AMQP compiled out.

   Two things the gating exposed, both now fixed in place rather than
   worked around:

   - The **durable-publish resolution** in `module_step` dispatched on
     `inflight_proto` to pick a response shape. That is per-protocol
     business, so each arm is gated with its protocol; a variant
     carrying neither compiles the whole resolution away, which is
     correct because nothing can populate the inflight table in that
     build.
   - The **inflight expiry sweep** emitted an AMQP Nack inline. Kafka
     producers retry on their own request timeout while AMQP
     confirm-mode publishers wait indefinitely — so the Nack is
     amqp-only, and saying so in a `#[cfg]` documents the asymmetry
     that was previously implicit.

## Non-goals

- No change to the module's graph surface. `session_processor` stays one
  graph module with the ports it has; this is entirely internal. The
  only config-visible addition is the `variant:` selector from phase 6.
- No behaviour change. Every phase is a refactor gated on the existing
  suites; anything that needs a semantic decision (for example whether
  `kstore` retention should differ per protocol) is out of scope and
  raised separately.
