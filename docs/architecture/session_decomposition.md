# Session Processor Decomposition

Source: `modules/app/session_processor/`

`session_processor` is one graph module — one scheduler entity, one
arena, the ports its manifest declares — decomposed internally into
five components, each a source file owning its own state behind an
entry-point boundary. `mod.rs` holds the dispatch: the phase-ordered
step function, the protocol handlers, and the apply arms. This
document states the component split, the seam it follows, and the
rules it enforces.

## The seam: state ownership, not protocol

The split is **not** one component per protocol, and that constraint
is load-bearing. Two state structures are deliberately shared between
Kafka and AMQP:

- the durable-publish inflight table, with per-entry protocol
  discriminators — AMQP publish allocates from the same table as
  Kafka produce;
- the apply-side message store (`KPart`), which AMQP reads for
  Basic.Get and the delivery pump and Kafka reads for Fetch and
  ListOffsets.

A `kafka` component and an `amqp` component would both need direct
access to those tables, and neither could be lifted out without
taking the other's state with it. It would also break the variant
story: kafka could not be compiled out without removing the store
that AMQP needs.

Instead the components own state, and protocol framing
(`handle_kafka_*`, `handle_amqp_*`) is a set of thin adapters over
them. The variant gates (`#[cfg]`) cut at the adapters, which is
clean, rather than at components, which would not be.

## The components

| Component | Source | Owns |
|---|---|---|
| `sessions` | `sessions.rs` | The session arena and per-session QoS inflight sub-table, keep-alive / expiry / Will sweeps, prefetch credit and subscription-outstanding windows. Record types are private; callers see copy-out views (`SessionView`, `WillView`, `InflightView`). |
| `store` | `store.rs` | `KPart` message logs and the protocol-neutral durable-publish inflight — the primitive behind Kafka produce/fetch and AMQP publish/Basic.Get. |
| `consumers` | `consumers.rs` | Consumer-group membership, committed offsets, and AMQP push consumers. Record types are private; membership, generation bumps, delivery-tag arithmetic and prefetch credit are operations, not exposed fields. |
| `correlate` | `correlate.rs` | Correlation tables, the commit-gating stash (a publish is released only once dedup verdict and durability proof have both arrived, in either order), and the pending delivery/ack slots. |
| `worker` | `worker.rs` | The SessionCtrlV1 session-worker role: attachments by `conn_id`, delivery cursors, drain-to-quiescent, the opaque export/import over `sessions::export_record`, resume and return-to-service, and the frozen/settling states a moved session passes through. See [session_continuity.md](session_continuity.md). |

Every interaction with a component's state crosses one of its entry
points; `mod.rs` contains no direct reads or writes of component
state. The invariants live where the state lives: for example
`sessions::commit_connect` owns the session-epoch bump (the fencing
token every in-flight round-trip validates against),
`sessions::next_sub_packet_id` wraps to 1 because MQTT reserves
packet id 0, `sessions::keepalive_expired` applies the 1.5× grace of
MQTT 3.1.1, and `consumers::consumer_ack` owns the contiguous
delivery-tag range that makes single acks, `multiple` acks and the
`dt == 0` "ack everything" idiom exact.

Two boundary conventions matter for anyone extending a component:

- Accessors copy out rather than lending slices: a borrowed
  variable-length slice makes the caller's `copy_from_slice` emit a
  length-mismatch panic path the bare-metal PIC link cannot resolve.
- The dispatch table owns the shared read/write scratch buffers and
  passes borrowed payloads to handlers — the same pattern the
  `protocol`, `messaging` and `flow` composites follow — so no
  component needs its own scratch arena.

## The apply dispatcher is not a component

The apply arms (`apply_qop_*`, the committed-entry dispatcher, reset
handling) are functions with no state of their own: they read and
mutate the four component tables and nothing else. A component with
no state struct is not a component; the apply dispatcher is the
module's own ordered dispatch, and it lives in `mod.rs`. The seam it
implements is specified in [apply_path.md](apply_path.md).

## Module state shape

`ModuleState` is exactly: channel handles, params, the four component
states, module-level telemetry counters, and the shared read/write
buffers the dispatch table owns.

## Tenancy

The tenant is read from the session wherever a session is reachable.
Three sites are marked `TENANCY GAP` in the source, where no session
exists to read from (CONNECT arrival, and the two correlation-keyed
handlers); they cannot be closed inside this module — see
[multi_tenancy.md](multi_tenancy.md) for the status and what closing
them requires.

## Variants

`session_processor` ships `full` (default) / `mqtt` / `kafka` /
`amqp` variants, matching `protocol`'s. The per-protocol handlers,
their apply arms and the durable-publish resolution are
`#[cfg]`-gated; MQTT is the base every variant carries. Two
asymmetries the gating makes explicit:

- The durable-publish resolution dispatches on the inflight's
  protocol to pick a response shape; a variant carrying neither Kafka
  nor AMQP compiles the whole resolution away, which is correct
  because nothing can populate the inflight table in that build.
- The inflight expiry sweep emits a Nack only for AMQP: Kafka
  producers retry on their own request timeout, while AMQP
  confirm-mode publishers wait indefinitely.

The graph surface is unchanged across variants; the only
config-visible knob is the `variant:` selector.
