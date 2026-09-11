# Session continuity: `protocol` as the anchor, `session_processor` as the worker

Source: `modules/app/protocol/anchor.rs` (the anchor),
`modules/app/session_processor/worker.rs` (the worker),
`modules/common/cores/session_identity_core.rs` (the identity rule),
`examples/linux/handoff.yaml` (the pair that moves),
`tests/integration/session_handoff.sh` (the gates).

Fluxor owns session continuity — the continuity classes, the
SessionCtrlV1 control plane, the anchor / worker / directory roles, the
opaque handoff and its delivery cursors
(`../../../fluxor/docs/architecture/protocol_surfaces.md`). Quantum is
the textbook case for it: a broker front door, where clients connect
once and stay, subscriptions and QoS state are designed to outlive a
connection, and a maintenance window that closes connections is visible
to every client at once. This document describes how the front door is
a declared Fluxor **transport anchor** and the session processor the
**session worker** behind it, so the worker can be replaced during
maintenance while the client sees a bounded pause and nothing else.

## Why the anchor is a separate role

Continuity needs two halves, and they belong in different modules.

The durable half is the session state: `session_processor` is the Raft
apply callback, so a session record is quorum-committed, fenced by its
own counter and durable before it is acknowledged — exactly what a
worker must guarantee if another worker is to pick the session up.

The other half is the client's attachment, and it cannot live in the
worker: a connection keyed by `conn_id` inside the module being
replaced disappears with it. So the attachment is the anchor's, in
`protocol` — the module that already owns the socket, the codec state
and the response multiplexing, none of which moves. The worker behind
it can then be replaced while the connection stays open.

## Two identities, two counters

Quantum keys a session by `tenant:client_id` and routes responses by
`conn_id`; Fluxor names an attachment by a 16-byte `session_id`. The
mapping is single-valued in both directions and lives in
`session_identity_core.rs`:

    session_id = [anchor_id:8][conn_id:2 BE][attach_generation:4 BE][0:2]

The anchor mints it at the connection's first classified bytes.
`attach_generation` is an anchor-wide counter, so an id is never reused
even though a `conn_id` is; a worker recovers the connection an
envelope will name from the id itself, which `CMD_SC_ATTACH` does not
otherwise carry. The worker's session table binds `conn_id` to the MQTT
session at CONNECT.

Quantum and Fluxor each keep a monotone counter over a session, and
they mean different things. They carry different names, and a stated
rule:

| Counter | Owner | Advances on | Keys |
|---|---|---|---|
| `session_generation` | Quantum (`sessions.rs`) | every committed CONNECT: clean start, expiry, takeover | the dedupe table `(tenant, stream, generation, msg_id)`, every QoS completion |
| `session_epoch` | Fluxor (SessionCtrlV1) | every authoritative rebind of the attachment to a worker | every control message; the directory's binding |

1. A rebind never changes the generation. The dedupe ledger and the
   in-flight table are keyed by it, and a move must leave every key
   reachable — otherwise a PUBREL after the move misses its PUBREC's
   record and the flow completes twice.
2. A CONNECT never changes the epoch. A new connection is a new
   attachment at epoch 1; the MQTT session it resumes keeps its own
   generation.
3. An import is admitted only at the generation of any replicated
   record the importing worker already holds; a blob behind the log is
   refused `STATUS_STALE_EPOCH`.
4. Epochs only move forward: RESUME at the current epoch is the
   return-to-service of a refused handoff, below it is stale, above it
   after an import is the rebind.

The `Binding` in the core carries no generation field at all, so there
is nothing a rebind could bump by mistake; `tools/core_tests` holds the
rule.

## The seam

The anchor↔worker seam is the `proposals_out → codec_in` /
`codec_out → responses_in` edge pair, carrying framed envelopes
`[conn_id][proto][pkt_type][flags][fields]`, plus a SessionCtrlV1
sideband per worker (`ctrl_out` / `ctrl_in`). A second worker is a
second full set (`proposals2_out`, `responses2_in`, `ctrl2_*`),
following the `ctrl2` / `data2` shape of Fluxor's `echo_anchor` and
wave's `http`: channels are static, and the anchor's forwarding flip is
the activation.

Every codec hands its decoded envelopes to `anchor::forward_env`
instead of writing the proposal edge itself. The anchor forwards to the
worker the connection is bound to, or holds the envelope while the
attachment is not live.

**Delivery cursors.** Per connection, in envelope bytes (header
included) — the anchor's transfer unit at this seam, which is what the
contract asks both sides to agree on. The anchor's `forwarded` advances
when an envelope is accepted onto the bound worker's channel and its
`relayed` when a response from that worker is accepted by the codec
onto the frame edge, never when merely read, since a refused response
is offered again. The worker counts the same two on `codec_in` reads
and `codec_out` writes. `EXPORT_BEGIN` carries the worker's pair; the
anchor admits it with `cursors_admit` or refuses the handoff.

**The hold.** Envelopes for a connection whose attachment is not live
— attaching, or mid-swap — are held in one bounded FIFO and released
in order when it is. The buffer and its overflow policy are declared
configuration on `protocol`: `hold_bytes` (default 64 KiB) and
`hold_overflow`, which CLOSES the connection by default because a
silently dropped publish is a QoS promise broken; `1` drops the
envelope and counts it.

**Ordering across the seam.** Each worker's response bus is drained
before its control channel, and a worker's control channel is read
only when its response bus polls empty, so everything the worker
emitted before `DRAINED` has been counted before its export is read.

## The swap

`handoff_after_records` on the anchor (0 = never; the gates' trigger,
capped by `handoff_max_swaps`) moves every connection bound to the
current default worker to the other one, and new connections attach
there from that moment. A deployment drives swaps from its control
plane through the same path. Per connection:

1. `DRAIN` to the bound worker.
2. The worker drains to quiescent — no proposal of the session awaits
   its Raft assignment, nothing of its is parked, and it has been quiet
   for `QUIET_STEPS` — then declares `DRAINED` and exports.
3. The anchor admits the cursors and relays `EXPORT_BEGIN / CHUNK… /
   END` verbatim to the standby.
4. The standby answers `IMPORT_BEGIN / IMPORT_END`; on success the
   anchor sends `RESUME` at epoch + 1.
5. On `RESUMED` the forwarding target flips, the epoch advances, the
   held ingress is released to the importing worker, the exporting one
   is detached, and the directory is told with one `ATTACH` at the new
   epoch naming the importing worker.

Cursors that disagree, an import the standby refuses, a worker error
mid-swap, or a window past `session_drain_ms` refuse the handoff: the
standby is detached and discards its half-import, the exporting worker
is returned to service with `RESUME` at the session's current epoch
(nothing was committed anywhere, so nothing advanced), and the held
ingress goes back to it on its `RESUMED`. The client observes a pause
bounded by the deadline and nothing else. `session_drain_ms` must sit
below the shortest client keep-alive the deployment admits — Fluxor's
validator enforces the same inequality for `transport_migratable`, and
here it constrains admission, not just configuration.

## What moves

The record `sessions::export_record` builds (`QSR1`): identity,
generation, the slot index, negotiated parameters (protocol version,
clean start, keep-alive, expiry, receive maximum), the Will, the flow
windows and the QoS in-flight ledger (packet id, QoS, phase,
direction, generation, wal index, raft partition, correlation, and
whether the PUBREC went out). It is what the importing worker needs
that the committed log does not hand it. It is bounded by
`RECORD_MAX`; an oversize record is refused at `EXPORT_BEGIN`, never
truncated.

Subscriptions are deliberately absent: they live in `topic_engine`,
keyed by the slot index the session keeps across the move.

**Export points are protocol-coherent.** The anchor's per-connection
reassembly is anchor state, so an export never lands mid-packet by
construction. A QoS 2 flow between PUBREC and PUBREL is a stable,
durably recorded point and the record carries it; a flow whose PUBLISH
or PUBREL is still in Raft is not, and the drain waits for it.

## Two workers on one node

The handoff pair shares `topic_engine`, `flow` and `messaging`, all
keyed by session slot. Three rules keep the two workers from acting on
one session at once:

- **Slot ranges.** Each worker allocates new sessions from its own
  range (`slot_base` / `slot_count`), so slot numbers are unique across
  the pair, and a session keeps its index when it moves: the importer
  places it at the same index, which is free there because the ranges
  are disjoint. A lent index stays off the exporter's free stack until
  the session comes back.
- **Ownership at apply.** With `co_located_workers` set, a worker
  applies only the committed entries of sessions it holds or of slots
  it allocates and has not lent; a single-worker node applies
  everything, as a Raft follower must.
- **One recipient per reply.** `topic_engine` records the worker
  holding each subscription (byte 9 of the subscribe record) and
  delivers on that worker's port (`deliver_out` / `deliver_out2`);
  `MSG_TOPIC_MOVE` re-points a moved session's subscriptions.
  `messaging` answers on the port of the worker that asked (`op_in` /
  `result_out`, `op_in2` / `result_out2`). `flow`'s completions are
  teed, and carry the `wal_index` of the registration they answer, so
  a stale completion cannot be read as another registration's.

**Frozen sessions.** Between export and `DETACH` a session is
`Exported` on the exporting worker: held, but neither consumed nor
emitted for. A topic delivery that still reaches it is parked in the
offline queue by slot; an ack completion is dropped. After `RESUME`
the importing worker stays settling for `RESUME_SETTLE_MS`, parking
its own deliveries beside them, then drains the queue once in sequence
order — so nothing overtakes a delivery the exporting worker parked a
step before it saw `DETACH`. In-flight publishes are re-registered
with `flow` on `RESUME`; a QoS 2 flow whose PUBREC already went out is
not, and a PUBLISH retransmitted with DUP against an in-flight QoS 2
flow is answered from the ledger rather than started again.

## Declaration and validation

Every serving graph declares the class it requires and the config tool
validates the structure at build (`fluxor build --check <graph>`),
including that the capability providers are present:

```yaml
continuity:
  - id: quantum_edge
    class: edge_anchored
    anchor: protocol                     # transport.anchor.stream
    workers: [session_processor]         # session.worker, session.handoff
```

`examples/linux/handoff.yaml` declares two workers (which requires
`session.handoff` on both) and clustor's `session_directory` as the
directory member. The anchor states every binding to it in the
contract's own verbs — `ATTACH` when a session is minted, `ATTACH`
again at the next epoch naming the new worker when it swaps, `DETACH`
on close — so one verb carries a binding whatever changed about it, and
the directory reads the epoch to tell a first binding from a rebind.

It counts the verdicts that come back. A binding refused as stale is
the fencing signal: the cluster holds a newer generation of this
session than the anchor believes it owns. The reservation grant the
directory proposes once a binding commits belongs to the transport, so
the anchor reads past it.

`transport_migratable` is the same graph widened on bare metal:
`mechanism: platform_replicated_state` with the AEAD class
(`on_wire_sequence` or `unencrypted`; `implicit_counter` is rejected
outright), `failover_budget_ms` strictly below `client_keepalive_ms`,
a `transport.anchor.stream.secure` anchor (the `tls` module), the
directory, and the fence in two halves — `ip`'s, whose cutoff reaches
the wire on the target, and an out-of-band fence agent. The validator
refuses it on a hosted target, where TCP belongs to the host kernel and
cannot be checkpointed; the hosted gate proves that refusal.

Both the directory and the fence agent are members placed on another
node, because the host that must be proved quiet is the one that may
have failed — Fluxor's reference agent cuts the board's power through
the bench's plug. `examples/rig/pi5_migratable.yaml` is that graph: the
same front door with `tls` as the anchor, and a bench driver that holds
a QoS 2 subscription across a real cut. The widening is a declaration
and a composition, not a second implementation.

## Gates

- `tools/core_tests/src/session_identity_tests.rs` — the identity rule:
  fails if a rebind could move the generation, a CONNECT could move the
  epoch, or a stale epoch could be resumed.
- `tests/integration/session_handoff.sh` — the gates, each on
  `handoff.yaml` rendered with the swap trigger it needs, driven by
  `session_handoff_test.py` with the broker's `MON_SESSION` log as the
  second witness: a subscribed session survives a worker replacement
  with no reconnect and `session_present` intact; a QoS 2 flow crossing
  a handoff delivered exactly once, with the handoff between PUBREC and
  PUBREL (and a DUP retransmission after the move) and wherever the
  drain lets it land; subscriptions and retained delivery unchanged
  across the move, and a session attached after it; an export whose
  cursors disagree refused and the session kept; a hosted composition
  admitted at `edge_anchored` and refused at `transport_migratable`.
- `tests/integration/session_handoff_peer.sh` — the independent peer:
  mosquitto on both ends of a QoS 2 burst while the anchor swaps the
  sessions repeatedly inside it. Exactly once, in order, one connection.

Both run under `make test-mqtt-suite`, sequentially with the other
broker smokes, because each boots a broker on port 9090. That keeps
them out of `fluxor ci`, which builds and lints but boots nothing.

## Parameters

`protocol` (the anchor): `anchor_id` (eight bytes on every session id
and `MON_SESSION` line), `handoff_after_records`, `handoff_max_swaps`,
`session_drain_ms` (default 500), `hold_bytes` (65536),
`hold_overflow` (0 = close, 1 = drop), `default_worker`.

`session_processor` (the worker): `worker_id`, `co_located_workers`
(bitmask of the other worker ids on the node), `slot_base`,
`slot_count`, and `export_cursor_skew` — test-only fault injection for
the cursor gate, 0 in every deployment.

**Ceilings.** A worker steps at most `MAX_HANDOFFS` (1024) attachments
that are past `Attached` — draining, mid-import, or settling after a
RESUME. A swap larger than that is refused per connection with
`STATUS_NO_CAPACITY` rather than accepted and stalled, so the sessions
beyond the bound keep serving on the worker they are on; the `hfull`
counter on the `[sess]` line is what says it happened. Imports are
bounded separately at `IMPORT_SLOTS` (256) concurrent reassemblies.

## Non-goals

Continuity for the client-side modules (`mqtt_client`, `kafka_client`,
`amqp_client`, the sinks): they are the wrong side of the connection.
Any change to Fluxor's contract. Any weakening of durability-gated
acknowledgement to make a handoff cheaper: the drain waits for Raft,
and where continuity and exactly-once conflict, exactly-once wins.
