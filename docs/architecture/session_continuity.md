# Session continuity: the anchor and `session_processor` as the worker

Source: `modules/app/quantum_mqtt_anchor/anchor.rs` (the anchor, used by
`quantum_mqtt_anchor` and mounted by `protocol`; see
[mqtt_anchor.md](mqtt_anchor.md)),
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
replaced disappears with it. So the attachment is the anchor's: in
`quantum_mqtt_anchor` for an MQTT front door, or in `protocol` for the
multi-protocol one, the module that already owns the socket, the codec
state and the response multiplexing, none of which moves. The worker behind
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
worker the connection is bound to, wrapped in `MSG_ANCHOR_DELIVERY`
with the session, epoch and delivery id it was decided under (see
[mqtt_anchor.md](mqtt_anchor.md#the-delivery-envelope)), or holds the
envelope while the attachment is not live.

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
— attaching, or mid-swap — are held in one bounded FIFO with their
delivery ids and released in order when it is. The buffer and its
overflow policy are declared configuration on the anchor: `hold_bytes`
(default 64 KiB) and `hold_overflow`, which CLOSES the connection by
default because a silently dropped publish is a QoS promise broken;
`1` drops the envelope and counts it. Reading from the transport stops
at `hold_high_water_pct` of the hold, so the client's TCP window closes
before the hold can overflow.

**Ordering across the seam.** A client that goes away is reported to
its bound worker in band, as a `MSG_CONN_CLOSED` delivery behind the
client's last frames, so nothing the client sent is overtaken by the
end of its binding. Each worker's response bus is drained
before its control channel, and a worker's control channel is read
only when its response bus polls empty, so everything the worker
emitted before `DRAINED` has been counted before its export is read.

## The swap

A swap moves every connection bound to the current default worker to
the other one, and new connections attach there from that moment. An
operator or control plane starts it with `MSG_ANCHOR_SWAP`: a `QASW`
record proposed through the log (`POST /propose`) and forwarded by the
workers that apply it, or a command on the anchor's `cmd_in`
([mqtt_anchor.md](mqtt_anchor.md#starting-a-handoff)).
`handoff_after_records` (0 = never, capped by `handoff_max_swaps`) is
the gates' record-count trigger for the same swap. Per connection:

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

The record `sessions::export_record` builds (`QSR2`): identity,
generation, the slot index, the session's placement shard, negotiated
parameters (protocol version,
clean start, keep-alive, expiry, receive maximum), the Will, the flow
windows and the QoS in-flight ledger (packet id, QoS, phase,
direction, generation, wal index, raft partition, correlation, and
whether the PUBREC went out), the worker's book of the session's
subscriptions (the source of a site export, [anchor_continuity.md](anchor_continuity.md#a-session-at-a-surviving-site-qss1)),
then the handoff tail: the anchor delivery-id high-water the session has
consumed and the per-partition apply cursor the state reflects. It is
what the importing worker needs that the committed log does not hand
it. It is bounded by `RECORD_MAX`; an oversize record is refused at
`EXPORT_BEGIN`, never truncated, and a record whose tail cannot be read
is refused as corrupt.

The subscriptions themselves stay in `topic_engine`, keyed by the slot
index the session keeps across the move; `MSG_TOPIC_MOVE` re-points
their deliveries to the importing worker.

**Export points are protocol-coherent.** The anchor's per-connection
reassembly is anchor state, so an export never lands mid-packet by
construction. A QoS 2 flow between PUBREC and PUBREL is a stable,
durably recorded point and the record carries it; a flow whose PUBLISH
or PUBREL is still in Raft is not, and the drain waits for it.

## Exact replay

Source: `modules/common/cores/session_replay_core.rs`, gated by
`tools/core_tests/src/session_replay_tests.rs`.

**The cursor.** Both workers consume the same committed stream, but
while the session is the exporter's the importer skips its entries. So
the importer compares the record's cursor E with its own apply
position I, per partition:

| Condition | Outcome |
|---|---|
| The importer skipped an entry of the session past E | refused `STATUS_CURSOR_MISMATCH`: the entry is in neither the record nor the importer's future; the handoff is refused and the session stays where it is |
| The importer is behind E on some partition | admitted with a **replay floor**: the session's entries at or below E are already in the record and are skipped (`floorskip=`) until the importer's cursor passes E, so none is applied twice |
| Neither | admitted |

The importer tracks the most recent entry it skipped per slot, which is
exact for a session whose entries ride one partition (every
session-keyed op rides its session shard's partition) and checks only
the latest skip otherwise. Floors are bounded (`REPLAY_FLOORS`, 64); a
full table refuses the import `STATUS_NOT_READY` rather than admit one
that would apply twice. Refusing whenever I > E would be simpler and
wrong: the two workers routinely sit a few entries apart, so it would
refuse almost every handoff.

**Delivery ids.** The importer adopts the record's delivery
high-water, so a frame the anchor retries after the move is refused as
a duplicate there too ([mqtt_anchor.md](mqtt_anchor.md#the-delivery-envelope)).

**Outbound deliveries are in the log.** What a subscriber is owed does
not live only in the memory of the worker that sent it, so neither a
worker crash nor a node restart loses it. The worker logs a QoS 1/2
PUBLISH as `QOP_DELIVER` (session, generation, packet id, QoS and the
encoded PUBLISH body) when it is sent, and its progress as
`QOP_DELIVER_ACK`: released when a QoS 2 subscriber's PUBREC arrives and
the PUBREL goes out, complete at PUBACK or PUBCOMP. Every worker applies
both into its outbound ledger, whatever slot the entry names, because
the ledger has no side effects. A clean-start CONNECT forgets what the
session was owed.

What a session is owed is therefore rebuilt from the log alone, and a
resumed session (a `clean_start=0` CONNECT, or an adoption after a
worker loss) has it resent under its original packet ids, in the order
it was first sent (`redlv=`; MQTT 3.1.1 and 5.0 §4.4). A delivery the
subscriber has not answered goes out as a DUP PUBLISH; a QoS 2 receiver
that already holds it answers PUBREC again (§4.3.3). A QoS 2 delivery
already released goes out as its PUBREL, never as the PUBLISH: the
receiver may have handed the message on and forgotten the packet id at
PUBREL, and a second PUBLISH would then be a second copy.

The bounds are explicit. The ledger holds 256 owed deliveries per worker
with bodies up to 1920 bytes inline, and resends at most a session's
in-flight window (16) at a resume. A delivery past either ledger bound is
not logged and is counted (`dunlog=`, and the ledger's own refusal
count), so it is not reconstructible; a full ledger refuses rather than
evicts. Logging costs two proposals per QoS 1 delivery and three per
QoS 2, and `log_deliveries: 0` turns it off.

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

## What is claimed

| Event | Outcome | Mechanism | Gate |
|---|---|---|---|
| Planned worker replacement (operator `QASW` record, or the record trigger) | No reconnect; QoS 1/2 exactly once; `MON_SESSION` shows the epoch flip | DRAIN, export, import, RESUME at epoch + 1 | `session_handoff.sh`: `survive`, `qos2_between`, `qos2_across`, `subs_retained`, `planned_restart` |
| Unexpected worker loss, inside `worker_loss_ms` + adoption < client keep-alive budget | No reconnect for sessions with an acknowledged checkpoint; others are closed and resume by reconnect | checkpoints to the standby, the anchor's replay window, adoption at epoch + 1, ledger redelivery ([mqtt_anchor.md](mqtt_anchor.md#worker-loss)) | `session_handoff.sh`: `worker_loss` |
| Planned ingress-host maintenance | Stays connected where the transport moves the connection (Fluxor's `TRANSPORT_CONTINUITY` under Wormhole's paired anchors): the anchor re-anchors the session with its worker, quiesced, so exactly | `quantum_mqtt_anchor` `continuity: 1`, `session_processor` seats ([anchor_continuity.md](anchor_continuity.md#in-the-modules)) | `anchor_continuity_tests.rs`, `production_seat_tests.rs` (cores); the module graphs `examples/paired_anchor/quantum_host.yaml` and `quantum_worker.yaml` |
| **Abrupt ingress-host power loss** | Stays connected when the transport moved the connection under a crash-continuous pairing, with responses in flight or not: the anchor's horizon is the bytes `tls` reports the standby holds, and the replacement writes only what the standby lacks ([anchor_continuity.md](anchor_continuity.md#in-the-modules)). | `quantum_mqtt_anchor` `continuity: 1`, `session_processor` seats | `anchor_continuity_tests.rs`, `production_seat_tests.rs` (cores); the module graphs `examples/paired_anchor/quantum_host.yaml` and `quantum_worker.yaml` |
| Site loss | Resume at the surviving site from the session's `QSS1` record when one was exported (`MSG_SITE_EXPORT`), admitted by `site_admit`; otherwise reconnect and resume from the surviving site's log | `session_processor` `site_out` ([anchor_continuity.md](anchor_continuity.md#a-session-at-a-surviving-site-qss1)) | `session_site_tests.rs` (cores) |

## Declaration and validation

Every serving graph declares the class it requires and the config tool
validates the structure at build (`fluxor build --check <graph>`),
including that the capability providers are present:

```yaml
continuity:
  - id: quantum_edge
    class: edge_anchored
    anchor: quantum_mqtt_anchor          # transport.anchor.stream
    workers: [session_processor]         # session.worker, session.handoff
```

A multi-protocol graph names `protocol`, which declares the same
capability over the same anchor.

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
`mechanism: platform_replicated_state` with the anchor's AEAD class
(`implicit_counter` for the `tls` module, whose records count in
lockstep; the declaration must match the anchor's own manifest fact, and
the class is admitted only because `tls` declares an exact mirror
horizon), `failover_budget_ms` strictly below `client_keepalive_ms`,
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

Over QUIC the class has a second mechanism, one the wire carries itself.
`examples/linux/quic.yaml` declares, beneath the `edge_anchored` entry:

```yaml
  - id: quantum_quic_path
    class: transport_migratable
    mechanism: native_primitive          # RFC 9000 §9 connection migration
    anchor: quic                         # transport.mux.quic + transport.anchor.mux
```

A client that changes address — a NAT rebinding, or an active migration
onto the server's spare connection ID — keeps its one QUIC connection,
and the MQTT session riding it never sees a reconnect. The serving host
does not move, so there is no directory, fence, reservation or failover
budget, and the validator refuses those terms here. The anchor must be
the module carrying the mux in server mode with migration enabled, so
it is admitted on a hosted target as well as on bare metal. It is a
different guarantee from `platform_replicated_state`: the client moves,
not the server.

## Gates

- `tools/core_tests/src/session_identity_tests.rs` — the identity rule:
  fails if a rebind could move the generation, a CONNECT could move the
  epoch, or a stale epoch could be resumed.
- `tools/core_tests/src/session_replay_tests.rs` — exact replay: the
  tail round-trips; a record behind an entry the importer skipped is a
  cursor mismatch; an importer behind the record replays from the
  record's cursor and no earlier; the log rebuilds exactly the owed
  deliveries, oldest first; a released QoS 2 delivery is owed its
  PUBREL; a replayed delivery replaces rather than duplicates; an ack
  never completes a newer delivery; a full ledger refuses and counts.
- `tests/integration/session_handoff.sh` — the gates, each on
  `handoff.yaml` rendered with the swap trigger it needs, driven by
  `session_handoff_test.py` with the broker's `MON_SESSION` log as the
  second witness: a subscribed session survives a worker replacement
  with no reconnect and `session_present` intact; a QoS 2 flow crossing
  a handoff delivered exactly once, with the handoff between PUBREC and
  PUBREL (and a DUP retransmission after the move) and wherever the
  drain lets it land; subscriptions and retained delivery unchanged
  across the move, and a session attached after it; an export whose
  cursors disagree refused and the session kept, with an unacked
  outbound QoS 1 delivery found in the WAL as `QOP_DELIVER` and its
  PUBACK as `QOP_DELIVER_ACK`; a hosted composition
  admitted at `edge_anchored` and refused at `transport_migratable`.
- `tests/integration/session_handoff_peer.sh` — the independent peer:
  mosquitto on both ends of a QoS 2 burst while the anchor swaps the
  sessions repeatedly inside it. Exactly once, in order, one connection.
- `tests/integration/mqtt_quic_migration.sh` — the `native_primitive`
  evidence: one aioquic connection (an independent QUIC stack) carrying
  one MQTT session that publishes and subscribes at QoS 2, moved through
  a rebinding and an active migration with publishes in flight. Exactly
  once, in order, no DUP; the server challenges each new path and stops
  sending to the old one; one handshake, one CONNACK.

All three run under `make test-mqtt-suite`, sequentially with the other
broker smokes, because each boots a broker on a fixed port. That keeps
them out of `fluxor ci`, which builds and lints but boots nothing.

## Parameters

The anchor (`quantum_mqtt_anchor`, or `protocol`): `anchor_id` (eight
bytes on every session id and `MON_SESSION` line),
`handoff_after_records`, `handoff_max_swaps`, `session_drain_ms`
(default 500), `hold_bytes` (65536), `hold_overflow` (0 = close,
1 = drop), `default_worker`, `liveness_only` (0),
`hold_high_water_pct` (75), and for worker loss `worker_probe_ms`,
`worker_loss_ms` and `replay_bytes` (all 0 = off); see
[mqtt_anchor.md](mqtt_anchor.md#parameters).

`session_processor` (the worker): `worker_id`, `co_located_workers`
(bitmask of the other worker ids on the node), `slot_base`,
`slot_count`, `export_cursor_skew` — test-only fault injection for
the cursor gate, 0 in every deployment — `keyed_only` (default 1:
refuse any proposal that names no routing key; see
[partitioning.md](partitioning.md#unkeyed-proposals)),
`log_deliveries` (default 1: log outbound QoS 1/2 deliveries, see
[Exact replay](#exact-replay)), `checkpoint_ms` (0 = no checkpoints to
the standby; see [mqtt_anchor.md](mqtt_anchor.md#worker-loss)), and
`fault_stop_on_ping` — test-only fault injection for the
`worker_loss` gate, 0 in every deployment.

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
