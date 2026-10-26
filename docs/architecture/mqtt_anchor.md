# The MQTT anchor: `quantum_mqtt_anchor`

Source: `modules/app/quantum_mqtt_anchor/` (the module and the anchor
role, `anchor.rs`), `modules/common/cores/mqtt_anchor_core.rs` (every
decision that needs no syscall), `modules/app/protocol/mqtt.rs` (the
MQTT codec, mounted), `modules/app/session_processor/worker.rs` (the
worker's half of the seam), `tools/core_tests/src/mqtt_anchor_tests.rs`
(the gates on the core).

The anchor is the client-facing half of an `edge_anchored` MQTT session.
It holds the client's TCP connection open while the session worker
behind it is drained, replaced or lost, and it keeps only what that
needs.
[session_continuity.md](session_continuity.md) describes the handoff the
anchor runs; this document describes the anchor.

`protocol`, the multi-protocol front door, mounts the same `anchor.rs`,
so there is one anchor implementation. A graph that serves MQTT alone
uses `quantum_mqtt_anchor`, which carries no protocol classifier and no
Kafka or AMQP codec. `examples/linux/handoff.yaml` is that graph.

## What the anchor owns

| What the anchor keeps | Where it lives |
|---|---|
| tenant / listener identity | `AnchorConn::tenant` (a listener carries no tenant yet, so it is `TENANT_UNRESOLVED`; see [multi_tenancy.md](multi_tenancy.md)), `AnchorConn::listener` (`listener_id` param) |
| MQTT version, client-id hash | `AnchorConn::mqtt_version`, `client_hash` (`fnv1a_64(client_id)`, the worker's session identity), recorded from CONNECT |
| session epoch | `AnchorConn::epoch`, advanced only at a committed rebind |
| incremental fixed-header / remaining-length parser state and partial frame bytes | the codec's per-connection reassembly buffer (`mqtt.rs`, inline bytes plus pooled pages), in the same module |
| negotiated keep-alive, last safe inbound / outbound activity | `keepalive_s`, `last_in_ms`, `last_out_ms` |
| bounded frames waiting for a worker, with their delivery ids | the hold (`hold_bytes`), each entry `[conn][type][len][delivery_id]` |
| bounded worker responses waiting for transport acceptance, with their production ids | the held response, offered again under `next_production` until the frame edge takes it |
| worker binding / generation and current handoff operation | `AnchorConn::worker`, `phase` |
| CONNECT admission, AUTH exchange, drain, liveness-only flags | `FLAG_CONNECT_ADMITTED`, `FLAG_AUTH_EXCHANGE`, `FLAG_DRAINING`, `FLAG_LIVENESS_ONLY` |

Subscriptions, retained messages, offline queues, packet-id / QoS state,
dedupe and authorization are the worker's and the committed log's. The
anchor is not a second MQTT database.

## The delivery envelope

While a worker is attached, every envelope the anchor hands it is
wrapped in `MSG_ANCHOR_DELIVERY` (`wire.rs`):

    [conn_id:u16 LE][session_id:16][session_epoch:u32 LE]
    [delivery_id:u64 LE][inner_msg_type:u8][inner payload after conn_id]

The delivery id is assigned when the frame is first taken from the
client. A refused write keeps its id (the counter advances only when the
frame is written or held), a held frame is released under the id it was
held with, and ids are per session, start at 1 and only increase.

The worker admits each envelope against the binding it holds
(`mqtt_anchor_core::admit_delivery`):

| Envelope | Verdict |
|---|---|
| bound session, bound epoch, id above the consumed high-water | consumed, as the inner envelope |
| an epoch below the bound one | refused: a stale binding (`dstale=`) |
| an epoch above the bound one | refused: the worker has not been resumed there |
| an id at or below the high-water | refused as a retry (`ddup=`) |
| no live binding for that session | refused (`dunk=`) |

Both sides count the wrapped envelope in the delivery cursors, so the
`EXPORT_BEGIN` equality check is unchanged in meaning.

With no worker wired (`ctrl_out` unwired), the anchor is transparent and
envelopes cross bare.

**A high-water, not a dedupe entry.** The retry check is a per-session
high-water rather than an entry in `messaging`'s dedupe shard. Delivery
ids are per session, monotone and delivered in order on one channel, so
"at or below the highest consumed" is exact, and it costs no messaging
round trip per frame. The high-water crosses a handoff inside the
session's handoff record.

## Production ids

The response the anchor is about to hand the transport is
`next_production`, and the counter advances only when the frame edge
accepts it. A response the edge refuses is held whole and offered again
first, under the same id, which is what keeps the id stable until it is
accepted.

Across an ingress takeover the ids must survive the anchor itself: the
worker numbers its responses, keeps each until the transport's mirrored
output horizon covers it, and re-offers the rest to a replacement anchor
under the same ids ([anchor_continuity.md](anchor_continuity.md); the
anchor runs it under `continuity: 1`).

**Stale emitters are fenced.** Only the worker a connection is bound to
may emit for it. A response from the other worker (one that exported
the session, or one declared lost) is dropped and counted (`stale=` on
the `[anchor] hb` line, which both front doors emit), not relayed.

## Liveness

A syntactically valid PINGREQ never reaches a worker as work. What the
anchor does with it depends on whether a worker is authoritative and on
the route policy (`liveness_only`):

| Worker | `liveness_only` | Anchor |
|---|---|---|
| attached and live | either | answers PINGRESP and forwards `MSG_SESSION_LIVENESS` to the worker |
| absent (attaching, mid-handoff) | 1 | answers PINGRESP, and nothing else |
| absent | 0 | owes the PINGRESP and sends it when a worker is authoritative again |

PUBLISH, SUBSCRIBE, UNSUBSCRIBE and AUTH are never acknowledged by the
anchor. While no worker is authoritative they are held (bounded), and
released in order with their delivery ids when one is. Under anchor
continuity the worker answers every PINGREQ: the anchor forwards the
mark, held like any frame while no worker is authoritative
([anchor_continuity.md](anchor_continuity.md)).

**The liveness mark.** The worker's keep-alive sweep refreshes a session
only when it receives an envelope for it, and a PINGREQ is answered at
the anchor, so a client that does nothing but ping (an idle subscriber,
say) would look silent to the worker and be torn down at 1.5x its
keep-alive while alive and pinging. Every answered PINGREQ therefore
also sends a liveness mark (`MSG_SESSION_LIVENESS`,
`[conn][proto][PINGREQ][0]`), on the plain seam too. The worker counts
the mark as activity and nothing else.

**The anchor watches keep-alive while the worker cannot.** The worker's
sweep exempts a session mid-handoff, so the anchor closes a connection
whose client has been silent past 1.5x its negotiated keep-alive while
no worker is authoritative (`kaclose=`).

## The hold and backpressure

The hold is one bounded FIFO (`hold_bytes`, at most 128 KiB). Reading
from the transport stops when the hold reaches `hold_high_water_pct` of
its capacity (default 75). The stream contract
(`contracts/net/net_proto.rs`) has no receive-window verb, so not
reading is the backpressure: `peer_router`'s channel fills, then the
transport's, and the client's TCP window closes before the hold can
overflow. The pause is anchor-wide, not per connection, so one stalled
session holds back reads for every connection on the anchor. That is
the honest cost of a contract with no per-flow window. An overflow
still closes the connection by default (`hold_overflow = 0`), because a
silently dropped publish is a QoS promise broken.

A frame too large for the seam (a wrapped envelope past one channel
record) closes its connection and is counted (`oversize_closed`). The
codec's own packet limit is below that, so this is a bounded outcome
for a frame no worker could have read, not a new limit.

**Closing.** When the client goes away the anchor ends the session's
binding on both workers. The bound worker, when live, is told in band:
a `MSG_CONN_CLOSED` delivery on its data channel, behind the frames the
client sent last, so a DISCONNECT is consumed before the binding ends.
A `DETACH` on the control channel would race those frames, because a
worker reads its control channel first in a step. The other worker, and
a bound worker mid-handoff (whose frames are held, not in flight), get
`DETACH`. A connection the transport reports closed returns its slot to
idle at once. One the anchor closes itself (a hold overflow, an
oversize frame, a keep-alive expiry, a session it cannot adopt) is
released the same way and then waits, dropping anything still decoded
for it, until the transport's close notice confirms the socket is gone;
a close request the frame edge refuses is retried each step.

## Starting a handoff

The primary trigger is an operator or control-plane command,
`MSG_ANCHOR_SWAP` (`[anchor_id:8][mode:u8][seq:u64 LE]`, mode 0 =
planned). It arrives one of two ways:

- **Through the log.** An operator proposes a `QASW` record
  (`["QASW"][anchor_id:8][mode:u8]`) with `POST /propose` on the
  diagnostic port; `handoff.yaml` wires `operations.proposal` into
  `consensus.proposals_tagged`. The request is answered `200` once the
  record applies, so a graph that offers the route also wires
  `consensus.proposal_assigned` and `consensus.applied` back into
  `operations`; without them every proposal is answered `503` when its
  correlation times out. Every worker that applies the record
  forwards it to its anchor on `ctrl_out`, with
  `seq = (partition << 48) | raft index`. The anchor executes it once,
  however many workers forward it, and only if it names this anchor (or
  all of them, with an all-zero id). Raft indices order records within
  a partition and not across partitions, so the anchor keeps the
  highest sequence executed per partition.
- **Directly**, on `quantum_mqtt_anchor`'s `cmd_in`, from a control
  plane wired to the anchor. `protocol` has no `cmd_in`; a
  `protocol`-hosted anchor takes commands through the log.

A command acts on the sessions bound to the default worker when it
executes. With none bound it changes nothing, not even which worker new
connections attach to or whether a worker is lost: a node replays its
log at boot, before any client attaches, and every `QASW` record it
holds reaches the anchor again then.

`handoff_after_records` (with `handoff_max_swaps`) is the record-count
trigger the gates use; it runs the same swap.

## Worker loss

When the worker serving a session is lost, the anchor keeps the socket,
holds or backpressures, answers liveness only, activates the standby,
has it reconstruct committed state and import the replicated volatile
state, commits a higher epoch and resumes forwarding.

**Checkpoints.** Every `checkpoint_ms`, the worker serving a session
sends a checkpoint of it through the anchor to the standby
(`MSG_ANCHOR_CKPT_BEGIN / CHUNK / END`: the QSR2 handoff record, in the
export's three-frame layout). It sends one only when the session's
consumed delivery high-water has moved and the session is at a coherent
point (nothing of it awaiting Raft, parked, or in the commit-gating
stash), so it is the same protocol-coherent state a planned export
carries. The standby keeps it as a non-live **shadow** and does not
import it, then acknowledges it with the delivery high-water it covers
(`MSG_ANCHOR_CKPT_ACK`).

**The replay window.** While a standby is there to adopt from, the
anchor retains every frame a loss would have to replay (`needs_replay`:
MQTT work other than CONNECT and PINGREQ, and other than QoS 0 PUBLISH,
which is at-most-once) in a bounded FIFO (`replay_bytes`), and trims it
to the frames above each acknowledged checkpoint. A frame that cannot be
retained marks a **replay gap** on its connection; only a checkpoint
that covers the last such frame closes it.

**Detection.** The anchor probes each worker every `worker_probe_ms`
(`MSG_ANCHOR_PROBE`). A worker that answers none for `worker_loss_ms`
is declared lost; with either at 0 silence declares nothing. An
operator `QASW` record with mode 1 declares the default worker lost at
once. A lost worker is fenced for good: nothing it emits is relayed,
its control frames are ignored, and nothing more is sent to it.

**Adoption.** For each connection bound to the lost worker:

| State | Outcome |
|---|---|
| an acknowledged checkpoint and no replay gap | adopted: `MSG_ANCHOR_ADOPT` at epoch + 1; the standby commits the shadow as an import, binds the session, re-points its subscriptions, re-registers its in-flight publishes, redelivers what the log says it is owed (`QOP_DELIVER`), and answers RESUMED; the anchor then replays the retained frames above the checkpoint under their original delivery ids, releases what it held, and the connection never closes |
| no checkpoint yet, a replay gap, no standby, or the standby refuses | closed, honestly, so the client reconnects and resumes from committed state |
| mid-handoff toward the lost worker | the handoff is refused and the session stays where it is bound |

The replayed frames' ops are idempotent at apply (the dedupe ledger for
PUBLISH and PUBREL, set semantics for SUBSCRIBE and UNSUBSCRIBE), which
is why entries the standby skipped past the checkpoint are re-derived
by the replay rather than refused at import. While the session is
adopted it is held like any other non-authoritative one: PINGREQ is
answered under `liveness_only`, application work is held, and the
anchor watches keep-alive.

Two limits are stated rather than hidden. A committed publish that
`topic_engine` routed to the lost worker before an adopted subscriber's
subscriptions were re-pointed is lost to that subscriber: the fan-out
delivers into the dead worker. The adoption re-points subscribers
first, in connection order, which narrows the window without closing
it. And a lost worker stays lost until it is restarted; the anchor does
not reinstate it.

`worker_loss_ms` plus the adoption must sit below the shortest client
keep-alive budget admitted (1.5x keep-alive). `handoff.yaml` probes
every 100 ms and declares a loss at 1.5 s.

## Parameters

`anchor_id`, `handoff_after_records`, `handoff_max_swaps`,
`session_drain_ms` (500), `hold_bytes` (65536), `hold_overflow`
(0 = close), `default_worker`, `liveness_only` (0), `hold_high_water_pct`
(75), `listener_id` (0), `worker_probe_ms` (0 = no probes),
`worker_loss_ms` (0, or no probes = only an operator declares a loss),
`replay_bytes` (0 = no replay window, so a loss closes the lost worker's
connections), `continuity` (0; 1 runs anchor continuity — see
[anchor_continuity.md](anchor_continuity.md); a swap or an adoption then
carries the production numbering to the worker the session moves to).
`protocol` takes the same anchor parameters under its own tags
(`liveness_only` 23, `hold_high_water_pct` 24, `worker_probe_ms` 25,
`worker_loss_ms` 26, `replay_bytes` 27).

The worker side: `checkpoint_ms` (0 = no checkpoints) and, for the
gate only, `fault_stop_on_ping` on `session_processor`.

## Gates

- `tools/core_tests/src/mqtt_anchor_tests.rs`: the delivery envelope
  round-trips and is never truncated; the worker accepts new work at the
  bound epoch and refuses old epochs, future epochs, retried ids and
  foreign sessions; delivery and production ids are stable until
  committed; a live worker gets the ping as a liveness mark, and an
  absent one gets an answer only under liveness-only policy; PUBLISH,
  SUBSCRIBE, UNSUBSCRIBE and AUTH are application work; reads pause at
  the high-water mark before the hold fills; the FIFO keeps order across
  a stall; a swap command executes once and only for its anchor, with
  its sequence ordered per partition; a worker is lost only past the
  declared window and only while probes are sent; a loss adopts only a
  session with an acknowledged checkpoint and no replay gap and closes
  the rest; a checkpoint closes a gap only past its last missing frame;
  a handoff toward the lost worker is refused; a planned move resets the
  checkpoint; a late acknowledgement does not rewind it.
- `tests/integration/session_handoff.sh`: the handoff gates run on
  `handoff.yaml` with this module as the anchor, including
  `planned_restart` (an operator `QASW` record swaps the worker under a
  continuous QoS 1/2 publisher) and `worker_loss` (the worker goes dark
  and the standby adopts its sessions with no client disconnect).
