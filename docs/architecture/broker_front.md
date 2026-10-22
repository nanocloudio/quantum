# The broker's in-graph fronts

Source: `modules/app/broker_front/`, `modules/common/front_session.rs`,
`modules/common/subscribe_exchange.rs`, `modules/common/publish_exchange.rs`,
`packaging/stack/quantum_broker.yaml`

## Overview

An application reaches quantum's broker one way: exchange edges (fluxor's
exchange contract, `modules/sdk/contracts/exchange.rs`). Whether the broker
is a foreign one behind a protocol client, quantum running in the
application's own graph, or quantum placed on another node is a placement
choice, made in wiring:

| Placement | Publish | Subscribe |
|---|---|---|
| A foreign broker (mosquitto, a remote quantum) | `mqtt_sink` | `mqtt_client`'s `app_out` stream |
| quantum in the same graph | `broker_front`'s publish front | `broker_front`'s subscription front |
| quantum on another node | the same fronts, `node:`-placed, through fluxor's `remote_channel` exchange endpoints | the same |

`broker_front` is a provider module over the broker's internals. It reaches
`session_processor` the way a transport codec does, through the codec
envelope, with no MQTT bytes and no socket in between. One broker session
carries the publish front; each subscription is a session of its own. Both
fronts answer with the conventions of the protocol clients they stand in
for, so an application swaps one for the other by wiring alone.

The broker and both fronts ship as one published stack, `quantum_broker`
([packaging/stack/quantum_broker.yaml](../../packaging/stack/quantum_broker.yaml)), which an application
includes and wires by its exports.

## The publish front

The publish front takes exactly the `PUBLISH` exchange `mqtt_sink` takes and
answers it exactly as `mqtt_sink` does. The two share one implementation of
the provider half, `modules/common/publish_exchange.rs`.

| | `mqtt_sink` | publish front |
|---|---|---|
| Ports | `request_in` / `response_out` | `publish_in` / `publish_out` |
| Method | `METHOD_PUBLISH` | `METHOD_PUBLISH` |
| `target` | the ordering key, up to `KEY_MAX` | the ordering key, up to `KEY_MAX` |
| Body | the record, the MQTT payload byte for byte | the record, the MQTT payload byte for byte |
| Flags | `MORE`, `BROADCAST`; anything else 400 | `MORE`, `BROADCAST`; anything else 400 |
| Topic | the `topic` param | the `topic` param (`publish_topic` on the stack) |
| 200, empty body | the broker's QoS 1 PUBACK | the broker's QoS 1 PUBACK, gated on Clustor durability |
| 413 | a record past `max_payload` or the Maximum Packet Size the connected broker advertises, or one that broker ends the connection for (below) | a record past the bounds |
| 503 | every collecting slot taken | every collecting slot taken |
| Losing the broker | `LINK DOWN`; `LINK UP` at the next CONNACK | `LINK DOWN`; `LINK UP` at the next CONNACK |
| Capability | `stream.ordered_ack.sink` | `stream.ordered_ack.sink` |

**Which topic.** The topic is a param, as `mqtt_sink`'s is, and the request's
`target` is the ordering key and never the topic. One front publishes to one
topic, which is one ordering unit: request order is broker order, and
`BROADCAST` is a plain publish (`broadcast = "degenerate"`). An application
publishing to several topics wires one front per topic, as it would wire one
`mqtt_sink` per topic.

**Durable acceptance.** The front writes each record as a QoS 1 PUBLISH on its
session. `session_processor` proposes it to Raft, and the PUBACK it answers
with is the one a TCP client would get: after the entry is committed and the
durability proofs say it is on disk. That PUBACK is the 200.

**Losing the session.** The publish session is lost when an acknowledgement
is overdue past `ack_timeout_ms`, or when the broker ends the session (it is
served on another node). Every publish awaiting the broker is then
unknowable, so the front writes `LINK DOWN`, drops request records until its
`LINK UP` is away, and opens a new session after a backoff (100 ms, doubling
to 5 s). The CONNACK writes `LINK UP`, and the requester re-issues each
exchange it held open, exactly as it does for `mqtt_sink`.

**Whose bound.** `mqtt_sink` talks to any broker, so the size it holds a
record to is the connected broker's, learned at run time: the Maximum Packet
Size its MQTT 5 CONNACK advertises (quantum's broker advertises
`wire::MQTT5_MAX_PACKET`), checked before the PUBLISH is sent. A broker that
advertises none is sent records up to `max_payload`; one it will not take is
answered 413 when the broker says so (DISCONNECT with Packet too large), or,
from a broker that can only close the connection (MQTT 3.1.1), when the
connection closes on the same record twice. The front is the broker, so it
holds a record to the broker's own bound directly.

**Bounds.** A record past what the front carries is refused 413, never
truncated. The bound is the least of three: what the broker commits as a
QoS 1 publish (its Raft entry within Clustor's 2048-byte entry body, less 41
bytes of headers and the topic), what one session envelope carries, and what
one subscription BODY hands back. The broker's bound is the binding one:
`2007 - topic_len` bytes. The capability fact `max_payload = 1752` is that
room on the longest topic the front takes (255 bytes). A requester that
declares a larger fact fails the build.

## The subscription front

A subscription is a held exchange.

| | |
|---|---|
| Ports | `subscribe_in` / `subscribe_out` |
| Request | `GET`, no flags, no body |
| `target` | an MQTT topic filter (`+` and `#` as MQTT defines them), up to 256 bytes |
| Request header `session` | optional: names a durable session to create or resume (1 to 64 printable bytes) |
| First answer | HEAD 200, flags `HOLD \| MORE`, content type `application/vnd.quantum.message`; a durable subscription adds `session-present: 0` or `1` |
| Then | one BODY per message, flag `MORE`, never past the response credit |
| Ends | the requester's `ABORT`, or the provider's `ABORT` (below) |

The HEAD is written once the broker has taken the subscription (its SUBACK),
so a message published after the requester reads the HEAD is delivered on it.
Refusals are a terminal HEAD marked `RAISED`, before any 200:

| Status | When |
|---|---|
| 400 | not a `GET`, flags set, a filter MQTT does not allow, a session name that is empty or carries a space or control byte, a repeat HEAD for an open exchange |
| 409 | the named durable session already has a live subscription |
| 413 | a filter past 256 bytes, a session name past 64 |
| 502 | the broker refused the session (it is served on another node) or the filter |
| 503 | every subscription slot is taken (8 per front) |
| 504 | the broker did not take the subscription within `ack_timeout_ms` |

### Messages

Each BODY carries one whole message:

```
[topic_len u16 BE][topic][payload_len u32 BE][payload]
```

The topic is the message's own (a wildcard filter matches many), laid out as
an MQTT PUBLISH carries it. The payload is byte for byte what an MQTT
subscriber to the same filter receives. The framing is self-delimiting, so it
survives a relay that joins BODY records into one byte stream, such as an
HTTP/1.1 response with no declared length.

### Credit

The response credit is the subscriber's backpressure. The front writes a BODY
only when the credit granted so far (the HEAD's `resp_credit` plus every
`CREDIT` record) covers its data. Messages wait in the front, in order, until
it does.

The broker delivers to each subscription's session at QoS 1 with a receive
maximum of one. The front acknowledges a delivery only when its BODY has been
written, so at most one QoS 1 message per subscription waits in the front. The
next one waits in the broker, which is where an MQTT subscriber's backlog
waits as well. A QoS 0 message carries no acknowledgement and is not held
back by the receive maximum. When QoS 0 messages arrive faster than the
subscriber credits them and outrun what the front holds for it (two of the
largest messages), the front ends the subscription with `ABORT STALLED`. It
does not drop messages without saying so.

### Ending

- **`ABORT` from the requester** ends the subscription. The front writes a
  DISCONNECT for its session, and nothing more for the exchange. A live-only
  session ends there. A durable one is kept for `session_expiry_s`.
- **`ABORT FAILED`** from the front: the broker ended the session, because it
  is served on another node.
- **`ABORT TOO_LARGE`** from the front: a message whose framing passes one
  BODY. Only a foreign publisher can produce one, since the publish front
  refuses such a record. The message is acknowledged, so a durable session
  is not handed it again on every resume.
- **`ABORT STALLED`** from the front: see Credit above.

### Resume

quantum keeps a position for a subscriber: its MQTT session. The `session`
header names it, and it is the session's MQTT client id. The first `GET`
with a given name creates the session and the HEAD says
`session-present: 0`. After the subscription ends, the broker keeps the
session for `session_expiry_s` and queues the QoS 1 messages its filters
match. A later `GET` with the same name resumes it. The HEAD says
`session-present: 1`, and the queued messages are delivered first, in order.
A resume that reads `session-present: 0` lost its session (it expired): the
messages published in between are gone, as they would be for an MQTT client.

A session carries no per-message position. The session is the cursor, so
there is no `after:` header and no position field in a BODY. Two
consequences follow from quantum's session semantics:

- A message the broker had delivered to the session but the front had not yet
  passed on when the subscription ended (at most one at QoS 1) is not
  redelivered on resume. quantum releases a resumed session's unacknowledged
  deliveries and counts them (`sub_inflight_dropped_on_resume`).
- A session's subscriptions accumulate as MQTT's do: resuming with a
  different filter adds it to the session.

A subscription without a `session` header is live-only. It receives what is
published while it is open and nothing from before. Its session is
`<client_id>-s<hex>` and ends with the exchange.

## Reaching the broker: the codec envelope

`broker_front` writes the envelope `protocol::mqtt` writes to
`session_processor.codec_in`, and reads the answers `session_processor` writes
to `codec_out`. The layouts are in `modules/common/front_session.rs`. It does
not share the codec's pair. The two do not share a fan, because an
`OctetStream` fan may split one writer's frame around another's. Instead
`session_processor` gives the front a pair of its own: `front_in` (in[10]) and
`front_out` (out[12]).

- The front's sessions use connection ids from `wire::FRONT_CONN_BASE`
  (`0xF000`) up: `0xF000` for the publish session, then one per subscription
  in turn. An id is reused only after the whole range, so an answer still in
  flight for an ended session never reaches the next one. Transport
  connections stay below the line, because `peer_router`'s slot index stays
  below its 16384-entry table.
- `session_processor` drains `codec_in` and `front_in` through the same codec
  path, offering each first in turn. It drops an envelope that names a
  connection outside its input's namespace (`nsdrop` on its heartbeat).
- A response for a session at or above the line leaves on `front_out`. Every
  other response leaves on `codec_out`.

Front sessions declare no keep-alive and are not anchored. The `protocol`
anchor's session continuity covers transport connections, and the front ends
its own sessions explicitly.

## The stack

`packaging/stack/quantum_broker.yaml` is the stack `quantum_broker`: the single-node
broker (Clustor's `peer_router`, `partition_router`, `consensus`,
`durability`, `control_plane`, `admission`, `gateway`; quantum's `protocol`
and `session_processor` in their `mqtt` variants, `flow`, `topic_engine`,
`messaging`) and `broker_front`, wired as `examples/linux/minimal.yaml`
wires the broker plus the front's pair.

| Param | Type | Default | |
|---|---|---|---|
| `publish_topic` | string | `""` | The topic every publish lands on. Required when `publish_in` is wired |
| `client_id` | string | `quantum-front` | The publish session's client id, at most 58 bytes |
| `ack_timeout_ms` | integer | 10000 | 1 to 3 600 000 |
| `session_expiry_s` | integer | 3600 | How long a durable session outlives its exchange |
| `listen_port` | integer | 9090 | The MQTT listener's TCP port |

| Export | Endpoint | |
|---|---|---|
| `publish_in` / `publish_out` | `front.publish_in` / `front.publish_out` | The publish front |
| `subscribe_in` / `subscribe_out` | `front.subscribe_in` / `front.subscribe_out` | The subscription front |
| `net_in` / `net_out` | `peer_router.net_in` / `peer_router.net_out` | The MQTT listener, for foreign clients over TCP. Wire both or neither |

The including graph owns what a stack cannot carry: `target`, `platform:
{ net: {} }` when the listener is wired, `scheduler: { accept_cycles: true }`
(the request/answer edges make the graph cyclic), and a `continuity:` block
if it wants the session anchor declared. An application that wired
`mqtt_sink` wires the stack instead by pointing the same two edges at
`<as>.publish_in` and `<as>.publish_out` and setting `publish_topic` to the
sink's `topic`.

```yaml
include:
  - stack: quantum_broker          # by name, or a path to its file
    as: broker
    params: { publish_topic: orders }
wiring:
  - { from: app.request_out, to: broker.publish_in }
  - { from: broker.publish_out, to: app.response_in }
```

`fluxor build --check packaging/stack/quantum_broker.yaml` checks the stack alone.
[examples/broker_stack/linux.yaml](../../examples/broker_stack/linux.yaml)
includes it by path, publishes with fluxor's `exchange_poster`, subscribes
through wave's `http`, and exports the listener.
`tests/integration/broker_stack.sh` drives it end to end. It checks that a
publish is answered 200 once, and that a held subscription, a wildcard
subscription and a foreign MQTT client all receive the exact bytes. It checks
the 413 bound, the refusals,
that `ABORT` ends a subscription, and that a durable session resumes with the
messages published while it was detached.

## Foreign brokers

The protocol clients stay as the way to brokers quantum does not run.
`mqtt_client`'s `app_out` stream remains the subscription path to a foreign
broker. A held-exchange subscription client for foreign brokers, presenting
the subscription front's surface over an MQTT connection, is the symmetric
follow-up. `mqtt_sink` already is that client for publishing.
