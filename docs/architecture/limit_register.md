# Limit register

This is the inventory of the deliberate finite bounds on quantum's exchange
surfaces: the provider half every sink answers through, the broker's in-graph
fronts ([broker_front.md](broker_front.md)), and the broker bounds they are
held to. A ceiling in one of the files below that is absent here is a defect.
When quantum is composed with Clustor, fluxor and a requester, the first bound
a request meets is the effective one.

`Capacity` limits retained work or state. `Shape` limits one key, record or
request. `Pacing` only limits work per step and must still make progress.
"Refuse" means an explicit answer (a status or an `ABORT`). "Backpressure"
leaves the work in its channel for a later step.

## Broker bounds the fronts are held to

| Limit | Kind | Symbol | Source | Value | Exhaustion behaviour |
|---|---|---|---|---:|---|
| QoS 1 op body | Shape | `QOS1_OP_MAX` | modules/common/wire.rs | 2048 | The apply side holds each QoS 1+ publish, commit-gated, in a stash slot of this size. A larger op body is not proposed: the publish is refused as for `RAFT_ENTRY_MAX`. |
| Raft entry body | Shape | `RAFT_ENTRY_MAX` | modules/common/wire.rs | 2048 | Clustor's `wal_frame::MAX_ENTRY_BODY`: a proposal past it is never committed. A QoS 1 publish's entry carries 41 bytes besides its topic and payload, so it binds before `QOS1_OP_MAX`. A PUBLISH past either (`wire::publish_fits`) is refused before it is proposed, Packet too large: DISCONNECT with reason 0x95 to an MQTT 5 client, then the connection closed; an MQTT 3.1.1 client's connection is closed. Counted (`toolarge=` on `session_processor`'s heartbeat). |
| MQTT 5 Maximum Packet Size | Shape | `MQTT5_MAX_PACKET` | modules/common/wire.rs | RAFT_ENTRY_MAX - QOS1_ENTRY_FIXED + MQTT5_PUBLISH_FRAMING_MIN | Derived: 2014, advertised in every MQTT 5 CONNACK — the largest packet whose PUBLISH, at any QoS, fits the Raft entry. A larger PUBLISH is refused as for `RAFT_ENTRY_MAX` when its entry does not fit. |
| Envelope payload | Shape | `MAX_PAYLOAD` | modules/common/wire.rs | 0xFFFF | The `len u16` of the wire envelope `[msg_type][len][payload]`. |
| Wire message | Shape | `MAX_MSG` | modules/common/wire.rs | crate::abi::CHANNEL_BUFFER_SIZE | One envelope written or read whole (8192 bytes). A larger message is refused by the writer; a reader discards one it cannot hold, whole. |

## The publish provider

| Limit | Kind | Symbol | Source | Value | Exhaustion behaviour |
|---|---|---|---|---:|---|
| Publish header block | Shape | `HEADERS_MAX` | modules/common/publish_exchange.rs | 512 | A publish whose headers pass it is refused 413. A sink reads none of them. |

## The broker's in-graph fronts

| Limit | Kind | Symbol | Source | Value | Exhaustion behaviour |
|---|---|---|---|---:|---|
| Session envelope | Shape | `WIRE_MAX` | modules/common/front_session.rs | 8192 | The SDK's `CHANNEL_BUFFER_SIZE` (the module holds the two equal at build). An envelope that would pass it is not built. |
| Envelope payload | Shape | `ENVELOPE_MAX` | modules/common/front_session.rs | WIRE_MAX - WIRE_HDR | Derived. |
| Session client id | Shape | `CLIENT_ID_MAX` | modules/common/front_session.rs | 64 | A CONNECT with a longer client id is not built. |
| Control envelope | Shape | `CONTROL_MAX` | modules/common/front_session.rs | AT_BODY + 15 + CLIENT_ID_MAX | Derived: the largest CONNECT. The module reserves room for one control step before reading anything that could owe one. |
| Subscription filter | Shape | `FILTER_MAX` | modules/common/subscribe_exchange.rs | 256 | A longer filter is refused 413. |
| Durable session name | Shape | `SESSION_MAX` | modules/common/subscribe_exchange.rs | 64 | A longer `session` header is refused 413. It is the session's client id, so it holds to `CLIENT_ID_MAX`. |
| Publish topic | Shape | `TOPIC_MAX` | modules/app/broker_front/mod.rs | 255 | The most a string param carries. A longer `topic` refuses construction. |
| Front client id | Shape | `CLIENT_ID_MAX` | modules/app/broker_front/mod.rs | front_session::CLIENT_ID_MAX - 6 | Room for the `-sXXXX` a live-only subscription's session appends. A longer `client_id` refuses construction. |
| Publishes awaiting the broker | Capacity | `INFLIGHT_CAP` | modules/app/broker_front/mod.rs | 32 | Backpressure: `publish_in` is not read while the window is full. |
| Publishes collected at once | Capacity | `REQUEST_SLOTS` | modules/app/broker_front/mod.rs | 2 | A publish arriving with every slot collecting is refused 503. |
| Publish records owed | Capacity | `OWED_CAP` | modules/app/broker_front/mod.rs | INFLIGHT_CAP + 3 | Derived so that no owed answer is ever refused room: the window, one read, and the LINK pair. |
| Subscriptions per front | Capacity | `SUB_SLOTS` | modules/app/broker_front/mod.rs | 8 | A subscription arriving with every slot open is refused 503. |
| Unacknowledged deliveries per subscription | Capacity | `SUB_RECV_MAX` | modules/app/broker_front/mod.rs | 1 | The session's receive maximum: the broker holds the next QoS 1 message until the front has passed the last one on. |
| Session envelopes owed | Capacity | `TX_CAP` | modules/app/broker_front/mod.rs | 2 * front_session::WIRE_MAX | Backpressure: a publish is read only with room for a whole envelope and a control step behind it. Any other record is read only with a control step's room. |

The largest record the publish front takes is derived from these bounds and
is not a constant of its own. It is the least of `wire::qos1_payload_room`,
`front_session::publish_room` and `subscribe_exchange::payload_room` for the
configured topic, so `2007 - topic_len` bytes. A record past it is refused
413. The capability fact `max_payload = 1752` is that room on a 255-byte
topic, and the module holds the two equal at build. A subscription holds at
most two of the largest messages a BODY carries (`HOLD_TWO`). A QoS 0 stream
that outruns it ends the subscription with `ABORT STALLED`.

```limit-register
QOS1_OP_MAX | modules/common/wire.rs | 2048
RAFT_ENTRY_MAX | modules/common/wire.rs | 2048
MQTT5_MAX_PACKET | modules/common/wire.rs | RAFT_ENTRY_MAX - QOS1_ENTRY_FIXED + MQTT5_PUBLISH_FRAMING_MIN
MAX_PAYLOAD | modules/common/wire.rs | 0xFFFF
MAX_MSG | modules/common/wire.rs | crate::abi::CHANNEL_BUFFER_SIZE
MAX_MSG | modules/common/wire.rs | crate::abi::CHANNEL_BUFFER_SIZE
HEADERS_MAX | modules/common/publish_exchange.rs | 512
WIRE_MAX | modules/common/front_session.rs | 8192
ENVELOPE_MAX | modules/common/front_session.rs | WIRE_MAX - WIRE_HDR
CLIENT_ID_MAX | modules/common/front_session.rs | 64
CONTROL_MAX | modules/common/front_session.rs | AT_BODY + 15 + CLIENT_ID_MAX
FILTER_MAX | modules/common/subscribe_exchange.rs | 256
SESSION_MAX | modules/common/subscribe_exchange.rs | 64
TOPIC_MAX | modules/app/broker_front/mod.rs | 255
CLIENT_ID_MAX | modules/app/broker_front/mod.rs | front_session::CLIENT_ID_MAX - 6
INFLIGHT_CAP | modules/app/broker_front/mod.rs | 32
REQUEST_SLOTS | modules/app/broker_front/mod.rs | 2
OWED_CAP | modules/app/broker_front/mod.rs | INFLIGHT_CAP + 3
SUB_SLOTS | modules/app/broker_front/mod.rs | 8
SUB_RECV_MAX | modules/app/broker_front/mod.rs | 1
TX_CAP | modules/app/broker_front/mod.rs | 2 * front_session::WIRE_MAX
```
