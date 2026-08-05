# AMQP Adapter

The AMQP adapter (`protocol`'s amqp component + the AMQP path through
`session_processor`, `topic_engine`, `messaging`)
implements AMQP 0-9-1 over TLS/TCP. The codec also handles enough of
AMQP 1.0's framing for protocol-header negotiation, but the in-tree
adapter targets 0-9-1 semantics (exchanges, queues, bindings,
channels). Common entities and invariants live in
[messaging_model.md](messaging_model.md).

## Connection and authentication

- AMQP clients open one TCP connection per broker, then multiplex
  multiple channels over that connection.
- SASL mechanisms supported: `PLAIN`, `EXTERNAL`. PLAIN credentials
  map to CP-Raft principals; EXTERNAL takes the mTLS-validated
  certificate identity directly.
- Protocol header negotiation accepts `AMQP\x00\x00\x09\x01`
  (AMQP 0-9-1) and falls back per the spec for other version tags.
- Channel multiplexing is bounded by `channel_max` (default 2047) and
  per-frame size by `frame_max` (default 131072).
- Heartbeats default to 60s; missed heartbeats trigger connection
  drop with `connection.close{reply-code=connection-forced}`.

## Addressing and routing

Queues and exchanges are tenant-scoped. Link endpoints hash to PRGs
by:

```
prg = hash64(tenant_id, address) % tenant_prg_count
```

where `address` is the queue name (for direct consumption) or the
exchange+routing-key (for publication).

### Exchange types

| Type | Match rule |
|---|---|
| `direct` | Exact match between routing key and binding key. |
| `fanout` | Broadcast to all bound queues; routing key ignored. |
| `topic` | Wildcard match with `*` (one word) and `#` (zero or more words) against the dotted routing key. |
| `headers` | Match against AMQP `headers` argument table, evaluated per binding's `x-match` (`all` or `any`). |

Routing decisions happen in `topic_engine`, which maintains
per-tenant binding tables and resolves publish-time fan-out
identically across all four exchange types.

## Delivery semantics

| Settlement | Behaviour |
|---|---|
| `settled=true` (no-ack) | Fire-and-forget after syntax validation. |
| `settled=false` (with ack) | At-least-once until `Basic.Ack` from consumer. ACK after WAL durability. |

`Basic.Reject` and `Basic.Nack` map to dedupe state transitions: a
rejected message is removed from inflight and (if `requeue=true`)
re-enters the queue at the head; if `requeue=false` and a
dead-letter exchange is configured, the message is routed there
instead.

### Transactions — not implemented

`Tx.Select` / `Tx.Commit` / `Tx.Rollback` are not served; there is no
two-phase pipeline behind them, on either the AMQP or the Kafka side
(see [kafka_adapter.md](kafka_adapter.md)). Channels must not be put
into transactional mode.

Exactly-once delivery is valid only under XO-BOUND (see
[messaging_model.md](messaging_model.md)). `forward_seq` plus
delivery tags provide the dedupe primitive; the cluster must be
healthy or in a fenced DR promotion for the guarantee to hold.

## Flow control and credits

AMQP 1.0 credit-based flow control is mirrored in the 0-9-1 path
through `flow`'s prefetch component:

- `Basic.Qos` `prefetch_count` and `prefetch_size` set per-consumer
  credit windows.
- Link credits persist in session state so credits survive reconnect
  and replay.
- When `flow`'s backpressure component raises pressure, credits clamp to
  zero with `Channel.Flow{active=false}` (or AMQP 1.0
  `Flow{drain=true}`) until pressure clears.

The translation table in [flow_control.md](flow_control.md) covers
the full backpressure → protocol-signal mapping.

## Durable subscriptions

| Subscription type | Persistence |
|---|---|
| `Queue.Declare{durable=true}` | Queue survives broker restart; messages persisted via Clustor WAL. |
| `Queue.Declare{exclusive=true}` | Auto-delete when the declaring connection closes; lifecycle tracked in session state. |
| Shared subscriptions | Distribute delivery tags by hash modulo group size (same primitive as MQTT shared subscriptions). |

## Dirty epoch mapping

`dirty_epoch` from CP-Raft maps to AMQP's link-detach mechanism:

- During link attach: detach the link with `amqp:link:detach-forced`
  so the client reconnects under the new epoch.
- During in-flight publishes: pause delivery
  (`Channel.Flow{active=false}`), wait for the epoch to stabilise,
  then resume.
- Persistent queues are unaffected — their PRG ownership may change
  but the durable state is replicated to the new owner before the
  epoch bump.
