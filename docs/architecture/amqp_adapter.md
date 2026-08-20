# AMQP Adapter

The AMQP adapter (`protocol`'s amqp component + the AMQP path through
`session_processor`, `topic_engine`, `messaging`)
implements AMQP 0-9-1 over TCP (TLS-terminated where the graph wires
the `tls` module). Only the 0-9-1 protocol header
(`AMQP\x00\x00\x09\x01`) is accepted; any other version tag fails
closed. Common entities and invariants live in
[messaging_model.md](messaging_model.md).

## Connection and authentication

- AMQP clients open one TCP connection per broker, then multiplex
  multiple channels over that connection.
- `Connection.Start` advertises the `PLAIN` mechanism, but the
  credentials in `Connection.StartOk` are not verified — client
  authentication is not implemented.
- Channel multiplexing is bounded by the advertised channel-max of
  16, and frame size by the advertised frame-max of 8192 bytes
  (whole frame including header and end octet).
- Heartbeats are advertised at 60s, and heartbeat frames are echoed.

## Addressing and routing

Queues and exchanges are tenant-scoped. Link endpoints hash to PRGs
by:

```
prg = hash64(tenant_id, address) % tenant_prg_count
```

where `address` is the queue name (for direct consumption) or the
exchange+routing-key (for publication).

### Exchange types

Status: design target, not wired. The codec accepts
`Exchange.Declare` without recording the exchange type, and publishes
route through the default exchange: routing flattens to an
`exchange.routing-key` subject matched by `topic_engine`. The target
design adds the four standard exchange types (`direct`, `fanout`,
`topic`, `headers`) with per-tenant binding tables resolved at
publish time.

## Delivery semantics

| Settlement | Behaviour |
|---|---|
| `settled=true` (no-ack) | Fire-and-forget after syntax validation. |
| `settled=false` (with ack) | At-least-once until `Basic.Ack` from consumer. ACK after WAL durability. |

`Basic.Reject` and `Basic.Nack` remove the delivery from the
consumer's unacked window; `requeue` is honoured by re-delivery from
the message log. There is no dead-letter exchange.

### Transactions — not implemented

`Tx.Select` / `Tx.Commit` / `Tx.Rollback` are not served; there is no
two-phase pipeline behind them, on either the AMQP or the Kafka side
(see [kafka_adapter.md](kafka_adapter.md)). Channels must not be put
into transactional mode.

Delivery is at-least-once: publisher confirms gate `Basic.Ack` on
quorum durability, and consumer acks bound redelivery, but there is
no exactly-once path on the AMQP side.

## Flow control and credits

Consumer credit in the 0-9-1 path runs through `flow`'s prefetch
component:

- `Basic.Qos` `prefetch_count` sets the per-consumer credit window.
- When `flow`'s backpressure component raises pressure, credits clamp
  until pressure clears.

The translation table in [flow_control.md](flow_control.md) covers
the full backpressure → protocol-signal mapping.

## Durable subscriptions

Queue contents are persisted via the clustor WAL: a queue's messages
survive broker restart because the message log they live in is
rebuilt by replay. Consumer registrations are connection-scoped and
re-established by the client on reconnect.

## Dirty epoch mapping

Status: design target, not wired — routing epochs today come from
the synthetic control plane. The target mapping pauses delivery
(`Channel.Flow{active=false}`) until the epoch stabilises, then
resumes; persistent queues are unaffected because their durable
state is replicated to the new owner before the epoch bump.
