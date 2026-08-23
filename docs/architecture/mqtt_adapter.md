# MQTT Adapter

The MQTT adapter (`protocol`'s mqtt component + the MQTT path through
`session_processor`, `topic_engine`, `messaging`) implements MQTT
3.1, 3.1.1, and 5.0 over TCP (TLS-terminated where the graph wires
the `tls` module) and QUIC. This document is the MQTT reference; the Kafka and AMQP
peers live in [kafka_adapter.md](kafka_adapter.md) and
[amqp_adapter.md](amqp_adapter.md).

MQTT is Quantum's reference adapter. Where Kafka or AMQP behaviour
differs from MQTT, those documents call out the override explicitly;
everything else inherits from here.

## Connection establishment

CONNECT binds to `client_id` scoped per tenant. The session-binding
rule:

| Connect flag | Behaviour |
|---|---|
| `clean_start=true` | Increment `session_epoch`, purge prior inflight state, begin fresh session record. |
| `clean_start=false` | Reuse the stored session record if `session_epoch` matches; resume inflight QoS 1/2 packets and drain the offline queue. |
| Same `client_id`, different connection | Fenced takeover: increment `session_epoch`, drop the prior connection, begin a fresh apply context for the new connection. |

The keep-alive deadline is the client-proposed value × 1.5. A missing
PINGREQ within that window triggers DISCONNECT and Will processing.

## Authentication and authorisation

Client authentication and per-tenant ACL enforcement are not
implemented: the control plane emits a synthetic single-tenant record
today (see [multi_tenancy.md](multi_tenancy.md)), and CONNECT is
accepted without credential validation. The target model — mTLS
identity feeding tenant-scoped ACLs — is described in
[security.md](security.md), which states what is wired and what is
design.

## QoS semantics

| Level | Wire | Durability |
|---|---|---|
| **QoS 0** | PUBLISH | Fire-and-forget after syntax validation. No ACK. |
| **QoS 1** | PUBLISH → PUBACK | PUBACK after WAL entries reach quorum durability via `flow`'s ack component. |
| **QoS 2** | PUBLISH → PUBREC → PUBREL → PUBCOMP | Four-phase handshake; each phase transition persists in WAL and requires quorum durability before progressing. |

QoS 2's four-phase state is owned by `session_processor` and lives in
two records the apply path writes on every node: the publisher's
in-flight slot, which the state machine reads to decide the next
response, and the dedupe entry (`DedupeState.phase`), which is keyed
by message identity and outlives that slot. Both are rebuilt by
replaying the committed PUBLISH and PUBREL entries, so a node that
restarts, or a follower that is promoted mid-transaction, resumes the
phase the log proves rather than one the failed leader held in
memory. Transitions only advance, so a repeated PUBREL — from a
client whose PUBCOMP was lost, or from replay — is answered again
without republishing the message.

Exactly-once is scoped to that contract: the broker accepts each
message once and completes each transaction once. It says nothing
about side effects a subscriber performs on delivery.

Cross-PRG publishes carry a `forward_seq` idempotence key (see
[partitioning.md](partitioning.md)) so replay after failover fences
duplicates.

## Shared subscriptions

`$share/<group>/<topic-filter>` groups distribute each delivery to
one member by a stable seeded hash modulo group size, so the
distribution is deterministic for a fixed member set. Ordering is
preserved within a PRG; cross-PRG ordering is not guaranteed (see
[partitioning.md](partitioning.md) Routing guarantees).

## Offline delivery

Sessions with `clean_start=false` accumulate deliveries in
`messaging`'s offline component while disconnected. On reconnect:

1. `session_processor` validates the resumed session against the
   stored record.
2. Offline-queue entries are drained in sequence order via
   `messaging.result_out` → `session_processor.messaging_in`.
3. Drained messages re-enter the inflight slots and obey the same
   QoS contract as live publishes.

Per-message expiry follows the offline-queue TTL (72h). Expired
entries are swept and the retention floor republished so WAL
compaction can advance.

## Will

CONNECT may include a Will message:

- Will is published when the session terminates without a clean
  DISCONNECT (network failure, keep-alive timeout, broker-initiated
  drain).
- MQTT 5 `Will Delay Interval` defers publication; if the client
  reconnects within the delay, the Will is cancelled.
- The Will payload is persisted with the session record so it
  survives session migration via leader transfer.

## Retained messages

Retained payloads live in `messaging`'s retained component, stored
inline and rebuilt by WAL replay. New subscriptions receive the
current retained payload for matching topics immediately after
SUBACK. Setting an empty payload with `retain=true` clears the
retained record.

## MQTT 5 features

| Feature | Status |
|---|---|
| Topic aliases | Implemented; the broker caps aliases at 16 per connection (`min(client topic_alias_maximum, 16)`, 0 for pre-5 clients). |
| Subscription identifiers | Implemented. |
| User properties | Decoded by `protocol`'s mqtt component and carried through the publish proposal (V2 body) to subscribers, order intact. |
| Receive Maximum | Implemented; caps concurrent unacked QoS 1+ deliveries per subscriber. |
| Session expiry interval | Implemented; drives the session-expiry sweep. |
| Will delay interval | Implemented; deferred fire, cancelled by reconnect within the delay. |

## Dirty epoch mapping

Status: design target, not wired — routing epochs today come from
the synthetic control plane and no rejection path fires. The target
mapping, per **ROUTING-EPOCH** in
[messaging_model.md](messaging_model.md):

- During CONNECT: reject with `0x9C` (Use another server) when the
  node should not handle this session.
- During PUBLISH: close the connection to force reconnect under the
  new epoch.

## QUIC

MQTT-over-QUIC is bridged by the `mqtt_quic_adapter` module, which
sits between fluxor's `quic` foundation module and `protocol`'s mqtt
component without changing either side; the QUIC graph wires it on
its own UDP listener. `protocol`'s router classifies the bridged
stream the same way as a TCP connection. Connection migration is a
QUIC-transport concern; the session record is unaffected.
