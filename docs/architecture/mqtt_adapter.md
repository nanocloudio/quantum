# MQTT Adapter

The MQTT adapter (`mqtt_codec` + the MQTT path through
`session_processor`, `topic_engine`, `dedup_engine`, `offline_queue`,
`retained_store`) implements MQTT 3.1, 3.1.1, and 5.0 over TLS/TCP
and QUIC. This document is the MQTT reference; the Kafka and AMQP
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
| `clean_start=false` | Reuse the stored session record if `session_epoch` matches; resume inflight QoS 1/2 packets and drain `offline_queue`. |
| Same `client_id`, different connection | Fenced takeover: increment `session_epoch`, DISCONNECT the prior connection with reason `0x8E` (Session taken over), begin a fresh apply context for the new connection. |

Keep-alive defaults to the client-proposed value × 1.5
(operator-configurable). Missing PINGREQ within the keep-alive window
triggers DISCONNECT and Will processing.

## Authentication and authorization

CP-Raft stores tenant ACLs and feature flags; the manifest flows
through `cp_bridge` → `tenant_manager` → `session_processor`. Stale
CP caches force disconnect with `MQTT-5 0x87` (Not authorized) rather
than risking authorisation against expired policy.

mTLS is mandatory. The client certificate is validated against the
per-tenant trust bundle; the SPIFFE ID (when present) feeds into
RBAC.

## QoS semantics

| Level | Wire | Durability |
|---|---|---|
| **QoS 0** | PUBLISH | Fire-and-forget after syntax validation. No ACK. |
| **QoS 1** | PUBLISH → PUBACK | PUBACK after WAL entries reach quorum durability via `ack_tracker`. |
| **QoS 2** | PUBLISH → PUBREC → PUBREL → PUBCOMP | Four-phase handshake; each phase transition persists in WAL and requires quorum durability before progressing. |

QoS 2's four-phase state is owned by `session_processor` and tracked
in the dedupe entry (`DedupeState.phase`). Replay reconstructs the
exact phase a session was in at crash time, so PUBREC / PUBREL /
PUBCOMP emit only once per message regardless of node failure between
phases.

Cross-PRG publishes use `ForwardPublish` with `forward_seq` (see
[partitioning.md](partitioning.md)). Missing `PublishAcked` triggers
replay; fenced PRGs revert in-flight exchanges to pending state until
strict fallback clears.

## Shared subscriptions

`$share/<group>/<topic-filter>` groups distribute deliveries by
hashing the publish identifier with a stable, versioned hash seed
modulo group size. Best-effort ordering is preserved within a PRG;
cross-PRG ordering is not guaranteed (see
[partitioning.md](partitioning.md) Routing guarantees).

The hash seed is versioned in the routing manifest so changes to the
distribution function are observable as a routing-epoch bump.

## Offline delivery

Sessions with `clean_start=false` accumulate deliveries in
`offline_queue` while disconnected. On reconnect:

1. `session_processor` validates the resumed session against the
   stored record.
2. Offline-queue entries are drained in sequence order via
   `offline_queue.drain_out` → `session_processor.offline_drain`.
3. Drained messages re-enter the inflight slots and obey the same
   QoS contract as live publishes.

Per-message expiry inherits the per-session / per-tenant
`offline_queue_ttl` (default 72h, max 7d). Expired entries are GC'd
on the next `offline_queue` sweep and bump
`earliest_offline_queue_index` so WAL compaction can advance.

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

Retained payloads live in `retained_store` (content-addressed,
snapshot-persisted). New subscriptions receive the current retained
payload for matching topics immediately after SUBACK. Setting an
empty payload with `retain=true` clears the retained record.

## MQTT 5 features

| Feature | Status |
|---|---|
| Topic aliases | Implemented (`topic_alias_max = 65535` per graph default). |
| Subscription identifiers | Implemented; preserved through forwards. |
| User properties | Decoded by `mqtt_codec`; passed through `WorkloadForwardEnvelope` to subscribers. |
| Reason strings | Emitted on error responses; bounded to keep response size predictable. |
| Server keep-alive | Default 60s; overridable per tenant. |
| Server reference | Emitted on DISCONNECT during planned drain to hint reconnection to other nodes. |
| Session expiry interval | Drives `session_ttl_default_ms`; respects the tenant max. |

## Dirty epoch mapping

Per **ROUTING-EPOCH** in [messaging_model.md](messaging_model.md),
MQTT maps `dirty_epoch` to:

- During CONNECT: reject with `0x95` (Topic name invalid) if the
  resolved partition no longer owns the session's topics, or `0x9C`
  (Use another server) if the node should not handle this session at
  all.
- During PUBLISH: NACK with `0x91` (Packet identifier in use) is not
  used — instead, the connection closes with `0x80` (Unspecified
  error) to force reconnect under the new epoch.

## QUIC

The QUIC listener path is structurally identical to TLS/TCP. The
`tls` module accepts QUIC streams alongside TLS sessions, and
`protocol_router` ALPN-demuxes `mqtt-quic` into the same
`mqtt_codec`. Resume semantics inherit from QUIC:

- 0-RTT is disabled for non-CONNECT packets (the codec rejects 0-RTT
  data outside CONNECT).
- Connection migration is supported by QUIC at the transport layer;
  the session record is unaffected.

See [guides/interop.md](../guides/interop.md) for QUIC interop
scenarios.
