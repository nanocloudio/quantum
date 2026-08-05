# Kafka Adapter

The Kafka adapter (`protocol`'s kafka component + the Kafka path through
`session_processor` and `topic_engine`) implements the Kafka binary
protocol
against Quantum's unified PRG substrate. Common entities and
invariants live in [messaging_model.md](messaging_model.md); the
partition model lives in [partitioning.md](partitioning.md).

## Connection and authentication

- Kafka clients open one TCP connection per broker; Quantum's
  `protocol`'s router component ALPN-demuxes the stream into `protocol`'s kafka component.
- SASL identities (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512) map to
  CP-Raft principals as optional additives to mTLS. mTLS is the
  primary auth surface; SASL provides per-app identity within the
  cert-validated transport.
- Transactional IDs are issued by CP-Raft and persisted in the
  tenant record so producer fencing survives broker restart.

## Topics, partitions, routing

Topics are tenant-scoped. Each Kafka partition maps to a PRG via:

```
prg = hash64(tenant_id, topic, partition_id) % tenant_prg_count
```

Raft voter membership is authoritative for durability. A partition's
writes are durable when the WAL entries reach quorum across the
partition's three voters, regardless of which Kafka broker the
producer connected to.

The Kafka `Metadata` response advertises one logical broker per node;
partition leadership is resolved through CP-Raft and exposed to
clients as the `Leader` field per partition.

## Produce semantics

| `acks` | Behaviour |
|---|---|
| `acks=0` | Edge acceptance only — disabled unless the tenant policy explicitly allows lossy ingest. Default policy denies. |
| `acks=1` | Mapped to quorum durability. Leader-only acknowledgement is intentionally not supported; `acks=1` and `acks=all` both wait for quorum. |
| `acks=all` | Quorum durability. Default for production tenants. |

The `acks=1` collapse is deliberate. Leader-only ack is a
Kafka-historical optimisation that trades durability for latency, and
the **ACK-DURABILITY** invariant (see
[messaging_model.md](messaging_model.md)) does not admit that trade.
Producers that need lower latency should reduce `linger.ms` and
`batch.size`, not weaken the ack contract.

### Idempotent producers

Idempotent producers carry a producer ID + epoch + sequence number.
Quantum tracks these via the same dedup infrastructure that
backs MQTT QoS 2:

- Per-`(producer_id, partition)` sequence numbers detect and reject duplicates.
- Producer epoch fencing (Kafka `INVALID_PRODUCER_EPOCH`) follows the same `session_epoch` mechanism as MQTT.
- The dedupe window expires after `dedupe_ttl_default_ms` (72h default).

### Transactions — not implemented

Quantum answers `InitProducerId` so idempotent producers complete
their handshake and connect, but it issues a producer id without
enforcing sequence-number dedupe
([protocol/kafka.rs](../../modules/app/protocol/kafka.rs)
`API_INIT_PRODUCER_ID`). Retries are therefore **at-least-once, not
exactly-once**.

There is no transactional path: `BeginTxn` / `EndTxn` are not served,
no transaction markers are written to the WAL, and
`isolation.level=read_committed` is not honoured. Producers must not
be configured with a `transactional.id`. Transactional
consume-transform-produce is future work.

## Consumer groups and offsets

`session_processor` serves the group lifecycle APIs (`JoinGroup`,
`SyncGroup`, `Heartbeat`, `LeaveGroup`, `OffsetCommit`,
`OffsetFetch`) against its own group and offset tables:

| Timer | Default |
|---|---|
| Session timeout | 10s |
| Rebalance timeout | 60s |
| Heartbeat interval | 3s |

Group metadata lives in the session-processor state. Offset commits
are WAL entries — they survive node failure exactly like session
records. The default rebalance strategy is `cooperative-sticky`:
members keep their partitions across most rebalances and only
relinquish moved partitions, avoiding stop-the-world consumption
pauses.

Fetch sessions enforce tenant quotas via `ThrottleEnvelope`,
translated by `flow`'s backpressure component into Kafka's native
`throttle_time_ms` field.

## Retention and catch-up

| Retention type | Behaviour |
|---|---|
| Time-based | Per-topic, evaluated on compaction; entries past the retention window are eligible for WAL truncation subject to dedupe / offline-queue floors. |
| Size-based | Same mechanism; the floor is whichever of (time, size) yields the smaller retained window. |
| Log compaction | Per-topic key-based compaction — the last value per key is retained; suitable for changelog topics. |

Catch-up reads from snapshots + WAL via Clustor's standard
`admission`-permitted read path. There is no separate "log offset"
storage — offsets are WAL indexes interpreted through the topic's
view of the WAL.

## Dirty epoch mapping

`dirty_epoch` from CP-Raft maps to Kafka's `NOT_LEADER_OR_FOLLOWER`:

- Produce requests against a stale-epoch view of the partition
  leader receive `NOT_LEADER_OR_FOLLOWER`, prompting the client to
  refresh metadata.
- Fetch requests follow the same path; the client retries against
  the updated leader.
- The `Metadata` response that follows the epoch bump advertises the
  new leader and is treated as authoritative.
