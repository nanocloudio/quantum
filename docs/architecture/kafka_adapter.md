# Kafka Adapter

The Kafka adapter (`protocol`'s kafka component + the Kafka path through
`session_processor` and `topic_engine`) implements the Kafka binary
protocol
against Quantum's unified PRG substrate. Common entities and
invariants live in [messaging_model.md](messaging_model.md); the
partition model lives in [partitioning.md](partitioning.md).

## Connection and authentication

- Kafka clients open one TCP connection per broker; `protocol`'s
  router classifies the stream from its first bytes and hands it to
  the kafka codec.
- SASL is not implemented: the ApiVersions table advertises no SASL
  handshake APIs and connections are accepted without credential
  validation. Transport security comes from the `tls` module where
  the graph wires it.

## Topics, partitions, routing

Topics are tenant-scoped. Each Kafka partition maps to a PRG via:

```
prg = hash64(tenant_id, topic, partition_id) % tenant_prg_count
```

Raft voter membership is authoritative for durability. A partition's
writes are durable when the WAL entries reach quorum across the
partition's three voters, regardless of which Kafka broker the
producer connected to.

The Kafka `Metadata` response advertises one logical broker per
node, with this node as leader for the partitions it serves.

## Produce semantics

| `acks` | Behaviour |
|---|---|
| `acks=0` | Fire-and-forget: the produce proposal is untagged and no response is gated on it. |
| `acks=1` | Mapped to quorum durability. Leader-only acknowledgement is intentionally not supported; `acks=1` and `acks=all` both wait for quorum. |
| `acks=all` | Quorum durability; the ProduceResponse is gated on the durability proof. |

The `acks=1` collapse is deliberate. Leader-only ack is a
Kafka-historical optimisation that trades durability for latency, and
the **ACK-DURABILITY** invariant (see
[messaging_model.md](messaging_model.md)) does not admit that trade.
Producers that need lower latency should reduce `linger.ms` and
`batch.size`, not weaken the ack contract.

### Idempotent producers and transactions — not implemented

Quantum answers `InitProducerId` so idempotent producers complete
their handshake and connect, but it issues a producer id without
enforcing sequence-number dedupe (`API_INIT_PRODUCER_ID` in
`modules/app/protocol/kafka.rs`). Retries are therefore
**at-least-once, not exactly-once**.

There is no transactional path: `BeginTxn` / `EndTxn` are not served,
no transaction markers are written to the WAL, and
`isolation.level=read_committed` is not honoured. Producers must not
be configured with a `transactional.id`. Transactional
consume-transform-produce is future work.

## Consumer groups and offsets

`session_processor` serves the group lifecycle APIs (`JoinGroup`,
`SyncGroup`, `Heartbeat`, `LeaveGroup`, `OffsetCommit`,
`OffsetFetch`) against its own group and offset tables. Partition
assignment is client-side, per the Kafka group protocol: the elected
group leader computes it and pushes it via `SyncGroup`; the broker
stores and echoes membership and assignments and signals staleness
with `REBALANCE_IN_PROGRESS`. The broker keeps no server-side
session/heartbeat timers of its own.

Group metadata lives in the session-processor state. Offset commits
are WAL entries — they survive node failure exactly like session
records. The OffsetCommitResponse is not gated on durability: a lost
commit re-consumes from the previous offset, which the
at-least-once contract already admits.

## Retention and catch-up

There is no separate "log offset" storage — offsets are WAL indexes
interpreted through the topic's view of the WAL, and Fetch serves
from the apply-side message store's bounded ring.

Per-topic retention policies (time- or size-based) and key-based log
compaction are not implemented; the ring evicts oldest entries when
full.

## Dirty epoch mapping

Status: design target, not wired — routing epochs today come from
the synthetic control plane. The target mapping is Kafka's
`NOT_LEADER_OR_FOLLOWER`:

- Produce requests against a stale-epoch view of the partition
  leader receive `NOT_LEADER_OR_FOLLOWER`, prompting the client to
  refresh metadata.
- Fetch requests follow the same path; the client retries against
  the updated leader.
- The `Metadata` response that follows the epoch bump advertises the
  new leader and is treated as authoritative.
