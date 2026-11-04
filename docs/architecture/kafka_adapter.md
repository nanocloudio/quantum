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

## Response order

The Kafka protocol answers a connection's requests in the order they
were sent, and clients rely on it: Java's `NetworkClient` fails the
connection on a response whose correlation id is not its oldest
request's, librdkafka treats one as a protocol error. Quantum's answers
are made in different places at different times — the codec answers
ApiVersions, Metadata, FindCoordinator and InitProducerId itself;
`session_processor` answers Fetch, ListOffsets and the group calls at
once, a Produce once it is quorum durable, an OffsetCommit likewise, a
Fetch from the WAL once the read returns — so the codec puts them back
in order (`modules/common/cores/kafka_order_core.rs`):

- Each request that will be answered (all but an `acks=0` Produce)
  takes the next place in its connection's queue when the codec takes
  it. An answer for the oldest place is written at once; an answer for
  a later place waits in a slot until every answer ahead of it is
  written.
- At most 8 requests are in flight per connection (the bound Java's
  `max.in.flight.requests.per.connection` sets on the client). Slots
  are shared and reserved when a request is taken, sized by its API
  (small for fixed-shape answers, large for those carrying records or
  lists); the oldest request needs none, so a connection that is not
  pipelining holds none, and an answer that arrives always has its
  room — taking answers off the response bus never waits on another
  answer behind it there. A request past the bound, or one no slot is
  free for, stays in the reassembly buffer; the raw record that would
  overflow the buffer is held and the codec reads no raw records until
  it lands. That is the backpressure on a pipelining client.
- A request that will never be answered closes the connection in its
  turn, behind the answers ahead of it and before any after it: a
  malformed one, an API this broker does not serve, one
  `session_processor` marks with the close marker (a Kafka response
  whose `api_key` is `wire::KAFKA_API_CLOSE`, sent for any request a
  handler left without an answer), and one whose answer has not come
  15 s after it became the oldest (the backstop for an answer lost on
  the way). The client resends on a new connection what it still
  wants — exactly, for an idempotent producer.
- `session_processor` answers every request it expires: a Produce whose
  durability never came answers `REQUEST_TIMED_OUT` for each unresolved
  partition after 8 s, an OffsetCommit fails its parked answer the same
  way.
- A connection moving between session workers (an anchored front door's
  swap, [session_continuity.md](session_continuity.md#the-swap)) moves
  only once the worker it leaves owes it no answer: a Produce or
  OffsetCommit still awaiting durability or its apply, or a Fetch still
  reading the WAL, is answered by that worker, because the answer — its
  correlation, its place in the queue, the partition outcome it reports
  — exists only there and the anchor fences that worker's output once
  the connection moves. The anchor holds the connection's new requests
  while the worker drains, so the owed answers only finish; past the
  drain deadline the move is refused and the same worker keeps the
  connection and answers them in their turn. The 15 s backstop is not
  part of a swap.

MQTT and AMQP need no such sequencing. MQTT's ordering rules on
acknowledgements (§4.6: PUBACK and PUBREC in the order the PUBLISHes
arrived) are the receiver's rules, which a server follows as the sender
of deliveries — and Quantum sends a subscriber's PUBREL when its PUBREC
arrives, in order, through the response hold. AMQP publisher confirms
may arrive in any order (each Basic.Ack / Nack names one delivery tag,
`multiple` unset), and a channel carries one synchronous method at a
time.

## Topics, partitions, routing

Topics are tenant-scoped. Each Kafka partition is its own routing key,
so it has its own shard, its own log and its own offset sequence:

```
shard = hash64(RKEY_KAFKA, tenant_id, topic, partition_id) % VIRTUAL_SHARDS
prg   = shard_map[shard]  ?:  jump_consistent_hash(shard, prg_count)
```

The partition comes from the producer's own partitioner and is never
re-derived from record contents. The advertised partition count is
clamped to `1..=16`, which is what a full 8-topic Metadata response can
encode.

Raft voter membership is authoritative for durability. A partition's
writes are durable when the WAL entries reach quorum across the
partition's three voters, regardless of which Kafka broker the
producer connected to.

The Kafka `Metadata` response advertises every node in the cluster as a
broker, each on its own advertised host, and names the current raft
leader as the leader of each partition — exact while one raft group
serves them all. A node with no host configured for a peer still
advertises that peer's id, because Kafka clients identify brokers by
`node_id` and renumbering would send a reconnecting client to the wrong
node.

> **Status: consumer-group coordination is leader-local.**
> `FindCoordinator` always answers with this node, so coordination does
> not distribute. Group state is bounded at 8 groups / 8 members / 64
> committed offsets per node and is rebuilt by the clients when they
> rejoin after a failover; there is no broker-side heartbeat or session
> expiry. Committed offsets themselves are durable — each commit emits
> an untagged `QOP_KAFKA_OFFSET` through Raft — but the OffsetCommit
> response is not gated on the durability proof.

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

### Idempotent producers

`InitProducerId` issues a producer id whose high bits carry the issuing
node's id, so two nodes cannot hand out the same one — idempotence
state is keyed by `(producer_id, partition)`, and colliding ids would
have unrelated producers reading each other's sequence numbers.

Sequences are enforced on the APPLY side, not at propose time. Apply is
the replicated state machine: every replica runs it, and a verdict
reached there is the same verdict after a failover or a WAL replay. A
propose-time check would be leader-local and would disagree with itself
the moment leadership moved.

| Sequence | Verdict |
|---|---|
| `last + 1` | Accepted and appended |
| Already accepted | `DUPLICATE_SEQUENCE` — acknowledged WITHOUT appending, which is what makes a producer retry safe |
| Ahead of `last + 1` | `OUT_OF_ORDER_SEQUENCE` — refused, because accepting it would silently lose the batches in between |
| Older epoch | `INVALID_PRODUCER_EPOCH` — a zombie producer, fenced by a newer incarnation |

A batch carrying Kafka's "no producer id" (`-1`) has not opted into
idempotence and is always accepted.

### Transactions — not implemented

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
session/heartbeat timers of its own, but it does release a member when
its connection drops rather than only on an explicit `LeaveGroup`:
clients commonly close without leaving, and ghosts would otherwise
accumulate until the member table fills and the group wedges.

Group metadata lives in the session-processor state. With two workers
on one node (`co_located_workers`) it lives in ONE of them, the anchor's
coordinator: every group and offset call goes there whichever worker
the connection is bound to, while Produce, Fetch and ListOffsets stay
with the connection's worker — every worker applies the whole log
(docs/architecture/session_continuity.md, "Two workers on one node").
The codec's sequencer writes the connection's answers in request order
whichever worker gives them. Offset commits
are WAL entries — they survive node failure exactly like session
records. The OffsetCommitResponse is not gated on durability: a lost
commit re-consumes from the previous offset, which the
at-least-once contract already admits.

## Retention and catch-up

A Kafka offset is a **logical per-partition offset**, assigned on the
apply side, not a WAL index. The two are unrelated numbers: a WAL index
counts raft entries across everything the node commits, so reporting
one to a producer would tell it an offset no consumer could ever fetch.

Fetch is served from the apply-side message store first. A consumer
whose offset has aged out of that in-memory ring is **not** refused: the
Kafka log and the raft log are the same bytes, so the record is still
durable, and the fetch falls through to a cold read that resolves the
offset back to a raft index and reads it from the WAL.

That makes two lifetimes run over one set of bytes, so they have to be
reconciled. Raft compaction is driven by snapshots, which know nothing
about Kafka retention; left alone it would retire segments a consumer
inside its retention window can still ask for, and the cold read would
then have nothing to read. The broker therefore publishes a
**compaction floor** — the lowest raft index it still needs — to
`durability`, and compaction stops there.

Two deletion policies run over the log, and unlike Kafka they are
independent rather than alternatives: **time retention**
(`kafka_retention_ms`) and **key compaction** (`kafka_compact`). Both
default to off, because a config upgrade must not start deleting data
under a running broker. With both off, the ring evicts its oldest
entries when it fills. There is no per-topic policy surface yet; both
switches are broker-wide.

## Placement and dirty epoch

A Kafka client already implements a metadata-refresh loop, so a request
this node should not serve is answered with the error code that drives
it rather than with a proxied hop:

- `NOT_LEADER_OR_FOLLOWER` when another PRG owns the partition — a
  settled answer, so the client refreshes metadata and goes to the
  right broker.
- `LEADER_NOT_AVAILABLE` for the transient dispositions — the shard is
  mid-fence, or this node's placement view is behind the sender's —
  which the client retries.

Answering `REQUEST_TIMED_OUT` instead, or simply going quiet, would
leave the client retrying the same broker until its own timeout without
ever learning that the partition moved. The full disposition table is
in [partitioning.md](partitioning.md#edge-routing).
