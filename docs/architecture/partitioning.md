# Partitioning & Routing

How Quantum shards tenants across Partition Raft Groups (PRGs), how
the control plane places PRGs on nodes, and how routing decisions
remain consistent across rebalances. Assumes familiarity with the
entities and invariants in [messaging_model.md](messaging_model.md),
in particular **ROUTING-EPOCH**.

**Status: the routing model is wired; the authority that drives it is
not.** Keys are hashed to virtual shards, shards resolve to PRGs, and
every module that routes resolves them the same way. What is missing is
a control plane with something to say: placement, rebalance and epoch
migration are described below as the operating model the routing is
built for, and the sections that depend on that authority say so.

## System model

### Components

| Component | Role |
|---|---|
| **Edge Listener** | TLS termination, SNI / ALPN demux (`tls` + `peer_router` + `protocol`'s router component); forwards framed packets to the session processor. |
| **Session Processor** | Adapter logic, dedupe, offline queues, Will / retained features. All durable state mutation occurs inside the Raft apply loop. Helpers may precompute, but every mutation commits through the same apply path. |
| **Control Agent** | Per-node helper (`control_plane` + `governance`'s tenants component): syncs CP-Raft objects, publishes health, manages certificates, requests rebalance actions. |

### Assumptions

- Additional PRGs scale linearly with session count and publish
  load.
- Control-plane outages shorter than the cache grace window retain
  cached metadata; longer outages trigger strict fallback (see
  [control_plane.md](control_plane.md) Cache states).

## Sharding

Routing is two mappings, and keeping them separate is what makes the
cluster resizable:

```
shard = hash64(domain, tenant_id, key) % VIRTUAL_SHARDS   (2^18, fixed)
prg   = shard_map[shard]  ?:  jump_consistent_hash(shard, prg_count)
```

The **virtual shard space is fixed for the life of a cluster**, so a
shard id means the same thing on every node and in every log record.
Only the second mapping moves: growing from N to N+1 PRGs relocates
exactly the 1/(N+1) of shards that land on the new PRG and nothing
else, so a cluster widens without re-homing the shard space and without
a dense map. The control plane's `shard_map` is a sparse set of
overrides on top of that baseline — the entries a migration is moving
right now — bounded so that every node applies the same map or refuses
the same entry, never half of one.

Keys are **domain-separated** so an MQTT topic and a Kafka topic of the
same name cannot collide, and **tenant-separated** so two tenants'
identically-named objects stay independent:

| Domain | Key | What the shard owns |
|---|---|---|
| MQTT session | `(tenant, client_id)`, equivalently `(tenant, stream_hash)` | The session record, its inflight state and its offline queue |
| MQTT topic | `(tenant, normalized_topic)` | Matching of publishes against subscriptions, and the retained message |
| Kafka | `(tenant, topic, partition)` | That partition's log |
| Kafka group | `(tenant, group_id)` | Coordinator state and committed offsets |
| AMQP queue | `(tenant, queue_name)` | The queue |

A session and a topic it publishes to are separate keys, so they land
on the same PRG only when their shards happen to. That is expected, and
[Cross-PRG delivery](#cross-prg-delivery) is why it costs nothing.

The Kafka partition is part of the key rather than derived from record
contents, so a producer's own partitioner alone decides placement.

The hash algorithm and seed are versioned. Any change requires a
routing-epoch migration coordinated by the control plane: every node
must drain the old hash before the new hash starts producing routing
decisions.

### Shards and raft partitions are different things

`shard_map` names the PRG that **serves** a shard. Where that shard's
writes are **logged** is a separate mapping, owned by the substrate:
`partition_router` sends a keyed proposal to `shard % num_partitions`.
The two namespaces are sized from different things — PRG count follows
the node count, partition count is a graph parameter — and an ownership
override must never be read as a partition override: it would send a
shard's writes to a raft group holding none of its history.

A proposal therefore carries its shard explicitly
(`MSG_CLIENT_PROPOSAL_KEYED`, a `[shard_id:u32 LE]` prefix) rather than
leaving the substrate to infer placement from the bytes. Routing is
then stable per key: every publish to one topic lands on one partition
regardless of payload.

### Production tenant sizing

Every tenant owns at least one PRG. Production tenants provision
**at least three PRGs** so that the load of any single PRG can be
absorbed by the others during rebalance or failure. Single-PRG
tenants are dev/test only.

## Placement and rebalance

**Status: design target.** There is no CP-Raft: the `control_plane`
module emits a synthetic proof and a placeholder tenant record on a
timer ([control_plane.md](control_plane.md)), so nothing issues a
placement decision. The substrate also declines to change replica
membership online — `ADD_VOTER` and `REMOVE_VOTER` answer
`ADMIN_STATUS_UNSUPPORTED`, because quorum tracking enforces the old
majority rather than the union of old and new during joint consensus,
and accepting a membership change without union quorum risks losing
committed entries. The sequence below is what the routing model is
built to serve.

CP-Raft assigns PRGs to nodes honouring:

- **Locality** — prefer placements close to the tenant's primary client population.
- **Storage budget** — respect per-node I/O and disk-tier profiles.
- **Replica anti-affinity** — no two replicas of the same PRG land on the same node (and ideally not the same rack).
- **Noisy-neighbour constraints** — keep PRGs of the same tenant spread across nodes when possible.

Membership changes are the substrate's concern.

### Rebalance sequence

1. The control plane issues a placement plan with a new routing
   epoch.
2. New PRGs (or new replicas of existing PRGs) clone via learner
   catch-up — they consume snapshots and WAL frames until they are
   caught up to the leader.
3. The control plane publishes the new routing epoch.
4. Listeners begin admitting sessions / topics that map to the new
   PRG layout.
5. Publishes routed to old PRGs after the epoch flip are rejected
   with `dirty_epoch`.

Steps 2–3 are the long pole — learner catch-up speed depends on
snapshot size and replication bandwidth. The runbook for executing
this is in [guides/scaling.md](../guides/scaling.md).

## Routing guarantees

- Routing decisions are stable within a routing epoch. A session
  that connects in epoch `N` keeps the same PRG for the duration of
  epoch `N` (or until the node hosting that PRG fails over).
- Messages to the same topic on the same PRG are observed in WAL
  order. The `session_processor` apply loop is deterministic per-PRG.
- Cross-PRG ordering is not guaranteed. Shared subscriptions,
  retained messages on cross-PRG topics, and any application logic
  that assumes global ordering will see reordering during rebalance
  and under concurrent publish from multiple PRGs.

For applications that require global ordering for a subject, route
those subjects to a single PRG by tenant configuration.

## Edge routing

Any node accepts any connection. A listener classifies each frame
against its placement view before acting on it, and the classification
is the same on every node and in every protocol:

| Disposition | When | Meaning |
|---|---|---|
| Local | This node's PRG owns the shard | Serve it here |
| Forward | Another PRG owns it | Not this node's to serve |
| Refresh-then-retry | The sender states a NEWER epoch than this node holds | Our map is behind; do not decide from it |
| Redirect | The sender states an OLDER epoch | The sender is acting on a revoked placement |
| Fenced | The shard is mid-transfer | Briefly unavailable, which is the correct answer |

The epoch comparison comes before the ownership test. Deciding
ownership from a view already known to be stale is exactly how a frame
reaches a group that no longer owns it. Every routed frame therefore
carries the epoch it was decided under, and a placement update is
accepted only if it is STRICTLY newer: two placements published at one
epoch would otherwise both apply, the second silently winning, at which
point the epoch no longer identifies a placement at all.

The fence is raised on the PRG a migration is moving, and it stops the
losing owner before it starts the gaining one. A fenced shard is still
owned here — an aborted migration lifts the fence and leaves ownership
where it was — so a fence withholds service without releasing state,
while a reassignment releases it. Release is what keeps a former owner
from answering from a copy the real owner is already diverging from:
sessions, retained messages and queued deliveries for a shard this node
has lost are dropped when the placement update lands.

### How a non-local disposition reaches the client

Today every non-local disposition is answered in the client's own
protocol rather than proxied:

- **Kafka** already implements a metadata-refresh loop, so it is given
  the error code that drives it: `NOT_LEADER_OR_FOLLOWER` for a settled
  elsewhere-owner, `LEADER_NOT_AVAILABLE` for the transient states
  (fenced, or our view is behind), which the client retries. Inventing
  a redirect for a protocol that has one would only pin the client to
  the wrong broker.
- **MQTT** is placed at CONNECT, before the optimistic CONNACK: a
  connection whose session state belongs to another PRG is answered
  `0x9D` "Server moved" (`0x9C` "Use another server" while fenced), or
  CONNACK `0x03` on 3.1.1, the only refusal that version can express.
  Accepting it would be worse — the client would believe it is
  connected and SUBSCRIBE against a session that can never exist.

**Status: transparent forwarding is a design target.** A session
already established on the wrong node should be forwarded to the owner
and its reply proxied back, invisibly, because MQTT and AMQP carry
subscriptions, inflight QoS state and delivery credit on the
connection, and a reconnect destroys all three. Redirecting at CONNECT
is safe because there is no session yet to lose; there is no forwarding
transport for the established case, which is why placement is decided
at connect time.

## Cross-PRG delivery

There is no forwarding hop. Every node applies every publish from the
shared log, so the node holding a subscriber's SESSION always sees the
publish; `topic_engine` delivers only where the session lives.

Session locality — not topic ownership — is the delivery test, and it
is the stricter of the two, which is what makes it safe: exactly one
node holds a given session, so exactly one node delivers, and a
duplicate cannot arise. "Do I own the topic?" is the wrong question,
because the node that must write to the subscriber's socket is the one
holding its session, and at `prg_count > 1` that is usually a different
node.

A subscription is therefore recorded on BOTH the topic's owner and the
session's owner, and nowhere else: the topic's owner is where publishes
arrive and must be matched, the session's owner is the only node that
can write to the subscriber. `MSG_TOPIC_SUBSCRIBE` carries the
session's `stream_hash` for that reason — a local slot index means
nothing on another node. A wildcard subscription cannot belong to one
topic's shard, since it spans topics hashing to many PRGs, so it is
held locally by every PRG and matched against the topics that PRG owns.

Should PRGs ever stop sharing a log, what has to cross is the
DELIVERY, not the publish: a message carrying `stream_hash`, routed to
the session's PRG and consumed by `session_processor` there. Never by
`topic_engine` — a delivery that re-enters publish matching on the far
side is a routing loop, not a delivery.

## Operator surfaces

The rebalance workflow above is driven through the control plane's
admin surface once a real control plane lands; today the implemented
admin operations are the substrate's (`freeze`, `thaw`,
`transfer-leader`, `durability-mode`, `snapshot`). See
[guides/high_availability.md](../guides/high_availability.md) for
the operator-level restart workflow.
