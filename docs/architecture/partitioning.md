# Partitioning & Routing

How Quantum shards tenants across Partition Raft Groups (PRGs), how
the control plane places PRGs on nodes, and how routing decisions
remain consistent across rebalances. Assumes familiarity with the
entities and invariants in [messaging_model.md](messaging_model.md),
in particular **ROUTING-EPOCH**.

**Status: partly wired.** Multi-partition graphs run today —
`partition_router` fans proposals across partitions, `wal_index` and
durability proofs are partition-keyed end to end, and
`forward_coordinator` implements the cross-PRG forward fence. The
control-plane side — placement, rebalance, epoch migration — is the
target design, pending a real control plane
([control_plane.md](control_plane.md)).

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

Sessions and topics hash per tenant:

```
session_partition = hash64(tenant_id, client_id)         % tenant_prg_count
topic_partition   = hash64(tenant_id, normalized_topic)  % tenant_prg_count
```

The same ring serves both. A session and its primary topic land on
the same PRG when their hashes align, and on different PRGs (with
cross-PRG forwarding) when they do not.

The hash algorithm and seed are versioned. Any change requires a
routing-epoch migration coordinated by the control plane: every node
must drain the old hash before the new hash starts producing routing
decisions.

### Production tenant sizing

Every tenant owns at least one PRG. Production tenants provision
**at least three PRGs** so that the load of any single PRG can be
absorbed by the others during rebalance or failure. Single-PRG
tenants are dev/test only.

## Placement and rebalance

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
those subjects to a single PRG by tenant configuration. For
applications that can tolerate per-PRG ordering, the cross-PRG
`forward_seq` is sufficient to detect and reject duplicates during
forwards.

## Cross-PRG forwarding

When `session_partition ≠ topic_partition`, the session PRG forwards
the publish to the topic PRG via `forward_coordinator`. Mechanics:

- `forward_coordinator` tracks
  `(ingress_prg, egress_prg, routing_epoch) → monotone_seq` and
  persists this state inside the PRG snapshot.
- Same-node forwards bypass the network entirely —
  `forward_coordinator.local_out` feeds the destination partition's
  proposal path directly.
- Cross-node forwards leave on `forward_coordinator.remote_out`
  through `peer_router`, sharing connections with regular
  replication traffic.
- On replay, `forward_seq` lets the destination PRG detect and drop
  duplicates from before the failover.

The forwarding path is protocol-agnostic: the forward envelope
carries the `(ingress, egress, epoch, seq)` tuple plus opaque payload
bytes, so MQTT, Kafka, and AMQP all use the same forward primitive.

## Operator surfaces

The rebalance workflow above is driven through the control plane's
admin surface once a real control plane lands; today the implemented
admin operations are the substrate's (`freeze`, `thaw`,
`transfer-leader`, `durability-mode`, `snapshot`). See
[guides/high_availability.md](../guides/high_availability.md) for
the operator-level restart workflow.
