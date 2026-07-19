# Partitioning & Routing

How Quantum shards tenants across Partition Raft Groups (PRGs), how
CP-Raft places PRGs on nodes, and how routing decisions remain
consistent across rebalances. Assumes familiarity with the entities
and invariants in [messaging_model.md](messaging_model.md), in
particular **ROUTING-EPOCH**.

## System model

### Components

| Component | Role |
|---|---|
| **Edge Listener** | TLS termination, SNI / ALPN demux (`tls` + `peer_router` + `protocol_router`); forwards framed packets to the session processor. |
| **Session Processor** | Adapter logic, dedupe, offline queues, Will / retained features. All durable state mutation occurs inside the Raft apply loop. Helpers may precompute, but every mutation commits through the same apply path. |
| **Control Agent** | Per-node helper (subsumed by `cp_bridge` + `tenant_manager` in the fluxor-native build): syncs CP-Raft objects, publishes health, manages certificates, requests rebalance actions. |

### Environment

- Linux ≥ 5.15 with `io_uring`.
- NVMe SSDs with write barriers enabled.
- Clocks synchronised via PHC/PTP. Excessive skew fences all PRGs on
  the affected node from leadership and from emitting any protocol
  ACK that requires quorum durability until clocks recover.

### Assumptions

- Additional PRGs scale linearly with session count and publish load.
- CP-Raft outages shorter than `controlplane.cache_grace_ms` retain
  cached metadata; longer outages trigger strict fallback per
  Clustor §9.1.
- Follower reads inherit the same gating: if Clustor refuses
  `ReadIndex`, adapters fail closed with `ControlPlaneUnavailable`.

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
CP-Raft-coordinated routing-epoch migration: every node must drain
the old hash before the new hash starts producing routing decisions.

### Production tenant sizing

Every tenant owns at least one PRG. Production tenants provision
**at least three PRGs** so that the load of any single PRG can be
absorbed by the others during rebalance or failure. Single-PRG
tenants are dev/test only.

## Placement and rebalance

CP-Raft assigns PRGs to nodes honouring:

- **Locality** — prefer placements close to the tenant's primary client population.
- **NVMe budget** — respect per-node `io_profile` (`latency_sensitive` / `throughput_heavy` / `balanced`) and `disk_tier` (`nvme` / `ssd` / `hdd` / `any`).
- **Replica anti-affinity** — no two replicas of the same PRG land on the same node (and ideally not the same rack).
- **Noisy-neighbour constraints** — keep PRGs of the same tenant spread across nodes when possible.

Membership changes follow Clustor §9.9.

### Rebalance sequence

1. CP-Raft issues a placement plan with a new routing epoch.
2. New PRGs (or new replicas of existing PRGs) clone via Clustor
   learner catch-up — they consume snapshots and WAL frames until
   they're caught up to the leader.
3. CP publishes the new routing epoch.
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
  `forward_coordinator.local_forward` emits directly to
  `raft_engine.proposals` on the destination PRG's apply core.
- Cross-node forwards go through `peer_router.repl_tx`, framed
  identically to Raft AppendEntries, so they share TLS sessions and
  connection-pool slots with regular replication traffic.
- On replay, `forward_seq` lets the destination PRG detect and drop
  duplicates from before the failover.

The forwarding path is protocol-agnostic: the envelope
(`WorkloadForwardEnvelope`) carries a schema ID plus the payload
bytes and capability bits, so MQTT, Kafka, and AMQP all use the same
forward primitive.

## Operator surfaces

- **Routing snapshot.** `http_surface` `/admin` lists the current epoch, placements, and CP cache freshness.
- **Drain a node.** `admin_handler` accepts a drain command that stops session admission but keeps existing sessions alive; combine with leader transfer for rolling restarts.
- **Move a PRG.** Issue a placement plan via `admin_handler`; CP-Raft validates anti-affinity and emits a new epoch.

See [guides/high_availability.md](../guides/high_availability.md) for
the operator-level workflows that exercise these surfaces.
