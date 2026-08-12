# Scaling

How to grow (and shrink) the cluster's node count and Partition Raft
Group (PRG) count with no or near-zero downtime, keeping 3 replicas and
3 voters per PRG throughout. Traffic moves only by routing-epoch change:
`control_plane` emits an epoch event, `session_processor` fences
sessions on the old epoch, and stale-epoch publishes are rejected with
`dirty_epoch`. Each step below states the health predicates that gate
it, so the runbook is equally usable by an operator or an automation
supervisor.

## Model

- Every PRG has 3 replicas, all voters.
- The control plane is embedded (the only mode in the fluxor-native
  build) and exposes routing/placements and `/readyz` via
  `gateway`.
- New nodes ship the same TLS trust bundle as the existing cluster.
- Raft transport pools per-peer connections in `peer_router`, so
  multiple PRGs on a node share the same TLS sessions to peers.

Inputs to a scaling decision: current and desired node count (N → N′),
current and desired PRG count (P → P′), per-tenant PRG overrides and
total tenant load, and a placement template (3 replicas across distinct
nodes/AZs). Size the target with `P′ = ceil(total_load / target_per_prg_load)`.

## Pre-flight (all must hold)

- Control-plane cache is Fresh on every node; strict-fallback is false.
- All PRGs ready; replication and apply lag under thresholds; the
  durability fence is inactive.
- The TLS trust bundle is readable on all nodes.
- The routing epoch is monotone and observed by every listener.

## Scaling up

Grow capacity first, then define and bootstrap new PRGs, then move
traffic — so quorum is never at risk.

1. **Add nodes (N → N′), one at a time.** On each new node verify
   `/readyz`, TLS identity in the correct trust domain, and
   reachability to the control-plane endpoints. Placements are unchanged
   at this point; existing PRGs stay put.
2. **Define new PRGs.** Choose new PRG ids (e.g. tenant `default` gains
   partitions P..P′−1) and 3 distinct nodes for each — bias toward new
   nodes to offload, or spread across old+new for resilience. Create
   placement records at `routing_epoch = current + 1` (published later).
3. **Bootstrap new PRGs.** Start their Raft groups on the chosen
   placements; wait for leader election and replication readiness
   (commit index advancing, no fences, low lag). Confirm the CP
   placement caches contain the new PRGs and are Fresh.
4. **Publish the new epoch.** Bump `routing_epoch` by 1 and publish
   placements for all PRGs. Verify every node ingests the new epoch
   (cache Fresh, epoch matches). Listeners now admit sessions/topics
   mapping to the new PRGs.
5. **Rebalance traffic.** Direct new tenants/topics to the new PRGs
   first. To move existing tenants/topics, update their placements and
   bump `routing_epoch` per move, in small batches to bound the fence
   window. After each move, confirm caches Fresh everywhere, PRGs ready,
   strict-fallback false.
6. **Validate.** Check replication/apply lag, `/readyz`, cache state,
   routing epoch, and placement count. Roll back a `routing_epoch` bump
   if caches do not reach Fresh within a timeout, or if any PRG falls
   out of ready.

## Scaling down

Shrinking is explicit and operator-initiated — it requires an armed
shrink plan so that a routine restart never triggers an accidental
shrink or rebalance. A shrink plan carries a plan id and target
placements and lives in control-plane state; supervisors reject shrink
operations unless a plan is armed.

High-level flow (example: 4 PRGs → 3, keeping 3 replicas/voters each):

1. Pre-flight as above: caches Fresh, strict-fallback false, all PRGs
   healthy, lag below thresholds.
2. Freeze new placements on the PRG(s) to be removed (mark them draining
   in the control plane).
3. Migrate a small tenant/topic batch to the target PRGs, bump
   `routing_epoch`, wait for Fresh everywhere; repeat until the draining
   PRG is empty.
4. Once empty, adjust the draining PRG's Raft membership if needed
   (still 3 voters until removal), delete its placement, and bump
   `routing_epoch`.
5. Only after the epoch is stable and caches Fresh, remove or repurpose
   the freed nodes.

Roll back if caches do not reach Fresh after an epoch bump, or if any
PRG falls out of ready.

### Shrink admin API (mTLS)

Shrink plans are managed through Clustor's `operations` admin surface,
which `full.yaml` and `pi5.yaml` reach via
`gateway.admin_req → operations.admin_req`, returning on
`operations.responses → gateway.admin_responses`.

| Endpoint | Action |
|---|---|
| `POST /admin/shrink-plan` | Create a plan |
| `POST /admin/shrink-plan/arm` | Arm a plan (one at a time) |
| `POST /admin/shrink-plan/cancel` | Cancel / roll back |
| `GET /admin/shrink-plan` | List plans |

Create payload:

```json
{
  "plan_id": "shrink-p4",
  "target_placements": [
    {
      "prg_id": "tenantA:3",
      "target_members": ["node-a", "node-b", "node-c"],
      "target_routing_epoch": 42
    }
  ]
}
```

Arm with `{"plan_id":"shrink-p4"}` (fails if another plan is already
armed); cancel with the same body (state becomes `RolledBack` if it was
armed). While armed, the control-plane routing publication substitutes
the target placements and advertises the plan id; migrate tenants/topics
and bump `routing_epoch` only while the plan is armed, then cancel or
mark done and remove nodes. Shrink metrics surface via CP
(`cp.shrink_plans.total`, `.armed`, `.cancelled`).
