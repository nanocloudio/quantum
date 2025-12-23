# Autoscale Upward Runbook (PRG = Partition Raft Group)

Goal: grow the cluster node count and PRG count with no/near-zero downtime, keeping 3 replicas and 3 voters per PRG. This is structured so it can be automated; each step notes the checks and state needed for intelligent scaling.

## Assumptions
- Each PRG always has 3 replicas (all voters).
- Control plane is embedded or external but exposes routing/placements and readyz.
- TLS trust domain is consistent across nodes; new nodes ship the same cert materials.
- Routing epoch changes are the only way to move traffic; listeners reject stale epochs.
- Raft transport uses per-peer connection pooling/multiplexing (clustor transport) so multiple PRGs on a node can share the same TLS sessions to peers, keeping FD/handshake overhead low.

## Inputs for an automation loop
- Current node count N; desired node count N'.
- Current PRG count P; desired PRG count P' (P' > P for scale-up).
- Per-tenant PRG count (if overrides exist) and total tenant load.
- Placement template: 3 replicas spread across distinct nodes/AZs.
- Health signals: cp cache fresh, PRG ready, replication/apply lag, strict-fallback=false.

## Pre-flight checks (block automation if any fail)
- Control-plane cache is Fresh on all nodes; strict-fallback is false.
- All PRGs ready; replication/apply lag under thresholds; durability fence inactive.
- TLS/trust bundle readable on all nodes.
- Routing epoch monotone and observed by listeners.

## Phase 1: Add node capacity
1) Increase StatefulSet/VM group from N→N' (one at a time).  
2) On each new node: verify readyz/livez, TLS identity in correct trust domain, and ability to reach control-plane endpoints.  
3) Do not change placements yet; PRGs stay on existing nodes, so no quorum risk.

## Phase 2: Define new PRGs
1) Choose new PRG ids (e.g., tenant `default` gains partitions P..P'-1).  
2) Select 3 distinct nodes for each new PRG (prefer spreading across old+new nodes if you want higher resilience; otherwise bias to new nodes for load offload).  
3) Create placement records for each new PRG with routing_epoch = current_epoch+1 (will publish after bootstrap).

## Phase 3: Bootstrap new PRGs
1) Start the Raft groups for the new PRGs using their 3-node placements.  
2) Wait for leader election and replication readiness: commit index advancing, no fences, replication lag low.  
3) Confirm control-plane placement caches contain the new PRGs and are Fresh.

## Phase 4: Publish new routing epoch
1) Bump routing_epoch by 1 and publish the placements for all PRGs (existing + new).  
2) Verify all nodes ingest the new epoch (cache Fresh, epoch matches).  
3) Listeners should now admit sessions/topics that map to the new PRGs.

## Phase 5: Rebalance traffic
1) Direct new tenants/topics to the new PRGs first (minimizes churn).  
2) If moving existing tenants/topics, update their placements and bump routing_epoch each time; move small batches to limit fence windows.  
3) After each move: ensure cp cache Fresh everywhere, PRGs ready, strict-fallback=false.

## Phase 6: Validation and steady state
- Check metrics: replication lag, apply lag, readyz, cp cache_state, routing_epoch, placement_count.  
- Audit logs for rebalance events (if enabled).  
- When stable, mark rebalancing complete.

## Automation cues (hook points)
- Trigger scale-up when lag/queue depth cross thresholds or when tenant/PRG utilization exceeds target.  
- Use a planner: compute P' = ceil(total_load / target_per_prg_load).  
- Place replicas with a spread policy (node/AZ anti-affinity).  
- Gate each phase on health predicates above; auto-roll back a routing_epoch bump if caches do not reach Fresh within a timeout.

## Scaling down (explicit, operator-initiated)
- Require an explicit “shrink” command/intent so restarts do not auto-shrink or rebalance by accident. Treat shrink as a change request with a plan id.
- High-level flow with clustor shrink plans (example: 4 PRGs → 3 PRGs, keeping 3 replicas/voters per PRG):
  1) Pre-flight: caches Fresh, strict-fallback=false, all PRGs healthy, lag below thresholds.
  2) Freeze new placements on the PRG(s) to be removed (mark them draining in control plane).
  3) Choose target PRGs for migration; update placements for a small tenant/topic batch to target PRGs, bump routing_epoch, wait for Fresh everywhere; repeat until draining PRG empty.
  4) Once empty, shrink the draining PRG’s Raft group membership if needed (still 3 voters until removal), then delete its placement and bump routing_epoch.
  5) Only after the routing_epoch is stable and caches Fresh, remove the nodes that are no longer needed (or repurpose them).
- Automation hooks: the shrink intent should live in control-plane state (plan id + target PRGs) so supervisors reject shrink operations unless explicitly armed. Roll back if caches do not reach Fresh after an epoch bump, or if any PRG falls out of ready.
- Clustor shrink support (admin HTTP):
  - Endpoints (mTLS): `POST /admin/shrink-plan` (create), `POST /admin/shrink-plan/arm`, `POST /admin/shrink-plan/cancel`, `GET /admin/shrink-plan` (list). One plan can be armed at a time.
  - Example create payload:
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
  - Arm the plan: `POST /admin/shrink-plan/arm` with `{"plan_id":"shrink-p4"}`. Arm fails if another plan is already armed.
  - Cancel/roll back: `POST /admin/shrink-plan/cancel` with `{"plan_id":"shrink-p4"}`; state becomes RolledBack if it was armed.
  - Effects: When armed, the control-plane routing publication substitutes the target placements and advertises the plan id; shrink plan metrics surface via CP metrics (`cp.shrink_plans.total`, `cp.shrink_plans.armed`, `cp.shrink_plans.cancelled`).
  - Operators/automation should only migrate tenants/topics and bump routing_epoch while a plan is armed; once migrations complete and caches are Fresh, cancel/roll back or mark done and then remove nodes.
  - Embedded CP in Quantum is a minimal server (routing/features only); shrink admin endpoints require an external/real CP admin surface.
