# Scaling

How to grow (and shrink) the cluster's node count and Partition Raft
Group (PRG) count, keeping three replicas and three voters per PRG
throughout.

**Status: the migration control surface is wired; the work each phase
names is not.**

Shard placement is key-based and every routing module resolves shards
through the same map, so a migration has something real to move. The
migration itself is driven through the admin surface — `POST
/admin/migrate-begin`, then `/admin/migrate-phase-done` once per phase
— and walks:

```
PLANNED → PROVISIONING → COPYING → CATCHING_UP → FENCED → ACTIVE → RETIRING → DONE
```

Three properties hold across that walk. Each phase record is committed
through Raft before that phase's work may begin, so a controller that
dies mid-migration rebuilds the migration from the log on restart and
carries on from the phase last recorded. `/admin/migrate-abort` is
refused once the migration has fenced — past that point the move is
one-way, because both ends have already agreed to stop serving the old
placement. And the FENCED phase raises a routing fence that listeners
honour at both ends of the move, so the losing PRG stops accepting
before the gaining one starts.

What is missing is everything the phases are named after:

1. **No per-phase work.** The state machine sequences the phases and
   enforces their safety rules, but activating a slot, installing a
   snapshot and streaming a WAL tail are not implemented — so
   `migrate-phase-done` asserts the completion of work nothing
   performed.
2. **No online membership change.** `ADD_VOTER` and `REMOVE_VOTER`
   answer `ADMIN_STATUS_UNSUPPORTED`. The joint-consensus state machine
   applies config entries, but quorum tracking enforces only the *old*
   majority rather than the union of old and new, and accepting a
   membership change without union quorum risks losing committed
   entries — so refusing is the safe answer. Learner creation and
   catch-up are likewise design targets.
3. **No automation.** Migration commands are accepted only by the Raft
   leader, so there is exactly one controller, but an operator still
   drives every phase by hand.

The runbooks below are the operating model the partitioning design
([partitioning.md](../architecture/partitioning.md)) is built for.

## Model

- Every PRG has 3 replicas, all voters.
- Traffic moves only by routing-epoch change: the control plane
  emits an epoch event, `session_processor` fences sessions on the
  old epoch, and stale-epoch publishes are rejected.
- New nodes ship the same TLS trust material as the existing
  cluster.

Inputs to a scaling decision: current and desired node count
(N → N′), current and desired PRG count (P → P′), per-tenant load,
and a placement template (3 replicas across distinct nodes). Size
the target with `P′ = ceil(total_load / target_per_prg_load)`.

## Pre-flight (all must hold)

- Control-plane cache is Fresh on every node; strict-fallback is
  inactive.
- All PRGs ready; replication and apply lag under thresholds.
- The routing epoch is monotone and observed by every listener.

## Scaling up

Grow capacity first, then define and bootstrap new PRGs, then move
traffic — so quorum is never at risk.

1. **Add nodes (N → N′), one at a time.** Verify each new node's
   identity and reachability. Placements are unchanged at this
   point; existing PRGs stay put.
2. **Define new PRGs.** Choose new PRG ids and three distinct nodes
   for each — bias toward new nodes to offload, or spread across
   old and new for resilience. Create placement records at
   `routing_epoch = current + 1` (published later).
3. **Bootstrap new PRGs.** Start their Raft groups on the chosen
   placements; wait for leader election and replication readiness.
4. **Publish the new epoch.** Bump `routing_epoch` and publish
   placements for all PRGs. Verify every node ingests the new
   epoch. Listeners then admit sessions and topics mapping to the
   new PRGs.
5. **Rebalance traffic.** Direct new tenants and topics to the new
   PRGs first. To move existing ones, update their placements and
   bump `routing_epoch` per move, in small batches to bound the
   fence window.
6. **Validate.** Check replication lag, cache state, routing epoch,
   and placement count after each step; roll back an epoch bump
   that does not reach Fresh everywhere within a timeout.

## Scaling down

Shrinking is explicit and operator-initiated: it requires an armed
shrink plan held in control-plane state, so a routine restart can
never trigger an accidental shrink. High-level flow:

1. Pre-flight as above.
2. Freeze new placements on the PRGs to be removed (mark them
   draining).
3. Migrate tenants and topics to the surviving PRGs in small
   batches, bumping `routing_epoch` per batch, until the draining
   PRG is empty.
4. Delete the empty PRG's placement and bump `routing_epoch`.
5. Only after the epoch is stable everywhere, remove or repurpose
   the freed nodes.

Roll back if caches do not reach Fresh after an epoch bump, or if
any PRG falls out of ready.
