# Disaster Recovery and Upgrades

Quantum's disaster-recovery model and how rolling upgrades preserve
durability. The DR orchestration lives in `governance`'s dr
component; the persistence primitives (WAL, snapshots, durability
ledger) are clustor's.

**Status: orchestration skeleton only.** `governance`'s dr component
implements the scheduling state machine — checkpoint cadence, archive
cursors, the promotion gate — but ships no data: the archive
destination is not wired, so its counters advance without moving
bytes, and promotion gating is a state transition rather than an
enforced fence. Single-node crash recovery, by contrast, is real and
needs no orchestration: it is WAL replay, exercised on every restart.
The model below is the target design. **Status: design target, not
wired.**

## Scope

Two concerns live here:

1. **DR** — cross-region replication, controlled promotion of a
   standby site after a primary failure, and the bounded-duplicate
   window during unfenced promotion.
2. **Upgrades** — rolling restart, leader transfer, and the
   conditions under which upgrades are safe to perform.

Single-node crash recovery is not DR — it is WAL replay, handled by
the substrate with no orchestration needed. This document covers
loss of a whole site or controlled migration between sites.

## DR model

Cross-region replication is asynchronous and runs on top of snapshot
export plus WAL archive shipping:

| Component | Role |
|---|---|
| `durability` | Snapshots and the WAL segments that archives are cut from. |
| `governance`'s dr component | Schedules checkpoint exports, tracks archive cursors, drives controlled promotion. |
| Cross-region transport | Operator-provided (object storage, replication link). Quantum writes archives, the operator ships them. |
| Standby site | Imports snapshots + WAL archives, replays into PRG state, ready to promote on demand. |

Scheduling defaults in the dr component:

| Setting | Default |
|---|---|
| `checkpoint_interval_s` | 900 (15 min) |
| `wal_archive_interval_s` | 300 (5 min) |
| `fence_commit_required` | on |

The WAL archive interval bounds the duplicate window of an unfenced
promotion (below).

## Exactly-once and DR

Exactly-once semantics are valid only when the four-phase durability
contract is engaged and the cluster is healthy or undergoing a
controlled, fenced promotion:

| Cluster state | Exactly-once guarantee |
|---|---|
| Healthy primary | Holds (per ACK-DURABILITY). |
| Single-node crash, replay in progress | Holds — replay reproduces inflight state deterministically. |
| Controlled fenced DR promotion | Holds — the fence ensures the old primary cannot ACK new writes while the standby promotes. |
| Unfenced DR promotion | Degrades to at-least-once during the duplicate window. |
| Split-brain (both sites accept writes) | Broken; remediation is merge by acceptance order with audit. |

The duplicate window for unfenced promotion is bounded by the WAL
archive interval plus the in-flight replication delta. Within that
window, clients may observe duplicate deliveries on QoS 2 paths.

## Controlled promotion

The operator-driven sequence:

1. **Verify standby readiness** — promote only when standby
   replication lag is below the operator threshold.
2. **Fence the primary** — stop new writes from being accepted;
   existing writes drain.
3. **Verify durability quiescence** — confirm no entries past the
   fence index.
4. **Promote the standby** — activate the standby PRG ring; bump the
   routing epoch; publish the new primary identity.
5. **Cut over clients** — route new connections to the promoted
   site.
6. **Decommission the old primary** once no clients remain.

Steps 2–4 are the load-bearing fence: they guarantee the old primary
cannot ACK a write that the new primary does not have, preserving
ACK-DURABILITY across the promotion.

## Unfenced promotion

When the primary is unreachable and no fence can be issued, the
standby is promoted without verifying the primary stopped writing.
Writes accepted by the old primary after the last shipped archive
may not appear on the new primary; clients see those as failed
publishes and retry, and the retries land on a broker with no dedupe
state for them. The result is duplicate delivery within the dedupe
TTL window. Prefer controlled promotion; unfenced is the option of
last resort during true site loss.

## Upgrades

Rolling upgrade is the standard pattern for a Raft-replicated state
machine, with one quantum-specific consideration: module ABI changes
require all nodes to run the new module set before the new graph
YAML can be deployed.

### Compatible upgrade

A compatible upgrade keeps the graph topology unchanged and updates
module implementations. For each node in sequence: transfer PRG
leadership away (the `transfer-leader` admin operation), stop the
service, replace the `.fmod` artefacts and runtime binaries, start,
wait for WAL replay to complete, and move to the next node. As long
as a quorum of every PRG remains up, writes continue to ACK
throughout.

### Topology / ABI upgrade

If the new build changes the module ABI, the graph wiring, or a
storage format (WAL frame layout, snapshot schema), a rolling
restart with mixed versions can wedge. For these upgrades: take a
maintenance window, drain the cluster, replace artefacts on all
nodes, and restart together. Storage-format changes additionally
require the new module to read both old and new frames, or an
offline conversion; there is no automated format migration.
