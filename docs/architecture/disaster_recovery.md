# Disaster Recovery and Upgrades

Quantum's disaster-recovery model and how rolling upgrades preserve
durability. The DR primitives are owned by `governance`'s dr component
(orchestration) and Clustor's `durability` (chunked snapshot
export/import, WAL, and the durability ledger). The upgrade path is
mechanically standard for a Raft-replicated state machine; the interesting
properties are the fencing rules that bound the duplicate window
during promotion.

## Scope

Two concerns live here:

1. **DR** — cross-region replication, controlled promotion of a
   standby site after a primary failure, and the bounded-duplicate
   window during unfenced promotion.
2. **Upgrades** — rolling restart, leader transfer, and the
   conditions under which upgrades are safe to perform.

Single-node crash recovery is not DR — it's WAL replay, handled
inherently by Clustor with no orchestration needed. This document
covers loss of a whole site or controlled migration between sites.

## DR model

Cross-region replication is asynchronous and runs on top of Clustor's
snapshot export + WAL archive shipping:

| Component | Role |
|---|---|
| `durability` | Periodic full + incremental snapshots, 1 MiB chunked, AEAD-encrypted, Ed25519-signed. |
| `governance`'s dr component | Schedules checkpoint exports, ships WAL archives with cursor tracking, drives controlled promotion. |
| Cross-region transport | Operator-provided (object storage, blob store, dedicated replication link). Quantum writes archives, operator ships them. |
| Standby site | Imports snapshots + WAL archives, replays into PRG state, ready to promote on demand. |

Defaults:

| Setting | Default |
|---|---|
| `checkpoint_interval_s` | 900 (15 min) |
| `wal_archive_interval_s` | 300 (5 min) |
| `cp_snapshot_interval_s` | 3600 (1 hour) |
| `controlled_promotion` | true |
| `fence_commit_required` | true |

The 5-minute WAL archive interval bounds the duplicate window of an
unfenced promotion (see below).

## XO-BOUND and DR

The **XO-BOUND** guardrail from
[messaging_model.md](messaging_model.md) is the load-bearing rule for
DR. Restated:

> Adapters MUST NOT expose protocol-level exactly-once semantics
> unless both conditions hold: (1) the adapter opts into the
> four-phase durability contract (§9.3 QoS 2), and (2) the cluster
> is healthy or undergoing a controlled, fenced DR promotion.

Translated into operational consequences:

| Cluster state | Exactly-once guarantee |
|---|---|
| Healthy primary | Holds (per ACK-DURABILITY). |
| Single-node crash, replay in progress | Holds — replay reproduces inflight state deterministically. |
| Controlled fenced DR promotion | Holds — `FenceCommit` ensures the old primary cannot ACK new writes while the standby promotes. |
| Unfenced DR promotion (primary lost without fence) | Degrades to at-least-once during the duplicate window. |
| Split-brain (both sites accept writes) | Broken — XO-BOUND violated; remediation is to merge by acceptance order with audit. |

The duplicate window for unfenced promotion is bounded by
`wal_archive_interval_s` plus the in-flight replication delta —
typically minutes. Within that window, clients may observe duplicate
deliveries on QoS 2 / Kafka transactional / AMQP exactly-once paths.

## Controlled promotion

`controlled_promotion = true` (default) requires the operator-driven
sequence:

1. **Verify standby readiness.** `governance`'s dr component reports
   `standby_replication_lag_seconds` on `/admin`; promote only when
   lag is below operator threshold.
2. **FenceCommit on primary.** `operations` issues a fence command
   that stops new writes from being accepted at the primary. Existing
   writes drain.
3. **Verify durability quiescence.** Standby's `durability` ledger
   confirms no new entries past the fence index.
4. **Promote standby.** `governance`'s dr component activates the standby PRG ring;
   `control_plane` bumps routing epoch; CP-Raft publishes the new
   primary identity.
5. **Cut over clients.** L4 / VIP routes new connections to the
   promoted site; old primary serves only durability lookups for
   in-flight ACKs.
6. **Decommission old primary** once no clients remain. The old
   primary becomes the new standby if its state is recoverable.

Steps 2–4 are the load-bearing fence: they guarantee the old primary
cannot ACK a write that the new primary doesn't have, preserving
ACK-DURABILITY across the promotion.

## Unfenced promotion

Unfenced promotion happens when the primary is unreachable and fence
cannot be issued. The standby is promoted without verifying the
primary stopped writing.

Consequences:

- Writes accepted by the old primary after the last shipped WAL
  archive may not appear on the new primary. Clients see those as
  failed publishes and retry.
- Retries land on the new primary, which has no dedupe state for
  them. The result is duplicate delivery within the dedupe TTL
  window.
- `governance`'s audit component records every unfenced promotion with the
  duplicate-window estimate.

Operators should prefer controlled promotion; unfenced is the option
of last resort during true site loss.

## Upgrades

Rolling upgrade is the standard Raft pattern, with one
Quantum-specific consideration: module ABI changes require all nodes
to run the new module set before the new graph YAML can be deployed.

### Compatible upgrade

A compatible upgrade keeps the graph topology unchanged and updates
module implementations:

1. Verify cluster health (`/readyz` ≥ 99%, no fences active, no DR
   backlog).
2. For each node in sequence:
   - Drain the listener via `/admin` (stops new CONNECTs).
   - Transfer PRG leadership away from the node via `operations`.
   - Wait for active-session quiescence (or DISCONNECT with grace).
   - `systemctl stop quantum`; replace `/opt/quantum/modules/*.fmod`
     and `/opt/quantum/bin/fluxor*` as needed;
     `systemctl start quantum`.
   - Wait for `/readyz` green (Clustor replays the WAL).
   - Re-admit at the L4; clear drain.
3. Confirm post-upgrade health.

Per-PRG durability is preserved throughout — as long as two replicas
of every PRG remain up, writes continue to ACK.

### Topology / ABI upgrade

If the new build changes:

- Module ABI (`fluxor` SDK version)
- Graph wiring (new module added, port renamed, edge changed)
- Storage format (WAL frame layout, snapshot schema)

then a rolling restart with mixed versions can deadlock or corrupt
state. For these upgrades:

1. Prepare a maintenance window.
2. Drain the entire cluster.
3. Replace artifacts on all nodes.
4. Restart all nodes simultaneously.
5. Validate via the smoke harness
   ([guides/interop.md](../guides/interop.md)).

Storage-format upgrades additionally require either
backward-compatible read paths (the new module reads both old and new
frames) or an offline conversion step. The fluxor-native build does
not currently include automated format migration; format changes are
infrequent and operator-coordinated.

## Fault injection

Per Clustor §11 + the `test_fault` foundation module, the following
fault classes are exercisable against a running graph:

| Fault class | Exercised by |
|---|---|
| Link drops between peers | `test_fault` + `peer_router` |
| Disk latency (synthetic fsync stalls) | `test_fault` + `durability` |
| CP-Raft outage (cache state transitions) | `test_fault` + `control_plane` |
| PRG relocation mid-publish | `operations` placement plan |
| Module step-time overrun | `test_fault` + scheduler tier limits |

Deterministic replay tests assert **WAL-SOURCE** and
**DETERMINISTIC-REPLAY** invariants for each release. The validation
procedure lives in [guides/performance.md](../guides/performance.md).

## Operator surfaces

- `/admin` — drain, leader transfer, fence command, snapshot trigger, shrink plan, throttle override.
- `governance`'s dr component status on `/admin` — current checkpoint cursor, WAL archive lag, standby replication lag.
- Audit log — every DR event signed and retained for forensics.

The end-to-end operator workflow for rolling upgrade lives in
[guides/high_availability.md](../guides/high_availability.md); the DR
runbook itself is operator-specific and not bundled in this repo.
