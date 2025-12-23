# QuantuMQTT – Distributed Raft-Based Multi-Tenant MQTT Broker
Version: Draft 1.0  
Language: Rust (no GC runtime)  
Deployment: Single-binary, self-arranging, StatefulSet-style cluster  

---

## Abstract

QuantuMQTT is a cloud-grade, fault-tolerant, Raft-replicated MQTT broker engineered for one hundred million concurrent persistent connections with MQTT QoS 2 exactly-once semantics (within the broker) and deterministic recovery.  
It supports MQTT v3.1, v3.1.1, and v5.0 over both TLS/TCP and QUIC/HTTP-3 transports.  
The design ensures 99.999 % availability, zero-loss durability for Strict partitions (the default), and multi-tenant isolation with dynamic scaling, zero-downtime upgrades, and cross-region disaster recovery.

Each partition of MQTT state is maintained by an independent Raft group (PRG) with synchronous quorum replication on NVMe storage.  
A lightweight control plane (CP-Raft) governs cluster membership, tenant configuration, quotas, security material, and routing metadata.  
The system achieves deterministic replay, strict ordering within topic partitions, and full auditability for compliance and forensic reconstruction.

---

## Summary Table

| Property | Target / Guarantee |
|-----------|-------------------|
| **Concurrent connections** | 100 million (global, predominantly idle/light telemetry) |
| **Max per tenant** | 1 048 576 |
| **Publish / Deliver rate** | ≈ 500 000 TPS aggregate (QoS 2) at r≈0.5; stretch ≈ 1 000 000 TPS only with r ≤ 0.3 or higher per-PRG budget / 500 msg/s per PRG |
| **Payload sizes** | Avg 8 KiB / Max 16 MiB |
| **Latency (p99)** | ≤ 10 ms in-AZ, single-PRG path (cross-PRG budgeted separately) |
| **Availability** | 99.999 % |
| **RPO** | 0 (fenced DR) / ≤ replication lag if region lost without fence |
| **RTO** | ≤ 2 hours (region failure) |
| **Durability ack** | After quorum fsync commit (Strict default and only mode with the no-ACK-loss guarantee; Group-fsync is opt-in, weaker, non-default) |
| **Transport** | MQTT 3.1 / 3.1.1 / 5.0 over TLS and QUIC |
| **Auth** | mTLS (per tenant or provider CA) |
| **Exactly-once** | MQTT QoS 2 within broker in-region and during controlled DR; uncontrolled DR = at-least-once within replication lag |
| **Multi-tenancy** | Hard isolation, per-tenant quotas |
| **Scaling unit** | 3-voter Raft Partition (PRG) |
| **Sessions per PRG** | Target 50 k  / Cap 75 k |
| **Control plane** | 3-voter CP-Raft cluster |
| **Storage** | NVMe (local) + optional object archive |
| **Deployment** | Single binary on Kubernetes StatefulSet-like |
| **Upgrades** | Rolling, zero-downtime |
| **Audit retention** | 12 months |
| **Trace retention** | 30 days |
| **Session TTL** | 72 h default / 7 d max |
| **Dedup horizon** | 72 h default / 7 d max |
| **Keep Alive** | 300 s (default) |
| **Security** | AES-GCM at rest, TLS 1.3/QUIC TLS, Ed25519 signatures |
| **DR lag target** | ≤ 5 s steady / ≤ 30 s peak |
| **CPU efficiency** | Design 10 µs/publish; v1 must-meet ≤ 60–80 µs/publish (per replica, with TLS/Raft/WAL); stretch ≤ 35–50 µs |
| **Storage amp** | ≤ 1.2 × payload bytes |
| **Network amp** | ≤ 1.1 × payload bytes |
| **Power target** | ≤ 0.25 W / 10 k idle conn |
| **Observability** | Prometheus + OTel tracing |
| **Runbooks** | Summary in Appendix A; full ops/tuning/troubleshooting live in ops handbook |

Table values are operational targets/overlays; Clustor governs all durability and consistency semantics.
MQTT version support: 3.1, 3.1.1, and 5.0; MQTT 5-only features (reason codes, Enhanced Auth, Receive Maximum) degrade as described in §10.7.

---

# 0  Specification Provenance and Normative Scope
[Normative] QuantuMQTT is an overlay on Clustor and inherits Clustor’s normative hierarchy and tagging model. Clustor remains the sole authority for consensus, durability, compaction, strict-fallback, cache freshness, integrity, and DR semantics; any MQTT statement is subordinate to Clustor §§0, 3, 6, 8, 9, and 11.
[Operational] Paragraph tags follow Clustor conventions: **[Normative]** (must implement), **[Operational]** (runbook/operational policy), **[Informative]** (descriptive background).
[Normative] All MQTT-layer guarantees must cite the controlling Clustor clause (e.g., “per Clustor §3.4/§6.5”) where applicable; no “silent semantics” are implied by descriptive text.
[Normative] Determinism and replay are governed by Clustor WAL/snapshot ordering; MQTT must not introduce alternate durable channels, ordering sources, or speculative state outside Clustor WAL entries and manifests.
[Normative] Vocabulary registry (MQTT runtime nouns):
- `session_epoch`: Monotone counter fencing session state; increments on clean reconnect/takeover.
- `dedupe table`: Product-state map keyed `(tenant_id, client_id, session_epoch, message_id, direction)` used for QoS 2 replay protection.
- `earliest_dedup_index_union`: Lowest Raft index across session/topic PRGs required to reconstruct dedupe state.
- `forward_seq`: Monotone per (session PRG, topic PRG epoch) idempotence key for cross-PRG forwards.
- `ForwardPublish` / `TopicPublish` / `PublishAcked`: Clustor log entries representing QoS 2 cross-PRG phases.
- `offline_queue`: Persistent per-session queue for offline delivery.
- `earliest_offline_queue_index`: Lowest Raft index required to reconstruct offline-queue entries retained in snapshots/WAL.
- `effective_product_floor`: `max(clustor_floor, earliest_dedup_index_union, earliest_offline_queue_index)`; product-level compaction blocker.
- `routing_epoch`: Clustor routing epoch controlling PRG selection.
- `read_gate.can_serve_readindex`: Clustor predicate gating freshness-dependent reads.
- `MQTT-01` and `MQTT-DET-01`: invariants defined in §1.8 and §21.1.
- `dirty epoch`: Alias for Clustor `RoutingEpochMismatch`/`ModeConflict` envelopes indicating stale routing epoch.

# 1  Overview

QuantuMQTT unifies a horizontally-scalable MQTT message fabric with a strongly-consistent Raft replication core.  
It aims to deliver industrial-scale connectivity for IoT, telemetry, financial, and mission-critical workloads while preserving sub-10 ms latency and strict message ordering within each topic shard. Exactly-once QoS 2 semantics hold within an in-region healthy quorum and during controlled DR promotions; uncontrolled regional failover deliberately relaxes to at-least-once within the replication-lag window.
This overview describes product intent; all durability/consistency semantics remain those of Clustor.

### 1.0 Normative Scope (per Clustor §0)
[Normative] QuantuMQTT is purely an overlay on Clustor; all consensus, durability, read-gate, compaction, strict-fallback, cache, and DR semantics come directly from Clustor §§0, 3, 6, 8, 9, and 11. MQTT text may tighten but never relax those guardrails.

### 1.1 Goals
* Deterministic persistence/replay for all QoS; exactly-once with RPO 0 when fenced.  
* Multi-tenant isolation and quota enforcement with live upgrades and cert rotation.  
* Resource efficiency: ≤ 4 KiB/idle connection; CPU design 10 µs/publish, must-meet ≤ 60–80 µs (stretch 35–50).  
* Full observability/audit plus mid-session re-auth for long-lived sessions (§9.2).

### 1.2 Architecture Summary
Layers: Control Plane (ControlPlaneRaft, “CP-Raft” as alias, for metadata/tenant config/ACLs/quotas/certs/routing), Data Plane PRGs (session state, inflight maps, retained/subscription index, WAL), and Edge Listeners (TLS 1.3 + QUIC with SNI/ALPN routing and mTLS). Every pod hosts all three in one binary; PRGs/ControlPlaneRaft are Clustor partitions (3 voters each) with leader affinity for SP/WAL.

### 1.3 Target Workloads
- IoT/telemetry fleets, mobile/edge QUIC, financial lossless QoS 2, multi-tenant SaaS.  
- 100 M connection scale assumes idle/light telemetry; chatty QoS 2 workloads require proportionally more PRGs/nodes.

### 1.4 Design Philosophy
* **Linearizable per partition; durability before ACK.**  
* **Isolation by design** with deterministic, bounded-latency execution.  
* **Platform clarity:** Linux + io_uring; TCP/TLS mandatory, QUIC optional.  
* **Single-binary simplicity** to reduce coordination and drift.

### 1.5 Component Roles
Edge Listener (auth/routing), Session Processor (MQTT state/QoS), Replication Engine (Raft/WAL/snapshot), Storage Manager (segments/checkpoints/archive), Control Agent (CP reconcile/health/rebalance), Metrics/Audit service.

### 1.6 Normative References
MQTT 3.1.1, MQTT 5.0, Raft (Ongaro & Ousterhout 2014), QUIC RFC 9000 / HTTP/3 RFC 9114, TLS 1.3 RFC 8446.
MQTT 3.1.1 is the baseline wire behaviour; MQTT 3.1 is supported for legacy interop (CONNECT/return-code differences only) and should be avoided for new deployments in favor of 3.1.1 or 5.0.

### 1.7 Terminology
Terminology: **PRG** (3-voter Raft partition), **SP** (per-partition MQTT state machine), **CP** (control-plane Raft cluster), **Tenant** (isolated namespace), **ClientID** (session ID bound to mTLS identity), **QoS** (0/1/2), **WAL** (write-ahead log), **Checkpoint** (persistent snapshot).

### 1.8 Dependence on Clustor Consensus Core
[Normative] PRGs are Clustor partitions; CP-Raft is ControlPlaneRaft. All WAL/snapshot/ledger/scrub/backpressure/strict-fallback/observer behaviour applies unmodified (Clustor §§0, 3, 5–9, 11). If wording here seems to differ, treat QuantuMQTT as “Clustor + MQTT overlays” and defer to Clustor. QuantuMQTT configures profiles and APIs only.

| QuantuMQTT term | Clustor term / section |
|-----------------|------------------------|
| Partition Raft Group (PRG) | Raft partition (§§0.2, 3, 4) |
| CP-Raft | ControlPlaneRaft (§11) with MQTT objects |
| Strict / Group-fsync | `durability_mode` / `DurabilityTransition` (§6.1–§6.2, §3.4 ACK) |
| `PERMANENT_EPOCH` fence | `RoutingEpochMismatch` / `ModeConflict` (§0.3, §11.1) mapped in §10.5 |
| Durability fence | Strict-fallback gate (§0.5, §3.1.1) → MQTT `PERMANENT_DURABILITY` |
| WAL / checkpoint layout | WAL, durability ledger, manifests, scrub (§§5–9) |
| Flow control / backpressure | PID/throttle envelopes (§10) → MQTT in §13 |
| Control-plane degraded/TTL | Cache state + strict-fallback (§§0.5, 3.3, 11.1) surfaced in §8.7 |

Operational defaults (e.g., Latency/ConsistencyProfile with 3 voters) use Clustor profiles unless explicitly overridden by MQTT policy.

#### Forbidden behaviours (MQTT overlay)
- No local session-cache or ACL reads when Clustor read gates fail; freshness-bound reads must fail closed (`PERMANENT_DURABILITY`) per Clustor §3.3/§0.5.  
- No ack-before-commit, even if only the topic PRG commits; MQTT ACKs wait for Clustor §3.4/§6.5 on all relevant log entries.  
- No dedupe-driven compaction that weakens Clustor’s mandatory floor; product-level floors only raise bounds (Clustor §8).  
- No DR promotion without Clustor `FenceCommit` or durability-ledger/manifest verification (§11.2).  
- No MQTT auth/ACL evaluation using stale CP cache beyond Clustor windows or while CP is `Expired` (Clustor §11).  
- No alternate durability modes beyond Clustor profile selection; Strict remains the default and sole mode carrying the no-ACK-loss guarantee (Clustor §6.1/§6.2).

**MQTT-01 invariant:** All MQTT correctness properties (ordering, exactly-once, replay) derive solely from Clustor log entries and snapshot manifests. MQTT MUST NOT introduce parallel durable channels, alternative commit paths, or speculative state beyond what Clustor records.
Tenant policy or feature tiers may tighten limits but can never relax Clustor guardrails (durability mode, strict-fallback, read gates, nonce/manifest rules, observer/lease gating).

---

# 2  Partitioning and Routing

## 2.1  Sharding Model
QuantuMQTT partitions each tenant’s MQTT state into independent Raft groups called **Partition Raft Groups (PRGs)**.  
PRGs are **single-tenant**: a PRG’s WAL/snapshots only contain one tenant’s data; `tenant_id` is tracked in CP metadata, not inside the PRG keyspace, so keys stored on disk omit tenant identifiers.
Sharding and routing described here are MQTT overlay policies; they do not alter Clustor partition semantics, routing epochs, or placement guarantees.

Canonical partition functions (tenant-local rings):
```
session_partition = hash64(tenant_id, client_id) % tenant_prg_count
topic_partition   = hash64(tenant_id, topic_normalized) % tenant_prg_count
```

- `session_partition` pins all session-bound state, inflight maps, dedupe tables, and offline queues to one PRG per (tenant, client_id).  
- `topic_partition` defines the topic-partition owning ordering, retained store, and subscription index for a specific topic.  
- A given (tenant, topic) maps to exactly one `topic_partition`; topics are never striped across multiple PRGs.  
- Both hashes map into the same tenant-local ring (`tenant_prg_count`), so routing is a pure function of `tenant_id` plus the relevant key; no cross-tenant colocation ever occurs.  
- PRG identity is `(tenant_id, partition_index)`; CP-Raft maintains this map and routes only within the tenant ring.  
- Per-session ordering is guaranteed within `session_partition`; per-topic ordering is guaranteed within `topic_partition`.  
- Scaling for a tenant is achieved by increasing `tenant_prg_count`; CP reassigns `(tenant_id, partition_index)` pairs while keeping the hash algorithm stable, and the larger ring intentionally moves ≈`1/tenant_prg_count` of keys.  
- When `session_partition != topic_partition`, the session PRG forwards publish ops to the owning topic PRG and only ACKs after that PRG commits; cross-PRG hops are counted in latency budgets and traced.

Hashing uses a jump-consistent tenant ring; increasing `tenant_prg_count` moves ≈`1/tenant_prg_count` of keys. CP issues redirects and migration hints; session state is replayed on the destination before redirect.

Expected cross-PRG ratio: typically 50–80 % session→topic→session. Tenants with higher ratios should provision more PRGs or co-locate hot topics near dominant session keys; hot single topics are not auto-split and must be sharded by the tenant.
Operational guidance: monitor `cross_prg_ratio` per tenant. Sustained `r > 0.6` should trigger topic naming/placement tuning; `r > 0.7` should trigger re-sharding (increase `tenant_prg_count`) or co-location of hot topics with dominant sessions to preserve throughput/latency budgets.

### 2.1.1  Partition Count
100 M active connections at 50,000 sessions/PRG ⇒ ~2,000 PRGs (~6,000 voters); each PRG leader hosts its primary SP.

`tenant_prg_count` per tenant = `ceil(max_sessions_per_tenant / 50,000)` and `ceil(max_publish_rate_per_tenant / 500 msg/s)` (from §14.2), capped by placement guardrails in §14.2/14.5.

### 2.1.2  Placement
Leaders/followers sit on distinct nodes, spread across ≥2 AZs (prefer 3) with balanced leader/follower mix. Default guardrail is one migration at a time; per-tenant emergencies may run up to `min(4, tenant_prg_count/20)` within utilisation caps.

### 2.1.3  Leader Affinity
Session Processors (SPs) execute colocated with their Raft leader.  
Follower SPs replay logs continuously in standby mode to enable sub-second failover.

### 2.1.4  Rebalance Protocol
- CP increments the tenant rebalance epoch and pre-seeds destination PRGs with session/topic state via replay before any redirect is issued.  
- For session moves, the source marks sessions `MIGRATING`, fences inflight via `session_epoch++`, drains/drops unacked QoS 2 entries from the old epoch, and only then advertises redirect. Offline queues and dedupe tables move with the checkpoint/WAL stream so the destination resumes deterministically.  
- Dual-routing/redirect grace is capped at 10 minutes; after that the old PRG rejects with `PERMANENT_EPOCH` to prevent flip-flop across DNS/LB caches. Edge caches include the epoch in their key to avoid prolonged double work.  
- Cross-PRG QoS 2 forwards (`forward_seq`) include the rebalance epoch in their idempotence key; the destination topic PRG treats stale-epoch forwards as duplicates to avoid duplicate delivery during the cutover.  

### 2.1.5  Routing & Rebalance Invariants (Normative)
| Invariant | Required property |
|-----------|-------------------|
| SessionEpoch fencing | `session_epoch++` MUST fence inflight QoS 2 state before redirect; prior inflight state MUST NOT be reused. |
| ForwardSeq continuity | `forward_seq` MUST be monotone per (session_PRG, topic_PRG, routing_epoch) and treated as duplicate if epoch-stale. |
| Redirect grace | Dual-routing window MUST NOT exceed 10 minutes; after expiry, PRG MUST emit `PERMANENT_EPOCH` for stale routes. |
| Routing authority | PRG selection MUST reference Clustor `routing_epoch`; no local cache or hint may override it. |
| No cache bypass | During rebalance, no local-session cache read may bypass Clustor read gates; freshness checks apply uniformly. |

### 2.1.6  PRG Mapping Implementation
Mapping is deterministic: `partition_index = hash64(tenant_id, client_id|topic_normalized) % tenant_prg_count`; ControlPlaneRaft maintains the map from `(tenant_id, partition_index)` to the physical PRG/placement. There is no additional indirection layer beyond this logical slot→PRG map.

---

## 2.2  Tenant Routing
Each tenant is identified by its SNI/ALPN tuple during handshake:

| Field | Example | Purpose |
|--------|----------|----------|
| SNI hostname | `tenant123.mqtt.example.com` | Virtual hosting |
| ALPN | `mqtt`, `mqttv5`, `mqtt-quic` | Protocol negotiation |
| QUIC Connection ID | Encodes shard hint | Sticky routing |

Routing occurs as follows:
- Edge validates mTLS/token, extracts `tenant_id` from SNI, computes `session_partition`, and redirects to the PRG leader; cross-PRG publishes forward to the owning topic PRG before ACK (§4.6).

### 2.2.1  Routing Updates
Leader changes stream to edges (< 1 s); caches refresh on each delta. The 15–30 minute TTL from §8.7 applies only when CP is unreachable.

---

## 2.3  Shared-Subscription Distribution
Shared subscriptions (`$share/group/topic`) are balanced and are owned by the topic PRG that owns the topic hash (never spread across multiple partitions):
- **Default:** round-robin delivery.
- **Optional:** hash-based load distribution on message key for stickiness.
- Slow consumers yield immediately to preserve throughput; no head-of-line blocking.
If shared-subscription load becomes the per-tenant hotspot, follow Appendix A.7; redesigning distribution (e.g., sharding hot shared groups) is in scope if repeated hot spots persist.

---

## 2.4  Connection Stickiness
To prevent message reordering across reconnects:
- The broker binds `(tenant_id, client_id)` to a specific PRG.
- Edge nodes maintain a consistent-hash cache for that mapping.
- Reconnects are routed to the same PRG unless a rebalance epoch is active.

---

# 3  Replication and Consistency

## 3.1  Raft Fundamentals
PRGs are Clustor partitions (3 voters, Latency/ConsistencyProfile unless CP assigns WAN). Elections, ReadIndex/lease gates, strict-fallback, quorum durability, and replay are per Clustor §§0.2, 3, 4; MQTT always runs with `commit_visibility=DurableOnly`.

**Consistency guarantees**
- SP consumes commits in Clustor order; ACKs and `wal_committed_index` follow Clustor §3.4/§6.5.
- Strict-fallback truth table applies unchanged: no leases/follower reads/Group-fsync and no MQTT cache or “best-effort” shortcuts.
- MQTT surfaces Clustor envelopes (`ControlPlaneUnavailable`, `ModeConflict`, `RoutingEpochMismatch`) via §10.5 reason codes.

### 3.1.1  MQTT Read-Gate Rule (Normative)
- Freshness-dependent reads (offline dequeue, retained lookup, subscription evaluation needing current state, inflight inspection, auth/token checks needing CP) MUST observe the Clustor ReadIndex/lease predicate (Clustor §3.3/§0.5) and fail closed with `PERMANENT_DURABILITY` when `read_gate.can_serve_readindex == false`; no cache fallback.
- Strict-fallback or `controlplane.cache_state=Expired` do not loosen these gates; those paths surface `ControlPlaneUnavailable{CacheExpired}` → `PERMANENT_DURABILITY`. `Not Authorized` is reserved for binding failures with a healthy CP.

---

## 3.2  Commit Lifecycle
MQTT publish/ack flows ride the Clustor ACK contract (§3.4/§6.5): append, quorum fsync, ledger proof, then apply/emit PUBACK/PUBREC/PUBCOMP. Group-Fsync, when whitelisted, follows Clustor §6.2. Cross-PRG flows in §4.6 simply wait for both PRGs’ ACK contracts.

--- 

## 3.3  Snapshotting and Compaction
Snapshot manifests, delta snapshots, compaction floors, nonce reservation handling, startup scrub, background scrub, and Quarantine behaviour are governed by Clustor §§8–9 and §6.3/§6.4; values stated here are policy overlays only. QuantuMQTT sets operational targets (5 min effective full snapshot cadence, ~60 s delta) via ControlPlaneRaft profile knobs but does not change Clustor safety gates. Checkpoints carry MQTT session/dedupe/retained payloads encoded in the Clustor snapshot format.

---

## 3.4  Raft Metadata
Election/heartbeat windows, append batching, WAL segment sizing, and observer limits use the Clustor profile defaults in App.B (§3.2, §6.1) for the chosen profile (Latency/ConsistencyProfile in-region; WAN for high-latency tenants). ControlPlaneRaft may tighten within Clustor’s bounds for specific tenants but cannot relax Clustor ceilings or disable guardrails (strict-fallback, device latency gates, io_writer_mode downgrades).

---

## 3.5  Failover and Recovery
Failover, routing epochs, and strict-fallback follow Clustor (§§3.2–3.3, §0.5). If quorum is lost, clients see `PERMANENT_EPOCH`/`PERMANENT_DURABILITY`. During strict-fallback, ACKs wait for the Clustor ledger, ReadIndex/leases stay blocked, and Group-Fsync is unavailable.

---

# 4  Durability and Acknowledgment Policy

## 4.1  Ack Semantics
MQTT acknowledgements use the Clustor ACK contract (§3.4/§6.5) with `durability_mode=Strict` and `commit_visibility=DurableOnly`; the no-ACK-loss guarantee depends on those settings. PUBACK/PUBREC/PUBCOMP emit only after the relevant PRG satisfies that contract. Group-Fsync is whitelisted-only, dev/test-only, and follows Clustor §6.2 predicates unchanged; it is never permitted on production PRGs.

---

## 4.2  Exactly-Once Semantics (QoS 2)
Guarantee scope: MQTT QoS 2 exactly-once delivery **within broker state** across reconnects, Raft failover, and fenced DR promotion. Once messages leave QuantuMQTT via bridges/sinks, downstream semantics depend on the adapter.

- Exactly-once within the broker is synonymous with a successful Clustor commit plus durability-ledger proof under the ACK contract; there is no alternate MQTT-only definition.  
- QoS 2 correctness anchors to Clustor `raft_commit_index`/`wal_committed_index` with `commit_visibility=DurableOnly`; strict-fallback downgrades per Clustor §0.5 and reason-code mapping in §10.5.
- Dedup horizon: 72 h (configurable to 7 d). Dedup key: `(tenant_id, client_id, session_epoch, message_id, direction)` with `direction ∈ {publish, ack}`. `session_epoch` increments on clean reconnect or takeover; inflight/dedupe reset only on epoch change.
- MID reuse allowed only after full ACK + dedupe expiry or after `session_epoch++`; early reuse (including wrap) is duplicate. `session_epoch++` fences prior inflight.
- Dedup tables persist via WAL + checkpoints; WAL entries include `session_epoch` and direction for deterministic replay.
- **Compaction floor (simplified):** Each PRG computes `effective_product_floor = max(clustor_floor, local_dedup_floor, earliest_offline_queue_index, forward_chain_floor)`.  
  - `clustor_floor` per Clustor §8.  
  - `local_dedup_floor` = earliest index needed to rebuild the PRG’s dedupe table.  
  - `forward_chain_floor` (session PRG only) waits for a topic manifest covering the relevant `TopicPublish`; topic PRGs never wait on session manifests.  
  - Truncation below `effective_product_floor` requires SnapshotAuthorizationRecord + CompactionAuthAck; each PRG enforces its own floor.
- Dedup map bounds: `dedup_max_entries_per_session = 4,096`; per-tenant cap 64 M entries (~8 GiB). Hitting caps blocks new QoS 2 publishes until entries age out or `session_epoch++`; WAL retention must preserve needed entries until manifest-authorized. At 72 h horizon this is ~one publish/63 s; higher rates need shorter horizons or periodic `session_epoch++`.
- High-rate QoS 2 guidance: the default 72 h / 4,096-entry window is for low-rate publishers. High-rate publishers must shorten the dedupe horizon and/or rotate sessions (`session_epoch++` via clean reconnect/takeover) to avoid exhaustion; CP may cap horizons for such tenants.
- Message IDs remain durable across reconnect/failover; last-packet-wins within the dedupe window.
- DR interaction: dedupe tables ship with WAL/snapshots. Controlled promotion preserves exactly-once (RPO 0); uncontrolled promotion permits at-least-once within `(last_durable_index_source, last_durable_index_applied_target]` per Clustor ledger proofs.

| QoS 2 mode | Semantics |
|------------|-----------|
| In-region, healthy quorum | Exactly-once within broker |
| Controlled (fenced) DR promotion | Exactly-once preserved |
| Uncontrolled DR promotion (lag window) | At-least-once within replication lag window |

---

## 4.3  Fsync and Flush Policy
Fsync semantics, io_writer_mode downgrades, and durability gating follow Clustor §6.1 and §6.2 (including device-latency and io_uring fallback rules) without alteration. QuantuMQTT does not introduce alternative flush cadences; any fsync failures, latency violations, or scrub issues rely on Clustor strict-fallback and Quarantine handling, surfaced to MQTT clients via the reason-code mapping in §10.5.

---

## 4.4  Session and Queue Persistence
All inflight QoS 1/2, subscription data, and offline queues are stored on disk via Clustor WAL + snapshots; this is descriptive of Clustor-backed persistence and does not modify Clustor semantics. Crash or restart yields deterministic resume. Offline queues obey tenant and session quotas (default 64 MiB / 1 000 messages).

---

## 4.5  Ordering Guarantees
- Per-topic FIFO ordering as defined by MQTT spec.  
- Cross-topic order not guaranteed.  
- During slow-consumer shedding, order is preserved among retained subscribers; messages may be skipped for those throttled beyond limits.

---

## 4.6  QoS 2 Across Session/Topic PRGs
ForwardPublish and PublishAcked are regular Clustor log entries; each PRG commits independently per Clustor. Forward RPCs are non-durable and never treated as committed state.
- Dedupe tables and inflight state live on the **session PRG**. The **topic PRG** maintains ordering/retained state plus a lightweight idempotence map keyed by `(origin_session_part, session_epoch, message_id)` so forwarded publishes are applied once; `forward_seq` is monotonic per (session PRG, topic PRG epoch) so rebalances cannot collide with historical keys.  
- Sequence for `session_partition != topic_partition` (single client publish):  
  1. Session PRG appends `ForwardPublish{topic_part, session_epoch, message_id, forward_seq}` and commits (dedupe checked here).  
  2. Session PRG sends forward RPC to topic PRG with `forward_seq` idempotence key.  
  3. Topic PRG appends `TopicPublish{origin_session_part, session_epoch, message_id, forward_seq}` and commits; duplicates are dropped via the idempotence map.  
  4. Session PRG appends `PublishAcked{session_epoch, message_id, forward_seq}` once it observes the topic commit index, then emits PUBREC/PUBCOMP to the client.  
- **Ordering/ACK gating:** Forward RPCs MUST carry the committed `ForwardPublish` index. QoS 2 ACK is allowed only after topic `TopicPublish` and session `ForwardPublish/PublishAcked` each satisfy Clustor §3.4/§6.5; no speculative sequence numbers or forward RPCs count as committed.
- **Compaction/snapshots:** Session PRG lowers `forward_chain_floor` only after a topic manifest covering the `TopicPublish`; topic PRG depends only on its manifest. Both follow §4.2 and Clustor §8.
- **Quarantine/fallback:** Quarantine or strict-fallback on either PRG holds QoS 2 inflight and surfaces `PERMANENT_DURABILITY`; replay re-forwards committed entries. Will/retained follow the topic commit; ordering is the topic PRG’s Raft order only.

### 4.6.1  Cross-PRG Failure Handling (QoS 2)
- Quarantine on topic or session PRG → `PERMANENT_DURABILITY`, hold QoS 2 inflight; resume after both PRGs revalidate manifests (session case requires reconnect/replay).  
- Strict-fallback on topic or session PRG → `PERMANENT_DURABILITY`, hold inflight until ACK contract satisfied; replay re-applies committed `ForwardPublish`.  
- ControlPlaneRaft outage on forward path → `PERMANENT_DURABILITY`; block inflight until CP freshness returns (no downgrade to “Not Authorized”).

### 4.6.2  Cross-Partition QoS 2 Invariants (Normative)
- **Two-PRG ACK eligibility:** A `PublishAcked` entry MUST NOT be appended until (a) the topic PRG satisfies Clustor §6.5 for the corresponding `TopicPublish`, and (b) the session PRG satisfies §6.5 for its own `ForwardPublish`; MQTT ACK emission requires both PRGs to satisfy the Clustor ACK contract (§3.4/§6.5) with ledger proofs. This gating is purely application-level; each PRG remains an independent Clustor commit.
- **Cross-PRG effective compaction floor:** Each PRG enforces the unified `effective_product_floor` in §4.2; the session PRG’s `forward_chain_floor` is lowered only after seeing a topic manifest for the `TopicPublish` index, and the topic PRG never waits on session manifests.
- **Manifest lineage:** WAL containing dedupe/offline data for the forward chain MUST NOT be truncated on a PRG until that PRG has a SnapshotAuthorizationRecord + CompactionAuthAck covering the relevant indices (Clustor §8).
- **Deterministic forward replay:** Forward RPCs are non-durable; replay of cross-PRG flows MUST derive solely from committed WAL entries, with `forward_seq` monotone per (session_PRG, topic_PRG, routing_epoch).
- **Epoch binding:** `forward_seq` and dedupe keys MUST include the routing/forward epoch to prevent collisions across rebalances; stale-epoch forwards MUST be treated as duplicates.
- **Forward retries:** Any replay of `ForwardPublish`/`TopicPublish` derives only from WAL replay; no independent forward RPC retry path is permitted outside Clustor log replay.
- **Topic-side idempotence maps:** Topic PRG idempotence/dedupe maps follow the same snapshot/manifest compaction rules as session PRGs; no MQTT-only TTL-based truncation is permitted.

---

## 4.7  Bridge and Sink Semantics
Exactly-once is scoped to broker state. Bridges/sinks (Kafka, S3, HTTP, etc.) deliver **at-least-once** downstream; adapters should expose idempotence keys `(tenant_id, part_id, session_epoch, message_id, forward_seq)`. Retries continue until adapter TTL, then audit. [Normative] Sinks MUST NOT claim guarantees beyond the broker; downstream idempotence is adapter-defined.

# 5  Storage Layout

## 5.1  Overview
PRG storage uses the Clustor WAL/snapshot layout under `/var/lib/quantumqtt/partitions/<part_id>/...`; MQTT state is encoded inside Clustor log entries and snapshots. NVMe (ext4/xfs) remains the baseline; filesystem acceptability follows Clustor.

---

## 5.2  Write-Ahead Log (WAL)
WAL format/AEAD/ledger/scrub follow Clustor §§5–6/§9. MQTT payload fields include ClientID, `session_epoch`, `message_id`, flags, payload length/body; tenant identity is in CP metadata (single-tenant PRGs). WAL footprint targets ≤3× latest checkpoint; older segments archive per §17.4 via Clustor manifest/ledger rules.

---

## 5.3  Checkpoints
Snapshot/manifest/compaction behaviour is per Clustor §8/§9.1/§9.2. CP targets ~5 min full snapshots with ~60 s deltas, retaining two fulls and up to five incrementals. Offline queues and persistent session state share the dedupe compaction floor; WAL covering them is compacted only after the covering snapshot is acknowledged via the Clustor chain.

---

## 5.4  Retained Store
- One retained message per topic (per MQTT).  
- Persisted via Clustor WAL entries and snapshots.  
- Tenant quotas: 64 GiB default / 1 TiB max.  
- Retained index stored as B+Tree keyed by topic hash; lazy loaded on demand.

---

## 5.5  Integrity and Encryption
Integrity, AEAD, MAC epochs, nonce handling, and ledger proofs follow Clustor §§6.5, 9.2, and 12; QuantuMQTT does not introduce alternate crypto or integrity mechanisms. ControlPlaneRaft issues key epochs and validates manifests per Clustor; tenant-scoped KEKs/DEKs are minted through the same mechanism with weekly rotation aligned to Clustor defaults and dual-read support. Any integrity or AEAD failure triggers Clustor scrub/quarantine, surfaced to MQTT clients via §10.5 reason-code mapping.

---

# 6  Session Processor (SP)

## 6.1  Function
The Session Processor executes the MQTT state machine atop the replicated log:
1. Receive COMMIT events from Raft leader.
2. Apply to in-memory session maps.
3. Update inflight and dedupe tables.
4. Deliver to active subscribers.
5. Persist checkpoints periodically.

---

## 6.2  In-Memory Structures
Structures/bounds: `inflight` ≤ 1024, `dedup` 72 h window, `subscriptions` ≤ 256/client, `offline_queue` ≤ 1 000 default, `retained_index_cache` LRU 1 000. Memory per idle connection ≤ 3 KiB median / 4 KiB p99.

---

## 6.3  Apply Loop
Single mutator task pinned to one runtime worker (no cross-PRG locks); throughput 500 msg/s target / 1,000 cap per PRG with ≤ 10 ms p99 apply latency. Ordering is the Clustor commit order.

---

## 6.4  Late or Failed Delivery
Messages older than 72 h or exceeding tenant TTL are dropped with audit reason `TTL_EXPIRED`.  
Subscriber disconnects before delivery → queue persisted; Will published if applicable.

---

# 7  Retained and Offline Handling

## 7.1  Retained Message Lifecycle
- On `PUBLISH` with `retain=true`: broker replaces previous retained entry.  
- Retained payload persisted in WAL and checkpoint.  
- On `PUBLISH` with zero-length payload and `retain=true`: retained entry deleted.  
- Subscribers with `retain-as-published=true` receive retained flag per MQTT 5.

## 7.2  Offline Queue Processing
- Persistent sessions retain queued messages until reconnected or expired.  
- Expiry = min(session_expiry_interval, message_TTL).  
- Overflow beyond quota → oldest QoS 0 dropped, QoS 1/2 blocked until space.  
- Queue ordering strictly preserved per client.
[Normative] Offline-queue dequeue and delivery follow the read-gate rule in §3.1.1; under strict-fallback or CP expiry they fail closed with `PERMANENT_DURABILITY`.

---

# 8  Control Plane (ControlPlaneRaft)

## 8.1  Purpose
The control plane is a ControlPlaneRaft deployment per Clustor §11 (CP-Raft is an alias; ControlPlaneRaft is the canonical term). It carries tenant descriptors, placements, durability proofs, feature gates, DR fences, override ledger entries, and key epochs. Cache freshness, strict-fallback interaction, system log entry semantics, and `/readyz` exposure follow Clustor; QuantuMQTT adds MQTT-specific object schemas and API endpoints but does not alter ControlPlaneRaft behaviour.

## 8.2  Durable Objects
ControlPlaneRaft objects: `ClusterConfig` (nodes/placement/durability profile), `TenantSpec` (quotas/trust/ACLs), `RoutingMap` (tenant→PRG, `routing_epoch`), `QuotaPolicy`, `CertificateBundle`, `AuditIndex`, mirrored `DurabilityLedger` (Clustor §11.1), `DRFence/FenceCommit` (Clustor §11.2), `FeatureManifest` (lease/snapshot/observer/PID gates). All obey Clustor wire/catalog rules and cache semantics; RPO intra-region remains 0.

### 8.2.1  Capacity Targets and Scale-Out
Targets (enforced via CP admission/override ledger): max tenants 5,000 (hard 10,000), routing map ~2,000 PRGs total (per-tenant per §14.2 caps), write QPS ≤ 200 for quota/ACL/cert deltas with `/v1/dr/*` p99 < 500 ms. If CP load exceeds these bands per Clustor §11, new tenants/quota/ACL writes pause until scaled (observers/shards). High-churn tenants (frequent ACL/quota/cert mutations) may be rate-limited or isolated to dedicated ControlPlaneRaft cohorts.

---

## 8.3  Tenant Management
- Tenants created via `/v1/tenants`, persisted as ControlPlaneRaft tenant objects.  
- Each tenant assigned namespace prefix and SNI entry.  
- CA roots and certs uploaded for BYO trust or inherited from provider.  
- Quotas and policies adjustable at runtime; propagation follows ControlPlaneRaft cache freshness and strict-fallback (§0.5).

---

## 8.4  Configuration Reconciliation
Control agents reconcile CP state every 500–1000 ms: validate membership/health, placement/balance, quotas/policies, leader transfers, and stale metadata GC. Work respects `controlplane.cache_state` and strict-fallback; gated operations wait for Fresh cache.

---

## 8.5  Epoch and Versioning
- `routing_epoch` and durability-mode epochs follow Clustor (§§4, 11); topology or config change ⇒ epoch++.  
- Rebalance epochs are represented as `routing_epoch` increments in ControlPlaneRaft; no parallel epoch counter is permitted for routing, and MQTT `PERMANENT_EPOCH` is solely a surface mapping of Clustor `RoutingEpochMismatch` envelopes (§0.3/§4).  
- Clients include the advertised epoch; mismatches return Clustor `RoutingEpochMismatch` surfaced via MQTT `PERMANENT_EPOCH`.  
- Prevents split-brain across partial quorums and aligns with strict-fallback.

---

## 8.6  API Surface
REST + gRPC (`/v1`) backed by ControlPlaneRaft objects and Clustor wire/catalog rules: `/v1/tenants`, `/v1/tenants/{id}/quotas`, `/v1/tenants/{id}/acls`, `/v1/tenants/{id}/trust`, `/v1/cluster/state`, `/v1/dr/fence`, `/v1/dr/promote`. Writes are linearizable/auditable; degraded periods use `ControlPlaneUnavailable` codes.

---

## 8.7  Control Plane Degraded Mode
Clustor `controlplane.cache_state` and strict-fallback apply unchanged (Clustor §§0.5, 3.3, 11); MQTT surfacing only:
- `Fresh/Cached/Stale`: use cached tenant/routing/ACL; freshness-gated reads obey §3.1.1.
- `Expired`: admin/DR/durability transitions, key rotations, JWKS/ACL/quota reloads fail with `ControlPlaneUnavailable{CacheExpired}` → `PERMANENT_DURABILITY`; listeners fence tenants after grace.
- Strict-fallback `LocalOnly`: writes stay Strict; ReadIndex/leases/follower reads blocked → `PERMANENT_DURABILITY`/`PERMANENT_EPOCH`.
- DR fencing/promotion (`/v1/dr/*`) requires Fresh CP.

---

# 9  Security Model

## 9.1  Design Principles
Security is woven into every layer of QuantuMQTT.  
The platform assumes hostile multitenancy and untrusted networks.  
Core invariants:

- **All connections authenticated:** every MQTT connection must present valid mTLS credentials or a signed access token.  
- **All inter-node links secured:** internal Raft and CP communications use mutual TLS with short-lived certificates.  
- **No plaintext traffic anywhere.**  
- **Crypto agility:** all algorithms upgradeable without data migration.  
- **Auditable and deterministic:** all security events recorded and signed.

## 9.2  Certificates and PKI
- Provider CA: Root → Cluster Intermediate → Node and Tenant certs; tenant BYO roots/certs are scoped to that tenant’s SNI and cannot impersonate others.
- Node enrollment: one-use bootstrap token validated against CP returns node cert + KMS material.
- Validity: node certs 30 d; tenant certs 24 h. TLS 1.3 with `TLS_AES_128_GCM_SHA256` or `TLS_AES_256_GCM_SHA384`.
- Cert expiry vs session TTL: sessions exceeding cert validity must re-auth; listeners trigger AUTH re-auth 5 minutes before expiry and suppress Will on failure. Offline queues persist per TTL/quota.
- Renewal pacing: target 1 % of active clients every 15 minutes (~1.1k handshakes/s at 100 M sessions) with jitter; long-lived devices may renew on reconnect if >6 h validity remains.
- CP degraded/strict-fallback: renewal and re-auth that require CP follow §8.7/§3.1.1 and fail closed (`PERMANENT_DURABILITY`), not `Not Authorized`, when CP freshness is unavailable.

## 9.3  Authentication
- **mTLS (primary):** ClientID bound to certificate subject/SAN.  
- **Alternative tokens (optional):** JWT/OAuth2 for devices unable to perform mTLS; token to identity map stored in CP-Raft.  
- **Server verification:** always mutual TLS; server presents provider or tenant certificate chain via SNI context.

## 9.4  Authorization
- Wildcard ACLs stored per tenant in CP-Raft; evaluated at connection time and cached per epoch.  
- Violations are audited; subscription evaluation needing retained/offline state follows the read-gate rule (§3.1.1) and fails closed when gated.

## 9.5  Encryption at Rest
- AES-256-GCM for WAL blocks (per segment), checkpoints (per file), control-plane snapshots (cluster-wide), retained store (per partition); keys via KMS envelope per node. Weekly rotation with 7-day dual-read support.

## 9.6  Auditing and Forensics
- Immutable JSON-lines log with per-record HMAC signed by CP’s Ed25519 key; 12-month retention in object store.  
- Categories: connection events, ACL changes, key issuance, node enrollment, leadership change, durability fence, quota breach.  
- Logs are hashed/indexed in CP-Raft and streamed to object store; retention is operator-controlled and not tenant-modifiable.

## 9.7  Hardening
- `seccomp` filters allow minimal syscalls.  
- Drop all capabilities except `NET_BIND_SERVICE`.  
- No core dumps.  
- Run as non-root; secrets under `/etc/quantumqtt/secure` mode 0700.  
- Signed binaries; signature verified at startup.

---

# 10  Authentication and Authorization

## 10.1  Connection Establishment
Client initiates TCP/TLS or QUIC; server validates cert/token, extracts tenant from SNI, verifies ClientID binding, then accepts or rejects with audit.

### 10.1.1  Binding Rules
ClientID ↔ mTLS identity binding enforced; reconnects from the same identity preempt the old session (Will suppressed); different identity for the same ClientID is rejected and audited.

## 10.2  Authorization Enforcement
- Topics checked against wildcard ACL tree (per tenant).  
- ACL tree precompiled to deterministic DFA for O(log n) matching.  
- Subscription or publish failing ACL → disconnect with reason code.
ACL evaluation uses cached CP state; no MQTT-side bypass or speculative ACL read is allowed beyond what ControlPlaneRaft and Clustor predicates permit. Any ACL/subscription read requiring freshness follows the read-gate rule in §3.1.1 and fails closed with availability (`PERMANENT_DURABILITY`), not “Not Authorized.”
MQTT version note: MQTT 3.1/3.1.1 clients receive generic disconnect/CONNACK codes rather than the detailed MQTT 5 reason codes in §10.5 (§10.7 governs downgrade behaviour).

## 10.3  Will Message Policy
- Clean disconnect or takeover → Will suppressed.  
- Unclean disconnect/timeout → Will after `server_keepalive * 1.5`.  
- Rejection or Clustor fence/epoch mismatch/CP expiry/strict-fallback disconnect → Will suppressed.

## 10.4  Access Token Flow
Device classes that cannot present mTLS credentials use MQTT 5 Enhanced Auth with short-lived bearer tokens:
1. Client establishes TLS 1.3 with server authentication only (tenant SNI selects trust context).  
2. Client issues CONNECT + `AUTH` packet containing a signed JWT/OAuth2 token bound to `client_id`, `tenant_id`, and nonce supplied by the listener.  
3. Listener validates token signature/claims locally (cached JWKS) and, on cache miss, performs CP-Raft introspection; verdict cached ≤ 60 s.  
4. Upon success, the control agent stamps the session with `auth_epoch` so replays or stolen tokens cannot attach to different ClientIDs.  
5. Token expiry shorter than session expiry forces proactive re-auth via MQTT AUTH exchange; failure closes the session and suppresses Will.

Tokens must include `aud=quantumqtt`, `scope=mqtt.connect`, and a monotone `cnf` thumbprint tying them to the TLS exporter secret.  Connections missing any bind are rejected with reason `0x87` (Not Authorized) and audited as `TOKEN_BINDING_MISMATCH`.
Token-only flows inherit the same QoS 2/dedupe semantics as mTLS. CP-dependent auth/re-auth/token introspection obey §8.7 and the read-gate rule (§3.1.1); failures under CP expiry or strict-fallback surface `PERMANENT_DURABILITY` (not `Not Authorized`). Inter-node traffic remains mTLS-only; tokens are never accepted on Raft/CP links.

## 10.5  Error / Reason-Code Mapping
Internal error names map to MQTT 5.0 reason codes as follows (see §10.7 for MQTT 3.1/3.1.1 handling):
| Internal | MQTT Reason | Notes |
|----------|-------------|-------|
| `PERMANENT_EPOCH` | `0x9D` (Server Moved) | Client should reconnect using new leader hint. |
| `PERMANENT_DURABILITY` | `0x88` (Server Unavailable) | Emitted on durability fence; Will suppressed. |
| `TRANSIENT_BACKPRESSURE` | `0x97` (Quota Exceeded) | Advertised when buffers full; client should retry. |
| `TOKEN_BINDING_MISMATCH` | `0x87` (Not Authorized) | Token rejected due to binding failure. |
| `SESSION_TAKEN_OVER` | `0x8E` (Session Taken Over) | Replacement connection from same identity preempts existing session. |
| `OVER_QUOTA` | `0x97` (Quota Exceeded) | Tenant/session resource hard cap. |
`ControlPlaneUnavailable{CacheExpired|NeededForReadIndex}` always maps to `PERMANENT_DURABILITY`. [Normative] Mapping is intentionally lossy; `PERMANENT_DURABILITY` signals Clustor safety gating, not auth failure.
Design trade-off: multiple safety causes (durability fence, CP cache expiry, strict-fallback/read-gate blocks, DR fencing) intentionally collapse to 0x88. Operators MUST correlate 0x88 events with diagnostics/metrics (e.g., `durability_fence_active`, `controlplane_cache_state`, strict-fallback/read-gate indicators) and logs/traces to distinguish cause. Implementation MUST route all CP-expired/strict-fallback/read-gate failures through this mapping and MUST NOT emit `Not Authorized` or other codes for these safety conditions.

### 10.6  MQTT Authentication & Authorization → Clustor Cache State Mapping (Normative)
| Operation | CP Fresh/Cached/Stale | CP Expired | strict_fallback=true |
|-----------|-----------------------|------------|----------------------|
| mTLS session start | Allowed per ACL/trust from last Fresh snapshot | Fail-closed (`PERMANENT_DURABILITY`) | Fail-closed unless the Clustor truth table explicitly allows a read-free path; no cache-only fallback |
| mTLS re-auth | Allowed | Fail-closed (`PERMANENT_DURABILITY`) | Fail-closed (strict-fallback blocks the read predicate; no MQTT fallback) |
| Token introspection | Allowed (local cache or CP lookup) | Fail-closed (`PERMANENT_DURABILITY`) | Fail-closed (strict-fallback blocks the read predicate; no MQTT fallback) |
| ACL evaluation | Allowed using last Fresh snapshot; no new derivations | Fail-closed (`PERMANENT_DURABILITY`) | Fail-closed (`PERMANENT_DURABILITY`) |
| Retained/subscription reads | Per §3.1.1 read-gate predicate | Fail-closed (`PERMANENT_DURABILITY`) | Fail-closed (`PERMANENT_DURABILITY`) |
| Cert/CA rotation, JWKS refresh | Allowed when Fresh | Fail-closed (`ControlPlaneUnavailable{CacheExpired}` → `PERMANENT_DURABILITY`) | Fail-closed |

### 10.7  Version-Specific Behaviour (MQTT 3.1 / 3.1.1 / 5.0)
- MQTT 5 is required for reason codes in §10.5, Enhanced Auth/token flows (§10.4), and dynamic Receive Maximum/backpressure surfacing (§13.3/§13.4).  
- MQTT 3.1/3.1.1 clients:
  - No Receive Maximum negotiation; server enforces the same internal credit logic and may throttle, delay ACKs, or disconnect on overload with generic return codes (CONNECT/CONNACK refusal or server-side disconnect). Backpressure events are audited/metrics-emitted.  
  - Enhanced Auth is unavailable; only mTLS or simple username/password (per tenant policy) are allowed.  
  - Error mapping uses disconnect/CONNACK return codes only; detailed MQTT 5 reason codes are not sent. Behaviour remains aligned with Clustor safety outcomes (durability fences, epoch mismatch) but surfaces as generic server-unavailable/moved disconnects.  
- QoS semantics, dedupe, and exactly-once rules apply uniformly across versions once connected; version differences are limited to signaling and auth mechanisms.

---

# 11  Multi-Tenant Isolation

## 11.1  Namespace Enforcement
Each tenant’s topics are prefixed:
```
tenant/<tenant_id>/...
```
and filtered at publish/subscribe boundaries.  
Listeners inject/verify the prefix based on SNI so clients can use tenant-local topic names if configured; cross-tenant wildcarding is disallowed.

## 11.2  Resource Quotas
Quotas (default / max): connections 1,048,576 / same; retained storage 64 GiB / 1 TiB; persistent session store 128 GiB / 2 TiB; offline queue per session 64 MiB/1,000 msgs (max 1 GiB/50,000 msgs); subscription filters 1 M / 10 M; shared-sub groups 50 K / 500 K. Over-quota → throttle then disconnect after 24 h grace at 1.5× quota (tier-specific; resets only after usage stays below quota for a full window).

## 11.3  Noisy Neighbour Controls
- CPU, I/O, and network shaping enforced via cgroups per tenant slice.  
- Hard caps for connection, publish, and subscription rates.  
- Breach triggers throttle → reject → disconnect escalation.

## 11.4  Tenant Data Isolation
- Distinct keyspaces in PRG shards; no cross-tenant records.  
- KMS keys unique per tenant.  
- Audit, metrics, and logs tagged by tenant ID.

## 11.5  Tenant and Feature Limits
Max tenants 5,000 (hard 10,000) per §8.2.1; max PRGs per tenant 512 (up to 1,024 with approval and global cap adherence). DR shipping on by default for production tiers. Group-fsync is only allowed for explicitly whitelisted dev/test tenants and is never permitted on production PRGs; PRGs remain strictly single-tenant.

---

# 12  QUIC Transport and Networking

## 12.1  QUIC Layer
MQTT over QUIC/HTTP/3.

### 12.1.1  Defaults
| Parameter | Value |
|------------|--------|
| `max_idle_timeout` | 360 s |
| `initial_rtt` | 50 ms |
| `max_udp_payload_size` | 1350 B |
| `max_ack_delay` | 25 ms |
| `active_connection_id_limit` | 8 |
`max_ack_delay` stays below Clustor heartbeat/append windows (50 ms) and must not be tuned to interfere with Raft timing; QUIC timing cannot override Clustor Raft timing parameters.

### 12.1.2  Features
- 0-RTT for CONNECT only; QoS 1/2 data is rejected because it cannot satisfy the Clustor ACK contract.  
- Connection migration with path validation; shard hint in Connection ID is advisory and revalidated against CP `routing_epoch`.  
- If path migration lands on an edge with stale routing epoch, the listener revalidates and either reroutes internally or disconnects with `PERMANENT_EPOCH`; stale delivery is not permitted.  
- Stateless reset/preferred address supported; retry tokens valid 60 s with anti-amplification.

### 12.1.3  Performance
- Typical handshake latency 1.5 RTTs.  
- QUIC congestion control: BBRv2 by default; operators requiring alternative fairness characteristics (e.g., CUBIC) may select another controller per ops guidance.  
- Flow pacing integrates with Raft backpressure signals.

### 12.1.4  Platform Notes
- Linux + io_uring only; QUIC/H3 optional. TCP/TLS is the reference path and must remain available.

### 12.1.5  Stream Mapping
- Single long-lived bidirectional H3 stream per connection; no per-publish streams.  
- QUIC/H3 is a non-durable edge; ordering/determinism anchor at the Raft/SP boundary. QUIC timers/randomness must not influence partition state before WAL commit, and ordering follows topic PRG Raft order.

---

# 13  Flow Control and Backpressure

## 13.1  Goals
Maintain bounded memory and latency at full concurrency while ensuring fairness among tenants.

Flow control and backpressure are implemented by Clustor’s PID/throttle controller (§10) and exposed via Clustor `ThrottleEnvelope`/credit hints; QuantuMQTT maps those signals to MQTT `Receive Maximum`, tenant throttles, and MQTT reason codes. Partition-level credits, disk/CPU backpressure, and structural lag handling therefore follow Clustor semantics; the MQTT behaviour below is an overlay for client experience.
MQTT version note: dynamic `Receive Maximum`/reason-code surfacing applies to MQTT 5; MQTT 3.1/3.1.1 clients follow the same internal credit logic but see throttling/disconnect per §10.7.

## 13.2  Control Boundaries
Stages: Client↔Broker (MQTT Receive-Maximum/keep-alive/windows), Leader↔Followers (Raft AppendEntries credits), Commit↔Apply (queue depth thresholds), Apply↔Delivery (subscriber credits), Broker↔Control Plane (lag/health feedback).

Cross-PRG publishes couple the session PRG to the topic PRG. When the topic PRG is saturated, it reduces forward RPC credits, which stalls `ForwardPublish` processing on the session PRG, stops ACK emission to clients, and advertises reduced `Receive Maximum` so clients see explicit backpressure (`TRANSIENT_BACKPRESSURE` / 0x97) rather than accumulating unbounded inflight state. Metrics expose this as `forward_backpressure_seconds` per PRG.

---

## 13.3  Client-Side Credits
Leader recalculates advertised `Receive Maximum` every 200 ms (MQTT 5 only):
```
credit_hint = base_window * (0.5 + 0.5 * headroom) * follower_factor
```
where headroom = (1 - queue_utilization).  
Follower lag reduces factor.  
Client adapts inflight window accordingly.

---

## 13.4  Backpressure Responses
Reason codes map from Clustor `ThrottleEnvelope` and `ControlPlaneUnavailable` (§10, §3.3); Clustor throttles/credits remain authoritative. Strict-fallback paths follow §3.1.1 and surface availability (`PERMANENT_DURABILITY`).
- Queue full → `TRANSIENT_BACKPRESSURE` / 0x97.  
- Routing epoch mismatch (“dirty epoch”) → `PERMANENT_DURABILITY`.  
- CPU or I/O saturation → dynamic tenant rate limit.  
- Violations persist → disconnect with reason.
MQTT 3.1/3.1.1 clients receive the same throttling/overload behaviour but without MQTT 5 reason codes; server may delay ACKs or disconnect with generic return codes (see §10.7).

---

## 13.5  Queue Thresholds
Queue thresholds: Commit→Apply 50 K (pause ACKs), Apply→Delivery 20 K (drop oldest QoS 0), Retained write buffer 16 MB (throttle publishers), WAL dirty bytes ≤ `group_fsync_max_bytes` (sync flush).

---

## 13.6  Telemetry Metrics
- `queue_depth{type}`, `apply_lag_seconds`, `replication_lag_seconds`, `reject_overload_total`, `tenant_throttle_events_total`, `leader_credit_hint`, `forward_backpressure_seconds`.

---

# 14  Performance and Scalability

Operational values overlay Clustor profiles; assume `strict_fallback=false` and Fresh CP.

**Performance SLOs (Normative)**
- Per PRG steady state: 500 QoS 2 publishes/s; peak 1,000 publishes/s.  
- Latency: ≤ 10 ms p99 single-PRG; cross-PRG adds ≈ 3–4 ms per hop plus client RTT.  
- v1 must-meet CPU budget: ≤ 60–80 µs/publish per replica.

**Performance Targets (Informative)**
- Aggregate throughput ≈ Σ(PRG throughput)/(1+2r); with ~2,000 PRGs at 500 msg/s and typical r=0.5–0.8 this yields ~384–500k msg/s. Stretch to ~1,000,000 msg/s requires r ≤ 0.3 or higher per-PRG capacity.  
- Design 10 µs/publish; stretch 35–50 µs with IO/batching.

## 14.1  Connection Efficiency
Target 3.0 KiB idle TCP / 3.5 KiB idle QUIC via shared timer wheels, per-tenant epoll, header pooling; keep-alive 300 s and LB idle timeout ≥ 400 s.

## 14.2  Partition Sizing
| Metric | Target | Cap |
|---------|---------|-----|
| Sessions per PRG | 50,000 | 75,000 |
| Active publishers | 2,000 | 5,000 |
| Throughput | 500 msg/s | 1,000 msg/s |
| Retained slice | 16 GiB | 32 GiB |
| Session state | 32 GiB | 64 GiB |

Rebalancer maintains ≤ ±15 % variance. If logical state nears 32–64 GiB or a replica’s working set exceeds 16 GiB, CP increases `tenant_prg_count` before hitting memory guardrails.

## 14.3  Horizontal Scaling
New nodes auto-discover and request CP-Raft assignment; default one PRG move at a time (per-tenant up to `min(4, tenant_prg_count/20)`), with redirects handling redistribution and < 2 s reconnects.

## 14.4  CPU, Storage, and Network Efficiency
CPU v1 must-meet ≤ 60–80 µs/publish (stretch 35–50; 10 µs design), storage amp ≤ 1.2×, network amp ≤ 1.1×, power ≤ 0.25 W / 10k idle conn with 60–80 % utilisation; adjust PRG density or node counts if measurements exceed targets.
Operational requirement: enable WAL/commit batching (AppendEntries and fsync) to reach the µs/publish targets; single-message fsync or unbatched commits will not meet these budgets.

## 14.5  Cross-PRG QoS 2 Throughput Model
Cross-PRG publishes require two rounds of commits on the session PRG (initial `ForwardPublish`, then `PublishAcked`) and one commit on the topic PRG.  With PRG capacity expressed as a Raft-commit budget, the effective cost per publish is:
```
cost_per_publish ≈ 1*(1 - r) + 3*r = 1 + 2r
effective_cluster_tps ≈ (Σ PRG_commit_capacity) / (1 + 2r)
```
where `r` is the fraction of publishes that traverse session→topic. Examples (2,000 PRGs × 500 msg/s commit budget):
All commits in this path still obey Clustor’s global system-log entry semantics and durability-ledger ordering; the throughput model assumes the Clustor ACK contract on each commit.
- `r = 0.5`: 1,000,000 / 2 ≈ **500,000** publishes/s effective.
- `r = 0.8`: 1,000,000 / 2.6 ≈ **384,000** publishes/s effective.

Scale PRG counts using the `1 + 2r` factor; `cross_prg_ratio` metrics are exposed so tenants can size accordingly. Achieving ~1,000,000 publishes/s with 2,000 PRGs requires r ≤ 0.3 or proportionally higher per-PRG commit capacity.

---

# 15  Upgrade and Rollout Policy

## 15.1  Rolling Upgrades
- Max one node unavailable.  
- Leader transferred before shutdown.  
- Each pod checkpoints and fsyncs before termination.  
- Traffic drains gracefully; no client disruption.

## 15.2  Binary Compatibility
- Wire and state compatible across minor versions.  
- Version skew ≤ 1 minor.  
- Epoch fences protect schema changes.

## 15.3  Hot Reload
- ACLs, quotas, routing, certificates reload live.  
- No restart required.  
- Live key rotation: new keys inserted in memory, old retained until expiry.
Hot reloads require `controlplane.cache_state=Fresh`; `Expired`/strict-fallback states fail closed with `ControlPlaneUnavailable` and must not mint or reload security material until cache freshness returns.

## 15.4  Rollback
- Any binary/config ≤ 30 days old restorable.  
- Performed via CP snapshot restore and epoch increment.

## 15.5  Testing and Chaos
- Health gates on `/readyz` ≥ 99 % partitions ready.  
- Inject faults for election delay, fsync slow, KMS outage; verify recovery paths.

---

# 16  Observability and Telemetry
Observability: Prometheus/OpenMetrics `/metrics` (~250 ms) for Raft/WAL/session/tenant/network/security/CP (tenant labels optional), OpenTelemetry tracing (1 % steady, 100 % on alerts), structured JSON logging with rotation and `/v1/loglevel`, immutable audit stream mirrored to CP-Raft and object store (12 months), and alerts for fsync/lag/cert expiry/durability fences/quota breaches.

---

# 17  Disaster Recovery and Cluster Recovery

## 17.1  Objectives
- Modes: **Controlled (fenced)** RPO=0; **Uncontrolled** lag-bounded (≤ 5 s steady / ≤ 30 s spike) with at-least-once duplicates per §4.2.  
- **RTO ≤ 2 h** regional failure; deterministic and auditable.  
- Controlled path requires healthy CP; otherwise use the uncontrolled path.  
- DR obeys Clustor §11.2 strict-fallback, ledger, manifest, and fence rules; fence epoch shared across session/topic PRGs. Duplicate window in uncontrolled mode is `(last_durable_index_source, last_applied_index_target]`.

## 17.2  Failure Classes
| Class | Example | Effect | Recovery |
|--------|----------|---------|----------|
| Node loss | VM/pod down | Quorum ok | Auto |
| Disk loss | NVMe fail | Replica rebuild | Snapshot catch-up |
| Two replicas lost | Partition fenced | Operator restore quorum |
| CP quorum loss | Mgmt degraded | CP snapshot restore |
| WAL corruption | Partial write | Auto truncate |
| Regional outage | Site lost | Promote DR region |

## 17.3  Cross-Region Replication
- Continuous WAL/checkpoint shipping with Clustor durability-ledger proofs and manifest validation; target lag ≤ 5 s (≤ 30 s spike).  
- Promotion via `dr/fence` + `dr/promote`; requires FenceCommit, durability-ledger proof on standby, and manifest/segment chain at or above the fenced index before MQTT resumes. Cutover RTO ≈ 30–60 min.  
- If fencing is impossible, operators may promote accepting lag-bounded loss; Clustor proofs remain mandatory.  
- Replication is per-tenant opt-in; dedupe/idempotence maps ship with WAL/checkpoints in Raft order.

## 17.4  Backups
| Artifact | Frequency | Retention |
|-----------|------------|------------|
| CP snapshot | 60 s | 24 h |
| Checkpoint | 5 min (leader-driven) | 24 h |
| WAL archive | post-compaction | 30 d |
| Audit logs | continuous | 12 mo |

Nightly export to object store; quarterly full-restore test.

## 17.5  Recovery Path
1. Restore CP snapshot.  
2. Replay WALs to last committed index.  
3. Reload checkpoints.  
4. Resume Raft leadership and traffic.

## 17.6  Control Plane Disaster Recovery
- CP-Raft ships WAL + 60 s snapshots to standby; standby runs as observers until `dr/promote`, then converts to voters and fences the failed region’s CP certs.  
- CP client certs are region-scoped; during DR listeners accept only the promoted region bundle to avoid split-brain.  
- After cutover, rejoin the recovered region as observers, validate hashes, then re-enable writes; if hashes diverge, pick a winner (typically the promoted region) and resync—no merge.

---

# 18  Compliance and Auditability
QuantuMQTT aligns with common cloud controls: full signed audit trail, deterministic replay, 12-month log retention, encryption in transit/at rest, and tenant isolation suitable for SOC2/ISO27001 equivalence.

---

# 19  Configuration and Operational Controls
Operational controls below are policy overlays for QuantuMQTT; they do not alter Clustor semantics.

## 19.1  Hierarchy
Cluster → Node → Partition → Tenant → Definition  
Configurations stored in CP-Raft as versioned JSON.

## 19.2  Reload Behavior
| Parameter | Reload | Notes |
|------------|---------|-------|
| Flow control, quotas | Hot (Fresh cache only) | Immediate; blocked under `controlplane.cache_state=Expired` |
| TLS certs | Hot (Fresh cache only) | Replace in memory; CP expiry forces failure with `ControlPlaneUnavailable` |
| WAL/FSYNC mode | Reconfigure | Requires checkpoint |
| NUMA binding | Restart | Affects pinning |

## 19.3  Versioning and Rollback
- Each config version tagged and auditable.  
- Rollback via `/v1/config/rollback?to=<version>`.

## 19.4  Safety
- Mixed versions within a partition forbidden.  
- Epoch++ for topology changes.

---

# 20  Threading and Concurrency
- Single Tokio runtime with io_uring. Each PRG uses one single-threaded mutator task that owns all PRG state; helpers stream bytes only. Runtime worker/thread mapping is left to the scheduler (no requirement for one OS thread per PRG).  
- No cross-partition locks; watchdog fences PRGs that stop making progress (clients see `PERMANENT_DURABILITY`).  
- Worker placement/rebalancing and batching policies are operational concerns and live in the ops handbook; none of these alter Clustor semantics.

---

# 21  Testing and Verification

## 21.1  Deterministic Replay
Given identical WAL and checkpoints, replay yields byte-identical state; unit tests verify this. Clustor provides deterministic Raft/WAL/ledger/snapshot proofs (Clustor §§0, 3, 6.5, 8, App.C); QuantuMQTT adds MQTT-specific fixtures (WAL→MQTT events, QoS 2 dedupe, cross-PRG `forward_seq` continuity).  
[Normative] **MQTT-DET-01:** MQTT deterministic replay MUST depend solely on Clustor WAL ordering and CP snapshot ordering; no wall-clock or transport-derived source may influence SP apply, routing, or ACKs.
- Tick/RNG facade wraps Clustor deterministic sources; `Instant::now()`/`rand::thread_rng()` forbidden in SP/Raft paths.
- Snapshot encoding is deterministic (subscriptions lexicographic; offline queues by `(client_id, enqueue_index)`; dedupe by `(tenant_id, client_id, session_epoch, message_id, direction)`; retained trie order; manifests canonical JSON).
- Cross-PRG forwards derive only from committed WAL entries; forward RPCs are non-authoritative.
- WAL/snapshot format changes ship dual-read compatibility tests.

## 21.2  Fault Injection
Test RPCs: `InjectThreadDelay`, `CorruptLastBlock`, `DisableBackpressure`, `TimeShiftCertExpiry`.  
Used in CI to validate recovery.

## 21.3  Performance Tests
- Latency targets: ≤ 10 ms p99 single-PRG; ≤ 14–16 ms p99 cross-PRG; failover < 300 ms single-node / < 2 h region.  
- Scale test: 100 M connections across ~10k nodes with lower per-node density to stress CP/DR/placement.  
- Cross-PRG QoS 2 tests cover rebalance `forward_seq` continuity and DR promotions (duplicates per §4.2). Validation overlays only; Clustor guarantees remain.

## 21.4  Implementation Gate Checklist
- CI must pass deterministic replay, fault-injection, performance suites, and Clustor conformance/model-check fixtures.  
- Cross-PRG QoS 2 paths (forward_seq/epoch/DR replay) require property-based/model checks.  
- Lint/safety scans: zero high/critical; medium findings need risk acceptance.  
- `/readyz`/`/livez`, metrics, tracing validated in staging (~≥10 % scale).  
- Code layout: production under `src/`; integration/replay tests under `tests/`; avoid dead-code flags.  
- Release notes capture incompatible config changes, migrations, observability additions.

## 21.5  Deterministic Coding Guidelines
- Core Raft/SP modules expose time/RNG only via a recorded tick/RNG facade (`Instant::now()`/`rand::thread_rng()` banned); production and replay share this source.  
- Crates with background timers/backoff must be audited for determinism; prefer explicit abstractions. `cfg(test)` hooks only in interface crates to surface deterministic controls, not to change production behaviour. Clustor provides the deterministic Raft core; no alternate timing sources are permitted.

---

# 22  Summary of Guarantees

| Property | Guarantee |
|-----------|------------|
| Ordering | Total within topic partition |
| Consistency | Linearizable within partition |
| Durability | Quorum fsync (Strict only; Group-fsync is opt-in and does not carry the no-ACK-loss guarantee) |
| Availability | 99.999 % |
| Exactly-once | MQTT QoS 2 within broker in healthy region or controlled DR; uncontrolled DR = at-least-once within replication lag (downstream adapters define their own) |
| Replay | Deterministic |
| Security | mTLS, AES-GCM, Ed25519 |
| Audit | Immutable 12 mo |
| DR | RPO 0 (fenced) / ≤ lag if catastrophic, RTO ≤ 2 h |
| Upgrade | Zero-downtime rolling |
| Multi-tenancy | Hard isolation |
| Observability | Unified metrics, logs, traces |
| Efficiency | 10 µs/msg design, v1 must-meet ≤ 60–80 µs/msg (stretch 35–50); ≤ 4 KiB idle conn |

---

# 23  Implementation Roadmap
- **v1:** TCP/TLS (QUIC optional), Strict durability, cross-region DR for production tenants, deterministic replay within a build, performance per §14.2/§14.6, CP as a single 3-voter cluster (observers optional).  
- **v1.x:** Cross-build deterministic replay, QUIC/H3 reaches TCP parity, DR shipping default for production, tuned backpressure, CP scale-out with observers.  
- **v2:** Pursue 20–30 µs then 10 µs targets, consider CP sharding if needed, expand optional features (Group-fsync labs, bridges). Out-of-scope: non-Linux, >3 AZ quorum per PRG, relaxed durability.
The roadmap is a product-plan overlay; it does not modify Clustor semantics.

# Appendix A – Operational Runbooks

## A.1  Node Replacement
- Mark node unhealthy and fence affected partitions (epoch++); launch replacement (auto-enroll).  
- Assign PRGs, catch up from snapshots/WAL, then clear fence when `/readyz` ≥ 99 % and lag/fsync metrics are healthy.

---

## A.2  Disaster Recovery (DR) Drill
- Pre-check DR region healthy, lag ≤ 5 s, no fences.  
- Fence primary (`/v1/dr/fence`), verify DR caught up, promote (`/v1/dr/promote`), reroute traffic, monitor readiness/lag, then rejoin primary as follower.

---

## A.3  Handling Durability Fence
- On `wal_durability_fence_active`, inspect disk/FS health; force quorum fsync; evict bad replica if needed; clear fence and re-enable Group-fsync per policy.

---

## A.4  Quota Breach and Throttling
- Alerts at 0.95 / 1.05; throttle and notify.  
- After 24 h >1.5× quota, disconnect excess and block publishes; apply new limits via CP if upsold.

---

## A.5  Certificate Rotation
- Upload new chain via CP; listeners hot-swap; maintain dual-trust 24 h; verify `security_cert_expiry_seconds` > 3 days.

---

## A.6  Rolling Upgrade
- `maxUnavailable=1`; per pod transfer leadership, checkpoint/fsync, drain/terminate.  
- Validate readiness ≥ 99 %, lag and tail latency within SLO.

---

## A.7  Subscription Index Hotspot
- Detect `subscription_match_cpu_seconds` hotspot; move tenant topic hash ranges to a new PRG (epoch++); verify routing and `apply_lag_seconds` during migration.

---
