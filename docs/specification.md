1. Title
Quantum Messaging Overlay Technical Specification v0.1

2. Overview
- Quantum is a multi-protocol messaging overlay on the Clustor Raft core.
- **XO-BOUND** (exactly-once semantic guardrail): Adapters MUST NOT expose protocol-level exactly-once semantics unless both conditions hold: (1) the adapter opts into the four-phase durability contract (§9.3 QoS 2), and (2) the cluster is healthy or undergoing a controlled, fenced DR promotion.
- **L1** (Latency Scope): ≤10 ms p99 in-AZ server-side latency measured from edge ingress receipt to WAL quorum commit, excluding client RTT.
- Every tenant owns ≥1 PRG; production tenants MUST provision ≥3 PRGs (§5, §12).
- CP-Raft governs tenant metadata, routing epochs, quotas, and PKI material.
- Edge listeners terminate TLS 1.3/QUIC and route to PRGs by SNI + ALPN.

3. Scope
- This document specifies tenant routing, adapter contracts, and isolation boundaries. MQTT is the reference adapter; Kafka and AMQP sections explicitly override MQTT-defined behavior where specified.
- Clustor remains authoritative for Raft, durability, read gates, strict fallback, storage layout, and ledger ordering (Clustor spec §§3–12).

4. Non-Goals
- Re-defining consensus, WAL, or durability semantics already covered in Clustor.
- Supporting messaging deployments without per-tenant isolation or without mTLS authentication.

5. Terminology and Concepts

5.1 Core Terms
| Term | Definition |
| --- | --- |
| PRG | Partition Raft Group (tenant-scoped shard). |
| CP-Raft | ControlPlaneRaft cluster holding tenant manifests, routing epochs, quotas, PKI bundles, feature gates, and durability proofs. |
| Protocol Adapter | Module that maps a wire protocol (MQTT, Kafka, AMQP) onto session processor APIs and Clustor durability primitives. |
| Session Processor (SP) | Messaging state machine inside each PRG: handles connection binding, QoS/acks, inflight replay, offline queues, and protocol reason codes. |
| session_epoch | Monotone counter per `(tenant_id, stream_id)` fencing session/stream state; increments on clean reconnect or fenced takeover. |
| stream_id | Protocol-defined logical stream identifier (protocol-specific). |
| dedupe entry | `(tenant_id, stream_id, session_epoch, message_id)` map rejecting duplicates until `dedupe_ttl` expires. |
| offline_queue | Persistent FIFO storing durable deliveries for disconnected or throttled sessions. |
| forward_seq | Idempotence key for cross-PRG forwarding; monotonically increasing per `(ingress PRG, egress PRG, routing_epoch)` and persisted so replay fences duplicates. |
| dirty_epoch | Routing-epoch mismatch condition; adapters map it to protocol-specific outcomes (reject, disconnect, or retry). |

5.2 Default Timers
- Default timers (operator-configurable, with tenant overrides bounded by operator policy): `session_ttl_default_ms`, `dedupe_ttl_default_ms`, `offline_queue_ttl_default_ms` default to `259_200_000 ms`; maximum `604_800_000 ms`.

6. System Model

6.1 Components
- **Edge Listener**: SNI routing, client cert validation, forwards framed packets to PRGs via the Session Processor.
- **Session Processor (SP)**: Adapter logic, dedupe, offline queues, Will/retained features. All durable state mutation occurs inside the Raft apply loop; helpers may precompute state, but all mutations commit through the same apply path.
- **Control Agent**: Per-node helper syncing CP-Raft objects, publishing health, managing certificates, requesting rebalance actions.

6.2 Environment
- Linux ≥5.15 with io_uring, NVMe SSDs with write barriers enabled.
- Clocks synchronized via PHC/PTP; excessive skew MUST fence all PRGs on the affected node from leadership and from emitting protocol ACKs that require quorum durability until clocks recover.

6.3 Crash Model
- Crash model and write-path ordering are defined by Clustor §§6.2 and 10.5.

6.4 Assumptions
- Additional PRGs scale linearly with session count and publish load.
- CP-Raft outages shorter than `controlplane.cache_grace_ms` retain cached metadata; stale/expired caches trigger strict fallback per Clustor §9.1.
- Follower reads inherit the same gating: if Clustor refuses ReadIndex, adapters MUST fail closed with `ControlPlaneUnavailable`.

7. Messaging Data Model

7.1 Entities
- **Session record**: `{tenant_id, stream_id, session_epoch, auth_chain_digest, connected_at, keep_alive?, protocol_state}`.
- **Routing record**: `{subject, subscribers[], forward_seq, last_emit_index}` where `last_emit_index` tracks the last WAL index emitted by this PRG for that subject.
- **Retained record**: `{subject, payload_ref, updated_at}`. `payload_ref` references content-addressed storage.
- **Dedupe entry**: `DedupeKey → DedupeState{phase, publish_index, ack_index, expiry_at}`; entries expire after `dedupe_ttl`.
- **Offline queue entry**: `{sequence, payload_ref, expiry_at}`.
- **earliest_dedupe_index**: Per-PRG WAL index below which all dedupe entries have expired.
- **earliest_offline_queue_index**: Per-PRG WAL index below which all offline queue entries have expired.

7.2 Invariants
- **WAL-SOURCE**: Every observable messaging effect stems from WAL entries or signed snapshots; no parallel durable channels exist.
- **DETERMINISTIC-REPLAY**: Replay from WAL + snapshot deterministically reconstructs all durable messaging state. Compaction MUST NOT truncate WAL entries required to reconstruct state at or above `earliest_dedupe_index` or `earliest_offline_queue_index`.
- **ACK-DURABILITY**: Protocol-level ACKs emit only after WAL entries reach quorum durability.
- **ROUTING-EPOCH**: State transitions carry the CP-Raft routing epoch; mismatches result in `dirty_epoch` rejection.

8. Partitioning and Routing

8.1 Sharding
- Sessions and topics hash per tenant: `session_partition = hash64(tenant_id, client_id) % tenant_prg_count`, `topic_partition = hash64(tenant_id, normalized_topic) % tenant_prg_count`.
- Hash algorithm and seed are versioned; any change requires a CP-Raft-coordinated routing-epoch migration.

8.2 Placement and Rebalance
- CP-Raft assigns PRGs to nodes honoring locality, NVMe budget, and noisy-neighbor constraints. Membership changes follow Clustor §9.9.
- Rebalance: (1) CP-Raft issues placement plan; (2) PRGs clone via learner catch-up; (3) new routing epoch published; (4) clients reconnect. Publishes routed to old PRGs after the epoch flip are rejected with `dirty_epoch`.

8.3 Routing Guarantees
- Routing decisions remain stable within a routing epoch. Messages to the same topic on the same PRG are observed in WAL order; cross-PRG ordering is not guaranteed.

9. Protocol Lifecycles and QoS

9.1 MQTT Connection Establishment
- CONNECT binds to `client_id` scoped per tenant. Clean sessions increment `session_epoch` and purge prior inflight; persistent sessions reuse stored state.
- Keep Alive timeout: server defaults to client value × 1.5 (operator-configurable).

9.2 MQTT Authentication and Authorization
- CP-Raft stores tenant ACLs and feature flags. Stale caches force disconnect with `MQTT-5 0x87 (Not authorized)`.

9.3 MQTT QoS Semantics
- **QoS 0**: Fire-and-forget after syntax validation.
- **QoS 1**: PUBACK after quorum durability.
- **QoS 2**: Four-phase handshake state transitions persist in WAL; each requires quorum durability. Cross-PRG publishes use `ForwardPublish` with `forward_seq`.
- Cross-PRG duplicates detected via `forward_seq`; missing `PublishAcked` triggers replay. Fenced PRGs revert in-flight exchanges to pending state until strict fallback clears.

9.4 MQTT Shared Subscriptions
- Groups distribute messages by hashing delivery identifiers with a stable, versioned hash seed modulo group size. Best-effort ordering preserved within a PRG; no ordering guarantees across PRGs (§8.3).

9.5 MQTT Offline Delivery
- Offline queue entries inherit per-message expiry using §5 defaults unless tenants lower it.

9.6 Kafka Adapter

9.6.1 Connection and Authentication
- SASL identities map to CP-Raft principals as optional additives to mTLS (§13). Transactional IDs issued by CP-Raft.

9.6.2 Topics, Partitions, and Routing
- Topics are tenant-scoped. Each partition maps to a PRG via `hash64(tenant_id, topic, partition_id) % tenant_prg_count`. Raft voter membership is authoritative for durability.

9.6.3 Produce Semantics
- `acks=0`: Edge acceptance only; disabled unless tenant allows lossy ingest.
- `acks=1`, `acks=all`: Both map to quorum durability; leader-only acknowledgments are intentionally not supported.
- Idempotent producers: `forward_seq` + transactional ID prevent duplicates. Transactions: Begin/Commit/Abort in WAL; commit recorded after all involved PRGs reach `wal_committed_index`.

9.6.4 Consumer Groups and Offsets
- Group metadata in session processor. Offsets commit as WAL entries. Cooperative-sticky rebalance default. Fetch sessions enforce tenant quotas via `ThrottleEnvelope`.

9.6.5 Retention and Catch-Up
- Retention per topic (time/size). Compaction obeys Clustor floors (§7.2). Catch-up from snapshots + WAL.

9.7 AMQP 1.0 Adapter

9.7.1 Connection and Authentication
- SASL optional, binds to CP-Raft identities. Transport security per §13.

9.7.2 Addressing and Routing
- Queues and exchanges tenant-scoped. Link endpoints hash to PRGs by `(tenant_id, address)`.

9.7.3 Delivery Semantics
- `settled=true`: Fire-and-forget.
- `settled=false`: At-least-once until `Disposition{state=Accepted}`; ACK after WAL durability.
- Transactions: Commit waits for `wal_committed_index` on all PRGs.
- Exactly-once semantics are valid only under XO-BOUND (§2); implementation leverages `forward_seq` plus delivery tags.

9.7.4 Flow Control and Credits
- Link credits persist in session state for deterministic replay. Backpressure clamps credits to zero with `drain=true` until pressure clears.

9.7.5 Offline and Durable Subscriptions
- Durable subscriptions persist link state. Shared subscriptions hash delivery tags modulo group size.

10. Control Plane (CP-Raft)

10.1 Responsibilities
- Tenant records: PRG count, quotas, ACLs, RBAC, certificates, compliance policies.
- Routing epochs, placement manifests, feature flags, throttling budgets.
- Durability proofs per Clustor §9.8; adapters MUST stall reads/acks when durability proof verification fails.

10.2 APIs and Caches
- Admin surface: `Tenants`, `Routes`, `Certificates`, `Quotas`, `Throttle`. Signed requests, canonical JSON responses.
- Cache states: **Fresh** (within TTL), **Stale** (TTL exceeded, grace active—blocks new handshakes requiring policy evaluation), **Expired** (grace exceeded—tears down policy-bound operations).

11. Multi-Tenant Isolation and Quotas
- **Namespaces**: All resources scoped per tenant. No cross-tenant access unless CP-Raft defines a bridge policy.
- **Resource quotas**: Operator-defined per tenant. Enforced via `ThrottleEnvelope{reason=QuotaExceeded}`.
- **Noisy-neighbor control**: Token buckets; sustained overage >60 s triggers disconnects.

12. Flow Control and Performance

12.1 Normative Requirements
- **Credits**: Sessions MUST start with 10 in-flight QoS 1/2 slots. Adapters MUST enforce the lower of client and tenant policy.
- **Backpressure**: PRG MUST emit `ThrottleEnvelope` with reason, backlog, retry hints. Edge MUST translate to protocol-native signals.
- **Per-PRG throughput floor**: Implementations SHOULD sustain ≥500 publishes/s per PRG measured at QoS 2 semantics (four-phase handshake) under steady-state load.
- **Node-local latency ceiling**: Sustained violations MUST trigger operator alerts.
- Flow control integrates with Clustor §10.6.

12.2 Informational Planning Targets (Non-Normative)
The following figures guide capacity planning but are not compliance requirements:
- Session density: 50k steady / 75k burst per PRG.
- Cluster aggregate throughput: per-PRG floor (§12.1) × PRG count.

13. Security Model
- **Transport**: TLS 1.3 with mTLS mandatory; QUIC supported.
- **Identity**: Tenant-issued or provider-issued client cert; optional JWT extends RBAC but never replaces mTLS.
- **Authorization**: Policy bundles in CP-Raft define allowed subjects, QoS ceilings, retention, permissions.
- **At-rest encryption**: Clustor AES-GCM; retained/offline payloads reuse encryption domains.
- **Signing**: Ed25519 for manifests, proofs, audit logs (per Clustor §9.8).

14. Observability and Operations
- **Metrics**: Prometheus exporters for per-tenant counters, PRG health, dedupe depth, offline queue age.
- **Tracing**: OpenTelemetry spans for CONNECT, publish, forward, authorization, disk I/O.
- **Audit**: CONNECT/DISCONNECT, auth failures, ACL decisions, throttles, DR events.
- **Runbooks**: Appendix A references ops handbook.

15. Disaster Recovery and Upgrades
- **DR**: Cross-region replication via Clustor snapshot export/import. Unfenced DR operates outside XO-BOUND guarantees and may degrade to at-least-once; the duplicate window is bounded by replication lag. Controlled promotions require `FenceCommit` plus durability-ledger verification.
- **Upgrades**: Rolling across PRGs with zero downtime. Nodes drain (≤30 s), transfer leadership, upgrade, rejoin. CP data hot-reloadable via `Reload` RPCs.
- **Testing**: Fault injection covers link drops, disk latency, CP-Raft outages, PRG relocation. Deterministic replay tests assert WAL-SOURCE and DETERMINISTIC-REPLAY for each release.
