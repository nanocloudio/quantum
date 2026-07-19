# Observability

Quantum's observability surface: metrics, traces, and audit. Three
modules produce the bulk of operator-visible output — `telemetry_agg`
(substrate metrics), `metrics_aggregator` (high-cardinality
application metrics), and `audit_logger` (signed compliance events)
— all surfaced through `http_surface`.

The metric model deliberately splits "fan-in count" from "dimensional
aggregation". Substrate counters are low-cardinality and
high-frequency; application metrics are high-cardinality and
slower-changing; audit is structured tamper-evident logging with a
different consumer entirely. Conflating them produces a single module
whose state arena is hard to size and whose retention policy is
impossible to set correctly.

## Metrics

### Three sources

| Source | Cardinality | Tick rate | Surfaces on |
|---|---|---|---|
| `telemetry_agg` (Clustor substrate) | Low (Raft state, WAL throughput, replicator lag, fsync time) | 1s | `/metrics` directly |
| `metrics_aggregator` (Quantum) | High (per-tenant × per-protocol × per-PRG) | 10s rollup default | Forwarded to `telemetry_agg` for export |
| `backpressure_propagator` (Quantum) | Per-protocol counters + queue-depth gauges | Real-time | Streamed via `telemetry_agg` |

The aggregator-then-export pattern keeps cardinality explosion
contained: per-dimension storm guards in `metrics_aggregator` cap the
number of unique label combinations exposed per export window, so a
misconfigured client cannot blow up the metric registry.

### Substrate counters (low-cardinality)

| Metric | Type | Purpose |
|---|---|---|
| `clustor_raft_term` | gauge | Current Raft term per PRG. |
| `clustor_raft_commit_index` | gauge | Latest committed WAL index per PRG. |
| `clustor_wal_fsync_seconds` | histogram | Per-fsync duration. |
| `clustor_apply_lag_seconds` | gauge | Apply lag vs. commit. |
| `clustor_replicator_lag_bytes` | gauge | Replicator behind leader in bytes. |
| `clustor_replicator_lag_entries` | gauge | Replicator behind in WAL entries. |
| `clustor_cp_cache_state` | gauge | 0=Fresh, 1=Cached, 2=Stale, 3=Expired. |
| `clustor_cp_cache_age_ms` | gauge | Age of cached CP manifest. |
| `clustor_durability_proof_lag_seconds` | gauge | Time from quorum durable to proof emit. |

These are emitted by `telemetry_agg` at 1Hz with no per-tenant
dimension; cardinality is bounded by node count × PRG count per node.

### Application metrics (high-cardinality)

Dimensioned by `(tenant, protocol, prg)` where applicable:

| Metric | Type | Purpose |
|---|---|---|
| `quantum_publish_total` | counter | Publish count per `(tenant, protocol, prg, qos)`. |
| `quantum_publish_bytes_total` | counter | Publish byte volume. |
| `quantum_publish_latency_seconds` | histogram | Edge ingress to ACK emit. |
| `quantum_forward_total` | counter | Cross-PRG forwards per `(ingress, egress)`. |
| `quantum_forward_latency_seconds` | histogram | Forward request → ack. |
| `quantum_active_sessions` | gauge | Current open sessions per `(tenant, protocol)`. |
| `quantum_subscription_count` | gauge | Active subscriptions per tenant. |
| `quantum_offline_queue_depth` | gauge | Per `(tenant, prg)` offline queue length. |
| `quantum_dedupe_shard_utilization` | gauge | Fill ratio per dedupe shard. |
| `quantum_session_duration_seconds` | histogram | CONNECT → DISCONNECT lifetime. |
| `quantum_backpressure_signal_total` | counter | Translated signals per `(tenant, protocol, reason)`. |
| `quantum_quota_exceeded_total` | counter | Per-tenant quota violations. |

Cardinality budget: `cardinality_limit = 100 000` default. Beyond
that, `metrics_aggregator` aggregates excess dimensions into an
`__overflow__` bucket and emits a warning event.

### Operator queries

| Question | Metric / endpoint |
|---|---|
| Is the broker accepting new connections? | `quantum_active_sessions` rate; `/readyz` |
| Are publishes acknowledged? | `quantum_publish_latency_seconds` p99; `quantum_backpressure_signal_total` rate |
| Is consensus healthy? | `clustor_raft_term` stability; `clustor_replicator_lag_bytes` headroom |
| Are we durable? | `clustor_durability_proof_lag_seconds` near zero |
| Is one tenant hot? | `quantum_publish_total` by tenant; `quantum_quota_exceeded_total` by tenant |
| Is CP stale? | `clustor_cp_cache_state == Fresh`; `clustor_cp_cache_age_ms` < `refresh_fresh_ms` |

## Tracing

OpenTelemetry spans for the hot path:

| Span | Emitted at | Attributes |
|---|---|---|
| `quantum.connect` | CONNECT handshake | tenant, protocol, client_id, session_epoch, auth outcome |
| `quantum.publish` | Publish accepted at edge → ACK emit | tenant, topic, qos, payload size, forward path, durability latency |
| `quantum.forward` | Cross-PRG forward issue → destination ack | ingress PRG, egress PRG, forward_seq, network or local path |
| `quantum.authorize` | ACL evaluation | tenant, subject, operation, role, decision |
| `quantum.disk_io` | WAL append / fsync / snapshot read | byte count, latency, fsync grouping count |
| `quantum.dr` | DR event lifecycle | event class, target site, fence state |

Spans propagate through `WorkloadForwardEnvelope` so cross-PRG and
cross-node operations carry the originating trace context.

## Audit

`audit_logger` is structurally distinct from metrics — different
consumers, different retention, different correctness requirements —
and lives in its own module for that reason (see the alignment
analysis in [native_fluxor.md](../native_fluxor.md)).

### Event classes

| Class | Examples |
|---|---|
| `auth` | CONNECT success/failure, mTLS cert digest, SPIFFE ID, SASL outcome |
| `acl` | ACL allow/deny per (session, subject, operation) |
| `throttle` | Quota exceeded with tenant ID, sustained duration, metric value |
| `admin` | Tenant CRUD, placement plans, throttle overrides, leader transfer, shrink plans |
| `dr` | Checkpoint export, WAL archive shipment, fenced promotion outcome |
| `breakglass` | Every elevated action with operator identity and reason |
| `disconnect` | DISCONNECT emit including reason code |

### Properties

| Property | Value |
|---|---|
| Format | Structured JSON with canonical field order |
| Signature | Ed25519 per-entry; chained per-segment for tamper detection |
| Retention | 400 days default; configurable per event class |
| Sink | Local file + optional SIEM forward via OpenTelemetry logs |
| Schema versioning | Each entry carries a `schema_version` field; consumers must validate |

Audit events do not flow through the metrics pipeline. Confusing the
two is a common antipattern — metrics get decimated and aggregated to
manage cost; audit must be preserved verbatim for compliance.

## Readiness and why surfaces

| Endpoint | Purpose |
|---|---|
| `/readyz` | Green only when CP cache is Fresh, all PRGs have completed replay, durability fences are clear, and the listener is not draining. Returns 503 with a one-line summary when red. |
| `/why` | Structured explanation of any non-ready state. Returns the gating condition (CP Stale, PRG replaying, fence active, listener draining), the module that owns the condition, and the elapsed time in that state. |
| `/healthz` | Liveness — process responds at all. Cheap, no quorum read. Use for "should we restart this container" checks; use `/readyz` for "should the load balancer route to this node". |

The Why surface is the primary tool for diagnosing "node is up but
not serving traffic" incidents. It is intentionally structured
(machine-readable JSON) so on-call dashboards can correlate without
parsing free text.

## Storm guards

Every observability module has a storm guard:

- `telemetry_agg.incident_storm_guard = 50` — caps incident-related
  metric emissions per export window so a single misbehaving module
  cannot saturate the export channel.
- `metrics_aggregator.per_dimension_storm_guard = 50` — same idea,
  applied per-dimension to prevent one tenant from monopolising the
  cardinality budget.
- `audit_logger` rate-limits per event class with overflow events
  when limits are hit; an "audit lost" warning is itself audited.

These exist because observability surfaces are the most common source
of self-inflicted denial-of-service in production systems: a metric
explosion or an audit storm can starve the apply loop. The storm
guards trade fidelity for safety under pathological conditions.

## Operator surfaces

- `/metrics` — Prometheus exposition. Default port `9100`; scrape interval 15s recommended.
- `/why` — Polled by operator dashboards; not by load balancers.
- `/admin` audit replay — `admin_handler` exposes a windowed audit query (signed-only, no mutation).
- OpenTelemetry traces — forwarded to whatever OTLP collector the deployment configures; standard span format.

The validation workflow that exercises these surfaces under load
lives in [guides/performance.md](../guides/performance.md).
