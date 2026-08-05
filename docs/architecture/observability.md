# Observability

Quantum's observability surface: metrics, traces, and audit. Three
modules produce the bulk of operator-visible output — `operations`
(substrate metrics), `governance`'s telemetry component (high-cardinality
application metrics), and `governance`'s audit component (signed compliance events)
— all surfaced through `gateway`.

The metric model deliberately splits "fan-in count" from "dimensional
aggregation". Substrate counters are low-cardinality and
high-frequency; application metrics are high-cardinality and
slower-changing; audit is structured tamper-evident logging with a
different consumer entirely. These stay separate concerns with separate
state and separate retention: `governance`'s telemetry and audit
components each own their table and their own bound, and neither can
reach into the other's. Co-locating them in one module shares a
scheduler entity, not an arena or a retention policy.

## Metrics

### Three sources

| Source | Cardinality | Tick rate | Surfaces on |
|---|---|---|---|
| `operations` (Clustor substrate) | Low (Raft state, WAL throughput, replication lag, fsync time) | 1s | `/metrics` directly |
| `governance`'s telemetry component (Quantum) | High (per-tenant × per-protocol × per-PRG) | 10s rollup default | Forwarded to `operations` for export |
| `flow`'s backpressure component (Quantum) | Per-protocol counters + queue-depth gauges | Real-time | Streamed via `operations` |

The aggregator-then-export pattern keeps cardinality explosion
contained: per-dimension storm guards in `governance`'s telemetry component cap the
number of unique label combinations exposed per export window, so a
misconfigured client cannot blow up the metric registry.

### Substrate counters (low-cardinality)

| Metric | Type | Purpose |
|---|---|---|
| `clustor_raft_term` | gauge | Current Raft term per PRG. |
| `clustor_raft_commit_index` | gauge | Latest committed WAL index per PRG. |
| `clustor_wal_fsync_seconds` | histogram | Per-fsync duration. |
| `clustor_apply_lag_seconds` | gauge | Apply lag vs. commit. |
| `clustor_replication_lag_bytes` | gauge | Follower behind leader in bytes. |
| `clustor_replication_lag_entries` | gauge | Follower behind in WAL entries. |
| `clustor_cp_cache_state` | gauge | 0=Fresh, 1=Cached, 2=Stale, 3=Expired. |
| `clustor_cp_cache_age_ms` | gauge | Age of cached CP manifest. |
| `clustor_durability_proof_lag_seconds` | gauge | Time from quorum durable to proof emit. |

These are emitted by `operations` at 1Hz with no per-tenant
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
that, `governance`'s telemetry component aggregates excess dimensions into an
`__overflow__` bucket and emits a warning event.

### Operator queries

| Question | Metric / endpoint |
|---|---|
| Is the broker accepting new connections? | `quantum_active_sessions` rate; `/readyz` |
| Are publishes acknowledged? | `quantum_publish_latency_seconds` p99; `quantum_backpressure_signal_total` rate |
| Is consensus healthy? | `clustor_raft_term` stability; `clustor_replication_lag_bytes` headroom |
| Are we durable? | `clustor_durability_proof_lag_seconds` near zero |
| Is one tenant hot? | `quantum_publish_total` by tenant; `quantum_quota_exceeded_total` by tenant |
| Is CP stale? | `clustor_cp_cache_state == Fresh`; `clustor_cp_cache_age_ms` < `refresh_fresh_ms` |

## Export id-table budget

`fluxor validate` warns that the OTLP id-table exceeds its cap on the
larger graphs:

```
warning: observability id-table is 2696 bytes (> 2048 cap); names beyond
the 2030-byte boundary won't resolve in otlp_http
```

The table is built by the config tool as one `module_idx:instrument_id=name;`
entry per declared instrument across every module in the graph, and the
`otlp_http` exporter parses it into a fixed `IDTABLE_MAX = 2048` buffer.
Past that boundary an instrument still *emits* — the sample carries a
numeric `(module, id)` pair and is exported — but it resolves to no name
on the collector side. So the failure is silent and cosmetic-looking,
and it lands on whichever modules sort last by graph index.

Attribution for `quantum-pi5.yaml` (2626 B by this accounting; the tool
reports 2696 B including instruments this table omits):

| module | instruments | bytes |
|---|---:|---:|
| `consensus` (substrate) | 32 | 616 |
| `operations` (substrate) | 21 | 430 |
| `durability` (substrate) | 18 | 372 |
| `governance` | 10 | 211 |
| `flow` | 10 | 169 |
| `protocol` | 8 | 155 |
| `messaging` | 11 | 144 |
| `topic_engine` | 6 | 114 |
| `forward_coordinator` | 6 | 105 |
| `session_processor` | 6 | 98 |
| everything else | 11 | 212 |

**The three Clustor substrate modules are 54% of the table.** Quantum's
own application modules total 679 B, and their declared lists match what
the code emits — there is no padding to trim. Deleting every Quantum
application instrument would barely clear the 578 B overflow, at the
cost of the entire application-metrics surface.

So this is not a Quantum-side problem to fix by trimming. The options,
in order of preference:

1. **Raise `IDTABLE_MAX`** in fluxor
   (`tools/src/config/builder.rs`, and the matching exporter buffer).
   The graph has outgrown a 2 KiB table; 4 KiB restores headroom for
   both the substrate and the application tier. This is a Fluxor change
   and is the right one.
2. **Shorten instrument names.** Entry overhead is ~5 B plus the name,
   so the ~39 Quantum instruments at ~12 characters average would save
   roughly 150 B if cut to 8. Not enough alone, and it degrades
   readability at the collector.
3. **Drop instruments.** Only worth considering for the substrate,
   which is where the bytes are — and that is a Clustor decision.

Until (1) lands, treat any missing metric *name* at the collector on a
large graph as this cap rather than a broken emitter: check whether the
instrument's module sorts late in the graph's module list.

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

`governance`'s audit component is structurally distinct from metrics — different
consumers, different retention, different correctness requirements —
and lives in its own module for that reason.

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

- `operations.incident_storm_guard = 50` — caps incident-related
  metric emissions per export window so a single misbehaving module
  cannot saturate the export channel.
- `governance` telemetry's `per_dimension_storm_guard = 50` — same idea,
  applied per-dimension to prevent one tenant from monopolising the
  cardinality budget.
- `governance`'s audit component rate-limits per event class with overflow events
  when limits are hit; an "audit lost" warning is itself audited.

These exist because observability surfaces are the most common source
of self-inflicted denial-of-service in production systems: a metric
explosion or an audit storm can starve the apply loop. The storm
guards trade fidelity for safety under pathological conditions.

## Operator surfaces

- `/metrics` — Prometheus exposition. Default port `9100`; scrape interval 15s recommended.
- `/why` — Polled by operator dashboards; not by load balancers.
- `/admin` audit replay — `operations` exposes a windowed audit query (signed-only, no mutation).
- OpenTelemetry traces — forwarded to whatever OTLP collector the deployment configures; standard span format.

The validation workflow that exercises these surfaces under load
lives in [guides/performance.md](../guides/performance.md).
