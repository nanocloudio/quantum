# Observability

Quantum's observability surface: metrics, the diagnostic HTTP
endpoints, and audit. Three modules produce the bulk of
operator-visible output — `operations` (substrate metrics fan-in and
the HTTP surface), `governance`'s telemetry component
(high-cardinality application metrics), and `governance`'s audit
component (chained compliance events).

The metric model deliberately splits "fan-in count" from "dimensional
aggregation". Substrate counters are low-cardinality and
high-frequency; application metrics are high-cardinality and
slower-changing; audit is structured tamper-evident logging with a
different consumer entirely. These stay separate concerns with
separate state and separate retention: `governance`'s telemetry and
audit components each own their table and their own bound, and
neither can reach into the other's.

## Metrics

### Three sources

| Source | Cardinality | Surfaces on |
|---|---|---|
| Substrate modules (`consensus`, `durability`, `admission`) | Low: Raft state, WAL throughput, credit state | Fan into `operations.ingest` |
| `governance`'s telemetry component | High: dimensional per-(tenant, protocol, PRG) rollups | Forwarded to `operations.ingest` |
| `flow`'s backpressure component | Per-protocol signal counters and queue-depth gauges | Fan into the same aggregation |

Metrics are identified numerically, not by name: each module's
`manifest.toml` declares its instruments under `[observability]`,
each composite component stamps its own metric identity so the
aggregator can key per component, and samples carry a
`(module, instrument)` id pair on the wire.

The aggregator-then-export pattern keeps cardinality contained:
`governance`'s telemetry component caps unique label combinations at
100 000 (`cardinality_limit`) and aggregates the excess rather than
growing without bound.

### Export name resolution

Telemetry leaves the device id-interned: the export engine drains the
telemetry ring and emits compact batches whose samples carry the
numeric id pair — there is no on-device name table. The host
collector resolves names from the build-time instrument table the
config tool generates for the deployed graph (one
`module_idx:instrument_id=name;` entry per declared instrument).

A missing metric *name* at the collector therefore means the
collector's table is stale for the deployed graph — regenerate it
from the same config build — not that the device dropped anything.
Missing *samples* are a different symptom entirely (emitter gating,
ring overflow, or transport loss).

## Diagnostic HTTP endpoints

`operations` owns what each request means; wave's `http` module
(`app` variant) owns the HTTP mechanics, on its own listener wired
to `operations`' `request`/`response` envelope ports. A graph that
needs no admin/debug surface omits the wave module entirely:

| Endpoint | Purpose |
|---|---|
| `GET /readyz` | 200 when the cached readiness byte is set, 503 otherwise. |
| `GET /why` | The cached explanation payload for non-ready states. |
| `GET /metrics` | The cached metrics export. |
| `POST /admin/<op>` | Role-gated admin commands; 202 means the command reached the admin component. |

The readiness, why, and metrics payloads are caches fed over
`operations`' input ports; a graph that does not wire those feeds
serves empty bodies.

## Audit

`governance`'s audit component is structurally distinct from metrics
— different consumers, different retention, different correctness
requirements.

- Events are structured, sequence-numbered, and chained with
  HMAC-SHA256 over `[seq][timestamp][event]`, so truncation and
  tampering are detectable given the key. The key is currently
  derived at boot; durable key material is part of the security
  design ([security.md](security.md)).
- Event sources: `operations` admin verdicts, `session_processor`
  lifecycle events, and `governance`'s tenants and dr components
  (over in-module seam rings rather than graph edges).
- Audit events do not flow through the metrics pipeline. Metrics get
  decimated and aggregated to manage cost; audit must be preserved
  verbatim.

## Rate bounds

Observability surfaces are a classic source of self-inflicted
overload, so each is bounded: the telemetry component's cardinality
cap, per-step drain budgets on every metrics input, and fixed-size
event tables in the audit component. The bounds trade fidelity for
safety under pathological conditions.
