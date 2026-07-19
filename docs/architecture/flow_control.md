# Flow Control

How Quantum admits work into the apply pipeline, propagates pressure
back to clients, and issues per-session consumer credits. Three
modules cooperate: `flow_controller` (substrate, node-wide credits),
`prefetch_controller` (Quantum, per-session credits), and
`backpressure_propagator` (Quantum, translation to protocol signals).

The throughput floor and latency targets stated here are the
normative performance contracts; the harnesses that exercise them
live in [guides/performance.md](../guides/performance.md).

## Normative requirements

| Requirement | Value |
|---|---|
| Default inflight credit per session | 10 QoS 1/2 slots (MQTT) / equivalent for Kafka, AMQP |
| Per-PRG throughput floor | ≥ 500 publishes/sec sustained at QoS 2 semantics (four-phase handshake) under steady-state load |
| In-AZ p99 server-side latency | ≤ 10 ms from edge ingress to WAL quorum commit, excluding client RTT |
| Backpressure signal | `ThrottleEnvelope{reason, backlog, retry_hints}` — adapter translates to protocol-native response |

Adapters enforce the lower of (client credit, tenant policy ceiling).
Backpressure signals are protocol-native, not generic — clients see
something they already know how to interpret.

## Three layers

| Layer | Module | Scope | Driven by |
|---|---|---|---|
| **Proposal admission** | `flow_controller` (Clustor) | Node-wide singleton | Replicator lag signal (Q16.16 PID over entry + byte credits) |
| **Consumer prefetch** | `prefetch_controller` (Quantum) | Per-session | Apply-to-delivery queue depth |
| **Backpressure translation** | `backpressure_propagator` (Quantum) | Per-protocol | Queue depths + envelope from `flow_controller` |

The split is deliberate. Proposal admission is a node-level resource
decision (do we have headroom to accept another publish into the WAL
pipeline?); prefetch is a per-session demand decision (how many
messages should this consumer hold inflight?); translation is a
protocol decision (how do we tell the client to slow down in their
language?).

## Proposal admission (`flow_controller`)

A dual-token PID controller in Q16.16 fixed-point arithmetic. Two
independent credit pools:

| Credit type | Default cap | Driven by |
|---|---|---|
| Entry credits | 4096 (max in flight) | Replicator lag in WAL indexes |
| Byte credits | 64 MiB | Replicator lag in bytes |

The PID samples every 100ms (default) and adjusts credit availability
based on the lag error term. Profiles tune the gain triple:

| Profile | When to use |
|---|---|
| `Latency` | Low-latency workloads with steady load; aggressive ramp-down on lag. |
| `Throughput` | Bulk publish workloads; tolerates moderate lag for higher sustained rate. |
| `WAN` | High-RTT replication paths; integral term dominates to absorb transient lag. |

Anti-windup integral clamping prevents the controller from
accumulating credit debt during sustained overload. When credits
exhaust, `throttle_gate` rejects new proposals at the apply boundary;
rejected requests trigger backpressure-propagator translation.

## Consumer prefetch (`prefetch_controller`)

Per-session credit windows for consumer delivery. The data shape is
fundamentally different from `flow_controller`'s singleton PID —
credits live per session, with their own state and lifecycle.

| Setting | Default | Source |
|---|---|---|
| `default_prefetch_count` | 10 messages | Tenant policy (lowered by client `Receive Maximum` / `prefetch_count` / consumer config) |
| `adaptive_scaling` | true | Reduces credits when apply-to-delivery lag exceeds threshold |
| `apply_delivery_lag_threshold` | 1000 entries | Empirical default; raise for bulk consumers, lower for tight-latency consumers |

Protocol-specific bindings:

- **MQTT** — Maps to `Receive Maximum` in MQTT 5 CONNACK; for MQTT 3.x, enforced at the broker without wire negotiation.
- **Kafka** — Enforces fetch-session quota; throttled via `throttle_time_ms` in `FetchResponse`.
- **AMQP** — `Basic.Qos` `prefetch_count` / `prefetch_size`; AMQP 1.0 `flow` performative.

Credit updates flow to `session_processor.prefetch_credits` for
inflight accounting. When apply-to-delivery lag exceeds the
threshold, credits per session reduce until the lag normalises, then
ramp back to the configured maximum.

## Backpressure translation (`backpressure_propagator`)

The operational observability surface for flow control. Tracks queue
depths, evaluates configurable thresholds, and emits per-protocol
metrics that operators watch on dashboards.

### Queue depth thresholds

| Queue | Default threshold | Action when crossed |
|---|---|---|
| Commit-to-apply | 10 000 entries | Emit `TransientBackpressure` → protocol pause-ack signal. |
| Apply-to-delivery | 5 000 entries | Drop QoS 0 publishes; throttle QoS 1/2. |
| Retained write buffer | 16 MiB | Block retained writes until drained. |
| WAL dirty bytes | disabled by default | Block proposal admission if non-zero. |

### Envelope-to-protocol mapping

| Envelope | MQTT 5 | Kafka | AMQP 0-9-1 |
|---|---|---|---|
| `TransientBackpressure` | `0x97` (Quota exceeded) reason code | `THROTTLING_QUOTA_EXCEEDED` + `throttle_time_ms` | `Channel.Flow{active=false}` |
| `PermanentDurability` | DISCONNECT with `0x99` (Payload format invalid) semantics | `connection.close` with `INVALID_CONFIG` | `connection.close{reply-code=resource-error}` |
| `PermanentEpoch` | `dirty_epoch` rejection per [mqtt_adapter.md](mqtt_adapter.md) | `NOT_LEADER_OR_FOLLOWER` | `link-detach{detach-forced}` |

The translation is mechanical — `backpressure_propagator` does not
make policy decisions, it does protocol mapping. Policy lives in
`flow_controller` and `tenant_manager`.

### Metrics

Every translated signal increments a counter dimensioned by
`(tenant, protocol, prg, reason)`. Gauges expose current queue depths
for each tracked queue. These flow through `metrics_aggregator` to
`telemetry_agg` and surface on `/metrics` — they are the primary
signal operators use to detect that flow control is engaging in
production. See [observability.md](observability.md).

## Why three modules, not one

A previous design absorbed all three concerns into one module. Two
problems pushed the split:

1. **Per-session vs. node-wide state.** Prefetch state lives per
   session; admission state lives once per node. Combining them
   produces a module whose state arena scales with session count for
   one concern and is constant for another — confusing to size and
   harder to reason about.
2. **PID vs. queue-depth observer.** Admission control is a PID loop
   with sample periods, integral windup, and gain tuning.
   Backpressure translation is a stateless observation + lookup.
   They share no math; combining them just couples release schedules.

The module-alignment analysis in [native_fluxor.md](../native_fluxor.md)
walks through the full reasoning.

## Operator surfaces

- `/metrics` exposes credit headroom, queue depths, PID error term, replicator lag in bytes and entries, and per-protocol backpressure signal counts.
- `/why` includes flow-control state in the "node not ready" explanation when the runtime is gated by sustained backpressure.
- `admin_handler` accepts profile changes (`Latency` ↔ `Throughput` ↔ `WAN`) and quota overrides without restart; useful for planned-spike incidents.

The validation workflow that exercises these surfaces under load
lives in [guides/performance.md](../guides/performance.md).
