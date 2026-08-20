# Flow Control

How Quantum admits work into the apply pipeline, propagates pressure
back to clients, and issues per-session consumer credits. Three
modules cooperate: `admission` (substrate, node-wide credits),
`flow`'s prefetch component (Quantum, per-session credits), and
`flow`'s backpressure component (Quantum, translation to protocol signals).

The throughput floor and latency figures stated here are design
contracts the flow-control machinery is sized against, not measured
results.

## Normative requirements

| Requirement | Value |
|---|---|
| Default inflight credit per session | 10 QoS 1/2 slots (MQTT) / equivalent for Kafka, AMQP |
| Per-PRG throughput floor | ≥ 500 publishes/sec sustained at QoS 2 semantics (four-phase handshake) under steady-state load |
| In-AZ p99 server-side latency | ≤ 10 ms from edge ingress to WAL quorum commit, excluding client RTT |
| Backpressure signal | A throttle envelope carrying reason and backlog, translated to a protocol-native response |

Adapters enforce the lower of (client credit, tenant policy ceiling).
Backpressure signals are protocol-native, not generic — clients see
something they already know how to interpret.

## Three layers

| Layer | Module | Scope | Driven by |
|---|---|---|---|
| **Proposal admission** | `admission` (Clustor) | Node-wide singleton | Replication lag signal from `consensus.lag_signal` (Q16.16 PID over entry + byte credits) |
| **Consumer prefetch** | `flow`'s prefetch component (Quantum) | Per-session | Apply-to-delivery queue depth |
| **Backpressure translation** | `flow`'s backpressure component (Quantum) | Per-protocol | Queue depths + rejects from `gateway` |

The split is deliberate. Proposal admission is a node-level resource
decision (do we have headroom to accept another publish into the WAL
pipeline?); prefetch is a per-session demand decision (how many
messages should this consumer hold inflight?); translation is a
protocol decision (how do we tell the client to slow down in their
language?).

## Proposal admission (`admission`)

A dual-token PID controller in Q16.16 fixed-point arithmetic. Two
independent credit pools:

| Credit type | Default cap | Driven by |
|---|---|---|
| Entry credits | 4096 (max in flight, `entry_credit_max`) | Replication lag in WAL indexes |
| Byte credits | 64 KiB (`byte_credit_max_kib`) | Replication lag in bytes |

The PID samples every 100ms (`sample_period_ms`) and adjusts credit
availability based on the lag error term.

Anti-windup integral clamping prevents the controller from
accumulating credit debt during sustained overload. When credits
exhaust, `gateway`'s throttle component rejects new proposals at the
apply boundary;
rejected requests trigger backpressure-propagator translation.

## Consumer prefetch (`flow`'s prefetch component)

Per-session credit windows for consumer delivery. The data shape is
fundamentally different from `admission`'s singleton PID —
credits live per session, with their own state and lifecycle.

| Setting | Default | Source |
|---|---|---|
| `default_prefetch` | 10 messages | Lowered by client `Receive Maximum` / `prefetch_count` / consumer config |
| Lag threshold | 4/5 of the prefetch cap (8 with the default window) | Apply-to-delivery lag beyond this halves per-session credits until the lag normalises |

Protocol-specific bindings:

- **MQTT** — Maps to `Receive Maximum` in MQTT 5 CONNACK; for MQTT 3.x, enforced at the broker without wire negotiation.
- **Kafka** — Enforces fetch-session quota; throttled via `throttle_time_ms` in `FetchResponse`.
- **AMQP** — `Basic.Qos` `prefetch_count` / `prefetch_size`; AMQP 1.0 `flow` performative.

Credit updates flow to `session_processor.prefetch_credits` for
inflight accounting. When apply-to-delivery lag exceeds the
threshold, credits per session reduce until the lag normalises, then
ramp back to the configured maximum.

## Backpressure translation (`flow`'s backpressure component)

The operational observability surface for flow control. Tracks queue
depths, evaluates configurable thresholds, and emits per-protocol
metrics that operators watch on dashboards.

The component also has an `envelope_in` port and an `on_envelope`
handler for a substrate-supplied throttle envelope, but no deployment
graph wires it — the substrate declares no emitter for it — so today
the translation runs on queue depths and `gateway.rejected` alone.
Wiring the envelope path needs a substrate-side decision about what
emits it.

### Queue depth thresholds

| Queue | Default threshold | Action when crossed |
|---|---|---|
| Commit-to-apply | 10 000 entries | Emit `TransientBackpressure` → protocol pause-ack signal. |
| Apply-to-delivery | 5 000 entries | Drop QoS 0 publishes; throttle QoS 1/2. |
| Retained write buffer | 16 MiB | Block retained writes until drained. |

### Signal-to-protocol mapping

| Signal class | MQTT 5 | Kafka | AMQP 0-9-1 |
|---|---|---|---|
| `BP_TRANSIENT` | `0x97` (Quota exceeded) reason code | `THROTTLING_QUOTA_EXCEEDED` + `throttle_time_ms` | `Channel.Flow{active=false}` |
| `BP_PERMANENT_DURABILITY` | DISCONNECT | `connection.close` | `connection.close{reply-code=resource-error}` |
| `BP_PERMANENT_EPOCH` | `dirty_epoch` handling per [mqtt_adapter.md](mqtt_adapter.md) | `NOT_LEADER_OR_FOLLOWER` | Connection close |

The translation is mechanical — `flow`'s backpressure component does not
make policy decisions, it does protocol mapping. Policy lives in
`admission` and `governance`'s tenants component.

### Metrics

Every translated signal increments a counter dimensioned by
`(tenant, protocol, prg, reason)`. Gauges expose current queue depths
for each tracked queue. These flow through `governance`'s telemetry component to
`operations` — the signal operators use to detect that flow control
is engaging. See [observability.md](observability.md).

The three concerns are separate modules because their state and math
differ: admission control is a node-wide PID loop (sample periods,
integral windup, gain tuning), prefetch is per-session credit state, and
backpressure translation is a stateless observation + lookup. See
[architecture.md](../architecture.md#flow-control-and-acknowledgement)
for the module boundaries.

## Operator surfaces

- The metrics export carries credit headroom, queue depths, and per-protocol backpressure signal counts.


