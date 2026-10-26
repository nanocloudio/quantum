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

## Ack registration (`flow`'s ack component)

A durability-gated publish is only acknowledged once `flow`'s ack
component holds a registration for it, so that registration is
reserved before the publish is accepted, not attempted afterwards.
`session_processor` reserves a parking slot for it when it binds the
proposal's correlation; a publish that cannot get one is refused
while it is still the client's to retry, and counted as throttled.

The ack component stops reading its input while its inflight table is
full rather than accepting a registration it cannot hold, and it makes
every per-step budget decision before taking a record off the channel —
a record's type is not known until it is read, so a drain that could
exceed a budget stops instead of consuming and discarding. The
registration waits in the channel, `session_processor` parks it in the
slot it reserved, and both sides retry on later ticks. Saturation
therefore shows up as bounded backpressure and rising latency, never
as a publish that was acknowledged and then forgotten.

A completion is released only once its MSG_ACK_EMIT has been written in
full; a short or refused write leaves the entry active for a later step.
Registrations carry the session generation the publish was accepted
under, and a completion whose generation no longer occupies the slot is
dropped — MQTT packet ids are client-chosen, so slot and packet id alone
would match a delayed completion against whichever client now holds the
slot.

## A session's in-flight table (`session_processor`)

An MQTT session's QoS 1/2 flows share one in-flight table of 16: the
client's publishes until they are answered, and the deliveries to it
until it acknowledges them. A client may pipeline publishes past it, and
MQTT 3.1.1 gives the broker no way to say so, so a publish that finds
the table full while one of the session's own flows is still being
committed — a QoS 1 or QoS 2 PUBLISH before its answer, a PUBREL before
its PUBCOMP — waits whole on its codec input, as if still in the
channel, until that commit frees or moves on a slot; the pressure
reaches the client's TCP window instead of the publish being dropped.
The input is not read past it meanwhile, so the other sessions on that
input wait with it, for a commit. The deliveries leave half the table to
the publishes, so a full table always holds one a commit will free,
never only flows that wait on acknowledgements still behind the publish
in the client's stream. Only a table held entirely by QoS 2 publishes
awaiting their PUBREL drops the next one, counted as throttled.

A delivery that cannot go yet waits where its reason says. One waiting
on something shared that clears without any client — the codec edge,
admission credit, its seat ring's horizon — is parked (8 slots), and the
parked ones are retried oldest first: a session's later deliveries wait
behind its earlier ones, so it receives them in the order they were
published. One waiting on its own client — its flow credit, its receive
maximum, its half of the in-flight table — joins its session's offline
queue instead (`[sess] hb spill=`), with whatever the session has parked
moved ahead of it; the session's later deliveries join the queue behind
them, even while the queue's head is parked here as a drain's answers,
and the queue is drained to the session in turn. So a client slow to
acknowledge holds back only its own deliveries.

`deliver_in` is read only while a park slot is free. Two of the eight
are never credited to drains, whose answers may wait on a client, so a
slot always frees without one. Meanwhile `topic_engine` holds its
fan-out on the full edge rather than dropping
([messaging_model.md](messaging_model.md#routing-record)).

A QoS 1/2 publish reaches `topic_engine` from its stash once its dedup
verdict and its durability are both in. Stashes are released in the
order they became ready, which is commit order, and one the topic bus
refuses holds back every stash behind it, so a subscriber receives a
publisher's messages in the order they were published. The apply path
reads a committed entry only with a stash slot free, so a stash is never
refused; a slot frees on its verdict and the topic bus, never on a
client.

## The exchange with `messaging` (`session_processor`)

Nothing either side asks of the other is asked twice, so nothing is
dropped on a full edge in either direction, and neither direction waits
on the other.

- **Toward `messaging`.** Dedup checks and phases, retained writes and
  reads, offline enqueues, releases and drains all go through one ordered
  outbox: what the op edge refuses waits there, in order, and is written
  as it has room. An input that files requests is read only while the
  outbox has room for the most its next record can file, beyond the room
  kept for what answers already credited may file: the apply path (a
  committed entry's worth), the control-plane bus (a placement update may
  release every session's queue) and `deliver_in` (a delivery joining its
  session's queue, with what the session has parked).
  Codec input files nothing toward `messaging` directly, so it is never
  held for it.
- **From `messaging`.** `messaging_in` is always read. It carries the
  dedup verdicts committed publishes wait on, and `messaging` holds its
  answers while they are not read, which would back up the op edge in
  turn. So the deliveries it carries are bounded at the source: a
  retained read or an offline drain carries a credit, `max`, and the
  worker reserves room for that many before it asks. `messaging` answers
  at most that many and ends with a marker (`MSG_RETAINED_READ_END` with
  the entry to resume from, `MSG_OFFLINE_DRAINED` with how many are left),
  and the worker asks again for the rest. A drain's answers are the oldest
  of its session's queue and must reach it in order, so each is credited
  a park slot. A retained read's answers may join an offline queue
  instead, so reads share a fixed credit of 8, backed by outbox room for
  that: a read finishes without any client acknowledging anything.
- **Whose answer.** A read or a drain remembers the identity of the
  session it was asked for, and each answer is checked against the
  identity its slot holds when it arrives. An answer for a session that
  has since been released, or whose slot a new session now holds, is
  dropped, and its credit comes back; slots are reused, and an answer
  asked for one session never reaches the next. A request `messaging`
  cannot read past its session slot is answered with its end marker
  alone, so no credit is ever stranded.
- **Order behind a backlog.** While a session's offline queue drains — a
  reconnect under `!clean_start`, a move's settle — its fresh deliveries
  join the queue behind what is there, and the session receives the
  backlog and then them, in order. The backlog clears once a drain finds
  nothing left and nothing joined meanwhile.

No wait closes a cycle, because nothing a commit waits on waits on a
client. A codec input held for a commit waits on the apply path. The
apply path waits on room in the outbox, on the topic and control-plane
edges, on a free retained-read slot and on a free stash slot. The outbox
drains whenever `messaging` reads its op edge, which needs only that its
answers are read, and the worker always reads them. A read slot frees
when its scan completes, which needs only read credit and outbox room. A
stash frees on its verdict and the topic edge. The topic edge drains as
`topic_engine` fans publishes out, and a fan-out held on a full deliver
edge waits only for the worker to read `deliver_in`, which waits on
outbox room and a free park slot; a slot not held by a drain's answer
frees on something shared, or its delivery moves to its session's queue.
Only drains' answers and offline queues wait on clients, and nothing
waits on them but the deliveries to those clients.

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


