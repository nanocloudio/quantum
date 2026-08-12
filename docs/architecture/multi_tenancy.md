# Multi-Tenancy

Quantum's tenant isolation model: how resources are scoped, how
quotas are enforced, how noisy neighbours are contained. Tenant
records themselves live in CP-Raft (see
[control_plane.md](control_plane.md)); enforcement happens in
`governance`'s tenants component plus the throttle and backpressure modules.

## Namespaces

Every resource is scoped to a tenant:

| Resource | Scoping |
|---|---|
| Topics / queues / partitions | `(tenant_id, name)` — no cross-tenant name collisions, no cross-tenant addressability. |
| Sessions | `(tenant_id, client_id)` — `session_processor` rejects connects whose presented identity does not match the cert-derived tenant. |
| Dedupe entries | `(tenant_id, stream_id, session_epoch, message_id)` — see [messaging_model.md](messaging_model.md). |
| PRGs | One ring per tenant; `tenant_prg_count` is set in the tenant record. |
| Certificates | Per-tenant trust bundle stored in CP-Raft. |
| ACLs / RBAC | Per-tenant policy bundle. |
| Quotas | Per-tenant token-bucket state in `governance`'s tenants component. |

No cross-tenant access exists unless CP-Raft defines an explicit
bridge policy — and the default is "no bridges". Adding a bridge
requires a CP admin operation, leaves an audit trail, and is enforced
at the `topic_engine` / `governance`'s tenants component level rather than at the
application layer.

## Resource quotas

Operator policy in CP-Raft sets the ceiling per tenant; tenants may
set tighter quotas internally but cannot exceed the operator ceiling.

| Quota | Unit | Enforced by |
|---|---|---|
| Connection count | open sessions | `session_processor` rejects CONNECT with `0x97` (Quota exceeded) when the tenant cap is reached. |
| Publish rate | messages/sec | `gateway`'s throttle component consumes per-tenant tokens from `governance`'s tenants component. |
| Publish bandwidth | bytes/sec | Same path; bytes counted separately. |
| Subscription count | active subscriptions | `topic_engine` rejects SUBSCRIBE past the cap. |
| Offline queue depth | entries per session | `messaging`'s offline component drops oldest with audit when cap is hit. |
| Retained payload count | retained topics per tenant | `messaging`'s retained component rejects new retained writes past the cap. |
| PRG count | partitions per tenant | CP-Raft refuses placement plans that exceed the operator ceiling. |

Quota violations surface as `ThrottleEnvelope{reason=QuotaExceeded}`,
which `flow`'s backpressure component translates into protocol-native
signals (MQTT `0x97`, Kafka `THROTTLING_QUOTA_EXCEEDED`, AMQP
`Channel.Flow{active=false}`).

## Noisy-neighbour control

Token-bucket enforcement is per-tenant. The bucket is sized from
operator policy (`refill_rate`, `burst_capacity`); over-spend depletes
the bucket and triggers throttling.

Sustained overage — defined as bucket exhausted for
`noisy_neighbor_threshold_s` (default 60s) — triggers `governance`'s tenants component
to emit a disconnect event to `session_processor`. The disconnect:

- Targets the highest-volume sessions first (capped publish rate per
  session).
- Issues protocol-appropriate close: MQTT `0x97` (Quota exceeded),
  Kafka `connection.close`, AMQP
  `connection.close{reply-code=resource-locked}`.
- Logs to `governance`'s audit component with the tenant ID, session ID, sustained
  duration, and the metric value that crossed the threshold.

The threshold + disconnect pattern is the load-shedding mechanism
that keeps a single misbehaving tenant from monopolising apply-loop
capacity. Tenants whose normal load approaches the threshold should
raise the operator quota rather than relying on the disconnect-recover
loop.

## Implementation status

**Tenancy is not plumbed end to end.** Every graph in `examples/`
currently produces exactly one tenant (id 0), and the mechanisms below
describe the design rather than what runs today.

What exists: `Session` carries a `tenant` field, the apply path threads
`p.tenant` from the proposal header, `governance`'s tenants component
enforces per-tenant token buckets, and the dedup key, retained store and
metric envelopes are all tenant-scoped. So the *plumbing* is tenant-aware
from apply onwards.

What is missing is the source. `control_plane.tenant_records` is now
wired into `governance.records_in` in every graph that carries both, so
quota enforcement is CP-driven rather than bootstrap-only. But the
record it delivers is the synthetic default
`[tenant_id = 0][max_rate = 10000]` — clustor's `control_plane/cp.rs`
marks it a placeholder — so it populates the quota table without
establishing which tenant a *client* belongs to.

That mapping is the actual gap, and it is upstream: nothing assigns a
session a tenant other than 0 until the control plane emits real tenant
data keyed by something a CONNECT carries. Three sites in
`session_processor` are marked `TENANCY GAP` where a tenant has to be
chosen and none is derivable:

- **CONNECT arrival** — no session exists yet, so the tenant would have
  to come from the control plane (from the client's credential or the
  ALPN/SNI namespace).
- **`finalise_stash`** and the **PROPOSAL_ASSIGNED** handler — both work
  off a stash keyed by correlation id, not by session.

Everywhere a session *is* reachable, the tenant is now read from it
rather than assumed (§8 rule 6: per-instance state, not a module
constant). Closing the remaining three needs two things Quantum cannot supply on
its own: real tenant records from the control plane (clustor side), and
a decision about what a CONNECT is keyed on — client credential, ALPN /
SNI namespace, or connection-level identity. Until both land, treat the
sections below as the target design.

## Isolation boundaries

| Boundary | Mechanism |
|---|---|
| **Network** | TLS / mTLS termination per tenant; SNI selects the cert chain; client cert validation against the per-tenant trust bundle. |
| **Storage** | Per-PRG snapshots and WAL frames; tenant data never lives in shared frames. |
| **Compute** | Apply-loop fairness via per-tenant credit budgets in `admission`; no tenant can starve another's apply scheduling. |
| **Memory** | Per-PRG state arena bounded by manifest; runaway state in one tenant's PRG cannot grow into another tenant's arena. |
| **Logs / metrics** | `governance`'s telemetry component cardinality includes the tenant dimension; `governance`'s audit component tags every event with the tenant ID. |

## Tenant lifecycle

| Operation | Path |
|---|---|
| **Create** | Admin posts a tenant manifest to `/admin`; CP-Raft commits; `control_plane` distributes the manifest; placement reconciler assigns PRGs. |
| **Update** | Same path; updates take effect on the next CP cache refresh (default within 5s). |
| **Delete** | Admin posts a tenant delete; CP-Raft marks the tenant draining; `session_processor` disconnects active sessions with `Server unavailable`; placement reconciler tears down PRGs; storage is GC'd after a retention window. |
| **Migrate** | Routing-epoch flip per [partitioning.md](partitioning.md); the tenant's PRG ring is re-placed atomically. |

Delete is two-phase: a "draining" state during which session
disconnects propagate but storage persists for forensics, followed by
a "gone" state after which storage is reclaimed. The retention window
between the two is operator policy.

## Operator surfaces

- **Tenant inspection.** `/admin` returns the current tenant manifest, active session count, current quota consumption, and noisy-neighbour state.
- **Quota override.** `operations` accepts a temporary quota override with an expiry; useful for planned-spike incidents.
- **Force disconnect.** `operations` accepts a per-tenant or per-session disconnect with audit trail. Used for incident response, not routine operations.

See [guides/scaling.md](../guides/scaling.md) for the operator
workflows that exercise these surfaces.
