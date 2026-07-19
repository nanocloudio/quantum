# Multi-Tenancy

Quantum's tenant isolation model: how resources are scoped, how
quotas are enforced, how noisy neighbours are contained. Tenant
records themselves live in CP-Raft (see
[control_plane.md](control_plane.md)); enforcement happens in
`tenant_manager` plus the throttle and backpressure modules.

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
| Quotas | Per-tenant token-bucket state in `tenant_manager`. |

No cross-tenant access exists unless CP-Raft defines an explicit
bridge policy — and the default is "no bridges". Adding a bridge
requires a CP admin operation, leaves an audit trail, and is enforced
at the `topic_engine` / `tenant_manager` level rather than at the
application layer.

## Resource quotas

Operator policy in CP-Raft sets the ceiling per tenant; tenants may
set tighter quotas internally but cannot exceed the operator ceiling.

| Quota | Unit | Enforced by |
|---|---|---|
| Connection count | open sessions | `session_processor` rejects CONNECT with `0x97` (Quota exceeded) when the tenant cap is reached. |
| Publish rate | messages/sec | `throttle_gate` consumes per-tenant tokens from `tenant_manager`. |
| Publish bandwidth | bytes/sec | Same path; bytes counted separately. |
| Subscription count | active subscriptions | `topic_engine` rejects SUBSCRIBE past the cap. |
| Offline queue depth | entries per session | `offline_queue` drops oldest with audit when cap is hit. |
| Retained payload count | retained topics per tenant | `retained_store` rejects new retained writes past the cap. |
| PRG count | partitions per tenant | CP-Raft refuses placement plans that exceed the operator ceiling. |

Quota violations surface as `ThrottleEnvelope{reason=QuotaExceeded}`,
which `backpressure_propagator` translates into protocol-native
signals (MQTT `0x97`, Kafka `THROTTLING_QUOTA_EXCEEDED`, AMQP
`Channel.Flow{active=false}`).

## Noisy-neighbour control

Token-bucket enforcement is per-tenant. The bucket is sized from
operator policy (`refill_rate`, `burst_capacity`); over-spend depletes
the bucket and triggers throttling.

Sustained overage — defined as bucket exhausted for
`noisy_neighbor_threshold_s` (default 60s) — triggers `tenant_manager`
to emit a disconnect event to `session_processor`. The disconnect:

- Targets the highest-volume sessions first (capped publish rate per
  session).
- Issues protocol-appropriate close: MQTT `0x97` (Quota exceeded),
  Kafka `connection.close`, AMQP
  `connection.close{reply-code=resource-locked}`.
- Logs to `audit_logger` with the tenant ID, session ID, sustained
  duration, and the metric value that crossed the threshold.

The threshold + disconnect pattern is the load-shedding mechanism
that keeps a single misbehaving tenant from monopolising apply-loop
capacity. Tenants whose normal load approaches the threshold should
raise the operator quota rather than relying on the disconnect-recover
loop.

## Isolation boundaries

| Boundary | Mechanism |
|---|---|
| **Network** | TLS / mTLS termination per tenant; SNI selects the cert chain; client cert validation against the per-tenant trust bundle. |
| **Storage** | Per-PRG snapshots and WAL frames; tenant data never lives in shared frames. |
| **Compute** | Apply-loop fairness via per-tenant credit budgets in `flow_controller`; no tenant can starve another's apply scheduling. |
| **Memory** | Per-PRG state arena bounded by manifest; runaway state in one tenant's PRG cannot grow into another tenant's arena. |
| **Logs / metrics** | `metrics_aggregator` cardinality includes the tenant dimension; `audit_logger` tags every event with the tenant ID. |

## Tenant lifecycle

| Operation | Path |
|---|---|
| **Create** | Admin posts a tenant manifest to `/admin`; CP-Raft commits; `cp_bridge` distributes the manifest; placement reconciler assigns PRGs. |
| **Update** | Same path; updates take effect on the next CP cache refresh (default within 5s). |
| **Delete** | Admin posts a tenant delete; CP-Raft marks the tenant draining; `session_processor` disconnects active sessions with `Server unavailable`; placement reconciler tears down PRGs; storage is GC'd after a retention window. |
| **Migrate** | Routing-epoch flip per [partitioning.md](partitioning.md); the tenant's PRG ring is re-placed atomically. |

Delete is two-phase: a "draining" state during which session
disconnects propagate but storage persists for forensics, followed by
a "gone" state after which storage is reclaimed. The retention window
between the two is operator policy.

## Operator surfaces

- **Tenant inspection.** `/admin` returns the current tenant manifest, active session count, current quota consumption, and noisy-neighbour state.
- **Quota override.** `admin_handler` accepts a temporary quota override with an expiry; useful for planned-spike incidents.
- **Force disconnect.** `admin_handler` accepts a per-tenant or per-session disconnect with audit trail. Used for incident response, not routine operations.

See [guides/scaling.md](../guides/scaling.md) for the operator
workflows that exercise these surfaces.
