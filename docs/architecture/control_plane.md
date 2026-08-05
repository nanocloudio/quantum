# Control Plane

What CP-Raft owns, how the data plane consumes it, how the embedded
operating mode is structured, and how to bootstrap a first node. The
data-plane invariants the control plane enforces live in
[messaging_model.md](messaging_model.md) (particularly
**ROUTING-EPOCH**) and [partitioning.md](partitioning.md).

The control plane runs **embedded only** — CP-Raft is part of the same
process graph, not a separate service. It is configured through the
graph YAML like any other subsystem.

## Responsibilities

CP-Raft is the authority for:

- **Tenant records** — PRG count, quotas, ACLs, RBAC roles, certificates, compliance policies.
- **Routing manifests** — placement plans, routing epochs, partition-to-node assignments.
- **Feature gates** — per-tenant protocol feature toggles, QoS ceilings, schema versions.
- **Throttling budgets** — per-tenant token-bucket limits, fetch quotas, fairness policies.
- **Durability proofs** — per Clustor §9.8; adapters stall reads and acks when proof verification fails.

CP-Raft is not in the publish path. The data plane reads tenant /
routing / feature data from CP, caches it, and enforces it locally;
CP failures degrade gracefully through the cache.

## APIs

| Endpoint | Purpose |
|---|---|
| `/routing` | Current routing manifest (placements + epoch). Polled by `control_plane`. |
| `/features` | Per-tenant feature gates and capability manifest. |
| `/admin` | Mutations: tenant create / delete, placement plans, throttle overrides, shrink plans, leader transfer. mTLS-required, signed requests, canonical-JSON responses. |
| `/why` | Structured explanation surface for CP state machine decisions. |
| `/healthz` | Liveness; cheap, no quorum read. |
| `/readyz` | Readiness; gated on quorum freshness + manifest reachability. |

The HTTP surface is served by `gateway` (Clustor substrate module)
using the manifest produced by CP-Raft's apply path.

## Cache states

`admission`'s proof cache runs a four-state FSM per cached object,
published on `admission.cache_state`:

| State | Meaning | Effect on data plane |
|---|---|---|
| **Fresh** | Within TTL (`refresh_fresh_ms`, default 60s). | All operations admitted. |
| **Cached** | Past TTL but within grace; `control_plane` polls at the faster `refresh_stale_ms` cadence (default 600ms) to recover. | Operations admitted; background refresh in flight. |
| **Stale** | Grace exceeded (`grace_period_s`, default 120s). | New handshakes requiring policy evaluation are blocked. Existing sessions continue but cannot establish new authorisations. |
| **Expired** | Long-term outage beyond grace. | Policy-bound operations tear down. Adapters return `ControlPlaneUnavailable` and clients must reconnect once CP returns. |

The Stale → Expired transition is the strict-fallback boundary.
Operators can override `strict_fallback` per-PRG via `operations`
for documented incident responses, but the default is fail-closed.

## Embedded mode

Embedded CP-Raft is the only supported operating mode. The CP-Raft
consensus core, durability ledger, HTTP surface, and storage all run
inside the same Quantum graph that serves the data plane — no
separate process, no separate Raft cluster.

### Composition

- CP-Raft consensus runs in the same Clustor substrate modules
  (`consensus` and `durability`) that serve the data plane; CP gets
  its own PRG namespace, not a separate runtime.
- `control_plane` polls CP-Raft and emits three output classes:
  proofs (`control_plane.proof`), tenant records
  (`control_plane.tenant_records`), capability manifests
  (`control_plane.capabilities`).
- `control_plane.epoch_events` carries epoch changes so
  `session_processor` can fence in-flight state on rebalance.
- `gateway` exposes the CP HTTP API on the bind configured in the
  graph YAML.
- All CP on-disk state lives under `data/cp/` colocated with PRG
  storage (`data/cp/raft`, `data/cp/snapshots`, `data/cp/manifests`,
  `data/cp/system_log`).

### Network defaults

| Bind | Default | Notes |
|---|---|---|
| CP HTTP (TLS) | `127.0.0.1:19000` | Widened to `0.0.0.0` for multi-node embedded experiments. |
| CP Raft RPC (mTLS) | `127.0.0.1:19001` | Widened similarly. |
| Public HTTP (`gateway`) | `0.0.0.0:9100` | Operator-facing `/readyz`, `/metrics`, `/admin`. |
| MQTT TCP listener | `0.0.0.0:8883` (production) / `127.0.0.1:9090` (minimal smoke) | Configured per graph YAML. |

Bind validation in the graph YAML rejects collisions between the CP
binds, the public HTTP surface, the Raft peer port, and the
MQTT/QUIC listeners.

### TLS strategy

- Default: reuse the TCP listener's TLS chain / key / CA for CP HTTP
  and CP Raft RPC.
- Optional override: a dedicated `control_plane.embedded_tls` tuple
  (chain / key / CA) isolates CP credentials from listener material
  — useful when CP and data-plane traffic terminate on different
  SPIFFE roots.
- Trust domain derives from the SPIFFE ID when present; otherwise
  from `RAFT_TRUST_DOMAIN` env var; otherwise `local`.
- Production must supply explicit TLS material. Dev builds may
  self-generate an ephemeral CP identity under `storage_dir/cp/certs`;
  production does not auto-generate.

## Bootstrap

### First-node start

1. Generate or reuse TLS chain / key / CA (listener defaults work
   for local dev).
2. Set `control_plane.mode = "embedded"` in the graph YAML and
   choose non-conflicting binds (defaults are usually fine).
3. Seed tenants and placements via `control_plane.bootstrap_tenants`
   and `control_plane.bootstrap_placements`. If left empty, the
   runtime creates a `local` tenant with one placement targeting
   `durability.replica_id`.
4. Launch with `fluxor run configs/<config>.yaml` (or
   `systemctl start quantum`).

### Multi-node embedded

1. Provision additional nodes with the same trust domain.
2. Widen CP HTTP and Raft binds to `0.0.0.0` (or your replication
   network).
3. Raise `durability.quorum_size` to match voter count.
4. Provide explicit placements for each replica in
   `bootstrap_placements`.
5. Start nodes in sequence; each waits for CP-Raft leader election
   before opening its data-plane listeners.

Multi-node embedded is the path to HA. Single-node embedded with
`quorum_size = 1` is dev/test only.

## Lifecycle ordering

| Phase | Order |
|---|---|
| **Startup** | Embedded CP (Raft, HTTP, TLS) initialises first; then data-plane PRGs and listeners start. Routing data is available before PRG spin-up. |
| **Drain** | Listeners and PRGs drain first; then CP HTTP and Raft drain, manifests flush, process exits. Keeps CP state durable through shutdown. |
| **Readiness** | Overall runtime readiness requires CP to be up, serving routing / features, and publishing freshness signals. `/readyz` continues to require a Fresh CP cache and cleared fences. |

## API and manifest stability

`/routing` and `/features` are a stable payload and semantic contract
independent of where CP runs. That independence is intentional: it
fixes the surface that adapter clients and operator tooling consume,
and it keeps the contract testable without mocking CP internals.

## Storage layout

```
data/cp/
  raft/          # CP-Raft WAL segments
  snapshots/     # CP checkpoints
  manifests/     # current routing / feature / tenant manifests
  system_log/    # CP-Raft system events
  certs/         # ephemeral identity (dev only — production supplies explicit certs)
```

Defaults assume `storage_dir = data`, yielding `data/cp/…` with no
extra configuration needed for dev. Mount this on the same durable
volume as PRG data — CP state has the same crash-consistency
requirements.

## Destructive operations

Operations that mutate CP state in non-reversible ways (schema
migrations, ledger wipes, forced rebalances, shrink plans without
drain) require explicit opt-in even in embedded mode. Default posture
is "deny" unless a dedicated `--force` flag or
`QUANTUM_ALLOW_DESTRUCTIVE` env var is set. See
[guides/scaling.md](../guides/scaling.md) for the shrink-plan
workflow that exercises these surfaces.
