# Control Plane

What the control plane owns, how the data plane consumes it, and how
the caching contract degrades when it is unavailable. The data-plane
invariants the control plane enforces live in
[messaging_model.md](messaging_model.md) (particularly
**ROUTING-EPOCH**) and [partitioning.md](partitioning.md).

**Status: the consumption contract is wired; the authority behind it
is synthetic.** The `control_plane` module, its output ports, and the
`admission` proof-cache FSM all run in every graph. But the module
does not yet talk to a real CP-Raft service: it emits a synthetic
proof on schedule and a placeholder tenant record (tenant id 0), so
the data plane exercises the full consumption path against stand-in
data. The sections below separate the two.

## What is wired

### The control_plane module

Source: clustor's `control_plane` module (consumed through the
store).

`control_plane` emits five output classes, each on its own port:

| Port | Payload | Consumer |
|---|---|---|
| `proof` | CP proof, emitted on a state-dependent schedule | `admission` |
| `tenant_records` | Tenant quota records | `governance`'s tenants component |
| `capabilities` | Capability manifests | `session_processor` |
| `routing` | Epoch-based partition routing | `gateway` |
| `epoch_events` | Epoch-change events, so `session_processor` can fence in-flight state on rebalance | `session_processor` |

Today the proof is synthetic and the tenant record is the
placeholder default; see
[multi_tenancy.md](multi_tenancy.md#implementation-status).

### Cache states

`admission`'s proof cache runs a four-state FSM per cached object,
published on `admission.cache_state`:

| State | Meaning | Effect on data plane |
|---|---|---|
| **Fresh** | Within TTL (`fresh_threshold_s`, default 60). | All operations admitted. |
| **Cached** | Past TTL but within grace; background refresh in flight. | Operations admitted. |
| **Stale** | Grace exceeded (`grace_period_s`, default 120s). | New handshakes requiring policy evaluation are blocked. Existing sessions continue but cannot establish new authorisations. |
| **Expired** | Long-term outage beyond grace. | Policy-bound operations tear down; clients must reconnect once the control plane returns. |

The Stale → Expired transition is the strict-fallback boundary, and
`admission.strict_fallback` drives `consensus.cp_state` so the
substrate applies the fail-closed override.

## Design target

The remainder of this document is the target design for a real
embedded CP-Raft: **Status: design target, not wired.**

### Responsibilities

CP-Raft is the authority for:

- **Tenant records** — PRG count, quotas, ACLs, roles, certificates.
- **Routing manifests** — placement plans, routing epochs,
  partition-to-node assignments.
- **Feature gates** — per-tenant protocol feature toggles, QoS
  ceilings, schema versions.
- **Throttling budgets** — per-tenant token-bucket limits, fetch
  quotas, fairness policies.

CP-Raft is not in the publish path. The data plane reads tenant /
routing / feature data from CP, caches it, and enforces it locally;
CP failures degrade gracefully through the cache states above.

### Embedded composition

The control plane runs embedded — part of the same process graph
that serves the data plane, not a separate service:

- CP-Raft consensus runs in the same substrate modules (`consensus`
  and `durability`) that serve the data plane; CP gets its own PRG
  namespace, not a separate runtime.
- The CP admin surface is served alongside the existing
  `operations` HTTP endpoints, on binds configured in the graph
  YAML.
- CP on-disk state is colocated with PRG storage and has the same
  crash-consistency requirements.

### Bootstrap

A first node with no CP state self-bootstraps: it creates a default
tenant with one placement targeting the local replica and issues the
initial routing epoch. Multi-node embedded operation provisions
additional nodes with the same trust material, widens the binds, and
starts nodes in sequence, each waiting for CP-Raft leader election
before opening its data-plane listeners.

### Lifecycle ordering

| Phase | Order |
|---|---|
| **Startup** | Embedded CP initialises first; then data-plane PRGs and listeners start, so routing data is available before PRG spin-up. |
| **Drain** | Listeners and PRGs drain first; then CP drains, manifests flush, the process exits. |
| **Readiness** | Overall runtime readiness requires CP to be up, serving routing data, and publishing freshness signals. |

### Destructive operations

Operations that mutate CP state in non-reversible ways (ledger
wipes, forced rebalances, shrink plans without drain) require
explicit opt-in even in embedded mode; the default posture is deny.
