# Bootstrapping & Seed Data

Quantum bootstraps tenants and PRGs from the graph YAML and CP-Raft
bootstrap seeds — there is no manifest-driven fixture pack. This guide
covers how a fresh runtime comes up with usable state and where that
state lands on disk.

## Bootstrap path

When the runtime starts with no CP-Raft state, the embedded control
plane self-bootstraps (see
[control_plane.md](../architecture/control_plane.md)):

- `control_plane` / CP-Raft auto-create a default tenant
  (`tenant_id = "local"`) with one PRG placement targeting
  `durability.replica_id`.
- `control_plane` issues the initial routing epoch on
  `control_plane.routing`.
- `session_processor`, `topic_engine` and `messaging` (dedup / offline /
  retained) start with empty state arenas — no preloaded
  subscriptions or retained payloads.

For explicit, non-default tenants, populate
`control_plane.bootstrap_tenants` and `control_plane.bootstrap_placements`
in the graph YAML; the embedded CP applies them on first launch and
persists them into the CP state directory.

## On-disk runtime state

State is written relative to the runtime's working directory in a dev
run (a production install relocates these under `/var/lib/quantum/` — see
[deployment.md](deployment.md#runtime-state)):

| Path | Contents |
|---|---|
| `data/raft/` | Per-PRG Raft WAL segments (`wal_p<n>`) + durability ledger |
| `data/cp/` | Embedded CP-Raft state — `raft/`, `snapshots/`, `manifests/`, `system_log/` |
| `data/prg_<tenant>_<partition>/` | Per-PRG application state (sessions, dedupe shards, retained payloads, offline-queue entries) |

These are runtime state, not committed fixtures; they grow as the
runtime runs.

## Producing deterministic seed data

There is no fixture compiler — the WAL frame layout is owned by Clustor's
`durability` module and the PRG snapshot layout by the Quantum
apply-state modules, so seed data is produced by running the broker, not by writing
blobs from outside it. Two supported approaches:

1. **Record a scenario.** Launch the minimal graph, drive it with
   `ops/scripts/chaos.sh publish-burst` or a `pubsub_test.py`-style
   driver, then snapshot the `data/` directory. It is gitignored, so a
   snapshot that must be kept belongs under `tests/` in the shadow repo.
   This yields real, replay-able state.
2. **Bootstrap via CP YAML.** Define tenants and placements
   deterministically with `control_plane.bootstrap_tenants` +
   `bootstrap_placements`. This covers tenant/PRG layout but not
   in-flight messaging state.

CI validates the runtime end-to-end rather than fixture digests: the
`module_graph_mqtt.sh`, `module_graph_load.sh`, and `wal_durability_test.py`
harnesses exercise the real wire surfaces, catching the schema-drift and
missing-field regressions a fixture-digest check would.
