# Workload Fixture Seeding

> **Status (May 2026):** Fixture-based seeding from the old Rust binary (`quantum init`, `make dev-seed`, the `tools/seed_workload_data` crate, and `data/fixtures/mqtt/`) was removed when the fluxor-native rewrite landed. The runtime currently bootstraps tenants and PRGs via the graph YAML and CP-Raft bootstrap seeds, not from a manifest-driven fixture pack. This document captures the current state plus what needs to be rebuilt if fixture seeding returns.

## Current bootstrap path

When the runtime starts with no CP-Raft state, [control_plane.md](../architecture/control_plane.md) describes the fallback:

- `cp_bridge` / CP-Raft auto-create a default tenant (`tenant_id = "local"`) with one PRG placement targeting `durability.replica_id`.
- `placement_router` issues the initial routing epoch.
- `session_processor`, `topic_engine`, `dedup_engine`, `offline_queue`, and `retained_store` start with empty state arenas — there is no preloaded subscription set or retained payload.

For explicit, non-default tenants, populate `control_plane.bootstrap_tenants` and `control_plane.bootstrap_placements` in the graph YAML; the embedded CP applies them on first launch and persists into `data/cp/`.

## On-disk runtime state

After a run, `data/` contains (real, not fixture):

| Path | Contents |
|---|---|
| `data/raft/` | Per-PRG Raft WAL segments (`wal_p<n>`) + durability ledger |
| `data/cp/` | Embedded CP-Raft state — `raft/`, `snapshots/`, `manifests/`, `system_log/` |
| `data/prg_<tenant>_<partition>/` | Per-PRG application state snapshots (sessions, dedupe shards, retained payloads, offline-queue entries) |

These directories grow as the runtime runs. They are **not** committed fixtures; they are runtime state.

## What was removed

The old seeding system relied on:

- `data/fixtures/mqtt/manifest.json` — tenant + listener + per-partition WAL seed manifest.
- `data/fixtures/mqtt/tenants/*.json` — serialised tenant capability specs.
- `data/fixtures/mqtt/prg/<tenant>/<partition>/snapshot.json` — checkpointed `PersistedPrgState`.
- `data/fixtures/mqtt/prg/<tenant>/<partition>/wal_seed.json` — ordered Clustor WAL indexes to materialise.
- `tools/seed_workload_data/main.rs` — Rust binary that read the manifest and wrote real WAL frames + capability registry into `data/`.
- `quantum init` / `make dev-seed` — operator entry points.
- `cargo test --test fixture_seeding` — schema/manifest drift check.

None of these files or commands exist in the fluxor-native build. `data/fixtures/` is absent. The `tools/seed_workload_data/` crate was deleted along with the rest of `src/`.

## Why it was removed

The old seeder serialised state in a format owned by the legacy Tokio/Rust crate. The fluxor-native build owns its WAL frame layout inside Clustor's `wal` module and its PRG snapshot layout inside `session_processor` / `dedup_engine` / etc. Porting the seeder means reproducing those layouts from outside the runtime — a substantial undertaking that has not been done.

## If you need deterministic seed data today

Three options, in increasing order of work:

1. **Run the runtime against a recorded scenario.** Launch the minimal graph, drive it with `ops/scripts/chaos.sh publish-burst` or a bespoke `pubsub_test.py`-style driver, then snapshot `data/` and commit the resulting directory. This produces a real, replay-able state without a fixture compiler.

2. **Bootstrap via CP YAML.** Use `control_plane.bootstrap_tenants` + `bootstrap_placements` in the graph YAML to define tenants and placements deterministically. This covers tenant + PRG layout but not in-flight messaging state.

3. **Re-implement the fixture compiler against the new layouts.** Write a host tool that emits WAL frames and PRG snapshot blobs using the same encoders the Quantum modules use internally. Open issue if this is needed — it has not been scoped in the current roadmap.

## CI validation

Today, CI validates the runtime end-to-end rather than fixture digests:

- `tests/integration/module_graph_mqtt.sh` exercises the MQTT/AMQP/Kafka handshake byte-for-byte against a live graph.
- `tests/integration/module_graph_load.sh` drives sustained QoS-1 publishes and asserts every PUBACK returns.
- `tests/integration/wal_durability_test.py` asserts WAL segments grow under load on every partition.

These cover the regressions the old fixture-digest assertions caught (schema drift, missing capability fields) by exercising the real wire surfaces instead.
