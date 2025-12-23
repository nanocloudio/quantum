# Workload Fixture Seeding

Quantum now ships deterministic fixtures for MQTT workloads so developers, CI
jobs, and long-running tests can hydrate PRGs and the capability registry in a
single step.

## Layout

- `data/fixtures/mqtt/manifest.json` – tracks fixture version, tenant entries,
  listener descriptors, and per-partition snapshot/WAL seeds.
- `data/fixtures/mqtt/tenants/*.json` – serialized tenant capability specs
  (protocol assignments, feature flags, resource limits).
- `data/fixtures/mqtt/prg/<tenant>/<partition>/snapshot.json` – checkpointed
  `PersistedPrgState` payload referenced by both the manifest and WAL seeds.
- `data/fixtures/mqtt/prg/<tenant>/<partition>/wal_seed.json` – ordered list of
  clustor indexes that should exist in the WAL segment.

## Tooling

Use the `quantum init` subcommand (or `make dev-seed`) to hydrate a local
`data/` directory:

```shell
# Seed everything (default workload mqtt)
make dev-seed

# Seed a single tenant into a scratch directory without writing
cargo run -- init \
  --fixtures data/fixtures/mqtt \
  --data-root /tmp/quantum-seed \
  --tenant tenant_mqtt_only \
  --dry-run
```

The tool:

1. Validates manifest + listener descriptors.
2. Copies metadata/snapshot files into `data/prg_<tenant>_<partition>/`.
3. Generates deterministic WAL frames using the fixture WAL seeds.
4. Writes the merged capability registry to `data/cp/capabilities.json`.
5. Emits `data/seed_report.json` containing the digest + epoch that CI can
   capture as an artifact.

Set `TENANT=<id>` before `make dev-seed` to restrict seeding.

## Validation

- `cargo test --test fixture_seeding` runs the tool in `--dry-run` mode against
  the checked-in fixtures to catch schema or manifest drift.
- `quantum init` exits non-zero if any file is missing, malformed, or if
  the WAL encoder cannot serialize the seeded state.

Update fixtures whenever protocol/capability schemas change so that snapshot,
WAL, and capability digests continue to match what the workload expects.
