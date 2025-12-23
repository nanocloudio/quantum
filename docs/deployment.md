# Deployment & Packaging

Quantum ships as a single binary. The optional `make package` target simply creates an `artifacts/` staging directory for downstream container/Deb/RPM tooling.

## Build & Package
- `make build-release` to produce the optimized binary.
- `make package` to prime the `artifacts/` directory (drop binaries, checksums, or metadata there as your packaging flow requires).

## Runtime Files
- Persistent storage: `paths.storage_dir` (clustor WAL/snapshots). Mount as a durable volume.
- TLS: `listeners.tcp/ quic` key/chain/CA files; reload supported via `ReloadableTlsConfig`.
- Control plane bundle: optional `paths.cp_certs` for CP trust.

## Health & Readiness
- Ready = CP cache fresh + PRGs placed + durability fence inactive (`Runtime::ready`).
- Liveness = process responsive, even if CP stale (will reject MQTT with `PERMANENT_DURABILITY`/`PERMANENT_EPOCH`).
- Metrics endpoint: configurable via `telemetry.metrics_bind`.

## Upgrade / Rollout
- Use rolling restart; `Runtime::hot_reload` guards against stale CP cache.
- Prefer strict-fallback during partial outages; durability fences block ACKs until cleared.
- QUIC/TLS cert rotation: drop new certs on disk and trigger reload via `ReloadableTlsConfig::reload_if_changed`.
