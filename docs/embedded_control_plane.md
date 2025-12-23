# Embedded Control Plane Operating Modes

This note captures the embedded control-plane decisions; external ControlPlaneRaft deployments are no longer supported in this build.

## Operating modes
- `embedded_cp`: ControlPlaneRaft plus its HTTP API run inside the Quantum binary, sharing the Tokio runtime, tracing, metrics surface, and shutdown signals with the data plane (only supported mode).
- `external_cp`: previously supported remote ControlPlaneRaft; kept here for historical context but rejected by config validation.

## Defaults and selection
- Only supported mode: `embedded_cp` with `durability.quorum_size = 1` for a single-voter CP-Raft on the same node (raise if you run multi-node experiments).
- Mode is chosen via `control_plane.mode` with an env override `QUANTUM_CP_MODE`; any attempt to set `external` is rejected.

## Upgrade and compatibility expectations
- Embedded CP uses the same HTTP surface (`/routing`, `/features`, admin/why endpoints) and response shapes as the historical external CP client path to preserve parity and tests.
- On-disk schema matches ControlPlaneRaft: operators migrating from older external deployments must switch to embedded before running this build; the formats remain compatible, but mixed-mode clusters are unsupported.

## Embedded composition
- ControlPlaneRaft consensus core and durability ledger.
- CP HTTP server for routing/features (plus why/admin surfaces already in clustor).
- CP storage (WAL, snapshots, manifests, system log) colocated under the MQTT `storage_dir`.
- Runs on the shared Tokio runtime and reuses existing tracing/metrics exporters and shutdown hooks.

## Clustor dependencies/features
- Reuse existing `clustor` features already enabled in Cargo: `net`, `async-net`, `snapshot-crypto`, and `admin-http` for HTTP surfaces and TLS plumbing.
- CP server wiring uses `clustor::control_plane::core` (CP-Raft state machine), `control_plane::capabilities` (feature manifest), and `net::control_plane` HTTP/Why servers plus `MtlsIdentityManager` for mutual TLS.
- No additional optional clustor features are needed beyond what is already enabled for the current client path.

## Networking defaults (embedded)
- CP HTTP bind (TLS): `127.0.0.1:19000` by default to match existing CP endpoint shape and avoid listener ports `1883/1884`.
- CP Raft RPC bind (mTLS): `127.0.0.1:19001` by default to avoid collisions with MQTT listener and telemetry binds; overrideable for multi-node experiments.
- Both binds become explicit config fields (`control_plane.embedded_http_bind`, `control_plane.embedded_raft_bind`) so they can be widened to `0.0.0.0` in clustered dev setups.

## TLS strategy and trust domain
- Default: reuse the TCP listener TLS chain/key for embedded CP HTTP and Raft RPC, keeping config minimal and aligning identities across surfaces.
- Trust bundle default: prefer `paths.cp_certs`; if absent in embedded mode, fall back to the listener client CA to enable out-of-the-box local testing without extra files.
- Optional override: a dedicated `control_plane.embedded_tls` tuple (chain/key/ca) may be provided to isolate CP credentials from listener material.
- Trust domain: derive from the configured certificate SPIFFE ID when present; otherwise fall back to `RAFT_TRUST_DOMAIN` or `local` to keep single-node/dev flows working without custom SPIFFE roots.
- Bootstrap identity: if no listener/embedded certs are provided, dev builds may self-generate an ephemeral CP identity stored under `storage_dir/cp/certs`; production must supply explicit TLS material and trust bundles (no auto-generation).

## First-node bootstrap steps
- Generate or reuse TLS chain/key/CA (listener defaults work for local dev).
- Set `control_plane.mode = "embedded"` and choose non-conflicting binds for `embedded_http_bind` and `embedded_raft_bind` (defaults `127.0.0.1:19000/19001`).
- Seed tenants/placements via `control_plane.bootstrap_tenants` and `bootstrap_placements`; if left empty, the runtime creates a `local` tenant with one placement targeting `durability.replica_id`.
- Start the binary with optional CLI overrides for local testing: `quantum --cp-mode embedded --cp-http-bind 127.0.0.1:19000 --cp-raft-bind 127.0.0.1:19001`.

## Scaling or switching modes
- External CP mode has been removed. For multi-node embedded experiments, widen the HTTP/Raft binds, increase `durability.quorum_size`, and supply explicit placements for each replica; no mixed embedded/external topologies are supported.

## Operational caveats
- Embedded mode is single-binary and intended for dev/test; quorum defaults to 1 and is not HA.
- HTTP surface matches the external CP for parity/testing; keep the HTTP path available even when using in-process adapters.
- Binds must not collide with MQTT listeners or telemetry endpoints; validation enforces this for embedded configs.

## Migration and release considerations
- Deprecation posture: embedded is now the only supported control-plane mode in this build; external configs must be converted before upgrading.
- Upgrade/migration note: configs without `control_plane.mode` default to `embedded`; specifying `external` will fail validation. Call this out in release notes alongside embedded metrics (`quantum_controlplane_cache_age_ms`), CLI overrides (`--cp-mode`, bind flags), and validation changes around embedded binds/ephemeral ports.
- Follow-up backlog: multi-node embedded clustering remains future work; keep a backlog item to track removing legacy ring-based routing fallbacks once embedded placements are always available.

## Storage layout
- All embedded CP state lives under the existing `paths.storage_dir` in a dedicated `cp/` subtree to avoid mixing with PRG data.
- Use clustor’s standard layout under that root: `cp/raft` for WAL/segments, `cp/snapshots` for checkpoints, `cp/manifests` for catalog metadata, and `cp/system_log`/ledger files as required by ControlPlaneRaft.
- Defaults assume `storage_dir = data`, yielding `data/cp/...` with no extra configuration required for dev.

## Persistence and format compatibility
- Embedded CP uses the exact ControlPlaneRaft on-disk schema as the external deployment (ledger, manifests, placements, feature manifests). Switching modes does not change file formats; only the hosting topology differs. No mixed-mode clusters; switch modes via drain + config change, not data migration.

## Bootstrap seeds
- Preferred source: explicit config seeds (`control_plane.bootstrap_tenants`, `control_plane.bootstrap_placements`) when provided.
- Fallback: auto-create a default tenant (`tenant_id = "local"`) and a single PRG placement targeting the local node/replica ID when no seeds are supplied, ensuring embedded dev mode can start without external CP state.

## Lifecycle ordering
- Startup: initialize embedded CP (Raft, HTTP, TLS) first, then start data-plane PRGs and listeners so routing data is available before PRG spin-up.
- Shutdown/drain: stop listeners/PRGs, then drain CP HTTP/Raft and flush manifests before process exit to keep CP state durable.

## Readiness coupling
- Overall runtime readiness requires embedded CP to be up, serving routing/feature manifests, and publishing freshness signals; the existing readiness gate continues to require a fresh CP cache and cleared fences before reporting ready.

## API and manifest parity
- Embedded CP serves `/routing` and `/features` with the same payloads and semantics as the external CP HTTP service, preserving client expectations and test coverage. `/healthz` is exposed for liveness. Admin/why endpoints remain available when the corresponding clustor features are enabled.

## Telemetry surfaces
- CP metrics/logs are emitted via the existing telemetry HTTP server; embed CP-specific counters (cache state, Raft role, publish lag) into the shared `/metrics` surface and reuse tracing sinks for CP events.

## Feature flag parity
- Maintain identical feature-gate and capability behaviour between embedded runs and the historical external HTTP surface; no embedded-only feature shortcuts. Use the same clustor feature toggles that are already enabled for the client path.

## Durability quorum rules
- Embedded mode explicitly allows `durability.quorum_size = 1` for single-node dev; raise this when running multi-node experiments.

## Guarding destructive actions
- Destructive/migratory operations (schema migrations, ledger wipes, forced rebalances) require explicit opts even in embedded mode; default posture is "deny" unless a dedicated flag/env is set for dev.
