# Configuration

Quantum is configured by a **graph YAML**: one file describes one
deployable broker as a set of modules, their parameters, the wiring
between their ports, and the platform and scheduler settings the runtime
boots under. There is no separate flat config file — the graph *is* the
configuration. `fluxor validate configs/<file>.yaml` checks a graph
against the current module manifests; `fluxor run configs/<file>.yaml`
boots it.

The architecture the graph expresses is described in
[architecture.md](../architecture.md); this guide covers the config
surface an operator edits.

## Shipped configs

| File | Target | Purpose |
|---|---|---|
| `configs/quantum-pi5.yaml` | pi5 | Full bare-metal deployment graph — 27 modules, the Linux set plus `tls` (see [bring_up.md](bring_up.md)) |
| `configs/quantum-pi5-smoke.yaml` | pi5 | Minimal `modules: []` graph for the netboot smoke |
| `configs/quantum-linux.yaml` | linux | Full graph on the Linux platform stack — 26 modules: 7 Clustor substrate + 19 Quantum |
| `configs/quantum-linux-2p.yaml` | linux | Two-partition variant for WAL-durability tests |
| `configs/quantum-linux-minimal.yaml` | linux | Reduced MQTT-only graph for smokes |
| `configs/quantum-node0.yaml` … `node2.yaml` | linux | Three-node cluster for replication tests |

## Anatomy of a graph

```yaml
target: linux            # board or host token: pi5 | linux (silicon bcm2712 is named only in module builds)
tick_us: 1000            # base scheduler tick

scheduler:
  accept_cycles: true    # request/response + flow-control feedback make
                         # the graph cyclic; v1 fluxor requires opting in

module_search_paths:     # where the fluxor tool resolves .fmod manifests
  - ../../clustor/modules/app

platform:
  net: {}                # platform capability surfaces (see below)

modules:                 # every module in the graph, with parameters
  - name: consensus
    params:
      proposal_batch_timeout_ms: 1
  - name: durability
    params:
      fsync_mode: 1
      group_window_ms: 0
      group_max_pending: 8
      fence_depth: 8
      segment_bytes: 4194304
  - name: protocol

wiring:                  # port-to-port channel edges
  - from: linux_net.net_out
    to: peer_router.net_in
  - from: consensus.log_append
    to: durability.entries
    rate: transaction
```

### Platform stack

The `platform` block and the transport modules differ by target:

- **Linux** (`target: linux`) uses `platform.net: {}` and the
  `linux_net` module for sockets. `gateway` frames the client and HTTP
  request paths, `operations` serves the diagnostic surface, and
  multi-listener graphs add `protocol` to merge protocol and HTTP
  responses onto `peer_router.client_resp`.
- **Bare-metal** (`target: pi5`, the board over `bcm2712` silicon) uses the on-device
  stack — `ip`, `tls`, `gateway` — described in
  [architecture.md](../architecture.md#module-reference) and brought up
  per [bring_up.md](bring_up.md).

The application modules (`protocol`, `session_processor`,
`topic_engine`, …) are identical across targets; only the platform layer
below them changes.

### Modules and parameters

Each entry under `modules` names a module and may set parameters the
module's `manifest.toml` declares — for example `durability.fsync_mode`,
`durability.segment_bytes`, `consensus.proposal_batch_timeout_ms`, and
`admission.entry_credit_max`. Parameters left unset take the manifest
default. `fluxor validate` rejects unknown parameters and unsatisfied
port wiring.

### Wiring and channel buffers

Each `wiring` edge connects an output port to an input port. The default
channel buffer is 8 KiB; load-oriented graphs raise the client-path
edges to `buffer_bytes: 65536` so bursts of simultaneous
CONNECTs/PUBLISHes are not dropped at the channel layer under
concurrent load. An edge may also carry a `rate:` class — the
`consensus.log_append → durability.entries` edge is classed
`transaction` so the WAL entry pump receives a real per-step byte
grant and batches records into one fsync.

### Environment interpolation

Values may interpolate environment variables with `${VAR}` — used for
TLS material in production graphs:

```yaml
- name: tls
  cert_path: "${TLS_CERT_PATH}"
  key_path: "${TLS_KEY_PATH}"
  ca_path: "${TLS_CA_PATH}"
```

## Listeners and ports

The shipped Linux configs bind the cleartext MQTT listener on
`127.0.0.1:9090` — the host and port the integration harnesses and chaos
drivers default to (override with `QUANTUM_HOST` / `QUANTUM_PORT`, or
`--host` / `--port`). Production bare-metal graphs terminate mTLS in the
`tls` module; mTLS is mandatory there. The HTTP surface (`/readyz`,
`/why`, `/metrics`, `/raft`, `/admin`) binds separately — see
[deployment.md](deployment.md#health-and-readiness).

## Protocol selection

A protocol is enabled by including its codec module in the graph.
`quantum-linux-minimal.yaml` wires `protocol`'s mqtt component, `protocol`'s amqp component, and
`protocol`'s kafka component behind `protocol`'s router component, which demuxes by ALPN tag and
falls back to `mqtt`. Build-time Cargo feature gates in the workspace
manifests (`Cargo.toml`) gate which codecs and transports compile;
`all-protocols` builds MQTT + Kafka + AMQP together. To serve only one
protocol, ship a graph that wires only that codec.

## Durability tuning

The `durability` module owns the durability/throughput trade-off, set
entirely by its parameters:

| Parameter | Effect |
|---|---|
| `fsync_mode: 0` | Strict — fsync per entry (default; lowest data-loss window) |
| `fsync_mode: 1` | Group — batch the fsync syscall across a window |
| `group_window_ms` | Group-mode batching window |
| `group_max_pending` | Group-mode max entries per batched fsync |
| `fence_depth` | Async fsync fences kept in flight |
| `segment_bytes` | WAL segment size before rotation |

Group fsync trades a bounded durability window for throughput; the
measured curve is in [performance.md](performance.md).
`consensus.durability_mode` (`strict` / `group_fsync` / `relaxed`) must
agree with the WAL mode. See [architecture.md](../architecture.md#durability-model) for
how these feed the commit cascade.
