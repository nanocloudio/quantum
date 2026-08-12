# Configuration

Quantum is configured by a **graph YAML**: one file describes one
deployable broker as a set of modules, their parameters, the wiring
between their ports, and the platform and scheduler settings the runtime
boots under. There is no separate flat config file — the graph *is* the
configuration. `fluxor build --check examples/linux/full.yaml` checks a graph
against the current module manifests; `fluxor run examples/linux/full.yaml`
boots it.

The architecture the graph expresses is described in
[architecture.md](../architecture.md); this guide covers the config
surface an operator edits.

## The graphs

Every graph lives under `examples/`, which is shadow-tracked
(`../../standards/test-tracking.md`) — a clone of the primary repository has
none of them.

| File | Target | Purpose |
|---|---|---|
| `examples/linux/full.yaml` | linux | Full graph on the Linux platform stack — 14 modules: 7 Clustor substrate + 7 Quantum |
| `examples/linux/minimal.yaml` | linux | Reduced MQTT-only graph for smokes — 13 modules |
| `examples/linux/quic.yaml` | linux | MQTT-over-QUIC ingress via `quic` + `mqtt_quic_adapter` — 15 modules |
| `examples/linux/two_partition.yaml` | linux | Two-partition variant for WAL-durability tests — 15 modules |
| `examples/linux/multi_domain.yaml` | linux | Local twin of the pi5 multi-domain layout — 13 modules |
| `examples/linux/node0.yaml` … `node2.yaml` | linux | Three-node cluster for replication tests — 12 modules each |
| `examples/rig/pi5.yaml` | pi5 | Full bare-metal deployment graph — 15 modules, the Linux set plus `tls` (see [bring_up.md](bring_up.md)) |
| `examples/rig/pi5_bench.yaml` | pi5 | The pi5 graph plus the in-graph load injector — 16 modules |
| `examples/rig/pi5_kafka_bench.yaml` | pi5 | Kafka produce bench for the rig — 13 modules |
| `examples/rig/pi5_consensus_bench.yaml` | pi5 | Consensus/WAL bench, no protocol surface — 8 modules |
| `examples/rig/pi5_smoke.yaml` | pi5 | Minimal `modules: []` graph for the netboot smoke |

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
  request paths and `operations` serves the diagnostic surface; both
  `protocol.frames_out` and `gateway.responses` fan into
  `peer_router.client_resp`, which the graph merges.
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
default. `fluxor build --check` rejects unknown parameters and unsatisfied
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

All three protocols live in the one `protocol` module, whose router
demuxes each connection by ALPN tag (falling back to `mqtt`) and hands it
to the owning codec. A graph that names `protocol` with no `variant:` —
as `minimal.yaml` does — gets the `full` variant and serves
all three.

To serve fewer, select a variant in the graph YAML; the codecs a
deployment doesn't need are compiled out of the artefact:

```yaml
  - name: protocol
    variant: kafka        # full (default) | mqtt | kafka | amqp
  - name: session_processor
    variant: kafka        # match the protocol variant
```

Variants are declared in each module's `manifest.toml` and selected per
graph — there are no Cargo feature gates in the shipped artefacts
(`standards/dependencies.md` §9a). The saving is real: the kafka-only
`protocol` artefact is 13 KB against 51 KB for `full`.

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
