# Configuration

Quantum is configured by a **graph YAML**: one file describes one
deployable broker as a set of modules, their parameters, the wiring
between their ports, and the platform and scheduler settings the
runtime boots under. There is no separate flat config file — the
graph *is* the configuration. `fluxor build --check <graph.yaml>`
checks a graph against the current module manifests;
`fluxor run <graph.yaml>` (or `fluxor run -` with the config on
stdin) boots it.

The architecture the graph expresses is described in
[architecture.md](../architecture.md); the canonical deployment
config is embedded in [running.md](running.md). This guide covers
the config surface an operator edits.

## Anatomy of a graph

```yaml
target: linux            # board or host token: pi5 | linux
tick_us: 1000            # base scheduler tick

scheduler:
  accept_cycles: true    # request/response + flow-control feedback
                         # make the graph cyclic; cycles must be
                         # opted into

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
    buffer_bytes: 65536
  - from: consensus.log_append
    to: durability.entries
    rate: transaction
```

### Platform stack

The `platform` block and the transport modules differ by target:

- **Linux** (`target: linux`) uses `platform.net: {}` and the
  runtime-supplied `linux_net` endpoint for sockets.
- **Bare-metal** (`target: pi5`, the board over `bcm2712` silicon)
  uses the on-device stack — `ip` and the `tls` foundation module in
  front of `peer_router`.

The application modules (`protocol`, `session_processor`,
`topic_engine`, …) are identical across targets; only the platform
layer below them changes.

### Modules and parameters

Each entry under `modules` names a module and may set parameters the
module's `manifest.toml` declares — for example
`durability.fsync_mode` and `consensus.proposal_batch_timeout_ms`.
Parameters left unset take the manifest default. `fluxor build
--check` rejects unknown parameters and unsatisfied port wiring.

### Wiring and channel buffers

Each `wiring` edge connects an output port to an input port. The
default channel buffer is 8 KiB; the client-path edges are raised to
`buffer_bytes: 65536` so bursts of simultaneous CONNECTs/PUBLISHes
are not dropped at the channel layer under concurrent load. An edge
may also carry a `rate:` class — the
`consensus.log_append → durability.entries` edge is classed
`transaction` so the WAL entry pump receives a real per-step byte
grant and batches records into one fsync.

## Listeners and ports

The TCP listener is `peer_router`'s `listen_port` parameter (default
9090), bound on all interfaces. All three protocols share it: the
`protocol` router classifies each connection from its first bytes.
Bare-metal graphs put the `tls` module in front, so the same port
terminates TLS.

## Protocol selection

All three protocols live in the one `protocol` module, whose router
classifies each connection and hands it to the owning codec. A graph
that names `protocol` with no `variant:` gets the `full` variant and
serves all three.

To serve fewer, select a variant; the codecs a deployment does not
need are compiled out of the artefact:

```yaml
  - name: protocol
    variant: mqtt         # full (default) | mqtt | kafka | amqp
  - name: session_processor
    variant: mqtt         # match the protocol variant
```

Variants are declared in each module's `manifest.toml` and selected
per graph — there are no Cargo feature gates in the shipped
artefacts. An MQTT-only deployment loads substantially less code.

## Durability tuning

The `durability` module owns the durability/throughput trade-off,
set entirely by its parameters:

| Parameter | Effect |
|---|---|
| `fsync_mode: 0` | Strict — fsync per entry (default; smallest data-loss window) |
| `fsync_mode: 1` | Group — batch the fsync across a window |
| `group_window_ms` | Group-mode batching window |
| `group_max_pending` | Group-mode max entries per batched fsync |
| `fence_depth` | Async fsync fences kept in flight |
| `segment_bytes` | WAL segment size before rotation |

Group fsync trades a bounded durability window for throughput. See
[architecture.md](../architecture.md#durability-model) for how these
feed the commit cascade.
