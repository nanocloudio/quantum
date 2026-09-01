# Quantum

Quantum is a multi-protocol message broker (MQTT 3.1/3.1.1/5.0, Kafka,
AMQP 0-9-1) built as a graph of cooperative modules on the
[fluxor](../fluxor/) runtime, layered on the [clustor](../clustor/)
Raft substrate. There is no monolithic broker binary: the runtime is
`fluxor-linux` (or the bare-metal kernel on a Raspberry Pi 5), and
every operational concern — codecs, session state, dedupe, retained
store, topic routing, flow control, governance — is a
position-independent `.fmod` module loaded into that runtime.

A deployed broker graph is 13 modules across six execution domains:
the clustor substrate (`peer_router`, `consensus`, `durability`,
`gateway`, `admission`, `control_plane`, `operations`) plus quantum's
application modules (`protocol`, `session_processor`, `topic_engine`,
`messaging`, `flow`, `governance`). Graphs that want the HTTP
diagnostic/admin surface add wave's `http` module
(`app` variant) on its own listener; bare-metal deployments add the
fluxor `tls` foundation module. The full structural reference —
layers, execution domains, module reference, message graph — is
[docs/architecture.md](docs/architecture.md).

## Why Quantum

- **Durability-gated acknowledgements.** Session records, dedupe
  tables, offline queues and retained payloads commit through
  clustor's WAL → fsync → quorum pipeline. PUBACK / PUBREC / PUBCOMP,
  Kafka `ProduceResponse` and AMQP `Basic.Ack` are emitted on
  durability proofs, not on local apply.
- **Multi-protocol on one substrate.** The `protocol` module
  classifies each inbound connection from its first bytes and hands
  it to the owning codec (mqtt, kafka, amqp); all three feed one
  `session_processor` and `topic_engine`, and the same Raft pipeline
  serves them all.
- **Deterministic backpressure.** `admission` runs a PID loop on
  replication lag for proposal admission; `flow` issues per-session
  consumer credits and translates substrate pressure into
  protocol-native signals (MQTT `0x97`, Kafka
  `THROTTLING_QUOTA_EXCEEDED`, AMQP `Channel.Flow`).
- **Composable, not monolithic.** Every module has a fixed step
  function, explicit ports, a bounded state arena and a manifest
  declaring its scheduling tier. The graph is a YAML file; modules
  can be swapped and the graph reshaped per deployment.

## Quick start

```sh
fluxor modules build --target bcm2712   # build quantum's modules
fluxor run - <<'EOF'
<the broker graph — embedded in docs/guides/running.md>
EOF
```

The deployment config pipes straight into `fluxor run` from the run
guide, which carries it inline: nothing else to fetch. The broker
serves MQTT (and Kafka and AMQP, over the same listener) on port
9090; a QoS 1 publish round-trips once the WAL entry is durable.
[docs/guides/running.md](docs/guides/running.md) has the full config
plus bring-up and smoke checks.

## Setup

Quantum consumes fluxor and clustor through the local OCI store
(`$FLUXOR_STORE`, default `~/.local/share/fluxor/store`),
digest-pinned in the committed `fluxor.lock`.

```sh
# one-time, per developer machine
cd ~/Development/nanocloudio
git clone git@github.com:nanocloudio/fluxor.git
git clone git@github.com:nanocloudio/clustor.git
git clone git@github.com:nanocloudio/quantum.git
make -C fluxor install     # bootstrap the fluxor CLI onto PATH
make -C fluxor publish     # publish SDK, runtime, foundation modules
make -C clustor publish    # publish the substrate modules

# in quantum's checkout
cd quantum
fluxor sync                             # materialise pinned artefacts
fluxor modules build --target bcm2712   # build quantum's .fmod set
```

To pick up new upstream publishes: `make publish` in the upstream
checkout, then `fluxor update` here to advance `fluxor.lock` to the
latest digests, `fluxor sync`, and commit the lockfile. When
iterating on several repos at once, list the checkouts in
`~/.fluxor/workspace.toml`; workspace members resolve `:latest`
automatically and `fluxor sync` writes the resolved digests through
the lockfile. `fluxor workspace status` reports the active mode.

## Build and run

```sh
fluxor modules build --target bcm2712   # module artefacts → target/fluxor/bcm2712/modules/*.fmod
fluxor build --check <graph.yaml>       # validate a graph against the module manifests
fluxor run <graph.yaml>                 # validate, pack, exec fluxor-linux
fluxor run - <<'EOF' … EOF              # same, config from stdin
```

`bcm2712` is the sole module target (`aarch64-unknown-none` silicon
token; `--all` builds it too). The Makefile is the lifecycle only
(`make help` lists it); everything else is the `fluxor` CLI invoked
directly.

For production installs (systemd, `/opt/quantum`) see
[docs/guides/deployment.md](docs/guides/deployment.md).

## Repository layout

| Path | Contents |
|---|---|
| `modules/app/` | `no_std` PIC module source, one directory per module (`mod.rs` + `manifest.toml`): the seven broker modules, `mqtt_quic_adapter`, the outbound clients (`mqtt_client`, `kafka_client`, `amqp_client`, `nats_client`), the message sinks (`mqtt_sink`, `kafka_sink`, `amqp_sink`), and `capability_registry` |
| `modules/common/` | Shared source `#[path]`-mounted by the modules that use it: wire constants and proposal encoding (`wire.rs`), shared types, and the protocol cores under `cores/` |
| `tools/` | Standalone host crates (`quantum-bench`, `telemetry_guard`, `wire_lint`) and repository scripts; see [tools/README.md](tools/README.md) |
| `wire/` | Wire schemas: `mqtt.json`, `kafka.json`, `amqp.json`, `quic.json`, `catalog.json` |
| `telemetry/` | Telemetry catalogue, shape-checked by `telemetry_guard` |
| `ops/scripts/` | Operator tooling: `install.sh` (systemd install) |
| `ops/systemd/` | `quantum.service` unit file |
| `docs/` | Reference documentation, indexed by [docs/overview.md](docs/overview.md) |
| `fluxor.toml` / `fluxor.lock` | Project manifest and digest-pinned dependency lockfile |
| `Makefile` | Lifecycle aliases over the `fluxor` CLI; `make help` lists them |

`data/`, `wal/` and `certs/` are runtime state created by a running
graph; they are local-only and safe to delete between runs.

There is no root Cargo workspace and no `crates/`. Each module under
`modules/app/` builds as a standalone PIC object packed into a
`.fmod` by `fluxor modules build`; shared source in `modules/common/`
is `#[path]`-mounted rather than linked. The host crates under
`tools/` stand alone.

## Documentation

Start here:

- [docs/overview.md](docs/overview.md) — index of the full doc set
- [docs/guides/running.md](docs/guides/running.md) — validated
  bring-up with the embedded deployment config
- [docs/architecture.md](docs/architecture.md) — layers, execution
  domains, module reference, message graph, durability cascade

Architecture references live under
[docs/architecture/](docs/architecture/); operational guides under
[docs/guides/](docs/guides/). Clustor's own documentation is the
authority for the substrate modules' internals.
