# Dependency Management

Quantum is a graph of `.fmod` modules loaded by the Fluxor runtime. The deployable surface is the modules tree, not a Cargo binary, and there is no root Cargo workspace: shared source in `modules/common/` is `#[path]`-mounted by the modules that use it and published to the store as a source artefact, while the host-side tools under `tools/` are standalone crates.

Cross-project dependencies (Fluxor + Clustor) flow through the local Fluxor OCI store (`$FLUXOR_STORE`, default `~/.local/share/fluxor/store`) per the contract in `standards/dependencies.md`, pinned by digest in `fluxor.lock`. The store is local-first, and Registry:2 compatibility is a standing invariant, so remote serving layers on top of the same constructs.

## Required Checkouts

Quantum builds against Fluxor and Clustor. Both are declared in `fluxor.toml`:

```toml
[dependencies]
fluxor  = "0.0.1"
clustor = "0.0.1"
```

For active cross-repo iteration, list the colocated checkouts in `~/.fluxor/workspace.toml`. Workspace members resolve to their most recently published artefacts (`:latest`) and `fluxor sync` writes the resolved digests through the lockfile; a per-artifact advisory prints when a member's inputs changed since its last publish:

```toml
[workspace]
members = [
  "/home/<you>/Development/nanocloudio/fluxor",
  "/home/<you>/Development/nanocloudio/clustor",
  "/home/<you>/Development/nanocloudio/quantum",
]
```

`fluxor workspace status` reports the active mode and the resolved member set.

| Project | Role | Notes |
|---|---|---|
| [`fluxor`](../../../fluxor) | Runtime + toolchain + module SDK | Provides the `fluxor` host CLI, the `fluxor-linux` runtime, foundation `.fmod` modules (`ip`, `tls`, `linux_net`, …), the `fluxor-abi` source crate, and the platform target/stack catalog. |
| [`clustor`](../../../clustor) | Substrate `.fmod` modules + `clustor-common` crate | Produces the seven-module substrate palette — `peer_router`, `consensus`, `durability`, `gateway`, `admission`, `control_plane`, `operations` — plus the optional `partition_router`, `session_directory`, and `consensus_bench` modules. |

## Resolving and syncing

```sh
fluxor sync               # resolve fluxor.lock against the store; materialise fmods + runtime into target/
fluxor update             # (when adopting new upstream publishes) advance pins to the latest digests
fluxor modules build --target bcm2712   # build Quantum's PIC modules
```

`fluxor sync` materialises:

- `fluxor-abi` source under `target/fluxor/fluxor-abi/sdk/<file>.rs` — `#[path]`-included by quantum's PIC modules.
- Foundation `.fmod` artefacts under `target/fluxor/<silicon>/modules/`.
- Substrate `.fmod` artefacts (Clustor) under the same directory.
- `fluxor-linux` runtime binary under `target/aarch64-unknown-linux-gnu/release/`.

In live workspace mode each member's sources / fmods / runtime resolve to that member's most recently published digests (`:latest`), written through `fluxor.lock`; the layout under `target/` is the same.

## Toolchain

| Tool | Why | How to install |
|---|---|---|
| `rustc` with `aarch64-unknown-none` target | Compiles modules to PIC objects | `rustup target add aarch64-unknown-none` |
| `rust-lld` | Links module objects against `module.ld` with `--gc-sections --no-undefined` | Ships with the Rust toolchain (`rustup component add llvm-tools-preview` if missing) |
| `fluxor` host tool | `update`, `sync`, `build`, `run`, `rig`, `modules build` | `make -C ../fluxor install` (bootstrap only — the installed launcher thereafter resolves the CLI from the store) |

Quantum modules build for `aarch64-unknown-none` only — bare-metal `--crate-type=lib` builds with no `std`, no allocator, and no async runtime.

### Fluxor build requirements

The full graph runs at the upper end of stock Fluxor's static limits, so
the linked Fluxor build must raise the following kernel and tool
constants. The figures leave headroom for extra listeners and a handful
of debug modules without another rebuild.

| Constant | Stock | Required | Site |
|---|---|---|---|
| `MAX_MODULES` | 24 | 64 | `fluxor/src/kernel/{config,event,scheduler}.rs`, `fluxor/tools/src/{config,modules}.rs` |
| `MAX_GRAPH_EDGES` | 64 | 128 | as above |
| Linux state arena | 256 KB | 4 MB | `fluxor-linux` build — covers `session_processor` (~440 KB) and `topic_engine` (~570 KB) |

## Runtime Dependencies

The `fluxor-linux` runtime relies on:

- Linux ≥ 5.15 with `io_uring` (per [partitioning.md](../architecture/partitioning.md) Environment).
- NVMe SSDs with write barriers enabled for the WAL.
- A clock synchronised via PHC/PTP; excessive skew fences PRGs from leadership.

No system libraries are linked into modules — they are statically packaged and loaded by the runtime.

## Test / CI Dependencies

For E2E harnesses under [tests/integration/](../../tests/integration/) and the chaos drivers under [ops/scripts/](../../ops/scripts/):

| Tool | Where used |
|---|---|
| `python3` (stdlib only) | Every Python harness — no `paho-mqtt`, no third-party packages |
| `bash` | All driver scripts |
| `ss` (iproute2) | `runtime_smoke.sh` listener check |
| `mosquitto_pub` / `mosquitto_sub` | Optional interop scenarios; see [interop.md](interop.md) |

Set `MOSQUITTO_PUB_BIN` / `MOSQUITTO_SUB_BIN` if the binaries are not in `$PATH`.

## Auditing & Hygiene

Reproducibility is enforced by the committed `fluxor.lock` — store-resolved artefacts, pinned by `sha256:` digest.

| Check | Command |
|---|---|
| Lockfile consistent with `fluxor.toml` + store state | Part of `make ci` (the `lockfile-consistency` phase) |
| Graph YAML matches current module manifests | `for c in examples/*/*.yaml; do fluxor build --check "$c" || break; done` (one config per invocation) |
| Modules compile cleanly for every supported target | `fluxor modules build --all --out target` |
| Runtime end-to-end behaviour | `tests/integration/module_graph_mqtt.sh && tests/integration/module_graph_load.sh` (after a modules build) |

When upstream cuts a new fluxor or clustor release, re-run `fluxor update && fluxor sync && fluxor modules build --all --out target`, then re-check every graph, to catch ABI drift in the module SDK or substrate output ports.

## Policy Reminders

- **No `std`, no `alloc`** in module code. State arenas are bounded and declared in the module manifest; the Fluxor kernel sizes them at load time.
- **No async runtime.** Modules expose a `step()` function; the scheduler calls it on a fixed tick or under poll-mode.
- **No dynamic linking beyond `.fmod`.** Modules import only the SDK ABI and produce only their declared output ports.
- **Wire schemas live in [wire/](../../wire/).** Updates to `mqtt.json`, `kafka.json`, or `amqp.json` should be paired with codec-module changes and exercised via `tests/integration/module_graph_mqtt.sh`.
