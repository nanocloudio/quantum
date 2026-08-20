# Dependency Management

Quantum is a graph of `.fmod` modules loaded by the fluxor runtime.
The deployable surface is the modules tree, not a Cargo binary, and
there is no root Cargo workspace: shared source in `modules/common/`
is `#[path]`-mounted by the modules that use it and published to the
store as a source artefact, while the host-side tools under `tools/`
are standalone crates.

Cross-project dependencies (fluxor + clustor) flow through the local
fluxor OCI store (`$FLUXOR_STORE`, default
`~/.local/share/fluxor/store`), pinned by digest in the committed
`fluxor.lock`.

## Declared dependencies

```toml
[dependencies]
fluxor  = "0.0.1"
clustor = "0.0.1"
```

| Project | Provides |
|---|---|
| fluxor | The `fluxor` host CLI, the `fluxor-linux` runtime, foundation `.fmod` modules (`ip`, `tls`, `quic`, …), the `fluxor-abi` source crate, and the platform target catalogue. |
| clustor | The substrate `.fmod` modules (`peer_router`, `consensus`, `durability`, `gateway`, `admission`, `control_plane`, `operations`, plus the standalone `partition_router`) and the `clustor-common` source artefact. |

## Resolving and syncing

```sh
fluxor sync      # resolve fluxor.lock against the store; materialise into target/
fluxor update    # advance pins to the latest published digests
```

`fluxor sync` materialises:

- `fluxor-abi` source under `target/fluxor/fluxor-abi/` —
  `#[path]`-included by quantum's PIC modules;
- foundation and substrate `.fmod` artefacts under
  `target/fluxor/bcm2712/modules/`;
- the `fluxor-linux` runtime binary under
  `target/aarch64-unknown-linux-gnu/release/`.

`fluxor.lock` is committed and always the resolver. For active
cross-repo iteration, list the colocated checkouts in
`~/.fluxor/workspace.toml`; workspace members resolve to their most
recently published artefacts (`:latest`) and `fluxor sync` writes the
resolved digests through the lockfile. `fluxor workspace status`
reports the active mode and the resolved member set.

## Bumping a pin

```sh
make -C ../clustor publish   # (or ../fluxor) after upstream changes
fluxor update                # advance fluxor.lock
fluxor sync                  # re-materialise
git add fluxor.lock && git commit -m "Bump fluxor / clustor"
```

After a bump, rebuild the modules
(`fluxor modules build --target bcm2712`) and re-check the graphs
you deploy to catch ABI drift in the module SDK or substrate ports.

## Toolchain

| Tool | Why | How to install |
|---|---|---|
| `rustc` with `aarch64-unknown-none` target | Compiles modules to PIC objects | `rustup target add aarch64-unknown-none` |
| `rust-lld` | Links module objects against `module.ld` | Ships with the Rust toolchain (`rustup component add llvm-tools-preview` if missing) |
| `fluxor` CLI | `sync`, `update`, `build`, `run`, `modules build` | `make -C ../fluxor install` (bootstrap only — the installed launcher thereafter resolves the CLI from the store) |

Quantum modules build for `aarch64-unknown-none` only — bare-metal
builds with no `std`, no allocator, and no async runtime.

## Module policy

- **No `std`, no `alloc`** in module code. State arenas are bounded
  and declared in the module manifest; the kernel sizes them at load
  time.
- **No async runtime.** Modules expose a `step()` function; the
  scheduler calls it on a fixed tick or under poll-mode.
- **No dynamic linking beyond `.fmod`.** Modules import only the SDK
  ABI and produce only their declared output ports.
- **Wire schemas live in `wire/`.** Updates to `mqtt.json`,
  `kafka.json`, or `amqp.json` should be paired with the matching
  codec-module change.
