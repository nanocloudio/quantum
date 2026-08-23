# Deployment and Packaging

This guide covers the **Linux deployment**: the `fluxor-linux`
userspace runtime managed by systemd, which is the path for Linux
hosts, staging, and development. The bare-metal target is summarised
at the end.

Quantum deploys as the fluxor runtime plus a directory of `.fmod`
module artefacts. There is no monolithic binary to ship — packaging
means assembling the runtime, the quantum modules, the clustor
substrate modules, and a graph YAML on disk in a layout the systemd
unit understands.

## Build artefacts

Run the full build in dependency order:

```sh
# 1. Fluxor toolchain + Linux runtime, then the CLI onto PATH
make -C ../fluxor build && make -C ../fluxor install

# 2. Substrate and quantum modules (resolved through the store)
cd ../quantum && fluxor sync && fluxor modules build --target bcm2712
```

Outputs:

| Path | What |
|---|---|
| `target/aarch64-unknown-linux-gnu/release/fluxor` / `fluxor-linux` | Host CLI and userspace runtime (materialised by `fluxor sync`) |
| `target/fluxor/bcm2712/modules/*.fmod` | Module artefacts — quantum's own builds plus the store-resolved clustor and foundation modules |

Module artefacts are keyed by silicon: `bcm2712` is the sole module
target (`aarch64-unknown-none`).

## Production install

`ops/scripts/install.sh` (run as root) provisions `/opt/quantum`,
creates a `quantum` system user, copies in the runtime and modules,
installs the systemd unit, and runs `systemctl daemon-reload`:

```sh
sudo ./ops/scripts/install.sh
sudo systemctl enable --now quantum
journalctl -u quantum -f
```

Layout after install:

```
/opt/quantum/
  bin/fluxor             # host CLI
  bin/fluxor-linux       # runtime binary
  modules/*.fmod         # quantum + clustor + foundation artefacts
  target/                # symlinks into modules/ so the runtime finds blobs
/etc/quantum/            # graph YAMLs (operator-managed)
/var/lib/quantum/        # WAL and runtime state (mount a durable volume)
/var/log/quantum/        # logs (also routed to the journal)
```

Override the destinations with environment variables:

```sh
sudo PREFIX=/srv/quantum CONFIG_DIR=/srv/quantum/etc DATA_DIR=/srv/quantum/data ./ops/scripts/install.sh
```

Put the graph you intend to run in `/etc/quantum/` — for example,
save the embedded config from [running.md](running.md) as
`/etc/quantum/broker.yaml`.

## systemd unit

The shipped unit is `ops/systemd/quantum.service`. It runs the
runtime as the `quantum` user and execs
`fluxor run ${QUANTUM_CONFIG}`; defaults:

```ini
Environment=QUANTUM_ROOT=/opt/quantum
Environment=FLUXOR_BIN=/opt/quantum/bin/fluxor
Environment=FLUXOR_LINUX_BIN=/opt/quantum/bin/fluxor-linux
Environment=QUANTUM_CONFIG=/etc/quantum/pi5.yaml
Environment=RUST_LOG=info
Environment=FLUXOR_PROJECT_ROOT=/opt/quantum
WorkingDirectory=/var/lib/quantum
ExecStart=/opt/quantum/bin/fluxor run ${QUANTUM_CONFIG}
Restart=on-failure
RestartSec=5s
```

### Switching graph configs

Drop a systemd override:

```sh
sudo systemctl edit quantum
```

```ini
[Service]
Environment=QUANTUM_CONFIG=/etc/quantum/broker.yaml
```

Then `sudo systemctl restart quantum`.

## Runtime state

The runtime writes WAL segments (`wal/`), Raft metadata (`raft/`)
and snapshot state (`data/`) under its working directory, and
`fluxor run` regenerates the derived `config.bin`/`modules.bin`
there too (`target/linux/<config>/`). The shipped unit therefore
runs with `/var/lib/quantum` as its working directory, so everything
written at runtime lands there; mount it on a durable volume backed
by NVMe — the WAL relies on fsync correctness. The rest of the
filesystem, `/opt/quantum` included, stays read-only to the service;
the CLI resolves the install tree through
`FLUXOR_PROJECT_ROOT=/opt/quantum` rather than the working
directory.

## Upgrades

`.fmod` artefacts are self-contained; updating a module means
replacing one file under `/opt/quantum/modules/` and restarting the
runtime. There is no Cargo rebuild on the production host. For
multi-node clusters, follow the rolling-restart sequence in
[high_availability.md](high_availability.md); for ABI or
storage-format changes see
[disaster recovery and upgrades](../architecture/disaster_recovery.md).

## Packaging into containers or OS packages

The shipped `install.sh` is the canonical layout. To roll a container
or OS package: build the runtime and `.fmod` artefacts on a build
host, stage them into the `install.sh` layout (`bin/`, `modules/`,
plus a config dir), ship the systemd unit or an equivalent, and
mount the state directory as durable storage. The build outputs are
stable files, so downstream packaging is left to whatever tooling
the operator already uses.

## The bare-metal target

The production deployment target runs the fluxor kernel directly on
a Raspberry Pi 5 (BCM2712) with no Linux on the device — fluxor *is*
the kernel, and the same quantum and clustor modules run on the
on-device platform stack (`ip`, `tls`) instead of `linux_net`. The
kernel image is produced by the fluxor tooling from the firmware,
the module set, and a pi5-target graph; the device has no
interactive shell, so diagnostics stream over the network (fluxor's
UDP debug output). The Linux deployment above is the same module
graph on a hosted platform layer, which is what makes it a faithful
staging environment.
