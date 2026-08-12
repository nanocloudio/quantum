# Deployment & Packaging

This guide covers the **Linux deployment**: the `fluxor-linux` userspace
runtime managed by systemd, which is the target for Linux hosts and the
usual path for staging and development. The production bare-metal target
— Fluxor as the kernel on BCM2712, with no Linux on the device — is
brought up through the hardware rig; see [bring_up.md](bring_up.md).

Quantum deploys as the Fluxor runtime plus a directory of `.fmod` module artifacts. There is no monolithic binary to ship — packaging means assembling the runtime, the Quantum modules, the Clustor substrate modules, and a graph YAML on disk in a layout the systemd unit understands.

## Build artifacts

Run the full build in dependency order:

```sh
# 1. Fluxor toolchain + Linux runtime, then the CLI onto PATH
cd ../fluxor && make build && make install

# 2. Clustor substrate modules
cd ../clustor && fluxor modules build --target bcm2712

# 3. Quantum application modules, plus the foundation fmods it depends on
cd ../quantum && fluxor sync && fluxor modules build --target bcm2712
```

Outputs:

| Path | What |
|---|---|
| `../fluxor/target/aarch64-unknown-linux-gnu/release/fluxor` | Host toolchain (`validate`, `build`, `run`, `sync`, `modules build`) |
| `../fluxor/target/aarch64-unknown-linux-gnu/release/fluxor-linux` | Userspace runtime |
| `<project>/target/fluxor/bcm2712/modules/*.fmod` | The `.fmod` artefacts each project builds. Every project writes into its own tree; `fluxor sync` copies the cross-project ones (foundation, and in canonical mode the Clustor substrate) into the consuming project's tree. |

Module artefacts are keyed by silicon: `bcm2712` is the sole module target (`aarch64-unknown-none`). Firmware images for the board are built separately with `make -C ../fluxor firmware TARGET=pi5`.

## Production install

[ops/scripts/install.sh](../../ops/scripts/install.sh) provisions `/opt/quantum`, creates a `quantum` system user, and copies in the runtime + modules:

```sh
sudo ./ops/scripts/install.sh
```

Layout after install:

```
/opt/quantum/
  bin/fluxor             # toolchain binary
  bin/fluxor-linux       # runtime binary
  modules/*.fmod         # all Quantum + Clustor + foundation .fmod artifacts
  target/                # symlinks pointing into modules/ so the runtime finds blobs
/etc/quantum/             # graph YAMLs (operator-managed)
/var/lib/quantum/         # WAL, snapshots, CP-Raft state (mount a durable volume here)
/var/log/quantum/         # logs (also routed to the journal)
```

Override the install destination with env vars:

```sh
sudo PREFIX=/srv/quantum CONFIG_DIR=/srv/quantum/etc DATA_DIR=/srv/quantum/data ./ops/scripts/install.sh
```

## systemd unit

The shipped unit is [ops/systemd/quantum.service](../../ops/systemd/quantum.service). It runs the runtime as the `quantum` user, exec's `fluxor run ${QUANTUM_CONFIG}`, and pushes logs to the journal.

Defaults:

```ini
Environment=QUANTUM_ROOT=/opt/quantum
Environment=FLUXOR_BIN=/opt/quantum/bin/fluxor
Environment=FLUXOR_LINUX_BIN=/opt/quantum/bin/fluxor-linux
Environment=QUANTUM_CONFIG=/etc/quantum/pi5.yaml
ExecStart=/opt/quantum/bin/fluxor run ${QUANTUM_CONFIG}
Restart=on-failure
RestartSec=5s
```

`install.sh` does not copy the unit — drop it into `/etc/systemd/system/quantum.service` yourself, then enable:

```sh
sudo cp ops/systemd/quantum.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now quantum
journalctl -u quantum -f
```

### Switching graph configs

Drop a systemd override:

```sh
sudo systemctl edit quantum
```

```ini
[Service]
Environment=QUANTUM_CONFIG=/etc/quantum/full.yaml
```

Then `sudo systemctl restart quantum`.

## Runtime state

| Path | Contents | Notes |
|---|---|---|
| `/var/lib/quantum/raft/` | Per-PRG Raft WAL segments and ledger | 64 MiB segment rotation (set in graph YAML) |
| `/var/lib/quantum/cp/` | CP-Raft state (WAL, snapshots, manifests) | Embedded CP only — see [control_plane.md](../architecture/control_plane.md) |
| `/var/lib/quantum/prg_<tenant>_<partition>/` | Per-PRG application state (sessions, dedupe shards, retained, offline queue) | Snapshot-persisted; sized by tenant policy |

Mount `/var/lib/quantum` on a durable volume backed by NVMe with write barriers enabled. The WAL relies on `fdatasync` correctness.

## TLS / certificates

- `tls` module reads chain / key / CA paths from the graph YAML (`cert_path`, `key_path`, `ca_path`). These are interpolated from environment via `${TLS_CERT_PATH}` etc.
- mTLS is mandatory in production graphs.
- Embedded CP can reuse the listener bundle or take its own — see [control_plane.md](../architecture/control_plane.md).
- Certificate rotation: replace the files on disk and `systemctl reload quantum`. The `tls` module re-reads on SIGHUP if the graph wires it.

## Health and readiness

`gateway` exposes (default `:9100`):

| Endpoint | Meaning |
|---|---|
| `/readyz` | Green when CP cache is fresh, PRGs have completed replay, durability fences are clear, and the listener is not draining |
| `/why` | Structured fault explanation for non-ready states |
| `/metrics` | Prometheus exposition (substrate metrics fanned into `operations` + dimensional metrics from `governance`'s telemetry component) |
| `/raft` | Per-PRG Raft state for diagnostics |
| `/admin` | Idempotency-keyed admin workflows (partition CRUD, durability toggles, leader transfer, snapshot triggers, shrink/grow plans) |

Wire L4 health checks to `/readyz`. Liveness is "the process responds at all"; readiness is "this node should receive traffic."

## Upgrade / rollout

Follow the rolling-restart sequence in [high_availability.md](high_availability.md):

1. Verify CP-Raft health (`/readyz` cluster-wide ≥ 99%).
2. Optionally transfer leadership away from the target node (per-PRG via `/admin`).
3. Flip the listener into drain mode; remove from L4 pool.
4. Wait for active session quiescence (or send DISCONNECT after grace).
5. `systemctl stop quantum`, replace `/opt/quantum/modules/*.fmod` + binaries, `systemctl start quantum`.
6. Wait for `/readyz` green — Clustor replays the WAL deterministically.
7. Re-admit at the L4. Clear drain.

`.fmod` artifacts are content-addressed and self-contained; updating a module means replacing one file under `/opt/quantum/modules/` and restarting the runtime. There is no Cargo rebuild on the production host.

## Packaging into containers / Deb / RPM

The shipped `install.sh` is the canonical layout. To roll a container or OS package:

1. Build `fluxor`, `fluxor-linux`, and all `.fmod` artifacts on a build host with `aarch64-unknown-none` available.
2. Stage them into the `install.sh` layout (`bin/`, `modules/`, plus an `/etc/quantum` config dir).
3. Ship the systemd unit (or an equivalent for your init system).
4. Mount `/var/lib/quantum` as durable storage.

There is no `make package` target — the build outputs are stable, content-addressed files and downstream packaging is left to whatever tooling the operator already uses (Dockerfile, Debian `dh-systemd`, `rpmbuild`, etc.).
