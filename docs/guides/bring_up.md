# Bare-Metal Bring-Up

The production deployment target runs the Fluxor kernel directly on a
Raspberry Pi 5 (BCM2712), with **no Linux on the device** —
Fluxor *is* the kernel. This guide covers bringing the broker up on that
target via the hardware rig. For the Linux/systemd deployment path (a
`fluxor-linux` userspace process, used for development and Linux hosts),
see [deployment.md](deployment.md).

## Rig topology

Two machines are involved; keep them straight:

| Box | Role | OS |
|---|---|---|
| Dev host | Builds firmware + modules, serves proxy-DHCP + TFTP (dnsmasq), drives the rig CLI | Raspberry Pi OS / Linux |
| DUT (`pi5-a`, NVMe HAT+) | PXE-boots `kernel_2712.img` from the dev host and runs **Fluxor as the kernel** | none (bare-metal) |

The rig profile at `~/.config/fluxor/labs/default/rigs/pi5-a.{toml,md}`
(private, per-host) binds power control (a TP-Link smart plug via the
`power-kasa_local` backend), the serial console, and UDP telemetry. The
chip-level bring-up requirements (`enable_rp1_uart=1`, `pciex4_reset=0`,
`BOOT_ORDER=0xf12`) are documented in Fluxor's
`.context/pi5-bare-metal.md`.

## Build recipe

Quantum's bare-metal build and scenarios live in these files:

| File | Purpose |
|---|---|
| `.fluxor-rig.toml` | `[build.pi5]` recipe — builds Fluxor's pi5 firmware from the sibling `../fluxor` checkout, builds Quantum's PIC modules, runs `fluxor sync` to pull foundation/SDK artefacts, then `fluxor build` to produce a single kernel image |
| `configs/quantum-pi5-smoke.yaml` | Minimal `modules: []` graph the smoke boots — exercises the dev-host → DUT pipeline through kernel handoff |
| `configs/quantum-pi5.yaml` | Full-graph pi5 deployment (27 modules); group-fsync is controlled by `durability`'s `fsync_mode` / `group_window_ms` / `group_max_pending`, and `root_path: 1` puts WAL segments and snapshots at the FAT32 root |
| `tests/hardware/quantum_pi5_boot.toml` | Smoke scenario; pass signal is `observe.netboot_fetch` matching `kernel_2712.img` |

## Running the smoke

```sh
fluxor rig test --scenario tests/hardware/quantum_pi5_boot.toml
```

This builds the artefact, acquires the rig lock, stages
`target/pi5/images/quantum-pi5-smoke.img` into the TFTP root as
`kernel_2712.img`, power-cycles the DUT via the smart plug, watches
dnsmasq's journal for the netboot fetch, and reports pass/fail. `--plan`
dry-runs the resolution without touching the rig.

## Layering richer pass signals

The netboot-fetch baseline is the first rung. As each device surface
comes online, stronger pass rules layer on top via the rig profile:

1. **Console regex** — a `console.serial` pass rule, once the RP1 UART
   path is reachable from a bare-metal kernel.
2. **MQTT CONNECT** — a pass rule against the full `quantum-pi5.yaml`
   graph, once NVMe-backed FAT32 (for the persistent WAL) is wired
   through the kernel image.
3. **UDP log capture** — a `telemetry.monitor_udp` pass rule against
   `platform.debug.to = net` (net debug over UDP; there is no UART path
   on the DUT).

## Debugging on the DUT

The DUT has no interactive shell. Diagnostics come over the network:
`platform.debug.to = net` streams the kernel log over UDP, and the HTTP
surface's `/metrics` is scraped remotely. Power is cycled through the
rig's smart-plug binding; a hung DUT is recovered with a power-cycle,
not a console.

## Relationship to the Linux path

The Linux graphs (`quantum-linux*.yaml`) exercise the same Quantum and
Clustor application modules on top of the `linux_net` / `linux_fs`
platform layer. They are the right target for fast local iteration and
for the integration harnesses ([cli.md](cli.md)), but they are **not**
the production surface — the bare-metal kernel described here is. See
[configuration.md](configuration.md#platform-stack) for how the platform
layer differs between the two.
