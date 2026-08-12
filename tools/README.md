# Tools

Three roles, one directory each — a file is named for what it is, its directory
for whose it is, the convention `wave/tools/` and `spectra/tools/` use.

| Directory | Role |
| --- | --- |
| `ci/` | Gates, run by `fluxor ci` as `[ci.test]` scripts |
| `load/` | Off-DUT load drivers that orchestrate a bench run |
| `quantum-bench/`, `telemetry_guard/`, `wire_lint/` | Host Cargo crates |

Shell orchestrates processes and greps their output, which is what a gate and a
bench driver are. Rust holds anything that must parse a wire format or a metrics
export the same way the modules do.

## `ci/`

| Script | Gate |
| --- | --- |
| `shadow_guard.sh` | Hard-fails when `tests/` or `examples/` is not materialised, so CI cannot run zero tests and report green (`../../standards/test-tracking.md` §7) |
| `host_crates.sh` | fmt, clippy and test for the three host crates. With no root workspace, `fluxor ci` phase 2 is omitted and nothing else reaches them |

`make lint` and `make test` drive the same three crates in the same order, so the
Makefile and CI cannot disagree about what "linted" means.

## `load/`

`kafka_rig.sh` runs a produce matrix against a DUT and writes one
provenance-stamped JSON per point into `$OUTDIR` (default `target/perf/`). It
takes the DUT address as an argument — rig topology lives in the rig profile
(`~/.config/fluxor/labs/<lab>/rigs/<rig>.toml`), never in the repo. Point it at
`127.0.0.1` for a local smoke and the records carry `driver_is_dut_host: true`,
which marks them harness-bound rather than rig-trustworthy.

## Host crates

Each stands alone with its own `Cargo.toml` and its own `target/`; there is no
root workspace, so each inlines the `../../standards/lints.md` baseline directly.

| Crate | What it does |
| --- | --- |
| `quantum-bench` | Off-DUT drivers: `quantum-kafka-loadgen`, `quantum-mqtt-loadgen` (open-loop, driving the real codec path) and `quantum-scrape` (binary `/metrics` → JSON baseline record). Std-only, no dependencies, so it builds on an offline Pi |
| `telemetry_guard` | Structure-checks telemetry catalog JSON (`--catalog <path>`): a top-level object with optional `metrics` / `traces` / `logs` arrays |
| `wire_lint` | SHA-256 compares a wire catalog against the artefact it mirrors (`--expected` / `--candidate`), catching drift in shared frame definitions |
