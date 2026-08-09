#!/usr/bin/env bash
# THE HOST CRATES' fmt/clippy/test, AS A CI GATE.
#
# Quantum has no root cargo workspace (retired with `crates/quantum-common`;
# the common helpers live in `modules/common` and are `#[path]`-mounted by the
# PIC modules, not linked). That changes what `fluxor ci` runs, in two ways:
#
#   * Phases 1.1/1.2 see no root `Cargo.toml` and switch to fmt-checking and
#     clippying the PIC module sources directly — a GAIN, since `modules/**`
#     was previously linted by neither. But it means the host crates under
#     `tools/` are now fmt/clippy'd by nothing.
#   * Phase 2 is omitted (no root manifest, no `host_tools_crate`), so the
#     three tool crates are built and tested by nothing.
#
# This closes both holes. Without it CI would summarise GREEN with the
# loadgens unbuilt and no host crate linted (standards/make.md §5 — the loam
# trap): exactly the failure mode Chronicle hit when it removed its crates.
#
# `[ci.test] scripts` in fluxor.toml is what makes this run.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

CRATES=("tools/telemetry_guard" "tools/wire_lint" "tools/quantum-bench")

# Each crate is formatted BY NAME rather than with `cargo fmt --all`: a
# future path dependency into `modules/**` would otherwise reformat the PIC
# sources, which `fluxor ci` phase 1.1 now owns directly. Clippy needs no
# such guard — it lints the local package, not its dependencies.
declare -A PKG=(
  ["tools/telemetry_guard"]="telemetry_guard"
  ["tools/wire_lint"]="wire_lint"
  ["tools/quantum-bench"]="quantum-bench"
)

fail=0

for c in "${CRATES[@]}"; do
  dir="$ROOT/$c"
  if [ ! -f "$dir/Cargo.toml" ]; then
    echo "FAIL $c: no Cargo.toml — a host crate this gate names has moved or gone."
    fail=1
    continue
  fi

  echo "== $c: fmt =="
  if ! (cd "$dir" && cargo fmt -p "${PKG[$c]}" -- --check); then
    echo "FAIL $c: rustfmt"
    fail=1
  fi

  echo "== $c: clippy =="
  if ! (cd "$dir" && cargo clippy --all-targets --all-features -- -D warnings); then
    echo "FAIL $c: clippy"
    fail=1
  fi

  echo "== $c: test =="
  if ! (cd "$dir" && cargo test --all-targets --all-features); then
    echo "FAIL $c: cargo test"
    fail=1
  fi
done

if [ "$fail" -ne 0 ]; then
  echo "host-crates gate: FAILED"
  exit 1
fi
echo "host-crates gate: OK"
