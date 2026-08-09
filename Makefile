# quantum Makefile — the lifecycle only: clean / build / test / lint /
# ci / publish. Anything else is the `fluxor` CLI directly
# (`fluxor modules build`, `fluxor run`, `fluxor
# update`, `fluxor sync`, `fluxor build --check …`) — a make target that
# merely renames one CLI command is bloat, not convenience.
# See ~/Development/nanocloudio/standards/make.md.

.PHONY: help build test lint ci publish clean test-mqtt-suite

SHELL       := /bin/bash
.SHELLFLAGS := -euo pipefail -c

.DEFAULT_GOAL := build

help:
	@echo "quantum lifecycle:"
	@echo "  make build     cargo build --all-targets, per host crate"
	@echo "  make test      cargo test --all-targets, per host crate"
	@echo "  make lint      rustfmt --check + clippy -D warnings, per host crate"
	@echo "  make ci        fluxor ci — the full gate (lints, hygiene,"
	@echo "                 tests, strict module build, lockfile checks)"
	@echo "  make publish   fluxor publish — publish artefacts into the store"
	@echo "  make clean     cargo clean + module artefacts"
	@echo ""
	@echo "quantum-specific:"
	@echo "  make test-mqtt-suite   build PIC modules, then run all seven MQTT"
	@echo "                         integration smokes strictly sequentially"
	@echo "                         (the TCP smokes each bind port 9090)"
	@echo ""
	@echo "Not make targets (use the CLI directly):"
	@echo "  fluxor modules build --target bcm2712 --out target   PIC modules —"
	@echo "                            prerequisite for every integration smoke below"
	@echo "  fluxor modules build --all --out target              every fluxor.toml target"
	@echo "  fluxor run configs/…                                 bring-up"
	@echo "  fluxor update / sync                                 store consumption"
	@echo "  fluxor build --check configs/quantum-linux.yaml      graph-YAML validation"
	@echo "                            (one config per invocation; loop for all)"
	@echo "  fluxor publish --local                               local-only publish"
	@echo "  tools/ci-shadow-guard.sh                             shadow-checkout hard-fail (ci 3.5)"
	@echo "  tools/host_crates_e2e.sh                             host-crate fmt/clippy/test (ci 3.5)"
	@echo ""
	@echo "Integration smokes (run the script directly; build modules first):"
	@echo "  tests/integration/module_graph_mqtt.sh           MQTT/AMQP/Kafka handshake e2e"
	@echo "  tests/integration/module_graph_load.sh           QoS 1 load (CLIENTS= MSGS_PER_CLIENT=)"
	@echo "  tests/integration/module_graph_pubsub.sh         pub/sub end-to-end delivery"
	@echo "  tests/integration/module_graph_resume.sh         persistent-session resume"
	@echo "  tests/integration/module_graph_mqtt_advanced.sh  QoS 2 + multi-level '#' + PINGREQ"
	@echo "  tests/integration/module_graph_will_delay.sh     MQTT 5 Will Delay Interval"
	@echo "  tests/integration/module_graph_mqtt_quic.sh      MQTT over QUIC (UDP 4443; skips w/o aioquic)"
	@echo "  tests/integration/module_graph_kafka.sh          Kafka ApiVersions handshake"
	@echo "  tests/integration/multi_node.sh                  3-node raft cluster (ports 9090-9092)"
	@echo "One-time setup: make -C ../fluxor install"

# No root Cargo workspace: the deployable surface is modules/, built by
# `fluxor modules build`. The three host crates under tools/ stand alone,
# so each lifecycle target drives them per-crate — the same set, and the
# same order, that `tools/host_crates_e2e.sh` enforces in CI.
HOST_CRATES := tools/telemetry_guard tools/wire_lint tools/quantum-bench

build:
	@for c in $(HOST_CRATES); do (cd $$c && cargo build --all-targets) || exit 1; done

test:
	@for c in $(HOST_CRATES); do (cd $$c && cargo test --all-targets) || exit 1; done

lint:
	@for c in $(HOST_CRATES); do \
	  (cd $$c && cargo fmt -- --check && \
	   cargo clippy --all-targets --all-features -- -D warnings) || exit 1; done

ci:
	fluxor ci

publish:
	fluxor publish

clean:
	@for c in $(HOST_CRATES); do (cd $$c && cargo clean) || exit 1; done
	fluxor modules clean

# The one project-specific target (standards/make.md §1.3 / §4): a
# genuine composition whose ordering knowledge lives nowhere else.
# The TCP smokes all bind the same port-9090 listener, so the seven
# scripts must run strictly sequentially, and the graph boots from
# prebuilt PIC fmods, so the module build must precede them. Each
# individual script is invoked directly (see `make help`).
test-mqtt-suite:
	fluxor modules build --target bcm2712 --out target
	bash tests/integration/module_graph_mqtt.sh
	bash tests/integration/module_graph_pubsub.sh
	bash tests/integration/module_graph_resume.sh
	bash tests/integration/module_graph_mqtt_advanced.sh
	bash tests/integration/module_graph_will_delay.sh
	bash tests/integration/module_graph_load.sh
	bash tests/integration/module_graph_mqtt_quic.sh
