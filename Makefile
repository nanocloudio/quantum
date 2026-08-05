# quantum Makefile — the lifecycle only: clean / build / test / lint /
# ci / publish. Anything else is the `fluxor` CLI directly
# (`fluxor modules build`, `fluxor run`, `fluxor up`, `fluxor
# update`, `fluxor sync`, `fluxor validate …`) — a make target that
# merely renames one CLI command is bloat, not convenience.
# See ~/Development/nanocloudio/standards/make.md.

.PHONY: help build test lint ci publish clean test-mqtt-suite

SHELL       := /bin/bash
.SHELLFLAGS := -euo pipefail -c

.DEFAULT_GOAL := build

help:
	@echo "quantum lifecycle:"
	@echo "  make build     cargo build --workspace --all-targets"
	@echo "  make test      cargo test --workspace"
	@echo "  make lint      rustfmt --check + clippy -D warnings"
	@echo "  make ci        fluxor ci — the full gate (lints, hygiene,"
	@echo "                 tests, strict module build, lockfile checks)"
	@echo "  make publish   fluxor publish — canonical registry publish"
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
	@echo "  fluxor run configs/… / fluxor up …                   bring-up"
	@echo "  fluxor update / sync                                 registry consumption"
	@echo "  fluxor validate configs/quantum-*.yaml               graph-YAML validation"
	@echo "  fluxor publish --local                               local-only publish"
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
	@echo "One-time setup: cargo install --locked --path ../fluxor/tools"

build:
	cargo build --workspace --all-targets

test:
	cargo test --workspace

lint:
	cargo fmt --all -- --check
	cargo clippy --workspace --all-targets --all-features -- -D warnings

ci:
	fluxor ci

publish:
	fluxor publish

clean:
	cargo clean
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
