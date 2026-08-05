# Quantum Documentation

Quantum is a multi-protocol messaging overlay (MQTT 3.1/3.1.1/5.0,
Kafka, AMQP 0-9-1) built as a graph of cooperative modules on the
[Fluxor](../../fluxor) runtime atop the [Clustor](../../clustor) Raft
substrate. 26 `.fmod` modules — 7 Clustor substrate modules plus 19
Quantum application modules — compose into a single-process broker
that delivers quorum-durable exactly-once
semantics, multi-tenant isolation, deterministic backpressure, and a
uniform on-disk format across all three protocols.

This documentation set covers the normative behaviour modules
implement, the operational workflows for running and scaling the
broker, and the build / packaging surfaces operators interact with.
Architecture docs are normative — they describe contracts the modules
and graph wiring honour. Guides are operational — they describe how
to drive the system to achieve a goal.

## Start Here

- [architecture.md](architecture.md) — the structural reference: layers, execution domains, every module, the graph, the durability cascade
- [architecture/messaging_model.md](architecture/messaging_model.md) — entities and invariants every adapter maps onto
- [architecture/partitioning.md](architecture/partitioning.md) — PRG sharding, routing epochs, placement
- [guides/deployment.md](guides/deployment.md) — install layout, systemd unit, runtime state

## Architecture

How the system works. These are the authoritative references; each
covers one concern and assumes the reader has read this overview.
[architecture.md](architecture.md) is the top-level structural
reference — the layers, execution domains, module reference, message
graph, and durability cascade; the canonical wireable graph lives in
[configs/](../configs/). The documents below drill into one concern
each.

- [architecture.md](architecture.md) — layers, execution domains, module reference, message graph, durability cascade
- [architecture/messaging_model.md](architecture/messaging_model.md) — entities, invariants (WAL-SOURCE, DETERMINISTIC-REPLAY, ACK-DURABILITY, ROUTING-EPOCH), default timers, terminology
- [architecture/apply_path.md](architecture/apply_path.md) — the propose/apply seam, proposal format, reset handling, snapshot format
- [architecture/partitioning.md](architecture/partitioning.md) — PRG sharding, placement, routing epochs, rebalance, system model
- [architecture/mqtt_adapter.md](architecture/mqtt_adapter.md) — connection lifecycle, QoS 0/1/2 semantics, shared subscriptions, offline delivery
- [architecture/kafka_adapter.md](architecture/kafka_adapter.md) — topics, partitions, produce semantics, consumer groups, transactions, retention
- [architecture/amqp_adapter.md](architecture/amqp_adapter.md) — addressing, delivery semantics, flow control, durable subscriptions
- [architecture/control_plane.md](architecture/control_plane.md) — CP-Raft responsibilities, cache states, embedded mode, bootstrap
- [architecture/multi_tenancy.md](architecture/multi_tenancy.md) — namespaces, quotas, noisy-neighbour enforcement
- [architecture/flow_control.md](architecture/flow_control.md) — credits, backpressure translation, throughput floor, prefetch
- [architecture/security.md](architecture/security.md) — TLS / mTLS, identity, RBAC, at-rest crypto, signing
- [architecture/observability.md](architecture/observability.md) — metrics, tracing, audit logging
- [architecture/disaster_recovery.md](architecture/disaster_recovery.md) — DR orchestration, fenced promotion, upgrades, fault injection
- [architecture/session_decomposition.md](architecture/session_decomposition.md) — how `session_processor` splits into components, and the rules each one enforces

## Guides

How to drive the system. Operational patterns and recipes.

- [guides/configuration.md](guides/configuration.md) — the graph-YAML config surface, platform stack, listeners, durability tuning
- [guides/deployment.md](guides/deployment.md) — Linux/systemd install: build artifacts, `/opt/quantum` layout, unit, runtime state
- [guides/bring_up.md](guides/bring_up.md) — the bare-metal production path: rig topology, build recipe, smoke, pass signals
- [guides/high_availability.md](guides/high_availability.md) — front-door patterns, rolling-restart runbook, client expectations
- [guides/scaling.md](guides/scaling.md) — scale-up / shrink runbooks with CP-Raft hooks
- [guides/performance.md](guides/performance.md) — targets, harnesses, validated behaviour, validation procedure
- [guides/interop.md](guides/interop.md) — exercising the broker against `mosquitto_pub` / `mosquitto_sub` / Paho
- [guides/cli.md](guides/cli.md) — `fluxor` tool, `make` targets, smoke harnesses, chaos drivers
- [guides/dependencies.md](guides/dependencies.md) — Fluxor / Clustor checkouts, toolchain, build requirements, runtime deps
- [guides/dev_seeding.md](guides/dev_seeding.md) — bootstrap path, on-disk state, producing seed data

## Conventions

- **Present tense, declarative.** Architecture docs describe what is, not what will be.
- **Normative language sparingly.** MUST / SHOULD only where there is a genuine conformance contract for adapters or operators. Settled architectural facts read as facts.
- **Tables for option matrices** (modes, durability levels, defaults).
- **Code blocks for wire formats, YAML excerpts, and command lines.** Not for prose.
- **Module names in `monospace`.** Section anchors (`§7.2`) reference the architecture doc's own section, not external numbering.
- **Cross-references are markdown links to relative paths.** Never bare filenames or fictional URLs.

The build/run quickstart lives in the repo-level
[README.md](../README.md), which covers the project overview,
orientation, and operator commands. This documentation set picks up
where the README leaves off.
