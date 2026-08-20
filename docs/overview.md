# Quantum Documentation

Quantum is a multi-protocol message broker (MQTT 3.1/3.1.1/5.0,
Kafka, AMQP 0-9-1) built as a graph of cooperative modules on the
fluxor runtime atop the clustor Raft substrate. A deployed broker
graph composes the 7-module clustor substrate with 7 quantum
application modules into a single-process broker with
durability-gated acknowledgements, deterministic backpressure, and a
uniform on-disk format across all three protocols.

Architecture documents describe contracts the modules and graph
wiring honour; where a mechanism is designed but not yet wired, the
document says so with an explicit status note. Guides are
operational: how to drive the system to achieve a goal.

## Start here

- [guides/running.md](guides/running.md) — validated single-node
  bring-up with the embedded deployment config and smoke checks
- [architecture.md](architecture.md) — the structural reference:
  layers, execution domains, every module, the graph, the durability
  cascade
- [architecture/messaging_model.md](architecture/messaging_model.md)
  — entities and invariants every adapter maps onto

## Architecture

How the system works. [architecture.md](architecture.md) is the
top-level structural reference; the documents below drill into one
concern each.

- [architecture.md](architecture.md) — layers, execution domains, module reference, message graph, durability cascade
- [architecture/messaging_model.md](architecture/messaging_model.md) — entities, invariants (WAL-SOURCE, DETERMINISTIC-REPLAY, ACK-DURABILITY, ROUTING-EPOCH), timers, terminology
- [architecture/apply_path.md](architecture/apply_path.md) — the propose/apply seam, proposal format, reset handling, snapshot design
- [architecture/partitioning.md](architecture/partitioning.md) — PRG sharding, placement, routing epochs, rebalance
- [architecture/session_decomposition.md](architecture/session_decomposition.md) — how `session_processor` splits into components, and the rules each one enforces
- [architecture/mqtt_adapter.md](architecture/mqtt_adapter.md) — connection lifecycle, QoS 0/1/2 semantics, shared subscriptions, offline delivery
- [architecture/kafka_adapter.md](architecture/kafka_adapter.md) — topics, partitions, produce semantics, consumer groups
- [architecture/amqp_adapter.md](architecture/amqp_adapter.md) — addressing, delivery semantics, flow control
- [architecture/control_plane.md](architecture/control_plane.md) — the control-plane consumption contract, cache states, embedded design
- [architecture/multi_tenancy.md](architecture/multi_tenancy.md) — namespaces, quotas, noisy-neighbour enforcement, tenancy status
- [architecture/flow_control.md](architecture/flow_control.md) — credits, backpressure translation, prefetch
- [architecture/security.md](architecture/security.md) — what is wired, and the target transport/identity/authorisation model
- [architecture/observability.md](architecture/observability.md) — metrics, diagnostic endpoints, audit
- [architecture/disaster_recovery.md](architecture/disaster_recovery.md) — DR orchestration status, fenced promotion, upgrades

## Guides

How to drive the system.

- [guides/running.md](guides/running.md) — the validated bring-up, with the deployment config embedded
- [guides/configuration.md](guides/configuration.md) — the graph-YAML config surface, platform stack, variants, durability tuning
- [guides/deployment.md](guides/deployment.md) — Linux/systemd install, `/opt/quantum` layout, the bare-metal target
- [guides/high_availability.md](guides/high_availability.md) — guarantees, front-door pattern, rolling-restart runbook
- [guides/scaling.md](guides/scaling.md) — scale-up / shrink runbooks (design target)
- [guides/cli.md](guides/cli.md) — the `fluxor` CLI surface and protocol clients
- [guides/dependencies.md](guides/dependencies.md) — the store workflow, toolchain, module policy

## Conventions

- **Present tense, declarative.** Docs describe what is; design
  targets are labelled with an explicit status note.
- **Normative language sparingly.** MUST / SHOULD only where there
  is a genuine conformance contract.
- **Tables for option matrices**; prose for explanation.
- **Module names in `monospace`.** Section references (`§7.2`) point
  at a document's own sections, never external numbering.
- **Cross-references are markdown links to relative paths.**

The build/run quick start lives in the repo-level
[README.md](../README.md); this documentation set picks up where the
README leaves off.
