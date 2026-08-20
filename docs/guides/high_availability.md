# High Availability

Quantum achieves high availability through the substrate: every
Partition Raft Group (PRG) survives node loss as long as a quorum
remains, and durable state replays deterministically when a node
returns. This guide covers the guarantees, the front-door pattern,
and the rolling-restart runbook.

Parts of the surrounding machinery — readiness-gated load-balancer
integration, listener drain, epoch-driven client steering — depend
on a real control plane and are design targets; see
[control_plane.md](../architecture/control_plane.md). The runbook
below uses only implemented operations.

## Guarantees

- **State durability and replay.** Session state, dedupe shards,
  offline queues and retained payloads all commit through the WAL;
  any node restart or crash replays deterministically once the node
  rejoins.
- **ACK contract.** `flow`'s ack component consumes durability
  proofs directly and only then emits PUBACK / PUBREC / Kafka
  `ProduceResponse` / AMQP `Basic.Ack`. Client-visible QoS
  guarantees persist through node loss as long as a quorum survives.
- **Leadership mobility.** The substrate's `transfer-leader` admin
  operation moves PRG leadership off a node before it restarts, so
  writes stay available while it is down.

## Front door

Run each node's listener behind an L4 load balancer or VIP that only
multiplexes TCP. Health-check against the node's diagnostic
endpoint where the graph wires it, or at minimum against listener
liveness, and remove a node from the pool before restarting it.
MQTT, Kafka and AMQP clients all carry reconnect logic; a removed
node's clients reconnect to the remaining pool members.

## Rolling restart

For each node in sequence:

1. Remove the node from the load-balancer pool.
2. Transfer PRG leadership away from the node (the
   `transfer-leader` admin operation, per partition).
3. `systemctl stop quantum`; replace `/opt/quantum/modules/*.fmod`
   and/or `bin/fluxor*` as needed; `systemctl start quantum`.
4. Wait for WAL replay to complete and the node to serve traffic
   again.
5. Re-add the node to the pool and move to the next.

This sequence loses no data and keeps write availability as long as
a quorum of every PRG remains up.

## Client experience during restarts

- **Active sessions on the restarting node** see socket closure and
  reconnect to another pool member. MQTT clients are already
  required to handle reconnect/retry.
- **QoS guarantees.** Because session and dedupe state commit
  through the WAL, retransmitted PUBLISH/PUBREL packets are detected
  as duplicates; QoS 1/2 messages remain exactly-once inside the
  broker. Clients may observe at-least-once delivery during
  in-flight retries, which matches MQTT expectations.
- **Latency.** A reconnect costs one connection handshake; durable
  state is already replayed before the node serves traffic.
- **Backoff.** Clients should reconnect with exponential backoff and
  jitter to avoid thundering herds after a restart.
- **Session mobility.** MQTT has no in-protocol way to migrate a
  live session without a reconnect; the HA story rests on the
  reconnect/retry contract, with durable session state making the
  reconnect transparent.
