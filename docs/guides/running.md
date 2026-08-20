# Running Quantum

This guide brings up a single-node broker on a Linux machine and
smoke-checks it over MQTT. It is self-contained: the deployment
config is embedded below and pipes straight into `fluxor run`, so
there is nothing else to fetch.

## Prerequisites

Quantum runs on the [fluxor](../../../fluxor/) runtime and consumes
the clustor substrate modules through the local OCI store. One-time
setup on a development machine:

```sh
git clone git@github.com:nanocloudio/fluxor.git ../fluxor
git clone git@github.com:nanocloudio/clustor.git ../clustor
make -C ../fluxor install     # put the fluxor CLI on PATH
make -C ../fluxor publish     # publish SDK, runtime, foundation modules
make -C ../clustor publish    # publish the substrate modules

# in this repository
fluxor sync                              # materialise pinned artefacts
fluxor modules build --target bcm2712    # build quantum's modules
```

See [dependencies.md](dependencies.md) for the dependency workflow
behind these commands.

An MQTT client is needed for the smoke checks; the examples below use
`mosquitto_pub` / `mosquitto_sub` (Debian/Ubuntu package
`mosquitto-clients`).

## Single-node broker

The graph below is a 13-module single-node broker: the clustor
substrate (one-voter Raft, WAL, admission) plus quantum's protocol
stack, serving cleartext MQTT on port 9090. One node is its own
quorum, so publishes commit at local durability without peer
traffic.

From the repository root:

```sh
fluxor run - <<'EOF'
# Quantum single-node broker: MQTT over cleartext TCP on port 9090.
# Clustor substrate (8 modules) + Quantum protocol stack (5 modules);
# the linux_net transport endpoint is supplied by the runtime.

target: linux
tick_us: 1000

# Request/response edges and flow-control feedback make the graph
# cyclic; the scheduler accepts cycles only when opted in.
scheduler:
  accept_cycles: true

platform:
  net: {}

modules:
  # Clustor substrate
  - name: peer_router
  - name: partition_router
  - name: consensus
    params:
      proposal_batch_timeout_ms: 1
  - name: durability
    params:
      fsync_mode: 1
      group_window_ms: 0
      group_max_pending: 8
      fence_depth: 8
      segment_bytes: 4194304
  - name: control_plane
  - name: admission
  - name: gateway
  - name: operations

  # Quantum protocol stack
  - name: protocol
  - name: session_processor
  - name: flow
  - name: topic_engine
  - name: messaging

wiring:
  # Transport: linux_net <-> peer_router. Client-path channels are
  # raised from the 8 KiB default so bursts of simultaneous
  # CONNECTs/PUBLISHes are not dropped at the channel layer.
  - from: linux_net.net_out
    to: peer_router.net_in
    buffer_bytes: 65536
  - from: peer_router.net_out
    to: linux_net.net_in
    buffer_bytes: 65536

  # Client path: peer_router -> protocol codecs
  - from: peer_router.cleartext
    to: protocol.raw_in
    buffer_bytes: 65536
  - from: peer_router.peer_rx
    to: consensus.ack

  # HTTP/admin leg
  - from: protocol.http_out
    to: operations.request

  # Client responses fan in at the transport
  - from: operations.response
    to: peer_router.client_resp
  - from: protocol.frames_out
    to: peer_router.client_resp
    buffer_bytes: 65536

  # Codec <-> session_processor
  - from: protocol.proposals_out
    to: session_processor.codec_in
    buffer_bytes: 65536
  - from: session_processor.codec_out
    to: protocol.responses_in
    buffer_bytes: 65536

  # session_processor -> Raft. The untagged path flows through
  # partition_router; the tagged path (QoS 1+ PUBLISH) carries the
  # partition id end-to-end and goes to consensus directly.
  - from: session_processor.proposals
    to: partition_router.proposals
  - from: partition_router.proposals_p0
    to: consensus.proposals_partitioned
  - from: session_processor.proposals_tagged
    to: consensus.proposals_tagged
  - from: consensus.proposal_assigned
    to: session_processor.assigned_in

  - from: consensus.net_out
    to: peer_router.peer_tx
  - from: consensus.rpc_out
    to: peer_router.peer_tx

  # Persistence. The durability edge is rate-classed so the WAL entry
  # pump receives a real per-step byte grant and batches records.
  - from: consensus.log_append
    to: durability.entries
    rate: transaction
  - from: consensus.entry_request
    to: durability.entry_request
    buffer_bytes: 8192
  - from: durability.entry_reply
    to: consensus.entry_reply
    buffer_bytes: 8192
  # Boot handoff: Raft resumes at the WAL replay high-water mark
  # before intake opens.
  - from: durability.replay_complete
    to: consensus.wal_replay_complete
    rate: transaction
  # Leader self-durability: Raft learns its own WAL flush progress
  # directly rather than waiting on the heartbeat round.
  - from: durability.flushed
    to: consensus.wal_flushed
  - from: consensus.cross_durability_ack
    to: durability.ack
  - from: durability.quorum_durable
    to: consensus.durable

  # Commit -> apply -> session_processor
  - from: consensus.applied
    to: session_processor.committed_in
  - from: consensus.committed_entries
    to: session_processor.committed_in

  # Flow composite: ack / backpressure / prefetch
  - from: durability.quorum_durable
    to: flow.durability_in
  - from: session_processor.forward_out
    to: flow.forward_in
  - from: flow.ack_out
    to: session_processor.ack_in

  # Admission credits + flow inputs
  - from: admission.credits
    to: gateway.credit_supply
  - from: admission.credits
    to: session_processor.flow_in
  - from: consensus.lag_signal
    to: admission.lag
  - from: gateway.rejected
    to: flow.rejected_in
  - from: flow.flow_out
    to: session_processor.flow_in

  # Topic engine
  - from: session_processor.topic_out
    to: topic_engine.op_in
  - from: topic_engine.deliver_out
    to: session_processor.deliver_in

  # Messaging composite: dedup / offline / retained
  - from: session_processor.messaging_out
    to: messaging.op_in
  - from: messaging.result_out
    to: session_processor.messaging_in

  # Control plane
  - from: control_plane.proof
    to: admission.proof
  - from: admission.cache_state
    to: consensus.cp_state
  - from: admission.strict_fallback
    to: consensus.cp_state

  # Metrics fan-in
  - from: consensus.metrics
    to: operations.ingest
  - from: durability.metrics
    to: operations.ingest
  - from: admission.metrics
    to: operations.ingest
  - from: session_processor.metrics_out
    to: operations.ingest
EOF
```

`fluxor run` validates the config against the module manifests,
packs the module table, and executes `fluxor-linux` in the
foreground; logs go to stderr. The MQTT listener is
`peer_router`'s `listen_port` (default 9090, bound on all
interfaces). WAL segments land under `wal/` and snapshot state under
`data/` in the working directory; both are recreated on demand.

## Smoke checks

With the broker running, in a second terminal:

```sh
# QoS 1 publish/subscribe round-trip
mosquitto_sub -h 127.0.0.1 -p 9090 -t 'sensors/#' -q 1 -C 1 &
sleep 1
mosquitto_pub -h 127.0.0.1 -p 9090 -t 'sensors/room1/temp' -q 1 -m '23.5'
```

The subscriber prints `23.5` and exits. The PUBACK behind that
publish was emitted only after the WAL entry reached durability, so
the round-trip exercises the full propose → WAL → fsync → durability
proof → ack pipeline rather than the codec alone.

```sh
# Retained message: publish with -r, then read back on a fresh
# subscription
mosquitto_pub -h 127.0.0.1 -p 9090 -t 'demo/retained' -q 1 -r -m 'retained-payload'
mosquitto_sub -h 127.0.0.1 -p 9090 -t 'demo/#' -q 1 -C 1
```

The second command returns `retained-payload` immediately: the
retained store replayed the payload to the new subscriber.

## Stopping

`Ctrl+C` in the `fluxor run` terminal stops the runtime. State under
`wal/` and `data/` persists; the next run replays the WAL and
recovers durable state (retained messages, persistent sessions).
Delete both directories for a fresh start.

## Beyond one node

- The listener port and every module parameter shown above are
  ordinary graph values; see
  [configuration.md](configuration.md) for the config surface.
- TLS termination adds the fluxor `tls` module in front of
  `peer_router`; the bare-metal deployment does this by default (see
  [deployment.md](deployment.md)).
- Kafka and AMQP live in the same `protocol` module: the graph above
  carries its default `full` variant, which compiles in all three
  codecs. The router classifies each connection from its first bytes,
  so all protocols share the one listener.
