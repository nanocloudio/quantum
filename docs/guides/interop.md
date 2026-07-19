# MQTT Interop Suite

Quantum speaks standard MQTT 3.1/3.1.1/5.0 on the wire (`mqtt_codec` module), so any off-the-shelf MQTT client should work without modification. This document captures the scenarios the build is exercised against and how to run them. They are opt-in — they rely on external binaries and a running graph, not on the in-tree test harness.

## Prerequisites

- A running Quantum graph with the MQTT listener exposed. The minimal graph (`fluxor run configs/quantum-linux-minimal.yaml`) binds `127.0.0.1:9090` cleartext; production graphs bind TLS.
- `mosquitto_pub` / `mosquitto_sub` installed. Override path with `MOSQUITTO_PUB_BIN` / `MOSQUITTO_SUB_BIN` if not in `$PATH`.
- Paho MQTT C/Python sample client for advanced scenarios (QUIC, MQTT 5 takeover). Override path with `PAHO_SAMPLE`.
- TLS assets (client cert + key + CA) matching the listener configuration when targeting a TLS listener.

## In-tree smoke

Two scripts cover the basic interop loop end-to-end with stdlib Python (no `paho-mqtt` dependency):

```sh
./tests/integration/pubsub_test.py     # subscriber + publisher round-trip
./tests/integration/qos1_test.py       # QoS-1 PUBACK with packet_id match
```

Both default to `127.0.0.1:9090`; override via `--host` / `--port` or `QUANTUM_HOST` / `QUANTUM_PORT`.

The byte-for-byte multi-protocol assertion lives in [tests/integration/module_graph_mqtt.sh](../tests/integration/module_graph_mqtt.sh) (also the first leg of `make test-mqtt-suite`) — that one exercises MQTT, AMQP `Connection.Start`, and Kafka `ApiVersions` against the same running graph.

## Mosquitto scenarios

### 1. TCP/TLS QoS 1/2 round-trip

```sh
mosquitto_sub -h <host> -p <port> -t tenant/topic -q 2 \
    --cafile ca.pem --cert client.pem --key client.key

mosquitto_pub -h <host> -p <port> -t tenant/topic -q 2 -m "payload" \
    --cafile ca.pem --cert client.pem --key client.key
```

Expect PUBREC → PUBREL → PUBCOMP after durability fsync. Retained delivery is exercised by adding `-r` on `mosquitto_pub`.

### 2. Shared subscription distribution

```sh
# Two subscribers in the same share group
mosquitto_sub -h <host> -p <port> -t '$share/group/tenant/topic' -q 1 -i sub-a &
mosquitto_sub -h <host> -p <port> -t '$share/group/tenant/topic' -q 1 -i sub-b &

# 100 publishes
for i in $(seq 1 100); do
    mosquitto_pub -h <host> -p <port> -t tenant/topic -q 1 -m "msg-$i"
done
```

Expect balanced (≈50/50) non-duplicate fan-out. The hash seed in `topic_engine` is stable per epoch, so the distribution is deterministic for a fixed pair of subscriber identifiers.

### 3. Will + retained

1. Connect a "publisher" with a Will payload on `tenant/will-topic` and `clean_start=true`.
2. Force-disconnect (drop TCP).
3. Subscribe to `tenant/will-topic` — first the Will, then any retained payload on subsequent subscribes.

```sh
mosquitto_pub -h <host> -p <port> -t tenant/state -m online -r --will-topic tenant/will-topic --will-payload offline ...
```

### 4. QUIC QoS-2 resume (Paho)

QUIC requires the `quic` listener to be wired in the graph (not present in `quantum-linux-minimal.yaml`). Once present:

- Connect via Paho with ALPN `mqtt-quic`, `clean_start=false`.
- Force-disconnect mid-inflight; reconnect.
- Verify inflight QoS-2 packets resume without duplicate delivery. `dedup_engine` + `session_processor` four-phase state machine should cover this.

## What the durability contract looks like on the wire

`ack_tracker` consumes `durability_ledger.quorum_durable` directly and only then emits PUBACK / PUBREC. Long fsync windows therefore show up as latency, never as data loss. Expect bursty fsync to manifest as elongated p99 PUBACK latency under high publish rates; the WAL's group-fsync window (configured via `wal.fsync_mode` / `group_window_ms` / `group_max_pending`) amortises small publishes, but a single large flush can stall an inflight slot for the duration of the fsync.

## Notes on QUIC

- ALPN `mqtt-quic` is the routing tag the `protocol_router` looks for; `protocol_router.default_protocol = mqtt` falls back to MQTT-over-TLS on unknown ALPN.
- 0-RTT should be disabled except for CONNECT (the codec rejects 0-RTT data for non-CONNECT packets).
- Certificate material must permit QUIC ALPN selection.

## What is not currently tested

- Sustained Kafka consumer-group rebalance under load (the codec decodes the API surface but `consumer_group_coordinator` integration tests are still minimal).
- AMQP exchange-binding correctness against `rabbitmq-perf-test` — `amqp_codec` decodes the wire but the high-level routing matrix has not been swept end-to-end.

These are tracked as gaps; contribute coverage as needed.
