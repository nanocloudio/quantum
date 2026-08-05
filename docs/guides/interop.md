# MQTT Interop Suite

Quantum speaks standard MQTT 3.1/3.1.1/5.0 on the wire (`protocol`'s mqtt component module), so any off-the-shelf MQTT client should work without modification. This document captures the scenarios the build is exercised against and how to run them. They are opt-in — they rely on external binaries and a running graph, not on the in-tree test harness.

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

The byte-for-byte multi-protocol assertion lives in [tests/integration/module_graph_mqtt.sh](../../tests/integration/module_graph_mqtt.sh) (also the first leg of `make test-mqtt-suite`) — that one exercises MQTT, AMQP `Connection.Start`, and Kafka `ApiVersions` against the same running graph.

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
- Verify inflight QoS-2 packets resume without duplicate delivery. `messaging`'s dedup component + `session_processor` four-phase state machine should cover this.

## What the durability contract looks like on the wire

`flow`'s ack component consumes `durability.quorum_durable` directly and only then emits PUBACK / PUBREC. Long fsync windows therefore show up as latency, never as data loss. Expect bursty fsync to manifest as elongated p99 PUBACK latency under high publish rates; the WAL's group-fsync window (configured via `durability.fsync_mode` / `group_window_ms` / `group_max_pending`) amortises small publishes, but a single large flush can stall an inflight slot for the duration of the fsync.

## MQTT feature coverage

Each feature below is exercised end-to-end through the full pipeline
(TCP → `protocol`'s mqtt component → `session_processor` → Raft → durability → apply →
response) by an integration test under `tests/integration/`, run via
`make test-mqtt-suite`.

| Feature | What it proves | Test |
|---|---|---|
| CONNECT → CONNACK, QoS 1 PUBACK | The durability path (WAL → quorum → ack) | `module_graph_mqtt.sh` |
| QoS 0 pub/sub, `+` single-level wildcard | Subscribe apply + topic-engine fan-out | `pubsub_test.py` |
| `#` multi-level wildcard | Wildcard matching across depths | `wildcards_test.py` |
| QoS 2 four-phase (PUBLISH/PUBREC/PUBREL/PUBCOMP) | Dedup gate + two-stage ack emission | `qos2_test.py` |
| PINGREQ → PINGRESP | Codec-synthesised heartbeat | `pingreq_test.py` |
| Retained messages (exact + `+` wildcard on subscribe) | Retained write + read-back on new subscription | `retained_test.py`, `wildcard_retained_test.py` |
| Persistent-session resume | Clean-disconnect drop vs non-clean park; `session_present` | `session_resume_test.py` |
| MQTT 5 Session Expiry Interval | Slot resurrection within / purge past the interval | `session_expiry_test.py` |
| Will message | Fires on ungraceful disconnect via keep-alive sweep | `will_test.py` |
| MQTT 5 Will Delay Interval | Deferred fire; cancelled by reconnect mid-delay | `will_delay_test.py` |
| MQTT 5 Topic Alias | Alias establishment + substitution | `topic_alias_test.py` |
| MQTT 5 User Properties | Pairs preserved end-to-end, order intact | `user_property_test.py` |
| MQTT 5 Receive Maximum | Inflight cap honoured; held publishes released in order | `receive_maximum_test.py` |
| Shared subscriptions `$share/<g>/<topic>` | Each publish to exactly one group member | `shared_subscription_test.py` |
| MQTT 5 CONNACK/SUBACK/UNSUBACK property framing | Spec-required reason + property bytes | `connack_mqtt5_test.py` |
| Kafka ApiVersions handshake | Cross-protocol codec on the same graph | `module_graph_kafka.sh` |

## Notes on QUIC

- **MQTT-over-QUIC** is bridged by the `mqtt_quic_adapter` module, which sits between Fluxor's `quic` foundation module and `protocol`'s mqtt component without changing either side. `configs/quantum-linux-quic.yaml` wires it on UDP 4443; the graph validates and boots (UDP binds, Raft leader elected). The end-to-end smoke (`tests/integration/module_graph_mqtt_quic.sh`, the last leg of `make test-mqtt-suite`) drives a CONNECT + QoS 1 PUBLISH over a QUIC bidi stream using `aioquic`, and skips (exit 77) when `aioquic` is not installed — `pip install aioquic` runs it locally.
- ALPN `mqtt-quic` is the routing tag the `protocol` router looks for; the router's `default_protocol = mqtt` falls back to MQTT-over-TLS on unknown ALPN.
- 0-RTT should be disabled except for CONNECT (the codec rejects 0-RTT data for non-CONNECT packets).
- Certificate material must permit QUIC ALPN selection.

## What is not currently tested

- Sustained Kafka consumer-group rebalance under load (the codec decodes the API surface but `session_processor`'s group-lifecycle integration tests are still minimal).
- Kafka and AMQP transactions are not implemented; see [kafka_adapter.md](../architecture/kafka_adapter.md).
- AMQP exchange-binding correctness against `rabbitmq-perf-test` — `protocol`'s amqp component decodes the wire but the high-level routing matrix has not been swept end-to-end.

These are tracked as gaps; contribute coverage as needed.
