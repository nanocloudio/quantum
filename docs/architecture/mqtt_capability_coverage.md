# MQTT capability coverage

The state of MQTT protocol support in Quantum as of the apply-side
state machine work. Tests are the load-bearing correctness signal;
anything not under test is not load-bearing.

The companion architecture documents are
[mqtt_adapter.md](mqtt_adapter.md) (codec surface) and
[../apply_side_state_machine.md](../apply_side_state_machine.md)
(propose-then-apply seam every op flows through).

## Tested and passing

Integration tests live in `tests/integration/` and run end-to-end
through the full pipeline (TCP → `mqtt_codec` → `session_processor`
propose → raft → durability → apply → response). All 17 pass via
`make test-mqtt-suite`.

| Test | What it covers |
|---|---|
| `test-graph` (`qos1_test.py`) | CONNECT (clean_start=true) → CONNACK; QoS 1 PUBLISH → PUBACK round-trip. The minimal proof the durability path works. |
| `test-pubsub` (`pubsub_test.py`) | Two clients; subscriber on `test/+` (single-level wildcard); publisher publishes QoS 0; subscriber receives the payload. Validates `apply_qop_subscribe` → `MSG_TOPIC_SUBSCRIBE`, single-level `+` matching in `mqtt_topic_match`, and the topic-engine fan-out path. |
| `test-resume` (`session_resume_test.py`) | Full persistent-session lifecycle: clean-disconnect drops slot via `MSG_SESSION_DROP`; non-clean disconnect parks `persisted=1`; reconnect resurrects with `session_present=1` and the surviving subscription receives the publisher's PUBLISH. |
| `test-load` (`qos1_load_test.py`) | Sustained QoS 1 load (default 200 publishes × 1 client). Validates no leak in inflight / correlation / stash pools across many cycles. p50 ~8ms. |
| `test-mqtt-advanced` (`qos2_test.py`) | QoS 2 four-phase: PUBLISH → PUBREC → PUBREL → PUBCOMP all matched by `packet_id`. Validates `OP_PUBREL` correlation path, QOP_PUBLISH stash + dedup gate, `apply_qop_pubrel` phase transition, and `ack_tracker` firing two MSG_ACK_EMITs (PUBLISH commit → PUBREC, PUBREL commit → PUBCOMP). |
| `test-mqtt-advanced` (`wildcards_test.py`) | Multi-level `#` wildcard: subscriber on `test/#` receives all three of `test/a`, `test/b/c`, `test/x/y/z`. Validates `#` matching in `mqtt_topic_match`. The three publishes are sent in a single `sendall` so the whole pipeline (codec drain → codec_in fan-in → raft per-proposal flush → topic_engine fan-out → response_mux fan-in → peer_router.client_resp) is exercised on burst traffic without pacing. |
| `test-mqtt-advanced` (`pingreq_test.py`) | PINGREQ → PINGRESP heartbeat. `mqtt_codec` synthesizes the response directly without involving `session_processor`. |
| `test-mqtt-advanced` (`retained_test.py`) | Publisher PUBLISHes with `retain=1` then disconnects; a fresh subscriber arrives, subscribes to the same exact-match topic, and receives a PUBLISH carrying the latched topic + payload. Exercises the extended `MSG_RETAINED_WRITE` (inline topic + payload), `retained_store`'s read-back with echoed `(session_slot, sub_qos)`, and the new `MSG_RETAINED_READ` handler in `session_processor`'s `messaging_in` loop that builds a `MSG_TOPIC_DELIVER` envelope and runs it through `try_deliver`. |
| `test-mqtt-advanced` (`will_test.py`) | Publisher CONNECTs with a Will section (`will/state`, payload `client-vanished`, `keep_alive=1s`); the TCP socket is RST-closed without a clean DISCONNECT. The keep-alive sweep notices the missing activity after 1.5 × keep_alive_s, proposes `QOP_DISCONNECT` with `reason=QDISC_REASON_KEEPALIVE`, and `apply_qop_disconnect` fires the stored Will as an apply-side `MSG_TOPIC_PUBLISH`. The pre-existing subscriber receives it. Exercises the extended `QOP_CONNECT` body with the Will trailer, the new Session Will fields, and the disconnect-reason-gated apply fire path. |
| `test-mqtt-advanced` (`session_expiry_test.py`) | MQTT 5 CONNECT with `SessionExpiryInterval=2s` and `clean_start=0`. Disconnect, reconnect within 2s → CONNACK `session_present=1` (slot resurrected); disconnect, wait past 2s → CONNACK `session_present=0` (sweep purged it). Exercises the additive `session_expiry_s` trailer on `QOP_CONNECT`, the apply-side stamp of `disconnected_at_ms`, and the Phase 5d sweep in `session_processor`'s metrics tick. |
| `test-mqtt-advanced` (`topic_alias_test.py`) | MQTT 5 subscriber CONNECTs with `TopicAliasMaximum=10`; an MQTT 3.1.1 publisher then PUBLISHes 3× to one topic. First subscriber-side delivery carries the full topic + `TopicAlias=1` (broker establishes the mapping); the next two carry an empty topic + `TopicAlias=1` (broker substitutes). Exercises the per-conn `sub_aliases` table, the `TopicAliasMaximum` CONNECT property capture, and the alias-aware MQTT 5 PUBLISH encoder. |
| `test-mqtt-advanced` (`user_property_test.py`) | MQTT 5 publisher attaches two `UserProperty` pairs (`req-id=42`, `trace=xyz`) to a QoS 0 PUBLISH; MQTT 5 subscriber receives the PUBLISH and must see both pairs preserved (order intact). Exercises the QOP_PUBLISH V2 trailer carrying the user_props block, MSG_TOPIC_PUBLISH / MSG_TOPIC_DELIVER forwarding the block verbatim between topic and payload, and `encode_mqtt5_publish_with_alias` emitting each pair as an MQTT 5 `0x26` property. |
| `test-mqtt-advanced` (`shared_subscription_test.py`) | Two subscribers join the same `$share/g1/work` group; publisher sends 6 PUBLISHes; the broker delivers each PUBLISH to exactly one of the two members via the per-publish `shared_member_index` hash of `(topic_hash, publish_ordinal)`. The split is 3/3 in steady state. Exercises `parse_shared_prefix` on the subscription filter, `SharedGroup` membership management in topic_engine, and the shared-only delivery branch in the PUBLISH fan-out. |
| `test-mqtt-advanced` (`connack_mqtt5_test.py`) | MQTT 5 CONNECT followed by SUBSCRIBE + UNSUBSCRIBE. Asserts the CONNACK body is the spec-required 3 bytes (`[ack_flags][reason][0x00 props]`), the SUBACK body is 4 bytes (`[packet_id][0x00 props][reason]`), and the UNSUBACK body is 4 bytes (`[packet_id][0x00 props][0x00 reason]` — MQTT 3.1.1 has no reason codes here, MQTT 5 requires one per topic). Exercises `splice_mqtt5_ack` in mqtt_codec. |
| `test-mqtt-advanced` (`wildcard_retained_test.py`) | Three retained-publish-then-disconnect rounds latch matching topics + one non-matching sibling; a fresh subscriber on `wild/+` receives all three matching retained PUBLISHes (broker emits them back-to-back as soon as the SUBSCRIBE commits) and does NOT receive the sibling. Exercises retained_store's `wire::mqtt_topic_match` iteration on `MSG_RETAINED_READ` and the envelope-framed `response_mux` / `peer_router.client_resp` chain on burst delivery. |
| `test-mqtt-advanced` (`receive_maximum_test.py`) | MQTT 5 subscriber CONNECTs with `ReceiveMaximum = 2` and subscribes at QoS 1; publisher fires 3 QoS 1 PUBLISHes back-to-back. The subscriber receives exactly 2, the 3rd is held until both held publishes are PUBACKed, then the 3rd arrives in order. Exercises the new `Session.receive_maximum` field plumbed through the QOP_CONNECT trailer and the cap inside `try_deliver`. |
| `test-will-delay` (`will_delay_test.py`) | MQTT 5 Will Delay Interval. Scenario A: publisher CONNECTs with Will + `WillDelayInterval=2s` + `keep_alive=1s` and RST-closes. The keep-alive sweep proposes `QOP_DISCONNECT(KEEPALIVE)`, the apply schedules `pending_will_fire_at_ms = now + 2s` instead of firing immediately, and the metrics-tick sweep fires the Will after the delay elapses. Subscriber sees no delivery for ~2.5s after kill, then receives the Will. Scenario B: a fresh CONNECT on the same client_id mid-delay cancels the deferred Will entirely (MQTT 5 §3.1.3.2.2). Own runner script — the timing flakes against the shared `mqtt-advanced` runner's accumulated state. |

## Open work


### Cross-repo (substrate / design-out-of-scope)

- **WAL durability across restarts.** Quantum's `wal` module
  declares `requires_contract = "fs"` but clustor's linux FS
  provider rejects the WAL's relative paths and the WAL falls
  back to in-memory (`[wal] no fs`). Replay tests stay deferred
  until either the provider supports create-on-open or
  `linux-minimal.yaml` configures a writable WAL root.

### MQTT-over-QUIC — adapter shipped, e2e gated on aioquic

- **`mqtt_quic_adapter`** ([modules/app/mqtt_quic_adapter/](../../modules/app/mqtt_quic_adapter/))
  bridges fluxor's `quic` foundation module to `mqtt_codec`
  without changing either side. Inbound strips
  `MSG_QUIC_STREAM_DATA` (0x13) → `[conn_id][mqtt bytes]`;
  outbound consumes envelope-framed `MSG_CLIENT_FRAME` from
  `response_mux.mux_out` and re-emits as `MSG_QUIC_STREAM_WRITE`
  (0x14).
- **`quantum-linux-quic.yaml`** ([configs/quantum-linux-quic.yaml](../../configs/quantum-linux-quic.yaml))
  wires `linux_net (UDP 4443) → quic → mqtt_quic_adapter →
  mqtt_codec → session_processor → raft → …`. Validates and
  boots: UDP 4443 binds, raft leader is elected.
- **End-to-end smoke** ([tests/integration/mqtt_quic_test.py](../../tests/integration/mqtt_quic_test.py)
  + [module_graph_mqtt_quic.sh](../../tests/integration/module_graph_mqtt_quic.sh),
  the last leg of `make test-mqtt-suite`) drives a CONNECT + QoS 1 PUBLISH
  over the QUIC bidi stream using `aioquic`. The runner exits
  77 (skip) when `aioquic` isn't installed — stdlib has no QUIC
  stack and we don't want to wedge CI. `pip install aioquic`
  unskips locally.
- **Known CLI-staleness caveat.** The installed `fluxor` binary
  predates the `alpn` addition to `NON_PARAM_KEYS` in
  `fluxor/tools/src/config.rs`. The yaml carries `alpn: "mqtt"`
  as a commented entry until the CLI is rebuilt; until then
  the smoke runs with `QUANTUM_QUIC_NO_ALPN=1` to negotiate
  no ALPN, which still exercises the full data path.
- **Less-common MQTT 5 features.** Server-redirect (CONNACK
  reason `0x9C`), subscription identifiers, response topic,
  correlation data, request/response, content-type. mqtt_codec's
  property table acknowledges the IDs but no propagation exists.
  Each is a ~1-day op-body extension once the use case is real;
  none gated today.
