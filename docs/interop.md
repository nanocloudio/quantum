# MQTT Interop Suite

Use these steps to exercise Quantum against common MQTT clients. Tests are opt-in and rely on external binaries; run with `cargo test --test interop -- --ignored`.

## Prerequisites
- Running quantum listener with TLS (or QUIC if available).
- `mosquitto_pub` / `mosquitto_sub` installed (set `MOSQUITTO_PUB_BIN` / `MOSQUITTO_SUB_BIN` if not in `$PATH`).
- Paho sample client installed (set `PAHO_SAMPLE` to the binary).
- TLS assets: client key/chain and CA matching the listener configuration.

## Scenarios
1. **TCP/TLS QoS1/2 round-trip**
   - `mosquitto_sub -h <host> -p <port> -t tenant/topic -q 2 --cafile ca.pem --cert client.pem --key key.pem`
   - `mosquitto_pub -h <host> -p <port> -t tenant/topic -q 2 -m "payload" --cafile ca.pem --cert client.pem --key key.pem`
   - Expect PUBREC/PUBREL/PUBCOMP after durability fsync and retained delivery when `-r` is used.
2. **QUIC QoS2 resume**
   - `PAHO_SAMPLE` with `mqtt-quic` ALPN, reconnect with `clean_start=false`, and verify inflight resumes without duplicate deliveries.
3. **Shared subscription distribution**
   - Launch two `mosquitto_sub` consumers on `$share/group/tenant/topic`; publish 100 messages and confirm balanced, non-duplicate fan-out.
4. **Will + retained**
   - Connect with Will payload; force disconnect; subscriber should receive Will then retained state on next subscribe.

## Notes
- The interop tests intentionally wait for durability ACKs before returning PUBACK/PUBREC/PUBREL/PUBCOMP; long fsyncs will surface as latency, not data loss.
- QUIC requires the `quic` feature; ensure certificates permit QUIC ALPN and 0-RTT is disabled except CONNECT.
