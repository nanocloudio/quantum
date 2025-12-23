# Quantum Quickstart

## TCP/TLS
1. Generate TLS keys/certs and place under `/etc/quantum/tls`.
2. Configure `listeners.tcp` in `config.toml`:
   ```toml
   [listeners.tcp]
   bind = "0.0.0.0:8883"
   tls_chain_path = "/etc/quantum/tls/server.crt"
   tls_key_path = "/etc/quantum/tls/server.key"
   client_ca_path = "/etc/quantum/tls/ca.crt"
   alpn = ["mqtt", "mqttv5"]
   ```
3. Start the binary: `quantum --config /etc/quantum/config.toml`.

## QUIC
1. Enable `quic` feature and configure:
   ```toml
   [listeners.quic]
   bind = "0.0.0.0:8443"
   tls_chain_path = "/etc/quantum/tls/server.crt"
   tls_key_path = "/etc/quantum/tls/server.key"
   client_ca_path = "/etc/quantum/tls/ca.crt"
   alpn = ["mqtt-quic"]
   max_idle_timeout_seconds = 360
   max_ack_delay_ms = 25
   ```
2. QUIC retry tokens are valid for 60s; path migration triggers routing-epoch validation.

## Shared Subscriptions
- Use `$share/group/topic` filters; the topic PRG owning the topic distributes round-robin by default.

## CP-Driven Rebalance
- Update `tenant_prg_count` via CP; session routing caches will fence stale epochs and disconnect with `PERMANENT_EPOCH` after grace.
***
