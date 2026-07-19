# Performance & Load Validation

## Targets

- **Per-PRG throughput floor (normative):** ≥500 QoS-2 publishes/s sustained under steady-state load (spec §12.1); design cap 1,000 msg/s.
- **In-AZ p99 server-side latency (normative):** ≤10 ms from edge ingress to WAL quorum commit, excluding client RTT (spec §2 **L1**).
- **Per-PRG session density (planning target, non-normative):** 50k steady / 75k burst (spec §12.2).

## Harnesses

| Harness | What it drives | What to watch |
|---|---|---|
| [tests/integration/module_graph_load.sh](../tests/integration/module_graph_load.sh) | QoS-1 publishes through the full Raft pipeline; asserts every PUBACK returns. `CLIENTS=N MSGS_PER_CLIENT=M` overrides defaults. | PUBACK latency distribution, missing acks, listener TCP retransmits. |
| [tests/integration/qos1_test.py](../tests/integration/qos1_test.py) | Single QoS-1 PUBLISH; asserts PUBACK with matching packet_id. Fast-path sanity check that the proposal → batch → WAL → fsync → durability → ack chain is alive. | Pass/fail. |
| [tests/integration/wal_durability_test.py](../tests/integration/wal_durability_test.py) | 24 distinct clients across both partitions of `quantum-linux-2p.yaml`; asserts every WAL segment grew. | Per-partition WAL size; durability regressions where unwired downstream back-pressures the WAL writer. |
| [ops/scripts/chaos.sh publish-burst N=… M=…](../ops/scripts/chaos.sh) | Synthetic publish flood across N messages × M topics from a single TCP client. | Throughput (pub/s), runtime resource use, queue depths. |
| [ops/scripts/chaos.sh connect-storm N=…](../ops/scripts/chaos.sh) | N parallel CONNECTs. | Listener accept rate, CONNECT → CONNACK latency under load. |

For long-running real-load testing, drive the running graph with external generators (`mqtt-storm`, Paho flood, `kcat -P` for Kafka, `rabbitmq-perf-test` for AMQP) — Quantum speaks the standard wire protocols, so any off-the-shelf load tool works.

## Suggested procedure

1. Build modules for the target hardware (`fluxor modules build --target bcm2712 --out target` here; `make modules TARGET=bcm2712` in `clustor`).
2. Launch the graph: `fluxor run configs/quantum-linux.yaml` (or `quantum-cm5.yaml` on a CM5).
3. Confirm baseline health: `runtime_smoke.sh`, `/readyz` green, leader elected.
4. Pin the test tenant to one PRG via the graph YAML (or CP bootstrap seeds).
5. Run a publish flood at the target rate for ≥5 minutes per QoS level (0, 1, 2). Suggested: `chaos.sh publish-burst N=300000 M=10` per 10-minute window for QoS-0 sustained.
6. Capture metrics from `http_surface` (`/metrics`):
   - **Substrate (from `telemetry_agg`):** `clustor_wal_fsync_seconds`, `clustor_apply_lag_seconds`, `clustor_replicator_lag_bytes`, `clustor_commit_index`.
   - **Quantum (from `metrics_aggregator`):** per-(tenant, protocol, prg) publish/subscribe/forward counts; publish-latency histogram; session-duration histogram; offline-queue depth; dedupe-shard utilisation.
   - **Backpressure (from `backpressure_propagator`):** translated protocol signal counts (MQTT `0x97`, Kafka `THROTTLING_QUOTA_EXCEEDED`, AMQP `drain=true`); current queue-depth gauges (`commit_to_apply`, `apply_to_delivery`).
   - **Flow control (from `flow_controller`):** entry credit headroom, byte credit headroom, PID error term, lag signal.
7. Validate ACKs never precede durability proofs — `ack_tracker` consumes `durability_ledger.quorum_durable` directly, so this is structural in the current build, but exercise crash/restart to confirm replay reproduces inflight state.

## Backpressure validation

- Drive load past the configured envelope (`flow_controller.entry_credit_max`, `byte_credit_max`).
- Expect QoS-0 drops once `backpressure_propagator.apply_to_delivery_drop_qos0` is crossed (default 5000).
- Expect MQTT `0x97 (Quota exceeded)` on QoS-1/2 once `commit_to_apply_pause_ack` is crossed (default 10000).
- Verify `prefetch_controller` reduces per-session credits when `apply_delivery_lag_threshold` is exceeded.
- Confirm the system unwinds cleanly when load drops — credits restored, backpressure signals cease, metric rates return to baseline.

## Reporting

- Capture p99 PUBACK latency and p99 fsync time per QoS level.
- Capture per-PRG sustained pub/s and the QoS profile that produced it.
- Regressions ≥10% on either p99 latency or sustained throughput block release.
- Note the target hardware (CM5 vs. bcm2712), `TARGET=` value, and Fluxor/Clustor commit hashes alongside results.

## Crash / replay validation

After a sustained run:

1. `kill -9` the runtime (or `systemctl stop quantum`).
2. Restart it; watch `/readyz` flip from red → green as Clustor replays.
3. Reconnect the test client with `clean_start=false`; verify retained messages, offline-queue drain, and inflight QoS-2 state all reproduce.
4. Failure here points at WAL frame encoding, snapshot determinism, or `session_processor` apply-loop bugs — drop into module-level traces to localise.
