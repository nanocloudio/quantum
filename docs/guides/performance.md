# Performance & Load Validation

## Targets

The normative throughput floor and latency contract live in
[flow_control.md](../architecture/flow_control.md); the operator-facing
summary:

- **Per-PRG throughput floor (normative):** ≥500 QoS-2 publishes/s sustained under steady-state load; design cap 1,000 msg/s.
- **Missing metric names at the collector** on the larger graphs are the
  OTLP id-table cap, not a broken emitter — see
  [architecture/observability.md](../architecture/observability.md)
  "Export id-table budget".
- **In-AZ p99 server-side latency (normative):** ≤10 ms from edge ingress to WAL quorum commit, excluding client RTT. Single-node Linux at `tick_us: 1000` currently measures p50 3.4 / p99 4.7 ms on `module_graph_load.sh`. If you see that regress toward ~10 ms, check that the hot-path modules still return `StepOutcome::Burst` when they consume a record — without it every reply edge and every kernel-inserted tee/merge costs a full tick (see [architecture.md](../architecture.md) "Pipelining within a tick").
- **Per-PRG session density (planning target, non-normative):** 50k steady / 75k burst.

## Harnesses

| Harness | What it drives | What to watch |
|---|---|---|
| [tests/integration/module_graph_load.sh](../../tests/integration/module_graph_load.sh) | QoS-1 publishes through the full Raft pipeline; asserts every PUBACK returns. `CLIENTS=N MSGS_PER_CLIENT=M` overrides defaults. | PUBACK latency distribution, missing acks, listener TCP retransmits. |
| [tests/integration/qos1_test.py](../../tests/integration/qos1_test.py) | Single QoS-1 PUBLISH; asserts PUBACK with matching packet_id. Fast-path sanity check that the proposal → batch → WAL → fsync → durability → ack chain is alive. | Pass/fail. |
| [tests/integration/wal_durability_test.py](../../tests/integration/wal_durability_test.py) | 24 distinct clients across both partitions of `quantum-linux-2p.yaml`; asserts every WAL segment grew. | Per-partition WAL size; durability regressions where unwired downstream back-pressures the WAL writer. |
| [ops/scripts/chaos.sh publish-burst N=… M=…](../../ops/scripts/chaos.sh) | Synthetic publish flood across N messages × M topics from a single TCP client. | Throughput (pub/s), runtime resource use, queue depths. |
| [ops/scripts/chaos.sh connect-storm N=…](../../ops/scripts/chaos.sh) | N parallel CONNECTs. | Listener accept rate, CONNECT → CONNACK latency under load. |

For long-running real-load testing, drive the running graph with external generators (`mqtt-storm`, Paho flood, `kcat -P` for Kafka, `rabbitmq-perf-test` for AMQP) — Quantum speaks the standard wire protocols, so any off-the-shelf load tool works.

## Suggested procedure

1. Build modules for the target hardware (`fluxor modules build --target bcm2712 --out target` here; `make modules TARGET=bcm2712` in `clustor`).
2. Launch the graph: `fluxor run configs/quantum-linux.yaml` (or `quantum-pi5.yaml` on a Pi 5).
3. Confirm baseline health: `runtime_smoke.sh`, `/readyz` green, leader elected.
4. Pin the test tenant to one PRG via the graph YAML (or CP bootstrap seeds).
5. Run a publish flood at the target rate for ≥5 minutes per QoS level (0, 1, 2). Suggested: `chaos.sh publish-burst N=300000 M=10` per 10-minute window for QoS-0 sustained.
6. Capture metrics from `gateway` (`/metrics`):
   - **Substrate (from `operations`):** `clustor_wal_fsync_seconds`, `clustor_apply_lag_seconds`, `clustor_replication_lag_bytes`, `clustor_commit_index`.
   - **Quantum (from `governance`'s telemetry component):** per-(tenant, protocol, prg) publish/subscribe/forward counts; publish-latency histogram; session-duration histogram; offline-queue depth; dedupe-shard utilisation.
   - **Backpressure (from `flow`'s backpressure component):** translated protocol signal counts (MQTT `0x97`, Kafka `THROTTLING_QUOTA_EXCEEDED`, AMQP `drain=true`); current queue-depth gauges (`commit_to_apply`, `apply_to_delivery`).
   - **Flow control (from `admission`):** entry credit headroom, byte credit headroom, PID error term, lag signal.
7. Validate ACKs never precede durability proofs — `flow`'s ack component consumes `durability.quorum_durable` directly, so this is structural, but exercise crash/restart to confirm replay reproduces inflight state.

## Backpressure validation

- Drive load past the configured envelope (`admission.entry_credit_max`, `admission.byte_credit_max_kib`).
- Expect QoS-0 drops once `flow`'s backpressure `apply_to_delivery_drop_qos0` threshold is crossed (default 5000).
- Expect MQTT `0x97 (Quota exceeded)` on QoS-1/2 once `commit_to_apply_pause_ack` is crossed (default 10000).
- Verify `flow`'s prefetch component reduces per-session credits when `apply_delivery_lag_threshold` is exceeded.
- Confirm the system unwinds cleanly when load drops — credits restored, backpressure signals cease, metric rates return to baseline.

## Validated behaviour

The minimal MQTT graph (`configs/quantum-linux-minimal.yaml`) boots and
serves real protocol traffic; `make test-mqtt-suite` exercises the
following end-to-end against a live graph. These are functional
validations — throughput and latency are measured per the procedure
above on the target hardware, not fixed here.

| Path | Status |
|---|---|
| MQTT 3.1.1 CONNECT → CONNACK | pass |
| MQTT QoS 1 PUBLISH → PUBACK (WAL → quorum → ack) | pass |
| MQTT QoS 2 four-phase (PUBLISH/PUBREC/PUBREL/PUBCOMP) | pass |
| SUBSCRIBE → cross-client PUBLISH delivery (`+`/`#` wildcards) | pass |
| MQTT 5 topic alias, user properties preserved end-to-end | pass |
| MQTT 5 `$share/<g>/<topic>` shared subscription (round-robin) | pass |
| MQTT 5 Receive Maximum (inflight cap honoured) | pass |
| MQTT 5 Will / Will Delay Interval (fire after delay; cancel on reconnect) | pass |
| Persistent-session resume across DISCONNECT/CONNECT | pass |
| Kafka ApiVersions handshake | pass |

As a Linux dev-host orientation point (BCM2712 / Cortex-A76,
`fluxor-linux` bound to `127.0.0.1:9090`), CONNECT→CONNACK and QoS 1
PUBLISH→PUBACK round-trips land in the high-single-digit-millisecond
range. These exercise the Quantum modules plus the Linux platform stack
(`linux_net`, `linux_fs`); they are not the production surface — the
bare-metal kernel ([bring_up.md](bring_up.md)) is. Treat dev-host
numbers as a smoke reference and capture real figures on the target
per the procedure above.

## Reporting

- Capture p99 PUBACK latency and p99 fsync time per QoS level.
- Capture per-PRG sustained pub/s and the QoS profile that produced it.
- Regressions ≥10% on either p99 latency or sustained throughput block release.
- Note the target hardware (Pi 5 bare-metal vs. Linux host), the config `target:` value, and Fluxor/Clustor commit hashes alongside results.

## Crash / replay validation

After a sustained run:

1. `kill -9` the runtime (or `systemctl stop quantum`).
2. Restart it; watch `/readyz` flip from red → green as Clustor replays.
3. Reconnect the test client with `clean_start=false`; verify retained messages, offline-queue drain, and inflight QoS-2 state all reproduce.
4. Failure here points at WAL frame encoding, snapshot determinism, or `session_processor` apply-loop bugs — drop into module-level traces to localise.
