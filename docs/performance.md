# Performance & Load Validation

- **Target:** 500 msg/s per PRG (design cap 1,000 msg/s) with Strict durability.
- **Harness:** use `tests/ops.rs::ack_contract_handles_burst_without_durability_fence` as a fast-path sanity check; extend with real load generators (e.g., `mqtt-storm`, `paho` flood) against a single PRG.
- **Suggested procedure:**
  1. Start quantum with telemetry enabled; pin tenant to 1 PRG.
  2. Run a publish flood (QoS1 and QoS2) at 500 msg/s for 5 minutes.
  3. Record metrics: `queue_depth_apply`, `queue_depth_delivery`, `apply_lag_seconds`, `replication_lag_seconds`, `leader_credit_hint`.
  4. Validate no ACKs precede durability fences; check WAL/snapshot replay by restarting and verifying retained/offline queues.
- **Backpressure:** verify Receive Maximum/credit hints adjust as replication lag increases; expect QoS0 drops if thresholds exceeded.
- **Reporting:** capture p99 latency and fsync time; regressions beyond +10 % should block release.
