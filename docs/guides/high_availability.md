# High Availability Guide

This note describes how Quantum achieves high availability on top of Clustor today, what must be added to reach the desired operating model, how to structure the front door (VIP / L4 load balancer), and the expected impact of rolling restarts on clients.

## What Already Exists
- **State durability & replay:** Every Partition Raft Group (PRG) is a three-voter Clustor partition; session state (`session_processor`), dedupe shards (`dedup_engine`), offline queues (`offline_queue`), retained payloads (`retained_store`), consumer-group state (`consumer_group_coordinator`), and transaction state (`transaction_coordinator`) all commit through Clustor's WAL + snapshots. Any node restart or crash replays deterministically once the node rejoins.
- **ACK / reason-code contract:** `ack_tracker` consumes durability proofs from `durability_ledger` directly and only then emits PUBACK / PUBREC / Kafka `ProduceResponse` / AMQP `Basic.Ack`. Client-visible QoS guarantees persist through node loss as long as any quorum survives.
- **Routing & placement authority:** CP-Raft owns tenant placement, routing epochs, and feature gates. `http_surface.readyz_path` reports green only when CP cache is fresh, PRGs have completed replay, and durability fences are clear, so a listener can be marked unhealthy before it accepts traffic.
- **Draining hooks:** Listeners stop admitting new connections when the runtime is signalled to drain; existing sessions stay live until their grace window expires. TLS material can be refreshed without restarting the process (the `tls` module re-reads on signal).

These behaviours are inherited from Clustor substrate modules (consensus, replay, durability ledger, strict-fallback) or implemented in the Quantum graph (`session_processor` lifecycle, `backpressure_propagator` drain signal translation, `http_surface` readiness reporting).

## Gaps to Close

| Area | Needs Quantum Work | Needs Clustor Work |
| --- | --- | --- |
| Automated placement hygiene | Add CP placement policies that prevent more than one replica of a PRG landing on the same node/rack; surface alerts from `metrics_aggregator` when the policy cannot be satisfied | None (substrate enforces replica fencing once placement is decided) |
| Load balancer integration | Publish drain status and strict-fallback state through `http_surface` in a form L4 health checks can consume; provide operator scripts to flip drain flags during rollout | None |
| Leader transfer before restart | Expose a batched `/admin` endpoint that requests Raft leader transfer for every PRG owned by a target node | The per-PRG leader-transfer primitive already exists in `admin_handler`; this is a wrapper |
| Rolling restart orchestration | Automate the sequence (drain → transfer → restart → re-admit) as an `ops/scripts/` driver; today it is documented but manual | None |
| Client signalling | Emit `metrics_aggregator` counters that distinguish intentional drain from unexpected failure so operators can correlate client reconnects | None |

Clustor already satisfies the durability and quorum semantics; the remaining work is Quantum-side orchestration and operator tooling.

## Front-Door Patterns

1. **Preferred: node-terminated TLS behind an L4 load balancer or VIP.**
   - Each node terminates TLS 1.3 + mTLS, keeping SNI/ALPN routing and client cert inspection unified with Clustor read-gate awareness.
   - An external L4 load balancer (or BGP-announced VIP) only multiplexes TCP; health checks call `/readyz` and listener-specific endpoints to remove nodes that are draining, in strict-fallback, or replaying state.
2. **Alternative: centralized TLS termination.**
   - Only choose this when certificates or DDoS controls must be centralized. Preserve end-to-end metadata (SNI, client cert chain) via TLS passthrough or re-encryption so listeners can enforce routing epochs and ACLs correctly.
   - The appliance must understand drain signals and Clustor safety states, or else it becomes a single point of failure that could forward traffic to nodes mid-replay.

Regardless of option, the front door must react to Quantum readiness (CP freshness + PRG readiness + fences) rather than basic TCP liveness, and it must be able to drain connections on demand.

## Rolling Restart Process

1. **Pre-checks**
   - Verify ControlPlaneRaft is healthy (`/v1/cluster/state`, `/readyz` ≥ 99 % PRGs ready).
   - Confirm load balancer automation is reachable and that spare node capacity exists.
2. **Leader transfer (optional but recommended)**
   - Ask Clustor to move PRG leadership away from the target node so writes stay available when it leaves. This is a metadata call per partition via CP tooling.
3. **Drain the listener**
   - Flip the runtime into drain mode via `http_surface` `/admin` (stop accepting new MQTT CONNECTs, keep existing sessions alive).
   - Remove the node from the L4/VIP pool once the drain flag is set; `/readyz` should fail immediately.
4. **Wait for client quiescence**
   - Watch the active-session gauge from `metrics_aggregator`; optionally send DISCONNECT with `Server unavailable` (issued by `session_processor`) to accelerate failover once a grace period expires.
5. **Restart the node**
   - `systemctl stop quantum`, replace `/opt/quantum/modules/*.fmod` and/or `bin/fluxor*` as needed, `systemctl start quantum`. Clustor's WAL + snapshots replay deterministically on the next start.
   - Track replay progress via per-PRG readiness gauges. The node must not rejoin the LB until `/readyz` is green and listener metrics report no replay backlog.
6. **Re-admit**
   - Clear drain mode and re-register the node at the L4/VIP. Allow the CP placement reconciler to move leaders back if needed.
7. **Post-checks**
   - Confirm no PRG remains in strict-fallback and that client reconnect rates return to baseline. Review telemetry for throttles or read-gate violations.

This sequence yields zero data loss and keeps write availability as long as two replicas of every PRG remain up.

## Client Experience During Restarts

- **Active sessions on the draining node:** Clients will see socket closure (FIN/RST) or explicit DISCONNECT once the grace window ends. MQTT guidance already requires clients to reconnect/retry, so they should immediately reconnect to another hostname/IP from the LB pool.
- **QoS guarantees:** Because the session/forward state is committed through Clustor, retransmitted PUBLISH/PUBREL packets are detected as duplicates; QoS 1/2 messages remain exactly-once inside the broker. Clients may see at-least-once delivery during in-flight retries, which aligns with MQTT expectations.
- **Latency impact:** Reconnect handshake adds one TLS + MQTT CONNECT round trip. After reconnect, the client resumes anywhere in the world because PRGs replay prior state before the node is marked ready. There is no need for client-level session migration steps.
- **Backoff expectations:** Encourage clients to implement exponential backoff with jitter to avoid thundering herds, but typical reconnect after single-node drain is near-instant because other nodes stay healthy.
- **MQTT 5 session migration:** MQTT 5 still requires a new network connection to move a session; the broker can only hint at a new endpoint via `Server Reference` when it closes the existing link. There is no in-protocol way to migrate an active session/stateful transport without a reconnect, so the HA plan relies on clients honoring the reconnect/retry guidance.

When the front door honors readiness/drain status, the only client-visible effect of a rolling restart is a short reconnect; the data plane maintains durability throughout thanks to Clustor.
