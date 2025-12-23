# High Availability Guide

This note describes how Quantum achieves high availability on top of Clustor today, what must be added to reach the desired operating model, how to structure the front door (VIP / L4 load balancer), and the expected impact of rolling restarts on clients.

## What Already Exists
- **State durability & replay:** Every Partitioned Raft Group (PRG) is a three-voter Clustor partition; MQTT session, offline queue, and retained state live exclusively in Clustor WAL + snapshots. Any node restart or crash replays deterministically once the node rejoins the ring.
- **ACK / reason-code contract:** MQTT acknowledgements already ride the Clustor AckContract (quorum append → fsync → ledger proof). Client-visible QoS guarantees therefore persist through node loss or restart as long as any quorum survives.
- **Routing & placement authority:** ControlPlaneRaft owns tenant placement, routing epochs, and feature gates. Runtime `/readyz` wiring already checks CP cache freshness plus PRG readiness, so a listener can be marked unhealthy before it accepts traffic.
- **Draining hooks:** Listeners can be removed from service by stopping connection admission (drain flag) before the runtime fully shuts down. TLS configs are hot-reloadable, so TLS termination on the node is compatible with draining.

These behaviours already ship in Quantum because they are inherited directly from Clustor (consensus, replay, ack contracts, strict-fallback) or explicitly implemented in the runtime (readiness, drain controls, TLS reload).

## Gaps to Close

| Area | Needs Quantum Work | Needs Clustor Work |
| --- | --- | --- |
| Automated placement hygiene | Integrate CP placement policies that prevent more than one replica of a PRG landing on the same node/rack; expose alerts when the policy cannot be satisfied | None (Clustor already enforces replica fencing once placement is decided) |
| Load balancer integration | Publish `/readyz` / strict-fallback states and listener drain status in a form LB health checks can consume; provide scripts to flip drain flags during rollout | None |
| Leader transfer before restart | Add tooling in Quantum/CP to request Clustor leader transfer for affected PRGs prior to draining | Optional read-gate/transfer APIs already exist; may require exposing an operator-friendly batch endpoint |
| Rolling restart orchestration | Document and automate the sequence (drain → transfer → restart → re-admit). Provide CLI/API hooks to coordinate with L4/VIP | None |
| Client signaling | Emit metrics + logs that flag intentional drain vs. unexpected failure so operators can correlate client reconnects | None |

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
   - Flip the runtime into drain mode (stop accepting new MQTT CONNECTs, keep existing sessions alive).
   - Remove the node from the L4/VIP pool once the drain flag is set; health checks should fail immediately.
4. **Wait for client quiescence**
   - Watch MQTT metrics for dwindling active sessions; optionally send DISCONNECT with `Server unavailable` to accelerate failover once a grace period expires.
5. **Restart the node**
   - Stop the quantum process, apply the update/config, and start it again. Clustor storage replays WAL/snapshots automatically.
   - Track replay progress (PRG readiness metrics). The node must not rejoin the LB until `/readyz` and listener metrics report green.
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
