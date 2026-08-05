# Quantum CLI Surface

There is no `quantum` binary. Quantum ships a graph of `.fmod` modules that the `fluxor` and `fluxor-linux` binaries load and execute. Operator and diagnostic workflows are split across:

1. **`fluxor` tool** — graph validation, build, launch.
2. **`systemctl` + the bundled systemd unit** — managed runtime lifecycle.
3. **`make` targets in this repo** — the build/test/lint/ci lifecycle plus `make test-mqtt-suite`.
4. **Scripts under `ops/scripts/` and `tests/integration/`** — chaos drivers and smoke harnesses.
5. **Standard MQTT clients** (`mosquitto_pub` / `mosquitto_sub` / Paho) for protocol interop.

---

## Runtime control

### Validate a graph YAML

```sh
fluxor validate configs/quantum-*.yaml
# or, for a single config:
fluxor validate configs/quantum-linux.yaml
```

Checks the YAML against current module manifests: domain assignments, port connectivity, arena sizing, scheduler tier compatibility, and `MAX_MODULES` / `MAX_GRAPH_EDGES` budgets.

### Build the binary blobs without running

```sh
fluxor build configs/quantum-node0.yaml
# emits the config + modules wire blobs under target/
```

Useful for CI / packaging — pre-builds the wire blobs that `fluxor run` would otherwise generate on launch.

### Launch the runtime

```sh
fluxor run configs/quantum-linux-minimal.yaml
```

`fluxor run` is the operator entry point: validate → build blobs → exec `fluxor-linux <config.bin> <modules.bin>`. The runtime stays in the foreground; logs go to stdout/stderr.

For a managed install:

```sh
sudo systemctl start quantum         # starts the service
sudo systemctl status quantum
journalctl -u quantum -f             # follow logs
sudo systemctl reload quantum        # graceful config reload (if supported by your unit override)
sudo systemctl stop quantum
```

Swap graph configs by editing the systemd unit's `Environment=QUANTUM_CONFIG=...`; see [deployment.md](deployment.md).

---

## Smoke and diagnostic harnesses

These live in [tests/integration/](../../tests/integration/) and require a built graph (`fluxor modules build --target … --out target` here; `make modules TARGET=…` in `clustor`).

| Script | What it does |
|---|---|
| `runtime_smoke.sh` | Launches the minimal graph in the background, verifies every module loaded, waits for Raft leader election, sends a probe MQTT 3.1.1 CONNECT, asserts CONNACK. Exit 0 = pass. |
| `multi_node.sh` | Pre-builds 3 node configs, spawns 3 `fluxor-linux` processes in separate working directories, asserts leader election + log replication across all three. |
| `module_graph_mqtt.sh` | Multi-protocol E2E: MQTT/AMQP/Kafka byte-for-byte handshake assertions against the running graph. First leg of `make test-mqtt-suite`. |
| `module_graph_load.sh` | Sustained QoS-1 load (`CLIENTS=2 MSGS_PER_CLIENT=10` default) asserting every PUBACK returns. Also runs inside `make test-mqtt-suite`. |
| `pubsub_test.py` | Stdlib-only Python: subscriber + publisher, asserts the subscriber sees the payload within a timeout. |
| `qos1_test.py` | Stdlib-only Python: publishes a QoS-1 message and asserts PUBACK with matching packet_id. Implies the full Raft → WAL → fsync → durability → apply → ack pipeline ran. |
| `wal_durability_test.py` | Runs against the 2-partition config; asserts every per-partition WAL segment grew, catching durability regressions where an unwired downstream port back-pressures the WAL writer. |

All Python harnesses speak just enough MQTT 3.1.1 on a raw TCP socket — no `paho-mqtt` dependency. Override host/port via `--host` / `--port` flags or `QUANTUM_HOST` / `QUANTUM_PORT` env vars (default `127.0.0.1:9090`).

---

## Chaos / fault injection

[ops/scripts/chaos.sh](../../ops/scripts/chaos.sh) drives the running broker with parameterised load patterns:

```sh
./ops/scripts/chaos.sh connect-storm N=500           # 500 concurrent MQTT CONNECTs
./ops/scripts/chaos.sh publish-burst N=10000 M=20    # 10k PUBLISHes spread across 20 topics
./ops/scripts/chaos.sh slow-publisher N=100 K=200    # 1 PUBLISH every 200ms × 100 rounds
./ops/scripts/chaos.sh topic-fanout                  # 1 publisher → N subscribers
./ops/scripts/chaos.sh help                          # full mode list
```

Defaults: `HOST=127.0.0.1`, `PORT=9090`. Override via env. Requires the runtime to be running.

---

## Protocol interop with off-the-shelf clients

Quantum speaks standard MQTT 3.1.1 / 5.0, Kafka, and AMQP 0-9-1 on the wire — use whatever client you already trust. For MQTT:

```sh
# Subscribe (Mosquitto)
mosquitto_sub -h 127.0.0.1 -p 9090 -t 'sensors/#' -q 1

# Publish (Mosquitto)
mosquitto_pub -h 127.0.0.1 -p 9090 -t 'sensors/room1/temp' -m '23.5' -q 1

# TLS / mTLS variants
mosquitto_sub -h <host> -p 8883 -t 'tenant/topic' -q 2 \
    --cafile certs/ca.pem --cert certs/client.pem --key certs/client.key
```

See [interop.md](interop.md) for the full Mosquitto / Paho test plan and the four scenarios the build is exercised against (TLS QoS 1/2 round-trip, QUIC resume, shared-subscription distribution, Will + retained).

---

## Repository `make` targets

The Makefile is the lifecycle only (`build` / `test` / `lint` / `ci` /
`publish` / `clean` — see `make help`) plus one composition,
`make test-mqtt-suite`. Everything else is the `fluxor` CLI or a
script invoked directly:

| Command | Purpose |
|---|---|
| `fluxor modules build --target bcm2712 --out target` | Build all Quantum `.fmod` artifacts |
| `fluxor modules build --all --out target` | Build for every supported target (bcm2712) |
| `fluxor modules clean` | Remove built `.fmod` / `.elf` / `.o` |
| `tests/integration/module_graph_mqtt.sh` | Multi-protocol E2E against a running graph |
| `tests/integration/module_graph_load.sh` | Sustained-load + backpressure E2E |
| `fluxor validate configs/quantum-*.yaml` | Validate every shipped graph YAML |
| `make help` | Print the lifecycle plus the CLI commands/scripts that are deliberately not targets |
