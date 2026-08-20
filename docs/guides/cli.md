# Quantum CLI Surface

There is no `quantum` binary. Quantum ships a graph of `.fmod`
modules that the `fluxor` and `fluxor-linux` binaries load and
execute. Operator workflows split across:

1. **The `fluxor` CLI** — dependency sync, module build, graph
   validation, launch.
2. **`systemctl` + the bundled systemd unit** — managed runtime
   lifecycle.
3. **Standard protocol clients** (`mosquitto_pub` /
   `mosquitto_sub`, Kafka and AMQP clients) — the broker speaks the
   standard wire protocols, so off-the-shelf tooling works.

## The fluxor CLI

```sh
fluxor sync                              # resolve fluxor.lock against the store;
                                         # materialise modules + runtime into target/
fluxor update                            # advance pins to the latest published digests
fluxor modules build --target bcm2712    # build quantum's .fmod artefacts
fluxor modules clean                     # remove built .fmod / .elf / .o
fluxor build --check <graph.yaml>        # validate a graph against module manifests
fluxor build <graph.yaml>                # pre-build the config + module wire blobs
fluxor run <graph.yaml>                  # validate, build blobs, exec fluxor-linux
fluxor run - <<'EOF' … EOF               # same, config from stdin
```

`fluxor build --check` validates one config per invocation: domain
assignments, port connectivity, arena sizing, scheduler tier
compatibility, and graph budgets.

`fluxor run` is the operator entry point: validate → build blobs →
exec `fluxor-linux`. The runtime stays in the foreground; logs go to
stderr.

## Managed runtime

For a production install ([deployment.md](deployment.md)):

```sh
sudo systemctl start quantum
sudo systemctl status quantum
journalctl -u quantum -f
sudo systemctl stop quantum
```

Swap graph configs by overriding the unit's
`Environment=QUANTUM_CONFIG=…`.

## Protocol clients

The broker binds one listener (default port 9090) and classifies
each connection from its first bytes, so all three protocols share
it. MQTT examples (validated against the run guide's graph):

```sh
# Subscribe
mosquitto_sub -h 127.0.0.1 -p 9090 -t 'sensors/#' -q 1

# Publish
mosquitto_pub -h 127.0.0.1 -p 9090 -t 'sensors/room1/temp' -m '23.5' -q 1

# Retained
mosquitto_pub -h 127.0.0.1 -p 9090 -t 'demo/retained' -q 1 -r -m 'payload'
```

Kafka clients complete the ApiVersions handshake and produce/consume
against the same port; AMQP 0-9-1 clients negotiate
`Connection.Start` there too. Protocol-level caveats (no Kafka
transactions, no AMQP exchange types beyond the default exchange)
are stated in the adapter references under
[../architecture/](../architecture/).

## Make targets

The Makefile is the lifecycle only; `make help` lists it. Build,
publish, and clean are thin aliases over the `fluxor` CLI.
