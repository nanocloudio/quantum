# Quantum CLI

The `quantum` binary provides diagnostic and operational subcommands modeled
after kcat/kafkacat for quick MQTT broker interaction.

## Commands

```
quantum start      Start the Quantum broker
quantum init       Seed workload fixtures (dev/CI)
quantum subscribe  Stream messages from topics (kcat -C style)
quantum publish    Send messages to topics (kcat -P style)
quantum inspect    WAL and storage inspection
quantum snapshot   Snapshot management
quantum workload   Schema tooling
quantum telemetry  Telemetry replay/fetch
quantum simulator  Overload simulation
quantum chaos      Chaos scenario planning
quantum synthetic  Synthetic probe generation
```

## Subscribe Command

Stream MQTT messages to stdout as newline-delimited JSON. Runs continuously
until Ctrl+C or SIGTERM, reconnecting automatically on broker disconnect.

```shell
quantum subscribe \
  --host 127.0.0.1 \
  --port 8883 \
  --cert client.pem \
  --key client-key.pem \
  --ca ca.pem \
  --topic "sensors/+/temperature,alerts/#"
```

### Options

| Flag | Default | Description |
|------|---------|-------------|
| `--host` | `127.0.0.1` | Broker hostname or IP |
| `--port` | `8883` | Broker TLS port |
| `--cert` | required | PEM file with client certificate chain |
| `--key` | required | PEM file with client private key |
| `--ca` | required | PEM file with broker CA certificate |
| `--topic` | required | Comma-separated topic filters |
| `--qos` | `qos1` | QoS level: `qos0`, `qos1`, `qos2` |
| `--format` | `json` | Output format: `json`, `raw` |
| `--client-id` | auto | MQTT client identifier |

### Output Format

JSON mode emits one object per line:

```json
{"ts":"2024-01-15T10:30:00.123Z","topic":"sensors/room1/temperature","qos":1,"payload":"23.5"}
```

For binary payloads that are not valid UTF-8, the payload is base64-encoded:

```json
{"ts":"2024-01-15T10:30:00.456Z","topic":"sensors/room1/raw","qos":1,"payload_b64":"AQID"}
```

Raw mode prints only the payload bytes, useful for binary pipelines.

### Behavior

- Connects via MQTT 5 with mTLS client authentication
- Subscribes to all specified topics after connection
- Reconnects with exponential backoff (100ms to 30s) on disconnect
- Exits non-zero on TLS or authentication failure
- Logs connection events to stderr, keeping stdout clean for piping

## Publish Command

Send MQTT messages from the command line or stdin.

### Single-Topic Mode

Publish a single message when `--topic` is specified:

```shell
# Message from command line
quantum publish \
  --host 127.0.0.1 --port 8883 \
  --cert client.pem --key client-key.pem --ca ca.pem \
  --topic "sensors/room1/temperature" \
  --message "23.5"

# Message from stdin (entire stdin becomes payload)
echo -n "23.5" | quantum publish \
  --host 127.0.0.1 --port 8883 \
  --cert client.pem --key client-key.pem --ca ca.pem \
  --topic "sensors/room1/temperature"
```

### Multi-Topic Stdin Mode

Stream multiple messages when `--topic` is omitted. Each line is parsed as
`topic<delimiter>payload`:

```shell
# Default colon delimiter
cat <<EOF | quantum publish --cert ... --key ... --ca ...
sensors/room1/temperature:23.5
sensors/room2/temperature:24.1
alerts/fire:EVACUATE
EOF
```

### Options

| Flag | Default | Description |
|------|---------|-------------|
| `--host` | `127.0.0.1` | Broker hostname or IP |
| `--port` | `8883` | Broker TLS port |
| `--cert` | required | PEM file with client certificate chain |
| `--key` | required | PEM file with client private key |
| `--ca` | required | PEM file with broker CA certificate |
| `--topic` | optional | Target topic (enables single-topic mode) |
| `--message` | optional | Payload (requires `--topic`) |
| `--delimiter` | `:` | Field separator for multi-topic mode |
| `--qos` | `qos1` | QoS level: `qos0`, `qos1`, `qos2` |
| `--retain` | false | Set retain flag on messages |
| `--binary` | false | Decode payload as base64 (multi-topic mode) |
| `--format` | `json` | Output format for acks/errors |
| `--client-id` | auto | MQTT client identifier |

### Delimiter Options

The `--delimiter` flag supports escape sequences for topics containing colons:

| Value | Result | Use case |
|-------|--------|----------|
| `:` | Colon (default) | Simple topic names |
| `\t` | Tab character | Topics with colons |
| `\n` | Newline | Special protocols |
| `\r` | Carriage return | Special protocols |
| `0x1f` | Unit separator | Binary-safe delimiter |

Example with tab delimiter:

```shell
# Topics containing colons like "urn:device:123"
printf 'urn:device:123\t{"temp":23.5}\n' | quantum publish \
  --cert ... --key ... --ca ... \
  --delimiter '\t'
```

### Binary Payloads

Use `--binary` in multi-topic mode to send base64-encoded payloads:

```shell
# Send binary data
echo "sensors/raw:AQIDBA==" | quantum publish \
  --cert ... --key ... --ca ... \
  --binary
```

### Behavior

- Waits for PUBACK/PUBREC before reporting success (QoS 1/2)
- Malformed lines in multi-topic mode are logged to stderr and skipped
- Ctrl+C or SIGTERM triggers graceful shutdown
- Exits non-zero on TLS, auth, or publish failure

## TLS Configuration

All commands require mTLS with three PEM files:

1. **Client certificate** (`--cert`): Your client's X.509 certificate chain
2. **Client private key** (`--key`): RSA, EC, or PKCS#8 private key
3. **CA certificate** (`--ca`): The broker's CA for server verification

Generate test certificates:

```shell
# Using openssl (production)
openssl req -x509 -newkey rsa:4096 -keyout ca-key.pem -out ca.pem -days 365 -nodes
openssl req -newkey rsa:4096 -keyout client-key.pem -out client.csr -nodes
openssl x509 -req -in client.csr -CA ca.pem -CAkey ca-key.pem -out client.pem -days 365

# Using rcgen (dev/test, see examples/client)
cargo run --example gen_certs
```

### Common TLS Errors

| Error | Cause | Fix |
|-------|-------|-----|
| `no private key found` | Key file missing or wrong format | Check key file exists and is PEM-encoded |
| `certificate verify failed` | CA mismatch or expired cert | Verify CA matches broker's certificate |
| `handshake failure` | Protocol mismatch | Ensure broker accepts TLS 1.2+ |

## Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | TLS/connection error |
| 1 | Authentication failure |
| 1 | Publish failure (message rejected) |

## Comparison with kcat

| kcat | quantum | Notes |
|------|---------|-------|
| `kcat -C -b broker -t topic` | `quantum subscribe --topic topic` | Consume messages |
| `kcat -P -b broker -t topic` | `quantum publish --topic topic` | Produce messages |
| `-K:` (key delimiter) | `--delimiter ':'` | Field separator |
| `-X security.protocol=ssl` | `--cert/--key/--ca` | TLS configuration |
| `-o beginning` | N/A | MQTT has no offsets |

## Examples

### Monitor all sensor data

```shell
quantum subscribe \
  --cert client.pem --key client-key.pem --ca ca.pem \
  --topic "sensors/#" \
  --format json | jq .
```

### Publish test messages in a loop

```shell
for i in $(seq 1 100); do
  echo "test/counter:$i"
done | quantum publish \
  --cert client.pem --key client-key.pem --ca ca.pem
```

### Bridge to another system

```shell
quantum subscribe --cert ... --topic "source/#" --format raw | \
  while read -r payload; do
    curl -X POST -d "$payload" http://other-system/ingest
  done
```

### Retained message setup

```shell
quantum publish \
  --cert client.pem --key client-key.pem --ca ca.pem \
  --topic "config/version" \
  --message "1.2.3" \
  --retain
```
