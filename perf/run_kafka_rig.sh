#!/usr/bin/env bash
# Kafka produce bench driver (run on the driver host, NOT the DUT).
#
# Topology (mirrors lattice's rig methodology):
#   DUT:    Pi 5 at 192.168.1.9 running quantum-pi5-kafka-bench.yaml
#           (netbooted via `fluxor rig test --scenario
#            tests/hardware/quantum_pi5_kafka_bench.toml`), Kafka on :9092.
#   Driver: this host (192.168.1.10) running quantum-kafka-loadgen.
#
# Driver != DUT numbers are rig-trustworthy. Point HOST at 127.0.0.1 for
# a local smoke and the results are labelled HARNESS_BOUND-suspect by
# provenance (host == localhost).
#
# Usage:
#   perf/run_kafka_rig.sh [host [port]]        # default 192.168.1.9 9092
#   MATRIX="rate:conns:batch:vsize:acks ..." perf/run_kafka_rig.sh
#
# Each matrix point runs for DURATION (default 30 s) and lands a
# provenance-stamped JSON in perf/results/.

set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
HOST="${1:-192.168.1.9}"
PORT="${2:-9092}"
DURATION="${DURATION:-30}"
TOPIC="${TOPIC:-bench}"
OUTDIR="$ROOT/perf/results"
LOADGEN="$ROOT/target/release/quantum-kafka-loadgen"

# rate = produce requests/s per conn; offered msgs/s = rate*conns*batch.
# Default matrix: latency point, mid-throughput, batch throughput, and a
# ceiling probe. acks=-1 everywhere (quorum durability — the honest number).
# Record batches must stay under the substrate's ~2 KiB entry ceiling
# (session_processor KAFKA_MAX_RECORDS_BYTES = 1900): batch*(value+~14)+61.
MATRIX="${MATRIX:-100:2:1:64:-1 250:4:1:64:-1 100:4:16:64:-1 250:4:4:256:-1}"

if [ ! -x "$LOADGEN" ]; then
    echo "building loadgen..."
    (cd "$ROOT" && cargo build --release -p quantum-bench)
fi
mkdir -p "$OUTDIR"

SHA=$(git -C "$ROOT" rev-parse --short=12 HEAD 2>/dev/null || echo unknown)
DIRTY=$(git -C "$ROOT" diff --quiet 2>/dev/null && echo clean || echo dirty)
CFG="$ROOT/configs/quantum-pi5-kafka-bench.yaml"
CFG_SHA=$(sha256sum "$CFG" | cut -c1-12)

echo "== quantum kafka produce bench → $HOST:$PORT (quantum $SHA/$DIRTY, cfg $CFG_SHA)"

# Fail fast if the broker isn't answering the handshake.
"$LOADGEN" --host "$HOST" --port "$PORT" --topic "$TOPIC" --handshake-only >/dev/null \
    || { echo "FAIL: broker at $HOST:$PORT did not complete the Kafka handshake"; exit 1; }

for point in $MATRIX; do
    IFS=: read -r RATE CONNS BATCH VSIZE ACKS <<<"$point"
    ID="kafka-r${RATE}x${CONNS}-b${BATCH}-v${VSIZE}-a${ACKS}-${SHA}"
    OUT="$OUTDIR/$ID.json"
    echo "-- $ID (duration ${DURATION}s)"
    RESULT=$("$LOADGEN" \
        --host "$HOST" --port "$PORT" --topic "$TOPIC" \
        --rate "$RATE" --conns "$CONNS" --batch "$BATCH" \
        --value-size "$VSIZE" --acks "$ACKS" --duration "$DURATION")
    python3 - "$OUT" <<PYEOF
import json, sys
result = json.loads('''$RESULT''')
doc = {
    "schema": "quantum-kafka-perf/1",
    "run_id": "$ID",
    "quantum_sha": "$SHA",
    "tree": "$DIRTY",
    "config": "quantum-pi5-kafka-bench.yaml",
    "config_sha256_12": "$CFG_SHA",
    "dut": "$HOST:$PORT",
    "driver_is_dut_host": $( { [ "$HOST" = "127.0.0.1" ] || [ "$HOST" = "localhost" ]; } && echo True || echo False ),
    "result": result,
}
with open(sys.argv[1], "w") as f:
    json.dump(doc, f, indent=2)
print(f"   -> {sys.argv[1]}")
tail = result.get("produce_tail", {})
print(f"   p50={tail.get('p50_us','?')}us p99={tail.get('p99_us','?')}us "
      f"achieved={result.get('achieved_req_rate','?')}req/s "
      f"verdict={result.get('headroom_verdict','?')}")
PYEOF
    sleep 2
done
echo "== done — results in perf/results/"
