#!/usr/bin/env bash
# Chaos harness: drive the running Quantum graph with
# parameterized fault injection (link drops, latency, burst load).
#
# Modes:
#   connect-storm    Open N MQTT connections in parallel
#   publish-burst    Send N PUBLISH packets across M topics
#   slow-publisher   Send 1 packet every K ms for N rounds
#   topic-fanout     One publisher → N subscribers
#
# Requires the runtime to be running (start with `fluxor run configs/quantum-linux-minimal.yaml` first).

set -euo pipefail

MODE=${1:-help}
PORT=${PORT:-9090}
HOST=${HOST:-127.0.0.1}
N=${N:-100}
M=${M:-10}
K=${K:-100}

case "${MODE}" in
    help|--help|-h)
        sed -n '/^# Modes:/,/^# Requires/p' "$0"
        echo ""
        echo "Usage: $0 <mode> [N=count] [M=topics] [K=interval_ms]"
        exit 0
        ;;

    connect-storm)
        echo "==> opening ${N} concurrent MQTT connections"
        python3 - <<EOF
import socket, struct, threading, time

def conn(i):
    try:
        s = socket.create_connection(("${HOST}", ${PORT}), timeout=3)
        cid = f"chaos-{i}".encode()
        body = b"\x00\x04MQTT\x05\x02\x00\x3c\x00\x00" + struct.pack(">H", len(cid)) + cid
        s.send(b"\x10" + bytes([len(body)]) + body)
        time.sleep(0.5)
        s.close()
    except Exception as e:
        print(f"conn {i}: {e}")

threads = [threading.Thread(target=conn, args=(i,)) for i in range(${N})]
t0 = time.monotonic()
for t in threads: t.start()
for t in threads: t.join()
dt = time.monotonic() - t0
print(f"  ${N} connections completed in {dt:.2f}s ({${N}/dt:.0f} conn/s)")
EOF
        ;;

    publish-burst)
        echo "==> sending ${N} PUBLISH packets across ${M} topics"
        python3 - <<EOF
import socket, struct, time

s = socket.create_connection(("${HOST}", ${PORT}), timeout=3)
cid = b"chaos-burst"
cb = b"\x00\x04MQTT\x05\x02\x00\x3c\x00\x00" + struct.pack(">H", len(cid)) + cid
s.send(b"\x10" + bytes([len(cb)]) + cb)

t0 = time.monotonic()
for i in range(${N}):
    topic = f"chaos/topic-{i % ${M}}".encode()
    payload = f"msg-{i}".encode()
    body = struct.pack(">H", len(topic)) + topic + payload
    s.send(b"\x30" + bytes([len(body)]) + body)
dt = time.monotonic() - t0
print(f"  ${N} publishes in {dt*1000:.0f}ms ({${N}/dt:.0f} pub/s)")
s.close()
EOF
        ;;

    slow-publisher)
        echo "==> 1 PUBLISH every ${K}ms × ${N} rounds"
        python3 - <<EOF
import socket, struct, time

s = socket.create_connection(("${HOST}", ${PORT}), timeout=3)
cid = b"chaos-slow"
cb = b"\x00\x04MQTT\x05\x02\x00\x3c\x00\x00" + struct.pack(">H", len(cid)) + cid
s.send(b"\x10" + bytes([len(cb)]) + cb)

for i in range(${N}):
    topic = b"chaos/slow"
    payload = f"slow-{i}".encode()
    body = struct.pack(">H", len(topic)) + topic + payload
    s.send(b"\x30" + bytes([len(body)]) + body)
    print(f"  sent {i+1}/${N}")
    time.sleep(${K} / 1000.0)
s.close()
EOF
        ;;

    *)
        echo "Unknown mode: ${MODE}" >&2
        echo "Run: $0 help" >&2
        exit 1
        ;;
esac

echo "==> chaos run complete"
