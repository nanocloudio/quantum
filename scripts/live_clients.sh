#!/usr/bin/env bash
# Live protocol tests for Quantum's messaging clients — proves each client works
# STANDALONE (no chronicle) against a REAL broker.
#
#   ./scripts/live_clients.sh            # all
#   ./scripts/live_clients.sh nats amqp  # a subset
set -u
cd "$(dirname "$0")/.."
FLX=${FLUXOR_BIN:-fluxor}
pass=0
fail=0
ok() { echo "  PASS  $1"; pass=$((pass + 1)); }
no() { echo "  FAIL  $1: $2"; fail=$((fail + 1)); }

# NOTE: the payload is the printf FORMAT string, not a %b argument — bash's
# `%b` does not interpret \xHH, which binary protocol frames need.
# LEAK GUARD: `fluxor run` spawns fluxor-linux as a CHILD; a timeout that
# kills only the parent orphans the child, which runs its scheduler loop
# forever holding a ~100 MB arena. So: background the parent, wait bounded,
# then reap the CHILD FIRST (pkill -P only works while the parent lives) —
# and sweep any survivor from THIS repo's target dir on exit.
trap 'pkill -KILL -f "$PWD/target/.*fluxor-linux" 2>/dev/null' EXIT
run_graph() {
  local t pid i out
  t=$(mktemp)
  printf "${2-}" | "$FLX" run "$1" >"$t" 2>/dev/null &
  pid=$!
  i=$(( ${3:-10} * 2 ))
  while kill -0 "$pid" 2>/dev/null && [ "$i" -gt 0 ]; do sleep 0.5; i=$((i - 1)); done
  pkill -TERM -P "$pid" 2>/dev/null # the fluxor-linux child, first
  kill "$pid" 2>/dev/null
  wait "$pid" 2>/dev/null
  out=$(cat "$t")
  rm -f "$t"
  printf '%s' "${out##*config.bin$'\n'}"
}
want() { case "$1" in *"$2"*) return 0 ;; *) return 1 ;; esac }

kafka() {
  docker rm -f qtm-kafka >/dev/null 2>&1
  docker run -d --rm --name qtm-kafka -p 19092:9092 \
    -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
    -e KAFKA_LISTENERS='PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093' \
    -e KAFKA_ADVERTISED_LISTENERS='PLAINTEXT://127.0.0.1:19092' \
    -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
    -e KAFKA_CONTROLLER_QUORUM_VOTERS='1@localhost:9093' \
    -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP='CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT' \
    -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
    -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
    apache/kafka:3.7.0 >/dev/null 2>&1
  timeout 180 bash -c 'until docker exec qtm-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1; do sleep 3; done'
  # Topic listing succeeds before the GROUP COORDINATOR is serving, and the
  # client's JoinGroup/SyncGroup needs it — wait on the consumer-group API too.
  timeout 120 bash -c 'until docker exec qtm-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1; do sleep 3; done'
  local r
  r=$(run_graph examples/kafka_client/linux.yaml '' 25)
  if want "$r" "group member stable"; then
    ok "kafka  (ApiVersions -> FindCoordinator -> JoinGroup -> SyncGroup)"
  else
    no kafka "status='$r'"
  fi
  docker rm -f qtm-kafka >/dev/null 2>&1
}

amqp() {
  docker rm -f qtm-rabbit >/dev/null 2>&1
  docker run -d --rm --name qtm-rabbit -p 15672:5672 rabbitmq:3-alpine >/dev/null 2>&1
  # Probe the AMQP port itself: `rabbitmqctl status` reports the Erlang node up
  # before the 5672 listener accepts, which is not the readiness we need.
  timeout 120 bash -c 'until (exec 3<>/dev/tcp/127.0.0.1/15672) 2>/dev/null; do sleep 2; done'
  local r
  r=$(run_graph examples/amqp_client/linux.yaml)
  if want "$r" "channel 1 open"; then
    ok "amqp   (PLAIN auth handshake + Channel.Open)"
  else
    no amqp "status='$r'"
  fi
  docker rm -f qtm-rabbit >/dev/null 2>&1
}

nats() {
  docker rm -f qtm-nats >/dev/null 2>&1
  docker run -d --rm --name qtm-nats -p 14222:4222 nats:latest >/dev/null 2>&1
  timeout 30 bash -c 'until (exec 3<>/dev/tcp/127.0.0.1/14222) 2>/dev/null; do sleep 1; done'
  # The subscriber runs in the background; publish from the host once it is up
  # (the nats:latest image ships no shell, so speak the wire protocol directly).
  local log
  log=$(mktemp)
  (timeout 8 "$FLX" run examples/nats_client/linux.yaml </dev/null >"$log" 2>/dev/null) &
  local runner=$!
  timeout 10 bash -c "until grep -q 'config.bin' '$log' 2>/dev/null; do sleep 1; done"
  sleep 1
  python3 -c "
import socket, time
s = socket.create_connection(('127.0.0.1', 14222), 3); s.settimeout(2)
s.recv(1024)                                  # INFO
s.sendall(b'CONNECT {\"verbose\":false}\r\n')
time.sleep(0.2)
s.sendall(b'PUB orders.new 5\r\nhello\r\n')
time.sleep(0.3); s.close()" 2>/dev/null
  wait $runner 2>/dev/null
  # The inner timeout kills the fluxor CLI only — sweep its orphaned runtime now
  # rather than leaving it to the EXIT trap.
  pkill -KILL -f "$PWD/target/.*fluxor-linux" 2>/dev/null
  local out
  out=$(cat "$log")
  rm -f "$log"
  if want "${out##*config.bin$'\n'}" hello; then
    ok "nats   (SUB orders.> receives a published message)"
  else
    no nats "received='${out##*config.bin$'\n'}'"
  fi
  docker rm -f qtm-nats >/dev/null 2>&1
}

for t in ${*:-kafka amqp nats}; do "$t"; done
echo "== $pass passed, $fail failed =="
[ "$fail" -eq 0 ]
