#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

HOST="${RAFT_CPP_INTEROP_HOST:-127.0.0.1}"
PORT="${RAFT_CPP_INTEROP_PORT:-11081}"
PEER_ID="${RAFT_CPP_INTEROP_PEER_ID:-cpp-node}"
TERM="${RAFT_CPP_INTEROP_TERM:-3}"
LAST_LOG_INDEX="${RAFT_CPP_INTEROP_LAST_LOG_INDEX:-7}"
LAST_LOG_TERM="${RAFT_CPP_INTEROP_LAST_LOG_TERM:-3}"

if [[ -n "${RAFT_CPP_BIN:-}" ]]; then
  cpp_bin="${RAFT_CPP_BIN}"
elif [[ -x "build/raft_cpp_smoke" ]]; then
  cpp_bin="build/raft_cpp_smoke"
elif [[ -x "/tmp/raft-cpp-build/raft_cpp_smoke" ]]; then
  cpp_bin="/tmp/raft-cpp-build/raft_cpp_smoke"
else
  echo "Could not find raft_cpp_smoke. Set RAFT_CPP_BIN or build the C++ experiment first." >&2
  exit 1
fi

server_log="${TMPDIR:-/tmp}/raft-cpp-interop-server.log"

"${cpp_bin}" serve-stateful "${HOST}" "${PORT}" "${PEER_ID}" "${TERM}" "${LAST_LOG_INDEX}" "${LAST_LOG_TERM}" >"${server_log}" 2>&1 &
server_pid=$!

cleanup() {
  kill "${server_pid}" >/dev/null 2>&1 || true
  wait "${server_pid}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

for _ in $(seq 1 20); do
  if grep -q "server listening" "${server_log}" 2>/dev/null; then
    break
  fi
  sleep 0.2
done

echo "==> Java vote-request -> C++"
./run-java-probe.sh vote-request "${HOST}" "${PORT}" java-candidate "${LAST_LOG_INDEX}" "${LAST_LOG_TERM}" "${TERM}"

echo
echo "==> Java append-entries -> C++"
./run-java-probe.sh append-entries "${HOST}" "${PORT}" java-leader "${LAST_LOG_INDEX}" "${LAST_LOG_TERM}" "${LAST_LOG_INDEX}" "${TERM}"

echo
echo "==> Java install-snapshot -> C++"
./run-java-probe.sh install-snapshot "${HOST}" "${PORT}" java-leader "${LAST_LOG_INDEX}" "${LAST_LOG_TERM}" "${TERM}"

echo
echo "==> C++ server log"
cat "${server_log}"

echo
echo "Interop smoke passed."
