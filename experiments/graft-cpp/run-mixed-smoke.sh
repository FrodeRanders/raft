#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

HOST="${RAFT_CPP_MIXED_HOST:-127.0.0.1}"
JAVA_PORT="${RAFT_CPP_MIXED_JAVA_PORT:-11581}"
CPP_PORT="${RAFT_CPP_MIXED_CPP_PORT:-11582}"
CPP_PEER_ID="${RAFT_CPP_MIXED_CPP_PEER_ID:-cpp-node}"
JAVA_PEER_ID="${RAFT_CPP_MIXED_JAVA_PEER_ID:-java-peer}"
JAVA_TIMEOUT_MS="${RAFT_CPP_MIXED_JAVA_TIMEOUT_MS:-10000}"
CPP_TERM="${RAFT_CPP_MIXED_CPP_TERM:-4}"
CPP_LAST_LOG_INDEX="${RAFT_CPP_MIXED_CPP_LAST_LOG_INDEX:-7}"
CPP_LAST_LOG_TERM="${RAFT_CPP_MIXED_CPP_LAST_LOG_TERM:-3}"

if [[ -n "${RAFT_CPP_BIN:-}" ]]; then
  cpp_bin="${RAFT_CPP_BIN}"
elif [[ -x "build/graft_smoke" ]]; then
  cpp_bin="build/graft_smoke"
elif [[ -x "/tmp/graft-cpp-build/graft_smoke" ]]; then
  cpp_bin="/tmp/graft-cpp-build/graft_smoke"
else
  echo "Could not find graft_smoke. Set RAFT_CPP_BIN or build the C++ implementation first." >&2
  exit 1
fi

tmp_root="${TMPDIR:-/tmp}/graft-cpp-mixed-smoke"
java_data_dir="${tmp_root}/java-data"
cpp_state="${tmp_root}/cpp-node.state"
java_log="${tmp_root}/java-peer.log"
cpp_log="${tmp_root}/cpp-node.log"

rm -rf "${tmp_root}"
mkdir -p "${java_data_dir}"

cleanup() {
  kill "${java_pid:-}" >/dev/null 2>&1 || true
  kill "${cpp_pid:-}" >/dev/null 2>&1 || true
  wait "${java_pid:-}" >/dev/null 2>&1 || true
  wait "${cpp_pid:-}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

./run-java-peer.sh \
  "${JAVA_PEER_ID}" "${HOST}" "${JAVA_PORT}" "${JAVA_TIMEOUT_MS}" "${java_data_dir}" \
  "${CPP_PEER_ID}@${HOST}:${CPP_PORT}" >"${java_log}" 2>&1 &
java_pid=$!

"${cpp_bin}" serve-active-persistent \
  "${HOST}" "${CPP_PORT}" "${CPP_PEER_ID}" "${cpp_state}" "${CPP_TERM}" "${CPP_LAST_LOG_INDEX}" "${CPP_LAST_LOG_TERM}" \
  "${JAVA_PEER_ID}@${HOST}:${JAVA_PORT}" >"${cpp_log}" 2>&1 &
cpp_pid=$!

sleep 5

echo "==> Java client-put -> C++ leader"
put_output="$(./run-java-probe.sh client-put "${HOST}" "${CPP_PORT}" k v1 mixed-java-client)"
echo "${put_output}"
grep -q "success: true" <<<"${put_output}"
grep -q "status: OK" <<<"${put_output}"

echo
echo "==> Java client-get -> C++ leader"
get_output="$(./run-java-probe.sh client-get "${HOST}" "${CPP_PORT}" k mixed-java-client)"
echo "${get_output}"
grep -q "success: true" <<<"${get_output}"
grep -q "get.value: v1" <<<"${get_output}"

echo
echo "==> Java telemetry -> Java follower"
telemetry_output="$(./run-java-probe.sh telemetry "${HOST}" "${JAVA_PORT}")"
echo "${telemetry_output}"
grep -q "state: FOLLOWER" <<<"${telemetry_output}"
grep -q "leaderId: ${CPP_PEER_ID}" <<<"${telemetry_output}"
grep -Eq "commitIndex: ([89]|[1-9][0-9]+)" <<<"${telemetry_output}"
grep -Eq "lastApplied: ([89]|[1-9][0-9]+)" <<<"${telemetry_output}"

echo
echo "==> Java client-get -> Java follower"
follower_query_output="$(./run-java-probe.sh client-get "${HOST}" "${JAVA_PORT}" k mixed-java-client)"
echo "${follower_query_output}"
grep -Eq "status: (REDIRECT|NOT_LEADER)" <<<"${follower_query_output}"
grep -q "leaderId: ${CPP_PEER_ID}" <<<"${follower_query_output}"
grep -q "leaderHost: ${HOST}" <<<"${follower_query_output}"
grep -q "leaderPort: ${CPP_PORT}" <<<"${follower_query_output}"

echo
echo "==> Java cluster-summary -> Java follower"
summary_output="$(./run-java-probe.sh cluster-summary "${HOST}" "${JAVA_PORT}")"
echo "${summary_output}"
grep -Eq "status: (REDIRECT|NOT_LEADER)" <<<"${summary_output}"
grep -Eq "redirectLeaderId: ${CPP_PEER_ID}|leaderId: ${CPP_PEER_ID}" <<<"${summary_output}"

echo
echo "==> C++ log"
cat "${cpp_log}"

echo
echo "==> Java log"
cat "${java_log}"

echo
echo "Mixed Java/C++ smoke passed."
