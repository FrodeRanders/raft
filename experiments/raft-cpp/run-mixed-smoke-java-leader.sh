#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

HOST="${RAFT_CPP_MIXED_HOST:-127.0.0.1}"
JAVA_PORT="${RAFT_CPP_MIXED_JAVA_PORT:-11681}"
CPP_PORT="${RAFT_CPP_MIXED_CPP_PORT:-11682}"
CPP_PEER_ID="${RAFT_CPP_MIXED_CPP_PEER_ID:-cpp-node}"
JAVA_PEER_ID="${RAFT_CPP_MIXED_JAVA_PEER_ID:-java-peer}"
JAVA_TIMEOUT_MS="${RAFT_CPP_MIXED_JAVA_TIMEOUT_MS:-10000}"

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

tmp_root="${TMPDIR:-/tmp}/raft-cpp-mixed-smoke-java-leader"
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

"${cpp_bin}" serve-persistent \
  "${HOST}" "${CPP_PORT}" "${CPP_PEER_ID}" "${cpp_state}" 0 0 0 \
  "${JAVA_PEER_ID}@${HOST}:${JAVA_PORT}" >"${cpp_log}" 2>&1 &
cpp_pid=$!

./run-java-peer.sh \
  "${JAVA_PEER_ID}" "${HOST}" "${JAVA_PORT}" "${JAVA_TIMEOUT_MS}" "${java_data_dir}" \
  "${CPP_PEER_ID}@${HOST}:${CPP_PORT}" >"${java_log}" 2>&1 &
java_pid=$!

for _ in $(seq 1 40); do
  if telemetry_output="$(./run-java-probe.sh telemetry "${HOST}" "${JAVA_PORT}" true 2>/dev/null)"; then
    if grep -q "state: LEADER" <<<"${telemetry_output}" && grep -q "success: true" <<<"${telemetry_output}"; then
      break
    fi
  fi
  sleep 0.5
done

echo "==> Java client-put -> Java leader"
put_output="$(./run-java-probe.sh client-put "${HOST}" "${JAVA_PORT}" k v1 mixed-java-client)"
echo "${put_output}"
grep -q "success: true" <<<"${put_output}"
grep -Eq "status: (OK|ACCEPTED)" <<<"${put_output}"

echo
echo "==> Java client-get -> Java leader"
get_output="$(./run-java-probe.sh client-get "${HOST}" "${JAVA_PORT}" k mixed-java-client)"
echo "${get_output}"
grep -q "success: true" <<<"${get_output}"
grep -q "get.value: v1" <<<"${get_output}"

echo
echo "==> C++ dump-state -> C++ follower"
dump_output="$("${cpp_bin}" dump-state "${cpp_state}")"
echo "${dump_output}"
grep -q "peer_id: ${CPP_PEER_ID}" <<<"${dump_output}"
grep -Eq "commit_index: ([1-9][0-9]*)" <<<"${dump_output}"
grep -Eq "last_applied: ([1-9][0-9]*)" <<<"${dump_output}"
grep -q "kv\\[k\\]=v1" <<<"${dump_output}"

echo
echo "==> C++ client-get -> C++ follower"
set +e
cpp_follower_query="$("${cpp_bin}" client-get "${HOST}" "${CPP_PORT}" k mixed-cpp-client 2>&1)"
cpp_follower_query_status=$?
set -e
echo "${cpp_follower_query}"
test "${cpp_follower_query_status}" -eq 0 -o "${cpp_follower_query_status}" -eq 2
grep -Eq "status: (REDIRECT|NOT_LEADER)" <<<"${cpp_follower_query}"
grep -q "leader_id: ${JAVA_PEER_ID}" <<<"${cpp_follower_query}"
grep -q "leader_host: ${HOST}" <<<"${cpp_follower_query}"
grep -q "leader_port: ${JAVA_PORT}" <<<"${cpp_follower_query}"

echo
echo "==> Java telemetry -> Java leader"
telemetry_output="$(./run-java-probe.sh telemetry "${HOST}" "${JAVA_PORT}" true)"
echo "${telemetry_output}"
grep -q "state: LEADER" <<<"${telemetry_output}"
grep -q "leaderId: ${JAVA_PEER_ID}" <<<"${telemetry_output}"
grep -Eq "commitIndex: ([1-9][0-9]*)" <<<"${telemetry_output}"

echo
echo "==> C++ log"
cat "${cpp_log}"

echo
echo "==> Java log"
cat "${java_log}"

echo
echo "Mixed Java/C++ smoke (Java leader) passed."
