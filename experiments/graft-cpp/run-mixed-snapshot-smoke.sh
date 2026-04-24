#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

HOST="${RAFT_CPP_MIXED_HOST:-127.0.0.1}"
CPP_PORT="${RAFT_CPP_MIXED_CPP_PORT:-11881}"
JAVA_VOTER_PORT="${RAFT_CPP_MIXED_JAVA_VOTER_PORT:-11882}"
JAVA_LAGGED_PORT="${RAFT_CPP_MIXED_JAVA_LAGGED_PORT:-11883}"
CPP_PEER_ID="${RAFT_CPP_MIXED_CPP_PEER_ID:-cpp-node}"
JAVA_VOTER_ID="${RAFT_CPP_MIXED_JAVA_VOTER_ID:-java-voter}"
JAVA_LAGGED_ID="${RAFT_CPP_MIXED_JAVA_LAGGED_ID:-java-lagged}"
JAVA_TIMEOUT_MS="${RAFT_CPP_MIXED_JAVA_TIMEOUT_MS:-10000}"
CPP_TERM="${RAFT_CPP_MIXED_CPP_TERM:-4}"
CPP_LAST_LOG_INDEX="${RAFT_CPP_MIXED_CPP_LAST_LOG_INDEX:-7}"
CPP_LAST_LOG_TERM="${RAFT_CPP_MIXED_CPP_LAST_LOG_TERM:-3}"
CPP_REPLICATE_INTERVAL_MS="${RAFT_CPP_MIXED_CPP_REPLICATE_INTERVAL_MS:-500}"
CPP_SNAPSHOT_THRESHOLD="${RAFT_CPP_MIXED_CPP_SNAPSHOT_THRESHOLD:-2}"

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

tmp_root="${TMPDIR:-/tmp}/graft-cpp-mixed-snapshot-smoke"
cpp_state="${tmp_root}/cpp-node.state"
java_voter_data_dir="${tmp_root}/java-voter-data"
java_lagged_data_dir="${tmp_root}/java-lagged-data"
cpp_log="${tmp_root}/cpp-node.log"
java_voter_log="${tmp_root}/java-voter.log"
java_lagged_log="${tmp_root}/java-lagged.log"

rm -rf "${tmp_root}"
mkdir -p "${java_voter_data_dir}" "${java_lagged_data_dir}"

cleanup() {
  kill "${cpp_pid:-}" >/dev/null 2>&1 || true
  kill "${java_voter_pid:-}" >/dev/null 2>&1 || true
  kill "${java_lagged_pid:-}" >/dev/null 2>&1 || true
  wait "${cpp_pid:-}" >/dev/null 2>&1 || true
  wait "${java_voter_pid:-}" >/dev/null 2>&1 || true
  wait "${java_lagged_pid:-}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

./run-java-peer.sh \
  "${JAVA_VOTER_ID}" "${HOST}" "${JAVA_VOTER_PORT}" "${JAVA_TIMEOUT_MS}" "${java_voter_data_dir}" \
  "${CPP_PEER_ID}@${HOST}:${CPP_PORT}" \
  "${JAVA_LAGGED_ID}@${HOST}:${JAVA_LAGGED_PORT}" >"${java_voter_log}" 2>&1 &
java_voter_pid=$!

./run-java-peer.sh \
  "${JAVA_LAGGED_ID}" "${HOST}" "${JAVA_LAGGED_PORT}" "${JAVA_TIMEOUT_MS}" "${java_lagged_data_dir}" \
  "${CPP_PEER_ID}@${HOST}:${CPP_PORT}" \
  "${JAVA_VOTER_ID}@${HOST}:${JAVA_VOTER_PORT}" >"${java_lagged_log}" 2>&1 &
java_lagged_pid=$!

"${cpp_bin}" serve-active-persistent-workload \
  "${HOST}" "${CPP_PORT}" "${CPP_PEER_ID}" "${cpp_state}" \
  "${CPP_TERM}" "${CPP_LAST_LOG_INDEX}" "${CPP_LAST_LOG_TERM}" \
  "${CPP_REPLICATE_INTERVAL_MS}" "${CPP_SNAPSHOT_THRESHOLD}" \
  "${JAVA_VOTER_ID}@${HOST}:${JAVA_VOTER_PORT}" \
  "${JAVA_LAGGED_ID}@${HOST}:${JAVA_LAGGED_PORT}" >"${cpp_log}" 2>&1 &
cpp_pid=$!

for _ in $(seq 1 40); do
  if telemetry_output="$("${cpp_bin}" telemetry "${HOST}" "${CPP_PORT}" true 2>/dev/null)"; then
    if grep -q "state: LEADER" <<<"${telemetry_output}" && grep -q "success: true" <<<"${telemetry_output}"; then
      break
    fi
  fi
  sleep 0.5
done

echo "==> Commit mixed-language KV state before lagging one Java follower"
for i in 1 2 3; do
  put_output="$(./run-java-probe.sh client-put "${HOST}" "${CPP_PORT}" "pre${i}" "v${i}" mixed-java-client)"
  echo "${put_output}"
  grep -q "success: true" <<<"${put_output}"
done

echo
echo "==> Stop lagging Java follower while C++ leader keeps quorum with the live Java voter"
kill "${java_lagged_pid}" >/dev/null 2>&1 || true
wait "${java_lagged_pid}" >/dev/null 2>&1 || true
unset java_lagged_pid

echo
echo "==> Continue writes while auto-compaction runs on the C++ leader"
for i in 4 5 6 7 8; do
  put_output="$(./run-java-probe.sh client-put "${HOST}" "${CPP_PORT}" "post${i}" "v${i}" mixed-java-client)"
  echo "${put_output}"
  grep -q "success: true" <<<"${put_output}"
done

echo
echo "==> C++ leader telemetry after compaction window"
leader_telemetry="$("${cpp_bin}" telemetry "${HOST}" "${CPP_PORT}" true)"
echo "${leader_telemetry}"
grep -q "state: LEADER" <<<"${leader_telemetry}"
grep -q "member\\[${JAVA_VOTER_ID}\\]" <<<"${leader_telemetry}"

echo
echo "==> C++ leader persisted state after compaction window"
leader_dump="$("${cpp_bin}" dump-state "${cpp_state}")"
echo "${leader_dump}"
grep -Eq "snapshot_index: ([1-9][0-9]*)" <<<"${leader_dump}"

echo
echo "==> Restart lagging Java follower"
./run-java-peer.sh \
  "${JAVA_LAGGED_ID}" "${HOST}" "${JAVA_LAGGED_PORT}" "${JAVA_TIMEOUT_MS}" "${java_lagged_data_dir}" \
  "${CPP_PEER_ID}@${HOST}:${CPP_PORT}" \
  "${JAVA_VOTER_ID}@${HOST}:${JAVA_VOTER_PORT}" >"${java_lagged_log}" 2>&1 &
java_lagged_pid=$!

echo
echo "==> Wait for lagging Java follower to recover through snapshot-backed catch-up"
for _ in $(seq 1 60); do
  lagged_telemetry="$(./run-java-probe.sh telemetry "${HOST}" "${JAVA_LAGGED_PORT}" true 2>/dev/null || true)"
  if grep -Eq "snapshotIndex: ([1-9][0-9]*)" <<<"${lagged_telemetry}" && grep -Eq "lastApplied: (1[0-9]|[2-9][0-9]+)" <<<"${lagged_telemetry}"; then
    break
  fi
  sleep 0.5
done
echo "${lagged_telemetry}"
grep -q "state: FOLLOWER" <<<"${lagged_telemetry}"
grep -q "leaderId: ${CPP_PEER_ID}" <<<"${lagged_telemetry}"
grep -Eq "snapshotIndex: ([1-9][0-9]*)" <<<"${lagged_telemetry}"
grep -Eq "lastApplied: (1[0-9]|[2-9][0-9]+)" <<<"${lagged_telemetry}"

echo
echo "==> Java client-get through the C++ leader still sees the latest value"
get_output="$(./run-java-probe.sh client-get "${HOST}" "${CPP_PORT}" post8 mixed-java-client)"
echo "${get_output}"
grep -q "success: true" <<<"${get_output}"
grep -q "get.value: v8" <<<"${get_output}"

echo
echo "==> Lagged Java follower redirects reads to the C++ leader"
follower_query_output="$(./run-java-probe.sh client-get "${HOST}" "${JAVA_LAGGED_PORT}" pre1 mixed-java-client)"
echo "${follower_query_output}"
grep -Eq "status: (REDIRECT|NOT_LEADER)" <<<"${follower_query_output}"
grep -q "leaderId: ${CPP_PEER_ID}" <<<"${follower_query_output}"
grep -q "leaderHost: ${HOST}" <<<"${follower_query_output}"
grep -q "leaderPort: ${CPP_PORT}" <<<"${follower_query_output}"

echo
echo "==> C++ leader summary after lagged follower recovery"
leader_summary="$("${cpp_bin}" cluster-summary "${HOST}" "${CPP_PORT}")"
echo "${leader_summary}"
grep -q "member\\[${JAVA_LAGGED_ID}\\]" <<<"${leader_summary}"

echo
echo "==> C++ leader log"
cat "${cpp_log}"

echo
echo "==> Live Java voter log"
cat "${java_voter_log}"

echo
echo "==> Lagged Java follower log"
cat "${java_lagged_log}"

echo
echo "Mixed Java/C++ snapshot smoke passed."
