#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

HOST="${RAFT_CPP_MIXED_HOST:-127.0.0.1}"
CPP_PORT="${RAFT_CPP_MIXED_CPP_PORT:-11981}"
CPP_VOTER_PORT="${RAFT_CPP_MIXED_CPP_VOTER_PORT:-11982}"
JAVA_LEARNER_PORT="${RAFT_CPP_MIXED_JAVA_LEARNER_PORT:-11983}"
CPP_PEER_ID="${RAFT_CPP_MIXED_CPP_PEER_ID:-cpp-node}"
CPP_VOTER_ID="${RAFT_CPP_MIXED_CPP_VOTER_ID:-cpp-voter}"
JAVA_LEARNER_ID="${RAFT_CPP_MIXED_JAVA_LEARNER_ID:-java-learner}"
JAVA_TIMEOUT_MS="${RAFT_CPP_MIXED_JAVA_TIMEOUT_MS:-10000}"
CPP_TERM="${RAFT_CPP_MIXED_CPP_TERM:-4}"
CPP_LAST_LOG_INDEX="${RAFT_CPP_MIXED_CPP_LAST_LOG_INDEX:-7}"
CPP_LAST_LOG_TERM="${RAFT_CPP_MIXED_CPP_LAST_LOG_TERM:-3}"

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

tmp_root="${TMPDIR:-/tmp}/raft-cpp-mixed-membership-cpp-leader-smoke"
cpp_state="${tmp_root}/cpp-node.state"
cpp_voter_state="${tmp_root}/cpp-voter.state"
java_learner_data_dir="${tmp_root}/java-learner-data"
cpp_log="${tmp_root}/cpp-node.log"
cpp_voter_log="${tmp_root}/cpp-voter.log"
java_learner_log="${tmp_root}/java-learner.log"

rm -rf "${tmp_root}"
mkdir -p "${java_learner_data_dir}"

cleanup() {
  kill "${cpp_pid:-}" >/dev/null 2>&1 || true
  kill "${cpp_voter_pid:-}" >/dev/null 2>&1 || true
  kill "${java_learner_pid:-}" >/dev/null 2>&1 || true
  wait "${cpp_pid:-}" >/dev/null 2>&1 || true
  wait "${cpp_voter_pid:-}" >/dev/null 2>&1 || true
  wait "${java_learner_pid:-}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

"${cpp_bin}" serve-persistent \
  "${HOST}" "${CPP_VOTER_PORT}" "${CPP_VOTER_ID}" "${cpp_voter_state}" 0 0 0 \
  "${CPP_PEER_ID}@${HOST}:${CPP_PORT}" >"${cpp_voter_log}" 2>&1 &
cpp_voter_pid=$!

./run-java-peer.sh \
  "${JAVA_LEARNER_ID}" "${HOST}" "${JAVA_LEARNER_PORT}" "${JAVA_TIMEOUT_MS}" "${java_learner_data_dir}" \
  --join-seed "${CPP_PEER_ID}@${HOST}:${CPP_PORT}" >"${java_learner_log}" 2>&1 &
java_learner_pid=$!

"${cpp_bin}" serve-active-persistent \
  "${HOST}" "${CPP_PORT}" "${CPP_PEER_ID}" "${cpp_state}" "${CPP_TERM}" "${CPP_LAST_LOG_INDEX}" "${CPP_LAST_LOG_TERM}" \
  "${CPP_VOTER_ID}@${HOST}:${CPP_VOTER_PORT}" >"${cpp_log}" 2>&1 &
cpp_pid=$!

for _ in $(seq 1 40); do
  if telemetry_output="$("${cpp_bin}" telemetry "${HOST}" "${CPP_PORT}" true 2>/dev/null)"; then
    if grep -q "state: LEADER" <<<"${telemetry_output}" && grep -q "success: true" <<<"${telemetry_output}"; then
      break
    fi
  fi
  sleep 0.5
done

echo "==> Admit Java learner through C++ leader"
join_output="$("${cpp_bin}" join-cluster "${HOST}" "${CPP_PORT}" "${JAVA_LEARNER_ID}" "${HOST}" "${JAVA_LEARNER_PORT}" LEARNER mixed-cpp-client)"
echo "${join_output}"
grep -q "success: true" <<<"${join_output}"

echo
echo "==> Join status after admission"
for _ in $(seq 1 20); do
  join_status_output="$("${cpp_bin}" join-status "${HOST}" "${CPP_PORT}" "${JAVA_LEARNER_ID}" mixed-cpp-client 2>/dev/null || true)"
  if grep -Eq "status: (PENDING|IN_JOINT_CONSENSUS|COMPLETED)" <<<"${join_status_output}"; then
    break
  fi
  sleep 0.5
done
echo "${join_status_output}"
grep -Eq "status: (PENDING|IN_JOINT_CONSENSUS|COMPLETED)" <<<"${join_status_output}"

echo
echo "==> C++ client-put while Java learner is non-voting"
put_output="$("${cpp_bin}" client-put "${HOST}" "${CPP_PORT}" k v1 mixed-cpp-client)"
echo "${put_output}"
grep -q "success: true" <<<"${put_output}"

echo
echo "==> Java learner catches up before promotion"
for _ in $(seq 1 30); do
  learner_telemetry="$(./run-java-probe.sh telemetry "${HOST}" "${JAVA_LEARNER_PORT}" true 2>/dev/null || true)"
  if grep -q "state: FOLLOWER" <<<"${learner_telemetry}" && grep -Eq "lastApplied: ([89]|[1-9][0-9]+)" <<<"${learner_telemetry}"; then
    break
  fi
  sleep 0.5
done
echo "${learner_telemetry}"
grep -q "state: FOLLOWER" <<<"${learner_telemetry}"
grep -q "leaderId: ${CPP_PEER_ID}" <<<"${learner_telemetry}"
grep -Eq "lastApplied: ([89]|[1-9][0-9]+)" <<<"${learner_telemetry}"

echo
echo "==> Promote Java learner through C++ joint configuration"
joint_output="$("${cpp_bin}" reconfigure "${HOST}" "${CPP_PORT}" joint \
  "${CPP_VOTER_ID}@${HOST}:${CPP_VOTER_PORT}" \
  "${JAVA_LEARNER_ID}@${HOST}:${JAVA_LEARNER_PORT}")"
echo "${joint_output}"
grep -q "success: true" <<<"${joint_output}"

echo
echo "==> Wait for Java learner to observe joint configuration"
for _ in $(seq 1 30); do
  learner_summary="$("./run-java-probe.sh" cluster-summary "${HOST}" "${JAVA_LEARNER_PORT}" 2>/dev/null || true)"
  if grep -q "jointConsensus: true" <<<"${learner_summary}" || grep -q "member\\[${JAVA_LEARNER_ID}\\].*voting=true.*role=VOTER" <<<"${learner_summary}"; then
    break
  fi
  sleep 0.5
done
echo "${learner_summary}"
grep -Eq "jointConsensus: true|member\\[${JAVA_LEARNER_ID}\\].*voting=true.*role=VOTER" <<<"${learner_summary}"

echo
echo "==> Finalize C++ joint configuration"
finalize_output="$("${cpp_bin}" reconfigure "${HOST}" "${CPP_PORT}" finalize "${CPP_VOTER_ID}@${HOST}:${CPP_VOTER_PORT}")"
echo "${finalize_output}"
grep -q "success: true" <<<"${finalize_output}"

echo
echo "==> Join status after finalize"
for _ in $(seq 1 20); do
  final_join_status_output="$("${cpp_bin}" join-status "${HOST}" "${CPP_PORT}" "${JAVA_LEARNER_ID}" mixed-cpp-client 2>/dev/null || true)"
  if grep -q "status: COMPLETED" <<<"${final_join_status_output}"; then
    break
  fi
  sleep 0.5
done
echo "${final_join_status_output}"
grep -q "status: COMPLETED" <<<"${final_join_status_output}"

echo
echo "==> C++ client-put after Java learner promotion"
put_output_after="$("${cpp_bin}" client-put "${HOST}" "${CPP_PORT}" k2 v2 mixed-cpp-client)"
echo "${put_output_after}"
grep -q "success: true" <<<"${put_output_after}"

echo
echo "==> Java learner remains caught up after promotion"
for _ in $(seq 1 30); do
  learner_telemetry_after="$(./run-java-probe.sh telemetry "${HOST}" "${JAVA_LEARNER_PORT}" true 2>/dev/null || true)"
  if grep -Eq "lastApplied: (1[0-9]|[2-9][0-9]+)" <<<"${learner_telemetry_after}"; then
    break
  fi
  sleep 0.5
done
echo "${learner_telemetry_after}"
grep -q "state: FOLLOWER" <<<"${learner_telemetry_after}"
grep -q "leaderId: ${CPP_PEER_ID}" <<<"${learner_telemetry_after}"
grep -Eq "lastApplied: (1[0-9]|[2-9][0-9]+)" <<<"${learner_telemetry_after}"

echo
echo "==> Java learner redirects reads to the C++ leader"
learner_query_output="$(./run-java-probe.sh client-get "${HOST}" "${JAVA_LEARNER_PORT}" k mixed-java-client)"
echo "${learner_query_output}"
grep -Eq "status: (REDIRECT|NOT_LEADER)" <<<"${learner_query_output}"
grep -q "leaderId: ${CPP_PEER_ID}" <<<"${learner_query_output}"

echo
echo "==> C++ leader summary after Java learner promotion"
leader_summary="$("${cpp_bin}" cluster-summary "${HOST}" "${CPP_PORT}")"
echo "${leader_summary}"
grep -q "member\\[${JAVA_LEARNER_ID}\\].*voting=true" <<<"${leader_summary}"
grep -q "joint_consensus: false" <<<"${leader_summary}"

echo
echo "==> C++ leader log"
cat "${cpp_log}"

echo
echo "==> C++ voter log"
cat "${cpp_voter_log}"

echo
echo "==> Java learner log"
cat "${java_learner_log}"

echo
echo "Mixed Java/C++ membership smoke (C++ leader) passed."
