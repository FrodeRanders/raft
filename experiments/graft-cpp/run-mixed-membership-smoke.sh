#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

HOST="${RAFT_CPP_MIXED_HOST:-127.0.0.1}"
JAVA_PORT="${RAFT_CPP_MIXED_JAVA_PORT:-11781}"
CPP_VOTER_PORT="${RAFT_CPP_MIXED_CPP_VOTER_PORT:-11782}"
CPP_LEARNER_PORT="${RAFT_CPP_MIXED_CPP_LEARNER_PORT:-11783}"
JAVA_PEER_ID="${RAFT_CPP_MIXED_JAVA_PEER_ID:-java-peer}"
CPP_VOTER_ID="${RAFT_CPP_MIXED_CPP_VOTER_ID:-cpp-voter}"
CPP_LEARNER_ID="${RAFT_CPP_MIXED_CPP_LEARNER_ID:-cpp-learner}"
JAVA_TIMEOUT_MS="${RAFT_CPP_MIXED_JAVA_TIMEOUT_MS:-10000}"

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

tmp_root="${TMPDIR:-/tmp}/graft-cpp-mixed-membership-smoke"
java_data_dir="${tmp_root}/java-data"
cpp_voter_state="${tmp_root}/cpp-voter.state"
cpp_learner_state="${tmp_root}/cpp-learner.state"
java_log="${tmp_root}/java-peer.log"
cpp_voter_log="${tmp_root}/cpp-voter.log"
cpp_learner_log="${tmp_root}/cpp-learner.log"

rm -rf "${tmp_root}"
mkdir -p "${java_data_dir}"

cleanup() {
  kill "${java_pid:-}" >/dev/null 2>&1 || true
  kill "${cpp_voter_pid:-}" >/dev/null 2>&1 || true
  kill "${cpp_learner_pid:-}" >/dev/null 2>&1 || true
  wait "${java_pid:-}" >/dev/null 2>&1 || true
  wait "${cpp_voter_pid:-}" >/dev/null 2>&1 || true
  wait "${cpp_learner_pid:-}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

"${cpp_bin}" serve-persistent \
  "${HOST}" "${CPP_VOTER_PORT}" "${CPP_VOTER_ID}" "${cpp_voter_state}" 0 0 0 \
  "${JAVA_PEER_ID}@${HOST}:${JAVA_PORT}" >"${cpp_voter_log}" 2>&1 &
cpp_voter_pid=$!

"${cpp_bin}" serve-persistent \
  "${HOST}" "${CPP_LEARNER_PORT}" "${CPP_LEARNER_ID}" "${cpp_learner_state}" 0 0 0 \
  "${JAVA_PEER_ID}@${HOST}:${JAVA_PORT}" >"${cpp_learner_log}" 2>&1 &
cpp_learner_pid=$!

./run-java-peer.sh \
  "${JAVA_PEER_ID}" "${HOST}" "${JAVA_PORT}" "${JAVA_TIMEOUT_MS}" "${java_data_dir}" \
  "${CPP_VOTER_ID}@${HOST}:${CPP_VOTER_PORT}" >"${java_log}" 2>&1 &
java_pid=$!

for _ in $(seq 1 40); do
  if telemetry_output="$(./run-java-probe.sh telemetry "${HOST}" "${JAVA_PORT}" true 2>/dev/null)"; then
    if grep -q "state: LEADER" <<<"${telemetry_output}" && grep -q "success: true" <<<"${telemetry_output}"; then
      break
    fi
  fi
  sleep 0.5
done

echo "==> Join C++ learner through Java leader"
join_output="$(./run-java-probe.sh join-cluster "${HOST}" "${JAVA_PORT}" "${CPP_LEARNER_ID}@${HOST}:${CPP_LEARNER_PORT}/learner" mixed-java-client)"
echo "${join_output}"
grep -q "success: true" <<<"${join_output}"

echo
echo "==> Join status after admission"
join_status_output="$(./run-java-probe.sh join-status "${HOST}" "${JAVA_PORT}" "${CPP_LEARNER_ID}" mixed-java-client)"
echo "${join_status_output}"
grep -Eq "status: (PENDING|IN_JOINT_CONSENSUS|COMPLETED)" <<<"${join_status_output}"

echo
echo "==> Java client-put while learner is non-voting"
put_output="$(./run-java-probe.sh client-put "${HOST}" "${JAVA_PORT}" k v1 mixed-java-client)"
echo "${put_output}"
grep -q "success: true" <<<"${put_output}"

echo
echo "==> C++ learner catches up before promotion"
for _ in $(seq 1 20); do
  learner_dump="$("${cpp_bin}" dump-state "${cpp_learner_state}" 2>/dev/null || true)"
  if grep -q "kv\\[k\\]=v1" <<<"${learner_dump}"; then
    break
  fi
  sleep 0.5
done
echo "${learner_dump}"
grep -q "kv\\[k\\]=v1" <<<"${learner_dump}"

echo
echo "==> Promote learner through Java leader"
promote_output="$(./run-java-probe.sh reconfigure "${HOST}" "${JAVA_PORT}" promote "${CPP_LEARNER_ID}@${HOST}:${CPP_LEARNER_PORT}/learner")"
echo "${promote_output}"
grep -q "success: true" <<<"${promote_output}"

echo
echo "==> Wait for promoted learner to appear as voter"
for _ in $(seq 1 30); do
  summary_output="$(./run-java-probe.sh cluster-summary "${HOST}" "${JAVA_PORT}" 2>/dev/null || true)"
  if grep -q "member\\[${CPP_LEARNER_ID}\\].*voting=true.*role=VOTER" <<<"${summary_output}"; then
    break
  fi
  sleep 0.5
done
echo "${summary_output}"
grep -q "member\\[${CPP_LEARNER_ID}\\].*voting=true.*role=VOTER" <<<"${summary_output}"

echo
echo "==> Finalize promotion joint consensus"
finalize_output="$(./run-java-probe.sh reconfigure "${HOST}" "${JAVA_PORT}" finalize)"
echo "${finalize_output}"
grep -q "success: true" <<<"${finalize_output}"

echo
echo "==> Join status after finalize"
for _ in $(seq 1 20); do
  final_join_status_output="$(./run-java-probe.sh join-status "${HOST}" "${JAVA_PORT}" "${CPP_LEARNER_ID}" mixed-java-client 2>/dev/null || true)"
  if grep -q "status: COMPLETED" <<<"${final_join_status_output}"; then
    break
  fi
  sleep 0.5
done
echo "${final_join_status_output}"
grep -q "status: COMPLETED" <<<"${final_join_status_output}"

echo
echo "==> Java client-put after promotion"
put_output_after="$(./run-java-probe.sh client-put "${HOST}" "${JAVA_PORT}" k2 v2 mixed-java-client)"
echo "${put_output_after}"
grep -q "success: true" <<<"${put_output_after}"

echo
echo "==> C++ learner state after promotion"
for _ in $(seq 1 20); do
  learner_dump_after="$("${cpp_bin}" dump-state "${cpp_learner_state}" 2>/dev/null || true)"
  if grep -q "kv\\[k2\\]=v2" <<<"${learner_dump_after}"; then
    break
  fi
  sleep 0.5
done
echo "${learner_dump_after}"
grep -q "kv\\[k\\]=v1" <<<"${learner_dump_after}"
grep -q "kv\\[k2\\]=v2" <<<"${learner_dump_after}"

echo
echo "==> Java telemetry after promotion"
telemetry_after="$(./run-java-probe.sh telemetry "${HOST}" "${JAVA_PORT}" true)"
echo "${telemetry_after}"
grep -q "state: LEADER" <<<"${telemetry_after}"

echo
echo "==> C++ voter log"
cat "${cpp_voter_log}"

echo
echo "==> C++ learner log"
cat "${cpp_learner_log}"

echo
echo "==> Java log"
cat "${java_log}"

echo
echo "Mixed Java/C++ membership smoke passed."
