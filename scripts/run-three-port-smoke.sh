#!/usr/bin/env bash
# Combined three-port smoke test: Rust ↔ Java ↔ C++
# Mirrors individual two-port suites and combines them into one.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
GRAFT_CPP_DIR="${REPO_ROOT}/graft-cpp"

HOST="${RAFT_MIXED_HOST:-127.0.0.1}"
JAVA_PORT="${RAFT_MIXED_JAVA_PORT:-12781}"
CPP_PORT="${RAFT_MIXED_CPP_PORT:-12782}"
RUST_PORT="${RAFT_MIXED_RUST_PORT:-12783}"
JAVA_PEER_ID="${RAFT_MIXED_JAVA_PEER_ID:-java-peer}"
CPP_PEER_ID="${RAFT_MIXED_CPP_PEER_ID:-cpp-node}"
RUST_PEER_ID="${RAFT_MIXED_RUST_PEER_ID:-rust-node}"
JAVA_TIMEOUT_MS="${RAFT_MIXED_JAVA_TIMEOUT_MS:-10000}"

# -- Locate binaries ---------------------------------------------------

if [[ -n "${RAFT_CPP_BIN:-}" ]]; then
  cpp_bin="${RAFT_CPP_BIN}"
elif [[ -x "${GRAFT_CPP_DIR}/build/graft_smoke" ]]; then
  cpp_bin="${GRAFT_CPP_DIR}/build/graft_smoke"
elif [[ -x "/tmp/graft-cpp-build/graft_smoke" ]]; then
  cpp_bin="/tmp/graft-cpp-build/graft_smoke"
else
  echo "Could not find graft_smoke. Set RAFT_CPP_BIN or build the C++ implementation." >&2
  exit 1
fi

if [[ -n "${RAFT_RUST_BIN:-}" ]]; then
  rust_bin="${RAFT_RUST_BIN}"
elif [[ -x "${REPO_ROOT}/graft-rust/target/debug/graft-kv" ]]; then
  rust_bin="${REPO_ROOT}/graft-rust/target/debug/graft-kv"
elif [[ -x "${REPO_ROOT}/graft-rust/target/release/graft-kv" ]]; then
  rust_bin="${REPO_ROOT}/graft-rust/target/release/graft-kv"
else
  echo "Could not find graft-kv binary. Set RAFT_RUST_BIN or build the Rust implementation." >&2
  exit 1
fi

# -- Prepare Java probe ------------------------------------------------

(cd "${GRAFT_CPP_DIR}" && ./prepare-java-probe.sh)

# Warm up: run a no-op Java probe call so Maven compilation is done before timing-sensitive sections.
run_java_probe telemetry "${HOST}" "0" true 2>/dev/null || true

run_java_probe() {
  "${GRAFT_CPP_DIR}/run-java-probe.sh" "$@"
}
run_java_peer() {
  "${GRAFT_CPP_DIR}/run-java-peer.sh" "$@"
}

# -- Shared helpers ----------------------------------------------------

start_java() {
  local peer_id="$1" port="$2" timeout_ms="$3" data_dir="$4" log_file="$5"
  shift 5
  run_java_peer \
    "${peer_id}" "${HOST}" "${port}" "${timeout_ms}" "${data_dir}" \
    "$@" >"${log_file}" 2>&1 &
}

start_cpp_follower() {
  local peer_id="$1" port="$2" state_file="$3" known_leader="$4" log_file="$5"
  "${cpp_bin}" serve-persistent \
    "${HOST}" "${port}" "${peer_id}" "${state_file}" 0 0 0 \
    "${known_leader}" >"${log_file}" 2>&1 &
}

start_cpp_leader() {
  local peer_id="$1" port="$2" state_file="$3" term="$4" last_log_index="$5" last_log_term="$6" log_file="$7"
  shift 7
  "${cpp_bin}" serve-active-persistent \
    "${HOST}" "${port}" "${peer_id}" "${state_file}" "${term}" "${last_log_index}" "${last_log_term}" \
    "$@" >"${log_file}" 2>&1 &
}

start_rust_leader() {
  local peer_id="$1" port="$2" state_file="$3" term="$4" last_log_index="$5" last_log_term="$6" log_file="$7"
  shift 7
  "${rust_bin}" serve-active-persistent \
    "${HOST}" "${port}" "${peer_id}" "${state_file}" \
    "${term}" "${last_log_index}" "${last_log_term}" \
    "$@" >"${log_file}" 2>&1 &
}

start_rust_follower() {
  local peer_id="$1" port="$2" known_leader="$3" log_file="$4"
  "${rust_bin}" serve-stateful \
    "${HOST}" "${port}" "${peer_id}" 0 0 0 \
    "${known_leader}" >"${log_file}" 2>&1 &
}

wait_for_leader_java() {
  local host="$1" port="$2" label="$3"
  for _ in $(seq 1 40); do
    if telemetry_output="$(run_java_probe telemetry "${host}" "${port}" true 2>/dev/null)"; then
      if grep -q "state: LEADER" <<<"${telemetry_output}" && grep -q "success: true" <<<"${telemetry_output}"; then
        return 0
      fi
    fi
    sleep 0.5
  done
  echo "Timed out waiting for ${label} leader at ${host}:${port}" >&2
  return 1
}

wait_for_leader_cpp() {
  local host="$1" port="$2" label="$3"
  for _ in $(seq 1 40); do
    if telemetry_output="$("${cpp_bin}" telemetry "${host}" "${port}" true 2>/dev/null)"; then
      if grep -q "state: LEADER" <<<"${telemetry_output}"; then
        return 0
      fi
    fi
    sleep 0.5
  done
  echo "Timed out waiting for ${label} leader at ${host}:${port}" >&2
  return 1
}

wait_for_leader_rust() {
  local host="$1" port="$2" label="$3"
  for _ in $(seq 1 40); do
    if telemetry_output="$("${rust_bin}" telemetry "${host}" "${port}" --require-leader-summary 2>/dev/null)"; then
      if grep -q "state: LEADER" <<<"${telemetry_output}"; then
        return 0
      fi
    fi
    sleep 0.5
  done
  echo "Timed out waiting for ${label} leader at ${host}:${port}" >&2
  return 1
}

# ======================================================================
# Scenario 1: Rust leader, Java + C++ followers
# ======================================================================
echo "==> Scenario 1: Rust leader, Java + C++ followers"

tmp_root="${TMPDIR:-/tmp}/three-port-smoke-1"
rm -rf "${tmp_root}"
mkdir -p "${tmp_root}"

rust_state="${tmp_root}/rust.state"
cpp_state="${tmp_root}/cpp.state"
java_data="${tmp_root}/java-data"
mkdir -p "${java_data}"

rust_log="${tmp_root}/rust.log"
cpp_log="${tmp_root}/cpp.log"
java_log="${tmp_root}/java.log"

cleanup() {
  kill "${java_pid:-}" >/dev/null 2>&1 || true
  kill "${cpp_pid:-}" >/dev/null 2>&1 || true
  kill "${rust_pid:-}" >/dev/null 2>&1 || true
  wait "${java_pid:-}" >/dev/null 2>&1 || true
  wait "${cpp_pid:-}" >/dev/null 2>&1 || true
  wait "${rust_pid:-}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

start_java     "${JAVA_PEER_ID}" "${JAVA_PORT}" "${JAVA_TIMEOUT_MS}" "${java_data}" "${java_log}" \
               "${RUST_PEER_ID}@${HOST}:${RUST_PORT}"
java_pid=$!

start_cpp_follower "${CPP_PEER_ID}" "${CPP_PORT}" "${cpp_state}" \
               "${RUST_PEER_ID}@${HOST}:${RUST_PORT}" "${cpp_log}"
cpp_pid=$!

# Give Java and C++ followers time to start before Rust leader sends votes.
sleep 8

start_rust_leader "${RUST_PEER_ID}" "${RUST_PORT}" "${rust_state}" 4 7 3 "${rust_log}" \
               "${JAVA_PEER_ID}@${HOST}:${JAVA_PORT}" \
               "${CPP_PEER_ID}@${HOST}:${CPP_PORT}"
rust_pid=$!

sleep 3
wait_for_leader_rust "${HOST}" "${RUST_PORT}" "Rust"

echo "==> Rust client-put -> Rust leader"
put="$("${rust_bin}" client-put "${HOST}" "${RUST_PORT}" k v1 three-port-client)"
echo "${put}"
grep -q "success: true" <<<"${put}"

echo
echo "==> Rust client-get -> Rust leader"
get="$("${rust_bin}" client-get "${HOST}" "${RUST_PORT}" k three-port-client)"
echo "${get}"
grep -q "get.value: v1" <<<"${get}"

echo
echo "==> Java telemetry -> Java follower (verify cross-replication)"
j_tel="$(run_java_probe telemetry "${HOST}" "${JAVA_PORT}" true)"
echo "${j_tel}"
grep -q "state: FOLLOWER" <<<"${j_tel}"
grep -q "leaderId: ${RUST_PEER_ID}" <<<"${j_tel}"

echo
echo "==> C++ dump-state -> C++ follower (verify cross-replication)"
cpp_dump="$("${cpp_bin}" dump-state "${cpp_state}")"
echo "${cpp_dump}"
grep -q "kv\[k\]=v1" <<<"${cpp_dump}"

echo
echo "==> Rust telemetry -> Rust leader"
r_tel="$("${rust_bin}" telemetry "${HOST}" "${RUST_PORT}" --require-leader-summary)"
echo "${r_tel}"
grep -q "state: LEADER" <<<"${r_tel}"
grep -Eq "last_applied: ([1-9][0-9]*)" <<<"${r_tel}"

cleanup
trap - EXIT

# ======================================================================
# Scenario 2: C++ leader, Java + Rust followers
# ======================================================================
echo
echo "==> Scenario 2: C++ leader, Java + Rust followers"

tmp_root="${TMPDIR:-/tmp}/three-port-smoke-2"
rm -rf "${tmp_root}"
mkdir -p "${tmp_root}"

cpp_state="${tmp_root}/cpp.state"
java_data="${tmp_root}/java-data"
mkdir -p "${java_data}"

rust_log="${tmp_root}/rust.log"
cpp_log="${tmp_root}/cpp.log"
java_log="${tmp_root}/java.log"

cleanup() {
  kill "${java_pid:-}" >/dev/null 2>&1 || true
  kill "${cpp_pid:-}" >/dev/null 2>&1 || true
  kill "${rust_pid:-}" >/dev/null 2>&1 || true
  wait "${java_pid:-}" >/dev/null 2>&1 || true
  wait "${cpp_pid:-}" >/dev/null 2>&1 || true
  wait "${rust_pid:-}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

start_java     "${JAVA_PEER_ID}" "${JAVA_PORT}" "${JAVA_TIMEOUT_MS}" "${java_data}" "${java_log}" \
               "${CPP_PEER_ID}@${HOST}:${CPP_PORT}"
java_pid=$!

start_rust_follower "${RUST_PEER_ID}" "${RUST_PORT}" \
               "${CPP_PEER_ID}@${HOST}:${CPP_PORT}" "${rust_log}"
rust_pid=$!

# Give followers time to start before C++ leader sends votes.
sleep 8

start_cpp_leader "${CPP_PEER_ID}" "${CPP_PORT}" "${cpp_state}" 4 7 3 "${cpp_log}" \
               "${JAVA_PEER_ID}@${HOST}:${JAVA_PORT}" \
               "${RUST_PEER_ID}@${HOST}:${RUST_PORT}"
cpp_pid=$!

sleep 3
wait_for_leader_cpp "${HOST}" "${CPP_PORT}" "C++"

echo "==> C++ client-put -> C++ leader"
put="$("${cpp_bin}" client-put "${HOST}" "${CPP_PORT}" k v1 three-port-client)"
echo "${put}"
grep -q "success: true" <<<"${put}"

echo
echo "==> C++ client-get -> C++ leader"
get="$("${cpp_bin}" client-get "${HOST}" "${CPP_PORT}" k three-port-client)"
echo "${get}"
grep -q "value: v1" <<<"${get}"

echo
echo "==> Java telemetry -> Java follower"
j_tel="$(run_java_probe telemetry "${HOST}" "${JAVA_PORT}" true)"
echo "${j_tel}"
grep -q "state: FOLLOWER" <<<"${j_tel}"
grep -q "leaderId: ${CPP_PEER_ID}" <<<"${j_tel}"

echo
echo "==> Rust telemetry -> Rust follower"
r_tel="$("${rust_bin}" telemetry "${HOST}" "${RUST_PORT}")"
echo "${r_tel}"
grep -q "state: FOLLOWER" <<<"${r_tel}"
grep -Eq "last_applied: ([1-9][0-9]*)" <<<"${r_tel}"

cleanup
trap - EXIT

echo
echo "Three-port smoke suite passed."


