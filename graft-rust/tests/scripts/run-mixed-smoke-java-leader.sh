#!/usr/bin/env bash
# Cross-language smoke test: Java leader, Rust follower.
# Mirrors graft-cpp/run-mixed-smoke-java-leader.sh with Rust in place of C++.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

HOST="${RAFT_MIXED_HOST:-127.0.0.1}"
JAVA_PORT="${RAFT_MIXED_JAVA_PORT:-12681}"
RUST_PORT="${RAFT_MIXED_RUST_PORT:-12682}"
RUST_PEER_ID="${RAFT_MIXED_RUST_PEER_ID:-rust-node}"
JAVA_PEER_ID="${RAFT_MIXED_JAVA_PEER_ID:-java-peer}"
JAVA_TIMEOUT_MS="${RAFT_MIXED_JAVA_TIMEOUT_MS:-10000}"

# Locate the Rust smoke binary
if [[ -n "${RAFT_RUST_BIN:-}" ]]; then
  rust_bin="${RAFT_RUST_BIN}"
elif [[ -x "${REPO_ROOT}/target/debug/graft-kv" ]]; then
  rust_bin="${REPO_ROOT}/target/debug/graft-kv"
elif [[ -x "${REPO_ROOT}/target/release/graft-kv" ]]; then
  rust_bin="${REPO_ROOT}/target/release/graft-kv"
else
  echo "Could not find graft-kv binary. Build with 'cargo build -p graft-app-kv' or set RAFT_RUST_BIN." >&2
  exit 1
fi

# Prepare the Java probe (reuses the existing graft-cpp/java-probe)
JAVA_PROBE_DIR="${REPO_ROOT}/../graft-cpp"
if [[ ! -f "${JAVA_PROBE_DIR}/prepare-java-probe.sh" ]]; then
  JAVA_PROBE_DIR="${REPO_ROOT}/graft-cpp"
fi
if [[ -f "${JAVA_PROBE_DIR}/prepare-java-probe.sh" ]]; then
  (cd "${JAVA_PROBE_DIR}" && ./prepare-java-probe.sh)
else
  echo "WARNING: could not find prepare-java-probe.sh; assuming Java probe is already built" >&2
fi

run_java_probe() {
  if [[ -f "${JAVA_PROBE_DIR}/run-java-probe.sh" ]]; then
    "${JAVA_PROBE_DIR}/run-java-probe.sh" "$@"
  else
    echo "ERROR: run-java-probe.sh not found" >&2
    exit 1
  fi
}
run_java_peer() {
  if [[ -f "${JAVA_PROBE_DIR}/run-java-peer.sh" ]]; then
    "${JAVA_PROBE_DIR}/run-java-peer.sh" "$@"
  else
    echo "ERROR: run-java-peer.sh not found" >&2
    exit 1
  fi
}

tmp_root="${TMPDIR:-/tmp}/graft-rust-mixed-smoke-java-leader"
java_data_dir="${tmp_root}/java-data"
rust_state="${tmp_root}/rust-node.state"
java_log="${tmp_root}/java-peer.log"
rust_log="${tmp_root}/rust-node.log"

rm -rf "${tmp_root}"
mkdir -p "${java_data_dir}"

cleanup() {
  kill "${java_pid:-}" >/dev/null 2>&1 || true
  kill "${rust_pid:-}" >/dev/null 2>&1 || true
  wait "${java_pid:-}" >/dev/null 2>&1 || true
  wait "${rust_pid:-}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

# Start Rust follower (passive, no elections)
"${rust_bin}" serve-persistent \
  "${HOST}" "${RUST_PORT}" "${RUST_PEER_ID}" "${rust_state}" 0 0 0 \
  "${JAVA_PEER_ID}@${HOST}:${JAVA_PORT}" >"${rust_log}" 2>&1 &
rust_pid=$!

# Start Java peer (will become leader)
run_java_peer \
  "${JAVA_PEER_ID}" "${HOST}" "${JAVA_PORT}" "${JAVA_TIMEOUT_MS}" "${java_data_dir}" \
  "${RUST_PEER_ID}@${HOST}:${RUST_PORT}" >"${java_log}" 2>&1 &
java_pid=$!

# Wait for Java leader
echo "==> Waiting for Java leader..."
for _ in $(seq 1 40); do
  if telemetry_output="$(run_java_probe telemetry "${HOST}" "${JAVA_PORT}" true 2>/dev/null)"; then
    if grep -q "state: LEADER" <<<"${telemetry_output}" && grep -q "success: true" <<<"${telemetry_output}"; then
      break
    fi
  fi
  sleep 0.5
done

echo "==> Java client-put -> Java leader"
put_output="$(run_java_probe client-put "${HOST}" "${JAVA_PORT}" k v1 rust-client)"
echo "${put_output}"
grep -q "success: true" <<<"${put_output}"
grep -q "status: ACCEPTED" <<<"${put_output}"

echo
echo "==> Java client-get -> Java leader"
get_output="$(run_java_probe client-get "${HOST}" "${JAVA_PORT}" k rust-client)"
echo "${get_output}"
grep -q "success: true" <<<"${get_output}"
grep -q "get.value: v1" <<<"${get_output}"

echo
echo "==> Rust telemetry -> Rust follower"
rust_telemetry="$("${rust_bin}" telemetry "${HOST}" "${RUST_PORT}" --require-leader-summary)"
echo "${rust_telemetry}"
grep -q "state: FOLLOWER" <<<"${rust_telemetry}"
grep -q "leader_id: ${JAVA_PEER_ID}" <<<"${rust_telemetry}"
grep -Eq "commit_index: ([1-9][0-9]*)" <<<"${rust_telemetry}"

echo
echo "==> Rust client-get -> Rust follower (should redirect)"
rust_query="$("${rust_bin}" client-get "${HOST}" "${RUST_PORT}" k mixed-rust-client 2>&1 || true)"
echo "${rust_query}"
grep -q "status: NOT_LEADER" <<<"${rust_query}"
grep -q "leader_id: ${JAVA_PEER_ID}" <<<"${rust_query}"

echo
echo "==> Rust dump-state -> Rust follower"
if "${rust_bin}" dump-state "${rust_state}" 2>/dev/null; then
  true # dump-state is best-effort
fi

echo
echo "==> Java telemetry -> Java leader"
telemetry_output="$(run_java_probe telemetry "${HOST}" "${JAVA_PORT}" true)"
echo "${telemetry_output}"
grep -q "state: LEADER" <<<"${telemetry_output}"
grep -q "leaderId: ${JAVA_PEER_ID}" <<<"${telemetry_output}"

echo
echo "==> Rust log"
cat "${rust_log}"

echo
echo "==> Java log"
cat "${java_log}"

echo
echo "Mixed Java/Rust smoke (Java leader) passed."
