#!/usr/bin/env bash
# Cross-language smoke test: Rust leader, Java follower.
# Mirrors graft-cpp/run-mixed-smoke.sh with Rust in place of C++.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

HOST="${RAFT_MIXED_HOST:-127.0.0.1}"
JAVA_PORT="${RAFT_MIXED_JAVA_PORT:-12781}"
RUST_PORT="${RAFT_MIXED_RUST_PORT:-12782}"
RUST_PEER_ID="${RAFT_MIXED_RUST_PEER_ID:-rust-node}"
JAVA_PEER_ID="${RAFT_MIXED_JAVA_PEER_ID:-java-peer}"
JAVA_TIMEOUT_MS="${RAFT_MIXED_JAVA_TIMEOUT_MS:-10000}"
RUST_TERM="${RAFT_MIXED_RUST_TERM:-4}"
RUST_LAST_LOG_INDEX="${RAFT_MIXED_RUST_LAST_LOG_INDEX:-7}"
RUST_LAST_LOG_TERM="${RAFT_MIXED_RUST_LAST_LOG_TERM:-3}"

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

JAVA_PROBE_DIR="${REPO_ROOT}/../graft-cpp"
if [[ ! -f "${JAVA_PROBE_DIR}/prepare-java-probe.sh" ]]; then
  JAVA_PROBE_DIR="${REPO_ROOT}/graft-cpp"
fi
if [[ -f "${JAVA_PROBE_DIR}/prepare-java-probe.sh" ]]; then
  (cd "${JAVA_PROBE_DIR}" && ./prepare-java-probe.sh)
else
  echo "ERROR: prepare-java-probe.sh not found at ${JAVA_PROBE_DIR}" >&2
  exit 1
fi

run_java_probe() {
  "${JAVA_PROBE_DIR}/run-java-probe.sh" "$@"
}
run_java_peer() {
  "${JAVA_PROBE_DIR}/run-java-peer.sh" "$@"
}

tmp_root="${TMPDIR:-/tmp}/graft-rust-mixed-smoke-rust-leader"
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

# Start Java follower
run_java_peer \
  "${JAVA_PEER_ID}" "${HOST}" "${JAVA_PORT}" "${JAVA_TIMEOUT_MS}" "${java_data_dir}" \
  "${RUST_PEER_ID}@${HOST}:${RUST_PORT}" >"${java_log}" 2>&1 &
java_pid=$!

# Start Rust leader (seeded with higher term so it wins)
"${rust_bin}" serve-active-persistent \
  "${HOST}" "${RUST_PORT}" "${RUST_PEER_ID}" "${rust_state}" \
  "${RUST_TERM}" "${RUST_LAST_LOG_INDEX}" "${RUST_LAST_LOG_TERM}" \
  "${JAVA_PEER_ID}@${HOST}:${JAVA_PORT}" >"${rust_log}" 2>&1 &
rust_pid=$!

sleep 5

# Wait for Rust leader
echo "==> Waiting for Rust leader..."
leader_found=false
for _ in $(seq 1 60); do
  if telemetry_output="$("${rust_bin}" telemetry "${HOST}" "${RUST_PORT}" --require-leader-summary 2>/dev/null)"; then
    if grep -q "state: LEADER" <<<"${telemetry_output}"; then
      leader_found=true
      break
    fi
  fi
  sleep 1
done
if [[ "${leader_found}" != "true" ]]; then
  echo "ERROR: Rust leader did not become ready at ${HOST}:${RUST_PORT}" >&2
  echo "Rust log:"
  cat "${rust_log}" 2>/dev/null || true
  exit 1
fi

echo "==> Rust client-put -> Rust leader"
put_output="$("${rust_bin}" client-put "${HOST}" "${RUST_PORT}" k v1 rust-client)"
echo "${put_output}"
grep -q "success: true" <<<"${put_output}"
grep -q "status: ACCEPTED" <<<"${put_output}"

echo
echo "==> Rust client-get -> Rust leader"
get_output="$("${rust_bin}" client-get "${HOST}" "${RUST_PORT}" k rust-client)"
echo "${get_output}"
grep -q "success: true" <<<"${get_output}"
grep -q "get.value: v1" <<<"${get_output}"

echo
echo "==> Java telemetry -> Java follower"
telemetry_output="$(run_java_probe telemetry "${HOST}" "${JAVA_PORT}")"
echo "${telemetry_output}"
grep -q "state: FOLLOWER" <<<"${telemetry_output}"
grep -q "leaderId: ${RUST_PEER_ID}" <<<"${telemetry_output}"
grep -Eq "commitIndex: ([89]|[1-9][0-9]+)" <<<"${telemetry_output}"

echo
echo "==> Java client-get -> Java follower (should redirect)"
follower_query="$(run_java_probe client-get "${HOST}" "${JAVA_PORT}" k rust-client)"
echo "${follower_query}"
grep -q "status: REDIRECT" <<<"${follower_query}"
grep -q "leaderId: ${RUST_PEER_ID}" <<<"${follower_query}"

echo
echo "==> Rust telemetry -> Rust leader"
rust_telemetry="$("${rust_bin}" telemetry "${HOST}" "${RUST_PORT}" --require-leader-summary)"
echo "${rust_telemetry}"
grep -q "state: LEADER" <<<"${rust_telemetry}"

echo
echo "==> Rust log"
cat "${rust_log}"

echo
echo "==> Java log"
cat "${java_log}"

echo
echo "Mixed Java/Rust smoke (Rust leader) passed."
