#!/usr/bin/env bash
# Raw RPC interop test: Rust client sends VoteRequest/AppendEntries/InstallSnapshot
# to a Java peer, verifying wire protocol compatibility at the message level.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

HOST="${RAFT_MIXED_HOST:-127.0.0.1}"
JAVA_PORT="${RAFT_MIXED_JAVA_PORT:-12981}"
RUST_CLIENT_ID="${RAFT_MIXED_RUST_PEER_ID:-rust-probe}"
JAVA_PEER_ID="${RAFT_MIXED_JAVA_PEER_ID:-java-probe}"

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
fi

run_java_probe() { "${JAVA_PROBE_DIR}/run-java-probe.sh" "$@"; }
run_java_peer() { "${JAVA_PROBE_DIR}/run-java-peer.sh" "$@"; }

tmp_root="${TMPDIR:-/tmp}/graft-rust-interop-smoke"
java_data_dir="${tmp_root}/java-data"
java_log="${tmp_root}/java-peer.log"

rm -rf "${tmp_root}"
mkdir -p "${java_data_dir}"

cleanup() { kill "${java_pid:-}" >/dev/null 2>&1 || true; wait "${java_pid:-}" >/dev/null 2>&1 || true; }
trap cleanup EXIT

# Start a Java peer (single node cluster — bootstrap so it becomes leader)
run_java_peer \
  "${JAVA_PEER_ID}" "${HOST}" "${JAVA_PORT}" 10000 "${java_data_dir}" \
  >"${java_log}" 2>&1 &
java_pid=$!
sleep 3

echo "==> Rust -> Java VoteRequest"
v_output="$("${rust_bin}" vote-request "${HOST}" "${JAVA_PORT}" "${RUST_CLIENT_ID}" 0 0 1)"
echo "${v_output}"
grep -q "success: true" <<<"${v_output}"

echo
echo "==> Rust -> Java AppendEntries (heartbeat)"
ae_output="$("${rust_bin}" append-entries "${HOST}" "${JAVA_PORT}" "${RUST_CLIENT_ID}" 0 0 0 1)"
echo "${ae_output}"
grep -q "success: true" <<<"${ae_output}"

echo
echo "==> Rust -> Java InstallSnapshot"
is_output="$("${rust_bin}" install-snapshot "${HOST}" "${JAVA_PORT}" "${RUST_CLIENT_ID}" 10 1 1)"
echo "${is_output}"
grep -q "success: true" <<<"${is_output}"

echo
echo "==> Java log"
cat "${java_log}"

echo
echo "Interop wire protocol smoke passed."
