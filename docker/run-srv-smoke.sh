#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

usage() {
  cat <<'EOF'
Usage: docker/run-srv-smoke.sh <java|cpp|rust|mixed> [--no-build]

Builds and starts a CoreDNS-backed Docker Compose Raft cluster, waits for the
published node ports, probes cluster summary, and tears the cluster down again.

Modes:
  java   - 3 Java nodes
  cpp    - 3 C++ nodes
  rust   - 3 Rust nodes
  mixed  - 1 Java + 1 C++ + 1 Rust node

Examples:
  docker/run-srv-smoke.sh java
  docker/run-srv-smoke.sh rust
  docker/run-srv-smoke.sh mixed --no-build
EOF
}

if [[ $# -lt 1 || "$1" == "-h" || "$1" == "--help" ]]; then
  usage
  exit 0
fi

mode="$1"
shift

build=1
while [[ $# -gt 0 ]]; do
  case "$1" in
    --no-build) build=0; shift ;;
    *)
      echo "Unknown option: $1" >&2; usage >&2; exit 2 ;;
  esac
done

case "${mode}" in
  java|cpp|rust|mixed)
    compose_file="docker/compose-srv-${mode}.yml"
    ;;
  *)
    echo "Unknown mode: ${mode}" >&2; usage >&2; exit 2 ;;
esac

if ! docker info >/dev/null 2>&1; then
  echo "Docker daemon is not reachable; start Docker and retry." >&2
  exit 1
fi

# Resolve a probe binary: prefer C++ graft_smoke, fall back to Rust graft-kv.
# Both speak the same protobuf wire protocol, so either can probe any node.
if [[ -x graft-cpp/build/graft_smoke ]]; then
  probe_bin="graft-cpp/build/graft_smoke"
elif [[ -x graft-rust/target/debug/graft-kv ]]; then
  probe_bin="graft-rust/target/debug/graft-kv"
elif [[ -x graft-rust/target/release/graft-kv ]]; then
  probe_bin="graft-rust/target/release/graft-kv"
else
  echo "No probe binary found. Build graft-cpp or graft-rust first." >&2
  exit 1
fi

# Helper: probe a node's cluster-summary via the probe binary.
probe_summary() {
  local port="$1"
  if [[ "${probe_bin}" == *graft_smoke ]]; then
    "./${probe_bin}" cluster-summary 127.0.0.1 "${port}" smoke-probe 2>/dev/null || true
  else
    "./${probe_bin}" cluster-summary 127.0.0.1 "${port}" 2>/dev/null || true
  fi
}

cleanup() {
  docker compose -f "${compose_file}" down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

if (( build )); then
  docker compose -f "${compose_file}" build
fi

docker compose -f "${compose_file}" up -d

wait_for_port() {
  local port="$1"
  local deadline=$((SECONDS + 60))
  while (( SECONDS < deadline )); do
    local output
    output="$(probe_summary "${port}")"
    if grep -q '^status: \(OK\|REDIRECT\)' <<<"${output}"; then
      return 0
    fi
    sleep 1
  done
  echo "Timed out waiting for raft node on host port ${port}" >&2
  docker compose -f "${compose_file}" logs >&2 || true
  return 1
}

wait_for_port 17001
wait_for_port 17002
wait_for_port 17003

wait_for_healthy_leader() {
  local deadline=$((SECONDS + 60))
  while (( SECONDS < deadline )); do
    if { probe_summary 17001; probe_summary 17002; probe_summary 17003; } | grep -q '^cluster_health\|^health'; then
      return 0
    fi
    sleep 1
  done
  echo "No node reported a healthy leader cluster summary." >&2
  docker compose -f "${compose_file}" logs >&2 || true
  return 1
}

wait_for_healthy_leader

echo "Cluster summaries:"
probe_summary 17001
probe_summary 17002
probe_summary 17003

echo "SRV ${mode} smoke completed."
