#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

usage() {
  cat <<'EOF'
Usage: docker/run-srv-smoke.sh <java|cpp|mixed> [--no-build]

Builds and starts a CoreDNS-backed Docker Compose Raft cluster, waits for the
published node ports, probes cluster summary through graft_smoke, and tears the
cluster down again.

Examples:
  docker/run-srv-smoke.sh java
  docker/run-srv-smoke.sh cpp
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
    --no-build)
      build=0
      shift
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

case "${mode}" in
  java|cpp|mixed)
    compose_file="docker/compose-srv-${mode}.yml"
    ;;
  *)
    echo "Unknown mode: ${mode}" >&2
    usage >&2
    exit 2
    ;;
esac

if ! docker info >/dev/null 2>&1; then
  echo "Docker daemon is not reachable; start Docker and retry." >&2
  exit 1
fi

if [[ ! -x graft-cpp/build/graft_smoke ]]; then
  echo "Missing graft-cpp/build/graft_smoke; build C++ first with: cmake --build graft-cpp/build" >&2
  exit 1
fi

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
    output="$(./graft-cpp/build/graft_smoke cluster-summary 127.0.0.1 "${port}" smoke-probe 2>/dev/null || true)"
    if grep -q '^status: ' <<<"${output}"; then
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
    if {
      ./graft-cpp/build/graft_smoke cluster-summary 127.0.0.1 17001 smoke-probe 2>/dev/null || true
      ./graft-cpp/build/graft_smoke cluster-summary 127.0.0.1 17002 smoke-probe 2>/dev/null || true
      ./graft-cpp/build/graft_smoke cluster-summary 127.0.0.1 17003 smoke-probe 2>/dev/null || true
    } | grep -q '^cluster_health: healthy$'; then
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
./graft-cpp/build/graft_smoke cluster-summary 127.0.0.1 17001 smoke-probe || true
./graft-cpp/build/graft_smoke cluster-summary 127.0.0.1 17002 smoke-probe || true
./graft-cpp/build/graft_smoke cluster-summary 127.0.0.1 17003 smoke-probe || true

echo "SRV ${mode} smoke completed."
