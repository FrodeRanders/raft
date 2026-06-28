#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

usage() {
  cat <<'EOF'
Usage: ./run-docker-srv.sh [suite] [extra Jepsen args...]

Suites:
  smoke      baseline Java, C++, Rust, and mixed Docker/SRV runs
  partition  Docker-network partition scenarios
  all        smoke + partition

Examples:
  ./run-docker-srv.sh
  ./run-docker-srv.sh smoke --time-limit 8 --concurrency 4
  ./run-docker-srv.sh partition --nemesis-interval 3
EOF
}

suite="all"
if [[ $# -gt 0 ]]; then
  case "$1" in
    smoke|partition|all)
      suite="$1"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
  esac
fi

if ! docker info >/dev/null 2>&1; then
  echo "Docker daemon is not reachable; start Docker and retry." >&2
  exit 1
fi

extra_args=("$@")
timestamp="$(date +%Y%m%dt%H%M%S)"
root_workdir="${TMPDIR:-/tmp}/raft-jepsen-docker-srv-${suite}-${timestamp}"

declare -i failures=0
declare -a failed_cases=()

run_case() {
  local name="$1"
  local mode="$2"
  shift 2

  local workdir="${root_workdir}/${name}"
  local project="raft-jepsen-${name}-${timestamp}"
  local -a cmd=(clojure -M:run
    --backend docker-srv
    --srv-mode "${mode}"
    --compose-project "${project}"
    --workdir "${workdir}")
  cmd+=("$@")
  cmd+=("${extra_args[@]}")

  echo
  echo "==> Running Docker/SRV Jepsen ${name}"
  echo "    workdir: ${workdir}"
  echo "    command: ${cmd[*]}"

  if "${cmd[@]}"; then
    echo "==> ${name}: PASS"
  else
    echo "==> ${name}: FAIL"
    failures+=1
    failed_cases+=("${name}")
  fi
}

run_smoke() {
  run_case docker-srv-java-baseline java --time-limit 8 --concurrency 4
  run_case docker-srv-cpp-baseline cpp --time-limit 8 --concurrency 4 --client-impl cpp
  run_case docker-srv-rust-baseline rust --time-limit 8 --concurrency 4 --client-impl rust
  run_case docker-srv-mixed-baseline mixed --time-limit 8 --concurrency 4 --client-impl mixed
}

run_partition() {
  run_case docker-srv-java-partition-one java --time-limit 12 --concurrency 4 --nemesis partition-one --nemesis-interval 3
  run_case docker-srv-mixed-process-pause mixed --time-limit 12 --concurrency 4 --client-impl mixed --nemesis process-pause --nemesis-interval 3
  run_case docker-srv-mixed-partition-leader mixed --time-limit 12 --concurrency 4 --client-impl mixed --nemesis partition-leader --nemesis-interval 3
  run_case docker-srv-rust-partition-one rust --time-limit 12 --concurrency 4 --client-impl rust --nemesis partition-one --nemesis-interval 3
}

case "${suite}" in
  smoke)
    run_smoke
    ;;
  partition)
    run_partition
    ;;
  all)
    run_smoke
    run_partition
    ;;
  *)
    echo "Unknown suite: ${suite}" >&2
    usage >&2
    exit 2
    ;;
esac

echo
echo "Docker/SRV suite workdir: ${root_workdir}"

if (( failures > 0 )); then
  echo "Failed cases: ${failed_cases[*]}" >&2
  exit 1
fi

echo "All Docker/SRV Jepsen cases passed."
