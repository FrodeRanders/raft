#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

usage() {
  cat <<'EOF'
Usage: ./run-suite.sh [suite] [extra run-local args...]

Suites:
  smoke     baseline + crash-restart + partition-one
  extended  richer validated local scenarios
  all       smoke + extended

Examples:
  ./run-suite.sh
  ./run-suite.sh smoke
  ./run-suite.sh extended --time-limit 30 --concurrency 12
EOF
}

suite="all"
if [[ $# -gt 0 ]]; then
  case "$1" in
    smoke|extended|all)
      suite="$1"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
  esac
fi

extra_args=("$@")
timestamp="$(date +%Y%m%dT%H%M%S)"
root_workdir="${TMPDIR:-/tmp}/raft-jepsen-suite-${suite}-${timestamp}"

declare -i failures=0
declare -a failed_cases=()

run_case() {
  local name="$1"
  local base_port="$2"
  shift 2

  local workdir="${root_workdir}/${name}"
  local -a cmd=(./run-local.sh --base-port "${base_port}" --workdir "${workdir}")
  cmd+=("$@")
  cmd+=("${extra_args[@]}")

  echo
  echo "==> Running ${name}"
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
  run_case baseline 20080 --time-limit 10 --concurrency 10 --node-count 5
  run_case crash-restart 20180 --time-limit 10 --concurrency 10 --node-count 5 --nemesis crash-restart --nemesis-interval 3
  run_case partition-one 20280 --time-limit 10 --concurrency 10 --node-count 5 --nemesis partition-one --nemesis-interval 3
}

run_extended() {
  run_case persistence-loss-restart 20380 --time-limit 20 --concurrency 10 --node-count 5 --nemesis persistence-loss-restart --nemesis-interval 5 --snapshot-min-entries 5 --snapshot-chunk-bytes 1024
  run_case partition-leader 20480 --time-limit 20 --concurrency 10 --node-count 5 --nemesis partition-leader --nemesis-interval 5
  run_case partition-leader-minority 20580 --time-limit 20 --concurrency 10 --node-count 5 --nemesis partition-leader-minority --nemesis-interval 5
  run_case membership-join-promote 20680 --time-limit 20 --concurrency 10 --node-count 5 --nemesis membership-join-promote --nemesis-interval 3
  run_case membership-demote 20780 --time-limit 20 --concurrency 10 --node-count 5 --nemesis membership-demote --nemesis-interval 3
  run_case membership-remove-follower 20880 --time-limit 20 --concurrency 10 --node-count 5 --nemesis membership-remove-follower --nemesis-interval 3
  run_case membership-remove-leader 20980 --time-limit 20 --concurrency 10 --node-count 5 --nemesis membership-remove-leader --nemesis-interval 3
  run_case membership-remove-follower-partition-leader 21080 --time-limit 20 --concurrency 10 --node-count 5 --nemesis membership-remove-follower-partition-leader --nemesis-interval 3
  run_case multi-key-partition-leader 21180 --time-limit 20 --concurrency 10 --node-count 5 --workload multi-key --key-count 3 --nemesis partition-leader --nemesis-interval 5
  run_case snapshot-partition-leader 21280 --time-limit 20 --concurrency 10 --node-count 5 --nemesis partition-leader --nemesis-interval 5 --snapshot-min-entries 5 --snapshot-chunk-bytes 1024
  run_case seven-node-partition-leader-minority 21380 --time-limit 20 --concurrency 10 --node-count 7 --nemesis partition-leader-minority --nemesis-interval 5
}

case "${suite}" in
  smoke)
    run_smoke
    ;;
  extended)
    run_extended
    ;;
  all)
    run_smoke
    run_extended
    ;;
  *)
    echo "Unknown suite: ${suite}" >&2
    usage >&2
    exit 2
    ;;
esac

echo
echo "Suite workdir: ${root_workdir}"

if (( failures > 0 )); then
  echo "Failed cases: ${failed_cases[*]}" >&2
  exit 1
fi

echo "All suite cases passed."
