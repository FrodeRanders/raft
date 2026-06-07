#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

usage() {
  cat <<'EOF'
Usage: ./run-suite.sh [suite] [extra run-local args...]

Suites:
  smoke     baseline + crash-restart + partition-one
  extended  richer validated local scenarios
  mixed     static Java/C++ interop smoke cases
  all       smoke + extended

Examples:
  ./run-suite.sh
  ./run-suite.sh smoke
  ./run-suite.sh mixed --cpp-bin ../graft-cpp/build/graft_smoke
  ./run-suite.sh extended --time-limit 30 --concurrency 12
EOF
}

suite="all"
if [[ $# -gt 0 ]]; then
  case "$1" in
    smoke|extended|mixed|all)
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
  run_case process-pause 20390 --time-limit 20 --concurrency 10 --node-count 5 --nemesis process-pause --nemesis-interval 5
  run_case clock-skew 20440 --time-limit 20 --concurrency 10 --node-count 5 --nemesis clock-skew --nemesis-interval 5 --clock-skew-millis 5000
  run_case snapshot-boundary-restart 20460 --time-limit 60 --concurrency 10 --node-count 5 --nemesis snapshot-boundary-restart --nemesis-interval 5 --snapshot-min-entries 3 --snapshot-chunk-bytes 1024
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

run_mixed() {
  run_case mixed-java-leader-cpp-follower 21480 --time-limit 8 --concurrency 4 --node-count 3 --node-impls java,cpp,java
  run_case mixed-cpp-first 21580 --time-limit 8 --concurrency 4 --node-count 3 --node-impls cpp,java,java
  run_case mixed-process-pause 21590 --time-limit 12 --concurrency 4 --node-count 3 --node-impls java,cpp,java --nemesis process-pause --nemesis-interval 3
  run_case mixed-cpp-joiner 21680 --time-limit 12 --concurrency 4 --node-count 3 --node-impls java,cpp,java --nemesis membership-join-promote --nemesis-interval 3 --joining-impl cpp
  run_case mixed-cpp-leader-cpp-joiner 21780 --time-limit 12 --concurrency 4 --node-count 3 --node-impls cpp,java,java --nemesis membership-join-promote --nemesis-interval 3 --joining-impl cpp
  run_case mixed-cpp-client 21880 --time-limit 8 --concurrency 4 --node-count 3 --node-impls java,cpp,java --client-impl cpp
  run_case mixed-target-client 21980 --time-limit 8 --concurrency 4 --node-count 3 --node-impls cpp,java,java --client-impl mixed
  run_case mixed-partition-leader 22080 --time-limit 12 --concurrency 4 --node-count 3 --node-impls java,cpp,java --nemesis partition-leader --nemesis-interval 3
  run_case mixed-partition-leader-minority 22180 --time-limit 12 --concurrency 4 --node-count 3 --node-impls cpp,java,java --nemesis partition-leader-minority --nemesis-interval 3
  run_case mixed-persistence-loss-restart 22280 --time-limit 12 --concurrency 4 --node-count 3 --node-impls java,cpp,java --nemesis persistence-loss-restart --nemesis-interval 3 --snapshot-min-entries 5 --snapshot-chunk-bytes 1024
  run_case mixed-remove-follower 22380 --time-limit 12 --concurrency 4 --node-count 3 --node-impls java,cpp,java --nemesis membership-remove-follower --nemesis-interval 3
  run_case mixed-remove-leader 22480 --time-limit 12 --concurrency 4 --node-count 3 --node-impls cpp,java,java --nemesis membership-remove-leader --nemesis-interval 3
}

case "${suite}" in
  smoke)
    run_smoke
    ;;
  extended)
    run_extended
    ;;
  mixed)
    run_mixed
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
