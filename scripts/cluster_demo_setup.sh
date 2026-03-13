#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

export RAFT_BUILD_JAR="${RAFT_BUILD_JAR:-1}"
export RAFT_CLEAN_DATA="${RAFT_CLEAN_DATA:-1}"
export RAFT_LOG_DIR="${RAFT_LOG_DIR:-./raft-demo}"
export RAFT_DATA_DIR="${RAFT_DATA_DIR:-./raft-data}"
export RAFT_DEMO_TELEMETRY_INTERVAL_SECONDS="${RAFT_DEMO_TELEMETRY_INTERVAL_SECONDS:-10}"
export RAFT_SUMMARY_INTERVAL_SECONDS="${RAFT_SUMMARY_INTERVAL_SECONDS:-10}"
export RAFT_STATISTICS_INTERVAL_SECONDS="${RAFT_STATISTICS_INTERVAL_SECONDS:-30}"
export RAFT_TELEMETRY_RATE_LIMIT_PER_MINUTE="${RAFT_TELEMETRY_RATE_LIMIT_PER_MINUTE:-120}"
export RAFT_TELEMETRY_EXPORTER="${RAFT_TELEMETRY_EXPORTER:-none}"
export RAFT_DEMO_AUTORUN="${RAFT_DEMO_AUTORUN:-1}"

echo "Starting cluster demo setup with opinionated defaults"
echo "  clean data: ${RAFT_CLEAN_DATA}"
echo "  build jar: ${RAFT_BUILD_JAR}"
echo "  telemetry interval: ${RAFT_DEMO_TELEMETRY_INTERVAL_SECONDS}s"
echo "  summary interval: ${RAFT_SUMMARY_INTERVAL_SECONDS}s"
echo "  statistics interval: ${RAFT_STATISTICS_INTERVAL_SECONDS}s"
echo "  auto reconfigure demo: ${RAFT_DEMO_AUTORUN}"
echo

"${ROOT_DIR}/scripts/test_setup.sh"

if [[ "${RAFT_DEMO_AUTORUN}" == "1" ]]; then
    echo
    echo "Running automated cluster reconfiguration demo"
    "${ROOT_DIR}/scripts/cluster_reconfiguration_demo.sh"
    echo "Demo flow log: ${RAFT_LOG_DIR}/demo-flow.log"
    echo "Demo event log: ${RAFT_LOG_DIR}/demo-events.jsonl"
    echo "Telemetry snapshot log: ${RAFT_LOG_DIR}/telemetry-snapshots.jsonl"
    echo "Merged timeline view:"
    echo "  ${ROOT_DIR}/scripts/cluster_demo_timeline.sh ${RAFT_LOG_DIR}"
fi
