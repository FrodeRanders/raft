#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

VERSION="$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)"
JAR_NAME="raft-dist/target/raft-${VERSION}.jar"

DATA_DIR="${RAFT_DATA_DIR:-./raft-data}"
LOG_DIR="${RAFT_LOG_DIR:-./raft-demo}"
PORTS=(${RAFT_PORTS:-10080 10081 10082 10083 10084})

TELEMETRY_INTERVAL="${RAFT_DEMO_TELEMETRY_INTERVAL_SECONDS:-15}"
SUMMARY_INTERVAL="${RAFT_SUMMARY_INTERVAL_SECONDS:-0}"
STATISTICS_INTERVAL="${RAFT_STATISTICS_INTERVAL_SECONDS:-30}"
TELEMETRY_RATE_LIMIT="${RAFT_TELEMETRY_RATE_LIMIT_PER_MINUTE:-120}"
TELEMETRY_EXPORTER="${RAFT_TELEMETRY_EXPORTER:-none}"
CLEAN_DATA="${RAFT_CLEAN_DATA:-0}"
BUILD_JAR="${RAFT_BUILD_JAR:-1}"
SUMMARY_LOG="${LOG_DIR}/cluster-summary.jsonl"
SUMMARY_PID_FILE="${LOG_DIR}/cluster-summary.pid"

mkdir -p "${LOG_DIR}"

json_escape() {
    local value="${1:-}"
    value="${value//\\/\\\\}"
    value="${value//\"/\\\"}"
    value="${value//$'\n'/\\n}"
    value="${value//$'\r'/\\r}"
    value="${value//$'\t'/\\t}"
    printf '%s' "${value}"
}

emit_summary_record() {
    local summary_json="$1"
    local source="${2:-background-loop}"
    local ts
    ts="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
    printf '{"schemaVersion":"1","recordType":"cluster-summary","ts":"%s","source":"%s","data":%s}\n' \
        "${ts}" \
        "$(json_escape "${source}")" \
        "${summary_json}" >> "${SUMMARY_LOG}"
}

echo "Preparing local Raft test setup in ${ROOT_DIR}"
echo "Ports: ${PORTS[*]}"
echo "Data dir: ${DATA_DIR}"
echo "Log dir: ${LOG_DIR}"
echo "Telemetry interval: ${TELEMETRY_INTERVAL}s"
echo "Summary interval: ${SUMMARY_INTERVAL}s"
echo "Statistics interval: ${STATISTICS_INTERVAL}s"
echo "Telemetry exporter: ${TELEMETRY_EXPORTER}"
echo

echo "Stopping any existing cluster processes"
"${ROOT_DIR}/scripts/kill_raft.sh"

if [[ "${CLEAN_DATA}" == "1" ]]; then
    echo "Cleaning ${DATA_DIR} and ${LOG_DIR}"
    rm -rf "${DATA_DIR}" "${LOG_DIR}"
fi

mkdir -p "${DATA_DIR}" "${LOG_DIR}"
rm -f "${SUMMARY_PID_FILE}"

if [[ "${BUILD_JAR}" == "1" ]]; then
    echo "Building runnable jar"
    mvn -q -DskipTests package
fi

if [[ ! -f "${JAR_NAME}" ]]; then
    echo "Jar not found: ${JAR_NAME}"
    exit 1
fi

for ((i=0; i<${#PORTS[@]}; i++)); do
    MY_PORT="${PORTS[$i]}"
    PEERS=()
    NODE_LOG_DIR="${LOG_DIR}/node-${MY_PORT}"
    mkdir -p "${NODE_LOG_DIR}"

    for ((j=0; j<${#PORTS[@]}; j++)); do
        if [[ "${i}" -ne "${j}" ]]; then
            PEERS+=("${PORTS[$j]}")
        fi
    done

    echo "Starting node ${MY_PORT} with peers ${PEERS[*]}"
    java \
        -Draft.data.dir="${DATA_DIR}" \
        -Dlog-path="${NODE_LOG_DIR}" \
        -Draft.demo.telemetry.interval.seconds="${TELEMETRY_INTERVAL}" \
        -Draft.statistics.interval.seconds="${STATISTICS_INTERVAL}" \
        -Draft.telemetry.rate.limit.per.minute="${TELEMETRY_RATE_LIMIT}" \
        -Draft.telemetry.exporter="${TELEMETRY_EXPORTER}" \
        -jar "${JAR_NAME}" "${MY_PORT}" "${PEERS[@]}" \
        > "${NODE_LOG_DIR}/stdout.log" 2>&1 &
done

sleep 2

if [[ "${SUMMARY_INTERVAL}" -gt 0 ]]; then
    echo "Starting cluster summary loop every ${SUMMARY_INTERVAL}s -> ${SUMMARY_LOG}"
    (
        while true; do
            summary_json="$(java -jar "${JAR_NAME}" cluster-summary --json "${PORTS[0]}" 2>> "${LOG_DIR}/cluster-summary.stderr.log" | tr -d '\n')" || true
            if [[ -n "${summary_json:-}" ]]; then
                emit_summary_record "${summary_json}" "background-loop"
            fi
            sleep "${SUMMARY_INTERVAL}"
        done
    ) &
    echo $! > "${SUMMARY_PID_FILE}"
fi

echo
echo "Cluster started."
echo "Useful commands:"
echo "  Manual cluster view:"
echo "    java -jar ${JAR_NAME} telemetry ${PORTS[0]}"
echo "  Manual cluster summary:"
echo "    java -jar ${JAR_NAME} cluster-summary --json ${PORTS[0]}"
if [[ "${SUMMARY_INTERVAL}" -gt 0 ]]; then
echo "  Follow cluster summary stream:"
echo "    tail -f ${SUMMARY_LOG}"
fi
echo "  Follow internal log for node ${PORTS[0]}:"
echo "    tail -f ${LOG_DIR}/node-${PORTS[0]}/raft.log"
echo "  Follow telemetry log for node ${PORTS[0]}:"
echo "    tail -f ${LOG_DIR}/node-${PORTS[0]}/telemetry.log"
echo "  Follow statistics log for node ${PORTS[0]}:"
echo "    tail -f ${LOG_DIR}/node-${PORTS[0]}/statistics.log"
echo "  Stop cluster:"
echo "    ${ROOT_DIR}/scripts/kill_raft.sh"
echo

ps -ef | grep "${JAR_NAME}" | grep -v grep || true
