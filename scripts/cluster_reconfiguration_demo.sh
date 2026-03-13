#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

VERSION="$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)"
JAR_NAME="raft-dist/target/raft-${VERSION}.jar"

PORTS=(${RAFT_PORTS:-10080 10081 10082 10083 10084})
SEED_PORT="${RAFT_DEMO_FLOW_SEED_PORT:-${PORTS[0]}}"
JOIN_PORT="${RAFT_DEMO_JOIN_PORT:-10085}"
LOG_DIR="${RAFT_LOG_DIR:-./raft-demo}"
FLOW_LOG="${LOG_DIR}/demo-flow.log"
EVENT_LOG="${LOG_DIR}/demo-events.jsonl"
SUMMARY_LOG="${LOG_DIR}/cluster-summary.jsonl"
TELEMETRY_LOG="${LOG_DIR}/telemetry-snapshots.jsonl"
WAIT_TIMEOUT_SECONDS="${RAFT_DEMO_FLOW_TIMEOUT_SECONDS:-30}"
POLL_INTERVAL_SECONDS="${RAFT_DEMO_FLOW_POLL_SECONDS:-1}"
DATA_DIR="${RAFT_DATA_DIR:-./raft-data}"
TELEMETRY_INTERVAL="${RAFT_DEMO_TELEMETRY_INTERVAL_SECONDS:-10}"
STATISTICS_INTERVAL="${RAFT_STATISTICS_INTERVAL_SECONDS:-30}"
TELEMETRY_RATE_LIMIT="${RAFT_TELEMETRY_RATE_LIMIT_PER_MINUTE:-120}"
TELEMETRY_EXPORTER="${RAFT_TELEMETRY_EXPORTER:-none}"

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

emit_event() {
    local type="$1"
    local message="$2"
    local detail="${3:-}"
    local ts
    ts="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
    printf '{"schemaVersion":"1","recordType":"demo-event","ts":"%s","eventType":"%s","message":"%s"' \
        "${ts}" \
        "$(json_escape "${type}")" \
        "$(json_escape "${message}")" >> "${EVENT_LOG}"
    if [[ -n "${detail}" ]]; then
        printf ',"detail":"%s"' "$(json_escape "${detail}")" >> "${EVENT_LOG}"
    fi
    printf '}\n' >> "${EVENT_LOG}"
}

emit_json_record() {
    local target_file="$1"
    local record_type="$2"
    local source="$3"
    local payload_json="$4"
    local ts
    ts="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
    printf '{"schemaVersion":"1","recordType":"%s","ts":"%s","source":"%s","data":%s}\n' \
        "$(json_escape "${record_type}")" \
        "${ts}" \
        "$(json_escape "${source}")" \
        "${payload_json}" >> "${target_file}"
}

summary_json() {
    java -jar "${JAR_NAME}" cluster-summary --json "${SEED_PORT}" 2>> "${FLOW_LOG}" | tr -d '\n'
}

telemetry_json() {
    local port="$1"
    java -jar "${JAR_NAME}" telemetry --json "${port}" 2>> "${FLOW_LOG}" | tr -d '\n'
}

json_string_field() {
    local json="$1"
    local key="$2"
    printf '%s' "${json}" | grep -o "\"${key}\": \"[^\"]*\"" | head -1 | cut -d'"' -f4
}

json_bool_field() {
    local json="$1"
    local key="$2"
    printf '%s' "${json}" | grep -o "\"${key}\": [^,}]*" | head -1 | awk -F': ' '{print $2}'
}

wait_for_condition() {
    local description="$1"
    local condition="$2"
    local started
    started="$(date +%s)"
    while true; do
        local json
        json="$(summary_json)"
        if eval "${condition}"; then
            emit_json_record "${SUMMARY_LOG}" "cluster-summary" "demo-flow" "${json}"
            echo "[demo-flow] ${description}: ${json}" >> "${FLOW_LOG}"
            emit_event "condition_met" "${description}" "${json}"
            return 0
        fi
        if (( "$(date +%s)" - started >= WAIT_TIMEOUT_SECONDS )); then
            echo "[demo-flow] timeout waiting for ${description}" >> "${FLOW_LOG}"
            emit_event "timeout" "${description}"
            return 1
        fi
        sleep "${POLL_INTERVAL_SECONDS}"
    done
}

wait_for_join_status() {
    local target_port="$1"
    local peer_id="$2"
    local expected_status="$3"
    local started
    started="$(date +%s)"
    while true; do
        local status_line
        status_line="$(java -jar "${JAR_NAME}" join-status "${target_port}" "${peer_id}" 2>> "${FLOW_LOG}")"
        echo "[demo-flow] join-status ${peer_id}: ${status_line}" >> "${FLOW_LOG}"
        if [[ "${status_line}" == *"status=${expected_status}"* ]]; then
            emit_event "join_status" "join status reached ${expected_status} for ${peer_id}" "${status_line}"
            return 0
        fi
        if (( "$(date +%s)" - started >= WAIT_TIMEOUT_SECONDS )); then
            echo "[demo-flow] timeout waiting for join status ${expected_status} for ${peer_id}" >> "${FLOW_LOG}"
            emit_event "timeout" "join status ${expected_status} for ${peer_id}"
            return 1
        fi
        sleep "${POLL_INTERVAL_SECONDS}"
    done
}

echo "[demo-flow] starting automated reconfiguration demo" >> "${FLOW_LOG}"
emit_event "start" "starting automated reconfiguration demo"
initial_summary="$(summary_json)"
leader_id="$(json_string_field "${initial_summary}" "leaderId")"
if [[ -z "${leader_id}" ]]; then
    echo "[demo-flow] could not determine leader from summary: ${initial_summary}" >> "${FLOW_LOG}"
    emit_event "error" "could not determine leader from initial summary" "${initial_summary}"
    exit 1
fi
leader_port="${leader_id#server-}"
echo "[demo-flow] initial leader=${leader_id}" >> "${FLOW_LOG}"
emit_json_record "${SUMMARY_LOG}" "cluster-summary" "demo-flow" "${initial_summary}"
emit_event "leader_detected" "initial leader detected" "${initial_summary}"

JOINT_MEMBERS=()
for port in "${PORTS[@]}"; do
    if [[ "server-${port}" != "${leader_id}" ]]; then
        JOINT_MEMBERS+=("${port}")
    fi
done

echo "[demo-flow] submitting joint reconfiguration without ${leader_id}" >> "${FLOW_LOG}"
emit_event "reconfigure_joint" "submitting joint reconfiguration without ${leader_id}" "${JOINT_MEMBERS[*]}"
java -jar "${JAR_NAME}" reconfigure joint "${SEED_PORT}" "${JOINT_MEMBERS[@]}" >> "${FLOW_LOG}" 2>&1

wait_for_condition \
    "joint consensus after leader removal request" \
    '[[ "$(json_bool_field "${json}" "jointConsensus")" == "true" ]]'

echo "[demo-flow] submitting finalize reconfiguration" >> "${FLOW_LOG}"
emit_event "reconfigure_finalize" "submitting finalize reconfiguration"
java -jar "${JAR_NAME}" reconfigure finalize "${SEED_PORT}" >> "${FLOW_LOG}" 2>&1

wait_for_condition \
    "finalized configuration after leader removal" \
    '[[ "$(json_bool_field "${json}" "jointConsensus")" == "false" && "$(json_string_field "${json}" "leaderId")" != "'"${leader_id}"'" ]]'

post_finalize_summary="$(summary_json)"
new_leader_id="$(json_string_field "${post_finalize_summary}" "leaderId")"
new_leader_port="${new_leader_id#server-}"
join_peer_id="server-${JOIN_PORT}"
emit_event "leader_detected" "post-removal leader detected" "${post_finalize_summary}"

echo "[demo-flow] starting join-mode node ${join_peer_id} via seed ${new_leader_id}" >> "${FLOW_LOG}"
emit_event "join_node_start" "starting join-mode node ${join_peer_id}" "seed=${new_leader_id}"
mkdir -p "${LOG_DIR}/node-${JOIN_PORT}"
java \
    -Draft.data.dir="${DATA_DIR}" \
    -Dlog-path="${LOG_DIR}/node-${JOIN_PORT}" \
    -Draft.demo.telemetry.interval.seconds="${TELEMETRY_INTERVAL}" \
    -Draft.statistics.interval.seconds="${STATISTICS_INTERVAL}" \
    -Draft.telemetry.rate.limit.per.minute="${TELEMETRY_RATE_LIMIT}" \
    -Draft.telemetry.exporter="${TELEMETRY_EXPORTER}" \
    -jar "${JAR_NAME}" join "${JOIN_PORT}" "${new_leader_port}" \
    > "${LOG_DIR}/node-${JOIN_PORT}/stdout.log" 2>&1 &

echo "[demo-flow] waiting for ${join_peer_id} to enter joint consensus" >> "${FLOW_LOG}"
emit_event "join_wait" "waiting for ${join_peer_id} to enter joint consensus"
wait_for_join_status "${new_leader_port}" "${join_peer_id}" "IN_JOINT_CONSENSUS"

echo "[demo-flow] waiting for ${join_peer_id} to complete join" >> "${FLOW_LOG}"
emit_event "join_wait" "waiting for ${join_peer_id} to complete join"
wait_for_join_status "${new_leader_port}" "${join_peer_id}" "COMPLETED"

wait_for_condition \
    "finalized configuration after new node join" \
    '[[ "$(json_bool_field "${json}" "jointConsensus")" == "false" && "$(json_string_field "${json}" "leaderId")" != "" ]]'

final_telemetry="$(telemetry_json "${new_leader_port}")"
if [[ "${final_telemetry}" != *"\"id\": \"${join_peer_id}\""* ]]; then
    echo "[demo-flow] joined node ${join_peer_id} missing from final telemetry: ${final_telemetry}" >> "${FLOW_LOG}"
    emit_event "error" "joined node ${join_peer_id} missing from final telemetry" "${final_telemetry}"
    exit 1
fi
emit_json_record "${TELEMETRY_LOG}" "telemetry-snapshot" "demo-flow" "${final_telemetry}"
emit_event "join_completed" "joined node ${join_peer_id} present in final telemetry" "${final_telemetry}"

echo "[demo-flow] completed automated reconfiguration demo" >> "${FLOW_LOG}"
emit_event "complete" "completed automated reconfiguration demo"
