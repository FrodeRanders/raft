#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

LOG_DIR="${1:-${RAFT_LOG_DIR:-./raft-demo}}"
SUMMARY_LOG="${LOG_DIR}/cluster-summary.jsonl"
EVENT_LOG="${LOG_DIR}/demo-events.jsonl"
TELEMETRY_LOG="${LOG_DIR}/telemetry-snapshots.jsonl"

extract_ts() {
    sed -n 's/.*"ts":"\([^"]*\)".*/\1/p'
}

extract_string_field() {
    local key="$1"
    sed -n "s/.*\"${key}\":\"\\([^\"]*\\)\".*/\\1/p"
}

extract_optional_string_field() {
    local key="$1"
    sed -n "s/.*\"${key}\": \"\\([^\"]*\\)\".*/\\1/p"
}

extract_optional_plain_field() {
    local key="$1"
    sed -n "s/.*\"${key}\": \\([^,}]*\\).*/\\1/p"
}

emit_lines() {
    local file="$1"
    [[ -f "${file}" ]] || return 0

    while IFS= read -r line; do
        [[ -n "${line}" ]] || continue
        local ts kind detail
        ts="$(printf '%s\n' "${line}" | extract_ts)"
        kind="$(printf '%s\n' "${line}" | extract_string_field "recordType")"
        case "${kind}" in
            demo-event)
                detail="event=$(printf '%s\n' "${line}" | extract_string_field "eventType") message=$(printf '%s\n' "${line}" | extract_string_field "message")"
                ;;
            cluster-summary)
                detail="leader=$(printf '%s\n' "${line}" | extract_optional_string_field "leaderId") health=$(printf '%s\n' "${line}" | extract_optional_string_field "clusterHealth") reason=$(printf '%s\n' "${line}" | extract_optional_string_field "clusterStatusReason")"
                ;;
            telemetry-snapshot)
                detail="peer=$(printf '%s\n' "${line}" | extract_optional_string_field "peerId") state=$(printf '%s\n' "${line}" | extract_optional_string_field "state") term=$(printf '%s\n' "${line}" | extract_optional_plain_field "term")"
                ;;
            *)
                detail="raw=${line}"
                ;;
        esac
        printf '%s\t%-18s %s\n' "${ts}" "${kind}" "${detail}"
    done < "${file}"
}

{
    emit_lines "${SUMMARY_LOG}"
    emit_lines "${EVENT_LOG}"
    emit_lines "${TELEMETRY_LOG}"
} | sort
