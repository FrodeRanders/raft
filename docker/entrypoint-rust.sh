#!/usr/bin/env bash
# Entrypoint for the Rust Raft Docker image.
# Reads RAFT_* environment variables and launches graft-kv.
set -euo pipefail

NODE_ID="${RAFT_NODE_ID:-rust-1}"
BIND_HOST="${RAFT_BIND_HOST:-0.0.0.0}"
BIND_PORT="${RAFT_BIND_PORT:-7000}"
DATA_DIR="${RAFT_DATA_DIR:-/data}"
STATE_FILE="${DATA_DIR}/raft.state"

mkdir -p "${DATA_DIR}"

resolve_host() {
    local host="$1"
    if [[ "${host}" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
        printf '%s\n' "${host}"
        return 0
    fi

    local line
    line="$(getent ahostsv4 "${host}" 2>/dev/null | head -n 1 || true)"
    if [[ -n "${line}" ]]; then
        set -- ${line}
        printf '%s\n' "$1"
        return 0
    fi

    printf '%s\n' "${host}"
}

normalize_peer_spec() {
    local spec="$1"
    local id rest addr role host port resolved

    if [[ "${spec}" == *@* ]]; then
        id="${spec%%@*}"
        rest="${spec#*@}"
    else
        printf '%s\n' "${spec}"
        return 0
    fi

    if [[ "${rest}" == */* ]]; then
        addr="${rest%%/*}"
        role="/${rest#*/}"
    else
        addr="${rest}"
        role=""
    fi

    host="${addr%:*}"
    port="${addr##*:}"
    resolved="$(resolve_host "${host}")"
    printf '%s@%s:%s%s\n' "${id}" "${resolved}" "${port}" "${role}"
}

# Build the peer list. Peers can be specified explicitly via RAFT_PEERS
# (comma-separated id@host:port), or discovered via RAFT_CLUSTER_SRV
# (DNS SRV record).
PEER_ARGS=()
if [[ -n "${RAFT_PEERS:-}" ]]; then
    IFS=',' read -ra SPECS <<< "${RAFT_PEERS}"
    for spec in "${SPECS[@]}"; do
        spec="$(echo "${spec}" | xargs)"
        [[ -n "${spec}" ]] && PEER_ARGS+=("$(normalize_peer_spec "${spec}")")
    done
elif [[ -n "${RAFT_CLUSTER_SRV:-}" ]]; then
    # Resolve SRV records using dig. Format: id@host:port for each target.
    if command -v dig &>/dev/null; then
        while IFS= read -r line; do
            [[ -z "${line}" ]] && continue
            # dig +short SRV output: "10 10 7000 raft-1.raft.local."
            read -r _prio _weight port hostname <<< "${line}"
            hostname="${hostname%.}" # strip trailing dot
            peer_id="${hostname%%.*}" # first component of hostname
            PEER_ARGS+=("${peer_id}@${hostname}:${port}")
        done < <(dig +short SRV "${RAFT_CLUSTER_SRV}" 2>/dev/null || true)
    else
        echo "WARNING: dig not found, cannot resolve SRV records" >&2
        echo "Install dnsutils or set RAFT_PEERS explicitly" >&2
    fi
fi

echo "[graft-kv entrypoint] node=${NODE_ID} bind=${BIND_HOST}:${BIND_PORT} peers=${PEER_ARGS[*]:-none}" >&2

exec /app/graft-kv serve-active-persistent \
    "${BIND_HOST}" "${BIND_PORT}" \
    "${NODE_ID}" "${STATE_FILE}" \
    0 0 0 \
    "${PEER_ARGS[@]}"
