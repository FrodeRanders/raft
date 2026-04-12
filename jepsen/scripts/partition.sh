#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "usage: partition.sh <isolate|heal> [port ...]" >&2
  exit 1
fi

ACTION="$1"
shift

OS="$(uname -s)"

ensure_root() {
  if [[ "${EUID:-$(id -u)}" -ne 0 ]]; then
    echo "partition.sh requires root privileges" >&2
    exit 2
  fi
}

anchor_name="raft-jepsen"
pf_anchor_file="/etc/pf.anchors/${anchor_name}"

apply_pf_rules() {
  local ports=("$@")
  {
    echo "block drop quick on lo0 proto tcp from any to any port { $(IFS=,; echo "${ports[*]}") }"
    echo "block drop quick on lo0 proto tcp from any port { $(IFS=,; echo "${ports[*]}") } to any"
  } > "${pf_anchor_file}"

  if ! grep -q "anchor \"${anchor_name}\"" /etc/pf.conf; then
    printf '\nanchor "%s"\nload anchor "%s" from "%s"\n' "${anchor_name}" "${anchor_name}" "${pf_anchor_file}" >> /etc/pf.conf
  fi
  pfctl -f /etc/pf.conf >/dev/null
  pfctl -e >/dev/null 2>&1 || true
}

heal_pf_rules() {
  rm -f "${pf_anchor_file}"
  pfctl -f /etc/pf.conf >/dev/null
}

iptables_bin() {
  if command -v iptables >/dev/null 2>&1; then
    command -v iptables
  elif command -v /sbin/iptables >/dev/null 2>&1; then
    command -v /sbin/iptables
  else
    echo ""
  fi
}

apply_iptables_rules() {
  local ipt="$1"
  shift
  local ports=("$@")
  for port in "${ports[@]}"; do
    "${ipt}" -I INPUT -i lo -p tcp --dport "${port}" -j DROP
    "${ipt}" -I OUTPUT -o lo -p tcp --sport "${port}" -j DROP
  done
}

heal_iptables_rules() {
  local ipt="$1"
  shift
  local ports=("$@")
  for port in "${ports[@]}"; do
    while "${ipt}" -D INPUT -i lo -p tcp --dport "${port}" -j DROP >/dev/null 2>&1; do :; done
    while "${ipt}" -D OUTPUT -o lo -p tcp --sport "${port}" -j DROP >/dev/null 2>&1; do :; done
  done
}

case "${ACTION}" in
  isolate)
    ensure_root
    if [[ $# -lt 1 ]]; then
      echo "isolate requires at least one port" >&2
      exit 1
    fi
    case "${OS}" in
      Darwin)
        apply_pf_rules "$@"
        ;;
      Linux)
        IPT="$(iptables_bin)"
        if [[ -z "${IPT}" ]]; then
          echo "iptables not found" >&2
          exit 3
        fi
        apply_iptables_rules "${IPT}" "$@"
        ;;
      *)
        echo "unsupported OS: ${OS}" >&2
        exit 4
        ;;
    esac
    ;;
  heal)
    ensure_root
    case "${OS}" in
      Darwin)
        heal_pf_rules
        ;;
      Linux)
        IPT="$(iptables_bin)"
        if [[ -z "${IPT}" ]]; then
          echo "iptables not found" >&2
          exit 3
        fi
        heal_iptables_rules "${IPT}" "$@"
        ;;
      *)
        echo "unsupported OS: ${OS}" >&2
        exit 4
        ;;
    esac
    ;;
  *)
    echo "unknown action: ${ACTION}" >&2
    exit 1
    ;;
esac
