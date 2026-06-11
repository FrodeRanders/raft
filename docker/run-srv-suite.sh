#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

modes=(java cpp rust mixed)
if [[ $# -gt 0 ]]; then
  case "$1" in
    java|cpp|rust|mixed)
      modes=("$1")
      shift
      ;;
    -h|--help)
      cat <<'EOF'
Usage: docker/run-srv-suite.sh [java|cpp|rust|mixed] [--no-build]

Runs all Docker SRV smoke cases by default, or only the selected mode.
Remaining options are passed through to docker/run-srv-smoke.sh.
EOF
      exit 0
      ;;
  esac
fi

for mode in "${modes[@]}"; do
  echo
  echo "==> Running Docker SRV smoke: ${mode}"
  docker/run-srv-smoke.sh "${mode}" "$@"
done

echo
echo "All Docker SRV smoke cases completed."
