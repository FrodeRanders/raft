#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

for mode in java cpp mixed; do
  echo
  echo "==> Running Docker SRV smoke: ${mode}"
  docker/run-srv-smoke.sh "${mode}" "$@"
done

echo
echo "All Docker SRV smoke cases completed."
