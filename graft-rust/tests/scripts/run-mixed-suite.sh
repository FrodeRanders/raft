#!/usr/bin/env bash
# Master orchestrator for all Rust↔Java cross-language smoke tests.
# Runs each scenario sequentially. Mirrors graft-cpp/run-mixed-suite.sh.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "${SCRIPT_DIR}"

echo "==> Scenario 1: Rust leader, Java follower"
./run-mixed-smoke-rust-leader.sh

echo
echo "==> Scenario 2: Java leader, Rust follower"
./run-mixed-smoke-java-leader.sh

echo
echo "==> Scenario 3: Raw RPC interop (VoteRequest, AppendEntries, InstallSnapshot)"
./run-interop-smoke.sh

echo
echo "Mixed Java/Rust suite passed."
