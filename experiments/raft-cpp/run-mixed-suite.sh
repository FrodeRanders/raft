#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

echo "==> Scenario 1: C++ leader, Java follower"
./run-mixed-smoke.sh

echo
echo "==> Scenario 2: Java leader, C++ follower"
./run-mixed-smoke-java-leader.sh

echo
echo "==> Scenario 3: Java leader admits and promotes C++ learner"
./run-mixed-membership-smoke.sh

echo
echo "Mixed Java/C++ suite passed."
