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
echo "==> Scenario 4: C++ leader admits and promotes Java learner"
./run-mixed-membership-cpp-leader-smoke.sh

echo
echo "==> Scenario 5: C++ leader snapshot catch-up to lagged Java follower"
./run-mixed-snapshot-smoke.sh

echo
echo "Mixed Java/C++ suite passed."
