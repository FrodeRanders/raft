#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

exec mvn -q -f java-probe/pom.xml exec:java -Dexec.args="$*"
