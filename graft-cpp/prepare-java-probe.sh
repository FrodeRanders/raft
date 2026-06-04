#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

mvn -q -f ../pom.xml -pl :graft-cpp-java-probe -am install -DskipTests -DskipJepsenTests=true
