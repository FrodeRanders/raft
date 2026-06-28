#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

argv=(
  mvn -q -f java-probe/pom.xml exec:java
  -Dexec.mainClass=org.gautelis.raft.cpp.InteropProbe
  "-Dexec.args=$*"
  "-Dexec.jvmArgs=-Djava.net.preferIPv4Stack=true"
)

exec "${argv[@]}"
