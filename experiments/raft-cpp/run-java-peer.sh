#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

cmd=(
  mvn -q -f java-probe/pom.xml exec:java
  -Dexec.mainClass=org.gautelis.raft.experiments.cpp.JavaPeerMain
  "-Dexec.args=$*"
)

if [[ -n "${RAFT_JAVA_PEER_JVM_ARGS:-}" ]]; then
  cmd+=("-Dexec.jvmArgs=${RAFT_JAVA_PEER_JVM_ARGS}")
fi

exec "${cmd[@]}"
