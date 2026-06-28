#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

cmd=(
  mvn -q -f java-probe/pom.xml exec:java
  -Dexec.mainClass=org.gautelis.raft.cpp.JavaPeerMain
  "-Dexec.args=$*"
)

if [[ -n "${RAFT_JAVA_PEER_JVM_ARGS:-}" ]]; then
  cmd+=("-Dexec.jvmArgs=${RAFT_JAVA_PEER_JVM_ARGS} -Djava.net.preferIPv4Stack=true")
else
  cmd+=("-Dexec.jvmArgs=-Djava.net.preferIPv4Stack=true")
fi

exec "${cmd[@]}"
