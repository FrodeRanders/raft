# Java Probe

This is a disconnected Java-side interoperability probe for the C++ transport experiment.

It reuses the existing Java Netty transport client and sends real Raft RPCs to a target endpoint, which is useful for verifying that the C++ listener can decode and answer Java-originated messages.

## Commands

```text
../run-java-probe.sh vote-request <host> <port> <candidate-id> <last-log-index> <last-log-term> [term]
../run-java-probe.sh append-entries <host> <port> <leader-id> <prev-log-index> <prev-log-term> <leader-commit> [term]
../run-java-probe.sh install-snapshot <host> <port> <leader-id> <last-included-index> <last-included-term> [term]
```

## Prerequisite

The probe depends on the normal Java modules being available in the local Maven repository. A simple way to ensure that is:

```text
mvn -q -pl raft-transport-netty -am install -DskipTests
```

Then you can run, for example:

```text
../run-java-probe.sh vote-request 127.0.0.1 11080 java-candidate 0 0
```

For the current end-to-end mixed-language check, use:

```text
../run-interop-smoke.sh
```

That script starts the C++ stateful server, runs the Java probe commands against it, and tears the server down afterward.
