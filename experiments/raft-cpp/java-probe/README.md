# Java Probe

This is a disconnected Java-side interoperability probe for the C++ transport experiment.

It reuses the existing Java transport/runtime stack in two ways:

- `InteropProbe` sends real Java-originated requests to a target endpoint
- `JavaPeerMain` starts one bounded Java KV peer that can participate in a mixed Java/C++ smoke

That makes this directory useful for both:

- RPC interoperability checks against the C++ endpoint
- one bounded replicated-node check with a Java follower and a C++ leader

## Commands

```text
../run-java-probe.sh vote-request <host> <port> <candidate-id> <last-log-index> <last-log-term> [term]
../run-java-probe.sh append-entries <host> <port> <leader-id> <prev-log-index> <prev-log-term> <leader-commit> [term]
../run-java-probe.sh install-snapshot <host> <port> <leader-id> <last-included-index> <last-included-term> [term]
../run-java-probe.sh client-put <host> <port> <key> <value> [peer-id]
../run-java-probe.sh client-get <host> <port> <key> [peer-id]
../run-java-probe.sh telemetry <host> <port> [require-leader-summary]
../run-java-probe.sh cluster-summary <host> <port>
../run-java-peer.sh <peer-id> <host> <port> <timeout-millis> <data-dir> [peer-id@host:port ...] [--join-seed <peer-id@host:port>]
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
../run-mixed-smoke.sh
```

`run-interop-smoke.sh` starts the C++ stateful server, runs Java probe commands against it, and tears the server down afterward.

`run-mixed-smoke.sh` starts:

- one real Java KV peer
- one active persistent C++ peer

and then verifies a bounded replicated-node scenario:

- Java client command to the C++ leader
- successful Java client read from the C++ leader
- Java follower telemetry showing replicated/committed progress under the C++ leader
- Java follower redirect behavior for client query and cluster-summary

`run-mixed-smoke-java-leader.sh` exercises the reverse direction:

- one real Java KV leader
- one persistent C++ follower
- Java client write/read against the Java leader
- C++ follower state verification through `dump-state`
- C++ follower redirect metadata back to the Java leader

`run-mixed-membership-smoke.sh` exercises a bounded mixed-language membership flow:

- Java leader with one C++ voting follower
- admission of a second C++ node as learner
- learner catch-up on real replicated KV state before promotion
- learner promotion through the Java leader
- verification that the promoted C++ member becomes voting and continues replicating state

`run-mixed-suite.sh` runs both bounded mixed-language scenarios serially.
