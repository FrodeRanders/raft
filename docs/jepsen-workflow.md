## Jepsen Developer Workflow

This note keeps the classic Maven test loop and the local Jepsen loop aligned.

`mvn test` now includes a small Jepsen smoke suite through Surefire:

- baseline 5-node Jepsen smoke run
- 5-node crash/restart smoke run
- 5-node partition smoke run

That means the default Maven test path now depends on:

- Java 21
- Clojure CLI
- a working local Jepsen environment
- non-interactive `sudo` for the partition helper

### 1. Fast Java feedback

Run the focused unit and adapter tests first when changing the KV, protocol, or runtime layers:

```text
mvn -q -pl raft-tests -am -Dsurefire.failIfNoSpecifiedTests=false -Dtest=KeyValueStateMachineTest,KeyValueCliSupportTest,ClientResponseHandlerTest test
```

Use the full module test suite when touching broader Raft behavior:

```text
mvn -q -pl raft-tests -am test
```

If you need to skip the Jepsen smoke tests temporarily:

```text
mvn test -DskipJepsenTests=true
```

### 2. Build the runnable jar

The local Jepsen harness runs against `raft-dist`, so rebuild it after Java or protobuf changes:

```text
mvn -q -pl raft-dist -am package
```

### 3. Run Jepsen locally

Start with a short smoke test:

```text
cd jepsen
./run-local.sh --time-limit 10 --concurrency 10 --node-count 5 --workdir /tmp/raft-jepsen-smoke
```

Then run one fault mode at a time:

```text
./run-local.sh --time-limit 20 --concurrency 10 --node-count 5 --nemesis crash-restart --workdir /tmp/raft-jepsen-crash
./run-local.sh --time-limit 20 --concurrency 10 --node-count 5 --nemesis partition-leader --workdir /tmp/raft-jepsen-partition-leader
./run-local.sh --time-limit 20 --concurrency 10 --node-count 5 --nemesis partition-leader-minority --workdir /tmp/raft-jepsen-partition-leader-minority
./run-local.sh --time-limit 20 --concurrency 10 --node-count 5 --nemesis membership-join-promote --workdir /tmp/raft-jepsen-membership-join
./run-local.sh --time-limit 20 --concurrency 10 --node-count 5 --nemesis membership-demote --workdir /tmp/raft-jepsen-membership-demote
./run-local.sh --time-limit 20 --concurrency 10 --node-count 5 --nemesis membership-remove-follower --workdir /tmp/raft-jepsen-membership-remove
./run-local.sh --time-limit 20 --concurrency 10 --node-count 5 --nemesis membership-remove-leader --workdir /tmp/raft-jepsen-membership-remove-leader
./run-local.sh --time-limit 20 --concurrency 10 --node-count 5 --nemesis membership-remove-follower-partition-leader --workdir /tmp/raft-jepsen-membership-remove-follower-partition-leader
```

Run local Jepsen tests serially, not in parallel. The harness shares host-local ports and packet-filter state.

The Surefire-driven Jepsen tests already do this and use fixed private port ranges.

### 4. Check results

Look at the latest Jepsen result file:

```text
cat jepsen/store/current/results.edn
```

Healthy runs end with:

```text
:linearizable {:valid? true, ...}
:timeline {:valid? true}
:valid? true
```

For fault runs, also inspect:

```text
cat jepsen/store/current/history.edn
cat /tmp/raft-jepsen-smoke/observations/cluster-events.jsonl
```

### 5. Partition prerequisites

The partition nemeses need non-interactive privilege for `jepsen/scripts/partition.sh`. On macOS, configure `sudoers` as documented in `jepsen/README.md` and verify:

```text
sudo -n /Users/froran/Projects/gautelis/raft/jepsen/scripts/partition.sh heal
```

Without that setup, the partition Jepsen smoke test inside `mvn test` will fail with an explicit prerequisite message.
