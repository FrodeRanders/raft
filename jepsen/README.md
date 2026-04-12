# Local Jepsen Harness

This directory contains the first Jepsen harness for the repository: a local key-value test built around the shaded `raft-dist` jar.

## Scope

- 5 local voter nodes by default
- key-value application only
- linearizable register workload over a single hot key
- no membership changes yet

## Prerequisites

- Java 21
- Clojure CLI
- built runnable jar

Build the jar from the repository root:

```text
mvn -q -pl raft-dist -am package
```

## Run

From this directory:

```text
./run-local.sh
```

Optional overrides:

```text
./run-local.sh --time-limit 60 --concurrency 12 --base-port 11080
./run-local.sh --node-count 3
./run-local.sh --node-count 5 --nemesis crash-restart
```

Supported options:

- `--jar <path>`: explicit path to `raft-dist` jar
- `--time-limit <seconds>`: Jepsen workload duration
- `--concurrency <n>`: Jepsen client concurrency
- `--base-port <port>`: first node port, default `10080`
- `--node-count <n>`: number of local nodes, default `5`
- `--nemesis <mode>`: `none`, `crash-restart`, or `partition-one`
- `--nemesis-interval <seconds>`: delay between crash/restart actions
- `--workdir <path>`: local node state/log directory

## Layout

- `src/raft_jepsen/core.clj`: main Jepsen test definition and CLI entrypoint
- `src/raft_jepsen/db.clj`: local process lifecycle for N JVM nodes
- `src/raft_jepsen/client.clj`: shell-based client using the JSON KV CLI
- `src/raft_jepsen/nemesis.clj`: process crash/restart nemesis
- `src/raft_jepsen/observer.clj`: JSONL observability snapshots for correlation
- `scripts/partition.sh`: privileged local packet-filter helper for partition tests

## Notes

- The harness relies on the repository’s `command --json` and `query --json` surfaces.
- Successful writes are interpreted as committed/applied completions.
- The harness defaults to 5 nodes now, but `--node-count 3` is still useful for faster local debugging.
- `crash-restart` stops a node process without deleting its state directory, then starts it again from the same persisted data.
- `partition-one` isolates one random node by blocking its local TCP port through `pfctl` on macOS or `iptables` on Linux.
- The partition helper requires root privileges and modifies local packet-filter rules; review it before use.
- Observations are written to `work/observations/cluster-events.jsonl` unless `--workdir` overrides the root.
- Snapshots are captured during DB setup/teardown, nemesis start/stop actions, and client-side `RETRY`/`REDIRECT`/failure paths.
- The implementation was scaffolded in this environment but not executed here because the Clojure CLI was not installed.
