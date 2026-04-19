# Local Jepsen Harness

This directory contains the first Jepsen harness for the repository: a local key-value test built around the shaded `raft-dist` jar.

## Scope

- 5 local voter nodes by default
- key-value application only
- linearizable CAS-register workload over a single hot key
- optional membership-change scenarios for join, role changes, and explicit removal
- optional multi-key and snapshot-stress runs for broader recovery coverage

## Prerequisites

- Java 21
- Clojure CLI
- built runnable jar
- non-interactive `sudo` for partition tests

Build the jar from the repository root:

```text
mvn -q -pl raft-dist -am package
```

The default Maven test path now also invokes a small Jepsen smoke suite through Surefire:

```text
mvn test
```

That smoke suite runs baseline, crash/restart, and partition Jepsen checks. If you need to skip them temporarily:

```text
mvn test -DskipJepsenTests=true
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
./run-local.sh --node-count 5 --nemesis persistence-loss-restart --snapshot-min-entries 5
./run-local.sh --node-count 5 --nemesis partition-leader
./run-local.sh --node-count 5 --nemesis partition-leader-minority
./run-local.sh --node-count 5 --nemesis membership-join-promote
./run-local.sh --node-count 5 --nemesis membership-demote
./run-local.sh --node-count 5 --nemesis membership-remove-follower
./run-local.sh --node-count 5 --nemesis membership-remove-leader
./run-local.sh --node-count 5 --nemesis membership-remove-follower-partition-leader
```

Run local Jepsen tests serially, not in parallel.
The harness uses host-local ports and a shared packet-filter helper, so two local runs at the same time can interfere with each other and produce invalid results.

Supported options:

- `--jar <path>`: explicit path to `raft-dist` jar
- `--time-limit <seconds>`: Jepsen workload duration
- `--concurrency <n>`: Jepsen client concurrency
- `--base-port <port>`: first node port, default `10080`
- `--node-count <n>`: number of local nodes, default `5`
- `--nemesis <mode>`: `none`, `crash-restart`, `persistence-loss-restart`, `partition-one`, `partition-leader`, `partition-leader-minority`, `membership-join-promote`, `membership-demote`, `membership-remove-follower`, `membership-remove-leader`, or `membership-remove-follower-partition-leader`
- `--nemesis-interval <seconds>`: delay between crash/restart actions
- `--workdir <path>`: local node state/log directory

## Scenario Guide

These Jepsen scenarios are meant to approximate concrete runtime conditions rather than synthetic protocol events in isolation.

- `none`: baseline cluster behavior with concurrent `write`, `read`, and `cas` load.
  This is the "healthy system" reference point.
- `crash-restart`: stops a node process without deleting its state directory, then starts it again from the same persisted data.
  This emulates JVM crash, host reboot, or process supervisor restart where the node's local Raft state survives.
- `persistence-loss-restart`: stops one non-leader follower, deletes its local `data` directory, then restarts it from empty state.
  This emulates disk replacement, volume loss, or state-directory corruption recovery where the node must rejoin from replicated log or snapshot transfer.
- `partition-one`: isolates one random node by blocking its local TCP port.
  This emulates a single-node network reachability failure, firewall issue, or local routing break.
- `partition-leader`: discovers the current leader and isolates it.
  This emulates leader connectivity loss and tests step-down, redirect/retry behavior, and re-election safety.
- `partition-leader-minority`: isolates the current leader together with one additional node.
  This emulates a split where the old leader is stranded in a minority and must not behave as if it still has quorum.
- `membership-join-promote`: starts a sixth node in join mode as a learner, then promotes it to voter under load.
  This emulates planned cluster expansion.
- `membership-demote`: demotes an existing non-leader voter to learner under load.
  This emulates shrinking quorum participation while keeping a replica available for catch-up reads or downstream replication.
- `membership-remove-follower`: removes a non-leader follower through explicit joint consensus plus finalize.
  This emulates planned decommissioning of a healthy replica.
- `membership-remove-leader`: removes the current leader through the same explicit reconfiguration flow.
  This emulates leader decommissioning or topology change during maintenance.
- `membership-remove-follower-partition-leader`: begins follower removal, waits for joint consensus, then isolates the leader before healing and finalizing.
  This emulates reconfiguration during an overlapping network fault.

The workload layer can also be varied:

- single-key CAS register: strongest focused signal for strict linearizability on one hot key
- multi-key workload: independent per-key histories to catch routing or state-application errors that do not appear on a single key
- aggressive snapshot settings: lower `--snapshot-min-entries` and smaller `--snapshot-chunk-bytes` to force snapshot creation and transfer during fault runs

## Layout

- `src/raft_jepsen/core.clj`: main Jepsen test definition and CLI entrypoint
- `src/raft_jepsen/db.clj`: local process lifecycle for N JVM nodes
- `src/raft_jepsen/client.clj`: shell-based client using the JSON KV CLI
- `src/raft_jepsen/nemesis.clj`: process crash/restart nemesis
- `src/raft_jepsen/observer.clj`: JSONL observability snapshots for correlation
- `scripts/partition.sh`: privileged local packet-filter helper for partition tests

## Notes

- The harness relies on the repository’s `command --json` and `query --json` surfaces, including KV `cas`.
- Successful writes are interpreted as committed/applied completions.
- The default workload mixes `write`, `read`, and `cas` operations over a small value set so CAS paths are exercised with both matches and mismatches.
- The harness defaults to 5 nodes now, but `--node-count 3` is still useful for faster local debugging.
- The partition helper re-execs itself through `sudo -n` when needed. Jepsen invokes it non-interactively, so passwordless sudo must be configured for the helper path.
- `mvn test` now includes a partition Jepsen smoke test, so the `sudo -n` prerequisite applies to normal Maven testing as well.
- On macOS, a practical sudoers entry is:

```text
<your-user> ALL=(root) NOPASSWD: /Users/froran/Projects/gautelis/raft/jepsen/scripts/partition.sh *
```

- Install that with `sudo visudo -f /etc/sudoers.d/raft-jepsen`.
- Verify the privilege path before running Jepsen:

```text
sudo -n /Users/froran/Projects/gautelis/raft/jepsen/scripts/partition.sh heal
```

- If you need to disable auto-escalation for debugging, set `RAFT_JEPSEN_SUDO_MODE=off`.
- Observations are written to `work/observations/cluster-events.jsonl` unless `--workdir` overrides the root.
- Snapshots are captured during DB setup/teardown, nemesis start/stop actions, and client-side `RETRY`/`REDIRECT`/failure paths.
- The implementation was scaffolded in this environment but not executed here because the Clojure CLI was not installed.
