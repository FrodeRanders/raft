# Jepsen Hardening Backlog

This backlog captures scenarios that are not yet fully covered by the current
Jepsen suites. The order reflects practical value for Raft safety and the
library's intended use as a Java/C++ clustering substrate.

## Priority 1: Clock Skew And Read Leases

Status: local harness implemented; Docker/SRV and combined scenarios still open

Read-lease behavior depends on time assumptions. Add a clock-skew scenario that
stresses leader leases, follower reads/redirects, and leadership changes while
client reads are checked for linearizability.

Useful directions:

- injectable logical clock in Java and C++ runtimes, controlled by test hooks
- local Jepsen nemesis that restarts one node with an advanced Raft clock
- read-heavy workload variant that maximizes lease-read exposure
- combined clock-skew plus leader partition scenario

## Priority 2: Process Pauses

Status: implemented and validated in local and Docker/SRV harnesses

Crash/restart is not equivalent to a paused process. A paused leader may resume
with stale assumptions after the rest of the cluster has moved on. Add a nemesis
that sends `SIGSTOP`/`SIGCONT` to local node processes and Docker containers.

## Priority 3: Snapshot Boundary Failures

Status: post-compaction restart implemented but not yet deterministic on all
developer machines; creation/transfer crash hooks still open

Existing snapshot stress lowers thresholds, but does not deliberately crash at
snapshot creation, snapshot installation, or immediately after compaction.
The MacBook Pro M2 quality analysis showed that `snapshot-boundary-restart` can
time out even when the workload remains linearizable. Treat that as a harness
precondition failure, not a Raft safety failure: the nemesis did not prove that a
non-leader node had actually compacted before trying to restart it.

Useful directions:

- split `snapshot-boundary-restart` into a deterministic warm-up and the actual
  restart fault
- explicitly drive enough unique writes before the nemesis runs, instead of
  relying on the bounded random workload to happen to trigger compaction
- wait for and require observed `snapshotIndex > 0` on a non-leader before
  claiming the test reached a snapshot boundary
- return first-class timeout diagnostics when the precondition is not met:
  include recent per-node telemetry samples, observed leader, snapshot index and
  term, telemetry success/failure, and the required predicate (implemented)
- reinterpret `snapshot-partition-leader` as "leader partition with low snapshot
  thresholds configured" until it also requires an observed snapshot boundary
- crash during snapshot creation
- crash during snapshot transfer/install
- restart just after compaction
- verify catch-up and reads after the boundary

## Priority 4: Mixed Java/C++ Extended Matrix

Status: expanded in local mixed suite; snapshot-transfer and full Docker/SRV mixed matrix still open

Mixed runs exist, but the full extended suite is still mostly local/static.
Extend Java/C++ combinations across leader partition, leader removal, follower
removal during joint consensus, snapshot transfer, persistence-loss restart, and
both Java-led and C++-led clusters.

## Priority 5: Docker/SRV Dynamic Membership

Status: open

Docker/SRV Jepsen currently uses a fixed three-node SRV zone. Dynamic membership
needs generated Compose and DNS records so join/promote/demote/remove scenarios
can run in the same DNS-seeded deployment model.

## Priority 6: Disk Corruption And Torn Writes

Status: open

Persistence-loss restart is covered, but not corrupted state, truncated logs,
torn snapshots, or stale/copy-restored state. Add storage fault injection around
Raft metadata, log entries, snapshots, and cluster configuration.

## Priority 7: Read-Heavy Soak

Status: open

Reference-data use is read-dominant. Add longer-running read-heavy tests with
low write rate, high read rate, periodic restarts/partitions, and snapshot churn.

## Priority 8: Stronger Workload Evidence

Status: unique operation values and operation-limit CLI implemented;
regression/stress defaults still open

Current smoke/regression histories often contain only tens of successful
operations, and the value domain is intentionally tiny (`v0` through `v4`).
That is useful for quick feedback, but it weakens Jepsen evidence because
repeated values make stale reads easier to linearize.

Useful directions:

- add a `--unique-values` mode for extended/stress runs
- generate values that identify the operation, and identify the process when
  Jepsen generator context exposes it, for example `p7-op42` or structured
  equivalents encoded by the client
- add an `--operation-limit` option so smoke tests can remain bounded while
  regression/stress profiles can run much longer or be time-limited only
- keep the small value set available for fast smoke runs where readability and
  speed matter more than maximum checker discrimination

## Priority 9: Suite Profiles And Reproducibility

Status: open

The suite currently behaves mostly like a local smoke/regression suite. Split
the intended uses so failures and runtime expectations are easier to interpret
across Apple Silicon, older Intel macOS, and Docker/SRV runs.

Useful directions:

- define `smoke` as quick feedback: 10-20 seconds, low concurrency, small value
  set, bounded operations
- define `regression` as normal confidence: 60-120 seconds, higher operation
  count, unique values, all supported nemeses
- define `stress` as long-form validation: 10-60 minutes, high concurrency,
  repeated seeds, and heavier snapshot/read workloads
- print and persist the random seed for each run so timing-sensitive failures
  can be reproduced
- record profile, seed, operation limit, value mode, node implementations, and
  nemesis settings in the Jepsen store metadata or run log

## Additional Network Faults

Status: open

Current partitions isolate nodes. Add asymmetric reachability, packet loss, high
latency, message reordering, and flapping links where the host platform supports
those faults.

## Invalid Or Overlapping Membership Operations

Status: open

Add administrative stress cases: promote while removal is active, concurrent
join requests, remove during promote, finalize too early, repeated finalize, and
leader crash mid-joint-consensus.
