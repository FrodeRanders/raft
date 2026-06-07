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

Status: open

Existing snapshot stress lowers thresholds, but does not deliberately crash at
snapshot creation, snapshot installation, or immediately after compaction.

Useful directions:

- crash during snapshot creation
- crash during snapshot transfer/install
- restart just after compaction
- verify catch-up and reads after the boundary

## Priority 4: Mixed Java/C++ Extended Matrix

Status: open

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
