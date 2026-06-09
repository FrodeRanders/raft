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

Status: deterministic warm-up and snapshot creation/transfer crash nemeses
implemented; `snapshot-partition-leader` interpretation documented

The harness now drives a deterministic burst of 50 unique-valued writes before
the `snapshot-boundary-restart` nemesis begins polling. This guarantees that
enough entries exist to trigger compaction (with `--snapshot-min-entries 3`),
eliminating the Apple Silicon timeout caused by the bounded random workload
completing before any follower compacted.

Useful directions:

- [x] split `snapshot-boundary-restart` into a deterministic warm-up and the actual
  restart fault
- [x] explicitly drive enough unique writes before the nemesis runs, instead of
  relying on the bounded random workload to happen to trigger compaction
- [x] wait for and require observed `snapshotIndex > 0` on a non-leader before
  claiming the test reached a snapshot boundary
- [x] return first-class timeout diagnostics when the precondition is not met:
  include recent per-node telemetry samples, observed leader, snapshot index and
  term, telemetry success/failure, and the required predicate
- [ ] reinterpret `snapshot-partition-leader` as "leader partition with low snapshot
  thresholds configured" until it also requires an observed snapshot boundary
  (interpretation documented in `docs/jepsen-quality-analysis.md`)
- [x] crash during snapshot creation
- [x] crash during snapshot transfer/install
- [x] restart just after compaction
- [x] verify catch-up and reads after the boundary

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

Status: unique values enabled in extended suite defaults; stress profile with
high operation limits configured

The extended suite now uses `--unique-values true` across all 14 cases, generating
values like `p7-op42` that identify both the Jepsen process and the operation
counter. This gives Knossos much stronger discriminating power against stale reads.
Smoke tests retain the small value set for fast, readable feedback. The stress
profile uses 5000-operation limits with unique values for long-form validation.

Useful directions:

- [x] add a `--unique-values` mode for extended/stress runs
- [x] generate values that identify the operation, and identify the process when
  Jepsen generator context exposes it, for example `p7-op42` or structured
  equivalents encoded by the client
- [x] add an `--operation-limit` option so smoke tests can remain bounded while
  regression/stress profiles can run much longer or be time-limited only
- [x] keep the small value set available for fast smoke runs where readability and
  speed matter more than maximum checker discrimination

## Priority 9: Suite Profiles And Reproducibility

Status: smoke/regression/stress profiles defined; random seed reported and persisted
in observer events; run metadata saved to `metadata.edn` in workdir

Profiles are now clearly separated:

- `smoke`: 10s, concurrency 10, small value set, 3 cases (baseline, crash-restart, partition-one)
- `extended` (regression): 20-60s, concurrency 10, unique values, 16 cases covering all local nemeses
- `stress`: 300s (5min), concurrency 20, unique values, 5000 operation limit, 9 cases

The random seed is generated from `System/nanoTime` at startup, printed to stdout,
and persisted in every JSONL observer event for post-mortem correlation.

Useful directions:

- [x] define `smoke` as quick feedback: 10-20 seconds, low concurrency, small value
  set, bounded operations
- [x] define `regression` as normal confidence: 60-120 seconds, higher operation
  count, unique values, all supported nemeses
- [x] define `stress` as long-form validation: 10-60 minutes, high concurrency,
  repeated seeds, and heavier snapshot/read workloads
- [x] print and persist the random seed for each run so timing-sensitive failures
  can be reproduced
- [x] record profile, seed, operation limit, value mode, node implementations, and
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
