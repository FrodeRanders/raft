# Jepsen Harness Plan

This file tracks the incremental work needed to add a Jepsen test harness alongside the existing Maven/JUnit suite.

## Checklist

- [x] Add machine-readable JSON output for KV `command` operations.
- [x] Add machine-readable JSON output for KV `query` operations.
- [x] Change client write acknowledgement semantics so success means committed/applied, not merely accepted for replication.
- [x] Define the minimal Jepsen test model around the KV application only.
- [x] Build a minimal local Jepsen harness for a 3-node cluster.
- [x] Add process crash/restart nemeses.
- [x] Add network partition nemeses.
- [x] Add observability hooks to correlate Jepsen histories with node telemetry and cluster-summary output.
- [x] Expand the harness to 5-node runs.
- [x] Add leader-targeted partition nemesis scenarios.
- [x] Add majority-loss partition scenarios.
- [x] Add longer 5-node Jepsen runs for repeated election and recovery cycles.
- [x] Add membership-change scenarios covering join, promote/demote, and finalize flows.
- [x] Extend the KV workload beyond a plain register model, ideally with CAS support.
- [x] Document developer workflow for running classic tests and Jepsen tests side by side.

## Next Backlog

- [x] Add `membership-remove-leader`.
- [x] Combine membership changes with leader partitions or crash/restart nemeses.
- [x] Run longer non-Maven Jepsen scenarios for the strongest membership/fault combinations.
- [x] Add 7-node adversarial runs.
- [x] Add multi-key workloads using independent histories.
- [x] Add snapshot/compaction scenarios under faults.
- [x] Add disk/persistence failure scenarios.

## Current Status

- `Classic test suite`: in place under `raft-tests`.
- `Runnable external node process`: in place via `raft-dist`.
- `Cluster lifecycle scripts`: in place via `scripts/start_raft.sh` and `scripts/kill_raft.sh`.
- `Operational JSON output`: partially in place for telemetry and cluster administration commands.
- `KV JSON output`: done.
- `Commit/applied write acknowledgements`: done.
- `Minimal KV Jepsen model`: done in `docs/jepsen-kv-model.md`.
- `Jepsen harness`: local scaffold in `jepsen/`, now defaulting to 5-node runs and including opt-in crash/restart and partition nemeses plus JSONL observation capture.
- `Partition validation`: passing 5-node `partition-one` run with a real isolated node and linearizable result.
- `Leader-targeted partition validation`: passing short runs for `partition-leader` and `partition-leader-minority`.
- `Longer 5-node runs`: passing when executed serially. Earlier failures were caused by running two local Jepsen processes in parallel against shared host ports and packet-filter state, which made those results non-diagnostic.
- `Longer 5-node runs`: 60-second `partition-leader` CAS validation passes after correcting the Jepsen client to treat `UNREACHABLE` and `TIMEOUT` writes/CAS operations as indeterminate outcomes instead of definite failures. See `jepsen/store/raft-kv-local-5n/20260419T115044.891+0200/results.edn`.
- `Membership scenarios`: validated for `membership-join-promote`, `membership-demote`, `membership-remove-follower`, and now includes a `membership-remove-leader` nemesis for current-leader removal under load.
- `Membership + fault combinations`: now includes `membership-remove-follower-partition-leader`, which starts follower removal, isolates the leader during joint consensus, then heals and finalizes to a stable removed-member configuration.
- `Longer membership + fault combinations`: validated with a 60-second `membership-remove-follower-partition-leader` run. See `jepsen/store/raft-kv-local-5n/20260419T122527.290+0200/results.edn`.
- `7-node adversarial runs`: validated with a 7-node `partition-leader-minority` CAS run. See `jepsen/store/raft-kv-local-7n/20260419T115501.822+0200/results.edn`.
- `Stronger KV workload`: the KV demo now exposes a first-class CAS command/result path, and the Jepsen harness now drives a single-key `cas-register` workload with mixed `write`, `read`, and `cas` operations.
- `Multi-key workload`: validated with independent per-key linearizability checks under `partition-leader`. See `jepsen/store/raft-kv-local-5n/20260419T122319.068+0200/results.edn`.
- `Snapshot/compaction under faults`: validated by forcing aggressive snapshot settings during a faulted `partition-leader` run. See `jepsen/store/raft-kv-local-5n/20260419T122921.719+0200/results.edn`.
- `Disk/persistence failures`: validated with `persistence-loss-restart`, which wipes one follower's local data directory before restart and verifies recovery under load. See `jepsen/store/raft-kv-local-5n/20260419T123252.350+0200/results.edn`.
- `Developer workflow`: documented in `docs/jepsen-workflow.md` with the Maven test/build loop, local Jepsen commands, and result inspection flow.

## Notes

- The write acknowledgement change is a prerequisite for a clean Jepsen history, because the current success path acknowledges acceptance for replication rather than committed/applied completion.
- The recommended implementation order is:
  1. JSON output for KV commands and queries.
  2. Commit/applied acknowledgements for writes.
  3. Minimal 3-node Jepsen harness.
  4. Crash nemeses.
  5. Network partitions.
  6. Leader-targeted partition scenarios.
  7. Majority-loss partition scenarios.
  8. Longer 5-node runs.
  9. Membership-change scenarios.
  10. Stronger KV workloads, ideally including CAS.
  11. Developer workflow notes for running Maven and Jepsen validation side by side.
