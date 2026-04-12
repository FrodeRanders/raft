# Minimal Jepsen KV Model

This document defines the first Jepsen model for this repository. It is intentionally narrow: a 3-node cluster, the key-value application only, no membership changes yet.

## Scope

- System under test: `raft-dist` running the default key-value application.
- Cluster size: 3 voters.
- Client surface:
  - `command --json put <target> <key> <value>`
  - `command --json delete <target> <key>`
  - `query --json get <target> <key>`
- Out of scope for the first harness:
  - `clear`
  - learners
  - join/reconfigure/promote/demote/finalize flows
  - snapshot-specific validation

## Correctness Goal

The first Jepsen checker should validate linearizable single-register behavior per key.

Recommended first workload:
- operate on a single hot key such as `k`
- writes are `put(k, value)`
- reads are `get(k)`

This keeps the first checker simple and aligns directly with the current linearizable read path in the leader.

## Operation Mapping

Jepsen operation to CLI mapping:

- `:write v`
  - invoke: `java -jar ... command --json put <node> k <v>`
  - success when JSON contains:
    - `"success": true`
    - `"status": "ACCEPTED"`
  - completion means the write was committed and applied

- `:read`
  - invoke: `java -jar ... query --json get <node> k`
  - success when JSON contains:
    - `"success": true`
    - `"status": "OK"`
  - value is taken from:
    - `"result": { "found": true, "value": "..." }`
    - or `"result": { "found": false }` for nil/absent

- Optional later extension:
  - `:delete`
  - map to `command --json delete <node> k`
  - interpret as writing `nil`

## Client Semantics

The Jepsen client should treat responses as follows:

- `ACCEPTED` on `command`
  - operation succeeds
  - no follow-up confirmation read is required

- `OK` on `query`
  - operation succeeds with returned value

- `REDIRECT`
  - client may retry against the returned leader endpoint
  - if the harness uses the jar CLI only, it may instead treat this as retryable and re-issue to another node

- `RETRY`
  - transient failure
  - record as `:fail` or retry, but do not treat as success
  - especially relevant for reads during unstable leadership

- `REJECTED`, `INVALID`, `UNAUTHENTICATED`, `FORBIDDEN`
  - test/harness failure for the minimal KV model
  - these should not appear in the normal first Jepsen run

## Minimal Generator

Recommended initial generator:

- 70% writes
- 30% reads
- values are unique monotonic strings, for example `client-3-op-42`
- target node chosen uniformly from the 3 nodes

This is enough to exercise:
- leader election churn
- follower redirects
- leader read barriers
- quorum loss behavior under later nemesis work

## Checker

Recommended first checker:

- Jepsen `register` model over a single key
- linearizability checker over successful operations only

Interpretation:

- initial value: `nil`
- `put(k, v)` sets register to `v`
- `get(k)` returns `v` or `nil`

## Failure Handling Rules

For the first harness, keep the result policy simple:

- process timeout while invoking CLI:
  - treat as `:info`

- malformed JSON:
  - treat as harness error

- non-zero exit with parseable JSON:
  - use returned status to classify the op

- non-zero exit without JSON:
  - treat as `:fail`

## Deployment Assumptions

The first Jepsen run should assume:

- static 3-node cluster
- fixed ports
- persistent data directories per node
- no authentication
- one local JVM per node process

Recommended JVM startup shape:

- `java -Draft.data.dir=<node-data-dir> -jar raft-dist/target/raft-<version>.jar <self> <peer1> <peer2>`

## Observability for the First Harness

The minimal model does not require extra product changes, but the harness should collect:

- process stdout/stderr
- `telemetry --json --summary`
- `cluster-summary --json`

These are not part of the correctness checker yet; they are for debugging failing histories.

## Exit Criteria

The minimal KV Jepsen model is complete when:

- a 3-node Jepsen run can start and stop the cluster
- writes and reads are issued through the JSON CLI surface
- successful histories can be checked as a linearizable register
- redirects and retries are handled without misclassifying operations as successful commits
