# Consensus fault validation, 28–29 October 2026

The campaign found additional safety and parity defects after the first
corrections in [consensus-hardening.md](../consensus-hardening.md). Original
failures and passing repeats are retained separately in the
[evidence record](2026-10-29-consensus.json), with commands, operation counts,
decisive history excerpts and hashes. The baseline revision is
`1a818d416a40330bea2055e03f61580679baa9f6` plus local changes. Binary hashes
describe the final artifacts at collection, not every intermediate failed run.

## Failures and corrections

The first mixed snapshot/fault history failed linearizability. A C++ client
completed `w2-v0006` at 38.687 seconds, then completed `w2-v0007` at 39.156.
A read invoked at 39.815 returned the older `w2-v0006` at 43.342. The evidence
includes the operations and a concurrent Java write; that different value
cannot explain the observed reversion. This is an observed bad history, not a
claim that a single later change explains every possible execution.

Investigation found these independent defects:

- C++ returned write success after any follower acknowledgement, without
  requiring the submitted entry to be committed/applied. Completion now checks
  its index and submitting term atomically. Rust's index-only commit wait could
  likewise complete after a leadership change; it now captures the submission
  term under the append lock and rechecks it through response construction.
  Unconfirmed C++ writes return `ACCEPTED_NOT_COMMITTED`. C++ keeps application
  results from the most recent 1024 compacted indices for pending replies.
- Rust's KV application wrote JSON snapshots while Java/C++ expected binary.
  A failed decode could retain old application contents while installation
  advanced Raft metadata. New Rust writes use binary, legacy JSON remains
  readable, and invalid native restores fail. Rust's workload no longer bypasses
  core compaction with separate prefix deletion and unwrapped snapshot writes.
- Native binary helpers used ordinary UTF-8 in a Java `writeUTF` format.
  Shared fixtures now cover NUL and supplementary Unicode, and native codecs
  enforce encoded length limits and reject malformed/trailing records.
- Rust PROMOTE copied the supplied old `LEARNER` role. The accepted promotion
  therefore could leave the node a learner. Role actions now choose the new
  role explicitly. Committed joiners get replication cursors; status reports
  preserve each side of a role transition. The demo's 500 ms node deadline was
  also shorter than its 750 ms heartbeat interval; its election base is now
  1500 ms.
- C++ JOINT inserted the existing leader back into an explicitly proposed
  configuration that excluded it. Leader removal now preserves the caller's
  complete proposal. Its direct regression and mixed removal repeat pass.

Membership failures are preserved too: the first promotion reached a former
leader and was explicitly rejected before admission; the next timed out waiting
for an accepted promotion to become a voter. Leader removal initially timed out
because the proposed joint membership still contained the leader.

## Harness corrections

Crash scenarios now force process termination and bypass shutdown hooks.
Original ordinary-stop restart captures are identified separately. Docker
partition/heal commands verify actual network attachment and surface failures.
Docker's event captures independently retain connect/disconnect sequences.
The checker records missing write/CAS confirmation, including a write `RETRY`, as uncertain, rather than
assuming the command had no effect. Promotion rediscovery is limited to the
specific C++ rejection emitted before admission; uncertain mutations are not
replayed.

The first Java-only container run passed its original checker but had **zero
confirmed mutations**. It is retained as limited evidence. The checker now
requires a successful mutation and read. Java clients targeting Java containers
run there so redirects can resolve advertised Docker SRV names/internal ports.
Native direct probes continue using host endpoints. Container campaigns were
repeated after this correction.

An attempted `--seed` invocation was rejected before any workload ran. The
printed seed is metadata, and the current harness does not seed every random
choice. The final snapshot campaign is a fresh run, not a deterministic replay.

## Results

The Java suite passed 163 of 166 tests with three skipped. Rust passed 43
executable tests with one ignored documentation example. C++ passed 43 cases and
348 assertions. Harness regressions passed 10 tests and 30 assertions. Rust
format checks passed. The permanent three-language smoke test also passed a
write and two fresh fenced reads with each language leading once.

All 15 listed scenarios have a passing history with confirmed mutation/read
progress. Earlier failures and the first limited Java container run remain
separate records. Counts are from the selected passing history, not totals
across repeats.

| Scenario | Confirmed mutations | Successful reads | Result |
| --- | ---: | ---: | --- |
| Java baseline | 6 | 9 | Pass |
| Java orderly restart (original harness) | 14 | 11 | Pass |
| Java process pause | 8 | 15 | Pass |
| Java observed-compaction restart | 30 | 21 | Pass |
| Mixed baseline | 4 | 16 | Pass |
| Mixed orderly restart (original harness) | 14 | 21 | Pass |
| Mixed snapshot/catch-up fault repeat | 15 | 15 | Pass |
| Mixed learner join and promotion | 20 | 26 | Pass |
| Mixed leader removal | 8 | 15 | Pass |
| Docker Java partition, corrected routing | 7 | 5 | Pass |
| Docker mixed observed-leader partition | 5 | 5 | Pass |
| Docker mixed process pause | 10 | 10 | Pass |
| Java abrupt process restart | 12 | 16 | Pass |
| Mixed abrupt process restart | 7 | 12 | Pass |
| Final mixed snapshot/catch-up campaign | 24 | 22 | Pass |

Charlotte's native working tree passed the 17-test no-network boot and both
20-test socket-LAN guests, including catalog replication, remote invocation and
generation-fenced unregister. Killing the owned QEMU after acknowledged term 1
and rebooting the same virtual NVMe disk advanced to term 2; the ordinary
process-restart fixture then reached term 3. Recovery again passed 17/0/0.
The evidence includes source/kernel/service hashes and decisive serial excerpts.
Charlotte also contains its own self-contained report at
`docs/reports/audits/2026-10-29-raft-native-validation.md`.

## Reproduction and limits

Build a fresh C++ tree with CMake, the Rust `graft-kv` binary, and the Java shaded
`raft-dist/target/raft-1.0-SNAPSHOT.jar`. Pass that exact JAR; a wildcard can also
select documentation artifacts. Run each recorded `clojure -M:run` command from
`jepsen`, replacing `<cpp-build>` and `<new-workdir>`. Build Docker images through
the appropriate `docker/compose-srv-*.yml` file. Java's container now uses JRE 25,
matching the project's emitted bytecode. Run host Jepsen campaigns serially and
use isolated Docker projects. Harness checks are `clojure -M:test`.

These are short campaigns, with bounded operation counts and unique write
values; they are not soak tests or an exhaustive proof. The snapshot nemesis
cycles recovery/catch-up and process loss but does not observe the precise byte
at which a transfer is interrupted. Passing a repeat does not erase an original
failure or prove exact schedule reproduction. Older histories' mutation/read
counts are recorded even when their checker did not enforce progress.

Process loss leaves the host filesystem and caches alive. Physical power loss,
native interruption during log/snapshot publication, simultaneous multi-voter
restart, new formal-model execution and throughput remain unqualified. Native
Charlotte capability IPC/relmsg fixtures do not establish a mixed TCP cluster
with the three upstream implementations. Existing Charlotte management TLS
availability limitations remain separate.

Java's `GRF2` durable format is a forward upgrade. Older binaries cannot read
it; retain existing data backups before upgrading. The detailed record and
failure policy are documented in the hardening note.
