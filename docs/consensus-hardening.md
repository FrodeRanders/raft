# Consensus safety corrections

The Java, C++ and Rust implementations now share regression traces for delayed
replication replies, append retries, stale snapshots and read authority. These
corrections address the October 2026 review; passing these tests is not a complete
Raft proof or a power-loss qualification.

| Boundary | Required behavior | Regression evidence |
| --- | --- | --- |
| Replication replies | Require the current leadership term and the contacted peer; preserve confirmed progress on reordered replies. | Java `RaftSafetyRegressionTest`; Rust `safety_regression`; C++ `raft_safety_regression_tests`. Rust's runtime delegates replies to the fenced core. |
| Log matching | An identical shorter retry retains the existing suffix. Commit progress cannot decrease, and a heartbeat cannot commit an unverified follower suffix. | Java heartbeat/acknowledgement tests; Rust and C++ shorter-retry tests. |
| Snapshots | Ignore checkpoints at or below committed progress. Bind continuation bytes to sender, RPC term, snapshot boundary and exact offset. Preserve a matching suffix and complete membership, including learners and addresses. | All three safety suites; Rust durable suffix recovery; C++ snapshot codec test. |
| Reads | Distributed reads require a newly appended, committed and applied current-term no-op. An earlier delayed reply cannot authorize a later read. A single-voter fast path requires an applied current-term commit. | Three-node C++ delayed-reply witness, Java client query tests, Rust runtime read tests. |
| Write completion | A follower response is insufficient by itself. Confirm commitment and application of the submitted index in its submitting term; a replacement entry at that index cannot complete the old command. | C++ five-voter completion/singleton regressions; Rust submission-term wait regression. Unconfirmed C++ writes return `ACCEPTED_NOT_COMMITTED`. |
| Membership | Include new-only voters when checking the joint entry. Recover election membership from retained configuration entries as well as snapshots. Learners neither campaign nor vote. | Java singleton voter addition; C++ joint-quorum test; Rust role/election checks. Charlotte has corresponding joint-quorum and uncompacted-removal recovery tests. |
| Storage failure | Do not acknowledge a mutation whose durable publication failed. | Java permanently fences the node; Rust and C++ terminate the process on durable mutation failure. Rust has a subprocess regression for this policy. |

## Read cost and API behavior

The old receipt-time lease is no longer permission to serve subsequent distributed
reads. A delayed reply may have been produced before another leader was elected.
Each distributed read now incurs log persistence and quorum replication; no
throughput or latency qualification is claimed. Existing lease configuration is
retained at API boundaries, but cannot restore that unsafe shortcut.

The Rust timer uses delayed first election ticks and checks learner/joining
eligibility before returning for a current leader. Election state is captured
under one node lock. Runtime query completion rechecks leadership and term after
waiting for its fence.

The Rust demo now uses a 1500 ms election base, exceeding the shared runtime's
750 ms heartbeat interval. Its previous 500 ms node deadline could trigger
elections between healthy C++ heartbeats. Scheduling intervals still depend on
load and link latency; this is not a latency or availability guarantee.

Rust PROMOTE/DEMOTE now set the requested new role explicitly instead of copying
the caller's description of the old role. Committed joiners receive replication
cursors before promotion, and status reports keep current and proposed roles
separate. C++ JOINT preserves the caller's complete proposed membership, including
intentional leader removal; it no longer inserts the removed leader back in.

The KV application snapshots use the same binary grammar in all three languages:
a big-endian entry count followed by Java `writeUTF` key/value pairs. Native
encoders handle modified UTF-8, including NUL and UTF-16 surrogate pairs, and
reject lengths above 65535 encoded bytes. Their decoders reject truncation,
duplicates and trailing data. Rust still reads its older local JSON snapshots;
new snapshots use binary. Its workload driver delegates compaction to the core,
preserving membership and publishing snapshot/suffix in one durable operation.
The former JSON/binary mismatch could silently preserve old application state
while snapshot metadata advanced. Invalid Rust/C++ KV restores now fail.

## Durable records

Java writes one `GRF2` record containing the snapshot index/term, snapshot bytes and
length-delimited retained log entries. All numbers are big endian. The record is:
magic u32, snapshot index i64, snapshot term i64, snapshot byte length i32, snapshot
bytes, entry count i32, then each protobuf entry's i32 length and bytes. The writer
forces the temporary file, atomically replaces the record and forces its parent
directory. New-format readers reject truncation and trailing bytes.

Existing consistent Java log/`.meta` pairs remain readable. The first subsequent
mutation publishes the complete single-file record; the obsolete `.meta` file is
ignored once the new header is present. This is a forward format change: older
binaries cannot read `GRF2`. It does not repair a legacy pair already torn by an
earlier crash, so retain backups before upgrading existing development stores.

Rust retains its text records but rejects malformed/missing fields and I/O errors
instead of substituting empty state. Log entries now also retain their peer ID.
New snapshot envelopes use standard padded Base64; C++ also reads earlier Rust
envelopes without padding. Snapshot replacement and its matching suffix use one durable publication, with
serialized mutations and file/directory sync. Storage mutation errors abort the
process because unwinding only a Tokio task would leave its shared node usable.

C++ publishes a complete temporary state file, synchronizes it, renames it and
synchronizes the parent directory. Runtime callers capture the node state under
the store publication lock so an older concurrent capture cannot overwrite a
newer one. Recovery starts as a follower and discards unconfirmed remote match
progress. Runtime endpoint updates cannot overwrite recovered or replicated voting
membership. Durable mutation errors abort rather than entering the runtime's RPC
retry path. This implementation uses the POSIX file durability interface.

The stores are still simple complete-record implementations. Filesystem/device
flush behavior and actual power-loss recovery require separate testing.

## Validation

Run the Java suite with `mvn -q -pl raft-tests -am test -DskipJepsenTests=true`, the
Rust suite with `cargo test --workspace` from `graft-rust`, and the C++ suite with a
fresh CMake build followed by `graft_unit_tests`. Rust also passes
`cargo fmt --all -- --check`.

The full Java run reported 166 tests with three skipped and no failures;
43 Rust executable tests passed with one ignored documentation example, as did
43 C++ cases (348 assertions) and 60 Charlotte host tests. Ten Jepsen harness
tests passed 30 assertions. The Charlotte AArch64
core check, the bundled AArch64 service build, and both Rust workspace format
checks passed.

The permanent `scripts/run-consensus-regression-smoke.py` test starts a mixed
three-node cluster with each implementation taking a turn as leader. All three
leader choices passed a write and two consecutive fenced reads. Build the Java
probe (`mvn -pl :graft-cpp-java-probe -am package -DskipTests`), run the Java tests,
and build both native binaries before invoking it. `RAFT_CPP_BIN` and
`RAFT_RUST_BIN` can select binaries outside the default build directories.
The runner allocates local ports and terminates only its own child process groups.

The Charlotte core has two additional regressions:
`uncommitted_joint_requires_old_and_new_majorities` and
`restart_recovers_removal_from_uncompacted_configuration_entries`, plus
`recovery_discovers_new_only_voters_before_application_replay` for the service
connection-discovery peer set. Recovery of election membership does not claim that ordinary application entries have already
been committed or applied. Charlotte retains its stronger policy that an
uncommitted FINALIZE, including one covered by a later no-op, requires both joint
majorities.

The follow-up Jepsen/native campaign and its original failures are recorded in
[the validation report](validation/2026-10-29-consensus.md). Charlotte passed the
17-test no-network boot, both 20-test network guests and targeted abrupt-QEMU
term recovery on a reused virtual NVMe disk. Physical device power-loss tests,
interrupted native log/snapshot publication, simultaneous multi-voter restart and
throughput benchmarks remain separate validation work.
