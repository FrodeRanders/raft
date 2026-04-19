# Split Brain, Partitions, And Consistency Boundaries

This note explains what this repository's Raft implementation does and does not guarantee when the network partitions and the cluster appears to "split" into two or more disconnected groups.

It is important to separate two different concerns:

1. consistency of the replicated Raft log and the state machine driven by that log
2. consistency of everything around that log, such as clients, retries, caches, projections, and external side effects

Raft addresses the first concern directly. The second remains an application and integration responsibility.

## What "Split Brain" Means Here

In informal distributed-systems language, "split brain" usually means:

- the network partitions
- more than one side behaves as if it is the authoritative cluster
- writes may proceed independently on both sides
- after healing, the data sets diverge and require reconciliation

In a correct Raft system, that outcome should not happen for committed state.

What can happen is:

- the cluster partitions into a majority side and one or more minority sides (more on this below)
- nodes on multiple sides may temporarily believe they should lead
- an old leader may continue trying to act like a leader until it notices quorum loss
- clients may see redirects, retries, timeouts, or ambiguous outcomes

What should not happen is:

- two different partitions both committing conflicting log entries for the same log position in the same configuration
- a minority partition continuing to commit authoritative writes as if it still had quorum

So the useful distinction is:

- apparent split brain at the transport or liveness level can happen
- committed split brain at the replicated-log level should not

## Why "Majority" And "Minority" Are Well-Defined

The terms majority and minority are not decided dynamically by "who can currently see whom."

In Raft, elections and commitment are evaluated against the configured cluster membership.

That means:

- there is a specific current voting set
- every candidate is trying to win votes from that full set
- a leader is elected only by receiving votes from a majority of that configured set
- an entry is committed only when replicated to a majority of that configured set

So if a 5-node voting configuration is:

- `A`
- `B`
- `C`
- `D`
- `E`

then the quorum threshold is 3, because the configured cluster has 5 voters.

If the network partitions into:

- `A B C`
- `D E`

then:

- `A B C` is the majority side because it still contains 3 of the 5 configured voters
- `D E` is the minority side because it contains only 2 of the 5 configured voters

This remains true even if:

- `D` can only see `E`
- `D` and `E` are otherwise healthy
- they might locally believe they form a complete little world

They do not become a new independent cluster just because they can talk to each other. Reachability affects liveness, but configuration defines legitimacy. The network decides who can talk, but the configuration decides who counts.

They are still only 2 members out of the configured 5-member voting set, which means:

- they cannot elect a legitimate leader for that configuration
- they cannot commit new entries for that configuration

The same logic applies to any partition shape.

For example, if a 7-node cluster splits into:

- `4 + 3`

then only the `4` side can make committed progress, because quorum is 4 for a 7-voter configuration.

This is the key reason partitions do not simply re-form into independent sub-clusters that continue living on their own. Raft leadership and commitment are defined relative to the existing replicated membership configuration, not to temporary post-partition visibility.

The only time this changes is when membership itself changes through Raft:

- joint consensus
- finalize
- promotion
- demotion
- removal

But those are themselves replicated configuration changes.

So the cluster cannot legitimately "shrink itself by accident" just because some nodes disappeared behind a partition. A new voting set only becomes authoritative after the cluster commits that configuration change through the log.

## What Raft Guarantees

For the authoritative replicated state machine, Raft gives these safety properties:

- only a leader with a quorum can commit new log entries
- once an entry is committed, future leaders must contain that entry
- conflicting uncommitted entries on losing sides are eventually overwritten
- after partition healing, followers converge to the majority-selected log
- membership changes are serialized through the log and protected by joint consensus

Applied to this implementation, that means:

- ordinary writes are accepted only through the leader path
- write success is defined as committed and applied, not just appended locally
- linearizable reads are served only by a leader that can establish a fresh quorum-backed read window
- removed nodes decommission themselves after finalized removal

In practice, under a partition:

- the majority side may continue to elect a leader and commit writes
- the minority side may be alive but should lose the ability to commit new authoritative writes
- once the partition heals, minority-side uncommitted divergence is discarded

That is why the Jepsen partition scenarios focus on:

- isolating one node
- isolating the leader
- cutting the leader into a minority
- overlapping reconfiguration with a partition

Those tests are validating the claim that only one side can keep making committed progress.

## What Raft Does Not Guarantee

Raft does not automatically make the rest of the application ecosystem consistent.

It does not solve:

- client retries after ambiguous timeouts
- duplicate command submission
- side effects that happen before commit is known
- cache invalidation across failover
- stale read models or projections
- reconciliation of data written outside the Raft leader path
- domain-level merge policy for concurrent offline edits
- idempotency of calls into external systems

This is the point that often gets mislabeled as "eventual consistency."

Inside the Raft state machine, the goal is not eventual consistency. The goal is strong consistency for committed state.

Around the state machine, many components are only eventually consistent:

- materialized views
- denormalized read stores
- caches
- search indexes
- downstream analytics pipelines
- webhook consumers
- external billing, email, or provisioning systems

Those systems must converge eventually, but Raft alone does not define how.

## The Three Important Outcome Classes

When a client issues a command during instability, there are three fundamentally different outcomes:

### 1. Definitely committed

The leader had quorum, the log entry committed, and the state machine applied it.

This repository explicitly moved ordinary write acknowledgements toward that meaning. That is the clean case.

### 2. Definitely not committed

The request was rejected because:

- the node was not leader
- the cluster had no usable leader
- the request was invalid or unauthorized
- the compare-and-set precondition did not match

This can safely be treated as no state change in the authoritative replicated state machine.

### 3. Ambiguous

The client timed out, disconnected, or lost contact while the cluster was unstable.

In that case:

- the command may have committed
- or it may have been dropped
- the client cannot safely assume either outcome

This ambiguity exists even in a correct Raft system, because the client can lose the response after the cluster has already committed the command.

That is why the Jepsen harness now treats some outcomes as indeterminate rather than definite failures. The ambiguity is real at the client boundary even when the core consensus behavior is correct.

## Where Inconsistency Can Still Appear

The word "inconsistency" is still useful, but it needs to be scoped carefully.

### A. Uncommitted local divergence

A minority or superseded leader may append entries locally that never commit.

That local log tail is inconsistent with the surviving majority for a while.

Raft handles this by rollback and overwrite during reconciliation.

Developers usually do not need custom business logic for this case as long as application state changes are only driven by committed entries.

### B. Side effects emitted too early

Suppose application code sends an email, charges a card, provisions a resource, or updates another datastore before the Raft command is known to be committed.

Then a partition or failover may produce:

- an external side effect that happened
- but no corresponding committed entry in the authoritative log

Raft cannot repair that automatically. This is now an application-level inconsistency.

### C. Duplicate execution from retries

If a client times out and retries without an idempotency key, the cluster may eventually apply the same logical intent twice.

Raft preserves log consistency, but not business-level uniqueness unless the application models it.

### D. Stale or lagging read models

If a system derives secondary views from the Raft log asynchronously, those views may lag after partitions, failovers, or restart recovery.

Again, that is not a Raft safety failure. It is an eventually consistent projection.

### E. Writes outside the leader path

If any service allows local writes on followers, offline replicas, edge nodes, or separate databases not serialized through the Raft leader, then conflict resolution becomes an application concern.

At that point you are no longer relying on Raft alone for authoritative writes.

## What Developers Need To Accommodate

If the application wants to stay correct under partitions and failovers, developers should account for the following patterns.

### 1. Treat authoritative state as log-derived

The cleanest model is:

- only committed Raft log entries mutate authoritative state
- everything else is derived from or synchronized to that state

This prevents most split-brain-style reconciliation problems.

### 2. Use idempotency keys for client commands

Clients should attach a stable command identity for operations that may be retried.

That lets the application distinguish:

- "same logical command retried after timeout"
- from "new logical command"

Without that, ambiguous client failures can become duplicate business actions.

### 3. Delay irreversible side effects until commit is known

If a side effect is not reversible, it should not happen before the corresponding Raft command is committed and applied.

Safer patterns are:

- commit first, then emit side effects from an outbox or committed-event stream
- or emit compensatable side effects only

### 4. Make side-effect consumers replay-safe

Anything downstream of the committed log should tolerate:

- duplicate delivery
- delayed delivery
- reordered observation across independent streams

This is standard outbox/projection discipline.

### 5. Define reconciliation rules for external projections

For caches, search indexes, or read models, developers need to define:

- how staleness is detected
- how a projection catches up after outage
- whether rebuild-from-log is supported
- whether consumers should prefer monotonic versions, timestamps, or log indexes

### 6. Define domain merge rules if offline mutation is allowed

If the product intentionally allows updates outside the single Raft leader path, then Raft no longer provides the whole answer.

Developers must define:

- last-writer-wins or version-based conflict rules
- per-field merge behavior
- tombstone/delete precedence
- human review workflows for irreconcilable conflicts

That is application-level distributed data management, not consensus alone.

## Strong Consistency vs Eventual Consistency In This Repository

For the built-in key-value demo and the core write path, the intended model is:

- authoritative state is strongly consistent
- writes succeed only when committed and applied
- reads are linearizable leader reads

So for the core demo workload, the target is not eventual consistency.

Eventual consistency appears in surrounding layers such as:

- telemetry snapshots
- observation pipelines
- downstream projections you might build on top
- operational interpretation during a live outage

If a developer extends this repository into a broader platform, they should be explicit about which data is:

- strongly consistent through Raft
- eventually consistent through asynchronous replication or projection
- locally cached and allowed to be stale

That boundary should be documented per subsystem.

## Practical Interpretation Of Partitioned States

During a live partition, the safest operational interpretation is:

- majority side: may continue as the authoritative cluster
- minority side: may still be reachable, but should not be trusted for authoritative writes
- old leader in minority: should be treated as stale until quorum is re-established
- clients with timeouts: must treat the command outcome as unknown until confirmed

For application operators and client developers, the key rule is:

- never infer "command definitely failed" from transport failure alone

Instead, use:

- idempotent retry
- explicit post-failure read/lookup
- command status tracking if the domain needs it

## How The Existing Jepsen Tests Relate

The current Jepsen harness validates the Raft side of this story:

- `partition-one`: a single isolated node should not corrupt committed state
- `partition-leader`: leader loss should cause safe failover
- `partition-leader-minority`: the old leader should not keep making authoritative progress in a minority
- `membership-remove-follower-partition-leader`: reconfiguration should remain safe when leadership is disrupted
- `persistence-loss-restart`: a wiped follower should recover from the surviving cluster without corrupting committed state
- multi-key and CAS workloads: correctness should hold across independent keys and conditional writes

These tests do not validate application-specific reconciliation for:

- external side effects
- caches
- asynchronous read models
- business-level deduplication
- merge semantics for writes that bypass the single leader

Those remain product responsibilities.

## Recommended Documentation Standard For Applications Built On This

Any real application using this Raft layer should document:

1. which data is authoritative and log-backed
2. which APIs return definite success, definite failure, or ambiguous outcome
3. how client retries are deduplicated
4. when side effects are emitted relative to commit
5. how projections rebuild or catch up after outage
6. whether any writes occur outside the Raft leader path
7. what conflict resolution policy applies if they do

If those items are left implicit, teams often assume Raft solves more than it actually does.

## Bottom Line

Raft prevents committed split brain for the authoritative replicated log.

Raft does not eliminate:

- ambiguous client outcomes during failures
- duplicate intent from retries
- stale projections
- premature external side effects
- domain conflicts created outside the leader/commit path

So the right mental model is:

- Raft gives strong consistency for committed state
- everything around that state still needs explicit application-level consistency design
