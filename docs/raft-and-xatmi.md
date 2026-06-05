# Raft And XATMI Transaction Thinking

This note is for application developers and architects who are familiar with XATMI, XA, two-phase commit, service transactions, or similar middleware transaction models.

Raft and XATMI can look similar from far away because both coordinate distributed work and both force applications to think carefully about commit, failure, retry, and recovery. They are not the same mechanism. Treating Raft as "another XA transaction manager" will lead to the wrong design.

## Short Version

Raft answers:

```text
Which command is committed at log index N, in what order, according to a quorum?
```

XATMI/XA-style transaction processing answers:

```text
Can this transaction commit atomically across a set of enlisted services/resources?
```

Raft is a replicated state-machine consensus mechanism. XATMI is an application service and transaction coordination model.

In this project, a Raft application command is closer to a durable, replicated service request than to a full XA transaction.

## Where They Are Similar

Both models have a request that crosses process boundaries:

```text
client -> service/runtime -> coordinated outcome -> response
```

Both must handle:

- timeout
- retry
- process crash
- network partition
- durable recovery
- duplicate request delivery
- unclear client outcome
- authorization and admission before mutation

Both force the application to answer the same practical question:

```text
If the client did not receive the response, did the operation happen?
```

That question is often more important operationally than the exact API used.

## Where They Differ

### Raft Commits Log Entries

Raft takes an application command and appends it to a replicated log. Once the leader has replicated the entry to a quorum, the entry is committed. Every correct replica applies committed entries in the same order.

```text
client command
  -> leader appends log entry
  -> leader replicates to followers
  -> quorum acknowledges
  -> entry becomes committed
  -> application state machine apply() runs in log order
  -> command result can be returned
```

The replicated log is the serialization point.

### XATMI/XA Coordinates Resources

XATMI-style transaction processing typically coordinates a unit of work across services and resource managers. XA/two-phase commit asks all enlisted resources to prepare and then commit or roll back.

```text
begin transaction
  -> call service A
  -> update resource B
  -> update resource C
  -> prepare all enlisted resources
  -> commit all, or roll back all
```

The transaction coordinator is trying to make multiple independent participants agree on one atomic outcome.

### The Core Difference

```text
Raft:
  one replicated log orders commands for one replicated state machine

XATMI/XA:
  one transaction coordinates multiple participants/resources
```

Raft gives you linearizable replication of a state machine. It does not automatically give you an ACID transaction across arbitrary external systems.

## Commit Semantics

### Raft Commit

A Raft command is committed when it is durably replicated to a quorum according to the Raft rules. After commit, it will be applied by replicas in log order.

For the domain application, the important boundary is:

```text
apply() is called only for committed log entries.
```

The application does not decide whether an entry is committed. Raft does.

### XA Commit

An XA transaction commits when all required resources have successfully passed the transaction protocol. If a resource cannot prepare or commit, the transaction manager must resolve the outcome using XA recovery rules.

For the domain application, the important boundary is:

```text
the resource manager participates in prepare/commit/rollback
```

That is not what a Raft application state machine does.

## Ambiguous Outcomes

Both models have ambiguous client outcomes.

Example:

```text
client sends request
leader commits command
leader crashes before response reaches client
client sees timeout
```

From the client's perspective, the operation is uncertain. It may have committed. Retrying blindly may create a duplicate business operation unless the application has a request identity.

The same pattern exists in XATMI/XA:

```text
client calls service
transaction commits
network fails before response
client sees timeout
```

The remedy is the same design discipline:

- assign a stable request id
- store enough outcome information to recognize retries
- make retry behavior idempotent from the client's perspective

## Idempotency And Request Identity

Application developers should treat request identity as part of the domain command model.

Good command shape:

```text
commandId: 8f0d...
requesterId: reference-admin
operation: upsertProduct
productId: P123
payload: ...
```

The state machine can then keep a small outcome table:

```text
commandId -> command result
```

On first application:

```text
if commandId is unknown:
    apply domain mutation
    store result by commandId
    return result
```

On retry:

```text
if commandId is known:
    return previously stored result
```

This is useful in both Raft and XATMI-style systems. It is especially important when clients retry after timeout.

## Determinism

Raft application commands must be deterministic.

Every node applies the same committed command at the same log index. Therefore every node must produce the same state transition.

Avoid this inside `apply()`:

- local wall-clock decisions
- random number generation
- reading local files not replicated by Raft
- calling external services
- checking local node identity
- using non-deterministic iteration order when it affects state

Prefer this:

- include timestamps in the command if the business event needs a timestamp
- include generated ids in the command if the business event needs ids
- validate before submission where possible
- make `apply()` a pure transition over current replicated state plus command bytes

XATMI services can also suffer from non-determinism, but Raft makes it stricter because all replicas must replay the same command stream.

## External Resources

The safest Raft application state machine does not call external systems during `apply()`.

If `apply()` calls a database, message broker, REST service, or mainframe transaction service, then the operation is no longer just a replicated state-machine transition. You have introduced another participant with its own failure and recovery semantics.

That may be necessary, but it must be designed explicitly.

### Recommended Pattern: Raft As Source Of Truth

For reference data, masterdata, and similar distribution use cases:

```text
management system validates change
  -> submits command to Raft
  -> Raft commits command
  -> application state machine updates local replicated state
  -> local readers use fast in-memory/hash-table lookups
```

The authoritative distributed value is the Raft log/state machine. External stores can feed it or observe it, but should not be part of the committed application transition unless deliberately modeled.

### Outbox Pattern

If a committed Raft command must trigger external side effects, use an outbox-style approach:

```text
apply committed command:
    update replicated domain state
    record pending side effect in replicated state

separate worker:
    observes pending side effect
    performs external action idempotently
    submits follow-up command marking side effect complete
```

This keeps Raft apply deterministic and gives side effects a recoverable state.

### Avoid Hidden XA Inside Raft Apply

Do not casually do this:

```text
apply():
    start XA transaction
    update external database
    call remote service
    commit XA transaction
```

That design combines Raft commit with a second transaction protocol. It may be correct in a specific architecture, but it is no longer a simple Raft state machine. It needs a full failure analysis.

## Mapping Concepts

| XATMI/XA concept | Rough Raft-related concept | Important difference |
| --- | --- | --- |
| Service request | Client command/query | Raft commands are replicated before application mutation |
| Transaction commit | Log entry commit | Raft commit is quorum replication, not multi-resource prepare/commit |
| Resource manager | Application state machine | State machine should be deterministic and local |
| Transaction id | Command id / request id | Application should model idempotency explicitly |
| Timeout | Timeout | Both can leave client outcome ambiguous |
| Recovery log | Raft log and snapshots | Raft log orders the replicated state machine |
| Rollback | Usually not applicable after commit | Raft commands are not rolled back after commitment; compensating commands are used |
| Service routing | Leader redirect / client retry | Writes must reach the leader |
| Read-only service | Linearizable query | Raft read safety must be established first |

## Rollback And Compensation

In XA, rollback is part of the transaction protocol before commit.

In Raft, once an entry is committed and applied, the application does not roll it back. If the business process needs reversal, submit a new compensating command.

Example:

```text
commit: upsert product P123 to version 7
later correction: upsert product P123 to version 8
```

or:

```text
commit: activate reference value X
later compensation: deactivate reference value X
```

That compensation is itself a new committed fact in the log.

## Reads And Lookup Workloads

XATMI applications often think in terms of service calls. A read service may call a database or another service.

In this Raft design, the target use case is often:

```text
few writes, many reads
```

For reference data and masterdata distribution, the common pattern is:

```text
central governance and quality assurance
  -> low-rate replicated changes through Raft
  -> high-rate local lookups from domain state
```

A local lookup is safe only after Raft has established that the node can serve the read. In this implementation, the runtime does that before calling the application's query method.

Application developers should not implement quorum checks in the query code.

## Cluster Configuration Is Not Domain Data

This is a common source of confusion.

Cluster configuration includes:

- current voters
- next voters during joint consensus
- learners
- membership transition state
- quorum rules

That state belongs to Raft. It is not reference data. It is not masterdata. It is not a key/value entry owned by the application.

The application snapshot stores business state. The Raft snapshot wrapper stores cluster configuration.

```text
Raft snapshot wrapper
  currentMembers
  nextMembers
  applicationSnapshot
```

Applications must not reserve keys such as:

```text
_raft/config
_raft/members
_cluster/voters
```

Those keys would blur the safety boundary.

## Transaction Boundaries For Domain Developers

When designing a domain command, ask:

1. What is the smallest deterministic state change?
2. What command id identifies this request across retries?
3. What result should be returned if the same command id is seen again?
4. What state must be included in snapshots?
5. What external side effects are needed, if any?
6. Can side effects be modeled as follow-up commands?
7. What authorization/admission rule applies before the command enters Raft?

Do not ask the application to decide:

- who the leader is
- what quorum means
- whether a membership change is committed
- where cluster configuration is stored
- whether a read lease is valid

Those are Raft runtime questions.

## Presenting This Internally

A useful way to explain the architecture:

```text
XATMI/XA coordinates a transaction across resources.
Raft replicates a deterministic state machine by agreeing on a command log.
```

For the reference-data use case:

```text
The management database remains good at governed change management.
Raft is used to distribute accepted changes to many local readers.
Local readers do not call the central database for every lookup.
They read the replicated local state after Raft has made it safe.
```

For the masterdata use case:

```text
Each committed data change becomes an ordered fact.
Every replica applies the same facts in the same order.
Recovery uses snapshots plus log replay.
Retries use command ids to avoid duplicate business effects.
```

## Design Guidance

Use Raft when:

- the application can be represented as a deterministic state machine
- the replicated state is small or compactable enough for snapshots
- writes are lower volume than reads
- local read performance matters
- the system benefits from clear leader/quorum semantics

Use XA/XATMI-style coordination when:

- one business transaction must atomically update multiple independent resource managers
- participants cannot be represented as one replicated state machine
- prepare/commit/rollback across external systems is the actual requirement

Use both only with explicit design:

- Raft for replicated state
- XA/XATMI for external systems that truly require transaction coordination
- idempotent bridges between them
- clear recovery and compensation rules

## Practical Rule

For a domain application developer using this project:

```text
Your command is the transaction-like business request.
Raft makes that request durable, ordered, replicated, and recoverable.
Your state machine makes the deterministic business change.
Raft cluster configuration stays outside your domain data.
```
