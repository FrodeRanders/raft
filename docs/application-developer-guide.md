# Application Developer Guide

This project is intended to provide Raft cluster capability as library functionality. A domain application should be able to plug in its own state machine and get replicated writes, safe reads, snapshots, membership changes, telemetry, and recovery without learning or storing Raft internals.

Reference data and key/value storage are examples of applications on top of Raft. They are not the boundary.

If your background is XATMI, XA, two-phase commit, or service transactions, read [Raft And XATMI Transaction Thinking](raft-and-xatmi.md) as a companion note.

## Mental Model

A Raft-enabled application has two parts:

- Raft runtime: owns consensus, replication, membership, persistence of Raft metadata, read safety, transport, redirects, snapshots as Raft messages, and telemetry.
- Domain application: owns command schema, query schema, command validation, domain state, domain snapshots, and domain authorization/admission policy.

The critical rule is:

```text
Domain application state is replicated by Raft.
Raft cluster configuration is owned by Raft.
```

Do not store cluster configuration in a domain key/value store or in reserved domain keys. Cluster configuration is control-plane state. It is replicated through internal Raft commands and persisted by Raft snapshots and Raft metadata.

## Boundary

### Raft Owns

- leader election
- vote and term persistence
- log replication
- commit index and last-applied tracking
- membership and joint consensus
- learner/voter role transitions
- chunked `InstallSnapshot`
- wrapping snapshots with Raft metadata such as current and next cluster members
- read barriers and read leases
- redirect metadata
- cluster telemetry and management RPCs

### Application Owns

- command bytes accepted from clients
- query bytes accepted from clients
- applying committed commands to domain state
- producing command results when the language/runtime interface supports them
- serving domain queries once Raft has established read safety
- encoding and decoding domain snapshots
- domain authorization and write-admission decisions
- application CLI or API ergonomics

### Snapshot Shape

Snapshots have two layers:

```text
Raft snapshot wrapper
  version
  currentMembers
  nextMembers
  applicationSnapshot bytes
```

The application only sees `applicationSnapshot bytes`.

The wrapper is the Raft library's responsibility. It stores the committed cluster configuration at the snapshot boundary so a recovering node restores both:

- the domain state machine
- the Raft membership configuration needed for future elections, replication, and quorum decisions

## Java Domain Applications

The Java side already has the main application state-machine contracts in `raft-state-machine`.

Use `SnapshotStateMachine` for replicated command application and snapshot support:

```java
public interface SnapshotStateMachine {
    void apply(long term, byte[] command);
    byte[] snapshot();
    void restore(byte[] snapshotData);
}
```

Use `QueryableStateMachine` when the application supports reads:

```java
public interface QueryableStateMachine extends SnapshotStateMachine {
    byte[] query(byte[] request);
}
```

Use `ResultSnapshotStateMachine` when committed writes should return an application result to the client:

```java
public interface ResultSnapshotStateMachine extends SnapshotStateMachine {
    byte[] applyWithResult(long term, byte[] command);
}
```

If the state machine only implements `SnapshotStateMachine`, committed writes can still mutate application state, but the command result is empty.

### Implement A State Machine

A domain state machine should:

- parse application command bytes
- validate command shape
- mutate only application state
- return snapshots containing only application state
- restore only application state
- not parse or write Raft membership commands
- not store cluster members in the application data model

Example shape:

```java
public final class ProductReferenceStateMachine implements QueryableStateMachine, ResultSnapshotStateMachine {
    private final Map<String, Product> products = new HashMap<>();

    @Override
    public synchronized void apply(long term, byte[] commandBytes) {
        applyWithResult(term, commandBytes);
    }

    @Override
    public synchronized byte[] applyWithResult(long term, byte[] commandBytes) {
        ProductCommand command = ProductCommandCodec.decode(commandBytes);
        switch (command.type()) {
            case UPSERT -> products.put(command.id(), command.product());
            case REMOVE -> products.remove(command.id());
        }
        return ProductCommandResultCodec.accepted(command.id());
    }

    @Override
    public synchronized byte[] query(byte[] requestBytes) {
        ProductQuery query = ProductQueryCodec.decode(requestBytes);
        Product product = products.get(query.id());
        return ProductQueryResultCodec.encode(product);
    }

    @Override
    public synchronized byte[] snapshot() {
        return ProductSnapshotCodec.encode(products);
    }

    @Override
    public synchronized void restore(byte[] snapshotData) {
        products.clear();
        products.putAll(ProductSnapshotCodec.decode(snapshotData));
    }
}
```

The exact domain codec can be protobuf, JSON, flat binary, or something else. The Raft layer treats it as opaque bytes.

### Wire Commands And Queries

External clients send:

- `ClientCommandRequest` for writes
- `ClientQueryRequest` for reads

The request payload is application-owned. The runtime handles:

- leader redirect
- write admission
- command authorization/authentication
- log submission
- commit wait
- read safety before query execution

The application should not call Raft internals from inside `apply` or `query`.

### Runtime Packaging

A Java domain application should normally provide:

- a state-machine implementation
- a small application module implementing `RaftApplicationModule`
- a factory implementing `RaftApplicationFactory`
- optional CLI helpers for domain commands and queries
- optional policies for authentication, authorization, and write admission

`RaftApplicationModule` lets the distribution discover and select the application:

```java
public interface RaftApplicationModule {
    default boolean supportsRuntimeMode(String mode) { return false; }
    boolean supportsCliCommand(String command);
    void printUsage(PrintStream err);
    void runCli(String[] args, CliRuntimeContext context);
    default RaftApplicationFactory createFactory(RuntimeAdapterContext context) {
        throw new UnsupportedOperationException("Module does not provide a runtime adapter factory");
    }
}
```

For examples, see:

- `raft-app-kv`
- `raft-app-reference`

### Java Checklist

1. Define command and query payloads.
2. Implement `QueryableStateMachine` or `SnapshotStateMachine`.
3. Ensure snapshots contain only domain state.
4. Create a runtime adapter/factory that supplies the state machine.
5. Add admission and authorization policies if the domain needs them.
6. Add CLI/API support for encoding client commands and queries.
7. Validate with Java unit tests.
8. Validate with local cluster tests or Jepsen when behavior is distributed or safety-critical.

## C++ Domain Applications

The C++ side exposes the same conceptual boundary through `ApplicationStateMachine`.

```cpp
class ApplicationStateMachine {
public:
    virtual ~ApplicationStateMachine() = default;

    virtual std::string apply(std::int64_t index,
                              std::int64_t term,
                              std::string_view command) = 0;

    virtual std::string query(std::string_view request) const = 0;

    virtual std::string snapshot() const = 0;

    virtual void restore(std::string_view snapshot) = 0;
};
```

The index and term are supplied so applications can include them in audit records, diagnostics, or idempotency metadata if useful. The application should not use them to affect Raft behavior.

### Implement A State Machine

Example shape:

```cpp
class ProductReferenceStateMachine final : public graft::ApplicationStateMachine {
public:
    std::string apply(std::int64_t index,
                      std::int64_t term,
                      std::string_view command_bytes) override {
        auto command = ProductCommandCodec::decode(command_bytes);
        if (command.type == ProductCommand::Type::upsert) {
            products_[command.id] = command.product;
        } else if (command.type == ProductCommand::Type::remove) {
            products_.erase(command.id);
        }
        return ProductCommandResultCodec::accepted(index, term, command.id);
    }

    std::string query(std::string_view request_bytes) const override {
        auto query = ProductQueryCodec::decode(request_bytes);
        auto found = products_.find(query.id);
        return ProductQueryResultCodec::encode(found == products_.end()
                                                   ? std::nullopt
                                                   : std::optional<Product>{found->second});
    }

    std::string snapshot() const override {
        return ProductSnapshotCodec::encode(products_);
    }

    void restore(std::string_view snapshot_bytes) override {
        products_ = ProductSnapshotCodec::decode(snapshot_bytes);
    }

private:
    std::unordered_map<std::string, Product> products_;
};
```

The current `KeyValueStateMachine` is the default/example application. It implements this interface, but it is not the Raft model.

### Wire The Application Into A Node

`RaftNode::Config` accepts an application state machine:

```cpp
auto app = std::make_shared<ProductReferenceStateMachine>();

auto node = std::make_shared<graft::RaftNode>(graft::RaftNode::Config{
    .peer_id = "n1",
    .current_term = 0,
    .last_log_index = 0,
    .last_log_term = 0,
    .commit_index = 0,
    .snapshot_index = 0,
    .snapshot_term = 0,
    .voting_peers = {"n2", "n3"},
    .application = app,
});
```

If no application is supplied, the C++ implementation uses the key/value application for tests and smoke runs.

### C++ Snapshot And Restore

The C++ application should return only domain bytes from `snapshot()`.

Raft wraps those bytes with cluster membership metadata. On restore, Raft unwraps the application bytes and calls:

```cpp
application->restore(application_snapshot_bytes);
```

After restore, Raft replays committed log entries after the snapshot index. This keeps application recovery independent of domain-specific persistence fields in Raft metadata.

### C++ Checklist

1. Implement `graft::ApplicationStateMachine`.
2. Keep command/query payloads domain-owned.
3. Keep `snapshot()` and `restore()` focused on domain state only.
4. Pass the application into `RaftNode::Config`.
5. Keep cluster membership out of domain storage.
6. Add unit tests for command application, query behavior, snapshot/restore, and Raft replay.
7. Run C++ unit tests and at least one mixed Java/C++ smoke if the app uses the shared wire path.

## Application Commands Versus Raft Internal Commands

The Raft log contains two categories of entries:

- internal Raft commands
- application commands

Internal Raft commands are owned by the Raft library. They include membership operations such as join, joint configuration, finalize, promote, and demote.

Application commands are opaque domain bytes. They are delivered to the application state machine only after commitment.

The application must not:

- emit internal Raft commands as domain writes
- interpret internal Raft commands as domain commands
- store current/next members in domain tables
- reserve domain keys such as `_raft/config`

The Raft implementation must not:

- require the application to know cluster membership
- require the application to persist Raft metadata
- call application query methods before read safety is established

## Read Semantics

Reads are not just local map lookups from the perspective of a distributed system.

The runtime must establish read safety first:

- the node must be the leader, or the request is redirected/rejected
- in a multi-node cluster, the leader must have a valid read lease or complete a read barrier
- after that, the application `query` method can execute against local state

Application query code should assume that if it is called by the runtime, Raft has already handled read safety. It should not try to run elections, contact peers, or validate quorum.

## Write Semantics

Writes are submitted as client commands:

1. runtime authenticates and authorizes the request if configured
2. runtime applies domain write-admission policy
3. leader appends the command to the Raft log
4. leader replicates to a quorum
5. entry is committed
6. Raft calls application `apply`
7. command result is returned to the client when the application produced one

Applications should make `apply` deterministic. Every node that applies the same committed command at the same log position must produce the same resulting state.

## Admission And Authorization

Admission and authorization live above Raft mechanics.

Examples:

- reference-data writes may only be accepted by central voter nodes
- learners may reject writes instead of redirecting
- an allow-list may restrict who can mutate a dataset
- a gateway may authenticate callers and map them to requester identities

These policies should decide whether a command is allowed to enter the replicated log. They should not affect election, replication, or membership safety.

## Packaging Guidance

### Java

Keep reusable Raft libraries separate from domain apps:

- Raft library modules: wire, core, storage, state-machine, membership, runtime, transport, telemetry
- application modules: reference data, key/value, masterdata, or other domain packages
- distribution module: assembles selected runtime, transport, and applications

### C++

Keep the same conceptual separation:

- `core`: Raft node and application state-machine interface
- `runtime`: replication loops and RPC request handling
- `transport`: Boost.Asio transport
- `storage`: persistent Raft state/log support
- `app`: domain state machines and CLI/API adapters

The current C++ tree is smaller than the Java reactor, but domain applications should still be written against the application interface rather than against `RaftNode` internals.

## Testing Guidance

Minimum application tests:

- command decode and validation
- deterministic `apply`
- query response encoding
- snapshot/restore round trip
- restore from Raft snapshot plus committed log replay
- rejection of invalid commands

Minimum cluster tests:

- single-node write/read
- multi-node leader write and follower redirect
- restart recovery
- snapshot recovery
- learner catch-up if the application expects read replicas
- mixed Java/C++ smoke when interoperability matters

Use Jepsen-style tests for changes that affect:

- command linearizability
- membership changes
- read leases/barriers
- snapshot and restart behavior
- mixed Java/C++ clusters

## Common Mistakes

- Storing Raft membership in the domain store.
- Using a reserved domain key for cluster configuration.
- Calling application query code before the leader has established read safety.
- Making `apply` depend on local wall-clock time or local node identity.
- Treating learners as safe write targets.
- Letting application code initiate Raft membership changes directly.
- Putting authentication secrets or requester identities inside replicated domain commands when they belong in request metadata.

## Design Rule

If a piece of state is needed to decide Raft safety, quorum, leadership, membership, replication, or recovery, it belongs to Raft.

If a piece of state is part of the business dataset being replicated, it belongs to the application.
