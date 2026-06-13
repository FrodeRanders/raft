# graft-rust

This directory contains the Rust implementation. It is a peer implementation of the Java Raft functionality, built separately with Cargo so it can be used in Rust deployments without making the Maven reactor or CMake builds responsible for Rust builds.

The goal is functional equivalence with the Java implementation at the shared protocol and behavior boundary:

- reuse the existing protobuf contract from `raft-wire/src/main/proto/raft.proto`
- reuse the existing `Envelope` framing used by the Java Netty transport
- keep Java, C++, and Rust nodes interoperable in the same Raft cluster
- converge the Rust runtime, persistence, membership, and application behavior with the Java implementation over time

## Scope

The current implementation provides:

- protobuf code generation from the shared `raft.proto` via `prost-build`
- tokio-based async TCP transport with shared varint32 length framing
- `Envelope { correlation_id, type, payload }` request/response exchange
- a full Raft consensus engine (elections, log replication, commit/apply, snapshots, joint-consensus membership)
- file-backed and in-memory persistent storage
- pluggable state machine interface with linearizable read support
- a CLI binary (`graft-smoke`) with subcommands for serving, client ops, membership, and smoke tests
- auth/admission policy hooks (authenticator, authorizer, rate limiter, stuck detector)
- integration tests covering elections, AppendEntries, membership, snapshots, storage
- cross-language smoke scripts verifying Rust ↔ Java ↔ C++ interoperability

## Java/C++/Rust Design Relationship

The three implementations are intentionally coupled at the protocol boundary and intentionally separate at the build/runtime boundary.

```text
                 shared contract
        ../raft-wire/src/main/proto/raft.proto
          Envelope + Raft RPCs + KV commands
                      |
        +-------------+-------------+-------------+
        |             |                           |
 Java impl.      C++ impl.                   Rust impl.
 Maven reactor   graft-cpp                   graft-rust
        |             |                           |
 Netty transport  Boost.Asio transport      Tokio transport
 Java RaftNode/   C++ RaftNode/             Rust RaftNode/
 runtime          runtime                   runtime
 Java storage/    C++ storage/              Rust storage/
 state machine    state machine             state machine
```

The strongest cohesion is the shared wire contract:

- all three sides use the same protobuf schema from `raft-wire`
- all three sides use the same `Envelope { correlation_id, type, payload }` shape
- all three sides use the same raw varint32 length-prefix framing
- all three sides encode replicated client work as `StateMachineCommand`
- all three sides encode membership transitions as `InternalRaftCommand`
- all three sides expose telemetry, cluster summaries, reconfiguration status, and redirect metadata through shared messages

The Java implementation is currently the more mature layered design:

- `raft-wire`: protobuf contract and Java protocol mapping
- `raft-core`: core Raft node and message handling
- `raft-storage`: storage abstractions and file/in-memory stores
- `raft-state-machine`: application state-machine contracts
- `raft-membership`: cluster configuration and snapshot wrapping
- `raft-runtime`: bootstrap, request policies, adapters, authentication, and CLI support
- `raft-transport-netty`: concrete Netty transport

The Rust implementation mirrors the same conceptual layers as separate Cargo crates:

- `graft-proto`: generated Rust protobuf types from the shared schema, varint32 framing
- `graft-core`: transport-agnostic Raft consensus engine (`RaftNode`, traits for `LogStore`, `PersistentStateStore`, `RaftTransport`, `StateMachine`)
- `graft-storage`: `FileLogStore`, `InMemoryLogStore`, `FilePersistentStateStore`, `InMemoryPersistentStateStore`
- `graft-transport`: tokio-based `RaftClient` and `RaftServer` with protobuf envelope framing
- `graft-runtime`: active event loop (`RaftRuntime`), RPC dispatch (`RaftHandler`), auth/admission policies, membership encoders, seed providers
- `graft-telemetry`: Prometheus scrape endpoint and OTLP HTTP push export (configured via `RAFT_TELEMETRY_*` environment variables)
- `graft-app-kv`: example key-value state machine (`KeyValueStateMachine`) and `graft-smoke` CLI binary

The current design goal is not source-level parity. It is behavioral parity at the protocol boundary first, followed by gradual convergence of internal layering where the Java modules already show the intended seams.

## Crate Structure

```
graft-rust/
├── graft-proto/       # Shared protobuf types + varint32 wire framing
├── graft-core/        # Transport-agnostic Raft consensus engine
├── graft-storage/     # Persistent log & term/vote storage backends
├── graft-transport/   # Async TCP networking with protobuf envelope framing
├── graft-runtime/     # Active event loop, RPC dispatch, auth, membership
├── graft-telemetry/   # Prometheus scrape endpoint + OTLP push export
├── graft-app-kv/      # Example key-value state machine + CLI
└── graft-tests/       # Integration & unit tests
```

## Why Tokio

The Java implementation uses Netty, but Netty is only the transport substrate. The actual on-the-wire protocol is:

1. raw varint32 length prefix
2. serialized protobuf `Envelope`
3. `Envelope.type` naming the request/response message
4. protobuf payload inside `Envelope.payload`

Tokio is a natural Rust replacement for the transport layer because it gives explicit control over async TCP I/O without forcing a new protocol, and integrates directly with the Rust async ecosystem.

## Async Runtime: Tokio

All async I/O is built on **tokio** (v1, `features = ["full"]`):

| Component | Async Pattern |
|---|---|
| `RaftServer` (inbound) | `TcpListener::accept()` loop, `tokio::spawn` per connection, `AsyncReadExt`/`AsyncWriteExt` |
| `RaftClient` (outbound) | `TcpStream::connect()`, `tokio::sync::Mutex` for connection pooling, `tokio::sync::oneshot` channels for request/response correlation, `tokio::time::timeout` for 5s RPC deadlines |
| `RaftRuntime` (event loop) | `tokio::select!` over two `tokio::time::interval` timers (election + heartbeat), `tokio::spawn` for concurrent VoteRequest fan-out |

For the `RaftNode` consensus state (the hot path), a standard **`parking_lot::Mutex`** is used rather than `tokio::sync::Mutex`. Critical sections on the RaftNode are short, synchronous, in-memory operations — no I/O or `.await` points — so a non-async mutex avoids unnecessary scheduling overhead. The runtime layer (`RaftRuntime`) wraps the node in `Arc<parking_lot::Mutex<RaftNode>>`.

## Key Dependencies

| Crate | Purpose |
|---|---|
| `tokio` (full) | Async runtime: TCP, timers, spawning, I/O |
| `prost` / `prost-build` | Protobuf serialization from shared `.proto` |
| `bytes` | Zero-copy byte buffers (`BytesMut`, `Buf`, `BufMut`) used in wire framing |
| `parking_lot` | Fast `Mutex` for synchronizing `RaftNode` state |
| `dashmap` | Concurrent `HashMap` for client connection pooling and in-flight requests |
| `indexmap` | Deterministic-order `HashMap` for cluster membership (needed for stable quorum computation) |
| `rand` | Randomization for election timeout jitter and backoff |
| `thiserror` | Derive `Error` for transport and consensus error types |
| `anyhow` | Flexible error handling in the runtime and app layers |
| `tracing` / `tracing-subscriber` | Structured logging and span instrumentation |
| `serde` / `serde_json` | JSON serialization for app commands and snapshots |
| `clap` (derive) | CLI argument parsing for the KV app binary |
| `tempfile` | Scratch directories/files in tests |

## Architecture

### Transport-Agnostic Core (`graft-core`)

`RaftNode` is the consensus state machine, implementing the full Raft protocol (Figures 2 & 3 from Ongaro's paper):

- **Elections**: Randomized timeouts with linear backoff per election round. Vote granting uses the standard candidate-log-up-to-date check. Joint-consensus quorum enforced for leader election during reconfiguration.
- **Log replication**: Leader tracks per-follower `nextIndex` / `matchIndex`. On mismatch, `nextIndex` decrements; on success, `matchIndex` advances. Commit index advances when a quorum of the current (and during joint consensus, both old and new) configuration has replicated.
- **Snapshotting**: Chunked `InstallSnapshot` RPC. Leader sends one chunk per heartbeat tick, tracking per-follower byte offsets. Follower accumulates chunks and installs atomically when `done=true`.
- **Membership**: Joint-consensus via `InternalRaftCommand` protobuf entries (Join → Joint → Finalize). Auto-finalize triggers when all new members have caught up. `configuration_at(index)` replays committed config commands to reconstruct membership at any log index for historically-correct quorum checks.
- **Read barriers**: Linearizable reads use a read lease (leader contacted a majority within a configurable window) or an explicit contact-majority barrier.

`RaftNode` has **zero I/O dependencies**. It depends on three traits injected by the runtime:

- `LogStore` — append-only log (snapshot index/term, append, truncate, compact, iterate)
- `PersistentStateStore` — durable `currentTerm` and `votedFor`
- `RaftTransport` — fire-and-forget RPC callbacks (`send_vote_request`, `send_append_entries`, `send_install_snapshot`)

### Transport (`graft-transport`)

`RaftClient` provides async outbound RPC with two modes:

1. **Connection-per-call** (`send_rpc`): Opens a fresh TCP connection, sends the protobuf `Envelope`, waits for a response with a 5-second deadline. Used for election votes, heartbeats, and snapshot chunks.
2. **Pooled connections** (`send_request`): Maintains per-peer `TcpStream` connections in a `DashMap`, with `oneshot` channels keyed by correlation ID for asynchronous response matching.

`RaftServer` accepts inbound TCP connections, reads varint32-framed protobuf `Envelope` messages, dispatches to a `MessageHandler` trait implementation, and writes back the response.

The codec (`codec.rs`) implements varint32 length-prefixed framing byte-for-byte identical to the Java (`writeRawVarint32`) and C++ (`graft::encode_varint32`) implementations.

### Runtime (`graft-runtime`)

`RaftRuntime` is the active event loop:

- An **election timer** (randomized 1500–3000ms base) triggers candidate transitions, sends `VoteRequest` RPCs to all remote voters via spawned tasks, counts responses, and checks joint-majority quorum to become leader.
- A **heartbeat timer** (750ms) sends `AppendEntries` (or `InstallSnapshot` chunks for lagging followers), decodes responses, updates leader bookkeeping, advances the commit index, and checks auto-finalize conditions.

`RaftHandler` implements `MessageHandler` to dispatch incoming envelopes to the appropriate `RaftNode` method, with pluggable `Authenticator`, `Authorizer`, and `AdmissionPolicy` hooks for security and admission control.

### Wire Protocol (`graft-proto`)

The `graft-proto` crate compiles the **same `raft.proto`** used by Java and C++ via `prost-build`. This ensures byte-identical protobuf message layouts across all three implementations. The crate also provides free functions for varint32 encoding/decoding and envelope framing used by the transport layer.

## Storage (`graft-storage`)

Two backend families:

| Backend | Log | Persistent State |
|---|---|---|
| In-memory | `InMemoryLogStore` | `InMemoryPersistentStateStore` |
| File-backed | `FileLogStore` | `FilePersistentStateStore` |

File backends use simple line-oriented key=value formats (base64 for binary data) with atomic writes via tempfile → rename → fsync. Suitable for testing and development; not designed as production append-only stores.

## Telemetry Export

The Rust implementation supports two layers of telemetry:

1. **In-cluster telemetry** (protobuf RPC) — `TelemetryRequest` / `TelemetryResponse` and `ClusterSummaryRequest` / `ClusterSummaryResponse` handled through the shared wire protocol in `RaftHandler`. This is the on-demand operational snapshot available from any cluster member.

2. **External telemetry endpoints** — Prometheus scrape endpoint and OTLP push, configured via environment variables:

```text
# Exporter selection: "none" (default), "prometheus", "otlp", or "otel-log"
export RAFT_TELEMETRY_EXPORTER=prometheus

# Prometheus
export RAFT_TELEMETRY_PROMETHEUS_HOST=127.0.0.1
export RAFT_TELEMETRY_PROMETHEUS_PORT=9108
export RAFT_TELEMETRY_PROMETHEUS_PATH=/metrics

# OTLP (requires `--features otlp` at build time)
export RAFT_TELEMETRY_EXPORTER=otlp
export RAFT_TELEMETRY_OTLP_ENDPOINT=http://127.0.0.1:4318/v1/metrics
export RAFT_TELEMETRY_OTLP_TIMEOUT_MILLIS=2000
export RAFT_TELEMETRY_OTLP_HEADERS=Authorization=Bearer token

# OTEL JSON log (single-line JSON emitted via tracing::info!(target: "OTEL"))
export RAFT_TELEMETRY_EXPORTER=otel-log

# Common
export RAFT_TELEMETRY_EXPORT_INTERVAL_SECONDS=15
```

For Prometheus, the node starts an HTTP listener on the configured port and serves metric text at the configured path (using only tokio, no external HTTP library). For OTLP, the node periodically pushes metrics to the configured endpoint via `reqwest`. For OTEL JSON log, the node periodically emits single-line JSON via `tracing::info!(target: "OTEL")`. Metrics include term, commit/applied/log/snapshot indices, member counts, replication status, and reconfiguration age.

## Build & Run

### Building

```bash
cargo build --release
```

The `graft-app-kv` crate produces the `graft-smoke` binary.

### Run

First start a local Java node or cluster. Then from the repository root:

```bash
# Passive (non-participating) server for transport checks
graft-rust/target/release/graft-smoke serve 127.0.0.1 11080 rust-stub

# Active server with elections, replication, membership
graft-rust/target/release/graft-smoke serve-active 127.0.0.1 11082 rust-node 3 7 3 peer-a@127.0.0.1:11081

# Active persistent server with file-backed state
graft-rust/target/release/graft-smoke serve-active-persistent 127.0.0.1 11083 rust-node /tmp/rust-node.state 3 7 3 peer-a@127.0.0.1:11081

# Client operations
graft-rust/target/release/graft-smoke client-put 127.0.0.1 11082 k v1 rust-node
graft-rust/target/release/graft-smoke client-get 127.0.0.1 11082 k rust-node
graft-rust/target/release/graft-smoke client-cas 127.0.0.1 11082 k false "" v1 rust-node

# Membership management
graft-rust/target/release/graft-smoke join-cluster 127.0.0.1 11083 peer-b 127.0.0.1 11084 voter rust-node
graft-rust/target/release/graft-smoke join-status 127.0.0.1 11083 peer-b rust-node
graft-rust/target/release/graft-smoke reconfigure 127.0.0.1 11083 joint peer-a@127.0.0.1:11081 peer-b@127.0.0.1:11084
graft-rust/target/release/graft-smoke reconfigure 127.0.0.1 11083 finalize peer-a@127.0.0.1:11081 peer-b@127.0.0.1:11084

# Operational
graft-rust/target/release/graft-smoke cluster-summary 127.0.0.1 10080
graft-rust/target/release/graft-smoke telemetry 127.0.0.1 10080
graft-rust/target/release/graft-smoke compact-snapshot 127.0.0.1 11083

# Smoke-test protocol operations
graft-rust/target/release/graft-smoke vote-request 127.0.0.1 10080 rust-candidate 0 0
graft-rust/target/release/graft-smoke append-entries 127.0.0.1 10080 rust-leader 0 0 0
graft-rust/target/release/graft-smoke install-snapshot 127.0.0.1 10080 rust-leader 0 0
graft-rust/target/release/graft-smoke election-round rust-node 3 7 3 peer-a@127.0.0.1:11081
graft-rust/target/release/graft-smoke heartbeat-round rust-node 3 7 3 peer-a@127.0.0.1:11081
graft-rust/target/release/graft-smoke replicate-once rust-node 3 hello 7 3 peer-a@127.0.0.1:11081
```

Cross-language smoke suites:

```bash
graft-rust/tests/scripts/run-mixed-smoke-rust-leader.sh
graft-rust/tests/scripts/run-mixed-smoke-java-leader.sh
graft-rust/tests/scripts/run-mixed-suite.sh
graft-rust/tests/scripts/run-interop-smoke.sh
```

### Testing

```bash
cargo test                    # All workspace tests
cargo test -p graft-tests     # Integration tests only
cargo test -p graft-core      # Core unit tests
```

Integration tests in `graft-tests/tests/integration_test.rs` cover elections, AppendEntries, membership transitions, snapshots, storage persistence, and non-voter/decommission behavior, using in-memory stubs for all transport and storage backends.

## Important Boundary

This implementation reuses the existing protocol and framing and exercises real replicated-node paths, but it is still not a production-grade peer. The Java implementation remains the reference for:

- Java runtime wiring
- full storage implementation
- reference-data application payloads
- full Java-equivalent read lease tracking and richer membership-transition internals
- the default Maven/JUnit/Jepsen validation path

The Rust side is currently best understood as an interoperability and convergence track: it proves the shared wire protocol, replicated KV behavior, membership admission/promotion, persistence, snapshot catch-up, and mixed Java/Rust smoke scenarios.

## Next Steps

The natural next steps are now convergence and hardening:

1. split the active runtime further into clearer wire/core/storage/state-machine/runtime/transport/app Cargo crates as the code grows
2. make Rust membership status expose richer transition metadata
3. broaden mixed Jepsen coverage for Rust peers
4. continue aligning Rust persistence and recovery semantics with the Java reference implementation
