# raft-cpp

This directory is a disconnected C++ transport experiment. It does not participate in the Maven build and does not interfere with the Java implementation.

The goal is not to rewrite the whole project immediately. The first goal is narrower:

- reuse the existing protobuf contract from `raft-wire/src/main/proto/raft.proto`
- reuse the existing `Envelope` framing used by the Java Netty transport
- prove that a C++ process can talk to a running Java node over the same wire protocol

## Scope

The current scaffold implements:

- protobuf code generation from the existing shared `raft.proto`
- Boost.Asio TCP transport
- raw varint32 length framing compatible with the Java transport
- `Envelope { correlation_id, type, payload }` request/response exchange
- a small CLI with:
  - `cluster-summary`
  - `telemetry`
  - `vote-request`
  - `append-entries`
  - `install-snapshot`
  - `serve`
  - `serve-stateful`
  - `serve-active`
  - `election-round`
  - `heartbeat-round`
  - `replicate-once`

This is enough to establish the wire-level interoperability path before migrating any Raft logic.

There is also a disconnected Java-side probe under [java-probe/README.md](java-probe/README.md) which reuses the existing Netty transport client to send real Java-originated Raft RPCs into the C++ endpoint.

## Why Boost.Asio

The Java implementation uses Netty, but Netty is only the transport substrate. The actual on-the-wire protocol is:

1. raw varint32 length prefix
2. serialized protobuf `Envelope`
3. `Envelope.type` naming the request/response message
4. protobuf payload inside `Envelope.payload`

Boost.Asio is a good C++ replacement for the transport layer because it gives explicit control over TCP I/O without forcing a new protocol.

## Build

Prerequisites:

- CMake
- a C++20 compiler
- protobuf compiler and C++ runtime
- Boost.Asio / Boost.System

Build from the repository root:

```text
cmake -S experiments/raft-cpp -B experiments/raft-cpp/build
cmake --build experiments/raft-cpp/build
```

That produces:

```text
experiments/raft-cpp/build/raft_cpp_smoke
```

The build now also defines reusable CMake targets:

- `raftcpp_proto`
  - generated protobuf types for the shared `raft.proto`
- `raftcpp_transport`
  - header-level transport/client/server/handler layer for follow-on C++ work

## Run

First start a local Java node or cluster, for example with the existing jar/scripts.

Then from the repository root:

```text
experiments/raft-cpp/build/raft_cpp_smoke cluster-summary 127.0.0.1 10080
experiments/raft-cpp/build/raft_cpp_smoke telemetry 127.0.0.1 10080
experiments/raft-cpp/build/raft_cpp_smoke vote-request 127.0.0.1 10080 cpp-candidate 0 0
experiments/raft-cpp/build/raft_cpp_smoke append-entries 127.0.0.1 10080 cpp-leader 0 0 0
experiments/raft-cpp/build/raft_cpp_smoke install-snapshot 127.0.0.1 10080 cpp-leader 0 0
experiments/raft-cpp/build/raft_cpp_smoke serve 127.0.0.1 11080 cpp-stub
experiments/raft-cpp/build/raft_cpp_smoke serve-stateful 127.0.0.1 11081 cpp-node 3 7 3
experiments/raft-cpp/build/raft_cpp_smoke serve-active 127.0.0.1 11082 cpp-node 3 7 3 peer-a@127.0.0.1:11081
experiments/raft-cpp/build/raft_cpp_smoke election-round cpp-node 3 7 3 peer-a@127.0.0.1:11081
experiments/raft-cpp/build/raft_cpp_smoke heartbeat-round cpp-node 3 7 3 peer-a@127.0.0.1:11081
experiments/raft-cpp/build/raft_cpp_smoke replicate-once cpp-node 3 hello 7 3 peer-a@127.0.0.1:11081
experiments/raft-cpp/run-interop-smoke.sh
```

Optional peer id override:

```text
experiments/raft-cpp/build/raft_cpp_smoke cluster-summary 127.0.0.1 10080 cpp-cli
```

The replication probes intentionally default to `term=0` unless you pass an explicit final argument. That makes them useful as transport-compatibility checks without assuming the C++ side is participating in the live cluster correctly yet.

The current `install-snapshot` command sends a minimal single-chunk request with:

- `offset=0`
- `done=true`
- empty `snapshot_data`

That is enough to verify wire compatibility and response decoding. It is not yet a semantic snapshot-transfer implementation.

The `serve` command starts a minimal inbound C++ endpoint that accepts:

- `VoteRequest`
- `AppendEntriesRequest`
- `InstallSnapshotRequest`

and returns well-formed protobuf responses using the same `Envelope` framing as the Java transport.

This server is intentionally a stub:

- it does not maintain real Raft state
- it does not participate safely in a mixed cluster
- it replies with conservative non-participating responses
  - vote not granted
  - append not accepted
  - snapshot not accepted

Its purpose is transport compatibility testing only.

The `serve-stateful` command wires in a simple in-memory handler with a little local Raft-like state:

- current term
- remembered vote
- last log index/term
- last installed snapshot index/term

It is still not a real Raft node, but it behaves more plausibly:

- grants votes only when the candidate term and log look acceptable
- accepts append requests only when the previous log position matches its in-memory state
- accepts snapshot installation and updates its local snapshot/log position

This is useful for probing Java-to-C++ transport with responses that depend on request content instead of fixed stub answers.

The `serve-active` command is the first combined inbound/outbound node mode.

- it starts the inbound C++ RPC server
- it shares one `raftcpp::RaftNode` between that server and a small outbound runtime
- if the node is not leader, it waits for a randomized election timeout and then runs an election round
- if the node is leader, it sends heartbeat rounds on a shorter fixed cadence
- inbound leader traffic resets the local election deadline via shared node activity tracking
- scheduling is driven by Asio timers rather than a fixed sleep loop

This is still intentionally minimal:

- no persistent storage
- no full log replication
- no membership changes
- no state machine application

But it is the first mode where one C++ process can both receive and initiate Raft RPCs using the shared wire protocol.

The new `election-round` and `heartbeat-round` commands are the first outbound-runtime layer on the C++ side.

- `election-round`
  - starts a local election in `raftcpp::RaftNode`
  - sends `VoteRequest` RPCs to the listed peers
  - feeds the returned `VoteResponse` values back into the node
  - reports whether quorum was reached and the node became leader
- `heartbeat-round`
  - forces the local node into leader state
  - builds heartbeat-style `AppendEntriesRequest` messages for each listed peer
  - sends them and updates per-peer replication progress from the responses
- `replicate-once`
  - forces the local node into leader state
  - appends one synthetic local log entry
  - sends a non-empty `AppendEntriesRequest` carrying that entry to each listed peer
  - updates per-peer match progress from the responses

Peer lists for these commands use:

```text
<peer-id>@<host>:<port>
```

For example:

```text
experiments/raft-cpp/build/raft_cpp_smoke election-round cpp-node 3 7 3 peer-a@127.0.0.1:11081 peer-b@127.0.0.1:11082
```

`run-interop-smoke.sh` automates the current mixed-language check:

- starts the C++ stateful server in the background
- sends Java `vote-request`, `append-entries`, and `install-snapshot` probes into it
- prints the C++ server log
- tears the C++ server down automatically

It defaults to:

- host `127.0.0.1`
- port `11081`
- peer id `cpp-node`
- term `3`
- last log index `7`
- last log term `3`

These can be overridden with:

- `RAFT_CPP_BIN`
- `RAFT_CPP_INTEROP_HOST`
- `RAFT_CPP_INTEROP_PORT`
- `RAFT_CPP_INTEROP_PEER_ID`
- `RAFT_CPP_INTEROP_TERM`
- `RAFT_CPP_INTEROP_LAST_LOG_INDEX`
- `RAFT_CPP_INTEROP_LAST_LOG_TERM`

Internally, the server now routes inbound RPCs through a small handler interface. The current CLI wires in a default `StubRpcHandler`, but that seam is intended to be replaced later by real C++ Raft logic.

That transition has now started:

- `raftcpp::RaftNode` holds term, role, leader, vote, log position, commit index, and snapshot position
- it also tracks configured voting peers, quorum size, candidate vote accumulation, and per-peer replication progress
- the stateful handler delegates into that node object instead of keeping Raft-ish state inline inside the handler

This is still only a partial implementation, but it moves the experiment from "transport stub with ad hoc state" toward a real consensus core.

## Current Design

- `include/raftcpp/envelope_codec.hpp`
  - varint32 framing compatible with the Java encoder/decoder
- `include/raftcpp/raft_client.hpp`
  - minimal synchronous request/response client over Boost.Asio
- `include/raftcpp/raft_node.hpp`
  - early C++ Raft state core for vote, append, snapshot, election, and heartbeat request handling
- `include/raftcpp/raft_runtime.hpp`
  - minimal active runtime that fans out vote requests and heartbeats to configured peer endpoints
- `include/raftcpp/rpc_handler.hpp`
  - pluggable handler interface for inbound Raft RPCs plus stub and stateful adapters
- `include/raftcpp/raft_server.hpp`
  - minimal synchronous inbound dispatcher for the core Raft RPC envelopes
- `raftcpp_transport` CMake target
  - reusable transport/protocol layer for future C++ node work
- `java-probe/`
  - disconnected Java interoperability probe using the existing Java transport stack
- `src/main.cpp`
  - simple CLI proving interoperability against Java nodes

## Next Steps

The natural next steps, in order, are:

1. make the server event-driven
2. implement semantic multi-chunk `install-snapshot`
3. move the inbound server from a blocking loop to the same async/event-driven model as the active scheduler
4. implement a minimal C++ Raft node using the shared wire protocol
5. test mixed Java/C++ clusters explicitly

## Important Boundary

This experiment reuses the existing protocol and framing, but it does not yet attempt to reproduce:

- Java runtime wiring
- storage implementation
- membership logic
- state-machine application
- Jepsen integration

It is deliberately focused on transport compatibility first.
