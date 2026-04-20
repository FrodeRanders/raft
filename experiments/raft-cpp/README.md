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
- `client-put`
- `client-cas`
- `client-get`
- `join-cluster`
- `join-status`
- `reconfigure`
- `vote-request`
  - `append-entries`
  - `install-snapshot`
  - `serve`
  - `serve-stateful`
  - `serve-persistent`
  - `serve-active`
  - `serve-active-persistent`
  - `serve-active-persistent-workload`
  - `election-round`
  - `heartbeat-round`
  - `replicate-once`
  - `replicate-once-persistent`
  - `replicate-put-persistent`
  - `compact-snapshot`
  - `dump-state`

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
experiments/raft-cpp/build/raft_cpp_smoke client-put 127.0.0.1 11082 k v1 cpp-node
experiments/raft-cpp/build/raft_cpp_smoke client-cas 127.0.0.1 11082 k false "" v1 cpp-node
experiments/raft-cpp/build/raft_cpp_smoke client-get 127.0.0.1 11082 k cpp-node
experiments/raft-cpp/build/raft_cpp_smoke join-cluster 127.0.0.1 11083 peer-b 127.0.0.1 11084 voter cpp-node
experiments/raft-cpp/build/raft_cpp_smoke join-status 127.0.0.1 11083 peer-b cpp-node
experiments/raft-cpp/build/raft_cpp_smoke reconfigure 127.0.0.1 11083 joint peer-a@127.0.0.1:11081 peer-b@127.0.0.1:11084
experiments/raft-cpp/build/raft_cpp_smoke reconfigure 127.0.0.1 11083 finalize peer-a@127.0.0.1:11081 peer-b@127.0.0.1:11084
experiments/raft-cpp/build/raft_cpp_smoke vote-request 127.0.0.1 10080 cpp-candidate 0 0
experiments/raft-cpp/build/raft_cpp_smoke append-entries 127.0.0.1 10080 cpp-leader 0 0 0
experiments/raft-cpp/build/raft_cpp_smoke install-snapshot 127.0.0.1 10080 cpp-leader 0 0
experiments/raft-cpp/build/raft_cpp_smoke install-snapshot 127.0.0.1 10080 cpp-leader 6 3 3 snapshot-alpha
experiments/raft-cpp/build/raft_cpp_smoke serve 127.0.0.1 11080 cpp-stub
experiments/raft-cpp/build/raft_cpp_smoke serve-stateful 127.0.0.1 11081 cpp-node 3 7 3
experiments/raft-cpp/build/raft_cpp_smoke serve-persistent 127.0.0.1 11082 cpp-node /tmp/cpp-node.state 3 7 3 peer-a@127.0.0.1:11081
experiments/raft-cpp/build/raft_cpp_smoke serve-active 127.0.0.1 11082 cpp-node 3 7 3 peer-a@127.0.0.1:11081
experiments/raft-cpp/build/raft_cpp_smoke serve-active-persistent 127.0.0.1 11083 cpp-node /tmp/cpp-node.state 3 7 3 peer-a@127.0.0.1:11081
experiments/raft-cpp/build/raft_cpp_smoke serve-active-persistent-workload 127.0.0.1 11084 cpp-node /tmp/cpp-node.state 3 7 3 1500 peer-a@127.0.0.1:11081
experiments/raft-cpp/build/raft_cpp_smoke serve-active-persistent-workload 127.0.0.1 11084 cpp-node /tmp/cpp-node.state 3 7 3 1500 4 peer-a@127.0.0.1:11081
experiments/raft-cpp/build/raft_cpp_smoke election-round cpp-node 3 7 3 peer-a@127.0.0.1:11081
experiments/raft-cpp/build/raft_cpp_smoke heartbeat-round cpp-node 3 7 3 peer-a@127.0.0.1:11081
experiments/raft-cpp/build/raft_cpp_smoke replicate-once cpp-node 3 hello 7 3 peer-a@127.0.0.1:11081
experiments/raft-cpp/build/raft_cpp_smoke replicate-once-persistent cpp-node /tmp/cpp-node.state 3 hello 7 3 peer-a@127.0.0.1:11081
experiments/raft-cpp/build/raft_cpp_smoke replicate-put-persistent cpp-node /tmp/cpp-node.state 3 k v1 7 3 peer-a@127.0.0.1:11081
experiments/raft-cpp/build/raft_cpp_smoke compact-snapshot cpp-node /tmp/cpp-node.state 8 compact-alpha 3 8 3
experiments/raft-cpp/build/raft_cpp_smoke dump-state /tmp/cpp-node.state
experiments/raft-cpp/run-interop-smoke.sh
```

Optional peer id override:

```text
experiments/raft-cpp/build/raft_cpp_smoke cluster-summary 127.0.0.1 10080 cpp-cli
```

The replication probes intentionally default to `term=0` unless you pass an explicit final argument. That makes them useful as transport-compatibility checks without assuming the C++ side is participating in the live cluster correctly yet.

The CLI `install-snapshot` command sends a single request with:

- `offset=0`
- `done=true`
- optional `snapshot_data`

That is enough for direct probing, and the runtime now also supports chunked snapshot fallback during follower catch-up. So the bounded prototype can now:

- persist snapshot metadata
- persist snapshot payload
- discard covered synthetic log entries
- recover that snapshot state after restart
- stream snapshot payload to followers in multiple `InstallSnapshotRequest` chunks during catch-up

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

The `serve-persistent` command uses the same basic state model, but persists node metadata and the synthetic entry list to a small local file. It can also be started with optional `peer-spec` arguments so follower-side client redirects can include concrete leader host/port metadata. It currently persists:

- current term
- voted-for
- leader id
- explicit voting-peer membership
- last log index/term
- commit index
- snapshot index/term
- snapshot payload
- last applied index
- applied key/value state for replicated `StateMachineCommand` entries
- previous log position
- the latest synthetic entry payload used by the experiment
- the synthetic entry list generated by the current prototype
- per-peer replication progress (`next_index` and `match_index`)

This is still far from a real storage engine, but it is enough to validate restart recovery for the current bounded C++ prototype.

The C++ server now also accepts the shared client envelopes:

- `ClientCommandRequest`
- `ClientQueryRequest`

For the bounded prototype, queries operate on the applied KV state with leader-only semantics once a node is participating in a multi-node topology. Commands now work in two modes:

- single-node persistent servers auto-commit locally
- active leaders replicate them through the existing distributed runtime

The remaining gap is broader cluster-grade behavior such as redirect host/port metadata, retries, and richer client semantics, not the basic replicated command path itself.

The stateful and active C++ servers now also answer the existing admin probes:

- `cluster-summary`
- `telemetry`

That makes it possible to inspect follower and leader log/commit state through the same shared protocol, instead of relying only on server log output. After restart, both views rebuild member rows from persisted voting membership plus per-peer progress, so recovered topology and replication state stay visible without reading the state file directly.

The `serve-active` command is the first combined inbound/outbound node mode.

- it starts the inbound C++ RPC server
- it shares one `raftcpp::RaftNode` between that server and a small outbound runtime
- if the node is not leader, it waits for a randomized election timeout and then runs an election round
- if the node is leader, it sends heartbeat rounds on a shorter fixed cadence
- those rounds now also perform bounded follower catch-up: heartbeat when current, append when lagging behind the log, and snapshot fallback when lagging behind the snapshot boundary
- inbound leader traffic resets the local election deadline via shared node activity tracking
- scheduling is driven by Asio timers rather than a fixed sleep loop

This is still intentionally minimal:

- no persistent storage
- no full log replication
- no membership changes
- no state machine application

But it is the first mode where one C++ process can both receive and initiate Raft RPCs using the shared wire protocol.

The `serve-active-persistent` command combines that active runtime with the same small file-backed state store used by `serve-persistent`. That means the bounded prototype can now retain active-node term/log/commit state across restart as well.

The `serve-active-persistent-workload` command extends that further by periodically originating bounded synthetic entries once the node is leader. That gives the prototype a self-contained way to exercise persistent leader-side replication and restart recovery without depending on a separate one-shot CLI command.

It also accepts an optional `snapshot-threshold` argument after `replicate-interval-ms`. When present, the leader auto-compacts its own persistent log into a local snapshot once `commit_index - snapshot_index` reaches that threshold.

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
  - sends an `AppendEntriesRequest` carrying any missing synthetic entries to each listed peer
  - retries with progressively earlier `prev_log_index` values when a follower rejects the append
  - falls back to `InstallSnapshotRequest` once a follower backs up to the leader's snapshot boundary
  - updates per-peer match progress from the responses
  - advances the leader commit index once the entry reaches quorum
  - sends a follow-up heartbeat carrying the new `leader_commit` so followers can observe the commit decision
- `replicate-once-persistent`
  - does the same bounded replication step
  - but flushes leader-side state to a file-backed store before and after the replication round
  - so the resulting committed synthetic entry can be recovered after restart
- `replicate-put-persistent`
  - builds a real protobuf `StateMachineCommand.put`
  - replicates and commits it through the same bounded persistent path
  - lets the prototype exercise durable application-state changes instead of only opaque log bytes
- `compact-snapshot`
  - loads or initializes a persistent node state file
  - compacts the synthetic log up to a committed index into a local snapshot artifact
  - persists the resulting snapshot boundary and truncated post-snapshot log tail
- `dump-state`
  - loads a persistent node state file
  - prints the stored application state, including `last_applied` and key/value entries

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

Recent bounded replication work also means the prototype can now demonstrate follower catch-up over multiple `AppendEntries` attempts instead of only a single happy-path append. In a local smoke run with the leader at log index `7` and a follower at index `5`, `replicate-once` needed three attempts before the follower accepted the new entry and the leader advanced commit:

```text
replication-response peer=peer-a attempt=1 success=false match_index=0
replication-response peer=peer-a attempt=2 success=false match_index=0
replication-response peer=peer-a attempt=3 success=true match_index=8
heartbeat-response peer=peer-a success=true match_index=8
```

Snapshot handling is now also meaningful in the same bounded sense. In a local smoke run against a persistent follower, sending:

```text
raft_cpp_smoke install-snapshot 127.0.0.1 11281 cpp-leader 6 3 3 snapshot-alpha
```

produced persisted recovery state like:

```text
last_log_index=7
commit_index=6
snapshot_index=6
snapshot_term=3
snapshot_data=snapshot-alpha
previous_log_index=6
previous_log_term=3
log_entry=7,3,bootstrap-7
```

So the follower retained the snapshot artifact and truncated the covered log prefix on disk.

Leader-side catch-up now also uses that snapshot state. In a local smoke run where the leader had a persisted snapshot at index `6` and the follower was still at index `5`, `replicate-once-persistent` produced:

```text
replication-response peer=peer-a attempt=1 success=false match_index=0
replication-response peer=peer-a attempt=2 success=false match_index=0
snapshot-response peer=peer-a attempt=3 success=true last_included_index=6
replication-response peer=peer-a attempt=4 success=true match_index=8
heartbeat-response peer=peer-a success=true match_index=8
```

So the bounded C++ runtime can now demonstrate both append backtracking and snapshot fallback during follower catch-up.

The prototype can now also create that snapshot boundary locally instead of depending only on a separately injected `install-snapshot`. In a local smoke run:

```text
raft_cpp_smoke compact-snapshot cpp-node /tmp/cpp-node.state 8 compact-alpha 3 8 3
```

produced:

```text
compacted: true
snapshot_index: 8
snapshot_term: 3
snapshot_data_size: 13
last_log_index: 8
commit_index: 8
```

and a later catch-up run against a follower at index `5` used that locally-created snapshot boundary:

```text
replication-response peer=peer-a attempt=1 success=false match_index=0
snapshot-response peer=peer-a attempt=2 success=true last_included_index=8
replication-response peer=peer-a attempt=3 success=true match_index=9
heartbeat-response peer=peer-a success=true match_index=9
```

The active persistent workload mode can now create those snapshots on its own. In a local smoke run with a low threshold:

```text
raft_cpp_smoke serve-active-persistent-workload 127.0.0.1 11311 cpp-node /tmp/cpp-node.state 3 7 3 300 2 peer-a@127.0.0.1:11312
```

the resulting persisted leader state showed:

```text
last_log_index=18
commit_index=18
snapshot_index=18
snapshot_term=4
snapshot_data=auto-snapshot-18
```

So the bounded prototype can now originate entries, commit them, compact them locally into snapshots, and later use those snapshots for follower catch-up.

That snapshot fallback is now chunked rather than single-shot. In a local smoke run with a leader snapshot payload of `chunked-snapshot-payload`, the leader logged:

```text
replication-response peer=peer-a attempt=1 success=false match_index=0
snapshot-response peer=peer-a attempt=2 offset=0 bytes=8 success=true last_included_index=8
snapshot-response peer=peer-a attempt=2 offset=8 bytes=8 success=true last_included_index=8
snapshot-response peer=peer-a attempt=2 offset=16 bytes=8 success=true last_included_index=8
replication-response peer=peer-a attempt=3 success=true match_index=9
heartbeat-response peer=peer-a success=true match_index=9
```

and the persistent follower ended up with:

```text
last_log_index=9
commit_index=9
snapshot_index=8
snapshot_term=3
snapshot_data=chunked-snapshot-payload
log_entry=9,3,tail
```

The prototype now also applies committed `StateMachineCommand` entries into a bounded KV state machine and persists that applied state. In a local smoke run:

1. `replicate-put-persistent` committed `put(k, v1)` on the leader
2. the leader compacted to a snapshot
3. a lagging follower caught up through snapshot fallback

and `dump-state` on that follower produced:

```text
peer_id: peer-b
current_term: 3
commit_index: 9
last_applied: 9
snapshot_index: 8
snapshot_term: 3
kv[k]=v1
```

So the bounded C++ prototype now carries real replicated application state through commit, persistence, local compaction, and follower snapshot recovery.

That state machine is now reachable through the shared client API in the single-node case. In a local smoke run against `serve-persistent`:

```text
raft_cpp_smoke client-put 127.0.0.1 11381 k v1 cpp-node
raft_cpp_smoke client-get 127.0.0.1 11381 k cpp-node
```

the responses were:

```text
peer_id: cpp-node
status: OK
success: true
leader_id: cpp-node
leader_host: 127.0.0.1
leader_port: 11381
message: command committed and applied
```

and:

```text
peer_id: cpp-node
status: OK
success: true
leader_id: cpp-node
leader_host: 127.0.0.1
leader_port: 11381
message: query completed
found: true
value: v1
```

with persisted state:

```text
peer_id: cpp-node
current_term: 3
commit_index: 1
last_applied: 1
snapshot_index: 0
snapshot_term: 0
kv[k]=v1
```

The same client API path now also works through the active multi-node prototype. In a two-node smoke run against `serve-active-persistent` plus one persistent follower:

```text
raft_cpp_smoke client-put 127.0.0.1 11391 k v1 cpp-node
raft_cpp_smoke client-cas 127.0.0.1 11391 k true v1 v2 cpp-node
raft_cpp_smoke client-get 127.0.0.1 11391 k cpp-node
```

returned:

```text
peer_id: cpp-node
status: OK
success: true
leader_id: cpp-node
leader_host: 127.0.0.1
leader_port: 11391
message: command committed and applied
```

and:

```text
peer_id: cpp-node
status: OK
success: true
leader_id: cpp-node
leader_host: 127.0.0.1
leader_port: 11391
message: query completed
found: true
value: v1
```

while the follower persisted:

```text
peer_id: peer-a
current_term: 4
commit_index: 8
last_applied: 8
snapshot_index: 0
snapshot_term: 0
kv[k]=v1
```

So the bounded C++ prototype now supports the shared client command/query API for both single-node and active multi-node replicated KV operations.

`ClientCommandResponse.result` is now populated for commands that have a typed application result. In the current shared protocol that means `CAS`. For example, in both the single-node and distributed smokes:

```text
raft_cpp_smoke client-cas 127.0.0.1 11583 k false "" v1 cpp-client
raft_cpp_smoke client-cas 127.0.0.1 11583 k true nope v2 cpp-client
```

returned:

```text
peer_id: cpp-node
status: OK
success: true
leader_id: cpp-node
leader_host: 127.0.0.1
leader_port: 11583
message: command committed and applied
cas.key: k
cas.matched: true
cas.expected_present: false
cas.expected_value:
cas.new_value: v1
cas.current_present: true
cas.current_value: v1
```

and then:

```text
peer_id: cpp-node
status: OK
success: true
leader_id: cpp-node
leader_host: 127.0.0.1
leader_port: 11583
message: command committed and applied
cas.key: k
cas.matched: false
cas.expected_present: true
cas.expected_value: nope
cas.new_value: v2
cas.current_present: true
cas.current_value: v1
```

So the C++ prototype now mirrors the Java-side behavior more closely at the client boundary: commands are not just committed and applied, they can also return typed application results when the protocol defines them.

The prototype now also has a bounded membership-control path over the shared admin envelopes:

- `join-cluster`
  - commits a bounded join-admission command through a replicated `InternalRaftCommand`
- `join-status`
  - reports `PENDING`, `IN_JOINT_CONSENSUS`, `COMPLETED`, or `UNKNOWN`
- `reconfigure joint`
  - commits a bounded joint configuration through a replicated `InternalRaftCommand`
  - then updates the active runtime peer set after the command has committed
- `reconfigure finalize`
  - commits a bounded finalize command through the same replicated internal-command path

This is not yet a full Java-parity membership implementation. In particular:

- it does not implement learner catch-up rules or joint-quorum commit mechanics
- it is a bounded control-plane scaffold for the C++ prototype, not a production-grade reconfiguration engine

But it is enough to exercise the shared protocol and a realistic local flow. In a local smoke run:

```text
raft_cpp_smoke join-cluster 127.0.0.1 11682 peer-b 127.0.0.1 11683 voter cpp-client
raft_cpp_smoke join-status 127.0.0.1 11682 peer-b cpp-client
raft_cpp_smoke reconfigure 127.0.0.1 11682 joint peer-a@127.0.0.1:11681 peer-b@127.0.0.1:11683
raft_cpp_smoke cluster-summary 127.0.0.1 11682 cpp-client
raft_cpp_smoke reconfigure 127.0.0.1 11682 finalize peer-a@127.0.0.1:11681 peer-b@127.0.0.1:11683
raft_cpp_smoke join-status 127.0.0.1 11682 peer-b cpp-client
```

the observed states were:

```text
status: PENDING
```

and that pending admission is now replicated too. In a local smoke run:

```text
raft_cpp_smoke join-status 127.0.0.1 11982 peer-b cpp-client
raft_cpp_smoke join-status 127.0.0.1 11981 peer-b cpp-client
```

both leader and follower returned:

```text
status: PENDING
```

then:

```text
status: IN_JOINT_CONSENSUS
joint_consensus: true
members: 3
member[cpp-node] ...
member[peer-a] ...
member[peer-b] ...
```

and finally:

```text
status: COMPLETED
joint_consensus: false
members: 3
```

After the latest refinement, the important signal is that the follower now learns those membership transitions through committed internal log application too. In a local smoke run:

```text
raft_cpp_smoke cluster-summary 127.0.0.1 11882 cpp-client
raft_cpp_smoke cluster-summary 127.0.0.1 11881 cpp-client
```

both leader and follower reported the same committed joint configuration:

```text
joint_consensus: true
members: 3
member[cpp-node] ...
member[peer-a] ...
member[peer-b] ...
```

and after `reconfigure ... finalize ...` both sides reported:

```text
joint_consensus: false
members: 3
```

So the bounded C++ prototype now has a real replicated membership-control seam: the leader drives reconfiguration through internal log entries, and followers reflect the committed membership state rather than relying on leader-local handler mutation.

Follower-side query behavior is now also aligned with that model. In a two-node smoke run, querying the passive follower directly returned:

```text
peer_id: peer-a
status: NOT_LEADER
success: false
leader_id: cpp-node
leader_host: 127.0.0.1
leader_port: 11391
message: query must target leader
```

So once a node has learned another leader, it no longer auto-promotes itself for client reads just because it lacks an explicit peer configuration file, and client redirects can now be actionable when the node was started with known peer endpoints.

That catch-up is no longer limited to the one-shot `replicate-once` path. In a local smoke run with a persistent leader started in `serve-active-persistent` and a follower behind the leader's snapshot boundary, the follower recovered during normal scheduled leader rounds and persisted:

```text
last_log_index=8
commit_index=8
snapshot_index=8
snapshot_term=3
snapshot_data=compact-alpha
```

So the timer-driven active node can now use append or snapshot fallback during ordinary leader maintenance, not just during explicit replication commands.
