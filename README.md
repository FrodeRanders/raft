# Graft

[![CI](https://github.com/FrodeRanders/raft/actions/workflows/ci.yml/badge.svg)](https://github.com/FrodeRanders/raft/actions/workflows/ci.yml)
[![License: Apache 2.0](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
![Java](https://img.shields.io/badge/Java-25-green.svg)
[![Version](https://img.shields.io/badge/version-1.0--SNAPSHOT-yellow.svg)](pom.xml)

This repo contains three *interoperable* implementations of the Raft consensus protocol, making it possible to build clusters with applications developed in Java, C++, and Rust:

- An asynchronous Raft **reference implementation** in Java, built on [Netty 4.2](https://netty.io).
- An asynchronous [Raft implementation in C++](./graft-cpp/README.md), built on [Boost.Asio](https://www.boost.org/doc/libs/release/doc/html/boost_asio.html).
- An asynchronous [Raft implementation in Rust](./graft-rust/README.md), built on [Tokio](https://tokio.rs).

All three implementations support Raft joint-consensus reconfiguration, persisted configuration state, and chunked `InstallSnapshot` transfer.

## Shared Wire Contract & Architecture

All three share a single protobuf contract and wire protocol, making them interoperable in the same cluster. The coupling is at the protocol boundary, while builds and runtimes remain independent.

```text
                 shared contract
        ../raft-wire/src/main/proto/raft.proto
          Envelope + Raft RPCs + KV commands
                      |
        +-------------+-------------+------------------------+
        |                           |                        | 
     Java impl.                 C++ impl.                Rust impl.
  Maven reactor                graft-cpp                 graft-rust
        |                           |                        |
 Netty transport              Boost.Asio                  Tokio
 Java RaftNode/runtime        C++ RaftNode/runtime        Rust RaftNode/runtime
 Java storage/state machine   C++ storage/state machine   Rust storage/state machine
```

The on-the-wire protocol is identical for all three implementations:

1. raw varint32 length prefix
2. serialized protobuf `Envelope { correlation_id, type, payload }`
3. `Envelope.type` naming the RPC request or response message
4. protobuf payload inside `Envelope.payload`

All three encode replicated client work as `StateMachineCommand`, membership transitions as `InternalRaftCommand`, and expose telemetry, cluster summaries, reconfiguration status, and redirect metadata through the same shared message types.

### Layer Mapping

Each implementation mirrors the same conceptual layers:

| Layer | Java (Maven module) | C++ (header dir) | Rust (Cargo crate) |
|---|---|---|---|
| Wire protocol | `raft-wire` | `include/graft/wire/` | `graft-proto` |
| Transport | `raft-transport-netty` (Netty) | `include/graft/transport/` (Boost.Asio) | `graft-transport` (Tokio) |
| Core consensus | `raft-core` | `include/graft/core/` | `graft-core` |
| Membership | `raft-membership` | (in core) | (in core) |
| State machine | `raft-state-machine` | (in app) | `graft-core` (traits) |
| Storage | `raft-storage` | `include/graft/storage/` | `graft-storage` |
| Runtime | `raft-runtime` | `include/graft/runtime/` | `graft-runtime` |
| Application | `raft-app-kv`, `raft-app-reference` | `include/graft/app/` | `graft-app-kv` |
| Telemetry | `raft-telemetry` | `include/graft/telemetry/` | `graft-telemetry` |
| Tests | `raft-tests` | `tests/` (Catch2) | `graft-tests` |

### Cross-Language Coverage

Mixed-language validation currently covers:

- Java, C++, and Rust nodes participating in the same cluster
- Java CLI clients talking to Java, C++, or Rust leaders
- C++ and Rust `graft_smoke` clients driving Jepsen put/get/CAS workloads
- Followers catching up from leaders of any implementation language
- Membership joiners admitted by leaders of any implementation language
- Learner promotion, role-aware membership summaries, and reconfiguration across languages

```text
cd jepsen
./run-suite.sh mixed
graft-cpp/run-mixed-suite.sh
```

### Important Boundary

All three implementations reuse the exact same protocol and framing, and all three exercise real replicated-node paths, but the **Java implementation remains the production-grade reference**. The C++ and Rust implementations are interoperability and convergence tracks. See their respective READMEs for current scope and remaining limitations.

## Building

### Java (reference)

Builds as a Maven reactor from the repository root:

```text
mvn -q package
```

The runnable shaded jar is produced at `raft-dist/target/raft-1.0-SNAPSHOT.jar`.

Java modules: `raft-wire` → `raft-membership` → `raft-state-machine` → `raft-storage` → `raft-core` → `raft-transport-netty` → `raft-telemetry` → `raft-runtime` → `raft-app-kv` → `raft-app-reference` → `raft-dist` → `raft-tests`.

### C++

Builds with CMake from the repository root:

```text
cmake -S graft-cpp -B graft-cpp/build
cmake --build graft-cpp/build
```

Produces `graft-cpp/build/graft_smoke`. See [graft-cpp/README.md](graft-cpp/README.md) for prerequisites and details.

### Rust

Builds with Cargo from the repository root:

```text
cargo build --release
```

Produces `graft-rust/target/release/graft-smoke`. See [graft-rust/README.md](graft-rust/README.md) for crate structure and details.

## Docs

For a concise description of what each Java module contains, see [docs/module-overview.md](docs/module-overview.md).

For guidance on building domain applications on top of the Raft libraries, see [docs/application-developer-guide.md](docs/application-developer-guide.md).

For a fuller developer manual covering both application integration and internal Raft machinery development, see [docs/developer-manual/raft-developer-manual.tex](docs/developer-manual/raft-developer-manual.tex) ([PDF](docs/developer-manual/raft-developer-manual.pdf)).

## Jepsen validation of cluster functionality

The repository now includes a local Jepsen harness, documented in [jepsen/README.md](jepsen/README.md). It complements the classic JUnit/Maven suite by exercising the runnable `raft-dist` node processes under concurrent client load and injected failures.

At a high level, the Jepsen tests check that the key-value demo remains linearizable while the cluster is exposed to conditions that resemble real operational failures:

- node crashes and clean restarts
- follower disk/state loss and fresh recovery
- follower or leader network isolation
- loss of leader quorum
- membership changes while the cluster is under load
- membership changes combined with partitions
- aggressive snapshot/compaction settings during faults
- 5-node and 7-node quorum topologies
- single-key CAS-register and multi-key workloads
- mixed Java/C++ clusters, including C++ client operations and C++ leader membership admission/promotion

The default `mvn test` path now includes a small Jepsen smoke suite in addition to the normal Java tests. That smoke suite covers:

- baseline 5-node execution
- crash/restart
- partition

That means `mvn test` now assumes:

- Java 21
- Clojure CLI
- a built local Jepsen environment
- non-interactive `sudo` for the partition helper on systems where packet filtering requires privilege

Use this when you want the full default test path:

```text
mvn test
```

If you need to skip only the Jepsen smoke suite temporarily:

```text
mvn test -DskipJepsenTests=true
```

### What The Jepsen Scenarios Emulate

- `crash-restart`: a process dies but its local Raft state survives, which is the normal host restart or JVM crash case.
- `persistence-loss-restart`: one follower loses its local on-disk state and has to recover by replay or snapshot from the cluster, which approximates disk replacement, state-volume loss, or data-directory corruption recovery.
- `partition-one`: one node becomes isolated from the rest of the cluster, which approximates a single-host networking failure or firewall misconfiguration.
- `partition-leader`: the current leader is isolated, which approximates leader loss of connectivity and forces step-down plus re-election behavior.
- `partition-leader-minority`: the leader is cut into a minority partition, which approximates a partial datacenter/network split where the old leader must not continue serving as if quorum still exists.
- `membership-join-promote`: a new learner joins and is promoted to voter under load, which approximates controlled cluster expansion.
- `membership-demote`: an existing voter is turned into a learner under load, which approximates reducing quorum membership while keeping a replica online.
- `membership-remove-follower`: a non-leader follower is removed through explicit joint consensus and finalize, which approximates planned cluster shrink or node decommissioning.
- `membership-remove-leader`: the current leader is removed through reconfiguration, which approximates leader decommissioning during a maintenance event.
- `membership-remove-follower-partition-leader`: a membership change overlaps with leader isolation, which approximates maintenance or topology change during an active network fault.

The stronger local Jepsen runs go beyond the Maven smoke suite. They are intended to answer production-style questions such as:

- does a wiped follower recover safely from the surviving quorum?
- does the old leader stop behaving like a leader after isolation?
- do membership changes remain safe when leadership changes mid-flight?
- does aggressive snapshotting still preserve correct state under recovery and partitions?
- do independent keys remain linearizable under the same faults?

For exact commands, prerequisites, and result inspection, see [jepsen/README.md](jepsen/README.md) and [docs/jepsen-workflow.md](docs/jepsen-workflow.md).

For a more detailed discussion of partitions, apparent split brain, ambiguous client outcomes, and the boundary between Raft guarantees and application-level reconciliation, see [docs/split-brain-and-consistency.md](docs/split-brain-and-consistency.md).

There are two protocol variants across branches:
- On the `main` branch, messages use protobuf encoding over varint32-framed Netty transport.
- On the `json-on-the-wire` branch, messages use JSON envelopes over Netty transport.

## Cluster Management
There are two distinct concerns:

1. Bootstrap / discovery
Nodes need some initial addresses so they can find the running cluster.

2. Raft membership
Nodes only become voting members when the leader commits a configuration change through the Raft log.

### Bootstrapping a cluster
The current bootstrap mechanism is static startup configuration:

- `Application` starts a node with its own port plus a list of peer ports
- the helper scripts start a cluster with a fixed initial member set
- this is a reasonable way to form an initial cluster

This startup peer list is a transport seed, not the full source of truth for membership. Once the cluster is running, the replicated Raft configuration becomes authoritative.

For local runs, `./scripts/start_raft.sh` is still the easiest way to form an initial cluster. It gives each node enough seed addresses to elect a leader and start exchanging Raft traffic, but it does not lock the cluster to that initial member set.

### API
Cluster management and ordinary client access use typed protobuf messages:

- `ClientCommandRequest` / `ClientCommandResponse`
- `ClientQueryRequest` / `ClientQueryResponse`
- `JoinClusterRequest` / `JoinClusterResponse`
- `JoinClusterStatusRequest` / `JoinClusterStatusResponse`
- `ReconfigureClusterRequest` / `ReconfigureClusterResponse`

`ClientCommandRequest` carries a typed `StateMachineCommand` payload for ordinary replicated writes. `ClientQueryRequest` carries a typed `StateMachineQuery` payload for ordinary reads. Both respond with explicit status and include the leader endpoint on redirects so a client can retry directly. The first query type is key-value `GET(key)`. `JoinClusterRequest` is the convenience path for adding a single new member. The leader turns it into a joint-consensus transition and finalizes it automatically after the joining node has caught up enough. `JoinClusterStatusRequest` reports whether the join is still pending, active in joint consensus, completed, or unknown. `ReconfigureClusterRequest` exposes explicit `JOINT` and `FINALIZE` actions for manual control.
`ReconfigureClusterRequest` also supports focused `PROMOTE` and `DEMOTE` actions for role changes.

Peers are either:

- `VOTER`: participates in elections and quorum
- `LEARNER`: replicates state but does not vote, does not start elections, and does not count toward quorum

This is the recommended topology for centrally managed reference data:

- keep a small central voter set
- add remote or edge replicas as learners

Requests should normally be sent to the current leader. Followers forward typed cluster-management requests to the leader when they know who it is; decommissioned nodes reject them.

Ordinary typed queries are served as linearizable leader reads. The leader only answers them when it has applied through its current `commitIndex` and holds a fresh quorum-backed leader lease. If the lease is stale, the leader attempts a short quorum heartbeat barrier to refresh it. If it cannot re-establish a linearizable read window, the query returns `status=RETRY` instead of serving a potentially stale read.

### Adapter policies and authorization
Raft itself still enforces the hard safety rule that only the leader appends client commands to the replicated log. On top of that, the runnable adapter layer can now apply an application-facing write-admission policy and a separate authorization check for client writes.

The default `BasicAdapter` keeps the original demo behavior:

- leaders accept writes
- non-leaders redirect to the known leader when possible
- otherwise the request is rejected

For reference-data distribution there is now a dedicated adapter mode:

- `ReferenceDataAdapter`
- leader accepts writes
- follower voters redirect to the leader
- learners reject writes instead of redirecting them
- an optional requester-id allow-list can reject writes before the leader submits them to Raft

You can select the adapter at startup with:

```text
-Draft.adapter.mode=basic
-Draft.adapter.mode=reference-data
```

The simple demo authorizer is configured with:

```text
-Draft.command.authorizer.allow-list=reference-admin,master-data-service
```

This allow-list uses `ClientCommandRequest.peerId` as the requester identity. That is useful for demos and for structuring the code correctly, but it is not a strong security boundary by itself. In a production setup, the request identity should come from an authenticated transport or gateway layer and then be mapped into the authorization decision.

Optional request authentication is also available for client and admin requests:

```text
-Draft.request.auth.mode=none
-Draft.request.auth.mode=shared-secret
-Draft.request.auth.shared-secret=top-secret
```

The CLI side can send credentials with:

```text
-Draft.request.auth.client-scheme=shared-secret
-Draft.request.auth.client-token=top-secret
```

Behavior by mode:

- `none`: development/test default; no authentication is required
- `shared-secret`: external requests must carry `auth_scheme=shared-secret` plus the configured token

This authentication hook currently applies to ordinary writes, ordinary reads, join/join-status, reconfiguration, telemetry, cluster summary, and reconfiguration-status requests. It is intentionally kept above the Raft mechanics so development remains simple while production-style deployments can demand stronger request validation.

### Adding a new node
A new node can be started with addresses of existing members so it can contact the cluster, but it is not part of the voting configuration until the leader commits a membership change through the Raft log.

Operationally, the safe flow is:

1. Start the new node with at least one reachable seed address from the existing cluster
2. Send `JoinClusterRequest` for the new `Peer` to any reachable cluster member
3. If that member is not the leader, it forwards the typed request to the leader
4. The leader enters joint consensus, lets the new node catch up by log replication or chunked `InstallSnapshot`, and finalizes the new configuration
5. Verify the new node now appears in the finalized configuration with the intended role

You can poll progress with `JoinClusterStatusRequest`.

There is also an explicit join startup mode:

```text
java -jar raft-dist/target/raft-1.0-SNAPSHOT.jar join 127.0.0.1:10085/learner 127.0.0.1:10081
```

In that mode the new node starts in passive join mode, repeatedly submits `JoinClusterRequest` through the seed member, and waits to become part of the committed configuration before it starts participating normally.

### Removing a node
Removing a node uses the same two-phase flow:

1. Send `ReconfigureClusterRequest(Action.JOINT, peers)` to the current leader, omitting the member you want to remove
2. Send `ReconfigureClusterRequest(Action.FINALIZE, ...)` to the current leader

For the joint step, send `ReconfigureClusterRequest(Action.JOINT, peers)` with the new intended membership.

Once the finalized configuration excludes the local node, this implementation decommissions it:

- it steps down if it was leader
- it stops starting elections
- it rejects client/admin submissions
- the server closes

### Current limitations
Cluster management is replicated through typed requests, but it is still intentionally minimal:

- bootstrap discovery is still based on configured seed peers
- cluster formation and operator workflows are still manual; there is no built-in service discovery or orchestration layer

### Persistence and recovery
Cluster configuration is persisted as part of Raft state:

- committed configuration changes survive restart
- snapshots carry configuration metadata as well as application snapshot state
- a removed node that restarts comes back decommissioned
- a surviving node that restarts comes back with the latest committed membership

## Telemetry

Telemetry is split into two layers: **in-cluster telemetry** (protobuf RPCs available over the shared wire protocol, supported by all three implementations) and **external telemetry endpoints** (Prometheus scrape / OTLP push, available depending on implementation maturity).

### In-Cluster Telemetry (protobuf RPC)

All three implementations handle these protobuf message pairs through the shared `Envelope` framing:

| Request | Response | Description |
|---|---|---|
| `TelemetryRequest` | `TelemetryResponse` | Detailed node-level inspection |
| `ClusterSummaryRequest` | `ClusterSummaryResponse` | Leader-oriented cluster-wide status |
| `ReconfigurationStatusRequest` | `ReconfigurationStatusResponse` | Membership change progress |

The data exposed through these RPCs includes local Raft state, term, leader, vote, commit/applied/log/snapshot indexes, current and next membership sets, pending joins, leader replication progress per follower, transport response-time statistics per peer, cluster health, quorum status, and per-member transition detail (role, current/next role, blocking quorum information).

Rate limiting applies to these in-cluster requests (default 30/min/requester). Long-running reconfigurations are flagged as `reconfiguration-stuck` after a configurable threshold (default 60s).

For manual inspection:

```text
java -jar raft-dist/target/raft-1.0-SNAPSHOT.jar telemetry 127.0.0.1:10080
java -jar raft-dist/target/raft-1.0-SNAPSHOT.jar cluster-summary --json 127.0.0.1:10080
java -jar raft-dist/target/raft-1.0-SNAPSHOT.jar reconfiguration-status --json 127.0.0.1:10080
```

### Telemetry Endpoints (Prometheus / OTLP)

In addition to the in-cluster RPCs, nodes can expose metrics to external monitoring systems.

#### Java (reference — full support)

The `raft-telemetry` module supports three exporters, selected via `raft.telemetry.exporter`:

| Exporter | Property value | Mechanism |
|---|---|---|
| Prometheus | `prometheus` | HTTP scrape endpoint (default `http://127.0.0.1:9108/metrics`) — supported in Java, C++, Rust |
| OTLP HTTP | `opentelemetry` / `otlp` | Periodic JSON push to collector (default `http://127.0.0.1:4318/v1/metrics`) — supported in Java, Rust (behind `otlp` feature); config recognized in C++ |
| OTEL JSON log | `otel-log` / `opentelemetry-log` | Single-line JSON via dedicated OTEL logger — supported in Java, C++, Rust |

A daemon thread (`raft-telemetry-export-{peerId}`) periodically calls `RaftNode.telemetrySnapshot()` and publishes to the configured exporter.

Configuration (Java system properties / env vars for C++/Rust):

| Property / Env Var | Default | Description |
|---|---|---|
| `raft.telemetry.exporter` / `RAFT_TELEMETRY_EXPORTER` | `none` | `none`, `prometheus`, `opentelemetry`/`otlp` |
| `raft.telemetry.prometheus.host` / `RAFT_TELEMETRY_PROMETHEUS_HOST` | `127.0.0.1` | Bind address |
| `raft.telemetry.prometheus.port` / `RAFT_TELEMETRY_PROMETHEUS_PORT` | `9108` | Listen port |
| `raft.telemetry.prometheus.path` / `RAFT_TELEMETRY_PROMETHEUS_PATH` | `/metrics` | Scrape path |
| `raft.telemetry.otlp.endpoint` / `RAFT_TELEMETRY_OTLP_ENDPOINT` | `http://127.0.0.1:4318/v1/metrics` | Collector URL |
| `raft.telemetry.otlp.timeout.millis` / `RAFT_TELEMETRY_OTLP_TIMEOUT_MILLIS` | `2000` | Request timeout |
| `raft.telemetry.otlp.headers` / `RAFT_TELEMETRY_OTLP_HEADERS` | — | Comma-separated `key=value` pairs |
| `raft.telemetry.export.interval.seconds` / `RAFT_TELEMETRY_EXPORT_INTERVAL_SECONDS` | `15` | Publish cadence |

#### C++

Prometheus endpoint via `RAFT_TELEMETRY_EXPORTER=prometheus`. The active server starts an additional HTTP listener on the configured port and serves Raft metrics in Prometheus text format. OTLP configuration is recognized but push export is not yet implemented. See [graft-cpp/README.md](graft-cpp/README.md).

#### Rust

Prometheus endpoint via `RAFT_TELEMETRY_EXPORTER=prometheus` (raw TCP listener, no external HTTP library). OTLP push is available behind the `otlp` Cargo feature (`cargo build --features otlp`), using `reqwest`. See [graft-rust/README.md](graft-rust/README.md).

#### Metrics

The following gauges are exported by all three implementations (Java includes additional per-peer replication and transport-latency metrics not yet available in C++/Rust):

```
raft_term                      raft_commit_index             raft_last_applied
raft_last_log_index            raft_last_log_term            raft_snapshot_index
raft_snapshot_term             raft_joint_consensus          raft_members_total
raft_voting_members            raft_followers_replicating    raft_pending_joins
raft_joining                   raft_decommissioned           raft_reconfiguration_age_millis
```

Java additionally exports: `raft_members_promoting`, `raft_members_demoting`, `raft_members_joining`, `raft_members_removing`, and per-peer `raft_replication_*` / `raft_transport_response_*` gauges.

## Next Steps

See the C++ ([graft-cpp/README.md](graft-cpp/README.md)) and Rust ([graft-rust/README.md](graft-rust/README.md)) READMEs for their respective convergence roadmaps.

### Other discovery mechanisms
If you do not want to rely on a full static peer list at startup, typical alternatives are:

- a small set of seed nodes
- DNS-based service discovery
- a service registry such as Consul or Kubernetes service/endpoints
- cloud instance/tag discovery

Those mechanisms help a node find the cluster, but they do not replace Raft membership changes. Membership must still be changed through replicated configuration entries.

## Demonstration
The previous README contained a long historical transcript from an older static-cluster demo. That transcript no longer matched the current implementation, which now supports joint-consensus reconfiguration, persisted configuration, decommissioned members, and chunked snapshot transfer.

For a current local run:

1. Build the runnable jar.
2. Start the bootstrap cluster with `./scripts/cluster_demo_setup.sh` for the full cluster-management demo, or `./scripts/start_raft.sh` for the minimal launcher.
3. Watch the node logs to see leader election and steady-state heartbeats.
4. Send client commands and typed reconfiguration requests to the leader.
5. Stop the local cluster with `./scripts/kill_raft.sh`.

Typical things to observe during a manual run:

- one node becomes leader after the initial election timeout window
- ordinary client commands replicate through the Raft log
- `ReconfigureClusterRequest(Action.JOINT, ...)` enters joint consensus
- `ReconfigureClusterRequest(Action.FINALIZE, ...)` commits the new voting set
- a removed leader steps down and decommissions itself after finalized removal
- followers that are too far behind switch from `AppendEntries` to chunked `InstallSnapshot`

`./scripts/cluster_demo_setup.sh` also enables a background summary stream by default:

- `raft-demo/cluster-summary.jsonl` records periodic `cluster-summary --json` snapshots using a shared JSONL envelope
- `raft-demo/node-<port>/telemetry.log` continues to hold the richer human-readable per-node view
- `raft-demo/demo-events.jsonl` records timestamped scripted actions and milestones using the same schema version envelope
- `raft-demo/telemetry-snapshots.jsonl` records typed telemetry snapshots captured by the demo flow
- `./scripts/cluster_demo_timeline.sh raft-demo` merges those JSONL artifacts into a readable time-ordered view
- disable the summary loop with `RAFT_SUMMARY_INTERVAL_SECONDS=0`
- by default it also runs `scripts/cluster_reconfiguration_demo.sh`, which removes the initial leader through `JOINT` then `FINALIZE`, then starts a new join-mode node and waits for it to complete admission
- disable the automated reconfiguration flow with `RAFT_DEMO_AUTORUN=0`

The integration tests exercise these flows in a more reliable and repeatable way than a pasted console transcript. In particular:

- leader election and log replication
- joint-consensus membership changes
- removed-node decommissioning
- restart and snapshot recovery with persisted membership
- chunked `InstallSnapshot` transfer

## Tools

Protobuf (lite) codegen
```
./scripts/protoc-lite.sh
```
The protobuf Maven plugin uses this wrapper to force lite code generation. It prefers the Maven-cached protoc
(`com.google.protobuf:protoc` at `PROTOC_VERSION`, defined in the script). If Maven downloaded the file without the executable bit, the wrapper fixes that before invoking it. It no longer falls back to a system `protoc`, because that can silently use the wrong protobuf version.
If you bump `protobuf.version` in `pom.xml`, update `PROTOC_VERSION` in `scripts/protoc-lite.sh` as well.
