# raft
An asynchronous Raft implementation in Java, built on netty.io 4.2.
This implementation supports Raft joint-consensus reconfiguration, persisted configuration state, and chunked `InstallSnapshot` transfer.

There are two different implementations: 
- On the `'main'` branch, messages are packaged according to protobuf and sent/received through Netty.
- On the `'json-on-the-wire'` branch, messages are exchanged using JSON envelopes (akin to MCP thinking) and sent through Netty.

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

### API surface
Cluster management and ordinary client access now use typed protobuf messages, not free-form command strings:

- `ClientCommandRequest` / `ClientCommandResponse`
- `ClientQueryRequest` / `ClientQueryResponse`
- `JoinClusterRequest` / `JoinClusterResponse`
- `JoinClusterStatusRequest` / `JoinClusterStatusResponse`
- `ReconfigureClusterRequest` / `ReconfigureClusterResponse`

`ClientCommandRequest` carries a typed `StateMachineCommand` payload for ordinary replicated writes. `ClientQueryRequest` carries a typed `StateMachineQuery` payload for ordinary reads. Both respond with explicit status and include the leader endpoint on redirects so a client can retry directly. The first query type is key-value `GET(key)`. `JoinClusterRequest` is the convenience path for adding a single new member. The leader turns it into a joint-consensus transition and finalizes it automatically after the joining node has caught up enough. `JoinClusterStatusRequest` reports whether the join is still pending, active in joint consensus, completed, or unknown. `ReconfigureClusterRequest` exposes explicit `JOINT` and `FINALIZE` actions for manual control.

Peers can now be either:

- `VOTER`: participates in elections and quorum
- `LEARNER`: replicates state but does not vote, does not start elections, and does not count toward quorum

This is the recommended topology for centrally managed reference data:

- keep a small central voter set
- add remote or edge replicas as learners

Requests should normally be sent to the current leader. Followers forward typed cluster-management requests to the leader when they know who it is; decommissioned nodes reject them.

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
java -jar target/raft.jar join 127.0.0.1:10085/learner 127.0.0.1:10081
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
Nodes now expose:

- `TelemetryRequest` / `TelemetryResponse` for detailed node-level inspection
- `ClusterSummaryRequest` / `ClusterSummaryResponse` for leader-oriented cluster-wide status

The response includes:

- local Raft state, term, leader, and vote
- commit/applied/log/snapshot indexes
- current and next membership sets
- pending joins
- leader replication progress per follower when applicable
- transport response-time statistics per peer

For manual inspection from the prompt:

```text
java -jar target/raft.jar telemetry 127.0.0.1:10080
```

That prints a concise node summary to STDOUT, including replication and transport timing information when available.

Typed admin helpers are also available from the jar:

```text
java -jar target/raft.jar command put 127.0.0.1:10080 demo-key demo-value
java -jar target/raft.jar command delete 127.0.0.1:10080 demo-key
java -jar target/raft.jar command clear 127.0.0.1:10080
java -jar target/raft.jar query get 127.0.0.1:10080 demo-key
java -jar target/raft.jar cluster-summary --json 127.0.0.1:10080
java -jar target/raft.jar join-request 127.0.0.1:10080 127.0.0.1:10085/learner
java -jar target/raft.jar join-status 127.0.0.1:10080 server-10085
java -jar target/raft.jar reconfigure joint 127.0.0.1:10080 10081 10082 10083 10085/learner
java -jar target/raft.jar reconfigure finalize 127.0.0.1:10080
java -jar target/raft.jar reconfigure promote 127.0.0.1:10080 server-10085
java -jar target/raft.jar reconfigure demote 127.0.0.1:10080 server-10085
```

Any peer spec in the CLI can use `/learner` or `/voter`. If omitted, `voter` is the default.
`promote` turns a learner into a voter through a joint-consensus transition, and `demote` keeps the member replicating while removing it from quorum and elections.

For machine-readable inspection:

```text
java -jar target/raft.jar telemetry --json 127.0.0.1:10080
```

That emits the same telemetry view as structured JSON, including cluster health, quorum status, blocking peer ids, replication detail, and transport timing samples.

For the dedicated cluster-wide status endpoint:

```text
java -jar target/raft.jar cluster-summary --json 127.0.0.1:10080
```

That emits a leader summary with:

- cluster health and reason
- quorum availability
- blocking current/next quorum peers
- per-member cluster view including reachability, freshness, health, lag, failure counts, and role-transition state

Per-member cluster summaries now expose:

- `role`: the effective role currently presented by the leader view
- `currentRole`: the role in the active committed configuration
- `nextRole`: the role in the latest log-known target configuration
- `roleTransition`: one of `steady`, `promoting`, `demoting`, `joining`, or `removing`

That makes in-flight role changes visible before a learner promotion or voter demotion has fully finalized.

If the request reaches a follower, it redirects to the leader and includes the leader endpoint so the CLI can retry directly.

Leader-aware status pulls:

- a telemetry request can ask for a leader summary
- if it reaches a follower, the follower responds with `status=REDIRECT` and enough peer information to locate the leader
- the CLI helper follows one redirect hop automatically

Rate limiting:

- telemetry requests are rate-limited per requester id
- default limit is `30` requests per minute per requester
- configure with `-Draft.telemetry.rate.limit.per.minute=<n>`

Exporter scaffold:

- `-Draft.telemetry.exporter=none` is the default
- `-Draft.telemetry.exporter=prometheus` starts a local scrape endpoint
- `-Draft.telemetry.prometheus.host=127.0.0.1` controls the bind address
- `-Draft.telemetry.prometheus.port=9108` controls the listen port
- `-Draft.telemetry.prometheus.path=/metrics` controls the scrape path
- `-Draft.telemetry.export.interval.seconds=15` controls the publish cadence from a running node
- `-Draft.telemetry.exporter=opentelemetry` or `-Draft.telemetry.exporter=otlp` sends OTLP/HTTP metrics to `-Draft.telemetry.otlp.endpoint=http://127.0.0.1:4318/v1/metrics`
- `-Draft.telemetry.otlp.headers=Authorization=Bearer ...` adds comma-separated HTTP headers to OTLP export requests
- `-Draft.telemetry.otlp.timeout.millis=2000` controls the OTLP export request timeout
- `-Draft.telemetry.exporter=otel-log` keeps the previous structured log exporter available through the `OTEL` logger in `otel.log`
- demo telemetry logging emits a compact one-line headline every interval and a full detailed snapshot every `-Draft.demo.telemetry.detail.every=<n>` intervals

Current exporter metrics include the usual Raft term/index/replication gauges plus transition counters derived from active vs latest-known membership:

- Prometheus:
  `raft_members_promoting`
  `raft_members_demoting`
  `raft_members_joining`
  `raft_members_removing`
- OTLP:
  `raft.members.promoting`
  `raft.members.demoting`
  `raft.members.joining`
  `raft.members.removing`

Those counters are intended for alerting on long-running role changes or membership transitions that do not converge.

## Next Steps
There are no committed follow-up items at the moment.

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
2. Start the bootstrap cluster with `./scripts/demo_setup.sh` for the full demo view, or `./scripts/start_raft.sh` for the minimal launcher.
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

`./scripts/demo_setup.sh` also enables a background summary stream by default:

- `raft-demo/cluster-summary.jsonl` records periodic `cluster-summary --json` snapshots using a shared JSONL envelope
- `raft-demo/node-<port>/telemetry.log` continues to hold the richer human-readable per-node view
- `raft-demo/demo-events.jsonl` records timestamped scripted actions and milestones using the same schema version envelope
- `raft-demo/telemetry-snapshots.jsonl` records typed telemetry snapshots captured by the demo flow
- `./scripts/demo_timeline.sh raft-demo` merges those JSONL artifacts into a readable time-ordered view
- disable the summary loop with `RAFT_SUMMARY_INTERVAL_SECONDS=0`
- by default it also runs `scripts/demo_reconfigure.sh`, which removes the initial leader through `JOINT` then `FINALIZE`, then starts a new join-mode node and waits for it to complete admission
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
(`com.google.protobuf:protoc` at `PROTOC_VERSION`, defined in the script) and falls back to `protoc` on `PATH`.
If you bump `protobuf.version` in `pom.xml`, update `PROTOC_VERSION` in `scripts/protoc-lite.sh` as well.
