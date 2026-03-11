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

### Command surface
Cluster management currently uses the replicated `AdminCommand` path, not ordinary client commands.

Supported commands are:

- `config joint <peer-spec>,<peer-spec>,...`
- `config finalize`

The command must be sent to the current leader. Followers and decommissioned nodes reject it.

A peer spec can be:

- an existing known id such as `server-10080`
- a new member written as `id@host:port`

Example:

```text
config joint server-10080,server-10081,server-10082,server-10085@127.0.0.1:10085
config finalize
```

### Adding a new node
A new node can be started with addresses of existing members so it can contact the cluster, but it is not part of the voting configuration until the leader commits a membership change through the Raft log.

Operationally, the safe flow is:

1. Start the new node with at least one reachable seed address from the existing cluster
2. Send `config joint ...` to the current leader, including the new node
3. Let the new node catch up by log replication or chunked `InstallSnapshot`
4. Send `config finalize` to the current leader
5. Verify the new node now appears in the finalized voting set

At the moment there is no dedicated `join` RPC. "Join" means "start the node so it can reach the cluster, then have the leader replicate a joint-consensus configuration change that includes it."

### Removing a node
Removing a node uses the same two-phase flow:

1. Send `config joint ...` to the current leader, omitting the member you want to remove
2. Send `config finalize` to the current leader

Once the finalized configuration excludes the local node, this implementation decommissions it:

- it steps down if it was leader
- it stops starting elections
- it rejects client/admin submissions
- the server closes

### Current limitations
Cluster management works through the replicated admin command path, but it is still intentionally minimal:

- there is no dedicated higher-level `join` RPC yet; membership is managed through config commands
- bootstrap discovery is still based on configured seed peers
- there are no learner / non-voting members
- cluster formation and operator workflows are still manual; there is no built-in service discovery or orchestration layer

### Persistence and recovery
Cluster configuration is persisted as part of Raft state:

- committed configuration changes survive restart
- snapshots carry configuration metadata as well as application snapshot state
- a removed node that restarts comes back decommissioned
- a surviving node that restarts comes back with the latest committed membership

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
2. Start the bootstrap cluster with `./scripts/start_raft.sh`.
3. Watch the node logs to see leader election and steady-state heartbeats.
4. Send client commands and admin reconfiguration commands to the leader.
5. Stop the local cluster with `./scripts/kill_raft.sh`.

Typical things to observe during a manual run:

- one node becomes leader after the initial election timeout window
- ordinary client commands replicate through the Raft log
- `config joint ...` enters joint consensus
- `config finalize` commits the new voting set
- a removed leader steps down and decommissions itself after finalized removal
- followers that are too far behind switch from `AppendEntries` to chunked `InstallSnapshot`

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
