# raft
An asynchronous Raft implementation in Java, built on netty.io 4.2.

There are two different implementations: 
- On the `'main'` branch, messages are packaged according to protobuf and sent/received through Netty.
- On the `'json-on-the-wire'` branch, messages are exchanged using JSON envelopes (akin to MCP thinking) and sent through Netty.

## Demonstration
Start
```
➜ ./scripts/start_raft.sh 
Starting Raft node on port 10080 with peers 10081 10082 10083 10084
Starting Raft node on port 10081 with peers 10080 10082 10083 10084
Starting Raft node on port 10082 with peers 10080 10081 10083 10084
Starting Raft node on port 10083 with peers 10080 10081 10082 10084
Starting Raft node on port 10084 with peers 10080 10081 10082 10083
All Raft nodes launched.
```
Looking in raft.log:
```
RaftServer: Raft server started on port 10084
RaftServer: Raft server started on port 10082
RaftServer: Raft server started on port 10083
RaftServer: Raft server started on port 10081
RaftServer: Raft server started on port 10080

RaftNode: server-10084 initiating new election...
RaftNode: server-10082 initiating new election...
RaftNode: server-10083 initiating new election...
RaftNode: server-10081 initiating new election...
RaftNode: server-10080 initiating new election...
RaftNode: server-10084@1 received vote request for term 1 from server-10083
RaftNode: server-10084@1 (CANDIDATE) will *not* grant vote to server-10083 for term 1 (already voted for server-10084)
RaftNode: server-10083@1 received vote request for term 1 from server-10084
RaftNode: server-10083@1 (CANDIDATE) will *not* grant vote to server-10084 for term 1 (already voted for server-10083)
RaftNode: server-10084@1 received vote request for term 1 from server-10081
RaftNode: server-10084@1 (CANDIDATE) will *not* grant vote to server-10081 for term 1 (already voted for server-10084)
RaftNode: server-10083@1 received vote request for term 1 from server-10081
RaftNode: server-10083@1 (CANDIDATE) will *not* grant vote to server-10081 for term 1 (already voted for server-10083)
RaftNode: server-10081@1 received vote request for term 1 from server-10084
RaftNode: server-10081@1 (CANDIDATE) will *not* grant vote to server-10084 for term 1 (already voted for server-10081)
RaftNode: server-10081@1 received vote request for term 1 from server-10083
RaftNode: server-10081@1 (CANDIDATE) will *not* grant vote to server-10083 for term 1 (already voted for server-10081)
RaftNode: server-10083@1 received vote request for term 1 from server-10082
RaftNode: server-10083@1 (CANDIDATE) will *not* grant vote to server-10082 for term 1 (already voted for server-10083)
RaftNode: server-10081@1 received vote request for term 1 from server-10082
RaftNode: server-10081@1 (CANDIDATE) will *not* grant vote to server-10082 for term 1 (already voted for server-10081)
RaftNode: server-10084@1 received vote request for term 1 from server-10082
RaftNode: server-10084@1 (CANDIDATE) will *not* grant vote to server-10082 for term 1 (already voted for server-10084)
RaftNode: server-10082@1 received vote request for term 1 from server-10083
RaftNode: server-10082@1 (CANDIDATE) will *not* grant vote to server-10083 for term 1 (already voted for server-10082)
RaftNode: server-10082@1 received vote request for term 1 from server-10084
RaftNode: server-10082@1 (CANDIDATE) will *not* grant vote to server-10084 for term 1 (already voted for server-10082)
RaftNode: server-10082@1 received vote request for term 1 from server-10081
RaftNode: server-10082@1 (CANDIDATE) will *not* grant vote to server-10081 for term 1 (already voted for server-10082)
RaftNode: server-10084@1 received vote request for term 1 from server-10080
RaftNode: server-10082@1 received vote request for term 1 from server-10080
RaftNode: server-10084@1 (CANDIDATE) will *not* grant vote to server-10080 for term 1 (already voted for server-10084)
RaftNode: server-10082@1 (CANDIDATE) will *not* grant vote to server-10080 for term 1 (already voted for server-10082)
RaftNode: server-10083@1 received vote request for term 1 from server-10080
RaftNode: server-10081@1 received vote request for term 1 from server-10080
RaftNode: server-10083@1 (CANDIDATE) will *not* grant vote to server-10080 for term 1 (already voted for server-10083)
RaftNode: server-10081@1 (CANDIDATE) will *not* grant vote to server-10080 for term 1 (already voted for server-10081)
RaftNode: server-10080@1 received vote request for term 1 from server-10083
RaftNode: server-10080@1 (CANDIDATE) will *not* grant vote to server-10083 for term 1 (already voted for server-10080)
RaftNode: server-10080@1 received vote request for term 1 from server-10084
RaftNode: server-10080@1 (CANDIDATE) will *not* grant vote to server-10084 for term 1 (already voted for server-10080)
RaftNode: server-10080@1 received vote request for term 1 from server-10081
RaftNode: server-10080@1 (CANDIDATE) will *not* grant vote to server-10081 for term 1 (already voted for server-10080)
RaftNode: server-10080@1 received vote request for term 1 from server-10082
RaftNode: server-10080@1 (CANDIDATE) will *not* grant vote to server-10082 for term 1 (already voted for server-10080)
RaftNode: server-10084 initiating new election...
RaftNode: server-10082 initiating new election...
RaftNode: server-10083@1 received vote request for term 2 from server-10084
RaftNode: server-10083@1 steps from CANDIDATE to FOLLOWER since requested term 2 is higher
RaftNode: server-10083@2 grants vote to server-10084 for term 2 and remains FOLLOWER => accepted
RaftNode: server-10082@2 received vote request for term 2 from server-10084
RaftNode: server-10082@2 (CANDIDATE) will *not* grant vote to server-10084 for term 2 (already voted for server-10082)
RaftNode: server-10081@1 received vote request for term 2 from server-10084
RaftNode: server-10083@2 received vote request for term 2 from server-10082
RaftNode: server-10083@2 (FOLLOWER) will *not* grant vote to server-10082 for term 2 (already voted for server-10084)
RaftNode: server-10084@2 received vote request for term 2 from server-10082
RaftNode: server-10084@2 (CANDIDATE) will *not* grant vote to server-10082 for term 2 (already voted for server-10084)
RaftNode: server-10080@1 received vote request for term 2 from server-10084
RaftNode: server-10080@1 steps from CANDIDATE to FOLLOWER since requested term 2 is higher
RaftNode: server-10081@1 steps from CANDIDATE to FOLLOWER since requested term 2 is higher
RaftNode: server-10080@2 grants vote to server-10084 for term 2 and remains FOLLOWER => accepted
RaftNode: server-10081@2 grants vote to server-10084 for term 2 and remains FOLLOWER => accepted
RaftNode: server-10080@2 received vote request for term 2 from server-10082
RaftNode: server-10080@2 (FOLLOWER) will *not* grant vote to server-10082 for term 2 (already voted for server-10084)
RaftNode: server-10081@2 received vote request for term 2 from server-10082
RaftNode: server-10081@2 (FOLLOWER) will *not* grant vote to server-10082 for term 2 (already voted for server-10084)
RaftNode: server-10084@2 elected LEADER (4 votes granted >= majority 3)
RaftNode: server-10082@2 steps from CANDIDATE to FOLLOWER due to heartbeat in current term
```
Here we manually kill `server-10084` (the elected leader), which results in a new leader election:
```
➜ ps -ef
...
501 39092     1   0  1:41PM ttys000    0:01.34 /usr/bin/java -jar target/raft-1.0-SNAPSHOT.jar 10080 10081 10082 10083 10084
501 39093     1   0  1:41PM ttys000    0:01.65 /usr/bin/java -jar target/raft-1.0-SNAPSHOT.jar 10081 10080 10082 10083 10084
501 39094     1   0  1:41PM ttys000    0:01.32 /usr/bin/java -jar target/raft-1.0-SNAPSHOT.jar 10082 10080 10081 10083 10084
501 39095     1   0  1:41PM ttys000    0:01.32 /usr/bin/java -jar target/raft-1.0-SNAPSHOT.jar 10083 10080 10081 10082 10084
501 39096     1   0  1:41PM ttys000    0:01.32 /usr/bin/java -jar target/raft-1.0-SNAPSHOT.jar 10084 10080 10081 10082 10083
...

➜ kill -9 39096
```
After not getting any heartbeats from `server-10084` for the relevant (timeout) period,
`server-10082` initiates a new leader election (and failing to reach out to `server-10084`):
```
RaftNode: server-10082 initiating new election...
RaftNode: server-10083@2 received vote request for term 3 from server-10082
RaftNode: server-10083@2 remains FOLLOWER since requested term 3 is higher
RaftNode: server-10083@3 grants vote to server-10082 for term 3 and remains FOLLOWER => accepted
RaftNode: server-10081@2 received vote request for term 3 from server-10082
RaftNode: server-10081@2 remains FOLLOWER since requested term 3 is higher
RaftNode: server-10081@3 grants vote to server-10082 for term 3 and remains FOLLOWER => accepted
RaftClient: server-10082 could not request vote for term 3 from server-10084: cannot connect
RaftNode: server-10080@2 received vote request for term 3 from server-10082
RaftNode: server-10080@2 remains FOLLOWER since requested term 3 is higher
RaftNode: server-10080@3 grants vote to server-10082 for term 3 and remains FOLLOWER => accepted
RaftNode: server-10082@3 elected LEADER (4 votes granted >= majority 3)
```
Here we manually kill `server-10082` (the leader), which results in a new leader election:
```
RaftNode: server-10083 initiating new election...
RaftClient: server-10083 could not request vote for term 4 from server-10084: cannot connect
RaftClient: server-10083 could not request vote for term 4 from server-10082: cannot connect
RaftNode: server-10081@3 received vote request for term 4 from server-10083
RaftNode: server-10081@3 remains FOLLOWER since requested term 4 is higher
RaftNode: server-10081@4 grants vote to server-10083 for term 4 and remains FOLLOWER => accepted
RaftNode: server-10080@3 received vote request for term 4 from server-10083
RaftNode: server-10080@3 remains FOLLOWER since requested term 4 is higher
RaftNode: server-10080@4 grants vote to server-10083 for term 4 and remains FOLLOWER => accepted
RaftNode: server-10083@4 elected LEADER (3 votes granted >= majority 3)
```
Here we manually kill yet another server, `server-10083` (the leader), leaving only two (2) 
remaining nodes in the cluster and there is no longer any possibility for a majority vote. 
Therefore, the cluster cannot reach consensus around a leader:
```
RaftNode: server-10081 initiating new election...
RaftClient: server-10081 could not request vote for term 5 from server-10082: cannot connect
RaftClient: server-10081 could not request vote for term 5 from server-10084: cannot connect
RaftClient: server-10081 could not request vote for term 5 from server-10083: cannot connect
RaftNode: server-10080@4 received vote request for term 5 from server-10081
RaftNode: server-10080@4 remains FOLLOWER since requested term 5 is higher
RaftNode: server-10080@5 grants vote to server-10081 for term 5 and remains FOLLOWER => accepted

RaftNode: server-10081 initiating new election...
RaftClient: server-10081 could not request vote for term 6 from server-10083: cannot connect
RaftClient: server-10081 could not request vote for term 6 from server-10084: cannot connect
RaftClient: server-10081 could not request vote for term 6 from server-10082: cannot connect
RaftNode: server-10080@5 received vote request for term 6 from server-10081
RaftNode: server-10080@5 remains FOLLOWER since requested term 6 is higher
RaftNode: server-10080@6 grants vote to server-10081 for term 6 and remains FOLLOWER => accepted

RaftNode: server-10081 initiating new election...
RaftClient: server-10081 could not request vote for term 7 from server-10083: cannot connect
RaftClient: server-10081 could not request vote for term 7 from server-10084: cannot connect
RaftClient: server-10081 could not request vote for term 7 from server-10082: cannot connect
RaftNode: server-10080@6 received vote request for term 7 from server-10081
RaftNode: server-10080@6 remains FOLLOWER since requested term 7 is higher
RaftNode: server-10080@7 grants vote to server-10081 for term 7 and remains FOLLOWER => accepted

RaftNode: server-10080 initiating new election...
RaftClient: server-10080 could not request vote for term 8 from server-10083: cannot connect
RaftClient: server-10080 could not request vote for term 8 from server-10082: cannot connect
RaftClient: server-10080 could not request vote for term 8 from server-10084: cannot connect
RaftNode: server-10081@7 received vote request for term 8 from server-10080
RaftNode: server-10081@7 steps from CANDIDATE to FOLLOWER since requested term 8 is higher
RaftNode: server-10081@8 grants vote to server-10080 for term 8 and remains FOLLOWER => accepted

RaftNode: server-10081 initiating new election...
RaftClient: server-10081 could not request vote for term 9 from server-10083: cannot connect
RaftClient: server-10081 could not request vote for term 9 from server-10084: cannot connect
RaftClient: server-10081 could not request vote for term 9 from server-10082: cannot connect
RaftNode: server-10080@8 received vote request for term 9 from server-10081
RaftNode: server-10080@8 steps from CANDIDATE to FOLLOWER since requested term 9 is higher
RaftNode: server-10080@9 grants vote to server-10081 for term 9 and remains FOLLOWER => accepted

RaftNode: server-10080 initiating new election...
RaftClient: server-10080 could not request vote for term 10 from server-10083: cannot connect
RaftClient: server-10080 could not request vote for term 10 from server-10082: cannot connect
RaftClient: server-10080 could not request vote for term 10 from server-10084: cannot connect
RaftNode: server-10081@9 received vote request for term 10 from server-10080
RaftNode: server-10081@9 steps from CANDIDATE to FOLLOWER since requested term 10 is higher
RaftNode: server-10081@10 grants vote to server-10080 for term 10 and remains FOLLOWER => accepted

RaftNode: server-10081 initiating new election...
RaftClient: server-10081 could not request vote for term 11 from server-10083: cannot connect
RaftClient: server-10081 could not request vote for term 11 from server-10082: cannot connect
RaftClient: server-10081 could not request vote for term 11 from server-10084: cannot connect
RaftNode: server-10080@10 received vote request for term 11 from server-10081
RaftNode: server-10080@10 steps from CANDIDATE to FOLLOWER since requested term 11 is higher
RaftNode: server-10080@11 grants vote to server-10081 for term 11 and remains FOLLOWER => accepted

RaftNode: server-10081 initiating new election...
RaftClient: server-10081 could not request vote for term 12 from server-10084: cannot connect
RaftClient: server-10081 could not request vote for term 12 from server-10083: cannot connect
RaftClient: server-10081 could not request vote for term 12 from server-10082: cannot connect
RaftNode: server-10080@11 received vote request for term 12 from server-10081
RaftNode: server-10080@11 remains FOLLOWER since requested term 12 is higher
RaftNode: server-10080@12 grants vote to server-10081 for term 12 and remains FOLLOWER => accepted

RaftNode: server-10081 initiating new election...
RaftClient: server-10081 could not request vote for term 13 from server-10083: cannot connect
RaftClient: server-10081 could not request vote for term 13 from server-10082: cannot connect
RaftClient: server-10081 could not request vote for term 13 from server-10084: cannot connect
RaftNode: server-10080@12 received vote request for term 13 from server-10081
RaftNode: server-10080@12 remains FOLLOWER since requested term 13 is higher
RaftNode: server-10080@13 grants vote to server-10081 for term 13 and remains FOLLOWER => accepted

RaftNode: server-10080 initiating new election...
RaftClient: server-10080 could not request vote for term 14 from server-10084: cannot connect
RaftClient: server-10080 could not request vote for term 14 from server-10083: cannot connect
RaftClient: server-10080 could not request vote for term 14 from server-10082: cannot connect
RaftNode: server-10081@13 received vote request for term 14 from server-10080
RaftNode: server-10081@13 steps from CANDIDATE to FOLLOWER since requested term 14 is higher
RaftNode: server-10081@14 grants vote to server-10080 for term 14 and remains FOLLOWER => accepted

RaftNode: server-10081 initiating new election...
RaftClient: server-10081 could not request vote for term 15 from server-10083: cannot connect
RaftClient: server-10081 could not request vote for term 15 from server-10084: cannot connect
RaftClient: server-10081 could not request vote for term 15 from server-10082: cannot connect
RaftNode: server-10080@14 received vote request for term 15 from server-10081
RaftNode: server-10080@14 steps from CANDIDATE to FOLLOWER since requested term 15 is higher
RaftNode: server-10080@15 grants vote to server-10081 for term 15 and remains FOLLOWER => accepted

RaftNode: server-10081 initiating new election...
RaftClient: server-10081 could not request vote for term 16 from server-10082: cannot connect
RaftClient: server-10081 could not request vote for term 16 from server-10084: cannot connect
RaftClient: server-10081 could not request vote for term 16 from server-10083: cannot connect
RaftNode: server-10080@15 received vote request for term 16 from server-10081
RaftNode: server-10080@15 remains FOLLOWER since requested term 16 is higher
RaftNode: server-10080@16 grants vote to server-10081 for term 16 and remains FOLLOWER => accepted

RaftNode: server-10081 initiating new election...
RaftClient: server-10081 could not request vote for term 17 from server-10084: cannot connect
RaftClient: server-10081 could not request vote for term 17 from server-10082: cannot connect
RaftClient: server-10081 could not request vote for term 17 from server-10083: cannot connect
RaftNode: server-10080@16 received vote request for term 17 from server-10081
RaftNode: server-10080@16 remains FOLLOWER since requested term 17 is higher
RaftNode: server-10080@17 grants vote to server-10081 for term 17 and remains FOLLOWER => accepted
```
We restart `server-10083`, leaving us with the minimally three (3) nodes in the cluster.
This enables the cluster to reach consensus:
```
RaftServer: Raft server started on port 10083
RaftNode: server-10081 initiating new election...
RaftClient: server-10081 could not request vote for term 18 from server-10084: cannot connect
RaftClient: server-10081 could not request vote for term 18 from server-10082: cannot connect
RaftNode: server-10080@17 received vote request for term 18 from server-10081
RaftNode: server-10080@17 remains FOLLOWER since requested term 18 is higher
RaftNode: server-10080@18 grants vote to server-10081 for term 18 and remains FOLLOWER => accepted
RaftNode: server-10083@0 received vote request for term 18 from server-10081
RaftNode: server-10083@0 remains FOLLOWER since requested term 18 is higher
RaftNode: server-10083@18 grants vote to server-10081 for term 18 and remains FOLLOWER => accepted
RaftNode: server-10081@18 elected LEADER (3 votes granted >= majority 3)
```
We restart `server-10084`:
```
RaftServer: Raft server started on port 10084
RaftNode: server-10084@0 remains FOLLOWER since requested term 18 is higher
```
We restart `server-10082`:
```
RaftServer: Raft server started on port 10082
RaftNode: server-10082@0 remains FOLLOWER since requested term 18 is higher
```
Checking that all server nodes are up:
```
➜  lsof -iTCP -sTCP:LISTEN -nP | grep 1008
java      33194 froran  110u  IPv6 0xe532d8dbe41e1fdf      0t0  TCP *:10080 (LISTEN)
java      33195 froran  110u  IPv6 0x6d2fe2df2b8e0640      0t0  TCP *:10081 (LISTEN)
java      33196 froran  110u  IPv6 0x60f078d0866a1fa3      0t0  TCP *:10082 (LISTEN)
java      33197 froran  110u  IPv6 0x31d0288b343d3232      0t0  TCP *:10083 (LISTEN)
java      33198 froran  110u  IPv6 0xdcae40cb0056ee3a      0t0  TCP *:10084 (LISTEN)
```

By suspending the process for `server-10082`, effectively freezing its 
ability to interoperate with its cluster, it does not receive any heartbeats. 
This triggers a timeout directly when it is unfrozen, which triggers a new election:

```
RaftNode: server-10082 initiating new election...
RaftNode: server-10081@18 received vote request for term 19 from server-10082
RaftNode: server-10081@18 steps from LEADER to FOLLOWER since requested term 19 is higher
RaftNode: server-10081@19 grants vote to server-10082 for term 19 and remains FOLLOWER => accepted
RaftNode: server-10083@18 received vote request for term 19 from server-10082
RaftNode: server-10080@18 received vote request for term 19 from server-10082
RaftNode: server-10083@18 remains FOLLOWER since requested term 19 is higher
RaftNode: server-10080@18 remains FOLLOWER since requested term 19 is higher
RaftNode: server-10080@19 grants vote to server-10082 for term 19 and remains FOLLOWER => accepted
RaftNode: server-10083@19 grants vote to server-10082 for term 19 and remains FOLLOWER => accepted
RaftNode: server-10084@18 received vote request for term 19 from server-10082
RaftNode: server-10084@18 remains FOLLOWER since requested term 19 is higher
RaftNode: server-10084@19 grants vote to server-10082 for term 19 and remains FOLLOWER => accepted
RaftNode: server-10082@19 elected LEADER (5 votes granted >= majority 3)
```

We suspend the process for `server-10082`, now the leader of term 19, to
the effect of not issuing any heartbeats for a while. This triggers a new election:

```
RaftNode: server-10083 initiating new election...
RaftNode: server-10080@19 received vote request for term 20 from server-10083
RaftNode: server-10084@19 received vote request for term 20 from server-10083
RaftNode: server-10084@19 remains FOLLOWER since requested term 20 is higher
RaftNode: server-10080@19 remains FOLLOWER since requested term 20 is higher
RaftNode: server-10084@20 grants vote to server-10083 for term 20 and remains FOLLOWER => accepted
RaftNode: server-10080@20 grants vote to server-10083 for term 20 and remains FOLLOWER => accepted
RaftNode: server-10081@19 received vote request for term 20 from server-10083
RaftNode: server-10081@19 remains FOLLOWER since requested term 20 is higher
RaftNode: server-10081@20 grants vote to server-10083 for term 20 and remains FOLLOWER => accepted
```

We re-awake the process for `server-10082` (which is the leader for term 19)
amidst the re-negotiation, where `server-10083` is about to become the leader
for term 20. 

`server-10082` receives a vote request on term 20 from `server-10083` and yields leadership:

```
RaftNode: server-10082@19 received vote request for term 20 from server-10083
RaftNode: server-10082@19 steps from LEADER to FOLLOWER since requested term 20 is higher
RaftNode: server-10082@20 grants vote to server-10083 for term 20 and remains FOLLOWER => accepted
RaftNode: server-10083@20 elected LEADER (5 votes granted >= majority 3)
```

Killing all server nodes:
```
➜ ./scripts/kill_raft.sh
Killing processes matching pattern: java -jar target/raft-1.0-SNAPSHOT.jar
```

## Tools

Protobuf (lite) codegen
```
./scripts/protoc-lite.sh
```
The protobuf Maven plugin uses this wrapper to force lite code generation. It prefers the Maven-cached protoc
(`com.google.protobuf:protoc` at `PROTOC_VERSION`, defined in the script) and falls back to `protoc` on `PATH`.
If you bump `protobuf.version` in `pom.xml`, update `PROTOC_VERSION` in `scripts/protoc-lite.sh` as well.

