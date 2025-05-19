# raft
An asynchronous Raft implementation in Java, built on netty.io 4.2.

Start
```
➜  ./start_raft.sh 
Starting Raft node on port 10080 with peers 10081 10082 10083 10084
Starting Raft node on port 10081 with peers 10080 10082 10083 10084
Starting Raft node on port 10082 with peers 10080 10081 10083 10084
Starting Raft node on port 10083 with peers 10080 10081 10082 10084
Starting Raft node on port 10084 with peers 10080 10081 10082 10083
All Raft nodes launched.
```
Looking in raft.log:
```
RaftServer: Raft server started on port 10083
RaftServer: Raft server started on port 10080
RaftServer: Raft server started on port 10081
RaftServer: Raft server started on port 10082
RaftServer: Raft server started on port 10084

RaftStateMachine: server-10083 initiating new election...
RaftStateMachine: server-10081 initiating new election...
RaftStateMachine: server-10080 initiating new election...
RaftStateMachine: server-10082 initiating new election...
RaftStateMachine: server-10084 initiating new election...
RaftStateMachine: server-10081 received vote request for term 1 from server-10080
RaftStateMachine: server-10081 (CANDIDATE) will *not* grant vote to server-10080 for term 1 (already voted for server-10081)
RaftStateMachine: server-10081 received vote request for term 1 from server-10082
RaftStateMachine: server-10081 (CANDIDATE) will *not* grant vote to server-10082 for term 1 (already voted for server-10081)
RaftStateMachine: server-10082 received vote request for term 1 from server-10080
RaftStateMachine: server-10082 (CANDIDATE) will *not* grant vote to server-10080 for term 1 (already voted for server-10082)
RaftStateMachine: server-10082 received vote request for term 1 from server-10081
RaftStateMachine: server-10082 (CANDIDATE) will *not* grant vote to server-10081 for term 1 (already voted for server-10082)
RaftStateMachine: server-10080 received vote request for term 1 from server-10081
RaftStateMachine: server-10080 (CANDIDATE) will *not* grant vote to server-10081 for term 1 (already voted for server-10080)
RaftStateMachine: server-10083 received vote request for term 1 from server-10081
RaftStateMachine: server-10083 (CANDIDATE) will *not* grant vote to server-10081 for term 1 (already voted for server-10083)
RaftStateMachine: server-10080 received vote request for term 1 from server-10082
RaftStateMachine: server-10080 (CANDIDATE) will *not* grant vote to server-10082 for term 1 (already voted for server-10080)
RaftStateMachine: server-10081 received vote request for term 1 from server-10083
RaftStateMachine: server-10081 (CANDIDATE) will *not* grant vote to server-10083 for term 1 (already voted for server-10081)
RaftStateMachine: server-10080 received vote request for term 1 from server-10083
RaftStateMachine: server-10080 (CANDIDATE) will *not* grant vote to server-10083 for term 1 (already voted for server-10080)
RaftStateMachine: server-10083 received vote request for term 1 from server-10080
RaftStateMachine: server-10083 (CANDIDATE) will *not* grant vote to server-10080 for term 1 (already voted for server-10083)
RaftStateMachine: server-10082 received vote request for term 1 from server-10083
RaftStateMachine: server-10082 (CANDIDATE) will *not* grant vote to server-10083 for term 1 (already voted for server-10082)
RaftStateMachine: server-10080 received vote request for term 1 from server-10084
RaftStateMachine: server-10080 (CANDIDATE) will *not* grant vote to server-10084 for term 1 (already voted for server-10080)
RaftStateMachine: server-10083 received vote request for term 1 from server-10082
RaftStateMachine: server-10083 (CANDIDATE) will *not* grant vote to server-10082 for term 1 (already voted for server-10083)
RaftStateMachine: server-10083 received vote request for term 1 from server-10084
RaftStateMachine: server-10083 (CANDIDATE) will *not* grant vote to server-10084 for term 1 (already voted for server-10083)
RaftStateMachine: server-10084 received vote request for term 1 from server-10083
RaftStateMachine: server-10084 (CANDIDATE) will *not* grant vote to server-10083 for term 1 (already voted for server-10084)
RaftStateMachine: server-10084 received vote request for term 1 from server-10080
RaftStateMachine: server-10084 (CANDIDATE) will *not* grant vote to server-10080 for term 1 (already voted for server-10084)
RaftStateMachine: server-10082 received vote request for term 1 from server-10084
RaftStateMachine: server-10082 (CANDIDATE) will *not* grant vote to server-10084 for term 1 (already voted for server-10082)
RaftStateMachine: server-10084 received vote request for term 1 from server-10082
RaftStateMachine: server-10084 (CANDIDATE) will *not* grant vote to server-10082 for term 1 (already voted for server-10084)
RaftStateMachine: server-10084 received vote request for term 1 from server-10081
RaftStateMachine: server-10084 (CANDIDATE) will *not* grant vote to server-10081 for term 1 (already voted for server-10084)
RaftStateMachine: server-10081 received vote request for term 1 from server-10084
RaftStateMachine: server-10081 (CANDIDATE) will *not* grant vote to server-10084 for term 1 (already voted for server-10081)

RaftStateMachine: server-10083 initiating new election...
RaftStateMachine: server-10084 received vote request for term 2 from server-10083
RaftStateMachine: server-10081 received vote request for term 2 from server-10083
RaftStateMachine: server-10082 received vote request for term 2 from server-10083
RaftStateMachine: server-10080 received vote request for term 2 from server-10083
RaftStateMachine: server-10084 steps from CANDIDATE to FOLLOWER since requested term is 2 (> 1)
RaftStateMachine: server-10084 grants vote to server-10083 for term 2 and remains FOLLOWER => accepted
RaftStateMachine: server-10081 steps from CANDIDATE to FOLLOWER since requested term is 2 (> 1)
RaftStateMachine: server-10081 grants vote to server-10083 for term 2 and remains FOLLOWER => accepted
RaftStateMachine: server-10082 steps from CANDIDATE to FOLLOWER since requested term is 2 (> 1)
RaftStateMachine: server-10080 steps from CANDIDATE to FOLLOWER since requested term is 2 (> 1)
RaftStateMachine: server-10082 grants vote to server-10083 for term 2 and remains FOLLOWER => accepted
RaftStateMachine: server-10080 grants vote to server-10083 for term 2 and remains FOLLOWER => accepted
RaftStateMachine: server-10083 elected LEADER (4 votes granted >= majority 3)
```
Here we manually kill 'server-10083', which results in a new leader election:
```
➜  ps -ef
...
501 39092     1   0  1:41PM ttys000    0:01.34 /usr/bin/java -jar target/raft-1.0-SNAPSHOT.jar 10080 10081 10082 10083 10084
501 39093     1   0  1:41PM ttys000    0:01.65 /usr/bin/java -jar target/raft-1.0-SNAPSHOT.jar 10081 10080 10082 10083 10084
501 39094     1   0  1:41PM ttys000    0:01.32 /usr/bin/java -jar target/raft-1.0-SNAPSHOT.jar 10082 10080 10081 10083 10084
501 39095     1   0  1:41PM ttys000    0:01.32 /usr/bin/java -jar target/raft-1.0-SNAPSHOT.jar 10083 10080 10081 10082 10084
501 39096     1   0  1:41PM ttys000    0:01.32 /usr/bin/java -jar target/raft-1.0-SNAPSHOT.jar 10084 10080 10081 10082 10083
...

➜  kill -9 39095
```
After not getting any heartbeats from server-10083 for the relevant (timeout) period,
server-10081 initiates a new leader election (and failing to reach out to server-10083):
```
RaftStateMachine: server-10081 initiating new election...
RaftStateMachine: server-10084 received vote request for term 3 from server-10081
RaftStateMachine: server-10084 remains FOLLOWER since requested term is 3 (> 2)
RaftStateMachine: server-10084 grants vote to server-10081 for term 3 and remains FOLLOWER => accepted
RaftStateMachine: server-10082 received vote request for term 3 from server-10081
RaftStateMachine: server-10082 remains FOLLOWER since requested term is 3 (> 2)
RaftClient: Could not request vote for term 3 from server-10083: cannot connect

RaftStateMachine: server-10080 initiating new election...
RaftStateMachine: server-10082 grants vote to server-10081 for term 3 and remains FOLLOWER => accepted
RaftStateMachine: server-10084 received vote request for term 3 from server-10080
RaftStateMachine: server-10080 received vote request for term 3 from server-10081
RaftStateMachine: server-10084 (FOLLOWER) will *not* grant vote to server-10080 for term 3 (already voted for server-10081)
RaftStateMachine: server-10080 (CANDIDATE) will *not* grant vote to server-10081 for term 3 (already voted for server-10080)
RaftStateMachine: server-10081 received vote request for term 3 from server-10080
RaftStateMachine: server-10081 (CANDIDATE) will *not* grant vote to server-10080 for term 3 (already voted for server-10081)
RaftStateMachine: server-10082 received vote request for term 3 from server-10080
RaftStateMachine: server-10082 (FOLLOWER) will *not* grant vote to server-10080 for term 3 (already voted for server-10081)
RaftClient: Could not request vote for term 3 from server-10083: cannot connect
RaftStateMachine: server-10081 elected LEADER (3 votes granted >= majority 3)
```
Here we manually kill 'server-10081', which results in a new leader election:
```
RaftStateMachine: server-10082 initiating new election...
RaftStateMachine: server-10084 received vote request for term 4 from server-10082
RaftStateMachine: server-10084 remains FOLLOWER since requested term is 4 (> 3)
RaftStateMachine: server-10084 grants vote to server-10082 for term 4 and remains FOLLOWER => accepted
RaftClient: Could not request vote for term 4 from server-10083: cannot connect
RaftClient: Could not request vote for term 4 from server-10081: cannot connect
RaftStateMachine: server-10080 received vote request for term 4 from server-10082
RaftStateMachine: server-10080 steps from CANDIDATE to FOLLOWER since requested term is 4 (> 3)
RaftStateMachine: server-10080 grants vote to server-10082 for term 4 and remains FOLLOWER => accepted
RaftStateMachine: server-10082 elected LEADER (3 votes granted >= majority 3)
```
Here we manually kill yet another server, 'server-10082', leaving only two (2) remaining nodes
in the cluster and there is no longer any possibility for a majority vote. Therefore, the cluster
cannot reach consensus around a leader:
```
RaftStateMachine: server-10080 initiating new election...
RaftClient: Could not request vote for term 5 from server-10083: cannot connect
RaftClient: Could not request vote for term 5 from server-10082: cannot connect
RaftStateMachine: server-10084 received vote request for term 5 from server-10080
RaftStateMachine: server-10084 remains FOLLOWER since requested term is 5 (> 4)
RaftStateMachine: server-10084 grants vote to server-10080 for term 5 and remains FOLLOWER => accepted
RaftClient: Could not request vote for term 5 from server-10081: cannot connect

RaftStateMachine: server-10084 initiating new election...
RaftClient: Could not request vote for term 6 from server-10083: cannot connect
RaftClient: Could not request vote for term 6 from server-10081: cannot connect
RaftClient: Could not request vote for term 6 from server-10082: cannot connect
RaftStateMachine: server-10080 received vote request for term 6 from server-10084
RaftStateMachine: server-10080 steps from CANDIDATE to FOLLOWER since requested term is 6 (> 5)
RaftStateMachine: server-10080 grants vote to server-10084 for term 6 and remains FOLLOWER => accepted

RaftStateMachine: server-10080 initiating new election...
RaftStateMachine: server-10084 received vote request for term 7 from server-10080
RaftStateMachine: server-10084 steps from CANDIDATE to FOLLOWER since requested term is 7 (> 6)
RaftStateMachine: server-10084 grants vote to server-10080 for term 7 and remains FOLLOWER => accepted
RaftClient: Could not request vote for term 7 from server-10083: cannot connect
RaftClient: Could not request vote for term 7 from server-10081: cannot connect
RaftClient: Could not request vote for term 7 from server-10082: cannot connect

RaftStateMachine: server-10084 initiating new election...
RaftClient: Could not request vote for term 8 from server-10083: cannot connect
RaftClient: Could not request vote for term 8 from server-10082: cannot connect
RaftClient: Could not request vote for term 8 from server-10081: cannot connect
RaftStateMachine: server-10080 received vote request for term 8 from server-10084
RaftStateMachine: server-10080 steps from CANDIDATE to FOLLOWER since requested term is 8 (> 7)
RaftStateMachine: server-10080 grants vote to server-10084 for term 8 and remains FOLLOWER => accepted

RaftStateMachine: server-10080 initiating new election...
RaftStateMachine: server-10084 received vote request for term 9 from server-10080
RRaftClient: Could not request vote for term 9 from server-10082: cannot connect
RaftClient: Could not request vote for term 9 from server-10083: cannot connect
RaftStateMachine: server-10084 steps from CANDIDATE to FOLLOWER since requested term is 9 (> 8)
RaftClient: Could not request vote for term 9 from server-10081: cannot connect
RaftStateMachine: server-10084 grants vote to server-10080 for term 9 and remains FOLLOWER => accepted

RaftStateMachine: server-10084 initiating new election...
RaftClient: Could not request vote for term 10 from server-10083: cannot connect
RaftClient: Could not request vote for term 10 from server-10082: cannot connect
RaftClient: Could not request vote for term 10 from server-10081: cannot connect
RaftStateMachine: server-10080 received vote request for term 10 from server-10084
RaftStateMachine: server-10080 steps from CANDIDATE to FOLLOWER since requested term is 10 (> 9)
RaftStateMachine: server-10080 grants vote to server-10084 for term 10 and remains FOLLOWER => accepted

RaftStateMachine: server-10080 initiating new election...
RaftClient: Could not request vote for term 11 from server-10081: cannot connect
RaftClient: Could not request vote for term 11 from server-10083: cannot connect
RaftStateMachine: server-10084 received vote request for term 11 from server-10080
RaftStateMachine: server-10084 steps from CANDIDATE to FOLLOWER since requested term is 11 (> 10)
RaftClient: Could not request vote for term 11 from server-10082: cannot connect
RaftStateMachine: server-10084 grants vote to server-10080 for term 11 and remains FOLLOWER => accepted

RaftStateMachine: server-10084 initiating new election...
RaftClient: Could not request vote for term 12 from server-10082: cannot connect
RaftClient: Could not request vote for term 12 from server-10083: cannot connect
RaftClient: Could not request vote for term 12 from server-10081: cannot connect
RaftStateMachine: server-10080 received vote request for term 12 from server-10084
RaftStateMachine: server-10080 steps from CANDIDATE to FOLLOWER since requested term is 12 (> 11)
RaftStateMachine: server-10080 grants vote to server-10084 for term 12 and remains FOLLOWER => accepted

RaftStateMachine: server-10080 initiating new election...
RaftStateMachine: server-10084 received vote request for term 13 from server-10080
RaftClient: Could not request vote for term 13 from server-10083: cannot connect
RaftStateMachine: server-10084 steps from CANDIDATE to FOLLOWER since requested term is 13 (> 12)
RaftStateMachine: server-10084 grants vote to server-10080 for term 13 and remains FOLLOWER => accepted
RaftClient: Could not request vote for term 13 from server-10081: cannot connect
RaftClient: Could not request vote for term 13 from server-10082: cannot connect

RaftStateMachine: server-10084 initiating new election...
RaftClient: Could not request vote for term 14 from server-10081: cannot connect
RaftClient: Could not request vote for term 14 from server-10082: cannot connect
RaftClient: Could not request vote for term 14 from server-10083: cannot connect
RaftStateMachine: server-10080 received vote request for term 14 from server-10084
RaftStateMachine: server-10080 steps from CANDIDATE to FOLLOWER since requested term is 14 (> 13)
RaftStateMachine: server-10080 grants vote to server-10084 for term 14 and remains FOLLOWER => accepted

RaftStateMachine: server-10080 initiating new election...
RaftStateMachine: server-10084 received vote request for term 15 from server-10080
RaftClient: Could not request vote for term 15 from server-10081: cannot connect
RaftStateMachine: server-10084 steps from CANDIDATE to FOLLOWER since requested term is 15 (> 14)
RaftClient: Could not request vote for term 15 from server-10083: cannot connect
RaftStateMachine: server-10084 grants vote to server-10080 for term 15 and remains FOLLOWER => accepted
RaftClient: Could not request vote for term 15 from server-10082: cannot connect
```
Here we restart 'server-10083', leaving us with the minimally three (3) nodes in the cluster.
This enables the cluster to reach consensus -- after a bit of haggling:
```
RaftServer: Raft server started on port 10083

RaftStateMachine: server-10083 initiating new election...
RaftClient: Could not request vote for term 1 from server-10081: cannot connect
RaftClient: Could not request vote for term 1 from server-10082: cannot connect
RaftStateMachine: server-10080 received vote request for term 1 from server-10083
RaftStateMachine: server-10084 received vote request for term 1 from server-10083
RaftStateMachine: server-10080 (CANDIDATE) has term 15 which is newer than requested term 1 from server-10083 => reject
RaftStateMachine: server-10084 (FOLLOWER) has term 15 which is newer than requested term 1 from server-10083 => reject

RaftStateMachine: server-10084 initiating new election...
RaftStateMachine: server-10080 received vote request for term 16 from server-10084
RaftStateMachine: server-10080 steps from CANDIDATE to FOLLOWER since requested term is 16 (> 15)
RaftStateMachine: server-10080 grants vote to server-10084 for term 16 and remains FOLLOWER => accepted
RaftClient: Could not request vote for term 16 from server-10081: cannot connect
RaftClient: Could not request vote for term 16 from server-10082: cannot connect
RaftStateMachine: server-10083 received vote request for term 16 from server-10084
RaftStateMachine: server-10083 steps from CANDIDATE to FOLLOWER since requested term is 16 (> 1)
RaftStateMachine: server-10083 grants vote to server-10084 for term 16 and remains FOLLOWER => accepted
RaftStateMachine: server-10084 elected LEADER (3 votes granted >= majority 3)
```
Here we restart 'server-10081':
```
RaftServer: Raft server started on port 10081
RaftStateMachine: server-10081 remains FOLLOWER since requested term is 16 (higher than my 0)
```
Here we restart 'server-10082':
```
RaftServer: Raft server started on port 10082
RaftStateMachine: server-10082 remains FOLLOWER since requested term is 16 (higher than my 0)
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

Killing all server nodes:
```
➜  ./kill_raft.sh
Killing processes matching pattern: java -jar target/raft-1.0-SNAPSHOT.jar
```
