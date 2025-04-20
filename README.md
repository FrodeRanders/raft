# raft
A Raft implementation in Java on Netty.io 5.0.

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
RaftServer: Raft server started on port 10081
RaftServer: Raft server started on port 10083
RaftServer: Raft server started on port 10082
RaftServer: Raft server started on port 10084
RaftServer: Raft server started on port 10080
RaftStateMachine: server-10081 initiating new election...
RaftStateMachine: server-10083 initiating new election...
RaftStateMachine: server-10082 received vote request for term 1 from server-10083
RaftStateMachine: server-10082 steps to FOLLOWER since requested term is 1 (> 0)
RaftStateMachine: server-10082 grants vote to server-10083 for term 1 => accepted
RaftStateMachine: server-10080 received vote request for term 1 from server-10083
RaftStateMachine: server-10080 steps to FOLLOWER since requested term is 1 (> 0)
RaftStateMachine: server-10084 received vote request for term 1 from server-10081
RaftStateMachine: server-10080 grants vote to server-10083 for term 1 => accepted
RaftStateMachine: server-10084 steps to FOLLOWER since requested term is 1 (> 0)
RaftStateMachine: server-10084 grants vote to server-10081 for term 1 => accepted
RaftStateMachine: server-10082 received vote request for term 1 from server-10081
RaftStateMachine: server-10082 will *not* grant vote to server-10081 for term 1 (already voted for server-10083)
RaftStateMachine: server-10084 received vote request for term 1 from server-10083
RaftStateMachine: server-10084 will *not* grant vote to server-10083 for term 1 (already voted for server-10081)
RaftStateMachine: server-10080 received vote request for term 1 from server-10081
RaftStateMachine: server-10080 will *not* grant vote to server-10081 for term 1 (already voted for server-10083)
RaftStateMachine: server-10083 received vote request for term 1 from server-10081
RaftStateMachine: server-10083 will *not* grant vote to server-10081 for term 1 (already voted for server-10083)
RaftStateMachine: server-10081 received vote request for term 1 from server-10083
RaftStateMachine: server-10081 will *not* grant vote to server-10083 for term 1 (already voted for server-10081)
RaftStateMachine: server-10083 elected LEADER (3 votes granted >= majority 3)
```
Here we manually kill 'server-10083', which initiate a new leader election:
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
server-10080 initiates a new leader election (and failing to reach out to server-10083):
```
RaftStateMachine: server-10080 initiating new election...
RaftClient: Failed to connect to server-10083 at localhost/127.0.0.1:10083
RaftClient: Could not request vote for term 2 from server-10083: cannot connect
RaftStateMachine: server-10082 received vote request for term 2 from server-10080
RaftStateMachine: server-10082 steps to FOLLOWER since requested term is 2 (> 1)
RaftStateMachine: server-10082 grants vote to server-10080 for term 2 => accepted
RaftStateMachine: server-10081 received vote request for term 2 from server-10080
RaftStateMachine: server-10081 steps to FOLLOWER since requested term is 2 (> 1)
RaftStateMachine: server-10081 grants vote to server-10080 for term 2 => accepted
RaftStateMachine: server-10084 received vote request for term 2 from server-10080
RaftStateMachine: server-10084 steps to FOLLOWER since requested term is 2 (> 1)
RaftStateMachine: server-10084 grants vote to server-10080 for term 2 => accepted
RaftStateMachine: server-10080 elected LEADER (4 votes granted >= majority 3)
```
Killing all server nodes:
```
➜  ./kill_raft.sh
Killing processes matching pattern: java -jar target/raft-1.0-SNAPSHOT.jar
```

Here is another run where two out of five servers are killed, and the three remaining have
some negotiations going before reaching consensus:
```
RaftServer: Raft server started on port 10083
RaftServer: Raft server started on port 10084
RaftServer: Raft server started on port 10080
RaftServer: Raft server started on port 10081
RaftServer: Raft server started on port 10082
RaftStateMachine: server-10081 initiating new election...
RaftStateMachine: server-10083 received vote request for term 1 from server-10081
RaftStateMachine: server-10083 steps to FOLLOWER since requested term is 1 (> 0)
RaftStateMachine: server-10083 grants vote to server-10081 for term 1 and steps to FOLLOWER => accepted
RaftStateMachine: server-10080 received vote request for term 1 from server-10081
RaftStateMachine: server-10080 steps to FOLLOWER since requested term is 1 (> 0)
RaftStateMachine: server-10080 grants vote to server-10081 for term 1 and steps to FOLLOWER => accepted
RaftStateMachine: server-10082 received vote request for term 1 from server-10081
RaftStateMachine: server-10082 steps to FOLLOWER since requested term is 1 (> 0)
RaftStateMachine: server-10082 grants vote to server-10081 for term 1 and steps to FOLLOWER => accepted
RaftStateMachine: server-10084 received vote request for term 1 from server-10081
RaftStateMachine: server-10084 steps to FOLLOWER since requested term is 1 (> 0)
RaftStateMachine: server-10084 grants vote to server-10081 for term 1 and steps to FOLLOWER => accepted
RaftStateMachine: server-10081 elected LEADER (5 votes granted >= majority 3)

<server-10081 is killed>

RaftStateMachine: server-10080 initiating new election...
RaftClient: Failed to connect to server-10081 at localhost/127.0.0.1:10081
RaftClient: Could not request vote for term 2 from server-10081: cannot connect
RaftStateMachine: server-10082 received vote request for term 2 from server-10080
RaftStateMachine: server-10082 steps to FOLLOWER since requested term is 2 (> 1)
RaftStateMachine: server-10082 grants vote to server-10080 for term 2 and steps to FOLLOWER => accepted
RaftStateMachine: server-10083 received vote request for term 2 from server-10080
RaftStateMachine: server-10084 received vote request for term 2 from server-10080
RaftStateMachine: server-10083 steps to FOLLOWER since requested term is 2 (> 1)
RaftStateMachine: server-10083 grants vote to server-10080 for term 2 and steps to FOLLOWER => accepted
RaftStateMachine: server-10084 steps to FOLLOWER since requested term is 2 (> 1)
RaftStateMachine: server-10084 grants vote to server-10080 for term 2 and steps to FOLLOWER => accepted
RaftStateMachine: server-10080 elected LEADER (3 votes granted >= majority 3)

<server-10080 is killed>

RaftStateMachine: server-10084 initiating new election...
RaftClient: Failed to connect to server-10080 at localhost/127.0.0.1:10080
RaftClient: Failed to connect to server-10081 at localhost/127.0.0.1:10081
RaftClient: Could not request vote for term 3 from server-10080: cannot connect
RaftClient: Could not request vote for term 3 from server-10081: cannot connect
RaftStateMachine: server-10083 received vote request for term 3 from server-10084
RaftStateMachine: server-10083 steps to FOLLOWER since requested term is 3 (> 2)
RaftStateMachine: server-10083 grants vote to server-10084 for term 3 and steps to FOLLOWER => accepted
RaftStateMachine: server-10082 received vote request for term 3 from server-10084
RaftStateMachine: server-10082 steps to FOLLOWER since requested term is 3 (> 2)
RaftStateMachine: server-10082 grants vote to server-10084 for term 3 and steps to FOLLOWER => accepted
RaftStateMachine: server-10083 initiating new election...
RaftClient: Failed to connect to server-10081 at localhost/127.0.0.1:10081
RaftClient: Could not request vote for term 4 from server-10081: cannot connect
RaftClient: Failed to connect to server-10080 at localhost/127.0.0.1:10080
RaftClient: Could not request vote for term 4 from server-10080: cannot connect
RaftStateMachine: server-10084 received vote request for term 4 from server-10083
RaftStateMachine: server-10084 steps to FOLLOWER since requested term is 4 (> 3)
RaftStateMachine: server-10084 grants vote to server-10083 for term 4 and steps to FOLLOWER => accepted
RaftStateMachine: server-10082 received vote request for term 4 from server-10083
RaftStateMachine: server-10082 steps to FOLLOWER since requested term is 4 (> 3)
RaftStateMachine: server-10082 grants vote to server-10083 for term 4 and steps to FOLLOWER => accepted
RaftStateMachine: server-10084 initiating new election...
RaftClient: Failed to connect to server-10080 at localhost/127.0.0.1:10080
RaftClient: Failed to connect to server-10081 at localhost/127.0.0.1:10081
RaftStateMachine: server-10082 received vote request for term 5 from server-10084
RaftClient: Could not request vote for term 5 from server-10080: cannot connect
RaftClient: Could not request vote for term 5 from server-10081: cannot connect
RaftStateMachine: server-10082 steps to FOLLOWER since requested term is 5 (> 4)
RaftStateMachine: server-10082 grants vote to server-10084 for term 5 and steps to FOLLOWER => accepted
RaftStateMachine: server-10083 received vote request for term 5 from server-10084
RaftStateMachine: server-10083 steps to FOLLOWER since requested term is 5 (> 4)
RaftStateMachine: server-10083 grants vote to server-10084 for term 5 and steps to FOLLOWER => accepted
RaftStateMachine: server-10084 initiating new election...
RaftStateMachine: server-10083 received vote request for term 6 from server-10084
RaftStateMachine: server-10083 steps to FOLLOWER since requested term is 6 (> 5)
RaftStateMachine: server-10083 grants vote to server-10084 for term 6 and steps to FOLLOWER => accepted
RaftClient: Failed to connect to server-10080 at localhost/127.0.0.1:10080
RaftClient: Could not request vote for term 6 from server-10080: cannot connect
RaftClient: Failed to connect to server-10081 at localhost/127.0.0.1:10081
RaftClient: Could not request vote for term 6 from server-10081: cannot connect
RaftStateMachine: server-10082 received vote request for term 6 from server-10084
RaftStateMachine: server-10082 steps to FOLLOWER since requested term is 6 (> 5)
RaftStateMachine: server-10082 grants vote to server-10084 for term 6 and steps to FOLLOWER => accepted
RaftStateMachine: server-10082 initiating new election...
RaftClient: Failed to connect to server-10080 at localhost/127.0.0.1:10080
RaftClient: Failed to connect to server-10081 at localhost/127.0.0.1:10081
RaftClient: Could not request vote for term 7 from server-10080: cannot connect
RaftClient: Could not request vote for term 7 from server-10081: cannot connect
RaftStateMachine: server-10083 received vote request for term 7 from server-10082
RaftStateMachine: server-10083 steps to FOLLOWER since requested term is 7 (> 6)
RaftStateMachine: server-10083 grants vote to server-10082 for term 7 and steps to FOLLOWER => accepted
RaftStateMachine: server-10084 received vote request for term 7 from server-10082
RaftStateMachine: server-10084 steps to FOLLOWER since requested term is 7 (> 6)
RaftStateMachine: server-10084 grants vote to server-10082 for term 7 and steps to FOLLOWER => accepted
RaftStateMachine: server-10083 initiating new election...
RaftStateMachine: server-10084 received vote request for term 8 from server-10083
RaftStateMachine: server-10084 steps to FOLLOWER since requested term is 8 (> 7)
RaftStateMachine: server-10084 grants vote to server-10083 for term 8 and steps to FOLLOWER => accepted
RaftClient: Failed to connect to server-10080 at localhost/127.0.0.1:10080
RaftClient: Failed to connect to server-10081 at localhost/127.0.0.1:10081
RaftClient: Could not request vote for term 8 from server-10080: cannot connect
RaftClient: Could not request vote for term 8 from server-10081: cannot connect
RaftStateMachine: server-10082 received vote request for term 8 from server-10083
RaftStateMachine: server-10082 steps to FOLLOWER since requested term is 8 (> 7)
RaftStateMachine: server-10082 grants vote to server-10083 for term 8 and steps to FOLLOWER => accepted
RaftStateMachine: server-10082 initiating new election...
RaftClient: Failed to connect to server-10081 at localhost/127.0.0.1:10081
RaftClient: Failed to connect to server-10080 at localhost/127.0.0.1:10080
RaftClient: Could not request vote for term 9 from server-10081: cannot connect
RaftClient: Could not request vote for term 9 from server-10080: cannot connect
RaftStateMachine: server-10084 received vote request for term 9 from server-10082
RaftStateMachine: server-10084 steps to FOLLOWER since requested term is 9 (> 8)
RaftStateMachine: server-10083 received vote request for term 9 from server-10082
RaftStateMachine: server-10084 grants vote to server-10082 for term 9 and steps to FOLLOWER => accepted
RaftStateMachine: server-10083 steps to FOLLOWER since requested term is 9 (> 8)
RaftStateMachine: server-10083 grants vote to server-10082 for term 9 and steps to FOLLOWER => accepted
RaftStateMachine: server-10084 initiating new election...
RaftStateMachine: server-10082 received vote request for term 10 from server-10084
RaftClient: Failed to connect to server-10080 at localhost/127.0.0.1:10080
RaftStateMachine: server-10082 steps to FOLLOWER since requested term is 10 (> 9)
RaftClient: Could not request vote for term 10 from server-10080: cannot connect
RaftStateMachine: server-10082 grants vote to server-10084 for term 10 and steps to FOLLOWER => accepted
RaftClient: Failed to connect to server-10081 at localhost/127.0.0.1:10081
RaftClient: Could not request vote for term 10 from server-10081: cannot connect
RaftStateMachine: server-10083 received vote request for term 10 from server-10084
RaftStateMachine: server-10083 steps to FOLLOWER since requested term is 10 (> 9)
RaftStateMachine: server-10083 grants vote to server-10084 for term 10 and steps to FOLLOWER => accepted
RaftStateMachine: server-10083 initiating new election...
RaftStateMachine: server-10084 received vote request for term 11 from server-10083
RaftStateMachine: server-10082 received vote request for term 11 from server-10083
RaftStateMachine: server-10084 steps to FOLLOWER since requested term is 11 (> 10)
RaftStateMachine: server-10082 steps to FOLLOWER since requested term is 11 (> 10)
RaftStateMachine: server-10084 grants vote to server-10083 for term 11 and steps to FOLLOWER => accepted
RaftStateMachine: server-10082 grants vote to server-10083 for term 11 and steps to FOLLOWER => accepted
RaftClient: Failed to connect to server-10080 at localhost/127.0.0.1:10080
RaftClient: Could not request vote for term 11 from server-10080: cannot connect
RaftClient: Failed to connect to server-10081 at localhost/127.0.0.1:10081
RaftClient: Could not request vote for term 11 from server-10081: cannot connect
RaftStateMachine: server-10082 initiating new election...
RaftStateMachine: server-10083 received vote request for term 12 from server-10082
RaftStateMachine: server-10084 received vote request for term 12 from server-10082
RaftStateMachine: server-10083 steps to FOLLOWER since requested term is 12 (> 11)
RaftStateMachine: server-10083 grants vote to server-10082 for term 12 and steps to FOLLOWER => accepted
RaftStateMachine: server-10084 steps to FOLLOWER since requested term is 12 (> 11)
RaftStateMachine: server-10084 grants vote to server-10082 for term 12 and steps to FOLLOWER => accepted
RaftClient: Failed to connect to server-10081 at localhost/127.0.0.1:10081
RaftClient: Could not request vote for term 12 from server-10081: cannot connect
RaftClient: Failed to connect to server-10080 at localhost/127.0.0.1:10080
RaftClient: Could not request vote for term 12 from server-10080: cannot connect
RaftStateMachine: server-10083 initiating new election...
RaftClient: Failed to connect to server-10081 at localhost/127.0.0.1:10081
RaftClient: Could not request vote for term 13 from server-10081: cannot connect
RaftStateMachine: server-10084 received vote request for term 13 from server-10083
RaftClient: Failed to connect to server-10080 at localhost/127.0.0.1:10080
RaftStateMachine: server-10084 steps to FOLLOWER since requested term is 13 (> 12)
RaftClient: Could not request vote for term 13 from server-10080: cannot connect
RaftStateMachine: server-10084 grants vote to server-10083 for term 13 and steps to FOLLOWER => accepted
RaftStateMachine: server-10082 received vote request for term 13 from server-10083
RaftStateMachine: server-10082 steps to FOLLOWER since requested term is 13 (> 12)
RaftStateMachine: server-10082 grants vote to server-10083 for term 13 and steps to FOLLOWER => accepted
RaftStateMachine: server-10084 initiating new election...
RaftClient: Failed to connect to server-10081 at localhost/127.0.0.1:10081
RaftStateMachine: server-10082 received vote request for term 14 from server-10084
RaftClient: Could not request vote for term 14 from server-10081: cannot connect
RaftStateMachine: server-10082 steps to FOLLOWER since requested term is 14 (> 13)
RaftStateMachine: server-10082 grants vote to server-10084 for term 14 and steps to FOLLOWER => accepted
RaftStateMachine: server-10083 received vote request for term 14 from server-10084
RaftStateMachine: server-10083 steps to FOLLOWER since requested term is 14 (> 13)
RaftStateMachine: server-10083 grants vote to server-10084 for term 14 and steps to FOLLOWER => accepted
RaftClient: Failed to connect to server-10080 at localhost/127.0.0.1:10080
RaftClient: Could not request vote for term 14 from server-10080: cannot connect
RaftStateMachine: server-10082 initiating new election...
RaftStateMachine: server-10084 received vote request for term 15 from server-10082
RaftStateMachine: server-10083 received vote request for term 15 from server-10082
RaftStateMachine: server-10084 steps to FOLLOWER since requested term is 15 (> 14)
RaftStateMachine: server-10083 steps to FOLLOWER since requested term is 15 (> 14)
RaftStateMachine: server-10084 grants vote to server-10082 for term 15 and steps to FOLLOWER => accepted
RaftStateMachine: server-10083 grants vote to server-10082 for term 15 and steps to FOLLOWER => accepted
RaftClient: Failed to connect to server-10080 at localhost/127.0.0.1:10080
RaftClient: Could not request vote for term 15 from server-10080: cannot connect
RaftClient: Failed to connect to server-10081 at localhost/127.0.0.1:10081
RaftClient: Could not request vote for term 15 from server-10081: cannot connect
RaftStateMachine: server-10083 initiating new election...
RaftClient: Failed to connect to server-10081 at localhost/127.0.0.1:10081
RaftClient: Could not request vote for term 16 from server-10081: cannot connect
RaftStateMachine: server-10082 received vote request for term 16 from server-10083
RaftClient: Failed to connect to server-10080 at localhost/127.0.0.1:10080
RaftClient: Could not request vote for term 16 from server-10080: cannot connect
RaftStateMachine: server-10082 steps to FOLLOWER since requested term is 16 (> 15)
RaftStateMachine: server-10082 grants vote to server-10083 for term 16 and steps to FOLLOWER => accepted
RaftStateMachine: server-10084 received vote request for term 16 from server-10083
RaftStateMachine: server-10084 steps to FOLLOWER since requested term is 16 (> 15)
RaftStateMachine: server-10084 grants vote to server-10083 for term 16 and steps to FOLLOWER => accepted
RaftStateMachine: server-10082 initiating new election...
RaftStateMachine: server-10084 received vote request for term 17 from server-10082
RaftStateMachine: server-10084 steps to FOLLOWER since requested term is 17 (> 16)
RaftStateMachine: server-10084 grants vote to server-10082 for term 17 and steps to FOLLOWER => accepted
RaftClient: Failed to connect to server-10080 at localhost/127.0.0.1:10080
RaftClient: Could not request vote for term 17 from server-10080: cannot connect
RaftStateMachine: server-10083 received vote request for term 17 from server-10082
RaftStateMachine: server-10083 steps to FOLLOWER since requested term is 17 (> 16)
RaftStateMachine: server-10083 grants vote to server-10082 for term 17 and steps to FOLLOWER => accepted
RaftClient: Failed to connect to server-10081 at localhost/127.0.0.1:10081
RaftClient: Could not request vote for term 17 from server-10081: cannot connect
RaftStateMachine: server-10084 initiating new election...
RaftClient: Failed to connect to server-10080 at localhost/127.0.0.1:10080
RaftClient: Failed to connect to server-10081 at localhost/127.0.0.1:10081
RaftClient: Could not request vote for term 18 from server-10081: cannot connect
RaftStateMachine: server-10083 received vote request for term 18 from server-10084
RaftStateMachine: server-10083 steps to FOLLOWER since requested term is 18 (> 17)
RaftStateMachine: server-10083 grants vote to server-10084 for term 18 and steps to FOLLOWER => accepted
RaftStateMachine: server-10082 received vote request for term 18 from server-10084
RaftStateMachine: server-10082 steps to FOLLOWER since requested term is 18 (> 17)
RaftStateMachine: server-10082 grants vote to server-10084 for term 18 and steps to FOLLOWER => accepted
RaftClient: Could not request vote for term 18 from server-10080: cannot connect
RaftStateMachine: server-10084 elected LEADER (3 votes granted >= majority 3)
```
