# raft
A Raft implementation in Java on Netty.io.

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
[main] [INFO ] NettyRaftServer: Raft server started on port 10080
[main] [INFO ] NettyRaftServer: Raft server started on port 10084
[main] [INFO ] NettyRaftServer: Raft server started on port 10081
[main] [INFO ] NettyRaftServer: Raft server started on port 10082
[main] [INFO ] NettyRaftServer: Raft server started on port 10083
[nioEventLoopGroup-2-0] [DEBUG] RaftStateMachine: server-10083 received vote request for term 1 from server-10081
[nioEventLoopGroup-2-0] [DEBUG] RaftStateMachine: server-10083 steps down from leader to follower since requested term is 1 (> 0)
[nioEventLoopGroup-2-0] [DEBUG] RaftStateMachine: server-10083 grants vote to server-10081 for term 1 => accepted
[nioEventLoopGroup-2-0] [DEBUG] RaftStateMachine: server-10084 received vote request for term 1 from server-10081
[nioEventLoopGroup-2-0] [DEBUG] RaftStateMachine: server-10084 steps down from leader to follower since requested term is 1 (> 0)
[nioEventLoopGroup-2-0] [DEBUG] RaftStateMachine: server-10082 received vote request for term 1 from server-10081
[nioEventLoopGroup-2-0] [DEBUG] RaftStateMachine: server-10082 steps down from leader to follower since requested term is 1 (> 0)
[nioEventLoopGroup-2-0] [DEBUG] RaftStateMachine: server-10082 grants vote to server-10081 for term 1 => accepted
[nioEventLoopGroup-2-0] [DEBUG] RaftStateMachine: server-10084 grants vote to server-10081 for term 1 => accepted
[nioEventLoopGroup-2-1] [DEBUG] RaftStateMachine: server-10080 received vote request for term 1 from server-10081
[nioEventLoopGroup-2-1] [DEBUG] RaftStateMachine: server-10080 steps down from leader to follower since requested term is 1 (> 0)
[nioEventLoopGroup-2-1] [DEBUG] RaftStateMachine: server-10080 grants vote to server-10081 for term 1 => accepted
[nioEventLoopGroup-0-1] [DEBUG] RaftStateMachine: Elected LEADER (4 votes granted >= majority 3): Peer{id=server-10081, address=localhost/127.0.0.1:10081}
```
Here we manually kill 'server-10081', which initiate a new leader election:
```
➜  ps -ef
...
501 39092     1   0  1:41PM ttys000    0:01.34 /usr/bin/java -jar target/raft-1.0-SNAPSHOT.jar 10080 10081 10082 10083 10084
501 39093     1   0  1:41PM ttys000    0:01.65 /usr/bin/java -jar target/raft-1.0-SNAPSHOT.jar 10081 10080 10082 10083 10084
501 39094     1   0  1:41PM ttys000    0:01.32 /usr/bin/java -jar target/raft-1.0-SNAPSHOT.jar 10082 10080 10081 10083 10084
501 39095     1   0  1:41PM ttys000    0:01.32 /usr/bin/java -jar target/raft-1.0-SNAPSHOT.jar 10083 10080 10081 10082 10084
501 39096     1   0  1:41PM ttys000    0:01.32 /usr/bin/java -jar target/raft-1.0-SNAPSHOT.jar 10084 10080 10081 10082 10083
...

➜  kill -9 39093
```
This results in...
```
[nioEventLoopGroup-0-2] [WARN ] NettyRaftClient: Failed to connect to server-10081 at localhost/127.0.0.1:10081
[nioEventLoopGroup-0-2] [INFO ] NettyRaftClient: Could not request vote for term 2 from server-10081: cannot connect
[nioEventLoopGroup-2-4] [DEBUG] RaftStateMachine: server-10082 received vote request for term 2 from server-10083
[nioEventLoopGroup-2-4] [DEBUG] RaftStateMachine: server-10082 steps down from leader to follower since requested term is 2 (> 1)
[nioEventLoopGroup-2-2] [DEBUG] RaftStateMachine: server-10080 received vote request for term 2 from server-10083
[nioEventLoopGroup-2-4] [DEBUG] RaftStateMachine: server-10082 grants vote to server-10083 for term 2 => accepted
[nioEventLoopGroup-2-2] [DEBUG] RaftStateMachine: server-10080 steps down from leader to follower since requested term is 2 (> 1)
[nioEventLoopGroup-2-2] [DEBUG] RaftStateMachine: server-10080 grants vote to server-10083 for term 2 => accepted
[nioEventLoopGroup-2-1] [DEBUG] RaftStateMachine: server-10084 received vote request for term 2 from server-10083
[nioEventLoopGroup-2-1] [DEBUG] RaftStateMachine: server-10084 steps down from leader to follower since requested term is 2 (> 1)
[nioEventLoopGroup-2-1] [DEBUG] RaftStateMachine: server-10084 grants vote to server-10083 for term 2 => accepted
[nioEventLoopGroup-0-6] [DEBUG] RaftStateMachine: Elected LEADER (3 votes granted >= majority 3): Peer{id=server-10083, address=localhost/127.0.0.1:10083}
```
Here we manually kill 'server-10083', which initiate a new leader election:
```
[nioEventLoopGroup-0-4] [WARN ] NettyRaftClient: Failed to connect to server-10081 at localhost/127.0.0.1:10081
[nioEventLoopGroup-0-1] [WARN ] NettyRaftClient: Failed to connect to server-10083 at localhost/127.0.0.1:10083
[nioEventLoopGroup-0-4] [INFO ] NettyRaftClient: Could not request vote for term 3 from server-10081: cannot connect
[nioEventLoopGroup-0-1] [INFO ] NettyRaftClient: Could not request vote for term 3 from server-10083: cannot connect
[nioEventLoopGroup-2-3] [DEBUG] RaftStateMachine: server-10082 received vote request for term 3 from server-10080
[nioEventLoopGroup-2-3] [DEBUG] RaftStateMachine: server-10082 steps down from leader to follower since requested term is 3 (> 2)
[nioEventLoopGroup-2-3] [DEBUG] RaftStateMachine: server-10082 grants vote to server-10080 for term 3 => accepted
[nioEventLoopGroup-2-4] [DEBUG] RaftStateMachine: server-10084 received vote request for term 3 from server-10080
[nioEventLoopGroup-2-4] [DEBUG] RaftStateMachine: server-10084 steps down from leader to follower since requested term is 3 (> 2)
[nioEventLoopGroup-2-4] [DEBUG] RaftStateMachine: server-10084 grants vote to server-10080 for term 3 => accepted
[nioEventLoopGroup-2-4] [DEBUG] RaftStateMachine: server-10084 received vote request for term 4 from server-10080
[nioEventLoopGroup-2-4] [DEBUG] RaftStateMachine: server-10084 steps down from leader to follower since requested term is 4 (> 3)
[nioEventLoopGroup-2-4] [DEBUG] RaftStateMachine: server-10084 grants vote to server-10080 for term 4 => accepted
[nioEventLoopGroup-2-3] [DEBUG] RaftStateMachine: server-10082 received vote request for term 4 from server-10080
[nioEventLoopGroup-2-3] [DEBUG] RaftStateMachine: server-10082 steps down from leader to follower since requested term is 4 (> 3)
[nioEventLoopGroup-2-3] [DEBUG] RaftStateMachine: server-10082 grants vote to server-10080 for term 4 => accepted
[nioEventLoopGroup-0-5] [WARN ] NettyRaftClient: Failed to connect to server-10081 at localhost/127.0.0.1:10081
[nioEventLoopGroup-0-5] [INFO ] NettyRaftClient: Could not request vote for term 4 from server-10081: cannot connect
[nioEventLoopGroup-0-11] [DEBUG] RaftStateMachine: Elected LEADER (3 votes granted >= majority 3): Peer{id=server-10080, address=localhost/127.0.0.1:10080}
```
Here somewhere we start killing all server nodes (after having run the demo):
```
➜  ./kill_raft.sh
Killing processes matching pattern: java -jar target/raft-1.0-SNAPSHOT.jar
```
This results in...
```
[nioEventLoopGroup-0-7] [WARN ] NettyRaftClient: Failed to connect to server-10083 at localhost/127.0.0.1:10083
[nioEventLoopGroup-0-7] [INFO ] NettyRaftClient: Could not request vote for term 4 from server-10083: cannot connect
```
