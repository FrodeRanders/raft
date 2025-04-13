package org.gautelis.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.gautelis.raft.model.Peer;
import org.junit.jupiter.api.Test;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

class RaftTest {
    private static final Logger log = LogManager.getLogger(RaftTest.class);


    @Test
    void initiation() {
        int myPort = 10080;
        Peer me = new Peer("server0", new InetSocketAddress("localhost", myPort));

        List<Peer> peers = new ArrayList<>();
        peers.add(new Peer("server1", new InetSocketAddress("localhost", 10081)));
        peers.add(new Peer("server2", new InetSocketAddress("localhost", 10082)));
        peers.add(new Peer("server3", new InetSocketAddress("localhost", 10083)));

        long timeoutMillis = 5000;
        NettyRaftClient raftClient = new NettyRaftClient();

        RaftStateMachine stateMachine = new RaftStateMachine(peers, me, timeoutMillis, raftClient);

        NettyRaftServer raftServer = new NettyRaftServer(stateMachine, myPort);

        try {
            raftServer.start();
        }
        catch (InterruptedException ie) {
            log.info("Interrupted!", ie);
        }
    }

}