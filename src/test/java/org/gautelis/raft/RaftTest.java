package org.gautelis.raft;

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
        Peer me = new Peer("server-" + myPort, new InetSocketAddress("localhost", myPort));

        List<Peer> peers = new ArrayList<>();
        peers.add(new Peer("server-" + (myPort + 1), new InetSocketAddress("localhost", myPort + 1)));
        peers.add(new Peer("server-" + (myPort + 2), new InetSocketAddress("localhost", myPort + 2)));
        peers.add(new Peer("server-" + (myPort + 3), new InetSocketAddress("localhost", myPort + 3)));

        long timeoutMillis = 2000;

        ClusterMember member = new ClusterMember(timeoutMillis, me, peers);

        Thread _t = new Thread(() -> {
            try {
                Thread.sleep(10_000);
            } catch (InterruptedException ignore) {}

            member.inform("I'll explain it to you slowly");
        });
        _t.start();

        member.start();
    }
}