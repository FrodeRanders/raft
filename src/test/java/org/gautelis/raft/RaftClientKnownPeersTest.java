package org.gautelis.raft;

import org.gautelis.raft.storage.*;
import org.gautelis.raft.statemachine.*;
import org.gautelis.raft.transport.netty.*;
import org.gautelis.raft.serialization.ProtoMapper;

import org.gautelis.raft.protocol.Peer;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

class RaftClientKnownPeersTest {
    private static final Logger log = LoggerFactory.getLogger(RaftClientKnownPeersTest.class);

    @Test
    void broadcastReturnsKnownPeerAsUnreachableAfterExplicitRegistration() throws Exception {
        log.info("*** Testcase *** Explicit known-peer broadcast path: verifies registered peers are tracked and reported unreachable when connect fails");
        RaftClient client = new RaftClient("test", null);
        try {
            Peer peer = new Peer("B", new InetSocketAddress("127.0.0.1", 1));
            client.setKnownPeers(List.of(peer));

            Collection<Peer> unreachable = client.broadcast("CustomType", 1, "hello".getBytes(java.nio.charset.StandardCharsets.UTF_8));

            // connect attempt callback is async; allow event loop to update unreachable set
            Thread.sleep(150);
            assertTrue(unreachable.contains(peer));
        } finally {
            client.shutdown();
        }
    }
}
