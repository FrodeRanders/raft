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
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PeerTest {
    private static final Logger log = LoggerFactory.getLogger(PeerTest.class);

    @Test
    void peersWithSameIdAreEqualAndShareHashCode() {
        log.info("*** Testcase *** Peer identity equality: verifies peers with same id compare equal and share hash code");
        Peer first = new Peer("A", new InetSocketAddress("127.0.0.1", 10080));
        Peer second = new Peer("A", new InetSocketAddress("127.0.0.1", 10081));

        assertEquals(first, second);
        assertEquals(first.hashCode(), second.hashCode());
    }

    @Test
    void mapLookupWorksAcrossEquivalentPeerInstances() {
        log.info("*** Testcase *** Peer map-key behavior: verifies id-equivalent peer instances resolve the same map entry");
        Peer key = new Peer("A", new InetSocketAddress("127.0.0.1", 10080));
        Peer equivalent = new Peer("A", new InetSocketAddress("127.0.0.1", 10081));

        Map<Peer, String> map = new HashMap<>();
        map.put(key, "channel");

        assertTrue(map.containsKey(equivalent));
        assertEquals("channel", map.get(equivalent));
    }

    @Test
    void peerRejectsNullOrBlankId() {
        log.info("*** Testcase *** Peer id validation: verifies peer construction rejects null or blank ids");
        assertThrows(IllegalArgumentException.class, () -> new Peer(null, new InetSocketAddress("127.0.0.1", 10080)));
        assertThrows(IllegalArgumentException.class, () -> new Peer("   ", new InetSocketAddress("127.0.0.1", 10080)));
    }
}
