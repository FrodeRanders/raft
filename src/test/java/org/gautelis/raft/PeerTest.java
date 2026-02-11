package org.gautelis.raft;

import org.gautelis.raft.model.Peer;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PeerTest {

    @Test
    void peersWithSameIdAreEqualAndShareHashCode() {
        Peer first = new Peer("A", new InetSocketAddress("127.0.0.1", 10080));
        Peer second = new Peer("A", new InetSocketAddress("127.0.0.1", 10081));

        assertEquals(first, second);
        assertEquals(first.hashCode(), second.hashCode());
    }

    @Test
    void mapLookupWorksAcrossEquivalentPeerInstances() {
        Peer key = new Peer("A", new InetSocketAddress("127.0.0.1", 10080));
        Peer equivalent = new Peer("A", new InetSocketAddress("127.0.0.1", 10081));

        Map<Peer, String> map = new HashMap<>();
        map.put(key, "channel");

        assertTrue(map.containsKey(equivalent));
        assertEquals("channel", map.get(equivalent));
    }
}
