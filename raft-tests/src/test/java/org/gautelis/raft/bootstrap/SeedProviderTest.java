/*
 * Copyright (C) 2026 Frode Randers
 * All rights reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gautelis.raft.bootstrap;

import org.gautelis.raft.protocol.Peer;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SeedProviderTest {
    private static void announce(String message) {
        System.out.println("TC: " + message);
    }

    @Test
    void dnsSrvSeedProviderDerivesPeerIdsFromSrvTargets() {
        announce("Cluster bootstrap: seed provider derives peer IDs from DNS SRV record");
        DnsSrvResolver resolver = service -> List.of(
                new DnsSrvResolver.SrvRecord(10, 10, 7000, "raft-2.raft.local."),
                new DnsSrvResolver.SrvRecord(10, 10, 7000, "raft-1.raft.local.")
        );

        List<SeedEndpoint> seeds = new DnsSrvSeedProvider("_raft._tcp.raft.local", resolver).seeds();

        assertEquals("raft-1", seeds.getFirst().peerId());
        assertEquals("raft-1.raft.local.", seeds.get(0).host());
        assertEquals(7000, seeds.get(0).port());
        assertEquals("raft-2", seeds.get(1).peerId());
    }

    @Test
    void jndiSrvParserAcceptsStandardSrvRecordShape() {
        announce("Cluster bootstrap: JNDI DNS SRV resolver can parse standard SRV record");
        DnsSrvResolver.SrvRecord record = JndiDnsSrvResolver.parseSrvRecord("10 33 7000 raft-0.raft.default.svc.cluster.local.");

        assertEquals(10, record.priority());
        assertEquals(33, record.weight());
        assertEquals(7000, record.port());
        assertEquals("raft-0.raft.default.svc.cluster.local", record.target());
    }

    @Test
    void staticSeedProviderPreservesExplicitPeerSpecs() {
        announce("Cluster bootstrap: static seed provider preserves explicit peer specs");
        Peer voter = new Peer("n1", new InetSocketAddress("localhost", 10080));
        Peer learner = new Peer("n2", new InetSocketAddress("localhost", 10081), Peer.Role.LEARNER);

        List<SeedEndpoint> seeds = new StaticSeedProvider(List.of(voter, learner)).seeds();

        assertEquals("n1", seeds.getFirst().peerId());
        assertEquals("localhost", seeds.getFirst().host());
        assertEquals(10080, seeds.get(0).port());
        assertEquals(Peer.Role.VOTER, seeds.get(0).role());
        assertEquals("n2", seeds.get(1).peerId());
        assertEquals(Peer.Role.LEARNER, seeds.get(1).role());
    }
}
