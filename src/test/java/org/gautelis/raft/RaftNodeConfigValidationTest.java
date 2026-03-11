/*
 * Copyright (C) 2025-2026 Frode Randers
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
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RaftNodeConfigValidationTest {
    private static final Logger log = LoggerFactory.getLogger(RaftNodeConfigValidationTest.class);
    private static void announce(String message) {
        System.out.println("*** Testcase *** " + message);
    }

    static class NoopRaftClient extends RaftClient {
        NoopRaftClient() {
            super("test", null);
        }

        @Override
        public void shutdown() {
            // no-op for tests
        }
    }

    @Test
    void rejectsDuplicatePeerIdWithConflictingAddress() {
        log.info("*** Testcase *** Membership conflict detection: verifies duplicate peer ids with different addresses are rejected at node construction");
        Peer me = new Peer("A", new InetSocketAddress("127.0.0.1", 10080));
        Peer b1 = new Peer("B", new InetSocketAddress("127.0.0.1", 10081));
        Peer b2 = new Peer("B", new InetSocketAddress("127.0.0.1", 10082));

        assertThrows(IllegalArgumentException.class, () -> new RaftNode(
                me,
                List.of(b1, b2),
                100,
                null,
                new NoopRaftClient(),
                new InMemoryLogStore(),
                new InMemoryPersistentStateStore(),
                System::currentTimeMillis,
                new Random(1)
        ));
    }

    @Test
    void rejectsSelfPeerWithConflictingAddress() {
        log.info("*** Testcase *** Self identity conflict detection: verifies self id duplicated with different address is rejected");
        Peer me = new Peer("A", new InetSocketAddress("127.0.0.1", 10080));
        Peer duplicateSelf = new Peer("A", new InetSocketAddress("127.0.0.1", 10081));

        assertThrows(IllegalArgumentException.class, () -> new RaftNode(
                me,
                List.of(duplicateSelf),
                100,
                null,
                new NoopRaftClient(),
                new InMemoryLogStore(),
                new InMemoryPersistentStateStore(),
                System::currentTimeMillis,
                new Random(1)
        ));
    }

    @Test
    void nodeCanTrackJointConsensusConfigurationState() {
        announce("Configuration state tracking: node tracks joint and finalized memberships");
        Peer me = new Peer("A", new InetSocketAddress("127.0.0.1", 10080));
        Peer b = new Peer("B", new InetSocketAddress("127.0.0.1", 10081));
        Peer c = new Peer("C", new InetSocketAddress("127.0.0.1", 10082));
        Peer d = new Peer("D", new InetSocketAddress("127.0.0.1", 10083));

        RaftNode node = new RaftNode(
                me,
                List.of(b, c),
                100,
                null,
                new NoopRaftClient(),
                new InMemoryLogStore(),
                new InMemoryPersistentStateStore(),
                System::currentTimeMillis,
                new Random(1)
        );

        assertFalse(node.getClusterConfigurationForTest().isJointConsensus());
        assertEquals(3, node.getClusterConfigurationForTest().allMembers().size());

        node.transitionToJointConfigurationForTest(List.of(me, b, d));

        assertTrue(node.getClusterConfigurationForTest().isJointConsensus());
        assertEquals(4, node.getClusterConfigurationForTest().allMembers().size());
        assertTrue(node.getClusterConfigurationForTest().contains("C"));
        assertTrue(node.getClusterConfigurationForTest().contains("D"));

        node.finalizeConfigurationTransitionForTest();

        assertFalse(node.getClusterConfigurationForTest().isJointConsensus());
        assertEquals(3, node.getClusterConfigurationForTest().allMembers().size());
        assertFalse(node.getClusterConfigurationForTest().contains("C"));
        assertTrue(node.getClusterConfigurationForTest().contains("D"));
    }

    @Test
    void leaderRejectsOverlappingJointConfigurationChange() {
        announce("Configuration guard overlap: leader rejects second joint change while joint consensus is active");
        Peer a = new Peer("A", new InetSocketAddress("127.0.0.1", 10080));
        Peer b = new Peer("B", new InetSocketAddress("127.0.0.1", 10081));
        Peer d = new Peer("D", new InetSocketAddress("127.0.0.1", 10083));
        Peer e = new Peer("E", new InetSocketAddress("127.0.0.1", 10084));

        RaftNodeElectionTest.MutableTime time = new RaftNodeElectionTest.MutableTime(0);
        RaftNode node = new RaftNode(
                a,
                List.of(),
                100,
                null,
                new NoopRaftClient(),
                new InMemoryLogStore(),
                new InMemoryPersistentStateStore(),
                time,
                new Random(1)
        );

        node.setLastHeartbeatMillisForTest(0);
        time.set(10_000);
        node.electionTickForTest();
        node.handleVoteResponsesForTest(List.of(), 1);
        assertTrue(node.isLeader());

        assertTrue(node.submitJointConfigurationChange(List.of(a, b, d)));
        node.transitionToJointConfigurationForTest(List.of(a, b, d));

        assertFalse(node.submitJointConfigurationChange(List.of(a, b, e)));
    }

    @Test
    void leaderRejectsFinalizeWithoutJointConsensus() {
        announce("Configuration guard finalize: leader rejects finalize when no joint transition is active");
        Peer a = new Peer("A", new InetSocketAddress("127.0.0.1", 10080));

        RaftNodeElectionTest.MutableTime time = new RaftNodeElectionTest.MutableTime(0);
        RaftNode node = new RaftNode(
                a,
                List.of(),
                100,
                null,
                new NoopRaftClient(),
                new InMemoryLogStore(),
                new InMemoryPersistentStateStore(),
                time,
                new Random(1)
        );

        node.setLastHeartbeatMillisForTest(0);
        time.set(10_000);
        node.electionTickForTest();
        node.handleVoteResponsesForTest(List.of(), 1);
        assertTrue(node.isLeader());

        assertFalse(node.submitFinalizeConfigurationChange());
    }
}
