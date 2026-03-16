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

import org.gautelis.raft.protocol.Peer;
import org.gautelis.raft.protocol.VoteRequest;
import org.gautelis.raft.protocol.VoteResponse;
import org.gautelis.raft.storage.InMemoryLogStore;
import org.gautelis.raft.transport.netty.RaftClient;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RaftConfigurationReplicationTest {
    private static void announce(String message) {
        System.out.println("TC: " + message);
    }

    private static Peer peer(String id) {
        return new Peer(id, null);
    }

    private static RaftNode newTestNode(
            Peer me,
            List<Peer> peers,
            RaftClient raftClient,
            InMemoryLogStore logStore,
            RaftNode.TimeSource timeSource,
            int randomSeed
    ) {
        return TestRaftNodeBuilder.forPeer(me)
                .withPeers(peers)
                .withTimeoutMillis(100)
                .withStateMachine(null)
                .withClient(raftClient)
                .withLogStore(logStore)
                .withTimeSource(timeSource)
                .withRandom(new Random(randomSeed))
                .build();
    }

    @Test
    void committedJointConfigurationEntryMovesLeaderIntoJointConsensus() {
        announce("Configuration replication joint entry: committed joint config moves cluster into joint consensus");
        RaftNodeElectionTest.MutableTime time = new RaftNodeElectionTest.MutableTime(0);

        Peer a = peer("A");
        Peer b = peer("B");
        Peer c = peer("C");
        Peer d = peer("D");

        Map<String, RaftNode> nodes = new HashMap<>();

        RaftNodeElectionTest.QueuedRaftClient clientA = new RaftNodeElectionTest.QueuedRaftClient("A", nodes);
        RaftNodeElectionTest.QueuedRaftClient clientB = new RaftNodeElectionTest.QueuedRaftClient("B", nodes);
        RaftNodeElectionTest.QueuedRaftClient clientC = new RaftNodeElectionTest.QueuedRaftClient("C", nodes);

        RaftNode nodeA = newTestNode(a, List.of(b, c), clientA, new InMemoryLogStore(), time, 1);
        RaftNode nodeB = newTestNode(b, List.of(a, c), clientB, new InMemoryLogStore(), time, 2);
        RaftNode nodeC = newTestNode(c, List.of(a, b), clientC, new InMemoryLogStore(), time, 3);

        nodes.put("A", nodeA);
        nodes.put("B", nodeB);
        nodes.put("C", nodeC);

        nodeA.setLastHeartbeatMillisForTest(0);
        time.set(10_000);
        nodeA.electionTickForTest();
        clientA.flush();
        assertTrue(nodeA.isLeader());

        assertTrue(nodeA.submitJointConfigurationChange(List.of(a, b, d)));
        clientA.flush();
        nodeA.heartbeatTickForTest();
        clientA.flush();

        assertTrue(nodeA.getClusterConfigurationForTest().isJointConsensus());
        assertTrue(nodeA.getClusterConfigurationForTest().contains("D"));
        assertTrue(nodeB.getClusterConfigurationForTest().isJointConsensus());
        assertTrue(nodeC.getClusterConfigurationForTest().isJointConsensus());
    }

    @Test
    void committedFinalizeEntryCompletesConfigurationTransition() {
        announce("Configuration replication finalize entry: committed finalize completes joint transition");
        RaftNodeElectionTest.MutableTime time = new RaftNodeElectionTest.MutableTime(0);

        Peer a = peer("A");
        Peer b = peer("B");
        Peer c = peer("C");
        Peer d = peer("D");

        Map<String, RaftNode> nodes = new HashMap<>();

        RaftNodeElectionTest.QueuedRaftClient clientA = new RaftNodeElectionTest.QueuedRaftClient("A", nodes);
        RaftNodeElectionTest.QueuedRaftClient clientB = new RaftNodeElectionTest.QueuedRaftClient("B", nodes);
        RaftNodeElectionTest.QueuedRaftClient clientC = new RaftNodeElectionTest.QueuedRaftClient("C", nodes);
        RaftNodeElectionTest.QueuedRaftClient clientD = new RaftNodeElectionTest.QueuedRaftClient("D", nodes);

        RaftNode nodeA = newTestNode(a, List.of(b, c), clientA, new InMemoryLogStore(), time, 1);
        RaftNode nodeB = newTestNode(b, List.of(a, c), clientB, new InMemoryLogStore(), time, 2);
        RaftNode nodeC = newTestNode(c, List.of(a, b), clientC, new InMemoryLogStore(), time, 3);
        RaftNode nodeD = newTestNode(d, List.of(a, b), clientD, new InMemoryLogStore(), time, 4);

        nodes.put("A", nodeA);
        nodes.put("B", nodeB);
        nodes.put("C", nodeC);
        nodes.put("D", nodeD);

        nodeA.setLastHeartbeatMillisForTest(0);
        time.set(10_000);
        nodeA.electionTickForTest();
        clientA.flush();
        assertTrue(nodeA.isLeader());

        assertTrue(nodeA.submitJointConfigurationChange(List.of(a, b, d)));
        clientA.flush();
        nodeA.heartbeatTickForTest();
        clientA.flush();
        assertTrue(nodeA.getClusterConfigurationForTest().isJointConsensus());

        // Bring D into the committed joint configuration before finalizing.
        nodeA.heartbeatTickForTest();
        clientA.flush();

        assertTrue(nodeA.submitFinalizeConfigurationChange());
        clientA.flush();
        nodeA.heartbeatTickForTest();
        clientA.flush();

        assertFalse(nodeA.getClusterConfigurationForTest().isJointConsensus());
        assertTrue(nodeA.getClusterConfigurationForTest().contains("D"));
        assertFalse(nodeA.getClusterConfigurationForTest().contains("C"));
        assertFalse(nodeB.getClusterConfigurationForTest().isJointConsensus());
        assertTrue(nodeD.getClusterConfigurationForTest().contains("D"));
    }

    @Test
    void effectiveConfigurationCanBeResolvedByLogIndex() {
        announce("Configuration by log index: effective membership resolves correctly across transition entries");
        RaftNodeElectionTest.MutableTime time = new RaftNodeElectionTest.MutableTime(0);

        Peer a = peer("A");
        Peer b = peer("B");
        Peer c = peer("C");
        Peer d = peer("D");

        Map<String, RaftNode> nodes = new HashMap<>();

        RaftNodeElectionTest.QueuedRaftClient clientA = new RaftNodeElectionTest.QueuedRaftClient("A", nodes);
        RaftNodeElectionTest.QueuedRaftClient clientB = new RaftNodeElectionTest.QueuedRaftClient("B", nodes);
        RaftNodeElectionTest.QueuedRaftClient clientC = new RaftNodeElectionTest.QueuedRaftClient("C", nodes);
        RaftNodeElectionTest.QueuedRaftClient clientD = new RaftNodeElectionTest.QueuedRaftClient("D", nodes);

        InMemoryLogStore storeA = new InMemoryLogStore();

        RaftNode nodeA = newTestNode(a, List.of(b, c), clientA, storeA, time, 1);
        RaftNode nodeB = newTestNode(b, List.of(a, c), clientB, new InMemoryLogStore(), time, 2);
        RaftNode nodeC = newTestNode(c, List.of(a, b), clientC, new InMemoryLogStore(), time, 3);
        RaftNode nodeD = newTestNode(d, List.of(a, b), clientD, new InMemoryLogStore(), time, 4);

        nodes.put("A", nodeA);
        nodes.put("B", nodeB);
        nodes.put("C", nodeC);
        nodes.put("D", nodeD);

        nodeA.setLastHeartbeatMillisForTest(0);
        time.set(10_000);
        nodeA.electionTickForTest();
        clientA.flush();
        assertTrue(nodeA.isLeader());

        assertTrue(nodeA.submitJointConfigurationChange(List.of(a, b, d)));
        clientA.flush();
        nodeA.heartbeatTickForTest();
        clientA.flush();

        assertEquals(2, storeA.lastIndex());
        assertFalse(nodeA.getConfigurationAtIndexForTest(1).isJointConsensus());
        assertTrue(nodeA.getConfigurationAtIndexForTest(2).isJointConsensus());

        nodeA.heartbeatTickForTest();
        clientA.flush();
        assertTrue(nodeA.submitFinalizeConfigurationChange());
        clientA.flush();
        nodeA.heartbeatTickForTest();
        clientA.flush();

        assertEquals(3, storeA.lastIndex());
        assertTrue(nodeA.getConfigurationAtIndexForTest(2).isJointConsensus());
        assertFalse(nodeA.getConfigurationAtIndexForTest(3).isJointConsensus());
        assertTrue(nodeA.getConfigurationAtIndexForTest(3).contains("D"));
        assertFalse(nodeA.getConfigurationAtIndexForTest(3).contains("C"));
    }

    @Test
    void removedNodeDoesNotStartNewElectionAfterFinalizedRemoval() {
        announce("Removed node election suppression: finalized removed node does not start new election");
        RaftNodeElectionTest.MutableTime time = new RaftNodeElectionTest.MutableTime(0);

        Peer a = peer("A");
        Peer b = peer("B");
        Peer c = peer("C");
        Peer d = peer("D");

        Map<String, RaftNode> nodes = new HashMap<>();

        RaftNodeElectionTest.QueuedRaftClient clientA = new RaftNodeElectionTest.QueuedRaftClient("A", nodes);
        RaftNodeElectionTest.QueuedRaftClient clientB = new RaftNodeElectionTest.QueuedRaftClient("B", nodes);
        RaftNodeElectionTest.QueuedRaftClient clientC = new RaftNodeElectionTest.QueuedRaftClient("C", nodes);
        RaftNodeElectionTest.QueuedRaftClient clientD = new RaftNodeElectionTest.QueuedRaftClient("D", nodes);

        RaftNode nodeA = newTestNode(a, List.of(b, c), clientA, new InMemoryLogStore(), time, 1);
        RaftNode nodeB = newTestNode(b, List.of(a, c), clientB, new InMemoryLogStore(), time, 2);
        RaftNode nodeC = newTestNode(c, List.of(a, b), clientC, new InMemoryLogStore(), time, 3);
        RaftNode nodeD = newTestNode(d, List.of(a, b, c), clientD, new InMemoryLogStore(), time, 4);

        nodes.put("A", nodeA);
        nodes.put("B", nodeB);
        nodes.put("C", nodeC);
        nodes.put("D", nodeD);

        nodeA.setLastHeartbeatMillisForTest(0);
        time.set(10_000);
        nodeA.electionTickForTest();
        clientA.flush();
        assertTrue(nodeA.isLeader());

        assertTrue(nodeA.submitJointConfigurationChange(List.of(b, c, d)));
        clientA.flush();
        nodeA.heartbeatTickForTest();
        clientA.flush();
        nodeA.heartbeatTickForTest();
        clientA.flush();

        assertTrue(nodeA.submitFinalizeConfigurationChange());
        clientA.flush();
        nodeA.heartbeatTickForTest();
        clientA.flush();

        assertFalse(nodeA.getClusterConfigurationForTest().contains("A"));
        assertFalse(nodeA.isLeader());

        nodeA.setLastHeartbeatMillisForTest(0);
        time.set(20_000);
        nodeA.electionTickForTest();

        assertEquals(RaftNode.State.FOLLOWER, nodeA.getStateForTest());
    }

    @Test
    void survivingNodesRejectRemovedNodeAsCandidateAndLeader() {
        announce("Removed node survivor rejection: surviving nodes reject removed node as candidate and leader");
        RaftNodeElectionTest.MutableTime time = new RaftNodeElectionTest.MutableTime(0);

        Peer a = peer("A");
        Peer b = peer("B");
        Peer c = peer("C");
        Peer d = peer("D");

        Map<String, RaftNode> nodes = new HashMap<>();

        RaftNodeElectionTest.QueuedRaftClient clientA = new RaftNodeElectionTest.QueuedRaftClient("A", nodes);
        RaftNodeElectionTest.QueuedRaftClient clientB = new RaftNodeElectionTest.QueuedRaftClient("B", nodes);
        RaftNodeElectionTest.QueuedRaftClient clientC = new RaftNodeElectionTest.QueuedRaftClient("C", nodes);
        RaftNodeElectionTest.QueuedRaftClient clientD = new RaftNodeElectionTest.QueuedRaftClient("D", nodes);

        RaftNode nodeA = newTestNode(a, List.of(b, c), clientA, new InMemoryLogStore(), time, 1);
        RaftNode nodeB = newTestNode(b, List.of(a, c), clientB, new InMemoryLogStore(), time, 2);
        RaftNode nodeC = newTestNode(c, List.of(a, b), clientC, new InMemoryLogStore(), time, 3);
        RaftNode nodeD = newTestNode(d, List.of(a, b, c), clientD, new InMemoryLogStore(), time, 4);

        nodes.put("A", nodeA);
        nodes.put("B", nodeB);
        nodes.put("C", nodeC);
        nodes.put("D", nodeD);

        nodeA.setLastHeartbeatMillisForTest(0);
        time.set(10_000);
        nodeA.electionTickForTest();
        clientA.flush();
        assertTrue(nodeA.isLeader());

        assertTrue(nodeA.submitJointConfigurationChange(List.of(b, c, d)));
        clientA.flush();
        nodeA.heartbeatTickForTest();
        clientA.flush();
        nodeA.heartbeatTickForTest();
        clientA.flush();

        assertTrue(nodeA.submitFinalizeConfigurationChange());
        clientA.flush();
        nodeA.heartbeatTickForTest();
        clientA.flush();

        assertFalse(nodeB.getClusterConfigurationForTest().contains("A"));
        assertFalse(nodeC.getClusterConfigurationForTest().contains("A"));

        VoteRequest removedCandidate = new VoteRequest(nodeA.getTerm() + 1, "A", nodeA.getCommitIndexForTest(), nodeA.getTerm());
        VoteResponse voteFromB = nodeB.handleVoteRequest(removedCandidate);
        VoteResponse voteFromC = nodeC.handleVoteRequest(removedCandidate);

        assertFalse(voteFromB.isVoteGranted());
        assertFalse(voteFromC.isVoteGranted());

        var appendResponse = nodeB.handleAppendEntries(new org.gautelis.raft.protocol.AppendEntriesRequest(
                nodeB.getTerm(),
                "A",
                0,
                0,
                nodeB.getCommitIndexForTest(),
                List.of()
        ));
        assertFalse(appendResponse.isSuccess());
    }
}
