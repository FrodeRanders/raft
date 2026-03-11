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

import org.gautelis.raft.protocol.AdminCommand;
import org.gautelis.raft.protocol.ClusterMessage;
import org.gautelis.raft.protocol.Peer;
import org.gautelis.raft.bootstrap.BasicAdapter;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

class BasicAdapterCommandTest {
    private static final Logger log = LoggerFactory.getLogger(BasicAdapterCommandTest.class);
    private static void announce(String message) {
        System.out.println("*** Testcase *** " + message);
    }

    static final class MutableTime implements RaftNode.TimeSource {
        private long now;
        MutableTime(long now) { this.now = now; }
        @Override
        public long nowMillis() { return now; }
        void set(long now) { this.now = now; }
    }

    static class NoopRaftClient extends RaftClient {
        NoopRaftClient() {
            super("test", null);
        }

        @Override
        public void shutdown() {
            // no-op in tests
        }
    }

    static class TestAdapter extends BasicAdapter {
        TestAdapter(Peer me, List<Peer> peers) {
            super(100, me, peers);
        }

        void bind(RaftNode node) {
            this.stateMachine = node;
        }
    }

    private static Peer peer(String id) {
        return new Peer(id, null);
    }

    @Test
    void leaderAcceptsValidClusterCommandAndAppliesThroughLog() {
        log.info("*** Testcase *** Leader command ingestion: verifies valid ClusterMessage commands are submitted through Raft log and applied to state machine");
        Peer me = peer("A");
        MutableTime time = new MutableTime(0);
        KeyValueStateMachine kv = new KeyValueStateMachine();
        InMemoryLogStore store = new InMemoryLogStore();
        RaftNode node = new RaftNode(
                me,
                List.of(),
                100,
                null,
                kv,
                new NoopRaftClient(),
                store,
                new InMemoryPersistentStateStore(),
                time,
                new Random(1)
        );

        node.setLastHeartbeatMillisForTest(0);
        time.set(10_000);
        node.electionTickForTest();
        node.handleVoteResponsesForTest(List.of(), 1); // single-node election => leader

        TestAdapter adapter = new TestAdapter(me, List.of());
        adapter.bind(node);

        ClusterMessage message = new ClusterMessage(node.getTerm(), "client", "set x 42");
        byte[] payload = ProtoMapper.toProto(message).toByteString().toByteArray();
        adapter.handleMessage("corr-1", "ClusterMessage", payload, null);

        assertEquals("42", kv.get("x"));
        assertEquals(2, store.lastIndex());
    }

    @Test
    void invalidClusterCommandIsRejected() {
        log.info("*** Testcase *** Command validation reject path: verifies malformed ClusterMessage commands are rejected without log mutation");
        Peer me = peer("A");
        MutableTime time = new MutableTime(0);
        KeyValueStateMachine kv = new KeyValueStateMachine();
        InMemoryLogStore store = new InMemoryLogStore();
        RaftNode node = new RaftNode(
                me,
                List.of(),
                100,
                null,
                kv,
                new NoopRaftClient(),
                store,
                new InMemoryPersistentStateStore(),
                time,
                new Random(1)
        );

        node.setLastHeartbeatMillisForTest(0);
        time.set(10_000);
        node.electionTickForTest();
        node.handleVoteResponsesForTest(List.of(), 1); // single-node election => leader

        TestAdapter adapter = new TestAdapter(me, List.of());
        adapter.bind(node);

        ClusterMessage message = new ClusterMessage(node.getTerm(), "client", "bogus cmd");
        byte[] payload = ProtoMapper.toProto(message).toByteString().toByteArray();
        adapter.handleMessage("corr-2", "ClusterMessage", payload, null);

        assertNull(kv.get("x"));
        assertEquals(1, store.lastIndex());
    }

    @Test
    void followerRejectsValidClusterCommand() {
        log.info("*** Testcase *** Follower command rejection: verifies non-leader nodes reject otherwise valid ClusterMessage commands");
        Peer me = peer("A");
        KeyValueStateMachine kv = new KeyValueStateMachine();
        InMemoryLogStore store = new InMemoryLogStore();
        RaftNode node = new RaftNode(
                me,
                List.of(),
                100,
                null,
                kv,
                new NoopRaftClient(),
                store,
                new InMemoryPersistentStateStore(),
                System::currentTimeMillis,
                new Random(1)
        );

        TestAdapter adapter = new TestAdapter(me, List.of());
        adapter.bind(node);

        ClusterMessage message = new ClusterMessage(node.getTerm(), "client", "set y 7");
        byte[] payload = ProtoMapper.toProto(message).toByteString().toByteArray();
        adapter.handleMessage("corr-3", "ClusterMessage", payload, null);

        assertNull(kv.get("y"));
        assertEquals(0, store.lastIndex());
    }

    @Test
    void leaderAcceptsJointConfigurationCommandThroughAdapter() {
        announce("Adapter admin command joint config: leader accepts joint configuration through AdminCommand");
        Peer a = new Peer("A", null);
        Peer b = new Peer("B", null);
        Peer c = new Peer("C", null);
        Peer d = new Peer("D", new java.net.InetSocketAddress("127.0.0.1", 10083));

        RaftNodeElectionTest.MutableTime time = new RaftNodeElectionTest.MutableTime(0);
        Map<String, RaftNode> nodes = new HashMap<>();

        RaftNodeElectionTest.QueuedRaftClient clientA = new RaftNodeElectionTest.QueuedRaftClient("A", nodes);
        RaftNodeElectionTest.QueuedRaftClient clientB = new RaftNodeElectionTest.QueuedRaftClient("B", nodes);
        RaftNodeElectionTest.QueuedRaftClient clientC = new RaftNodeElectionTest.QueuedRaftClient("C", nodes);
        RaftNodeElectionTest.QueuedRaftClient clientD = new RaftNodeElectionTest.QueuedRaftClient("D", nodes);

        InMemoryLogStore storeA = new InMemoryLogStore();
        InMemoryLogStore storeB = new InMemoryLogStore();
        InMemoryLogStore storeC = new InMemoryLogStore();
        InMemoryLogStore storeD = new InMemoryLogStore();

        RaftNode nodeA = new RaftNode(a, List.of(b, c), 100, null, new KeyValueStateMachine(), clientA, storeA, new InMemoryPersistentStateStore(), time, new Random(1));
        RaftNode nodeB = new RaftNode(b, List.of(a, c), 100, null, new KeyValueStateMachine(), clientB, storeB, new InMemoryPersistentStateStore(), time, new Random(2));
        RaftNode nodeC = new RaftNode(c, List.of(a, b), 100, null, new KeyValueStateMachine(), clientC, storeC, new InMemoryPersistentStateStore(), time, new Random(3));
        RaftNode nodeD = new RaftNode(d, List.of(a, b, c), 100, null, new KeyValueStateMachine(), clientD, storeD, new InMemoryPersistentStateStore(), time, new Random(4));

        nodes.put("A", nodeA);
        nodes.put("B", nodeB);
        nodes.put("C", nodeC);
        nodes.put("D", nodeD);

        nodeA.setLastHeartbeatMillisForTest(0);
        time.set(10_000);
        nodeA.electionTickForTest();
        clientA.flush();
        assertTrue(nodeA.isLeader());

        TestAdapter adapter = new TestAdapter(a, List.of(b, c));
        adapter.bind(nodeA);

        AdminCommand message = new AdminCommand(nodeA.getTerm(), "client", "config joint A,B,D@127.0.0.1:10083");
        byte[] payload = ProtoMapper.toProto(message).toByteString().toByteArray();
        adapter.handleMessage("corr-config-1", "AdminCommand", payload, null);

        clientA.flush();
        nodeA.heartbeatTickForTest();
        clientA.flush();

        assertTrue(nodeA.getClusterConfigurationForTest().isJointConsensus());
        assertTrue(nodeA.getClusterConfigurationForTest().contains("D"));
    }

    @Test
    void clusterMessageCannotSubmitConfigurationCommand() {
        Peer me = peer("A");
        MutableTime time = new MutableTime(0);
        KeyValueStateMachine kv = new KeyValueStateMachine();
        InMemoryLogStore store = new InMemoryLogStore();
        RaftNode node = new RaftNode(
                me,
                List.of(),
                100,
                null,
                kv,
                new NoopRaftClient(),
                store,
                new InMemoryPersistentStateStore(),
                time,
                new Random(1)
        );

        node.setLastHeartbeatMillisForTest(0);
        time.set(10_000);
        node.electionTickForTest();
        node.handleVoteResponsesForTest(List.of(), 1);

        TestAdapter adapter = new TestAdapter(me, List.of());
        adapter.bind(node);

        ClusterMessage message = new ClusterMessage(node.getTerm(), "client", "config finalize");
        byte[] payload = ProtoMapper.toProto(message).toByteString().toByteArray();
        adapter.handleMessage("corr-config-cluster", "ClusterMessage", payload, null);

        assertEquals(1, store.lastIndex(), "only leader no-op should exist");
        assertFalse(node.getClusterConfigurationForTest().isJointConsensus());
    }

    @Test
    void malformedAdminPeerSpecificationsAreRejectedWithoutThrowing() {
        announce("Adapter admin command validation: malformed peer specifications are rejected without exceptions");
        Peer me = peer("A");
        MutableTime time = new MutableTime(0);
        InMemoryLogStore store = new InMemoryLogStore();
        RaftNode node = new RaftNode(
                me,
                List.of(),
                100,
                null,
                new KeyValueStateMachine(),
                new NoopRaftClient(),
                store,
                new InMemoryPersistentStateStore(),
                time,
                new Random(1)
        );

        node.setLastHeartbeatMillisForTest(0);
        time.set(10_000);
        node.electionTickForTest();
        node.handleVoteResponsesForTest(List.of(), 1);

        TestAdapter adapter = new TestAdapter(me, List.of());
        adapter.bind(node);

        long lastIndexBefore = store.lastIndex();

        byte[] unknownPeerPayload = ProtoMapper.toProto(new AdminCommand(node.getTerm(), "client", "config joint Z"))
                .toByteString().toByteArray();
        assertDoesNotThrow(() -> adapter.handleMessage("corr-admin-invalid-1", "AdminCommand", unknownPeerPayload, null));

        byte[] malformedAddressPayload = ProtoMapper.toProto(new AdminCommand(node.getTerm(), "client", "config joint D@127.0.0.1"))
                .toByteString().toByteArray();
        assertDoesNotThrow(() -> adapter.handleMessage("corr-admin-invalid-2", "AdminCommand", malformedAddressPayload, null));

        assertEquals(lastIndexBefore, store.lastIndex());
        assertFalse(node.getClusterConfigurationForTest().isJointConsensus());
    }

    @Test
    void leaderStepsDownWhenFinalizedConfigurationRemovesIt() {
        announce("Adapter admin finalize removal: leader steps down and rejects further commands after removal");
        Peer a = new Peer("A", null);
        Peer b = new Peer("B", null);
        Peer c = new Peer("C", null);
        Peer d = new Peer("D", new java.net.InetSocketAddress("127.0.0.1", 10083));

        RaftNodeElectionTest.MutableTime time = new RaftNodeElectionTest.MutableTime(0);
        Map<String, RaftNode> nodes = new HashMap<>();

        RaftNodeElectionTest.QueuedRaftClient clientA = new RaftNodeElectionTest.QueuedRaftClient("A", nodes);
        RaftNodeElectionTest.QueuedRaftClient clientB = new RaftNodeElectionTest.QueuedRaftClient("B", nodes);
        RaftNodeElectionTest.QueuedRaftClient clientC = new RaftNodeElectionTest.QueuedRaftClient("C", nodes);
        RaftNodeElectionTest.QueuedRaftClient clientD = new RaftNodeElectionTest.QueuedRaftClient("D", nodes);

        InMemoryLogStore storeA = new InMemoryLogStore();
        InMemoryLogStore storeB = new InMemoryLogStore();
        InMemoryLogStore storeC = new InMemoryLogStore();
        InMemoryLogStore storeD = new InMemoryLogStore();

        RaftNode nodeA = new RaftNode(a, List.of(b, c), 100, null, new KeyValueStateMachine(), clientA, storeA, new InMemoryPersistentStateStore(), time, new Random(1));
        RaftNode nodeB = new RaftNode(b, List.of(a, c), 100, null, new KeyValueStateMachine(), clientB, storeB, new InMemoryPersistentStateStore(), time, new Random(2));
        RaftNode nodeC = new RaftNode(c, List.of(a, b), 100, null, new KeyValueStateMachine(), clientC, storeC, new InMemoryPersistentStateStore(), time, new Random(3));
        RaftNode nodeD = new RaftNode(d, List.of(a, b, c), 100, null, new KeyValueStateMachine(), clientD, storeD, new InMemoryPersistentStateStore(), time, new Random(4));

        nodes.put("A", nodeA);
        nodes.put("B", nodeB);
        nodes.put("C", nodeC);
        nodes.put("D", nodeD);

        nodeA.setLastHeartbeatMillisForTest(0);
        time.set(10_000);
        nodeA.electionTickForTest();
        clientA.flush();
        assertTrue(nodeA.isLeader());

        TestAdapter adapter = new TestAdapter(a, List.of(b, c));
        adapter.bind(nodeA);

        byte[] jointPayload = ProtoMapper.toProto(new AdminCommand(nodeA.getTerm(), "client", "config joint B,C,D@127.0.0.1:10083"))
                .toByteString().toByteArray();
        adapter.handleMessage("corr-config-2", "AdminCommand", jointPayload, null);
        clientA.flush();
        nodeA.heartbeatTickForTest();
        clientA.flush();
        nodeA.heartbeatTickForTest();
        clientA.flush();
        assertTrue(nodeA.getClusterConfigurationForTest().isJointConsensus());

        byte[] finalizePayload = ProtoMapper.toProto(new AdminCommand(nodeA.getTerm(), "client", "config finalize"))
                .toByteString().toByteArray();
        adapter.handleMessage("corr-config-3", "AdminCommand", finalizePayload, null);
        clientA.flush();
        nodeA.heartbeatTickForTest();
        clientA.flush();

        long lastIndexAfterRemoval = storeA.lastIndex();
        assertFalse(nodeA.isLeader());
        assertFalse(nodeA.getClusterConfigurationForTest().contains("A"));
        assertTrue(nodeA.isDecommissionedForTest());

        byte[] clusterPayload = ProtoMapper.toProto(new ClusterMessage(nodeA.getTerm(), "client", "set x 9"))
                .toByteString().toByteArray();
        adapter.handleMessage("corr-config-4", "ClusterMessage", clusterPayload, null);

        byte[] adminPayload = ProtoMapper.toProto(new AdminCommand(nodeA.getTerm(), "client", "config joint B,C,D@127.0.0.1:10083"))
                .toByteString().toByteArray();
        adapter.handleMessage("corr-config-5", "AdminCommand", adminPayload, null);

        assertEquals(lastIndexAfterRemoval, storeA.lastIndex());
    }
}
