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

import org.gautelis.raft.protocol.VoteRequest;
import org.gautelis.raft.protocol.VoteResponse;
import org.gautelis.raft.storage.*;
import org.gautelis.raft.statemachine.*;
import org.gautelis.raft.transport.netty.*;
import org.gautelis.raft.serialization.ProtoMapper;

import org.gautelis.raft.protocol.AppendEntriesRequest;
import org.gautelis.raft.protocol.LogEntry;
import org.gautelis.raft.protocol.Peer;
import org.gautelis.raft.protocol.StateMachineCommand;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RaftCompactionPolicyTest {
    private static final Logger log = LoggerFactory.getLogger(RaftCompactionPolicyTest.class);
    private static void announce(String message) {
        System.out.println("*** Testcase *** " + message);
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

    static class SnapshottingStateMachine implements SnapshotStateMachine {
        private final List<String> applied = new ArrayList<>();

        @Override
        public void apply(long term, byte[] command) {
            applied.add(term + ":" + StateMachineCommand.decode(command).map(Object::toString).orElse("invalid"));
        }

        @Override
        public byte[] snapshot() {
            return String.join("|", applied).getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public void restore(byte[] snapshotData) {
            applied.clear();
        }
    }

    private static RaftNode newTestNode(
            Peer me,
            List<Peer> peers,
            SnapshotStateMachine stateMachine,
            RaftClient raftClient,
            LogStore logStore,
            RaftNode.TimeSource timeSource,
            int randomSeed
    ) {
        return TestRaftNodeBuilder.forPeer(me)
                .withPeers(peers)
                .withTimeoutMillis(100)
                .withStateMachine(stateMachine)
                .withClient(raftClient)
                .withLogStore(logStore)
                .withPersistentStateStore(new InMemoryPersistentStateStore())
                .withTimeSource(timeSource)
                .withRandom(new Random(randomSeed))
                .build();
    }

    @Test
    void compactionPolicyCreatesSnapshotWhenThresholdReached() {
        log.info("*** Testcase *** Automatic compaction policy: verifies reaching snapshot threshold creates local snapshot metadata and payload");
        String prev = System.getProperty("raft.snapshot.min.entries");
        System.setProperty("raft.snapshot.min.entries", "1");

        try {
            Peer a = new Peer("A", null);
            Peer b = new Peer("B", null);
            InMemoryLogStore store = new InMemoryLogStore();
            SnapshottingStateMachine sm = new SnapshottingStateMachine();
            RaftNode nodeB = newTestNode(b, List.of(a), sm, new NoopRaftClient(), store, System::currentTimeMillis, 1);

            // Append one command and commit it
            nodeB.handleAppendEntries(new AppendEntriesRequest(
                    2,
                    "A",
                    0,
                    0,
                    1,
                    List.of(new LogEntry(2, "A", StateMachineCommand.put("x", "10").encode()))
            ));

            assertEquals(1, store.snapshotIndex());
            assertEquals(2, store.snapshotTerm());
            assertFalse(store.snapshotData().length == 0);
            var decoded = ClusterConfigurationSnapshotCodec.decode(store.snapshotData());
            assertTrue(decoded.isPresent());
            assertTrue(new String(decoded.get().stateMachineSnapshot(), StandardCharsets.UTF_8).contains("PUT(x=10)"));
        } finally {
            if (prev == null) {
                System.clearProperty("raft.snapshot.min.entries");
            } else {
                System.setProperty("raft.snapshot.min.entries", prev);
            }
        }
    }

    @Test
    void snapshotCarriesClusterConfigurationMetadata() {
        announce("Compaction snapshot metadata: local snapshot carries cluster configuration");
        String prev = System.getProperty("raft.snapshot.min.entries");
        System.setProperty("raft.snapshot.min.entries", "1");

        try {
            Peer a = new Peer("A", null);
            Peer b = new Peer("B", null);
            Peer c = new Peer("C", null);
            Peer d = new Peer("D", null);

            RaftNodeElectionTest.MutableTime time = new RaftNodeElectionTest.MutableTime(0);
            Map<String, RaftNode> nodes = new HashMap<>();

            RaftNodeElectionTest.QueuedRaftClient clientA = new RaftNodeElectionTest.QueuedRaftClient("A", nodes);
            RaftNodeElectionTest.QueuedRaftClient clientB = new RaftNodeElectionTest.QueuedRaftClient("B", nodes);
            RaftNodeElectionTest.QueuedRaftClient clientC = new RaftNodeElectionTest.QueuedRaftClient("C", nodes);

            InMemoryLogStore storeA = new InMemoryLogStore();
            SnapshottingStateMachine sm = new SnapshottingStateMachine();

            RaftNode nodeA = newTestNode(a, List.of(b, c), sm, clientA, storeA, time, 1);
            RaftNode nodeB = newTestNode(b, List.of(a, c), new SnapshottingStateMachine(), clientB, new InMemoryLogStore(), time, 2);
            RaftNode nodeC = newTestNode(c, List.of(a, b), new SnapshottingStateMachine(), clientC, new InMemoryLogStore(), time, 3);

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

            var decoded = ClusterConfigurationSnapshotCodec.decode(storeA.snapshotData());
            assertTrue(decoded.isPresent());
            assertTrue(decoded.get().configuration().isJointConsensus());
            assertTrue(decoded.get().configuration().contains("D"));
        } finally {
            if (prev == null) {
                System.clearProperty("raft.snapshot.min.entries");
            } else {
                System.setProperty("raft.snapshot.min.entries", prev);
            }
        }
    }

    @Test
    void finalizedConfigurationSurvivesCompactionSnapshot() {
        announce("Compaction finalized configuration: finalized membership survives compaction snapshot");
        String prev = System.getProperty("raft.snapshot.min.entries");
        System.setProperty("raft.snapshot.min.entries", "1");

        try {
            Peer a = new Peer("A", null);
            Peer b = new Peer("B", null);
            Peer c = new Peer("C", null);
            Peer d = new Peer("D", null);

            RaftNodeElectionTest.MutableTime time = new RaftNodeElectionTest.MutableTime(0);
            Map<String, RaftNode> nodes = new HashMap<>();

            RaftNodeElectionTest.QueuedRaftClient clientA = new RaftNodeElectionTest.QueuedRaftClient("A", nodes);
            RaftNodeElectionTest.QueuedRaftClient clientB = new RaftNodeElectionTest.QueuedRaftClient("B", nodes);
            RaftNodeElectionTest.QueuedRaftClient clientC = new RaftNodeElectionTest.QueuedRaftClient("C", nodes);
            RaftNodeElectionTest.QueuedRaftClient clientD = new RaftNodeElectionTest.QueuedRaftClient("D", nodes);

            InMemoryLogStore storeA = new InMemoryLogStore();
            SnapshottingStateMachine sm = new SnapshottingStateMachine();

            RaftNode nodeA = newTestNode(a, List.of(b, c), sm, clientA, storeA, time, 1);
            RaftNode nodeB = newTestNode(b, List.of(a, c), new SnapshottingStateMachine(), clientB, new InMemoryLogStore(), time, 2);
            RaftNode nodeC = newTestNode(c, List.of(a, b), new SnapshottingStateMachine(), clientC, new InMemoryLogStore(), time, 3);
            RaftNode nodeD = newTestNode(d, List.of(a, b), new SnapshottingStateMachine(), clientD, new InMemoryLogStore(), time, 4);

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

            var decoded = ClusterConfigurationSnapshotCodec.decode(storeA.snapshotData());
            assertTrue(decoded.isPresent());
            assertFalse(decoded.get().configuration().isJointConsensus());
            assertFalse(decoded.get().configuration().contains("A"));
            assertTrue(decoded.get().configuration().contains("D"));
        } finally {
            if (prev == null) {
                System.clearProperty("raft.snapshot.min.entries");
            } else {
                System.setProperty("raft.snapshot.min.entries", prev);
            }
        }
    }
}
