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

import org.gautelis.raft.protocol.AppendEntriesRequest;
import org.gautelis.raft.protocol.InstallSnapshotRequest;
import org.gautelis.raft.protocol.LogEntry;
import org.gautelis.raft.protocol.Peer;
import org.gautelis.raft.protocol.StateMachineCommand;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

class RaftSnapshotStateMachineTest {
    private static final Logger log = LoggerFactory.getLogger(RaftSnapshotStateMachineTest.class);
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

    static class CapturingStateMachine implements SnapshotStateMachine {
        private byte[] restored = new byte[0];
        private final List<String> applied = new ArrayList<>();

        @Override
        public void apply(long term, byte[] command) {
            applied.add(term + ":" + StateMachineCommand.decode(command).map(Object::toString).orElse("invalid"));
        }

        @Override
        public byte[] snapshot() {
            return "snapshot-current".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        }

        @Override
        public void restore(byte[] snapshotData) {
            restored = snapshotData == null ? new byte[0] : java.util.Arrays.copyOf(snapshotData, snapshotData.length);
            applied.clear();
        }
    }

    private static Peer peer(String id) {
        return new Peer(id, null);
    }

    private static RaftNode newSnapshotNode(
            Peer me,
            List<Peer> peers,
            SnapshotStateMachine stateMachine,
            RaftNode.TimeSource timeSource,
            int randomSeed
    ) {
        return TestRaftNodeBuilder.forPeer(me)
                .withPeers(peers)
                .withTimeoutMillis(100)
                .withStateMachine(stateMachine)
                .withClient(new NoopRaftClient())
                .withLogStore(new InMemoryLogStore())
                .withPersistentStateStore(new InMemoryPersistentStateStore())
                .withTimeSource(timeSource)
                .withRandom(new Random(randomSeed))
                .build();
    }

    private static RaftNode newSnapshotNode(Peer me, List<Peer> peers, SnapshotStateMachine stateMachine, InMemoryLogStore logStore, RaftNode.TimeSource timeSource, int randomSeed) {
        return TestRaftNodeBuilder.forPeer(me)
                .withPeers(peers)
                .withTimeoutMillis(100)
                .withStateMachine(stateMachine)
                .withClient(new NoopRaftClient())
                .withLogStore(logStore)
                .withPersistentStateStore(new InMemoryPersistentStateStore())
                .withTimeSource(timeSource)
                .withRandom(new Random(randomSeed))
                .build();
    }

    @Test
    void installSnapshotRestoresStateMachineAndSubsequentCommitAppliesCommands() {
        log.info("*** Testcase *** Snapshot restore + post-snapshot apply: verifies InstallSnapshot restores state machine and later committed log entries still apply");
        Peer a = peer("A");
        Peer b = peer("B");

        InMemoryLogStore store = new InMemoryLogStore();
        CapturingStateMachine sm = new CapturingStateMachine();
        RaftNode nodeB = newSnapshotNode(b, List.of(a), sm, store, System::currentTimeMillis, 1);

        InstallSnapshotRequest snapshotRequest = new InstallSnapshotRequest(
                4,
                "A",
                5,
                4,
                "snapshot-5".getBytes(java.nio.charset.StandardCharsets.UTF_8)
        );
        var snapshotResponse = nodeB.handleInstallSnapshot(snapshotRequest);
        assertTrue(snapshotResponse.isSuccess());
        assertArrayEquals("snapshot-5".getBytes(java.nio.charset.StandardCharsets.UTF_8), sm.restored);
        assertEquals(5, nodeB.getCommitIndexForTest());
        assertEquals(5, nodeB.getLastAppliedForTest());

        // Replicate one post-snapshot command and commit it.
        nodeB.handleAppendEntries(new AppendEntriesRequest(
                4,
                "A",
                5,
                4,
                5,
                List.of(new LogEntry(4, "A", StateMachineCommand.put("x", "7").encode()))
        ));
        nodeB.handleAppendEntries(new AppendEntriesRequest(
                4,
                "A",
                6,
                4,
                6,
                List.of()
        ));

        assertEquals(1, sm.applied.size());
        assertEquals("4:" + StateMachineCommand.put("x", "7"), sm.applied.getFirst());
    }

    @Test
    void installSnapshotRestoresJointConfigurationState() {
        announce("InstallSnapshot joint configuration: restores joint membership state from wrapped snapshot");
        Peer a = peer("A");
        Peer b = peer("B");
        Peer c = peer("C");
        Peer d = new Peer("D", new java.net.InetSocketAddress("127.0.0.1", 10083));

        InMemoryLogStore store = new InMemoryLogStore();
        CapturingStateMachine sm = new CapturingStateMachine();
        RaftNode nodeB = newSnapshotNode(b, List.of(a, c), sm, store, System::currentTimeMillis, 1);

        ClusterConfiguration configuration = ClusterConfiguration
                .stable(List.of(a, b, c))
                .transitionTo(List.of(a, b, d));
        byte[] payload = ClusterConfigurationSnapshotCodec.encode(configuration, "snapshot-joint".getBytes(java.nio.charset.StandardCharsets.UTF_8));

        InstallSnapshotRequest snapshotRequest = new InstallSnapshotRequest(
                4,
                "A",
                5,
                4,
                payload
        );
        var snapshotResponse = nodeB.handleInstallSnapshot(snapshotRequest);
        assertTrue(snapshotResponse.isSuccess());
        assertArrayEquals("snapshot-joint".getBytes(java.nio.charset.StandardCharsets.UTF_8), sm.restored);
        assertTrue(nodeB.getClusterConfigurationForTest().isJointConsensus());
        assertTrue(nodeB.getClusterConfigurationForTest().contains("D"));
        assertTrue(nodeB.getClusterConfigurationForTest().contains("C"));
        assertFalse(nodeB.isLeader());
    }

    @Test
    void installSnapshotWithFinalizedRemovalDecommissionsRecipient() {
        announce("InstallSnapshot finalized removal: removed recipient decommissions after final snapshot");
        Peer a = peer("A");
        Peer b = peer("B");
        Peer c = peer("C");

        InMemoryLogStore store = new InMemoryLogStore();
        CapturingStateMachine sm = new CapturingStateMachine();
        RaftNodeElectionTest.MutableTime time = new RaftNodeElectionTest.MutableTime(0);
        RaftNode nodeA = newSnapshotNode(a, List.of(b, c), sm, store, time, 1);
        AtomicInteger decommissionCalls = new AtomicInteger();
        nodeA.setDecommissionListener(decommissionCalls::incrementAndGet);

        ClusterConfiguration configuration = ClusterConfiguration
                .stable(List.of(a, b, c))
                .transitionTo(List.of(b, c))
                .finalizeTransition();
        byte[] payload = ClusterConfigurationSnapshotCodec.encode(configuration, "snapshot-final".getBytes(java.nio.charset.StandardCharsets.UTF_8));

        InstallSnapshotRequest snapshotRequest = new InstallSnapshotRequest(
                4,
                "B",
                5,
                4,
                payload
        );
        var snapshotResponse = nodeA.handleInstallSnapshot(snapshotRequest);
        assertTrue(snapshotResponse.isSuccess());
        assertArrayEquals("snapshot-final".getBytes(java.nio.charset.StandardCharsets.UTF_8), sm.restored);
        assertFalse(nodeA.getClusterConfigurationForTest().contains("A"));
        assertFalse(nodeA.getClusterConfigurationForTest().isJointConsensus());
        assertTrue(nodeA.isDecommissionedForTest());
        assertEquals(1, decommissionCalls.get());

        nodeA.setLastHeartbeatMillisForTest(0);
        time.set(10_000);
        nodeA.electionTickForTest();
        assertEquals(RaftNode.State.FOLLOWER, nodeA.getStateForTest());
    }
}
