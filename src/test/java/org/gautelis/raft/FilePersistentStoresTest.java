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
import org.gautelis.raft.protocol.LogEntry;
import org.gautelis.raft.protocol.Peer;
import org.gautelis.raft.protocol.VoteRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FilePersistentStoresTest {
    private static final Logger log = LoggerFactory.getLogger(FilePersistentStoresTest.class);
    private static void announce(String message) {
        System.out.println("*** Testcase *** " + message);
    }

    static class NoopRaftClient extends RaftClient {
        NoopRaftClient() {
            super("test", null);
        }

        @Override
        public void shutdown() {
            // no-op
        }
    }

    private static Peer peer(String id) {
        return new Peer(id, null);
    }

    @Test
    void persistentStateStoreSurvivesReload(@TempDir Path tmp) {
        log.info("*** Testcase *** Persistent state reload: verifies term and votedFor survive FilePersistentStateStore restart");
        Path stateFile = tmp.resolve("state.properties");

        FilePersistentStateStore store = new FilePersistentStateStore(stateFile);
        store.setCurrentTerm(7);
        store.setVotedFor("B");

        FilePersistentStateStore reloaded = new FilePersistentStateStore(stateFile);
        assertEquals(7, reloaded.currentTerm());
        assertEquals("B", reloaded.votedFor().orElseThrow());
    }

    @Test
    void fileLogStoreSurvivesReload(@TempDir Path tmp) {
        log.info("*** Testcase *** File log reload: verifies appended entries and terms survive FileLogStore restart");
        Path logFile = tmp.resolve("log.bin");

        FileLogStore store = new FileLogStore(logFile);
        store.append(List.of(
                new LogEntry(1, "A", "one".getBytes(java.nio.charset.StandardCharsets.UTF_8)),
                new LogEntry(2, "A", "two".getBytes(java.nio.charset.StandardCharsets.UTF_8))
        ));

        FileLogStore reloaded = new FileLogStore(logFile);
        assertEquals(2, reloaded.lastIndex());
        assertEquals(2, reloaded.lastTerm());
        assertEquals(1, reloaded.termAt(1));
        assertEquals(2, reloaded.termAt(2));
    }

    @Test
    void fileLogStoreCompactionMetadataSurvivesReload(@TempDir Path tmp) {
        log.info("*** Testcase *** Compaction metadata reload: verifies snapshot index/term metadata survives FileLogStore restart");
        Path logFile = tmp.resolve("log.bin");

        FileLogStore store = new FileLogStore(logFile);
        store.append(List.of(
                new LogEntry(1, "A", "one".getBytes(java.nio.charset.StandardCharsets.UTF_8)),
                new LogEntry(2, "A", "two".getBytes(java.nio.charset.StandardCharsets.UTF_8)),
                new LogEntry(3, "A", "three".getBytes(java.nio.charset.StandardCharsets.UTF_8))
        ));
        store.compactUpTo(2);

        FileLogStore reloaded = new FileLogStore(logFile);
        assertEquals(2, reloaded.snapshotIndex());
        assertEquals(2, reloaded.snapshotTerm());
        assertEquals(3, reloaded.lastIndex());
        assertEquals(3, reloaded.lastTerm());
        assertEquals(3, reloaded.termAt(3));
    }

    @Test
    void raftNodeLoadsTermVoteAndLogAfterRestart(@TempDir Path tmp) {
        log.info("*** Testcase *** Node restart continuity: verifies RaftNode restores persisted term, vote, and log entries after restart");
        Path stateFile = tmp.resolve("node.state");
        Path logFile = tmp.resolve("node.log");

        Peer a = peer("A");
        Peer b = peer("B");

        FilePersistentStateStore persistentState = new FilePersistentStateStore(stateFile);
        FileLogStore logStore = new FileLogStore(logFile);
        RaftNode first = new RaftNode(
                b,
                List.of(a),
                100,
                null,
                new NoopRaftClient(),
                logStore,
                persistentState,
                System::currentTimeMillis,
                new Random(1)
        );

        VoteRequest voteRequest = new VoteRequest(5, "A", 0, 0);
        assertTrue(first.handleVoteRequest(voteRequest).isVoteGranted());
        first.handleAppendEntries(new AppendEntriesRequest(
                5,
                "A",
                0,
                0,
                1,
                List.of(new LogEntry(5, "A", "cmd".getBytes(java.nio.charset.StandardCharsets.UTF_8)))
        ));

        FilePersistentStateStore reloadedState = new FilePersistentStateStore(stateFile);
        FileLogStore reloadedLog = new FileLogStore(logFile);
        RaftNode restarted = new RaftNode(
                b,
                List.of(a),
                100,
                null,
                new NoopRaftClient(),
                reloadedLog,
                reloadedState,
                System::currentTimeMillis,
                new Random(1)
        );

        assertEquals(5, restarted.getTerm());
        assertEquals(1, reloadedLog.lastIndex());
        assertEquals(5, reloadedLog.lastTerm());
    }

    @Test
    void raftNodeRestoresJointConfigurationAfterRestart(@TempDir Path tmp) {
        announce("Node restart joint configuration: restores replicated joint config from persisted log");
        Path stateFile = tmp.resolve("node.state");
        Path logFile = tmp.resolve("node.log");

        Peer a = peer("A");
        Peer b = peer("B");
        Peer c = peer("C");
        Peer d = new Peer("D", null);

        FilePersistentStateStore persistentState = new FilePersistentStateStore(stateFile);
        FileLogStore logStore = new FileLogStore(logFile);
        RaftNode first = new RaftNode(
                a,
                List.of(b, c),
                100,
                null,
                new NoopRaftClient(),
                logStore,
                persistentState,
                System::currentTimeMillis,
                new Random(1)
        );

        persistentState.setCurrentTerm(7);
        logStore.append(List.of(
                new LogEntry(7, "A", "noop".getBytes(java.nio.charset.StandardCharsets.UTF_8)),
                new LogEntry(7, "A", ClusterConfigurationCommand.joint(List.of(a, b, d)))
        ));

        FilePersistentStateStore reloadedState = new FilePersistentStateStore(stateFile);
        FileLogStore reloadedLog = new FileLogStore(logFile);
        RaftNode restarted = new RaftNode(
                a,
                List.of(b, c, d),
                100,
                null,
                new NoopRaftClient(),
                reloadedLog,
                reloadedState,
                System::currentTimeMillis,
                new Random(1)
        );

        assertTrue(restarted.getClusterConfigurationForTest().isJointConsensus());
        assertTrue(restarted.getClusterConfigurationForTest().contains("D"));
        assertTrue(restarted.getClusterConfigurationForTest().contains("C"));
    }

    @Test
    void restartedNodeUsesFinalizedConfigurationForElections(@TempDir Path tmp) {
        announce("Node restart finalized configuration: surviving members elect leader using persisted finalized config");
        Path stateB = tmp.resolve("b.state");
        Path logB = tmp.resolve("b.log");
        Path stateC = tmp.resolve("c.state");
        Path logC = tmp.resolve("c.log");

        Peer a = peer("A");
        Peer b = peer("B");
        Peer c = peer("C");

        FilePersistentStateStore persistentStateB = new FilePersistentStateStore(stateB);
        FilePersistentStateStore persistentStateC = new FilePersistentStateStore(stateC);
        FileLogStore logStoreB = new FileLogStore(logB);
        FileLogStore logStoreC = new FileLogStore(logC);

        byte[] finalize = ClusterConfigurationCommand.finalizeTransition();
        logStoreB.append(List.of(
                new LogEntry(7, "A", new byte[0]),
                new LogEntry(7, "A", ClusterConfigurationCommand.joint(List.of(b, c))),
                new LogEntry(7, "A", finalize)
        ));
        logStoreC.append(List.of(
                new LogEntry(7, "A", new byte[0]),
                new LogEntry(7, "A", ClusterConfigurationCommand.joint(List.of(b, c))),
                new LogEntry(7, "A", finalize)
        ));
        persistentStateB.setCurrentTerm(7);
        persistentStateC.setCurrentTerm(7);

        RaftNodeElectionTest.MutableTime time = new RaftNodeElectionTest.MutableTime(0);
        java.util.Map<String, RaftNode> nodes = new java.util.HashMap<>();
        RaftNodeElectionTest.QueuedRaftClient clientB = new RaftNodeElectionTest.QueuedRaftClient("B", nodes);
        RaftNodeElectionTest.QueuedRaftClient clientC = new RaftNodeElectionTest.QueuedRaftClient("C", nodes);

        RaftNode restartedB = new RaftNode(
                b,
                List.of(a, c),
                100,
                null,
                clientB,
                new FileLogStore(logB),
                new FilePersistentStateStore(stateB),
                time,
                new Random(2)
        );
        RaftNode restartedC = new RaftNode(
                c,
                List.of(a, b),
                100,
                null,
                clientC,
                new FileLogStore(logC),
                new FilePersistentStateStore(stateC),
                time,
                new Random(3)
        );

        nodes.put("B", restartedB);
        nodes.put("C", restartedC);

        assertFalse(restartedB.getClusterConfigurationForTest().isJointConsensus());
        assertFalse(restartedB.getClusterConfigurationForTest().contains("A"));
        assertTrue(restartedB.getClusterConfigurationForTest().contains("B"));
        assertTrue(restartedB.getClusterConfigurationForTest().contains("C"));

        restartedB.setLastHeartbeatMillisForTest(0);
        time.set(10_000);
        restartedB.electionTickForTest();
        clientB.flush();

        assertTrue(restartedB.isLeader());
        assertEquals(8, restartedB.getTerm());
        assertEquals(8, restartedC.getTerm());
    }

    @Test
    void removedNodeRestartsAsDecommissioned(@TempDir Path tmp) {
        announce("Node restart removed member: restarted removed node remains decommissioned");
        Path stateFile = tmp.resolve("removed.state");
        Path logFile = tmp.resolve("removed.log");

        Peer a = peer("A");
        Peer b = peer("B");
        Peer c = peer("C");

        FilePersistentStateStore persistentState = new FilePersistentStateStore(stateFile);
        FileLogStore logStore = new FileLogStore(logFile);
        logStore.append(List.of(
                new LogEntry(9, "A", new byte[0]),
                new LogEntry(9, "A", ClusterConfigurationCommand.joint(List.of(b, c))),
                new LogEntry(9, "A", ClusterConfigurationCommand.finalizeTransition())
        ));
        persistentState.setCurrentTerm(9);

        RaftNode restarted = new RaftNode(
                a,
                List.of(b, c),
                100,
                null,
                new NoopRaftClient(),
                new FileLogStore(logFile),
                new FilePersistentStateStore(stateFile),
                System::currentTimeMillis,
                new Random(1)
        );

        assertTrue(restarted.isDecommissionedForTest());
        assertFalse(restarted.getClusterConfigurationForTest().contains("A"));
        assertEquals(RaftNode.State.FOLLOWER, restarted.getStateForTest());
    }

    @Test
    void finalizedConfigurationSnapshotSurvivesRestart(@TempDir Path tmp) {
        announce("Node restart finalized snapshot: finalized membership survives snapshot-backed restart");
        Path stateFile = tmp.resolve("survivor.state");
        Path logFile = tmp.resolve("survivor.log");

        Peer a = peer("A");
        Peer b = peer("B");
        Peer c = peer("C");

        FilePersistentStateStore persistentState = new FilePersistentStateStore(stateFile);
        FileLogStore logStore = new FileLogStore(logFile);
        ClusterConfiguration finalized = ClusterConfiguration
                .stable(List.of(a, b, c))
                .transitionTo(List.of(b, c))
                .finalizeTransition();

        logStore.installSnapshot(
                12,
                9,
                ClusterConfigurationSnapshotCodec.encode(finalized, "snapshot-final".getBytes(java.nio.charset.StandardCharsets.UTF_8))
        );
        persistentState.setCurrentTerm(9);

        RaftNode restarted = new RaftNode(
                b,
                List.of(a, c),
                100,
                null,
                new NoopRaftClient(),
                new FileLogStore(logFile),
                new FilePersistentStateStore(stateFile),
                System::currentTimeMillis,
                new Random(2)
        );

        assertFalse(restarted.getClusterConfigurationForTest().isJointConsensus());
        assertFalse(restarted.getClusterConfigurationForTest().contains("A"));
        assertTrue(restarted.getClusterConfigurationForTest().contains("B"));
        assertTrue(restarted.getClusterConfigurationForTest().contains("C"));
        assertFalse(restarted.isDecommissionedForTest());
    }
}
