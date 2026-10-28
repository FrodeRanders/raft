package org.gautelis.raft;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;
import org.gautelis.raft.protocol.*;
import org.gautelis.raft.storage.InMemoryLogStore;
import org.gautelis.raft.statemachine.SnapshotStateMachine;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class RaftSafetyRegressionTest {
    private static Peer peer(String id) { return new Peer(id, null); }
    private static RaftNode node(InMemoryLogStore store, SnapshotStateMachine sm) {
        return TestRaftNodeBuilder.forPeer(peer("a")).withPeers(List.of(peer("b"), peer("c")))
                .withLogStore(store).withStateMachine(sm).build();
    }
    private static void set(RaftNode node, String name, Object value) throws Exception {
        Field field = RaftNode.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(node, value);
    }
    private static long get(RaftNode node, String name) throws Exception {
        Field field = RaftNode.class.getDeclaredField(name);
        field.setAccessible(true);
        return field.getLong(node);
    }
    private static void acknowledge(RaftNode node, long term, long index) throws Exception {
        Method method = RaftNode.class.getDeclaredMethod("handleAppendEntriesResponse", Peer.class,
                AppendEntriesRequest.class, AppendEntriesResponse.class);
        method.setAccessible(true);
        method.invoke(node, peer("b"), new AppendEntriesRequest(term, "a", index - 1, term, 0,
                List.of(new LogEntry(term, "a"))), new AppendEntriesResponse(term, "b", true, index));
    }

    @Test void jointCommitCountsNewVoters() throws Exception {
        RaftNode node = TestRaftNodeBuilder.forPeer(peer("a")).withStateMachine(null).build();
        set(node, "currentTerm", 1L);
        set(node, "state", RaftNode.State.LEADER);
        assertTrue(node.submitJointConfigurationChange(List.of(peer("a"), peer("b"))));
        assertEquals(0, get(node, "commitIndex"));
        acknowledge(node, 1, 1);
        assertEquals(1, get(node, "commitIndex"));
    }

    @Test void heartbeatCannotCommitUnverifiedFollowerSuffix() throws Exception {
        InMemoryLogStore store = new InMemoryLogStore();
        RecordingMachine sm = new RecordingMachine();
        store.append(List.of(new LogEntry(1, "b", "common".getBytes()),
                new LogEntry(1, "c", "divergent".getBytes())));
        RaftNode node = node(store, sm);
        assertTrue(node.handleAppendEntries(new AppendEntriesRequest(2, "b", 1, 1, 2, List.of())).isSuccess());
        assertEquals(1, get(node, "commitIndex"));
        assertEquals("common", sm.state);
    }

    @Test void sameTermStepdownRetainsVote() throws Exception {
        RaftNode node = node(new InMemoryLogStore(), null);
        set(node, "currentTerm", 1L);
        set(node, "state", RaftNode.State.CANDIDATE);
        set(node, "votedFor", peer("a"));
        node.handleAppendEntries(new AppendEntriesRequest(1, "b", 0, 0, 0, List.of()));
        assertFalse(node.handleVoteRequest(new VoteRequest(1, "c", 0, 0)).isVoteGranted());
    }

    @Test void oldAcknowledgementCannotCommitReplacement() throws Exception {
        InMemoryLogStore store = new InMemoryLogStore();
        store.append(List.of(new LogEntry(3, "a")));
        RaftNode node = node(store, null);
        set(node, "currentTerm", 3L);
        set(node, "state", RaftNode.State.LEADER);
        acknowledge(node, 1, 1);
        assertEquals(0, get(node, "commitIndex"));
        assertFalse(node.canServeLinearizableRead());
    }

    @Test void reorderedAcknowledgementsCannotReduceConfirmedProgress() throws Exception {
        InMemoryLogStore store = new InMemoryLogStore();
        store.append(List.of(new LogEntry(3, "a"), new LogEntry(3, "a")));
        RaftNode node = node(store, null);
        set(node, "currentTerm", 3L);
        set(node, "state", RaftNode.State.LEADER);
        acknowledge(node, 3, 2);
        acknowledge(node, 3, 1);
        assertEquals(2, node.telemetrySnapshot().commitIndex());
        Field field = RaftNode.class.getDeclaredField("matchIndex");
        field.setAccessible(true);
        assertEquals(2L, ((java.util.Map<?, ?>) field.get(node)).get("b"));
    }

    @Test void staleSnapshotCannotRestoreOldApplicationState() throws Exception {
        RecordingMachine sm = new RecordingMachine();
        RaftNode node = node(new InMemoryLogStore(), sm);
        node.handleInstallSnapshot(new InstallSnapshotRequest(3, "b", 2, 1, "old".getBytes()));
        node.handleAppendEntries(new AppendEntriesRequest(3, "b", 2, 1, 3,
                List.of(new LogEntry(3, "b", "new".getBytes()))));
        node.handleInstallSnapshot(new InstallSnapshotRequest(3, "b", 2, 1, "old".getBytes()));
        assertEquals("new", sm.state);
        assertEquals(3, get(node, "lastApplied"));
    }

    @Test void restartPreservesVoteEvenWhenCandidateLeftMembership() {
        var state = new org.gautelis.raft.storage.InMemoryPersistentStateStore();
        state.setCurrentTerm(3);
        state.setVotedFor("removed");
        RaftNode node = TestRaftNodeBuilder.forPeer(peer("a")).withPeers(List.of(peer("b")))
                .withPersistentStateStore(state).build();
        assertFalse(node.handleVoteRequest(new VoteRequest(3, "b", 0, 0)).isVoteGranted());
        assertEquals("removed", state.votedFor().orElseThrow());
    }

    @Test void durableLogIgnoresObsoleteMetadataAndRejectsTruncation(@org.junit.jupiter.api.io.TempDir java.nio.file.Path directory) throws Exception {
        var file = directory.resolve("log");
        var store = new org.gautelis.raft.storage.FileLogStore(file);
        store.append(List.of(new LogEntry(1, "a"), new LogEntry(2, "a"), new LogEntry(2, "a")));
        store.installSnapshot(2, 2, "checkpoint".getBytes());
        java.nio.file.Files.writeString(directory.resolve("log.meta"), "snapshotIndex=100\nsnapshotTerm=99\n");
        var recovered = new org.gautelis.raft.storage.FileLogStore(file);
        assertEquals(2, recovered.snapshotIndex());
        assertEquals(3, recovered.lastIndex());
        assertArrayEquals("checkpoint".getBytes(), recovered.snapshotData());
        byte[] record = java.nio.file.Files.readAllBytes(file);
        java.nio.file.Files.write(file, java.util.Arrays.copyOf(record, record.length - 1));
        assertThrows(IllegalStateException.class, () -> new org.gautelis.raft.storage.FileLogStore(file));
    }

    @Test void failedLogWritePermanentlyFencesTheNode(@org.junit.jupiter.api.io.TempDir java.nio.file.Path directory) throws Exception {
        var folder = java.nio.file.Files.createDirectory(directory.resolve("data"));
        var store = new org.gautelis.raft.storage.FileLogStore(folder.resolve("log"));
        RaftNode node = nodeWithStore(store);
        set(node, "currentTerm", 1L);
        set(node, "state", RaftNode.State.LEADER);
        java.nio.file.Files.delete(folder);
        java.nio.file.Files.writeString(folder, "blocks persistent writes");
        assertThrows(RuntimeException.class, () -> node.submitCommand("command".getBytes()));
        assertFalse(node.isLeader());
        assertTrue(node.isDecommissioned());
        assertThrows(IllegalStateException.class, () -> node.handleVoteRequest(new VoteRequest(2, "b", 100, 2)));
        assertThrows(IllegalStateException.class, () -> node.queryStateMachine(new byte[0]));
    }

    private static RaftNode nodeWithStore(org.gautelis.raft.storage.LogStore store) {
        return TestRaftNodeBuilder.forPeer(peer("a")).withPeers(List.of(peer("b")))
                .withLogStore(store).withStateMachine(null).build();
    }

    private static class RecordingMachine implements SnapshotStateMachine {
        String state = "";
        public void apply(long term, byte[] command) { state = new String(command); }
        public byte[] snapshot() { return state.getBytes(); }
        public void restore(byte[] bytes) { state = new String(bytes); }
    }
}
