package org.gautelis.raft;

import org.gautelis.raft.model.AppendEntriesRequest;
import org.gautelis.raft.model.AppendEntriesResponse;
import org.gautelis.raft.model.InstallSnapshotRequest;
import org.gautelis.raft.model.InstallSnapshotResponse;
import org.gautelis.raft.model.LogEntry;
import org.gautelis.raft.model.Peer;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RaftAppendEntriesTest {
    private static final Logger log = LoggerFactory.getLogger(RaftAppendEntriesTest.class);

    static class NoopRaftClient extends RaftClient {
        NoopRaftClient() {
            super("test", null);
        }

        @Override
        public void shutdown() {
            // no-op in tests
        }
    }

    private static Peer peer(String id) {
        return new Peer(id, null);
    }

    @Test
    void staleTermAppendEntriesIsRejected() {
        log.info("*** Testcase *** AppendEntries stale-term rejection: verifies follower rejects append requests from lower terms");
        Peer a = peer("A");
        Peer b = peer("B");
        RaftNode nodeB = new RaftNode(b, List.of(a), 100, null, new NoopRaftClient(), new InMemoryLogStore(), System::currentTimeMillis, new Random(1));

        nodeB.handleAppendEntries(new AppendEntriesRequest(5, "A", 0, 0, 0, List.of()));

        AppendEntriesRequest req = new AppendEntriesRequest(
                4, "A", 0, 0, 0, List.of()
        );
        AppendEntriesResponse response = nodeB.handleAppendEntries(req);

        assertFalse(response.isSuccess());
        assertEquals(5, response.getTerm());
        assertEquals(5, nodeB.getTerm());
    }

    @Test
    void prevLogMismatchIsRejected() {
        log.info("*** Testcase *** AppendEntries prev-log mismatch: verifies follower rejects append when prevLogIndex/prevLogTerm do not match local log");
        Peer a = peer("A");
        Peer b = peer("B");
        InMemoryLogStore store = new InMemoryLogStore();
        store.append(List.of(new LogEntry(1, "B"), new LogEntry(2, "B")));

        RaftNode nodeB = new RaftNode(b, List.of(a), 100, null, new NoopRaftClient(), store, System::currentTimeMillis, new Random(1));

        AppendEntriesRequest req = new AppendEntriesRequest(
                3, "A", 2, 1, 0, List.of()
        );
        AppendEntriesResponse response = nodeB.handleAppendEntries(req);

        assertFalse(response.isSuccess());
        assertEquals(3, nodeB.getTerm());
        assertEquals(2, store.lastIndex());
    }

    @Test
    void conflictingSuffixIsTruncatedAndReplaced() {
        log.info("*** Testcase *** AppendEntries conflict resolution: verifies follower truncates conflicting suffix and appends leader entries");
        Peer a = peer("A");
        Peer b = peer("B");
        InMemoryLogStore store = new InMemoryLogStore();
        store.append(List.of(
                new LogEntry(1, "B", "one".getBytes(java.nio.charset.StandardCharsets.UTF_8)),
                new LogEntry(2, "B", "old".getBytes(java.nio.charset.StandardCharsets.UTF_8))
        ));

        RaftNode nodeB = new RaftNode(b, List.of(a), 100, null, new NoopRaftClient(), store, System::currentTimeMillis, new Random(1));

        LogEntry replacement = new LogEntry(3, "A", "new".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        AppendEntriesRequest req = new AppendEntriesRequest(
                3, "A", 1, 1, 2, List.of(replacement)
        );
        AppendEntriesResponse response = nodeB.handleAppendEntries(req);

        assertTrue(response.isSuccess());
        assertEquals(3, nodeB.getTerm());
        assertEquals(2, store.lastIndex());
        assertEquals(3, store.termAt(2));
        assertArrayEquals("new".getBytes(java.nio.charset.StandardCharsets.UTF_8), store.entryAt(2).getData());
        assertEquals(2, nodeB.getCommitIndexForTest());
    }

    @Test
    void selfLeaderIdAppendEntriesIsRejected() {
        log.info("*** Testcase *** Self-origin append rejection: verifies follower rejects AppendEntries claiming itself as leader");
        Peer a = peer("A");
        Peer b = peer("B");
        InMemoryLogStore store = new InMemoryLogStore();
        RaftNode nodeB = new RaftNode(b, List.of(a), 100, null, new NoopRaftClient(), store, System::currentTimeMillis, new Random(1));

        AppendEntriesRequest req = new AppendEntriesRequest(
                1, "B", 0, 0, 0, List.of()
        );
        AppendEntriesResponse response = nodeB.handleAppendEntries(req);

        assertFalse(response.isSuccess());
        assertEquals(0, store.lastIndex());
        assertEquals(0, nodeB.getTerm());
    }

    @Test
    void installSnapshotUpdatesFollowerSnapshotState() {
        log.info("*** Testcase *** InstallSnapshot follower update: verifies snapshot metadata/data, commitIndex, and lastApplied advance after install");
        Peer a = peer("A");
        Peer b = peer("B");
        InMemoryLogStore store = new InMemoryLogStore();
        store.append(List.of(
                new LogEntry(1, "B", "one".getBytes(java.nio.charset.StandardCharsets.UTF_8)),
                new LogEntry(2, "B", "two".getBytes(java.nio.charset.StandardCharsets.UTF_8))
        ));

        RaftNode nodeB = new RaftNode(b, List.of(a), 100, null, new NoopRaftClient(), store, System::currentTimeMillis, new Random(1));
        InstallSnapshotRequest req = new InstallSnapshotRequest(3, "A", 2, 2, "snap".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        InstallSnapshotResponse response = nodeB.handleInstallSnapshot(req);

        assertTrue(response.isSuccess());
        assertEquals(3, nodeB.getTerm());
        assertEquals(2, store.snapshotIndex());
        assertEquals(2, store.snapshotTerm());
        assertArrayEquals("snap".getBytes(java.nio.charset.StandardCharsets.UTF_8), store.snapshotData());
        assertEquals(2, nodeB.getCommitIndexForTest());
        assertEquals(2, nodeB.getLastAppliedForTest());
    }

    @Test
    void appendEntriesBelowSnapshotIsRejected() {
        log.info("*** Testcase *** Append below snapshot boundary: verifies follower rejects AppendEntries that reference compacted prefix");
        Peer a = peer("A");
        Peer b = peer("B");
        InMemoryLogStore store = new InMemoryLogStore();
        store.append(List.of(
                new LogEntry(1, "B", "one".getBytes(java.nio.charset.StandardCharsets.UTF_8)),
                new LogEntry(2, "B", "two".getBytes(java.nio.charset.StandardCharsets.UTF_8)),
                new LogEntry(3, "B", "three".getBytes(java.nio.charset.StandardCharsets.UTF_8))
        ));
        store.compactUpTo(2);

        RaftNode nodeB = new RaftNode(b, List.of(a), 100, null, new NoopRaftClient(), store, System::currentTimeMillis, new Random(1));
        AppendEntriesRequest req = new AppendEntriesRequest(3, "A", 1, 1, 2, List.of());
        AppendEntriesResponse response = nodeB.handleAppendEntries(req);

        assertFalse(response.isSuccess());
        assertEquals(2, response.getMatchIndex());
    }
}
