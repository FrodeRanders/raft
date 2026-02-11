package org.gautelis.raft;

import org.gautelis.raft.model.AppendEntriesRequest;
import org.gautelis.raft.model.LogEntry;
import org.gautelis.raft.model.Peer;
import org.gautelis.raft.model.VoteRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FilePersistentStoresTest {
    private static final Logger log = LoggerFactory.getLogger(FilePersistentStoresTest.class);

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
}
