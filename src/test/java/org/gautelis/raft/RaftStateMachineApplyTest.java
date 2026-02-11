package org.gautelis.raft;

import org.gautelis.raft.storage.*;
import org.gautelis.raft.statemachine.*;
import org.gautelis.raft.transport.netty.*;
import org.gautelis.raft.serialization.ProtoMapper;

import org.gautelis.raft.protocol.AppendEntriesRequest;
import org.gautelis.raft.protocol.LogEntry;
import org.gautelis.raft.protocol.Peer;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RaftStateMachineApplyTest {
    private static final Logger log = LoggerFactory.getLogger(RaftStateMachineApplyTest.class);

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
    void entryIsNotAppliedBeforeCommit() {
        log.info("*** Testcase *** Apply gate before commit: verifies replicated entries are not applied until commitIndex advances");
        Peer a = peer("A");
        Peer b = peer("B");
        List<String> applied = new ArrayList<>();

        CommandHandler commandHandler = (term, command) -> applied.add(term + ":" + command);
        RaftNode nodeB = new RaftNode(
                b,
                List.of(a),
                100,
                null,
                commandHandler,
                new NoopRaftClient(),
                new InMemoryLogStore(),
                new InMemoryPersistentStateStore(),
                System::currentTimeMillis,
                new Random(1)
        );

        AppendEntriesRequest appendUncommitted = new AppendEntriesRequest(
                2,
                "A",
                0,
                0,
                0,
                List.of(new LogEntry(2, "A", "set x=1".getBytes(StandardCharsets.UTF_8)))
        );
        var response = nodeB.handleAppendEntries(appendUncommitted);
        assertTrue(response.isSuccess());

        assertEquals(0, nodeB.getCommitIndexForTest());
        assertEquals(0, nodeB.getLastAppliedForTest());
        assertTrue(applied.isEmpty());
    }

    @Test
    void committedEntryIsAppliedExactlyOnce() {
        log.info("*** Testcase *** Idempotent apply on commit: verifies committed entries are applied exactly once and not re-applied on duplicate commit notifications");
        Peer a = peer("A");
        Peer b = peer("B");
        List<String> applied = new ArrayList<>();

        CommandHandler commandHandler = (term, command) -> applied.add(term + ":" + command);
        RaftNode nodeB = new RaftNode(
                b,
                List.of(a),
                100,
                null,
                commandHandler,
                new NoopRaftClient(),
                new InMemoryLogStore(),
                new InMemoryPersistentStateStore(),
                System::currentTimeMillis,
                new Random(1)
        );

        // Replicate command but do not commit.
        nodeB.handleAppendEntries(new AppendEntriesRequest(
                3,
                "A",
                0,
                0,
                0,
                List.of(new LogEntry(3, "A", "set y=2".getBytes(StandardCharsets.UTF_8)))
        ));
        assertTrue(applied.isEmpty());

        // Commit via leaderCommit advance.
        nodeB.handleAppendEntries(new AppendEntriesRequest(
                3,
                "A",
                1,
                3,
                1,
                List.of()
        ));

        assertEquals(1, nodeB.getCommitIndexForTest());
        assertEquals(1, nodeB.getLastAppliedForTest());
        assertEquals(1, applied.size());
        assertEquals("3:set y=2", applied.getFirst());

        // Repeat same commit index; should not re-apply.
        nodeB.handleAppendEntries(new AppendEntriesRequest(
                3,
                "A",
                1,
                3,
                1,
                List.of()
        ));
        assertEquals(1, applied.size());
    }

    @Test
    void applyUsesEntryTermNotCurrentNodeTerm() {
        log.info("*** Testcase *** Apply term source correctness: verifies applied commands use log-entry term rather than node current term");
        Peer a = peer("A");
        Peer b = peer("B");
        List<String> applied = new ArrayList<>();

        CommandHandler commandHandler = (term, command) -> applied.add(term + ":" + command);
        RaftNode nodeB = new RaftNode(
                b,
                List.of(a),
                100,
                null,
                commandHandler,
                new NoopRaftClient(),
                new InMemoryLogStore(),
                new InMemoryPersistentStateStore(),
                System::currentTimeMillis,
                new Random(1)
        );

        // Replicate entry from term 3
        nodeB.handleAppendEntries(new AppendEntriesRequest(
                3,
                "A",
                0,
                0,
                0,
                List.of(new LogEntry(3, "A", "set z=3".getBytes(StandardCharsets.UTF_8)))
        ));
        // Advance node term to 4 via empty AppendEntries heartbeat, without changing entry term.
        nodeB.handleAppendEntries(new AppendEntriesRequest(
                4,
                "A",
                1,
                3,
                0,
                List.of()
        ));

        // Commit the term-3 entry at node term 4.
        nodeB.handleAppendEntries(new AppendEntriesRequest(
                4,
                "A",
                1,
                3,
                1,
                List.of()
        ));

        assertEquals(1, applied.size());
        assertEquals("3:set z=3", applied.getFirst());
    }
}
