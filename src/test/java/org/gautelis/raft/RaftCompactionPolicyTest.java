package org.gautelis.raft;

import org.gautelis.raft.model.AppendEntriesRequest;
import org.gautelis.raft.model.LogEntry;
import org.gautelis.raft.model.Peer;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RaftCompactionPolicyTest {
    private static final Logger log = LoggerFactory.getLogger(RaftCompactionPolicyTest.class);

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
        public void apply(long term, String command) {
            applied.add(term + ":" + command);
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
            RaftNode nodeB = new RaftNode(
                    b,
                    List.of(a),
                    100,
                    null,
                    sm,
                    new NoopRaftClient(),
                    store,
                    new InMemoryPersistentStateStore(),
                    System::currentTimeMillis,
                    new Random(1)
            );

            // Append one command and commit it
            nodeB.handleAppendEntries(new AppendEntriesRequest(
                    2,
                    "A",
                    0,
                    0,
                    1,
                    List.of(new LogEntry(2, "A", "set x 10".getBytes(StandardCharsets.UTF_8)))
            ));

            assertEquals(1, store.snapshotIndex());
            assertEquals(2, store.snapshotTerm());
            assertFalse(store.snapshotData().length == 0);
            assertTrue(new String(store.snapshotData(), StandardCharsets.UTF_8).contains("set x 10"));
        } finally {
            if (prev == null) {
                System.clearProperty("raft.snapshot.min.entries");
            } else {
                System.setProperty("raft.snapshot.min.entries", prev);
            }
        }
    }
}
