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
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RaftSnapshotStateMachineTest {
    private static final Logger log = LoggerFactory.getLogger(RaftSnapshotStateMachineTest.class);

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
        public void apply(long term, String command) {
            applied.add(term + ":" + command);
        }

        @Override
        public byte[] snapshot() {
            return "snapshot-current".getBytes(StandardCharsets.UTF_8);
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

    @Test
    void installSnapshotRestoresStateMachineAndSubsequentCommitAppliesCommands() {
        log.info("*** Testcase *** Snapshot restore + post-snapshot apply: verifies InstallSnapshot restores state machine and later committed log entries still apply");
        Peer a = peer("A");
        Peer b = peer("B");

        InMemoryLogStore store = new InMemoryLogStore();
        CapturingStateMachine sm = new CapturingStateMachine();
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

        InstallSnapshotRequest snapshotRequest = new InstallSnapshotRequest(
                4,
                "A",
                5,
                4,
                "snapshot-5".getBytes(StandardCharsets.UTF_8)
        );
        var snapshotResponse = nodeB.handleInstallSnapshot(snapshotRequest);
        assertTrue(snapshotResponse.isSuccess());
        assertArrayEquals("snapshot-5".getBytes(StandardCharsets.UTF_8), sm.restored);
        assertEquals(5, nodeB.getCommitIndexForTest());
        assertEquals(5, nodeB.getLastAppliedForTest());

        // Replicate one post-snapshot command and commit it.
        nodeB.handleAppendEntries(new AppendEntriesRequest(
                4,
                "A",
                5,
                4,
                5,
                List.of(new LogEntry(4, "A", "set x=7".getBytes(StandardCharsets.UTF_8)))
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
        assertEquals("4:set x=7", sm.applied.getFirst());
    }
}
