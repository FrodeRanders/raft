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
import org.gautelis.raft.protocol.StateMachineCommand;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private static RaftNode newCommandHandlerNode(Peer me, List<Peer> peers, CommandHandler commandHandler) {
        return TestRaftNodeBuilder.forPeer(me)
                .withPeers(peers)
                .withTimeoutMillis(100)
                .withStateMachine(new CommandHandlerStateMachineAdapter(commandHandler))
                .withClient(new NoopRaftClient())
                .withLogStore(new InMemoryLogStore())
                .withPersistentStateStore(new InMemoryPersistentStateStore())
                .withTimeSource(System::currentTimeMillis)
                .withRandom(new Random(1))
                .build();
    }

    @Test
    void entryIsNotAppliedBeforeCommit() {
        log.info("TC: Apply gate before commit: verifies replicated entries are not applied until commitIndex advances");
        Peer a = peer("A");
        Peer b = peer("B");
        List<String> applied = new ArrayList<>();

        CommandHandler commandHandler = (term, command) -> applied.add(term + ":" + StateMachineCommand.decode(command).map(Object::toString).orElse("invalid"));
        RaftNode nodeB = newCommandHandlerNode(b, List.of(a), commandHandler);

        AppendEntriesRequest appendUncommitted = new AppendEntriesRequest(
                2,
                "A",
                0,
                0,
                0,
                List.of(new LogEntry(2, "A", StateMachineCommand.put("x", "1").encode()))
        );
        var response = nodeB.handleAppendEntries(appendUncommitted);
        assertTrue(response.isSuccess());

        assertEquals(0, nodeB.getCommitIndexForTest());
        assertEquals(0, nodeB.getLastAppliedForTest());
        assertTrue(applied.isEmpty());
    }

    @Test
    void committedEntryIsAppliedExactlyOnce() {
        log.info("TC: Idempotent apply on commit: verifies committed entries are applied exactly once and not re-applied on duplicate commit notifications");
        Peer a = peer("A");
        Peer b = peer("B");
        List<String> applied = new ArrayList<>();

        CommandHandler commandHandler = (term, command) -> applied.add(term + ":" + StateMachineCommand.decode(command).map(Object::toString).orElse("invalid"));
        RaftNode nodeB = newCommandHandlerNode(b, List.of(a), commandHandler);

        // Replicate command but do not commit.
        nodeB.handleAppendEntries(new AppendEntriesRequest(
                3,
                "A",
                0,
                0,
                0,
                List.of(new LogEntry(3, "A", StateMachineCommand.put("y", "2").encode()))
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
        assertEquals("3:" + StateMachineCommand.put("y", "2"), applied.getFirst());

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
        log.info("TC: Apply term source correctness: verifies applied commands use log-entry term rather than node current term");
        Peer a = peer("A");
        Peer b = peer("B");
        List<String> applied = new ArrayList<>();

        CommandHandler commandHandler = (term, command) -> applied.add(term + ":" + StateMachineCommand.decode(command).map(Object::toString).orElse("invalid"));
        RaftNode nodeB = newCommandHandlerNode(b, List.of(a), commandHandler);

        // Replicate entry from term 3
        nodeB.handleAppendEntries(new AppendEntriesRequest(
                3,
                "A",
                0,
                0,
                0,
                List.of(new LogEntry(3, "A", StateMachineCommand.put("z", "3").encode()))
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
        assertEquals("3:" + StateMachineCommand.put("z", "3"), applied.getFirst());
    }
}
