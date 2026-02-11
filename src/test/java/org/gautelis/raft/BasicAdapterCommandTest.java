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

import org.gautelis.raft.protocol.ClusterMessage;
import org.gautelis.raft.protocol.Peer;
import org.gautelis.raft.bootstrap.BasicAdapter;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class BasicAdapterCommandTest {
    private static final Logger log = LoggerFactory.getLogger(BasicAdapterCommandTest.class);

    static final class MutableTime implements RaftNode.TimeSource {
        private long now;
        MutableTime(long now) { this.now = now; }
        @Override
        public long nowMillis() { return now; }
        void set(long now) { this.now = now; }
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

    static class TestAdapter extends BasicAdapter {
        TestAdapter(Peer me, List<Peer> peers) {
            super(100, me, peers);
        }

        void bind(RaftNode node) {
            this.stateMachine = node;
        }
    }

    private static Peer peer(String id) {
        return new Peer(id, null);
    }

    @Test
    void leaderAcceptsValidClusterCommandAndAppliesThroughLog() {
        log.info("*** Testcase *** Leader command ingestion: verifies valid ClusterMessage commands are submitted through Raft log and applied to state machine");
        Peer me = peer("A");
        MutableTime time = new MutableTime(0);
        KeyValueStateMachine kv = new KeyValueStateMachine();
        InMemoryLogStore store = new InMemoryLogStore();
        RaftNode node = new RaftNode(
                me,
                List.of(),
                100,
                null,
                kv,
                new NoopRaftClient(),
                store,
                new InMemoryPersistentStateStore(),
                time,
                new Random(1)
        );

        node.setLastHeartbeatMillisForTest(0);
        time.set(10_000);
        node.electionTickForTest(); // single-node election => leader

        TestAdapter adapter = new TestAdapter(me, List.of());
        adapter.bind(node);

        ClusterMessage message = new ClusterMessage(node.getTerm(), "client", "set x 42");
        byte[] payload = ProtoMapper.toProto(message).toByteString().toByteArray();
        adapter.handleMessage("corr-1", "ClusterMessage", payload, null);

        assertEquals("42", kv.get("x"));
        assertEquals(1, store.lastIndex());
    }

    @Test
    void invalidClusterCommandIsRejected() {
        log.info("*** Testcase *** Command validation reject path: verifies malformed ClusterMessage commands are rejected without log mutation");
        Peer me = peer("A");
        MutableTime time = new MutableTime(0);
        KeyValueStateMachine kv = new KeyValueStateMachine();
        InMemoryLogStore store = new InMemoryLogStore();
        RaftNode node = new RaftNode(
                me,
                List.of(),
                100,
                null,
                kv,
                new NoopRaftClient(),
                store,
                new InMemoryPersistentStateStore(),
                time,
                new Random(1)
        );

        node.setLastHeartbeatMillisForTest(0);
        time.set(10_000);
        node.electionTickForTest(); // leader

        TestAdapter adapter = new TestAdapter(me, List.of());
        adapter.bind(node);

        ClusterMessage message = new ClusterMessage(node.getTerm(), "client", "bogus cmd");
        byte[] payload = ProtoMapper.toProto(message).toByteString().toByteArray();
        adapter.handleMessage("corr-2", "ClusterMessage", payload, null);

        assertNull(kv.get("x"));
        assertEquals(0, store.lastIndex());
    }

    @Test
    void followerRejectsValidClusterCommand() {
        log.info("*** Testcase *** Follower command rejection: verifies non-leader nodes reject otherwise valid ClusterMessage commands");
        Peer me = peer("A");
        KeyValueStateMachine kv = new KeyValueStateMachine();
        InMemoryLogStore store = new InMemoryLogStore();
        RaftNode node = new RaftNode(
                me,
                List.of(),
                100,
                null,
                kv,
                new NoopRaftClient(),
                store,
                new InMemoryPersistentStateStore(),
                System::currentTimeMillis,
                new Random(1)
        );

        TestAdapter adapter = new TestAdapter(me, List.of());
        adapter.bind(node);

        ClusterMessage message = new ClusterMessage(node.getTerm(), "client", "set y 7");
        byte[] payload = ProtoMapper.toProto(message).toByteString().toByteArray();
        adapter.handleMessage("corr-3", "ClusterMessage", payload, null);

        assertNull(kv.get("y"));
        assertEquals(0, store.lastIndex());
    }
}
