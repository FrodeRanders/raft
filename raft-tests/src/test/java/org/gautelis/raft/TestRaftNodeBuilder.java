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

import org.gautelis.raft.app.kv.KeyValueStateMachine;
import org.gautelis.raft.protocol.Peer;
import org.gautelis.raft.statemachine.SnapshotStateMachine;
import org.gautelis.raft.storage.InMemoryLogStore;
import org.gautelis.raft.storage.InMemoryPersistentStateStore;
import org.gautelis.raft.storage.LogStore;
import org.gautelis.raft.storage.PersistentStateStore;
import org.gautelis.raft.transport.RaftTransportClient;
import org.gautelis.raft.transport.netty.RaftClient;

import java.util.List;
import java.util.Random;

/**
 * Test-only fluent builder for constructing Raft nodes with sensible defaults.
 */
final class TestRaftNodeBuilder {
    private final Peer me;
    private List<Peer> peers = List.of();
    private long timeoutMillis = 100L;
    private MessageHandler messageHandler;
    private SnapshotStateMachine snapshotStateMachine = new KeyValueStateMachine();
    private RaftTransportClient raftClient = new NoopRaftClient();
    private LogStore logStore = new InMemoryLogStore();
    private PersistentStateStore persistentStateStore = new InMemoryPersistentStateStore();
    private RaftNode.TimeSource timeSource = System::currentTimeMillis;
    private Random random = new Random(1);

    private TestRaftNodeBuilder(Peer me) {
        this.me = me;
    }

    static TestRaftNodeBuilder forPeer(Peer me) {
        return new TestRaftNodeBuilder(me);
    }

    TestRaftNodeBuilder withPeers(List<Peer> peers) {
        this.peers = peers == null ? List.of() : List.copyOf(peers);
        return this;
    }

    TestRaftNodeBuilder withTimeoutMillis(long timeoutMillis) {
        this.timeoutMillis = timeoutMillis;
        return this;
    }

    TestRaftNodeBuilder withMessageHandler(MessageHandler messageHandler) {
        this.messageHandler = messageHandler;
        return this;
    }

    TestRaftNodeBuilder withStateMachine(SnapshotStateMachine snapshotStateMachine) {
        this.snapshotStateMachine = snapshotStateMachine;
        return this;
    }

    TestRaftNodeBuilder withClient(RaftTransportClient raftClient) {
        this.raftClient = raftClient;
        return this;
    }

    TestRaftNodeBuilder withLogStore(LogStore logStore) {
        this.logStore = logStore;
        return this;
    }

    TestRaftNodeBuilder withPersistentStateStore(PersistentStateStore persistentStateStore) {
        this.persistentStateStore = persistentStateStore;
        return this;
    }

    TestRaftNodeBuilder withTimeSource(RaftNode.TimeSource timeSource) {
        this.timeSource = timeSource;
        return this;
    }

    TestRaftNodeBuilder withRandom(Random random) {
        this.random = random;
        return this;
    }

    RaftNode build() {
        return new RaftNode(
                me,
                peers,
                timeoutMillis,
                messageHandler,
                snapshotStateMachine,
                raftClient,
                logStore,
                persistentStateStore,
                timeSource,
                random
        );
    }

    private static final class NoopRaftClient extends RaftClient {
        private NoopRaftClient() {
            super("test-builder", null);
        }

        @Override
        public void shutdown() {
            // no-op in tests
        }
    }
}
