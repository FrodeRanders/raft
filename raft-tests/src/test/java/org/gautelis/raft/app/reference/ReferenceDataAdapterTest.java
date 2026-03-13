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

import org.gautelis.raft.app.reference.ReferenceDataAdapter;
import org.gautelis.raft.app.reference.ReferenceDataCommand;
import org.gautelis.raft.app.reference.ReferenceDataQuery;
import org.gautelis.raft.app.reference.ReferenceDataQueryResult;
import org.gautelis.raft.app.reference.ReferenceDataStateMachine;
import org.gautelis.raft.protocol.ClientCommandRequest;
import org.gautelis.raft.protocol.ClientCommandResponse;
import org.gautelis.raft.protocol.ClientQueryRequest;
import org.gautelis.raft.protocol.ClientQueryResponse;
import org.gautelis.raft.protocol.Peer;
import org.gautelis.raft.storage.InMemoryLogStore;
import org.gautelis.raft.storage.InMemoryPersistentStateStore;
import org.gautelis.raft.transport.netty.RaftClient;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReferenceDataAdapterTest {
    static final class MutableTime implements RaftNode.TimeSource {
        private long now;

        MutableTime(long now) {
            this.now = now;
        }

        @Override
        public long nowMillis() {
            return now;
        }

        void set(long now) {
            this.now = now;
        }
    }

    static final class NoopRaftClient extends RaftClient {
        NoopRaftClient() {
            super("reference-test", null);
        }

        @Override
        public void shutdown() {
        }
    }

    static final class TestAdapter extends ReferenceDataAdapter {
        TestAdapter(Peer me, List<Peer> peers) {
            super(100, me, peers, Set.of());
        }

        void bind(RaftNode node) {
            this.stateMachine = node;
        }

        ClientCommandResponse command(ClientCommandRequest request) {
            return handleClientCommandRequest(request);
        }

        ClientQueryResponse query(ClientQueryRequest request) {
            return handleClientQueryRequest(request);
        }
    }

    private static RaftNode newReferenceDataNode(
            Peer me,
            ReferenceDataStateMachine stateMachine,
            RaftNode.TimeSource timeSource
    ) {
        return TestRaftNodeBuilder.forPeer(me)
                .withPeers(List.of())
                .withStateMachine(stateMachine)
                .withClient(new NoopRaftClient())
                .withLogStore(new InMemoryLogStore())
                .withPersistentStateStore(new InMemoryPersistentStateStore())
                .withTimeSource(timeSource)
                .withRandom(new Random(1))
                .build();
    }

    @Test
    void leaderAcceptsReferenceDataCommandsAndServesReferenceQuery() {
        Peer me = new Peer("A", null);
        MutableTime time = new MutableTime(0L);
        ReferenceDataStateMachine stateMachine = new ReferenceDataStateMachine();
        RaftNode node = newReferenceDataNode(me, stateMachine, time);

        node.setLastHeartbeatMillisForTest(0L);
        time.set(10_000L);
        node.electionTickForTest();
        node.handleVoteResponsesForTest(List.of(), 1);

        TestAdapter adapter = new TestAdapter(me, List.of());
        adapter.bind(node);

        ClientCommandResponse commandResponse = adapter.command(new ClientCommandRequest(
                node.getTerm(),
                "reference-admin",
                ReferenceDataCommand.upsertProduct("p1", "Widget").encode()
        ));
        assertTrue(commandResponse.isSuccess());
        assertEquals("ACCEPTED", commandResponse.getStatus());

        adapter.command(new ClientCommandRequest(
                node.getTerm(),
                "reference-admin",
                ReferenceDataCommand.upsertVariant("p1", "v1", "Blue").encode()
        ));

        ClientQueryResponse queryResponse = adapter.query(new ClientQueryRequest(
                node.getTerm(),
                "reference-query",
                ReferenceDataQuery.variantsForProduct("p1").encode()
        ));
        assertTrue(queryResponse.isSuccess());

        ReferenceDataQueryResult result = ReferenceDataQueryResult.decode(queryResponse.getResult()).orElseThrow();
        assertEquals("p1", result.productId());
        assertEquals(1, result.variants().size());
        assertEquals("Blue", result.variants().getFirst().variantName());
    }
}
