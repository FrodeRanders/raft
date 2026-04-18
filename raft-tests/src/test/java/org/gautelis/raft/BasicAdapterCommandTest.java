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
import org.gautelis.raft.storage.*;
import org.gautelis.raft.statemachine.*;
import org.gautelis.raft.transport.netty.*;
import org.gautelis.raft.serialization.ProtoMapper;
import org.gautelis.raft.app.reference.ReferenceDataCommand;
import org.gautelis.raft.app.reference.ReferenceDataStateMachine;

import org.gautelis.raft.protocol.ClientCommandRequest;
import org.gautelis.raft.protocol.ClientCommandResponse;
import org.gautelis.raft.protocol.ClientQueryRequest;
import org.gautelis.raft.protocol.ClientQueryResponse;
import org.gautelis.raft.protocol.AppendEntriesRequest;
import org.gautelis.raft.protocol.AppendEntriesResponse;
import org.gautelis.raft.protocol.ClusterSummaryRequest;
import org.gautelis.raft.protocol.ClusterSummaryResponse;
import org.gautelis.raft.protocol.JoinClusterRequest;
import org.gautelis.raft.protocol.JoinClusterResponse;
import org.gautelis.raft.protocol.JoinClusterStatusRequest;
import org.gautelis.raft.protocol.JoinClusterStatusResponse;
import org.gautelis.raft.protocol.Peer;
import org.gautelis.raft.protocol.ReconfigurationStatusRequest;
import org.gautelis.raft.protocol.ReconfigurationStatusResponse;
import org.gautelis.raft.protocol.ReconfigureClusterRequest;
import org.gautelis.raft.protocol.ReconfigureClusterResponse;
import org.gautelis.raft.protocol.StateMachineQuery;
import org.gautelis.raft.protocol.StateMachineQueryResult;
import org.gautelis.raft.protocol.StateMachineCommand;
import org.gautelis.raft.protocol.StateMachineCommandResult;
import org.gautelis.raft.protocol.TelemetryRequest;
import org.gautelis.raft.protocol.TelemetryResponse;
import org.gautelis.raft.protocol.VoteRequest;
import org.gautelis.raft.protocol.VoteResponse;
import org.gautelis.raft.bootstrap.AllowListCommandAuthorizer;
import org.gautelis.raft.bootstrap.BasicAdapter;
import org.gautelis.raft.app.reference.ReferenceDataAdapter;
import org.gautelis.raft.bootstrap.SharedSecretCommandAuthenticator;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class BasicAdapterCommandTest {
    private static final Logger log = LoggerFactory.getLogger(BasicAdapterCommandTest.class);
    private static void announce(String message) {
        System.out.println("TC: " + message);
    }

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

    static final class ImmediateRaftClient extends NoopRaftClient {
        private final Map<String, RaftNode> nodesById;

        ImmediateRaftClient(Map<String, RaftNode> nodesById) {
            this.nodesById = nodesById;
        }

        @Override
        public java.util.concurrent.CompletableFuture<List<VoteResponse>> requestVoteFromAll(java.util.Collection<Peer> peers, VoteRequest voteReq) {
            java.util.concurrent.CompletableFuture<List<VoteResponse>> promise = new java.util.concurrent.CompletableFuture<>();
            java.util.ArrayList<VoteResponse> responses = new java.util.ArrayList<>();
            for (Peer peer : peers) {
                RaftNode node = nodesById.get(peer.getId());
                if (node != null) {
                    responses.add(node.handleVoteRequest(voteReq));
                }
            }
            promise.complete(responses);
            return promise;
        }

        @Override
        public java.util.concurrent.CompletableFuture<AppendEntriesResponse> sendAppendEntries(Peer peer, AppendEntriesRequest req) {
            java.util.concurrent.CompletableFuture<AppendEntriesResponse> future = new java.util.concurrent.CompletableFuture<>();
            RaftNode node = nodesById.get(peer.getId());
            if (node == null) {
                future.complete(new AppendEntriesResponse(req.getTerm(), peer.getId(), false, -1));
            } else {
                future.complete(node.handleAppendEntries(req));
            }
            return future;
        }
    }

    static final class VoteOnlyRaftClient extends NoopRaftClient {
        private final Map<String, RaftNode> nodesById;

        VoteOnlyRaftClient(Map<String, RaftNode> nodesById) {
            this.nodesById = nodesById;
        }

        @Override
        public java.util.concurrent.CompletableFuture<List<VoteResponse>> requestVoteFromAll(java.util.Collection<Peer> peers, VoteRequest voteReq) {
            java.util.concurrent.CompletableFuture<List<VoteResponse>> promise = new java.util.concurrent.CompletableFuture<>();
            java.util.ArrayList<VoteResponse> responses = new java.util.ArrayList<>();
            for (Peer peer : peers) {
                RaftNode node = nodesById.get(peer.getId());
                if (node != null) {
                    responses.add(node.handleVoteRequest(voteReq));
                }
            }
            promise.complete(responses);
            return promise;
        }

        @Override
        public java.util.concurrent.CompletableFuture<AppendEntriesResponse> sendAppendEntries(Peer peer, AppendEntriesRequest req) {
            return java.util.concurrent.CompletableFuture.completedFuture(
                    new AppendEntriesResponse(req.getTerm(), peer.getId(), false, -1)
            );
        }
    }

    static final class ForwardingProbeRaftClient extends NoopRaftClient {
        private Peer forwardedPeer;
        private JoinClusterRequest forwardedJoinRequest;

        @Override
        public java.util.concurrent.CompletableFuture<JoinClusterResponse> sendJoinClusterRequest(Peer peer, JoinClusterRequest request) {
            forwardedPeer = peer;
            forwardedJoinRequest = request;
            return java.util.concurrent.CompletableFuture.completedFuture(
                    new JoinClusterResponse(request.getTerm(), peer.getId(), true, "FORWARDED", "forwarded", peer.getId())
            );
        }
    }

    static class TestAdapter extends BasicAdapter {
        private final SharedSecretCommandAuthenticator authenticator;

        TestAdapter(Peer me, List<Peer> peers) {
            this(me, peers, null);
        }

        TestAdapter(Peer me, List<Peer> peers, SharedSecretCommandAuthenticator authenticator) {
            super(100, me, peers);
            this.authenticator = authenticator;
        }

        void bind(RaftNode node) {
            this.stateMachine = node;
        }

        JoinClusterResponse join(JoinClusterRequest request) {
            return handleJoinClusterRequest(request);
        }

        ClientCommandResponse command(ClientCommandRequest request) {
            return handleClientCommandRequest(request);
        }

        ClientQueryResponse query(ClientQueryRequest request) {
            return handleClientQueryRequest(request);
        }

        JoinClusterStatusResponse joinStatus(JoinClusterStatusRequest request) {
            return handleJoinClusterStatusRequest(request);
        }

        ReconfigureClusterResponse reconfigure(ReconfigureClusterRequest request) {
            return handleReconfigureClusterRequest(request);
        }

        TelemetryResponse telemetry(TelemetryRequest request) {
            return handleTelemetryRequest(request);
        }

        ClusterSummaryResponse clusterSummary(ClusterSummaryRequest request) {
            return handleClusterSummaryRequest(request);
        }

        ReconfigurationStatusResponse reconfigurationStatus(ReconfigurationStatusRequest request) {
            return handleReconfigurationStatusRequest(request);
        }

        @Override
        protected org.gautelis.raft.bootstrap.ClientCommandAuthenticator clientCommandAuthenticator() {
            return authenticator == null ? super.clientCommandAuthenticator() : authenticator;
        }
    }

    static final class ReferenceDataTestAdapter extends ReferenceDataAdapter {
        ReferenceDataTestAdapter(Peer me, List<Peer> peers, Set<String> allowedRequesterIds) {
            super(100, me, peers, allowedRequesterIds);
        }

        ReferenceDataTestAdapter(Peer me, List<Peer> peers, Set<String> allowedRequesterIds, SharedSecretCommandAuthenticator authenticator) {
            super(100, me, peers, null, allowedRequesterIds, authenticator);
        }

        void bind(RaftNode node) {
            this.stateMachine = node;
        }

        ClientCommandResponse command(ClientCommandRequest request) {
            return handleClientCommandRequest(request);
        }
    }

    private static Peer peer(String id) {
        return new Peer(id, null);
    }

    private static Peer learner(String id) {
        return new Peer(id, null, Peer.Role.LEARNER);
    }

    private static TestRaftNodeBuilder raftNode(Peer me) {
        return TestRaftNodeBuilder.forPeer(me).withTimeoutMillis(100);
    }

    private static RaftNode newTestNode(
            Peer me,
            List<Peer> peers,
            SnapshotStateMachine stateMachine,
            RaftClient raftClient,
            LogStore logStore,
            PersistentStateStore persistentStateStore,
            RaftNode.TimeSource timeSource,
            int randomSeed
    ) {
        return raftNode(me)
                .withPeers(peers)
                .withStateMachine(stateMachine)
                .withClient(raftClient)
                .withLogStore(logStore)
                .withPersistentStateStore(persistentStateStore)
                .withTimeSource(timeSource)
                .withRandom(new Random(randomSeed))
                .build();
    }

    private static RaftNode newTestNode(
            Peer me,
            List<Peer> peers,
            SnapshotStateMachine stateMachine,
            RaftClient raftClient,
            RaftNode.TimeSource timeSource,
            int randomSeed
    ) {
        return newTestNode(
                me,
                peers,
                stateMachine,
                raftClient,
                new InMemoryLogStore(),
                new InMemoryPersistentStateStore(),
                timeSource,
                randomSeed
        );
    }

    private static RaftNode newTestNode(
            Peer me,
            List<Peer> peers,
            MessageHandler messageHandler,
            SnapshotStateMachine stateMachine,
            RaftClient raftClient,
            RaftNode.TimeSource timeSource,
            int randomSeed
    ) {
        return raftNode(me)
                .withPeers(peers)
                .withMessageHandler(messageHandler)
                .withStateMachine(stateMachine)
                .withClient(raftClient)
                .withTimeSource(timeSource)
                .withRandom(new Random(randomSeed))
                .build();
    }

    @Test
    void leaderAcceptsValidClusterCommandAndAppliesThroughLog() {
        log.info("TC: Leader command ingestion: verifies typed client commands are submitted through Raft log and applied to state machine");
        Peer me = peer("A");
        MutableTime time = new MutableTime(0);
        KeyValueStateMachine kv = new KeyValueStateMachine();
        InMemoryLogStore store = new InMemoryLogStore();
        RaftNode node = newTestNode(me, List.of(), kv, new NoopRaftClient(), store, new InMemoryPersistentStateStore(), time, 1);

        node.setLastHeartbeatMillisForTest(0);
        time.set(10_000);
        node.electionTickForTest();
        node.handleVoteResponsesForTest(List.of(), 1); // single-node election => leader

        TestAdapter adapter = new TestAdapter(me, List.of());
        adapter.bind(node);

        ClientCommandResponse response = adapter.command(new ClientCommandRequest(
                node.getTerm(), "client", StateMachineCommand.put("x", "42").encode()
        ));

        assertTrue(response.isSuccess());
        assertEquals("ACCEPTED", response.getStatus());
        assertEquals("Command committed and applied", response.getMessage());
        assertEquals("42", kv.get("x"));
        assertEquals(2, store.lastIndex());
    }

    @Test
    void leaderAcceptsCasCommandAndReturnsTypedResult() {
        log.info("TC: Leader CAS command: verifies adapter applies CAS through the normal client command API and returns typed result payload");
        Peer me = peer("A");
        MutableTime time = new MutableTime(0);
        KeyValueStateMachine kv = new KeyValueStateMachine();
        InMemoryLogStore store = new InMemoryLogStore();
        RaftNode node = newTestNode(me, List.of(), kv, new NoopRaftClient(), store, new InMemoryPersistentStateStore(), time, 1);

        node.setLastHeartbeatMillisForTest(0);
        time.set(10_000);
        node.electionTickForTest();
        node.handleVoteResponsesForTest(List.of(), 1);
        node.submitCommand(StateMachineCommand.put("x", "1").encode());

        TestAdapter adapter = new TestAdapter(me, List.of());
        adapter.bind(node);

        ClientCommandResponse response = adapter.command(new ClientCommandRequest(
                node.getTerm(), "client", StateMachineCommand.cas("x", "1", "2").encode()
        ));

        assertTrue(response.isSuccess());
        assertEquals("ACCEPTED", response.getStatus());
        StateMachineCommandResult result = StateMachineCommandResult.decode(response.getResult()).orElseThrow();
        assertEquals(StateMachineCommandResult.Type.CAS, result.getType());
        assertEquals("x", result.getKey());
        assertTrue(result.isMatched());
        assertTrue(result.isCurrentPresent());
        assertEquals("2", result.getCurrentValue());
        assertEquals("2", kv.get("x"));
        assertEquals(3, store.lastIndex());
    }

    @Test
    void leaderReturnsSuccessfulCasResponseWhenCompareFails() {
        log.info("TC: Leader CAS mismatch: verifies applied CAS mismatch is reported through typed result while preserving state");
        Peer me = peer("A");
        MutableTime time = new MutableTime(0);
        KeyValueStateMachine kv = new KeyValueStateMachine();
        InMemoryLogStore store = new InMemoryLogStore();
        RaftNode node = newTestNode(me, List.of(), kv, new NoopRaftClient(), store, new InMemoryPersistentStateStore(), time, 1);

        node.setLastHeartbeatMillisForTest(0);
        time.set(10_000);
        node.electionTickForTest();
        node.handleVoteResponsesForTest(List.of(), 1);
        node.submitCommand(StateMachineCommand.put("x", "1").encode());

        TestAdapter adapter = new TestAdapter(me, List.of());
        adapter.bind(node);

        ClientCommandResponse response = adapter.command(new ClientCommandRequest(
                node.getTerm(), "client", StateMachineCommand.cas("x", "wrong", "2").encode()
        ));

        assertTrue(response.isSuccess());
        assertEquals("ACCEPTED", response.getStatus());
        StateMachineCommandResult result = StateMachineCommandResult.decode(response.getResult()).orElseThrow();
        assertFalse(result.isMatched());
        assertTrue(result.isExpectedPresent());
        assertEquals("wrong", result.getExpectedValue());
        assertTrue(result.isCurrentPresent());
        assertEquals("1", result.getCurrentValue());
        assertEquals("1", kv.get("x"));
        assertEquals(3, store.lastIndex());
    }

    @Test
    void invalidClusterCommandIsRejected() {
        log.info("TC: Command validation reject path: verifies malformed typed client commands are rejected without log mutation");
        Peer me = peer("A");
        MutableTime time = new MutableTime(0);
        KeyValueStateMachine kv = new KeyValueStateMachine();
        InMemoryLogStore store = new InMemoryLogStore();
        RaftNode node = newTestNode(me, List.of(), kv, new NoopRaftClient(), store, new InMemoryPersistentStateStore(), time, 1);

        node.setLastHeartbeatMillisForTest(0);
        time.set(10_000);
        node.electionTickForTest();
        node.handleVoteResponsesForTest(List.of(), 1); // single-node election => leader

        TestAdapter adapter = new TestAdapter(me, List.of());
        adapter.bind(node);

        ClientCommandResponse response = adapter.command(new ClientCommandRequest(
                node.getTerm(), "client", new byte[] {0x01}
        ));

        assertFalse(response.isSuccess());
        assertEquals("INVALID", response.getStatus());
        assertNull(kv.get("x"));
        assertEquals(1, store.lastIndex());
    }

    @Test
    void followerRedirectsWritesUnderDefaultAdapterPolicy() {
        announce("Default adapter policy: followers redirect writes to the known leader");
        Peer a = peer("A");
        Peer b = peer("B");
        MutableTime time = new MutableTime(0);
        RaftNode follower = newTestNode(b, List.of(a), new KeyValueStateMachine(), new NoopRaftClient(), time, 1);
        follower.handleAppendEntries(new AppendEntriesRequest(1L, "A", 0L, 0L, 0L, List.of()));

        TestAdapter adapter = new TestAdapter(b, List.of(a));
        adapter.bind(follower);

        ClientCommandResponse response = adapter.command(new ClientCommandRequest(
                follower.getTerm(), "client", StateMachineCommand.put("x", "42").encode()
        ));

        assertFalse(response.isSuccess());
        assertEquals("REDIRECT", response.getStatus());
        assertEquals("A", response.getLeaderId());
    }

    @Test
    void referenceDataAdapterRejectsLearnerWritesInsteadOfRedirecting() {
        announce("Reference-data adapter policy: learners reject writes instead of redirecting them");
        Peer a = peer("A");
        Peer learner = new Peer("L", null, Peer.Role.LEARNER);
        MutableTime time = new MutableTime(0);
        RaftNode learnerNode = newTestNode(learner, List.of(a), new ReferenceDataStateMachine(), new NoopRaftClient(), time, 1);
        learnerNode.handleAppendEntries(new AppendEntriesRequest(1L, "A", 0L, 0L, 0L, List.of()));

        ReferenceDataTestAdapter adapter = new ReferenceDataTestAdapter(learner, List.of(a), Set.of());
        adapter.bind(learnerNode);

        ClientCommandResponse response = adapter.command(new ClientCommandRequest(
                learnerNode.getTerm(), "client", ReferenceDataCommand.upsertProduct("p1", "Widget").encode()
        ));

        assertFalse(response.isSuccess());
        assertEquals("REJECTED", response.getStatus());
        assertEquals("A", response.getLeaderId());
    }

    @Test
    void referenceDataAdapterRejectsUnauthorizedLeaderWrites() {
        announce("Reference-data adapter authorization: only allow-listed requesters may modify state");
        Peer me = peer("A");
        MutableTime time = new MutableTime(0);
        ReferenceDataStateMachine referenceData = new ReferenceDataStateMachine();
        InMemoryLogStore store = new InMemoryLogStore();
        RaftNode node = newTestNode(me, List.of(), referenceData, new NoopRaftClient(), store, new InMemoryPersistentStateStore(), time, 1);

        node.setLastHeartbeatMillisForTest(0);
        time.set(10_000);
        node.electionTickForTest();
        node.handleVoteResponsesForTest(List.of(), 1);

        ReferenceDataTestAdapter adapter = new ReferenceDataTestAdapter(me, List.of(), Set.of("reference-admin"));
        adapter.bind(node);

        ClientCommandResponse response = adapter.command(new ClientCommandRequest(
                node.getTerm(), "command-cli", ReferenceDataCommand.upsertProduct("p1", "Widget").encode()
        ));

        assertFalse(response.isSuccess());
        assertEquals("FORBIDDEN", response.getStatus());
        assertNull(referenceData.getProduct("p1"));
        assertEquals(1, store.lastIndex());
    }

    @Test
    void referenceDataAdapterAcceptsAuthorizedLeaderWrites() {
        announce("Reference-data adapter authorization: allow-listed requesters may modify state through the leader");
        Peer me = peer("A");
        MutableTime time = new MutableTime(0);
        ReferenceDataStateMachine referenceData = new ReferenceDataStateMachine();
        InMemoryLogStore store = new InMemoryLogStore();
        RaftNode node = newTestNode(me, List.of(), referenceData, new NoopRaftClient(), store, new InMemoryPersistentStateStore(), time, 1);

        node.setLastHeartbeatMillisForTest(0);
        time.set(10_000);
        node.electionTickForTest();
        node.handleVoteResponsesForTest(List.of(), 1);

        ReferenceDataTestAdapter adapter = new ReferenceDataTestAdapter(me, List.of(), AllowListCommandAuthorizer.parseAllowList("reference-admin"));
        adapter.bind(node);

        ClientCommandResponse response = adapter.command(new ClientCommandRequest(
                node.getTerm(), "reference-admin", ReferenceDataCommand.upsertProduct("p1", "Widget").encode()
        ));

        assertTrue(response.isSuccess());
        assertEquals("ACCEPTED", response.getStatus());
        assertEquals("Command committed and applied", response.getMessage());
        assertNotNull(referenceData.getProduct("p1"));
        assertEquals(2, store.lastIndex());
    }

    @Test
    void referenceDataAdapterRejectsMissingSharedSecret() {
        announce("Reference-data adapter authentication: shared-secret mode rejects missing client credentials");
        Peer me = peer("A");
        MutableTime time = new MutableTime(0);
        ReferenceDataStateMachine referenceData = new ReferenceDataStateMachine();
        InMemoryLogStore store = new InMemoryLogStore();
        RaftNode node = newTestNode(me, List.of(), referenceData, new NoopRaftClient(), store, new InMemoryPersistentStateStore(), time, 1);

        node.setLastHeartbeatMillisForTest(0);
        time.set(10_000);
        node.electionTickForTest();
        node.handleVoteResponsesForTest(List.of(), 1);

        ReferenceDataTestAdapter adapter = new ReferenceDataTestAdapter(
                me,
                List.of(),
                Set.of("reference-admin"),
                new SharedSecretCommandAuthenticator("top-secret")
        );
        adapter.bind(node);

        ClientCommandResponse response = adapter.command(new ClientCommandRequest(
                node.getTerm(), "reference-admin", ReferenceDataCommand.upsertProduct("p1", "Widget").encode()
        ));

        assertFalse(response.isSuccess());
        assertEquals("UNAUTHENTICATED", response.getStatus());
        assertNull(referenceData.getProduct("p1"));
        assertEquals(1, store.lastIndex());
    }

    @Test
    void referenceDataAdapterAcceptsValidSharedSecret() {
        announce("Reference-data adapter authentication: shared-secret mode accepts matching credentials");
        Peer me = peer("A");
        MutableTime time = new MutableTime(0);
        ReferenceDataStateMachine referenceData = new ReferenceDataStateMachine();
        InMemoryLogStore store = new InMemoryLogStore();
        RaftNode node = newTestNode(me, List.of(), referenceData, new NoopRaftClient(), store, new InMemoryPersistentStateStore(), time, 1);

        node.setLastHeartbeatMillisForTest(0);
        time.set(10_000);
        node.electionTickForTest();
        node.handleVoteResponsesForTest(List.of(), 1);

        ReferenceDataTestAdapter adapter = new ReferenceDataTestAdapter(
                me,
                List.of(),
                Set.of("reference-admin"),
                new SharedSecretCommandAuthenticator("top-secret")
        );
        adapter.bind(node);

        ClientCommandResponse response = adapter.command(new ClientCommandRequest(
                node.getTerm(),
                "reference-admin",
                ReferenceDataCommand.upsertProduct("p1", "Widget").encode(),
                "shared-secret",
                "top-secret"
        ));

        assertTrue(response.isSuccess());
        assertEquals("ACCEPTED", response.getStatus());
        assertEquals("Command committed and applied", response.getMessage());
        assertNotNull(referenceData.getProduct("p1"));
        assertEquals(2, store.lastIndex());
    }

    @Test
    void leaderReturnsRetryWhenWriteCannotBeCommittedAndApplied() {
        announce("Typed client command acknowledgement: leader does not acknowledge success before commit/apply");
        Peer a = peer("A");
        Peer b = peer("B");
        MutableTime time = new MutableTime(0);
        Map<String, RaftNode> nodes = new HashMap<>();
        VoteOnlyRaftClient clientA = new VoteOnlyRaftClient(nodes);
        VoteOnlyRaftClient clientB = new VoteOnlyRaftClient(nodes);
        KeyValueStateMachine kv = new KeyValueStateMachine();
        InMemoryLogStore store = new InMemoryLogStore();
        RaftNode node = newTestNode(a, List.of(b), kv, clientA, store, new InMemoryPersistentStateStore(), time, 1);
        RaftNode follower = newTestNode(b, List.of(a), new KeyValueStateMachine(), clientB, time, 2);
        nodes.put("A", node);
        nodes.put("B", follower);

        node.setLastHeartbeatMillisForTest(0);
        time.set(10_000);
        node.electionTickForTest();
        assertTrue(node.isLeader());

        TestAdapter adapter = new TestAdapter(a, List.of(b));
        adapter.bind(node);

        ClientCommandResponse response = adapter.command(new ClientCommandRequest(
                node.getTerm(), "client", StateMachineCommand.put("x", "42").encode()
        ));

        assertFalse(response.isSuccess());
        assertEquals("RETRY", response.getStatus());
        assertEquals("Command was not committed and applied before acknowledgement", response.getMessage());
        assertNull(kv.get("x"));
        assertEquals(0, node.getCommitIndexForTest());
        assertEquals(0, node.getLastAppliedForTest());
        assertEquals(2, store.lastIndex());
    }

    @Test
    void queryRequestRejectsMissingSharedSecret() {
        announce("Query authentication: shared-secret mode rejects unauthenticated linearizable reads");
        Peer me = peer("A");
        MutableTime time = new MutableTime(0);
        KeyValueStateMachine kv = new KeyValueStateMachine();
        kv.apply(1L, StateMachineCommand.put("x", "42").encode());
        InMemoryLogStore store = new InMemoryLogStore();
        RaftNode node = newTestNode(me, List.of(), kv, new NoopRaftClient(), store, new InMemoryPersistentStateStore(), time, 1);

        node.setLastHeartbeatMillisForTest(0);
        time.set(10_000);
        node.electionTickForTest();
        node.handleVoteResponsesForTest(List.of(), 1);

        TestAdapter adapter = new TestAdapter(me, List.of(), new SharedSecretCommandAuthenticator("top-secret"));
        adapter.bind(node);

        ClientQueryResponse response = adapter.query(new ClientQueryRequest(
                node.getTerm(), "query-cli", StateMachineQuery.get("x").encode()
        ));

        assertFalse(response.isSuccess());
        assertEquals("UNAUTHENTICATED", response.getStatus());
    }

    @Test
    void telemetryRequestAcceptsValidSharedSecret() {
        announce("Admin authentication: shared-secret mode accepts authenticated telemetry requests");
        Peer me = peer("A");
        MutableTime time = new MutableTime(0);
        RaftNode node = newTestNode(me, List.of(), new KeyValueStateMachine(), new NoopRaftClient(), time, 1);

        node.setLastHeartbeatMillisForTest(0);
        time.set(10_000);
        node.electionTickForTest();
        node.handleVoteResponsesForTest(List.of(), 1);

        TestAdapter adapter = new TestAdapter(me, List.of(), new SharedSecretCommandAuthenticator("top-secret"));
        adapter.bind(node);

        TelemetryResponse response = adapter.telemetry(new TelemetryRequest(
                node.getTerm(),
                "telemetry-cli",
                true,
                false,
                "shared-secret",
                "top-secret"
        ));

        assertTrue(response.isSuccess());
        assertEquals("OK", response.getStatus());
    }

    @Test
    void followerRejectsValidClusterCommand() {
        log.info("TC: Follower command rejection: verifies non-leader nodes reject typed client commands");
        Peer me = peer("A");
        KeyValueStateMachine kv = new KeyValueStateMachine();
        InMemoryLogStore store = new InMemoryLogStore();
        RaftNode node = newTestNode(me, List.of(), kv, new NoopRaftClient(), store, new InMemoryPersistentStateStore(), System::currentTimeMillis, 1);

        TestAdapter adapter = new TestAdapter(me, List.of());
        adapter.bind(node);

        ClientCommandResponse response = adapter.command(new ClientCommandRequest(
                node.getTerm(), "client", StateMachineCommand.put("y", "7").encode()
        ));

        assertFalse(response.isSuccess());
        assertEquals("REJECTED", response.getStatus());
        assertNull(kv.get("y"));
        assertEquals(0, store.lastIndex());
    }

    @Test
    void typedJoinCanAdmitLearnerWithoutChangingVotingQuorum() {
        announce("Typed learner join: leader admits learner member without expanding voting quorum");
        Peer a = peer("A");
        Peer d = new Peer("D", new java.net.InetSocketAddress("127.0.0.1", 10083), Peer.Role.LEARNER);

        MutableTime time = new MutableTime(0);
        RaftNode nodeA = newTestNode(a, List.of(), new KeyValueStateMachine(), new NoopRaftClient(), time, 1);

        nodeA.setLastHeartbeatMillisForTest(0);
        time.set(10_000);
        nodeA.electionTickForTest();
        nodeA.handleVoteResponsesForTest(List.of(), 1);
        assertTrue(nodeA.isLeader());

        TestAdapter adapter = new TestAdapter(a, List.of());
        adapter.bind(nodeA);

        JoinClusterResponse joinResponse = adapter.join(new JoinClusterRequest(nodeA.getTerm(), "client", d));
        assertTrue(joinResponse.isSuccess());
        assertTrue(nodeA.getClusterConfigurationForTest().contains("D"));
        assertTrue(nodeA.getClusterConfigurationForTest().isLearner("D"));

        TelemetryResponse telemetry = adapter.telemetry(new TelemetryRequest(nodeA.getTerm(), "client", false, true));
        assertEquals(1, telemetry.getVotingMembers());
        assertTrue(telemetry.getKnownPeers().stream().anyMatch(peer -> "D".equals(peer.getId()) && peer.isLearner()));

        ClusterSummaryResponse summary = adapter.clusterSummary(new ClusterSummaryRequest(nodeA.getTerm(), "client"));
        assertTrue(summary.getMembers().stream().anyMatch(member -> "D".equals(member.peerId()) && !member.voting() && "LEARNER".equals(member.role())));
    }

    @Test
    void typedPromoteLearnerChangesVotingRole() {
        announce("Typed learner promotion: leader promotes learner into voter role through structured request");
        Peer a = peer("A");
        Peer d = learner("D");

        MutableTime time = new MutableTime(0);
        Map<String, RaftNode> nodes = new HashMap<>();
        RaftNodeElectionTest.QueuedRaftClient clientA = new RaftNodeElectionTest.QueuedRaftClient("A", nodes);
        RaftNodeElectionTest.QueuedRaftClient clientD = new RaftNodeElectionTest.QueuedRaftClient("D", nodes);
        RaftNode nodeA = newTestNode(a, List.of(d), new KeyValueStateMachine(), clientA, time, 1);
        RaftNode nodeD = newTestNode(d, List.of(a), new KeyValueStateMachine(), clientD, time, 2);
        nodes.put("A", nodeA);
        nodes.put("D", nodeD);

        nodeA.transitionToJointConfigurationForTest(List.of(a, d));
        nodeA.finalizeConfigurationTransitionForTest();
        nodeA.setLastHeartbeatMillisForTest(0);
        time.set(10_000);
        nodeA.electionTickForTest();
        nodeA.handleVoteResponsesForTest(List.of(), 1);
        assertTrue(nodeA.isLeader());

        TestAdapter adapter = new TestAdapter(a, List.of(d));
        adapter.bind(nodeA);

        ReconfigureClusterResponse response = adapter.reconfigure(new ReconfigureClusterRequest(
                nodeA.getTerm(), "client", ReconfigureClusterRequest.Action.PROMOTE, List.of(d)
        ));
        assertTrue(response.isSuccess());
        ClusterSummaryResponse summary = adapter.clusterSummary(new ClusterSummaryRequest(nodeA.getTerm(), "client"));
        assertTrue(summary.getMembers().stream().anyMatch(member ->
                "D".equals(member.peerId())
                        && "LEARNER".equals(member.currentRole())
                        && "VOTER".equals(member.nextRole())
                        && "promoting".equals(member.roleTransition())));
        TelemetryResponse telemetry = adapter.telemetry(new TelemetryRequest(nodeA.getTerm(), "client", false, false));
        assertTrue(telemetry.getClusterMembers().stream().anyMatch(member ->
                "D".equals(member.peerId())
                        && "LEARNER".equals(member.currentRole())
                        && "VOTER".equals(member.nextRole())
                        && "promoting".equals(member.roleTransition())));
        for (int i = 0; i < 4; i++) {
            nodeA.heartbeatTickForTest();
            clientA.flush();
        }
        ClusterConfiguration submitted = nodeA.getConfigurationAtIndexForTest(nodeA.telemetrySnapshot().lastLogIndex());
        assertTrue(submitted.isVoter("D"));
        assertFalse(submitted.isLearner("D"));
    }

    @Test
    void typedDemoteVoterChangesReplicationOnlyRole() {
        announce("Typed voter demotion: leader demotes voter into learner role through structured request");
        Peer a = peer("A");
        Peer d = peer("D");

        MutableTime time = new MutableTime(0);
        Map<String, RaftNode> nodes = new HashMap<>();
        RaftNodeElectionTest.QueuedRaftClient clientA = new RaftNodeElectionTest.QueuedRaftClient("A", nodes);
        RaftNodeElectionTest.QueuedRaftClient clientD = new RaftNodeElectionTest.QueuedRaftClient("D", nodes);
        RaftNode nodeA = newTestNode(a, List.of(d), new KeyValueStateMachine(), clientA, time, 1);
        RaftNode nodeD = newTestNode(d, List.of(a), new KeyValueStateMachine(), clientD, time, 2);
        nodes.put("A", nodeA);
        nodes.put("D", nodeD);

        nodeA.setLastHeartbeatMillisForTest(0);
        time.set(10_000);
        nodeA.electionTickForTest();
        nodeA.handleVoteResponsesForTest(List.of(
                new VoteResponse(new VoteRequest(1L, "A", 0L, 0L), "D", true, 1L)
        ), 1);
        assertTrue(nodeA.isLeader());

        TestAdapter adapter = new TestAdapter(a, List.of(d));
        adapter.bind(nodeA);

        ReconfigureClusterResponse response = adapter.reconfigure(new ReconfigureClusterRequest(
                nodeA.getTerm(), "client", ReconfigureClusterRequest.Action.DEMOTE, List.of(d)
        ));
        assertTrue(response.isSuccess());
        ClusterSummaryResponse summary = adapter.clusterSummary(new ClusterSummaryRequest(nodeA.getTerm(), "client"));
        assertTrue(summary.getMembers().stream().anyMatch(member ->
                "D".equals(member.peerId())
                        && "VOTER".equals(member.currentRole())
                        && "LEARNER".equals(member.nextRole())
                        && "demoting".equals(member.roleTransition())));
        TelemetryResponse telemetry = adapter.telemetry(new TelemetryRequest(nodeA.getTerm(), "client", false, false));
        assertTrue(telemetry.getClusterMembers().stream().anyMatch(member ->
                "D".equals(member.peerId())
                        && "VOTER".equals(member.currentRole())
                        && "LEARNER".equals(member.nextRole())
                        && "demoting".equals(member.roleTransition())));
        for (int i = 0; i < 4; i++) {
            nodeA.heartbeatTickForTest();
            clientA.flush();
        }
        ClusterConfiguration submitted = nodeA.getConfigurationAtIndexForTest(nodeA.telemetrySnapshot().lastLogIndex());
        assertTrue(submitted.isLearner("D"));
        assertFalse(submitted.isVoter("D"));
    }

    @Test
    void followerRedirectsClientCommandToKnownLeaderEndpoint() {
        announce("Typed client redirect: follower returns leader id and endpoint for ordinary writes");
        Peer a = new Peer("A", new java.net.InetSocketAddress("127.0.0.1", 10080));
        Peer b = new Peer("B", new java.net.InetSocketAddress("127.0.0.1", 10081));

        RaftNodeElectionTest.MutableTime time = new RaftNodeElectionTest.MutableTime(0);
        Map<String, RaftNode> nodes = new HashMap<>();
        RaftNodeElectionTest.QueuedRaftClient clientA = new RaftNodeElectionTest.QueuedRaftClient("A", nodes);
        RaftNodeElectionTest.QueuedRaftClient clientB = new RaftNodeElectionTest.QueuedRaftClient("B", nodes);

        RaftNode nodeA = newTestNode(a, List.of(b), new KeyValueStateMachine(), clientA, time, 1);
        RaftNode nodeB = newTestNode(b, List.of(a), new KeyValueStateMachine(), clientB, time, 2);
        nodes.put("A", nodeA);
        nodes.put("B", nodeB);

        nodeA.setLastHeartbeatMillisForTest(0);
        time.set(10_000);
        nodeA.electionTickForTest();
        clientA.flush();
        nodeA.heartbeatTickForTest();
        clientA.flush();

        TestAdapter adapterB = new TestAdapter(b, List.of(a));
        adapterB.bind(nodeB);

        ClientCommandResponse response = adapterB.command(new ClientCommandRequest(
                nodeB.getTerm(), "client", StateMachineCommand.put("redirected", "1").encode()
        ));

        assertFalse(response.isSuccess());
        assertEquals("REDIRECT", response.getStatus());
        assertEquals("A", response.getLeaderId());
        assertEquals("127.0.0.1", response.getLeaderHost());
        assertEquals(10080, response.getLeaderPort());
    }

    @Test
    void leaderAnswersTypedClientQueryFromStateMachine() {
        announce("Typed client query: leader serves key lookup through structured query API");
        Peer me = peer("A");
        MutableTime time = new MutableTime(0);
        KeyValueStateMachine kv = new KeyValueStateMachine();
        InMemoryLogStore store = new InMemoryLogStore();
        RaftNode node = newTestNode(me, List.of(), kv, new NoopRaftClient(), store, new InMemoryPersistentStateStore(), time, 1);

        node.setLastHeartbeatMillisForTest(0);
        time.set(10_000);
        node.electionTickForTest();
        node.handleVoteResponsesForTest(List.of(), 1);
        node.submitCommand(StateMachineCommand.put("x", "42").encode());

        TestAdapter adapter = new TestAdapter(me, List.of());
        adapter.bind(node);

        ClientQueryResponse response = adapter.query(new ClientQueryRequest(
                node.getTerm(), "client", StateMachineQuery.get("x").encode()
        ));

        assertTrue(response.isSuccess());
        assertEquals("OK", response.getStatus());
        StateMachineQueryResult result = StateMachineQueryResult.decode(response.getResult()).orElseThrow();
        assertTrue(result.isFound());
        assertEquals("42", result.getValue());
    }

    @Test
    void followerRedirectsTypedClientQueryToKnownLeaderEndpoint() {
        announce("Typed query redirect: follower returns leader id and endpoint for ordinary reads");
        Peer a = new Peer("A", new java.net.InetSocketAddress("127.0.0.1", 10080));
        Peer b = new Peer("B", new java.net.InetSocketAddress("127.0.0.1", 10081));

        RaftNodeElectionTest.MutableTime time = new RaftNodeElectionTest.MutableTime(0);
        Map<String, RaftNode> nodes = new HashMap<>();
        RaftNodeElectionTest.QueuedRaftClient clientA = new RaftNodeElectionTest.QueuedRaftClient("A", nodes);
        RaftNodeElectionTest.QueuedRaftClient clientB = new RaftNodeElectionTest.QueuedRaftClient("B", nodes);

        RaftNode nodeA = newTestNode(a, List.of(b), new KeyValueStateMachine(), clientA, time, 1);
        RaftNode nodeB = newTestNode(b, List.of(a), new KeyValueStateMachine(), clientB, time, 2);
        nodes.put("A", nodeA);
        nodes.put("B", nodeB);

        nodeA.setLastHeartbeatMillisForTest(0);
        time.set(10_000);
        nodeA.electionTickForTest();
        clientA.flush();
        nodeA.heartbeatTickForTest();
        clientA.flush();

        TestAdapter adapterB = new TestAdapter(b, List.of(a));
        adapterB.bind(nodeB);

        ClientQueryResponse response = adapterB.query(new ClientQueryRequest(
                nodeB.getTerm(), "client", StateMachineQuery.get("redirected").encode()
        ));

        assertFalse(response.isSuccess());
        assertEquals("REDIRECT", response.getStatus());
        assertEquals("A", response.getLeaderId());
        assertEquals("127.0.0.1", response.getLeaderHost());
        assertEquals(10080, response.getLeaderPort());
    }

    @Test
    void leaderRejectsTypedClientQueryWhenLinearizableReadLeaseIsStale() {
        announce("Typed query lease: leader rejects query when quorum-backed linearizable read lease is stale");
        Peer a = new Peer("A", null);
        Peer b = new Peer("B", null);

        RaftNodeElectionTest.MutableTime time = new RaftNodeElectionTest.MutableTime(0);
        Map<String, RaftNode> nodes = new HashMap<>();
        RaftNodeElectionTest.QueuedRaftClient clientA = new RaftNodeElectionTest.QueuedRaftClient("A", nodes);
        RaftNodeElectionTest.QueuedRaftClient clientB = new RaftNodeElectionTest.QueuedRaftClient("B", nodes);

        RaftNode nodeA = newTestNode(a, List.of(b), new KeyValueStateMachine(), clientA, time, 1);
        RaftNode nodeB = newTestNode(b, List.of(a), new KeyValueStateMachine(), clientB, time, 2);
        nodes.put("A", nodeA);
        nodes.put("B", nodeB);

        nodeA.setLastHeartbeatMillisForTest(0);
        time.set(10_000);
        nodeA.electionTickForTest();
        clientA.flush();
        nodeA.heartbeatTickForTest();
        clientA.flush();
        assertTrue(nodeA.isLeader());

        TestAdapter adapterA = new TestAdapter(a, List.of(b));
        adapterA.bind(nodeA);

        time.set(10_100);
        ClientQueryResponse response = adapterA.query(new ClientQueryRequest(
                nodeA.getTerm(), "client", StateMachineQuery.get("x").encode()
        ));

        assertFalse(response.isSuccess());
        assertEquals("RETRY", response.getStatus());
    }

    @Test
    void leaderRefreshesLinearizableReadLeaseViaReadBarrierHeartbeat() {
        announce("Typed query barrier: leader refreshes stale linearizable read lease via quorum heartbeat before serving query");
        Peer a = new Peer("A", null);
        Peer b = new Peer("B", null);

        MutableTime time = new MutableTime(0);
        Map<String, RaftNode> nodes = new HashMap<>();
        ImmediateRaftClient clientA = new ImmediateRaftClient(nodes);
        ImmediateRaftClient clientB = new ImmediateRaftClient(nodes);

        RaftNode nodeA = newTestNode(a, List.of(b), new KeyValueStateMachine(), clientA, time, 1);
        RaftNode nodeB = newTestNode(b, List.of(a), new KeyValueStateMachine(), clientB, time, 2);
        nodes.put("A", nodeA);
        nodes.put("B", nodeB);

        nodeA.setLastHeartbeatMillisForTest(0);
        time.set(10_000);
        nodeA.electionTickForTest();
        nodeA.handleVoteResponsesForTest(List.of(
                new VoteResponse(new VoteRequest(1L, "A", 0L, 0L), "B", true, 1L)
        ), 1);
        assertTrue(nodeA.isLeader());

        nodeA.submitCommand(StateMachineCommand.put("x", "42").encode());

        TestAdapter adapterA = new TestAdapter(a, List.of(b));
        adapterA.bind(nodeA);

        time.set(10_100);
        assertFalse(nodeA.canServeLinearizableRead());

        ClientQueryResponse response = adapterA.query(new ClientQueryRequest(
                nodeA.getTerm(), "client", StateMachineQuery.get("x").encode()
        ));

        assertTrue(response.isSuccess());
        assertEquals("OK", response.getStatus());
        StateMachineQueryResult result = StateMachineQueryResult.decode(response.getResult()).orElseThrow();
        assertTrue(result.isFound());
        assertEquals("42", result.getValue());
        assertTrue(nodeA.canServeLinearizableRead());
    }

    @Test
    void telemetrySummarizesNodeStateAndStats() {
        announce("Telemetry snapshot: adapter exposes node state, membership, and transport statistics");
        Peer me = peer("A");
        MutableTime time = new MutableTime(0);
        NoopRaftClient client = new NoopRaftClient();
        RaftNode node = newTestNode(me, List.of(), new KeyValueStateMachine(), client, time, 1);

        node.setLastHeartbeatMillisForTest(0);
        time.set(10_000);
        node.electionTickForTest();
        node.handleVoteResponsesForTest(List.of(), 1);

        TestAdapter adapter = new TestAdapter(me, List.of());
        adapter.bind(node);

        TelemetryResponse telemetry = adapter.telemetry(new TelemetryRequest(node.getTerm(), "client", true, false));
        assertTrue(telemetry.isSuccess());
        assertEquals("OK", telemetry.getStatus());
        assertEquals("LEADER", telemetry.getState());
        assertEquals(node.getCommitIndexForTest(), telemetry.getCommitIndex());
        assertEquals("A", telemetry.getLeaderId());
        assertFalse(telemetry.isJointConsensus());
        assertEquals("healthy", telemetry.getClusterHealth());
        assertEquals("all-voters-healthy", telemetry.getClusterStatusReason());
        assertTrue(telemetry.isQuorumAvailable());
        assertEquals(1, telemetry.getVotingMembers());
        assertEquals(1, telemetry.getHealthyVotingMembers());
    }

    @Test
    void telemetryRedirectsLeaderSummaryRequestsFromFollowers() {
        announce("Telemetry redirect: follower points leader-summary request at known leader");
        Peer a = new Peer("A", null);
        Peer b = new Peer("B", null);

        RaftNodeElectionTest.MutableTime time = new RaftNodeElectionTest.MutableTime(0);
        Map<String, RaftNode> nodes = new HashMap<>();
        RaftNodeElectionTest.QueuedRaftClient clientA = new RaftNodeElectionTest.QueuedRaftClient("A", nodes);
        RaftNodeElectionTest.QueuedRaftClient clientB = new RaftNodeElectionTest.QueuedRaftClient("B", nodes);

        RaftNode nodeA = newTestNode(a, List.of(b), new KeyValueStateMachine(), clientA, time, 1);
        RaftNode nodeB = newTestNode(b, List.of(a), new KeyValueStateMachine(), clientB, time, 2);
        nodes.put("A", nodeA);
        nodes.put("B", nodeB);

        nodeA.setLastHeartbeatMillisForTest(0);
        time.set(10_000);
        nodeA.electionTickForTest();
        clientA.flush();
        nodeA.heartbeatTickForTest();
        clientA.flush();

        TestAdapter adapterB = new TestAdapter(b, List.of(a));
        adapterB.bind(nodeB);
        TelemetryResponse telemetry = adapterB.telemetry(new TelemetryRequest(nodeB.getTerm(), "client", false, true));

        assertFalse(telemetry.isSuccess());
        assertEquals("REDIRECT", telemetry.getStatus());
        assertEquals("A", telemetry.getRedirectLeaderId());
    }

    @Test
    void clusterSummaryRedirectsFollowersToLeaderEndpoint() {
        announce("Cluster summary redirect: follower points cluster summary request at known leader endpoint");
        Peer a = new Peer("A", new java.net.InetSocketAddress("127.0.0.1", 10080));
        Peer b = new Peer("B", new java.net.InetSocketAddress("127.0.0.1", 10081));

        RaftNodeElectionTest.MutableTime time = new RaftNodeElectionTest.MutableTime(0);
        Map<String, RaftNode> nodes = new HashMap<>();
        RaftNodeElectionTest.QueuedRaftClient clientA = new RaftNodeElectionTest.QueuedRaftClient("A", nodes);
        RaftNodeElectionTest.QueuedRaftClient clientB = new RaftNodeElectionTest.QueuedRaftClient("B", nodes);

        RaftNode nodeA = newTestNode(a, List.of(b), new KeyValueStateMachine(), clientA, time, 1);
        RaftNode nodeB = newTestNode(b, List.of(a), new KeyValueStateMachine(), clientB, time, 2);
        nodes.put("A", nodeA);
        nodes.put("B", nodeB);

        nodeA.setLastHeartbeatMillisForTest(0);
        time.set(10_000);
        nodeA.electionTickForTest();
        clientA.flush();
        nodeA.heartbeatTickForTest();
        clientA.flush();

        TestAdapter adapterB = new TestAdapter(b, List.of(a));
        adapterB.bind(nodeB);
        ClusterSummaryResponse summary = adapterB.clusterSummary(new ClusterSummaryRequest(nodeB.getTerm(), "client"));

        assertFalse(summary.isSuccess());
        assertEquals("REDIRECT", summary.getStatus());
        assertEquals("A", summary.getRedirectLeaderId());
        assertEquals("127.0.0.1", summary.getRedirectLeaderHost());
        assertEquals(10080, summary.getRedirectLeaderPort());
    }

    @Test
    void reconfigurationStatusRedirectsFollowersToLeaderEndpoint() {
        announce("Reconfiguration status redirect: follower points focused reconfiguration status request at known leader endpoint");
        Peer a = new Peer("A", new java.net.InetSocketAddress("127.0.0.1", 10080));
        Peer b = new Peer("B", new java.net.InetSocketAddress("127.0.0.1", 10081));

        RaftNodeElectionTest.MutableTime time = new RaftNodeElectionTest.MutableTime(0);
        Map<String, RaftNode> nodes = new HashMap<>();
        RaftNodeElectionTest.QueuedRaftClient clientA = new RaftNodeElectionTest.QueuedRaftClient("A", nodes);
        RaftNodeElectionTest.QueuedRaftClient clientB = new RaftNodeElectionTest.QueuedRaftClient("B", nodes);

        RaftNode nodeA = newTestNode(a, List.of(b), new KeyValueStateMachine(), clientA, time, 1);
        RaftNode nodeB = newTestNode(b, List.of(a), new KeyValueStateMachine(), clientB, time, 2);
        nodes.put("A", nodeA);
        nodes.put("B", nodeB);

        nodeA.setLastHeartbeatMillisForTest(0);
        time.set(10_000);
        nodeA.electionTickForTest();
        clientA.flush();
        nodeA.heartbeatTickForTest();
        clientA.flush();

        TestAdapter adapterB = new TestAdapter(b, List.of(a));
        adapterB.bind(nodeB);
        ReconfigurationStatusResponse status = adapterB.reconfigurationStatus(new ReconfigurationStatusRequest(nodeB.getTerm(), "client"));

        assertFalse(status.isSuccess());
        assertEquals("REDIRECT", status.getStatus());
        assertEquals("A", status.getRedirectLeaderId());
        assertEquals("127.0.0.1", status.getRedirectLeaderHost());
        assertEquals(10080, status.getRedirectLeaderPort());
    }

    @Test
    void leaderClusterSummaryIncludesMemberHealthView() {
        announce("Cluster summary view: leader exposes aggregate health and per-member cluster view");
        Peer a = new Peer("A", null);
        Peer b = new Peer("B", null);

        RaftNodeElectionTest.MutableTime time = new RaftNodeElectionTest.MutableTime(0);
        Map<String, RaftNode> nodes = new HashMap<>();
        RaftNodeElectionTest.QueuedRaftClient clientA = new RaftNodeElectionTest.QueuedRaftClient("A", nodes);
        RaftNodeElectionTest.QueuedRaftClient clientB = new RaftNodeElectionTest.QueuedRaftClient("B", nodes);

        RaftNode nodeA = newTestNode(a, List.of(b), new KeyValueStateMachine(), clientA, time, 1);
        RaftNode nodeB = newTestNode(b, List.of(a), new KeyValueStateMachine(), clientB, time, 2);
        nodes.put("A", nodeA);
        nodes.put("B", nodeB);

        nodeA.setLastHeartbeatMillisForTest(0);
        time.set(10_000);
        nodeA.electionTickForTest();
        clientA.flush();
        nodeA.heartbeatTickForTest();
        clientA.flush();

        TestAdapter adapterA = new TestAdapter(a, List.of(b));
        adapterA.bind(nodeA);
        ClusterSummaryResponse summary = adapterA.clusterSummary(new ClusterSummaryRequest(nodeA.getTerm(), "client"));

        assertTrue(summary.isSuccess());
        assertEquals("healthy", summary.getClusterHealth());
        assertEquals("all-voters-healthy", summary.getClusterStatusReason());
        assertEquals(2, summary.getMembers().size());
        var follower = summary.getMembers().stream().filter(member -> "B".equals(member.peerId())).findFirst().orElseThrow();
        assertTrue(follower.reachable());
        assertEquals("healthy", follower.health());
    }

    @Test
    void leaderTelemetryIncludesFollowerReachabilityAndFreshnessData() {
        announce("Telemetry replication detail: leader reports follower reachability and last successful contact");
        Peer a = new Peer("A", null);
        Peer b = new Peer("B", null);

        RaftNodeElectionTest.MutableTime time = new RaftNodeElectionTest.MutableTime(0);
        Map<String, RaftNode> nodes = new HashMap<>();
        RaftNodeElectionTest.QueuedRaftClient clientA = new RaftNodeElectionTest.QueuedRaftClient("A", nodes);
        RaftNodeElectionTest.QueuedRaftClient clientB = new RaftNodeElectionTest.QueuedRaftClient("B", nodes);

        RaftNode nodeA = newTestNode(a, List.of(b), new KeyValueStateMachine(), clientA, time, 1);
        RaftNode nodeB = newTestNode(b, List.of(a), new KeyValueStateMachine(), clientB, time, 2);
        nodes.put("A", nodeA);
        nodes.put("B", nodeB);

        nodeA.setLastHeartbeatMillisForTest(0);
        time.set(10_000);
        nodeA.electionTickForTest();
        clientA.flush();
        nodeA.heartbeatTickForTest();
        clientA.flush();

        TestAdapter adapterA = new TestAdapter(a, List.of(b));
        adapterA.bind(nodeA);
        TelemetryResponse telemetry = adapterA.telemetry(new TelemetryRequest(nodeA.getTerm(), "client", false, true));

        assertTrue(telemetry.isSuccess());
        assertFalse(telemetry.getReplication().isEmpty());
        var follower = telemetry.getReplication().stream().filter(item -> "B".equals(item.peerId())).findFirst().orElseThrow();
        assertTrue(follower.lastSuccessfulContactMillis() > 0L);
    }

    @Test
    void leaderTelemetryTracksRepeatedFollowerReplicationFailures() {
        announce("Telemetry follower health: repeated replication failures are exposed in leader telemetry");
        Peer a = new Peer("A", null);
        Peer b = new Peer("B", null);

        RaftNodeElectionTest.MutableTime time = new RaftNodeElectionTest.MutableTime(0);
        Map<String, RaftNode> nodes = new HashMap<>();
        RaftNodeElectionTest.QueuedRaftClient clientA = new RaftNodeElectionTest.QueuedRaftClient("A", nodes);
        RaftNodeElectionTest.QueuedRaftClient clientB = new RaftNodeElectionTest.QueuedRaftClient("B", nodes);

        RaftNode nodeA = newTestNode(a, List.of(b), new KeyValueStateMachine(), clientA, time, 1);
        RaftNode nodeB = newTestNode(b, List.of(a), new KeyValueStateMachine(), clientB, time, 2);
        nodes.put("A", nodeA);
        nodes.put("B", nodeB);

        nodeA.setLastHeartbeatMillisForTest(0);
        time.set(10_000);
        nodeA.electionTickForTest();
        clientA.flush();

        nodes.remove("B");
        nodeA.heartbeatTickForTest();
        clientA.flush();
        time.set(11_000);
        nodeA.heartbeatTickForTest();
        clientA.flush();

        TestAdapter adapterA = new TestAdapter(a, List.of(b));
        adapterA.bind(nodeA);
        TelemetryResponse telemetry = adapterA.telemetry(new TelemetryRequest(nodeA.getTerm(), "client", false, true));

        assertTrue(telemetry.isSuccess());
        var follower = telemetry.getReplication().stream().filter(item -> "B".equals(item.peerId())).findFirst().orElseThrow();
        assertEquals(2, follower.consecutiveFailures());
        assertTrue(follower.lastFailedContactMillis() > 0L);
        assertTrue(follower.lastSuccessfulContactMillis() > 0L);
        assertTrue(follower.lastFailedContactMillis() >= follower.lastSuccessfulContactMillis());
        assertEquals("at-risk", telemetry.getClusterHealth());
        assertEquals("current-quorum-unavailable", telemetry.getClusterStatusReason());
        assertFalse(telemetry.isQuorumAvailable());
        assertEquals(2, telemetry.getVotingMembers());
        assertEquals(1, telemetry.getHealthyVotingMembers());
        assertEquals(List.of("B"), telemetry.getBlockingCurrentQuorumPeerIds());
    }

    @Test
    void leaderTelemetryMarksClusterAtRiskWhenQuorumIsLost() {
        announce("Telemetry quorum risk: leader summary reports when healthy quorum is no longer available");
        Peer a = new Peer("A", null);
        Peer b = new Peer("B", null);
        Peer c = new Peer("C", null);

        RaftNodeElectionTest.MutableTime time = new RaftNodeElectionTest.MutableTime(0);
        Map<String, RaftNode> nodes = new HashMap<>();
        RaftNodeElectionTest.QueuedRaftClient clientA = new RaftNodeElectionTest.QueuedRaftClient("A", nodes);
        RaftNodeElectionTest.QueuedRaftClient clientB = new RaftNodeElectionTest.QueuedRaftClient("B", nodes);
        RaftNodeElectionTest.QueuedRaftClient clientC = new RaftNodeElectionTest.QueuedRaftClient("C", nodes);

        RaftNode nodeA = newTestNode(a, List.of(b, c), new KeyValueStateMachine(), clientA, time, 1);
        RaftNode nodeB = newTestNode(b, List.of(a, c), new KeyValueStateMachine(), clientB, time, 2);
        RaftNode nodeC = newTestNode(c, List.of(a, b), new KeyValueStateMachine(), clientC, time, 3);
        nodes.put("A", nodeA);
        nodes.put("B", nodeB);
        nodes.put("C", nodeC);

        nodeA.setLastHeartbeatMillisForTest(0);
        time.set(10_000);
        nodeA.electionTickForTest();
        clientA.flush();

        nodes.remove("B");
        nodes.remove("C");
        time.set(11_000);
        nodeA.heartbeatTickForTest();
        clientA.flush();
        time.set(12_000);
        nodeA.heartbeatTickForTest();
        clientA.flush();
        time.set(13_000);
        nodeA.heartbeatTickForTest();
        clientA.flush();

        TestAdapter adapterA = new TestAdapter(a, List.of(b, c));
        adapterA.bind(nodeA);
        TelemetryResponse telemetry = adapterA.telemetry(new TelemetryRequest(nodeA.getTerm(), "client", false, true));

        assertEquals("at-risk", telemetry.getClusterHealth());
        assertEquals("current-quorum-unavailable", telemetry.getClusterStatusReason());
        assertFalse(telemetry.isQuorumAvailable());
        assertFalse(telemetry.isCurrentQuorumAvailable());
        assertEquals(3, telemetry.getVotingMembers());
        assertEquals(1, telemetry.getHealthyVotingMembers());
        assertEquals(1, telemetry.getReachableVotingMembers());
        assertEquals(List.of("B", "C"), telemetry.getBlockingCurrentQuorumPeerIds());
    }

    @Test
    void leaderTelemetryDistinguishesNextQuorumRiskDuringJointConsensus() {
        announce("Telemetry joint split: leader summary distinguishes current quorum health from next quorum blockage");
        Peer a = new Peer("A", null);
        Peer b = new Peer("B", null);
        Peer c = new Peer("C", null);
        Peer d = new Peer("D", null);
        Peer e = new Peer("E", null);

        RaftNodeElectionTest.MutableTime time = new RaftNodeElectionTest.MutableTime(0);
        Map<String, RaftNode> nodes = new HashMap<>();
        RaftNodeElectionTest.QueuedRaftClient clientA = new RaftNodeElectionTest.QueuedRaftClient("A", nodes);
        RaftNodeElectionTest.QueuedRaftClient clientB = new RaftNodeElectionTest.QueuedRaftClient("B", nodes);
        RaftNodeElectionTest.QueuedRaftClient clientC = new RaftNodeElectionTest.QueuedRaftClient("C", nodes);

        RaftNode nodeA = newTestNode(a, List.of(b, c), new KeyValueStateMachine(), clientA, time, 1);
        RaftNode nodeB = newTestNode(b, List.of(a, c), new KeyValueStateMachine(), clientB, time, 2);
        RaftNode nodeC = newTestNode(c, List.of(a, b), new KeyValueStateMachine(), clientC, time, 3);
        nodes.put("A", nodeA);
        nodes.put("B", nodeB);
        nodes.put("C", nodeC);

        nodeA.setLastHeartbeatMillisForTest(0);
        time.set(10_000);
        nodeA.electionTickForTest();
        clientA.flush();
        nodeA.heartbeatTickForTest();
        clientA.flush();

        nodeA.transitionToJointConfigurationForTest(List.of(a, d, e));

        TestAdapter adapterA = new TestAdapter(a, List.of(b, c));
        adapterA.bind(nodeA);
        TelemetryResponse telemetry = adapterA.telemetry(new TelemetryRequest(nodeA.getTerm(), "client", false, true));

        assertTrue(telemetry.isJointConsensus());
        assertEquals("at-risk", telemetry.getClusterHealth());
        assertEquals("next-quorum-unavailable", telemetry.getClusterStatusReason());
        assertTrue(telemetry.isCurrentQuorumAvailable());
        assertFalse(telemetry.isNextQuorumAvailable());
        assertFalse(telemetry.isQuorumAvailable());
        assertTrue(telemetry.getBlockingCurrentQuorumPeerIds().isEmpty());
        assertEquals(List.of("D", "E"), telemetry.getBlockingNextQuorumPeerIds());
    }

    @Test
    void leaderTelemetryFlagsStuckReconfiguration() {
        announce("Telemetry stuck reconfiguration: leader reports long-running membership transition explicitly");
        String previous = System.getProperty("raft.telemetry.reconfiguration.stuck.millis");
        System.setProperty("raft.telemetry.reconfiguration.stuck.millis", "1000");
        try {
            Peer a = new Peer("A", null);
            Peer b = new Peer("B", null);
            Peer d = learner("D");

            RaftNodeElectionTest.MutableTime time = new RaftNodeElectionTest.MutableTime(0);
            Map<String, RaftNode> nodes = new HashMap<>();
            RaftNodeElectionTest.QueuedRaftClient clientA = new RaftNodeElectionTest.QueuedRaftClient("A", nodes);
            RaftNodeElectionTest.QueuedRaftClient clientB = new RaftNodeElectionTest.QueuedRaftClient("B", nodes);

            RaftNode nodeA = newTestNode(a, List.of(b), new KeyValueStateMachine(), clientA, time, 1);
            RaftNode nodeB = newTestNode(b, List.of(a), new KeyValueStateMachine(), clientB, time, 2);
            nodes.put("A", nodeA);
            nodes.put("B", nodeB);

            nodeA.setLastHeartbeatMillisForTest(0);
            time.set(10_000);
            nodeA.electionTickForTest();
            clientA.flush();
            nodeA.heartbeatTickForTest();
            clientA.flush();
            assertTrue(nodeA.isLeader());

            TestAdapter adapterA = new TestAdapter(a, List.of(b));
            adapterA.bind(nodeA);

            assertTrue(nodeA.submitJoinConfigurationChange(d));
            time.set(12_500);

            ClusterSummaryResponse summary = adapterA.clusterSummary(new ClusterSummaryRequest(nodeA.getTerm(), "client"));
            assertEquals("degraded", summary.getClusterHealth());
            assertEquals("reconfiguration-stuck", summary.getClusterStatusReason());
            assertTrue(summary.getReconfigurationAgeMillis() >= 2_500L);

            TelemetryResponse telemetry = adapterA.telemetry(new TelemetryRequest(nodeA.getTerm(), "client", false, false));
            assertEquals("degraded", telemetry.getClusterHealth());
            assertEquals("reconfiguration-stuck", telemetry.getClusterStatusReason());
            assertTrue(telemetry.getReconfigurationAgeMillis() >= 2_500L);
        } finally {
            if (previous == null) {
                System.clearProperty("raft.telemetry.reconfiguration.stuck.millis");
            } else {
                System.setProperty("raft.telemetry.reconfiguration.stuck.millis", previous);
            }
        }
    }

    @Test
    void reconfigurationStatusFocusesOnTransitionAndBlockers() {
        announce("Reconfiguration status view: leader exposes focused transition age and blocker detail");
        Peer a = new Peer("A", null);
        Peer b = new Peer("B", null);
        Peer d = learner("D");

        RaftNodeElectionTest.MutableTime time = new RaftNodeElectionTest.MutableTime(0);
        Map<String, RaftNode> nodes = new HashMap<>();
        RaftNodeElectionTest.QueuedRaftClient clientA = new RaftNodeElectionTest.QueuedRaftClient("A", nodes);
        RaftNodeElectionTest.QueuedRaftClient clientB = new RaftNodeElectionTest.QueuedRaftClient("B", nodes);

        RaftNode nodeA = newTestNode(a, List.of(b), new KeyValueStateMachine(), clientA, time, 1);
        RaftNode nodeB = newTestNode(b, List.of(a), new KeyValueStateMachine(), clientB, time, 2);
        nodes.put("A", nodeA);
        nodes.put("B", nodeB);

        nodeA.setLastHeartbeatMillisForTest(0);
        time.set(10_000);
        nodeA.electionTickForTest();
        clientA.flush();
        nodeA.heartbeatTickForTest();
        clientA.flush();
        assertTrue(nodeA.isLeader());

        TestAdapter adapterA = new TestAdapter(a, List.of(b));
        adapterA.bind(nodeA);

        assertTrue(nodeA.submitJoinConfigurationChange(d));
        time.set(12_500);

        ReconfigurationStatusResponse status = adapterA.reconfigurationStatus(new ReconfigurationStatusRequest(nodeA.getTerm(), "client"));
        assertTrue(status.isSuccess());
        assertTrue(status.isReconfigurationActive());
        assertTrue(status.isJointConsensus());
        assertTrue(status.getReconfigurationAgeMillis() >= 2_500L);
        var joining = status.getMembers().stream().filter(member -> "D".equals(member.peerId())).findFirst().orElseThrow();
        assertEquals("joining", joining.roleTransition());
        assertEquals("LEARNER", joining.nextRole());
    }

    @Test
    void telemetryRequestsAreRateLimited() {
        announce("Telemetry rate limit: repeated status pulls are rejected after configured threshold");
        String previous = System.getProperty("raft.telemetry.rate.limit.per.minute");
        System.setProperty("raft.telemetry.rate.limit.per.minute", "1");
        try {
            Peer me = peer("A");
            MutableTime time = new MutableTime(0);
            RaftNode node = newTestNode(me, List.of(), new KeyValueStateMachine(), new NoopRaftClient(), time, 1);

            TestAdapter adapter = new TestAdapter(me, List.of());
            adapter.bind(node);

            TelemetryResponse first = adapter.telemetry(new TelemetryRequest(node.getTerm(), "client", false, false));
            TelemetryResponse second = adapter.telemetry(new TelemetryRequest(node.getTerm(), "client", false, false));

            assertTrue(first.isSuccess());
            assertEquals("RATE_LIMITED", second.getStatus());
            assertFalse(second.isSuccess());
        } finally {
            if (previous == null) {
                System.clearProperty("raft.telemetry.rate.limit.per.minute");
            } else {
                System.setProperty("raft.telemetry.rate.limit.per.minute", previous);
            }
        }
    }

    @Test
    void leaderAcceptsTypedJointConfigurationThroughAdapter() {
        announce("Typed reconfigure joint: leader accepts joint configuration through structured request");
        Peer a = new Peer("A", null);
        Peer b = new Peer("B", null);
        Peer c = new Peer("C", null);
        Peer d = new Peer("D", new java.net.InetSocketAddress("127.0.0.1", 10083));

        RaftNodeElectionTest.MutableTime time = new RaftNodeElectionTest.MutableTime(0);
        Map<String, RaftNode> nodes = new HashMap<>();

        RaftNodeElectionTest.QueuedRaftClient clientA = new RaftNodeElectionTest.QueuedRaftClient("A", nodes);
        ForwardingProbeRaftClient clientB = new ForwardingProbeRaftClient();
        RaftNodeElectionTest.QueuedRaftClient clientC = new RaftNodeElectionTest.QueuedRaftClient("C", nodes);
        RaftNodeElectionTest.QueuedRaftClient clientD = new RaftNodeElectionTest.QueuedRaftClient("D", nodes);

        InMemoryLogStore storeA = new InMemoryLogStore();
        InMemoryLogStore storeB = new InMemoryLogStore();
        InMemoryLogStore storeC = new InMemoryLogStore();
        InMemoryLogStore storeD = new InMemoryLogStore();

        RaftNode nodeA = newTestNode(a, List.of(b, c), new KeyValueStateMachine(), clientA, storeA, new InMemoryPersistentStateStore(), time, 1);
        RaftNode nodeB = newTestNode(b, List.of(a, c), new KeyValueStateMachine(), clientB, storeB, new InMemoryPersistentStateStore(), time, 2);
        RaftNode nodeC = newTestNode(c, List.of(a, b), new KeyValueStateMachine(), clientC, storeC, new InMemoryPersistentStateStore(), time, 3);
        RaftNode nodeD = newTestNode(d, List.of(a, b, c), new KeyValueStateMachine(), clientD, storeD, new InMemoryPersistentStateStore(), time, 4);

        nodes.put("A", nodeA);
        nodes.put("B", nodeB);
        nodes.put("C", nodeC);
        nodes.put("D", nodeD);

        nodeA.setLastHeartbeatMillisForTest(0);
        time.set(10_000);
        nodeA.electionTickForTest();
        clientA.flush();
        assertTrue(nodeA.isLeader());

        TestAdapter adapter = new TestAdapter(a, List.of(b, c));
        adapter.bind(nodeA);

        ReconfigureClusterResponse response = adapter.reconfigure(new ReconfigureClusterRequest(
                nodeA.getTerm(), "client", ReconfigureClusterRequest.Action.JOINT, List.of(a, b, d)
        ));
        assertTrue(response.isSuccess());

        clientA.flush();
        nodeA.heartbeatTickForTest();
        clientA.flush();

        assertTrue(nodeA.getClusterConfigurationForTest().isJointConsensus());
        assertTrue(nodeA.getClusterConfigurationForTest().contains("D"));
    }

    @Test
    void unknownTypedReconfigurationPeersAreRejected() {
        announce("Typed reconfigure validation: unknown peer ids are rejected");
        Peer me = peer("A");
        MutableTime time = new MutableTime(0);
        InMemoryLogStore store = new InMemoryLogStore();
        RaftNode node = newTestNode(me, List.of(), new KeyValueStateMachine(), new NoopRaftClient(), store, new InMemoryPersistentStateStore(), time, 1);

        node.setLastHeartbeatMillisForTest(0);
        time.set(10_000);
        node.electionTickForTest();
        node.handleVoteResponsesForTest(List.of(), 1);

        TestAdapter adapter = new TestAdapter(me, List.of());
        adapter.bind(node);

        long lastIndexBefore = store.lastIndex();

        ReconfigureClusterResponse unknownPeer = adapter.reconfigure(new ReconfigureClusterRequest(
                node.getTerm(), "client", ReconfigureClusterRequest.Action.JOINT, List.of(new Peer("Z", null))
        ));
        assertEquals(lastIndexBefore, store.lastIndex());
        assertFalse(node.getClusterConfigurationForTest().isJointConsensus());
        assertFalse(unknownPeer.isSuccess());
    }

    @Test
    void leaderAcceptsJoinRequestAndAutoFinalizesConfiguration() {
        announce("Typed join request: leader admits new member and auto-finalizes after catch-up");
        Peer a = new Peer("A", null);
        Peer b = new Peer("B", null);
        Peer c = new Peer("C", null);
        Peer d = new Peer("D", new java.net.InetSocketAddress("127.0.0.1", 10083));

        RaftNodeElectionTest.MutableTime time = new RaftNodeElectionTest.MutableTime(0);
        Map<String, RaftNode> nodes = new HashMap<>();

        RaftNodeElectionTest.QueuedRaftClient clientA = new RaftNodeElectionTest.QueuedRaftClient("A", nodes);
        ForwardingProbeRaftClient clientB = new ForwardingProbeRaftClient();
        RaftNodeElectionTest.QueuedRaftClient clientC = new RaftNodeElectionTest.QueuedRaftClient("C", nodes);
        RaftNodeElectionTest.QueuedRaftClient clientD = new RaftNodeElectionTest.QueuedRaftClient("D", nodes);

        RaftNode nodeA = newTestNode(a, List.of(b, c), new KeyValueStateMachine(), clientA, time, 1);
        RaftNode nodeB = newTestNode(b, List.of(a, c), new KeyValueStateMachine(), clientB, time, 2);
        RaftNode nodeC = newTestNode(c, List.of(a, b), new KeyValueStateMachine(), clientC, time, 3);
        RaftNode nodeD = newTestNode(d, List.of(a), new KeyValueStateMachine(), clientD, time, 4);
        nodeD.enableJoiningMode();

        nodes.put("A", nodeA);
        nodes.put("B", nodeB);
        nodes.put("C", nodeC);
        nodes.put("D", nodeD);

        nodeA.setLastHeartbeatMillisForTest(0);
        time.set(10_000);
        nodeA.electionTickForTest();
        clientA.flush();
        assertTrue(nodeA.isLeader());

        TestAdapter adapter = new TestAdapter(a, List.of(b, c));
        adapter.bind(nodeA);

        JoinClusterResponse joinResponse = adapter.join(new JoinClusterRequest(nodeA.getTerm(), "client", d));
        assertTrue(joinResponse.isSuccess());

        for (int i = 0; i < 6; i++) {
            clientA.flush();
            nodeA.heartbeatTickForTest();
        }
        clientA.flush();

        assertFalse(nodeA.getClusterConfigurationForTest().isJointConsensus());
        assertTrue(nodeA.getClusterConfigurationForTest().contains("D"));
        assertTrue(nodeB.getClusterConfigurationForTest().contains("D"));
        assertTrue(nodeC.getClusterConfigurationForTest().contains("D"));
        assertTrue(nodeD.getClusterConfigurationForTest().contains("D"));
        assertFalse(nodeD.isJoining());
    }

    @Test
    void typedJoinRequestAndStatusUseStructuredProtocol() {
        announce("Typed join protocol: structured join request and status request report lifecycle without command strings");
        Peer a = new Peer("A", null);
        Peer b = new Peer("B", null);
        Peer c = new Peer("C", null);
        Peer d = new Peer("D", new java.net.InetSocketAddress("127.0.0.1", 10083));

        RaftNodeElectionTest.MutableTime time = new RaftNodeElectionTest.MutableTime(0);
        Map<String, RaftNode> nodes = new HashMap<>();

        RaftNodeElectionTest.QueuedRaftClient clientA = new RaftNodeElectionTest.QueuedRaftClient("A", nodes);
        ForwardingProbeRaftClient clientB = new ForwardingProbeRaftClient();
        RaftNodeElectionTest.QueuedRaftClient clientC = new RaftNodeElectionTest.QueuedRaftClient("C", nodes);
        RaftNodeElectionTest.QueuedRaftClient clientD = new RaftNodeElectionTest.QueuedRaftClient("D", nodes);

        RaftNode nodeA = newTestNode(a, List.of(b, c), new KeyValueStateMachine(), clientA, time, 1);
        RaftNode nodeB = newTestNode(b, List.of(a, c), new KeyValueStateMachine(), clientB, time, 2);
        RaftNode nodeC = newTestNode(c, List.of(a, b), new KeyValueStateMachine(), clientC, time, 3);
        RaftNode nodeD = newTestNode(d, List.of(a), new KeyValueStateMachine(), clientD, time, 4);
        nodeD.enableJoiningMode();

        nodes.put("A", nodeA);
        nodes.put("B", nodeB);
        nodes.put("C", nodeC);
        nodes.put("D", nodeD);

        nodeA.setLastHeartbeatMillisForTest(0);
        time.set(10_000);
        nodeA.electionTickForTest();
        clientA.flush();
        assertTrue(nodeA.isLeader());

        TestAdapter adapter = new TestAdapter(a, List.of(b, c));
        adapter.bind(nodeA);

        JoinClusterResponse joinResponse = adapter.join(new JoinClusterRequest(nodeA.getTerm(), "client", d));
        assertTrue(joinResponse.isSuccess());
        assertEquals("PENDING", joinResponse.getStatus());

        JoinClusterStatusResponse pending = adapter.joinStatus(new JoinClusterStatusRequest(nodeA.getTerm(), "client", "D"));
        assertTrue(pending.isSuccess());
        assertEquals("PENDING", pending.getStatus());

        for (int i = 0; i < 6; i++) {
            clientA.flush();
            nodeA.heartbeatTickForTest();
        }
        clientA.flush();

        JoinClusterStatusResponse completed = adapter.joinStatus(new JoinClusterStatusRequest(nodeA.getTerm(), "client", "D"));
        assertTrue(completed.isSuccess());
        assertEquals("COMPLETED", completed.getStatus());
    }

    @Test
    void typedReconfigureProtocolDrivesJointAndFinalizeWithoutCommandStrings() {
        announce("Typed reconfigure protocol: joint and finalize requests use structured API without command strings");
        Peer a = new Peer("A", null);
        Peer b = new Peer("B", null);
        Peer c = new Peer("C", null);
        Peer d = new Peer("D", new java.net.InetSocketAddress("127.0.0.1", 10083));

        RaftNodeElectionTest.MutableTime time = new RaftNodeElectionTest.MutableTime(0);
        Map<String, RaftNode> nodes = new HashMap<>();

        RaftNodeElectionTest.QueuedRaftClient clientA = new RaftNodeElectionTest.QueuedRaftClient("A", nodes);
        RaftNodeElectionTest.QueuedRaftClient clientB = new RaftNodeElectionTest.QueuedRaftClient("B", nodes);
        RaftNodeElectionTest.QueuedRaftClient clientC = new RaftNodeElectionTest.QueuedRaftClient("C", nodes);
        RaftNodeElectionTest.QueuedRaftClient clientD = new RaftNodeElectionTest.QueuedRaftClient("D", nodes);

        RaftNode nodeA = newTestNode(a, List.of(b, c), new KeyValueStateMachine(), clientA, time, 1);
        RaftNode nodeB = newTestNode(b, List.of(a, c), new KeyValueStateMachine(), clientB, time, 2);
        RaftNode nodeC = newTestNode(c, List.of(a, b), new KeyValueStateMachine(), clientC, time, 3);
        RaftNode nodeD = newTestNode(d, List.of(a, b, c), new KeyValueStateMachine(), clientD, time, 4);

        nodes.put("A", nodeA);
        nodes.put("B", nodeB);
        nodes.put("C", nodeC);
        nodes.put("D", nodeD);

        nodeA.setLastHeartbeatMillisForTest(0);
        time.set(10_000);
        nodeA.electionTickForTest();
        clientA.flush();
        assertTrue(nodeA.isLeader());

        TestAdapter adapter = new TestAdapter(a, List.of(b, c));
        adapter.bind(nodeA);

        ReconfigureClusterResponse joint = adapter.reconfigure(new ReconfigureClusterRequest(
                nodeA.getTerm(),
                "client",
                ReconfigureClusterRequest.Action.JOINT,
                List.of(a, b, d)
        ));
        assertTrue(joint.isSuccess());
        assertEquals("ACCEPTED", joint.getStatus());

        clientA.flush();
        nodeA.heartbeatTickForTest();
        clientA.flush();
        assertTrue(nodeA.getClusterConfigurationForTest().isJointConsensus());
        assertTrue(nodeD.getClusterConfigurationForTest().contains("D"));

        ReconfigureClusterResponse finalize = adapter.reconfigure(new ReconfigureClusterRequest(
                nodeA.getTerm(),
                "client",
                ReconfigureClusterRequest.Action.FINALIZE,
                List.of()
        ));
        assertTrue(finalize.isSuccess());
        assertEquals("ACCEPTED", finalize.getStatus());

        clientA.flush();
        nodeA.heartbeatTickForTest();
        clientA.flush();
        assertFalse(nodeA.getClusterConfigurationForTest().isJointConsensus());
        assertTrue(nodeA.getClusterConfigurationForTest().contains("D"));
    }

    @Test
    void typedStatusQueryReturnsUnknownForUnseenJoin() {
        announce("Typed join status unknown: status query reports unknown when no join is known");
        Peer me = peer("A");
        MutableTime time = new MutableTime(0);
        RaftNode node = newTestNode(me, List.of(), new KeyValueStateMachine(), new NoopRaftClient(), time, 1);

        TestAdapter adapter = new TestAdapter(me, List.of());
        adapter.bind(node);

        JoinClusterStatusResponse response = adapter.joinStatus(new JoinClusterStatusRequest(node.getTerm(), "client", "D"));
        assertFalse(response.isSuccess());
        assertEquals("UNKNOWN", response.getStatus());
    }

    @Test
    void followerForwardsTypedJoinRequestToKnownLeader() {
        announce("Typed join forwarding: follower relays join request to known leader");
        Peer a = new Peer("A", null);
        Peer b = new Peer("B", null);
        Peer c = new Peer("C", null);
        Peer d = new Peer("D", new java.net.InetSocketAddress("127.0.0.1", 10083));

        RaftNodeElectionTest.MutableTime time = new RaftNodeElectionTest.MutableTime(0);
        Map<String, RaftNode> nodes = new HashMap<>();

        TestAdapter adapterA = new TestAdapter(a, List.of(b, c));
        TestAdapter adapterB = new TestAdapter(b, List.of(a, c));
        TestAdapter adapterC = new TestAdapter(c, List.of(a, b));

        RaftNodeElectionTest.QueuedRaftClient clientA = new RaftNodeElectionTest.QueuedRaftClient("A", nodes);
        ForwardingProbeRaftClient clientB = new ForwardingProbeRaftClient();
        RaftNodeElectionTest.QueuedRaftClient clientC = new RaftNodeElectionTest.QueuedRaftClient("C", nodes);
        RaftNodeElectionTest.QueuedRaftClient clientD = new RaftNodeElectionTest.QueuedRaftClient("D", nodes);

        RaftNode nodeA = newTestNode(a, List.of(b, c), adapterA::handleMessage, new KeyValueStateMachine(), clientA, time, 1);
        RaftNode nodeB = newTestNode(b, List.of(a, c), adapterB::handleMessage, new KeyValueStateMachine(), clientB, time, 2);
        RaftNode nodeC = newTestNode(c, List.of(a, b), adapterC::handleMessage, new KeyValueStateMachine(), clientC, time, 3);
        RaftNode nodeD = newTestNode(d, List.of(b), new KeyValueStateMachine(), clientD, time, 4);
        nodeD.enableJoiningMode();

        nodes.put("A", nodeA);
        nodes.put("B", nodeB);
        nodes.put("C", nodeC);
        nodes.put("D", nodeD);

        adapterA.bind(nodeA);
        adapterB.bind(nodeB);
        adapterC.bind(nodeC);

        nodeA.setLastHeartbeatMillisForTest(0);
        time.set(10_000);
        nodeA.electionTickForTest();
        clientA.flush();
        assertTrue(nodeA.isLeader());

        nodeA.heartbeatTickForTest();
        clientA.flush();
        assertNotNull(nodeB.getKnownLeaderPeer());
        assertEquals("A", nodeB.getKnownLeaderPeer().getId());

        JoinClusterResponse forwarded = adapterB.join(new JoinClusterRequest(nodeB.getTerm(), "client", d));
        assertTrue(forwarded.isSuccess());
        assertEquals("FORWARDED", forwarded.getStatus());
        assertEquals("A", clientB.forwardedPeer.getId());
        assertEquals("D", clientB.forwardedJoinRequest.getJoiningPeer().getId());
    }

    @Test
    void leaderStepsDownWhenTypedFinalizeRemovesIt() {
        announce("Typed finalize removal: leader steps down and rejects further commands after removal");
        Peer a = new Peer("A", null);
        Peer b = new Peer("B", null);
        Peer c = new Peer("C", null);
        Peer d = new Peer("D", new java.net.InetSocketAddress("127.0.0.1", 10083));

        RaftNodeElectionTest.MutableTime time = new RaftNodeElectionTest.MutableTime(0);
        Map<String, RaftNode> nodes = new HashMap<>();

        RaftNodeElectionTest.QueuedRaftClient clientA = new RaftNodeElectionTest.QueuedRaftClient("A", nodes);
        RaftNodeElectionTest.QueuedRaftClient clientB = new RaftNodeElectionTest.QueuedRaftClient("B", nodes);
        RaftNodeElectionTest.QueuedRaftClient clientC = new RaftNodeElectionTest.QueuedRaftClient("C", nodes);
        RaftNodeElectionTest.QueuedRaftClient clientD = new RaftNodeElectionTest.QueuedRaftClient("D", nodes);

        InMemoryLogStore storeA = new InMemoryLogStore();
        InMemoryLogStore storeB = new InMemoryLogStore();
        InMemoryLogStore storeC = new InMemoryLogStore();
        InMemoryLogStore storeD = new InMemoryLogStore();

        RaftNode nodeA = newTestNode(a, List.of(b, c), new KeyValueStateMachine(), clientA, storeA, new InMemoryPersistentStateStore(), time, 1);
        RaftNode nodeB = newTestNode(b, List.of(a, c), new KeyValueStateMachine(), clientB, storeB, new InMemoryPersistentStateStore(), time, 2);
        RaftNode nodeC = newTestNode(c, List.of(a, b), new KeyValueStateMachine(), clientC, storeC, new InMemoryPersistentStateStore(), time, 3);
        RaftNode nodeD = newTestNode(d, List.of(a, b, c), new KeyValueStateMachine(), clientD, storeD, new InMemoryPersistentStateStore(), time, 4);

        nodes.put("A", nodeA);
        nodes.put("B", nodeB);
        nodes.put("C", nodeC);
        nodes.put("D", nodeD);

        nodeA.setLastHeartbeatMillisForTest(0);
        time.set(10_000);
        nodeA.electionTickForTest();
        clientA.flush();
        assertTrue(nodeA.isLeader());

        TestAdapter adapter = new TestAdapter(a, List.of(b, c));
        adapter.bind(nodeA);

        ReconfigureClusterResponse jointResponse = adapter.reconfigure(new ReconfigureClusterRequest(
                nodeA.getTerm(), "client", ReconfigureClusterRequest.Action.JOINT, List.of(b, c, d)
        ));
        assertTrue(jointResponse.isSuccess());
        clientA.flush();
        nodeA.heartbeatTickForTest();
        clientA.flush();
        nodeA.heartbeatTickForTest();
        clientA.flush();
        assertTrue(nodeA.getClusterConfigurationForTest().isJointConsensus());

        ReconfigureClusterResponse finalizeResponse = adapter.reconfigure(new ReconfigureClusterRequest(
                nodeA.getTerm(), "client", ReconfigureClusterRequest.Action.FINALIZE, List.of()
        ));
        assertTrue(finalizeResponse.isSuccess());
        clientA.flush();
        nodeA.heartbeatTickForTest();
        clientA.flush();

        long lastIndexAfterRemoval = storeA.lastIndex();
        assertFalse(nodeA.isLeader());
        assertFalse(nodeA.getClusterConfigurationForTest().contains("A"));
        assertTrue(nodeA.isDecommissionedForTest());

        ClientCommandResponse commandRejected = adapter.command(new ClientCommandRequest(
                nodeA.getTerm(), "client", StateMachineCommand.put("x", "9").encode()
        ));

        ReconfigureClusterResponse rejected = adapter.reconfigure(new ReconfigureClusterRequest(
                nodeA.getTerm(), "client", ReconfigureClusterRequest.Action.JOINT, List.of(b, c, d)
        ));

        assertEquals(lastIndexAfterRemoval, storeA.lastIndex());
        assertFalse(commandRejected.isSuccess());
        assertFalse(rejected.isSuccess());
    }
}
