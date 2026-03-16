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
import org.gautelis.raft.transport.MessageResponder;
import org.gautelis.raft.transport.netty.*;
import org.gautelis.raft.serialization.ProtoMapper;

import io.netty.channel.embedded.EmbeddedChannel;
import org.gautelis.raft.protocol.ClientCommandResponse;
import org.gautelis.raft.protocol.ClientQueryResponse;
import org.gautelis.raft.protocol.ClusterMemberSummary;
import org.gautelis.raft.protocol.ClusterSummaryResponse;
import org.gautelis.raft.protocol.AppendEntriesResponse;
import org.gautelis.raft.protocol.JoinClusterResponse;
import org.gautelis.raft.protocol.JoinClusterStatusResponse;
import org.gautelis.raft.protocol.ReconfigureClusterResponse;
import org.gautelis.raft.protocol.ReconfigurationStatusResponse;
import org.gautelis.raft.protocol.TelemetryResponse;
import org.gautelis.raft.protocol.VoteRequest;
import org.gautelis.raft.protocol.VoteResponse;
import org.gautelis.raft.proto.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClientResponseHandlerTest {
    private static final Logger log = LoggerFactory.getLogger(ClientResponseHandlerTest.class);

    private static ClientResponseHandler newHandler(
            Map<String, CompletableFuture<VoteResponse>> inFlightVotes,
            Map<String, CompletableFuture<ClientCommandResponse>> inFlightClientCommands,
            Map<String, CompletableFuture<ClientQueryResponse>> inFlightClientQueries,
            Map<String, CompletableFuture<ClusterSummaryResponse>> inFlightClusterSummary,
            Map<String, CompletableFuture<ReconfigurationStatusResponse>> inFlightReconfigurationStatus,
            Map<String, CompletableFuture<JoinClusterResponse>> inFlightJoinCluster,
            Map<String, CompletableFuture<JoinClusterStatusResponse>> inFlightJoinStatus,
            Map<String, CompletableFuture<ReconfigureClusterResponse>> inFlightReconfigureCluster,
            Map<String, CompletableFuture<TelemetryResponse>> inFlightTelemetry,
            MessageHandler messageHandler
    ) {
        return new ClientResponseHandler(
                new java.util.HashMap<>(inFlightVotes),
                new java.util.HashMap<String, CompletableFuture<org.gautelis.raft.protocol.AppendEntriesResponse>>(),
                new java.util.HashMap<String, CompletableFuture<org.gautelis.raft.protocol.InstallSnapshotResponse>>(),
                new java.util.HashMap<>(inFlightClientCommands),
                new java.util.HashMap<>(inFlightClientQueries),
                new java.util.HashMap<>(inFlightClusterSummary),
                new java.util.HashMap<>(inFlightReconfigurationStatus),
                new java.util.HashMap<>(inFlightJoinCluster),
                new java.util.HashMap<>(inFlightJoinStatus),
                new java.util.HashMap<>(inFlightReconfigureCluster),
                new java.util.HashMap<>(inFlightTelemetry),
                new java.util.HashMap<>(),
                new java.util.HashMap<String, ScheduledFuture<?>>(),
                new java.util.HashMap<String, org.gautelis.vopn.statistics.RunningStatistics>(),
                messageHandler
        );
    }

    private static ClientResponseHandler newHandler(MessageHandler messageHandler) {
        return newHandler(Map.of(), Map.of(), Map.of(), Map.of(), Map.of(), Map.of(), Map.of(), Map.of(), Map.of(), messageHandler);
    }

    static class CapturingMessageHandler implements MessageHandler {
        String correlationId;
        String type;
        byte[] payload;

        @Override
        public void handle(String correlationId, String type, byte[] payload, MessageResponder responder) {
            this.correlationId = correlationId;
            this.type = type;
            this.payload = payload;
        }
    }

    @Test
    void voteResponseCompletesFuture() throws Exception {
        log.info("TC: VoteResponse completes waiting future");

        CompletableFuture<VoteResponse> future = new CompletableFuture<>();
        Map<String, CompletableFuture<VoteResponse>> inFlight = new java.util.HashMap<>();
        inFlight.put("corr-1", future);

        ClientResponseHandler handler = newHandler(inFlight, Map.of(), Map.of(), Map.of(), Map.of(), Map.of(), Map.of(), Map.of(), Map.of(), null);
        EmbeddedChannel channel = new EmbeddedChannel(handler);

        VoteRequest req = new VoteRequest(2, "A");
        VoteResponse resp = new VoteResponse(req, "B", true, 2);
        Envelope envelope = ProtoMapper.wrap(
                "corr-1",
                "VoteResponse",
                ProtoMapper.toProto(resp).toByteString()
        );

        channel.writeInbound(envelope);

        assertTrue(future.isDone());
        assertFalse(future.isCompletedExceptionally());
        assertEquals("B", future.get().getPeerId());
        assertNull(channel.readOutbound());
        channel.finishAndReleaseAll();
    }

    @Test
    void appendEntriesResponseAccumulatesTransportStatistics() throws Exception {
        log.info("TC: AppendEntriesResponse records transport statistics for replication traffic");

        CompletableFuture<AppendEntriesResponse> future = new CompletableFuture<>();
        Map<String, CompletableFuture<AppendEntriesResponse>> inFlightAppendEntries = new java.util.HashMap<>();
        inFlightAppendEntries.put("corr-append-1", future);
        @SuppressWarnings("rawtypes")
        Map requestTimings = new java.util.HashMap<>();
        Class<?> timingClass = Class.forName("org.gautelis.raft.transport.netty.RequestTiming");
        java.lang.reflect.Constructor<?> ctor = timingClass.getDeclaredConstructor(String.class, String.class, long.class);
        ctor.setAccessible(true);
        requestTimings.put("corr-append-1", ctor.newInstance("B", "AppendEntriesRequest", System.nanoTime()));
        Map<String, org.gautelis.vopn.statistics.RunningStatistics> peerStats = new java.util.HashMap<>();

        ClientResponseHandler handler = new ClientResponseHandler(
                new java.util.HashMap<>(),
                inFlightAppendEntries,
                new java.util.HashMap<>(),
                new java.util.HashMap<>(),
                new java.util.HashMap<>(),
                new java.util.HashMap<>(),
                new java.util.HashMap<>(),
                new java.util.HashMap<>(),
                new java.util.HashMap<>(),
                new java.util.HashMap<>(),
                new java.util.HashMap<>(),
                requestTimings,
                new java.util.HashMap<String, ScheduledFuture<?>>(),
                peerStats,
                null
        );
        EmbeddedChannel channel = new EmbeddedChannel(handler);

        AppendEntriesResponse resp = new AppendEntriesResponse(2L, "B", true, 5L);
        Envelope envelope = ProtoMapper.wrap(
                "corr-append-1",
                "AppendEntriesResponse",
                ProtoMapper.toProto(resp).toByteString()
        );

        channel.writeInbound(envelope);

        assertTrue(future.isDone());
        var stats = peerStats.get("B\tAppendEntriesRequest");
        assertNotNull(stats);
        assertEquals(1L, stats.getCount());
        channel.finishAndReleaseAll();
    }

    @Test
    void joinClusterResponseCompletesFuture() throws Exception {
        log.info("TC: JoinClusterResponse completes waiting future");

        CompletableFuture<JoinClusterResponse> future = new CompletableFuture<>();
        Map<String, CompletableFuture<JoinClusterResponse>> inFlightJoinCluster = new java.util.HashMap<>(Map.of("corr-admin-1", future));

        ClientResponseHandler handler = newHandler(Map.of(), Map.of(), Map.of(), Map.of(), Map.of(), inFlightJoinCluster, Map.of(), Map.of(), Map.of(), null);
        EmbeddedChannel channel = new EmbeddedChannel(handler);

        JoinClusterResponse resp = new JoinClusterResponse(5, "A", true, "PENDING", "Join request accepted", "A");
        Envelope envelope = ProtoMapper.wrap(
                "corr-admin-1",
                "JoinClusterResponse",
                ProtoMapper.toProto(resp).toByteString()
        );

        channel.writeInbound(envelope);

        assertTrue(future.isDone());
        assertEquals("PENDING", future.get().getStatus());
        assertEquals("A", future.get().getLeaderId());
        channel.finishAndReleaseAll();
    }

    @Test
    void clientCommandResponseCompletesFuture() throws Exception {
        log.info("TC: ClientCommandResponse completes waiting future");

        CompletableFuture<ClientCommandResponse> future = new CompletableFuture<>();
        Map<String, CompletableFuture<ClientCommandResponse>> inFlightClientCommands = new java.util.HashMap<>(Map.of("corr-command-1", future));

        ClientResponseHandler handler = newHandler(Map.of(), inFlightClientCommands, Map.of(), Map.of(), Map.of(), Map.of(), Map.of(), Map.of(), Map.of(), null);
        EmbeddedChannel channel = new EmbeddedChannel(handler);

        ClientCommandResponse resp = new ClientCommandResponse(5, "A", true, "ACCEPTED", "Command accepted for replication", "A", "127.0.0.1", 10080);
        Envelope envelope = ProtoMapper.wrap(
                "corr-command-1",
                "ClientCommandResponse",
                ProtoMapper.toProto(resp).toByteString()
        );

        channel.writeInbound(envelope);

        assertTrue(future.isDone());
        assertEquals("ACCEPTED", future.get().getStatus());
        assertEquals("A", future.get().getLeaderId());
        assertEquals("127.0.0.1", future.get().getLeaderHost());
        assertEquals(10080, future.get().getLeaderPort());
        channel.finishAndReleaseAll();
    }

    @Test
    void clientQueryResponseCompletesFuture() throws Exception {
        log.info("TC: ClientQueryResponse completes waiting future");

        CompletableFuture<ClientQueryResponse> future = new CompletableFuture<>();

        ClientResponseHandler handler = newHandler(Map.of(), Map.of(), Map.of("corr-query-1", future), Map.of(), Map.of(), Map.of(), Map.of(), Map.of(), Map.of(), null);
        EmbeddedChannel channel = new EmbeddedChannel(handler);

        ClientQueryResponse resp = new ClientQueryResponse(5, "A", true, "OK", "Query completed", "A", "127.0.0.1", 10080, "result".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        Envelope envelope = ProtoMapper.wrap(
                "corr-query-1",
                "ClientQueryResponse",
                ProtoMapper.toProto(resp).toByteString()
        );

        channel.writeInbound(envelope);

        assertTrue(future.isDone());
        assertEquals("OK", future.get().getStatus());
        assertEquals("A", future.get().getLeaderId());
        assertEquals("127.0.0.1", future.get().getLeaderHost());
        assertEquals(10080, future.get().getLeaderPort());
        channel.finishAndReleaseAll();
    }

    @Test
    void clusterSummaryResponseCompletesFuture() throws Exception {
        log.info("TC: ClusterSummaryResponse completes waiting future");

        CompletableFuture<ClusterSummaryResponse> future = new CompletableFuture<>();

        ClientResponseHandler handler = newHandler(Map.of(), Map.of(), Map.of(), Map.of("corr-summary-1", future), Map.of(), Map.of(), Map.of(), Map.of(), Map.of(), null);
        EmbeddedChannel channel = new EmbeddedChannel(handler);

        ClusterSummaryResponse resp = new ClusterSummaryResponse(
                250L,
                5L,
                "A",
                true,
                "OK",
                "",
                "",
                0,
                "LEADER",
                "A",
                false,
                "healthy",
                "all-voters-healthy",
                true,
                true,
                true,
                2,
                2,
                2,
                0L,
                java.util.List.of(),
                java.util.List.of(),
                java.util.List.of(new ClusterMemberSummary("A", true, true, false, true, "VOTER", "VOTER", "", "steady", 0L, "", "", true, "fresh", "local", 2L, 1L, 0L, 0, 100L, 0L))
        );
        Envelope envelope = ProtoMapper.wrap(
                "corr-summary-1",
                "ClusterSummaryResponse",
                ProtoMapper.toProto(resp).toByteString()
        );

        channel.writeInbound(envelope);

        assertTrue(future.isDone());
        assertEquals("OK", future.get().getStatus());
        assertEquals("healthy", future.get().getClusterHealth());
        assertEquals(1, future.get().getMembers().size());
        channel.finishAndReleaseAll();
    }

    @Test
    void reconfigurationStatusResponseCompletesFuture() throws Exception {
        log.info("TC: ReconfigurationStatusResponse completes waiting future");

        CompletableFuture<ReconfigurationStatusResponse> future = new CompletableFuture<>();
        Map<String, CompletableFuture<ReconfigurationStatusResponse>> inFlightReconfigurationStatus =
                new java.util.HashMap<>(Map.of("corr-reconfig-status-1", future));

        ClientResponseHandler handler = newHandler(Map.of(), Map.of(), Map.of(), Map.of(), inFlightReconfigurationStatus, Map.of(), Map.of(), Map.of(), Map.of(), null);
        EmbeddedChannel channel = new EmbeddedChannel(handler);

        ReconfigurationStatusResponse resp = new ReconfigurationStatusResponse(
                250L,
                5L,
                "A",
                true,
                "OK",
                "",
                "",
                0,
                "LEADER",
                "A",
                true,
                true,
                3_000L,
                "degraded",
                "reconfiguration-stuck",
                true,
                true,
                false,
                java.util.List.of(),
                java.util.List.of("D"),
                java.util.List.of(new ClusterMemberSummary("D", false, false, true, false, "LEARNER", "", "LEARNER", "joining", 3_000L, "next", "role-transition", false, "stale", "down", 1L, 0L, 7L, 2, 0L, 250L))
        );
        Envelope envelope = ProtoMapper.wrap(
                "corr-reconfig-status-1",
                "ReconfigurationStatusResponse",
                ProtoMapper.toProto(resp).toByteString()
        );

        channel.writeInbound(envelope);

        assertTrue(future.isDone());
        assertEquals("reconfiguration-stuck", future.get().getClusterStatusReason());
        assertEquals(List.of("D"), future.get().getBlockingNextQuorumPeerIds());
        assertTrue(future.get().isReconfigurationActive());
        channel.finishAndReleaseAll();
    }

    @Test
    void telemetryResponseCompletesFuture() throws Exception {
        log.info("TC: TelemetryResponse completes waiting future");

        CompletableFuture<TelemetryResponse> future = new CompletableFuture<>();

        ClientResponseHandler handler = newHandler(Map.of(), Map.of(), Map.of(), Map.of(), Map.of(), Map.of(), Map.of(), Map.of(), Map.of("corr-telemetry-1", future), null);
        EmbeddedChannel channel = new EmbeddedChannel(handler);

        TelemetryResponse resp = new TelemetryResponse(250L, 5, "A", true, "OK", "", "LEADER", "A", "", false, false, 7, 7, 8, 5, 0, 0, 100, 200, false, java.util.List.of(), java.util.List.of(), java.util.List.of(), java.util.List.of(), java.util.List.of(), java.util.List.of(), "healthy", true, true, true, 1, 1, 1, 0L, "all-voters-healthy", java.util.List.of(), java.util.List.of(), java.util.List.of(new ClusterMemberSummary("A", true, true, false, true, "VOTER", "VOTER", "", "steady", 0L, "", "", true, "fresh", "local", 9L, 8L, 0L, 0, 100L, 0L)));
        Envelope envelope = ProtoMapper.wrap(
                "corr-telemetry-1",
                "TelemetryResponse",
                ProtoMapper.toProto(resp).toByteString()
        );

        channel.writeInbound(envelope);

        assertTrue(future.isDone());
        assertEquals("LEADER", future.get().getState());
        assertEquals(7, future.get().getCommitIndex());
        assertEquals("healthy", future.get().getClusterHealth());
        assertEquals("all-voters-healthy", future.get().getClusterStatusReason());
        channel.finishAndReleaseAll();
    }

    @Test
    void unknownTypeDelegatesToMessageHandler() throws Exception {
        log.info("TC: Unknown type delegates to message handler");

        Map<String, CompletableFuture<VoteResponse>> inFlight = new java.util.HashMap<>();
        CapturingMessageHandler messageHandler = new CapturingMessageHandler();

        ClientResponseHandler handler = newHandler(inFlight, Map.of(), Map.of(), Map.of(), Map.of(), Map.of(), Map.of(), Map.of(), Map.of(), messageHandler);
        EmbeddedChannel channel = new EmbeddedChannel(handler);

        byte[] payload = "ok".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        Envelope envelope = ProtoMapper.wrap("corr-2", "CustomType", payload);
        channel.writeInbound(envelope);

        assertEquals("corr-2", messageHandler.correlationId);
        assertEquals("CustomType", messageHandler.type);
        assertNotNull(messageHandler.payload);
        assertTrue(Arrays.equals(payload, messageHandler.payload));
        channel.finishAndReleaseAll();
    }

    @Test
    void missingFieldsAreIgnored() throws Exception {
        log.info("TC: Testing incomplete envelope; WARN logs expected for missing type/correlationId");

        Map<String, CompletableFuture<VoteResponse>> inFlight = new java.util.HashMap<>();
        ClientResponseHandler handler = newHandler(inFlight, Map.of(), Map.of(), Map.of(), Map.of(), Map.of(), Map.of(), Map.of(), Map.of(), null);
        EmbeddedChannel channel = new EmbeddedChannel(handler);

        Envelope noType = Envelope.newBuilder()
                .setCorrelationId("corr-3")
                .build();
        Envelope noCorrelation = Envelope.newBuilder()
                .setType("VoteResponse")
                .build();

        channel.writeInbound(noType);
        channel.writeInbound(noCorrelation);

        assertNull(channel.readOutbound());
        channel.finishAndReleaseAll();
    }
}
