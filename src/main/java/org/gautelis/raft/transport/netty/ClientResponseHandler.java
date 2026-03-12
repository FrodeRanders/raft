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
package org.gautelis.raft.transport.netty;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.gautelis.raft.MessageHandler;
import org.gautelis.raft.protocol.AppendEntriesResponse;
import org.gautelis.raft.protocol.ClientCommandResponse;
import org.gautelis.raft.protocol.ClientQueryResponse;
import org.gautelis.raft.protocol.ClusterSummaryResponse;
import org.gautelis.raft.protocol.InstallSnapshotResponse;
import org.gautelis.raft.protocol.JoinClusterResponse;
import org.gautelis.raft.protocol.JoinClusterStatusResponse;
import org.gautelis.raft.protocol.ReconfigurationStatusResponse;
import org.gautelis.raft.protocol.ReconfigureClusterResponse;
import org.gautelis.raft.protocol.TelemetryResponse;
import org.gautelis.raft.protocol.VoteResponse;
import org.gautelis.raft.proto.Envelope;
import org.gautelis.raft.serialization.ProtoMapper;
import org.gautelis.vopn.statistics.RunningStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;

public class ClientResponseHandler extends SimpleChannelInboundHandler<Envelope> {
    private static final Logger log = LoggerFactory.getLogger(ClientResponseHandler.class);

    //
    private final MessageHandler messageHandler;

    // This map is shared with RaftClient, so we can fulfill the waiting futures.
    // Key = correlationId, Value = future that awaits the response.
    private final Map<String, CompletableFuture<VoteResponse>> inFlightRequests;
    private final Map<String, CompletableFuture<AppendEntriesResponse>> inFlightAppendEntries;
    private final Map<String, CompletableFuture<InstallSnapshotResponse>> inFlightInstallSnapshot;
    private final Map<String, CompletableFuture<ClientCommandResponse>> inFlightClientCommands;
    private final Map<String, CompletableFuture<ClientQueryResponse>> inFlightClientQueries;
    private final Map<String, CompletableFuture<ClusterSummaryResponse>> inFlightClusterSummary;
    private final Map<String, CompletableFuture<ReconfigurationStatusResponse>> inFlightReconfigurationStatus;
    private final Map<String, CompletableFuture<JoinClusterResponse>> inFlightJoinCluster;
    private final Map<String, CompletableFuture<JoinClusterStatusResponse>> inFlightJoinStatus;
    private final Map<String, CompletableFuture<ReconfigureClusterResponse>> inFlightReconfigureCluster;
    private final Map<String, CompletableFuture<TelemetryResponse>> inFlightTelemetry;
    private final Map<String, RequestTiming> requestTimings;
    private final Map<String, ScheduledFuture<?>> requestTimeouts;
    private final Map<String, RunningStatistics> peerResponseStats;

    public ClientResponseHandler(
            Map<String, CompletableFuture<VoteResponse>> inFlightRequests,
            Map<String, CompletableFuture<AppendEntriesResponse>> inFlightAppendEntries,
            Map<String, CompletableFuture<InstallSnapshotResponse>> inFlightInstallSnapshot,
            Map<String, CompletableFuture<ClientCommandResponse>> inFlightClientCommands,
            Map<String, CompletableFuture<ClientQueryResponse>> inFlightClientQueries,
            Map<String, CompletableFuture<ClusterSummaryResponse>> inFlightClusterSummary,
            Map<String, CompletableFuture<JoinClusterResponse>> inFlightJoinCluster,
            Map<String, CompletableFuture<JoinClusterStatusResponse>> inFlightJoinStatus,
            Map<String, CompletableFuture<ReconfigureClusterResponse>> inFlightReconfigureCluster,
            Map<String, CompletableFuture<TelemetryResponse>> inFlightTelemetry,
            Map<String, RequestTiming> requestTimings,
            Map<String, ScheduledFuture<?>> requestTimeouts,
            Map<String, RunningStatistics> peerResponseStats,
            MessageHandler messageHandler
    ) {
        this(
                inFlightRequests,
                inFlightAppendEntries,
                inFlightInstallSnapshot,
                inFlightClientCommands,
                inFlightClientQueries,
                inFlightClusterSummary,
                new java.util.HashMap<>(),
                inFlightJoinCluster,
                inFlightJoinStatus,
                inFlightReconfigureCluster,
                inFlightTelemetry,
                requestTimings,
                requestTimeouts,
                peerResponseStats,
                messageHandler
        );
    }

    public ClientResponseHandler(
            Map<String, CompletableFuture<VoteResponse>> inFlightRequests,
            Map<String, CompletableFuture<AppendEntriesResponse>> inFlightAppendEntries,
            Map<String, CompletableFuture<InstallSnapshotResponse>> inFlightInstallSnapshot,
            Map<String, CompletableFuture<ClientCommandResponse>> inFlightClientCommands,
            Map<String, CompletableFuture<ClientQueryResponse>> inFlightClientQueries,
            Map<String, CompletableFuture<JoinClusterResponse>> inFlightJoinCluster,
            Map<String, CompletableFuture<JoinClusterStatusResponse>> inFlightJoinStatus,
            Map<String, CompletableFuture<ReconfigureClusterResponse>> inFlightReconfigureCluster,
            Map<String, CompletableFuture<TelemetryResponse>> inFlightTelemetry,
            Map<String, RequestTiming> requestTimings,
            Map<String, ScheduledFuture<?>> requestTimeouts,
            Map<String, RunningStatistics> peerResponseStats,
            MessageHandler messageHandler
    ) {
        this(
                inFlightRequests,
                inFlightAppendEntries,
                inFlightInstallSnapshot,
                inFlightClientCommands,
                inFlightClientQueries,
                new java.util.HashMap<>(),
                new java.util.HashMap<>(),
                inFlightJoinCluster,
                inFlightJoinStatus,
                inFlightReconfigureCluster,
                inFlightTelemetry,
                requestTimings,
                requestTimeouts,
                peerResponseStats,
                messageHandler
        );
    }

    public ClientResponseHandler(
            Map<String, CompletableFuture<VoteResponse>> inFlightRequests,
            Map<String, CompletableFuture<AppendEntriesResponse>> inFlightAppendEntries,
            Map<String, CompletableFuture<InstallSnapshotResponse>> inFlightInstallSnapshot,
            Map<String, CompletableFuture<ClientCommandResponse>> inFlightClientCommands,
            Map<String, CompletableFuture<ClientQueryResponse>> inFlightClientQueries,
            Map<String, CompletableFuture<ClusterSummaryResponse>> inFlightClusterSummary,
            Map<String, CompletableFuture<ReconfigurationStatusResponse>> inFlightReconfigurationStatus,
            Map<String, CompletableFuture<JoinClusterResponse>> inFlightJoinCluster,
            Map<String, CompletableFuture<JoinClusterStatusResponse>> inFlightJoinStatus,
            Map<String, CompletableFuture<ReconfigureClusterResponse>> inFlightReconfigureCluster,
            Map<String, CompletableFuture<TelemetryResponse>> inFlightTelemetry,
            Map<String, RequestTiming> requestTimings,
            Map<String, ScheduledFuture<?>> requestTimeouts,
            Map<String, RunningStatistics> peerResponseStats,
            MessageHandler messageHandler
    ) {
        this.inFlightRequests = inFlightRequests;
        this.inFlightAppendEntries = inFlightAppendEntries;
        this.inFlightInstallSnapshot = inFlightInstallSnapshot;
        this.inFlightClientCommands = inFlightClientCommands;
        this.inFlightClientQueries = inFlightClientQueries;
        this.inFlightClusterSummary = inFlightClusterSummary;
        this.inFlightReconfigurationStatus = inFlightReconfigurationStatus;
        this.inFlightJoinCluster = inFlightJoinCluster;
        this.inFlightJoinStatus = inFlightJoinStatus;
        this.inFlightReconfigureCluster = inFlightReconfigureCluster;
        this.inFlightTelemetry = inFlightTelemetry;
        this.requestTimings = requestTimings;
        this.requestTimeouts = requestTimeouts;
        this.peerResponseStats = peerResponseStats;
        this.messageHandler = messageHandler;
    }

    private void recordResponseTime(RequestTiming timing) {
        if (timing == null) {
            return;
        }
        double elapsedMillis = (System.nanoTime() - timing.startNanos()) / 1_000_000.0;
        // Transport stats are keyed by peer and RPC type so replication traffic
        // does not get blended into election or admin latency buckets.
        String key = timing.peerId() + "\t" + timing.rpcType();
        RunningStatistics stats = peerResponseStats.computeIfAbsent(key, ignored -> new RunningStatistics());
        synchronized (stats) {
            stats.addSample(elapsedMillis);
            if (log.isTraceEnabled()) {
                log.trace(
                        "Response time from {} {}: {} ms (n={}, mean={} ms, min={} ms, max={} ms)",
                        timing.peerId(),
                        timing.rpcType(),
                        String.format(java.util.Locale.ROOT, "%.3f", elapsedMillis),
                        stats.getCount(),
                        String.format(java.util.Locale.ROOT, "%.3f", stats.getMean()),
                        String.format(java.util.Locale.ROOT, "%.3f", stats.getMin()),
                        String.format(java.util.Locale.ROOT, "%.3f", stats.getMax())
                );
            }
        }
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Envelope envelope) throws Exception {
        String type = envelope.getType();
        if (type == null || type.isEmpty()) {
            log.warn("No 'type' in envelope");
            return;
        }

        String correlationId = envelope.getCorrelationId();
        if (correlationId == null || correlationId.isEmpty()) {
            log.warn("Incorrect message: No correlationId");
            return;
        }

        byte[] payload = envelope.getPayload().toByteArray();

        switch (type) {
            case "VoteResponse" -> {
                var response = ProtoMapper.parseVoteResponse(payload);
                if (response.isEmpty()) {
                    log.warn("Failed to parse VoteResponse payload");
                    return;
                }
                VoteResponse voteResponse = ProtoMapper.fromProto(response.get());

                // Lookup the future for this correlationId
                CompletableFuture<VoteResponse> fut = inFlightRequests.remove(correlationId);
                RequestTiming timing = requestTimings.remove(correlationId);
                ScheduledFuture<?> timeoutTask = requestTimeouts.remove(correlationId);
                if (timeoutTask != null) {
                    timeoutTask.cancel(false);
                }
                if (fut != null) {
                    fut.complete(voteResponse);
                    log.trace("Received {} for correlationId={}", voteResponse, correlationId);
                } else {
                    log.warn("No future found for VoteResponse correlationId={}", correlationId);
                }
                recordResponseTime(timing);
            }

            case "AppendEntriesResponse" -> {
                var response = ProtoMapper.parseAppendEntriesResponse(payload);
                if (response.isEmpty()) {
                    log.warn("Failed to parse AppendEntriesResponse payload");
                    return;
                }
                AppendEntriesResponse appendEntriesResponse = ProtoMapper.fromProto(response.get());

                CompletableFuture<AppendEntriesResponse> fut = inFlightAppendEntries.remove(correlationId);
                RequestTiming timing = requestTimings.remove(correlationId);
                ScheduledFuture<?> timeoutTask = requestTimeouts.remove(correlationId);
                if (timeoutTask != null) {
                    timeoutTask.cancel(false);
                }
                if (fut != null) {
                    fut.complete(appendEntriesResponse);
                    log.trace("Received {} for correlationId={}", appendEntriesResponse.getPeerId(), correlationId);
                } else {
                    // Benign race: request timeout/cleanup can remove the future before a late response arrives.
                    log.warn("No future found for AppendEntriesResponse correlationId={}", correlationId);
                }
                recordResponseTime(timing);
            }

            case "InstallSnapshotResponse" -> {
                var response = ProtoMapper.parseInstallSnapshotResponse(payload);
                if (response.isEmpty()) {
                    log.warn("Failed to parse InstallSnapshotResponse payload");
                    return;
                }
                InstallSnapshotResponse installSnapshotResponse = ProtoMapper.fromProto(response.get());

                CompletableFuture<InstallSnapshotResponse> fut = inFlightInstallSnapshot.remove(correlationId);
                RequestTiming timing = requestTimings.remove(correlationId);
                ScheduledFuture<?> timeoutTask = requestTimeouts.remove(correlationId);
                if (timeoutTask != null) {
                    timeoutTask.cancel(false);
                }
                if (fut != null) {
                    fut.complete(installSnapshotResponse);
                    log.trace("Received InstallSnapshotResponse from {} for correlationId={}", installSnapshotResponse.getPeerId(), correlationId);
                } else {
                    // Benign race: request timeout/cleanup can remove the future before a late response arrives.
                    log.warn("No future found for InstallSnapshotResponse correlationId={}", correlationId);
                }
                recordResponseTime(timing);
            }

            case "JoinClusterResponse" -> {
                var response = ProtoMapper.parseJoinClusterResponse(payload);
                if (response.isEmpty()) {
                    log.warn("Failed to parse JoinClusterResponse payload");
                    return;
                }
                JoinClusterResponse joinClusterResponse = ProtoMapper.fromProto(response.get());

                CompletableFuture<JoinClusterResponse> fut = inFlightJoinCluster.remove(correlationId);
                RequestTiming timing = requestTimings.remove(correlationId);
                ScheduledFuture<?> timeoutTask = requestTimeouts.remove(correlationId);
                if (timeoutTask != null) {
                    timeoutTask.cancel(false);
                }
                if (fut != null) {
                    fut.complete(joinClusterResponse);
                    log.trace("Received JoinClusterResponse from {} for correlationId={}", joinClusterResponse.getPeerId(), correlationId);
                } else {
                    log.warn("No future found for JoinClusterResponse correlationId={}", correlationId);
                }
                recordResponseTime(timing);
            }

            case "ClientCommandResponse" -> {
                var response = ProtoMapper.parseClientCommandResponse(payload);
                if (response.isEmpty()) {
                    log.warn("Failed to parse ClientCommandResponse payload");
                    return;
                }
                ClientCommandResponse clientCommandResponse = ProtoMapper.fromProto(response.get());

                CompletableFuture<ClientCommandResponse> fut = inFlightClientCommands.remove(correlationId);
                RequestTiming timing = requestTimings.remove(correlationId);
                ScheduledFuture<?> timeoutTask = requestTimeouts.remove(correlationId);
                if (timeoutTask != null) {
                    timeoutTask.cancel(false);
                }
                if (fut != null) {
                    fut.complete(clientCommandResponse);
                    log.trace("Received ClientCommandResponse from {} for correlationId={}", clientCommandResponse.getPeerId(), correlationId);
                } else {
                    log.warn("No future found for ClientCommandResponse correlationId={}", correlationId);
                }
                recordResponseTime(timing);
            }

            case "ClientQueryResponse" -> {
                var response = ProtoMapper.parseClientQueryResponse(payload);
                if (response.isEmpty()) {
                    log.warn("Failed to parse ClientQueryResponse payload");
                    return;
                }
                ClientQueryResponse clientQueryResponse = ProtoMapper.fromProto(response.get());

                CompletableFuture<ClientQueryResponse> fut = inFlightClientQueries.remove(correlationId);
                RequestTiming timing = requestTimings.remove(correlationId);
                ScheduledFuture<?> timeoutTask = requestTimeouts.remove(correlationId);
                if (timeoutTask != null) {
                    timeoutTask.cancel(false);
                }
                if (fut != null) {
                    fut.complete(clientQueryResponse);
                    log.trace("Received ClientQueryResponse from {} for correlationId={}", clientQueryResponse.getPeerId(), correlationId);
                } else {
                    log.warn("No future found for ClientQueryResponse correlationId={}", correlationId);
                }
                recordResponseTime(timing);
            }

            case "ClusterSummaryResponse" -> {
                var response = ProtoMapper.parseClusterSummaryResponse(payload);
                if (response.isEmpty()) {
                    log.warn("Failed to parse ClusterSummaryResponse payload");
                    return;
                }
                ClusterSummaryResponse clusterSummaryResponse = ProtoMapper.fromProto(response.get());

                CompletableFuture<ClusterSummaryResponse> fut = inFlightClusterSummary.remove(correlationId);
                RequestTiming timing = requestTimings.remove(correlationId);
                ScheduledFuture<?> timeoutTask = requestTimeouts.remove(correlationId);
                if (timeoutTask != null) {
                    timeoutTask.cancel(false);
                }
                if (fut != null) {
                    fut.complete(clusterSummaryResponse);
                    log.trace("Received ClusterSummaryResponse from {} for correlationId={}", clusterSummaryResponse.getPeerId(), correlationId);
                } else {
                    log.warn("No future found for ClusterSummaryResponse correlationId={}", correlationId);
                }
                recordResponseTime(timing);
            }

            case "ReconfigurationStatusResponse" -> {
                var response = ProtoMapper.parseReconfigurationStatusResponse(payload);
                if (response.isEmpty()) {
                    log.warn("Failed to parse ReconfigurationStatusResponse payload");
                    return;
                }
                ReconfigurationStatusResponse reconfigurationStatusResponse = ProtoMapper.fromProto(response.get());

                CompletableFuture<ReconfigurationStatusResponse> fut = inFlightReconfigurationStatus.remove(correlationId);
                RequestTiming timing = requestTimings.remove(correlationId);
                ScheduledFuture<?> timeoutTask = requestTimeouts.remove(correlationId);
                if (timeoutTask != null) {
                    timeoutTask.cancel(false);
                }
                if (fut != null) {
                    fut.complete(reconfigurationStatusResponse);
                    log.trace("Received ReconfigurationStatusResponse from {} for correlationId={}", reconfigurationStatusResponse.getPeerId(), correlationId);
                } else {
                    log.warn("No future found for ReconfigurationStatusResponse correlationId={}", correlationId);
                }
                recordResponseTime(timing);
            }

            case "TelemetryResponse" -> {
                var response = ProtoMapper.parseTelemetryResponse(payload);
                if (response.isEmpty()) {
                    log.warn("Failed to parse TelemetryResponse payload");
                    return;
                }
                TelemetryResponse telemetryResponse = ProtoMapper.fromProto(response.get());

                CompletableFuture<TelemetryResponse> fut = inFlightTelemetry.remove(correlationId);
                RequestTiming timing = requestTimings.remove(correlationId);
                ScheduledFuture<?> timeoutTask = requestTimeouts.remove(correlationId);
                if (timeoutTask != null) {
                    timeoutTask.cancel(false);
                }
                if (fut != null) {
                    fut.complete(telemetryResponse);
                } else {
                    log.warn("No future found for TelemetryResponse correlationId={}", correlationId);
                }
                recordResponseTime(timing);
            }

            case "JoinClusterStatusResponse" -> {
                var response = ProtoMapper.parseJoinClusterStatusResponse(payload);
                if (response.isEmpty()) {
                    log.warn("Failed to parse JoinClusterStatusResponse payload");
                    return;
                }
                JoinClusterStatusResponse joinClusterStatusResponse = ProtoMapper.fromProto(response.get());

                CompletableFuture<JoinClusterStatusResponse> fut = inFlightJoinStatus.remove(correlationId);
                RequestTiming timing = requestTimings.remove(correlationId);
                ScheduledFuture<?> timeoutTask = requestTimeouts.remove(correlationId);
                if (timeoutTask != null) {
                    timeoutTask.cancel(false);
                }
                if (fut != null) {
                    fut.complete(joinClusterStatusResponse);
                    log.trace("Received JoinClusterStatusResponse from {} for correlationId={}", joinClusterStatusResponse.getPeerId(), correlationId);
                } else {
                    log.warn("No future found for JoinClusterStatusResponse correlationId={}", correlationId);
                }
                recordResponseTime(timing);
            }

            case "ReconfigureClusterResponse" -> {
                var response = ProtoMapper.parseReconfigureClusterResponse(payload);
                if (response.isEmpty()) {
                    log.warn("Failed to parse ReconfigureClusterResponse payload");
                    return;
                }
                ReconfigureClusterResponse reconfigureClusterResponse = ProtoMapper.fromProto(response.get());

                CompletableFuture<ReconfigureClusterResponse> fut = inFlightReconfigureCluster.remove(correlationId);
                RequestTiming timing = requestTimings.remove(correlationId);
                ScheduledFuture<?> timeoutTask = requestTimeouts.remove(correlationId);
                if (timeoutTask != null) {
                    timeoutTask.cancel(false);
                }
                if (fut != null) {
                    fut.complete(reconfigureClusterResponse);
                    log.trace("Received ReconfigureClusterResponse from {} for correlationId={}", reconfigureClusterResponse.getPeerId(), correlationId);
                } else {
                    log.warn("No future found for ReconfigureClusterResponse correlationId={}", correlationId);
                }
                recordResponseTime(timing);
            }

            default -> {
                if (null != messageHandler) {
                    messageHandler.handle(correlationId, type, payload, ctx);
                }
            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Error in client response handler", cause);
        ctx.close();
    }
}
