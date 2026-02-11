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
import org.gautelis.raft.protocol.InstallSnapshotResponse;
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
    private final Map<String, RequestTiming> requestTimings;
    private final Map<String, ScheduledFuture<?>> requestTimeouts;
    private final Map<String, RunningStatistics> peerResponseStats;

    public ClientResponseHandler(
            Map<String, CompletableFuture<VoteResponse>> inFlightRequests,
            Map<String, CompletableFuture<AppendEntriesResponse>> inFlightAppendEntries,
            Map<String, CompletableFuture<InstallSnapshotResponse>> inFlightInstallSnapshot,
            Map<String, RequestTiming> requestTimings,
            Map<String, ScheduledFuture<?>> requestTimeouts,
            Map<String, RunningStatistics> peerResponseStats,
            MessageHandler messageHandler
    ) {
        this.inFlightRequests = inFlightRequests;
        this.inFlightAppendEntries = inFlightAppendEntries;
        this.inFlightInstallSnapshot = inFlightInstallSnapshot;
        this.requestTimings = requestTimings;
        this.requestTimeouts = requestTimeouts;
        this.peerResponseStats = peerResponseStats;
        this.messageHandler = messageHandler;
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

                if (timing != null) {
                    double elapsedMillis = (System.nanoTime() - timing.startNanos()) / 1_000_000.0;
                    RunningStatistics stats = peerResponseStats.computeIfAbsent(
                            timing.peerId(),
                            k -> new RunningStatistics()
                    );
                    if (log.isTraceEnabled()) {
                        synchronized (stats) {
                            stats.addSample(elapsedMillis);
                            log.trace(
                                    "Response time from {}: {} ms (n={}, mean={} ms, min={} ms, max={} ms)",
                                    timing.peerId(),
                                    String.format(java.util.Locale.ROOT, "%.3f", elapsedMillis),
                                    stats.getCount(),
                                    String.format(java.util.Locale.ROOT, "%.3f", stats.getMean()),
                                    String.format(java.util.Locale.ROOT, "%.3f", stats.getMin()),
                                    String.format(java.util.Locale.ROOT, "%.3f", stats.getMax())
                            );
                        }
                    } else {
                        synchronized (stats) {
                            stats.addSample(elapsedMillis);
                        }
                    }
                }
            }

            case "AppendEntriesResponse" -> {
                var response = ProtoMapper.parseAppendEntriesResponse(payload);
                if (response.isEmpty()) {
                    log.warn("Failed to parse AppendEntriesResponse payload");
                    return;
                }
                AppendEntriesResponse appendEntriesResponse = ProtoMapper.fromProto(response.get());

                CompletableFuture<AppendEntriesResponse> fut = inFlightAppendEntries.remove(correlationId);
                requestTimings.remove(correlationId);
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
            }

            case "InstallSnapshotResponse" -> {
                var response = ProtoMapper.parseInstallSnapshotResponse(payload);
                if (response.isEmpty()) {
                    log.warn("Failed to parse InstallSnapshotResponse payload");
                    return;
                }
                InstallSnapshotResponse installSnapshotResponse = ProtoMapper.fromProto(response.get());

                CompletableFuture<InstallSnapshotResponse> fut = inFlightInstallSnapshot.remove(correlationId);
                requestTimings.remove(correlationId);
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
