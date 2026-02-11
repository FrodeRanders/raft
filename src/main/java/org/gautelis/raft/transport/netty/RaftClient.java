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

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import org.gautelis.raft.MessageHandler;
import org.gautelis.raft.protocol.*;
import org.gautelis.raft.proto.Envelope;
import org.gautelis.raft.serialization.ProtoMapper;
import org.gautelis.vopn.statistics.RunningStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;


public class RaftClient {
    // Outbound RPC helper used by RaftNode.
    // Figure 2 mapping:
    // - Candidates send RequestVote RPCs during elections.
    // - Leaders send AppendEntries (heartbeats/replication) and InstallSnapshot.
    private static final Logger log = LoggerFactory.getLogger(RaftClient.class);
    private static final Logger statisticsLog = LoggerFactory.getLogger("STATISTICS");
    private static final int DEFAULT_STATS_INTERVAL_SECONDS = 120;
    private static final int DEFAULT_VOTE_REQUEST_TIMEOUT_MILLIS = 1_500;

    protected final String clientId; // mostly for logging purposes

    private final EventLoopGroup group = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());

    private final Bootstrap bootstrap;

    //
    private final Map<Peer, Channel> channels = new ConcurrentHashMap<>();
    private final Set<Peer> knownPeers = ConcurrentHashMap.newKeySet();

    // Correlation-id indexed in-flight requests. Completed by ClientResponseHandler when responses arrive.
    private final Map<String, CompletableFuture<VoteResponse>> inFlightRequests = new ConcurrentHashMap<>();
    private final Map<String, CompletableFuture<AppendEntriesResponse>> inFlightAppendEntries = new ConcurrentHashMap<>();
    private final Map<String, CompletableFuture<InstallSnapshotResponse>> inFlightInstallSnapshot = new ConcurrentHashMap<>();
    private final Map<String, RequestTiming> requestTimings = new ConcurrentHashMap<>();
    private final Map<String, ScheduledFuture<?>> requestTimeouts = new ConcurrentHashMap<>();
    private final Map<String, RunningStatistics> peerResponseStats = new ConcurrentHashMap<>();
    private final ScheduledFuture<?> statisticsTask;
    private final int voteRequestTimeoutMillis;

    private ChannelInitializer<SocketChannel> getChannelInitializer(MessageHandler messageHandler) {
        return new ChannelInitializer<>() {
            protected void initChannel(SocketChannel ch) {
                log.trace("{}: Initializing client channel: {}", clientId, ch);

                ChannelPipeline p = ch.pipeline();
                p.addLast(new ProtobufLiteDecoder());
                p.addLast(new ProtobufLiteEncoder());
                p.addLast(new ClientResponseHandler(inFlightRequests, inFlightAppendEntries, inFlightInstallSnapshot, requestTimings, requestTimeouts, peerResponseStats, messageHandler));
            }
        };
    }

    public RaftClient(String clientId, MessageHandler messageHandler) {
        this.clientId = clientId;
        voteRequestTimeoutMillis = Integer.getInteger("raft.vote.request.timeout.millis", DEFAULT_VOTE_REQUEST_TIMEOUT_MILLIS);

        bootstrap = new Bootstrap()
                .group(group)
                .channel(NioSocketChannel.class)
                .handler(getChannelInitializer(messageHandler))
                .option(ChannelOption.TCP_NODELAY, true) // disables Nagle's algorithm
                .option(ChannelOption.SO_KEEPALIVE, true) // helps detect dropped peers
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 1000);  // fast-fail if peer is down

        int intervalSeconds = Integer.getInteger("raft.statistics.interval.seconds", DEFAULT_STATS_INTERVAL_SECONDS);
        if (intervalSeconds > 0) {
            statisticsTask = group.next().scheduleAtFixedRate(
                    this::logResponseTimeStatistics,
                    intervalSeconds,
                    intervalSeconds,
                    TimeUnit.SECONDS
            );
        } else {
            statisticsTask = null;
        }
    }

    public void shutdown() {
        if (statisticsTask != null) {
            statisticsTask.cancel(false);
        }
        for (ScheduledFuture<?> timeoutTask : requestTimeouts.values()) {
            if (timeoutTask != null) {
                timeoutTask.cancel(false);
            }
        }
        requestTimeouts.clear();
        group.shutdownGracefully();
    }

    public void setKnownPeers(Collection<Peer> peers) {
        if (peers == null) {
            return;
        }
        for (Peer peer : peers) {
            if (peer != null) {
                knownPeers.add(peer);
            }
        }
    }

    public Future<List<VoteResponse>> requestVoteFromAll(
            Collection<Peer> peers,
            VoteRequest req
    ) {
        // Figure 2 ("Candidates"): candidate sends RequestVote RPCs to all other servers.
        // This method fans out vote requests and aggregates responses for leader election logic.
        // A Netty Promise that we'll complete once we have all responses
        Promise<List<VoteResponse>> promise = new DefaultPromise<>(group.next());

        Queue<VoteResponse> responses = new ConcurrentLinkedQueue<>();
        AtomicInteger responseCount = new AtomicInteger(0);

        // Edge case: if no peers
        if (peers.isEmpty()) {
            promise.setSuccess(List.of());
            return promise;
        }
        knownPeers.addAll(peers);
        int expectedResponses = peers.size();

        if (channels.isEmpty()) {
            log.trace("{}: No channels yet established", clientId);
        }
        else {
            log.trace("{}: {} channels available", clientId, channels.size());
        }

        for (Peer peer : peers) {
            Channel channel = channels.get(peer);
            if (channel == null || !channel.isActive()) {
                // We have not yet connected to this peer, or the channel died, so
                // we need to await a working connection before we can actually send
                // the request.
                connect(peer).addListener((ChannelFuture cf) -> {
                    if (cf.isSuccess()) {
                        Channel newChannel = cf.channel();

                        // Store the channel so next time we won't reconnect
                        channels.put(peer, newChannel);

                        log.trace("{} requesting vote for term {} from {}", clientId, req.getTerm(), peer.getId());
                        CompletableFuture<VoteResponse> future = sendVoteRequest(newChannel, peer, req);

                        // Attach aggregator callback
                        future.whenComplete((voteResponse, throwable) -> {
                            if (throwable != null) {
                                log.debug("{}: Vote request to {} failed: {}", clientId, peer.getId(), throwable.toString());
                                responses.add(new VoteResponse(req, peer.getId(), false, -1)); // Synthetic negative vote
                            } else {
                                responses.add(voteResponse);
                            }
                            checkDone(responses, responseCount, expectedResponses, promise);
                        });
                    }
                    else {
                        log.info("{} could not request vote for term {} from {}: cannot connect", clientId, req.getTerm(), peer.getId());

                        // treat it as negative
                        responses.add(new VoteResponse(req, peer.getId(), false, -1)); // Synthetic negative vote
                        checkDone(responses, responseCount, expectedResponses, promise);
                    }
                });
            }
            else {
                // Channel is active, so we can send the request right away
                log.trace("{} requesting vote for term {} from {}", clientId, req.getTerm(), peer.getId());
                CompletableFuture<VoteResponse> future = sendVoteRequest(channel, peer, req);

                // same aggregator callback as above
                future.whenComplete((voteResponse, throwable) -> {
                    if (throwable != null) {
                        log.info("{}: Vote request to {} failed: {}", clientId, peer.getId(), throwable.toString());
                        responses.add(new VoteResponse(req, peer.getId(),false, -1)); // Synthetic negative vote
                    } else {
                        responses.add(voteResponse);
                    }
                    checkDone(responses, responseCount, expectedResponses, promise);
                });
            }
        }

        return promise;
    }

    private ChannelFuture connect(Peer peer) {
        log.trace("{}: Connecting to {}", clientId, peer.getAddress());

        ChannelFuture cf = bootstrap.connect(peer.getAddress());
        cf.addListener((ChannelFuture f) -> {
            if (f.isSuccess()) {
                // Store or replace this channel (to peer)
                channels.put(peer, f.channel());
            } else {
                // We should log at some higher level, but since this situation
                // may continue for some time and flood the log we will refrain
                // from warn, info and even debug logging
                if (log.isTraceEnabled()) {
                    log.trace("{}: Failed to connect to {} at {}: {}", clientId, peer.getId(), peer.getAddress(), f.cause());
                }
            }
        });
        return cf;
    }


    private CompletableFuture<VoteResponse> sendVoteRequest(Channel ch, Peer peer, VoteRequest req) {
        String correlationId = java.util.UUID.randomUUID().toString();

        CompletableFuture<VoteResponse> future = new CompletableFuture<>();
        inFlightRequests.put(correlationId, future);
        requestTimings.put(correlationId, new RequestTiming(peer.getId(), System.nanoTime()));
        ScheduledFuture<?> timeoutTask = ch.eventLoop().schedule(() -> {
            if (future.isDone()) {
                return;
            }
            inFlightRequests.remove(correlationId);
            inFlightAppendEntries.remove(correlationId);
            inFlightInstallSnapshot.remove(correlationId);
            requestTimings.remove(correlationId);
            requestTimeouts.remove(correlationId);
            future.completeExceptionally(new TimeoutException("Timed out waiting for VoteResponse from " + peer.getId()));
        }, voteRequestTimeoutMillis, TimeUnit.MILLISECONDS);
        requestTimeouts.put(correlationId, timeoutTask);

        try {
            var voteRequest = ProtoMapper.toProto(req);
            Envelope envelope = ProtoMapper.wrap(correlationId, "VoteRequest", voteRequest.toByteString());

            ch.writeAndFlush(envelope)
                    .addListener(f -> {
                        if (!f.isSuccess()) {
                            // If we couldn't even send the request, treat as a failure
                            log.info("{}: Failed to send vote request: {}", clientId, f.cause().toString());
                            inFlightRequests.remove(correlationId);
                            inFlightAppendEntries.remove(correlationId);
                            inFlightInstallSnapshot.remove(correlationId);
                            requestTimings.remove(correlationId);
                            ScheduledFuture<?> pendingTimeout = requestTimeouts.remove(correlationId);
                            if (pendingTimeout != null) {
                                pendingTimeout.cancel(false);
                            }
                            future.completeExceptionally(f.cause());
                        }
                        else {
                            // The request is now 'in flight'. We'll complete 'future' once
                            // the server responds (see ClientResponseHandler).
                            log.trace("{}: Successfully sent vote request", clientId);
                        }
                    });
        } catch (Exception e) {
            log.info("Exception building or sending vote request: {}", e.getMessage(), e);
            inFlightRequests.remove(correlationId);
            inFlightAppendEntries.remove(correlationId);
            inFlightInstallSnapshot.remove(correlationId);
            requestTimings.remove(correlationId);
            ScheduledFuture<?> pendingTimeout = requestTimeouts.remove(correlationId);
            if (pendingTimeout != null) {
                pendingTimeout.cancel(false);
            }
            future.completeExceptionally(e);
        }
        return future;
    }

    public CompletableFuture<AppendEntriesResponse> sendAppendEntries(Peer peer, AppendEntriesRequest req) {
        // Figure 2 ("Leaders"): replication and heartbeat are both AppendEntries RPCs.
        knownPeers.add(peer);

        CompletableFuture<AppendEntriesResponse> outbound = new CompletableFuture<>();
        Channel channel = channels.get(peer);
        if (channel == null || !channel.isActive()) {
            connect(peer).addListener((ChannelFuture cf) -> {
                if (!cf.isSuccess()) {
                    outbound.complete(new AppendEntriesResponse(req.getTerm(), peer.getId(), false, -1));
                    return;
                }
                Channel newChannel = cf.channel();
                channels.put(peer, newChannel);
                sendAppendEntriesOverChannel(newChannel, peer, req).whenComplete((response, error) -> {
                    if (error != null) {
                        outbound.complete(new AppendEntriesResponse(req.getTerm(), peer.getId(), false, -1));
                    } else {
                        outbound.complete(response);
                    }
                });
            });
        } else {
            sendAppendEntriesOverChannel(channel, peer, req).whenComplete((response, error) -> {
                if (error != null) {
                    outbound.complete(new AppendEntriesResponse(req.getTerm(), peer.getId(), false, -1));
                } else {
                    outbound.complete(response);
                }
            });
        }
        return outbound;
    }

    private CompletableFuture<AppendEntriesResponse> sendAppendEntriesOverChannel(Channel ch, Peer peer, AppendEntriesRequest req) {
        String correlationId = java.util.UUID.randomUUID().toString();
        CompletableFuture<AppendEntriesResponse> future = new CompletableFuture<>();
        inFlightAppendEntries.put(correlationId, future);
        requestTimings.put(correlationId, new RequestTiming(peer.getId(), System.nanoTime()));

        ScheduledFuture<?> timeoutTask = ch.eventLoop().schedule(() -> {
            if (future.isDone()) {
                return;
            }
            inFlightAppendEntries.remove(correlationId);
            requestTimings.remove(correlationId);
            requestTimeouts.remove(correlationId);
            future.completeExceptionally(new TimeoutException("Timed out waiting for AppendEntriesResponse from " + peer.getId()));
        }, voteRequestTimeoutMillis, TimeUnit.MILLISECONDS);
        requestTimeouts.put(correlationId, timeoutTask);

        try {
            var appendEntriesRequest = ProtoMapper.toProto(req);
            Envelope envelope = ProtoMapper.wrap(correlationId, "AppendEntriesRequest", appendEntriesRequest.toByteString());
            ch.writeAndFlush(envelope).addListener(f -> {
                if (!f.isSuccess()) {
                    inFlightAppendEntries.remove(correlationId);
                    requestTimings.remove(correlationId);
                    ScheduledFuture<?> pendingTimeout = requestTimeouts.remove(correlationId);
                    if (pendingTimeout != null) {
                        pendingTimeout.cancel(false);
                    }
                    future.completeExceptionally(f.cause());
                }
            });
        } catch (Exception e) {
            inFlightAppendEntries.remove(correlationId);
            requestTimings.remove(correlationId);
            ScheduledFuture<?> pendingTimeout = requestTimeouts.remove(correlationId);
            if (pendingTimeout != null) {
                pendingTimeout.cancel(false);
            }
            future.completeExceptionally(e);
        }
        return future;
    }

    public CompletableFuture<InstallSnapshotResponse> sendInstallSnapshot(Peer peer, InstallSnapshotRequest req) {
        // Snapshot catch-up path when a follower is behind the compacted prefix.
        // This supports Figure 2 AppendEntries consistency flow together with log compaction.
        knownPeers.add(peer);

        CompletableFuture<InstallSnapshotResponse> outbound = new CompletableFuture<>();
        Channel channel = channels.get(peer);
        if (channel == null || !channel.isActive()) {
            connect(peer).addListener((ChannelFuture cf) -> {
                if (!cf.isSuccess()) {
                    outbound.complete(new InstallSnapshotResponse(req.getTerm(), peer.getId(), false, req.getLastIncludedIndex()));
                    return;
                }
                Channel newChannel = cf.channel();
                channels.put(peer, newChannel);
                sendInstallSnapshotOverChannel(newChannel, peer, req).whenComplete((response, error) -> {
                    if (error != null) {
                        outbound.complete(new InstallSnapshotResponse(req.getTerm(), peer.getId(), false, req.getLastIncludedIndex()));
                    } else {
                        outbound.complete(response);
                    }
                });
            });
        } else {
            sendInstallSnapshotOverChannel(channel, peer, req).whenComplete((response, error) -> {
                if (error != null) {
                    outbound.complete(new InstallSnapshotResponse(req.getTerm(), peer.getId(), false, req.getLastIncludedIndex()));
                } else {
                    outbound.complete(response);
                }
            });
        }
        return outbound;
    }

    private CompletableFuture<InstallSnapshotResponse> sendInstallSnapshotOverChannel(Channel ch, Peer peer, InstallSnapshotRequest req) {
        String correlationId = java.util.UUID.randomUUID().toString();
        CompletableFuture<InstallSnapshotResponse> future = new CompletableFuture<>();
        inFlightInstallSnapshot.put(correlationId, future);
        requestTimings.put(correlationId, new RequestTiming(peer.getId(), System.nanoTime()));

        ScheduledFuture<?> timeoutTask = ch.eventLoop().schedule(() -> {
            if (future.isDone()) {
                return;
            }
            inFlightInstallSnapshot.remove(correlationId);
            requestTimings.remove(correlationId);
            requestTimeouts.remove(correlationId);
            future.completeExceptionally(new TimeoutException("Timed out waiting for InstallSnapshotResponse from " + peer.getId()));
        }, voteRequestTimeoutMillis, TimeUnit.MILLISECONDS);
        requestTimeouts.put(correlationId, timeoutTask);

        try {
            var installSnapshotRequest = ProtoMapper.toProto(req);
            Envelope envelope = ProtoMapper.wrap(correlationId, "InstallSnapshotRequest", installSnapshotRequest.toByteString());
            ch.writeAndFlush(envelope).addListener(f -> {
                if (!f.isSuccess()) {
                    inFlightInstallSnapshot.remove(correlationId);
                    requestTimings.remove(correlationId);
                    ScheduledFuture<?> pendingTimeout = requestTimeouts.remove(correlationId);
                    if (pendingTimeout != null) {
                        pendingTimeout.cancel(false);
                    }
                    future.completeExceptionally(f.cause());
                }
            });
        } catch (Exception e) {
            inFlightInstallSnapshot.remove(correlationId);
            requestTimings.remove(correlationId);
            ScheduledFuture<?> pendingTimeout = requestTimeouts.remove(correlationId);
            if (pendingTimeout != null) {
                pendingTimeout.cancel(false);
            }
            future.completeExceptionally(e);
        }
        return future;
    }

    private void checkDone(
            Queue<VoteResponse> responses,
            AtomicInteger count,
            int expectedResponses,
            Promise<List<VoteResponse>> promise
    ) {
        int current = count.incrementAndGet();
        if (current == expectedResponses) {
            promise.trySuccess(new ArrayList<>(responses));
        }
    }

    public Collection<Peer> broadcast(String type, long term, byte[] payload) {
        // Generic best-effort broadcast utility for non-Raft control messages.
        Collection<Peer> unreachablePeers = new HashSet<>();

        for (Peer peer : knownPeers) {
            Channel channel = channels.get(peer);

            log.trace("{}: Broadcasting '{}' for term {} to {}", clientId, type, term, peer.getId());

            if (channel == null || !channel.isActive()) {
                // We are not connected to this peer, so we will not be able
                // to broadcast anything *this* round -- but we will initiate
                // a connect so that we are connected *next* round...

                connect(peer).addListener((ChannelFuture f) -> {
                    unreachablePeers.add(peer);

                    if (!f.isSuccess()) {
                        // We should log at some higher level, but since this situation
                        // may continue for some time and flood the log we will refrain
                        // from warn, info and even debug logging
                        if (log.isTraceEnabled()) {
                            log.trace("{}: Could not broadcast to {}", clientId, peer.getId(), f.cause());
                        }
                    }
                });
            } else {
                try {
                    byte[] safePayload = payload == null ? new byte[0] : payload;
                    Envelope envelope = ProtoMapper.wrap(
                            java.util.UUID.randomUUID().toString(),
                            type,
                            safePayload
                    );
                    channel.writeAndFlush(envelope)
                           .addListener(f -> {
                                if (!f.isSuccess()) {
                                    log.debug("{}: Could not broadcast to {}", clientId, peer.getId(), f.cause());
                                }
                            });
                } catch (Exception e) {
                    log.warn("{}: Failed to broadcast to {}", clientId, peer.getId(), e);
                }
            }
        }
        return unreachablePeers;
    }

    public Map<String, RunningStatistics> getResponseTimeStats() {
        return Collections.unmodifiableMap(peerResponseStats);
    }

    public RunningStatistics getResponseTimeStats(String peerId) {
        return peerResponseStats.get(peerId);
    }

    private void logResponseTimeStatistics() {
        if (!statisticsLog.isInfoEnabled()) {
            return;
        }

        if (peerResponseStats.isEmpty()) {
            statisticsLog.info("{}: response-times: haven't sent requests", clientId);
            return;
        }

        List<String> peerIds = new ArrayList<>(peerResponseStats.keySet());
        Collections.sort(peerIds);

        StringBuilder line = new StringBuilder(clientId).append(": response-times");
        for (String peerId : peerIds) {
            RunningStatistics stats = peerResponseStats.get(peerId);
            if (stats == null) {
                continue;
            }
            String fragment;
            synchronized (stats) {
                if (stats.getCount() == 0) {
                    fragment = String.format(
                            java.util.Locale.ROOT, " %s[n=0]", peerId);
                } else {
                    fragment = String.format(
                            java.util.Locale.ROOT,
                            " %s[n=%d mean=%.3fms min=%.3fms max=%.3fms cv=%.2f%%]",
                            peerId,
                            stats.getCount(),
                            stats.getMean(),
                            stats.getMin(),
                            stats.getMax(),
                            stats.getCV()
                    );
                }
            }
            line.append("\n   ").append(fragment);
        }

        statisticsLog.info(line.toString());
    }
}
