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
    private final Map<String, CompletableFuture<ClientCommandResponse>> inFlightClientCommands = new ConcurrentHashMap<>();
    private final Map<String, CompletableFuture<ClientQueryResponse>> inFlightClientQueries = new ConcurrentHashMap<>();
    private final Map<String, CompletableFuture<ClusterSummaryResponse>> inFlightClusterSummary = new ConcurrentHashMap<>();
    private final Map<String, CompletableFuture<JoinClusterResponse>> inFlightJoinCluster = new ConcurrentHashMap<>();
    private final Map<String, CompletableFuture<JoinClusterStatusResponse>> inFlightJoinStatus = new ConcurrentHashMap<>();
    private final Map<String, CompletableFuture<ReconfigureClusterResponse>> inFlightReconfigureCluster = new ConcurrentHashMap<>();
    private final Map<String, CompletableFuture<TelemetryResponse>> inFlightTelemetry = new ConcurrentHashMap<>();
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
                p.addLast(new ClientResponseHandler(inFlightRequests, inFlightAppendEntries, inFlightInstallSnapshot, inFlightClientCommands, inFlightClientQueries, inFlightClusterSummary, inFlightJoinCluster, inFlightJoinStatus, inFlightReconfigureCluster, inFlightTelemetry, requestTimings, requestTimeouts, peerResponseStats, messageHandler));
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
        // Replace, do not merge: membership changes should prune removed peers from
        // broadcast targets, channels, and response-time bookkeeping.
        Set<Peer> replacement = ConcurrentHashMap.newKeySet();
        if (peers != null) {
            for (Peer peer : peers) {
                if (peer != null) {
                    replacement.add(peer);
                }
            }
        }

        for (Peer existing : new ArrayList<>(knownPeers)) {
            if (!replacement.contains(existing)) {
                knownPeers.remove(existing);
                Channel removed = channels.remove(existing);
                if (removed != null) {
                    removed.close();
                }
                peerResponseStats.remove(existing.getId());
            }
        }
        knownPeers.addAll(replacement);
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
            inFlightJoinCluster.remove(correlationId);
            inFlightJoinStatus.remove(correlationId);
            inFlightReconfigureCluster.remove(correlationId);
            inFlightTelemetry.remove(correlationId);
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
                            inFlightJoinCluster.remove(correlationId);
                            inFlightJoinStatus.remove(correlationId);
                            inFlightReconfigureCluster.remove(correlationId);
                            inFlightTelemetry.remove(correlationId);
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
            inFlightJoinCluster.remove(correlationId);
            inFlightJoinStatus.remove(correlationId);
            inFlightReconfigureCluster.remove(correlationId);
            inFlightTelemetry.remove(correlationId);
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

    public CompletableFuture<Boolean> sendMessage(Peer peer, String type, byte[] payload) {
        knownPeers.add(peer);

        CompletableFuture<Boolean> outbound = new CompletableFuture<>();
        Channel channel = channels.get(peer);
        if (channel == null || !channel.isActive()) {
            connect(peer).addListener((ChannelFuture cf) -> {
                if (!cf.isSuccess()) {
                    outbound.complete(false);
                    return;
                }
                Channel newChannel = cf.channel();
                channels.put(peer, newChannel);
                sendMessageOverChannel(newChannel, type, payload).whenComplete((sent, error) -> {
                    if (error != null) {
                        outbound.complete(false);
                    } else {
                        outbound.complete(sent);
                    }
                });
            });
        } else {
            sendMessageOverChannel(channel, type, payload).whenComplete((sent, error) -> {
                if (error != null) {
                    outbound.complete(false);
                } else {
                    outbound.complete(sent);
                }
            });
        }
        return outbound;
    }

    public CompletableFuture<JoinClusterResponse> sendJoinClusterRequest(Peer peer, JoinClusterRequest request) {
        knownPeers.add(peer);

        CompletableFuture<JoinClusterResponse> outbound = new CompletableFuture<>();
        Channel channel = channels.get(peer);
        if (channel == null || !channel.isActive()) {
            connect(peer).addListener((ChannelFuture cf) -> {
                if (!cf.isSuccess()) {
                    outbound.complete(new JoinClusterResponse(request.getTerm(), peer.getId(), false, "UNREACHABLE", "Could not connect to peer", ""));
                    return;
                }
                Channel newChannel = cf.channel();
                channels.put(peer, newChannel);
                sendJoinClusterRequestOverChannel(newChannel, peer, request).whenComplete((response, error) -> {
                    if (error != null) {
                        outbound.complete(new JoinClusterResponse(request.getTerm(), peer.getId(), false, "FAILED", error.getMessage(), ""));
                    } else {
                        outbound.complete(response);
                    }
                });
            });
        } else {
            sendJoinClusterRequestOverChannel(channel, peer, request).whenComplete((response, error) -> {
                if (error != null) {
                    outbound.complete(new JoinClusterResponse(request.getTerm(), peer.getId(), false, "FAILED", error.getMessage(), ""));
                } else {
                    outbound.complete(response);
                }
            });
        }
        return outbound;
    }

    public CompletableFuture<ClientCommandResponse> sendClientCommandRequest(Peer peer, ClientCommandRequest request) {
        knownPeers.add(peer);

        CompletableFuture<ClientCommandResponse> outbound = new CompletableFuture<>();
        Channel channel = channels.get(peer);
        if (channel == null || !channel.isActive()) {
            connect(peer).addListener((ChannelFuture cf) -> {
                if (!cf.isSuccess()) {
                    outbound.complete(new ClientCommandResponse(request.getTerm(), peer.getId(), false, "UNREACHABLE", "Could not connect to peer", "", "", 0));
                    return;
                }
                Channel newChannel = cf.channel();
                channels.put(peer, newChannel);
                sendClientCommandRequestOverChannel(newChannel, peer, request).whenComplete((response, error) -> {
                    if (error != null) {
                        outbound.complete(new ClientCommandResponse(request.getTerm(), peer.getId(), false, "FAILED", error.getMessage(), "", "", 0));
                    } else {
                        outbound.complete(response);
                    }
                });
            });
        } else {
            sendClientCommandRequestOverChannel(channel, peer, request).whenComplete((response, error) -> {
                if (error != null) {
                    outbound.complete(new ClientCommandResponse(request.getTerm(), peer.getId(), false, "FAILED", error.getMessage(), "", "", 0));
                } else {
                    outbound.complete(response);
                }
            });
        }
        return outbound;
    }

    public CompletableFuture<ClientQueryResponse> sendClientQueryRequest(Peer peer, ClientQueryRequest request) {
        knownPeers.add(peer);

        CompletableFuture<ClientQueryResponse> outbound = new CompletableFuture<>();
        Channel channel = channels.get(peer);
        if (channel == null || !channel.isActive()) {
            connect(peer).addListener((ChannelFuture cf) -> {
                if (!cf.isSuccess()) {
                    outbound.complete(new ClientQueryResponse(request.getTerm(), peer.getId(), false, "UNREACHABLE", "Could not connect to peer", "", "", 0, new byte[0]));
                    return;
                }
                Channel newChannel = cf.channel();
                channels.put(peer, newChannel);
                sendClientQueryRequestOverChannel(newChannel, peer, request).whenComplete((response, error) -> {
                    if (error != null) {
                        outbound.complete(new ClientQueryResponse(request.getTerm(), peer.getId(), false, "FAILED", error.getMessage(), "", "", 0, new byte[0]));
                    } else {
                        outbound.complete(response);
                    }
                });
            });
        } else {
            sendClientQueryRequestOverChannel(channel, peer, request).whenComplete((response, error) -> {
                if (error != null) {
                    outbound.complete(new ClientQueryResponse(request.getTerm(), peer.getId(), false, "FAILED", error.getMessage(), "", "", 0, new byte[0]));
                } else {
                    outbound.complete(response);
                }
            });
        }
        return outbound;
    }

    public CompletableFuture<ClusterSummaryResponse> sendClusterSummaryRequest(Peer peer, ClusterSummaryRequest request) {
        knownPeers.add(peer);

        CompletableFuture<ClusterSummaryResponse> outbound = new CompletableFuture<>();
        Channel channel = channels.get(peer);
        if (channel == null || !channel.isActive()) {
            connect(peer).addListener((ChannelFuture cf) -> {
                if (!cf.isSuccess()) {
                    outbound.complete(new ClusterSummaryResponse(0L, request.getTerm(), peer.getId(), false, "UNREACHABLE", "", "", 0, "", "", false, "", "", false, false, false, 0, 0, 0, List.of(), List.of(), List.of()));
                    return;
                }
                Channel newChannel = cf.channel();
                channels.put(peer, newChannel);
                sendClusterSummaryRequestOverChannel(newChannel, peer, request).whenComplete((response, error) -> {
                    if (error != null) {
                        outbound.complete(new ClusterSummaryResponse(0L, request.getTerm(), peer.getId(), false, "FAILED", "", "", 0, "", "", false, "", "", false, false, false, 0, 0, 0, List.of(), List.of(), List.of()));
                    } else {
                        outbound.complete(response);
                    }
                });
            });
        } else {
            sendClusterSummaryRequestOverChannel(channel, peer, request).whenComplete((response, error) -> {
                if (error != null) {
                    outbound.complete(new ClusterSummaryResponse(0L, request.getTerm(), peer.getId(), false, "FAILED", "", "", 0, "", "", false, "", "", false, false, false, 0, 0, 0, List.of(), List.of(), List.of()));
                } else {
                    outbound.complete(response);
                }
            });
        }
        return outbound;
    }

    public CompletableFuture<JoinClusterStatusResponse> sendJoinClusterStatusRequest(Peer peer, JoinClusterStatusRequest request) {
        knownPeers.add(peer);

        CompletableFuture<JoinClusterStatusResponse> outbound = new CompletableFuture<>();
        Channel channel = channels.get(peer);
        if (channel == null || !channel.isActive()) {
            connect(peer).addListener((ChannelFuture cf) -> {
                if (!cf.isSuccess()) {
                    outbound.complete(new JoinClusterStatusResponse(request.getTerm(), peer.getId(), false, "UNREACHABLE", "Could not connect to peer", ""));
                    return;
                }
                Channel newChannel = cf.channel();
                channels.put(peer, newChannel);
                sendJoinClusterStatusRequestOverChannel(newChannel, peer, request).whenComplete((response, error) -> {
                    if (error != null) {
                        outbound.complete(new JoinClusterStatusResponse(request.getTerm(), peer.getId(), false, "FAILED", error.getMessage(), ""));
                    } else {
                        outbound.complete(response);
                    }
                });
            });
        } else {
            sendJoinClusterStatusRequestOverChannel(channel, peer, request).whenComplete((response, error) -> {
                if (error != null) {
                    outbound.complete(new JoinClusterStatusResponse(request.getTerm(), peer.getId(), false, "FAILED", error.getMessage(), ""));
                } else {
                    outbound.complete(response);
                }
            });
        }
        return outbound;
    }

    public CompletableFuture<ReconfigureClusterResponse> sendReconfigureClusterRequest(Peer peer, ReconfigureClusterRequest request) {
        knownPeers.add(peer);

        CompletableFuture<ReconfigureClusterResponse> outbound = new CompletableFuture<>();
        Channel channel = channels.get(peer);
        if (channel == null || !channel.isActive()) {
            connect(peer).addListener((ChannelFuture cf) -> {
                if (!cf.isSuccess()) {
                    outbound.complete(new ReconfigureClusterResponse(request.getTerm(), peer.getId(), false, "UNREACHABLE", "Could not connect to peer", ""));
                    return;
                }
                Channel newChannel = cf.channel();
                channels.put(peer, newChannel);
                sendReconfigureClusterRequestOverChannel(newChannel, peer, request).whenComplete((response, error) -> {
                    if (error != null) {
                        outbound.complete(new ReconfigureClusterResponse(request.getTerm(), peer.getId(), false, "FAILED", error.getMessage(), ""));
                    } else {
                        outbound.complete(response);
                    }
                });
            });
        } else {
            sendReconfigureClusterRequestOverChannel(channel, peer, request).whenComplete((response, error) -> {
                if (error != null) {
                    outbound.complete(new ReconfigureClusterResponse(request.getTerm(), peer.getId(), false, "FAILED", error.getMessage(), ""));
                } else {
                    outbound.complete(response);
                }
            });
        }
        return outbound;
    }

    public CompletableFuture<TelemetryResponse> sendTelemetryRequest(Peer peer, TelemetryRequest request) {
        knownPeers.add(peer);

        CompletableFuture<TelemetryResponse> outbound = new CompletableFuture<>();
        Channel channel = channels.get(peer);
        if (channel == null || !channel.isActive()) {
            connect(peer).addListener((ChannelFuture cf) -> {
                if (!cf.isSuccess()) {
                    outbound.complete(new TelemetryResponse(0L, request.getTerm(), peer.getId(), false, "UNREACHABLE", "", "", "", "", false, false, 0, 0, 0, 0, 0, 0, 0, 0, false, List.of(), List.of(), List.of(), List.of(), List.of(), List.of(), "", false, false, false, 0, 0, 0, "", List.of(), List.of(), List.of()));
                    return;
                }
                Channel newChannel = cf.channel();
                channels.put(peer, newChannel);
                sendTelemetryRequestOverChannel(newChannel, peer, request).whenComplete((response, error) -> {
                    if (error != null) {
                        outbound.complete(new TelemetryResponse(0L, request.getTerm(), peer.getId(), false, "FAILED", "", "", "", "", false, false, 0, 0, 0, 0, 0, 0, 0, 0, false, List.of(), List.of(), List.of(), List.of(), List.of(), List.of(), "", false, false, false, 0, 0, 0, "", List.of(), List.of(), List.of()));
                    } else {
                        outbound.complete(response);
                    }
                });
            });
        } else {
            sendTelemetryRequestOverChannel(channel, peer, request).whenComplete((response, error) -> {
                if (error != null) {
                    outbound.complete(new TelemetryResponse(0L, request.getTerm(), peer.getId(), false, "FAILED", "", "", "", "", false, false, 0, 0, 0, 0, 0, 0, 0, 0, false, List.of(), List.of(), List.of(), List.of(), List.of(), List.of(), "", false, false, false, 0, 0, 0, "", List.of(), List.of(), List.of()));
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

    private CompletableFuture<Boolean> sendMessageOverChannel(Channel ch, String type, byte[] payload) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        try {
            byte[] safePayload = payload == null ? new byte[0] : payload;
            Envelope envelope = ProtoMapper.wrap(
                    java.util.UUID.randomUUID().toString(),
                    type,
                    safePayload
            );
            ch.writeAndFlush(envelope).addListener(f -> {
                if (!f.isSuccess()) {
                    future.completeExceptionally(f.cause());
                    return;
                }
                future.complete(true);
            });
        } catch (Exception e) {
            future.completeExceptionally(e);
        }
        return future;
    }

    private CompletableFuture<ClientCommandResponse> sendClientCommandRequestOverChannel(Channel ch, Peer peer, ClientCommandRequest request) {
        String correlationId = java.util.UUID.randomUUID().toString();
        CompletableFuture<ClientCommandResponse> future = new CompletableFuture<>();
        inFlightClientCommands.put(correlationId, future);
        requestTimings.put(correlationId, new RequestTiming(peer.getId(), System.nanoTime()));

        ScheduledFuture<?> timeoutTask = ch.eventLoop().schedule(() -> {
            if (future.isDone()) {
                return;
            }
            inFlightClientCommands.remove(correlationId);
            requestTimings.remove(correlationId);
            requestTimeouts.remove(correlationId);
            future.complete(new ClientCommandResponse(request.getTerm(), peer.getId(), false, "TIMEOUT", "Timed out waiting for ClientCommandResponse", "", "", 0));
        }, voteRequestTimeoutMillis, TimeUnit.MILLISECONDS);
        requestTimeouts.put(correlationId, timeoutTask);

        try {
            var protoRequest = ProtoMapper.toProto(request);
            Envelope envelope = ProtoMapper.wrap(correlationId, "ClientCommandRequest", protoRequest.toByteString());
            ch.writeAndFlush(envelope).addListener(f -> {
                if (!f.isSuccess()) {
                    inFlightClientCommands.remove(correlationId);
                    requestTimings.remove(correlationId);
                    ScheduledFuture<?> pendingTimeout = requestTimeouts.remove(correlationId);
                    if (pendingTimeout != null) {
                        pendingTimeout.cancel(false);
                    }
                    future.completeExceptionally(f.cause());
                }
            });
        } catch (Exception e) {
            inFlightClientCommands.remove(correlationId);
            requestTimings.remove(correlationId);
            ScheduledFuture<?> pendingTimeout = requestTimeouts.remove(correlationId);
            if (pendingTimeout != null) {
                pendingTimeout.cancel(false);
            }
            future.completeExceptionally(e);
        }
        return future;
    }

    private CompletableFuture<ClientQueryResponse> sendClientQueryRequestOverChannel(Channel ch, Peer peer, ClientQueryRequest request) {
        String correlationId = java.util.UUID.randomUUID().toString();
        CompletableFuture<ClientQueryResponse> future = new CompletableFuture<>();
        inFlightClientQueries.put(correlationId, future);
        requestTimings.put(correlationId, new RequestTiming(peer.getId(), System.nanoTime()));

        ScheduledFuture<?> timeoutTask = ch.eventLoop().schedule(() -> {
            if (future.isDone()) {
                return;
            }
            inFlightClientQueries.remove(correlationId);
            requestTimings.remove(correlationId);
            requestTimeouts.remove(correlationId);
            future.complete(new ClientQueryResponse(request.getTerm(), peer.getId(), false, "TIMEOUT", "Timed out waiting for ClientQueryResponse", "", "", 0, new byte[0]));
        }, voteRequestTimeoutMillis, TimeUnit.MILLISECONDS);
        requestTimeouts.put(correlationId, timeoutTask);

        try {
            var protoRequest = ProtoMapper.toProto(request);
            Envelope envelope = ProtoMapper.wrap(correlationId, "ClientQueryRequest", protoRequest.toByteString());
            ch.writeAndFlush(envelope).addListener(f -> {
                if (!f.isSuccess()) {
                    inFlightClientQueries.remove(correlationId);
                    requestTimings.remove(correlationId);
                    ScheduledFuture<?> pendingTimeout = requestTimeouts.remove(correlationId);
                    if (pendingTimeout != null) {
                        pendingTimeout.cancel(false);
                    }
                    future.completeExceptionally(f.cause());
                }
            });
        } catch (Exception e) {
            inFlightClientQueries.remove(correlationId);
            requestTimings.remove(correlationId);
            ScheduledFuture<?> pendingTimeout = requestTimeouts.remove(correlationId);
            if (pendingTimeout != null) {
                pendingTimeout.cancel(false);
            }
            future.completeExceptionally(e);
        }
        return future;
    }

    private CompletableFuture<ClusterSummaryResponse> sendClusterSummaryRequestOverChannel(Channel ch, Peer peer, ClusterSummaryRequest request) {
        String correlationId = java.util.UUID.randomUUID().toString();
        CompletableFuture<ClusterSummaryResponse> future = new CompletableFuture<>();
        inFlightClusterSummary.put(correlationId, future);
        requestTimings.put(correlationId, new RequestTiming(peer.getId(), System.nanoTime()));

        ScheduledFuture<?> timeoutTask = ch.eventLoop().schedule(() -> {
            if (future.isDone()) {
                return;
            }
            inFlightClusterSummary.remove(correlationId);
            requestTimings.remove(correlationId);
            requestTimeouts.remove(correlationId);
            future.complete(new ClusterSummaryResponse(0L, request.getTerm(), peer.getId(), false, "TIMEOUT", "", "", 0, "", "", false, "", "", false, false, false, 0, 0, 0, List.of(), List.of(), List.of()));
        }, voteRequestTimeoutMillis, TimeUnit.MILLISECONDS);
        requestTimeouts.put(correlationId, timeoutTask);

        try {
            var protoRequest = ProtoMapper.toProto(request);
            Envelope envelope = ProtoMapper.wrap(correlationId, "ClusterSummaryRequest", protoRequest.toByteString());
            ch.writeAndFlush(envelope).addListener(f -> {
                if (!f.isSuccess()) {
                    inFlightClusterSummary.remove(correlationId);
                    requestTimings.remove(correlationId);
                    ScheduledFuture<?> pendingTimeout = requestTimeouts.remove(correlationId);
                    if (pendingTimeout != null) {
                        pendingTimeout.cancel(false);
                    }
                    future.completeExceptionally(f.cause());
                }
            });
        } catch (Exception e) {
            inFlightClusterSummary.remove(correlationId);
            requestTimings.remove(correlationId);
            ScheduledFuture<?> pendingTimeout = requestTimeouts.remove(correlationId);
            if (pendingTimeout != null) {
                pendingTimeout.cancel(false);
            }
            future.completeExceptionally(e);
        }
        return future;
    }

    private CompletableFuture<JoinClusterResponse> sendJoinClusterRequestOverChannel(Channel ch, Peer peer, JoinClusterRequest request) {
        String correlationId = java.util.UUID.randomUUID().toString();
        CompletableFuture<JoinClusterResponse> future = new CompletableFuture<>();
        inFlightJoinCluster.put(correlationId, future);
        requestTimings.put(correlationId, new RequestTiming(peer.getId(), System.nanoTime()));

        ScheduledFuture<?> timeoutTask = ch.eventLoop().schedule(() -> {
            if (future.isDone()) {
                return;
            }
            inFlightJoinCluster.remove(correlationId);
            requestTimings.remove(correlationId);
            requestTimeouts.remove(correlationId);
            future.complete(new JoinClusterResponse(request.getTerm(), peer.getId(), false, "TIMEOUT", "Timed out waiting for JoinClusterResponse", ""));
        }, voteRequestTimeoutMillis, TimeUnit.MILLISECONDS);
        requestTimeouts.put(correlationId, timeoutTask);

        try {
            var protoRequest = ProtoMapper.toProto(request);
            Envelope envelope = ProtoMapper.wrap(correlationId, "JoinClusterRequest", protoRequest.toByteString());
            ch.writeAndFlush(envelope).addListener(f -> {
                if (!f.isSuccess()) {
                    inFlightJoinCluster.remove(correlationId);
                    requestTimings.remove(correlationId);
                    ScheduledFuture<?> pendingTimeout = requestTimeouts.remove(correlationId);
                    if (pendingTimeout != null) {
                        pendingTimeout.cancel(false);
                    }
                    future.completeExceptionally(f.cause());
                }
            });
        } catch (Exception e) {
            inFlightJoinCluster.remove(correlationId);
            requestTimings.remove(correlationId);
            ScheduledFuture<?> pendingTimeout = requestTimeouts.remove(correlationId);
            if (pendingTimeout != null) {
                pendingTimeout.cancel(false);
            }
            future.completeExceptionally(e);
        }
        return future;
    }

    private CompletableFuture<JoinClusterStatusResponse> sendJoinClusterStatusRequestOverChannel(Channel ch, Peer peer, JoinClusterStatusRequest request) {
        String correlationId = java.util.UUID.randomUUID().toString();
        CompletableFuture<JoinClusterStatusResponse> future = new CompletableFuture<>();
        inFlightJoinStatus.put(correlationId, future);
        requestTimings.put(correlationId, new RequestTiming(peer.getId(), System.nanoTime()));

        ScheduledFuture<?> timeoutTask = ch.eventLoop().schedule(() -> {
            if (future.isDone()) {
                return;
            }
            inFlightJoinStatus.remove(correlationId);
            requestTimings.remove(correlationId);
            requestTimeouts.remove(correlationId);
            future.complete(new JoinClusterStatusResponse(request.getTerm(), peer.getId(), false, "TIMEOUT", "Timed out waiting for JoinClusterStatusResponse", ""));
        }, voteRequestTimeoutMillis, TimeUnit.MILLISECONDS);
        requestTimeouts.put(correlationId, timeoutTask);

        try {
            var protoRequest = ProtoMapper.toProto(request);
            Envelope envelope = ProtoMapper.wrap(correlationId, "JoinClusterStatusRequest", protoRequest.toByteString());
            ch.writeAndFlush(envelope).addListener(f -> {
                if (!f.isSuccess()) {
                    inFlightJoinStatus.remove(correlationId);
                    requestTimings.remove(correlationId);
                    ScheduledFuture<?> pendingTimeout = requestTimeouts.remove(correlationId);
                    if (pendingTimeout != null) {
                        pendingTimeout.cancel(false);
                    }
                    future.completeExceptionally(f.cause());
                }
            });
        } catch (Exception e) {
            inFlightJoinStatus.remove(correlationId);
            requestTimings.remove(correlationId);
            ScheduledFuture<?> pendingTimeout = requestTimeouts.remove(correlationId);
            if (pendingTimeout != null) {
                pendingTimeout.cancel(false);
            }
            future.completeExceptionally(e);
        }
        return future;
    }

    private CompletableFuture<ReconfigureClusterResponse> sendReconfigureClusterRequestOverChannel(Channel ch, Peer peer, ReconfigureClusterRequest request) {
        String correlationId = java.util.UUID.randomUUID().toString();
        CompletableFuture<ReconfigureClusterResponse> future = new CompletableFuture<>();
        inFlightReconfigureCluster.put(correlationId, future);
        requestTimings.put(correlationId, new RequestTiming(peer.getId(), System.nanoTime()));

        ScheduledFuture<?> timeoutTask = ch.eventLoop().schedule(() -> {
            if (future.isDone()) {
                return;
            }
            inFlightReconfigureCluster.remove(correlationId);
            requestTimings.remove(correlationId);
            requestTimeouts.remove(correlationId);
            future.complete(new ReconfigureClusterResponse(request.getTerm(), peer.getId(), false, "TIMEOUT", "Timed out waiting for ReconfigureClusterResponse", ""));
        }, voteRequestTimeoutMillis, TimeUnit.MILLISECONDS);
        requestTimeouts.put(correlationId, timeoutTask);

        try {
            var protoRequest = ProtoMapper.toProto(request);
            Envelope envelope = ProtoMapper.wrap(correlationId, "ReconfigureClusterRequest", protoRequest.toByteString());
            ch.writeAndFlush(envelope).addListener(f -> {
                if (!f.isSuccess()) {
                    inFlightReconfigureCluster.remove(correlationId);
                    requestTimings.remove(correlationId);
                    ScheduledFuture<?> pendingTimeout = requestTimeouts.remove(correlationId);
                    if (pendingTimeout != null) {
                        pendingTimeout.cancel(false);
                    }
                    future.completeExceptionally(f.cause());
                }
            });
        } catch (Exception e) {
            inFlightReconfigureCluster.remove(correlationId);
            requestTimings.remove(correlationId);
            ScheduledFuture<?> pendingTimeout = requestTimeouts.remove(correlationId);
            if (pendingTimeout != null) {
                pendingTimeout.cancel(false);
            }
            future.completeExceptionally(e);
        }
        return future;
    }

    private CompletableFuture<TelemetryResponse> sendTelemetryRequestOverChannel(Channel ch, Peer peer, TelemetryRequest request) {
        String correlationId = java.util.UUID.randomUUID().toString();
        CompletableFuture<TelemetryResponse> future = new CompletableFuture<>();
        inFlightTelemetry.put(correlationId, future);
        requestTimings.put(correlationId, new RequestTiming(peer.getId(), System.nanoTime()));

        ScheduledFuture<?> timeoutTask = ch.eventLoop().schedule(() -> {
            if (future.isDone()) {
                return;
            }
            inFlightTelemetry.remove(correlationId);
            requestTimings.remove(correlationId);
            requestTimeouts.remove(correlationId);
            future.complete(new TelemetryResponse(0L, request.getTerm(), peer.getId(), false, "TIMEOUT", "", "", "", "", false, false, 0, 0, 0, 0, 0, 0, 0, 0, false, List.of(), List.of(), List.of(), List.of(), List.of(), List.of(), "", false, false, false, 0, 0, 0, "", List.of(), List.of(), List.of()));
        }, voteRequestTimeoutMillis, TimeUnit.MILLISECONDS);
        requestTimeouts.put(correlationId, timeoutTask);

        try {
            var telemetryRequest = ProtoMapper.toProto(request);
            Envelope envelope = ProtoMapper.wrap(correlationId, "TelemetryRequest", telemetryRequest.toByteString());
            ch.writeAndFlush(envelope).addListener(f -> {
                if (!f.isSuccess()) {
                    inFlightTelemetry.remove(correlationId);
                    requestTimings.remove(correlationId);
                    ScheduledFuture<?> pendingTimeout = requestTimeouts.remove(correlationId);
                    if (pendingTimeout != null) {
                        pendingTimeout.cancel(false);
                    }
                    future.completeExceptionally(f.cause());
                }
            });
        } catch (Exception e) {
            inFlightTelemetry.remove(correlationId);
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
        // The set is updated from async connect callbacks, so it must be concurrent.
        Collection<Peer> unreachablePeers = ConcurrentHashMap.newKeySet();

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

    public List<TelemetryPeerStats> snapshotResponseTimeStats() {
        List<String> peerIds = new ArrayList<>(peerResponseStats.keySet());
        Collections.sort(peerIds);
        List<TelemetryPeerStats> snapshot = new ArrayList<>();
        for (String peerId : peerIds) {
            RunningStatistics stats = peerResponseStats.get(peerId);
            if (stats == null) {
                continue;
            }
            synchronized (stats) {
                snapshot.add(new TelemetryPeerStats(
                        peerId,
                        stats.getCount(),
                        stats.getCount() == 0 ? 0.0 : stats.getMean(),
                        stats.getCount() == 0 ? 0.0 : stats.getMin(),
                        stats.getCount() == 0 ? 0.0 : stats.getMax(),
                        stats.getCount() == 0 ? 0.0 : stats.getCV()
                ));
            }
        }
        return List.copyOf(snapshot);
    }

    public Collection<Peer> getKnownPeersForTest() {
        return Set.copyOf(knownPeers);
    }

    public boolean isPeerReachable(String peerId) {
        if (peerId == null || peerId.isBlank()) {
            return false;
        }
        for (Map.Entry<Peer, Channel> entry : channels.entrySet()) {
            if (peerId.equals(entry.getKey().getId())) {
                Channel channel = entry.getValue();
                return channel != null && channel.isActive();
            }
        }
        return false;
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
