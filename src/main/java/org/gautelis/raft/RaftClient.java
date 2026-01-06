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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import org.gautelis.raft.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.CompletableFuture;


public class RaftClient {
    private static final Logger log = LoggerFactory.getLogger(RaftClient.class);

    //
    //private final EventLoopGroup group = new NioEventLoopGroup();
    private final EventLoopGroup group = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());

    private final Bootstrap bootstrap;

    //
    private final ObjectMapper mapper = new ObjectMapper();
    private final Map<Peer, Channel> channels = new ConcurrentHashMap<>();

    // Shared map for in-flight requests
    private final Map<String, CompletableFuture<VoteResponse>> inFlightRequests = new ConcurrentHashMap<>();

    private ChannelInitializer<SocketChannel> getChannelInitializer(MessageHandler messageHandler) {
        return new ChannelInitializer<>() {
            protected void initChannel(SocketChannel ch) {
                log.trace("Initializing client channel: {}", ch);

                ChannelPipeline p = ch.pipeline();
                p.addLast(new ByteBufToJsonDecoder());
                p.addLast(new ClientResponseHandler(inFlightRequests, messageHandler));
            }
        };
    }

    public RaftClient(MessageHandler messageHandler) {
        bootstrap = new Bootstrap()
                .group(group)
                .channel(NioSocketChannel.class)
                .handler(getChannelInitializer(messageHandler))
                .option(ChannelOption.TCP_NODELAY, true) // disables Nagle's algorithm
                .option(ChannelOption.SO_KEEPALIVE, true) // helps detect dropped peers
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 1000);  // fast-fail if peer is down
    }

    public void shutdown() {
        group.shutdownGracefully();
    }

    public Future<List<VoteResponse>> requestVoteFromAll(
            Collection<Peer> peers,
            VoteRequest req
    ) {
        // A Netty Promise that we'll complete once we have all responses
        Promise<List<VoteResponse>> promise = new DefaultPromise<>(group.next());

        List<VoteResponse> responses = new ArrayList<>();
        AtomicInteger responseCount = new AtomicInteger(0);

        // Edge case: if no peers
        if (peers.isEmpty()) {
            promise.setSuccess(responses);
            return promise;
        }

        if (channels.isEmpty()) {
            log.trace("No channels yet established");
        }
        else {
            log.trace("{} channels available", channels.size());
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

                        log.trace("Request vote for term {} from {}", req.getTerm(), peer.getId());
                        CompletableFuture<VoteResponse> future = sendVoteRequest(newChannel, req);

                        // Attach aggregator callback
                        future.whenComplete((voteResponse, throwable) -> {
                            if (throwable != null) {
                                log.debug("Vote request to {} failed: {}", peer.getId(), throwable.toString());
                                responses.add(new VoteResponse(req, peer.getId(), false, -1)); // Synthetic negative vote
                            } else {
                                responses.add(voteResponse);
                            }
                            checkDone(responses, responseCount, promise);
                        });
                    }
                    else {
                        log.info("Could not request vote for term {} from {}: cannot connect", req.getTerm(), peer.getId());

                        // treat it as negative
                        responses.add(new VoteResponse(req, peer.getId(), false, -1)); // Synthetic negative vote
                        checkDone(responses, responseCount, promise);
                    }
                });
            }
            else {
                // Channel is active, so we can send the request right away
                log.trace("Request vote for term {} from {}", req.getTerm(), peer.getId());
                CompletableFuture<VoteResponse> future = sendVoteRequest(channel, req);

                // same aggregator callback as above
                future.whenComplete((voteResponse, throwable) -> {
                    if (throwable != null) {
                        log.info("Vote request to {} failed: {}", peer.getId(), throwable.toString());
                        responses.add(new VoteResponse(req, peer.getId(),false, -1)); // Synthetic negative vote
                    } else {
                        responses.add(voteResponse);
                    }
                    checkDone(responses, responseCount, promise);
                });
            }
        }

        return promise;
    }

    private ChannelFuture connect(Peer peer) {
        log.trace("Connecting to {}", peer.getAddress());

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
                    log.trace("Failed to connect to {} at {}: {}", peer.getId(), peer.getAddress(), f.cause());
                }
            }
        });
        return cf;
    }


    private CompletableFuture<VoteResponse> sendVoteRequest(
            Channel ch,
            VoteRequest req
    ) {
        Message requestMessage = new Message("VoteRequest", req);
        String correlationId = requestMessage.getCorrelationId();

        CompletableFuture<VoteResponse> future = new CompletableFuture<>();
        inFlightRequests.put(correlationId, future);

        try {
            String requestJson = mapper.writeValueAsString(requestMessage);

            ch.writeAndFlush(Unpooled.copiedBuffer(requestJson, StandardCharsets.UTF_8))
                    .addListener(f -> {
                        if (!f.isSuccess()) {
                            // If we couldn't even send the request, treat as a failure
                            log.info("Failed to send vote request: {}", f.cause().toString());
                            future.completeExceptionally(f.cause());
                        }
                        else {
                            // The request is now 'in flight'. We'll complete 'future' once
                            // the server responds (see ClientResponseHandler).
                            log.trace("Successfully sent vote request");
                        }
                    });
        } catch (Exception e) {
            log.info("Exception building or sending vote request: {}", e.getMessage(), e);
            future.completeExceptionally(e);
        }
        return future;
    }

    private void checkDone(
            List<VoteResponse> responses,
            AtomicInteger count,
            Promise<List<VoteResponse>> promise
    ) {
        int current = count.incrementAndGet();
        if (current == /* number of connected peers */ channels.size()) {
            promise.trySuccess(responses);
        }
    }

    public void broadcastHeartbeat(Heartbeat heartbeat) {
        for (Map.Entry<Peer, Channel> channelEntry : channels.entrySet()) {
            Peer peer = channelEntry.getKey();
            Channel channel = channelEntry.getValue();

            if (log.isTraceEnabled()) {
                log.trace("Broadcasting heartbeat for term {} to {}", heartbeat.getTerm(), peer.getId());
            }

            if (channel == null || !channel.isActive()) {
                // We are not connected to this peer, so we will not be able
                // to broadcast anything *this* round -- but we will initiate
                // a connect so that we are connected *next* round...

                connect(peer).addListener((ChannelFuture f) -> {
                    if (!f.isSuccess()) {
                        // We should log at some higher level, but since this situation
                        // may continue for some time and flood the log we will refrain
                        // from warn, info and even debug logging
                        if (log.isTraceEnabled()) {
                            log.trace("Could not broadcast to {} at {}", peer.getId(), peer.getAddress(), f.cause());
                        }
                    }
                });
            } else {
                try {
                    Message msg = new Message("Heartbeat", heartbeat);
                    String json = mapper.writeValueAsString(msg);

                    channel.writeAndFlush(Unpooled.copiedBuffer(json, StandardCharsets.UTF_8))
                            .addListener(f -> {
                                if (!f.isSuccess()) {
                                    log.debug("Could not broadcast heartbeat to {}", heartbeat.getPeerId(), f.cause());
                                }
                            });
                } catch (JsonProcessingException jpe) {
                    log.warn("{} failed to serialize heartbeat object: {}", heartbeat.getPeerId(), jpe.getMessage(), jpe);

                } catch (Exception e) {
                    log.warn("Failed to broadcast heartbeat to {}", heartbeat.getPeerId(), e);
                }
            }
        }
    }

    public void broadcastLogEntry(LogEntry logEntry) {
        for (Map.Entry<Peer, Channel> channelEntry : channels.entrySet()) {
            Peer peer = channelEntry.getKey();
            Channel channel = channelEntry.getValue();

            if (log.isTraceEnabled()) {
                log.trace("Broadcasting log entry for term {} to {}", logEntry.getTerm(), peer.getId());
            }

            if (channel == null || !channel.isActive()) {
                // We are not connected to this peer, so we will not be able
                // to broadcast anything *this* round -- but we will initiate
                // a connect so that we are connected *next* round...

                connect(peer).addListener((ChannelFuture f) -> {
                    if (!f.isSuccess()) {
                        // We should log at some higher level, but since this situation
                        // may continue for some time and flood the log we will refrain
                        // from warn, info and even debug logging
                        if (log.isTraceEnabled()) {
                            log.trace("Could not broadcast to {} at {}", peer.getId(), peer.getAddress(), f.cause());
                        }
                    }
                });
            } else {
                try {
                    Message msg = new Message("LogEntry", logEntry);
                    String json = mapper.writeValueAsString(msg);

                    channel.writeAndFlush(Unpooled.copiedBuffer(json, StandardCharsets.UTF_8))
                            .addListener(f -> {
                                if (!f.isSuccess()) {
                                    log.debug("Could not broadcast log entry to {}", logEntry.getPeerId(), f.cause());
                                }
                            });
                } catch (JsonProcessingException jpe) {
                    log.warn("{} failed to serialize log entry object: {}", logEntry.getPeerId(), jpe.getMessage(), jpe);

                } catch (Exception e) {
                    log.warn("Failed to broadcast log entry to {}", logEntry.getPeerId(), e);
                }
            }
        }
    }

    public Collection<Peer> broadcast(String type, long term, String json) {
        Collection<Peer> unreachablePeers = new HashSet<>();

        for (Map.Entry<Peer, Channel> channelEntry : channels.entrySet()) {
            Peer peer = channelEntry.getKey();
            Channel channel = channelEntry.getValue();

            log.trace("Broadcasting '{}' for term {} to {}", type, term, peer.getId());

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
                            log.trace("Could not broadcast to {}", peer.getId(), f.cause());
                        }
                    }
                });
            } else {
                try {
                    channel.writeAndFlush(Unpooled.copiedBuffer(json, StandardCharsets.UTF_8))
                           .addListener(f -> {
                                if (!f.isSuccess()) {
                                    log.debug("Could not broadcast to {}", peer.getId(), f.cause());
                                }
                            });
                } catch (Exception e) {
                    log.warn("Failed to broadcast to {}", peer.getId(), e);
                }
            }
        }
        return unreachablePeers;
    }
}
