package org.gautelis.raft;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.uuid.Generators;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
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


public class NettyRaftClient {
    private static final Logger log = LoggerFactory.getLogger(NettyRaftClient.class);

    private final EventLoopGroup group = new NioEventLoopGroup();
    private final Bootstrap bootstrap;
    private final ObjectMapper mapper = new ObjectMapper();
    private final Map<Peer, Channel> channels = new ConcurrentHashMap<>();

    // Shared map for in-flight requests
    private final Map<String, CompletableFuture<VoteResponse>> inFlightRequests = new ConcurrentHashMap<>();

    public NettyRaftClient() {
        bootstrap = new Bootstrap()
                .group(group)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline p = ch.pipeline();
                        p.addLast(new ByteBufToJsonDecoder());
                        p.addLast(new ClientResponseHandler(inFlightRequests));
                    }
                });
    }

    public Future<List<VoteResponse>> requestVoteFromAll(
            Collection<Peer> peers,
            VoteRequest req
    ) {
        // A Netty Promise that we'll complete once we have all responses
        Promise<List<VoteResponse>> promise = new DefaultPromise<>(group.next());

        List<VoteResponse> responses = new ArrayList<>();
        AtomicInteger count = new AtomicInteger(0);

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
                // We have not yet connected to this peer, or the channel died
                connect(peer).addListener((ChannelFuture cf) -> {
                    if (cf.isSuccess()) {
                        Channel newChannel = cf.channel();

                        // store the channel so next time we won't reconnect
                        channels.put(peer, newChannel);

                        log.trace("Request vote for term {} from {}", req.getTerm(), peer.getId());
                        CompletableFuture<VoteResponse> future = sendVoteRequest(newChannel, req);

                        // Attach aggregator callback
                        future.whenComplete((voteResponse, throwable) -> {
                            if (throwable != null) {
                                log.debug("Vote request to {} failed: {}", peer.getId(), throwable.toString());
                                responses.add(new VoteResponse(req, false)); // Negative vote
                            } else {
                                responses.add(voteResponse);
                            }
                            checkDone(responses, count, promise);
                        });
                    }
                    else {
                        log.info("Could not request vote for term {} from {}: cannot connect", req.getTerm(), peer.getId());

                        // treat it as negative
                        responses.add(new VoteResponse(req, false));
                        checkDone(responses, count, promise);
                    }
                });
            }
            else {
                // Channel is active, so we can send the request right away
                log.trace("Request vote for term {} from {}", req.getTerm(), peer.getId());
                CompletableFuture<VoteResponse> future = sendVoteRequest(channel, req);

                // same aggregator callback
                future.whenComplete((voteResponse, throwable) -> {
                    if (throwable != null) {
                        log.info("Vote request to {} failed: {}", peer.getId(), throwable.toString());
                        responses.add(new VoteResponse(req, false));
                    } else {
                        responses.add(voteResponse);
                    }
                    checkDone(responses, count, promise);
                });
            }
        }

        return promise;
    }

    private ChannelFuture connect(Peer peer) {
        ChannelFuture cf = bootstrap.connect(peer.getAddress());
        cf.addListener((ChannelFuture f) -> {
            if (f.isSuccess()) {
                channels.put(peer, f.channel());
            } else {
                log.warn("Failed to connect to {} at {}", peer.getId(), peer.getAddress());
            }
        });
        return cf;
    }


    private CompletableFuture<VoteResponse> sendVoteRequest(
            Channel ch,
            VoteRequest req
    ) {
        CompletableFuture<VoteResponse> future = new CompletableFuture<>();
        String correlationId = Generators.timeBasedEpochGenerator().generate().toString(); // Version 7
        inFlightRequests.put(correlationId, future);

        try {
            Message msg = new Message(correlationId, "VoteRequest", req);
            String json = mapper.writeValueAsString(msg);

            ch.writeAndFlush(Unpooled.copiedBuffer(json, StandardCharsets.UTF_8))
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
        if (current == /* number of peers */ channels.size()) {
            promise.setSuccess(responses);
        }
    }

    public void broadcastLogEntry(LogEntry logEntry) {
        for (Map.Entry<Peer, Channel> channelEntry : channels.entrySet()) {
            Peer peer = channelEntry.getKey();
            Channel channel = channelEntry.getValue();

            log.trace("Broadcasting {} for term {} to {}", logEntry.getType(), logEntry.getTerm(), peer.getId());

            if (channel == null || !channel.isActive()) {
                connect(peer).addListener((ChannelFuture f) -> {
                    if (f.isSuccess()) {
                        log.trace("Successfully broadcast {} for term {} to {}", logEntry.getType(), logEntry.getTerm(), peer.getId());
                    } else {
                        log.warn("Could not broadcast to {}: {}", logEntry.getPeerId(), f.cause());
                    }
                });
            } else {
                try {
                    String correlationId = Generators.timeBasedEpochGenerator().generate().toString(); // Version 7
                    Message msg = new Message(correlationId, "LogEntry", logEntry);
                    String json = mapper.writeValueAsString(msg);

                    channel.writeAndFlush(Unpooled.copiedBuffer(json, StandardCharsets.UTF_8))
                            .addListener(f -> {
                                if (!f.isSuccess()) {
                                    log.warn("Could not broadcast to {}", logEntry.getPeerId(), f.cause());
                                }
                            });
                } catch (Exception e) {
                    log.warn("Failed to broadcast to {}: {}", logEntry.getPeerId(), e.getMessage(), e);
                }
            }
        }
    }
}
