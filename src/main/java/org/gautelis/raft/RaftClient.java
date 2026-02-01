package org.gautelis.raft;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import org.gautelis.raft.model.*;
import org.gautelis.raft.proto.Envelope;
import org.gautelis.vopn.statistics.RunningStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.CompletableFuture;


public class RaftClient {
    private static final Logger log = LoggerFactory.getLogger(RaftClient.class);
    private static final Logger statisticsLog = LoggerFactory.getLogger("STATISTICS");
    private static final int DEFAULT_STATS_INTERVAL_SECONDS = 120;

    protected final String clientId; // mostly for logging purposes

    //private final EventLoopGroup group = new NioEventLoopGroup();
    private final EventLoopGroup group = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());

    private final Bootstrap bootstrap;

    //
    private final Map<Peer, Channel> channels = new ConcurrentHashMap<>();

    // Shared map for in-flight requests
    private final Map<String, CompletableFuture<VoteResponse>> inFlightRequests = new ConcurrentHashMap<>();
    private final Map<String, RequestTiming> requestTimings = new ConcurrentHashMap<>();
    private final Map<String, RunningStatistics> peerResponseStats = new ConcurrentHashMap<>();
    private final ScheduledFuture<?> statisticsTask;

    private ChannelInitializer<SocketChannel> getChannelInitializer(MessageHandler messageHandler) {
        return new ChannelInitializer<>() {
            protected void initChannel(SocketChannel ch) {
                log.trace("{}: Initializing client channel: {}", clientId, ch);

                ChannelPipeline p = ch.pipeline();
                p.addLast(new ProtobufLiteDecoder());
                p.addLast(new ProtobufLiteEncoder());
                p.addLast(new ClientResponseHandler(inFlightRequests, requestTimings, peerResponseStats, messageHandler));
            }
        };
    }

    public RaftClient(String clientId, MessageHandler messageHandler) {
        this.clientId = clientId;

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
                            checkDone(responses, responseCount, promise);
                        });
                    }
                    else {
                        log.info("{} could not request vote for term {} from {}: cannot connect", clientId, req.getTerm(), peer.getId());

                        // treat it as negative
                        responses.add(new VoteResponse(req, peer.getId(), false, -1)); // Synthetic negative vote
                        checkDone(responses, responseCount, promise);
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
                    checkDone(responses, responseCount, promise);
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

        try {
            var voteRequest = ProtoMapper.toProto(req);
            Envelope envelope = ProtoMapper.wrap(correlationId, "VoteRequest", voteRequest.toByteString());

            ch.writeAndFlush(envelope)
                    .addListener(f -> {
                        if (!f.isSuccess()) {
                            // If we couldn't even send the request, treat as a failure
                            log.info("{}: Failed to send vote request: {}", clientId, f.cause().toString());
                            inFlightRequests.remove(correlationId);
                            requestTimings.remove(correlationId);
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
            requestTimings.remove(correlationId);
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
                log.trace("{}: Broadcasting heartbeat for term {} to {}", clientId, heartbeat.getTerm(), peer.getId());
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
                            log.trace("{}: Could not broadcast to {} at {}", clientId, peer.getId(), peer.getAddress(), f.cause());
                        }
                    }
                });
            } else {
                try {
                    var hb = ProtoMapper.toProto(heartbeat);
                    Envelope envelope = ProtoMapper.wrap(
                            java.util.UUID.randomUUID().toString(),
                            "Heartbeat",
                            hb.toByteString()
                    );
                    channel.writeAndFlush(envelope)
                            .addListener(f -> {
                                if (!f.isSuccess()) {
                                    log.debug("{}: Could not broadcast heartbeat to {}", clientId, heartbeat.getPeerId(), f.cause());
                                }
                            });
                } catch (Exception e) {
                    log.warn("{}: Failed to broadcast heartbeat to {}", clientId, heartbeat.getPeerId(), e);
                }
            }
        }
    }

    public void broadcastLogEntry(LogEntry logEntry) {
        for (Map.Entry<Peer, Channel> channelEntry : channels.entrySet()) {
            Peer peer = channelEntry.getKey();
            Channel channel = channelEntry.getValue();

            if (log.isTraceEnabled()) {
                log.trace("{}: Broadcasting log entry for term {} to {}", clientId, logEntry.getTerm(), peer.getId());
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
                            log.trace("{}: Could not broadcast to {} at {}", clientId, peer.getId(), peer.getAddress(), f.cause());
                        }
                    }
                });
            } else {
                try {
                    var entry = ProtoMapper.toProto(logEntry);
                    Envelope envelope = ProtoMapper.wrap(
                            java.util.UUID.randomUUID().toString(),
                            "LogEntry",
                            entry.toByteString()
                    );
                    channel.writeAndFlush(envelope)
                            .addListener(f -> {
                                if (!f.isSuccess()) {
                                    log.debug("{}: Could not broadcast log entry to {}", clientId, logEntry.getPeerId(), f.cause());
                                }
                            });
                } catch (Exception e) {
                    log.warn("{}: Failed to broadcast log entry to {}", clientId, logEntry.getPeerId(), e);
                }
            }
        }
    }

    public Collection<Peer> broadcast(String type, long term, byte[] payload) {
        Collection<Peer> unreachablePeers = new HashSet<>();

        for (Map.Entry<Peer, Channel> channelEntry : channels.entrySet()) {
            Peer peer = channelEntry.getKey();
            Channel channel = channelEntry.getValue();

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
