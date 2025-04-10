package org.gautelis.raft;

import com.fasterxml.jackson.databind.ObjectMapper;
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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class NettyRaftClient {
    private final EventLoopGroup group = new NioEventLoopGroup();
    private final Bootstrap bootstrap;
    private final ObjectMapper mapper = new ObjectMapper();
    private final Map<Peer, Channel> channels = new ConcurrentHashMap<>();

    public NettyRaftClient() {
        bootstrap = new Bootstrap()
                .group(group)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline p = ch.pipeline();
                        p.addLast(new ByteBufToJsonDecoder());
                        p.addLast(new ClientResponseHandler());
                    }
                });
    }

    public Future<List<VoteResponse>> requestVoteFromAll(VoteRequest req) {
        // In a real scenario, you'd gather all peer channels, send the request,
        // and use a CountDownLatch or some aggregator Future to gather results.
        Promise<List<VoteResponse>> promise = new DefaultPromise<>(group.next());
        List<VoteResponse> responses = new ArrayList<>();
        AtomicInteger count = new AtomicInteger(0);

        for (Peer peer : getPeers()) {
            Channel ch = channels.get(peer);
            if (ch == null || !ch.isActive()) {
                // connect on-demand
                connect(peer).addListener((ChannelFuture cf) -> {
                    if (cf.isSuccess()) {
                        sendVoteRequest(cf.channel(), req, responses, count, promise);
                    } else {
                        // handle fail => default negative vote
                        responses.add(new VoteResponse(req.getTerm(), false));
                        checkDone(responses, count, promise);
                    }
                });
            } else {
                sendVoteRequest(ch, req, responses, count, promise);
            }
        }

        return promise;
    }

    private ChannelFuture connect(Peer peer) {
        return bootstrap.connect(peer.getAddress());
    }

    private void sendVoteRequest(Channel ch, VoteRequest req,
                                 List<VoteResponse> responses,
                                 AtomicInteger count,
                                 Promise<List<VoteResponse>> promise) {
        // The client response handler (ClientResponseHandler) must know how
        // to correlate the incoming response with the request. Possibly store
        // a callback or correlation ID. For simplicity, let's just do
        // "fire-and-forget" and rely on some shared structure.
        try {
            Message msg = new Message("VoteRequest", req);
            String json = mapper.writeValueAsString(msg);
            ch.writeAndFlush(Unpooled.copiedBuffer(json, StandardCharsets.UTF_8))
                    .addListener(f -> {
                        if (!f.isSuccess()) {
                            // treat it as a negative
                            responses.add(new VoteResponse(req.getTerm(), false));
                            checkDone(responses, count, promise);
                        }
                    });
        } catch (Exception e) {
            responses.add(new VoteResponse(req.getTerm(), false));
            checkDone(responses, count, promise);
        }
    }

    private void checkDone(List<VoteResponse> responses,
                           AtomicInteger count,
                           Promise<List<VoteResponse>> promise) {
        int current = count.incrementAndGet();
        if (current == getPeers().size()) {
            promise.setSuccess(responses);
        }
    }

    public void broadcastLogEntry(LogEntry entry) {
        // TODO
    }

    List<Peer> getPeers() {
        List<Peer> peers = new ArrayList<>(); // TODO
        return peers;
    }
}
