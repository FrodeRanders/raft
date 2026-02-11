package org.gautelis.raft;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.Future;
import org.gautelis.raft.model.AppendEntriesRequest;
import org.gautelis.raft.model.AppendEntriesResponse;
import org.gautelis.raft.model.Peer;
import org.gautelis.raft.model.VoteRequest;
import org.gautelis.raft.model.VoteResponse;
import org.gautelis.raft.proto.Envelope;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RaftClientIntegrationTest {
    private static final Logger log = LoggerFactory.getLogger(RaftClientIntegrationTest.class);

    static class VoteServerHandler extends SimpleChannelInboundHandler<Envelope> {
        private final String serverId;

        VoteServerHandler(String serverId) {
            this.serverId = serverId;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Envelope envelope) throws Exception {
            if (envelope.getType().isEmpty() || envelope.getCorrelationId().isEmpty()) {
                return;
            }
            String type = envelope.getType();
            if (!"VoteRequest".equals(type)) {
                return;
            }

            String correlationId = envelope.getCorrelationId();
            var reqProto = ProtoMapper.parseVoteRequest(envelope.getPayload().toByteArray());
            if (reqProto.isEmpty()) {
                return;
            }
            VoteRequest req = ProtoMapper.fromProto(reqProto.get());
            VoteResponse resp = new VoteResponse(req, serverId, true, req.getTerm());
            Envelope response = ProtoMapper.wrap(
                    correlationId,
                    "VoteResponse",
                    ProtoMapper.toProto(resp).toByteString()
            );
            ctx.writeAndFlush(response);
        }
    }

    static class SilentVoteServerHandler extends SimpleChannelInboundHandler<Envelope> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Envelope envelope) {
            // Intentionally ignore vote requests to exercise client-side timeout behavior.
        }
    }

    static class AppendEntriesServerHandler extends SimpleChannelInboundHandler<Envelope> {
        private final AtomicInteger receivedRequests;
        private final CountDownLatch latch;
        private final String peerId;

        AppendEntriesServerHandler(String peerId, AtomicInteger receivedRequests, CountDownLatch latch) {
            this.peerId = peerId;
            this.receivedRequests = receivedRequests;
            this.latch = latch;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Envelope envelope) throws Exception {
            if (!"AppendEntriesRequest".equals(envelope.getType())) {
                return;
            }
            if (envelope.getCorrelationId().isEmpty()) {
                return;
            }
            var request = ProtoMapper.parseAppendEntriesRequest(envelope.getPayload().toByteArray());
            if (request.isEmpty()) {
                return;
            }
            receivedRequests.incrementAndGet();
            latch.countDown();
            AppendEntriesResponse response = new AppendEntriesResponse(
                    request.get().getTerm(),
                    peerId,
                    true,
                    request.get().getPrevLogIndex() + request.get().getEntriesCount()
            );
            Envelope responseEnvelope = ProtoMapper.wrap(
                    envelope.getCorrelationId(),
                    "AppendEntriesResponse",
                    ProtoMapper.toProto(response).toByteString()
            );
            ctx.writeAndFlush(responseEnvelope);
        }
    }

    static class GenericServerHandler extends SimpleChannelInboundHandler<Envelope> {
        private final String expectedType;
        private final AtomicInteger receivedMessages;
        private final CountDownLatch latch;

        GenericServerHandler(String expectedType, AtomicInteger receivedMessages, CountDownLatch latch) {
            this.expectedType = expectedType;
            this.receivedMessages = receivedMessages;
            this.latch = latch;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Envelope envelope) {
            if (!expectedType.equals(envelope.getType())) {
                return;
            }
            receivedMessages.incrementAndGet();
            latch.countDown();
        }
    }

    @Test
    void requestVoteFromAllReceivesVoteResponse() throws Exception {
        log.info("*** Testcase *** requestVoteFromAll receives VoteResponse from server");

        Assumptions.assumeTrue(Boolean.getBoolean("netty.it"),
                "Set -Dnetty.it=true to enable this integration test.");

        EventLoopGroup boss = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        EventLoopGroup worker = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());

        RaftClient client = new RaftClient("test", null);
        Channel server = null;

        try {
            ServerBootstrap bootstrap = new ServerBootstrap()
                    .group(boss, worker)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast(new ProtobufLiteDecoder());
                            ch.pipeline().addLast(new ProtobufLiteEncoder());
                            ch.pipeline().addLast(new VoteServerHandler("B"));
                        }
                    });

            try {
                server = bootstrap.bind(0).sync().channel();
            } catch (Exception e) {
                Assumptions.assumeTrue(false, "Local bind not permitted in this environment: " + e.getMessage());
            }
            int port = ((InetSocketAddress) server.localAddress()).getPort();

            Peer peer = new Peer("B", new InetSocketAddress("127.0.0.1", port));
            VoteRequest req = new VoteRequest(3, "A");

            Future<List<VoteResponse>> future = client.requestVoteFromAll(List.of(peer), req);
            assertTrue(future.await(2, TimeUnit.SECONDS));
            assertTrue(future.isSuccess());

            List<VoteResponse> responses = future.getNow();
            assertEquals(1, responses.size());
            VoteResponse response = responses.get(0);
            assertEquals("B", response.getPeerId());
            assertTrue(response.isVoteGranted());
            assertEquals(3, response.getTerm());

        } finally {
            client.shutdown();
            if (server != null) {
                server.close().syncUninterruptibly();
            }
            boss.shutdownGracefully();
            worker.shutdownGracefully();
        }
    }

    @Test
    void requestVoteFromAllCompletesWhenPeerIsUnreachable() throws Exception {
        log.info("*** Testcase *** Unreachable peer vote completion: verifies vote aggregation completes with synthetic negative vote when connect fails");
        Assumptions.assumeTrue(Boolean.getBoolean("netty.it"),
                "Set -Dnetty.it=true to enable this integration test.");

        RaftClient client = new RaftClient("test", null);
        try {
            Peer peer = new Peer("B", new InetSocketAddress("127.0.0.1", 1));
            VoteRequest req = new VoteRequest(3, "A");

            Future<List<VoteResponse>> future = client.requestVoteFromAll(List.of(peer), req);
            assertTrue(future.await(2, TimeUnit.SECONDS));
            assertTrue(future.isSuccess());

            List<VoteResponse> responses = future.getNow();
            assertEquals(1, responses.size());
            assertEquals("B", responses.getFirst().getPeerId());
            assertFalse(responses.getFirst().isVoteGranted());
            assertEquals(-1, responses.getFirst().getCurrentTerm());
        } finally {
            client.shutdown();
        }
    }

    @Test
    void requestVoteFromAllTimesOutWhenPeerDoesNotRespond() throws Exception {
        log.info("*** Testcase *** Vote request timeout fallback: verifies non-responding peer yields timeout-based synthetic negative vote");
        Assumptions.assumeTrue(Boolean.getBoolean("netty.it"),
                "Set -Dnetty.it=true to enable this integration test.");

        System.setProperty("raft.vote.request.timeout.millis", "100");

        EventLoopGroup boss = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        EventLoopGroup worker = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());
        RaftClient client = new RaftClient("test", null);
        Channel server = null;

        try {
            ServerBootstrap bootstrap = new ServerBootstrap()
                    .group(boss, worker)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast(new ProtobufLiteDecoder());
                            ch.pipeline().addLast(new ProtobufLiteEncoder());
                            ch.pipeline().addLast(new SilentVoteServerHandler());
                        }
                    });
            try {
                server = bootstrap.bind(0).sync().channel();
            } catch (Exception e) {
                Assumptions.assumeTrue(false, "Local bind not permitted in this environment: " + e.getMessage());
            }
            int port = ((InetSocketAddress) server.localAddress()).getPort();

            Peer peer = new Peer("B", new InetSocketAddress("127.0.0.1", port));
            VoteRequest req = new VoteRequest(4, "A");

            Future<List<VoteResponse>> future = client.requestVoteFromAll(List.of(peer), req);
            assertTrue(future.await(3, TimeUnit.SECONDS));
            assertTrue(future.isSuccess());

            List<VoteResponse> responses = future.getNow();
            assertEquals(1, responses.size());
            assertEquals("B", responses.getFirst().getPeerId());
            assertFalse(responses.getFirst().isVoteGranted());
            assertEquals(-1, responses.getFirst().getCurrentTerm());
        } finally {
            System.clearProperty("raft.vote.request.timeout.millis");
            client.shutdown();
            if (server != null) {
                server.close().syncUninterruptibly();
            }
            boss.shutdownGracefully();
            worker.shutdownGracefully();
        }
    }

    @Test
    void sendAppendEntriesReconnectsToKnownPeerWithoutActiveChannel() throws Exception {
        log.info("*** Testcase *** AppendEntries reconnect send: verifies sendAppendEntries reconnects and delivers to a known peer without active channel");
        Assumptions.assumeTrue(Boolean.getBoolean("netty.it"),
                "Set -Dnetty.it=true to enable this integration test.");

        int port;
        try (ServerSocket reserved = new ServerSocket(0)) {
            port = reserved.getLocalPort();
        } catch (Exception e) {
            Assumptions.assumeTrue(false, "Local bind not permitted in this environment: " + e.getMessage());
            return;
        }

        RaftClient client = new RaftClient("test", null);
        EventLoopGroup boss = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        EventLoopGroup worker = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());
        Channel server = null;

        try {
            Peer peer = new Peer("B", new InetSocketAddress("127.0.0.1", port));
            VoteRequest req = new VoteRequest(5, "A");

            Future<List<VoteResponse>> firstAttempt = client.requestVoteFromAll(List.of(peer), req);
            assertTrue(firstAttempt.await(2, TimeUnit.SECONDS));
            assertTrue(firstAttempt.isSuccess());

            AtomicInteger receivedRequests = new AtomicInteger(0);
            CountDownLatch appendEntriesLatch = new CountDownLatch(1);

            ServerBootstrap bootstrap = new ServerBootstrap()
                    .group(boss, worker)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast(new ProtobufLiteDecoder());
                            ch.pipeline().addLast(new ProtobufLiteEncoder());
                            ch.pipeline().addLast(new AppendEntriesServerHandler("B", receivedRequests, appendEntriesLatch));
                        }
                    });
            try {
                server = bootstrap.bind(port).sync().channel();
            } catch (Exception e) {
                Assumptions.assumeTrue(false, "Local bind not permitted in this environment: " + e.getMessage());
            }

            AppendEntriesRequest appendEntriesRequest = new AppendEntriesRequest(5, "A", 0, 0, 0, List.of());

            client.sendAppendEntries(peer, appendEntriesRequest); // establish connection to known peer
            Thread.sleep(200);
            client.sendAppendEntries(peer, appendEntriesRequest); // send on active channel

            assertTrue(appendEntriesLatch.await(2, TimeUnit.SECONDS));
            assertTrue(receivedRequests.get() >= 1);
        } finally {
            client.shutdown();
            if (server != null) {
                server.close().syncUninterruptibly();
            }
            boss.shutdownGracefully();
            worker.shutdownGracefully();
        }
    }

    @Test
    void broadcastReconnectsToKnownPeerWithoutActiveChannel() throws Exception {
        log.info("*** Testcase *** Generic reconnect broadcast: verifies generic broadcast reconnects and delivers to a known peer without active channel");
        Assumptions.assumeTrue(Boolean.getBoolean("netty.it"),
                "Set -Dnetty.it=true to enable this integration test.");

        int port;
        try (ServerSocket reserved = new ServerSocket(0)) {
            port = reserved.getLocalPort();
        } catch (Exception e) {
            Assumptions.assumeTrue(false, "Local bind not permitted in this environment: " + e.getMessage());
            return;
        }

        RaftClient client = new RaftClient("test", null);
        EventLoopGroup boss = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        EventLoopGroup worker = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());
        Channel server = null;

        try {
            Peer peer = new Peer("B", new InetSocketAddress("127.0.0.1", port));
            VoteRequest req = new VoteRequest(6, "A");

            Future<List<VoteResponse>> firstAttempt = client.requestVoteFromAll(List.of(peer), req);
            assertTrue(firstAttempt.await(2, TimeUnit.SECONDS));
            assertTrue(firstAttempt.isSuccess());

            AtomicInteger receivedMessages = new AtomicInteger(0);
            CountDownLatch messageLatch = new CountDownLatch(1);

            ServerBootstrap bootstrap = new ServerBootstrap()
                    .group(boss, worker)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast(new ProtobufLiteDecoder());
                            ch.pipeline().addLast(new ProtobufLiteEncoder());
                            ch.pipeline().addLast(new GenericServerHandler("CustomType", receivedMessages, messageLatch));
                        }
                    });
            try {
                server = bootstrap.bind(port).sync().channel();
            } catch (Exception e) {
                Assumptions.assumeTrue(false, "Local bind not permitted in this environment: " + e.getMessage());
            }

            client.broadcast("CustomType", 6, "payload".getBytes(java.nio.charset.StandardCharsets.UTF_8)); // establish
            Thread.sleep(200);
            client.broadcast("CustomType", 6, "payload".getBytes(java.nio.charset.StandardCharsets.UTF_8)); // send

            assertTrue(messageLatch.await(2, TimeUnit.SECONDS));
            assertTrue(receivedMessages.get() >= 1);
        } finally {
            client.shutdown();
            if (server != null) {
                server.close().syncUninterruptibly();
            }
            boss.shutdownGracefully();
            worker.shutdownGracefully();
        }
    }
}
