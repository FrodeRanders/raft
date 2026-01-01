package org.gautelis.raft;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.Future;
import org.gautelis.raft.model.Message;
import org.gautelis.raft.model.Peer;
import org.gautelis.raft.model.VoteRequest;
import org.gautelis.raft.model.VoteResponse;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RaftClientIntegrationTest {
    static class VoteServerHandler extends SimpleChannelInboundHandler<JsonNode> {
        private final ObjectMapper mapper = new ObjectMapper();
        private final String serverId;

        VoteServerHandler(String serverId) {
            this.serverId = serverId;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, JsonNode jsonNode) throws Exception {
            if (!jsonNode.has("type") || !jsonNode.has("correlationId")) {
                return;
            }
            String type = jsonNode.get("type").asText();
            if (!"VoteRequest".equals(type)) {
                return;
            }

            String correlationId = jsonNode.get("correlationId").asText();
            VoteRequest req = mapper.treeToValue(jsonNode.get("payload"), VoteRequest.class);
            VoteResponse resp = new VoteResponse(req, serverId, true, req.getTerm());
            Message msg = new Message(correlationId, "VoteResponse", resp);

            String json = mapper.writeValueAsString(msg);
            ctx.writeAndFlush(Unpooled.copiedBuffer(json, StandardCharsets.UTF_8));
        }
    }

    @Test
    void requestVoteFromAllReceivesVoteResponse() throws Exception {
        Assumptions.assumeTrue(Boolean.getBoolean("netty.it"),
                "Set -Dnetty.it=true to enable this integration test.");

        EventLoopGroup boss = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        EventLoopGroup worker = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());

        RaftClient client = new RaftClient(null);
        Channel server = null;

        try {
            ServerBootstrap bootstrap = new ServerBootstrap()
                    .group(boss, worker)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast(new ByteBufToJsonDecoder());
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
}
