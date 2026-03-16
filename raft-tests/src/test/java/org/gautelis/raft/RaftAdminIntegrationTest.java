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

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.gautelis.raft.app.kv.KeyValueStateMachine;
import org.gautelis.raft.bootstrap.BasicAdapter;
import org.gautelis.raft.protocol.JoinClusterRequest;
import org.gautelis.raft.protocol.JoinClusterResponse;
import org.gautelis.raft.protocol.JoinClusterStatusRequest;
import org.gautelis.raft.protocol.JoinClusterStatusResponse;
import org.gautelis.raft.protocol.Peer;
import org.gautelis.raft.protocol.ReconfigureClusterRequest;
import org.gautelis.raft.protocol.ReconfigureClusterResponse;
import org.gautelis.raft.proto.Envelope;
import org.gautelis.raft.serialization.ProtoMapper;
import org.gautelis.raft.storage.InMemoryLogStore;
import org.gautelis.raft.storage.InMemoryPersistentStateStore;
import org.gautelis.raft.transport.netty.ProtobufLiteDecoder;
import org.gautelis.raft.transport.netty.ProtobufLiteEncoder;
import org.gautelis.raft.transport.netty.RaftClient;
import org.gautelis.raft.transport.netty.RaftServer;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RaftAdminIntegrationTest {
    private static void announce(String message) {
        System.out.println("TC: " + message);
    }

    static final class MutableTime implements RaftNode.TimeSource {
        private long now;

        MutableTime(long now) {
            this.now = now;
        }

        @Override
        public long nowMillis() {
            return now;
        }

        void set(long now) {
            this.now = now;
        }
    }

    static final class TestAdapter extends BasicAdapter {
        TestAdapter(Peer me, List<Peer> peers) {
            super(100, me, peers);
        }

        TestAdapter(Peer me, List<Peer> peers, Peer joinSeed) {
            super(100, me, peers, joinSeed);
        }

        void bind(RaftNode node) {
            this.stateMachine = node;
        }

        void triggerAutoJoinLoop() {
            startAutoJoinLoop();
        }
    }

    private static RaftNode newTransportNode(
            Peer me,
            List<Peer> peers,
            BasicAdapter adapter,
            KeyValueStateMachine stateMachine,
            RaftNode.TimeSource timeSource,
            int randomSeed
    ) {
        return TestRaftNodeBuilder.forPeer(me)
                .withTimeoutMillis(100)
                .withPeers(peers)
                .withMessageHandler(adapter == null ? null : adapter::handleMessage)
                .withStateMachine(stateMachine)
                .withClient(new RaftClient(me.getId(), adapter == null ? null : adapter::handleMessage))
                .withLogStore(new InMemoryLogStore())
                .withPersistentStateStore(new InMemoryPersistentStateStore())
                .withTimeSource(timeSource)
                .withRandom(new Random(randomSeed))
                .build();
    }

    @Test
    void typedReconfigureProtocolOverTransportDecommissionsRemovedLeader() throws Exception {
        announce("Typed reconfigure protocol transport: structured joint and finalize requests remove leader and preserve survivors");
        Assumptions.assumeTrue(Boolean.parseBoolean(System.getProperty("netty.it", "true")),
                "Set -Dnetty.it=false to disable this integration test.");

        int portA = reservePort();
        int portB = reservePort();
        int portC = reservePort();

        Peer peerA = new Peer("A", new InetSocketAddress("127.0.0.1", portA));
        Peer peerB = new Peer("B", new InetSocketAddress("127.0.0.1", portB));
        Peer peerC = new Peer("C", new InetSocketAddress("127.0.0.1", portC));

        MutableTime timeA = new MutableTime(0);
        MutableTime timeB = new MutableTime(0);
        MutableTime timeC = new MutableTime(0);

        TestAdapter adapterA = new TestAdapter(peerA, List.of(peerB, peerC));
        TestAdapter adapterB = new TestAdapter(peerB, List.of(peerA, peerC));
        TestAdapter adapterC = new TestAdapter(peerC, List.of(peerA, peerB));

        KeyValueStateMachine stateA = new KeyValueStateMachine();
        KeyValueStateMachine stateB = new KeyValueStateMachine();
        KeyValueStateMachine stateC = new KeyValueStateMachine();

        RaftNode nodeA = newTransportNode(peerA, List.of(peerB, peerC), adapterA, stateA, timeA, 1);
        RaftNode nodeB = newTransportNode(peerB, List.of(peerA, peerC), adapterB, stateB, timeB, 2);
        RaftNode nodeC = newTransportNode(peerC, List.of(peerA, peerB), adapterC, stateC, timeC, 3);

        adapterA.bind(nodeA);
        adapterB.bind(nodeB);
        adapterC.bind(nodeC);

        RaftServer serverA = new RaftServer(nodeA, portA);
        RaftServer serverB = new RaftServer(nodeB, portB);
        RaftServer serverC = new RaftServer(nodeC, portC);

        nodeA.setDecommissionListener(serverA::close);
        nodeB.setDecommissionListener(serverB::close);
        nodeC.setDecommissionListener(serverC::close);

        AtomicReference<Throwable> serverFailure = new AtomicReference<>();
        Thread threadA = startServer(serverA, "raft-typed-A", serverFailure);
        Thread threadB = startServer(serverB, "raft-typed-B", serverFailure);
        Thread threadC = startServer(serverC, "raft-typed-C", serverFailure);

        try {
            waitForServer(peerA, 5_000);
            waitForServer(peerB, 5_000);
            waitForServer(peerC, 5_000);

            nodeA.setLastHeartbeatMillisForTest(0);
            timeA.set(10_000);
            nodeA.electionTickForTest();
            assertTrue(waitUntil(nodeA::isLeader, 2_000), "node A should become leader");

            ReconfigureClusterResponse joint = sendReconfigureClusterRequest(peerA, nodeA.getTerm(),
                    new ReconfigureClusterRequest(nodeA.getTerm(), "test-client", ReconfigureClusterRequest.Action.JOINT, List.of(peerB, peerC)));
            assertTrue(joint.isSuccess());
            assertEquals("ACCEPTED", joint.getStatus());

            assertTrue(waitUntil(() -> {
                nodeA.heartbeatTickForTest();
                return nodeA.getClusterConfigurationForTest().isJointConsensus()
                        && nodeB.getClusterConfigurationForTest().isJointConsensus()
                        && nodeC.getClusterConfigurationForTest().isJointConsensus();
            }, 5_000), "joint configuration should replicate over transport");

            ReconfigureClusterResponse finalize = sendReconfigureClusterRequest(peerA, nodeA.getTerm(),
                    new ReconfigureClusterRequest(nodeA.getTerm(), "test-client", ReconfigureClusterRequest.Action.FINALIZE, List.of()));
            assertTrue(finalize.isSuccess());
            assertEquals("ACCEPTED", finalize.getStatus());

            assertTrue(waitUntil(() -> {
                nodeA.heartbeatTickForTest();
                return nodeA.isDecommissionedForTest()
                        && !nodeA.isLeader()
                        && !nodeA.getClusterConfigurationForTest().contains("A")
                        && !nodeB.getClusterConfigurationForTest().isJointConsensus()
                        && !nodeC.getClusterConfigurationForTest().isJointConsensus();
            }, 5_000), "finalized typed reconfiguration should remove the leader");

            threadA.join(5_000);
            assertFalse(threadA.isAlive(), "decommissioned leader server should stop");
            assertFalse(canConnect(portA), "decommissioned leader port should no longer accept connections");
            assertNull(serverFailure.get());
        } finally {
            serverA.close();
            serverB.close();
            serverC.close();
            threadA.join(5_000);
            threadB.join(5_000);
            threadC.join(5_000);
            nodeA.shutdown();
            nodeB.shutdown();
            nodeC.shutdown();
        }
    }

    @Test
    void typedJoinProtocolOverTransportReturnsStatusAndCanBePolled() throws Exception {
        announce("Typed join protocol transport: join request and status request complete admission over structured API");
        Assumptions.assumeTrue(Boolean.parseBoolean(System.getProperty("netty.it", "true")),
                "Set -Dnetty.it=false to disable this integration test.");

        int portA = reservePort();
        int portB = reservePort();
        int portC = reservePort();
        int portD = reservePort();

        Peer peerA = new Peer("A", new InetSocketAddress("127.0.0.1", portA));
        Peer peerB = new Peer("B", new InetSocketAddress("127.0.0.1", portB));
        Peer peerC = new Peer("C", new InetSocketAddress("127.0.0.1", portC));
        Peer peerD = new Peer("D", new InetSocketAddress("127.0.0.1", portD));

        MutableTime timeA = new MutableTime(0);
        MutableTime timeB = new MutableTime(0);
        MutableTime timeC = new MutableTime(0);
        MutableTime timeD = new MutableTime(0);

        TestAdapter adapterA = new TestAdapter(peerA, List.of(peerB, peerC));
        TestAdapter adapterB = new TestAdapter(peerB, List.of(peerA, peerC));
        TestAdapter adapterC = new TestAdapter(peerC, List.of(peerA, peerB));
        TestAdapter adapterD = new TestAdapter(peerD, List.of(peerA), peerA);

        KeyValueStateMachine stateA = new KeyValueStateMachine();
        KeyValueStateMachine stateB = new KeyValueStateMachine();
        KeyValueStateMachine stateC = new KeyValueStateMachine();
        KeyValueStateMachine stateD = new KeyValueStateMachine();

        RaftNode nodeA = newTransportNode(peerA, List.of(peerB, peerC), adapterA, stateA, timeA, 1);
        RaftNode nodeB = newTransportNode(peerB, List.of(peerA, peerC), adapterB, stateB, timeB, 2);
        RaftNode nodeC = newTransportNode(peerC, List.of(peerA, peerB), adapterC, stateC, timeC, 3);
        RaftNode nodeD = newTransportNode(peerD, List.of(peerA), adapterD, stateD, timeD, 4);
        nodeD.enableJoiningMode();

        adapterA.bind(nodeA);
        adapterB.bind(nodeB);
        adapterC.bind(nodeC);
        adapterD.bind(nodeD);

        RaftServer serverA = new RaftServer(nodeA, portA);
        RaftServer serverB = new RaftServer(nodeB, portB);
        RaftServer serverC = new RaftServer(nodeC, portC);
        RaftServer serverD = new RaftServer(nodeD, portD);

        nodeA.setDecommissionListener(serverA::close);
        nodeB.setDecommissionListener(serverB::close);
        nodeC.setDecommissionListener(serverC::close);
        nodeD.setDecommissionListener(serverD::close);

        AtomicReference<Throwable> serverFailure = new AtomicReference<>();
        Thread threadA = startServer(serverA, "raft-join-A", serverFailure);
        Thread threadB = startServer(serverB, "raft-join-B", serverFailure);
        Thread threadC = startServer(serverC, "raft-join-C", serverFailure);
        Thread threadD = startServer(serverD, "raft-join-D", serverFailure);

        try {
            waitForServer(peerA, 5_000);
            waitForServer(peerB, 5_000);
            waitForServer(peerC, 5_000);
            waitForServer(peerD, 5_000);

            nodeA.setLastHeartbeatMillisForTest(0);
            timeA.set(10_000);
            nodeA.electionTickForTest();
            assertTrue(waitUntil(nodeA::isLeader, 2_000), "node A should become leader");

            assertTrue(waitUntil(() -> {
                        nodeA.heartbeatTickForTest();
                        return nodeB.getKnownLeaderPeer() != null && "A".equals(nodeB.getKnownLeaderPeer().getId());
                    }, 5_000),
                    "node B should learn leader A");

            JoinClusterResponse joinResponse = sendJoinClusterRequest(peerB, nodeB.getTerm(), peerD);
            assertTrue(joinResponse.isSuccess());
            assertEquals("FORWARDED", joinResponse.getStatus());
            assertEquals("A", joinResponse.getLeaderId());

            assertTrue(waitUntil(() -> {
                nodeA.heartbeatTickForTest();
                JoinClusterStatusResponse status = sendJoinClusterStatusUnchecked(peerA, nodeA.getTerm(), "D");
                return status != null && "COMPLETED".equals(status.getStatus());
            }, 5_000), "join status should eventually become completed");

            assertTrue(nodeD.getClusterConfigurationForTest().contains("D"));
            assertFalse(nodeD.isJoining());
            assertNull(serverFailure.get());
        } finally {
            serverA.close();
            serverB.close();
            serverC.close();
            serverD.close();
            threadA.join(5_000);
            threadB.join(5_000);
            threadC.join(5_000);
            threadD.join(5_000);
            nodeA.shutdown();
            nodeB.shutdown();
            nodeC.shutdown();
            nodeD.shutdown();
        }
    }

    @Test
    void startupAutoJoinUsesTypedJoinProtocol() throws Exception {
        announce("Typed auto-join startup: join-mode node uses typed join protocol to admit itself through a single seed");
        Assumptions.assumeTrue(Boolean.parseBoolean(System.getProperty("netty.it", "true")),
                "Set -Dnetty.it=false to disable this integration test.");

        int portA = reservePort();
        int portB = reservePort();
        int portC = reservePort();
        int portD = reservePort();

        Peer peerA = new Peer("A", new InetSocketAddress("127.0.0.1", portA));
        Peer peerB = new Peer("B", new InetSocketAddress("127.0.0.1", portB));
        Peer peerC = new Peer("C", new InetSocketAddress("127.0.0.1", portC));
        Peer peerD = new Peer("D", new InetSocketAddress("127.0.0.1", portD));

        MutableTime timeA = new MutableTime(0);
        MutableTime timeB = new MutableTime(0);
        MutableTime timeC = new MutableTime(0);
        MutableTime timeD = new MutableTime(0);

        TestAdapter adapterA = new TestAdapter(peerA, List.of(peerB, peerC));
        TestAdapter adapterB = new TestAdapter(peerB, List.of(peerA, peerC));
        TestAdapter adapterC = new TestAdapter(peerC, List.of(peerA, peerB));
        TestAdapter adapterD = new TestAdapter(peerD, List.of(peerA), peerA);

        KeyValueStateMachine stateA = new KeyValueStateMachine();
        KeyValueStateMachine stateB = new KeyValueStateMachine();
        KeyValueStateMachine stateC = new KeyValueStateMachine();
        KeyValueStateMachine stateD = new KeyValueStateMachine();

        RaftNode nodeA = newTransportNode(peerA, List.of(peerB, peerC), adapterA, stateA, timeA, 1);
        RaftNode nodeB = newTransportNode(peerB, List.of(peerA, peerC), adapterB, stateB, timeB, 2);
        RaftNode nodeC = newTransportNode(peerC, List.of(peerA, peerB), adapterC, stateC, timeC, 3);
        RaftNode nodeD = newTransportNode(peerD, List.of(peerA), adapterD, stateD, timeD, 4);
        nodeD.enableJoiningMode();

        adapterA.bind(nodeA);
        adapterB.bind(nodeB);
        adapterC.bind(nodeC);
        adapterD.bind(nodeD);

        RaftServer serverA = new RaftServer(nodeA, portA);
        RaftServer serverB = new RaftServer(nodeB, portB);
        RaftServer serverC = new RaftServer(nodeC, portC);
        RaftServer serverD = new RaftServer(nodeD, portD);

        nodeA.setDecommissionListener(serverA::close);
        nodeB.setDecommissionListener(serverB::close);
        nodeC.setDecommissionListener(serverC::close);
        nodeD.setDecommissionListener(serverD::close);

        AtomicReference<Throwable> serverFailure = new AtomicReference<>();
        Thread threadA = startServer(serverA, "raft-autojoin-A", serverFailure);
        Thread threadB = startServer(serverB, "raft-autojoin-B", serverFailure);
        Thread threadC = startServer(serverC, "raft-autojoin-C", serverFailure);
        Thread threadD = startServer(serverD, "raft-autojoin-D", serverFailure);

        try {
            waitForServer(peerA, 5_000);
            waitForServer(peerB, 5_000);
            waitForServer(peerC, 5_000);
            waitForServer(peerD, 5_000);

            nodeA.setLastHeartbeatMillisForTest(0);
            timeA.set(10_000);
            nodeA.electionTickForTest();
            assertTrue(waitUntil(nodeA::isLeader, 2_000), "node A should become leader");

            adapterD.triggerAutoJoinLoop();

            assertTrue(waitUntil(() -> {
                nodeA.heartbeatTickForTest();
                return nodeD.getClusterConfigurationForTest().contains("D") && !nodeD.isJoining();
            }, 5_000), "auto-join loop should admit D through the typed join protocol");

            assertTrue(nodeA.getClusterConfigurationForTest().contains("D"));
            assertTrue(nodeB.getClusterConfigurationForTest().contains("D"));
            assertTrue(nodeC.getClusterConfigurationForTest().contains("D"));
            assertNull(serverFailure.get());
        } finally {
            serverA.close();
            serverB.close();
            serverC.close();
            serverD.close();
            threadA.join(5_000);
            threadB.join(5_000);
            threadC.join(5_000);
            threadD.join(5_000);
            nodeA.shutdown();
            nodeB.shutdown();
            nodeC.shutdown();
            nodeD.shutdown();
        }
    }

    private static Thread startServer(RaftServer server, String name, AtomicReference<Throwable> serverFailure) {
        Thread thread = new Thread(() -> {
            try {
                server.start();
            } catch (Throwable t) {
                serverFailure.compareAndSet(null, t);
            }
        }, name);
        thread.start();
        return thread;
    }

    private static int reservePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (Exception e) {
            Assumptions.assumeTrue(false, "Local bind not permitted in this environment: " + e.getMessage());
            throw e;
        }
    }

    private static void waitForServer(Peer peer, long timeoutMillis) throws Exception {
        assertTrue(waitUntil(() -> canConnect(peer.getAddress().getPort()), timeoutMillis),
                () -> "server did not start on port " + peer.getAddress().getPort());
    }

    private static boolean waitUntil(Check condition, long timeoutMillis) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            if (condition.evaluate()) {
                return true;
            }
            Thread.sleep(25);
        }
        return condition.evaluate();
    }

    private static JoinClusterResponse sendJoinClusterRequest(Peer peer, long term, Peer joiningPeer) throws Exception {
        EventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        try {
            Bootstrap bootstrap = new Bootstrap()
                    .group(group)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast(new ProtobufLiteDecoder());
                            ch.pipeline().addLast(new ProtobufLiteEncoder());
                        }
                    });
            Channel channel = bootstrap.connect(peer.getAddress()).sync().channel();
            CompletableFuture<JoinClusterResponse> responseFuture = new CompletableFuture<>();
            channel.pipeline().addLast("join-response", new SimpleChannelInboundHandler<Envelope>() {
                @Override
                protected void channelRead0(io.netty.channel.ChannelHandlerContext ctx, Envelope envelope) {
                    if (!"JoinClusterResponse".equals(envelope.getType())) {
                        return;
                    }
                    var parsed = ProtoMapper.parseJoinClusterResponse(envelope.getPayload().toByteArray());
                    if (parsed.isPresent()) {
                        responseFuture.complete(ProtoMapper.fromProto(parsed.get()));
                    } else {
                        responseFuture.completeExceptionally(new IllegalStateException("Invalid JoinClusterResponse payload"));
                    }
                }
            });
            byte[] payload = ProtoMapper.toProto(new JoinClusterRequest(term, "test-client", joiningPeer))
                    .toByteString()
                    .toByteArray();
            channel.writeAndFlush(ProtoMapper.wrap(UUID.randomUUID().toString(), "JoinClusterRequest", payload)).sync();
            JoinClusterResponse response = responseFuture.get();
            channel.close().sync();
            return response;
        } catch (Exception e) {
            Assumptions.assumeTrue(false, "Local network connect not permitted in this environment: " + e.getMessage());
            throw e;
        } finally {
            group.shutdownGracefully().syncUninterruptibly();
        }
    }

    private static JoinClusterStatusResponse sendJoinClusterStatus(Peer peer, long term, String targetPeerId) throws Exception {
        EventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        try {
            Bootstrap bootstrap = new Bootstrap()
                    .group(group)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast(new ProtobufLiteDecoder());
                            ch.pipeline().addLast(new ProtobufLiteEncoder());
                        }
                    });
            Channel channel = bootstrap.connect(peer.getAddress()).sync().channel();
            CompletableFuture<JoinClusterStatusResponse> responseFuture = new CompletableFuture<>();
            channel.pipeline().addLast("join-status-response", new SimpleChannelInboundHandler<Envelope>() {
                @Override
                protected void channelRead0(io.netty.channel.ChannelHandlerContext ctx, Envelope envelope) {
                    if (!"JoinClusterStatusResponse".equals(envelope.getType())) {
                        return;
                    }
                    var parsed = ProtoMapper.parseJoinClusterStatusResponse(envelope.getPayload().toByteArray());
                    if (parsed.isPresent()) {
                        responseFuture.complete(ProtoMapper.fromProto(parsed.get()));
                    } else {
                        responseFuture.completeExceptionally(new IllegalStateException("Invalid JoinClusterStatusResponse payload"));
                    }
                }
            });
            byte[] payload = ProtoMapper.toProto(new JoinClusterStatusRequest(term, "test-client", targetPeerId))
                    .toByteString()
                    .toByteArray();
            channel.writeAndFlush(ProtoMapper.wrap(UUID.randomUUID().toString(), "JoinClusterStatusRequest", payload)).sync();
            JoinClusterStatusResponse response = responseFuture.get();
            channel.close().sync();
            return response;
        } catch (Exception e) {
            Assumptions.assumeTrue(false, "Local network connect not permitted in this environment: " + e.getMessage());
            throw e;
        } finally {
            group.shutdownGracefully().syncUninterruptibly();
        }
    }

    private static JoinClusterStatusResponse sendJoinClusterStatusUnchecked(Peer peer, long term, String targetPeerId) {
        try {
            return sendJoinClusterStatus(peer, term, targetPeerId);
        } catch (Exception ignored) {
            return null;
        }
    }

    private static ReconfigureClusterResponse sendReconfigureClusterRequest(Peer peer, long term, ReconfigureClusterRequest request) throws Exception {
        EventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        try {
            Bootstrap bootstrap = new Bootstrap()
                    .group(group)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast(new ProtobufLiteDecoder());
                            ch.pipeline().addLast(new ProtobufLiteEncoder());
                        }
                    });
            Channel channel = bootstrap.connect(peer.getAddress()).sync().channel();
            CompletableFuture<ReconfigureClusterResponse> responseFuture = new CompletableFuture<>();
            channel.pipeline().addLast("reconfigure-response", new SimpleChannelInboundHandler<Envelope>() {
                @Override
                protected void channelRead0(io.netty.channel.ChannelHandlerContext ctx, Envelope envelope) {
                    if (!"ReconfigureClusterResponse".equals(envelope.getType())) {
                        return;
                    }
                    var parsed = ProtoMapper.parseReconfigureClusterResponse(envelope.getPayload().toByteArray());
                    if (parsed.isPresent()) {
                        responseFuture.complete(ProtoMapper.fromProto(parsed.get()));
                    } else {
                        responseFuture.completeExceptionally(new IllegalStateException("Invalid ReconfigureClusterResponse payload"));
                    }
                }
            });
            byte[] payload = ProtoMapper.toProto(request).toByteString().toByteArray();
            channel.writeAndFlush(ProtoMapper.wrap(UUID.randomUUID().toString(), "ReconfigureClusterRequest", payload)).sync();
            ReconfigureClusterResponse response = responseFuture.get();
            channel.close().sync();
            return response;
        } catch (Exception e) {
            Assumptions.assumeTrue(false, "Local network connect not permitted in this environment: " + e.getMessage());
            throw e;
        } finally {
            group.shutdownGracefully().syncUninterruptibly();
        }
    }

    private static boolean canConnect(int port) {
        EventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        try {
            Bootstrap bootstrap = new Bootstrap()
                    .group(group)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 200)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast(new ProtobufLiteDecoder());
                            ch.pipeline().addLast(new ProtobufLiteEncoder());
                        }
                    });
            Channel channel = bootstrap.connect("127.0.0.1", port).sync().channel();
            channel.close().sync();
            return true;
        } catch (Exception ignored) {
            return false;
        } finally {
            group.shutdownGracefully().syncUninterruptibly();
        }
    }

    @FunctionalInterface
    private interface Check {
        boolean evaluate() throws Exception;
    }
}
