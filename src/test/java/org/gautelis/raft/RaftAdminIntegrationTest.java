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
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.gautelis.raft.bootstrap.BasicAdapter;
import org.gautelis.raft.protocol.AdminCommand;
import org.gautelis.raft.protocol.Peer;
import org.gautelis.raft.serialization.ProtoMapper;
import org.gautelis.raft.statemachine.KeyValueStateMachine;
import org.gautelis.raft.storage.InMemoryLogStore;
import org.gautelis.raft.storage.InMemoryPersistentStateStore;
import org.gautelis.raft.transport.netty.ProtobufLiteDecoder;
import org.gautelis.raft.transport.netty.ProtobufLiteEncoder;
import org.gautelis.raft.transport.netty.RaftClient;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RaftAdminIntegrationTest {
    private static void announce(String message) {
        System.out.println("*** Testcase *** " + message);
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

        void bind(RaftNode node) {
            this.stateMachine = node;
        }
    }

    @Test
    void adminCommandReconfigurationOverTransportDecommissionsRemovedLeader() throws Exception {
        announce("AdminCommand transport reconfiguration: decommissions removed leader and preserves surviving cluster");
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

        RaftNode nodeA = new RaftNode(peerA, List.of(peerB, peerC), 100, adapterA::handleMessage,
                stateA, new RaftClient("A", adapterA::handleMessage), new InMemoryLogStore(),
                new InMemoryPersistentStateStore(), timeA, new Random(1));
        RaftNode nodeB = new RaftNode(peerB, List.of(peerA, peerC), 100, adapterB::handleMessage,
                stateB, new RaftClient("B", adapterB::handleMessage), new InMemoryLogStore(),
                new InMemoryPersistentStateStore(), timeB, new Random(2));
        RaftNode nodeC = new RaftNode(peerC, List.of(peerA, peerB), 100, adapterC::handleMessage,
                stateC, new RaftClient("C", adapterC::handleMessage), new InMemoryLogStore(),
                new InMemoryPersistentStateStore(), timeC, new Random(3));

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
        Thread threadA = startServer(serverA, "raft-it-A", serverFailure);
        Thread threadB = startServer(serverB, "raft-it-B", serverFailure);
        Thread threadC = startServer(serverC, "raft-it-C", serverFailure);

        try {
            waitForServer(peerA, 5_000);
            waitForServer(peerB, 5_000);
            waitForServer(peerC, 5_000);

            nodeA.setLastHeartbeatMillisForTest(0);
            timeA.set(10_000);
            nodeA.electionTickForTest();
            assertTrue(waitUntil(nodeA::isLeader, 2_000), "node A should become leader");

            sendAdminCommand(peerA, nodeA.getTerm(), "config joint B,C");
            assertTrue(waitUntil(() -> {
                nodeA.heartbeatTickForTest();
                return nodeA.getClusterConfigurationForTest().isJointConsensus()
                        && nodeB.getClusterConfigurationForTest().isJointConsensus()
                        && nodeC.getClusterConfigurationForTest().isJointConsensus();
            }, 5_000), "joint configuration should replicate over transport");

            sendAdminCommand(peerA, nodeA.getTerm(), "config finalize");
            AtomicReference<String> finalizeStatus = new AtomicReference<>("");
            assertTrue(waitUntil(() -> {
                nodeA.heartbeatTickForTest();
                boolean settled = nodeA.isDecommissionedForTest()
                        && !nodeA.isLeader()
                        && !nodeA.getClusterConfigurationForTest().contains("A")
                        && !nodeB.getClusterConfigurationForTest().isJointConsensus()
                        && !nodeC.getClusterConfigurationForTest().isJointConsensus();
                finalizeStatus.set("A{leader=" + nodeA.isLeader()
                        + ",decommissioned=" + nodeA.isDecommissionedForTest()
                        + ",config=" + nodeA.getClusterConfigurationForTest()
                        + "} B{config=" + nodeB.getClusterConfigurationForTest()
                        + "} C{config=" + nodeC.getClusterConfigurationForTest() + "}");
                return settled;
            }, 5_000), () -> "finalized configuration should remove the leader and settle followers; " + finalizeStatus.get());

            threadA.join(5_000);
            assertFalse(threadA.isAlive(), "decommissioned leader server should stop");
            assertFalse(canConnect(portA), "decommissioned leader port should no longer accept connections");
            assertNull(serverFailure.get());

            nodeB.setLastHeartbeatMillisForTest(0);
            timeB.set(20_000);
            nodeB.electionTickForTest();
            assertTrue(waitUntil(nodeB::isLeader, 5_000), "surviving cluster should elect a new leader");

            assertTrue(nodeB.submitCommand("set survivor 1"));
            assertTrue(waitUntil(() -> "1".equals(stateB.get("survivor")) && "1".equals(stateC.get("survivor")), 5_000),
                    "surviving leader should still replicate client commands");
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

    private static void sendAdminCommand(Peer peer, long term, String command) throws Exception {
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
            byte[] payload = ProtoMapper.toProto(new AdminCommand(term, "test-client", command))
                    .toByteString()
                    .toByteArray();
            channel.writeAndFlush(ProtoMapper.wrap(UUID.randomUUID().toString(), "AdminCommand", payload)).sync();
            channel.close().sync();
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
