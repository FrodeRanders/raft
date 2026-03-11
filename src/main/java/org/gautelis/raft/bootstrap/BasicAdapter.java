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
package org.gautelis.raft.bootstrap;

import io.netty.channel.ChannelHandlerContext;
import org.gautelis.raft.MessageHandler;
import org.gautelis.raft.RaftNode;
import org.gautelis.raft.RaftServer;
import org.gautelis.raft.protocol.AdminCommand;
import org.gautelis.raft.protocol.ClusterMessage;
import org.gautelis.raft.protocol.Peer;
import org.gautelis.raft.serialization.ProtoMapper;
import org.gautelis.raft.statemachine.KeyValueStateMachine;
import org.gautelis.raft.statemachine.SnapshotStateMachine;
import org.gautelis.raft.storage.FileLogStore;
import org.gautelis.raft.storage.FilePersistentStateStore;
import org.gautelis.raft.storage.InMemoryLogStore;
import org.gautelis.raft.storage.InMemoryPersistentStateStore;
import org.gautelis.raft.storage.LogStore;
import org.gautelis.raft.storage.PersistentStateStore;
import org.gautelis.raft.transport.netty.RaftClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class BasicAdapter {
    protected static final Logger log = LoggerFactory.getLogger(BasicAdapter.class);

    protected final long timeoutMillis;
    protected final Peer me;
    protected final List<Peer> peers;

    protected RaftNode stateMachine;

    public BasicAdapter(long timeoutMillis, Peer me, List<Peer> peers) {
        this.timeoutMillis = timeoutMillis;
        this.me = me;
        this.peers = peers;
    }

    public void start() {
        // Adapter bootstraps a runnable node with either in-memory or file-backed durability.
        String dataDir = System.getProperty("raft.data.dir");
        LogStore logStore;
        PersistentStateStore persistentStateStore;
        if (dataDir == null || dataDir.isBlank()) {
            logStore = new InMemoryLogStore();
            persistentStateStore = new InMemoryPersistentStateStore();
        } else {
            Path root = Path.of(dataDir);
            logStore = new FileLogStore(root.resolve(me.getId() + ".log"));
            persistentStateStore = new FilePersistentStateStore(root.resolve(me.getId() + ".state"));
        }

        MessageHandler messageHandler = this::handleMessage;
        SnapshotStateMachine snapshotStateMachine = new KeyValueStateMachine();

        stateMachine = new RaftNode(
                me, peers, timeoutMillis, messageHandler, snapshotStateMachine, new RaftClient(me.getId(), messageHandler), logStore, persistentStateStore
        );

        RaftServer server = new RaftServer(stateMachine, me.getAddress().getPort());
        stateMachine.setDecommissionListener(() -> {
            log.info("Node {} is decommissioned; closing server", me.getId());
            server.close();
        });

        try {
            server.start();
        }
        catch (InterruptedException ie) {
            log.info("Interrupted!", ie);
        }
        finally {
            stateMachine.shutdown();
        }
    }

    public void handleMessage(String correlationId, String type, byte[] payload, ChannelHandlerContext ctx) {
        if ("ClusterMessage".equals(type)) {
            // Client commands are accepted only on leader; RaftNode handles replication/commit.
            var parsed = ProtoMapper.parseClusterMessage(payload);
            if (parsed.isEmpty()) {
                log.warn("Invalid ClusterMessage payload");
                return;
            }
            ClusterMessage clusterMessage = ProtoMapper.fromProto(parsed.get());
            String command = clusterMessage.getMessage();
            if (!isValidClusterCommand(command)) {
                log.warn("Rejected invalid cluster command: '{}'", command);
                return;
            }

            if (stateMachine == null) {
                log.warn("Ignoring command while state machine is unavailable");
                return;
            }
            if (!submitClusterCommand(command)) {
                log.info("Rejected command at {}: node is not leader or command could not be applied", me.getId());
                return;
            }
            log.info("Accepted cluster command: {}", command);
            return;
        }
        if ("AdminCommand".equals(type)) {
            var parsed = ProtoMapper.parseAdminCommand(payload);
            if (parsed.isEmpty()) {
                log.warn("Invalid AdminCommand payload");
                return;
            }
            if (stateMachine == null) {
                log.warn("Ignoring admin command while state machine is unavailable");
                return;
            }
            AdminCommand adminCommand = ProtoMapper.fromProto(parsed.get());
            String command = adminCommand.getCommand();
            if (!isValidAdminCommand(command)) {
                log.warn("Rejected invalid admin command: '{}'", command);
                return;
            }
            if (!submitAdminCommand(command)) {
                log.info("Rejected admin command at {}: node is not leader, decommissioned, or command could not be applied", me.getId());
                return;
            }
            log.info("Accepted admin command: {}", command);
            return;
        }

        log.debug("Received '{}' message ({} bytes)", type, payload == null ? 0 : payload.length);
    }

    static boolean isValidClusterCommand(String command) {
        // Keep accepted command grammar intentionally small for predictable demos/tests.
        if (command == null) {
            return false;
        }
        String trimmed = command.trim();
        if (trimmed.isEmpty()) {
            return false;
        }
        String[] parts = trimmed.split("\\s+", 3);
        String op = parts[0].toLowerCase(java.util.Locale.ROOT);
        return switch (op) {
            case "set", "put" -> parts.length >= 3 && !parts[1].isBlank();
            case "del", "delete" -> parts.length >= 2 && !parts[1].isBlank();
            case "clear" -> parts.length == 1;
            default -> false;
        };
    }

    private boolean submitClusterCommand(String command) {
        return stateMachine.submitCommand(command);
    }

    static boolean isValidAdminCommand(String command) {
        String[] parts = command.trim().split("\\s+", 3);
        if (parts.length < 2) {
            return false;
        }
        if (!"config".equals(parts[0].toLowerCase(Locale.ROOT))) {
            return false;
        }
        String action = parts[1].toLowerCase(Locale.ROOT);
        return switch (action) {
            case "joint" -> parts.length == 3 && !parts[2].isBlank();
            case "finalize" -> parts.length == 2;
            default -> false;
        };
    }

    private boolean submitAdminCommand(String command) {
        String[] parts = command.trim().split("\\s+", 3);
        String action = parts[1].toLowerCase(Locale.ROOT);
        try {
            return switch (action) {
                // "joint" accepts either known ids or explicit id@host:port peer specs for new members.
                case "joint" -> parts.length == 3 && stateMachine.submitJointConfigurationChange(parsePeers(parts[2]));
                case "finalize" -> parts.length == 2 && stateMachine.submitFinalizeConfigurationChange();
                default -> false;
            };
        } catch (IllegalArgumentException e) {
            log.warn("Rejected invalid admin command '{}': {}", command, e.getMessage());
            return false;
        }
    }

    private List<Peer> parsePeers(String spec) {
        List<Peer> result = new ArrayList<>();
        for (String token : spec.split(",")) {
            String entry = token.trim();
            if (entry.isEmpty()) {
                continue;
            }
            result.add(resolvePeer(entry));
        }
        return result;
    }

    private Peer resolvePeer(String spec) {
        int at = spec.indexOf('@');
        if (at < 0) {
            // Bare ids are only valid for peers already known to the local adapter/node.
            if (me.getId().equals(spec)) {
                return me;
            }
            for (Peer peer : peers) {
                if (peer.getId().equals(spec)) {
                    return peer;
                }
            }
            if (stateMachine != null) {
                Peer peer = stateMachine.getPeerById(spec);
                if (peer != null) {
                    return peer;
                }
            }
            throw new IllegalArgumentException("Unknown peer id '" + spec + "'; use id@host:port for new members");
        }

        String id = spec.substring(0, at).trim();
        String addressSpec = spec.substring(at + 1).trim();
        int colon = addressSpec.lastIndexOf(':');
        if (id.isBlank() || colon <= 0 || colon == addressSpec.length() - 1) {
            throw new IllegalArgumentException("Invalid peer specification '" + spec + "'");
        }
        // Allow introducing a brand-new member without first baking it into static startup config.
        String host = addressSpec.substring(0, colon).trim();
        int port = Integer.parseInt(addressSpec.substring(colon + 1).trim());
        return new Peer(id, new InetSocketAddress(host, port));
    }
}
