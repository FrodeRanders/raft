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

import org.gautelis.raft.ClusterConfiguration;
import org.gautelis.raft.MessageHandler;
import org.gautelis.raft.RaftNode;
import org.gautelis.raft.protocol.ClientCommandRequest;
import org.gautelis.raft.protocol.ClientCommandResponse;
import org.gautelis.raft.protocol.ClientQueryRequest;
import org.gautelis.raft.protocol.ClientQueryResponse;
import org.gautelis.raft.protocol.ClusterMemberSummary;
import org.gautelis.raft.protocol.ClusterSummaryRequest;
import org.gautelis.raft.protocol.ClusterSummaryResponse;
import org.gautelis.raft.protocol.JoinClusterRequest;
import org.gautelis.raft.protocol.JoinClusterResponse;
import org.gautelis.raft.protocol.JoinClusterStatusRequest;
import org.gautelis.raft.protocol.JoinClusterStatusResponse;
import org.gautelis.raft.protocol.Peer;
import org.gautelis.raft.protocol.ReconfigurationStatusRequest;
import org.gautelis.raft.protocol.ReconfigurationStatusResponse;
import org.gautelis.raft.protocol.ReconfigureClusterRequest;
import org.gautelis.raft.protocol.ReconfigureClusterResponse;
import org.gautelis.raft.protocol.StateMachineCommand;
import org.gautelis.raft.protocol.TelemetryRequest;
import org.gautelis.raft.protocol.TelemetryResponse;
import org.gautelis.raft.serialization.ProtoMapper;
import org.gautelis.raft.statemachine.NoopSnapshotStateMachine;
import org.gautelis.raft.statemachine.SnapshotStateMachine;
import org.gautelis.raft.storage.FileLogStore;
import org.gautelis.raft.storage.FilePersistentStateStore;
import org.gautelis.raft.storage.InMemoryLogStore;
import org.gautelis.raft.storage.InMemoryPersistentStateStore;
import org.gautelis.raft.storage.LogStore;
import org.gautelis.raft.storage.PersistentStateStore;
import org.gautelis.raft.transport.RaftTransportClient;
import org.gautelis.raft.transport.RaftTransportServer;
import org.gautelis.raft.transport.MessageResponder;
import org.gautelis.raft.telemetry.TelemetryExporter;
import org.gautelis.raft.telemetry.TelemetryExporters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Wires protocol messages, storage, telemetry, and the state machine into a runnable Raft node.
 */
public class BasicAdapter {
    protected static final Logger log = LoggerFactory.getLogger(BasicAdapter.class);

    protected final long timeoutMillis;
    protected final Peer me;
    protected final List<Peer> peers;
    protected final Peer joinSeed;
    protected final RuntimeConfiguration runtimeConfiguration;
    protected final org.gautelis.raft.transport.RaftTransportFactory transportFactory;
    protected final TelemetryExporter telemetryExporter = TelemetryExporters.createFromProperties();
    private final Map<String, Deque<Long>> telemetryRequestHistory = new HashMap<>();

    protected RaftNode stateMachine;

    public BasicAdapter(long timeoutMillis, Peer me, List<Peer> peers) {
        this(AdapterSpec.forPeer(me)
                .withTimeoutMillis(timeoutMillis)
                .withPeers(peers)
                .build());
    }

    public BasicAdapter(long timeoutMillis, Peer me, List<Peer> peers, Peer joinSeed) {
        this(AdapterSpec.forPeer(me)
                .withTimeoutMillis(timeoutMillis)
                .withPeers(peers)
                .withJoinSeed(joinSeed)
                .build());
    }

    public BasicAdapter(long timeoutMillis, Peer me, List<Peer> peers, Peer joinSeed, RuntimeConfiguration runtimeConfiguration) {
        this(AdapterSpec.forPeer(me)
                .withTimeoutMillis(timeoutMillis)
                .withPeers(peers)
                .withJoinSeed(joinSeed)
                .withRuntimeConfiguration(runtimeConfiguration)
                .build());
    }

    public BasicAdapter(AdapterSpec spec) {
        this.timeoutMillis = spec.timeoutMillis();
        this.me = spec.me();
        this.peers = spec.peers();
        this.joinSeed = spec.joinSeed();
        this.runtimeConfiguration = spec.runtimeConfiguration();
        this.transportFactory = spec.transportFactory();
    }

    public void start() {
        if (transportFactory == null) {
            throw new IllegalStateException("No transport factory configured for adapter " + me.getId());
        }
        // Adapter bootstraps a runnable node with either in-memory or file-backed durability.
        String dataDir = runtimeConfiguration.dataDir();
        LogStore logStore;
        PersistentStateStore persistentStateStore;
        if (!runtimeConfiguration.hasDataDir()) {
            logStore = new InMemoryLogStore();
            persistentStateStore = new InMemoryPersistentStateStore();
        } else {
            Path root = Path.of(dataDir);
            logStore = new FileLogStore(root.resolve(me.getId() + ".log"));
            persistentStateStore = new FilePersistentStateStore(root.resolve(me.getId() + ".state"));
        }

        MessageHandler messageHandler = this::handleMessage;
        SnapshotStateMachine snapshotStateMachine = createSnapshotStateMachine();

        RaftTransportClient raftClient = transportFactory.createClient(me.getId(), messageHandler);
        stateMachine = new RaftNode(
                me, peers, timeoutMillis, messageHandler, snapshotStateMachine, raftClient, logStore, persistentStateStore
        );
        if (joinSeed != null) {
            // Join mode keeps the new node passive until the existing cluster admits it.
            stateMachine.enableJoiningMode();
        }
        publishTelemetrySnapshot();

        RaftTransportServer server = transportFactory.createServer(stateMachine, me.getAddress().getPort());
        stateMachine.setDecommissionListener(() -> {
            log.info("Node {} is decommissioned; closing server", me.getId());
            server.close();
        });
        startAutoJoinLoop();
        startTelemetryExportLoop();

        try {
            server.start();
        }
        catch (InterruptedException ie) {
            log.info("Interrupted!", ie);
        }
        finally {
            telemetryExporter.close();
            stateMachine.shutdown();
        }
    }

    protected SnapshotStateMachine createSnapshotStateMachine() {
        return new NoopSnapshotStateMachine();
    }

    private void publishTelemetrySnapshot() {
        if (stateMachine == null || !telemetryExporter.isEnabled()) {
            return;
        }
        telemetryExporter.publish(stateMachine.telemetrySnapshot(), stateMachine.getRaftClient().snapshotResponseTimeStats());
    }

    private void startTelemetryExportLoop() {
        if (!telemetryExporter.isEnabled() || runtimeConfiguration.telemetryExportIntervalSeconds() <= 0 || stateMachine == null) {
            return;
        }
        Thread exportThread = new Thread(() -> {
            while (stateMachine != null) {
                try {
                    Thread.sleep(runtimeConfiguration.telemetryExportIntervalSeconds() * 1_000L);
                    publishTelemetrySnapshot();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                } catch (RuntimeException e) {
                    log.debug("Telemetry export failed for {}", me.getId(), e);
                }
            }
        }, "raft-telemetry-export-" + me.getId());
        exportThread.setDaemon(true);
        exportThread.start();
    }

    protected void startAutoJoinLoop() {
        if (joinSeed == null || stateMachine == null) {
            return;
        }
        Thread joinThread = new Thread(() -> {
            while (stateMachine != null && stateMachine.isJoining()) {
                try {
                    stateMachine.getRaftClient().sendJoinClusterRequest(
                            joinSeed,
                            new JoinClusterRequest(stateMachine.getTerm(), me.getId(), me, outboundRequestAuthScheme(), outboundRequestAuthToken())
                    );
                    Thread.sleep(500L);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                } catch (RuntimeException e) {
                    log.debug("Join retry for {} via {} failed", me.getId(), joinSeed.getId(), e);
                }
            }
        }, "raft-join-" + me.getId());
        joinThread.setDaemon(true);
        joinThread.start();
    }

    public void handleMessage(String correlationId, String type, byte[] payload, MessageResponder responder) {
        if ("ClientCommandRequest".equals(type)) {
            // Ordinary client writes use a typed RPC now so callers get explicit redirect/reject status.
            var parsed = ProtoMapper.parseClientCommandRequest(payload);
            if (parsed.isEmpty()) {
                writeClientCommandResponse(responder, correlationId, new ClientCommandResponse(0L, me.getId(), false, "INVALID", "Invalid ClientCommandRequest payload", "", "", 0));
                return;
            }
            ClientCommandResponse response = handleClientCommandRequest(ProtoMapper.fromProto(parsed.get()));
            writeClientCommandResponse(responder, correlationId, response);
            return;
        }
        if ("ClientQueryRequest".equals(type)) {
            var parsed = ProtoMapper.parseClientQueryRequest(payload);
            if (parsed.isEmpty()) {
                writeClientQueryResponse(responder, correlationId, new ClientQueryResponse(0L, me.getId(), false, "INVALID", "Invalid ClientQueryRequest payload", "", "", 0, new byte[0]));
                return;
            }
            ClientQueryResponse response = handleClientQueryRequest(ProtoMapper.fromProto(parsed.get()));
            writeClientQueryResponse(responder, correlationId, response);
            return;
        }
        if ("JoinClusterRequest".equals(type)) {
            var parsed = ProtoMapper.parseJoinClusterRequest(payload);
            if (parsed.isEmpty()) {
                writeJoinResponse(responder, correlationId, new JoinClusterResponse(0L, me.getId(), false, "INVALID", "Invalid JoinClusterRequest payload", ""));
                return;
            }
            JoinClusterResponse response = handleJoinClusterRequest(ProtoMapper.fromProto(parsed.get()));
            writeJoinResponse(responder, correlationId, response);
            return;
        }
        if ("JoinClusterStatusRequest".equals(type)) {
            var parsed = ProtoMapper.parseJoinClusterStatusRequest(payload);
            if (parsed.isEmpty()) {
                writeJoinStatusResponse(responder, correlationId, new JoinClusterStatusResponse(0L, me.getId(), false, "INVALID", "Invalid JoinClusterStatusRequest payload", ""));
                return;
            }
            JoinClusterStatusResponse response = handleJoinClusterStatusRequest(ProtoMapper.fromProto(parsed.get()));
            writeJoinStatusResponse(responder, correlationId, response);
            return;
        }
        if ("ReconfigureClusterRequest".equals(type)) {
            var parsed = ProtoMapper.parseReconfigureClusterRequest(payload);
            if (parsed.isEmpty()) {
                writeReconfigureResponse(responder, correlationId, new ReconfigureClusterResponse(0L, me.getId(), false, "INVALID", "Invalid ReconfigureClusterRequest payload", ""));
                return;
            }
            ReconfigureClusterResponse response = handleReconfigureClusterRequest(ProtoMapper.fromProto(parsed.get()));
            writeReconfigureResponse(responder, correlationId, response);
            return;
        }
        if ("TelemetryRequest".equals(type)) {
            var parsed = ProtoMapper.parseTelemetryRequest(payload);
            if (parsed.isEmpty()) {
                writeTelemetryResponse(responder, correlationId, emptyTelemetry("INVALID"));
                return;
            }
            TelemetryResponse response = handleTelemetryRequest(ProtoMapper.fromProto(parsed.get()));
            writeTelemetryResponse(responder, correlationId, response);
            return;
        }
        if ("ClusterSummaryRequest".equals(type)) {
            var parsed = ProtoMapper.parseClusterSummaryRequest(payload);
            if (parsed.isEmpty()) {
                writeClusterSummaryResponse(responder, correlationId, emptyClusterSummary("INVALID"));
                return;
            }
            ClusterSummaryResponse response = handleClusterSummaryRequest(ProtoMapper.fromProto(parsed.get()));
            writeClusterSummaryResponse(responder, correlationId, response);
            return;
        }
        if ("ReconfigurationStatusRequest".equals(type)) {
            var parsed = ProtoMapper.parseReconfigurationStatusRequest(payload);
            if (parsed.isEmpty()) {
                writeReconfigurationStatusResponse(responder, correlationId, emptyReconfigurationStatus("INVALID"));
                return;
            }
            ReconfigurationStatusResponse response = handleReconfigurationStatusRequest(ProtoMapper.fromProto(parsed.get()));
            writeReconfigurationStatusResponse(responder, correlationId, response);
            return;
        }

        log.debug("Received '{}' message ({} bytes)", type, payload == null ? 0 : payload.length);
    }

    static boolean isValidClusterCommand(byte[] command) {
        var decoded = StateMachineCommand.decode(command);
        if (decoded.isEmpty()) {
            return false;
        }
        return switch (decoded.get().getType()) {
            case PUT -> !decoded.get().getKey().isBlank();
            case DELETE -> !decoded.get().getKey().isBlank();
            case CLEAR -> true;
        };
    }

    private boolean submitClusterCommand(byte[] command) {
        return stateMachine.submitCommand(command);
    }

    static boolean isValidClusterQuery(byte[] query) {
        var decoded = org.gautelis.raft.protocol.StateMachineQuery.decode(query);
        return decoded.isPresent() && !decoded.get().getKey().isBlank();
    }

    protected boolean isValidClientCommand(byte[] command) {
        return isValidClusterCommand(command);
    }

    protected boolean isValidClientQuery(byte[] query) {
        return isValidClusterQuery(query);
    }

    protected ClientCommandResponse handleClientCommandRequest(ClientCommandRequest request) {
        if (stateMachine == null || request == null) {
            return new ClientCommandResponse(0L, me.getId(), false, "INVALID", "Client command request is invalid", "", "", 0);
        }
        byte[] command = request.getCommand();
        if (!isValidClientCommand(command)) {
            Peer leader = knownLeader();
            return new ClientCommandResponse(stateMachine.getTerm(), me.getId(), false, "INVALID", "Command payload is invalid", leaderId(leader), leaderHost(leader), leaderPort(leader));
        }
        Peer leader = stateMachine.getKnownLeaderPeer();
        Peer.Role localMemberRole = stateMachine.getLocalMemberRole();
        ClientCommandAuthenticationResult authentication = clientCommandAuthenticator().authenticate(
                new ClientCommandAuthenticationContext(
                        request.getPeerId(),
                        request.getAuthScheme(),
                        request.getAuthToken(),
                        command,
                        me,
                        localMemberRole,
                        stateMachine.isLeader(),
                        stateMachine.isDecommissioned()
                )
        );
        if (!authentication.authenticated()) {
            return new ClientCommandResponse(stateMachine.getTerm(), me.getId(), false, authentication.status(), authentication.message(), leaderId(leader), leaderHost(leader), leaderPort(leader));
        }
        String principalId = authentication.principal() == null ? request.getPeerId() : authentication.principal().principalId();
        ClientWriteAdmissionDecision admissionDecision = clientWriteAdmissionPolicy().evaluate(
                new ClientWriteAdmissionContext(
                        principalId,
                        command,
                        me,
                        localMemberRole,
                        stateMachine.isLeader(),
                        stateMachine.isDecommissioned(),
                        leader
                )
        );
        if (admissionDecision.action() == ClientWriteAdmissionDecision.Action.REDIRECT) {
            return new ClientCommandResponse(stateMachine.getTerm(), me.getId(), false, admissionDecision.status(), admissionDecision.message(), leaderId(leader), leaderHost(leader), leaderPort(leader));
        }
        if (admissionDecision.action() == ClientWriteAdmissionDecision.Action.REJECT) {
            return new ClientCommandResponse(stateMachine.getTerm(), me.getId(), false, admissionDecision.status(), admissionDecision.message(), leaderId(leader), leaderHost(leader), leaderPort(leader));
        }
        ClientCommandAuthorizationResult authorization = clientCommandAuthorizer().authorize(
                new ClientCommandAuthorizationContext(
                        principalId,
                        command,
                        me,
                        localMemberRole,
                        stateMachine.isLeader(),
                        stateMachine.isDecommissioned()
                )
        );
        if (!authorization.allowed()) {
            return new ClientCommandResponse(stateMachine.getTerm(), me.getId(), false, authorization.status(), authorization.message(), leaderId(leader), leaderHost(leader), leaderPort(leader));
        }
        if (stateMachine.isLeader()) {
            if (!submitClusterCommand(command)) {
                return new ClientCommandResponse(stateMachine.getTerm(), me.getId(), false, "REJECTED", "Command could not be applied", leaderId(leader), leaderHost(leader), leaderPort(leader));
            }
            log.info("Accepted typed client command from {}", request.getPeerId());
            return new ClientCommandResponse(stateMachine.getTerm(), me.getId(), true, "ACCEPTED", "Command accepted for replication", leaderId(leader), leaderHost(leader), leaderPort(leader));
        }
        return new ClientCommandResponse(stateMachine.getTerm(), me.getId(), false, "REJECTED", "Node is not leader or command could not be applied", "", "", 0);
    }

    protected ClientWriteAdmissionPolicy clientWriteAdmissionPolicy() {
        return LeaderRedirectWriteAdmissionPolicy.INSTANCE;
    }

    protected ClientCommandAuthorizer clientCommandAuthorizer() {
        return ClientCommandAuthorizer.allowAll();
    }

    protected ClientCommandAuthenticator clientCommandAuthenticator() {
        return ClientCommandAuthenticator.none();
    }

    protected String outboundRequestAuthScheme() {
        return runtimeConfiguration.requestAuthScheme();
    }

    protected String outboundRequestAuthToken() {
        return runtimeConfiguration.requestAuthToken();
    }

    private ClientCommandAuthenticationResult authenticateExternalRequest(String requesterId, String authenticationScheme, String authenticationToken) {
        Peer.Role localMemberRole = stateMachine == null ? null : stateMachine.getLocalMemberRole();
        return clientCommandAuthenticator().authenticate(
                requesterId,
                authenticationScheme,
                authenticationToken,
                me,
                localMemberRole,
                stateMachine != null && stateMachine.isLeader(),
                stateMachine != null && stateMachine.isDecommissioned()
        );
    }

    protected ClientQueryResponse handleClientQueryRequest(ClientQueryRequest request) {
        if (stateMachine == null || request == null) {
            return new ClientQueryResponse(0L, me.getId(), false, "INVALID", "Client query request is invalid", "", "", 0, new byte[0]);
        }
        byte[] query = request.getQuery();
        if (!isValidClientQuery(query)) {
            Peer leader = knownLeader();
            return new ClientQueryResponse(stateMachine.getTerm(), me.getId(), false, "INVALID", "Query payload is invalid", leaderId(leader), leaderHost(leader), leaderPort(leader), new byte[0]);
        }
        ClientCommandAuthenticationResult authentication = authenticateExternalRequest(request.getPeerId(), request.getAuthScheme(), request.getAuthToken());
        if (!authentication.authenticated()) {
            Peer leader = knownLeader();
            return new ClientQueryResponse(stateMachine.getTerm(), me.getId(), false, authentication.status(), authentication.message(), leaderId(leader), leaderHost(leader), leaderPort(leader), new byte[0]);
        }
        if (stateMachine.isLeader()) {
            if (!stateMachine.canServeLinearizableRead() && !stateMachine.awaitLinearizableRead()) {
                Peer leader = knownLeader();
                return new ClientQueryResponse(stateMachine.getTerm(), me.getId(), false, "RETRY", "Leader cannot currently guarantee a linearizable read", leaderId(leader), leaderHost(leader), leaderPort(leader), new byte[0]);
            }
            byte[] result = stateMachine.queryStateMachine(query);
            if (result.length == 0) {
                Peer leader = knownLeader();
                return new ClientQueryResponse(stateMachine.getTerm(), me.getId(), false, "UNSUPPORTED", "State machine does not support queries", leaderId(leader), leaderHost(leader), leaderPort(leader), new byte[0]);
            }
            Peer leader = knownLeader();
            return new ClientQueryResponse(stateMachine.getTerm(), me.getId(), true, "OK", "Query completed", leaderId(leader), leaderHost(leader), leaderPort(leader), result);
        }
        Peer leader = stateMachine.getKnownLeaderPeer();
        if (leader != null && !stateMachine.isDecommissioned()) {
            return new ClientQueryResponse(stateMachine.getTerm(), me.getId(), false, "REDIRECT", "Node is not leader; send query to current leader", leaderId(leader), leaderHost(leader), leaderPort(leader), new byte[0]);
        }
        return new ClientQueryResponse(stateMachine.getTerm(), me.getId(), false, "REJECTED", "Node is not leader or query could not be applied", "", "", 0, new byte[0]);
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

    static String describe(Peer peer) {
        if (peer == null) {
            throw new IllegalArgumentException("Peer must not be null");
        }
        String roleSuffix = peer.getRole() == Peer.Role.VOTER ? "" : "/" + peer.getRole().name().toLowerCase(java.util.Locale.ROOT);
        if (peer.getAddress() == null) {
            return peer.getId() + roleSuffix;
        }
        return peer.getId() + "@" + peer.getAddress().getHostString() + ":" + peer.getAddress().getPort() + roleSuffix;
    }

    protected JoinClusterResponse handleJoinClusterRequest(JoinClusterRequest request) {
        if (stateMachine == null || request == null || request.getJoiningPeer() == null) {
            return new JoinClusterResponse(0L, me.getId(), false, "INVALID", "Join request is invalid", "");
        }
        ClientCommandAuthenticationResult authentication = authenticateExternalRequest(request.getPeerId(), request.getAuthScheme(), request.getAuthToken());
        if (!authentication.authenticated()) {
            return new JoinClusterResponse(stateMachine.getTerm(), me.getId(), false, authentication.status(), authentication.message(), knownLeaderId());
        }
        try {
            registerPeerReference(request.getJoiningPeer());
        } catch (IllegalArgumentException e) {
            return new JoinClusterResponse(stateMachine.getTerm(), me.getId(), false, "INVALID", e.getMessage(), knownLeaderId());
        }
        if (stateMachine.isLeader()) {
            boolean accepted = stateMachine.submitJoinConfigurationChange(request.getJoiningPeer());
            if (!accepted) {
                return new JoinClusterResponse(stateMachine.getTerm(), me.getId(), false, "REJECTED", "Join request could not be applied", knownLeaderId());
            }
            RaftNode.JoinStatus joinStatus = stateMachine.getJoinStatus(request.getJoiningPeer().getId());
            return new JoinClusterResponse(stateMachine.getTerm(), me.getId(), true, joinStatus.status(), joinStatus.message(), knownLeaderId());
        }
        Peer leader = stateMachine.getKnownLeaderPeer();
        if (leader != null && !stateMachine.isDecommissioned()) {
            stateMachine.getRaftClient().sendJoinClusterRequest(leader, request);
            return new JoinClusterResponse(stateMachine.getTerm(), me.getId(), true, "FORWARDED", "Join request forwarded to leader", leader.getId());
        }
        return new JoinClusterResponse(stateMachine.getTerm(), me.getId(), false, "REJECTED", "Node is not leader or join request could not be applied", knownLeaderId());
    }

    protected JoinClusterStatusResponse handleJoinClusterStatusRequest(JoinClusterStatusRequest request) {
        if (stateMachine == null || request == null) {
            return new JoinClusterStatusResponse(0L, me.getId(), false, "INVALID", "Join status request is invalid", "");
        }
        ClientCommandAuthenticationResult authentication = authenticateExternalRequest(request.getPeerId(), request.getAuthScheme(), request.getAuthToken());
        if (!authentication.authenticated()) {
            return new JoinClusterStatusResponse(stateMachine.getTerm(), me.getId(), false, authentication.status(), authentication.message(), knownLeaderId());
        }
        RaftNode.JoinStatus joinStatus = stateMachine.getJoinStatus(request.getTargetPeerId());
        return new JoinClusterStatusResponse(
                stateMachine.getTerm(),
                me.getId(),
                joinStatus.success(),
                joinStatus.status(),
                joinStatus.message(),
                knownLeaderId()
        );
    }

    protected ReconfigureClusterResponse handleReconfigureClusterRequest(ReconfigureClusterRequest request) {
        if (stateMachine == null || request == null || request.getAction() == null) {
            return new ReconfigureClusterResponse(0L, me.getId(), false, "INVALID", "Reconfiguration request is invalid", "");
        }
        ClientCommandAuthenticationResult authentication = authenticateExternalRequest(request.getPeerId(), request.getAuthScheme(), request.getAuthToken());
        if (!authentication.authenticated()) {
            return new ReconfigureClusterResponse(stateMachine.getTerm(), me.getId(), false, authentication.status(), authentication.message(), knownLeaderId());
        }
        try {
            for (Peer member : request.getMembers()) {
                registerPeerReference(member);
            }
        } catch (IllegalArgumentException e) {
            return new ReconfigureClusterResponse(stateMachine.getTerm(), me.getId(), false, "INVALID", e.getMessage(), knownLeaderId());
        }
        if (stateMachine.isLeader()) {
            boolean accepted = switch (request.getAction()) {
                case JOINT -> stateMachine.submitJointConfigurationChange(request.getMembers());
                case FINALIZE -> stateMachine.submitFinalizeConfigurationChange();
                case PROMOTE -> request.getMembers().size() == 1 && stateMachine.submitPromoteLearnerChange(request.getMembers().getFirst());
                case DEMOTE -> request.getMembers().size() == 1 && stateMachine.submitDemoteVoterChange(request.getMembers().getFirst());
            };
            if (!accepted) {
                return new ReconfigureClusterResponse(stateMachine.getTerm(), me.getId(), false, "REJECTED", "Reconfiguration request could not be applied", knownLeaderId());
            }
            String message = switch (request.getAction()) {
                case JOINT -> "Joint configuration accepted";
                case FINALIZE -> "Finalize configuration accepted";
                case PROMOTE -> "Learner promotion accepted";
                case DEMOTE -> "Voter demotion accepted";
            };
            return new ReconfigureClusterResponse(stateMachine.getTerm(), me.getId(), true, "ACCEPTED", message, knownLeaderId());
        }
        Peer leader = stateMachine.getKnownLeaderPeer();
        if (leader != null && !stateMachine.isDecommissioned()) {
            stateMachine.getRaftClient().sendReconfigureClusterRequest(leader, request);
            return new ReconfigureClusterResponse(stateMachine.getTerm(), me.getId(), true, "FORWARDED", "Reconfiguration request forwarded to leader", leader.getId());
        }
        return new ReconfigureClusterResponse(stateMachine.getTerm(), me.getId(), false, "REJECTED", "Node is not leader or reconfiguration request could not be applied", knownLeaderId());
    }

    private void writeJoinResponse(MessageResponder responder, String correlationId, JoinClusterResponse response) {
        if (responder == null || correlationId == null || correlationId.isBlank() || response == null) {
            return;
        }
        responder.write(correlationId, "JoinClusterResponse", ProtoMapper.toProto(response).toByteString().toByteArray());
    }

    private void writeClientCommandResponse(MessageResponder responder, String correlationId, ClientCommandResponse response) {
        if (responder == null || correlationId == null || correlationId.isBlank() || response == null) {
            return;
        }
        responder.write(correlationId, "ClientCommandResponse", ProtoMapper.toProto(response).toByteString().toByteArray());
    }

    private void writeClientQueryResponse(MessageResponder responder, String correlationId, ClientQueryResponse response) {
        if (responder == null || correlationId == null || correlationId.isBlank() || response == null) {
            return;
        }
        responder.write(correlationId, "ClientQueryResponse", ProtoMapper.toProto(response).toByteString().toByteArray());
    }

    private void writeJoinStatusResponse(MessageResponder responder, String correlationId, JoinClusterStatusResponse response) {
        if (responder == null || correlationId == null || correlationId.isBlank() || response == null) {
            return;
        }
        responder.write(correlationId, "JoinClusterStatusResponse", ProtoMapper.toProto(response).toByteString().toByteArray());
    }

    private void writeReconfigureResponse(MessageResponder responder, String correlationId, ReconfigureClusterResponse response) {
        if (responder == null || correlationId == null || correlationId.isBlank() || response == null) {
            return;
        }
        responder.write(correlationId, "ReconfigureClusterResponse", ProtoMapper.toProto(response).toByteString().toByteArray());
    }

    private void writeTelemetryResponse(MessageResponder responder, String correlationId, TelemetryResponse response) {
        if (responder == null || correlationId == null || correlationId.isBlank() || response == null) {
            return;
        }
        responder.write(correlationId, "TelemetryResponse", ProtoMapper.toProto(response).toByteString().toByteArray());
    }

    private void writeClusterSummaryResponse(MessageResponder responder, String correlationId, ClusterSummaryResponse response) {
        if (responder == null || correlationId == null || correlationId.isBlank() || response == null) {
            return;
        }
        responder.write(correlationId, "ClusterSummaryResponse", ProtoMapper.toProto(response).toByteString().toByteArray());
    }

    private void writeReconfigurationStatusResponse(MessageResponder responder, String correlationId, ReconfigurationStatusResponse response) {
        if (responder == null || correlationId == null || correlationId.isBlank() || response == null) {
            return;
        }
        responder.write(correlationId, "ReconfigurationStatusResponse", ProtoMapper.toProto(response).toByteString().toByteArray());
    }

    private void registerPeerReference(Peer peer) {
        if (peer == null) {
            throw new IllegalArgumentException("Peer must not be null");
        }
        resolvePeer(describe(peer));
    }

    private String knownLeaderId() {
        return leaderId(knownLeader());
    }

    private Peer knownLeader() {
        return stateMachine == null ? null : stateMachine.getKnownLeaderPeer();
    }

    private static String leaderId(Peer leader) {
        return leader == null ? "" : leader.getId();
    }

    private static String leaderHost(Peer leader) {
        return leader == null || leader.getAddress() == null ? "" : leader.getAddress().getHostString();
    }

    private static int leaderPort(Peer leader) {
        return leader == null || leader.getAddress() == null ? 0 : leader.getAddress().getPort();
    }

    protected TelemetryResponse handleTelemetryRequest(TelemetryRequest request) {
        if (stateMachine == null || request == null) {
            return emptyTelemetry("INVALID");
        }
        ClientCommandAuthenticationResult authentication = authenticateExternalRequest(request.getPeerId(), request.getAuthScheme(), request.getAuthToken());
        if (!authentication.authenticated()) {
            return emptyTelemetry(authentication.status());
        }
        if (!allowTelemetryRequest(request.getPeerId())) {
            return emptyTelemetry("RATE_LIMITED");
        }
        if (request.isRequireLeaderSummary() && !stateMachine.isLeader()) {
            RaftNode.TelemetrySnapshot snapshot = stateMachine.telemetrySnapshot();
            Peer leader = stateMachine.getKnownLeaderPeer();
            return new TelemetryResponse(
                    snapshot.observedAtMillis(),
                    snapshot.term(),
                    snapshot.peerId(),
                    false,
                    leader == null ? "NO_LEADER" : "REDIRECT",
                    leader == null ? "" : leader.getId(),
                    snapshot.state(),
                    snapshot.leaderId(),
                    snapshot.votedFor(),
                    snapshot.joining(),
                    snapshot.decommissioned(),
                    snapshot.commitIndex(),
                    snapshot.lastApplied(),
                    snapshot.lastLogIndex(),
                    snapshot.lastLogTerm(),
                    snapshot.snapshotIndex(),
                    snapshot.snapshotTerm(),
                    snapshot.lastHeartbeatMillis(),
                    snapshot.nextElectionDeadlineMillis(),
                    snapshot.configuration().isJointConsensus(),
                    List.copyOf(snapshot.configuration().currentMembers()),
                    List.copyOf(snapshot.configuration().nextMembers()),
                    snapshot.knownPeers(),
                    snapshot.pendingJoinIds(),
                    snapshot.replication(),
                    List.of(),
                    "",
                    false,
                    false,
                    false,
                    0,
                    0,
                    0,
                    0L,
                    "",
                    List.of(),
                    List.of(),
                    List.of()
            );
        }
        RaftNode.TelemetrySnapshot snapshot = stateMachine.telemetrySnapshot();
        List<org.gautelis.raft.protocol.TelemetryPeerStats> peerStats = request.isIncludePeerStats()
                ? stateMachine.getRaftClient().snapshotResponseTimeStats()
                : List.of();
        ClusterTelemetrySummary clusterSummary = summarizeCluster(snapshot);
        return new TelemetryResponse(
                snapshot.observedAtMillis(),
                snapshot.term(),
                snapshot.peerId(),
                true,
                "OK",
                "",
                snapshot.state(),
                snapshot.leaderId(),
                snapshot.votedFor(),
                snapshot.joining(),
                snapshot.decommissioned(),
                snapshot.commitIndex(),
                snapshot.lastApplied(),
                snapshot.lastLogIndex(),
                snapshot.lastLogTerm(),
                snapshot.snapshotIndex(),
                snapshot.snapshotTerm(),
                snapshot.lastHeartbeatMillis(),
                snapshot.nextElectionDeadlineMillis(),
                snapshot.configuration().isJointConsensus(),
                List.copyOf(snapshot.configuration().currentMembers()),
                List.copyOf(snapshot.configuration().nextMembers()),
                snapshot.knownPeers(),
                snapshot.pendingJoinIds(),
                snapshot.replication(),
                peerStats,
                clusterSummary.clusterHealth(),
                clusterSummary.quorumAvailable(),
                clusterSummary.currentQuorumAvailable(),
                clusterSummary.nextQuorumAvailable(),
                clusterSummary.votingMembers(),
                clusterSummary.healthyVotingMembers(),
                clusterSummary.reachableVotingMembers(),
                clusterSummary.reconfigurationAgeMillis(),
                clusterSummary.clusterStatusReason(),
                clusterSummary.blockingCurrentQuorumPeerIds(),
                clusterSummary.blockingNextQuorumPeerIds(),
                clusterSummary.members()
        );
    }

    protected ClusterSummaryResponse handleClusterSummaryRequest(ClusterSummaryRequest request) {
        if (stateMachine == null || request == null) {
            return emptyClusterSummary("INVALID");
        }
        ClientCommandAuthenticationResult authentication = authenticateExternalRequest(request.getPeerId(), request.getAuthScheme(), request.getAuthToken());
        if (!authentication.authenticated()) {
            return emptyClusterSummary(authentication.status());
        }
        if (!allowTelemetryRequest(request.getPeerId())) {
            return emptyClusterSummary("RATE_LIMITED");
        }
        RaftNode.TelemetrySnapshot snapshot = stateMachine.telemetrySnapshot();
        if (!stateMachine.isLeader()) {
            Peer leader = stateMachine.getKnownLeaderPeer();
            return new ClusterSummaryResponse(
                    snapshot.observedAtMillis(),
                    snapshot.term(),
                    snapshot.peerId(),
                    false,
                    leader == null ? "NO_LEADER" : "REDIRECT",
                    leaderId(leader),
                    leaderHost(leader),
                    leaderPort(leader),
                    snapshot.state(),
                    snapshot.leaderId(),
                    snapshot.configuration().isJointConsensus(),
                    "",
                    "",
                    false,
                    false,
                    false,
                    0,
                    0,
                    0,
                    0L,
                    List.of(),
                    List.of(),
                    List.of()
            );
        }
        ClusterTelemetrySummary clusterSummary = summarizeCluster(snapshot);
        return new ClusterSummaryResponse(
                snapshot.observedAtMillis(),
                snapshot.term(),
                snapshot.peerId(),
                true,
                "OK",
                "",
                "",
                0,
                snapshot.state(),
                snapshot.leaderId(),
                snapshot.configuration().isJointConsensus(),
                clusterSummary.clusterHealth(),
                clusterSummary.clusterStatusReason(),
                clusterSummary.quorumAvailable(),
                clusterSummary.currentQuorumAvailable(),
                clusterSummary.nextQuorumAvailable(),
                clusterSummary.votingMembers(),
                clusterSummary.healthyVotingMembers(),
                clusterSummary.reachableVotingMembers(),
                clusterSummary.reconfigurationAgeMillis(),
                clusterSummary.blockingCurrentQuorumPeerIds(),
                clusterSummary.blockingNextQuorumPeerIds(),
                clusterSummary.members()
        );
    }

    protected ReconfigurationStatusResponse handleReconfigurationStatusRequest(ReconfigurationStatusRequest request) {
        if (stateMachine == null || request == null) {
            return emptyReconfigurationStatus("INVALID");
        }
        ClientCommandAuthenticationResult authentication = authenticateExternalRequest(request.getPeerId(), request.getAuthScheme(), request.getAuthToken());
        if (!authentication.authenticated()) {
            return emptyReconfigurationStatus(authentication.status());
        }
        if (!allowTelemetryRequest(request.getPeerId())) {
            return emptyReconfigurationStatus("RATE_LIMITED");
        }
        RaftNode.TelemetrySnapshot snapshot = stateMachine.telemetrySnapshot();
        if (!stateMachine.isLeader()) {
            Peer leader = stateMachine.getKnownLeaderPeer();
            return new ReconfigurationStatusResponse(
                    snapshot.observedAtMillis(),
                    snapshot.term(),
                    snapshot.peerId(),
                    false,
                    leader == null ? "NO_LEADER" : "REDIRECT",
                    leaderId(leader),
                    leaderHost(leader),
                    leaderPort(leader),
                    snapshot.state(),
                    snapshot.leaderId(),
                    false,
                    snapshot.configuration().isJointConsensus(),
                    0L,
                    "",
                    "",
                    false,
                    false,
                    false,
                    List.of(),
                    List.of(),
                    List.of()
            );
        }
        ClusterTelemetrySummary clusterSummary = summarizeCluster(snapshot);
        boolean jointConsensus = snapshot.latestKnownConfiguration().isJointConsensus();
        boolean reconfigurationActive = jointConsensus
                || clusterSummary.reconfigurationAgeMillis() > 0L;
        List<String> blockingCurrentQuorumPeerIds = clusterSummary.blockingCurrentQuorumPeerIds().isEmpty()
                ? clusterSummary.members().stream()
                .filter(member -> member.blockingQuorums() != null && member.blockingQuorums().contains("current"))
                .map(ClusterMemberSummary::peerId)
                .toList()
                : clusterSummary.blockingCurrentQuorumPeerIds();
        List<String> blockingNextQuorumPeerIds = clusterSummary.blockingNextQuorumPeerIds().isEmpty()
                ? clusterSummary.members().stream()
                .filter(member -> member.blockingQuorums() != null && member.blockingQuorums().contains("next"))
                .map(ClusterMemberSummary::peerId)
                .toList()
                : clusterSummary.blockingNextQuorumPeerIds();
        return new ReconfigurationStatusResponse(
                snapshot.observedAtMillis(),
                snapshot.term(),
                snapshot.peerId(),
                true,
                "OK",
                "",
                "",
                0,
                snapshot.state(),
                snapshot.leaderId(),
                reconfigurationActive,
                jointConsensus,
                clusterSummary.reconfigurationAgeMillis(),
                clusterSummary.clusterHealth(),
                clusterSummary.clusterStatusReason(),
                clusterSummary.quorumAvailable(),
                clusterSummary.currentQuorumAvailable(),
                clusterSummary.nextQuorumAvailable(),
                blockingCurrentQuorumPeerIds,
                blockingNextQuorumPeerIds,
                clusterSummary.members()
        );
    }

    private synchronized boolean allowTelemetryRequest(String requesterId) {
        if (runtimeConfiguration.telemetryRateLimitPerMinute() <= 0) {
            return true;
        }
        long now = System.currentTimeMillis();
        String key = requesterId == null || requesterId.isBlank() ? "anonymous" : requesterId;
        Deque<Long> timestamps = telemetryRequestHistory.computeIfAbsent(key, ignored -> new ArrayDeque<>());
        long cutoff = now - 60_000L;
        while (!timestamps.isEmpty() && timestamps.peekFirst() < cutoff) {
            timestamps.removeFirst();
        }
        if (timestamps.size() >= runtimeConfiguration.telemetryRateLimitPerMinute()) {
            return false;
        }
        timestamps.addLast(now);
        return true;
    }

    private TelemetryResponse emptyTelemetry(String status) {
        return new TelemetryResponse(0L, 0L, me.getId(), false, status, "", "", "", "", false, false, 0, 0, 0, 0, 0, 0, 0, 0, false, List.of(), List.of(), List.of(), List.of(), List.of(), List.of(), "", false, false, false, 0, 0, 0, 0L, "", List.of(), List.of(), List.of());
    }

    private ClusterSummaryResponse emptyClusterSummary(String status) {
        return new ClusterSummaryResponse(0L, 0L, me.getId(), false, status, "", "", 0, "", "", false, "", "", false, false, false, 0, 0, 0, 0L, List.of(), List.of(), List.of());
    }

    private ReconfigurationStatusResponse emptyReconfigurationStatus(String status) {
        return new ReconfigurationStatusResponse(0L, 0L, me.getId(), false, status, "", "", 0, "", "", false, false, 0L, "", "", false, false, false, List.of(), List.of(), List.of());
    }

    private ClusterTelemetrySummary summarizeCluster(RaftNode.TelemetrySnapshot snapshot) {
        if (snapshot == null || !"LEADER".equals(snapshot.state())) {
            return new ClusterTelemetrySummary("", false, false, false, 0, 0, 0, 0L, "", List.of(), List.of(), List.of());
        }

        Map<String, org.gautelis.raft.protocol.TelemetryReplicationStatus> replicationByPeer = new HashMap<>();
        for (var replication : snapshot.replication()) {
            replicationByPeer.put(replication.peerId(), replication);
        }

        int reachableVotingMembers = 1;
        int healthyVotingMembers = 1;
        for (Peer peer : snapshot.configuration().allVotingMembers()) {
            if (snapshot.peerId().equals(peer.getId())) {
                continue;
            }
            var replication = replicationByPeer.get(peer.getId());
            if (replication != null && replication.reachable()) {
                reachableVotingMembers++;
            }
            if (isHealthyVotingPeer(replication, snapshot.observedAtMillis())) {
                healthyVotingMembers++;
            }
        }

        List<String> blockingCurrentQuorumPeerIds = blockingPeers(snapshot.configuration().currentVotingMembers(), snapshot.peerId(), replicationByPeer, snapshot.observedAtMillis());
        List<String> blockingNextQuorumPeerIds = snapshot.configuration().isJointConsensus()
                ? blockingPeers(snapshot.configuration().nextVotingMembers(), snapshot.peerId(), replicationByPeer, snapshot.observedAtMillis())
                : List.of();
        java.util.Set<String> blockingCurrentPeerSet = new java.util.LinkedHashSet<>(blockingCurrentQuorumPeerIds);
        java.util.Set<String> blockingNextPeerSet = new java.util.LinkedHashSet<>(blockingNextQuorumPeerIds);
        boolean currentQuorumAvailable = hasHealthyQuorum(snapshot.configuration().currentVotingMembers(), snapshot.peerId(), replicationByPeer, snapshot.observedAtMillis());
        boolean nextQuorumAvailable = snapshot.configuration().isJointConsensus()
                ? hasHealthyQuorum(snapshot.configuration().nextVotingMembers(), snapshot.peerId(), replicationByPeer, snapshot.observedAtMillis())
                : currentQuorumAvailable;
        boolean quorumAvailable = snapshot.configuration().isJointConsensus()
                ? currentQuorumAvailable && nextQuorumAvailable
                : currentQuorumAvailable;
        List<ClusterMemberSummary> members = buildClusterMemberSummaries(snapshot, replicationByPeer, blockingCurrentPeerSet, blockingNextPeerSet);

        long reconfigurationAgeMillis = transitionAgeMillis(snapshot);
        boolean reconfigurationStuck = runtimeConfiguration.telemetryReconfigurationStuckMillis() > 0L
                && reconfigurationAgeMillis >= runtimeConfiguration.telemetryReconfigurationStuckMillis();
        String clusterHealth = quorumAvailable
                ? (healthyVotingMembers == snapshot.configuration().allVotingMembers().size() ? "healthy" : "degraded")
                : "at-risk";
        if (reconfigurationStuck && "healthy".equals(clusterHealth)) {
            clusterHealth = "degraded";
        }
        String clusterStatusReason = describeClusterStatusReason(
                clusterHealth,
                snapshot.configuration().isJointConsensus(),
                currentQuorumAvailable,
                nextQuorumAvailable,
                reconfigurationStuck
        );
        return new ClusterTelemetrySummary(
                clusterHealth,
                quorumAvailable,
                currentQuorumAvailable,
                nextQuorumAvailable,
                snapshot.configuration().allVotingMembers().size(),
                healthyVotingMembers,
                reachableVotingMembers,
                reconfigurationAgeMillis,
                clusterStatusReason,
                List.copyOf(blockingCurrentQuorumPeerIds),
                List.copyOf(blockingNextQuorumPeerIds),
                members
        );
    }

    private List<ClusterMemberSummary> buildClusterMemberSummaries(
            RaftNode.TelemetrySnapshot snapshot,
            Map<String, org.gautelis.raft.protocol.TelemetryReplicationStatus> replicationByPeer,
            java.util.Set<String> blockingCurrentPeerSet,
            java.util.Set<String> blockingNextPeerSet
    ) {
        List<ClusterMemberSummary> members = new ArrayList<>();
        List<Peer> clusterPeers = new ArrayList<>();
        clusterPeers.add(new Peer(snapshot.peerId(), null));
        for (Peer peer : snapshot.knownPeers()) {
            if (!snapshot.peerId().equals(peer.getId())) {
                clusterPeers.add(peer);
            }
        }
        clusterPeers.sort(java.util.Comparator.comparing(Peer::getId));

        for (Peer peer : clusterPeers) {
            boolean local = snapshot.peerId().equals(peer.getId());
            Peer currentMember = snapshot.configuration().currentMembers().stream().filter(member -> peer.getId().equals(member.getId())).findFirst().orElse(null);
            Peer nextMember = snapshot.configuration().nextMembers().stream().filter(member -> peer.getId().equals(member.getId())).findFirst().orElse(null);
            boolean current = currentMember != null;
            boolean next = nextMember != null;
            Peer targetMember = targetConfigurationMember(snapshot.latestKnownConfiguration(), peer.getId());
            Peer effectiveMember = targetMember != null ? targetMember : (nextMember != null ? nextMember : (currentMember != null ? currentMember : peer));
            String currentRole = currentMember == null ? "" : currentMember.getRole().name();
            String nextRole = targetMember == null ? "" : targetMember.getRole().name();
            String roleTransition = describeRoleTransition(currentMember, targetMember);
            long transitionAgeMillis = ("steady".equals(roleTransition) || "known".equals(roleTransition)) ? 0L : transitionAgeMillis(snapshot);
            var replication = replicationByPeer.get(peer.getId());
            long matchIndex = local ? snapshot.lastLogIndex() : (replication == null ? 0L : replication.matchIndex());
            long nextIndex = local ? snapshot.lastLogIndex() + 1 : (replication == null ? 0L : replication.nextIndex());
            long lag = Math.max(0L, snapshot.lastLogIndex() - matchIndex);
            boolean reachable = local || (replication != null && replication.reachable());
            long lastSuccessfulContactMillis = local ? snapshot.lastHeartbeatMillis() : (replication == null ? 0L : replication.lastSuccessfulContactMillis());
            long lastFailedContactMillis = local ? 0L : (replication == null ? 0L : replication.lastFailedContactMillis());
            int consecutiveFailures = local ? 0 : (replication == null ? 0 : replication.consecutiveFailures());
            String freshness = describeFreshness(snapshot.observedAtMillis(), lastSuccessfulContactMillis);
            String health = describeHealth(local, reachable, freshness, consecutiveFailures);
            String blockingQuorums = describeBlockingQuorums(peer.getId(), blockingCurrentPeerSet, blockingNextPeerSet);
            String blockingReason = blockingQuorums.isBlank()
                    ? ""
                    : describeBlockingReason(local, reachable, freshness, lag, consecutiveFailures, roleTransition);
            members.add(new ClusterMemberSummary(
                    peer.getId(),
                    local,
                    current,
                    next,
                    effectiveMember.isVoter(),
                    effectiveMember.getRole().name(),
                    currentRole,
                    nextRole,
                    roleTransition,
                    transitionAgeMillis,
                    blockingQuorums,
                    blockingReason,
                    reachable,
                    freshness,
                    health,
                    nextIndex,
                    matchIndex,
                    lag,
                    consecutiveFailures,
                    lastSuccessfulContactMillis,
                    lastFailedContactMillis
            ));
        }
        return List.copyOf(members);
    }

    private String describeBlockingQuorums(String peerId, java.util.Set<String> blockingCurrentPeerSet, java.util.Set<String> blockingNextPeerSet) {
        boolean current = blockingCurrentPeerSet.contains(peerId);
        boolean next = blockingNextPeerSet.contains(peerId);
        if (current && next) {
            return "current,next";
        }
        if (current) {
            return "current";
        }
        if (next) {
            return "next";
        }
        return "";
    }

    private String describeBlockingReason(boolean local, boolean reachable, String freshness, long lag,
                                          int consecutiveFailures, String roleTransition) {
        if (local) {
            return "";
        }
        if (!reachable) {
            return "unreachable";
        }
        if (consecutiveFailures >= 3) {
            return "replication-failures";
        }
        if (lag > 0L) {
            return "lagging";
        }
        if ("stale".equals(freshness) || "unknown".equals(freshness)) {
            return "stale";
        }
        if (!"steady".equals(roleTransition) && !"known".equals(roleTransition)) {
            return "role-transition";
        }
        return "unknown";
    }

    private String describeRoleTransition(Peer currentMember, Peer nextMember) {
        if (currentMember == null && nextMember == null) {
            return "known";
        }
        if (currentMember == null) {
            return "joining";
        }
        if (nextMember == null) {
            return "removing";
        }
        if (currentMember.getRole() == nextMember.getRole()) {
            return "steady";
        }
        return nextMember.isVoter() ? "promoting" : "demoting";
    }

    private long transitionAgeMillis(RaftNode.TelemetrySnapshot snapshot) {
        long startedAt = snapshot.configurationTransitionStartedMillis();
        if (startedAt <= 0L) {
            return 0L;
        }
        return Math.max(0L, snapshot.observedAtMillis() - startedAt);
    }

    private Peer targetConfigurationMember(ClusterConfiguration configuration, String peerId) {
        if (configuration == null || peerId == null) {
            return null;
        }
        if (configuration.isJointConsensus()) {
            Peer next = configuration.nextMembers().stream().filter(member -> peerId.equals(member.getId())).findFirst().orElse(null);
            if (next != null) {
                return next;
            }
        }
        return configuration.currentMembers().stream().filter(member -> peerId.equals(member.getId())).findFirst().orElse(null);
    }

    private boolean hasHealthyQuorum(Collection<Peer> members, String selfId,
                                     Map<String, org.gautelis.raft.protocol.TelemetryReplicationStatus> replicationByPeer,
                                     long observedAtMillis) {
        int total = 0;
        int healthy = 0;
        for (Peer peer : members) {
            total++;
            if (selfId.equals(peer.getId())) {
                healthy++;
                continue;
            }
            if (isHealthyVotingPeer(replicationByPeer.get(peer.getId()), observedAtMillis)) {
                healthy++;
            }
        }
        return healthy >= ((total / 2) + 1);
    }

    private boolean isHealthyVotingPeer(org.gautelis.raft.protocol.TelemetryReplicationStatus replication, long observedAtMillis) {
        if (replication == null || !replication.reachable()) {
            return false;
        }
        if (replication.consecutiveFailures() >= 3) {
            return false;
        }
        long lastSuccess = replication.lastSuccessfulContactMillis();
        if (lastSuccess <= 0L) {
            return false;
        }
        long ageMillis = Math.max(0L, observedAtMillis - lastSuccess);
        return ageMillis < 15_000L;
    }

    private List<String> blockingPeers(Collection<Peer> members, String selfId,
                                       Map<String, org.gautelis.raft.protocol.TelemetryReplicationStatus> replicationByPeer,
                                       long observedAtMillis) {
        List<String> blocked = new ArrayList<>();
        for (Peer peer : members) {
            if (selfId.equals(peer.getId())) {
                continue;
            }
            if (!isHealthyVotingPeer(replicationByPeer.get(peer.getId()), observedAtMillis)) {
                blocked.add(peer.getId());
            }
        }
        blocked.sort(String::compareTo);
        return blocked;
    }

    private String describeClusterStatusReason(String clusterHealth, boolean jointConsensus,
                                               boolean currentQuorumAvailable, boolean nextQuorumAvailable,
                                               boolean reconfigurationStuck) {
        if ("healthy".equals(clusterHealth)) {
            return "all-voters-healthy";
        }
        if (reconfigurationStuck && !"at-risk".equals(clusterHealth)) {
            return "reconfiguration-stuck";
        }
        if ("degraded".equals(clusterHealth)) {
            return "followers-unhealthy";
        }
        if (jointConsensus && !currentQuorumAvailable && !nextQuorumAvailable) {
            return "current-and-next-quorum-unavailable";
        }
        if (!currentQuorumAvailable) {
            return "current-quorum-unavailable";
        }
        if (!nextQuorumAvailable) {
            return "next-quorum-unavailable";
        }
        return "unknown";
    }

    private String describeFreshness(long observedAtMillis, long lastContactMillis) {
        if (lastContactMillis <= 0) {
            return "unknown";
        }
        long ageMillis = Math.max(0L, observedAtMillis - lastContactMillis);
        if (ageMillis < 5_000L) {
            return "fresh";
        }
        if (ageMillis < 15_000L) {
            return "recent";
        }
        return "stale";
    }

    private String describeHealth(boolean local, boolean reachable, String freshness, int consecutiveFailures) {
        if (local) {
            return "local";
        }
        if (!reachable || consecutiveFailures >= 3) {
            return "unhealthy";
        }
        if (consecutiveFailures > 0 || "stale".equals(freshness) || "unknown".equals(freshness)) {
            return "degraded";
        }
        return "healthy";
    }

    private record ClusterTelemetrySummary(
            String clusterHealth,
            boolean quorumAvailable,
            boolean currentQuorumAvailable,
            boolean nextQuorumAvailable,
            int votingMembers,
            int healthyVotingMembers,
            int reachableVotingMembers,
            long reconfigurationAgeMillis,
            String clusterStatusReason,
            List<String> blockingCurrentQuorumPeerIds,
            List<String> blockingNextQuorumPeerIds,
            List<ClusterMemberSummary> members
    ) {
    }

    private Peer resolvePeer(String spec) {
        String trimmed = spec.trim();
        Peer.Role role = Peer.Role.VOTER;
        int slash = trimmed.lastIndexOf('/');
        if (slash > 0 && slash < trimmed.length() - 1) {
            role = Peer.Role.valueOf(trimmed.substring(slash + 1).trim().toUpperCase(java.util.Locale.ROOT));
            trimmed = trimmed.substring(0, slash).trim();
        }
        int at = trimmed.indexOf('@');
        if (at < 0) {
            // Bare ids are only valid for peers already known to the local adapter/node.
            if (me.getId().equals(trimmed)) {
                return new Peer(me.getId(), me.getAddress(), role == Peer.Role.VOTER ? me.getRole() : role);
            }
            for (Peer peer : peers) {
                if (peer.getId().equals(trimmed)) {
                    return new Peer(peer.getId(), peer.getAddress(), role == Peer.Role.VOTER ? peer.getRole() : role);
                }
            }
            if (stateMachine != null) {
                Peer peer = stateMachine.getPeerById(trimmed);
                if (peer != null) {
                    return new Peer(peer.getId(), peer.getAddress(), role == Peer.Role.VOTER ? peer.getRole() : role);
                }
            }
            throw new IllegalArgumentException("Unknown peer id '" + trimmed + "'; use id@host:port for new members");
        }

        String id = trimmed.substring(0, at).trim();
        String addressSpec = trimmed.substring(at + 1).trim();
        int colon = addressSpec.lastIndexOf(':');
        if (id.isBlank() || colon <= 0 || colon == addressSpec.length() - 1) {
            throw new IllegalArgumentException("Invalid peer specification '" + spec + "'");
        }
        // Allow introducing a brand-new member without first baking it into static startup config.
        String host = addressSpec.substring(0, colon).trim();
        int port = Integer.parseInt(addressSpec.substring(colon + 1).trim());
        return new Peer(id, new InetSocketAddress(host, port), role);
    }
}
