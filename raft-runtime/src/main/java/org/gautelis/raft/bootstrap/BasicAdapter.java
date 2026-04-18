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

import org.gautelis.raft.MessageHandler;
import org.gautelis.raft.RaftNode;
import org.gautelis.raft.protocol.ClientCommandRequest;
import org.gautelis.raft.protocol.ClientCommandResponse;
import org.gautelis.raft.protocol.ClientQueryRequest;
import org.gautelis.raft.protocol.ClientQueryResponse;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

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
    private final Map<String, RequestHandler> requestHandlers;
    private final ClientRequestHandler clientRequestHandler;
    private final MembershipRequestHandler membershipRequestHandler;
    private final OperationalRequestHandler operationalRequestHandler;

    protected RaftNode stateMachine;

    public BasicAdapter(long timeoutMillis, Peer me, List<Peer> peers) {
        this(AdapterSpec.forPeer(me)
                .withTimeoutMillis(timeoutMillis)
                .withPeers(peers)
                .buildDetached());
    }

    public BasicAdapter(long timeoutMillis, Peer me, List<Peer> peers, Peer joinSeed) {
        this(AdapterSpec.forPeer(me)
                .withTimeoutMillis(timeoutMillis)
                .withPeers(peers)
                .withJoinSeed(joinSeed)
                .buildDetached());
    }

    public BasicAdapter(long timeoutMillis, Peer me, List<Peer> peers, Peer joinSeed, RuntimeConfiguration runtimeConfiguration) {
        this(AdapterSpec.forPeer(me)
                .withTimeoutMillis(timeoutMillis)
                .withPeers(peers)
                .withJoinSeed(joinSeed)
                .withRuntimeConfiguration(runtimeConfiguration)
                .buildDetached());
    }

    public BasicAdapter(AdapterSpec spec) {
        this.timeoutMillis = spec.timeoutMillis();
        this.me = spec.me();
        this.peers = spec.peers();
        this.joinSeed = spec.joinSeed();
        this.runtimeConfiguration = spec.runtimeConfiguration();
        this.transportFactory = spec.transportFactory();
        AdapterHandlerContext handlerContext = new AdapterHandlerContext(
                log,
                me,
                runtimeConfiguration,
                () -> stateMachine,
                auth -> authenticateExternalRequest(auth.requesterId(), auth.authScheme(), auth.authToken()),
                this::isValidClientCommand,
                this::isValidClientQuery,
                this::submitClusterCommand,
                this::clientWriteAdmissionPolicy,
                this::clientCommandAuthorizer,
                this::clientCommandAuthenticator,
                this::registerPeerReference
        );
        this.clientRequestHandler = new ClientRequestHandler(handlerContext);
        this.membershipRequestHandler = new MembershipRequestHandler(handlerContext);
        this.operationalRequestHandler = new OperationalRequestHandler(handlerContext);
        this.requestHandlers = createRequestHandlers();
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
        stateMachine = RaftNode.forPeer(me)
                .withPeers(peers)
                .withTimeoutMillis(timeoutMillis)
                .withMessageHandler(messageHandler)
                .withSnapshotStateMachine(snapshotStateMachine)
                .withClient(raftClient)
                .withLogStore(logStore)
                .withPersistentStateStore(persistentStateStore)
                .build();
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
        RequestHandler handler = requestHandlers.get(type);
        if (handler != null) {
            handler.handle(correlationId, payload, responder);
            return;
        }
        log.debug("Received '{}' message ({} bytes)", type, payload == null ? 0 : payload.length);
    }

    private Map<String, RequestHandler> createRequestHandlers() {
        Map<String, RequestHandler> handlers = new HashMap<>();
        handlers.put("ClientCommandRequest", typedHandler(
                ProtoMapper::parseClientCommandRequest,
                this::invalidClientCommandResponse,
                proto -> handleClientCommandRequest(ProtoMapper.fromProto(proto)),
                AdapterResponseWriters::writeClientCommandResponse
        ));
        handlers.put("ClientQueryRequest", typedHandler(
                ProtoMapper::parseClientQueryRequest,
                this::invalidClientQueryResponse,
                proto -> handleClientQueryRequest(ProtoMapper.fromProto(proto)),
                AdapterResponseWriters::writeClientQueryResponse
        ));
        handlers.put("JoinClusterRequest", typedHandler(
                ProtoMapper::parseJoinClusterRequest,
                this::invalidJoinResponse,
                proto -> handleJoinClusterRequest(ProtoMapper.fromProto(proto)),
                AdapterResponseWriters::writeJoinResponse
        ));
        handlers.put("JoinClusterStatusRequest", typedHandler(
                ProtoMapper::parseJoinClusterStatusRequest,
                this::invalidJoinStatusResponse,
                proto -> handleJoinClusterStatusRequest(ProtoMapper.fromProto(proto)),
                AdapterResponseWriters::writeJoinStatusResponse
        ));
        handlers.put("ReconfigureClusterRequest", typedHandler(
                ProtoMapper::parseReconfigureClusterRequest,
                this::invalidReconfigureResponse,
                proto -> handleReconfigureClusterRequest(ProtoMapper.fromProto(proto)),
                AdapterResponseWriters::writeReconfigureResponse
        ));
        handlers.put("TelemetryRequest", typedHandler(
                ProtoMapper::parseTelemetryRequest,
                this::invalidTelemetryResponse,
                proto -> handleTelemetryRequest(ProtoMapper.fromProto(proto)),
                AdapterResponseWriters::writeTelemetryResponse
        ));
        handlers.put("ClusterSummaryRequest", typedHandler(
                ProtoMapper::parseClusterSummaryRequest,
                this::invalidClusterSummaryResponse,
                proto -> handleClusterSummaryRequest(ProtoMapper.fromProto(proto)),
                AdapterResponseWriters::writeClusterSummaryResponse
        ));
        handlers.put("ReconfigurationStatusRequest", typedHandler(
                ProtoMapper::parseReconfigurationStatusRequest,
                this::invalidReconfigurationStatusResponse,
                proto -> handleReconfigurationStatusRequest(ProtoMapper.fromProto(proto)),
                AdapterResponseWriters::writeReconfigurationStatusResponse
        ));
        return Map.copyOf(handlers);
    }

    private ClientCommandResponse invalidClientCommandResponse() {
        return new ClientCommandResponse(0L, me.getId(), false, "INVALID", "Invalid ClientCommandRequest payload", "", "", 0);
    }

    private ClientQueryResponse invalidClientQueryResponse() {
        return new ClientQueryResponse(0L, me.getId(), false, "INVALID", "Invalid ClientQueryRequest payload", "", "", 0, new byte[0]);
    }

    private JoinClusterResponse invalidJoinResponse() {
        return new JoinClusterResponse(0L, me.getId(), false, "INVALID", "Invalid JoinClusterRequest payload", "");
    }

    private JoinClusterStatusResponse invalidJoinStatusResponse() {
        return new JoinClusterStatusResponse(0L, me.getId(), false, "INVALID", "Invalid JoinClusterStatusRequest payload", "");
    }

    private ReconfigureClusterResponse invalidReconfigureResponse() {
        return new ReconfigureClusterResponse(0L, me.getId(), false, "INVALID", "Invalid ReconfigureClusterRequest payload", "");
    }

    private TelemetryResponse invalidTelemetryResponse() {
        return new TelemetryResponse(0L, 0L, me.getId(), false, "INVALID", "", "", "", "", false, false, 0, 0, 0, 0, 0, 0, 0, 0, false, List.of(), List.of(), List.of(), List.of(), List.of(), List.of(), "", false, false, false, 0, 0, 0, 0L, "", List.of(), List.of(), List.of());
    }

    private ClusterSummaryResponse invalidClusterSummaryResponse() {
        return new ClusterSummaryResponse(0L, 0L, me.getId(), false, "INVALID", "", "", 0, "", "", false, "", "", false, false, false, 0, 0, 0, 0L, List.of(), List.of(), List.of());
    }

    private ReconfigurationStatusResponse invalidReconfigurationStatusResponse() {
        return new ReconfigurationStatusResponse(0L, 0L, me.getId(), false, "INVALID", "", "", 0, "", "", false, false, 0L, "", "", false, false, false, List.of(), List.of(), List.of());
    }

    private <P, R> void handleTypedRequest(Optional<P> parsed,
                                           String correlationId,
                                           MessageResponder responder,
                                           Supplier<R> invalidResponse,
                                           Function<P, R> handler,
                                           ResponseWriter<R> writer) {
        if (parsed.isEmpty()) {
            writer.write(responder, correlationId, invalidResponse.get());
            return;
        }
        writer.write(responder, correlationId, handler.apply(parsed.get()));
    }

    private <P, R> RequestHandler typedHandler(Function<byte[], Optional<P>> parser,
                                               Supplier<R> invalidResponse,
                                               Function<P, R> handler,
                                               ResponseWriter<R> writer) {
        return (correlationId, payload, responder) -> handleTypedRequest(
                parser.apply(payload),
                correlationId,
                responder,
                invalidResponse,
                handler,
                writer
        );
    }

    @FunctionalInterface
    private interface ResponseWriter<R> {
        void write(MessageResponder responder, String correlationId, R response);
    }

    @FunctionalInterface
    private interface RequestHandler {
        void handle(String correlationId, byte[] payload, MessageResponder responder);
    }

    static boolean isValidClusterCommand(byte[] command) {
        var decoded = StateMachineCommand.decode(command);
        if (decoded.isEmpty()) {
            return false;
        }
        return switch (decoded.get().getType()) {
            case PUT -> !decoded.get().getKey().isBlank();
            case CAS -> !decoded.get().getKey().isBlank();
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
        return clientRequestHandler.handleClientCommandRequest(request);
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
        return clientRequestHandler.handleClientQueryRequest(request);
    }

    protected JoinClusterResponse handleJoinClusterRequest(JoinClusterRequest request) {
        return membershipRequestHandler.handleJoinClusterRequest(request);
    }

    protected JoinClusterStatusResponse handleJoinClusterStatusRequest(JoinClusterStatusRequest request) {
        return membershipRequestHandler.handleJoinClusterStatusRequest(request);
    }

    protected ReconfigureClusterResponse handleReconfigureClusterRequest(ReconfigureClusterRequest request) {
        return membershipRequestHandler.handleReconfigureClusterRequest(request);
    }

    private void registerPeerReference(Peer peer) {
        if (peer == null) {
            throw new IllegalArgumentException("Peer must not be null");
        }
        PeerSpecSupport.resolve(PeerSpecSupport.describe(peer), me, peers, stateMachine);
    }

    protected TelemetryResponse handleTelemetryRequest(TelemetryRequest request) {
        return operationalRequestHandler.handleTelemetryRequest(request);
    }

    protected ClusterSummaryResponse handleClusterSummaryRequest(ClusterSummaryRequest request) {
        return operationalRequestHandler.handleClusterSummaryRequest(request);
    }

    protected ReconfigurationStatusResponse handleReconfigurationStatusRequest(ReconfigurationStatusRequest request) {
        return operationalRequestHandler.handleReconfigurationStatusRequest(request);
    }

}
