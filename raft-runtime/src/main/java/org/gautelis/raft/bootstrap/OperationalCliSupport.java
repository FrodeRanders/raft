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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.gautelis.raft.protocol.ClusterSummaryRequest;
import org.gautelis.raft.protocol.ClusterSummaryResponse;
import org.gautelis.raft.protocol.JoinClusterResponse;
import org.gautelis.raft.protocol.JoinClusterStatusResponse;
import org.gautelis.raft.protocol.Peer;
import org.gautelis.raft.protocol.ReconfigurationStatusRequest;
import org.gautelis.raft.protocol.ReconfigurationStatusResponse;
import org.gautelis.raft.protocol.ReconfigureClusterRequest;
import org.gautelis.raft.protocol.ReconfigureClusterResponse;
import org.gautelis.raft.protocol.TelemetryRequest;
import org.gautelis.raft.protocol.TelemetryResponse;
import org.gautelis.raft.transport.RaftTransportClient;

import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Shared CLI support for operational cluster commands and telemetry views.
 */
public final class OperationalCliSupport {
    private static final Logger telemetryLog = LogManager.getLogger("TELEMETRY");
    private static final List<CommandSpec> COMMANDS = List.of(
            new CommandSpec(
                    "cluster-summary",
                    "  java -jar raft-dist/target/raft-1.0-SNAPSHOT.jar cluster-summary [--json] <target-port|target-host:port|id@target-host:port[/role]>",
                    OperationalCliCommands::runClusterSummary
            ),
            new CommandSpec(
                    "reconfiguration-status",
                    "  java -jar raft-dist/target/raft-1.0-SNAPSHOT.jar reconfiguration-status [--json] <target-port|target-host:port|id@target-host:port[/role]>",
                    OperationalCliCommands::runReconfigurationStatus
            ),
            new CommandSpec(
                    "telemetry",
                    "  java -jar raft-dist/target/raft-1.0-SNAPSHOT.jar telemetry [--json] [--summary] <target-port|target-host:port|id@target-host:port[/role]>",
                    OperationalCliCommands::runTelemetry
            ),
            new CommandSpec(
                    "join-request",
                    "  java -jar raft-dist/target/raft-1.0-SNAPSHOT.jar join-request <target-port|target-host:port|id@target-host:port[/role]> <joining-port|joining-host:port|id@joining-host:port[/role]>",
                    OperationalCliCommands::runJoinRequest
            ),
            new CommandSpec(
                    "join-status",
                    "  java -jar raft-dist/target/raft-1.0-SNAPSHOT.jar join-status <target-port|target-host:port|id@target-host:port[/role]> <joining-peer-id>",
                    OperationalCliCommands::runJoinStatus
            ),
            new CommandSpec(
                    "reconfigure",
                    "  java -jar raft-dist/target/raft-1.0-SNAPSHOT.jar reconfigure <joint|finalize|promote|demote> <target-port|target-host:port|id@target-host:port[/role]> [member-port|member-host:port|id@member-host:port[/role]]...",
                    OperationalCliCommands::runReconfigure
            )
    );

    private OperationalCliSupport() {
    }

    public static void printUsage(PrintStream err) {
        for (CommandSpec command : COMMANDS) {
            err.println(command.usageLine());
        }
    }

    public static boolean handleIfSupported(
            String[] args,
            CliRuntimeContext context
    ) {
        for (CommandSpec command : COMMANDS) {
            if (command.matches(args[0])) {
                command.handler().run(args, command.usageLine(), context);
                return true;
            }
        }
        return false;
    }

    public static boolean supportsCliCommand(String command) {
        for (CommandSpec spec : COMMANDS) {
            if (spec.matches(command)) {
                return true;
            }
        }
        return false;
    }

    public static void startDemoTelemetryLoop(
            Peer target,
            java.util.function.Function<String, RaftTransportClient> clientFactory,
            Supplier<String> authScheme,
            Supplier<String> authToken,
            RuntimeConfiguration configuration
    ) {
        int intervalSeconds = configuration == null ? 20 : configuration.demoTelemetryIntervalSeconds();
        int detailEvery = configuration == null ? 6 : configuration.demoTelemetryDetailEvery();
        if (intervalSeconds <= 0 || target == null || target.getAddress() == null) {
            return;
        }

        Thread thread = new Thread(() -> {
            RaftTransportClient client = clientFactory.apply("telemetry-demo");
            try {
                Thread.sleep(Math.max(2_000L, intervalSeconds * 1_000L));
                long sample = 0L;
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        TelemetryResponse response = TelemetryCliSupport.fetch(client, target, true, authScheme, authToken);
                        if (response.isSuccess()) {
                            sample++;
                            telemetryLog.info("{}", TelemetryCliSupport.renderHeadline(response));
                            if (sample == 1 || sample % detailEvery == 0) {
                                telemetryLog.info("\n{}", TelemetryCliSupport.render(response));
                            }
                        } else {
                            telemetryLog.info("telemetry status={} target={}", response.getStatus(), target.getId());
                        }
                    } catch (Exception e) {
                        telemetryLog.debug("telemetry pull failed for {}: {}", target.getId(), e.getMessage());
                    }
                    Thread.sleep(intervalSeconds * 1_000L);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                client.shutdown();
            }
        }, "raft-demo-telemetry-" + target.getId());
        thread.setDaemon(true);
        thread.start();
    }

    public static Peer parsePeerSpec(String spec, boolean local) {
        String trimmed = spec.trim();
        Peer.Role role = Peer.Role.VOTER;
        int slash = trimmed.lastIndexOf('/');
        if (slash > 0 && slash < trimmed.length() - 1) {
            role = Peer.Role.valueOf(trimmed.substring(slash + 1).trim().toUpperCase(Locale.ROOT));
            trimmed = trimmed.substring(0, slash).trim();
        }
        int at = trimmed.indexOf('@');
        String id = null;
        String endpoint = trimmed;
        if (at >= 0) {
            id = trimmed.substring(0, at).trim();
            endpoint = trimmed.substring(at + 1).trim();
        }

        String host = "localhost";
        String portSpec = endpoint;
        int colon = endpoint.lastIndexOf(':');
        if (colon > 0 && colon < endpoint.length() - 1) {
            host = endpoint.substring(0, colon).trim();
            portSpec = endpoint.substring(colon + 1).trim();
        }

        int port = Integer.parseInt(portSpec);
        if (id == null || id.isBlank()) {
            id = "server-" + port;
        }
        return new Peer(id, new InetSocketAddress(host, port), role);
    }

    static void failUsage(String usageLine) {
        System.err.println("Usage: " + usageLine.trim());
        System.exit(1);
    }

    static void failRequest(String message) {
        System.err.println(message);
        System.exit(2);
    }

    @FunctionalInterface
    private interface CommandHandler {
        void run(String[] args, String usageLine, CliRuntimeContext context);
    }

    private record CommandSpec(String name, String usageLine, CommandHandler handler) {
        private boolean matches(String command) {
            return name.equalsIgnoreCase(command);
        }
    }


    private static String renderTelemetryHeadline(TelemetryResponse response) {
        StringBuilder out = new StringBuilder();
        out.append("node=").append(response.getPeerId())
                .append(" state=").append(response.getState())
                .append(" term=").append(response.getTerm());
        if (!response.getLeaderId().isBlank()) {
            out.append(" leader=").append(response.getLeaderId());
        }
        out.append(" commit=").append(response.getCommitIndex())
                .append(" applied=").append(response.getLastApplied())
                .append(" health=").append(response.getClusterHealth())
                .append(" quorum=").append(response.isQuorumAvailable())
                .append(" currentQuorum=").append(response.isCurrentQuorumAvailable())
                .append(" nextQuorum=").append(response.isNextQuorumAvailable())
                .append(" healthyVoters=").append(response.getHealthyVotingMembers()).append('/').append(response.getVotingMembers());
        if (!response.getClusterStatusReason().isBlank()) {
            out.append(" reason=").append(response.getClusterStatusReason());
        }
        return out.toString();
    }

    static String renderTelemetry(TelemetryResponse response) {
        return TelemetryCliSupport.render(response);
    }

    static String renderReconfigureResponse(ReconfigureClusterRequest.Action action, ReconfigureClusterResponse response) {
        return "Reconfigure action=" + action.name()
                + " status=" + response.getStatus()
                + " success=" + response.isSuccess()
                + " peer=" + response.getPeerId()
                + (response.getLeaderId() == null || response.getLeaderId().isBlank() ? "" : " leader=" + response.getLeaderId())
                + " message=\"" + response.getMessage() + "\"";
    }

    static String renderJoinResponse(JoinClusterResponse response) {
        return "Join status=" + response.getStatus()
                + " success=" + response.isSuccess()
                + " peer=" + response.getPeerId()
                + (response.getLeaderId() == null || response.getLeaderId().isBlank() ? "" : " leader=" + response.getLeaderId())
                + " message=\"" + response.getMessage() + "\"";
    }

    static String renderJoinStatusResponse(JoinClusterStatusResponse response, String joiningPeerId) {
        return "JoinStatus target=" + joiningPeerId
                + " status=" + response.getStatus()
                + " success=" + response.isSuccess()
                + " peer=" + response.getPeerId()
                + (response.getLeaderId() == null || response.getLeaderId().isBlank() ? "" : " leader=" + response.getLeaderId())
                + " message=\"" + response.getMessage() + "\"";
    }

    static String renderTelemetryJson(TelemetryResponse response) {
        return TelemetryCliSupport.renderJson(response);
    }

    static String renderTelemetrySummaryJson(TelemetryResponse response) {
        return TelemetryCliSupport.renderSummaryJson(response);
    }

    static String renderClusterSummary(ClusterSummaryResponse response) {
        return ClusterAdminCliSupport.renderClusterSummary(response);
    }

    static String renderClusterSummaryJson(ClusterSummaryResponse response) {
        return ClusterAdminCliSupport.renderClusterSummaryJson(response);
    }

    static String renderReconfigurationStatus(ReconfigurationStatusResponse response) {
        return ClusterAdminCliSupport.renderReconfigurationStatus(response);
    }

    static String renderReconfigurationStatusJson(ReconfigurationStatusResponse response) {
        return ClusterAdminCliSupport.renderReconfigurationStatusJson(response);
    }

    static void appendClusterView(StringBuilder out, TelemetryResponse response) {
        if (!"LEADER".equals(response.getState())) {
            return;
        }

        Map<String, org.gautelis.raft.protocol.TelemetryPeerStats> statsByPeer = new HashMap<>();
        for (var stats : response.getPeerStats()) {
            if ("AppendEntriesRequest".equals(stats.rpcType())) {
                statsByPeer.put(stats.peerId(), stats);
            } else {
                statsByPeer.putIfAbsent(stats.peerId(), stats);
            }
        }

        out.append("Cluster view:\n");
        for (var member : response.getClusterMembers()) {
            var stats = statsByPeer.get(member.peerId());
            out.append("  ").append(member.peerId())
                    .append(" memberRole=").append(member.role().toLowerCase(Locale.ROOT))
                    .append(" currentRole=").append(member.currentRole().isBlank() ? "-" : member.currentRole().toLowerCase(Locale.ROOT))
                    .append(" nextRole=").append(member.nextRole().isBlank() ? "-" : member.nextRole().toLowerCase(Locale.ROOT))
                    .append(" transition=").append(member.roleTransition())
                    .append(" transitionAgeMillis=").append(member.transitionAgeMillis())
                    .append(" blockingQuorums=").append(member.blockingQuorums().isBlank() ? "-" : member.blockingQuorums())
                    .append(" blockingReason=").append(member.blockingReason().isBlank() ? "-" : member.blockingReason())
                    .append(" nodeRole=").append(member.local() ? "leader" : "peer")
                    .append(" current=").append(member.currentMember())
                    .append(" next=").append(member.nextMember())
                    .append(" health=").append(member.health())
                    .append(" reachable=").append(member.reachable())
                    .append(" freshness=").append(member.freshness())
                    .append(" failures=").append(member.consecutiveFailures())
                    .append(" match=").append(member.matchIndex())
                    .append(" nextIndex=").append(member.nextIndex())
                    .append(" lag=").append(member.lag());
            if (stats != null && stats.samples() > 0) {
                out.append(" mean=").append(String.format(Locale.ROOT, "%.3f", stats.meanMillis())).append("ms")
                        .append(" cv=").append(String.format(Locale.ROOT, "%.2f", stats.cvPercent())).append('%');
            }
            out.append('\n');
        }
    }

    static TelemetryResponse fetchTelemetry(
            RaftTransportClient client,
            Peer target,
            boolean includePeerStats,
            Supplier<String> authScheme,
            Supplier<String> authToken
    ) throws Exception {
        TelemetryResponse response = client.sendTelemetryRequest(
                target,
                new TelemetryRequest(0L, "telemetry-cli", includePeerStats, true, authScheme.get(), authToken.get())
        ).get(5, TimeUnit.SECONDS);
        if ("REDIRECT".equals(response.getStatus())) {
            Peer redirectedLeader = resolveRedirectLeader(response);
            if (redirectedLeader != null) {
                response = client.sendTelemetryRequest(
                        redirectedLeader,
                        new TelemetryRequest(0L, "telemetry-cli", includePeerStats, true, authScheme.get(), authToken.get())
                ).get(5, TimeUnit.SECONDS);
            }
        }
        return response;
    }

    static ClusterSummaryResponse fetchClusterSummary(RaftTransportClient client, Peer target, Supplier<String> authScheme, Supplier<String> authToken) throws Exception {
        ClusterSummaryResponse response = client.sendClusterSummaryRequest(
                target,
                new ClusterSummaryRequest(0L, "cluster-summary-cli", authScheme.get(), authToken.get())
        ).get(5, TimeUnit.SECONDS);
        if ("REDIRECT".equals(response.getStatus()) && !response.getRedirectLeaderHost().isBlank() && response.getRedirectLeaderPort() > 0) {
            response = client.sendClusterSummaryRequest(
                    new Peer(response.getRedirectLeaderId(), new InetSocketAddress(response.getRedirectLeaderHost(), response.getRedirectLeaderPort())),
                    new ClusterSummaryRequest(0L, "cluster-summary-cli", authScheme.get(), authToken.get())
            ).get(5, TimeUnit.SECONDS);
        }
        return response;
    }

    static ReconfigurationStatusResponse fetchReconfigurationStatus(RaftTransportClient client, Peer target, Supplier<String> authScheme, Supplier<String> authToken) throws Exception {
        ReconfigurationStatusResponse response = client.sendReconfigurationStatusRequest(
                target,
                new ReconfigurationStatusRequest(0L, "reconfiguration-status-cli", authScheme.get(), authToken.get())
        ).get(5, TimeUnit.SECONDS);
        if ("REDIRECT".equals(response.getStatus()) && !response.getRedirectLeaderHost().isBlank() && response.getRedirectLeaderPort() > 0) {
            response = client.sendReconfigurationStatusRequest(
                    new Peer(response.getRedirectLeaderId(), new InetSocketAddress(response.getRedirectLeaderHost(), response.getRedirectLeaderPort())),
                    new ReconfigurationStatusRequest(0L, "reconfiguration-status-cli", authScheme.get(), authToken.get())
            ).get(5, TimeUnit.SECONDS);
        }
        return response;
    }

    private static Peer resolveRedirectLeader(TelemetryResponse response) {
        if (response.getRedirectLeaderId().isBlank()) {
            return null;
        }
        for (Peer peer : response.getKnownPeers()) {
            if (response.getRedirectLeaderId().equals(peer.getId())) {
                return peer;
            }
        }
        return null;
    }

    static String renderPeers(List<Peer> peers) {
        if (peers == null || peers.isEmpty()) {
            return "[]";
        }
        List<String> rendered = new ArrayList<>();
        for (Peer peer : peers) {
            if (peer.getAddress() == null) {
                rendered.add(peer.getId() + roleSuffix(peer));
            } else {
                rendered.add(peer.getId() + "@" + peer.getAddress().getHostString() + ":" + peer.getAddress().getPort() + roleSuffix(peer));
            }
        }
        return rendered.toString();
    }

    private static String roleSuffix(Peer peer) {
        return peer.getRole() == Peer.Role.VOTER ? "" : "/" + peer.getRole().name().toLowerCase(Locale.ROOT);
    }

    static String describeFreshness(long observedAtMillis, long lastContactMillis) {
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

    static String describeHealth(boolean local, boolean reachable, String freshness, int consecutiveFailures) {
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

}
