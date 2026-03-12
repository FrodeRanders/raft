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
import org.gautelis.raft.protocol.StateMachineQuery;
import org.gautelis.raft.protocol.StateMachineQueryResult;
import org.gautelis.raft.protocol.TelemetryRequest;
import org.gautelis.raft.protocol.TelemetryResponse;
import org.gautelis.raft.transport.netty.RaftClient;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class Application {
    private static final Logger log = LogManager.getLogger(Application.class);
    private static final Logger telemetryLog = LogManager.getLogger("TELEMETRY");

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage:");
            System.err.println("  Peer specs may use an optional /learner or /voter suffix, for example id@host:port/learner");
            System.err.println("  java -jar target/raft.jar <my-port|my-host:port|id@my-host:port[/role]> <peer-port|peer-host:port|id@peer-host:port[/role]> ...");
            System.err.println("  java -jar target/raft.jar join <my-port|my-host:port|id@my-host:port[/role]> <seed-port|seed-host:port|id@seed-host:port[/role]>");
            System.err.println("  java -jar target/raft.jar command <put|delete|clear> <target-port|target-host:port|id@target-host:port[/role]> [key] [value]");
            System.err.println("  java -jar target/raft.jar query get <target-port|target-host:port|id@target-host:port[/role]> <key>");
            System.err.println("  java -jar target/raft.jar cluster-summary [--json] <target-port|target-host:port|id@target-host:port[/role]>");
            System.err.println("  java -jar target/raft.jar reconfiguration-status [--json] <target-port|target-host:port|id@target-host:port[/role]>");
            System.err.println("  java -jar target/raft.jar telemetry [--json] [--summary] <target-port|target-host:port|id@target-host:port[/role]>");
            System.err.println("  java -jar target/raft.jar join-request <target-port|target-host:port|id@target-host:port[/role]> <joining-port|joining-host:port|id@joining-host:port[/role]>");
            System.err.println("  java -jar target/raft.jar join-status <target-port|target-host:port|id@target-host:port[/role]> <joining-peer-id>");
            System.err.println("  java -jar target/raft.jar reconfigure <joint|finalize|promote|demote> <target-port|target-host:port|id@target-host:port[/role]> [member-port|member-host:port|id@member-host:port[/role]]...");
            System.exit(1);
        }

        if ("telemetry".equalsIgnoreCase(args[0])) {
            runTelemetry(args);
            return;
        }
        if ("command".equalsIgnoreCase(args[0])) {
            runCommand(args);
            return;
        }
        if ("query".equalsIgnoreCase(args[0])) {
            runQuery(args);
            return;
        }
        if ("cluster-summary".equalsIgnoreCase(args[0])) {
            runClusterSummary(args);
            return;
        }
        if ("reconfiguration-status".equalsIgnoreCase(args[0])) {
            runReconfigurationStatus(args);
            return;
        }
        if ("join-request".equalsIgnoreCase(args[0])) {
            runJoinRequest(args);
            return;
        }
        if ("join-status".equalsIgnoreCase(args[0])) {
            runJoinStatus(args);
            return;
        }
        if ("reconfigure".equalsIgnoreCase(args[0])) {
            runReconfigure(args);
            return;
        }

        boolean joinMode = "join".equalsIgnoreCase(args[0]);
        int offset = joinMode ? 1 : 0;
        if (args.length - offset < 2) {
            System.err.println("Not enough arguments for requested mode");
            System.exit(1);
        }

        Peer me = parsePeerSpec(args[offset], true);
        List<Peer> peers = new ArrayList<>();
        for (int i = offset + 1; i < args.length; i++) {
            peers.add(parsePeerSpec(args[i], false));
        }

        Peer joinSeed = joinMode ? peers.getFirst() : null;
        if (joinMode) {
            log.info("Starting Raft node {} in join mode via seed {}", me, joinSeed);
        } else {
            log.info("Starting Raft cluster with {} initial members; runtime configuration changes are supported through replicated config commands", peers.size() + 1);
        }

        long timeoutMillis = 2000;

        startDemoTelemetryLoop(me);
        BasicAdapter adapter = new BasicAdapter(timeoutMillis, me, peers, joinSeed);
        adapter.start();
    }

    private static void runTelemetry(String[] args) {
        boolean json = false;
        boolean summary = false;
        int targetIndex = -1;
        for (int i = 1; i < args.length; i++) {
            if ("--json".equalsIgnoreCase(args[i])) {
                json = true;
            } else if ("--summary".equalsIgnoreCase(args[i])) {
                summary = true;
            } else {
                targetIndex = i;
                break;
            }
        }
        if (summary && !json) {
            json = true;
        }
        if (args.length != targetIndex + 1) {
            System.err.println("Usage: java -jar target/raft.jar telemetry [--json] [--summary] <target-port|target-host:port|id@target-host:port>");
            System.exit(1);
        }

        Peer target = parsePeerSpec(args[targetIndex], false);
        RaftClient client = new RaftClient("telemetry-cli", null);
        try {
            TelemetryResponse response = fetchTelemetry(client, target, true);
            if (!response.isSuccess()) {
                System.err.println("Telemetry request failed for " + target.getId() + " status=" + response.getStatus());
                System.exit(2);
            }
            System.out.println(summary ? renderTelemetrySummaryJson(response) : (json ? renderTelemetryJson(response) : renderTelemetry(response)));
        } catch (Exception e) {
            System.err.println("Telemetry request failed: " + e.getMessage());
            System.exit(2);
        } finally {
            client.shutdown();
        }
    }

    private static void runClusterSummary(String[] args) {
        boolean json = false;
        int targetIndex = -1;
        for (int i = 1; i < args.length; i++) {
            if ("--json".equalsIgnoreCase(args[i])) {
                json = true;
            } else {
                targetIndex = i;
                break;
            }
        }
        if (args.length != targetIndex + 1) {
            System.err.println("Usage: java -jar target/raft.jar cluster-summary [--json] <target-port|target-host:port|id@target-host:port>");
            System.exit(1);
        }

        Peer target = parsePeerSpec(args[targetIndex], false);
        RaftClient client = new RaftClient("cluster-summary-cli", null);
        try {
            ClusterSummaryResponse response = fetchClusterSummary(client, target);
            if (!response.isSuccess()) {
                System.err.println("Cluster summary request failed for " + target.getId() + " status=" + response.getStatus());
                System.exit(2);
            }
            System.out.println(json ? renderClusterSummaryJson(response) : renderClusterSummary(response));
        } catch (Exception e) {
            System.err.println("Cluster summary request failed: " + e.getMessage());
            System.exit(2);
        } finally {
            client.shutdown();
        }
    }

    private static void runReconfigurationStatus(String[] args) {
        boolean json = false;
        int targetIndex = -1;
        for (int i = 1; i < args.length; i++) {
            if ("--json".equalsIgnoreCase(args[i])) {
                json = true;
            } else {
                targetIndex = i;
                break;
            }
        }
        if (args.length != targetIndex + 1) {
            System.err.println("Usage: java -jar target/raft.jar reconfiguration-status [--json] <target-port|target-host:port|id@target-host:port>");
            System.exit(1);
        }

        Peer target = parsePeerSpec(args[targetIndex], false);
        RaftClient client = new RaftClient("reconfiguration-status-cli", null);
        try {
            ReconfigurationStatusResponse response = fetchReconfigurationStatus(client, target);
            if (!response.isSuccess()) {
                System.err.println("Reconfiguration status request failed for " + target.getId() + " status=" + response.getStatus());
                System.exit(2);
            }
            System.out.println(json ? renderReconfigurationStatusJson(response) : renderReconfigurationStatus(response));
        } catch (Exception e) {
            System.err.println("Reconfiguration status request failed: " + e.getMessage());
            System.exit(2);
        } finally {
            client.shutdown();
        }
    }

    private static void runCommand(String[] args) {
        if (args.length < 3) {
            System.err.println("Usage: java -jar target/raft.jar command <put|delete|clear> <target-port|target-host:port|id@target-host:port> [key] [value]");
            System.exit(1);
        }

        String action = args[1].trim().toLowerCase(Locale.ROOT);
        Peer target = parsePeerSpec(args[2], false);
        byte[] command;
        switch (action) {
            case "put" -> {
                if (args.length != 5) {
                    System.err.println("Usage: java -jar target/raft.jar command put <target> <key> <value>");
                    System.exit(1);
                    return;
                }
                command = org.gautelis.raft.protocol.StateMachineCommand.put(args[3], args[4]).encode();
            }
            case "delete" -> {
                if (args.length != 4) {
                    System.err.println("Usage: java -jar target/raft.jar command delete <target> <key>");
                    System.exit(1);
                    return;
                }
                command = org.gautelis.raft.protocol.StateMachineCommand.delete(args[3]).encode();
            }
            case "clear" -> {
                if (args.length != 3) {
                    System.err.println("Usage: java -jar target/raft.jar command clear <target>");
                    System.exit(1);
                    return;
                }
                command = org.gautelis.raft.protocol.StateMachineCommand.clear().encode();
            }
            default -> {
                System.err.println("Unknown command action: " + args[1]);
                System.exit(1);
                return;
            }
        }

        RaftClient client = new RaftClient("command-cli", null);
        try {
            ClientCommandRequest request = new ClientCommandRequest(0L, "command-cli", command);
            ClientCommandResponse response = client.sendClientCommandRequest(target, request).get(5, TimeUnit.SECONDS);
            if ("REDIRECT".equals(response.getStatus()) && !response.getLeaderHost().isBlank() && response.getLeaderPort() > 0) {
                Peer leader = new Peer(response.getLeaderId(), new InetSocketAddress(response.getLeaderHost(), response.getLeaderPort()));
                response = client.sendClientCommandRequest(leader, request).get(5, TimeUnit.SECONDS);
            }
            System.out.println(renderClientCommandResponse(action, response));
            if (!response.isSuccess()) {
                System.exit(2);
            }
        } catch (Exception e) {
            System.err.println("Command request failed: " + e.getMessage());
            System.exit(2);
        } finally {
            client.shutdown();
        }
    }

    private static void runQuery(String[] args) {
        if (args.length != 4 || !"get".equalsIgnoreCase(args[1])) {
            System.err.println("Usage: java -jar target/raft.jar query get <target-port|target-host:port|id@target-host:port> <key>");
            System.exit(1);
        }

        Peer target = parsePeerSpec(args[2], false);
        String key = args[3].trim();
        if (key.isBlank()) {
            System.err.println("Query key must not be blank");
            System.exit(1);
        }

        RaftClient client = new RaftClient("query-cli", null);
        try {
            ClientQueryRequest request = new ClientQueryRequest(0L, "query-cli", StateMachineQuery.get(key).encode());
            ClientQueryResponse response = client.sendClientQueryRequest(target, request).get(5, TimeUnit.SECONDS);
            if ("REDIRECT".equals(response.getStatus()) && !response.getLeaderHost().isBlank() && response.getLeaderPort() > 0) {
                Peer leader = new Peer(response.getLeaderId(), new InetSocketAddress(response.getLeaderHost(), response.getLeaderPort()));
                response = client.sendClientQueryRequest(leader, request).get(5, TimeUnit.SECONDS);
            }
            System.out.println(renderClientQueryResponse(response));
            if (!response.isSuccess()) {
                System.exit(2);
            }
        } catch (Exception e) {
            System.err.println("Query request failed: " + e.getMessage());
            System.exit(2);
        } finally {
            client.shutdown();
        }
    }

    private static void runReconfigure(String[] args) {
        if (args.length < 3) {
            System.err.println("Usage: java -jar target/raft.jar reconfigure <joint|finalize|promote|demote> <target-port|target-host:port|id@target-host:port> [member-port|member-host:port|id@member-host:port]...");
            System.exit(1);
        }

        ReconfigureClusterRequest.Action action;
        try {
            action = ReconfigureClusterRequest.Action.valueOf(args[1].trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            System.err.println("Unknown reconfigure action: " + args[1]);
            System.exit(1);
            return;
        }

        Peer target = parsePeerSpec(args[2], false);
        List<Peer> members = new ArrayList<>();
        if (action == ReconfigureClusterRequest.Action.JOINT) {
            if (args.length < 4) {
                System.err.println("Joint reconfiguration requires at least one member");
                System.exit(1);
            }
            for (int i = 3; i < args.length; i++) {
                members.add(parsePeerSpec(args[i], false));
            }
        } else if (action == ReconfigureClusterRequest.Action.FINALIZE) {
            if (args.length != 3) {
                System.err.println("Finalize reconfiguration does not take member arguments");
                System.exit(1);
            }
        } else {
            if (args.length != 4) {
                System.err.println(action.name().toLowerCase(Locale.ROOT) + " reconfiguration requires exactly one member");
                System.exit(1);
            }
            members.add(parsePeerSpec(args[3], false));
        }

        RaftClient client = new RaftClient("reconfigure-cli", null);
        try {
            ReconfigureClusterResponse response = client.sendReconfigureClusterRequest(
                    target,
                    new ReconfigureClusterRequest(0L, "reconfigure-cli", action, members)
            ).get(5, TimeUnit.SECONDS);
            System.out.println(renderReconfigureResponse(action, response));
            if (!response.isSuccess()) {
                System.exit(2);
            }
        } catch (Exception e) {
            System.err.println("Reconfigure request failed: " + e.getMessage());
            System.exit(2);
        } finally {
            client.shutdown();
        }
    }

    private static void runJoinRequest(String[] args) {
        if (args.length != 3) {
            System.err.println("Usage: java -jar target/raft.jar join-request <target-port|target-host:port|id@target-host:port> <joining-port|joining-host:port|id@joining-host:port>");
            System.exit(1);
        }

        Peer target = parsePeerSpec(args[1], false);
        Peer joining = parsePeerSpec(args[2], false);
        RaftClient client = new RaftClient("join-cli", null);
        try {
            JoinClusterResponse response = client.sendJoinClusterRequest(
                    target,
                    new JoinClusterRequest(0L, "join-cli", joining)
            ).get(5, TimeUnit.SECONDS);
            System.out.println(renderJoinResponse(response));
            if (!response.isSuccess()) {
                System.exit(2);
            }
        } catch (Exception e) {
            System.err.println("Join request failed: " + e.getMessage());
            System.exit(2);
        } finally {
            client.shutdown();
        }
    }

    private static void runJoinStatus(String[] args) {
        if (args.length != 3) {
            System.err.println("Usage: java -jar target/raft.jar join-status <target-port|target-host:port|id@target-host:port> <joining-peer-id>");
            System.exit(1);
        }

        Peer target = parsePeerSpec(args[1], false);
        String joiningPeerId = args[2].trim();
        RaftClient client = new RaftClient("join-status-cli", null);
        try {
            JoinClusterStatusResponse response = client.sendJoinClusterStatusRequest(
                    target,
                    new JoinClusterStatusRequest(0L, "join-status-cli", joiningPeerId)
            ).get(5, TimeUnit.SECONDS);
            System.out.println(renderJoinStatusResponse(response, joiningPeerId));
            if (!response.isSuccess()) {
                System.exit(2);
            }
        } catch (Exception e) {
            System.err.println("Join status request failed: " + e.getMessage());
            System.exit(2);
        } finally {
            client.shutdown();
        }
    }

    private static void startDemoTelemetryLoop(Peer target) {
        int intervalSeconds = Integer.getInteger("raft.demo.telemetry.interval.seconds", 20);
        int detailEvery = Math.max(1, Integer.getInteger("raft.demo.telemetry.detail.every", 6));
        if (intervalSeconds <= 0 || target == null || target.getAddress() == null) {
            return;
        }

        Thread thread = new Thread(() -> {
            RaftClient client = new RaftClient("telemetry-demo", null);
            try {
                Thread.sleep(Math.max(2_000L, intervalSeconds * 1_000L));
                long sample = 0L;
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        TelemetryResponse response = fetchTelemetry(client, target, true);
                        if (response.isSuccess()) {
                            sample++;
                            telemetryLog.info("{}", renderTelemetryHeadline(response));
                            if (sample == 1 || sample % detailEvery == 0) {
                                telemetryLog.info("\n{}", renderTelemetry(response));
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

    private static Peer parsePeerSpec(String spec, boolean local) {
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

    private static String renderTelemetry(TelemetryResponse response) {
        StringBuilder out = new StringBuilder();
        out.append("Node ").append(response.getPeerId())
                .append(" status=").append(response.getStatus())
                .append(" state=").append(response.getState())
                .append(" term=").append(response.getTerm());
        if (!response.getLeaderId().isBlank()) {
            out.append(" leader=").append(response.getLeaderId());
        }
        out.append('\n');
        out.append("Flags: joining=").append(response.isJoining())
                .append(" decommissioned=").append(response.isDecommissioned());
        if (!response.getVotedFor().isBlank()) {
            out.append(" votedFor=").append(response.getVotedFor());
        }
        out.append('\n');
        out.append("Log: commit=").append(response.getCommitIndex())
                .append(" applied=").append(response.getLastApplied())
                .append(" lastIndex=").append(response.getLastLogIndex())
                .append(" lastTerm=").append(response.getLastLogTerm())
                .append(" snapshot=").append(response.getSnapshotIndex()).append('@').append(response.getSnapshotTerm())
                .append('\n');
        out.append("Config: joint=").append(response.isJointConsensus())
                .append(" current=").append(renderPeers(response.getCurrentMembers()))
                .append(" next=").append(renderPeers(response.getNextMembers()))
                .append('\n');
        out.append("Peers: known=").append(renderPeers(response.getKnownPeers()))
                .append(" pendingJoins=").append(response.getPendingJoinIds())
                .append('\n');
        if (!response.getClusterHealth().isBlank()) {
            out.append("Cluster: health=").append(response.getClusterHealth())
                    .append(" reason=").append(response.getClusterStatusReason())
                    .append(" quorum=").append(response.isQuorumAvailable())
                    .append(" currentQuorum=").append(response.isCurrentQuorumAvailable())
                    .append(" nextQuorum=").append(response.isNextQuorumAvailable())
                    .append(" healthyVoters=").append(response.getHealthyVotingMembers()).append('/').append(response.getVotingMembers())
                    .append(" reachableVoters=").append(response.getReachableVotingMembers()).append('/').append(response.getVotingMembers());
            if (!response.getBlockingCurrentQuorumPeerIds().isEmpty()) {
                out.append(" blockingCurrent=").append(response.getBlockingCurrentQuorumPeerIds());
            }
            if (!response.getBlockingNextQuorumPeerIds().isEmpty()) {
                out.append(" blockingNext=").append(response.getBlockingNextQuorumPeerIds());
            }
            out
                    .append('\n');
        }
        if (!response.getReplication().isEmpty()) {
            out.append("Replication:\n");
            for (var status : response.getReplication()) {
                out.append("  ").append(status.peerId())
                        .append(" next=").append(status.nextIndex())
                        .append(" match=").append(status.matchIndex())
                        .append(" failures=").append(status.consecutiveFailures())
                        .append('\n');
            }
        }
        appendClusterView(out, response);
        if (!response.getPeerStats().isEmpty()) {
            out.append("Transport stats:\n");
            for (var stats : response.getPeerStats()) {
                out.append("  ").append(stats.peerId())
                        .append(" n=").append(stats.samples())
                        .append(" mean=").append(String.format(Locale.ROOT, "%.3f", stats.meanMillis())).append("ms")
                        .append(" min=").append(String.format(Locale.ROOT, "%.3f", stats.minMillis())).append("ms")
                        .append(" max=").append(String.format(Locale.ROOT, "%.3f", stats.maxMillis())).append("ms")
                        .append(" cv=").append(String.format(Locale.ROOT, "%.2f", stats.cvPercent())).append('%')
                        .append('\n');
            }
        }
        return out.toString().trim();
    }

    private static String renderReconfigureResponse(ReconfigureClusterRequest.Action action, ReconfigureClusterResponse response) {
        return "Reconfigure action=" + action.name()
                + " status=" + response.getStatus()
                + " success=" + response.isSuccess()
                + " peer=" + response.getPeerId()
                + (response.getLeaderId() == null || response.getLeaderId().isBlank() ? "" : " leader=" + response.getLeaderId())
                + " message=\"" + response.getMessage() + "\"";
    }

    private static String renderClientCommandResponse(String action, ClientCommandResponse response) {
        return "Command action=" + action
                + " status=" + response.getStatus()
                + " success=" + response.isSuccess()
                + " peer=" + response.getPeerId()
                + (response.getLeaderId() == null || response.getLeaderId().isBlank() ? "" : " leader=" + response.getLeaderId())
                + (response.getLeaderHost() == null || response.getLeaderHost().isBlank() || response.getLeaderPort() <= 0
                ? ""
                : " leaderEndpoint=" + response.getLeaderHost() + ":" + response.getLeaderPort())
                + " message=\"" + response.getMessage() + "\"";
    }

    private static String renderClientQueryResponse(ClientQueryResponse response) {
        StringBuilder out = new StringBuilder();
        out.append("Query status=").append(response.getStatus())
                .append(" success=").append(response.isSuccess())
                .append(" peer=").append(response.getPeerId());
        if (response.getLeaderId() != null && !response.getLeaderId().isBlank()) {
            out.append(" leader=").append(response.getLeaderId());
        }
        if (response.getLeaderHost() != null && !response.getLeaderHost().isBlank() && response.getLeaderPort() > 0) {
            out.append(" leaderEndpoint=").append(response.getLeaderHost()).append(':').append(response.getLeaderPort());
        }
        out.append(" message=\"").append(response.getMessage()).append('"');
        if (response.isSuccess()) {
            StateMachineQueryResult.decode(response.getResult()).ifPresent(result -> {
                if (result.getType() == StateMachineQueryResult.Type.GET) {
                    out.append(" key=").append(result.getKey())
                            .append(" found=").append(result.isFound());
                    if (result.isFound()) {
                        out.append(" value=\"").append(result.getValue()).append('"');
                    }
                }
            });
        }
        return out.toString();
    }

    private static String renderJoinResponse(JoinClusterResponse response) {
        return "Join status=" + response.getStatus()
                + " success=" + response.isSuccess()
                + " peer=" + response.getPeerId()
                + (response.getLeaderId() == null || response.getLeaderId().isBlank() ? "" : " leader=" + response.getLeaderId())
                + " message=\"" + response.getMessage() + "\"";
    }

    private static String renderJoinStatusResponse(JoinClusterStatusResponse response, String joiningPeerId) {
        return "JoinStatus target=" + joiningPeerId
                + " status=" + response.getStatus()
                + " success=" + response.isSuccess()
                + " peer=" + response.getPeerId()
                + (response.getLeaderId() == null || response.getLeaderId().isBlank() ? "" : " leader=" + response.getLeaderId())
                + " message=\"" + response.getMessage() + "\"";
    }

    private static String renderTelemetryJson(TelemetryResponse response) {
        StringBuilder out = new StringBuilder();
        out.append("{\n");
        appendJsonField(out, "observedAtMillis", response.getObservedAtMillis(), true, 1);
        appendJsonField(out, "term", response.getTerm(), true, 1);
        appendJsonField(out, "peerId", response.getPeerId(), true, 1);
        appendJsonField(out, "success", response.isSuccess(), true, 1);
        appendJsonField(out, "status", response.getStatus(), true, 1);
        appendJsonField(out, "redirectLeaderId", response.getRedirectLeaderId(), true, 1);
        appendJsonField(out, "state", response.getState(), true, 1);
        appendJsonField(out, "leaderId", response.getLeaderId(), true, 1);
        appendJsonField(out, "votedFor", response.getVotedFor(), true, 1);
        appendJsonField(out, "joining", response.isJoining(), true, 1);
        appendJsonField(out, "decommissioned", response.isDecommissioned(), true, 1);
        appendJsonField(out, "commitIndex", response.getCommitIndex(), true, 1);
        appendJsonField(out, "lastApplied", response.getLastApplied(), true, 1);
        appendJsonField(out, "lastLogIndex", response.getLastLogIndex(), true, 1);
        appendJsonField(out, "lastLogTerm", response.getLastLogTerm(), true, 1);
        appendJsonField(out, "snapshotIndex", response.getSnapshotIndex(), true, 1);
        appendJsonField(out, "snapshotTerm", response.getSnapshotTerm(), true, 1);
        appendJsonField(out, "lastHeartbeatMillis", response.getLastHeartbeatMillis(), true, 1);
        appendJsonField(out, "nextElectionDeadlineMillis", response.getNextElectionDeadlineMillis(), true, 1);
        appendJsonField(out, "jointConsensus", response.isJointConsensus(), true, 1);
        appendJsonField(out, "clusterHealth", response.getClusterHealth(), true, 1);
        appendJsonField(out, "clusterStatusReason", response.getClusterStatusReason(), true, 1);
        appendJsonField(out, "quorumAvailable", response.isQuorumAvailable(), true, 1);
        appendJsonField(out, "currentQuorumAvailable", response.isCurrentQuorumAvailable(), true, 1);
        appendJsonField(out, "nextQuorumAvailable", response.isNextQuorumAvailable(), true, 1);
        appendJsonField(out, "votingMembers", response.getVotingMembers(), true, 1);
        appendJsonField(out, "healthyVotingMembers", response.getHealthyVotingMembers(), true, 1);
        appendJsonField(out, "reachableVotingMembers", response.getReachableVotingMembers(), true, 1);
        appendJsonField(out, "reconfigurationAgeMillis", response.getReconfigurationAgeMillis(), true, 1);
        appendJsonPeerArray(out, "currentMembers", response.getCurrentMembers(), true, 1);
        appendJsonPeerArray(out, "nextMembers", response.getNextMembers(), true, 1);
        appendJsonPeerArray(out, "knownPeers", response.getKnownPeers(), true, 1);
        appendJsonStringArray(out, "pendingJoinIds", response.getPendingJoinIds(), true, 1);
        appendJsonStringArray(out, "blockingCurrentQuorumPeerIds", response.getBlockingCurrentQuorumPeerIds(), true, 1);
        appendJsonStringArray(out, "blockingNextQuorumPeerIds", response.getBlockingNextQuorumPeerIds(), true, 1);
        appendJsonReplicationArray(out, "replication", response, true, 1);
        appendJsonPeerStatsArray(out, "peerStats", response, true, 1);
        appendJsonClusterMembers(out, "clusterMembers", response.getClusterMembers(), false, 1);
        out.append("}");
        return out.toString();
    }

    private static String renderTelemetrySummaryJson(TelemetryResponse response) {
        StringBuilder out = new StringBuilder();
        out.append("{\n");
        appendJsonField(out, "observedAtMillis", response.getObservedAtMillis(), true, 1);
        appendJsonField(out, "peerId", response.getPeerId(), true, 1);
        appendJsonField(out, "state", response.getState(), true, 1);
        appendJsonField(out, "leaderId", response.getLeaderId(), true, 1);
        appendJsonField(out, "term", response.getTerm(), true, 1);
        appendJsonField(out, "jointConsensus", response.isJointConsensus(), true, 1);
        appendJsonField(out, "clusterHealth", response.getClusterHealth(), true, 1);
        appendJsonField(out, "clusterStatusReason", response.getClusterStatusReason(), true, 1);
        appendJsonField(out, "quorumAvailable", response.isQuorumAvailable(), true, 1);
        appendJsonField(out, "currentQuorumAvailable", response.isCurrentQuorumAvailable(), true, 1);
        appendJsonField(out, "nextQuorumAvailable", response.isNextQuorumAvailable(), true, 1);
        appendJsonField(out, "votingMembers", response.getVotingMembers(), true, 1);
        appendJsonField(out, "healthyVotingMembers", response.getHealthyVotingMembers(), true, 1);
        appendJsonField(out, "reachableVotingMembers", response.getReachableVotingMembers(), true, 1);
        appendJsonField(out, "reconfigurationAgeMillis", response.getReconfigurationAgeMillis(), true, 1);
        appendJsonStringArray(out, "blockingCurrentQuorumPeerIds", response.getBlockingCurrentQuorumPeerIds(), true, 1);
        appendJsonStringArray(out, "blockingNextQuorumPeerIds", response.getBlockingNextQuorumPeerIds(), false, 1);
        out.append("}");
        return out.toString();
    }

    private static String renderClusterSummary(ClusterSummaryResponse response) {
        StringBuilder out = new StringBuilder();
        out.append("ClusterSummary peer=").append(response.getPeerId())
                .append(" status=").append(response.getStatus())
                .append(" state=").append(response.getState())
                .append(" term=").append(response.getTerm());
        if (!response.getLeaderId().isBlank()) {
            out.append(" leader=").append(response.getLeaderId());
        }
        out.append('\n');
        out.append("Cluster: health=").append(response.getClusterHealth())
                .append(" reason=").append(response.getClusterStatusReason())
                .append(" quorum=").append(response.isQuorumAvailable())
                .append(" currentQuorum=").append(response.isCurrentQuorumAvailable())
                .append(" nextQuorum=").append(response.isNextQuorumAvailable())
                .append(" reconfigAgeMillis=").append(response.getReconfigurationAgeMillis())
                .append(" healthyVoters=").append(response.getHealthyVotingMembers()).append('/').append(response.getVotingMembers())
                .append(" reachableVoters=").append(response.getReachableVotingMembers()).append('/').append(response.getVotingMembers());
        if (!response.getBlockingCurrentQuorumPeerIds().isEmpty()) {
            out.append(" blockingCurrent=").append(response.getBlockingCurrentQuorumPeerIds());
        }
        if (!response.getBlockingNextQuorumPeerIds().isEmpty()) {
            out.append(" blockingNext=").append(response.getBlockingNextQuorumPeerIds());
        }
        out.append('\n');
        out.append("Members:\n");
        for (var member : response.getMembers()) {
            out.append("  ").append(member.peerId())
                    .append(" role=").append(member.role().toLowerCase(Locale.ROOT))
                    .append(" currentRole=").append(member.currentRole().isBlank() ? "-" : member.currentRole().toLowerCase(Locale.ROOT))
                    .append(" nextRole=").append(member.nextRole().isBlank() ? "-" : member.nextRole().toLowerCase(Locale.ROOT))
                    .append(" transition=").append(member.roleTransition())
                    .append(" transitionAgeMillis=").append(member.transitionAgeMillis())
                    .append(" blockingQuorums=").append(member.blockingQuorums().isBlank() ? "-" : member.blockingQuorums())
                    .append(" blockingReason=").append(member.blockingReason().isBlank() ? "-" : member.blockingReason())
                    .append(" voting=").append(member.voting())
                    .append(" local=").append(member.local())
                    .append(" current=").append(member.currentMember())
                    .append(" next=").append(member.nextMember())
                    .append(" health=").append(member.health())
                    .append(" reachable=").append(member.reachable())
                    .append(" freshness=").append(member.freshness())
                    .append(" failures=").append(member.consecutiveFailures())
                    .append(" match=").append(member.matchIndex())
                    .append(" nextIndex=").append(member.nextIndex())
                    .append(" lag=").append(member.lag())
                    .append('\n');
        }
        return out.toString().trim();
    }

    private static String renderClusterSummaryJson(ClusterSummaryResponse response) {
        StringBuilder out = new StringBuilder();
        out.append("{\n");
        appendJsonField(out, "observedAtMillis", response.getObservedAtMillis(), true, 1);
        appendJsonField(out, "peerId", response.getPeerId(), true, 1);
        appendJsonField(out, "success", response.isSuccess(), true, 1);
        appendJsonField(out, "status", response.getStatus(), true, 1);
        appendJsonField(out, "redirectLeaderId", response.getRedirectLeaderId(), true, 1);
        appendJsonField(out, "redirectLeaderHost", response.getRedirectLeaderHost(), true, 1);
        appendJsonField(out, "redirectLeaderPort", response.getRedirectLeaderPort(), true, 1);
        appendJsonField(out, "state", response.getState(), true, 1);
        appendJsonField(out, "leaderId", response.getLeaderId(), true, 1);
        appendJsonField(out, "term", response.getTerm(), true, 1);
        appendJsonField(out, "jointConsensus", response.isJointConsensus(), true, 1);
        appendJsonField(out, "clusterHealth", response.getClusterHealth(), true, 1);
        appendJsonField(out, "clusterStatusReason", response.getClusterStatusReason(), true, 1);
        appendJsonField(out, "quorumAvailable", response.isQuorumAvailable(), true, 1);
        appendJsonField(out, "currentQuorumAvailable", response.isCurrentQuorumAvailable(), true, 1);
        appendJsonField(out, "nextQuorumAvailable", response.isNextQuorumAvailable(), true, 1);
        appendJsonField(out, "votingMembers", response.getVotingMembers(), true, 1);
        appendJsonField(out, "healthyVotingMembers", response.getHealthyVotingMembers(), true, 1);
        appendJsonField(out, "reachableVotingMembers", response.getReachableVotingMembers(), true, 1);
        appendJsonStringArray(out, "blockingCurrentQuorumPeerIds", response.getBlockingCurrentQuorumPeerIds(), true, 1);
        appendJsonStringArray(out, "blockingNextQuorumPeerIds", response.getBlockingNextQuorumPeerIds(), true, 1);
        appendJsonClusterMembers(out, "members", response.getMembers(), false, 1);
        out.append("}");
        return out.toString();
    }

    private static String renderReconfigurationStatus(ReconfigurationStatusResponse response) {
        StringBuilder out = new StringBuilder();
        out.append("ReconfigurationStatus peer=").append(response.getPeerId())
                .append(" status=").append(response.getStatus())
                .append(" state=").append(response.getState())
                .append(" term=").append(response.getTerm());
        if (!response.getLeaderId().isBlank()) {
            out.append(" leader=").append(response.getLeaderId());
        }
        out.append('\n');
        out.append("Reconfiguration: active=").append(response.isReconfigurationActive())
                .append(" jointConsensus=").append(response.isJointConsensus())
                .append(" ageMillis=").append(response.getReconfigurationAgeMillis())
                .append(" clusterHealth=").append(response.getClusterHealth())
                .append(" reason=").append(response.getClusterStatusReason())
                .append(" quorum=").append(response.isQuorumAvailable())
                .append(" currentQuorum=").append(response.isCurrentQuorumAvailable())
                .append(" nextQuorum=").append(response.isNextQuorumAvailable());
        if (!response.getBlockingCurrentQuorumPeerIds().isEmpty()) {
            out.append(" blockingCurrent=").append(response.getBlockingCurrentQuorumPeerIds());
        }
        if (!response.getBlockingNextQuorumPeerIds().isEmpty()) {
            out.append(" blockingNext=").append(response.getBlockingNextQuorumPeerIds());
        }
        out.append('\n');
        out.append("Members:\n");
        for (var member : response.getMembers()) {
            if ("steady".equals(member.roleTransition()) && member.blockingQuorums().isBlank()) {
                continue;
            }
            out.append("  ").append(member.peerId())
                    .append(" currentRole=").append(member.currentRole().isBlank() ? "-" : member.currentRole().toLowerCase(Locale.ROOT))
                    .append(" nextRole=").append(member.nextRole().isBlank() ? "-" : member.nextRole().toLowerCase(Locale.ROOT))
                    .append(" transition=").append(member.roleTransition())
                    .append(" transitionAgeMillis=").append(member.transitionAgeMillis())
                    .append(" blockingQuorums=").append(member.blockingQuorums().isBlank() ? "-" : member.blockingQuorums())
                    .append(" blockingReason=").append(member.blockingReason().isBlank() ? "-" : member.blockingReason())
                    .append(" health=").append(member.health())
                    .append(" reachable=").append(member.reachable())
                    .append(" freshness=").append(member.freshness())
                    .append(" failures=").append(member.consecutiveFailures())
                    .append(" lag=").append(member.lag())
                    .append('\n');
        }
        return out.toString().trim();
    }

    private static String renderReconfigurationStatusJson(ReconfigurationStatusResponse response) {
        StringBuilder out = new StringBuilder();
        out.append("{\n");
        appendJsonField(out, "observedAtMillis", response.getObservedAtMillis(), true, 1);
        appendJsonField(out, "peerId", response.getPeerId(), true, 1);
        appendJsonField(out, "success", response.isSuccess(), true, 1);
        appendJsonField(out, "status", response.getStatus(), true, 1);
        appendJsonField(out, "redirectLeaderId", response.getRedirectLeaderId(), true, 1);
        appendJsonField(out, "redirectLeaderHost", response.getRedirectLeaderHost(), true, 1);
        appendJsonField(out, "redirectLeaderPort", response.getRedirectLeaderPort(), true, 1);
        appendJsonField(out, "state", response.getState(), true, 1);
        appendJsonField(out, "leaderId", response.getLeaderId(), true, 1);
        appendJsonField(out, "term", response.getTerm(), true, 1);
        appendJsonField(out, "reconfigurationActive", response.isReconfigurationActive(), true, 1);
        appendJsonField(out, "jointConsensus", response.isJointConsensus(), true, 1);
        appendJsonField(out, "reconfigurationAgeMillis", response.getReconfigurationAgeMillis(), true, 1);
        appendJsonField(out, "clusterHealth", response.getClusterHealth(), true, 1);
        appendJsonField(out, "clusterStatusReason", response.getClusterStatusReason(), true, 1);
        appendJsonField(out, "quorumAvailable", response.isQuorumAvailable(), true, 1);
        appendJsonField(out, "currentQuorumAvailable", response.isCurrentQuorumAvailable(), true, 1);
        appendJsonField(out, "nextQuorumAvailable", response.isNextQuorumAvailable(), true, 1);
        appendJsonStringArray(out, "blockingCurrentQuorumPeerIds", response.getBlockingCurrentQuorumPeerIds(), true, 1);
        appendJsonStringArray(out, "blockingNextQuorumPeerIds", response.getBlockingNextQuorumPeerIds(), true, 1);
        appendJsonClusterMembers(out, "members", response.getMembers(), false, 1);
        out.append("}");
        return out.toString();
    }

    private static void appendClusterView(StringBuilder out, TelemetryResponse response) {
        if (!"LEADER".equals(response.getState())) {
            return;
        }

        Map<String, org.gautelis.raft.protocol.TelemetryPeerStats> statsByPeer = new HashMap<>();
        for (var stats : response.getPeerStats()) {
            statsByPeer.put(stats.peerId(), stats);
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

    private static TelemetryResponse fetchTelemetry(RaftClient client, Peer target, boolean includePeerStats) throws Exception {
        TelemetryResponse response = client.sendTelemetryRequest(
                target,
                new TelemetryRequest(0L, "telemetry-cli", includePeerStats, true)
        ).get(5, TimeUnit.SECONDS);
        if ("REDIRECT".equals(response.getStatus())) {
            Peer redirectedLeader = resolveRedirectLeader(response);
            if (redirectedLeader != null) {
                response = client.sendTelemetryRequest(
                        redirectedLeader,
                        new TelemetryRequest(0L, "telemetry-cli", includePeerStats, true)
                ).get(5, TimeUnit.SECONDS);
            }
        }
        return response;
    }

    private static ClusterSummaryResponse fetchClusterSummary(RaftClient client, Peer target) throws Exception {
        ClusterSummaryResponse response = client.sendClusterSummaryRequest(
                target,
                new ClusterSummaryRequest(0L, "cluster-summary-cli")
        ).get(5, TimeUnit.SECONDS);
        if ("REDIRECT".equals(response.getStatus()) && !response.getRedirectLeaderHost().isBlank() && response.getRedirectLeaderPort() > 0) {
            response = client.sendClusterSummaryRequest(
                    new Peer(response.getRedirectLeaderId(), new InetSocketAddress(response.getRedirectLeaderHost(), response.getRedirectLeaderPort())),
                    new ClusterSummaryRequest(0L, "cluster-summary-cli")
            ).get(5, TimeUnit.SECONDS);
        }
        return response;
    }

    private static ReconfigurationStatusResponse fetchReconfigurationStatus(RaftClient client, Peer target) throws Exception {
        ReconfigurationStatusResponse response = client.sendReconfigurationStatusRequest(
                target,
                new ReconfigurationStatusRequest(0L, "reconfiguration-status-cli")
        ).get(5, TimeUnit.SECONDS);
        if ("REDIRECT".equals(response.getStatus()) && !response.getRedirectLeaderHost().isBlank() && response.getRedirectLeaderPort() > 0) {
            response = client.sendReconfigurationStatusRequest(
                    new Peer(response.getRedirectLeaderId(), new InetSocketAddress(response.getRedirectLeaderHost(), response.getRedirectLeaderPort())),
                    new ReconfigurationStatusRequest(0L, "reconfiguration-status-cli")
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

    private static String renderPeers(List<Peer> peers) {
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

    private static boolean containsPeer(List<Peer> peers, String peerId) {
        if (peers == null || peerId == null) {
            return false;
        }
        for (Peer peer : peers) {
            if (peerId.equals(peer.getId())) {
                return true;
            }
        }
        return false;
    }

    private static org.gautelis.raft.protocol.TelemetryReplicationStatus findReplication(TelemetryResponse response, String peerId) {
        for (var replication : response.getReplication()) {
            if (peerId.equals(replication.peerId())) {
                return replication;
            }
        }
        return null;
    }

    private static String describeFreshness(long observedAtMillis, long lastContactMillis) {
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

    private static String describeHealth(boolean local, boolean reachable, String freshness, int consecutiveFailures) {
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

    private static void appendJsonField(StringBuilder out, String key, String value, boolean comma, int indent) {
        indent(out, indent).append('"').append(escapeJson(key)).append("\": ")
                .append('"').append(escapeJson(value == null ? "" : value)).append('"');
        if (comma) {
            out.append(',');
        }
        out.append('\n');
    }

    private static void appendJsonField(StringBuilder out, String key, long value, boolean comma, int indent) {
        indent(out, indent).append('"').append(escapeJson(key)).append("\": ").append(value);
        if (comma) {
            out.append(',');
        }
        out.append('\n');
    }

    private static void appendJsonField(StringBuilder out, String key, int value, boolean comma, int indent) {
        indent(out, indent).append('"').append(escapeJson(key)).append("\": ").append(value);
        if (comma) {
            out.append(',');
        }
        out.append('\n');
    }

    private static void appendJsonField(StringBuilder out, String key, boolean value, boolean comma, int indent) {
        indent(out, indent).append('"').append(escapeJson(key)).append("\": ").append(value);
        if (comma) {
            out.append(',');
        }
        out.append('\n');
    }

    private static void appendJsonPeerArray(StringBuilder out, String key, List<Peer> peers, boolean comma, int indent) {
        indent(out, indent).append('"').append(escapeJson(key)).append("\": [\n");
        for (int i = 0; i < peers.size(); i++) {
            Peer peer = peers.get(i);
            indent(out, indent + 1).append("{ ");
            out.append("\"id\": \"").append(escapeJson(peer.getId())).append('"');
            out.append(", \"role\": \"").append(escapeJson(peer.getRole().name())).append('"');
            if (peer.getAddress() != null) {
                out.append(", \"host\": \"").append(escapeJson(peer.getAddress().getHostString())).append('"');
                out.append(", \"port\": ").append(peer.getAddress().getPort());
            }
            out.append(" }");
            if (i + 1 < peers.size()) {
                out.append(',');
            }
            out.append('\n');
        }
        indent(out, indent).append(']');
        if (comma) {
            out.append(',');
        }
        out.append('\n');
    }

    private static void appendJsonStringArray(StringBuilder out, String key, List<String> values, boolean comma, int indent) {
        indent(out, indent).append('"').append(escapeJson(key)).append("\": [");
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) {
                out.append(", ");
            }
            out.append('"').append(escapeJson(values.get(i))).append('"');
        }
        out.append(']');
        if (comma) {
            out.append(',');
        }
        out.append('\n');
    }

    private static void appendJsonReplicationArray(StringBuilder out, String key, TelemetryResponse response, boolean comma, int indent) {
        indent(out, indent).append('"').append(escapeJson(key)).append("\": [\n");
        for (int i = 0; i < response.getReplication().size(); i++) {
            var status = response.getReplication().get(i);
            String freshness = describeFreshness(response.getObservedAtMillis(), status.lastSuccessfulContactMillis());
            String health = describeHealth(false, status.reachable(), freshness, status.consecutiveFailures());
            indent(out, indent + 1).append("{ ")
                    .append("\"peerId\": \"").append(escapeJson(status.peerId())).append('"')
                    .append(", \"nextIndex\": ").append(status.nextIndex())
                    .append(", \"matchIndex\": ").append(status.matchIndex())
                    .append(", \"reachable\": ").append(status.reachable())
                    .append(", \"lastSuccessfulContactMillis\": ").append(status.lastSuccessfulContactMillis())
                    .append(", \"consecutiveFailures\": ").append(status.consecutiveFailures())
                    .append(", \"lastFailedContactMillis\": ").append(status.lastFailedContactMillis())
                    .append(", \"freshness\": \"").append(escapeJson(freshness)).append('"')
                    .append(", \"health\": \"").append(escapeJson(health)).append('"')
                    .append(" }");
            if (i + 1 < response.getReplication().size()) {
                out.append(',');
            }
            out.append('\n');
        }
        indent(out, indent).append(']');
        if (comma) {
            out.append(',');
        }
        out.append('\n');
    }

    private static void appendJsonPeerStatsArray(StringBuilder out, String key, TelemetryResponse response, boolean comma, int indent) {
        indent(out, indent).append('"').append(escapeJson(key)).append("\": [\n");
        for (int i = 0; i < response.getPeerStats().size(); i++) {
            var stats = response.getPeerStats().get(i);
            indent(out, indent + 1).append("{ ")
                    .append("\"peerId\": \"").append(escapeJson(stats.peerId())).append('"')
                    .append(", \"samples\": ").append(stats.samples())
                    .append(", \"meanMillis\": ").append(String.format(Locale.ROOT, "%.3f", stats.meanMillis()))
                    .append(", \"minMillis\": ").append(String.format(Locale.ROOT, "%.3f", stats.minMillis()))
                    .append(", \"maxMillis\": ").append(String.format(Locale.ROOT, "%.3f", stats.maxMillis()))
                    .append(", \"cvPercent\": ").append(String.format(Locale.ROOT, "%.2f", stats.cvPercent()))
                    .append(" }");
            if (i + 1 < response.getPeerStats().size()) {
                out.append(',');
            }
            out.append('\n');
        }
        indent(out, indent).append(']');
        if (comma) {
            out.append(',');
        }
        out.append('\n');
    }

    private static void appendJsonClusterMembers(StringBuilder out, String key, List<org.gautelis.raft.protocol.ClusterMemberSummary> members, boolean comma, int indent) {
        indent(out, indent).append('"').append(escapeJson(key)).append("\": [\n");
        for (int i = 0; i < members.size(); i++) {
            var member = members.get(i);
            indent(out, indent + 1).append("{ ")
                    .append("\"peerId\": \"").append(escapeJson(member.peerId())).append('"')
                    .append(", \"local\": ").append(member.local())
                    .append(", \"currentMember\": ").append(member.currentMember())
                    .append(", \"nextMember\": ").append(member.nextMember())
                    .append(", \"voting\": ").append(member.voting())
                    .append(", \"role\": \"").append(escapeJson(member.role())).append('"')
                    .append(", \"currentRole\": \"").append(escapeJson(member.currentRole())).append('"')
                    .append(", \"nextRole\": \"").append(escapeJson(member.nextRole())).append('"')
                    .append(", \"roleTransition\": \"").append(escapeJson(member.roleTransition())).append('"')
                    .append(", \"transitionAgeMillis\": ").append(member.transitionAgeMillis())
                    .append(", \"blockingQuorums\": \"").append(escapeJson(member.blockingQuorums())).append('"')
                    .append(", \"blockingReason\": \"").append(escapeJson(member.blockingReason())).append('"')
                    .append(", \"reachable\": ").append(member.reachable())
                    .append(", \"freshness\": \"").append(escapeJson(member.freshness())).append('"')
                    .append(", \"health\": \"").append(escapeJson(member.health())).append('"')
                    .append(", \"nextIndex\": ").append(member.nextIndex())
                    .append(", \"matchIndex\": ").append(member.matchIndex())
                    .append(", \"lag\": ").append(member.lag())
                    .append(", \"consecutiveFailures\": ").append(member.consecutiveFailures())
                    .append(", \"lastSuccessfulContactMillis\": ").append(member.lastSuccessfulContactMillis())
                    .append(", \"lastFailedContactMillis\": ").append(member.lastFailedContactMillis())
                    .append(" }");
            if (i + 1 < members.size()) {
                out.append(',');
            }
            out.append('\n');
        }
        indent(out, indent).append(']');
        if (comma) {
            out.append(',');
        }
        out.append('\n');
    }

    private static StringBuilder indent(StringBuilder out, int indent) {
        for (int i = 0; i < indent; i++) {
            out.append("  ");
        }
        return out;
    }

    private static String escapeJson(String value) {
        StringBuilder escaped = new StringBuilder();
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            switch (ch) {
                case '\\' -> escaped.append("\\\\");
                case '"' -> escaped.append("\\\"");
                case '\n' -> escaped.append("\\n");
                case '\r' -> escaped.append("\\r");
                case '\t' -> escaped.append("\\t");
                default -> escaped.append(ch);
            }
        }
        return escaped.toString();
    }
}
