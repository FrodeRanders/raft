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
package org.gautelis.raft.telemetry;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.gautelis.raft.ClusterConfiguration;
import org.gautelis.raft.RaftNode;
import org.gautelis.raft.protocol.Peer;
import org.gautelis.raft.protocol.TelemetryPeerStats;
import org.gautelis.raft.protocol.TelemetryReplicationStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

public final class PrometheusTelemetryExporter implements TelemetryExporter {
    private static final Logger log = LoggerFactory.getLogger(PrometheusTelemetryExporter.class);

    private final HttpServer server;
    private final String path;
    private final AtomicReference<String> payload = new AtomicReference<>("# no metrics published yet\n");

    public PrometheusTelemetryExporter() {
        try {
            String host = System.getProperty("raft.telemetry.prometheus.host", "127.0.0.1").trim();
            int port = Integer.getInteger("raft.telemetry.prometheus.port", 9108);
            path = normalizedPath(System.getProperty("raft.telemetry.prometheus.path", "/metrics"));
            server = HttpServer.create(new InetSocketAddress(host, port), 0);
            server.createContext(path, this::handleMetrics);
            server.setExecutor(Executors.newSingleThreadExecutor(r -> {
                Thread thread = new Thread(r, "raft-prometheus-exporter");
                thread.setDaemon(true);
                return thread;
            }));
            server.start();
            log.info("Prometheus telemetry exporter listening on http://{}:{}{}", host, server.getAddress().getPort(), path);
        } catch (IOException e) {
            throw new IllegalStateException("Failed starting Prometheus telemetry exporter", e);
        }
    }

    int getListenPort() {
        return server.getAddress().getPort();
    }

    @Override
    public void publish(RaftNode.TelemetrySnapshot snapshot, List<TelemetryPeerStats> peerStats) {
        if (snapshot == null) {
            return;
        }
        payload.set(render(snapshot, peerStats == null ? List.of() : peerStats));
    }

    @Override
    public void close() {
        server.stop(0);
    }

    private void handleMetrics(HttpExchange exchange) throws IOException {
        if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(405, -1);
            exchange.close();
            return;
        }
        byte[] bytes = payload.get().getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
        exchange.sendResponseHeaders(200, bytes.length);
        try (OutputStream body = exchange.getResponseBody()) {
            body.write(bytes);
        } finally {
            exchange.close();
        }
    }

    private String render(RaftNode.TelemetrySnapshot snapshot, List<TelemetryPeerStats> peerStats) {
        StringBuilder out = new StringBuilder();
        appendGauge(out, "raft_term", snapshot.term(), labels("peer_id", snapshot.peerId()));
        appendGauge(out, "raft_commit_index", snapshot.commitIndex(), labels("peer_id", snapshot.peerId()));
        appendGauge(out, "raft_last_applied", snapshot.lastApplied(), labels("peer_id", snapshot.peerId()));
        appendGauge(out, "raft_last_log_index", snapshot.lastLogIndex(), labels("peer_id", snapshot.peerId()));
        appendGauge(out, "raft_joint_consensus", snapshot.configuration().isJointConsensus() ? 1 : 0, labels("peer_id", snapshot.peerId()));
        appendGauge(out, "raft_joining", snapshot.joining() ? 1 : 0, labels("peer_id", snapshot.peerId()));
        appendGauge(out, "raft_decommissioned", snapshot.decommissioned() ? 1 : 0, labels("peer_id", snapshot.peerId()));
        appendGauge(out, "raft_known_peers", snapshot.knownPeers().size(), labels("peer_id", snapshot.peerId()));
        appendGauge(out, "raft_current_members", snapshot.configuration().currentMembers().size(), labels("peer_id", snapshot.peerId()));
        appendGauge(out, "raft_next_members", snapshot.configuration().nextMembers().size(), labels("peer_id", snapshot.peerId()));
        appendGauge(out, "raft_current_voting_members", snapshot.configuration().currentVotingMembers().size(), labels("peer_id", snapshot.peerId()));
        appendGauge(out, "raft_next_voting_members", snapshot.configuration().nextVotingMembers().size(), labels("peer_id", snapshot.peerId()));
        appendGauge(out, "raft_pending_joins", snapshot.pendingJoinIds().size(), labels("peer_id", snapshot.peerId()));
        appendGauge(out, "raft_members_promoting", transitionCount(snapshot.configuration(), snapshot.latestKnownConfiguration(), "promoting"), labels("peer_id", snapshot.peerId()));
        appendGauge(out, "raft_members_demoting", transitionCount(snapshot.configuration(), snapshot.latestKnownConfiguration(), "demoting"), labels("peer_id", snapshot.peerId()));
        appendGauge(out, "raft_members_joining", transitionCount(snapshot.configuration(), snapshot.latestKnownConfiguration(), "joining"), labels("peer_id", snapshot.peerId()));
        appendGauge(out, "raft_members_removing", transitionCount(snapshot.configuration(), snapshot.latestKnownConfiguration(), "removing"), labels("peer_id", snapshot.peerId()));
        appendGauge(out, "raft_reconfiguration_age_millis", snapshot.configurationTransitionStartedMillis() <= 0L ? 0L : Math.max(0L, snapshot.observedAtMillis() - snapshot.configurationTransitionStartedMillis()), labels("peer_id", snapshot.peerId()));

        for (TelemetryReplicationStatus replication : snapshot.replication()) {
            String labels = labels("peer_id", snapshot.peerId(), "remote_peer_id", replication.peerId());
            appendGauge(out, "raft_replication_next_index", replication.nextIndex(), labels);
            appendGauge(out, "raft_replication_match_index", replication.matchIndex(), labels);
            appendGauge(out, "raft_replication_reachable", replication.reachable() ? 1 : 0, labels);
            appendGauge(out, "raft_replication_last_successful_contact_millis", replication.lastSuccessfulContactMillis(), labels);
            appendGauge(out, "raft_replication_last_failed_contact_millis", replication.lastFailedContactMillis(), labels);
            appendGauge(out, "raft_replication_consecutive_failures", replication.consecutiveFailures(), labels);
        }

        for (TelemetryPeerStats stats : peerStats) {
            String labels = labels("peer_id", snapshot.peerId(), "remote_peer_id", stats.peerId());
            appendGauge(out, "raft_transport_response_samples", stats.samples(), labels);
            appendGauge(out, "raft_transport_response_mean_millis", stats.meanMillis(), labels);
            appendGauge(out, "raft_transport_response_min_millis", stats.minMillis(), labels);
            appendGauge(out, "raft_transport_response_max_millis", stats.maxMillis(), labels);
            appendGauge(out, "raft_transport_response_cv_percent", stats.cvPercent(), labels);
        }
        return out.toString();
    }

    private static void appendGauge(StringBuilder out, String name, long value, String labels) {
        appendLine(out, name, Long.toString(value), labels);
    }

    private static void appendGauge(StringBuilder out, String name, double value, String labels) {
        appendLine(out, name, String.format(Locale.ROOT, "%.3f", value), labels);
    }

    private static void appendLine(StringBuilder out, String name, String value, String labels) {
        out.append(name);
        if (!labels.isEmpty()) {
            out.append('{').append(labels).append('}');
        }
        out.append(' ').append(value).append('\n');
    }

    private static String labels(String... kv) {
        StringBuilder out = new StringBuilder();
        for (int i = 0; i + 1 < kv.length; i += 2) {
            if (i > 0) {
                out.append(',');
            }
            out.append(kv[i]).append("=\"").append(escape(kv[i + 1])).append('"');
        }
        return out.toString();
    }

    private static String escape(String value) {
        return value == null ? "" : value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static String normalizedPath(String value) {
        if (value == null || value.isBlank()) {
            return "/metrics";
        }
        return value.startsWith("/") ? value : "/" + value;
    }

    private static long transitionCount(ClusterConfiguration active, ClusterConfiguration target, String transition) {
        long count = 0L;
        java.util.LinkedHashSet<String> peerIds = new java.util.LinkedHashSet<>();
        for (Peer peer : active.allMembers()) {
            peerIds.add(peer.getId());
        }
        for (Peer peer : target.allMembers()) {
            peerIds.add(peer.getId());
        }
        for (String peerId : peerIds) {
            if (transition.equals(describeRoleTransition(active, target, peerId))) {
                count++;
            }
        }
        return count;
    }

    private static String describeRoleTransition(ClusterConfiguration active, ClusterConfiguration target, String peerId) {
        Peer current = active.currentMembers().stream().filter(member -> peerId.equals(member.getId())).findFirst().orElse(null);
        Peer next = targetMember(target, peerId);
        if (current == null && next == null) {
            return "known";
        }
        if (current == null) {
            return "joining";
        }
        if (next == null) {
            return "removing";
        }
        if (current.getRole() == next.getRole()) {
            return "steady";
        }
        return next.isVoter() ? "promoting" : "demoting";
    }

    private static Peer targetMember(ClusterConfiguration configuration, String peerId) {
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
}
