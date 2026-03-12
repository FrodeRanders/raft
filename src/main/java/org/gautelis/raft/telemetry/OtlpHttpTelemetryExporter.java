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

import org.gautelis.raft.ClusterConfiguration;
import org.gautelis.raft.RaftNode;
import org.gautelis.raft.protocol.Peer;
import org.gautelis.raft.protocol.TelemetryPeerStats;
import org.gautelis.raft.protocol.TelemetryReplicationStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

final class OtlpHttpTelemetryExporter implements TelemetryExporter {
    private static final Logger log = LoggerFactory.getLogger(OtlpHttpTelemetryExporter.class);
    private static final String DEFAULT_ENDPOINT = "http://127.0.0.1:4318/v1/metrics";

    private final String mode;
    private final URI endpoint;
    private final Duration timeout;
    private final HttpClient client;
    private final Map<String, String> headers;
    private final String serviceName;
    private final String serviceNamespace;

    OtlpHttpTelemetryExporter(String mode) {
        this.mode = mode;
        this.endpoint = normalizeEndpoint(System.getProperty("raft.telemetry.otlp.endpoint", DEFAULT_ENDPOINT));
        this.timeout = Duration.ofMillis(Long.getLong("raft.telemetry.otlp.timeout.millis", 2_000L));
        this.client = HttpClient.newBuilder().connectTimeout(timeout).build();
        this.headers = parseHeaders(System.getProperty("raft.telemetry.otlp.headers", ""));
        this.serviceName = System.getProperty("raft.telemetry.otlp.service.name", "raft");
        this.serviceNamespace = System.getProperty("raft.telemetry.otlp.service.namespace", "org.gautelis");
    }

    @Override
    public void publish(RaftNode.TelemetrySnapshot snapshot, List<TelemetryPeerStats> peerStats) {
        if (snapshot == null) {
            return;
        }
        String payload = renderPayload(snapshot, peerStats == null ? List.of() : peerStats);
        HttpRequest.Builder request = HttpRequest.newBuilder(endpoint)
                .timeout(timeout)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(payload));
        for (Map.Entry<String, String> header : headers.entrySet()) {
            request.header(header.getKey(), header.getValue());
        }
        try {
            HttpResponse<Void> response = client.send(request.build(), HttpResponse.BodyHandlers.discarding());
            if (response.statusCode() >= 300) {
                log.debug("OTLP export to {} returned HTTP {}", endpoint, response.statusCode());
            }
        } catch (IOException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            log.debug("OTLP export to {} failed", endpoint, e);
        }
    }

    String renderPayload(RaftNode.TelemetrySnapshot snapshot, List<TelemetryPeerStats> peerStats) {
        StringBuilder out = new StringBuilder();
        long timeUnixNano = snapshot.observedAtMillis() * 1_000_000L;
        out.append('{');
        out.append("\"resourceMetrics\":[{");
        out.append("\"resource\":{");
        out.append("\"attributes\":[");
        appendAttribute(out, "service.name", serviceName, true);
        appendAttribute(out, "service.namespace", serviceNamespace, true);
        appendAttribute(out, "service.instance.id", snapshot.peerId(), true);
        appendAttribute(out, "raft.exporter.mode", mode, false);
        out.append("]},");
        out.append("\"scopeMetrics\":[{");
        out.append("\"scope\":{");
        appendJsonStringField(out, "name", "org.gautelis.raft.telemetry", true);
        appendJsonStringField(out, "version", "1.0", false);
        out.append("},");
        out.append("\"metrics\":[");

        boolean firstMetric = true;
        firstMetric = appendLongGauge(out, firstMetric, "raft.term", "Current term", "1", timeUnixNano, snapshot.term(),
                attrs("peer.id", snapshot.peerId(), "raft.state", snapshot.state()));
        firstMetric = appendLongGauge(out, firstMetric, "raft.commit.index", "Committed log index", "1", timeUnixNano, snapshot.commitIndex(),
                attrs("peer.id", snapshot.peerId()));
        firstMetric = appendLongGauge(out, firstMetric, "raft.last.applied", "Last applied log index", "1", timeUnixNano, snapshot.lastApplied(),
                attrs("peer.id", snapshot.peerId()));
        firstMetric = appendLongGauge(out, firstMetric, "raft.last.log.index", "Last log index", "1", timeUnixNano, snapshot.lastLogIndex(),
                attrs("peer.id", snapshot.peerId()));
        firstMetric = appendLongGauge(out, firstMetric, "raft.last.log.term", "Last log term", "1", timeUnixNano, snapshot.lastLogTerm(),
                attrs("peer.id", snapshot.peerId()));
        firstMetric = appendLongGauge(out, firstMetric, "raft.snapshot.index", "Snapshot index", "1", timeUnixNano, snapshot.snapshotIndex(),
                attrs("peer.id", snapshot.peerId()));
        firstMetric = appendLongGauge(out, firstMetric, "raft.snapshot.term", "Snapshot term", "1", timeUnixNano, snapshot.snapshotTerm(),
                attrs("peer.id", snapshot.peerId()));
        firstMetric = appendLongGauge(out, firstMetric, "raft.known.peers", "Known peer count", "1", timeUnixNano, snapshot.knownPeers().size(),
                attrs("peer.id", snapshot.peerId()));
        firstMetric = appendLongGauge(out, firstMetric, "raft.current.members", "Current cluster members", "1", timeUnixNano, snapshot.configuration().currentMembers().size(),
                attrs("peer.id", snapshot.peerId()));
        firstMetric = appendLongGauge(out, firstMetric, "raft.next.members", "Next cluster members", "1", timeUnixNano, snapshot.configuration().nextMembers().size(),
                attrs("peer.id", snapshot.peerId()));
        firstMetric = appendLongGauge(out, firstMetric, "raft.current.voting.members", "Current voting members", "1", timeUnixNano, snapshot.configuration().currentVotingMembers().size(),
                attrs("peer.id", snapshot.peerId()));
        firstMetric = appendLongGauge(out, firstMetric, "raft.next.voting.members", "Next voting members", "1", timeUnixNano, snapshot.configuration().nextVotingMembers().size(),
                attrs("peer.id", snapshot.peerId()));
        firstMetric = appendLongGauge(out, firstMetric, "raft.pending.joins", "Pending join count", "1", timeUnixNano, snapshot.pendingJoinIds().size(),
                attrs("peer.id", snapshot.peerId()));
        firstMetric = appendLongGauge(out, firstMetric, "raft.members.promoting", "Members currently promoting from learner to voter", "1", timeUnixNano, transitionCount(snapshot.configuration(), snapshot.latestKnownConfiguration(), "promoting"),
                attrs("peer.id", snapshot.peerId()));
        firstMetric = appendLongGauge(out, firstMetric, "raft.members.demoting", "Members currently demoting from voter to learner", "1", timeUnixNano, transitionCount(snapshot.configuration(), snapshot.latestKnownConfiguration(), "demoting"),
                attrs("peer.id", snapshot.peerId()));
        firstMetric = appendLongGauge(out, firstMetric, "raft.members.joining", "Members present in target configuration but not active configuration", "1", timeUnixNano, transitionCount(snapshot.configuration(), snapshot.latestKnownConfiguration(), "joining"),
                attrs("peer.id", snapshot.peerId()));
        firstMetric = appendLongGauge(out, firstMetric, "raft.members.removing", "Members present in active configuration but not target configuration", "1", timeUnixNano, transitionCount(snapshot.configuration(), snapshot.latestKnownConfiguration(), "removing"),
                attrs("peer.id", snapshot.peerId()));
        firstMetric = appendLongGauge(out, firstMetric, "raft.joint.consensus", "Joint consensus flag", "1", timeUnixNano, snapshot.configuration().isJointConsensus() ? 1 : 0,
                attrs("peer.id", snapshot.peerId()));
        firstMetric = appendLongGauge(out, firstMetric, "raft.joining", "Joining flag", "1", timeUnixNano, snapshot.joining() ? 1 : 0,
                attrs("peer.id", snapshot.peerId()));
        firstMetric = appendLongGauge(out, firstMetric, "raft.decommissioned", "Decommissioned flag", "1", timeUnixNano, snapshot.decommissioned() ? 1 : 0,
                attrs("peer.id", snapshot.peerId()));

        for (TelemetryReplicationStatus replication : snapshot.replication()) {
            Map<String, String> attrs = attrs("peer.id", snapshot.peerId(), "remote.peer.id", replication.peerId());
            firstMetric = appendLongGauge(out, firstMetric, "raft.replication.next.index", "Next index for follower replication", "1", timeUnixNano, replication.nextIndex(), attrs);
            firstMetric = appendLongGauge(out, firstMetric, "raft.replication.match.index", "Match index for follower replication", "1", timeUnixNano, replication.matchIndex(), attrs);
            firstMetric = appendLongGauge(out, firstMetric, "raft.replication.reachable", "Follower reachability", "1", timeUnixNano, replication.reachable() ? 1 : 0, attrs);
            firstMetric = appendLongGauge(out, firstMetric, "raft.replication.last.success.millis", "Last successful replication contact", "ms", timeUnixNano, replication.lastSuccessfulContactMillis(), attrs);
            firstMetric = appendLongGauge(out, firstMetric, "raft.replication.last.failure.millis", "Last failed replication contact", "ms", timeUnixNano, replication.lastFailedContactMillis(), attrs);
            firstMetric = appendLongGauge(out, firstMetric, "raft.replication.consecutive.failures", "Consecutive replication failures", "1", timeUnixNano, replication.consecutiveFailures(), attrs);
        }

        for (TelemetryPeerStats stats : peerStats) {
            Map<String, String> attrs = attrs("peer.id", snapshot.peerId(), "remote.peer.id", stats.peerId());
            firstMetric = appendLongGauge(out, firstMetric, "raft.transport.response.samples", "Transport response sample count", "1", timeUnixNano, stats.samples(), attrs);
            firstMetric = appendDoubleGauge(out, firstMetric, "raft.transport.response.mean.millis", "Mean transport response time", "ms", timeUnixNano, stats.meanMillis(), attrs);
            firstMetric = appendDoubleGauge(out, firstMetric, "raft.transport.response.min.millis", "Minimum transport response time", "ms", timeUnixNano, stats.minMillis(), attrs);
            firstMetric = appendDoubleGauge(out, firstMetric, "raft.transport.response.max.millis", "Maximum transport response time", "ms", timeUnixNano, stats.maxMillis(), attrs);
            firstMetric = appendDoubleGauge(out, firstMetric, "raft.transport.response.cv.percent", "Response time coefficient of variation", "%", timeUnixNano, stats.cvPercent(), attrs);
        }

        out.append("]}]}]}");
        return out.toString();
    }

    private static boolean appendLongGauge(StringBuilder out, boolean firstMetric, String name, String description, String unit,
                                           long timeUnixNano, long value, Map<String, String> attributes) {
        if (!firstMetric) {
            out.append(',');
        }
        out.append('{');
        appendJsonStringField(out, "name", name, true);
        appendJsonStringField(out, "description", description, true);
        appendJsonStringField(out, "unit", unit, true);
        out.append("\"gauge\":{\"dataPoints\":[{");
        appendDataPointPrefix(out, timeUnixNano, attributes);
        appendJsonStringField(out, "asInt", Long.toString(value), false);
        out.append("}]}}");
        return false;
    }

    private static boolean appendDoubleGauge(StringBuilder out, boolean firstMetric, String name, String description, String unit,
                                             long timeUnixNano, double value, Map<String, String> attributes) {
        if (!firstMetric) {
            out.append(',');
        }
        out.append('{');
        appendJsonStringField(out, "name", name, true);
        appendJsonStringField(out, "description", description, true);
        appendJsonStringField(out, "unit", unit, true);
        out.append("\"gauge\":{\"dataPoints\":[{");
        appendDataPointPrefix(out, timeUnixNano, attributes);
        appendJsonDoubleField(out, "asDouble", value, false);
        out.append("}]}}");
        return false;
    }

    private static void appendDataPointPrefix(StringBuilder out, long timeUnixNano, Map<String, String> attributes) {
        appendJsonStringField(out, "timeUnixNano", Long.toString(timeUnixNano), true);
        out.append("\"attributes\":[");
        boolean first = true;
        for (Map.Entry<String, String> attribute : attributes.entrySet()) {
            if (!first) {
                out.append(',');
            }
            appendAttributeObject(out, attribute.getKey(), attribute.getValue());
            first = false;
        }
        out.append("],");
    }

    private static void appendAttribute(StringBuilder out, String key, String value, boolean comma) {
        appendAttributeObject(out, key, value);
        if (comma) {
            out.append(',');
        }
    }

    private static void appendAttributeObject(StringBuilder out, String key, String value) {
        out.append('{');
        appendJsonStringField(out, "key", key, true);
        out.append("\"value\":{");
        appendJsonStringField(out, "stringValue", value, false);
        out.append("}}");
    }

    private static Map<String, String> attrs(String... kv) {
        Map<String, String> out = new LinkedHashMap<>();
        for (int i = 0; i + 1 < kv.length; i += 2) {
            out.put(kv[i], kv[i + 1]);
        }
        return out;
    }

    private static void appendJsonStringField(StringBuilder out, String key, String value, boolean comma) {
        out.append('"').append(escape(key)).append("\":\"").append(escape(value)).append('"');
        if (comma) {
            out.append(',');
        }
    }

    private static void appendJsonDoubleField(StringBuilder out, String key, double value, boolean comma) {
        out.append('"').append(escape(key)).append("\":").append(String.format(Locale.ROOT, "%.3f", value));
        if (comma) {
            out.append(',');
        }
    }

    private static Map<String, String> parseHeaders(String raw) {
        Map<String, String> headers = new LinkedHashMap<>();
        if (raw == null || raw.isBlank()) {
            return headers;
        }
        for (String part : raw.split(",")) {
            String entry = part.trim();
            if (entry.isEmpty()) {
                continue;
            }
            int separator = entry.indexOf('=');
            if (separator <= 0 || separator == entry.length() - 1) {
                continue;
            }
            headers.put(entry.substring(0, separator).trim(), entry.substring(separator + 1).trim());
        }
        return headers;
    }

    private static URI normalizeEndpoint(String configured) {
        String value = configured == null || configured.isBlank() ? DEFAULT_ENDPOINT : configured.trim();
        if (!value.endsWith("/v1/metrics")) {
            if (value.endsWith("/")) {
                value = value + "v1/metrics";
            } else {
                value = value + "/v1/metrics";
            }
        }
        return URI.create(value);
    }

    private static String escape(String value) {
        return value == null ? "" : value.replace("\\", "\\\\").replace("\"", "\\\"");
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
