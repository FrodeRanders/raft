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

import org.gautelis.raft.protocol.Peer;
import org.gautelis.raft.protocol.TelemetryResponse;

import java.util.List;
import java.util.Locale;

final class JsonFormatSupport {
    private JsonFormatSupport() {
    }

    static void appendJsonField(StringBuilder out, String key, String value, boolean comma, int indent) {
        indent(out, indent).append('"').append(escapeJson(key)).append("\": ")
                .append('"').append(escapeJson(value == null ? "" : value)).append('"');
        if (comma) {
            out.append(',');
        }
        out.append('\n');
    }

    static void appendJsonField(StringBuilder out, String key, long value, boolean comma, int indent) {
        indent(out, indent).append('"').append(escapeJson(key)).append("\": ").append(value);
        if (comma) {
            out.append(',');
        }
        out.append('\n');
    }

    static void appendJsonField(StringBuilder out, String key, int value, boolean comma, int indent) {
        indent(out, indent).append('"').append(escapeJson(key)).append("\": ").append(value);
        if (comma) {
            out.append(',');
        }
        out.append('\n');
    }

    static void appendJsonField(StringBuilder out, String key, boolean value, boolean comma, int indent) {
        indent(out, indent).append('"').append(escapeJson(key)).append("\": ").append(value);
        if (comma) {
            out.append(',');
        }
        out.append('\n');
    }

    static void appendJsonPeerArray(StringBuilder out, String key, List<Peer> peers, boolean comma, int indent) {
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

    static void appendJsonStringArray(StringBuilder out, String key, List<String> values, boolean comma, int indent) {
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

    static void appendJsonReplicationArray(StringBuilder out, String key, TelemetryResponse response, boolean comma, int indent) {
        indent(out, indent).append('"').append(escapeJson(key)).append("\": [\n");
        for (int i = 0; i < response.getReplication().size(); i++) {
            var status = response.getReplication().get(i);
            String freshness = OperationalCliSupport.describeFreshness(response.getObservedAtMillis(), status.lastSuccessfulContactMillis());
            String health = OperationalCliSupport.describeHealth(false, status.reachable(), freshness, status.consecutiveFailures());
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

    static void appendJsonPeerStatsArray(StringBuilder out, String key, TelemetryResponse response, boolean comma, int indent) {
        indent(out, indent).append('"').append(escapeJson(key)).append("\": [\n");
        for (int i = 0; i < response.getPeerStats().size(); i++) {
            var stats = response.getPeerStats().get(i);
            indent(out, indent + 1).append("{ ")
                    .append("\"peerId\": \"").append(escapeJson(stats.peerId())).append('"')
                    .append(", \"rpcType\": \"").append(escapeJson(stats.rpcType())).append('"')
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

    static void appendJsonClusterMembers(StringBuilder out, String key, List<org.gautelis.raft.protocol.ClusterMemberSummary> members, boolean comma, int indent) {
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
