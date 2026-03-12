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

import org.gautelis.raft.RaftNode;
import org.gautelis.raft.protocol.TelemetryPeerStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

final class OpenTelemetryJsonExporter implements TelemetryExporter {
    private static final Logger otelLog = LoggerFactory.getLogger("OTEL");

    private final String mode;

    OpenTelemetryJsonExporter(String mode) {
        this.mode = mode;
    }

    @Override
    public void publish(RaftNode.TelemetrySnapshot snapshot, List<TelemetryPeerStats> peerStats) {
        if (snapshot == null || !otelLog.isInfoEnabled()) {
            return;
        }
        StringBuilder out = new StringBuilder();
        out.append('{');
        appendString(out, "exporter", mode);
        appendLong(out, "observedAtMillis", snapshot.observedAtMillis());
        appendString(out, "peerId", snapshot.peerId());
        appendString(out, "state", snapshot.state());
        appendLong(out, "term", snapshot.term());
        appendString(out, "leaderId", snapshot.leaderId() == null ? "" : snapshot.leaderId());
        appendLong(out, "commitIndex", snapshot.commitIndex());
        appendLong(out, "lastApplied", snapshot.lastApplied());
        appendLong(out, "lastLogIndex", snapshot.lastLogIndex());
        appendBoolean(out, "jointConsensus", snapshot.configuration().isJointConsensus());
        appendBoolean(out, "joining", snapshot.joining());
        appendBoolean(out, "decommissioned", snapshot.decommissioned());
        appendLong(out, "knownPeers", snapshot.knownPeers().size());
        appendLong(out, "currentMembers", snapshot.configuration().currentMembers().size());
        appendLong(out, "nextMembers", snapshot.configuration().nextMembers().size());
        out.append("\"peerStats\":[");
        for (int i = 0; i < peerStats.size(); i++) {
            TelemetryPeerStats stats = peerStats.get(i);
            if (i > 0) {
                out.append(',');
            }
            out.append('{');
            appendString(out, "peerId", stats.peerId());
            appendString(out, "rpcType", stats.rpcType());
            appendLong(out, "samples", stats.samples());
            appendDouble(out, "meanMillis", stats.meanMillis());
            appendDouble(out, "minMillis", stats.minMillis());
            appendDouble(out, "maxMillis", stats.maxMillis());
            appendDouble(out, "cvPercent", stats.cvPercent());
            trimComma(out);
            out.append('}');
        }
        out.append("]}");
        otelLog.info(out.toString());
    }

    private static void appendString(StringBuilder out, String key, String value) {
        out.append('"').append(escape(key)).append("\":\"").append(escape(value)).append("\",");
    }

    private static void appendLong(StringBuilder out, String key, long value) {
        out.append('"').append(escape(key)).append("\":").append(value).append(',');
    }

    private static void appendDouble(StringBuilder out, String key, double value) {
        out.append('"').append(escape(key)).append("\":").append(String.format(java.util.Locale.ROOT, "%.3f", value)).append(',');
    }

    private static void appendBoolean(StringBuilder out, String key, boolean value) {
        out.append('"').append(escape(key)).append("\":").append(value).append(',');
    }

    private static void trimComma(StringBuilder out) {
        if (!out.isEmpty() && out.charAt(out.length() - 1) == ',') {
            out.setLength(out.length() - 1);
        }
    }

    private static String escape(String value) {
        return value == null ? "" : value.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
