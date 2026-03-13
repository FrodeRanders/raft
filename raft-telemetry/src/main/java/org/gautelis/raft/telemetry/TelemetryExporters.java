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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;

/**
 * Selects and builds the configured telemetry exporter implementation.
 */
public final class TelemetryExporters {
    private static final Logger log = LoggerFactory.getLogger(TelemetryExporters.class);

    private TelemetryExporters() {
    }

    public static TelemetryExporter createFromProperties() {
        String exporter = System.getProperty("raft.telemetry.exporter", "none").trim().toLowerCase(Locale.ROOT);
        return switch (exporter) {
            case "", "none" -> new NoopTelemetryExporter();
            case "prometheus" -> new PrometheusTelemetryExporter();
            case "opentelemetry", "otlp" -> new OtlpHttpTelemetryExporter(exporter);
            case "otel-log", "opentelemetry-log" -> new OpenTelemetryJsonExporter(exporter);
            default -> {
                log.warn("Unknown telemetry exporter '{}'; using no-op exporter", exporter);
                yield new NoopTelemetryExporter();
            }
        };
    }

    private static final class NoopTelemetryExporter implements TelemetryExporter {
        @Override
        public boolean isEnabled() {
            return false;
        }

        @Override
        public void publish(org.gautelis.raft.RaftNode.TelemetrySnapshot snapshot, java.util.List<org.gautelis.raft.protocol.TelemetryPeerStats> peerStats) {
        }
    }
}
