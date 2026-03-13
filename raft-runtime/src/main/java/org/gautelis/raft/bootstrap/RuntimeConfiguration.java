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

import java.util.Locale;
import java.util.Set;

/**
 * Immutable runtime configuration loaded from JVM system properties.
 */
public record RuntimeConfiguration(
        String dataDir,
        int telemetryExportIntervalSeconds,
        int telemetryRateLimitPerMinute,
        long telemetryReconfigurationStuckMillis,
        int demoTelemetryIntervalSeconds,
        int demoTelemetryDetailEvery,
        String adapterMode,
        Set<String> commandAllowList,
        String authMode,
        String requestAuthScheme,
        String requestAuthToken,
        String sharedSecret
) {
    public static RuntimeConfiguration load() {
        return new RuntimeConfiguration(
                System.getProperty("raft.data.dir", "").trim(),
                Integer.getInteger("raft.telemetry.export.interval.seconds", 15),
                Integer.getInteger("raft.telemetry.rate.limit.per.minute", 30),
                Long.getLong("raft.telemetry.reconfiguration.stuck.millis", 60_000L),
                Integer.getInteger("raft.demo.telemetry.interval.seconds", 20),
                Math.max(1, Integer.getInteger("raft.demo.telemetry.detail.every", 6)),
                System.getProperty("raft.adapter.mode", "basic").trim().toLowerCase(Locale.ROOT),
                AllowListCommandAuthorizer.parseAllowList(System.getProperty("raft.command.authorizer.allow-list", "")),
                System.getProperty("raft.request.auth.mode", "none").trim().toLowerCase(Locale.ROOT),
                System.getProperty("raft.request.auth.client-scheme", ""),
                System.getProperty("raft.request.auth.client-token", ""),
                System.getProperty("raft.request.auth.shared-secret", "")
        );
    }

    public boolean hasDataDir() {
        return dataDir != null && !dataDir.isBlank();
    }

    public String requireSharedSecret() {
        if (sharedSecret == null || sharedSecret.isBlank()) {
            throw new IllegalArgumentException("Missing required system property: raft.request.auth.shared-secret");
        }
        return sharedSecret;
    }
}
