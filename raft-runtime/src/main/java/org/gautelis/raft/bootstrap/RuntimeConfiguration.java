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
        String sharedSecret,
        String nodeId,
        String bindHost,
        int bindPort,
        String advertiseHost,
        int advertisePort,
        String clusterSrv,
        boolean bootstrapNewCluster
) {
    public static RuntimeConfiguration load() {
        return new RuntimeConfiguration(
                configured("raft.data.dir", "RAFT_DATA_DIR", "").trim(),
                Integer.getInteger("raft.telemetry.export.interval.seconds", 15),
                Integer.getInteger("raft.telemetry.rate.limit.per.minute", 30),
                Long.getLong("raft.telemetry.reconfiguration.stuck.millis", 60_000L),
                Integer.getInteger("raft.demo.telemetry.interval.seconds", 20),
                Math.max(1, Integer.getInteger("raft.demo.telemetry.detail.every", 6)),
                configured("raft.adapter.mode", "RAFT_ADAPTER_MODE", "basic").trim().toLowerCase(Locale.ROOT),
                AllowListCommandAuthorizer.parseAllowList(configured("raft.command.authorizer.allow-list", "RAFT_COMMAND_AUTHORIZER_ALLOW_LIST", "")),
                configured("raft.request.auth.mode", "RAFT_REQUEST_AUTH_MODE", "none").trim().toLowerCase(Locale.ROOT),
                configured("raft.request.auth.client-scheme", "RAFT_REQUEST_AUTH_CLIENT_SCHEME", ""),
                configured("raft.request.auth.client-token", "RAFT_REQUEST_AUTH_CLIENT_TOKEN", ""),
                configured("raft.request.auth.shared-secret", "RAFT_REQUEST_AUTH_SHARED_SECRET", ""),
                configured("raft.node.id", "RAFT_NODE_ID", "").trim(),
                configured("raft.bind.host", "RAFT_BIND_HOST", "0.0.0.0").trim(),
                configuredInt("raft.bind.port", "RAFT_BIND_PORT", 0),
                configured("raft.advertise.host", "RAFT_ADVERTISE_HOST", "").trim(),
                configuredInt("raft.advertise.port", "RAFT_ADVERTISE_PORT", 0),
                configured("raft.cluster.srv", "RAFT_CLUSTER_SRV", "").trim(),
                configuredBoolean("raft.bootstrap.new-cluster", "RAFT_BOOTSTRAP_NEW_CLUSTER", false)
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

    public boolean hasClusterSrv() {
        return clusterSrv != null && !clusterSrv.isBlank();
    }

    public boolean hasNodeId() {
        return nodeId != null && !nodeId.isBlank();
    }

    private static String configured(String propertyName, String environmentName, String defaultValue) {
        String property = System.getProperty(propertyName);
        if (property != null) {
            return property;
        }
        String environment = System.getenv(environmentName);
        return environment == null ? defaultValue : environment;
    }

    private static int configuredInt(String propertyName, String environmentName, int defaultValue) {
        String value = configured(propertyName, environmentName, "");
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        return Integer.parseInt(value.trim());
    }

    private static boolean configuredBoolean(String propertyName, String environmentName, boolean defaultValue) {
        String value = configured(propertyName, environmentName, "");
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        return switch (value.trim().toLowerCase(Locale.ROOT)) {
            case "true", "1", "yes", "on" -> true;
            case "false", "0", "no", "off" -> false;
            default -> throw new IllegalArgumentException("Invalid boolean value for " + propertyName + ": " + value);
        };
    }
}
