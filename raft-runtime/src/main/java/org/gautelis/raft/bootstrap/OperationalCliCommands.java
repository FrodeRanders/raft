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

import org.gautelis.raft.protocol.ClusterSummaryResponse;
import org.gautelis.raft.protocol.JoinClusterRequest;
import org.gautelis.raft.protocol.JoinClusterResponse;
import org.gautelis.raft.protocol.JoinClusterStatusRequest;
import org.gautelis.raft.protocol.JoinClusterStatusResponse;
import org.gautelis.raft.protocol.Peer;
import org.gautelis.raft.protocol.ReconfigurationStatusResponse;
import org.gautelis.raft.protocol.ReconfigureClusterRequest;
import org.gautelis.raft.protocol.ReconfigureClusterResponse;
import org.gautelis.raft.protocol.TelemetryResponse;
import org.gautelis.raft.transport.RaftTransportClient;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

final class OperationalCliCommands {
    private OperationalCliCommands() {
    }

    static void runTelemetry(String[] args, String usageLine, CliRuntimeContext context) {
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
            OperationalCliSupport.failUsage(usageLine);
        }

        Peer target = context.parsePeer().apply(args[targetIndex]);
        RaftTransportClient client = context.newClient("telemetry-cli");
        try {
            TelemetryResponse response = TelemetryCliSupport.fetch(client, target, true, context.authScheme(), context.authToken());
            if (!response.isSuccess()) {
                OperationalCliSupport.failRequest("Telemetry request failed for " + target.getId() + " status=" + response.getStatus());
            }
            System.out.println(summary ? TelemetryCliSupport.renderSummaryJson(response)
                    : (json ? TelemetryCliSupport.renderJson(response) : TelemetryCliSupport.render(response)));
        } catch (Exception e) {
            OperationalCliSupport.failRequest("Telemetry request failed: " + e.getMessage());
        } finally {
            client.shutdown();
        }
    }

    static void runClusterSummary(String[] args, String usageLine, CliRuntimeContext context) {
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
            OperationalCliSupport.failUsage(usageLine);
        }

        Peer target = context.parsePeer().apply(args[targetIndex]);
        RaftTransportClient client = context.newClient("cluster-summary-cli");
        try {
            ClusterSummaryResponse response = ClusterAdminCliSupport.fetchClusterSummary(client, target, context.authScheme(), context.authToken());
            if (!response.isSuccess()) {
                OperationalCliSupport.failRequest("Cluster summary request failed for " + target.getId() + " status=" + response.getStatus());
            }
            System.out.println(json ? ClusterAdminCliSupport.renderClusterSummaryJson(response) : ClusterAdminCliSupport.renderClusterSummary(response));
        } catch (Exception e) {
            OperationalCliSupport.failRequest("Cluster summary request failed: " + e.getMessage());
        } finally {
            client.shutdown();
        }
    }

    static void runReconfigurationStatus(String[] args, String usageLine, CliRuntimeContext context) {
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
            OperationalCliSupport.failUsage(usageLine);
        }

        Peer target = context.parsePeer().apply(args[targetIndex]);
        RaftTransportClient client = context.newClient("reconfiguration-status-cli");
        try {
            ReconfigurationStatusResponse response = ClusterAdminCliSupport.fetchReconfigurationStatus(client, target, context.authScheme(), context.authToken());
            if (!response.isSuccess()) {
                OperationalCliSupport.failRequest("Reconfiguration status request failed for " + target.getId() + " status=" + response.getStatus());
            }
            System.out.println(json ? ClusterAdminCliSupport.renderReconfigurationStatusJson(response)
                    : ClusterAdminCliSupport.renderReconfigurationStatus(response));
        } catch (Exception e) {
            OperationalCliSupport.failRequest("Reconfiguration status request failed: " + e.getMessage());
        } finally {
            client.shutdown();
        }
    }

    static void runReconfigure(String[] args, String usageLine, CliRuntimeContext context) {
        if (args.length < 3) {
            OperationalCliSupport.failUsage(usageLine);
        }

        ReconfigureClusterRequest.Action action;
        try {
            action = ReconfigureClusterRequest.Action.valueOf(args[1].trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            System.err.println("Unknown reconfigure action: " + args[1]);
            System.exit(1);
            return;
        }

        Peer target = context.parsePeer().apply(args[2]);
        List<Peer> members = new ArrayList<>();
        if (action == ReconfigureClusterRequest.Action.JOINT) {
            if (args.length < 4) {
                System.err.println("Joint reconfiguration requires at least one member");
                System.exit(1);
            }
            for (int i = 3; i < args.length; i++) {
                members.add(context.parsePeer().apply(args[i]));
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
            members.add(context.parsePeer().apply(args[3]));
        }

        RaftTransportClient client = context.newClient("reconfigure-cli");
        try {
            ReconfigureClusterResponse response = client.sendReconfigureClusterRequest(
                    target,
                    new ReconfigureClusterRequest(0L, "reconfigure-cli", action, members, context.authScheme().get(), context.authToken().get())
            ).get(5, TimeUnit.SECONDS);
            System.out.println(ClusterAdminCliSupport.renderReconfigureResponse(action, response));
            if (!response.isSuccess()) {
                OperationalCliSupport.failRequest("Reconfigure action failed status=" + response.getStatus());
            }
        } catch (Exception e) {
            OperationalCliSupport.failRequest("Reconfigure request failed: " + e.getMessage());
        } finally {
            client.shutdown();
        }
    }

    static void runJoinRequest(String[] args, String usageLine, CliRuntimeContext context) {
        if (args.length != 3) {
            OperationalCliSupport.failUsage(usageLine);
        }

        Peer target = context.parsePeer().apply(args[1]);
        Peer joining = context.parsePeer().apply(args[2]);
        RaftTransportClient client = context.newClient("join-cli");
        try {
            JoinClusterResponse response = client.sendJoinClusterRequest(
                    target,
                    new JoinClusterRequest(0L, "join-cli", joining, context.authScheme().get(), context.authToken().get())
            ).get(5, TimeUnit.SECONDS);
            System.out.println(ClusterAdminCliSupport.renderJoinResponse(response));
            if (!response.isSuccess()) {
                OperationalCliSupport.failRequest("Join request failed for " + target.getId() + " status=" + response.getStatus());
            }
        } catch (Exception e) {
            OperationalCliSupport.failRequest("Join request failed: " + e.getMessage());
        } finally {
            client.shutdown();
        }
    }

    static void runJoinStatus(String[] args, String usageLine, CliRuntimeContext context) {
        if (args.length != 3) {
            OperationalCliSupport.failUsage(usageLine);
        }

        Peer target = context.parsePeer().apply(args[1]);
        String joiningPeerId = args[2].trim();
        RaftTransportClient client = context.newClient("join-status-cli");
        try {
            JoinClusterStatusResponse response = client.sendJoinClusterStatusRequest(
                    target,
                    new JoinClusterStatusRequest(0L, "join-status-cli", joiningPeerId, context.authScheme().get(), context.authToken().get())
            ).get(5, TimeUnit.SECONDS);
            System.out.println(ClusterAdminCliSupport.renderJoinStatusResponse(response, joiningPeerId));
            if (!response.isSuccess()) {
                OperationalCliSupport.failRequest("Join status request failed for " + target.getId() + " status=" + response.getStatus());
            }
        } catch (Exception e) {
            OperationalCliSupport.failRequest("Join status request failed: " + e.getMessage());
        } finally {
            client.shutdown();
        }
    }
}
