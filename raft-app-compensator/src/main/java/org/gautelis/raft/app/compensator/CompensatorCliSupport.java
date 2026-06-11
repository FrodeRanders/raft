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
package org.gautelis.raft.app.compensator;

import org.gautelis.raft.bootstrap.CliRuntimeContext;
import org.gautelis.raft.protocol.ClusterMemberSummary;
import org.gautelis.raft.protocol.ClusterSummaryRequest;
import org.gautelis.raft.protocol.ClusterSummaryResponse;
import org.gautelis.raft.protocol.Peer;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * CLI support for the compensator work-partitioning demo.
 */
public final class CompensatorCliSupport {

    private CompensatorCliSupport() {}

    static void runStatus(String[] args, CliRuntimeContext context) {
        boolean json = false;
        int offset = 1;

        if (args.length > offset && "--json".equals(args[offset])) {
            json = true;
            offset++;
        }

        if (args.length <= offset) {
            System.err.println("Usage: compensator-status [--json] <target>");
            System.exit(1);
            return;
        }

        String targetSpec = args[offset];
        Peer targetPeer = context.parsePeer().apply(targetSpec);

        ClusterSummaryRequest request = new ClusterSummaryRequest(
                0, "compensator-status",
                "none", ""
        );

        try {
            ClusterSummaryResponse response = context.newClient("compensator-status")
                    .sendClusterSummaryRequest(targetPeer, request)
                    .get(5, TimeUnit.SECONDS);

            if (json) {
                System.out.println(renderJson(response));
            } else {
                System.out.println(renderText(response));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("Request interrupted");
        } catch (TimeoutException e) {
            System.err.println("Request timed out after 5 seconds");
        } catch (ExecutionException e) {
            System.err.println("Request failed: " + e.getCause().getMessage());
        }
    }

    private static String renderText(ClusterSummaryResponse response) {
        StringBuilder sb = new StringBuilder();
        sb.append("Compensator cluster status\n");
        sb.append("  Voting members: ").append(response.getVotingMembers()).append('\n');
        sb.append("  Healthy: ").append(response.getHealthyVotingMembers()).append('\n');
        sb.append("  Reachable: ").append(response.getReachableVotingMembers()).append('\n');
        sb.append('\n');

        List<ClusterMemberSummary> members = response.getMembers();
        if (members != null && !members.isEmpty()) {
            sb.append(String.format("  %-30s %-10s %-12s %-12s %-8s%n",
                    "Peer ID", "Role", "Health", "Reachable", "Member"));
            sb.append("  ").append("-".repeat(90)).append('\n');

            List<ClusterMemberSummary> sorted = members.stream()
                    .sorted(java.util.Comparator.comparing(ClusterMemberSummary::peerId))
                    .toList();

            for (ClusterMemberSummary m : sorted) {
                sb.append(String.format("  %-30s %-10s %-12s %-12s %-8s%n",
                        m.peerId(),
                        m.voting() ? "voter" : "learner",
                        m.health(),
                        m.reachable() ? "yes" : "no",
                        m.currentMember() ? "yes" : "no"));
            }
        }

        sb.append("\n  Work partitioning (deterministic, based on sorted voting member IDs):\n");
        List<ClusterMemberSummary> voters = members.stream()
                .filter(ClusterMemberSummary::voting)
                .sorted(java.util.Comparator.comparing(ClusterMemberSummary::peerId))
                .toList();
        int total = voters.size();
        for (int i = 0; i < total; i++) {
            ClusterMemberSummary v = voters.get(i);
            sb.append(String.format("    Compensator %d of %d: %s (%s)%n", i, total, v.peerId(), v.role()));
        }

        return sb.toString();
    }

    private static String renderJson(ClusterSummaryResponse response) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\n");
        sb.append("  \"votingMembers\": ").append(response.getVotingMembers()).append(",\n");
        sb.append("  \"healthyVotingMembers\": ").append(response.getHealthyVotingMembers()).append(",\n");
        sb.append("  \"reachableVotingMembers\": ").append(response.getReachableVotingMembers()).append(",\n");
        sb.append("  \"members\": [\n");

        List<ClusterMemberSummary> members = response.getMembers();
        if (members != null) {
            List<ClusterMemberSummary> sorted = members.stream()
                    .sorted(java.util.Comparator.comparing(ClusterMemberSummary::peerId))
                    .toList();
            for (int i = 0; i < sorted.size(); i++) {
                ClusterMemberSummary m = sorted.get(i);
                sb.append("    {");
                sb.append("\"peerId\": \"").append(m.peerId()).append("\", ");
                sb.append("\"voting\": ").append(m.voting()).append(", ");
                sb.append("\"role\": \"").append(m.role()).append("\", ");
                sb.append("\"health\": \"").append(m.health()).append("\", ");
                sb.append("\"reachable\": ").append(m.reachable()).append(", ");
                sb.append("\"currentMember\": ").append(m.currentMember());
                sb.append("}");
                if (i < sorted.size() - 1) sb.append(",");
                sb.append('\n');
            }
        }
        sb.append("  ]\n");
        sb.append("}\n");
        return sb.toString();
    }
}
