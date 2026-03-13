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

import org.gautelis.raft.protocol.ClusterSummaryRequest;
import org.gautelis.raft.protocol.ClusterSummaryResponse;
import org.gautelis.raft.protocol.JoinClusterResponse;
import org.gautelis.raft.protocol.JoinClusterStatusResponse;
import org.gautelis.raft.protocol.Peer;
import org.gautelis.raft.protocol.ReconfigurationStatusRequest;
import org.gautelis.raft.protocol.ReconfigurationStatusResponse;
import org.gautelis.raft.protocol.ReconfigureClusterRequest;
import org.gautelis.raft.protocol.ReconfigureClusterResponse;
import org.gautelis.raft.transport.RaftTransportClient;

import java.net.InetSocketAddress;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

final class ClusterAdminCliSupport {
    private ClusterAdminCliSupport() {
    }

    static ClusterSummaryResponse fetchClusterSummary(RaftTransportClient client, Peer target, Supplier<String> authScheme, Supplier<String> authToken) throws Exception {
        ClusterSummaryResponse response = client.sendClusterSummaryRequest(
                target,
                new ClusterSummaryRequest(0L, "cluster-summary-cli", authScheme.get(), authToken.get())
        ).get(5, TimeUnit.SECONDS);
        if ("REDIRECT".equals(response.getStatus()) && !response.getRedirectLeaderHost().isBlank() && response.getRedirectLeaderPort() > 0) {
            response = client.sendClusterSummaryRequest(
                    new Peer(response.getRedirectLeaderId(), new InetSocketAddress(response.getRedirectLeaderHost(), response.getRedirectLeaderPort())),
                    new ClusterSummaryRequest(0L, "cluster-summary-cli", authScheme.get(), authToken.get())
            ).get(5, TimeUnit.SECONDS);
        }
        return response;
    }

    static ReconfigurationStatusResponse fetchReconfigurationStatus(RaftTransportClient client, Peer target, Supplier<String> authScheme, Supplier<String> authToken) throws Exception {
        ReconfigurationStatusResponse response = client.sendReconfigurationStatusRequest(
                target,
                new ReconfigurationStatusRequest(0L, "reconfiguration-status-cli", authScheme.get(), authToken.get())
        ).get(5, TimeUnit.SECONDS);
        if ("REDIRECT".equals(response.getStatus()) && !response.getRedirectLeaderHost().isBlank() && response.getRedirectLeaderPort() > 0) {
            response = client.sendReconfigurationStatusRequest(
                    new Peer(response.getRedirectLeaderId(), new InetSocketAddress(response.getRedirectLeaderHost(), response.getRedirectLeaderPort())),
                    new ReconfigurationStatusRequest(0L, "reconfiguration-status-cli", authScheme.get(), authToken.get())
            ).get(5, TimeUnit.SECONDS);
        }
        return response;
    }

    static String renderClusterSummary(ClusterSummaryResponse response) {
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

    static String renderClusterSummaryJson(ClusterSummaryResponse response) {
        StringBuilder out = new StringBuilder();
        out.append("{\n");
        JsonFormatSupport.appendJsonField(out, "observedAtMillis", response.getObservedAtMillis(), true, 1);
        JsonFormatSupport.appendJsonField(out, "peerId", response.getPeerId(), true, 1);
        JsonFormatSupport.appendJsonField(out, "success", response.isSuccess(), true, 1);
        JsonFormatSupport.appendJsonField(out, "status", response.getStatus(), true, 1);
        JsonFormatSupport.appendJsonField(out, "redirectLeaderId", response.getRedirectLeaderId(), true, 1);
        JsonFormatSupport.appendJsonField(out, "redirectLeaderHost", response.getRedirectLeaderHost(), true, 1);
        JsonFormatSupport.appendJsonField(out, "redirectLeaderPort", response.getRedirectLeaderPort(), true, 1);
        JsonFormatSupport.appendJsonField(out, "state", response.getState(), true, 1);
        JsonFormatSupport.appendJsonField(out, "leaderId", response.getLeaderId(), true, 1);
        JsonFormatSupport.appendJsonField(out, "term", response.getTerm(), true, 1);
        JsonFormatSupport.appendJsonField(out, "jointConsensus", response.isJointConsensus(), true, 1);
        JsonFormatSupport.appendJsonField(out, "clusterHealth", response.getClusterHealth(), true, 1);
        JsonFormatSupport.appendJsonField(out, "clusterStatusReason", response.getClusterStatusReason(), true, 1);
        JsonFormatSupport.appendJsonField(out, "quorumAvailable", response.isQuorumAvailable(), true, 1);
        JsonFormatSupport.appendJsonField(out, "currentQuorumAvailable", response.isCurrentQuorumAvailable(), true, 1);
        JsonFormatSupport.appendJsonField(out, "nextQuorumAvailable", response.isNextQuorumAvailable(), true, 1);
        JsonFormatSupport.appendJsonField(out, "votingMembers", response.getVotingMembers(), true, 1);
        JsonFormatSupport.appendJsonField(out, "healthyVotingMembers", response.getHealthyVotingMembers(), true, 1);
        JsonFormatSupport.appendJsonField(out, "reachableVotingMembers", response.getReachableVotingMembers(), true, 1);
        JsonFormatSupport.appendJsonStringArray(out, "blockingCurrentQuorumPeerIds", response.getBlockingCurrentQuorumPeerIds(), true, 1);
        JsonFormatSupport.appendJsonStringArray(out, "blockingNextQuorumPeerIds", response.getBlockingNextQuorumPeerIds(), true, 1);
        JsonFormatSupport.appendJsonClusterMembers(out, "members", response.getMembers(), false, 1);
        out.append("}");
        return out.toString();
    }

    static String renderReconfigurationStatus(ReconfigurationStatusResponse response) {
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

    static String renderReconfigurationStatusJson(ReconfigurationStatusResponse response) {
        StringBuilder out = new StringBuilder();
        out.append("{\n");
        JsonFormatSupport.appendJsonField(out, "observedAtMillis", response.getObservedAtMillis(), true, 1);
        JsonFormatSupport.appendJsonField(out, "peerId", response.getPeerId(), true, 1);
        JsonFormatSupport.appendJsonField(out, "success", response.isSuccess(), true, 1);
        JsonFormatSupport.appendJsonField(out, "status", response.getStatus(), true, 1);
        JsonFormatSupport.appendJsonField(out, "redirectLeaderId", response.getRedirectLeaderId(), true, 1);
        JsonFormatSupport.appendJsonField(out, "redirectLeaderHost", response.getRedirectLeaderHost(), true, 1);
        JsonFormatSupport.appendJsonField(out, "redirectLeaderPort", response.getRedirectLeaderPort(), true, 1);
        JsonFormatSupport.appendJsonField(out, "state", response.getState(), true, 1);
        JsonFormatSupport.appendJsonField(out, "leaderId", response.getLeaderId(), true, 1);
        JsonFormatSupport.appendJsonField(out, "term", response.getTerm(), true, 1);
        JsonFormatSupport.appendJsonField(out, "reconfigurationActive", response.isReconfigurationActive(), true, 1);
        JsonFormatSupport.appendJsonField(out, "jointConsensus", response.isJointConsensus(), true, 1);
        JsonFormatSupport.appendJsonField(out, "reconfigurationAgeMillis", response.getReconfigurationAgeMillis(), true, 1);
        JsonFormatSupport.appendJsonField(out, "clusterHealth", response.getClusterHealth(), true, 1);
        JsonFormatSupport.appendJsonField(out, "clusterStatusReason", response.getClusterStatusReason(), true, 1);
        JsonFormatSupport.appendJsonField(out, "quorumAvailable", response.isQuorumAvailable(), true, 1);
        JsonFormatSupport.appendJsonField(out, "currentQuorumAvailable", response.isCurrentQuorumAvailable(), true, 1);
        JsonFormatSupport.appendJsonField(out, "nextQuorumAvailable", response.isNextQuorumAvailable(), true, 1);
        JsonFormatSupport.appendJsonStringArray(out, "blockingCurrentQuorumPeerIds", response.getBlockingCurrentQuorumPeerIds(), true, 1);
        JsonFormatSupport.appendJsonStringArray(out, "blockingNextQuorumPeerIds", response.getBlockingNextQuorumPeerIds(), true, 1);
        JsonFormatSupport.appendJsonClusterMembers(out, "members", response.getMembers(), false, 1);
        out.append("}");
        return out.toString();
    }

    static String renderReconfigureResponse(ReconfigureClusterRequest.Action action, ReconfigureClusterResponse response) {
        return OperationalCliSupport.renderReconfigureResponse(action, response);
    }

    static String renderJoinResponse(JoinClusterResponse response) {
        return OperationalCliSupport.renderJoinResponse(response);
    }

    static String renderJoinStatusResponse(JoinClusterStatusResponse response, String joiningPeerId) {
        return OperationalCliSupport.renderJoinStatusResponse(response, joiningPeerId);
    }
}
