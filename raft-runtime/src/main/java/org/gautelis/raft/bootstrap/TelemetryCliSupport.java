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
import org.gautelis.raft.transport.RaftTransportClient;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

final class TelemetryCliSupport {
    private TelemetryCliSupport() {
    }

    static String renderHeadline(TelemetryResponse response) {
        StringBuilder out = new StringBuilder();
        out.append("node=").append(response.getPeerId())
                .append(" state=").append(response.getState())
                .append(" term=").append(response.getTerm());
        if (!response.getLeaderId().isBlank()) {
            out.append(" leader=").append(response.getLeaderId());
        }
        out.append(" commit=").append(response.getCommitIndex())
                .append(" applied=").append(response.getLastApplied())
                .append(" health=").append(response.getClusterHealth())
                .append(" quorum=").append(response.isQuorumAvailable())
                .append(" currentQuorum=").append(response.isCurrentQuorumAvailable())
                .append(" nextQuorum=").append(response.isNextQuorumAvailable())
                .append(" healthyVoters=").append(response.getHealthyVotingMembers()).append('/').append(response.getVotingMembers());
        if (!response.getClusterStatusReason().isBlank()) {
            out.append(" reason=").append(response.getClusterStatusReason());
        }
        return out.toString();
    }

    static String render(TelemetryResponse response) {
        StringBuilder out = new StringBuilder();
        out.append("Node ").append(response.getPeerId())
                .append(" status=").append(response.getStatus())
                .append(" state=").append(response.getState())
                .append(" term=").append(response.getTerm());
        if (!response.getLeaderId().isBlank()) {
            out.append(" leader=").append(response.getLeaderId());
        }
        out.append('\n');
        out.append("Flags: joining=").append(response.isJoining())
                .append(" decommissioned=").append(response.isDecommissioned());
        if (!response.getVotedFor().isBlank()) {
            out.append(" votedFor=").append(response.getVotedFor());
        }
        out.append('\n');
        out.append("Log: commit=").append(response.getCommitIndex())
                .append(" applied=").append(response.getLastApplied())
                .append(" lastIndex=").append(response.getLastLogIndex())
                .append(" lastTerm=").append(response.getLastLogTerm())
                .append(" snapshot=").append(response.getSnapshotIndex()).append('@').append(response.getSnapshotTerm())
                .append('\n');
        out.append("Config: joint=").append(response.isJointConsensus())
                .append(" current=").append(OperationalCliSupport.renderPeers(response.getCurrentMembers()))
                .append(" next=").append(OperationalCliSupport.renderPeers(response.getNextMembers()))
                .append('\n');
        out.append("Peers: known=").append(OperationalCliSupport.renderPeers(response.getKnownPeers()))
                .append(" pendingJoins=").append(response.getPendingJoinIds())
                .append('\n');
        if (!response.getClusterHealth().isBlank()) {
            out.append("Cluster: health=").append(response.getClusterHealth())
                    .append(" reason=").append(response.getClusterStatusReason())
                    .append(" quorum=").append(response.isQuorumAvailable())
                    .append(" currentQuorum=").append(response.isCurrentQuorumAvailable())
                    .append(" nextQuorum=").append(response.isNextQuorumAvailable())
                    .append(" healthyVoters=").append(response.getHealthyVotingMembers()).append('/').append(response.getVotingMembers())
                    .append(" reachableVoters=").append(response.getReachableVotingMembers()).append('/').append(response.getVotingMembers());
            if (!response.getBlockingCurrentQuorumPeerIds().isEmpty()) {
                out.append(" blockingCurrent=").append(response.getBlockingCurrentQuorumPeerIds());
            }
            if (!response.getBlockingNextQuorumPeerIds().isEmpty()) {
                out.append(" blockingNext=").append(response.getBlockingNextQuorumPeerIds());
            }
            out.append('\n');
        }
        if (!response.getReplication().isEmpty()) {
            out.append("Replication:\n");
            for (var status : response.getReplication()) {
                out.append("  ").append(status.peerId())
                        .append(" next=").append(status.nextIndex())
                        .append(" match=").append(status.matchIndex())
                        .append(" failures=").append(status.consecutiveFailures())
                        .append('\n');
            }
        }
        OperationalCliSupport.appendClusterView(out, response);
        if (!response.getPeerStats().isEmpty()) {
            out.append("Transport stats:\n");
            for (var stats : response.getPeerStats()) {
                out.append("  ").append(stats.peerId())
                        .append("/").append(stats.rpcType().isBlank() ? "unknown" : stats.rpcType())
                        .append(" n=").append(stats.samples())
                        .append(" mean=").append(String.format(Locale.ROOT, "%.3f", stats.meanMillis())).append("ms")
                        .append(" min=").append(String.format(Locale.ROOT, "%.3f", stats.minMillis())).append("ms")
                        .append(" max=").append(String.format(Locale.ROOT, "%.3f", stats.maxMillis())).append("ms")
                        .append(" cv=").append(String.format(Locale.ROOT, "%.2f", stats.cvPercent())).append('%')
                        .append('\n');
            }
        }
        return out.toString().trim();
    }

    static String renderJson(TelemetryResponse response) {
        StringBuilder out = new StringBuilder();
        out.append("{\n");
        JsonFormatSupport.appendJsonField(out, "observedAtMillis", response.getObservedAtMillis(), true, 1);
        JsonFormatSupport.appendJsonField(out, "term", response.getTerm(), true, 1);
        JsonFormatSupport.appendJsonField(out, "peerId", response.getPeerId(), true, 1);
        JsonFormatSupport.appendJsonField(out, "success", response.isSuccess(), true, 1);
        JsonFormatSupport.appendJsonField(out, "status", response.getStatus(), true, 1);
        JsonFormatSupport.appendJsonField(out, "redirectLeaderId", response.getRedirectLeaderId(), true, 1);
        JsonFormatSupport.appendJsonField(out, "state", response.getState(), true, 1);
        JsonFormatSupport.appendJsonField(out, "leaderId", response.getLeaderId(), true, 1);
        JsonFormatSupport.appendJsonField(out, "votedFor", response.getVotedFor(), true, 1);
        JsonFormatSupport.appendJsonField(out, "joining", response.isJoining(), true, 1);
        JsonFormatSupport.appendJsonField(out, "decommissioned", response.isDecommissioned(), true, 1);
        JsonFormatSupport.appendJsonField(out, "commitIndex", response.getCommitIndex(), true, 1);
        JsonFormatSupport.appendJsonField(out, "lastApplied", response.getLastApplied(), true, 1);
        JsonFormatSupport.appendJsonField(out, "lastLogIndex", response.getLastLogIndex(), true, 1);
        JsonFormatSupport.appendJsonField(out, "lastLogTerm", response.getLastLogTerm(), true, 1);
        JsonFormatSupport.appendJsonField(out, "snapshotIndex", response.getSnapshotIndex(), true, 1);
        JsonFormatSupport.appendJsonField(out, "snapshotTerm", response.getSnapshotTerm(), true, 1);
        JsonFormatSupport.appendJsonField(out, "lastHeartbeatMillis", response.getLastHeartbeatMillis(), true, 1);
        JsonFormatSupport.appendJsonField(out, "nextElectionDeadlineMillis", response.getNextElectionDeadlineMillis(), true, 1);
        JsonFormatSupport.appendJsonField(out, "jointConsensus", response.isJointConsensus(), true, 1);
        JsonFormatSupport.appendJsonField(out, "clusterHealth", response.getClusterHealth(), true, 1);
        JsonFormatSupport.appendJsonField(out, "clusterStatusReason", response.getClusterStatusReason(), true, 1);
        JsonFormatSupport.appendJsonField(out, "quorumAvailable", response.isQuorumAvailable(), true, 1);
        JsonFormatSupport.appendJsonField(out, "currentQuorumAvailable", response.isCurrentQuorumAvailable(), true, 1);
        JsonFormatSupport.appendJsonField(out, "nextQuorumAvailable", response.isNextQuorumAvailable(), true, 1);
        JsonFormatSupport.appendJsonField(out, "votingMembers", response.getVotingMembers(), true, 1);
        JsonFormatSupport.appendJsonField(out, "healthyVotingMembers", response.getHealthyVotingMembers(), true, 1);
        JsonFormatSupport.appendJsonField(out, "reachableVotingMembers", response.getReachableVotingMembers(), true, 1);
        JsonFormatSupport.appendJsonField(out, "reconfigurationAgeMillis", response.getReconfigurationAgeMillis(), true, 1);
        JsonFormatSupport.appendJsonPeerArray(out, "currentMembers", response.getCurrentMembers(), true, 1);
        JsonFormatSupport.appendJsonPeerArray(out, "nextMembers", response.getNextMembers(), true, 1);
        JsonFormatSupport.appendJsonPeerArray(out, "knownPeers", response.getKnownPeers(), true, 1);
        JsonFormatSupport.appendJsonStringArray(out, "pendingJoinIds", response.getPendingJoinIds(), true, 1);
        JsonFormatSupport.appendJsonStringArray(out, "blockingCurrentQuorumPeerIds", response.getBlockingCurrentQuorumPeerIds(), true, 1);
        JsonFormatSupport.appendJsonStringArray(out, "blockingNextQuorumPeerIds", response.getBlockingNextQuorumPeerIds(), true, 1);
        JsonFormatSupport.appendJsonReplicationArray(out, "replication", response, true, 1);
        JsonFormatSupport.appendJsonPeerStatsArray(out, "peerStats", response, true, 1);
        JsonFormatSupport.appendJsonClusterMembers(out, "clusterMembers", response.getClusterMembers(), false, 1);
        out.append("}");
        return out.toString();
    }

    static String renderSummaryJson(TelemetryResponse response) {
        StringBuilder out = new StringBuilder();
        out.append("{\n");
        JsonFormatSupport.appendJsonField(out, "observedAtMillis", response.getObservedAtMillis(), true, 1);
        JsonFormatSupport.appendJsonField(out, "peerId", response.getPeerId(), true, 1);
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
        JsonFormatSupport.appendJsonField(out, "reconfigurationAgeMillis", response.getReconfigurationAgeMillis(), true, 1);
        JsonFormatSupport.appendJsonStringArray(out, "blockingCurrentQuorumPeerIds", response.getBlockingCurrentQuorumPeerIds(), true, 1);
        JsonFormatSupport.appendJsonStringArray(out, "blockingNextQuorumPeerIds", response.getBlockingNextQuorumPeerIds(), false, 1);
        out.append("}");
        return out.toString();
    }

    static TelemetryResponse fetch(RaftTransportClient client, Peer target, boolean includePeerStats, Supplier<String> authScheme, Supplier<String> authToken) throws Exception {
        TelemetryResponse response = client.sendTelemetryRequest(
                target,
                new org.gautelis.raft.protocol.TelemetryRequest(0L, "telemetry-cli", includePeerStats, true, authScheme.get(), authToken.get())
        ).get(5, TimeUnit.SECONDS);
        if ("REDIRECT".equals(response.getStatus())) {
            Peer redirectedLeader = resolveRedirectLeader(response);
            if (redirectedLeader != null) {
                response = client.sendTelemetryRequest(
                        redirectedLeader,
                        new org.gautelis.raft.protocol.TelemetryRequest(0L, "telemetry-cli", includePeerStats, true, authScheme.get(), authToken.get())
                ).get(5, TimeUnit.SECONDS);
            }
        }
        return response;
    }

    private static Peer resolveRedirectLeader(TelemetryResponse response) {
        if (response.getRedirectLeaderId().isBlank()) {
            return null;
        }
        for (Peer peer : response.getKnownPeers()) {
            if (response.getRedirectLeaderId().equals(peer.getId())) {
                return peer;
            }
        }
        return null;
    }
}
