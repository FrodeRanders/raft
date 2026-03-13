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

import org.gautelis.raft.ClusterConfiguration;
import org.gautelis.raft.RaftNode;
import org.gautelis.raft.protocol.ClusterMemberSummary;
import org.gautelis.raft.protocol.ClusterSummaryRequest;
import org.gautelis.raft.protocol.ClusterSummaryResponse;
import org.gautelis.raft.protocol.Peer;
import org.gautelis.raft.protocol.ReconfigurationStatusRequest;
import org.gautelis.raft.protocol.ReconfigurationStatusResponse;
import org.gautelis.raft.protocol.TelemetryRequest;
import org.gautelis.raft.protocol.TelemetryResponse;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

final class OperationalRequestHandler {
    private final AdapterHandlerContext context;
    private final Map<String, Deque<Long>> telemetryRequestHistory = new HashMap<>();

    OperationalRequestHandler(AdapterHandlerContext context) {
        this.context = context;
    }

    TelemetryResponse handleTelemetryRequest(TelemetryRequest request) {
        RaftNode stateMachine = context.stateMachineSupplier().get();
        if (stateMachine == null || request == null) {
            return emptyTelemetry("INVALID");
        }
        ClientCommandAuthenticationResult authentication = context.authenticator().apply(new AdapterRequestAuth(
                request.getPeerId(), request.getAuthScheme(), request.getAuthToken()
        ));
        if (!authentication.authenticated()) {
            return emptyTelemetry(authentication.status());
        }
        if (!allowTelemetryRequest(request.getPeerId())) {
            return emptyTelemetry("RATE_LIMITED");
        }
        if (request.isRequireLeaderSummary() && !stateMachine.isLeader()) {
            RaftNode.TelemetrySnapshot snapshot = stateMachine.telemetrySnapshot();
            Peer leader = stateMachine.getKnownLeaderPeer();
            return new TelemetryResponse(
                    snapshot.observedAtMillis(),
                    snapshot.term(),
                    snapshot.peerId(),
                    false,
                    leader == null ? "NO_LEADER" : "REDIRECT",
                    leader == null ? "" : leader.getId(),
                    snapshot.state(),
                    snapshot.leaderId(),
                    snapshot.votedFor(),
                    snapshot.joining(),
                    snapshot.decommissioned(),
                    snapshot.commitIndex(),
                    snapshot.lastApplied(),
                    snapshot.lastLogIndex(),
                    snapshot.lastLogTerm(),
                    snapshot.snapshotIndex(),
                    snapshot.snapshotTerm(),
                    snapshot.lastHeartbeatMillis(),
                    snapshot.nextElectionDeadlineMillis(),
                    snapshot.configuration().isJointConsensus(),
                    List.copyOf(snapshot.configuration().currentMembers()),
                    List.copyOf(snapshot.configuration().nextMembers()),
                    snapshot.knownPeers(),
                    snapshot.pendingJoinIds(),
                    snapshot.replication(),
                    List.of(),
                    "",
                    false,
                    false,
                    false,
                    0,
                    0,
                    0,
                    0L,
                    "",
                    List.of(),
                    List.of(),
                    List.of()
            );
        }
        RaftNode.TelemetrySnapshot snapshot = stateMachine.telemetrySnapshot();
        List<org.gautelis.raft.protocol.TelemetryPeerStats> peerStats = request.isIncludePeerStats()
                ? stateMachine.getRaftClient().snapshotResponseTimeStats()
                : List.of();
        ClusterTelemetrySummary clusterSummary = summarizeCluster(snapshot);
        return new TelemetryResponse(
                snapshot.observedAtMillis(),
                snapshot.term(),
                snapshot.peerId(),
                true,
                "OK",
                "",
                snapshot.state(),
                snapshot.leaderId(),
                snapshot.votedFor(),
                snapshot.joining(),
                snapshot.decommissioned(),
                snapshot.commitIndex(),
                snapshot.lastApplied(),
                snapshot.lastLogIndex(),
                snapshot.lastLogTerm(),
                snapshot.snapshotIndex(),
                snapshot.snapshotTerm(),
                snapshot.lastHeartbeatMillis(),
                snapshot.nextElectionDeadlineMillis(),
                snapshot.configuration().isJointConsensus(),
                List.copyOf(snapshot.configuration().currentMembers()),
                List.copyOf(snapshot.configuration().nextMembers()),
                snapshot.knownPeers(),
                snapshot.pendingJoinIds(),
                snapshot.replication(),
                peerStats,
                clusterSummary.clusterHealth(),
                clusterSummary.quorumAvailable(),
                clusterSummary.currentQuorumAvailable(),
                clusterSummary.nextQuorumAvailable(),
                clusterSummary.votingMembers(),
                clusterSummary.healthyVotingMembers(),
                clusterSummary.reachableVotingMembers(),
                clusterSummary.reconfigurationAgeMillis(),
                clusterSummary.clusterStatusReason(),
                clusterSummary.blockingCurrentQuorumPeerIds(),
                clusterSummary.blockingNextQuorumPeerIds(),
                clusterSummary.members()
        );
    }

    ClusterSummaryResponse handleClusterSummaryRequest(ClusterSummaryRequest request) {
        RaftNode stateMachine = context.stateMachineSupplier().get();
        if (stateMachine == null || request == null) {
            return emptyClusterSummary("INVALID");
        }
        ClientCommandAuthenticationResult authentication = context.authenticator().apply(new AdapterRequestAuth(
                request.getPeerId(), request.getAuthScheme(), request.getAuthToken()
        ));
        if (!authentication.authenticated()) {
            return emptyClusterSummary(authentication.status());
        }
        if (!allowTelemetryRequest(request.getPeerId())) {
            return emptyClusterSummary("RATE_LIMITED");
        }
        RaftNode.TelemetrySnapshot snapshot = stateMachine.telemetrySnapshot();
        if (!stateMachine.isLeader()) {
            Peer leader = stateMachine.getKnownLeaderPeer();
            return new ClusterSummaryResponse(
                    snapshot.observedAtMillis(),
                    snapshot.term(),
                    snapshot.peerId(),
                    false,
                    leader == null ? "NO_LEADER" : "REDIRECT",
                    leaderId(leader),
                    leaderHost(leader),
                    leaderPort(leader),
                    snapshot.state(),
                    snapshot.leaderId(),
                    snapshot.configuration().isJointConsensus(),
                    "",
                    "",
                    false,
                    false,
                    false,
                    0,
                    0,
                    0,
                    0L,
                    List.of(),
                    List.of(),
                    List.of()
            );
        }
        ClusterTelemetrySummary clusterSummary = summarizeCluster(snapshot);
        return new ClusterSummaryResponse(
                snapshot.observedAtMillis(),
                snapshot.term(),
                snapshot.peerId(),
                true,
                "OK",
                "",
                "",
                0,
                snapshot.state(),
                snapshot.leaderId(),
                snapshot.configuration().isJointConsensus(),
                clusterSummary.clusterHealth(),
                clusterSummary.clusterStatusReason(),
                clusterSummary.quorumAvailable(),
                clusterSummary.currentQuorumAvailable(),
                clusterSummary.nextQuorumAvailable(),
                clusterSummary.votingMembers(),
                clusterSummary.healthyVotingMembers(),
                clusterSummary.reachableVotingMembers(),
                clusterSummary.reconfigurationAgeMillis(),
                clusterSummary.blockingCurrentQuorumPeerIds(),
                clusterSummary.blockingNextQuorumPeerIds(),
                clusterSummary.members()
        );
    }

    ReconfigurationStatusResponse handleReconfigurationStatusRequest(ReconfigurationStatusRequest request) {
        RaftNode stateMachine = context.stateMachineSupplier().get();
        if (stateMachine == null || request == null) {
            return emptyReconfigurationStatus("INVALID");
        }
        ClientCommandAuthenticationResult authentication = context.authenticator().apply(new AdapterRequestAuth(
                request.getPeerId(), request.getAuthScheme(), request.getAuthToken()
        ));
        if (!authentication.authenticated()) {
            return emptyReconfigurationStatus(authentication.status());
        }
        if (!allowTelemetryRequest(request.getPeerId())) {
            return emptyReconfigurationStatus("RATE_LIMITED");
        }
        RaftNode.TelemetrySnapshot snapshot = stateMachine.telemetrySnapshot();
        if (!stateMachine.isLeader()) {
            Peer leader = stateMachine.getKnownLeaderPeer();
            return new ReconfigurationStatusResponse(
                    snapshot.observedAtMillis(),
                    snapshot.term(),
                    snapshot.peerId(),
                    false,
                    leader == null ? "NO_LEADER" : "REDIRECT",
                    leaderId(leader),
                    leaderHost(leader),
                    leaderPort(leader),
                    snapshot.state(),
                    snapshot.leaderId(),
                    false,
                    snapshot.configuration().isJointConsensus(),
                    0L,
                    "",
                    "",
                    false,
                    false,
                    false,
                    List.of(),
                    List.of(),
                    List.of()
            );
        }
        ClusterTelemetrySummary clusterSummary = summarizeCluster(snapshot);
        boolean jointConsensus = snapshot.latestKnownConfiguration().isJointConsensus();
        boolean reconfigurationActive = jointConsensus || clusterSummary.reconfigurationAgeMillis() > 0L;
        List<String> blockingCurrentQuorumPeerIds = clusterSummary.blockingCurrentQuorumPeerIds().isEmpty()
                ? clusterSummary.members().stream()
                .filter(member -> member.blockingQuorums() != null && member.blockingQuorums().contains("current"))
                .map(ClusterMemberSummary::peerId)
                .toList()
                : clusterSummary.blockingCurrentQuorumPeerIds();
        List<String> blockingNextQuorumPeerIds = clusterSummary.blockingNextQuorumPeerIds().isEmpty()
                ? clusterSummary.members().stream()
                .filter(member -> member.blockingQuorums() != null && member.blockingQuorums().contains("next"))
                .map(ClusterMemberSummary::peerId)
                .toList()
                : clusterSummary.blockingNextQuorumPeerIds();
        return new ReconfigurationStatusResponse(
                snapshot.observedAtMillis(),
                snapshot.term(),
                snapshot.peerId(),
                true,
                "OK",
                "",
                "",
                0,
                snapshot.state(),
                snapshot.leaderId(),
                reconfigurationActive,
                jointConsensus,
                clusterSummary.reconfigurationAgeMillis(),
                clusterSummary.clusterHealth(),
                clusterSummary.clusterStatusReason(),
                clusterSummary.quorumAvailable(),
                clusterSummary.currentQuorumAvailable(),
                clusterSummary.nextQuorumAvailable(),
                blockingCurrentQuorumPeerIds,
                blockingNextQuorumPeerIds,
                clusterSummary.members()
        );
    }

    private synchronized boolean allowTelemetryRequest(String requesterId) {
        if (context.runtimeConfiguration().telemetryRateLimitPerMinute() <= 0) {
            return true;
        }
        long now = System.currentTimeMillis();
        String key = requesterId == null || requesterId.isBlank() ? "anonymous" : requesterId;
        Deque<Long> timestamps = telemetryRequestHistory.computeIfAbsent(key, ignored -> new ArrayDeque<>());
        long cutoff = now - 60_000L;
        while (!timestamps.isEmpty() && timestamps.peekFirst() < cutoff) {
            timestamps.removeFirst();
        }
        if (timestamps.size() >= context.runtimeConfiguration().telemetryRateLimitPerMinute()) {
            return false;
        }
        timestamps.addLast(now);
        return true;
    }

    private TelemetryResponse emptyTelemetry(String status) {
        return new TelemetryResponse(0L, 0L, context.me().getId(), false, status, "", "", "", "", false, false, 0, 0, 0, 0, 0, 0, 0, 0, false, List.of(), List.of(), List.of(), List.of(), List.of(), List.of(), "", false, false, false, 0, 0, 0, 0L, "", List.of(), List.of(), List.of());
    }

    private ClusterSummaryResponse emptyClusterSummary(String status) {
        return new ClusterSummaryResponse(0L, 0L, context.me().getId(), false, status, "", "", 0, "", "", false, "", "", false, false, false, 0, 0, 0, 0L, List.of(), List.of(), List.of());
    }

    private ReconfigurationStatusResponse emptyReconfigurationStatus(String status) {
        return new ReconfigurationStatusResponse(0L, 0L, context.me().getId(), false, status, "", "", 0, "", "", false, false, 0L, "", "", false, false, false, List.of(), List.of(), List.of());
    }

    private ClusterTelemetrySummary summarizeCluster(RaftNode.TelemetrySnapshot snapshot) {
        if (snapshot == null || !"LEADER".equals(snapshot.state())) {
            return new ClusterTelemetrySummary("", false, false, false, 0, 0, 0, 0L, "", List.of(), List.of(), List.of());
        }

        Map<String, org.gautelis.raft.protocol.TelemetryReplicationStatus> replicationByPeer = new HashMap<>();
        for (var replication : snapshot.replication()) {
            replicationByPeer.put(replication.peerId(), replication);
        }

        int reachableVotingMembers = 1;
        int healthyVotingMembers = 1;
        for (Peer peer : snapshot.configuration().allVotingMembers()) {
            if (snapshot.peerId().equals(peer.getId())) {
                continue;
            }
            var replication = replicationByPeer.get(peer.getId());
            if (replication != null && replication.reachable()) {
                reachableVotingMembers++;
            }
            if (isHealthyVotingPeer(replication, snapshot.observedAtMillis())) {
                healthyVotingMembers++;
            }
        }

        List<String> blockingCurrentQuorumPeerIds = blockingPeers(snapshot.configuration().currentVotingMembers(), snapshot.peerId(), replicationByPeer, snapshot.observedAtMillis());
        List<String> blockingNextQuorumPeerIds = snapshot.configuration().isJointConsensus()
                ? blockingPeers(snapshot.configuration().nextVotingMembers(), snapshot.peerId(), replicationByPeer, snapshot.observedAtMillis())
                : List.of();
        java.util.Set<String> blockingCurrentPeerSet = new java.util.LinkedHashSet<>(blockingCurrentQuorumPeerIds);
        java.util.Set<String> blockingNextPeerSet = new java.util.LinkedHashSet<>(blockingNextQuorumPeerIds);
        boolean currentQuorumAvailable = hasHealthyQuorum(snapshot.configuration().currentVotingMembers(), snapshot.peerId(), replicationByPeer, snapshot.observedAtMillis());
        boolean nextQuorumAvailable = snapshot.configuration().isJointConsensus()
                ? hasHealthyQuorum(snapshot.configuration().nextVotingMembers(), snapshot.peerId(), replicationByPeer, snapshot.observedAtMillis())
                : currentQuorumAvailable;
        boolean quorumAvailable = snapshot.configuration().isJointConsensus()
                ? currentQuorumAvailable && nextQuorumAvailable
                : currentQuorumAvailable;
        List<ClusterMemberSummary> members = buildClusterMemberSummaries(snapshot, replicationByPeer, blockingCurrentPeerSet, blockingNextPeerSet);

        long reconfigurationAgeMillis = transitionAgeMillis(snapshot);
        boolean reconfigurationStuck = context.runtimeConfiguration().telemetryReconfigurationStuckMillis() > 0L
                && reconfigurationAgeMillis >= context.runtimeConfiguration().telemetryReconfigurationStuckMillis();
        String clusterHealth = quorumAvailable
                ? (healthyVotingMembers == snapshot.configuration().allVotingMembers().size() ? "healthy" : "degraded")
                : "at-risk";
        if (reconfigurationStuck && "healthy".equals(clusterHealth)) {
            clusterHealth = "degraded";
        }
        String clusterStatusReason = describeClusterStatusReason(
                clusterHealth,
                snapshot.configuration().isJointConsensus(),
                currentQuorumAvailable,
                nextQuorumAvailable,
                reconfigurationStuck
        );
        return new ClusterTelemetrySummary(
                clusterHealth,
                quorumAvailable,
                currentQuorumAvailable,
                nextQuorumAvailable,
                snapshot.configuration().allVotingMembers().size(),
                healthyVotingMembers,
                reachableVotingMembers,
                reconfigurationAgeMillis,
                clusterStatusReason,
                List.copyOf(blockingCurrentQuorumPeerIds),
                List.copyOf(blockingNextQuorumPeerIds),
                members
        );
    }

    private List<ClusterMemberSummary> buildClusterMemberSummaries(
            RaftNode.TelemetrySnapshot snapshot,
            Map<String, org.gautelis.raft.protocol.TelemetryReplicationStatus> replicationByPeer,
            java.util.Set<String> blockingCurrentPeerSet,
            java.util.Set<String> blockingNextPeerSet
    ) {
        List<ClusterMemberSummary> members = new ArrayList<>();
        List<Peer> clusterPeers = new ArrayList<>();
        clusterPeers.add(new Peer(snapshot.peerId(), null));
        for (Peer peer : snapshot.knownPeers()) {
            if (!snapshot.peerId().equals(peer.getId())) {
                clusterPeers.add(peer);
            }
        }
        clusterPeers.sort(java.util.Comparator.comparing(Peer::getId));

        for (Peer peer : clusterPeers) {
            boolean local = snapshot.peerId().equals(peer.getId());
            Peer currentMember = snapshot.configuration().currentMembers().stream().filter(member -> peer.getId().equals(member.getId())).findFirst().orElse(null);
            Peer nextMember = snapshot.configuration().nextMembers().stream().filter(member -> peer.getId().equals(member.getId())).findFirst().orElse(null);
            boolean current = currentMember != null;
            boolean next = nextMember != null;
            Peer targetMember = targetConfigurationMember(snapshot.latestKnownConfiguration(), peer.getId());
            Peer effectiveMember = targetMember != null ? targetMember : (nextMember != null ? nextMember : (currentMember != null ? currentMember : peer));
            String currentRole = currentMember == null ? "" : currentMember.getRole().name();
            String nextRole = targetMember == null ? "" : targetMember.getRole().name();
            String roleTransition = describeRoleTransition(currentMember, targetMember);
            long transitionAgeMillis = ("steady".equals(roleTransition) || "known".equals(roleTransition)) ? 0L : transitionAgeMillis(snapshot);
            var replication = replicationByPeer.get(peer.getId());
            long matchIndex = local ? snapshot.lastLogIndex() : (replication == null ? 0L : replication.matchIndex());
            long nextIndex = local ? snapshot.lastLogIndex() + 1 : (replication == null ? 0L : replication.nextIndex());
            long lag = Math.max(0L, snapshot.lastLogIndex() - matchIndex);
            boolean reachable = local || (replication != null && replication.reachable());
            long lastSuccessfulContactMillis = local ? snapshot.lastHeartbeatMillis() : (replication == null ? 0L : replication.lastSuccessfulContactMillis());
            long lastFailedContactMillis = local ? 0L : (replication == null ? 0L : replication.lastFailedContactMillis());
            int consecutiveFailures = local ? 0 : (replication == null ? 0 : replication.consecutiveFailures());
            String freshness = describeFreshness(snapshot.observedAtMillis(), lastSuccessfulContactMillis);
            String health = describeHealth(local, reachable, freshness, consecutiveFailures);
            String blockingQuorums = describeBlockingQuorums(peer.getId(), blockingCurrentPeerSet, blockingNextPeerSet);
            String blockingReason = blockingQuorums.isBlank()
                    ? ""
                    : describeBlockingReason(local, reachable, freshness, lag, consecutiveFailures, roleTransition);
            members.add(new ClusterMemberSummary(
                    peer.getId(),
                    local,
                    current,
                    next,
                    effectiveMember.isVoter(),
                    effectiveMember.getRole().name(),
                    currentRole,
                    nextRole,
                    roleTransition,
                    transitionAgeMillis,
                    blockingQuorums,
                    blockingReason,
                    reachable,
                    freshness,
                    health,
                    nextIndex,
                    matchIndex,
                    lag,
                    consecutiveFailures,
                    lastSuccessfulContactMillis,
                    lastFailedContactMillis
            ));
        }
        return List.copyOf(members);
    }

    private String describeBlockingQuorums(String peerId, java.util.Set<String> blockingCurrentPeerSet, java.util.Set<String> blockingNextPeerSet) {
        boolean current = blockingCurrentPeerSet.contains(peerId);
        boolean next = blockingNextPeerSet.contains(peerId);
        if (current && next) {
            return "current,next";
        }
        if (current) {
            return "current";
        }
        if (next) {
            return "next";
        }
        return "";
    }

    private String describeBlockingReason(boolean local, boolean reachable, String freshness, long lag,
                                          int consecutiveFailures, String roleTransition) {
        if (local) {
            return "";
        }
        if (!reachable) {
            return "unreachable";
        }
        if (consecutiveFailures >= 3) {
            return "replication-failures";
        }
        if (lag > 0L) {
            return "lagging";
        }
        if ("stale".equals(freshness) || "unknown".equals(freshness)) {
            return "stale";
        }
        if (!"steady".equals(roleTransition) && !"known".equals(roleTransition)) {
            return "role-transition";
        }
        return "unknown";
    }

    private String describeRoleTransition(Peer currentMember, Peer nextMember) {
        if (currentMember == null && nextMember == null) {
            return "known";
        }
        if (currentMember == null) {
            return "joining";
        }
        if (nextMember == null) {
            return "removing";
        }
        if (currentMember.getRole() == nextMember.getRole()) {
            return "steady";
        }
        return nextMember.isVoter() ? "promoting" : "demoting";
    }

    private long transitionAgeMillis(RaftNode.TelemetrySnapshot snapshot) {
        long startedAt = snapshot.configurationTransitionStartedMillis();
        if (startedAt <= 0L) {
            return 0L;
        }
        return Math.max(0L, snapshot.observedAtMillis() - startedAt);
    }

    private Peer targetConfigurationMember(ClusterConfiguration configuration, String peerId) {
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

    private boolean hasHealthyQuorum(Collection<Peer> members, String selfId,
                                     Map<String, org.gautelis.raft.protocol.TelemetryReplicationStatus> replicationByPeer,
                                     long observedAtMillis) {
        int total = 0;
        int healthy = 0;
        for (Peer peer : members) {
            total++;
            if (selfId.equals(peer.getId())) {
                healthy++;
                continue;
            }
            if (isHealthyVotingPeer(replicationByPeer.get(peer.getId()), observedAtMillis)) {
                healthy++;
            }
        }
        return healthy >= ((total / 2) + 1);
    }

    private boolean isHealthyVotingPeer(org.gautelis.raft.protocol.TelemetryReplicationStatus replication, long observedAtMillis) {
        if (replication == null || !replication.reachable()) {
            return false;
        }
        if (replication.consecutiveFailures() >= 3) {
            return false;
        }
        long lastSuccess = replication.lastSuccessfulContactMillis();
        if (lastSuccess <= 0L) {
            return false;
        }
        long ageMillis = Math.max(0L, observedAtMillis - lastSuccess);
        return ageMillis < 15_000L;
    }

    private List<String> blockingPeers(Collection<Peer> members, String selfId,
                                       Map<String, org.gautelis.raft.protocol.TelemetryReplicationStatus> replicationByPeer,
                                       long observedAtMillis) {
        List<String> blocked = new ArrayList<>();
        for (Peer peer : members) {
            if (selfId.equals(peer.getId())) {
                continue;
            }
            if (!isHealthyVotingPeer(replicationByPeer.get(peer.getId()), observedAtMillis)) {
                blocked.add(peer.getId());
            }
        }
        blocked.sort(String::compareTo);
        return blocked;
    }

    private String describeClusterStatusReason(String clusterHealth, boolean jointConsensus,
                                               boolean currentQuorumAvailable, boolean nextQuorumAvailable,
                                               boolean reconfigurationStuck) {
        if ("healthy".equals(clusterHealth)) {
            return "all-voters-healthy";
        }
        if (reconfigurationStuck && !"at-risk".equals(clusterHealth)) {
            return "reconfiguration-stuck";
        }
        if ("degraded".equals(clusterHealth)) {
            return "followers-unhealthy";
        }
        if (jointConsensus && !currentQuorumAvailable && !nextQuorumAvailable) {
            return "current-and-next-quorum-unavailable";
        }
        if (!currentQuorumAvailable) {
            return "current-quorum-unavailable";
        }
        if (!nextQuorumAvailable) {
            return "next-quorum-unavailable";
        }
        return "unknown";
    }

    private String describeFreshness(long observedAtMillis, long lastContactMillis) {
        if (lastContactMillis <= 0) {
            return "unknown";
        }
        long ageMillis = Math.max(0L, observedAtMillis - lastContactMillis);
        if (ageMillis < 5_000L) {
            return "fresh";
        }
        if (ageMillis < 15_000L) {
            return "recent";
        }
        return "stale";
    }

    private String describeHealth(boolean local, boolean reachable, String freshness, int consecutiveFailures) {
        if (local) {
            return "local";
        }
        if (!reachable || consecutiveFailures >= 3) {
            return "unhealthy";
        }
        if (consecutiveFailures > 0 || "stale".equals(freshness) || "unknown".equals(freshness)) {
            return "degraded";
        }
        return "healthy";
    }

    private static String leaderId(Peer leader) {
        return leader == null ? "" : leader.getId();
    }

    private static String leaderHost(Peer leader) {
        return leader == null || leader.getAddress() == null ? "" : leader.getAddress().getHostString();
    }

    private static int leaderPort(Peer leader) {
        return leader == null || leader.getAddress() == null ? 0 : leader.getAddress().getPort();
    }

    private record ClusterTelemetrySummary(
            String clusterHealth,
            boolean quorumAvailable,
            boolean currentQuorumAvailable,
            boolean nextQuorumAvailable,
            int votingMembers,
            int healthyVotingMembers,
            int reachableVotingMembers,
            long reconfigurationAgeMillis,
            String clusterStatusReason,
            List<String> blockingCurrentQuorumPeerIds,
            List<String> blockingNextQuorumPeerIds,
            List<ClusterMemberSummary> members
    ) {
    }
}
