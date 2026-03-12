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
package org.gautelis.raft.protocol;

import java.util.List;

public final class TelemetryResponse {
    private final long observedAtMillis;
    private final long term;
    private final String peerId;
    private final boolean success;
    private final String status;
    private final String redirectLeaderId;
    private final String state;
    private final String leaderId;
    private final String votedFor;
    private final boolean joining;
    private final boolean decommissioned;
    private final long commitIndex;
    private final long lastApplied;
    private final long lastLogIndex;
    private final long lastLogTerm;
    private final long snapshotIndex;
    private final long snapshotTerm;
    private final long lastHeartbeatMillis;
    private final long nextElectionDeadlineMillis;
    private final boolean jointConsensus;
    private final List<Peer> currentMembers;
    private final List<Peer> nextMembers;
    private final List<Peer> knownPeers;
    private final List<String> pendingJoinIds;
    private final List<TelemetryReplicationStatus> replication;
    private final List<TelemetryPeerStats> peerStats;
    private final String clusterHealth;
    private final boolean quorumAvailable;
    private final boolean currentQuorumAvailable;
    private final boolean nextQuorumAvailable;
    private final int votingMembers;
    private final int healthyVotingMembers;
    private final int reachableVotingMembers;
    private final long reconfigurationAgeMillis;
    private final String clusterStatusReason;
    private final List<String> blockingCurrentQuorumPeerIds;
    private final List<String> blockingNextQuorumPeerIds;
    private final List<ClusterMemberSummary> clusterMembers;

    public TelemetryResponse(long observedAtMillis, long term, String peerId, boolean success, String status, String redirectLeaderId,
                             String state, String leaderId, String votedFor,
                             boolean joining, boolean decommissioned, long commitIndex, long lastApplied,
                             long lastLogIndex, long lastLogTerm, long snapshotIndex, long snapshotTerm,
                             long lastHeartbeatMillis, long nextElectionDeadlineMillis, boolean jointConsensus,
                             List<Peer> currentMembers, List<Peer> nextMembers, List<Peer> knownPeers,
                             List<String> pendingJoinIds, List<TelemetryReplicationStatus> replication,
                             List<TelemetryPeerStats> peerStats, String clusterHealth,
                             boolean quorumAvailable, boolean currentQuorumAvailable, boolean nextQuorumAvailable,
                             int votingMembers, int healthyVotingMembers, int reachableVotingMembers,
                             long reconfigurationAgeMillis, String clusterStatusReason,
                             List<String> blockingCurrentQuorumPeerIds, List<String> blockingNextQuorumPeerIds,
                             List<ClusterMemberSummary> clusterMembers) {
        this.observedAtMillis = observedAtMillis;
        this.term = term;
        this.peerId = peerId;
        this.success = success;
        this.status = status;
        this.redirectLeaderId = redirectLeaderId;
        this.state = state;
        this.leaderId = leaderId;
        this.votedFor = votedFor;
        this.joining = joining;
        this.decommissioned = decommissioned;
        this.commitIndex = commitIndex;
        this.lastApplied = lastApplied;
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm = lastLogTerm;
        this.snapshotIndex = snapshotIndex;
        this.snapshotTerm = snapshotTerm;
        this.lastHeartbeatMillis = lastHeartbeatMillis;
        this.nextElectionDeadlineMillis = nextElectionDeadlineMillis;
        this.jointConsensus = jointConsensus;
        this.currentMembers = currentMembers == null ? List.of() : List.copyOf(currentMembers);
        this.nextMembers = nextMembers == null ? List.of() : List.copyOf(nextMembers);
        this.knownPeers = knownPeers == null ? List.of() : List.copyOf(knownPeers);
        this.pendingJoinIds = pendingJoinIds == null ? List.of() : List.copyOf(pendingJoinIds);
        this.replication = replication == null ? List.of() : List.copyOf(replication);
        this.peerStats = peerStats == null ? List.of() : List.copyOf(peerStats);
        this.clusterHealth = clusterHealth == null ? "" : clusterHealth;
        this.quorumAvailable = quorumAvailable;
        this.currentQuorumAvailable = currentQuorumAvailable;
        this.nextQuorumAvailable = nextQuorumAvailable;
        this.votingMembers = votingMembers;
        this.healthyVotingMembers = healthyVotingMembers;
        this.reachableVotingMembers = reachableVotingMembers;
        this.reconfigurationAgeMillis = reconfigurationAgeMillis;
        this.clusterStatusReason = clusterStatusReason == null ? "" : clusterStatusReason;
        this.blockingCurrentQuorumPeerIds = blockingCurrentQuorumPeerIds == null ? List.of() : List.copyOf(blockingCurrentQuorumPeerIds);
        this.blockingNextQuorumPeerIds = blockingNextQuorumPeerIds == null ? List.of() : List.copyOf(blockingNextQuorumPeerIds);
        this.clusterMembers = clusterMembers == null ? List.of() : List.copyOf(clusterMembers);
    }

    public long getObservedAtMillis() { return observedAtMillis; }
    public long getTerm() { return term; }
    public String getPeerId() { return peerId; }
    public boolean isSuccess() { return success; }
    public String getStatus() { return status; }
    public String getRedirectLeaderId() { return redirectLeaderId; }
    public String getState() { return state; }
    public String getLeaderId() { return leaderId; }
    public String getVotedFor() { return votedFor; }
    public boolean isJoining() { return joining; }
    public boolean isDecommissioned() { return decommissioned; }
    public long getCommitIndex() { return commitIndex; }
    public long getLastApplied() { return lastApplied; }
    public long getLastLogIndex() { return lastLogIndex; }
    public long getLastLogTerm() { return lastLogTerm; }
    public long getSnapshotIndex() { return snapshotIndex; }
    public long getSnapshotTerm() { return snapshotTerm; }
    public long getLastHeartbeatMillis() { return lastHeartbeatMillis; }
    public long getNextElectionDeadlineMillis() { return nextElectionDeadlineMillis; }
    public boolean isJointConsensus() { return jointConsensus; }
    public List<Peer> getCurrentMembers() { return currentMembers; }
    public List<Peer> getNextMembers() { return nextMembers; }
    public List<Peer> getKnownPeers() { return knownPeers; }
    public List<String> getPendingJoinIds() { return pendingJoinIds; }
    public List<TelemetryReplicationStatus> getReplication() { return replication; }
    public List<TelemetryPeerStats> getPeerStats() { return peerStats; }
    public String getClusterHealth() { return clusterHealth; }
    public boolean isQuorumAvailable() { return quorumAvailable; }
    public boolean isCurrentQuorumAvailable() { return currentQuorumAvailable; }
    public boolean isNextQuorumAvailable() { return nextQuorumAvailable; }
    public int getVotingMembers() { return votingMembers; }
    public int getHealthyVotingMembers() { return healthyVotingMembers; }
    public int getReachableVotingMembers() { return reachableVotingMembers; }
    public String getClusterStatusReason() { return clusterStatusReason; }
    public List<String> getBlockingCurrentQuorumPeerIds() { return blockingCurrentQuorumPeerIds; }
    public List<String> getBlockingNextQuorumPeerIds() { return blockingNextQuorumPeerIds; }
    public List<ClusterMemberSummary> getClusterMembers() { return clusterMembers; }
    public long getReconfigurationAgeMillis() { return reconfigurationAgeMillis; }
}
