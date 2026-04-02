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

/**
 * Returns the leader's cluster-wide health, quorum, and per-member summary view.
 */
public final class ClusterSummaryResponse {
    private final long observedAtMillis;
    private final long term;
    private final String peerId;
    private final boolean success;
    private final String status;
    private final String redirectLeaderId;
    private final String redirectLeaderHost;
    private final int redirectLeaderPort;
    private final String state;
    private final String leaderId;
    private final boolean jointConsensus;
    private final String clusterHealth;
    private final String clusterStatusReason;
    private final boolean quorumAvailable;
    private final boolean currentQuorumAvailable;
    private final boolean nextQuorumAvailable;
    private final int votingMembers;
    private final int healthyVotingMembers;
    private final int reachableVotingMembers;
    private final long reconfigurationAgeMillis;
    private final List<String> blockingCurrentQuorumPeerIds;
    private final List<String> blockingNextQuorumPeerIds;
    private final List<ClusterMemberSummary> members;

    /**
     * Creates a cluster summary response.
     *
     * @param observedAtMillis capture time in milliseconds
     * @param term responder term
     * @param peerId responding peer identifier
     * @param success whether the request succeeded
     * @param status machine-readable status
     * @param redirectLeaderId known leader identifier for redirects
     * @param redirectLeaderHost known leader host for redirects
     * @param redirectLeaderPort known leader port for redirects
     * @param state local node state name
     * @param leaderId known current leader identifier
     * @param jointConsensus whether the configuration is in joint consensus
     * @param clusterHealth summarized cluster health
     * @param clusterStatusReason human-readable explanation for cluster state
     * @param quorumAvailable whether any required quorum is currently available
     * @param currentQuorumAvailable whether the current quorum is available
     * @param nextQuorumAvailable whether the next quorum is available
     * @param votingMembers number of voting members
     * @param healthyVotingMembers number of healthy voting members
     * @param reachableVotingMembers number of reachable voting members
     * @param reconfigurationAgeMillis age of the current reconfiguration
     * @param blockingCurrentQuorumPeerIds peers blocking the current quorum
     * @param blockingNextQuorumPeerIds peers blocking the next quorum
     * @param members per-member summary rows
     */
    public ClusterSummaryResponse(long observedAtMillis, long term, String peerId, boolean success, String status,
                                  String redirectLeaderId, String redirectLeaderHost, int redirectLeaderPort,
                                  String state, String leaderId, boolean jointConsensus, String clusterHealth,
                                  String clusterStatusReason, boolean quorumAvailable, boolean currentQuorumAvailable,
                                  boolean nextQuorumAvailable, int votingMembers, int healthyVotingMembers,
                                  int reachableVotingMembers, long reconfigurationAgeMillis, List<String> blockingCurrentQuorumPeerIds,
                                  List<String> blockingNextQuorumPeerIds, List<ClusterMemberSummary> members) {
        this.observedAtMillis = observedAtMillis;
        this.term = term;
        this.peerId = peerId;
        this.success = success;
        this.status = status;
        this.redirectLeaderId = redirectLeaderId == null ? "" : redirectLeaderId;
        this.redirectLeaderHost = redirectLeaderHost == null ? "" : redirectLeaderHost;
        this.redirectLeaderPort = Math.max(0, redirectLeaderPort);
        this.state = state == null ? "" : state;
        this.leaderId = leaderId == null ? "" : leaderId;
        this.jointConsensus = jointConsensus;
        this.clusterHealth = clusterHealth == null ? "" : clusterHealth;
        this.clusterStatusReason = clusterStatusReason == null ? "" : clusterStatusReason;
        this.quorumAvailable = quorumAvailable;
        this.currentQuorumAvailable = currentQuorumAvailable;
        this.nextQuorumAvailable = nextQuorumAvailable;
        this.votingMembers = votingMembers;
        this.healthyVotingMembers = healthyVotingMembers;
        this.reachableVotingMembers = reachableVotingMembers;
        this.reconfigurationAgeMillis = reconfigurationAgeMillis;
        this.blockingCurrentQuorumPeerIds = blockingCurrentQuorumPeerIds == null ? List.of() : List.copyOf(blockingCurrentQuorumPeerIds);
        this.blockingNextQuorumPeerIds = blockingNextQuorumPeerIds == null ? List.of() : List.copyOf(blockingNextQuorumPeerIds);
        this.members = members == null ? List.of() : List.copyOf(members);
    }

    /**
     * Returns the capture time.
     *
     * @return capture time in milliseconds
     */
    public long getObservedAtMillis() { return observedAtMillis; }
    /**
     * Returns the responder term.
     *
     * @return responder term
     */
    public long getTerm() { return term; }
    /**
     * Returns the responding peer identifier.
     *
     * @return responding peer identifier
     */
    public String getPeerId() { return peerId; }
    /**
     * Indicates whether the request succeeded.
     *
     * @return {@code true} when the request succeeded
     */
    public boolean isSuccess() { return success; }
    /**
     * Returns the machine-readable status.
     *
     * @return machine-readable status
     */
    public String getStatus() { return status; }
    /**
     * Returns the redirect leader identifier.
     *
     * @return redirect leader identifier
     */
    public String getRedirectLeaderId() { return redirectLeaderId; }
    /**
     * Returns the redirect leader host.
     *
     * @return redirect leader host
     */
    public String getRedirectLeaderHost() { return redirectLeaderHost; }
    /**
     * Returns the redirect leader port.
     *
     * @return redirect leader port
     */
    public int getRedirectLeaderPort() { return redirectLeaderPort; }
    /**
     * Returns the local node state name.
     *
     * @return local node state name
     */
    public String getState() { return state; }
    /**
     * Returns the known leader identifier.
     *
     * @return known leader identifier
     */
    public String getLeaderId() { return leaderId; }
    /**
     * Indicates whether the configuration is in joint consensus.
     *
     * @return {@code true} when in joint consensus
     */
    public boolean isJointConsensus() { return jointConsensus; }
    /**
     * Returns the summarized cluster health.
     *
     * @return summarized cluster health
     */
    public String getClusterHealth() { return clusterHealth; }
    /**
     * Returns the human-readable cluster status explanation.
     *
     * @return human-readable cluster status explanation
     */
    public String getClusterStatusReason() { return clusterStatusReason; }
    /**
     * Indicates whether the required quorum is available.
     *
     * @return {@code true} when required quorum is available
     */
    public boolean isQuorumAvailable() { return quorumAvailable; }
    /**
     * Indicates whether the current quorum is available.
     *
     * @return {@code true} when the current quorum is available
     */
    public boolean isCurrentQuorumAvailable() { return currentQuorumAvailable; }
    /**
     * Indicates whether the next quorum is available.
     *
     * @return {@code true} when the next quorum is available
     */
    public boolean isNextQuorumAvailable() { return nextQuorumAvailable; }
    /**
     * Returns the number of voting members.
     *
     * @return number of voting members
     */
    public int getVotingMembers() { return votingMembers; }
    /**
     * Returns the number of healthy voting members.
     *
     * @return number of healthy voting members
     */
    public int getHealthyVotingMembers() { return healthyVotingMembers; }
    /**
     * Returns the number of reachable voting members.
     *
     * @return number of reachable voting members
     */
    public int getReachableVotingMembers() { return reachableVotingMembers; }
    /**
     * Returns the age of the current reconfiguration.
     *
     * @return age of the current reconfiguration in milliseconds
     */
    public long getReconfigurationAgeMillis() { return reconfigurationAgeMillis; }
    /**
     * Returns the peer ids blocking the current quorum.
     *
     * @return peer ids blocking the current quorum
     */
    public List<String> getBlockingCurrentQuorumPeerIds() { return blockingCurrentQuorumPeerIds; }
    /**
     * Returns the peer ids blocking the next quorum.
     *
     * @return peer ids blocking the next quorum
     */
    public List<String> getBlockingNextQuorumPeerIds() { return blockingNextQuorumPeerIds; }
    /**
     * Returns the per-member summary rows.
     *
     * @return per-member summary rows
     */
    public List<ClusterMemberSummary> getMembers() { return members; }
}
