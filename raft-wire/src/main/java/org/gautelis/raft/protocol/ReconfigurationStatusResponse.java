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
 * Returns a focused status view for cluster membership changes and their blockers.
 */
public final class ReconfigurationStatusResponse {
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
    private final boolean reconfigurationActive;
    private final boolean jointConsensus;
    private final long reconfigurationAgeMillis;
    private final String clusterHealth;
    private final String clusterStatusReason;
    private final boolean quorumAvailable;
    private final boolean currentQuorumAvailable;
    private final boolean nextQuorumAvailable;
    private final List<String> blockingCurrentQuorumPeerIds;
    private final List<String> blockingNextQuorumPeerIds;
    private final List<ClusterMemberSummary> members;

    /**
     * Creates a reconfiguration-status response.
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
     * @param reconfigurationActive whether a reconfiguration is active
     * @param jointConsensus whether the configuration is in joint consensus
     * @param reconfigurationAgeMillis age of the active reconfiguration
     * @param clusterHealth summarized cluster health
     * @param clusterStatusReason human-readable explanation for cluster state
     * @param quorumAvailable whether required quorum is currently available
     * @param currentQuorumAvailable whether the current quorum is available
     * @param nextQuorumAvailable whether the next quorum is available
     * @param blockingCurrentQuorumPeerIds peers blocking the current quorum
     * @param blockingNextQuorumPeerIds peers blocking the next quorum
     * @param members per-member summary rows
     */
    public ReconfigurationStatusResponse(long observedAtMillis, long term, String peerId, boolean success, String status,
                                         String redirectLeaderId, String redirectLeaderHost, int redirectLeaderPort,
                                         String state, String leaderId, boolean reconfigurationActive, boolean jointConsensus,
                                         long reconfigurationAgeMillis, String clusterHealth, String clusterStatusReason,
                                         boolean quorumAvailable, boolean currentQuorumAvailable, boolean nextQuorumAvailable,
                                         List<String> blockingCurrentQuorumPeerIds, List<String> blockingNextQuorumPeerIds,
                                         List<ClusterMemberSummary> members) {
        this.observedAtMillis = observedAtMillis;
        this.term = term;
        this.peerId = peerId == null ? "" : peerId;
        this.success = success;
        this.status = status == null ? "" : status;
        this.redirectLeaderId = redirectLeaderId == null ? "" : redirectLeaderId;
        this.redirectLeaderHost = redirectLeaderHost == null ? "" : redirectLeaderHost;
        this.redirectLeaderPort = Math.max(0, redirectLeaderPort);
        this.state = state == null ? "" : state;
        this.leaderId = leaderId == null ? "" : leaderId;
        this.reconfigurationActive = reconfigurationActive;
        this.jointConsensus = jointConsensus;
        this.reconfigurationAgeMillis = reconfigurationAgeMillis;
        this.clusterHealth = clusterHealth == null ? "" : clusterHealth;
        this.clusterStatusReason = clusterStatusReason == null ? "" : clusterStatusReason;
        this.quorumAvailable = quorumAvailable;
        this.currentQuorumAvailable = currentQuorumAvailable;
        this.nextQuorumAvailable = nextQuorumAvailable;
        this.blockingCurrentQuorumPeerIds = blockingCurrentQuorumPeerIds == null ? List.of() : List.copyOf(blockingCurrentQuorumPeerIds);
        this.blockingNextQuorumPeerIds = blockingNextQuorumPeerIds == null ? List.of() : List.copyOf(blockingNextQuorumPeerIds);
        this.members = members == null ? List.of() : List.copyOf(members);
    }

    /** @return capture time in milliseconds */
    public long getObservedAtMillis() {
        return observedAtMillis;
    }

    /** @return responder term */
    public long getTerm() {
        return term;
    }

    /** @return responding peer identifier */
    public String getPeerId() {
        return peerId;
    }

    /** @return {@code true} when the request succeeded */
    public boolean isSuccess() {
        return success;
    }

    /** @return machine-readable status */
    public String getStatus() {
        return status;
    }

    /** @return redirect leader identifier */
    public String getRedirectLeaderId() {
        return redirectLeaderId;
    }

    /** @return redirect leader host */
    public String getRedirectLeaderHost() {
        return redirectLeaderHost;
    }

    /** @return redirect leader port */
    public int getRedirectLeaderPort() {
        return redirectLeaderPort;
    }

    /** @return local node state name */
    public String getState() {
        return state;
    }

    /** @return known leader identifier */
    public String getLeaderId() {
        return leaderId;
    }

    /** @return {@code true} when a reconfiguration is active */
    public boolean isReconfigurationActive() {
        return reconfigurationActive;
    }

    /** @return {@code true} when in joint consensus */
    public boolean isJointConsensus() {
        return jointConsensus;
    }

    /** @return age of the current reconfiguration in milliseconds */
    public long getReconfigurationAgeMillis() {
        return reconfigurationAgeMillis;
    }

    /** @return summarized cluster health */
    public String getClusterHealth() {
        return clusterHealth;
    }

    /** @return human-readable cluster status explanation */
    public String getClusterStatusReason() {
        return clusterStatusReason;
    }

    /** @return {@code true} when required quorum is available */
    public boolean isQuorumAvailable() {
        return quorumAvailable;
    }

    /** @return {@code true} when the current quorum is available */
    public boolean isCurrentQuorumAvailable() {
        return currentQuorumAvailable;
    }

    /** @return {@code true} when the next quorum is available */
    public boolean isNextQuorumAvailable() {
        return nextQuorumAvailable;
    }

    /** @return peer ids blocking the current quorum */
    public List<String> getBlockingCurrentQuorumPeerIds() {
        return blockingCurrentQuorumPeerIds;
    }

    /** @return peer ids blocking the next quorum */
    public List<String> getBlockingNextQuorumPeerIds() {
        return blockingNextQuorumPeerIds;
    }

    /** @return per-member summary rows */
    public List<ClusterMemberSummary> getMembers() {
        return members;
    }
}
