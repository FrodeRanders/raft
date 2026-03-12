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

    public long getObservedAtMillis() {
        return observedAtMillis;
    }

    public long getTerm() {
        return term;
    }

    public String getPeerId() {
        return peerId;
    }

    public boolean isSuccess() {
        return success;
    }

    public String getStatus() {
        return status;
    }

    public String getRedirectLeaderId() {
        return redirectLeaderId;
    }

    public String getRedirectLeaderHost() {
        return redirectLeaderHost;
    }

    public int getRedirectLeaderPort() {
        return redirectLeaderPort;
    }

    public String getState() {
        return state;
    }

    public String getLeaderId() {
        return leaderId;
    }

    public boolean isReconfigurationActive() {
        return reconfigurationActive;
    }

    public boolean isJointConsensus() {
        return jointConsensus;
    }

    public long getReconfigurationAgeMillis() {
        return reconfigurationAgeMillis;
    }

    public String getClusterHealth() {
        return clusterHealth;
    }

    public String getClusterStatusReason() {
        return clusterStatusReason;
    }

    public boolean isQuorumAvailable() {
        return quorumAvailable;
    }

    public boolean isCurrentQuorumAvailable() {
        return currentQuorumAvailable;
    }

    public boolean isNextQuorumAvailable() {
        return nextQuorumAvailable;
    }

    public List<String> getBlockingCurrentQuorumPeerIds() {
        return blockingCurrentQuorumPeerIds;
    }

    public List<String> getBlockingNextQuorumPeerIds() {
        return blockingNextQuorumPeerIds;
    }

    public List<ClusterMemberSummary> getMembers() {
        return members;
    }
}
