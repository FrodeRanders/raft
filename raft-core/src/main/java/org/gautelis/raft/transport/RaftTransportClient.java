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
package org.gautelis.raft.transport;

import org.gautelis.raft.protocol.AppendEntriesRequest;
import org.gautelis.raft.protocol.AppendEntriesResponse;
import org.gautelis.raft.protocol.ClientCommandRequest;
import org.gautelis.raft.protocol.ClientCommandResponse;
import org.gautelis.raft.protocol.ClientQueryRequest;
import org.gautelis.raft.protocol.ClientQueryResponse;
import org.gautelis.raft.protocol.ClusterSummaryRequest;
import org.gautelis.raft.protocol.ClusterSummaryResponse;
import org.gautelis.raft.protocol.InstallSnapshotRequest;
import org.gautelis.raft.protocol.InstallSnapshotResponse;
import org.gautelis.raft.protocol.JoinClusterRequest;
import org.gautelis.raft.protocol.JoinClusterResponse;
import org.gautelis.raft.protocol.JoinClusterStatusRequest;
import org.gautelis.raft.protocol.JoinClusterStatusResponse;
import org.gautelis.raft.protocol.Peer;
import org.gautelis.raft.protocol.ReconfigurationStatusRequest;
import org.gautelis.raft.protocol.ReconfigurationStatusResponse;
import org.gautelis.raft.protocol.ReconfigureClusterRequest;
import org.gautelis.raft.protocol.ReconfigureClusterResponse;
import org.gautelis.raft.protocol.TelemetryPeerStats;
import org.gautelis.raft.protocol.TelemetryRequest;
import org.gautelis.raft.protocol.TelemetryResponse;
import org.gautelis.raft.protocol.VoteRequest;
import org.gautelis.raft.protocol.VoteResponse;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface RaftTransportClient {
    void shutdown();
    void setKnownPeers(Collection<Peer> peers);
    CompletableFuture<List<VoteResponse>> requestVoteFromAll(Collection<Peer> peers, VoteRequest req);
    CompletableFuture<AppendEntriesResponse> sendAppendEntries(Peer peer, AppendEntriesRequest req);
    CompletableFuture<InstallSnapshotResponse> sendInstallSnapshot(Peer peer, InstallSnapshotRequest req);
    CompletableFuture<Boolean> sendMessage(Peer peer, String type, byte[] payload);
    CompletableFuture<JoinClusterResponse> sendJoinClusterRequest(Peer peer, JoinClusterRequest request);
    CompletableFuture<ClientCommandResponse> sendClientCommandRequest(Peer peer, ClientCommandRequest request);
    CompletableFuture<ClientQueryResponse> sendClientQueryRequest(Peer peer, ClientQueryRequest request);
    CompletableFuture<ClusterSummaryResponse> sendClusterSummaryRequest(Peer peer, ClusterSummaryRequest request);
    CompletableFuture<ReconfigurationStatusResponse> sendReconfigurationStatusRequest(Peer peer, ReconfigurationStatusRequest request);
    CompletableFuture<JoinClusterStatusResponse> sendJoinClusterStatusRequest(Peer peer, JoinClusterStatusRequest request);
    CompletableFuture<ReconfigureClusterResponse> sendReconfigureClusterRequest(Peer peer, ReconfigureClusterRequest request);
    CompletableFuture<TelemetryResponse> sendTelemetryRequest(Peer peer, TelemetryRequest request);
    Collection<Peer> broadcast(String type, long term, byte[] payload);
    List<TelemetryPeerStats> snapshotResponseTimeStats();
    boolean isPeerReachable(String peerId);
}
