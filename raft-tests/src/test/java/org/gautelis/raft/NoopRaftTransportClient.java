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
package org.gautelis.raft;

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
import org.gautelis.raft.transport.RaftTransportClient;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Test transport that holds no Netty resources.
 */
public class NoopRaftTransportClient implements RaftTransportClient {
    @Override
    public void shutdown() {
    }

    @Override
    public void setKnownPeers(Collection<Peer> peers) {
    }

    @Override
    public CompletableFuture<List<VoteResponse>> requestVoteFromAll(Collection<Peer> peers, VoteRequest req) {
        return CompletableFuture.completedFuture(List.of());
    }

    @Override
    public CompletableFuture<AppendEntriesResponse> sendAppendEntries(Peer peer, AppendEntriesRequest req) {
        return CompletableFuture.completedFuture(new AppendEntriesResponse(req.getTerm(), peerId(peer), false, -1));
    }

    @Override
    public CompletableFuture<InstallSnapshotResponse> sendInstallSnapshot(Peer peer, InstallSnapshotRequest req) {
        return CompletableFuture.completedFuture(new InstallSnapshotResponse(req.getTerm(), peerId(peer), false, -1));
    }

    @Override
    public CompletableFuture<JoinClusterResponse> sendJoinClusterRequest(Peer peer, JoinClusterRequest request) {
        return CompletableFuture.completedFuture(new JoinClusterResponse(
                request.getTerm(), peerId(peer), false, "UNAVAILABLE", "No test transport target", null
        ));
    }

    @Override
    public CompletableFuture<ClientCommandResponse> sendClientCommandRequest(Peer peer, ClientCommandRequest request) {
        return CompletableFuture.completedFuture(new ClientCommandResponse(
                request.getTerm(), peerId(peer), false, "UNAVAILABLE", "No test transport target", null, null, 0, new byte[0]
        ));
    }

    @Override
    public CompletableFuture<ClientQueryResponse> sendClientQueryRequest(Peer peer, ClientQueryRequest request) {
        return CompletableFuture.completedFuture(new ClientQueryResponse(
                request.getTerm(), peerId(peer), false, "UNAVAILABLE", "No test transport target", null, null, 0, new byte[0]
        ));
    }

    @Override
    public CompletableFuture<ClusterSummaryResponse> sendClusterSummaryRequest(Peer peer, ClusterSummaryRequest request) {
        return CompletableFuture.completedFuture(new ClusterSummaryResponse(
                0L, request.getTerm(), peerId(peer), false, "UNAVAILABLE", "", "", 0,
                "", "", false, "", "No test transport target", false, false, false,
                0, 0, 0, 0L, List.of(), List.of(), List.of()
        ));
    }

    @Override
    public CompletableFuture<ReconfigurationStatusResponse> sendReconfigurationStatusRequest(
            Peer peer, ReconfigurationStatusRequest request
    ) {
        return CompletableFuture.completedFuture(new ReconfigurationStatusResponse(
                0L, request.getTerm(), peerId(peer), false, "UNAVAILABLE", "", "", 0,
                "", "", false, false, 0L, "", "No test transport target", false, false, false,
                List.of(), List.of(), List.of()
        ));
    }

    @Override
    public CompletableFuture<JoinClusterStatusResponse> sendJoinClusterStatusRequest(Peer peer, JoinClusterStatusRequest request) {
        return CompletableFuture.completedFuture(new JoinClusterStatusResponse(
                request.getTerm(), peerId(peer), false, "UNAVAILABLE", "No test transport target", null
        ));
    }

    @Override
    public CompletableFuture<ReconfigureClusterResponse> sendReconfigureClusterRequest(Peer peer, ReconfigureClusterRequest request) {
        return CompletableFuture.completedFuture(new ReconfigureClusterResponse(
                request.getTerm(), peerId(peer), false, "UNAVAILABLE", "No test transport target", null
        ));
    }

    @Override
    public CompletableFuture<TelemetryResponse> sendTelemetryRequest(Peer peer, TelemetryRequest request) {
        return CompletableFuture.completedFuture(new TelemetryResponse(
                0L, request.getTerm(), peerId(peer), false, "UNAVAILABLE", "", "", "", "",
                false, false, 0, 0, 0, 0, 0, 0, 0, 0, false,
                List.of(), List.of(), List.of(), List.of(), List.of(), List.of(), "",
                false, false, false, 0, 0, 0, 0L, "No test transport target",
                List.of(), List.of(), List.of()
        ));
    }

    @Override
    public Collection<Peer> broadcast(String type, long term, byte[] payload) {
        return List.of();
    }

    @Override
    public List<TelemetryPeerStats> snapshotResponseTimeStats() {
        return List.of();
    }

    @Override
    public boolean isPeerReachable(String peerId) {
        return false;
    }

    private static String peerId(Peer peer) {
        return peer == null ? "" : peer.getId();
    }
}
