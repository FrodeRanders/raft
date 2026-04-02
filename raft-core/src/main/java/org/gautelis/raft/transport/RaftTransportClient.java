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

/**
 * Client-side transport abstraction for Raft replication, client requests, and cluster management RPCs.
 */
public interface RaftTransportClient {
    /** Shuts down any resources held by the client. */
    void shutdown();

    /**
     * Updates the set of peers the client may need to contact.
     *
     * @param peers peers known to the node
     */
    void setKnownPeers(Collection<Peer> peers);

    /**
     * Sends vote requests to a collection of peers.
     *
     * @param peers peers to contact
     * @param req vote request payload
     * @return future resolving to the collected responses
     */
    CompletableFuture<List<VoteResponse>> requestVoteFromAll(Collection<Peer> peers, VoteRequest req);

    /**
     * Sends an AppendEntries request to one peer.
     *
     * @param peer destination peer
     * @param req append request payload
     * @return future resolving to the peer response
     */
    CompletableFuture<AppendEntriesResponse> sendAppendEntries(Peer peer, AppendEntriesRequest req);

    /**
     * Sends an InstallSnapshot request to one peer.
     *
     * @param peer destination peer
     * @param req snapshot request payload
     * @return future resolving to the peer response
     */
    CompletableFuture<InstallSnapshotResponse> sendInstallSnapshot(Peer peer, InstallSnapshotRequest req);

    /**
     * Sends a join-cluster request.
     *
     * @param peer destination peer
     * @param request join request
     * @return future resolving to the response
     */
    CompletableFuture<JoinClusterResponse> sendJoinClusterRequest(Peer peer, JoinClusterRequest request);

    /**
     * Sends a client command request.
     *
     * @param peer destination peer
     * @param request command request
     * @return future resolving to the response
     */
    CompletableFuture<ClientCommandResponse> sendClientCommandRequest(Peer peer, ClientCommandRequest request);

    /**
     * Sends a client query request.
     *
     * @param peer destination peer
     * @param request query request
     * @return future resolving to the response
     */
    CompletableFuture<ClientQueryResponse> sendClientQueryRequest(Peer peer, ClientQueryRequest request);

    /**
     * Sends a cluster summary request.
     *
     * @param peer destination peer
     * @param request summary request
     * @return future resolving to the response
     */
    CompletableFuture<ClusterSummaryResponse> sendClusterSummaryRequest(Peer peer, ClusterSummaryRequest request);

    /**
     * Sends a reconfiguration status request.
     *
     * @param peer destination peer
     * @param request status request
     * @return future resolving to the response
     */
    CompletableFuture<ReconfigurationStatusResponse> sendReconfigurationStatusRequest(Peer peer, ReconfigurationStatusRequest request);

    /**
     * Sends a join status request.
     *
     * @param peer destination peer
     * @param request status request
     * @return future resolving to the response
     */
    CompletableFuture<JoinClusterStatusResponse> sendJoinClusterStatusRequest(Peer peer, JoinClusterStatusRequest request);

    /**
     * Sends a reconfigure-cluster request.
     *
     * @param peer destination peer
     * @param request reconfiguration request
     * @return future resolving to the response
     */
    CompletableFuture<ReconfigureClusterResponse> sendReconfigureClusterRequest(Peer peer, ReconfigureClusterRequest request);

    /**
     * Sends a telemetry request.
     *
     * @param peer destination peer
     * @param request telemetry request
     * @return future resolving to the response
     */
    CompletableFuture<TelemetryResponse> sendTelemetryRequest(Peer peer, TelemetryRequest request);

    /**
     * Broadcasts a raw message to the currently known peers.
     *
     * @param type logical message type
     * @param term associated term value
     * @param payload encoded payload
     * @return peers that were targeted by the broadcast
     */
    Collection<Peer> broadcast(String type, long term, byte[] payload);

    /**
     * Returns a point-in-time copy of response-time statistics by peer.
     *
     * @return response-time snapshot
     */
    List<TelemetryPeerStats> snapshotResponseTimeStats();

    /**
     * Indicates whether a peer is currently considered reachable.
     *
     * @param peerId peer identifier
     * @return {@code true} when the peer is reachable
     */
    boolean isPeerReachable(String peerId);
}
