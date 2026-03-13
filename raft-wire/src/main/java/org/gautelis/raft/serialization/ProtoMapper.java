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
package org.gautelis.raft.serialization;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.gautelis.raft.protocol.ClientCommandRequest;
import org.gautelis.raft.protocol.ClientCommandResponse;
import org.gautelis.raft.protocol.ClientQueryRequest;
import org.gautelis.raft.protocol.ClientQueryResponse;
import org.gautelis.raft.protocol.ClusterMemberSummary;
import org.gautelis.raft.protocol.ReconfigurationStatusRequest;
import org.gautelis.raft.protocol.ReconfigurationStatusResponse;
import org.gautelis.raft.protocol.ClusterSummaryRequest;
import org.gautelis.raft.protocol.ClusterSummaryResponse;
import org.gautelis.raft.protocol.JoinClusterRequest;
import org.gautelis.raft.protocol.JoinClusterResponse;
import org.gautelis.raft.protocol.JoinClusterStatusRequest;
import org.gautelis.raft.protocol.JoinClusterStatusResponse;
import org.gautelis.raft.protocol.ReconfigureClusterRequest;
import org.gautelis.raft.protocol.ReconfigureClusterResponse;
import org.gautelis.raft.protocol.TelemetryPeerStats;
import org.gautelis.raft.protocol.TelemetryReplicationStatus;
import org.gautelis.raft.protocol.TelemetryRequest;
import org.gautelis.raft.protocol.TelemetryResponse;
import org.gautelis.raft.protocol.LogEntry;
import org.gautelis.raft.protocol.AppendEntriesRequest;
import org.gautelis.raft.protocol.AppendEntriesResponse;
import org.gautelis.raft.protocol.InstallSnapshotRequest;
import org.gautelis.raft.protocol.InstallSnapshotResponse;
import org.gautelis.raft.protocol.VoteRequest;
import org.gautelis.raft.protocol.VoteResponse;
import org.gautelis.raft.proto.Envelope;

import java.util.Optional;

/**
 * Converts between protobuf wire messages and the internal protocol/domain objects.
 */
public final class ProtoMapper {
    // Serialization boundary between in-memory Raft model objects and protobuf transport/storage messages.
    // Keeping mappings explicit makes protocol evolution safer and easier to review.
    private ProtoMapper() {}

    public static Envelope wrap(String correlationId, String type, ByteString payload) {
        return Envelope.newBuilder()
                .setCorrelationId(correlationId)
                .setType(type)
                .setPayload(payload)
                .build();
    }

    public static Envelope wrap(String correlationId, String type, byte[] payload) {
        return wrap(correlationId, type, ByteString.copyFrom(payload));
    }

    public static org.gautelis.raft.proto.VoteRequest toProto(VoteRequest request) {
        return org.gautelis.raft.proto.VoteRequest.newBuilder()
                .setTerm(request.getTerm())
                .setCandidateId(request.getCandidateId())
                .setLastLogIndex(request.getLastLogIndex())
                .setLastLogTerm(request.getLastLogTerm())
                .build();
    }

    public static VoteRequest fromProto(org.gautelis.raft.proto.VoteRequest request) {
        return new VoteRequest(
                request.getTerm(),
                request.getCandidateId(),
                request.getLastLogIndex(),
                request.getLastLogTerm()
        );
    }

    public static org.gautelis.raft.proto.VoteResponse toProto(VoteResponse response) {
        return org.gautelis.raft.proto.VoteResponse.newBuilder()
                .setPeerId(response.getPeerId())
                .setTerm(response.getTerm())
                .setVoteGranted(response.isVoteGranted())
                .setCurrentTerm(response.getCurrentTerm())
                .build();
    }

    public static VoteResponse fromProto(org.gautelis.raft.proto.VoteResponse response) {
        // VoteResponse carries the election term context directly; request placeholder is only structural.
        VoteRequest request = new VoteRequest(
                response.getTerm(),
                "",
                0L,
                0L
        );
        return new VoteResponse(request, response.getPeerId(), response.getVoteGranted(), response.getCurrentTerm());
    }

    public static org.gautelis.raft.proto.LogEntry toProto(LogEntry entry) {
        return org.gautelis.raft.proto.LogEntry.newBuilder()
                .setTerm(entry.getTerm())
                .setPeerId(entry.getPeerId())
                .setData(ByteString.copyFrom(entry.getData()))
                .build();
    }

    public static LogEntry fromProto(org.gautelis.raft.proto.LogEntry entry) {
        return new LogEntry(entry.getTerm(), entry.getPeerId(), entry.getData().toByteArray());
    }

    public static org.gautelis.raft.proto.AppendEntriesRequest toProto(AppendEntriesRequest request) {
        var builder = org.gautelis.raft.proto.AppendEntriesRequest.newBuilder()
                .setTerm(request.getTerm())
                .setLeaderId(request.getLeaderId())
                .setPrevLogIndex(request.getPrevLogIndex())
                .setPrevLogTerm(request.getPrevLogTerm())
                .setLeaderCommit(request.getLeaderCommit());
        for (LogEntry entry : request.getEntries()) {
            builder.addEntries(toProto(entry));
        }
        return builder.build();
    }

    public static AppendEntriesRequest fromProto(org.gautelis.raft.proto.AppendEntriesRequest request) {
        java.util.List<LogEntry> entries = new java.util.ArrayList<>();
        for (org.gautelis.raft.proto.LogEntry entry : request.getEntriesList()) {
            entries.add(fromProto(entry));
        }
        return new AppendEntriesRequest(
                request.getTerm(),
                request.getLeaderId(),
                request.getPrevLogIndex(),
                request.getPrevLogTerm(),
                request.getLeaderCommit(),
                entries
        );
    }

    public static org.gautelis.raft.proto.AppendEntriesResponse toProto(AppendEntriesResponse response) {
        return org.gautelis.raft.proto.AppendEntriesResponse.newBuilder()
                .setTerm(response.getTerm())
                .setPeerId(response.getPeerId())
                .setSuccess(response.isSuccess())
                .setMatchIndex(response.getMatchIndex())
                .build();
    }

    public static AppendEntriesResponse fromProto(org.gautelis.raft.proto.AppendEntriesResponse response) {
        return new AppendEntriesResponse(
                response.getTerm(),
                response.getPeerId(),
                response.getSuccess(),
                response.getMatchIndex()
        );
    }

    public static org.gautelis.raft.proto.InstallSnapshotRequest toProto(InstallSnapshotRequest request) {
        // Chunked snapshot streaming uses offset/done; one-shot callers still map to offset=0, done=true.
        return org.gautelis.raft.proto.InstallSnapshotRequest.newBuilder()
                .setTerm(request.getTerm())
                .setLeaderId(request.getLeaderId())
                .setLastIncludedIndex(request.getLastIncludedIndex())
                .setLastIncludedTerm(request.getLastIncludedTerm())
                .setOffset(request.getOffset())
                .setSnapshotData(ByteString.copyFrom(request.getSnapshotData()))
                .setDone(request.isDone())
                .build();
    }

    public static InstallSnapshotRequest fromProto(org.gautelis.raft.proto.InstallSnapshotRequest request) {
        return new InstallSnapshotRequest(
                request.getTerm(),
                request.getLeaderId(),
                request.getLastIncludedIndex(),
                request.getLastIncludedTerm(),
                request.getOffset(),
                request.getSnapshotData().toByteArray(),
                request.getDone()
        );
    }

    public static org.gautelis.raft.proto.InstallSnapshotResponse toProto(InstallSnapshotResponse response) {
        return org.gautelis.raft.proto.InstallSnapshotResponse.newBuilder()
                .setTerm(response.getTerm())
                .setPeerId(response.getPeerId())
                .setSuccess(response.isSuccess())
                .setLastIncludedIndex(response.getLastIncludedIndex())
                .build();
    }

    public static InstallSnapshotResponse fromProto(org.gautelis.raft.proto.InstallSnapshotResponse response) {
        return new InstallSnapshotResponse(
                response.getTerm(),
                response.getPeerId(),
                response.getSuccess(),
                response.getLastIncludedIndex()
        );
    }

    public static org.gautelis.raft.proto.ClientCommandRequest toProto(ClientCommandRequest request) {
        return org.gautelis.raft.proto.ClientCommandRequest.newBuilder()
                .setTerm(request.getTerm())
                .setPeerId(request.getPeerId() == null ? "" : request.getPeerId())
                .setCommand(ByteString.copyFrom(request.getCommand()))
                .setAuthScheme(request.getAuthScheme() == null ? "" : request.getAuthScheme())
                .setAuthToken(request.getAuthToken() == null ? "" : request.getAuthToken())
                .build();
    }

    public static ClientCommandRequest fromProto(org.gautelis.raft.proto.ClientCommandRequest request) {
        return new ClientCommandRequest(
                request.getTerm(),
                request.getPeerId(),
                request.getCommand().toByteArray(),
                request.getAuthScheme(),
                request.getAuthToken()
        );
    }

    public static org.gautelis.raft.proto.ClientCommandResponse toProto(ClientCommandResponse response) {
        return org.gautelis.raft.proto.ClientCommandResponse.newBuilder()
                .setTerm(response.getTerm())
                .setPeerId(response.getPeerId() == null ? "" : response.getPeerId())
                .setSuccess(response.isSuccess())
                .setStatus(response.getStatus() == null ? "" : response.getStatus())
                .setMessage(response.getMessage() == null ? "" : response.getMessage())
                .setLeaderId(response.getLeaderId() == null ? "" : response.getLeaderId())
                .setLeaderHost(response.getLeaderHost() == null ? "" : response.getLeaderHost())
                .setLeaderPort(response.getLeaderPort())
                .build();
    }

    public static ClientCommandResponse fromProto(org.gautelis.raft.proto.ClientCommandResponse response) {
        return new ClientCommandResponse(
                response.getTerm(),
                response.getPeerId(),
                response.getSuccess(),
                response.getStatus(),
                response.getMessage(),
                response.getLeaderId(),
                response.getLeaderHost(),
                response.getLeaderPort()
        );
    }

    public static org.gautelis.raft.proto.ClientQueryRequest toProto(ClientQueryRequest request) {
        return org.gautelis.raft.proto.ClientQueryRequest.newBuilder()
                .setTerm(request.getTerm())
                .setPeerId(request.getPeerId() == null ? "" : request.getPeerId())
                .setQuery(ByteString.copyFrom(request.getQuery()))
                .setAuthScheme(request.getAuthScheme() == null ? "" : request.getAuthScheme())
                .setAuthToken(request.getAuthToken() == null ? "" : request.getAuthToken())
                .build();
    }

    public static ClientQueryRequest fromProto(org.gautelis.raft.proto.ClientQueryRequest request) {
        return new ClientQueryRequest(
                request.getTerm(),
                request.getPeerId(),
                request.getQuery().toByteArray(),
                request.getAuthScheme(),
                request.getAuthToken()
        );
    }

    public static org.gautelis.raft.proto.ClientQueryResponse toProto(ClientQueryResponse response) {
        return org.gautelis.raft.proto.ClientQueryResponse.newBuilder()
                .setTerm(response.getTerm())
                .setPeerId(response.getPeerId() == null ? "" : response.getPeerId())
                .setSuccess(response.isSuccess())
                .setStatus(response.getStatus() == null ? "" : response.getStatus())
                .setMessage(response.getMessage() == null ? "" : response.getMessage())
                .setLeaderId(response.getLeaderId() == null ? "" : response.getLeaderId())
                .setLeaderHost(response.getLeaderHost() == null ? "" : response.getLeaderHost())
                .setLeaderPort(response.getLeaderPort())
                .setResult(ByteString.copyFrom(response.getResult()))
                .build();
    }

    public static ClientQueryResponse fromProto(org.gautelis.raft.proto.ClientQueryResponse response) {
        return new ClientQueryResponse(
                response.getTerm(),
                response.getPeerId(),
                response.getSuccess(),
                response.getStatus(),
                response.getMessage(),
                response.getLeaderId(),
                response.getLeaderHost(),
                response.getLeaderPort(),
                response.getResult().toByteArray()
        );
    }

    public static org.gautelis.raft.proto.ClusterSummaryRequest toProto(ClusterSummaryRequest request) {
        return org.gautelis.raft.proto.ClusterSummaryRequest.newBuilder()
                .setTerm(request.getTerm())
                .setPeerId(request.getPeerId() == null ? "" : request.getPeerId())
                .setAuthScheme(request.getAuthScheme() == null ? "" : request.getAuthScheme())
                .setAuthToken(request.getAuthToken() == null ? "" : request.getAuthToken())
                .build();
    }

    public static ClusterSummaryRequest fromProto(org.gautelis.raft.proto.ClusterSummaryRequest request) {
        return new ClusterSummaryRequest(request.getTerm(), request.getPeerId(), request.getAuthScheme(), request.getAuthToken());
    }

    public static org.gautelis.raft.proto.ReconfigurationStatusRequest toProto(ReconfigurationStatusRequest request) {
        return org.gautelis.raft.proto.ReconfigurationStatusRequest.newBuilder()
                .setTerm(request.getTerm())
                .setPeerId(request.getPeerId())
                .setAuthScheme(request.getAuthScheme() == null ? "" : request.getAuthScheme())
                .setAuthToken(request.getAuthToken() == null ? "" : request.getAuthToken())
                .build();
    }

    public static ReconfigurationStatusRequest fromProto(org.gautelis.raft.proto.ReconfigurationStatusRequest request) {
        return new ReconfigurationStatusRequest(request.getTerm(), request.getPeerId(), request.getAuthScheme(), request.getAuthToken());
    }

    public static org.gautelis.raft.proto.ClusterSummaryResponse toProto(ClusterSummaryResponse response) {
        var builder = org.gautelis.raft.proto.ClusterSummaryResponse.newBuilder()
                .setObservedAtMillis(response.getObservedAtMillis())
                .setTerm(response.getTerm())
                .setPeerId(response.getPeerId() == null ? "" : response.getPeerId())
                .setSuccess(response.isSuccess())
                .setStatus(response.getStatus() == null ? "" : response.getStatus())
                .setRedirectLeaderId(response.getRedirectLeaderId() == null ? "" : response.getRedirectLeaderId())
                .setRedirectLeaderHost(response.getRedirectLeaderHost() == null ? "" : response.getRedirectLeaderHost())
                .setRedirectLeaderPort(response.getRedirectLeaderPort())
                .setState(response.getState() == null ? "" : response.getState())
                .setLeaderId(response.getLeaderId() == null ? "" : response.getLeaderId())
                .setJointConsensus(response.isJointConsensus())
                .setClusterHealth(response.getClusterHealth() == null ? "" : response.getClusterHealth())
                .setClusterStatusReason(response.getClusterStatusReason() == null ? "" : response.getClusterStatusReason())
                .setQuorumAvailable(response.isQuorumAvailable())
                .setCurrentQuorumAvailable(response.isCurrentQuorumAvailable())
                .setNextQuorumAvailable(response.isNextQuorumAvailable())
                .setVotingMembers(response.getVotingMembers())
                .setHealthyVotingMembers(response.getHealthyVotingMembers())
                .setReachableVotingMembers(response.getReachableVotingMembers())
                .setReconfigurationAgeMillis(response.getReconfigurationAgeMillis())
                .addAllBlockingCurrentQuorumPeerIds(response.getBlockingCurrentQuorumPeerIds())
                .addAllBlockingNextQuorumPeerIds(response.getBlockingNextQuorumPeerIds());
        for (ClusterMemberSummary member : response.getMembers()) {
            builder.addMembers(org.gautelis.raft.proto.ClusterMemberSummary.newBuilder()
                    .setPeerId(member.peerId())
                    .setLocal(member.local())
                    .setCurrentMember(member.currentMember())
                    .setNextMember(member.nextMember())
                    .setVoting(member.voting())
                    .setRole(member.role() == null ? "" : member.role())
                    .setCurrentRole(member.currentRole() == null ? "" : member.currentRole())
                    .setNextRole(member.nextRole() == null ? "" : member.nextRole())
                    .setRoleTransition(member.roleTransition() == null ? "" : member.roleTransition())
                    .setTransitionAgeMillis(member.transitionAgeMillis())
                    .setBlockingQuorums(member.blockingQuorums() == null ? "" : member.blockingQuorums())
                    .setBlockingReason(member.blockingReason() == null ? "" : member.blockingReason())
                    .setReachable(member.reachable())
                    .setFreshness(member.freshness() == null ? "" : member.freshness())
                    .setHealth(member.health() == null ? "" : member.health())
                    .setNextIndex(member.nextIndex())
                    .setMatchIndex(member.matchIndex())
                    .setLag(member.lag())
                    .setConsecutiveFailures(member.consecutiveFailures())
                    .setLastSuccessfulContactMillis(member.lastSuccessfulContactMillis())
                    .setLastFailedContactMillis(member.lastFailedContactMillis()));
        }
        return builder.build();
    }

    public static ClusterSummaryResponse fromProto(org.gautelis.raft.proto.ClusterSummaryResponse response) {
        java.util.List<ClusterMemberSummary> members = new java.util.ArrayList<>();
        for (org.gautelis.raft.proto.ClusterMemberSummary member : response.getMembersList()) {
            members.add(new ClusterMemberSummary(
                    member.getPeerId(),
                    member.getLocal(),
                    member.getCurrentMember(),
                    member.getNextMember(),
                    member.getVoting(),
                    member.getRole(),
                    member.getCurrentRole(),
                    member.getNextRole(),
                    member.getRoleTransition(),
                    member.getTransitionAgeMillis(),
                    member.getBlockingQuorums(),
                    member.getBlockingReason(),
                    member.getReachable(),
                    member.getFreshness(),
                    member.getHealth(),
                    member.getNextIndex(),
                    member.getMatchIndex(),
                    member.getLag(),
                    member.getConsecutiveFailures(),
                    member.getLastSuccessfulContactMillis(),
                    member.getLastFailedContactMillis()
            ));
        }
        return new ClusterSummaryResponse(
                response.getObservedAtMillis(),
                response.getTerm(),
                response.getPeerId(),
                response.getSuccess(),
                response.getStatus(),
                response.getRedirectLeaderId(),
                response.getRedirectLeaderHost(),
                response.getRedirectLeaderPort(),
                response.getState(),
                response.getLeaderId(),
                response.getJointConsensus(),
                response.getClusterHealth(),
                response.getClusterStatusReason(),
                response.getQuorumAvailable(),
                response.getCurrentQuorumAvailable(),
                response.getNextQuorumAvailable(),
                response.getVotingMembers(),
                response.getHealthyVotingMembers(),
                response.getReachableVotingMembers(),
                response.getReconfigurationAgeMillis(),
                response.getBlockingCurrentQuorumPeerIdsList(),
                response.getBlockingNextQuorumPeerIdsList(),
                members
        );
    }

    public static org.gautelis.raft.proto.ReconfigurationStatusResponse toProto(ReconfigurationStatusResponse response) {
        var builder = org.gautelis.raft.proto.ReconfigurationStatusResponse.newBuilder()
                .setObservedAtMillis(response.getObservedAtMillis())
                .setTerm(response.getTerm())
                .setPeerId(response.getPeerId())
                .setSuccess(response.isSuccess())
                .setStatus(response.getStatus())
                .setRedirectLeaderId(response.getRedirectLeaderId())
                .setRedirectLeaderHost(response.getRedirectLeaderHost())
                .setRedirectLeaderPort(response.getRedirectLeaderPort())
                .setState(response.getState())
                .setLeaderId(response.getLeaderId())
                .setReconfigurationActive(response.isReconfigurationActive())
                .setJointConsensus(response.isJointConsensus())
                .setReconfigurationAgeMillis(response.getReconfigurationAgeMillis())
                .setClusterHealth(response.getClusterHealth())
                .setClusterStatusReason(response.getClusterStatusReason())
                .setQuorumAvailable(response.isQuorumAvailable())
                .setCurrentQuorumAvailable(response.isCurrentQuorumAvailable())
                .setNextQuorumAvailable(response.isNextQuorumAvailable())
                .addAllBlockingCurrentQuorumPeerIds(response.getBlockingCurrentQuorumPeerIds())
                .addAllBlockingNextQuorumPeerIds(response.getBlockingNextQuorumPeerIds());

        for (ClusterMemberSummary member : response.getMembers()) {
            builder.addMembers(org.gautelis.raft.proto.ClusterMemberSummary.newBuilder()
                    .setPeerId(member.peerId())
                    .setLocal(member.local())
                    .setCurrentMember(member.currentMember())
                    .setNextMember(member.nextMember())
                    .setVoting(member.voting())
                    .setRole(member.role())
                    .setCurrentRole(member.currentRole())
                    .setNextRole(member.nextRole())
                    .setRoleTransition(member.roleTransition())
                    .setTransitionAgeMillis(member.transitionAgeMillis())
                    .setBlockingQuorums(member.blockingQuorums() == null ? "" : member.blockingQuorums())
                    .setBlockingReason(member.blockingReason() == null ? "" : member.blockingReason())
                    .setReachable(member.reachable())
                    .setFreshness(member.freshness())
                    .setHealth(member.health())
                    .setNextIndex(member.nextIndex())
                    .setMatchIndex(member.matchIndex())
                    .setLag(member.lag())
                    .setConsecutiveFailures(member.consecutiveFailures())
                    .setLastSuccessfulContactMillis(member.lastSuccessfulContactMillis())
                    .setLastFailedContactMillis(member.lastFailedContactMillis())
                    .build());
        }
        return builder.build();
    }

    public static ReconfigurationStatusResponse fromProto(org.gautelis.raft.proto.ReconfigurationStatusResponse response) {
        java.util.List<ClusterMemberSummary> members = response.getMembersList().stream()
                .map(member -> new ClusterMemberSummary(
                        member.getPeerId(),
                        member.getLocal(),
                        member.getCurrentMember(),
                        member.getNextMember(),
                        member.getVoting(),
                        member.getRole(),
                        member.getCurrentRole(),
                        member.getNextRole(),
                        member.getRoleTransition(),
                        member.getTransitionAgeMillis(),
                        member.getBlockingQuorums(),
                        member.getBlockingReason(),
                        member.getReachable(),
                        member.getFreshness(),
                        member.getHealth(),
                        member.getNextIndex(),
                        member.getMatchIndex(),
                        member.getLag(),
                        member.getConsecutiveFailures(),
                        member.getLastSuccessfulContactMillis(),
                        member.getLastFailedContactMillis()
                ))
                .toList();
        return new ReconfigurationStatusResponse(
                response.getObservedAtMillis(),
                response.getTerm(),
                response.getPeerId(),
                response.getSuccess(),
                response.getStatus(),
                response.getRedirectLeaderId(),
                response.getRedirectLeaderHost(),
                response.getRedirectLeaderPort(),
                response.getState(),
                response.getLeaderId(),
                response.getReconfigurationActive(),
                response.getJointConsensus(),
                response.getReconfigurationAgeMillis(),
                response.getClusterHealth(),
                response.getClusterStatusReason(),
                response.getQuorumAvailable(),
                response.getCurrentQuorumAvailable(),
                response.getNextQuorumAvailable(),
                response.getBlockingCurrentQuorumPeerIdsList(),
                response.getBlockingNextQuorumPeerIdsList(),
                members
        );
    }

    public static org.gautelis.raft.proto.JoinClusterRequest toProto(JoinClusterRequest request) {
        var builder = org.gautelis.raft.proto.JoinClusterRequest.newBuilder()
                .setTerm(request.getTerm())
                .setPeerId(request.getPeerId() == null ? "" : request.getPeerId())
                .setAuthScheme(request.getAuthScheme() == null ? "" : request.getAuthScheme())
                .setAuthToken(request.getAuthToken() == null ? "" : request.getAuthToken());
        if (request.getJoiningPeer() != null) {
            builder.setJoiningPeerId(request.getJoiningPeer().getId());
            builder.setRole(request.getJoiningPeer().getRole().name());
            if (request.getJoiningPeer().getAddress() != null) {
                builder.setHost(request.getJoiningPeer().getAddress().getHostString());
                builder.setPort(request.getJoiningPeer().getAddress().getPort());
            }
        }
        return builder.build();
    }

    public static JoinClusterRequest fromProto(org.gautelis.raft.proto.JoinClusterRequest request) {
        java.net.InetSocketAddress address = request.getHost().isBlank() || request.getPort() <= 0
                ? null
                : new java.net.InetSocketAddress(request.getHost(), request.getPort());
        org.gautelis.raft.protocol.Peer.Role role = request.getRole().isBlank()
                ? org.gautelis.raft.protocol.Peer.Role.VOTER
                : org.gautelis.raft.protocol.Peer.Role.valueOf(request.getRole());
        return new JoinClusterRequest(
                request.getTerm(),
                request.getPeerId(),
                new org.gautelis.raft.protocol.Peer(request.getJoiningPeerId(), address, role),
                request.getAuthScheme(),
                request.getAuthToken()
        );
    }

    public static org.gautelis.raft.proto.JoinClusterResponse toProto(JoinClusterResponse response) {
        return org.gautelis.raft.proto.JoinClusterResponse.newBuilder()
                .setTerm(response.getTerm())
                .setPeerId(response.getPeerId() == null ? "" : response.getPeerId())
                .setSuccess(response.isSuccess())
                .setStatus(response.getStatus() == null ? "" : response.getStatus())
                .setMessage(response.getMessage() == null ? "" : response.getMessage())
                .setLeaderId(response.getLeaderId() == null ? "" : response.getLeaderId())
                .build();
    }

    public static JoinClusterResponse fromProto(org.gautelis.raft.proto.JoinClusterResponse response) {
        return new JoinClusterResponse(
                response.getTerm(),
                response.getPeerId(),
                response.getSuccess(),
                response.getStatus(),
                response.getMessage(),
                response.getLeaderId()
        );
    }

    public static org.gautelis.raft.proto.JoinClusterStatusRequest toProto(JoinClusterStatusRequest request) {
        return org.gautelis.raft.proto.JoinClusterStatusRequest.newBuilder()
                .setTerm(request.getTerm())
                .setPeerId(request.getPeerId() == null ? "" : request.getPeerId())
                .setTargetPeerId(request.getTargetPeerId() == null ? "" : request.getTargetPeerId())
                .setAuthScheme(request.getAuthScheme() == null ? "" : request.getAuthScheme())
                .setAuthToken(request.getAuthToken() == null ? "" : request.getAuthToken())
                .build();
    }

    public static JoinClusterStatusRequest fromProto(org.gautelis.raft.proto.JoinClusterStatusRequest request) {
        return new JoinClusterStatusRequest(
                request.getTerm(),
                request.getPeerId(),
                request.getTargetPeerId(),
                request.getAuthScheme(),
                request.getAuthToken()
        );
    }

    public static org.gautelis.raft.proto.JoinClusterStatusResponse toProto(JoinClusterStatusResponse response) {
        return org.gautelis.raft.proto.JoinClusterStatusResponse.newBuilder()
                .setTerm(response.getTerm())
                .setPeerId(response.getPeerId() == null ? "" : response.getPeerId())
                .setSuccess(response.isSuccess())
                .setStatus(response.getStatus() == null ? "" : response.getStatus())
                .setMessage(response.getMessage() == null ? "" : response.getMessage())
                .setLeaderId(response.getLeaderId() == null ? "" : response.getLeaderId())
                .build();
    }

    public static JoinClusterStatusResponse fromProto(org.gautelis.raft.proto.JoinClusterStatusResponse response) {
        return new JoinClusterStatusResponse(
                response.getTerm(),
                response.getPeerId(),
                response.getSuccess(),
                response.getStatus(),
                response.getMessage(),
                response.getLeaderId()
        );
    }

    public static org.gautelis.raft.proto.ReconfigureClusterRequest toProto(ReconfigureClusterRequest request) {
        var builder = org.gautelis.raft.proto.ReconfigureClusterRequest.newBuilder()
                .setTerm(request.getTerm())
                .setPeerId(request.getPeerId() == null ? "" : request.getPeerId())
                .setAction(request.getAction() == null ? "" : request.getAction().name())
                .setAuthScheme(request.getAuthScheme() == null ? "" : request.getAuthScheme())
                .setAuthToken(request.getAuthToken() == null ? "" : request.getAuthToken());
        for (org.gautelis.raft.protocol.Peer member : request.getMembers()) {
            var peerBuilder = org.gautelis.raft.proto.PeerSpec.newBuilder()
                    .setId(member.getId())
                    .setRole(member.getRole().name());
            if (member.getAddress() != null) {
                peerBuilder.setHost(member.getAddress().getHostString())
                        .setPort(member.getAddress().getPort());
            }
            builder.addMembers(peerBuilder);
        }
        return builder.build();
    }

    public static ReconfigureClusterRequest fromProto(org.gautelis.raft.proto.ReconfigureClusterRequest request) {
        java.util.List<org.gautelis.raft.protocol.Peer> members = new java.util.ArrayList<>();
        for (org.gautelis.raft.proto.PeerSpec member : request.getMembersList()) {
            java.net.InetSocketAddress address = member.getHost().isBlank() || member.getPort() <= 0
                    ? null
                    : new java.net.InetSocketAddress(member.getHost(), member.getPort());
            org.gautelis.raft.protocol.Peer.Role role = member.getRole().isBlank()
                    ? org.gautelis.raft.protocol.Peer.Role.VOTER
                    : org.gautelis.raft.protocol.Peer.Role.valueOf(member.getRole());
            members.add(new org.gautelis.raft.protocol.Peer(member.getId(), address, role));
        }
        return new ReconfigureClusterRequest(
                request.getTerm(),
                request.getPeerId(),
                ReconfigureClusterRequest.Action.valueOf(request.getAction()),
                members,
                request.getAuthScheme(),
                request.getAuthToken()
        );
    }

    public static org.gautelis.raft.proto.ReconfigureClusterResponse toProto(ReconfigureClusterResponse response) {
        return org.gautelis.raft.proto.ReconfigureClusterResponse.newBuilder()
                .setTerm(response.getTerm())
                .setPeerId(response.getPeerId() == null ? "" : response.getPeerId())
                .setSuccess(response.isSuccess())
                .setStatus(response.getStatus() == null ? "" : response.getStatus())
                .setMessage(response.getMessage() == null ? "" : response.getMessage())
                .setLeaderId(response.getLeaderId() == null ? "" : response.getLeaderId())
                .build();
    }

    public static ReconfigureClusterResponse fromProto(org.gautelis.raft.proto.ReconfigureClusterResponse response) {
        return new ReconfigureClusterResponse(
                response.getTerm(),
                response.getPeerId(),
                response.getSuccess(),
                response.getStatus(),
                response.getMessage(),
                response.getLeaderId()
        );
    }

    public static org.gautelis.raft.proto.TelemetryRequest toProto(TelemetryRequest request) {
        return org.gautelis.raft.proto.TelemetryRequest.newBuilder()
                .setTerm(request.getTerm())
                .setPeerId(request.getPeerId() == null ? "" : request.getPeerId())
                .setIncludePeerStats(request.isIncludePeerStats())
                .setRequireLeaderSummary(request.isRequireLeaderSummary())
                .setAuthScheme(request.getAuthScheme() == null ? "" : request.getAuthScheme())
                .setAuthToken(request.getAuthToken() == null ? "" : request.getAuthToken())
                .build();
    }

    public static TelemetryRequest fromProto(org.gautelis.raft.proto.TelemetryRequest request) {
        return new TelemetryRequest(
                request.getTerm(),
                request.getPeerId(),
                request.getIncludePeerStats(),
                request.getRequireLeaderSummary(),
                request.getAuthScheme(),
                request.getAuthToken()
        );
    }

    public static org.gautelis.raft.proto.TelemetryResponse toProto(TelemetryResponse response) {
        var builder = org.gautelis.raft.proto.TelemetryResponse.newBuilder()
                .setObservedAtMillis(response.getObservedAtMillis())
                .setTerm(response.getTerm())
                .setPeerId(response.getPeerId() == null ? "" : response.getPeerId())
                .setSuccess(response.isSuccess())
                .setStatus(response.getStatus() == null ? "" : response.getStatus())
                .setRedirectLeaderId(response.getRedirectLeaderId() == null ? "" : response.getRedirectLeaderId())
                .setState(response.getState() == null ? "" : response.getState())
                .setLeaderId(response.getLeaderId() == null ? "" : response.getLeaderId())
                .setVotedFor(response.getVotedFor() == null ? "" : response.getVotedFor())
                .setJoining(response.isJoining())
                .setDecommissioned(response.isDecommissioned())
                .setCommitIndex(response.getCommitIndex())
                .setLastApplied(response.getLastApplied())
                .setLastLogIndex(response.getLastLogIndex())
                .setLastLogTerm(response.getLastLogTerm())
                .setSnapshotIndex(response.getSnapshotIndex())
                .setSnapshotTerm(response.getSnapshotTerm())
                .setLastHeartbeatMillis(response.getLastHeartbeatMillis())
                .setNextElectionDeadlineMillis(response.getNextElectionDeadlineMillis())
                .setJointConsensus(response.isJointConsensus())
                .setClusterHealth(response.getClusterHealth() == null ? "" : response.getClusterHealth())
                .setQuorumAvailable(response.isQuorumAvailable())
                .setCurrentQuorumAvailable(response.isCurrentQuorumAvailable())
                .setNextQuorumAvailable(response.isNextQuorumAvailable())
                .setVotingMembers(response.getVotingMembers())
                .setHealthyVotingMembers(response.getHealthyVotingMembers())
                .setReachableVotingMembers(response.getReachableVotingMembers())
                .setClusterStatusReason(response.getClusterStatusReason() == null ? "" : response.getClusterStatusReason());
        builder.setReconfigurationAgeMillis(response.getReconfigurationAgeMillis());
        builder.addAllBlockingCurrentQuorumPeerIds(response.getBlockingCurrentQuorumPeerIds());
        builder.addAllBlockingNextQuorumPeerIds(response.getBlockingNextQuorumPeerIds());
        for (var peer : response.getCurrentMembers()) {
            builder.addCurrentMembers(toPeerSpec(peer));
        }
        for (var peer : response.getNextMembers()) {
            builder.addNextMembers(toPeerSpec(peer));
        }
        for (var peer : response.getKnownPeers()) {
            builder.addKnownPeers(toPeerSpec(peer));
        }
        builder.addAllPendingJoinIds(response.getPendingJoinIds());
        for (var status : response.getReplication()) {
            builder.addReplication(org.gautelis.raft.proto.TelemetryReplicationStatus.newBuilder()
                    .setPeerId(status.peerId())
                    .setNextIndex(status.nextIndex())
                    .setMatchIndex(status.matchIndex())
                    .setReachable(status.reachable())
                    .setLastSuccessfulContactMillis(status.lastSuccessfulContactMillis())
                    .setConsecutiveFailures(status.consecutiveFailures())
                    .setLastFailedContactMillis(status.lastFailedContactMillis())
                    .build());
        }
        for (var stats : response.getPeerStats()) {
            builder.addPeerStats(org.gautelis.raft.proto.TelemetryPeerStats.newBuilder()
                    .setPeerId(stats.peerId())
                    .setSamples(stats.samples())
                    .setMeanMillis(stats.meanMillis())
                    .setMinMillis(stats.minMillis())
                    .setMaxMillis(stats.maxMillis())
                    .setCvPercent(stats.cvPercent())
                    .setRpcType(stats.rpcType() == null ? "" : stats.rpcType())
                    .build());
        }
        for (ClusterMemberSummary member : response.getClusterMembers()) {
            builder.addClusterMembers(org.gautelis.raft.proto.ClusterMemberSummary.newBuilder()
                    .setPeerId(member.peerId())
                    .setLocal(member.local())
                    .setCurrentMember(member.currentMember())
                    .setNextMember(member.nextMember())
                    .setVoting(member.voting())
                    .setRole(member.role() == null ? "" : member.role())
                    .setCurrentRole(member.currentRole() == null ? "" : member.currentRole())
                    .setNextRole(member.nextRole() == null ? "" : member.nextRole())
                    .setRoleTransition(member.roleTransition() == null ? "" : member.roleTransition())
                    .setTransitionAgeMillis(member.transitionAgeMillis())
                    .setBlockingQuorums(member.blockingQuorums() == null ? "" : member.blockingQuorums())
                    .setBlockingReason(member.blockingReason() == null ? "" : member.blockingReason())
                    .setReachable(member.reachable())
                    .setFreshness(member.freshness() == null ? "" : member.freshness())
                    .setHealth(member.health() == null ? "" : member.health())
                    .setNextIndex(member.nextIndex())
                    .setMatchIndex(member.matchIndex())
                    .setLag(member.lag())
                    .setConsecutiveFailures(member.consecutiveFailures())
                    .setLastSuccessfulContactMillis(member.lastSuccessfulContactMillis())
                    .setLastFailedContactMillis(member.lastFailedContactMillis()));
        }
        return builder.build();
    }

    public static TelemetryResponse fromProto(org.gautelis.raft.proto.TelemetryResponse response) {
        java.util.List<org.gautelis.raft.protocol.Peer> currentMembers = new java.util.ArrayList<>();
        for (var member : response.getCurrentMembersList()) {
            currentMembers.add(fromPeerSpec(member));
        }
        java.util.List<org.gautelis.raft.protocol.Peer> nextMembers = new java.util.ArrayList<>();
        for (var member : response.getNextMembersList()) {
            nextMembers.add(fromPeerSpec(member));
        }
        java.util.List<org.gautelis.raft.protocol.Peer> knownPeers = new java.util.ArrayList<>();
        for (var member : response.getKnownPeersList()) {
            knownPeers.add(fromPeerSpec(member));
        }
        java.util.List<TelemetryReplicationStatus> replication = new java.util.ArrayList<>();
        for (var item : response.getReplicationList()) {
            replication.add(new TelemetryReplicationStatus(
                    item.getPeerId(),
                    item.getNextIndex(),
                    item.getMatchIndex(),
                    item.getReachable(),
                    item.getLastSuccessfulContactMillis(),
                    item.getConsecutiveFailures(),
                    item.getLastFailedContactMillis()
            ));
        }
        java.util.List<TelemetryPeerStats> peerStats = new java.util.ArrayList<>();
        for (var item : response.getPeerStatsList()) {
            peerStats.add(new TelemetryPeerStats(
                    item.getPeerId(),
                    item.getRpcType(),
                    item.getSamples(),
                    item.getMeanMillis(),
                    item.getMinMillis(),
                    item.getMaxMillis(),
                    item.getCvPercent()
            ));
        }
        java.util.List<ClusterMemberSummary> clusterMembers = new java.util.ArrayList<>();
        for (var member : response.getClusterMembersList()) {
            clusterMembers.add(new ClusterMemberSummary(
                    member.getPeerId(),
                    member.getLocal(),
                    member.getCurrentMember(),
                    member.getNextMember(),
                    member.getVoting(),
                    member.getRole(),
                    member.getCurrentRole(),
                    member.getNextRole(),
                    member.getRoleTransition(),
                    member.getTransitionAgeMillis(),
                    member.getBlockingQuorums(),
                    member.getBlockingReason(),
                    member.getReachable(),
                    member.getFreshness(),
                    member.getHealth(),
                    member.getNextIndex(),
                    member.getMatchIndex(),
                    member.getLag(),
                    member.getConsecutiveFailures(),
                    member.getLastSuccessfulContactMillis(),
                    member.getLastFailedContactMillis()
            ));
        }
        return new TelemetryResponse(
                response.getObservedAtMillis(),
                response.getTerm(),
                response.getPeerId(),
                response.getSuccess(),
                response.getStatus(),
                response.getRedirectLeaderId(),
                response.getState(),
                response.getLeaderId(),
                response.getVotedFor(),
                response.getJoining(),
                response.getDecommissioned(),
                response.getCommitIndex(),
                response.getLastApplied(),
                response.getLastLogIndex(),
                response.getLastLogTerm(),
                response.getSnapshotIndex(),
                response.getSnapshotTerm(),
                response.getLastHeartbeatMillis(),
                response.getNextElectionDeadlineMillis(),
                response.getJointConsensus(),
                currentMembers,
                nextMembers,
                knownPeers,
                response.getPendingJoinIdsList(),
                replication,
                peerStats,
                response.getClusterHealth(),
                response.getQuorumAvailable(),
                response.getCurrentQuorumAvailable(),
                response.getNextQuorumAvailable(),
                response.getVotingMembers(),
                response.getHealthyVotingMembers(),
                response.getReachableVotingMembers(),
                response.getReconfigurationAgeMillis(),
                response.getClusterStatusReason(),
                response.getBlockingCurrentQuorumPeerIdsList(),
                response.getBlockingNextQuorumPeerIdsList(),
                clusterMembers
        );
    }

    public static Optional<org.gautelis.raft.proto.VoteRequest> parseVoteRequest(byte[] payload) {
        try {
            return Optional.of(org.gautelis.raft.proto.VoteRequest.parseFrom(payload));
        } catch (InvalidProtocolBufferException e) {
            return Optional.empty();
        }
    }

    public static Optional<org.gautelis.raft.proto.VoteResponse> parseVoteResponse(byte[] payload) {
        try {
            return Optional.of(org.gautelis.raft.proto.VoteResponse.parseFrom(payload));
        } catch (InvalidProtocolBufferException e) {
            return Optional.empty();
        }
    }

    public static Optional<org.gautelis.raft.proto.ClientCommandRequest> parseClientCommandRequest(byte[] payload) {
        try {
            return Optional.of(org.gautelis.raft.proto.ClientCommandRequest.parseFrom(payload));
        } catch (InvalidProtocolBufferException e) {
            return Optional.empty();
        }
    }

    public static Optional<org.gautelis.raft.proto.ClientCommandResponse> parseClientCommandResponse(byte[] payload) {
        try {
            return Optional.of(org.gautelis.raft.proto.ClientCommandResponse.parseFrom(payload));
        } catch (InvalidProtocolBufferException e) {
            return Optional.empty();
        }
    }

    public static Optional<org.gautelis.raft.proto.ClientQueryRequest> parseClientQueryRequest(byte[] payload) {
        try {
            return Optional.of(org.gautelis.raft.proto.ClientQueryRequest.parseFrom(payload));
        } catch (InvalidProtocolBufferException e) {
            return Optional.empty();
        }
    }

    public static Optional<org.gautelis.raft.proto.ClientQueryResponse> parseClientQueryResponse(byte[] payload) {
        try {
            return Optional.of(org.gautelis.raft.proto.ClientQueryResponse.parseFrom(payload));
        } catch (InvalidProtocolBufferException e) {
            return Optional.empty();
        }
    }

    public static Optional<org.gautelis.raft.proto.ClusterSummaryRequest> parseClusterSummaryRequest(byte[] payload) {
        try {
            return Optional.of(org.gautelis.raft.proto.ClusterSummaryRequest.parseFrom(payload));
        } catch (InvalidProtocolBufferException e) {
            return Optional.empty();
        }
    }

    public static Optional<org.gautelis.raft.proto.ClusterSummaryResponse> parseClusterSummaryResponse(byte[] payload) {
        try {
            return Optional.of(org.gautelis.raft.proto.ClusterSummaryResponse.parseFrom(payload));
        } catch (InvalidProtocolBufferException e) {
            return Optional.empty();
        }
    }

    public static Optional<org.gautelis.raft.proto.ReconfigurationStatusRequest> parseReconfigurationStatusRequest(byte[] payload) {
        try {
            return Optional.of(org.gautelis.raft.proto.ReconfigurationStatusRequest.parseFrom(payload));
        } catch (InvalidProtocolBufferException e) {
            return Optional.empty();
        }
    }

    public static Optional<org.gautelis.raft.proto.ReconfigurationStatusResponse> parseReconfigurationStatusResponse(byte[] payload) {
        try {
            return Optional.of(org.gautelis.raft.proto.ReconfigurationStatusResponse.parseFrom(payload));
        } catch (InvalidProtocolBufferException e) {
            return Optional.empty();
        }
    }

    public static Optional<org.gautelis.raft.proto.JoinClusterRequest> parseJoinClusterRequest(byte[] payload) {
        try {
            return Optional.of(org.gautelis.raft.proto.JoinClusterRequest.parseFrom(payload));
        } catch (InvalidProtocolBufferException e) {
            return Optional.empty();
        }
    }

    public static Optional<org.gautelis.raft.proto.JoinClusterResponse> parseJoinClusterResponse(byte[] payload) {
        try {
            return Optional.of(org.gautelis.raft.proto.JoinClusterResponse.parseFrom(payload));
        } catch (InvalidProtocolBufferException e) {
            return Optional.empty();
        }
    }

    public static Optional<org.gautelis.raft.proto.JoinClusterStatusRequest> parseJoinClusterStatusRequest(byte[] payload) {
        try {
            return Optional.of(org.gautelis.raft.proto.JoinClusterStatusRequest.parseFrom(payload));
        } catch (InvalidProtocolBufferException e) {
            return Optional.empty();
        }
    }

    public static Optional<org.gautelis.raft.proto.JoinClusterStatusResponse> parseJoinClusterStatusResponse(byte[] payload) {
        try {
            return Optional.of(org.gautelis.raft.proto.JoinClusterStatusResponse.parseFrom(payload));
        } catch (InvalidProtocolBufferException e) {
            return Optional.empty();
        }
    }

    public static Optional<org.gautelis.raft.proto.ReconfigureClusterRequest> parseReconfigureClusterRequest(byte[] payload) {
        try {
            return Optional.of(org.gautelis.raft.proto.ReconfigureClusterRequest.parseFrom(payload));
        } catch (InvalidProtocolBufferException e) {
            return Optional.empty();
        }
    }

    public static Optional<org.gautelis.raft.proto.ReconfigureClusterResponse> parseReconfigureClusterResponse(byte[] payload) {
        try {
            return Optional.of(org.gautelis.raft.proto.ReconfigureClusterResponse.parseFrom(payload));
        } catch (InvalidProtocolBufferException e) {
            return Optional.empty();
        }
    }

    public static Optional<org.gautelis.raft.proto.TelemetryRequest> parseTelemetryRequest(byte[] payload) {
        try {
            return Optional.of(org.gautelis.raft.proto.TelemetryRequest.parseFrom(payload));
        } catch (InvalidProtocolBufferException e) {
            return Optional.empty();
        }
    }

    public static Optional<org.gautelis.raft.proto.TelemetryResponse> parseTelemetryResponse(byte[] payload) {
        try {
            return Optional.of(org.gautelis.raft.proto.TelemetryResponse.parseFrom(payload));
        } catch (InvalidProtocolBufferException e) {
            return Optional.empty();
        }
    }

    private static org.gautelis.raft.proto.PeerSpec toPeerSpec(org.gautelis.raft.protocol.Peer member) {
        var builder = org.gautelis.raft.proto.PeerSpec.newBuilder()
                .setId(member.getId())
                .setRole(member.getRole().name());
        if (member.getAddress() != null) {
            builder.setHost(member.getAddress().getHostString())
                    .setPort(member.getAddress().getPort());
        }
        return builder.build();
    }

    private static org.gautelis.raft.protocol.Peer fromPeerSpec(org.gautelis.raft.proto.PeerSpec member) {
        java.net.InetSocketAddress address = member.getHost().isBlank() || member.getPort() <= 0
                ? null
                : new java.net.InetSocketAddress(member.getHost(), member.getPort());
        org.gautelis.raft.protocol.Peer.Role role = member.getRole().isBlank()
                ? org.gautelis.raft.protocol.Peer.Role.VOTER
                : org.gautelis.raft.protocol.Peer.Role.valueOf(member.getRole());
        return new org.gautelis.raft.protocol.Peer(member.getId(), address, role);
    }

    public static Optional<org.gautelis.raft.proto.AppendEntriesRequest> parseAppendEntriesRequest(byte[] payload) {
        try {
            return Optional.of(org.gautelis.raft.proto.AppendEntriesRequest.parseFrom(payload));
        } catch (InvalidProtocolBufferException e) {
            return Optional.empty();
        }
    }

    public static Optional<org.gautelis.raft.proto.AppendEntriesResponse> parseAppendEntriesResponse(byte[] payload) {
        try {
            return Optional.of(org.gautelis.raft.proto.AppendEntriesResponse.parseFrom(payload));
        } catch (InvalidProtocolBufferException e) {
            return Optional.empty();
        }
    }

    public static Optional<org.gautelis.raft.proto.InstallSnapshotRequest> parseInstallSnapshotRequest(byte[] payload) {
        try {
            // Parsing stays explicit here so transport callers can reject malformed payloads
            // without letting protobuf exceptions escape the handler path.
            return Optional.of(org.gautelis.raft.proto.InstallSnapshotRequest.parseFrom(payload));
        } catch (InvalidProtocolBufferException e) {
            return Optional.empty();
        }
    }

    public static Optional<org.gautelis.raft.proto.InstallSnapshotResponse> parseInstallSnapshotResponse(byte[] payload) {
        try {
            return Optional.of(org.gautelis.raft.proto.InstallSnapshotResponse.parseFrom(payload));
        } catch (InvalidProtocolBufferException e) {
            return Optional.empty();
        }
    }
}
