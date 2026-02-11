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
import org.gautelis.raft.protocol.ClusterMessage;
import org.gautelis.raft.protocol.LogEntry;
import org.gautelis.raft.protocol.AppendEntriesRequest;
import org.gautelis.raft.protocol.AppendEntriesResponse;
import org.gautelis.raft.protocol.InstallSnapshotRequest;
import org.gautelis.raft.protocol.InstallSnapshotResponse;
import org.gautelis.raft.protocol.VoteRequest;
import org.gautelis.raft.protocol.VoteResponse;
import org.gautelis.raft.proto.Envelope;

import java.util.Optional;

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
        return org.gautelis.raft.proto.InstallSnapshotRequest.newBuilder()
                .setTerm(request.getTerm())
                .setLeaderId(request.getLeaderId())
                .setLastIncludedIndex(request.getLastIncludedIndex())
                .setLastIncludedTerm(request.getLastIncludedTerm())
                .setSnapshotData(ByteString.copyFrom(request.getSnapshotData()))
                .build();
    }

    public static InstallSnapshotRequest fromProto(org.gautelis.raft.proto.InstallSnapshotRequest request) {
        return new InstallSnapshotRequest(
                request.getTerm(),
                request.getLeaderId(),
                request.getLastIncludedIndex(),
                request.getLastIncludedTerm(),
                request.getSnapshotData().toByteArray()
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

    public static org.gautelis.raft.proto.ClusterMessage toProto(ClusterMessage message) {
        return org.gautelis.raft.proto.ClusterMessage.newBuilder()
                .setTerm(message.getTerm())
                .setPeerId(message.getPeerId())
                .setMessage(message.getMessage())
                .build();
    }

    public static ClusterMessage fromProto(org.gautelis.raft.proto.ClusterMessage message) {
        return new ClusterMessage(message.getTerm(), message.getPeerId(), message.getMessage());
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

    public static Optional<org.gautelis.raft.proto.ClusterMessage> parseClusterMessage(byte[] payload) {
        try {
            return Optional.of(org.gautelis.raft.proto.ClusterMessage.parseFrom(payload));
        } catch (InvalidProtocolBufferException e) {
            return Optional.empty();
        }
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
