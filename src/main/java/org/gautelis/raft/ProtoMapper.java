package org.gautelis.raft;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.gautelis.raft.model.ClusterMessage;
import org.gautelis.raft.model.Heartbeat;
import org.gautelis.raft.model.LogEntry;
import org.gautelis.raft.model.VoteRequest;
import org.gautelis.raft.model.VoteResponse;
import org.gautelis.raft.proto.Envelope;

import java.util.Optional;

public final class ProtoMapper {
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

    public static org.gautelis.raft.proto.Heartbeat toProto(Heartbeat heartbeat) {
        return org.gautelis.raft.proto.Heartbeat.newBuilder()
                .setTerm(heartbeat.getTerm())
                .setPeerId(heartbeat.getPeerId())
                .build();
    }

    public static Heartbeat fromProto(org.gautelis.raft.proto.Heartbeat heartbeat) {
        return new Heartbeat(heartbeat.getTerm(), heartbeat.getPeerId());
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
                .build();
    }

    public static LogEntry fromProto(org.gautelis.raft.proto.LogEntry entry) {
        return new LogEntry(entry.getTerm(), entry.getPeerId());
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

    public static Optional<org.gautelis.raft.proto.Heartbeat> parseHeartbeat(byte[] payload) {
        try {
            return Optional.of(org.gautelis.raft.proto.Heartbeat.parseFrom(payload));
        } catch (InvalidProtocolBufferException e) {
            return Optional.empty();
        }
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

    public static Optional<org.gautelis.raft.proto.LogEntry> parseLogEntry(byte[] payload) {
        try {
            return Optional.of(org.gautelis.raft.proto.LogEntry.parseFrom(payload));
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
}
