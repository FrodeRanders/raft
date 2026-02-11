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
package org.gautelis.raft.transport.netty;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.gautelis.raft.MessageHandler;
import org.gautelis.raft.RaftNode;
import org.gautelis.raft.protocol.*;
import org.gautelis.raft.proto.Envelope;
import org.gautelis.raft.serialization.ProtoMapper;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

@ChannelHandler.Sharable
public class RaftMessageHandler extends SimpleChannelInboundHandler<Envelope> {
    // Netty transport adapter for Raft RPCs.
    // Figure 2 receiver rules are implemented in RaftNode; this class only parses, dispatches, and replies.
    private static final Logger log = LoggerFactory.getLogger(RaftMessageHandler.class);

    private final RaftNode stateMachine;

    public RaftMessageHandler(RaftNode raftServer) {
        this.stateMachine = raftServer;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Error in server channel: {}", cause.getMessage(), cause);
        ctx.close();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Envelope envelope) throws Exception {
        // Transport-to-state-machine bridge:
        // map typed RPC envelopes onto RaftNode receiver implementations (Figure 2 request handlers).
        log.trace("Received message: {}", envelope);

        String correlationId = envelope.getCorrelationId();
        if (correlationId == null || correlationId.isEmpty()) {
            log.warn("Incorrect message: No correlationId");
            return;
        }
        String type = envelope.getType();
        if (type == null || type.isEmpty()) {
            log.warn("Incorrect message: No type");
            return;
        }
        byte[] payload = envelope.getPayload().toByteArray();

        switch (type) {
            case "VoteRequest" -> {
                // Figure 2 RequestVote receiver logic lives in RaftNode.handleVoteRequest.
                var parsed = ProtoMapper.parseVoteRequest(payload);
                if (parsed.isEmpty()) {
                    log.warn("Failed to parse VoteRequest payload");
                    return;
                }

                VoteRequest voteRequest = ProtoMapper.fromProto(parsed.get());
                VoteResponse voteResponse = stateMachine.handleVoteRequest(voteRequest);

                // Server response
                String peerId = voteRequest.getCandidateId();
                var responsePayload = ProtoMapper.toProto(voteResponse);
                Envelope response = ProtoMapper.wrap(correlationId, "VoteResponse", responsePayload.toByteString());

                try {
                    log.trace(
                            "Sending vote response to {}: {}",
                            peerId, voteResponse.isVoteGranted() ? "granted" : "denied"
                    );

                    ctx.writeAndFlush(response)
                            .addListener(f -> {
                                if (!f.isSuccess()) {
                                    log.warn("Could not send vote response to {}: {}", peerId, f.cause());
                                }
                            });
                } catch (Exception e) {
                    log.warn("Failed to send vote response to {}: {}", peerId, e.getMessage(), e);
                }
            }
            case "AppendEntriesRequest" -> {
                // Figure 2 AppendEntries receiver steps 1-5 live in RaftNode.handleAppendEntries.
                var parsed = ProtoMapper.parseAppendEntriesRequest(payload);
                if (parsed.isEmpty()) {
                    log.warn("Failed to parse AppendEntriesRequest payload");
                    return;
                }

                AppendEntriesRequest request = ProtoMapper.fromProto(parsed.get());
                AppendEntriesResponse response = stateMachine.handleAppendEntries(request);

                String peerId = request.getLeaderId();
                var responsePayload = ProtoMapper.toProto(response);
                Envelope envelopeResponse = ProtoMapper.wrap(correlationId, "AppendEntriesResponse", responsePayload.toByteString());
                ctx.writeAndFlush(envelopeResponse)
                        .addListener(f -> {
                            if (!f.isSuccess()) {
                                log.warn("Could not send AppendEntriesResponse to {}: {}", peerId, f.cause());
                            }
                        });
            }
            case "InstallSnapshotRequest" -> {
                // Snapshot installation receiver path (companion to log compaction/catch-up).
                var parsed = ProtoMapper.parseInstallSnapshotRequest(payload);
                if (parsed.isEmpty()) {
                    log.warn("Failed to parse InstallSnapshotRequest payload");
                    return;
                }

                InstallSnapshotRequest request = ProtoMapper.fromProto(parsed.get());
                InstallSnapshotResponse response = stateMachine.handleInstallSnapshot(request);

                String peerId = request.getLeaderId();
                var responsePayload = ProtoMapper.toProto(response);
                Envelope envelopeResponse = ProtoMapper.wrap(correlationId, "InstallSnapshotResponse", responsePayload.toByteString());
                ctx.writeAndFlush(envelopeResponse)
                        .addListener(f -> {
                            if (!f.isSuccess()) {
                                log.warn("Could not send InstallSnapshotResponse to {}: {}", peerId, f.cause());
                            }
                        });
            }
            default -> {
                MessageHandler messageHandler = stateMachine.getMessageHandler();
                if (null != messageHandler) {
                    messageHandler.handle(correlationId, type, payload, ctx);
                }
            }
        }
    }
}
