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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.gautelis.raft.model.*;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.nio.charset.StandardCharsets;

@ChannelHandler.Sharable
public class RaftMessageHandler extends SimpleChannelInboundHandler<JsonNode> {
    private static final Logger log = LoggerFactory.getLogger(RaftMessageHandler.class);

    private final ObjectMapper mapper = new ObjectMapper();
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
    protected void channelRead0(ChannelHandlerContext ctx, JsonNode jsonNode) throws Exception {
        log.trace("Received message: {}", jsonNode);

        // Expect a structure like: { "correlationId":"<UUID>", "type": "VoteRequest", "payload": ... }
        JsonNode _correlationId = jsonNode.get("correlationId");
        if (null == _correlationId) {
            log.warn("Incorrect message: No correlationId");
            return;
        }
        String correlationId = _correlationId.asText();
        String type = jsonNode.get("type").asText();
        JsonNode payload = jsonNode.get("payload");

        switch (type) {
            case "Heartbeat" -> {
                Heartbeat heartbeat = mapper.treeToValue(payload, Heartbeat.class);
                stateMachine.handleHeartbeat(heartbeat);
                // no expected response
            }
            case "LogEntry" -> {
                LogEntry entry = mapper.treeToValue(payload, LogEntry.class);
                stateMachine.handleLogEntry(entry);
                // no expected response
            }
            case "VoteRequest" -> {
                VoteRequest voteRequest = mapper.treeToValue(payload, VoteRequest.class);
                VoteResponse voteResponse = stateMachine.handleVoteRequest(voteRequest);

                // Server response
                String peerId = voteRequest.getCandidateId();
                Message response = new Message(correlationId, "VoteResponse", voteResponse);
                String responseJson = mapper.writeValueAsString(response);

                try {
                    log.trace(
                            "Sending vote response to {}: {}",
                            peerId, voteResponse.isVoteGranted() ? "granted" : "denied"
                    );

                    ctx.writeAndFlush(Unpooled.copiedBuffer(responseJson, StandardCharsets.UTF_8))
                            .addListener(f -> {
                                if (!f.isSuccess()) {
                                    log.warn("Could not send vote response to {}: {}", peerId, f.cause());
                                }
                            });
                } catch (Exception e) {
                    log.warn("Failed to send vote response to {}: {}", peerId, e.getMessage(), e);
                }
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
