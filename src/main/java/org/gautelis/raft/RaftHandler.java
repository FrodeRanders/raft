package org.gautelis.raft;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.gautelis.raft.model.LogEntry;
import org.gautelis.raft.model.Message;
import org.gautelis.raft.model.VoteRequest;
import org.gautelis.raft.model.VoteResponse;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.nio.charset.StandardCharsets;

@ChannelHandler.Sharable
public class RaftHandler extends SimpleChannelInboundHandler<JsonNode> {
    private static final Logger log = LoggerFactory.getLogger(RaftHandler.class);

    private final ObjectMapper mapper = new ObjectMapper();
    private final RaftStateMachine stateMachine;

    public RaftHandler(RaftStateMachine raftServer) {
        this.stateMachine = raftServer;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Error in channel: ", cause);
        ctx.close();
    }

    @Override
    protected void messageReceived(ChannelHandlerContext ctx, JsonNode jsonNode) throws Exception {
        log.trace("Received message: {}", jsonNode);

        // Expect a structure like: { "type": "VoteRequest", "payload": ... }
        String requestId = jsonNode.get("requestId").asText();
        String type = jsonNode.get("type").asText();
        JsonNode payload = jsonNode.get("payload");

        switch (type) {
            case "VoteRequest" -> {
                VoteRequest voteRequest = mapper.treeToValue(payload, VoteRequest.class);
                VoteResponse voteResponse = stateMachine.handleVoteRequest(voteRequest);

                // Server response
                String peerId = voteRequest.getCandidateId();
                Message responseMsg = new Message(requestId, "VoteResponse", voteResponse);
                String json = mapper.writeValueAsString(responseMsg);

                try {
                    log.trace("Sending on channel {}: vote response to {}: {}", ctx.channel().id(),  peerId, json);

                    ctx.writeAndFlush(Unpooled.copiedBuffer(json, StandardCharsets.UTF_8))
                            .addListener(f -> {
                                if (!f.isSuccess()) {
                                    log.warn("Could not send vote response to {}: {}", peerId, f.cause());
                                }
                            });
                } catch (Exception e) {
                    log.warn("Failed to send vote response to {}: {}", peerId, e.getMessage(), e);
                }
            }
            case "LogEntry" -> {
                LogEntry entry = mapper.treeToValue(payload, LogEntry.class);
                stateMachine.handleLogEntry(entry);
                // Possibly no direct response if it's just a heartbeat
            }
            // Other message types...
            default -> log.warn("Unknown message type: {}", type);
        }
    }
}
