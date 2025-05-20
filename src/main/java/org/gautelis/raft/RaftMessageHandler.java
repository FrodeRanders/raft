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
public class RaftMessageHandler extends SimpleChannelInboundHandler<JsonNode> {
    private static final Logger log = LoggerFactory.getLogger(RaftMessageHandler.class);

    private final ObjectMapper mapper = new ObjectMapper();
    private final RaftStateMachine stateMachine;

    public RaftMessageHandler(RaftStateMachine raftServer) {
        this.stateMachine = raftServer;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Error in channel: {}", cause.getMessage(), cause);
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
            case "LogEntry" -> {
                LogEntry entry = mapper.treeToValue(payload, LogEntry.class);
                stateMachine.handleLogEntry(entry);

                // no expected response if it's just a heartbeat
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
