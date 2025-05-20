package org.gautelis.raft;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.gautelis.raft.model.VoteResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class ClientResponseHandler extends SimpleChannelInboundHandler<JsonNode> {
    private static final Logger log = LoggerFactory.getLogger(ClientResponseHandler.class);

    private final ObjectMapper mapper = new ObjectMapper();

    //
    private final MessageHandler messageHandler;

    // This map is shared with NettyRaftClient, so we can fulfill the waiting futures.
    // Key = correlationId, Value = future that awaits the response.
    private final Map<String, CompletableFuture<VoteResponse>> inFlightRequests;

    public ClientResponseHandler(
            Map<String, CompletableFuture<VoteResponse>> inFlightRequests,
            MessageHandler messageHandler
    ) {
        this.inFlightRequests = inFlightRequests;
        this.messageHandler = messageHandler;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, JsonNode jsonNode) throws Exception {
        // e.g. {"correlationId": "...", "type": "VoteResponse", "payload": {...}}
        if (!jsonNode.has("type")) {
            log.warn("No 'type' in JSON: {}", jsonNode);
            return;
        }
        if (!jsonNode.has("correlationId")) {
            log.warn("No 'correlationId' in JSON: {}", jsonNode);
            return;
        }

        // Expect a structure like: { "correlationId":"<UUID>", "type": "VoteRequest", "payload": ... }
        JsonNode _correlationId = jsonNode.get("correlationId");
        if (null == _correlationId) {
            log.warn("Incorrect message: No correlationId");
            return;
        }
        String correlationId = _correlationId.asText();
        String type = jsonNode.get("type").asText();

        switch (type) {
            case "VoteResponse" -> {
                VoteResponse voteResponse = mapper.treeToValue(jsonNode.get("payload"), VoteResponse.class);

                // Lookup the future for this correlationId
                CompletableFuture<VoteResponse> fut = inFlightRequests.remove(correlationId);
                if (fut != null) {
                    fut.complete(voteResponse);
                    log.trace("Received {} for correlationId={}", voteResponse, correlationId);
                } else {
                    log.warn("No future found for VoteResponse correlationId={}", correlationId);
                }
            }

            default -> {
                if (null != messageHandler) {
                    messageHandler.handle(correlationId, type, jsonNode.get("payload"), ctx);
                }
            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Error in client response handler", cause);
        ctx.close();
    }
}