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

    // This map is shared with NettyRaftClient, so we can fulfill the waiting futures.
    // Key = correlationId, Value = future that awaits the response.
    private final Map<String, CompletableFuture<VoteResponse>> inFlightRequests;

    public ClientResponseHandler(Map<String, CompletableFuture<VoteResponse>> inFlightRequests) {
        this.inFlightRequests = inFlightRequests;
    }

    @Override
    protected void messageReceived(ChannelHandlerContext ctx, JsonNode jsonNode) throws Exception {
        // e.g. {"correlationId": "...", "type": "VoteResponse", "payload": {...}}
        if (!jsonNode.has("type")) {
            log.warn("No 'type' in JSON: {}", jsonNode);
            return;
        }
        if (!jsonNode.has("correlationId")) {
            log.warn("No 'correlationId' in JSON: {}", jsonNode);
            return;
        }

        String correlationId = jsonNode.get("correlationId").asText();  // must match

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

            // Possibly handle other message types (e.g. "AppendEntries", "HeartbeatResponse", etc.)
            default -> {
                log.warn("Unknown message type: {}", type);
            }
        }
    }


    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Error in client response handler", cause);
        ctx.close();
    }
}