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
    // Key = requestId, Value = future that awaits the response.
    private final Map<String, CompletableFuture<VoteResponse>> inFlightRequests;

    public ClientResponseHandler(Map<String, CompletableFuture<VoteResponse>> inFlightRequests) {
        this.inFlightRequests = inFlightRequests;
    }

    @Override
    protected void messageReceived(ChannelHandlerContext ctx, JsonNode jsonNode) throws Exception {
        // e.g. {"requestId": "...", "type": "VoteResponse", "payload": {...}}
        if (!jsonNode.has("type")) {
            log.warn("No 'type' in JSON: {}", jsonNode);
            return;
        }

        String type = jsonNode.get("type").asText();
        switch (type) {
            case "VoteResponse" -> {
                if (!jsonNode.has("requestId")) {
                    log.warn("No 'requestId' in VoteResponse JSON: {}", jsonNode);
                    return;
                }

                String requestId = jsonNode.get("requestId").asText();  // must match
                VoteResponse voteResponse = mapper.treeToValue(jsonNode.get("payload"), VoteResponse.class);

                // Lookup the future for this requestId
                CompletableFuture<VoteResponse> fut = inFlightRequests.remove(requestId);
                if (fut != null) {
                    fut.complete(voteResponse);
                } else {
                    log.warn("No future found for VoteResponse requestId={}", requestId);
                }

                log.trace("Received VoteResponse: {} for requestId={}", voteResponse, requestId);
            }

            // Possibly handle other message types (e.g. "AppendEntries", "HeartbeatResponse", etc.)
            default -> {
                log.warn("Unhandled message type: {}", type);
            }
        }
    }


    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Error in client response handler", cause);
        ctx.close();
    }
}