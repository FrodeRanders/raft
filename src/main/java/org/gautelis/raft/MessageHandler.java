package org.gautelis.raft;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import io.netty.channel.ChannelHandlerContext;

public interface MessageHandler {
    /**
     * Handles reception of requests/messages for both server and
     * client modes:
     *  - In server mode, a request initiated by a peer would normally
     *    result in the server issuing a response.
     *  - In client mode, a message (possibly a request) initiated
     *    by self could result in a response from a peer.
     * The 'handle' method is called upon receiving data, either by
     * 'RaftMessageHandler' (in server mode) or 'ClientResponseHandler'
     * (in client mode).
     * <p/>
     * @param correlationId
     * @param type
     * @param node
     * @param ctx
     * @throws JsonProcessingException
     */
    void handle(String correlationId, String type, JsonNode node, ChannelHandlerContext ctx) throws JsonProcessingException;
}
