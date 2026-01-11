package org.gautelis.raft;

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
     * @param payload raw payload bytes (e.g., protobuf-encoded)
     * @param ctx
     */
    void handle(String correlationId, String type, byte[] payload, ChannelHandlerContext ctx) throws Exception;
}
