package org.gautelis.raft;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.gautelis.raft.model.VoteResponse;
import org.gautelis.raft.proto.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class ClientResponseHandler extends SimpleChannelInboundHandler<Envelope> {
    private static final Logger log = LoggerFactory.getLogger(ClientResponseHandler.class);

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
    protected void channelRead0(ChannelHandlerContext ctx, Envelope envelope) throws Exception {
        String type = envelope.getType();
        if (type == null || type.isEmpty()) {
            log.warn("No 'type' in envelope");
            return;
        }

        String correlationId = envelope.getCorrelationId();
        if (correlationId == null || correlationId.isEmpty()) {
            log.warn("Incorrect message: No correlationId");
            return;
        }

        byte[] payload = envelope.getPayload().toByteArray();

        switch (type) {
            case "VoteResponse" -> {
                var response = ProtoMapper.parseVoteResponse(payload);
                if (response.isEmpty()) {
                    log.warn("Failed to parse VoteResponse payload");
                    return;
                }
                VoteResponse voteResponse = ProtoMapper.fromProto(response.get());

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
                    messageHandler.handle(correlationId, type, payload, ctx);
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
