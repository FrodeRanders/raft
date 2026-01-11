package org.gautelis.raft;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.gautelis.raft.model.*;
import org.gautelis.raft.proto.Envelope;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

@ChannelHandler.Sharable
public class RaftMessageHandler extends SimpleChannelInboundHandler<Envelope> {
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
            case "Heartbeat" -> {
                var parsed = ProtoMapper.parseHeartbeat(payload);
                if (parsed.isEmpty()) {
                    log.warn("Failed to parse Heartbeat payload");
                    return;
                }
                Heartbeat heartbeat = ProtoMapper.fromProto(parsed.get());
                stateMachine.handleHeartbeat(heartbeat);
                // no expected response
            }
            case "LogEntry" -> {
                var parsed = ProtoMapper.parseLogEntry(payload);
                if (parsed.isEmpty()) {
                    log.warn("Failed to parse LogEntry payload");
                    return;
                }
                LogEntry entry = ProtoMapper.fromProto(parsed.get());
                stateMachine.handleLogEntry(entry);
                // no expected response
            }
            case "VoteRequest" -> {
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
            default -> {
                MessageHandler messageHandler = stateMachine.getMessageHandler();
                if (null != messageHandler) {
                    messageHandler.handle(correlationId, type, payload, ctx);
                }
            }
        }
    }
}
