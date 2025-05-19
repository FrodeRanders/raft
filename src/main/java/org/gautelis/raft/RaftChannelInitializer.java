package org.gautelis.raft;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Server channel initializer
 */
public class RaftChannelInitializer extends ChannelInitializer<SocketChannel> {
    private static final Logger log = LoggerFactory.getLogger(RaftChannelInitializer.class);

    private final RaftStateMachine stateMachine;

    public RaftChannelInitializer(RaftStateMachine raftServer) {
        this.stateMachine = raftServer;
    }

    @Override
    protected void initChannel(SocketChannel ch) {
        log.trace("Setting up server channel to {}", ch.remoteAddress());

        ChannelPipeline p = ch.pipeline();

        // Optional: add a frame decoder if messages are delimited or uses length-based frames.
        // p.addLast(new LengthFieldBasedFrameDecoder(65536, 0, 4, 0, 4));

        // Consider decoding ByteBuf -> String, or directly to JsonNode (Jackson).
        p.addLast(new ByteBufToJsonDecoder());

        // The RaftHandler deals with the JSON messages and calls raftServer
        p.addLast(new RaftMessageHandler(stateMachine));
    }
}