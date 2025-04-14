package org.gautelis.raft;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;

public class RaftChannelInitializer extends ChannelInitializer<SocketChannel> {
    private final RaftStateMachine stateMachine;

    public RaftChannelInitializer(RaftStateMachine raftServer) {
        this.stateMachine = raftServer;
    }

    @Override
    protected void initChannel(SocketChannel ch) {
        ChannelPipeline p = ch.pipeline();

        // Optional: add a frame decoder if messages are delimited or uses length-based frames.
        // p.addLast(new LengthFieldBasedFrameDecoder(65536, 0, 4, 0, 4));

        // Consider decodeing ByteBuf -> String, or directly to JsonNode (Jackson).
        p.addLast(new ByteBufToJsonDecoder()); // see notes below for a custom decoder example

        // The RaftHandler  deals with the JSON messages and calls raftServer
        p.addLast(new RaftHandler(stateMachine));
    }
}