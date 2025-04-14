package org.gautelis.raft;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NettyRaftServer {
    private static final Logger log = LoggerFactory.getLogger(NettyRaftServer.class);

    private final RaftStateMachine stateMachine;  // your state machine
    private final int port;

    public NettyRaftServer(RaftStateMachine stateMachine, int port) {
        this.stateMachine = stateMachine;
        this.port = port;
    }

    public void start() throws InterruptedException {
        EventLoopGroup bossGroup = new NioEventLoopGroup(1);   // accept connections
        EventLoopGroup workerGroup = new NioEventLoopGroup();  // handle traffic

        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new RaftChannelInitializer(stateMachine))
                    .childOption(ChannelOption.SO_KEEPALIVE, true);


            stateMachine.startTimers(workerGroup);

            ChannelFuture f = b.bind(port).sync();
            String info = String.format("Raft server started on port %d", port);
            System.out.println(info);
            log.info(info);

            // Block until the server socket is closed.
            f.channel().closeFuture().sync();

        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }
}
