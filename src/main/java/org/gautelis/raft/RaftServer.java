package org.gautelis.raft;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RaftServer {
    private static final Logger log = LoggerFactory.getLogger(RaftServer.class);

    private final RaftNode stateMachine;  // your state machine
    private final int port;

    public RaftServer(RaftNode stateMachine, int port) {
        this.stateMachine = stateMachine;
        this.port = port;
    }

    private ChannelInitializer<SocketChannel> getChannelInitializer() {
        return new ChannelInitializer<>() {
            protected void initChannel(SocketChannel ch) {
                log.trace("Initializing server channel: {}", ch);

                ChannelPipeline p = ch.pipeline();
                p.addLast(new ByteBufToJsonDecoder());
                p.addLast(new RaftMessageHandler(stateMachine));
                p.addLast(new ChannelInboundHandlerAdapter() {
                    @Override
                    public void channelActive(ChannelHandlerContext ctx) throws Exception {
                        log.trace("Connection established from {}", ctx.channel().remoteAddress());
                        super.channelActive(ctx);
                    }
                });
            }
        };
    }

    public void start() throws InterruptedException {
        // Boss group: accepts connections
        EventLoopGroup bossGroup = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());

        // Worker group: handles I/O for established connections
        EventLoopGroup workerGroup = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());

        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(getChannelInitializer())
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childOption(ChannelOption.TCP_NODELAY, true);

            // Initialize RAFT timers (e.g., election + heartbeat scheduler)
            stateMachine.startTimers(workerGroup);

            // Bind and sync until ready
            ChannelFuture bindFuture = bootstrap.bind(port).sync();

            if (bindFuture.isSuccess()) {
                log.info("Raft server started on port {}", port);
            } else {
                log.error("Failed to bind to port {}: {}", port, bindFuture.cause().getMessage(), bindFuture.cause());
                return;
            }

            // Wait until the server socket is closed.
            bindFuture.channel().closeFuture().sync();
        }
        finally {
            log.info("Shutting down server on port {}", port);
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }
}
