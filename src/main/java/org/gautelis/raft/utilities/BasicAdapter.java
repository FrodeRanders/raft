package org.gautelis.raft.utilities;

import io.netty.channel.ChannelHandlerContext;
import org.gautelis.raft.*;
import org.gautelis.raft.model.Peer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class BasicAdapter {
    protected static final Logger log = LoggerFactory.getLogger(BasicAdapter.class);

    protected final long timeoutMillis;
    protected final Peer me;
    protected final List<Peer> peers;

    protected RaftNode stateMachine;

    public BasicAdapter(long timeoutMillis, Peer me, List<Peer> peers) {
        this.timeoutMillis = timeoutMillis;
        this.me = me;
        this.peers = peers;
    }

    public void start() {
        LogStore logStore = new InMemoryLogStore();

        MessageHandler messageHandler = this::handleMessage;

        stateMachine = new RaftNode(
                me, peers, timeoutMillis, messageHandler, new RaftClient(messageHandler), logStore
        );

        try {
            RaftServer server = new RaftServer(stateMachine, me.getAddress().getPort());
            server.start();
        }
        catch (InterruptedException ie) {
            log.info("Interrupted!", ie);
        }
        finally {
            stateMachine.shutdown();
        }
    }

    public void handleMessage(String correlationId, String type, byte[] payload, ChannelHandlerContext ctx) {
        log.debug(
                "Received '{}' message ({} bytes)",
                type, payload == null ? 0 : payload.length
        );
    }
}
