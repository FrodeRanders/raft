package org.gautelis.raft.bootstrap;

import io.netty.channel.ChannelHandlerContext;
import org.gautelis.raft.MessageHandler;
import org.gautelis.raft.RaftNode;
import org.gautelis.raft.RaftServer;
import org.gautelis.raft.protocol.ClusterMessage;
import org.gautelis.raft.protocol.Peer;
import org.gautelis.raft.serialization.ProtoMapper;
import org.gautelis.raft.statemachine.KeyValueStateMachine;
import org.gautelis.raft.statemachine.SnapshotStateMachine;
import org.gautelis.raft.storage.FileLogStore;
import org.gautelis.raft.storage.FilePersistentStateStore;
import org.gautelis.raft.storage.InMemoryLogStore;
import org.gautelis.raft.storage.InMemoryPersistentStateStore;
import org.gautelis.raft.storage.LogStore;
import org.gautelis.raft.storage.PersistentStateStore;
import org.gautelis.raft.transport.netty.RaftClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
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
        // Adapter bootstraps a runnable node with either in-memory or file-backed durability.
        String dataDir = System.getProperty("raft.data.dir");
        LogStore logStore;
        PersistentStateStore persistentStateStore;
        if (dataDir == null || dataDir.isBlank()) {
            logStore = new InMemoryLogStore();
            persistentStateStore = new InMemoryPersistentStateStore();
        } else {
            Path root = Path.of(dataDir);
            logStore = new FileLogStore(root.resolve(me.getId() + ".log"));
            persistentStateStore = new FilePersistentStateStore(root.resolve(me.getId() + ".state"));
        }

        MessageHandler messageHandler = this::handleMessage;
        SnapshotStateMachine snapshotStateMachine = new KeyValueStateMachine();

        stateMachine = new RaftNode(
                me, peers, timeoutMillis, messageHandler, snapshotStateMachine, new RaftClient(me.getId(), messageHandler), logStore, persistentStateStore
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
        if ("ClusterMessage".equals(type)) {
            // Client commands are accepted only on leader; RaftNode handles replication/commit.
            var parsed = ProtoMapper.parseClusterMessage(payload);
            if (parsed.isEmpty()) {
                log.warn("Invalid ClusterMessage payload");
                return;
            }
            ClusterMessage clusterMessage = ProtoMapper.fromProto(parsed.get());
            String command = clusterMessage.getMessage();
            if (!isValidClusterCommand(command)) {
                log.warn("Rejected invalid cluster command: '{}'", command);
                return;
            }

            if (stateMachine == null) {
                log.warn("Ignoring command while state machine is unavailable");
                return;
            }
            if (!stateMachine.submitCommand(command)) {
                log.info("Rejected command at {}: node is not leader", me.getId());
                return;
            }
            log.info("Accepted cluster command: {}", command);
            return;
        }

        log.debug("Received '{}' message ({} bytes)", type, payload == null ? 0 : payload.length);
    }

    static boolean isValidClusterCommand(String command) {
        // Keep accepted command grammar intentionally small for predictable demos/tests.
        if (command == null) {
            return false;
        }
        String trimmed = command.trim();
        if (trimmed.isEmpty()) {
            return false;
        }
        String[] parts = trimmed.split("\\s+", 3);
        String op = parts[0].toLowerCase(java.util.Locale.ROOT);
        return switch (op) {
            case "set", "put" -> parts.length >= 3 && !parts[1].isBlank();
            case "del", "delete" -> parts.length >= 2 && !parts[1].isBlank();
            case "clear" -> parts.length == 1;
            default -> false;
        };
    }
}
