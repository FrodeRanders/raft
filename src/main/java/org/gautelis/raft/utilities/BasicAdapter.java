package org.gautelis.raft.utilities;

import org.gautelis.raft.RaftClient;
import org.gautelis.raft.RaftServer;
import org.gautelis.raft.RaftStateMachine;
import org.gautelis.raft.model.LogEntry;
import org.gautelis.raft.model.Peer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class BasicAdapter {
    protected static final Logger log = LoggerFactory.getLogger(BasicAdapter.class);

    protected final long timeoutMillis;
    protected final Peer me;
    protected final List<Peer> peers;

    protected RaftClient raftClient;

    public BasicAdapter(long timeoutMillis, Peer me, List<Peer> peers) {
        this.timeoutMillis = timeoutMillis;
        this.me = me;
        this.peers = peers;
    }

    public void start() {
        RaftStateMachine stateMachine = new RaftStateMachine(
                me, peers, timeoutMillis, this::handleLogEntry
        );
        raftClient = stateMachine.getRaftClient();
        RaftServer server = new RaftServer(stateMachine, me.getAddress().getPort());

        try {
            server.start();
        }
        catch (InterruptedException ie) {
            log.info("Interrupted!", ie);
        }
    }

    public void handleLogEntry(long myTerm, LogEntry logEntry) {
        // This should be a LogEntry.Type.COMMAND
        log.debug(
                "Received {} log entry (their term {} {} my term)",
                logEntry.getType(), logEntry.getTerm(), myTerm == logEntry.getTerm() ? "==" : "!=", myTerm
        );
    }
}
