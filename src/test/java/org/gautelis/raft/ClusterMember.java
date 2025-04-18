package org.gautelis.raft;

import org.gautelis.raft.model.LogEntry;
import org.gautelis.raft.model.Peer;
import org.gautelis.raft.utilities.BasicAdapter;

import java.util.List;

public class ClusterMember extends BasicAdapter {
    public ClusterMember(long timeoutMillis, Peer me, List<Peer> peers) {
        super(timeoutMillis, me, peers);
    }

    public void handleLogEntry(long myTerm, LogEntry logEntry) {
        // This should be a LogEntry.Type.COMMAND
        log.debug(
                "Received {} log entry (their term {} {} my term)",
                logEntry.getType(), logEntry.getTerm(), myTerm == logEntry.getTerm() ? "==" : "!=", myTerm
        );
    }

}
