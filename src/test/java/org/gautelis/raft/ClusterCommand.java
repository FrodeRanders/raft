package org.gautelis.raft;

import org.gautelis.raft.model.LogEntry;

public class ClusterCommand extends LogEntry {
    private final String command;

    public ClusterCommand(Type type, long term, String peerId, String command) {
        super(type, term, peerId);
        this.command = command;
    }

    public String getCommand() {
        return command;
    }
}
