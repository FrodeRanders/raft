package org.gautelis.raft;

import org.gautelis.raft.model.LogEntry;

public interface LogHandler {
    void handle(long myTerm, LogEntry entry);
}
