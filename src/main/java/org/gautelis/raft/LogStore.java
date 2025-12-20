package org.gautelis.raft;

import org.gautelis.raft.model.LogEntry;

import java.util.List;

public interface LogStore {

    /**
     * Highest Raft log index present locally (1-based). 0 if empty.
     */
    long lastIndex();

    /**
     * Term of entry at lastIndex(); 0 if empty.
     */
    long lastTerm();

    /**
     * Term at a specific index; index==0 must return 0.
     */
    long termAt(long index);

    /**
     * Append entries to the log (entries must be COMMAND type).
     */
    void append(List<LogEntry> entries);
}
