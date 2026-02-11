package org.gautelis.raft.protocol;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class AppendEntriesRequest {
    private long term;
    private String leaderId;
    private long prevLogIndex;
    private long prevLogTerm;
    private long leaderCommit;
    private List<LogEntry> entries;
    public AppendEntriesRequest(
            long term,
            String leaderId,
            long prevLogIndex,
            long prevLogTerm,
            long leaderCommit,
            List<LogEntry> entries
    ) {
        // Raft AppendEntries semantics:
        // - empty entries == heartbeat
        // - prevLogIndex/prevLogTerm anchor consistency check on follower
        // - leaderCommit advertises leader's committed index for follower apply progress
        this.term = term;
        this.leaderId = leaderId;
        this.prevLogIndex = prevLogIndex;
        this.prevLogTerm = prevLogTerm;
        this.leaderCommit = leaderCommit;
        this.entries = entries == null ? List.of() : new ArrayList<>(entries);
    }

    public long getTerm() { return term; }
    public String getLeaderId() { return leaderId; }
    public long getPrevLogIndex() { return prevLogIndex; }
    public long getPrevLogTerm() { return prevLogTerm; }
    public long getLeaderCommit() { return leaderCommit; }
    public List<LogEntry> getEntries() { return Collections.unmodifiableList(entries == null ? List.of() : entries); }
}
