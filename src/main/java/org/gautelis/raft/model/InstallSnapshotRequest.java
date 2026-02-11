package org.gautelis.raft.model;

import java.util.Arrays;

public class InstallSnapshotRequest {
    private long term;
    private String leaderId;
    private long lastIncludedIndex;
    private long lastIncludedTerm;
    private byte[] snapshotData;
    public InstallSnapshotRequest(long term, String leaderId, long lastIncludedIndex, long lastIncludedTerm, byte[] snapshotData) {
        this.term = term;
        this.leaderId = leaderId;
        this.lastIncludedIndex = lastIncludedIndex;
        this.lastIncludedTerm = lastIncludedTerm;
        this.snapshotData = snapshotData == null ? new byte[0] : Arrays.copyOf(snapshotData, snapshotData.length);
    }

    public long getTerm() { return term; }
    public String getLeaderId() { return leaderId; }
    public long getLastIncludedIndex() { return lastIncludedIndex; }
    public long getLastIncludedTerm() { return lastIncludedTerm; }
    public byte[] getSnapshotData() { return Arrays.copyOf(snapshotData, snapshotData.length); }
}
