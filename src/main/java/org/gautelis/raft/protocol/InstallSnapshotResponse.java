package org.gautelis.raft.protocol;

public class InstallSnapshotResponse {
    private long term;
    private String peerId;
    private boolean success;
    private long lastIncludedIndex;
    public InstallSnapshotResponse(long term, String peerId, boolean success, long lastIncludedIndex) {
        this.term = term;
        this.peerId = peerId;
        this.success = success;
        this.lastIncludedIndex = lastIncludedIndex;
    }

    public long getTerm() { return term; }
    public String getPeerId() { return peerId; }
    public boolean isSuccess() { return success; }
    public long getLastIncludedIndex() { return lastIncludedIndex; }
}
