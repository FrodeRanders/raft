package org.gautelis.raft.protocol;

public class AppendEntriesResponse {
    private long term;
    private String peerId;
    private boolean success;
    private long matchIndex;
    public AppendEntriesResponse(long term, String peerId, boolean success, long matchIndex) {
        this.term = term;
        this.peerId = peerId;
        this.success = success;
        this.matchIndex = matchIndex;
    }

    public long getTerm() { return term; }
    public String getPeerId() { return peerId; }
    public boolean isSuccess() { return success; }
    public long getMatchIndex() { return matchIndex; }
}
