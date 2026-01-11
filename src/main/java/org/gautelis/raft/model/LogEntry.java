package org.gautelis.raft.model;

public class LogEntry {
    private long term;
    private String peerId;  // for heartbeat messages

    // Default constructor needed for Jackson
    protected LogEntry() {}

    public LogEntry(long term, String peerId) {
        this.term = term;
        this.peerId = peerId;
    }

    public long getTerm() { return term; }
    public String getPeerId() { return peerId; }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("LogEntry{");
        sb.append("term=").append(term);
        sb.append(", peerId=").append(peerId);
        sb.append('}');
        return sb.toString();
    }
}
