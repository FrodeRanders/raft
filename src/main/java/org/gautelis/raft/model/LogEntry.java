package org.gautelis.raft.model;

import java.util.Arrays;

public class LogEntry {
    private long term;
    private String peerId;
    private byte[] data;

    // Default constructor needed for Jackson
    protected LogEntry() {}

    public LogEntry(long term, String peerId) {
        this(term, peerId, new byte[0]);
    }

    public LogEntry(long term, String peerId, byte[] data) {
        this.term = term;
        this.peerId = peerId;
        this.data = data == null ? new byte[0] : Arrays.copyOf(data, data.length);
    }

    public long getTerm() { return term; }
    public String getPeerId() { return peerId; }
    public byte[] getData() { return Arrays.copyOf(data, data.length); }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("LogEntry{");
        sb.append("term=").append(term);
        sb.append(", peerId=").append(peerId);
        sb.append(", dataBytes=").append(data == null ? 0 : data.length);
        sb.append('}');
        return sb.toString();
    }
}
