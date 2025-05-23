package org.gautelis.raft.model;

public class LogEntry {
    public enum Type { HEARTBEAT, COMMAND }

    private Type type;
    private long term;
    private String peerId;  // for heartbeat messages

    // Default constructor needed for Jackson
    protected LogEntry() {}

    public LogEntry(Type type, long term, String peerId) {
        this.type = type;
        this.term = term;
        this.peerId = peerId;
    }

    public Type getType() { return type; }
    public long getTerm() { return term; }
    public String getPeerId() { return peerId; }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("LogEntry{");
        sb.append("type=").append(type);
        sb.append(", term=").append(term);
        sb.append(", peerId=").append(peerId);
        sb.append('}');
        return sb.toString();
    }
}
