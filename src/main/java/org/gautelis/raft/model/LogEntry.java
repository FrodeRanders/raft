package org.gautelis.raft.model;

public class LogEntry {
    public enum Type { HEARTBEAT, COMMAND }

    private Type type;
    private long term;
    private String peerId;  // for heartbeat messages

    // Default constructor needed for Jackson
    public LogEntry() {}

    public LogEntry(Type type, long term, String peerId) {
        this.type = type;
        this.term = term;
        this.peerId = peerId;
    }

    public Type getType() { return type; }
    public long getTerm() { return term; }
    public String getPeerId() { return peerId; }
}
