package org.gautelis.raft.model;

public class Heartbeat {
    private long term;
    private String peerId;

    // Default constructor needed for Jackson
    protected Heartbeat() {}

    public Heartbeat(long term, String peerId) {
        this.term = term;
        this.peerId = peerId;
    }

    public long getTerm() { return term; }
    public String getPeerId() { return peerId; }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Heartbeat{");
        sb.append("term=").append(term);
        sb.append(", peerId=").append(peerId);
        sb.append('}');
        return sb.toString();
    }
}
