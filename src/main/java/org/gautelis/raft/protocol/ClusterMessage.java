package org.gautelis.raft.protocol;

public class ClusterMessage {
    private long term;
    private String peerId;  // for heartbeat messages

    private String message;
    public ClusterMessage(long term, String peerId, String message) {
        this.term = term;
        this.peerId = peerId;
        this.message = message;
    }

    public long getTerm() {
        return term;
    }

    public String getPeerId() {
        return peerId;
    }

    public String getMessage() {
        return message;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("ClusterMessage{");
        sb.append("term=").append(term);
        sb.append(", peerId=").append(peerId);
        sb.append(", message='").append(message).append('\'');
        sb.append('}');
        return sb.toString();
    }
}