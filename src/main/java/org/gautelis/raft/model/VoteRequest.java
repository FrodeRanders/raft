package org.gautelis.raft.model;

public class VoteRequest {
    private long term;
    private String candidateId;

    private long lastLogIndex;
    private long lastLogTerm;

    public VoteRequest(long term, String candidateId) {
        this(term, candidateId, 0L, 0L);
    }

    public VoteRequest(long term, String candidateId, long lastLogIndex, long lastLogTerm) {
        this.term = term;
        this.candidateId = candidateId;
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm = lastLogTerm;
    }

    public long getTerm() { return term; }
    public String getCandidateId() { return candidateId; }

    public long getLastLogIndex() { return lastLogIndex; }
    public long getLastLogTerm() { return lastLogTerm; }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("VoteRequest{");
        sb.append("term=").append(term);
        sb.append(", candidate-id='").append(candidateId).append('\'');
        sb.append(", last-log-index=").append(lastLogIndex);
        sb.append(", last-log-term=").append(lastLogTerm);
        sb.append('}');
        return sb.toString();
    }
}
