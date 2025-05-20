package org.gautelis.raft.model;

public class VoteRequest {
    private long term;
    private String candidateId;

    // Default constructor needed for Jackson
    protected VoteRequest() {}

    public VoteRequest(long term, String candidateId) {
        this.term = term;
        this.candidateId = candidateId;
    }

    public long getTerm() { return term; }
    public String getCandidateId() { return candidateId; }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("VoteRequest{");
        sb.append("term=").append(term);
        sb.append(", candidateId='").append(candidateId).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
