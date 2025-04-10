package org.gautelis.raft.model;

public class VoteRequest {
    private long term;
    private String candidateId;

    // Default constructor needed for Jackson
    public VoteRequest() {}

    public VoteRequest(long term, String candidateId) {
        this.term = term;
        this.candidateId = candidateId;
    }

    public long getTerm() { return term; }
    public String getCandidateId() { return candidateId; }
}
