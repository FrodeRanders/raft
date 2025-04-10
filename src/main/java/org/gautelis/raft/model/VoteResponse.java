package org.gautelis.raft.model;

public class VoteResponse {
    private long term;
    private boolean voteGranted;

    // Default constructor needed for Jackson
    public VoteResponse() {}

    public VoteResponse(long term, boolean voteGranted) {
        this.term = term;
        this.voteGranted = voteGranted;
    }

    public long getTerm() { return term; }
    public boolean isVoteGranted() { return voteGranted; }
}
