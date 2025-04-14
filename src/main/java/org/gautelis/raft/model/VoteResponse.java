package org.gautelis.raft.model;

public class VoteResponse {
    private long term;
    private boolean voteGranted;

    // Default constructor needed for Jackson
    public VoteResponse() {}

    public VoteResponse(VoteRequest request, boolean voteGranted) {
        this.term = request.getTerm();
        this.voteGranted = voteGranted;
    }

    public long getTerm() { return term; }
    public boolean isVoteGranted() { return voteGranted; }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("VoteResponse{");
        builder.append("term=").append(term);
        builder.append(", voteGranted=").append(voteGranted);
        builder.append('}');
        return builder.toString();
    }
}
