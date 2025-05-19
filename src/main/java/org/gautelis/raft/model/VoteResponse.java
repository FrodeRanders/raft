package org.gautelis.raft.model;

public class VoteResponse {
    private long term;
    private boolean voteGranted;
    private long currentTerm;

    // Default constructor needed for Jackson
    protected VoteResponse() {}

    public VoteResponse(VoteRequest request, boolean voteGranted, long currentTerm) {
        this.term = request.getTerm();
        this.voteGranted = voteGranted;
        this.currentTerm = currentTerm;
    }

    public long getTerm() { return term; }
    public boolean isVoteGranted() { return voteGranted; }
    public long getCurrentTerm() { return currentTerm; }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("VoteResponse{");
        builder.append("term=").append(term);
        builder.append(", voteGranted=").append(voteGranted);
        builder.append(", currentTerm=").append(currentTerm);
        builder.append('}');
        return builder.toString();
    }
}
