package org.gautelis.raft.model;

public class VoteResponse {
    private String peerId;
    private long term;
    private boolean voteGranted;
    private long currentTerm;

    // Default constructor needed for Jackson
    protected VoteResponse() {}

    public VoteResponse(VoteRequest request, String peerId, boolean voteGranted, long currentTerm) {
        this.term = request.getTerm();
        this.peerId = peerId;
        this.voteGranted = voteGranted;
        this.currentTerm = currentTerm;
    }

    public String getPeerId() { return peerId; }
    public long getTerm() { return term; }
    public boolean isVoteGranted() { return voteGranted; }
    public long getCurrentTerm() { return currentTerm; }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("VoteResponse{");
        sb.append("peer-id=").append(peerId);
        sb.append(", term=").append(term);
        sb.append(", voteGranted=").append(voteGranted);
        sb.append(", currentTerm=").append(currentTerm);
        sb.append('}');
        return sb.toString();
    }
}
