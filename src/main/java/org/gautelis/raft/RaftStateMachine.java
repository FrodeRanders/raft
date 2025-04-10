package org.gautelis.raft;

import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.Future;
import org.gautelis.raft.model.LogEntry;
import org.gautelis.raft.model.Peer;
import org.gautelis.raft.model.VoteRequest;
import org.gautelis.raft.model.VoteResponse;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class RaftStateMachine {
    enum State { FOLLOWER, CANDIDATE, LEADER }
    private volatile State state = State.FOLLOWER;
    private long term = 0;
    private Peer votedFor = null;
    private long lastHeartbeat = 0;
    private long timeoutMillis;
    private final List<Peer> peers;
    private final Peer me;

    // Netty-based approach might store channels for each peer or
    // a separate Netty "client" to connect out:
    private final NettyRaftClient nettyRaftClient; // or similar

    public RaftStateMachine(List<Peer> peers, Peer me, long timeoutMillis, NettyRaftClient nettyRaftClient) {
        this.peers = peers;
        this.me = me;
        this.timeoutMillis = timeoutMillis;
        this.nettyRaftClient = nettyRaftClient;
    }

    public synchronized VoteResponse handleVoteRequest(VoteRequest req) {
        // Same logic as before
        if (votedFor != null && !votedFor.equals(req.getCandidateId())) {
            return new VoteResponse(term, false);
        }

        if (req.getTerm() > term) {
            // step down if needed
            term = req.getTerm();
            state = State.FOLLOWER;
            votedFor = null;
        }

        // etc.

        votedFor = null; // TODO new Peer(req.getCandidateId());
        return new VoteResponse(term, true);
    }

    public synchronized void handleLogEntry(LogEntry entry) {
        // If it's a heartbeat and term is higher, step down, etc.
        refreshTimeout();
    }

    public void startTimers(EventLoopGroup eventLoopGroup) {
        // Periodically check election timeout:
        eventLoopGroup.next().scheduleAtFixedRate(this::checkTimeout, timeoutMillis, timeoutMillis, TimeUnit.MILLISECONDS);

        // Periodically send heartbeat if leader:
        eventLoopGroup.next().scheduleAtFixedRate(this::broadcastHeartbeat, 1, 1, TimeUnit.SECONDS);
    }

    private synchronized void checkTimeout() {
        if (state != State.LEADER && hasTimedOut()) {
            newElection();
        }
    }

    private void newElection() {
        synchronized (this) {
            state = State.CANDIDATE;
            term++;
            votedFor = me;
            refreshTimeout();
        }

        VoteRequest voteReq = new VoteRequest(term, me.getId());
        nettyRaftClient.requestVoteFromAll(voteReq).addListener((Future<List<VoteResponse>> f) -> {
            if (f.isSuccess()) {
                List<VoteResponse> responses = f.getNow();
                handleVoteResponses(responses, voteReq.getTerm());
            }
        });
    }

    private synchronized void handleVoteResponses(List<VoteResponse> responses, long electionTerm) {
        // if we’re still in the same term and still candidate, see if we got a majority
        if (this.term == electionTerm && state == State.CANDIDATE) {
            long votesGranted = responses.stream().filter(VoteResponse::isVoteGranted).count() + 1; // plus my own vote
            int majority = (peers.size() + 1) / 2 + 1;
            if (votesGranted >= majority) {
                state = State.LEADER;
            }
        }
    }

    private synchronized void broadcastHeartbeat() {
        if (state == State.LEADER) {
            LogEntry entry = new LogEntry(LogEntry.Type.HEARTBEAT, term, me.getId());
            nettyRaftClient.broadcastLogEntry(entry);
        }
    }

    private synchronized boolean hasTimedOut() {
        return System.currentTimeMillis() - lastHeartbeat > timeoutMillis;
    }

    private synchronized void refreshTimeout() {
        lastHeartbeat = System.currentTimeMillis();
    }
}