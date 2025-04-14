package org.gautelis.raft;

import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.Future;
import org.gautelis.raft.model.LogEntry;
import org.gautelis.raft.model.Peer;
import org.gautelis.raft.model.VoteRequest;
import org.gautelis.raft.model.VoteResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *                                     ┌───────────────────────────┐
 *                                     │ S T A T E   D I A G R A M │
 *                                     └───────────────────────────┘
 *
 *
 *                            times out, starts election          receives votes from majority of servers
 *                           ┌───────────────────────────┐       ┌────────────────────────┐
 *                           │                           ▼       │                        ▼
 *                ┌──────────────────────┐        ┌─────────────────────┐       ┌───────────────────┐
 *  ──  starts up │                      │        │                     │       │                   │
 * │  │──────────▶│       Follower       │        │      Candidate      │       │      Leader       │
 *  ──            │                      │        │                     │       │                   │
 *                └──────────────────────┘        └─────────────────────┘       └───────────────────┘
 *                       ▲        ▲                      │       │                    ▲       │
 *                       │        └──────────────────────┘       └────────────────────┘       │
 *                       │            discovers current                times out,             │
 *                       │            leader or new term               new election           │
 *                       │                                                                    │
 *                       └────────────────────────────────────────────────────────────────────┘
 *                                          discovers server with higher term
 *
 *
 */
public class RaftStateMachine {
    private static final Logger log = LoggerFactory.getLogger(RaftStateMachine.class);

    enum State { FOLLOWER, CANDIDATE, LEADER }
    private volatile State state = State.FOLLOWER;
    private long term = 0;
    private Peer votedFor = null;
    private long lastHeartbeat = 0;
    private long timeoutMillis;
    //private final List<Peer> peers;
    private final Map<String, Peer> peers = new HashMap<>();
    private final Peer me;

    // Netty-based approach might store channels for each peer or
    // a separate Netty "client" to connect out:
    private final NettyRaftClient nettyRaftClient; // or similar

    public RaftStateMachine(Peer me, List<Peer> peers, long timeoutMillis, NettyRaftClient nettyRaftClient) {
        for (Peer peer : peers) {
            this.peers.putIfAbsent(peer.getId(), peer);
        }
        this.me = me;
        this.timeoutMillis = timeoutMillis;
        this.nettyRaftClient = nettyRaftClient;
    }

    public synchronized VoteResponse handleVoteRequest(VoteRequest req) {
        log.trace("Received vote request for term {} from {}", req.getTerm(), req.getCandidateId());

        // If the candidate's term is behind ours, immediately reject.
        if (req.getTerm() < term) {
            log.trace("{} has term {} which is newer than requested term {} from {} => reject",
                    me.getId(), term, req.getTerm(), req.getCandidateId());
            return new VoteResponse(req, false);
        }

        // If the candidate's term is higher, step down, become follower.
        if (req.getTerm() > term) {
            log.trace("{} steps down from leadership to follower since requested term is {} (> {})",
                    me.getId(), req.getTerm(), term);
            term = req.getTerm();
            state = State.FOLLOWER;
            votedFor = null;
            refreshTimeout();
        }

        // If we haven't voted yet OR we already voted for this candidate, grant the vote.
        if (votedFor == null || votedFor.getId().equals(req.getCandidateId())) {
            Peer peer = peers.get(req.getCandidateId());
            if (peer != null) {
                state = State.FOLLOWER;
                votedFor = peer;
                log.trace("{} grants vote to {} for term {} => accepted",
                        me.getId(), req.getCandidateId(), req.getTerm());
                refreshTimeout();
                return new VoteResponse(req, true);
            }
            else {
                log.warn("Unknown candidate {} => reject", req.getCandidateId());
                return new VoteResponse(req, false);
            }
        }
        else {
            // We already voted for someone else
            log.trace("{} will *not* grant vote to {} for term {} (already voted for {})",
                    me.getId(), req.getCandidateId(), req.getTerm(), votedFor.getId());
            return new VoteResponse(req, false);
        }
    }

    public synchronized void handleLogEntry(LogEntry entry) {
        long peerTerm = entry.getTerm();
        log.trace("Receive heartbeat for term {} from {}: {}", peerTerm, entry.getPeerId(), entry.getType());

        switch (entry.getType()) {
            case HEARTBEAT -> {
                // If the peer's term is higher, step down, become follower.
                if (peerTerm > term) {
                    log.trace("{} steps down from leadership to follower since requested term is {} (> {})",
                            me.getId(), peerTerm, term);
                    term = peerTerm;
                    state = State.FOLLOWER;
                    votedFor = null;
                }

                refreshTimeout();
            }

            default -> {

            }
        }
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
        log.trace("{} initiating new election...", me.getId());

        synchronized (this) {
            state = State.CANDIDATE;
            term++;
            votedFor = me;
            refreshTimeout();
        }

        VoteRequest voteReq = new VoteRequest(term, me.getId());
        log.trace("{} voting for myself for term {}", me.getId(), voteReq.getTerm());
        nettyRaftClient.requestVoteFromAll(peers.values(), voteReq)
                .addListener((Future<List<VoteResponse>> f) -> {
                    if (f.isSuccess()) {
                        log.trace("{} received responses for term {}", me.getId(), voteReq.getTerm());
                        List<VoteResponse> responses = f.getNow();
                        handleVoteResponses(responses, voteReq.getTerm());
                    }
                    else {
                        log.info("No vote response", f.cause());
                    }
        });
    }

    private synchronized void handleVoteResponses(List<VoteResponse> responses, long electionTerm) {
        log.trace("Handling vote responses from {} peers...", responses.size());

        if (log.isTraceEnabled()) {
            StringBuilder sb = new StringBuilder("Got vote responses for term ").append(term).append(":\n");
            for (VoteResponse response : responses) {
                sb.append("  ").append(response.isVoteGranted()).append("\n");
            }
            log.trace(sb.toString());
        }

        // if we’re still in the same term and still candidate, see if we got a majority
        if (this.term == electionTerm && state == State.CANDIDATE) {
            long votesGranted = responses.stream().filter(VoteResponse::isVoteGranted).count() + 1; // plus my own vote
            int majority = (peers.size() + 1) / 2 + 1;
            if (votesGranted >= majority) {
                log.trace("Elected LEADER ({} votes >= majority {}): {}", votesGranted, majority, me);
                state = State.LEADER;
            }
        }
    }

    private synchronized void broadcastHeartbeat() {
        if (state == State.LEADER) {
            log.trace("LEADER {} broadcasting heartbeat for term {}", me.getId(), term);

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