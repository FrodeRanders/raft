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
    private final Map<String, Peer> peers = new HashMap<>();
    private final Peer me;

    // Netty-based approach might store channels for each peer or
    // a separate Netty "client" to connect out:
    private final RaftClient nettyRaftClient; // or similar

    public RaftStateMachine(Peer me, List<Peer> peers, long timeoutMillis, RaftClient nettyRaftClient) {
        for (Peer peer : peers) {
            this.peers.putIfAbsent(peer.getId(), peer);
        }
        this.me = me;
        this.timeoutMillis = timeoutMillis;
        this.nettyRaftClient = nettyRaftClient;
    }

    public synchronized VoteResponse handleVoteRequest(VoteRequest req) {
        log.debug("{} received vote request for term {} from {}", me.getId(), req.getTerm(), req.getCandidateId());

        // If the candidate's term is behind ours, immediately reject.
        if (req.getTerm() < term) {
            if (log.isDebugEnabled()) {
                log.debug("{} ({}) has term {} which is newer than requested term {} from {} => reject",
                        me.getId(), state.name(), term, req.getTerm(), req.getCandidateId());
            }
            return new VoteResponse(req, false, term);
        }

        // If the candidate's term is higher, step down, become follower.
        if (req.getTerm() > term) {
            if (log.isDebugEnabled()) {
                String info = "remains FOLLOWER";
                if (state != State.FOLLOWER) {
                    info = "steps from " + state.name() + " to FOLLOWER";
                }
                log.debug(
                        "{} {} since requested term is {} (> {})",
                        me.getId(), info, req.getTerm(), term
                );
            }

            term = req.getTerm();
            state = State.FOLLOWER;
            votedFor = null;
            refreshTimeout();
        }

        // If we haven't voted yet OR we already voted for this candidate, grant the vote.
        if (votedFor == null || votedFor.getId().equals(req.getCandidateId())) {
            Peer peer = peers.get(req.getCandidateId());
            if (peer != null) {
                if (log.isDebugEnabled()) {
                    String info = "remains FOLLOWER";
                    if (state != State.FOLLOWER) {
                        info = "steps from " + state.name() + " to FOLLOWER";
                    }
                    log.debug("{} grants vote to {} for term {} and {} => accepted",
                            me.getId(), req.getCandidateId(), req.getTerm(), info);
                }

                state = State.FOLLOWER;
                votedFor = peer;
                refreshTimeout();
                return new VoteResponse(req, true, term);
            }
            else {
                log.warn("Unknown candidate {} => reject", req.getCandidateId());
                return new VoteResponse(req, false, term);
            }
        }
        else {
            // We already voted for someone else
            if (log.isDebugEnabled()) {
                log.debug("{} ({}) will *not* grant vote to {} for term {} (already voted for {})",
                        me.getId(), state.name(), req.getCandidateId(), req.getTerm(), votedFor.getId());
            }
            return new VoteResponse(req, false, term);
        }
    }

    public synchronized void handleLogEntry(LogEntry entry) {
        long peerTerm = entry.getTerm();
        log.trace("Receive heartbeat for term {} from {}: {}", peerTerm, entry.getPeerId(), entry.getType());

        switch (entry.getType()) {
            case HEARTBEAT -> {
                // If the peer's term is higher, step down, become follower.
                if (peerTerm > term) {
                    if (log.isDebugEnabled()) {
                        String info = "remains FOLLOWER";
                        if (state != State.FOLLOWER) {
                            info = "steps from " + state.name() + " to FOLLOWER";
                        }
                        log.debug(
                                "{} {} since requested term is {} (higher than my {})",
                                me.getId(), info, peerTerm, term
                        );
                    }
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

    private synchronized boolean hasTimedOut() {
        // Adding some entropy
        long entropy = timeoutMillis / 10; // one tenth of configured timeout
        long delta = Math.round(entropy * Math.random());
        return System.currentTimeMillis() - lastHeartbeat > (timeoutMillis + delta);
    }

    private synchronized void refreshTimeout() {
        lastHeartbeat = System.currentTimeMillis();
    }

    private void newElection() {
        log.debug("{} initiating new election...", me.getId());

        synchronized (this) {
            state = State.CANDIDATE;
            term++;
            votedFor = me;
            refreshTimeout();
        }

        VoteRequest voteReq = new VoteRequest(term, me.getId());
        log.trace("{} ({}) voting for myself for term {}", me.getId(), state.name(), voteReq.getTerm());
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
        StringBuilder sb = new StringBuilder(String.format("Handling vote responses from %d peers", responses.size()));
        if (log.isTraceEnabled()) {
            sb.append(":");
            for (VoteResponse response : responses) {
                sb.append(" /").append(response.isVoteGranted());
                sb.append("@").append(response.getCurrentTerm());
            }
        }
        log.trace(sb.toString());

        if (state == State.CANDIDATE) {
            if (this.term == electionTerm) {
                // We’re still in the same term we were when issuing vote
                // and we are still a candidate
                long votesGranted = 1; // since I voted for myself
                for (VoteResponse response : responses) {
                    if (response.isVoteGranted()) {
                        votesGranted++;
                    } else {
                        // This node rejected my vote, could be that I am trying to
                        // rejoin the cluster and have to get up to speed and update my term
                        long currentTerm = response.getCurrentTerm();
                        if (currentTerm > term) {
                            // I am lagging behind
                            log.info("{} rejoining cluster at term {} as FOLLOWER", me.getId(), currentTerm);
                            term = currentTerm;
                            state = State.FOLLOWER;
                            refreshTimeout();
                            return;
                        }
                    }
                }

                // Let's see if we got a majority
                int majority = (peers.size() + 1) / 2 + 1;
                if (votesGranted >= majority) {
                    if (log.isDebugEnabled()) {
                        log.debug("{} elected LEADER ({} votes granted >= majority {})", me.getId(), votesGranted, majority);
                    }
                    state = State.LEADER;
                }
            }
        }
    }

    private synchronized void broadcastHeartbeat() {
        if (state == State.LEADER) {
            if (log.isTraceEnabled()) {
                log.trace("LEADER {} broadcasting heartbeat for term {}", me.getId(), term);
            }

            LogEntry entry = new LogEntry(LogEntry.Type.HEARTBEAT, term, me.getId());
            nettyRaftClient.broadcastLogEntry(entry);
        }
    }
}