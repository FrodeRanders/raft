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
 *                       a) times out, starts election        b) receives votes from majority of servers
 *                       ┌───────────────────────────┐       ┌────────────────────────┐
 * starts up             │                           ▼       │                        ▼
 *    or      ┌──────────────────────┐        ┌─────────────────────┐       ┌───────────────────┐
 *  rejoins   │                      │        │                     │       │                   │
 * ──────────▶│       Follower       │        │      Candidate      │       │      Leader       │
 *            │                      │        │                     │◀──┐   │                   │
 *            └──────────────────────┘        └─────────────────────┘   │   └───────────────────┘
 *                   ▲        ▲                          │       │      │             │
 *                   │        └──────────────────────────┘       └──────┘             │
 *                   │           d) discovers current             e) times out,       │
 *                   │              leader or new term               new election     │
 *                   │                                                                │
 *                   └────────────────────────────────────────────────────────────────┘
 *                      c) discovers server with higher term (via received heartbeat)
 *
 *
 */
public class RaftNode {
    private static final Logger log = LoggerFactory.getLogger(RaftNode.class);

    enum State { FOLLOWER, CANDIDATE, LEADER }
    private volatile State state = State.FOLLOWER;

    // Logical clock used to identify stale leaders and candidates.
    private long currentTerm = 0L;

    // Track votes, so a node votes for at most one candidate in a given term
    private Peer votedFor = null;

    // Configurable timeout (in ms)
    private final long timeoutMillis;

    // Used for step-wise increasing delays before issuing a re-election
    private long electionSequenceCounter = 0L;

    // When was a heartbeat last received (timestamp in ms)
    private long lastHeartbeat = 0L;

    // Keeps track of peers in cluster
    private final Map<String, Peer> peers = new HashMap<>();
    private final Peer me;

    //
    private final LogHandler logHandler;
    private final MessageHandler messageHandler;

    // Netty-based approach might either
    //  - store channels for each peer, or
    //  - keep a separate client
    protected final RaftClient raftClient; // or similar

    public RaftNode(Peer me, List<Peer> peers, long timeoutMillis, LogHandler logHandler, MessageHandler messageHandler, RaftClient raftClient) {
        for (Peer peer : peers) {
            this.peers.putIfAbsent(peer.getId(), peer);
        }

        this.me = me;
        this.timeoutMillis = timeoutMillis;
        this.logHandler = logHandler;
        this.messageHandler = messageHandler;
        this.raftClient = raftClient;
    }

    public RaftNode(Peer me, List<Peer> peers, long timeoutMillis, LogHandler logHandler, MessageHandler messageHandler) {
        this(me, peers, timeoutMillis, logHandler, messageHandler, new RaftClient(messageHandler));
    }

    public void shutdown() {
        raftClient.shutdown();
    }

    public RaftClient getRaftClient() {
        return raftClient;
    }

    public long getTerm() {
        return currentTerm;
    }

    public String getId() {
        return me.getId();
    }

    /* package private */
    MessageHandler getMessageHandler() {
        return messageHandler;
    }

    public synchronized VoteResponse handleVoteRequest(VoteRequest req) {
        log.debug("{} received vote request for term {} from {}", me.getId(), req.getTerm(), req.getCandidateId());

        // If the candidate's term is behind ours, immediately reject.
        if (req.getTerm() < currentTerm) {
            if (log.isDebugEnabled()) {
                log.debug("{} ({}) has term {} which is newer than requested term {} from {} => reject",
                        me.getId(), state.name(), currentTerm, req.getTerm(), req.getCandidateId());
            }
            return new VoteResponse(req, false, /* my term */ currentTerm);
        }

        // If the candidate's term is higher, become follower.
        if (req.getTerm() > currentTerm) {
            //-------------------------------------
            // c) discovers node with higher term
            //-------------------------------------
            if (log.isDebugEnabled()) {
                String info = "remains FOLLOWER";
                if (state != State.FOLLOWER) {
                    info = "steps from " + state.name() + " to FOLLOWER";
                }
                log.debug(
                        "{} {} since requested term is {} (> {})",
                        me.getId(), info, req.getTerm(), currentTerm
                );
            }

            currentTerm = req.getTerm();
            state = State.FOLLOWER;
            votedFor = null;
            electionSequenceCounter = 0;
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
                electionSequenceCounter = 0;
                refreshTimeout();
                return new VoteResponse(req, true, /* my term */ currentTerm);
            }
            else {
                log.warn("Unknown candidate {} => reject", req.getCandidateId());
                return new VoteResponse(req, false, /* my term */ currentTerm);
            }
        }
        else {
            // We already voted for someone else
            if (log.isDebugEnabled()) {
                log.debug("{} ({}) will *not* grant vote to {} for term {} (already voted for {})",
                        me.getId(), state.name(), req.getCandidateId(), req.getTerm(), votedFor.getId());
            }
            return new VoteResponse(req, false, /* my term */ currentTerm);
        }
    }

    public synchronized void handleLogEntry(LogEntry entry) {
        switch (entry.getType()) {
            case HEARTBEAT -> {
                long peerTerm = entry.getTerm();
                log.trace("Receive heartbeat for term {} from {}: {}", peerTerm, entry.getPeerId(), entry.getType());

                // If the peer's term is higher, become follower.
                if (peerTerm > currentTerm) {
                    //----------------------------------------
                    // c) discovers server with higher term
                    // e) discovers new term
                    //----------------------------------------
                    if (log.isDebugEnabled()) {
                        String info = "remains FOLLOWER";
                        if (state != State.FOLLOWER) {
                            info = "steps from " + state.name() + " to FOLLOWER";
                        }
                        log.debug(
                                "{} {} since requested term is {} (higher than my {})",
                                me.getId(), info, peerTerm, currentTerm
                        );
                    }

                    currentTerm = peerTerm;
                    state = State.FOLLOWER;
                    votedFor = null;
                    electionSequenceCounter = 0;
                }

                refreshTimeout();
            }

            default -> {
                if (null != logHandler) {
                    logHandler.handle(/* my term */ currentTerm, entry); // entry has "their" term
                }
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
            //---------------------------------
            // a) times out, starts election
            // e) times out, new election
            //---------------------------------
            newElection();
        }
    }

    private synchronized boolean hasTimedOut() {
        long baseDelay = timeoutMillis / 10;
        baseDelay *= electionSequenceCounter;
        long delay = baseDelay + Math.round( baseDelay * Math.random());
        return System.currentTimeMillis() - lastHeartbeat > (timeoutMillis + delay);
    }

    private synchronized void refreshTimeout() {
        lastHeartbeat = System.currentTimeMillis();
    }

    private void newElection() {
        log.debug("{} initiating new election...", me.getId());

        synchronized (this) {
            state = State.CANDIDATE;
            currentTerm++;
            votedFor = me;
            electionSequenceCounter++;
            refreshTimeout();
        }

        VoteRequest voteReq = new VoteRequest(currentTerm, me.getId());
        log.trace("{} ({}) voting for myself for term {}", me.getId(), state.name(), voteReq.getTerm());
        raftClient.requestVoteFromAll(peers.values(), voteReq)
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
            // Ascertain that this response emanates from a request we sent in the
            // current term. We may receive responses from earlier election rounds
            // and these should be discarded.
            if (this.currentTerm == electionTerm) {
                long votesGranted = 1; // since I voted for myself
                for (VoteResponse response : responses) {
                    if (response.isVoteGranted()) {
                        votesGranted++;
                    } else {
                        // This node rejected my vote, could be that I am trying to
                        // rejoin the cluster and have to get up to speed and update my term

                        long currentTerm = response.getCurrentTerm();
                        if (currentTerm > this.currentTerm) {
                            //--------------------------------------------
                            // d) discovers current leader or new term
                            //--------------------------------------------
                            log.info("{} rejoining cluster at term {} as FOLLOWER", me.getId(), currentTerm);
                            this.currentTerm = currentTerm;
                            state = State.FOLLOWER;
                            electionSequenceCounter = 0;
                            refreshTimeout();
                            return;
                        }
                    }
                }

                // Let's see if we got a majority
                int majority = (peers.size() + 1) / 2 + 1;
                if (votesGranted >= majority) {
                    //--------------------------------------------
                    // b) receives votes from majority of nodes
                    //--------------------------------------------
                    if (log.isDebugEnabled()) {
                        log.debug("{} elected LEADER ({} votes granted >= majority {})", me.getId(), votesGranted, majority);
                    }
                    state = State.LEADER;
                    electionSequenceCounter = 0;
                }
            }
        }
    }

    private synchronized void broadcastHeartbeat() {
        if (state == State.LEADER) {
            if (log.isTraceEnabled()) {
                log.trace("LEADER {} broadcasting heartbeat for term {}", me.getId(), currentTerm);
            }

            LogEntry entry = new LogEntry(LogEntry.Type.HEARTBEAT, /* my term */ currentTerm, me.getId());
            raftClient.broadcastLogEntry(entry);
        }
    }
}