package org.gautelis.raft;

import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.Future;
import org.gautelis.raft.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
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
    private volatile long currentTerm = 0L;

    // Track votes, so a node votes for at most one candidate in a given term
    private volatile Peer votedFor = null;

    // Configurable timeout (in ms)
    private final long timeoutMillis;

    // Used for step-wise increasing delays before issuing a re-election
    private long electionSequenceCounter = 0L;

    // When was a heartbeat last received (timestamp in ms)
    private volatile long lastHeartbeat = 0L;
    private volatile long nextElectionDeadlineMillis = 0L;

    // Keeps track of peers in cluster
    private final Map<String, Peer> peers = new HashMap<>();
    private final Peer me;

    //
    private final LogStore logStore;

    //
    private final MessageHandler messageHandler;

    // Netty-based approach might either
    //  - store channels for each peer, or
    //  - keep a separate client
    protected final RaftClient raftClient; // or similar

    @FunctionalInterface
    interface TimeSource {
        long nowMillis();
    }

    private final TimeSource timeSource;
    private final Random rng;

    public RaftNode(Peer me, List<Peer> peers, long timeoutMillis, MessageHandler messageHandler, RaftClient raftClient, LogStore logStore) {
        this(me, peers, timeoutMillis, messageHandler, raftClient, logStore, System::currentTimeMillis, new Random());
    }

    RaftNode(Peer me,
             List<Peer> peers,
             long timeoutMillis,
             MessageHandler messageHandler,
             RaftClient raftClient,
             LogStore logStore,
             TimeSource timeSource,
             Random rng) {
        this.me = me;
        for (Peer peer : peers) {
            if (peer == null) continue;
            if (me != null && me.getId() != null && me.getId().equals(peer.getId())) {
                continue; // do not include self in peers
            }
            this.peers.putIfAbsent(peer.getId(), peer);
        }
        this.timeoutMillis = timeoutMillis;
        this.messageHandler = messageHandler;
        this.raftClient = raftClient;
        this.logStore = logStore;
        this.timeSource = timeSource;
        this.rng = rng;
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

    public boolean isLeader() {
        return state == State.LEADER;
    }

    /* package private */
    MessageHandler getMessageHandler() {
        return messageHandler;
    }

    private boolean isCandidateLogUpToDate(VoteRequest req) {
        long myLastTerm = logStore.lastTerm();
        long myLastIndex = logStore.lastIndex();

        if (req.getLastLogTerm() != myLastTerm) {
            return req.getLastLogTerm() > myLastTerm;
        }
        return req.getLastLogIndex() >= myLastIndex;
    }

    public synchronized VoteResponse handleVoteRequest(VoteRequest req) {
        log.debug("{}@{} received vote request for term {} from {}", me.getId(), currentTerm, req.getTerm(), req.getCandidateId());

        // If the candidate's term is behind ours, immediately reject.
        if (req.getTerm() < currentTerm) {
            if (log.isDebugEnabled()) {
                log.debug("{}@{} ({}) has higher term than requested term {} from {} => reject",
                        me.getId(), currentTerm, state.name(), req.getTerm(), req.getCandidateId());
            }
            return new VoteResponse(req, me.getId(), false, /* my term */ currentTerm);
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
                        "{}@{} {} since requested term {} is higher",
                        me.getId(), currentTerm, info, req.getTerm()
                );
            }

            currentTerm = req.getTerm();
            state = State.FOLLOWER;
            votedFor = null;
            electionSequenceCounter = 0;
            refreshTimeout();
        }

        //
        if (!isCandidateLogUpToDate(req)) {
            log.debug("{}@{} rejects vote for {}: candidate log is not up-to-date (cand term/index={}/{}, mine={}/{})",
                    me.getId(), currentTerm, req.getCandidateId(),
                    req.getLastLogTerm(), req.getLastLogIndex(),
                    logStore.lastTerm(), logStore.lastIndex());
            return new VoteResponse(req, me.getId(), false, currentTerm);
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
                    log.debug("{}@{} grants vote to {} for term {} and {} => accepted",
                            me.getId(), currentTerm, req.getCandidateId(), req.getTerm(), info);
                }

                state = State.FOLLOWER;
                votedFor = peer;
                electionSequenceCounter = 0;
                refreshTimeout();
                return new VoteResponse(req, me.getId(), true, /* my term */ currentTerm);
            }
            else {
                log.warn("{}@{}: Unknown candidate {} => reject", me.getId(), currentTerm, req.getCandidateId());
                return new VoteResponse(req, me.getId(), false, /* my term */ currentTerm);
            }
        }
        else {
            // We already voted for someone else
            if (log.isDebugEnabled()) {
                log.debug("{}@{} ({}) will *not* grant vote to {} for term {} (already voted for {})",
                        me.getId(), currentTerm, state.name(), req.getCandidateId(), req.getTerm(), votedFor.getId());
            }
            return new VoteResponse(req, me.getId(), false, /* my term */ currentTerm);
        }
    }

    public synchronized void handleHeartbeat(Heartbeat heartbeat) {
        long peerTerm = heartbeat.getTerm();
        String peerId = heartbeat.getPeerId();
        log.trace("{}@{} received heartbeat for term {} from {}", me.getId(), currentTerm, peerTerm, peerId);

        // Ignore heartbeats from nodes that are not members of this cluster.
        if (peerId == null || (!peerId.equals(me.getId()) && !peers.containsKey(peerId))) {
            if (log.isDebugEnabled()) {
                log.debug("{}@{} ignoring heartbeat from unknown peer {}", me.getId(), currentTerm, peerId);
            }
            return;
        }

        // Ignore stale heartbeats; do NOT refresh timeout on them.
        if (peerTerm < currentTerm) {
            log.trace("{}@{} ignoring stale heartbeat from {}: peerTerm={} < currentTerm={}", me.getId(), currentTerm, peerId, peerTerm, currentTerm);
            return;
        }

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
                        "{}@{} {} since requested term {} is higher",
                        me.getId(), currentTerm, info, peerTerm
                );
            }

            currentTerm = peerTerm;
            state = State.FOLLOWER;
            votedFor = null;
            electionSequenceCounter = 0;

        } else {
            // peerTerm == currentTerm: leader discovery for this term
            if (state != State.FOLLOWER) {
                if (log.isDebugEnabled()) {
                    log.debug("{}@{} steps from {} to FOLLOWER due to heartbeat in current term",
                            me.getId(), currentTerm, state.name());
                }
                state = State.FOLLOWER;
                votedFor = null;
                electionSequenceCounter = 0;
            }
        }

        refreshTimeout();
    }

    public synchronized void handleLogEntry(LogEntry entry) {
        long peerTerm = entry.getTerm();
        log.trace("{}@{} received log-entry for term {} from {}", me.getId(), currentTerm, peerTerm, entry.getPeerId());
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
        long now = timeSource.nowMillis();
        if (nextElectionDeadlineMillis == 0L) {
            nextElectionDeadlineMillis = lastHeartbeat + timeoutMillis + sampleElectionExtraDelay();
        }
        return now > nextElectionDeadlineMillis;
    }

    private synchronized void refreshTimeout() {
        long now = timeSource.nowMillis();
        lastHeartbeat = now;
        nextElectionDeadlineMillis = now + timeoutMillis + sampleElectionExtraDelay();
    }

    private long sampleElectionExtraDelay() {
        // Includes jitter and term-local backoff to avoid synchronized elections.
        long jitter = (long) (timeoutMillis * rng.nextDouble()); // [0..timeoutMillis)
        long backoffBase = (timeoutMillis / 10) * electionSequenceCounter;
        long backoffJitter = (long) (backoffBase * rng.nextDouble()); // [0..backoffBase)
        return jitter + backoffBase + backoffJitter;
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

        long lastIndex = logStore.lastIndex();
        long lastTerm  = logStore.lastTerm();

        VoteRequest voteReq = new VoteRequest(currentTerm, me.getId(), lastIndex, lastTerm);
        log.trace("{}@{} ({}) voting for myself for term {}", me.getId(), currentTerm, state.name(), voteReq.getTerm());
        raftClient.requestVoteFromAll(peers.values(), voteReq)
                .addListener((Future<List<VoteResponse>> f) -> {
                    if (f.isSuccess()) {
                        log.trace("{}@{} received responses for term {}", me.getId(), currentTerm, voteReq.getTerm());
                        List<VoteResponse> responses = f.getNow();
                        handleVoteResponses(responses, voteReq.getTerm());
                    }
                    else {
                        log.info("No vote response", f.cause());
                    }
        });
    }

   private synchronized void handleVoteResponses(List<VoteResponse> responses, long electionTerm) {
        StringBuilder sb = new StringBuilder(String.format("%s@%d handling vote responses from %d peers", me.getId(), currentTerm, responses.size()));
        if (log.isTraceEnabled()) {
            sb.append(":");
            for (VoteResponse response : responses) {
                sb.append(" /").append(response.getPeerId());
                sb.append("@").append(response.getCurrentTerm()); // Peer's view of current term
                sb.append(":").append(response.isVoteGranted());
            }
        }
        log.trace(sb.toString());

        if (state == State.CANDIDATE) {
            // Ascertain that this response emanates from a request we sent in the
            // current term. We may receive responses from earlier election rounds
            // and these should be discarded.
            if (currentTerm == electionTerm) {
                // Step down immediately if any response reports a newer term, regardless of voteGranted.
                long maxTerm = currentTerm;
                for (VoteResponse r : responses) {
                    if (r.getCurrentTerm() > maxTerm) {
                        maxTerm = r.getCurrentTerm();
                    }
                }
                if (maxTerm > currentTerm) {
                    log.info("{}@{} discovered higher term {} in vote responses, stepping down to FOLLOWER",
                            me.getId(), currentTerm, maxTerm);
                    currentTerm = maxTerm;
                    state = State.FOLLOWER;
                    votedFor = null;
                    electionSequenceCounter = 0;
                    refreshTimeout();
                    return;
                }

                long votesGranted = 1; // since I voted for myself
                for (VoteResponse response : responses) {
                    if (response.isVoteGranted()) {
                        votesGranted++;
                    } else {
                        // This node rejected my vote, could be that I am trying to
                        // rejoin the cluster and have to get up to speed and update my term

                        long responderTerm = response.getCurrentTerm();
                        if (responderTerm > this.currentTerm) {
                            //--------------------------------------------
                            // d) discovers current leader or new term
                            //--------------------------------------------
                            log.info("{}@{} rejoining cluster as FOLLOWER", me.getId(), this.currentTerm);
                            this.currentTerm = responderTerm;
                            state = State.FOLLOWER;
                            electionSequenceCounter = 0;
                            refreshTimeout();
                            return;
                        }
                    }
                }

                // Let's see if we got a majority
                int clusterSize = peers.size() + 1; // peers excludes me
                int majority = (clusterSize / 2) + 1;

                if (votesGranted >= majority) {
                    //--------------------------------------------
                    // b) receives votes from majority of nodes
                    //--------------------------------------------
                    if (log.isInfoEnabled()) {
                        log.info("{}@{} elected LEADER ({} votes granted >= majority {})", me.getId(), currentTerm, votesGranted, majority);
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
                log.trace("LEADER {}@{} broadcasting heartbeat", me.getId(), currentTerm);
            }

            Heartbeat heartbeat = new Heartbeat(/* my term */ currentTerm, me.getId());
            raftClient.broadcastHeartbeat(heartbeat);
        }
    }

    // ---------------------------
    // Test hooks (package-private)
    // ---------------------------
    State getStateForTest() {
        return state;
    }

    long getLastHeartbeatMillisForTest() {
        return lastHeartbeat;
    }

    void setLastHeartbeatMillisForTest(long v) {
        lastHeartbeat = v;
        nextElectionDeadlineMillis = 0L;
    }

    void electionTickForTest() {
        checkTimeout();
    }

    void heartbeatTickForTest() {
        broadcastHeartbeat();
    }

    void handleVoteResponsesForTest(java.util.List<VoteResponse> r, long t) {
        handleVoteResponses(r, t);
    }
}
