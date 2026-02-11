package org.gautelis.raft;

import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.Future;
import org.gautelis.raft.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.Arrays;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

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
 * Implementation note:
 * - Comments in this class reference the Raft paper "In Search of an Understandable Consensus Algorithm"
 *   (extended version, 2014), primarily Figure 2 (server rules) and Figure 3 (safety properties).
 * - Figure 2 is treated as executable guidance: most state transitions and RPC receiver logic below
 *   are annotated with the exact rule/step they implement.
 * - Figure 3 properties are called out where this implementation enforces them directly.
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
    private final Map<String, Long> nextIndex = new HashMap<>();
    private final Map<String, Long> matchIndex = new HashMap<>();
    private final Peer me;

    //
    private final LogStore logStore;

    //
    private final MessageHandler messageHandler;
    private final SnapshotStateMachine snapshotStateMachine;

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
    private long commitIndex = 0L;
    private long lastApplied = 0L;
    private final PersistentStateStore persistentState;
    private final int snapshotMinEntries;

    public RaftNode(Peer me, List<Peer> peers, long timeoutMillis, MessageHandler messageHandler, RaftClient raftClient, LogStore logStore) {
        this(me, peers, timeoutMillis, messageHandler, (SnapshotStateMachine) null, raftClient, logStore, new InMemoryPersistentStateStore(), System::currentTimeMillis, new Random());
    }

    public RaftNode(Peer me, List<Peer> peers, long timeoutMillis, MessageHandler messageHandler, RaftClient raftClient, LogStore logStore, PersistentStateStore persistentState) {
        this(me, peers, timeoutMillis, messageHandler, (SnapshotStateMachine) null, raftClient, logStore, persistentState, System::currentTimeMillis, new Random());
    }

    public RaftNode(Peer me, List<Peer> peers, long timeoutMillis, MessageHandler messageHandler, CommandHandler commandHandler, RaftClient raftClient, LogStore logStore, PersistentStateStore persistentState) {
        this(me, peers, timeoutMillis, messageHandler, commandHandler == null ? null : new CommandHandlerStateMachineAdapter(commandHandler), raftClient, logStore, persistentState, System::currentTimeMillis, new Random());
    }

    public RaftNode(Peer me, List<Peer> peers, long timeoutMillis, MessageHandler messageHandler, SnapshotStateMachine snapshotStateMachine, RaftClient raftClient, LogStore logStore, PersistentStateStore persistentState) {
        this(me, peers, timeoutMillis, messageHandler, snapshotStateMachine, raftClient, logStore, persistentState, System::currentTimeMillis, new Random());
    }

    RaftNode(Peer me,
             List<Peer> peers,
             long timeoutMillis,
             MessageHandler messageHandler,
             SnapshotStateMachine snapshotStateMachine,
             RaftClient raftClient,
             LogStore logStore,
             TimeSource timeSource,
             Random rng) {
        this(me, peers, timeoutMillis, messageHandler, snapshotStateMachine, raftClient, logStore, new InMemoryPersistentStateStore(), timeSource, rng);
    }

    RaftNode(Peer me,
             List<Peer> peers,
             long timeoutMillis,
             MessageHandler messageHandler,
             RaftClient raftClient,
             LogStore logStore,
             TimeSource timeSource,
             Random rng) {
        this(me, peers, timeoutMillis, messageHandler, (SnapshotStateMachine) null, raftClient, logStore, new InMemoryPersistentStateStore(), timeSource, rng);
    }

    RaftNode(Peer me,
             List<Peer> peers,
             long timeoutMillis,
             MessageHandler messageHandler,
             RaftClient raftClient,
             LogStore logStore,
             PersistentStateStore persistentState,
             TimeSource timeSource,
             Random rng) {
        this(me, peers, timeoutMillis, messageHandler, (SnapshotStateMachine) null, raftClient, logStore, persistentState, timeSource, rng);
    }

    RaftNode(Peer me,
             List<Peer> peers,
             long timeoutMillis,
             MessageHandler messageHandler,
             CommandHandler commandHandler,
             RaftClient raftClient,
             LogStore logStore,
             PersistentStateStore persistentState,
             TimeSource timeSource,
             Random rng) {
        this(me, peers, timeoutMillis, messageHandler, commandHandler == null ? null : new CommandHandlerStateMachineAdapter(commandHandler), raftClient, logStore, persistentState, timeSource, rng);
    }

    RaftNode(Peer me,
             List<Peer> peers,
             long timeoutMillis,
             MessageHandler messageHandler,
             SnapshotStateMachine snapshotStateMachine,
             RaftClient raftClient,
             LogStore logStore,
             PersistentStateStore persistentState,
             TimeSource timeSource,
             Random rng) {
        this.me = me;
        if (me == null || me.getId() == null || me.getId().isBlank()) {
            throw new IllegalArgumentException("Local peer must have non-null, non-blank id");
        }
        for (Peer peer : peers) {
            if (peer == null) continue;
            if (peer.getId() == null || peer.getId().isBlank()) {
                throw new IllegalArgumentException("Cluster peer must have non-null, non-blank id");
            }
            if (me.getId().equals(peer.getId())) {
                if (!Objects.equals(me.getAddress(), peer.getAddress())) {
                    throw new IllegalArgumentException("Conflicting peer configuration for id " + peer.getId() + ": " + me.getAddress() + " vs " + peer.getAddress());
                }
                continue; // do not include self in peers
            }
            Peer existing = this.peers.get(peer.getId());
            if (existing != null && !Objects.equals(existing.getAddress(), peer.getAddress())) {
                throw new IllegalArgumentException("Conflicting peer configuration for id " + peer.getId() + ": " + existing.getAddress() + " vs " + peer.getAddress());
            }
            this.peers.putIfAbsent(peer.getId(), peer);
        }
        this.timeoutMillis = timeoutMillis;
        this.messageHandler = messageHandler;
        this.snapshotStateMachine = snapshotStateMachine;
        this.raftClient = raftClient;
        this.logStore = logStore;
        this.persistentState = persistentState;
        this.timeSource = timeSource;
        this.rng = rng;
        this.snapshotMinEntries = Integer.getInteger("raft.snapshot.min.entries", 1_000);
        this.raftClient.setKnownPeers(this.peers.values());

        this.currentTerm = persistentState.currentTerm();
        this.votedFor = persistentState.votedFor()
                .map(this::resolvePeerById)
                .orElse(null);
        if (persistentState.votedFor().isPresent() && this.votedFor == null) {
            persistentState.setVotedFor(null);
        }
    }

    private Peer resolvePeerById(String peerId) {
        if (peerId == null) {
            return null;
        }
        if (me != null && peerId.equals(me.getId())) {
            return me;
        }
        return peers.get(peerId);
    }

    private void persistCurrentTerm(long term) {
        // Figure 2 ("All Servers"): if an RPC has higher term, node updates currentTerm before continuing.
        // Persisting here preserves election safety across restart (Figure 3: Election Safety).
        currentTerm = term;
        persistentState.setCurrentTerm(term);
    }

    private void persistVotedFor(Peer peer) {
        // Figure 2 RequestVote receiver and candidate conversion both mutate votedFor.
        // Persisting vote prevents double-voting after crashes (Figure 3: Election Safety).
        votedFor = peer;
        persistentState.setVotedFor(peer == null ? null : peer.getId());
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
        // Figure 2 RequestVote receiver step 2 and §5.4:
        // candidate log must be at least as up-to-date as receiver log.
        long myLastTerm = logStore.lastTerm();
        long myLastIndex = logStore.lastIndex();

        if (req.getLastLogTerm() != myLastTerm) {
            return req.getLastLogTerm() > myLastTerm;
        }
        return req.getLastLogIndex() >= myLastIndex;
    }

    public synchronized VoteResponse handleVoteRequest(VoteRequest req) {
        log.debug("{}@{} received vote request for term {} from {}", me.getId(), currentTerm, req.getTerm(), req.getCandidateId());

        // Figure 2 RequestVote receiver step 1:
        // reject stale terms immediately.
        if (req.getTerm() < currentTerm) {
            if (log.isDebugEnabled()) {
                log.debug("{}@{} ({}) has higher term than requested term {} from {} => reject",
                        me.getId(), currentTerm, state.name(), req.getTerm(), req.getCandidateId());
            }
            return new VoteResponse(req, me.getId(), false, /* my term */ currentTerm);
        }

        // Figure 2 ("All Servers"):
        // if RPC term > currentTerm, update term and convert to follower.
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

            persistCurrentTerm(req.getTerm());
            state = State.FOLLOWER;
            persistVotedFor(null);
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

        // Figure 2 RequestVote receiver step 2:
        // grant vote iff not voted (or same candidate) and candidate log is up-to-date.
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
                persistVotedFor(peer);
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

    public synchronized boolean submitCommand(String command) {
        // Figure 2 ("Leaders"): only the leader accepts client commands into the log.
        if (state != State.LEADER) {
            return false;
        }
        if (command == null || command.isBlank()) {
            return false;
        }

        LogEntry entry = new LogEntry(currentTerm, me.getId(), command.getBytes(StandardCharsets.UTF_8));
        logStore.append(Collections.singletonList(entry));
        // Figure 2 ("Leaders"): after local append, replicate to followers and eventually commit.
        advanceCommitIndexFromMajority();
        broadcastHeartbeat(); // replicate promptly instead of waiting for next tick
        return true;
    }

    public synchronized AppendEntriesResponse handleAppendEntries(AppendEntriesRequest request) {
        long leaderTerm = request.getTerm();
        String leaderId = request.getLeaderId();

        // Figure 2 AppendEntries receiver step 1: reject stale leader term.
        if (leaderTerm < currentTerm) {
            return new AppendEntriesResponse(currentTerm, me.getId(), false, logStore.lastIndex());
        }
        if (leaderId == null || leaderId.equals(me.getId()) || !peers.containsKey(leaderId)) {
            return new AppendEntriesResponse(currentTerm, me.getId(), false, logStore.lastIndex());
        }

        // Figure 2 ("All Servers"): higher term in RPC causes follower transition.
        if (leaderTerm > currentTerm) {
            persistCurrentTerm(leaderTerm);
            state = State.FOLLOWER;
            persistVotedFor(null);
            electionSequenceCounter = 0;
        } else if (state != State.FOLLOWER) {
            state = State.FOLLOWER;
            persistVotedFor(null);
            electionSequenceCounter = 0;
        }
        refreshTimeout();

        long prevLogIndex = request.getPrevLogIndex();
        long prevLogTerm = request.getPrevLogTerm();
        // Snapshot boundary handling: if follower already compacted this prefix,
        // leader must switch to InstallSnapshot (instead of AppendEntries retry).
        if (prevLogIndex < logStore.snapshotIndex()) {
            return new AppendEntriesResponse(currentTerm, me.getId(), false, logStore.snapshotIndex());
        }
        // Figure 2 AppendEntries receiver step 2:
        // reject if no entry at prevLogIndex or term mismatch.
        if (prevLogIndex > logStore.lastIndex()) {
            return new AppendEntriesResponse(currentTerm, me.getId(), false, logStore.lastIndex());
        }
        if (prevLogIndex > 0 && logStore.termAt(prevLogIndex) != prevLogTerm) {
            return new AppendEntriesResponse(currentTerm, me.getId(), false, prevLogIndex - 1);
        }

        long index = prevLogIndex + 1;
        List<LogEntry> entries = request.getEntries();
        int i = 0;
        // Figure 2 AppendEntries receiver step 3:
        // delete conflicting existing entry and all that follow.
        while (i < entries.size()) {
            LogEntry incoming = entries.get(i);
            if (index <= logStore.lastIndex()) {
                LogEntry local = logStore.entryAt(index);
                if (!sameEntry(local, incoming)) {
                    logStore.truncateFrom(index);
                    break;
                }
            } else {
                break;
            }
            index++;
            i++;
        }
        if (i < entries.size()) {
            // Figure 2 AppendEntries receiver step 4: append new entries.
            logStore.append(entries.subList(i, entries.size()));
        }

        // Figure 2 AppendEntries receiver step 5:
        // commit up to min(leaderCommit, lastIndex), then apply in-order.
        if (request.getLeaderCommit() > commitIndex) {
            commitIndex = Math.min(request.getLeaderCommit(), logStore.lastIndex());
            applyCommittedEntries();
        }
        return new AppendEntriesResponse(currentTerm, me.getId(), true, logStore.lastIndex());
    }

    public synchronized InstallSnapshotResponse handleInstallSnapshot(InstallSnapshotRequest request) {
        long leaderTerm = request.getTerm();
        String leaderId = request.getLeaderId();

        // Same term gate as AppendEntries receiver step 1 (Figure 2 semantics).
        if (leaderTerm < currentTerm) {
            return new InstallSnapshotResponse(currentTerm, me.getId(), false, logStore.snapshotIndex());
        }
        if (leaderId == null || leaderId.equals(me.getId()) || !peers.containsKey(leaderId)) {
            return new InstallSnapshotResponse(currentTerm, me.getId(), false, logStore.snapshotIndex());
        }

        // Figure 2 ("All Servers"): higher term from RPC forces follower transition.
        if (leaderTerm > currentTerm) {
            persistCurrentTerm(leaderTerm);
            state = State.FOLLOWER;
            persistVotedFor(null);
            electionSequenceCounter = 0;
        } else if (state != State.FOLLOWER) {
            state = State.FOLLOWER;
            persistVotedFor(null);
            electionSequenceCounter = 0;
        }

        // Snapshot install replaces compacted prefix and restores state machine checkpoint.
        // This is the mechanism that preserves catch-up when entries are no longer available.
        logStore.installSnapshot(
                request.getLastIncludedIndex(),
                request.getLastIncludedTerm(),
                request.getSnapshotData()
        );
        if (snapshotStateMachine != null) {
            snapshotStateMachine.restore(request.getSnapshotData());
        }
        commitIndex = Math.max(commitIndex, request.getLastIncludedIndex());
        lastApplied = Math.max(lastApplied, request.getLastIncludedIndex());
        refreshTimeout();
        return new InstallSnapshotResponse(currentTerm, me.getId(), true, logStore.snapshotIndex());
    }

    private boolean sameEntry(LogEntry left, LogEntry right) {
        return left.getTerm() == right.getTerm()
                && java.util.Objects.equals(left.getPeerId(), right.getPeerId())
                && Arrays.equals(left.getData(), right.getData());
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
        log.debug("{}@{} initiating new election...", me.getId(), currentTerm);

        synchronized (this) {
            // Figure 2 ("Candidates"): on conversion to candidate:
            // increment term, vote for self, reset election timer.
            state = State.CANDIDATE;
            persistCurrentTerm(currentTerm + 1);
            persistVotedFor(me);
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
                        log.info("{}@{} no vote response", me.getId(), currentTerm, f.cause());
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
                    persistCurrentTerm(maxTerm);
                    state = State.FOLLOWER;
                    persistVotedFor(null);
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
                            persistCurrentTerm(responderTerm);
                            state = State.FOLLOWER;
                            electionSequenceCounter = 0;
                            persistVotedFor(null);
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
                    // Figure 2 ("Leaders"): initialize replication state and begin heartbeat/append flow.
                    initializeLeaderReplicationState();
                }
            }
        }
    }

    private synchronized void broadcastHeartbeat() {
        if (state == State.LEADER) {
            // Figure 2 ("Leaders"):
            // - send empty AppendEntries as heartbeat
            // - send log entries starting at each follower's nextIndex
            for (Peer peer : peers.values()) {
                long configuredNext = nextIndex.getOrDefault(peer.getId(), logStore.lastIndex() + 1);
                if (logStore.snapshotIndex() > 0 && configuredNext <= logStore.snapshotIndex()) {
                    // Follower is behind compacted prefix; switch to InstallSnapshot.
                    byte[] snapshotBytes = logStore.snapshotData();
                    if (snapshotBytes.length == 0 && snapshotStateMachine != null) {
                        snapshotBytes = snapshotStateMachine.snapshot();
                    }
                    InstallSnapshotRequest installSnapshotRequest = new InstallSnapshotRequest(
                            currentTerm,
                            me.getId(),
                            logStore.snapshotIndex(),
                            logStore.snapshotTerm(),
                            snapshotBytes
                    );
                    raftClient.sendInstallSnapshot(peer, installSnapshotRequest).whenComplete((resp, error) -> {
                        if (error != null || resp == null) {
                            return;
                        }
                        handleInstallSnapshotResponse(peer, installSnapshotRequest, resp);
                    });
                    continue;
                }
                long minNext = logStore.snapshotIndex() + 1;
                long next = Math.max(minNext, configuredNext);
                long prev = Math.max(0, next - 1);
                long prevTerm = prev == 0 ? 0 : logStore.termAt(prev);
                List<LogEntry> entries = logStore.entriesFrom(next);

                AppendEntriesRequest req = new AppendEntriesRequest(
                        currentTerm,
                        me.getId(),
                        prev,
                        prevTerm,
                        commitIndex,
                        entries
                );
                raftClient.sendAppendEntries(peer, req).whenComplete((resp, error) -> {
                    if (error != null || resp == null) {
                        return;
                    }
                    handleAppendEntriesResponse(peer, req, resp);
                });
            }
        }
    }

    private synchronized void handleAppendEntriesResponse(Peer peer, AppendEntriesRequest request, AppendEntriesResponse response) {
        if (state != State.LEADER) {
            return;
        }
        if (response.getTerm() > currentTerm) {
            persistCurrentTerm(response.getTerm());
            state = State.FOLLOWER;
            persistVotedFor(null);
            electionSequenceCounter = 0;
            refreshTimeout();
            return;
        }
        if (!response.isSuccess()) {
            // Figure 2 ("Leaders"): on inconsistency, decrement nextIndex and retry.
            long currentNext = nextIndex.getOrDefault(peer.getId(), logStore.lastIndex() + 1);
            if (logStore.snapshotIndex() > 0 && response.getMatchIndex() <= logStore.snapshotIndex()) {
                nextIndex.put(peer.getId(), logStore.snapshotIndex());
                return;
            }
            long floor = logStore.snapshotIndex() + 1;
            nextIndex.put(peer.getId(), Math.max(floor, currentNext - 1));
            return;
        }

        long advanced = request.getPrevLogIndex() + request.getEntries().size();
        nextIndex.put(peer.getId(), advanced + 1);
        matchIndex.put(peer.getId(), advanced);
        // Figure 2 ("Leaders"): if replicated on majority and in current term, advance commitIndex.
        advanceCommitIndexFromMajority();
    }

    private synchronized void handleInstallSnapshotResponse(Peer peer, InstallSnapshotRequest request, InstallSnapshotResponse response) {
        if (state != State.LEADER) {
            return;
        }
        if (response.getTerm() > currentTerm) {
            persistCurrentTerm(response.getTerm());
            state = State.FOLLOWER;
            persistVotedFor(null);
            electionSequenceCounter = 0;
            refreshTimeout();
            return;
        }
        if (!response.isSuccess()) {
            return;
        }

        long index = request.getLastIncludedIndex();
        nextIndex.put(peer.getId(), index + 1);
        matchIndex.put(peer.getId(), index);
        // After snapshot catch-up, follower contributes to majority replication accounting.
        advanceCommitIndexFromMajority();
    }

    private synchronized void initializeLeaderReplicationState() {
        long next = Math.max(logStore.snapshotIndex() + 1, logStore.lastIndex() + 1);
        nextIndex.clear();
        matchIndex.clear();
        for (Peer peer : peers.values()) {
            nextIndex.put(peer.getId(), next);
            matchIndex.put(peer.getId(), logStore.snapshotIndex());
        }
    }

    private synchronized void advanceCommitIndexFromMajority() {
        int clusterSize = peers.size() + 1;
        int majority = (clusterSize / 2) + 1;

        long candidateCommit = commitIndex;
        // Figure 2 ("Leaders"): commit only entries from current term once stored on a majority.
        // This underpins Figure 3 safety (Leader Completeness + State Machine Safety).
        for (long n = commitIndex + 1; n <= logStore.lastIndex(); n++) {
            if (logStore.termAt(n) != currentTerm) {
                continue;
            }

            int replicated = 1; // leader has it
            for (Peer peer : peers.values()) {
                if (matchIndex.getOrDefault(peer.getId(), 0L) >= n) {
                    replicated++;
                }
            }
            if (replicated >= majority) {
                candidateCommit = n;
            }
        }

        if (candidateCommit > commitIndex) {
            commitIndex = candidateCommit;
            // Figure 2 ("All Servers"): apply entries in commit order.
            applyCommittedEntries();
        }
    }

    private synchronized void applyCommittedEntries() {
        // Figure 2 ("All Servers"): if commitIndex > lastApplied, increment lastApplied and apply.
        // Figure 3 State Machine Safety: this monotonic, in-order apply sequence prevents divergent applies.
        while (lastApplied < commitIndex) {
            lastApplied++;
            if (snapshotStateMachine == null) {
                continue;
            }

            LogEntry entry = logStore.entryAt(lastApplied);
            byte[] data = entry.getData();
            if (data.length == 0) {
                continue;
            }

            String command = new String(data, StandardCharsets.UTF_8);
            snapshotStateMachine.apply(entry.getTerm(), command);
        }
        maybeCompactLocalSnapshot();
    }

    private synchronized void maybeCompactLocalSnapshot() {
        if (snapshotStateMachine == null || snapshotMinEntries <= 0) {
            return;
        }
        if (commitIndex == 0 || commitIndex <= logStore.snapshotIndex()) {
            return;
        }

        long uncompacted = commitIndex - logStore.snapshotIndex();
        if (uncompacted < snapshotMinEntries) {
            return;
        }

        // Local compaction checkpoint:
        // snapshot state machine + compact log prefix up to commitIndex.
        long lastIncludedIndex = commitIndex;
        long lastIncludedTerm = logStore.termAt(lastIncludedIndex);
        byte[] snapshotBytes = snapshotStateMachine.snapshot();
        logStore.installSnapshot(lastIncludedIndex, lastIncludedTerm, snapshotBytes);
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

    long getCommitIndexForTest() {
        return commitIndex;
    }

    long getLastAppliedForTest() {
        return lastApplied;
    }
}
