/*
 * Copyright (C) 2025-2026 Frode Randers
 * All rights reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gautelis.raft;

import org.gautelis.raft.protocol.*;
import org.gautelis.raft.statemachine.CommandHandler;
import org.gautelis.raft.statemachine.CommandHandlerStateMachineAdapter;
import org.gautelis.raft.statemachine.SnapshotStateMachine;
import org.gautelis.raft.storage.InMemoryPersistentStateStore;
import org.gautelis.raft.storage.LogStore;
import org.gautelis.raft.storage.PersistentStateStore;
import org.gautelis.raft.transport.RaftTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ScheduledExecutorService;
import java.util.Arrays;
import java.util.Collections;
import java.util.NavigableMap;
import java.util.TreeMap;

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
 * - Cluster membership changes are replicated through the log. This implementation supports
 *   stable and joint-consensus transitions, along with configuration-aware elections, commits,
 *   and snapshots.
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
    private final Map<String, Long> lastFollowerContactMillis = new HashMap<>();
    private final Map<String, Integer> consecutiveFollowerFailures = new HashMap<>();
    private final Map<String, Long> lastFollowerFailureMillis = new HashMap<>();
    private final Peer me;
    private ClusterConfiguration clusterConfiguration;
    private ClusterConfiguration snapshotConfiguration;
    private final NavigableMap<Long, ClusterConfiguration> committedConfigurations = new TreeMap<>();
    private boolean decommissioned;
    private boolean joining;
    private String knownLeaderId;
    private Runnable decommissionListener = () -> {};
    private List<String> pendingAutoFinalizeMembers = List.of();
    private long pendingAutoFinalizeFenceIndex;
    private long configurationTransitionStartedMillis;
    private final java.util.Set<String> pendingJoinIds = new java.util.LinkedHashSet<>();
    // Leader-side chunk progress and follower-side partial assembly for InstallSnapshot streaming.
    private final Map<String, Long> snapshotOffsets = new HashMap<>();
    private PendingSnapshot pendingSnapshot;

    //
    private final LogStore logStore;

    //
    private final MessageHandler messageHandler;
    private final SnapshotStateMachine snapshotStateMachine;

    // Netty-based approach might either
    //  - store channels for each peer, or
    //  - keep a separate client
    protected final RaftTransportClient raftClient; // or similar

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
    private final int snapshotChunkBytes;
    private final long linearizableReadLeaseMillis;
    private final long linearizableReadTimeoutMillis;

    public static Builder forPeer(Peer me) {
        return new Builder(me);
    }

    /**
     * @deprecated Prefer {@link #forPeer(Peer)} and {@link Builder#build()} for new code.
     */
    @Deprecated(forRemoval = false)
    public RaftNode(Peer me, List<Peer> peers, long timeoutMillis, MessageHandler messageHandler, RaftTransportClient raftClient, LogStore logStore) {
        this(builderFor(me, peers, timeoutMillis, messageHandler, raftClient, logStore)
                .withPersistentStateStore(new InMemoryPersistentStateStore())
                .withTimeSource(System::currentTimeMillis)
                .withRandom(new Random()));
    }

    /**
     * @deprecated Prefer {@link #forPeer(Peer)} and {@link Builder#build()} for new code.
     */
    @Deprecated(forRemoval = false)
    public RaftNode(Peer me, List<Peer> peers, long timeoutMillis, MessageHandler messageHandler, RaftTransportClient raftClient, LogStore logStore, PersistentStateStore persistentState) {
        this(builderFor(me, peers, timeoutMillis, messageHandler, raftClient, logStore)
                .withPersistentStateStore(persistentState)
                .withTimeSource(System::currentTimeMillis)
                .withRandom(new Random()));
    }

    /**
     * @deprecated Prefer {@link #forPeer(Peer)} and {@link Builder#build()} for new code.
     */
    @Deprecated(forRemoval = false)
    public RaftNode(Peer me, List<Peer> peers, long timeoutMillis, MessageHandler messageHandler, CommandHandler commandHandler, RaftTransportClient raftClient, LogStore logStore, PersistentStateStore persistentState) {
        this(builderFor(me, peers, timeoutMillis, messageHandler, raftClient, logStore)
                .withCommandHandler(commandHandler)
                .withPersistentStateStore(persistentState)
                .withTimeSource(System::currentTimeMillis)
                .withRandom(new Random()));
    }

    /**
     * @deprecated Prefer {@link #forPeer(Peer)} and {@link Builder#build()} for new code.
     */
    @Deprecated(forRemoval = false)
    public RaftNode(Peer me, List<Peer> peers, long timeoutMillis, MessageHandler messageHandler, SnapshotStateMachine snapshotStateMachine, RaftTransportClient raftClient, LogStore logStore, PersistentStateStore persistentState) {
        this(builderFor(me, peers, timeoutMillis, messageHandler, raftClient, logStore)
                .withSnapshotStateMachine(snapshotStateMachine)
                .withPersistentStateStore(persistentState)
                .withTimeSource(System::currentTimeMillis)
                .withRandom(new Random()));
    }

    private RaftNode(Builder builder) {
        this.me = builder.me;
        if (me == null || me.getId() == null || me.getId().isBlank()) {
            throw new IllegalArgumentException("Local peer must have non-null, non-blank id");
        }
        for (Peer peer : builder.peers) {
            registerPeer(peer);
        }
        this.timeoutMillis = builder.timeoutMillis;
        this.messageHandler = builder.messageHandler;
        this.snapshotStateMachine = builder.snapshotStateMachine;
        this.raftClient = builder.raftClient;
        this.logStore = builder.logStore;
        this.persistentState = builder.persistentStateStore;
        this.timeSource = builder.timeSource;
        this.rng = builder.random;
        this.snapshotMinEntries = Integer.getInteger("raft.snapshot.min.entries", 1_000);
        this.snapshotChunkBytes = Integer.getInteger("raft.snapshot.chunk.bytes", 64 * 1024);
        this.linearizableReadLeaseMillis = Long.getLong("raft.linearizable.read.lease.millis", Math.max(1L, builder.timeoutMillis / 2L));
        this.linearizableReadTimeoutMillis = Long.getLong("raft.linearizable.read.timeout.millis", Math.max(50L, Math.min(500L, builder.timeoutMillis)));
        this.clusterConfiguration = ClusterConfiguration.stable(allConfiguredMembers());
        this.snapshotConfiguration = clusterConfiguration;
        var decodedSnapshot = ClusterConfigurationSnapshotCodec.decode(builder.logStore.snapshotData());
        if (decodedSnapshot.isPresent()) {
            this.snapshotConfiguration = decodedSnapshot.get().configuration();
            this.clusterConfiguration = snapshotConfiguration;
        }
        this.clusterConfiguration = configurationAt(builder.logStore.lastIndex());
        this.decommissioned = !this.clusterConfiguration.contains(me.getId());
        this.committedConfigurations.put(builder.logStore.snapshotIndex(), clusterConfiguration);
        this.raftClient.setKnownPeers(remoteReplicatedPeersFor(configurationAt(builder.logStore.lastIndex())));

        this.currentTerm = builder.persistentStateStore.currentTerm();
        this.votedFor = builder.persistentStateStore.votedFor()
                .map(this::resolvePeerById)
                .orElse(null);
        if (builder.persistentStateStore.votedFor().isPresent() && this.votedFor == null) {
            builder.persistentStateStore.setVotedFor(null);
        }
    }

    private static Builder builderFor(Peer me,
                                      List<Peer> peers,
                                      long timeoutMillis,
                                      MessageHandler messageHandler,
                                      RaftTransportClient raftClient,
                                      LogStore logStore) {
        return forPeer(me)
                .withPeers(peers)
                .withTimeoutMillis(timeoutMillis)
                .withMessageHandler(messageHandler)
                .withClient(raftClient)
                .withLogStore(logStore);
    }

    public static final class Builder {
        private final Peer me;
        private List<Peer> peers = List.of();
        private long timeoutMillis;
        private MessageHandler messageHandler;
        private SnapshotStateMachine snapshotStateMachine;
        private RaftTransportClient raftClient;
        private LogStore logStore;
        private PersistentStateStore persistentStateStore = new InMemoryPersistentStateStore();
        private TimeSource timeSource = System::currentTimeMillis;
        private Random random = new Random();

        private Builder(Peer me) {
            this.me = me;
        }

        public Builder withPeers(List<Peer> peers) {
            this.peers = peers == null ? List.of() : List.copyOf(peers);
            return this;
        }

        public Builder withTimeoutMillis(long timeoutMillis) {
            this.timeoutMillis = timeoutMillis;
            return this;
        }

        public Builder withMessageHandler(MessageHandler messageHandler) {
            this.messageHandler = messageHandler;
            return this;
        }

        public Builder withSnapshotStateMachine(SnapshotStateMachine snapshotStateMachine) {
            this.snapshotStateMachine = snapshotStateMachine;
            return this;
        }

        public Builder withCommandHandler(CommandHandler commandHandler) {
            this.snapshotStateMachine = commandHandler == null ? null : new CommandHandlerStateMachineAdapter(commandHandler);
            return this;
        }

        public Builder withClient(RaftTransportClient raftClient) {
            this.raftClient = raftClient;
            return this;
        }

        public Builder withLogStore(LogStore logStore) {
            this.logStore = logStore;
            return this;
        }

        public Builder withPersistentStateStore(PersistentStateStore persistentStateStore) {
            this.persistentStateStore = persistentStateStore;
            return this;
        }

        public Builder withTimeSource(TimeSource timeSource) {
            this.timeSource = timeSource;
            return this;
        }

        public Builder withRandom(Random random) {
            this.random = random;
            return this;
        }

        public RaftNode build() {
            if (raftClient == null) {
                throw new IllegalArgumentException("Raft node requires a transport client");
            }
            if (logStore == null) {
                throw new IllegalArgumentException("Raft node requires a log store");
            }
            if (persistentStateStore == null) {
                throw new IllegalArgumentException("Raft node requires a persistent state store");
            }
            if (timeSource == null) {
                throw new IllegalArgumentException("Raft node requires a time source");
            }
            if (random == null) {
                throw new IllegalArgumentException("Raft node requires a random source");
            }
            return new RaftNode(this);
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

    private void registerPeer(Peer peer) {
        if (peer == null) {
            return;
        }
        if (peer.getId() == null || peer.getId().isBlank()) {
            throw new IllegalArgumentException("Cluster peer must have non-null, non-blank id");
        }
        if (me.getId().equals(peer.getId())) {
            if (!Objects.equals(me.getAddress(), peer.getAddress())) {
                throw new IllegalArgumentException("Conflicting peer configuration for id " + peer.getId() + ": " + me.getAddress() + " vs " + peer.getAddress());
            }
            return;
        }
        Peer existing = peers.get(peer.getId());
        if (existing != null && !Objects.equals(existing.getAddress(), peer.getAddress())) {
            throw new IllegalArgumentException("Conflicting peer configuration for id " + peer.getId() + ": " + existing.getAddress() + " vs " + peer.getAddress());
        }
        peers.putIfAbsent(peer.getId(), peer);
    }

    private List<Peer> allConfiguredMembers() {
        List<Peer> members = new java.util.ArrayList<>();
        members.add(me);
        members.addAll(peers.values());
        return members;
    }

    private List<Peer> remoteVotingPeers() {
        return remoteVotingPeersFor(clusterConfiguration);
    }

    private List<Peer> remoteVotingPeersFor(ClusterConfiguration configuration) {
        return configuration.allVotingMembers().stream()
                .filter(peer -> !me.getId().equals(peer.getId()))
                .toList();
    }

    private List<Peer> remoteReplicatedPeersFor(ClusterConfiguration configuration) {
        return configuration.allMembers().stream()
                .filter(peer -> !me.getId().equals(peer.getId()))
                .toList();
    }

    private ClusterConfiguration latestKnownConfiguration() {
        return configurationAt(logStore.lastIndex());
    }

    private ClusterConfiguration activeConfiguration() {
        // Runtime admission decisions use committed configuration, not speculative
        // configuration entries that may already exist in the local uncommitted log.
        return clusterConfiguration;
    }

    private ClusterConfiguration configurationAt(long index) {
        if (index <= logStore.snapshotIndex()) {
            return snapshotConfiguration;
        }
        // Reconstruct membership at an arbitrary log index so leader commit checks
        // can use the configuration that was actually in force for that entry.
        ClusterConfiguration configuration = snapshotConfiguration;
        long start = Math.max(logStore.snapshotIndex() + 1, 1);
        long end = Math.min(index, logStore.lastIndex());
        for (long i = start; i <= end; i++) {
            LogEntry entry = logStore.entryAt(i);
            configuration = applyConfigurationCommand(configuration, entry.getData(), false);
        }
        return configuration;
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

    private static final class PendingSnapshot {
        private final String leaderId;
        private final long lastIncludedIndex;
        private final long lastIncludedTerm;
        // Buffer grows chunk-by-chunk until the sender marks the final InstallSnapshot RPC.
        private final ByteArrayOutputStream data = new ByteArrayOutputStream();

        private PendingSnapshot(String leaderId, long lastIncludedIndex, long lastIncludedTerm) {
            this.leaderId = leaderId;
            this.lastIncludedIndex = lastIncludedIndex;
            this.lastIncludedTerm = lastIncludedTerm;
        }
    }

    public record JoinStatus(boolean success, String status, String message) {}
    public record TelemetrySnapshot(
            long observedAtMillis,
            long term,
            String peerId,
            String state,
            String leaderId,
            String votedFor,
            boolean joining,
            boolean decommissioned,
            long commitIndex,
            long lastApplied,
            long lastLogIndex,
            long lastLogTerm,
            long snapshotIndex,
            long snapshotTerm,
            long lastHeartbeatMillis,
            long nextElectionDeadlineMillis,
            ClusterConfiguration configuration,
            ClusterConfiguration latestKnownConfiguration,
            long configurationTransitionStartedMillis,
            List<Peer> knownPeers,
            List<String> pendingJoinIds,
            List<TelemetryReplicationStatus> replication
    ) {}

    public void shutdown() {
        raftClient.shutdown();
    }

    public void setDecommissionListener(Runnable decommissionListener) {
        this.decommissionListener = decommissionListener == null ? () -> {} : decommissionListener;
    }

    public RaftTransportClient getRaftClient() {
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

    public MessageHandler getMessageHandler() {
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
        ClusterConfiguration effectiveConfiguration = activeConfiguration();

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
            knownLeaderId = null;
            persistVotedFor(null);
            electionSequenceCounter = 0;
            refreshTimeout();
        }

        if (joining || !effectiveConfiguration.isVoter(me.getId())) {
            return new VoteResponse(req, me.getId(), false, currentTerm);
        }

        if (!effectiveConfiguration.isVoter(req.getCandidateId())) {
            log.debug("{}@{} rejects vote for {}: candidate is not part of active configuration",
                    me.getId(), currentTerm, req.getCandidateId());
            return new VoteResponse(req, me.getId(), false, currentTerm);
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
            Peer peer = resolvePeerById(req.getCandidateId());
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

    public synchronized boolean submitCommand(byte[] command) {
        // Figure 2 ("Leaders"): only the leader accepts client commands into the log.
        if (state != State.LEADER || decommissioned) {
            return false;
        }
        if (command == null || command.length == 0) {
            return false;
        }

        LogEntry entry = new LogEntry(currentTerm, me.getId(), command);
        logStore.append(Collections.singletonList(entry));
        if (log.isTraceEnabled()) {
            log.trace("{}@{} appended client command locally at index {} (bytes={})",
                    me.getId(), currentTerm, logStore.lastIndex(), command.length);
        }

        // Figure 2 ("Leaders"): after local append, replicate to followers and eventually commit.
        advanceCommitIndexFromMajority();
        broadcastHeartbeat(); // replicate promptly instead of waiting for next tick
        return true;
    }

    public synchronized boolean submitJointConfigurationChange(List<Peer> proposedMembers) {
        if (state != State.LEADER || decommissioned || proposedMembers == null || proposedMembers.isEmpty()) {
            return false;
        }
        if (clusterConfiguration.isJointConsensus()) {
            return false;
        }

        List<Peer> normalized = new java.util.ArrayList<>();
        for (Peer peer : proposedMembers) {
            if (peer == null) {
                continue;
            }
            registerPeer(peer);
            normalized.add(canonicalPeer(peer));
        }
        ClusterConfiguration proposedConfiguration = clusterConfiguration.transitionTo(normalized);
        if (clusterConfiguration.sameMembershipAs(proposedConfiguration)) {
            return true;
        }
        if (log.isDebugEnabled()) {
            log.debug("{}@{} scheduling joint-configuration transition: {} -> {}",
                    me.getId(), currentTerm, describeConfiguration(clusterConfiguration), describeConfiguration(proposedConfiguration));
        }
        configurationTransitionStartedMillis = timeSource.nowMillis();
        return submitInternalCommand(ClusterConfigurationCommand.joint(normalized));
    }

    public synchronized boolean submitJoinConfigurationChange(Peer proposedMember) {
        if (state != State.LEADER || decommissioned || proposedMember == null) {
            return false;
        }
        registerPeer(proposedMember);
        Peer resolved = resolvePeerById(proposedMember.getId());
        if (resolved == null) {
            return false;
        }
        if (!clusterConfiguration.isJointConsensus() && clusterConfiguration.contains(resolved.getId())) {
            pendingJoinIds.remove(resolved.getId());
            if (log.isDebugEnabled()) {
                log.debug("{}@{} join request for {} ignored: already part of stable configuration {}",
                        me.getId(), currentTerm, resolved.getId(), describeConfiguration(clusterConfiguration));
            }
            return true;
        }
        if (clusterConfiguration.isJointConsensus()) {
            return false;
        }

        List<Peer> finalMembers = new java.util.ArrayList<>(clusterConfiguration.allMembers());
        if (!clusterConfiguration.contains(resolved.getId())) {
            finalMembers.add(resolved);
        }
        if (!submitJointConfigurationChange(finalMembers)) {
            return false;
        }
        pendingJoinIds.add(resolved.getId());
        pendingAutoFinalizeMembers = finalMembers.stream().map(Peer::getId).toList();
        pendingAutoFinalizeFenceIndex = logStore.lastIndex();
        if (log.isDebugEnabled()) {
            log.debug("{}@{} accepted join request for {} and waiting for joint commit/finalize at log index {}",
                    me.getId(), currentTerm, resolved.getId(), pendingAutoFinalizeFenceIndex);
        }
        return true;
    }

    public synchronized boolean submitFinalizeConfigurationChange() {
        if (state != State.LEADER || decommissioned || !clusterConfiguration.isJointConsensus()) {
            return false;
        }
        if (log.isDebugEnabled()) {
            log.debug("{}@{} scheduling finalize-configuration transition from {}",
                    me.getId(), currentTerm, describeConfiguration(clusterConfiguration));
        }
        clearPendingAutoFinalize();
        return submitInternalCommand(ClusterConfigurationCommand.finalizeTransition());
    }

    public synchronized boolean submitPromoteLearnerChange(Peer peer) {
        return submitRoleChange(peer, Peer.Role.VOTER);
    }

    public synchronized boolean submitDemoteVoterChange(Peer peer) {
        return submitRoleChange(peer, Peer.Role.LEARNER);
    }

    private boolean submitRoleChange(Peer peer, Peer.Role targetRole) {
        if (state != State.LEADER || decommissioned || peer == null || targetRole == null) {
            return false;
        }
        if (clusterConfiguration.isJointConsensus()) {
            return false;
        }
        registerPeer(peer);
        Peer resolved = canonicalPeer(peer);
        if (resolved == null || !clusterConfiguration.contains(peer.getId())) {
            return false;
        }
        if (resolved.getRole() == targetRole) {
            return true;
        }
        List<Peer> finalMembers = new java.util.ArrayList<>();
        for (Peer member : clusterConfiguration.allMembers()) {
            if (member.getId().equals(peer.getId())) {
                finalMembers.add(new Peer(member.getId(), member.getAddress(), targetRole));
            } else {
                finalMembers.add(member);
            }
        }
        if (log.isDebugEnabled()) {
            log.debug("{}@{} scheduling role change for {}: {} -> {}",
                    me.getId(), currentTerm, resolved.getId(), resolved.getRole(), targetRole);
        }
        if (!submitJointConfigurationChange(finalMembers)) {
            return false;
        }
        pendingAutoFinalizeMembers = finalMembers.stream().map(Peer::getId).toList();
        pendingAutoFinalizeFenceIndex = logStore.lastIndex();
        return true;
    }

    private boolean submitInternalCommand(byte[] command) {
        LogEntry entry = new LogEntry(currentTerm, me.getId(), command);
        logStore.append(Collections.singletonList(entry));
        if (log.isDebugEnabled()) {
            log.debug("{}@{} appended {} at log index {}",
                    me.getId(), currentTerm, describeInternalCommand(command), logStore.lastIndex());
        }
        advanceCommitIndexFromMajority();
        broadcastHeartbeat();
        return true;
    }

    public synchronized AppendEntriesResponse handleAppendEntries(AppendEntriesRequest request) {
        long leaderTerm = request.getTerm();
        String leaderId = request.getLeaderId();
        List<LogEntry> entries = request.getEntries();
        long firstIncomingIndex = request.getPrevLogIndex() + 1;
        if (log.isTraceEnabled() && !entries.isEmpty()) {
            log.trace("{}@{} received AppendEntries from {}@{} carrying {} (prev={}/{}, leaderCommit={})",
                    me.getId(), currentTerm, leaderId, leaderTerm,
                    describeEntryRange(firstIncomingIndex, entries),
                    request.getPrevLogIndex(), request.getPrevLogTerm(), request.getLeaderCommit());
        }

        // Figure 2 AppendEntries receiver step 1: reject stale leader term.
        if (leaderTerm < currentTerm) {
            if (log.isTraceEnabled() && !entries.isEmpty()) {
                log.trace("{}@{} rejected AppendEntries from {}@{}: stale term while carrying {}",
                        me.getId(), currentTerm, leaderId, leaderTerm, describeEntryRange(firstIncomingIndex, entries));
            }
            return new AppendEntriesResponse(currentTerm, me.getId(), false, logStore.lastIndex());
        }
        ClusterConfiguration effectiveConfiguration = activeConfiguration();
        if (!acceptsLeaderRpcFrom(leaderId, effectiveConfiguration)) {
            if (log.isTraceEnabled() && !entries.isEmpty()) {
                log.trace("{}@{} rejected AppendEntries from {}@{}: leader not accepted in {}",
                        me.getId(), currentTerm, leaderId, leaderTerm, describeConfiguration(effectiveConfiguration));
            }
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
        knownLeaderId = leaderId;
        refreshTimeout();

        long prevLogIndex = request.getPrevLogIndex();
        long prevLogTerm = request.getPrevLogTerm();

        // Snapshot boundary handling: if follower already compacted this prefix,
        // leader must switch to InstallSnapshot (instead of AppendEntries retry).
        if (prevLogIndex < logStore.snapshotIndex()) {
            if (log.isTraceEnabled()) {
                log.trace("{}@{} rejected AppendEntries from {}@{}: prev index {} is below snapshot boundary {}",
                        me.getId(), currentTerm, leaderId, leaderTerm, prevLogIndex, logStore.snapshotIndex());
            }
            return new AppendEntriesResponse(currentTerm, me.getId(), false, logStore.snapshotIndex());
        }

        // Figure 2 AppendEntries receiver step 2:
        // reject if no entry at prevLogIndex or term mismatch.
        if (prevLogIndex > logStore.lastIndex()) {
            if (log.isTraceEnabled()) {
                log.trace("{}@{} rejected AppendEntries from {}@{}: missing prev index {} (lastIndex={})",
                        me.getId(), currentTerm, leaderId, leaderTerm, prevLogIndex, logStore.lastIndex());
            }
            return new AppendEntriesResponse(currentTerm, me.getId(), false, logStore.lastIndex());
        }
        if (prevLogIndex > 0 && logStore.termAt(prevLogIndex) != prevLogTerm) {
            if (log.isTraceEnabled()) {
                log.trace("{}@{} rejected AppendEntries from {}@{}: prev term mismatch at index {} (expected {}, local {})",
                        me.getId(), currentTerm, leaderId, leaderTerm, prevLogIndex, prevLogTerm, logStore.termAt(prevLogIndex));
            }
            return new AppendEntriesResponse(currentTerm, me.getId(), false, prevLogIndex - 1);
        }

        long index = prevLogIndex + 1;

        // Figure 2 AppendEntries receiver step 3:
        // delete conflicting existing entry and all that follow.
        int i = 0;
        while (i < entries.size()) {
            LogEntry incoming = entries.get(i);
            if (index <= logStore.lastIndex()) {
                LogEntry local = logStore.entryAt(index);
                if (!sameEntry(local, incoming)) {
                    if (log.isTraceEnabled()) {
                        log.trace("{}@{} truncating local log from index {} before applying entry from {}@{} (incomingTerm={}, localTerm={})",
                                me.getId(), currentTerm, index, leaderId, leaderTerm, incoming.getTerm(), local.getTerm());
                    }
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
            if (log.isTraceEnabled()) {
                log.trace("{}@{} appended {} from leader {}@{}",
                        me.getId(), currentTerm,
                        describeEntryRange(prevLogIndex + 1 + i, entries.subList(i, entries.size())),
                        leaderId, leaderTerm);
            }
        }

        // Figure 2 AppendEntries receiver step 5:
        // commit up to min(leaderCommit, lastIndex), then apply in-order.
        if (request.getLeaderCommit() > commitIndex) {
            long previousCommitIndex = commitIndex;
            commitIndex = Math.min(request.getLeaderCommit(), logStore.lastIndex());
            if (log.isTraceEnabled() && commitIndex > previousCommitIndex) {
                log.trace("{}@{} advanced commit index from {} to {} from leader {}@{}",
                        me.getId(), currentTerm, previousCommitIndex, commitIndex, leaderId, leaderTerm);
            }
            applyCommittedEntries();
        }
        return new AppendEntriesResponse(currentTerm, me.getId(), true, logStore.lastIndex());
    }

    public synchronized InstallSnapshotResponse handleInstallSnapshot(InstallSnapshotRequest request) {
        long leaderTerm = request.getTerm();
        String leaderId = request.getLeaderId();
        if (log.isTraceEnabled()) {
            log.trace("{}@{} received InstallSnapshot chunk from {}@{} for snapshot {}/{} (offset={}, bytes={}, done={})",
                    me.getId(), currentTerm, leaderId, leaderTerm,
                    request.getLastIncludedIndex(), request.getLastIncludedTerm(),
                    request.getOffset(), request.getSnapshotData().length, request.isDone());
        }

        // Same term gate as AppendEntries receiver step 1 (Figure 2 semantics).
        if (leaderTerm < currentTerm) {
            return new InstallSnapshotResponse(currentTerm, me.getId(), false, logStore.snapshotIndex());
        }
        ClusterConfiguration effectiveConfiguration = activeConfiguration();
        if (!acceptsLeaderRpcFrom(leaderId, effectiveConfiguration)) {
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
        knownLeaderId = leaderId;

        if (!acceptSnapshotChunk(request)) {
            return new InstallSnapshotResponse(currentTerm, me.getId(), false, logStore.snapshotIndex());
        }
        if (!request.isDone()) {
            // Followers acknowledge each chunk so the leader can continue streaming at the next offset.
            refreshTimeout();
            return new InstallSnapshotResponse(currentTerm, me.getId(), true, request.getLastIncludedIndex());
        }

        byte[] snapshotBytes = pendingSnapshot == null ? request.getSnapshotData() : pendingSnapshot.data.toByteArray();
        pendingSnapshot = null;

        // Snapshot install replaces compacted prefix and restores state machine checkpoint.
        // This is the mechanism that preserves catch-up when entries are no longer available.
        logStore.installSnapshot(
                request.getLastIncludedIndex(),
                request.getLastIncludedTerm(),
                snapshotBytes
        );
        if (log.isDebugEnabled()) {
            log.debug("{}@{} installed snapshot from {}@{} at index {} term {} (bytes={})",
                    me.getId(), currentTerm, leaderId, leaderTerm,
                    request.getLastIncludedIndex(), request.getLastIncludedTerm(), snapshotBytes.length);
        }
        byte[] stateMachineSnapshot = snapshotBytes;
        var decodedSnapshot = ClusterConfigurationSnapshotCodec.decode(snapshotBytes);
        if (decodedSnapshot.isPresent()) {
            snapshotConfiguration = decodedSnapshot.get().configuration();
            clusterConfiguration = snapshotConfiguration;
            if (clusterConfiguration.contains(me.getId())) {
                updateDecommissioned(true);
                joining = false;
            } else {
                markDecommissioned();
            }
            committedConfigurations.headMap(request.getLastIncludedIndex(), true).clear();
            committedConfigurations.put(request.getLastIncludedIndex(), snapshotConfiguration);
            stateMachineSnapshot = decodedSnapshot.get().stateMachineSnapshot();
            raftClient.setKnownPeers(remoteReplicatedPeersFor(latestKnownConfiguration()));
        }
        if (snapshotStateMachine != null) {
            snapshotStateMachine.restore(stateMachineSnapshot);
        }
        commitIndex = Math.max(commitIndex, request.getLastIncludedIndex());
        lastApplied = Math.max(lastApplied, request.getLastIncludedIndex());
        refreshTimeout();
        return new InstallSnapshotResponse(currentTerm, me.getId(), true, logStore.snapshotIndex());
    }

    private boolean acceptSnapshotChunk(InstallSnapshotRequest request) {
        if (request.getOffset() == 0
                || pendingSnapshot == null
                || !Objects.equals(pendingSnapshot.leaderId, request.getLeaderId())
                || pendingSnapshot.lastIncludedIndex != request.getLastIncludedIndex()
                || pendingSnapshot.lastIncludedTerm != request.getLastIncludedTerm()) {
            // A new stream starts at offset 0 or whenever the sender/metadata changes.
            pendingSnapshot = new PendingSnapshot(request.getLeaderId(), request.getLastIncludedIndex(), request.getLastIncludedTerm());
        }

        if (request.getOffset() != pendingSnapshot.data.size()) {
            // Reject out-of-order or overlapping chunks so followers never install a corrupted snapshot.
            log.warn("{}@{} rejected snapshot chunk from {}: expected offset {}, got {}",
                    me.getId(), currentTerm, request.getLeaderId(), pendingSnapshot.data.size(), request.getOffset());
            return false;
        }
        pendingSnapshot.data.writeBytes(request.getSnapshotData());
        if (log.isTraceEnabled()) {
            log.trace("{}@{} accepted snapshot chunk from {} for snapshot index {} at offset {} (bytes={}, buffered={})",
                    me.getId(), currentTerm, request.getLeaderId(), request.getLastIncludedIndex(),
                    request.getOffset(), request.getSnapshotData().length, pendingSnapshot.data.size());
        }
        return true;
    }

    private boolean sameEntry(LogEntry left, LogEntry right) {
        return left.getTerm() == right.getTerm()
                && java.util.Objects.equals(left.getPeerId(), right.getPeerId())
                && Arrays.equals(left.getData(), right.getData());
    }

    public void startTimers(ScheduledExecutorService scheduler) {
        // Periodically check election timeout:
        scheduler.scheduleAtFixedRate(this::checkTimeout, timeoutMillis, timeoutMillis, TimeUnit.MILLISECONDS);

        // Periodically send heartbeat if leader:
        scheduler.scheduleAtFixedRate(this::broadcastHeartbeat, 1, 1, TimeUnit.SECONDS);
    }

    private synchronized void checkTimeout() {
        if (joining) {
            state = State.FOLLOWER;
            persistVotedFor(null);
            refreshTimeout();
            return;
        }
        if (!activeConfiguration().isVoter(me.getId())) {
            if (!activeConfiguration().contains(me.getId())) {
                markDecommissioned();
            } else {
                updateDecommissioned(true);
            }
            if (state != State.FOLLOWER) {
                state = State.FOLLOWER;
                persistVotedFor(null);
            }
            refreshTimeout();
            return;
        }
        updateDecommissioned(true);
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

        if (!activeConfiguration().isVoter(me.getId())) {
            if (!activeConfiguration().contains(me.getId())) {
                markDecommissioned();
            }
            log.debug("{}@{} will not start election because it is not a voter in the active configuration", me.getId(), currentTerm);
            return;
        }
        updateDecommissioned(true);

        synchronized (this) {
            // Figure 2 ("Candidates"): on conversion to candidate:
            // increment term, vote for self, reset election timer.
            state = State.CANDIDATE;
            knownLeaderId = null;
            persistCurrentTerm(currentTerm + 1);
            persistVotedFor(me);
            electionSequenceCounter++;
            refreshTimeout();
        }

        long lastIndex = logStore.lastIndex();
        long lastTerm  = logStore.lastTerm();

        VoteRequest voteReq = new VoteRequest(currentTerm, me.getId(), lastIndex, lastTerm);
        log.trace("{}@{} ({}) voting for myself for term {}", me.getId(), currentTerm, state.name(), voteReq.getTerm());
        raftClient.requestVoteFromAll(remoteVotingPeersFor(activeConfiguration()), voteReq)
                .whenComplete((responses, error) -> {
                    if (error == null) {
                        log.trace("{}@{} received responses for term {}", me.getId(), currentTerm, voteReq.getTerm());
                        handleVoteResponses(responses, voteReq.getTerm());
                    } else {
                        log.info("{}@{} no vote response", me.getId(), currentTerm, error);
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
            // Figure 2 ("All Servers"): seeing a higher term in any RPC response
            // forces term update and follower transition immediately.
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

            // Ascertain that this response emanates from a request we sent in the
            // current term. We may receive responses from earlier election rounds
            // and these should be discarded for vote-counting/leadership transition.
            if (currentTerm == electionTerm) {
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
                            knownLeaderId = null;
                            electionSequenceCounter = 0;
                            persistVotedFor(null);
                            refreshTimeout();
                            return;
                        }
                    }
                }

                // Let's see if we got a majority
                java.util.LinkedHashSet<String> voters = new java.util.LinkedHashSet<>();
                voters.add(me.getId());
                for (VoteResponse response : responses) {
                    if (response.isVoteGranted()) {
                        voters.add(response.getPeerId());
                    }
                }

                ClusterConfiguration electionConfiguration = activeConfiguration();
                if (electionConfiguration.hasJointMajority(voters)) {
                    //--------------------------------------------
                    // b) receives votes from majority of nodes
                    //--------------------------------------------
                    if (log.isInfoEnabled()) {
                        log.info("{}@{} elected LEADER after quorum from {}", me.getId(), currentTerm, voters);
                    }
                    state = State.LEADER;
                    knownLeaderId = me.getId();
                    electionSequenceCounter = 0;

                    // Figure 2 ("Leaders"): initialize replication state and begin heartbeat/append flow.
                    initializeLeaderReplicationState();
                    appendLeaderNoopEntry();
                    advanceCommitIndexFromMajority();
                    broadcastHeartbeat();
                }
            }
        }
    }

    private synchronized void broadcastHeartbeat() {
        if (state == State.LEADER) {
            // Figure 2 ("Leaders"):
            // - send empty AppendEntries as heartbeat
            // - send log entries starting at each follower's nextIndex
            for (Peer peer : remoteReplicatedPeersFor(latestKnownConfiguration())) {
                long configuredNext = nextIndex.getOrDefault(peer.getId(), logStore.lastIndex() + 1);
                if (logStore.snapshotIndex() > 0 && configuredNext <= logStore.snapshotIndex()) {
                    // Follower is behind compacted prefix; switch to InstallSnapshot streaming.
                    if (log.isTraceEnabled()) {
                        log.trace("{}@{} switching replication for {} to InstallSnapshot because nextIndex {} is at/before snapshot index {}",
                                me.getId(), currentTerm, peer.getId(), configuredNext, logStore.snapshotIndex());
                    }
                    sendInstallSnapshotChunk(peer);
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
                if (log.isTraceEnabled() && !entries.isEmpty()) {
                    log.trace("{}@{} sending AppendEntries to {} with {} (prev={}/{}, leaderCommit={})",
                            me.getId(), currentTerm, peer.getId(),
                            describeEntryRange(next, entries), prev, prevTerm, commitIndex);
                }
                raftClient.sendAppendEntries(peer, req).whenComplete((resp, error) -> {
                    if (error != null || resp == null) {
                        recordFollowerReplicationFailure(peer.getId());
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
            knownLeaderId = null;
            clearPendingAutoFinalize();
            persistVotedFor(null);
            electionSequenceCounter = 0;
            refreshTimeout();
            return;
        }

        if (!response.isSuccess()) {
            if (response.getMatchIndex() < 0) {
                recordFollowerReplicationFailure(peer.getId());
                return;
            }
            // Figure 2 ("Leaders"): on inconsistency, decrement nextIndex and retry.
            long currentNext = nextIndex.getOrDefault(peer.getId(), logStore.lastIndex() + 1);
            if (logStore.snapshotIndex() > 0 && response.getMatchIndex() <= logStore.snapshotIndex()) {
                nextIndex.put(peer.getId(), logStore.snapshotIndex());
                if (log.isTraceEnabled()) {
                    log.trace("{}@{} follower {} rejected {} and must receive snapshot catch-up (matchIndex={}, snapshotIndex={})",
                            me.getId(), currentTerm, peer.getId(),
                            describeEntryRange(request.getPrevLogIndex() + 1, request.getEntries()),
                            response.getMatchIndex(), logStore.snapshotIndex());
                }
                return;
            }
            long floor = logStore.snapshotIndex() + 1;
            nextIndex.put(peer.getId(), Math.max(floor, currentNext - 1));
            if (log.isTraceEnabled()) {
                log.trace("{}@{} follower {} rejected {} so nextIndex backs off from {} to {} (matchIndex={})",
                        me.getId(), currentTerm, peer.getId(),
                        describeEntryRange(request.getPrevLogIndex() + 1, request.getEntries()),
                        currentNext, nextIndex.get(peer.getId()), response.getMatchIndex());
            }
            return;
        }

        // Response success implies follower accepted the sent suffix;
        // advance indexes based on exactly what this request carried.
        long advanced = request.getPrevLogIndex() + request.getEntries().size();
        nextIndex.put(peer.getId(), advanced + 1);
        matchIndex.put(peer.getId(), advanced);
        recordFollowerReplicationSuccess(peer.getId());
        if (log.isTraceEnabled() && !request.getEntries().isEmpty()) {
            log.trace("{}@{} follower {} accepted {} -> matchIndex={}, nextIndex={}",
                    me.getId(), currentTerm, peer.getId(),
                    describeEntryRange(request.getPrevLogIndex() + 1, request.getEntries()),
                    advanced, advanced + 1);
        }

        // Figure 2 ("Leaders"): if replicated on majority and in current term, advance commitIndex.
        advanceCommitIndexFromMajority();
        maybeAutoFinalizeJointConfiguration();
    }

    private synchronized void handleInstallSnapshotResponse(Peer peer, InstallSnapshotRequest request, InstallSnapshotResponse response) {
        if (state != State.LEADER) {
            return;
        }

        if (response.getTerm() > currentTerm) {
            persistCurrentTerm(response.getTerm());
            state = State.FOLLOWER;
            knownLeaderId = null;
            clearPendingAutoFinalize();
            persistVotedFor(null);
            electionSequenceCounter = 0;
            refreshTimeout();
            return;
        }

        if (!response.isSuccess()) {
            recordFollowerReplicationFailure(peer.getId());
            if (log.isTraceEnabled()) {
                log.trace("{}@{} follower {} rejected InstallSnapshot chunk for snapshot {}/{} at offset {}",
                        me.getId(), currentTerm, peer.getId(),
                        request.getLastIncludedIndex(), request.getLastIncludedTerm(), request.getOffset());
            }
            return;
        }

        if (!request.isDone()) {
            // Successful chunk ack advances the sender-side cursor for this follower.
            snapshotOffsets.put(peer.getId(), request.getOffset() + request.getSnapshotData().length);
            if (log.isTraceEnabled()) {
                log.trace("{}@{} follower {} acknowledged InstallSnapshot chunk for snapshot {} up to offset {}",
                        me.getId(), currentTerm, peer.getId(),
                        request.getLastIncludedIndex(), snapshotOffsets.get(peer.getId()));
            }
            sendInstallSnapshotChunk(peer);
            return;
        }

        long index = request.getLastIncludedIndex();
        snapshotOffsets.remove(peer.getId());
        nextIndex.put(peer.getId(), index + 1);
        matchIndex.put(peer.getId(), index);
        recordFollowerReplicationSuccess(peer.getId());
        if (log.isDebugEnabled()) {
            log.debug("{}@{} follower {} installed snapshot through index {} term {}",
                    me.getId(), currentTerm, peer.getId(), index, request.getLastIncludedTerm());
        }

        // After snapshot catch-up, follower contributes to majority replication accounting.
        advanceCommitIndexFromMajority();
        maybeAutoFinalizeJointConfiguration();
    }

    private synchronized void sendInstallSnapshotChunk(Peer peer) {
        byte[] snapshotBytes = logStore.snapshotData();
        if (snapshotBytes.length == 0 && snapshotStateMachine != null) {
            snapshotBytes = snapshotStateMachine.snapshot();
        }

        // Each follower can be at a different snapshot offset while catching up.
        long offset = snapshotOffsets.getOrDefault(peer.getId(), 0L);
        if (offset > snapshotBytes.length) {
            offset = 0L;
            snapshotOffsets.put(peer.getId(), 0L);
        }

        int chunkSize = Math.max(1, snapshotChunkBytes);
        int start = (int) offset;
        int end = Math.min(snapshotBytes.length, start + chunkSize);
        boolean done = end >= snapshotBytes.length;
        byte[] chunk = Arrays.copyOfRange(snapshotBytes, start, end);

        InstallSnapshotRequest installSnapshotRequest = new InstallSnapshotRequest(
                currentTerm,
                me.getId(),
                logStore.snapshotIndex(),
                logStore.snapshotTerm(),
                offset,
                chunk,
                done
        );
        if (log.isTraceEnabled()) {
            log.trace("{}@{} sending InstallSnapshot chunk to {} for snapshot {}/{} (offset={}, bytes={}, done={})",
                    me.getId(), currentTerm, peer.getId(),
                    logStore.snapshotIndex(), logStore.snapshotTerm(),
                    offset, chunk.length, done);
        }
        raftClient.sendInstallSnapshot(peer, installSnapshotRequest).whenComplete((resp, error) -> {
            if (error != null || resp == null) {
                recordFollowerReplicationFailure(peer.getId());
                return;
            }
            handleInstallSnapshotResponse(peer, installSnapshotRequest, resp);
        });
    }

    private synchronized void initializeLeaderReplicationState() {
        long next = Math.max(logStore.snapshotIndex() + 1, logStore.lastIndex() + 1);
        nextIndex.clear();
        matchIndex.clear();
        consecutiveFollowerFailures.clear();
        lastFollowerFailureMillis.clear();
        lastFollowerContactMillis.clear();
        for (Peer peer : remoteReplicatedPeersFor(latestKnownConfiguration())) {
            nextIndex.put(peer.getId(), next);
            matchIndex.put(peer.getId(), logStore.snapshotIndex());
        }
    }

    private synchronized void appendLeaderNoopEntry() {
        // New leaders append a no-op entry in their own term so previously replicated
        // old-term entries can become committed once this term is established on a majority.
        logStore.append(Collections.singletonList(new LogEntry(currentTerm, me.getId(), new byte[0])));
        if (log.isTraceEnabled()) {
            log.trace("{}@{} appended leader no-op at index {}", me.getId(), currentTerm, logStore.lastIndex());
        }
    }

    private synchronized void advanceCommitIndexFromMajority() {
        long candidateCommit = commitIndex;
        java.util.Set<String> candidateReplicated = java.util.Set.of();

        // Figure 2 ("Leaders"): commit only entries from current term once stored on a majority.
        // This underpins Figure 3 safety (Leader Completeness + State Machine Safety).
        for (long n = commitIndex + 1; n <= logStore.lastIndex(); n++) {
            if (logStore.termAt(n) != currentTerm) {
                continue;
            }

            java.util.LinkedHashSet<String> replicated = new java.util.LinkedHashSet<>();
            replicated.add(me.getId()); // leader has it
            for (Peer peer : remoteVotingPeers()) {
                if (matchIndex.getOrDefault(peer.getId(), 0L) >= n) {
                    replicated.add(peer.getId());
                }
            }
            ClusterConfiguration effectiveConfiguration = configurationAt(n);
            if (effectiveConfiguration.hasJointMajority(replicated)) {
                candidateCommit = n;
                candidateReplicated = new java.util.LinkedHashSet<>(replicated);
            }
        }

        if (candidateCommit > commitIndex) {
            long previousCommitIndex = commitIndex;
            commitIndex = candidateCommit;
            if (log.isTraceEnabled()) {
                log.trace("{}@{} advanced leader commit index from {} to {} after quorum {}",
                        me.getId(), currentTerm, previousCommitIndex, candidateCommit, candidateReplicated);
            }

            // Figure 2 ("All Servers"): apply entries in commit order.
            applyCommittedEntries();
        }
    }

    private synchronized void applyCommittedEntries() {
        // Figure 2 ("All Servers"): if commitIndex > lastApplied, increment lastApplied and apply.
        // Figure 3 State Machine Safety: this monotonic, in-order apply sequence prevents divergent applies.
        while (lastApplied < commitIndex) {
            lastApplied++;
            LogEntry entry = logStore.entryAt(lastApplied);
            byte[] data = entry.getData();
            if (data.length == 0) {
                if (log.isTraceEnabled()) {
                    log.trace("{}@{} applied no-op entry at index {}", me.getId(), currentTerm, lastApplied);
                }
                continue;
            }

            if (applyConfigurationCommand(data)) {
                continue;
            }
            if (snapshotStateMachine == null) {
                continue;
            }
            if (log.isTraceEnabled()) {
                log.trace("{}@{} applying replicated command at index {} from {} term {} (bytes={})",
                        me.getId(), currentTerm, lastApplied, entry.getPeerId(), entry.getTerm(), data.length);
            }
            snapshotStateMachine.apply(entry.getTerm(), data);
        }
        maybeAutoFinalizeJointConfiguration();
        maybeCompactLocalSnapshot();
    }

    private synchronized boolean applyConfigurationCommand(byte[] command) {
        var parsed = ClusterConfigurationCommand.parse(command);
        if (parsed.isEmpty()) {
            return false;
        }

        clusterConfiguration = applyConfigurationCommand(clusterConfiguration, command, true);
        committedConfigurations.put(lastApplied, clusterConfiguration);
        if (log.isDebugEnabled()) {
            log.debug("{}@{} applied {} at log index {} -> {}",
                    me.getId(), currentTerm, describeInternalCommand(command), lastApplied, describeConfiguration(clusterConfiguration));
        }
        return true;
    }

    private ClusterConfiguration applyConfigurationCommand(ClusterConfiguration baseConfiguration, byte[] command, boolean updateTransport) {
        var parsed = ClusterConfigurationCommand.parse(command);
        if (parsed.isEmpty()) {
            return baseConfiguration;
        }
        ClusterConfigurationCommand.Parsed configurationCommand = parsed.get();
        ClusterConfiguration updated;
        switch (configurationCommand.type()) {
            case JOINT -> updated = applyJointConfiguration(baseConfiguration, configurationCommand.members(), updateTransport);
            case FINALIZE -> updated = applyFinalizedConfiguration(baseConfiguration, updateTransport);
            default -> throw new IllegalStateException("Unhandled configuration command " + configurationCommand.type());
        }
        return updated;
    }

    private ClusterConfiguration applyJointConfiguration(ClusterConfiguration baseConfiguration, List<Peer> proposedMembers, boolean updateTransport) {
        for (Peer peer : proposedMembers) {
            registerPeer(peer);
        }
        ClusterConfiguration updated = baseConfiguration.transitionTo(proposedMembers);
        if (updated.isJointConsensus() && !baseConfiguration.isJointConsensus() && configurationTransitionStartedMillis <= 0L) {
            configurationTransitionStartedMillis = timeSource.nowMillis();
        }
        if (updated.contains(me.getId())) {
            joining = false;
        }
        for (Peer peer : proposedMembers) {
            pendingJoinIds.remove(peer.getId());
        }
        if (updateTransport) {
            raftClient.setKnownPeers(remoteReplicatedPeersFor(updated));
        }
        if (updateTransport && state == State.LEADER) {
            long next = Math.max(logStore.snapshotIndex() + 1, logStore.lastIndex() + 1);
            for (Peer peer : remoteReplicatedPeersFor(updated)) {
                nextIndex.putIfAbsent(peer.getId(), next);
                matchIndex.putIfAbsent(peer.getId(), logStore.snapshotIndex());
            }
        }
        if (log.isDebugEnabled()) {
            log.debug("{}@{} entered joint configuration: {} -> {}",
                    me.getId(), currentTerm, describeConfiguration(baseConfiguration), describeConfiguration(updated));
        }
        return updated;
    }

    private ClusterConfiguration applyFinalizedConfiguration(ClusterConfiguration baseConfiguration, boolean updateTransport) {
        ClusterConfiguration updated = baseConfiguration.finalizeTransition();
        configurationTransitionStartedMillis = 0L;
        if (updated.contains(me.getId())) {
            joining = false;
        }
        for (Peer peer : updated.allMembers()) {
            pendingJoinIds.remove(peer.getId());
        }
        java.util.Set<String> activePeerIds = remoteReplicatedPeersFor(updated).stream()
                .map(Peer::getId)
                .collect(java.util.stream.Collectors.toSet());
        if (updateTransport) {
            // Once finalized, replication and transport state must forget removed members immediately.
            nextIndex.keySet().retainAll(activePeerIds);
            matchIndex.keySet().retainAll(activePeerIds);
            raftClient.setKnownPeers(remoteReplicatedPeersFor(updated));
            boolean localMember = updated.contains(me.getId());
            updateDecommissioned(localMember);
            if (!updated.contains(me.getId()) && state == State.LEADER) {
                // Commit of the finalized config is local at this point; send one final
                // AppendEntries round carrying the new leaderCommit before stepping down.
                broadcastHeartbeat();
                state = State.FOLLOWER;
                persistVotedFor(null);
                electionSequenceCounter = 0;
                refreshTimeout();
            }
            if (!localMember) {
                notifyDecommissionListener();
            }
        }
        clearPendingAutoFinalize();
        if (log.isDebugEnabled()) {
            log.debug("{}@{} finalized configuration: {} -> {}",
                    me.getId(), currentTerm, describeConfiguration(baseConfiguration), describeConfiguration(updated));
        }
        return updated;
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
        // snapshot state machine + committed cluster configuration + compacted log prefix.
        long lastIncludedIndex = commitIndex;
        long lastIncludedTerm = logStore.termAt(lastIncludedIndex);
        long previousSnapshotIndex = logStore.snapshotIndex();
        snapshotConfiguration = configurationAt(lastIncludedIndex);
        byte[] snapshotBytes = ClusterConfigurationSnapshotCodec.encode(snapshotConfiguration, snapshotStateMachine.snapshot());
        logStore.installSnapshot(lastIncludedIndex, lastIncludedTerm, snapshotBytes);
        if (log.isDebugEnabled()) {
            log.debug("{}@{} compacted local log into snapshot at index {} term {} (bytes={}, previousSnapshotIndex={})",
                    me.getId(), currentTerm, lastIncludedIndex, lastIncludedTerm, snapshotBytes.length, previousSnapshotIndex);
        }
        committedConfigurations.headMap(lastIncludedIndex, false).clear();
        committedConfigurations.put(lastIncludedIndex, snapshotConfiguration);
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

    ClusterConfiguration getClusterConfigurationForTest() {
        return clusterConfiguration;
    }

    boolean isDecommissionedForTest() {
        return decommissioned;
    }

    public Peer getPeerById(String peerId) {
        return resolvePeerById(peerId);
    }

    public synchronized void enableJoiningMode() {
        joining = true;
        state = State.FOLLOWER;
        knownLeaderId = null;
        persistVotedFor(null);
        electionSequenceCounter = 0;
        refreshTimeout();
        if (log.isDebugEnabled()) {
            log.debug("{}@{} entering join mode and waiting to be added to a cluster configuration", me.getId(), currentTerm);
        }
    }

    public synchronized boolean isJoining() {
        return joining;
    }

    public synchronized boolean isDecommissioned() {
        return decommissioned;
    }

    public synchronized Peer.Role getLocalMemberRole() {
        ClusterConfiguration effectiveConfiguration = activeConfiguration();
        if (effectiveConfiguration.isVoter(me.getId())) {
            return Peer.Role.VOTER;
        }
        if (effectiveConfiguration.contains(me.getId())) {
            return Peer.Role.LEARNER;
        }
        return null;
    }

    public synchronized Peer getKnownLeaderPeer() {
        if (state == State.LEADER) {
            return me;
        }
        return resolvePeerById(knownLeaderId);
    }

    public synchronized byte[] queryStateMachine(byte[] request) {
        if (!(snapshotStateMachine instanceof org.gautelis.raft.statemachine.QueryableStateMachine queryableStateMachine)) {
            return new byte[0];
        }
        return queryableStateMachine.query(request);
    }

    public synchronized boolean canServeLinearizableRead() {
        if (state != State.LEADER || decommissioned) {
            return false;
        }
        if (lastApplied < commitIndex) {
            return false;
        }

        // This is a lease-style fast path: the leader may answer immediately if
        // it has heard from a quorum of current voters recently enough.
        java.util.LinkedHashSet<String> freshVoters = new java.util.LinkedHashSet<>();
        freshVoters.add(me.getId());
        long now = timeSource.nowMillis();
        long freshnessCutoff = Math.max(0L, now - linearizableReadLeaseMillis);
        for (Peer peer : activeConfiguration().allVotingMembers()) {
            if (me.getId().equals(peer.getId())) {
                continue;
            }
            if (lastFollowerContactMillis.getOrDefault(peer.getId(), 0L) >= freshnessCutoff) {
                freshVoters.add(peer.getId());
            }
        }
        return activeConfiguration().hasJointMajority(freshVoters);
    }

    public boolean awaitLinearizableRead() {
        if (canServeLinearizableRead()) {
            return true;
        }

        final long termSnapshot;
        final long commitSnapshot;
        final long lastIndexSnapshot;
        final List<Peer> targets;
        synchronized (this) {
            if (state != State.LEADER || decommissioned) {
                return false;
            }
            // Freeze the leader/log view used for the read barrier so the
            // quorum check is tied to a single term and commit position.
            termSnapshot = currentTerm;
            commitSnapshot = commitIndex;
            lastIndexSnapshot = logStore.lastIndex();
            targets = List.copyOf(remoteVotingPeers());
            if (targets.isEmpty()) {
                return lastApplied >= commitIndex;
            }
        }

        java.util.concurrent.ConcurrentHashMap.KeySetView<String, Boolean> acknowledgements = java.util.concurrent.ConcurrentHashMap.newKeySet();
        acknowledgements.add(me.getId());
        java.util.concurrent.CountDownLatch done = new java.util.concurrent.CountDownLatch(targets.size());

        for (Peer peer : targets) {
            // An empty AppendEntries works as a read barrier: if a quorum
            // acknowledges it in this term, the leader can safely serve a
            // linearizable read without appending a dedicated log entry.
            AppendEntriesRequest req = new AppendEntriesRequest(
                    termSnapshot,
                    me.getId(),
                    lastIndexSnapshot,
                    logStore.termAt(lastIndexSnapshot),
                    commitSnapshot,
                    List.of()
            );
            raftClient.sendAppendEntries(peer, req).whenComplete((resp, error) -> {
                try {
                    synchronized (RaftNode.this) {
                        if (error == null && resp != null) {
                            handleAppendEntriesResponse(peer, req, resp);
                            if (state == State.LEADER && currentTerm == termSnapshot && resp.isSuccess()) {
                                acknowledgements.add(peer.getId());
                            }
                        } else {
                            recordFollowerReplicationFailure(peer.getId());
                        }
                    }
                } finally {
                    done.countDown();
                }
            });
        }

        // Use monotonic wall-clock time here instead of the node time source so
        // tests with a frozen logical clock still time out correctly.
        long deadlineNanos = System.nanoTime() + java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(linearizableReadTimeoutMillis);
        while (System.nanoTime() <= deadlineNanos) {
            synchronized (this) {
                if (activeConfiguration().hasJointMajority(acknowledgements) && canServeLinearizableRead()) {
                    return true;
                }
            }
            try {
                if (done.await(10L, java.util.concurrent.TimeUnit.MILLISECONDS)) {
                    synchronized (this) {
                        return activeConfiguration().hasJointMajority(acknowledgements) && canServeLinearizableRead();
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        synchronized (this) {
            return activeConfiguration().hasJointMajority(acknowledgements) && canServeLinearizableRead();
        }
    }

    public synchronized JoinStatus getJoinStatus(String peerId) {
        if (peerId == null || peerId.isBlank()) {
            return new JoinStatus(false, "INVALID", "Peer id must not be blank");
        }
        ClusterConfiguration active = activeConfiguration();
        if (!active.isJointConsensus() && active.contains(peerId)) {
            return new JoinStatus(true, "COMPLETED", "Peer is part of the finalized cluster configuration");
        }
        if (active.isJointConsensus() && active.contains(peerId)) {
            return new JoinStatus(true, "IN_JOINT_CONSENSUS", "Peer is present in the active joint configuration");
        }
        if (pendingJoinIds.contains(peerId)) {
            return new JoinStatus(true, "PENDING", "Join request accepted and awaiting joint configuration commit");
        }
        return new JoinStatus(false, "UNKNOWN", "No known join request or committed membership for peer");
    }

    public synchronized TelemetrySnapshot telemetrySnapshot() {
        List<Peer> knownPeers = new java.util.ArrayList<>(peers.values());
        knownPeers.sort(java.util.Comparator.comparing(Peer::getId));
        List<String> pendingJoins = new java.util.ArrayList<>(pendingJoinIds);
        java.util.Collections.sort(pendingJoins);
        List<TelemetryReplicationStatus> replication = new java.util.ArrayList<>();
        if (state == State.LEADER) {
            List<String> peerIds = new java.util.ArrayList<>(peers.keySet());
            java.util.Collections.sort(peerIds);
            for (String peerId : peerIds) {
                replication.add(new TelemetryReplicationStatus(
                        peerId,
                        nextIndex.getOrDefault(peerId, 0L),
                        matchIndex.getOrDefault(peerId, 0L),
                        raftClient.isPeerReachable(peerId),
                        lastFollowerContactMillis.getOrDefault(peerId, 0L),
                        consecutiveFollowerFailures.getOrDefault(peerId, 0),
                        lastFollowerFailureMillis.getOrDefault(peerId, 0L)
                ));
            }
        }
        return new TelemetrySnapshot(
                timeSource.nowMillis(),
                currentTerm,
                me.getId(),
                state.name(),
                knownLeaderId == null ? "" : knownLeaderId,
                votedFor == null ? "" : votedFor.getId(),
                joining,
                decommissioned,
                commitIndex,
                lastApplied,
                logStore.lastIndex(),
                logStore.lastTerm(),
                logStore.snapshotIndex(),
                logStore.snapshotTerm(),
                lastHeartbeat,
                nextElectionDeadlineMillis,
                clusterConfiguration,
                latestKnownConfiguration(),
                configurationTransitionStartedMillis,
                List.copyOf(knownPeers),
                List.copyOf(pendingJoins),
                List.copyOf(replication)
        );
    }

    ClusterConfiguration getConfigurationAtIndexForTest(long index) {
        return configurationAt(index);
    }

    void transitionToJointConfigurationForTest(List<Peer> proposedMembers) {
        clusterConfiguration = applyJointConfiguration(clusterConfiguration, proposedMembers, true);
    }

    void finalizeConfigurationTransitionForTest() {
        clusterConfiguration = applyFinalizedConfiguration(clusterConfiguration, true);
    }

    private void updateDecommissioned(boolean localMember) {
        decommissioned = !localMember;
    }

    private Peer canonicalPeer(Peer peer) {
        if (peer == null) {
            return null;
        }
        Peer existing = resolvePeerById(peer.getId());
        java.net.InetSocketAddress address = peer.getAddress() != null
                ? peer.getAddress()
                : (existing == null ? null : existing.getAddress());
        return new Peer(peer.getId(), address, peer.getRole());
    }

    private void recordFollowerReplicationSuccess(String peerId) {
        lastFollowerContactMillis.put(peerId, timeSource.nowMillis());
        consecutiveFollowerFailures.put(peerId, 0);
    }

    private void recordFollowerReplicationFailure(String peerId) {
        long now = timeSource.nowMillis();
        consecutiveFollowerFailures.merge(peerId, 1, Integer::sum);
        lastFollowerFailureMillis.put(peerId, now);
    }

    private boolean acceptsLeaderRpcFrom(String leaderId, ClusterConfiguration effectiveConfiguration) {
        if (leaderId == null || leaderId.equals(me.getId())) {
            return false;
        }
        if (joining) {
            return true;
        }
        return effectiveConfiguration.isVoter(leaderId);
    }

    private void clearPendingAutoFinalize() {
        pendingAutoFinalizeMembers = List.of();
        pendingAutoFinalizeFenceIndex = 0L;
    }

    private void maybeAutoFinalizeJointConfiguration() {
        if (state != State.LEADER || decommissioned || pendingAutoFinalizeMembers.isEmpty() || !clusterConfiguration.isJointConsensus()) {
            return;
        }
        long fenceIndex = pendingAutoFinalizeFenceIndex;
        if (fenceIndex <= 0 || commitIndex < fenceIndex) {
            return;
        }
        for (String memberId : pendingAutoFinalizeMembers) {
            if (me.getId().equals(memberId)) {
                continue;
            }
            if (matchIndex.getOrDefault(memberId, 0L) < fenceIndex) {
                return;
            }
        }
        if (log.isDebugEnabled()) {
            log.debug("{}@{} auto-finalizing joint configuration after all members replicated fence index {}: {}",
                    me.getId(), currentTerm, fenceIndex, pendingAutoFinalizeMembers);
        }
        clearPendingAutoFinalize();
        submitFinalizeConfigurationChange();
    }

    private String describeConfiguration(ClusterConfiguration configuration) {
        if (configuration == null) {
            return "<none>";
        }
        return configuration.allMembers().stream()
                .map(peer -> peer.getId() + ":" + peer.getRole())
                .sorted()
                .toList()
                + (configuration.isJointConsensus() ? " joint" : " stable");
    }

    private String describeInternalCommand(byte[] command) {
        var parsed = ClusterConfigurationCommand.parse(command);
        if (parsed.isEmpty()) {
            return "internal-command[unknown]";
        }
        ClusterConfigurationCommand.Parsed configurationCommand = parsed.get();
        return switch (configurationCommand.type()) {
            case JOINT -> "joint-configuration command " + configurationCommand.members().stream()
                    .map(peer -> peer.getId() + ":" + peer.getRole())
                    .sorted()
                    .toList();
            case FINALIZE -> "finalize-configuration command";
        };
    }

    private String describeEntryRange(long firstIndex, List<LogEntry> entries) {
        if (entries == null || entries.isEmpty()) {
            return "heartbeat";
        }
        long lastIndex = firstIndex + entries.size() - 1L;
        return "entries[" + firstIndex + ".." + lastIndex + ", count=" + entries.size() + "]";
    }

    private void markDecommissioned() {
        boolean alreadyDecommissioned = decommissioned;
        decommissioned = true;
        if (!alreadyDecommissioned) {
            notifyDecommissionListener();
        }
    }

    private void notifyDecommissionListener() {
        try {
            decommissionListener.run();
        } catch (RuntimeException e) {
            log.warn("{}@{} failed to run decommission callback", me.getId(), currentTerm, e);
        }
    }
}
