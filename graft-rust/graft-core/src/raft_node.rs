/*
 * Copyright (C) 2026 Frode Randers
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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::membership::ClusterConfiguration;
use crate::state_machine::StateMachine;
use crate::types::{LogEntry, NodeState, Peer};
use prost::Message;

// Implementation note
// ===================
// This module implements the Raft consensus algorithm as described in
// "In Search of an Understandable Consensus Algorithm" (Figure 2 and
// Figure 3). Figure 2 is treated as executable guidance: each rule is
// annotated where implemented. Figure 3 (State Machine Safety) governs
// the commit/apply pipeline.
//
// The state diagram transitions (referenced by labels a-e in the
// election/check_timeout sections) are:
//   a) times out, starts election
//   b) receives votes from majority of servers and becomes leader
//   c) discovers server with higher term → step down to Follower
//   d) discovers current leader or higher term → step down to Follower
//   e) times out, new election

// ---------------------------------------------------------------------------
// Store traits
// ---------------------------------------------------------------------------

/// Abstracts the append-only Raft log plus snapshot compaction metadata.
/// All methods take `&self` — implementations provide interior mutability
/// (typically a `Mutex<Vec<LogEntry>>`) so the log can be shared across
/// threads behind an `Arc`.
pub trait LogStore: Send + Sync {
    /// Highest compacted index included in local snapshot metadata. 0 if none.
    fn snapshot_index(&self) -> u64;
    /// Term of `snapshot_index()`. 0 if none.
    fn snapshot_term(&self) -> u64;
    /// Highest Raft log index present locally (1-based). Returns `snapshot_index()`
    /// when the log has been fully compacted.
    fn last_index(&self) -> u64;
    /// Term of entry at `last_index()`. 0 if empty.
    fn last_term(&self) -> u64;
    /// Term at a specific index. Index 0 always returns 0.
    fn term_at(&self, index: u64) -> u64;
    /// Entry at a specific 1-based index, or `None` if compacted/absent.
    fn entry_at(&self, index: u64) -> Option<LogEntry>;
    /// Append entries to the log. Entries must be command-type (not internal
    /// Raft RPC payloads).
    fn append(&self, entries: Vec<LogEntry>);
    /// Remove entries from `index` (inclusive) to the end of the log.
    fn truncate_from(&self, index: u64);
    /// Return a copy of entries starting from `index` (1-based, inclusive).
    /// Empty if `index > last_index()`.
    fn entries_from(&self, index: u64) -> Vec<LogEntry>;
    /// Compact the log prefix up to and including `index`. The compacted
    /// entries are replaced by snapshot metadata.
    fn compact_up_to(&self, index: u64);
    /// Snapshot payload bytes for InstallSnapshot transfer. Empty if no payload.
    fn snapshot_data(&self) -> Vec<u8>;
    /// Install a received snapshot: replaces log prefix with metadata and
    /// stores the snapshot payload for future transfers.
    fn install_snapshot(
        &self,
        last_included_index: u64,
        last_included_term: u64,
        snapshot_data: Vec<u8>,
    );
}

/// Small durable state that Raft must persist outside the replicated log:
/// the current term and the candidate voted for in that term. Surviving
/// these across restarts prevents a node from voting twice in the same
/// term or accepting stale entries after recovery.
///
/// Implementations provide interior mutability so they can be shared
/// behind an `Arc`.
pub trait PersistentStateStore: Send + Sync {
    fn current_term(&self) -> u64;
    fn set_current_term(&self, term: u64);
    fn voted_for(&self) -> Option<String>;
    fn set_voted_for(&self, peer_id: Option<String>);
}

/// Callback interface for sending Raft RPCs over the network. The
/// runtime/adapter layer provides the concrete TCP implementation.
/// RaftNode is deliberately transport-agnostic — it calls these
/// methods without knowing whether the transport is TCP, in-memory,
/// or a test stub.
pub trait RaftTransport: Send + Sync {
    fn send_vote_request(
        &self,
        peer: &Peer,
        term: u64,
        candidate_id: &str,
        last_log_index: u64,
        last_log_term: u64,
    );
    fn send_append_entries(
        &self,
        peer: &Peer,
        term: u64,
        leader_id: &str,
        prev_log_index: u64,
        prev_log_term: u64,
        leader_commit: u64,
        entries: Vec<LogEntry>,
    );
    fn send_install_snapshot(
        &self,
        peer: &Peer,
        term: u64,
        leader_id: &str,
        last_included_index: u64,
        last_included_term: u64,
        offset: u64,
        data: Vec<u8>,
        done: bool,
    );
    /// Signal that a heartbeat broadcast has been triggered — the transport
    /// layer initiates the actual send loop.
    fn broadcast_heartbeat_complete(&self);
}

// ---------------------------------------------------------------------------
// Pending snapshot assembly (follower side during InstallSnapshot)
// ---------------------------------------------------------------------------

/// Partial snapshot being received from the leader via InstallSnapshot RPC.
/// Accumulated chunk-by-chunk until the `done` flag signals completion,
/// at which point the snapshot is installed into the LogStore.
#[derive(Debug, Clone)]
pub struct PendingSnapshot {
    /// The last log index covered by this snapshot.
    pub last_included_index: u64,
    /// Term of the last included index.
    pub last_included_term: u64,
    /// Byte offset of the next expected chunk.
    pub offset: u64,
    /// Accumulated snapshot bytes so far.
    pub data: Vec<u8>,
}

// ---------------------------------------------------------------------------
// RaftNode — the consensus state machine
// ---------------------------------------------------------------------------

/// RaftNode is deliberately transport-agnostic. It is the consensus state
/// machine: given RPC requests and locally-triggered actions (election
/// timeouts, heartbeat ticks), it mutates durable Raft state and produces
/// responses. The runtime layer decides when to send those responses over
/// TCP and when to persist the returned state.
///
/// All mutation methods take `&mut self` — the runtime is responsible for
/// synchronization (typically via `Arc<Mutex<RaftNode>>`).
pub struct RaftNode {
    // -- Identity --
    /// This node's peer descriptor.
    pub me: Peer,

    // -- Raft paper "Persistent state on all servers" (Figure 2) --
    /// Logical clock. Monotonically increases; used to detect stale leaders
    /// and candidates. Persisted to `PersistentStateStore`.
    pub state: NodeState,
    /// Latest term the node has seen. Initialized to 0 on first boot.
    pub current_term: u64,
    /// Candidate id this node voted for in `current_term`, or None.
    /// Ensures at most one vote per term (Figure 2 "All Servers" rule).
    pub voted_for: Option<String>,

    // -- Election timing --
    /// Configurable base election timeout (ms). The actual deadline includes
    /// randomized jitter and per-round backoff to avoid synchronized elections.
    pub timeout_millis: u64,
    /// Monotonic timestamp of the last received AppendEntries (heartbeat) or
    /// granted vote. Resets the election clock.
    pub last_heartbeat: Instant,
    /// Absolute `Instant` deadline after which `has_timed_out()` returns true.
    pub timeout_at: Option<Instant>,
    /// Incremented each election round. Drives linear backoff (`timeout/10 *
    /// counter`) so repeated failures don't cause perpetual re-elections.
    pub election_sequence_counter: u64,

    // -- Raft paper "Volatile state on all servers" (Figure 2) --
    /// Highest log entry known to be committed. Initialized to 0, increases
    /// monotonically.
    pub commit_index: u64,
    /// Highest log entry applied to the state machine. `last_applied <= commit_index`.
    pub last_applied: u64,

    // -- Raft paper "Volatile state on leaders" (Figure 2) --
    /// Per-follower: index of the next log entry to send in AppendEntries.
    /// Re-initialized to `last_index + 1` on election.
    pub next_index: HashMap<String, u64>,
    /// Per-follower: highest log entry known to be replicated on that server.
    /// Used by `advance_commit_index_from_majority()` for quorum counting.
    pub match_index: HashMap<String, u64>,
    /// Per-follower: wall-clock millis of last successful replication contact.
    /// Feeds the read-lease barrier (linearizable read safety).
    pub last_follower_contact_millis: HashMap<String, u64>,
    /// Per-follower: count of consecutive AppendEntries failures. Operational
    /// telemetry, not part of formal Raft state.
    pub consecutive_follower_failures: HashMap<String, u32>,
    /// Per-follower: wall-clock millis of last replication failure.
    pub last_follower_failure_millis: HashMap<String, u64>,

    // -- Membership / Configuration --
    /// Currently committed (active) cluster configuration. Updated atomically
    /// when a configuration log entry commits.
    pub cluster_configuration: ClusterConfiguration,
    /// Configuration in force at the time of the last snapshot. Used to
    /// reconstruct historical configurations during `configuration_at()`.
    pub snapshot_configuration: ClusterConfiguration,
    /// Map of log index → configuration that committed at that index.
    /// Used by `configuration_at()` to replay config changes when computing
    /// quorums for historical log entries.
    pub committed_configurations: BTreeMap<u64, ClusterConfiguration>,
    /// True when this node has been removed from the active configuration.
    pub decommissioned: bool,
    /// True when the node is in "joining" mode (not yet a full member).
    pub joining: bool,
    /// The leader id this node currently recognizes. Reset on election start
    /// and step-down. Used for operational reporting and redirects.
    pub known_leader_id: Option<String>,
    /// Peers admitted via JOIN but not yet in the committed configuration.
    pub pending_join_ids: HashSet<String>,
    /// Members whose replication progress is being tracked for automatic
    /// finalization of a joint-consensus transition.
    pub pending_auto_finalize_members: Vec<String>,
    /// Log index that all `pending_auto_finalize_members` must reach before
    /// the leader auto-submits a FINALIZE command.
    pub pending_auto_finalize_fence_index: u64,
    /// Wall-clock time when joint consensus was entered. Used for operational
    /// health checks (stuck reconfiguration detection).
    pub configuration_transition_started: Option<Instant>,

    // -- Snapshot transfer state --
    /// Per-follower: byte offset for streaming InstallSnapshot chunks.
    /// Leader-side cursor; reset to 0 when a follower needs catch-up.
    pub snapshot_offsets: HashMap<String, u64>,
    /// Follower-side: partial snapshot being assembled from incoming chunks.
    pub pending_snapshot: Option<PendingSnapshot>,

    // -- Commit/apply bookkeeping --
    /// Cached application results for recently-committed entries, used so
    /// clients can receive results for read-after-write consistency.
    pub applied_command_results: BTreeMap<u64, Vec<u8>>,

    // -- Configuration knobs --
    /// Minimum uncompacted log entries before triggering local snapshot
    /// compaction. 0 disables automatic compaction.
    pub snapshot_min_entries: u64,
    /// Maximum bytes per InstallSnapshot chunk for streaming transfer.
    pub snapshot_chunk_bytes: u64,
    /// Duration (ms) during which a leader can serve linearizable reads
    /// without re-confirming its leadership via a contact-majority barrier.
    pub linearizable_read_lease_millis: u64,
    /// Maximum age (ms) of follower contact for that follower to count
    /// toward the read-barrier quorum.
    pub linearizable_read_timeout_millis: u64,

    // -- Peer address cache --
    /// All known peers and their addresses, indexed by peer id. Updated
    /// when the cluster configuration changes.
    pub known_peers: HashMap<String, Peer>,

    // -- Injected dependencies --
    /// The append-only replicated log.
    pub log_store: Arc<dyn LogStore>,
    /// Durable term and vote storage.
    pub persistent_state: Arc<dyn PersistentStateStore>,
    /// Application state machine (None for configuration-only nodes).
    pub state_machine: Option<Arc<dyn StateMachine>>,
}

impl RaftNode {
    /// Constructs a new RaftNode. Loads `current_term` and `voted_for` from
    /// the persistent state store, initializes the election clock,
    /// normalizes the peer set, and sets `commit_index`/`last_applied` to
    /// the log's snapshot boundary.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        me: Peer,
        timeout_millis: u64,
        log_store: Arc<dyn LogStore>,
        persistent_state: Arc<dyn PersistentStateStore>,
        state_machine: Option<Arc<dyn StateMachine>>,
        cluster_configuration: ClusterConfiguration,
        snapshot_min_entries: u64,
        snapshot_chunk_bytes: u64,
    ) -> Self {
        let current_term = persistent_state.current_term();
        let voted_for = persistent_state.voted_for();
        let now = Instant::now();

        // Normalize the peer set immediately so every later majority
        // calculation sees a consistent membership model, even when the
        // node was created from sparse input.
        let known_peers: HashMap<String, Peer> = cluster_configuration
            .all_members()
            .into_iter()
            .map(|p| (p.id.clone(), p.clone()))
            .collect();

        let snapshot_index = log_store.snapshot_index();

        let mut node = Self {
            me: me.clone(),
            state: NodeState::Follower,
            current_term,
            voted_for,
            timeout_millis,
            last_heartbeat: now,
            timeout_at: None,
            election_sequence_counter: 0,
            commit_index: snapshot_index,
            last_applied: snapshot_index,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            last_follower_contact_millis: HashMap::new(),
            consecutive_follower_failures: HashMap::new(),
            last_follower_failure_millis: HashMap::new(),
            cluster_configuration: cluster_configuration.clone(),
            snapshot_configuration: cluster_configuration.clone(),
            committed_configurations: BTreeMap::new(),
            decommissioned: false,
            joining: false,
            known_leader_id: None,
            pending_join_ids: HashSet::new(),
            pending_auto_finalize_members: Vec::new(),
            pending_auto_finalize_fence_index: 0,
            configuration_transition_started: None,
            snapshot_offsets: HashMap::new(),
            pending_snapshot: None,
            applied_command_results: BTreeMap::new(),
            snapshot_min_entries,
            snapshot_chunk_bytes,
            linearizable_read_lease_millis: 750,
            linearizable_read_timeout_millis: 500,
            known_peers,
            log_store,
            persistent_state,
            state_machine,
        };

        // Seed the election timer so the node doesn't immediately elect.
        if snapshot_index > 0 {
            if let Some(ref sm) = node.state_machine {
                sm.restore(&node.log_store.snapshot_data());
            }
        }
        node.refresh_timeout(now);
        node
    }

    // -----------------------------------------------------------------------
    // Timeout helpers
    // -----------------------------------------------------------------------

    /// Resets the election clock to `now + timeout + jitter + backoff`.
    /// Called after every heartbeat, granted vote, and step-down.
    pub fn refresh_timeout(&mut self, now: Instant) {
        self.last_heartbeat = now;
        let extra = self.sample_election_extra_delay();
        self.timeout_at = Some(now + Duration::from_millis(self.timeout_millis + extra));
    }

    /// True when the election deadline has elapsed. The deadline is
    /// established by `refresh_timeout()` and never goes backwards.
    pub fn has_timed_out(&self, now: Instant) -> bool {
        match self.timeout_at {
            Some(deadline) => now >= deadline,
            None => false,
        }
    }

    /// Produces randomized extra delay for the election deadline.
    /// Includes both uniform jitter ([0..timeoutMillis)) and a linear
    /// backoff per election round (timeout/10 * sequenceCounter), each
    /// with its own randomization. This avoids synchronized elections
    /// even after coordinated restarts.
    fn sample_election_extra_delay(&self) -> u64 {
        use rand::Rng;
        let mut rng = rand::thread_rng();

        let jitter: f64 = rng.gen();
        let jitter = (self.timeout_millis as f64 * jitter) as u64;

        let backoff_base = (self.timeout_millis / 10) * self.election_sequence_counter;
        let backoff_jitter: f64 = rng.gen();
        let backoff_jitter = (backoff_base as f64 * backoff_jitter) as u64;

        jitter + backoff_base + backoff_jitter
    }

    // -----------------------------------------------------------------------
    // Configuration helpers
    // -----------------------------------------------------------------------

    /// The currently-active cluster configuration (committed).
    pub fn active_configuration(&self) -> &ClusterConfiguration {
        &self.cluster_configuration
    }

    /// Remote voting peers in the given configuration (excludes self).
    fn remote_voting_peers_for(&self, config: &ClusterConfiguration) -> Vec<Peer> {
        config
            .current_voting_members()
            .into_iter()
            .filter(|p| p.id != self.me.id)
            .cloned()
            .collect()
    }

    /// Remote peers that should receive replication (voters + learners,
    /// excluding self).
    fn remote_replicated_peers_for(&self, config: &ClusterConfiguration) -> Vec<Peer> {
        config
            .all_members()
            .into_iter()
            .filter(|p| p.id != self.me.id)
            .cloned()
            .collect()
    }

    /// True when this node is a voter in the active configuration.
    pub fn is_voter_in_active_config(&self) -> bool {
        self.cluster_configuration.is_voter(&self.me.id)
    }

    /// True when this node appears in either membership set of the active
    /// configuration.
    pub fn is_peer_in_active_config(&self) -> bool {
        self.cluster_configuration.contains(&self.me.id)
    }

    // -----------------------------------------------------------------------
    // a) times out, starts election
    // e) times out, new election
    // -----------------------------------------------------------------------

    /// Periodic election-timeout check. Should be called by the runtime on
    /// a configurable interval. If the node is a Follower or Candidate and
    /// has timed out, starts a new election.
    ///
    /// Early-return conditions (before checking timeout):
    /// 1. Joining node — stays Follower, clears vote, refreshes timeout.
    /// 2. Non-voter / not-in-config — marks as decommissioned, stays Follower.
    pub fn check_timeout(&mut self, now: Instant, transport: &dyn RaftTransport) {
        // Joining nodes should not start elections.
        if self.joining {
            if self.state != NodeState::Follower {
                self.become_follower(now);
            }
            self.voted_for = None;
            self.persistent_state.set_voted_for(None);
            self.refresh_timeout(now);
            return;
        }

        // Non-voters or nodes not in the configuration at all should
        // not disrupt the cluster with elections.
        if !self.is_voter_in_active_config() {
            self.update_decommissioned(false);
            if self.state != NodeState::Follower {
                self.become_follower(now);
            }
            self.voted_for = None;
            self.persistent_state.set_voted_for(None);
            self.refresh_timeout(now);
            return;
        }

        self.update_decommissioned(true);

        // Figure 2: if election timeout elapses, start new election.
        if self.state != NodeState::Leader && self.has_timed_out(now) {
            self.new_election(transport);
        }
    }

    // -----------------------------------------------------------------------
    // Figure 2 ("Candidates"): on conversion to candidate
    // -----------------------------------------------------------------------

    /// Starts a new election: increments term, votes for self, resets the
    /// election timer, and sends RequestVote RPCs to all remote voting
    /// peers in the active configuration.
    ///
    /// Figure 2 rule: "On conversion to candidate, start election:
    /// - Increment currentTerm
    /// - Vote for self
    /// - Reset election timer
    /// - Send RequestVote RPCs to all other servers"
    fn new_election(&mut self, transport: &dyn RaftTransport) {
        if !self.is_voter_in_active_config() {
            tracing::info!("{} will not start election: not a voter", self.me.id);
            return;
        }

        // Figure 2 ("Candidates"): on conversion to candidate.
        self.state = NodeState::Candidate;
        self.known_leader_id = None;
        self.current_term += 1;
        self.persistent_state.set_current_term(self.current_term);
        self.voted_for = Some(self.me.id.clone());
        self.persistent_state
            .set_voted_for(Some(self.me.id.clone()));
        self.election_sequence_counter += 1;
        self.refresh_timeout(Instant::now());

        // The candidate's log must be at least as up-to-date as any
        // voter's log for the vote to be granted (Figure 2 "RequestVote RPC"
        // receiver implementation: §5.4.1 election restriction).
        let last_index = self.log_store.last_index();
        let last_term = self.log_store.last_term();
        let peers = self.remote_voting_peers_for(self.active_configuration());

        tracing::info!(
            "{} starting election for term {}, lastIndex={}, lastTerm={}",
            self.me.id,
            self.current_term,
            last_index,
            last_term
        );

        for peer in &peers {
            transport.send_vote_request(
                peer,
                self.current_term,
                &self.me.id,
                last_index,
                last_term,
            );
        }
    }

    // -----------------------------------------------------------------------
    // b) receives votes from majority of servers and becomes leader
    // -----------------------------------------------------------------------

    /// Handles VoteResponse messages from the election round identified
    /// by `election_term`.
    ///
    /// Two critical guards (matching the Java implementation):
    /// 1. Any response with a term higher than ours forces immediate
    ///    step-down (Figure 2 "All Servers" rule).
    /// 2. Responses from a term < `election_term` are silently discarded —
    ///    delayed responses from a prior election must not cause a false
    ///    leadership transition.
    ///
    /// Quorum is checked using `hasJointMajority()` against the active
    /// configuration, so during joint consensus the candidate must win
    /// votes from a majority of **both** old and new configurations.
    pub fn handle_vote_responses(
        &mut self,
        responses: Vec<VoteResponse>,
        election_term: u64,
        transport: &dyn RaftTransport,
    ) {
        let now = Instant::now();

        // Figure 2 "All Servers": if RPC response contains term T > currentTerm,
        // set currentTerm = T, convert to follower.
        let max_response_term = responses.iter().map(|r| r.current_term).max().unwrap_or(0);

        let max_term = std::cmp::max(self.current_term, max_response_term);
        if max_term > self.current_term {
            self.current_term = max_term;
            self.persistent_state.set_current_term(max_term);
            self.become_follower(now);
            return;
        }

        // Discard responses from a previous election round.
        if self.current_term != election_term {
            return;
        }

        let mut votes_granted: HashSet<String> = HashSet::new();
        votes_granted.insert(self.me.id.clone());

        for response in &responses {
            if response.vote_granted {
                votes_granted.insert(response.peer_id.clone());
            } else if response.current_term > self.current_term {
                // Seen a higher term from a rejection — step down.
                // Figure 2: "If votedFor is null and candidate's log is at
                // least as up-to-date as receiver's log, grant vote."
                // A rejection with a higher term means we're stale.
                self.current_term = response.current_term;
                self.persistent_state
                    .set_current_term(response.current_term);
                self.become_follower(now);
                return;
            }
        }

        let config = self.active_configuration().clone();
        if config.has_joint_majority(&votes_granted) {
            tracing::info!("{} won election for term {}", self.me.id, self.current_term);
            self.state = NodeState::Leader;
            self.known_leader_id = Some(self.me.id.clone());
            self.election_sequence_counter = 0;
            self.initialize_leader_replication_state();
            self.append_leader_noop_entry();
            self.advance_commit_index_from_majority();
            transport.broadcast_heartbeat_complete();
        }
    }

    // -----------------------------------------------------------------------
    // Figure 2 "Leaders": initialize nextIndex/matchIndex
    // -----------------------------------------------------------------------

    /// Initializes per-follower replication cursors when this node becomes
    /// leader. `nextIndex` is set to `lastIndex + 1` for every follower,
    /// and `matchIndex` is set to the snapshot index. This is conservative
    /// — the leader assumes followers need catch-up and will discover
    /// the correct position when the first AppendEntries response arrives.
    pub fn initialize_leader_replication_state(&mut self) {
        let last_index = self.log_store.last_index();
        let next = std::cmp::max(self.log_store.snapshot_index() + 1, last_index + 1);

        self.next_index.clear();
        self.match_index.clear();
        self.consecutive_follower_failures.clear();
        self.last_follower_failure_millis.clear();
        self.last_follower_contact_millis.clear();

        let config = self.cluster_configuration.clone();
        for peer in self.remote_replicated_peers_for(&config) {
            self.next_index.insert(peer.id.clone(), next);
            self.match_index
                .insert(peer.id.clone(), self.log_store.snapshot_index());
        }
    }

    /// Appends a no-op entry in the leader's current term. This implements
    /// the leader-completeness property (Figure 3): once the no-op commits
    /// on a majority, all prior entries from older terms are implicitly
    /// committed as well.
    pub fn append_leader_noop_entry(&mut self) {
        self.log_store
            .append(vec![LogEntry::noop(self.current_term, self.me.id.clone())]);
    }

    // -----------------------------------------------------------------------
    // Heartbeat / AppendEntries — leader side
    // -----------------------------------------------------------------------

    /// Broadcasts AppendEntries to every remote replicated peer. Heartbeats
    /// carry the previous-log position for the follower, which doubles as a
    /// consistency/read-barrier probe without sending entries.
    ///
    /// If a follower's `nextIndex` is at or before the snapshot boundary,
    /// an InstallSnapshot RPC is sent instead so the follower can catch up
    /// without needing the compacted log prefix.
    pub fn broadcast_heartbeat(&self, transport: &dyn RaftTransport) {
        if self.state != NodeState::Leader {
            return;
        }

        let config = self.cluster_configuration.clone();
        for peer in self.remote_replicated_peers_for(&config) {
            let snapshot_index = self.log_store.snapshot_index();
            let last_index = self.log_store.last_index();
            let next = *self.next_index.get(&peer.id).unwrap_or(&(last_index + 1));

            // Guard against sending entries that have been compacted.
            let min_next = snapshot_index + 1;
            let next = std::cmp::max(min_next, next);

            // Follower is behind the snapshot boundary — send snapshot catch-up.
            if next <= snapshot_index {
                let snapshot_data = self.log_store.snapshot_data();
                transport.send_install_snapshot(
                    &peer,
                    self.current_term,
                    &self.me.id,
                    snapshot_index,
                    self.log_store.snapshot_term(),
                    0,
                    snapshot_data,
                    true,
                );
                continue;
            }

            // Standard AppendEntries: prevLogIndex/prevLogTerm are the
            // entry immediately before the first entry being sent.
            let prev_log_index = if next > 0 { next - 1 } else { 0 };
            let prev_log_term = if prev_log_index == 0 {
                0
            } else {
                self.log_store.term_at(prev_log_index)
            };

            let entries = self.log_store.entries_from(next);

            transport.send_append_entries(
                &peer,
                self.current_term,
                &self.me.id,
                prev_log_index,
                prev_log_term,
                self.commit_index,
                entries,
            );
        }
    }

    // -----------------------------------------------------------------------
    // c) discovers server with higher term → steps down
    // d) discovers current leader or higher term → steps down
    // -----------------------------------------------------------------------

    /// Converts the node to Follower: clears leader-id tracking, resets
    /// the election counter, and persists the cleared vote.
    pub fn become_follower(&mut self, now: Instant) {
        self.state = NodeState::Follower;
        self.known_leader_id = None;
        self.clear_pending_auto_finalize();
        self.voted_for = None;
        self.persistent_state.set_voted_for(None);
        self.election_sequence_counter = 0;
        self.refresh_timeout(now);
    }

    // -----------------------------------------------------------------------
    // AppendEntries RPC — follower side (Figure 2 "AppendEntries RPC" receiver)
    // -----------------------------------------------------------------------

    /// Processes an incoming AppendEntries request on the follower/receiver
    /// side. Implements the full Figure 2 receiver rules:
    ///
    /// 1. Reply false if term < currentTerm (§5.1)
    /// 2. If term > currentTerm, update term and become follower (§5.2)
    /// 3. If prevLogIndex/prevLogTerm don't match, reply false (§5.3)
    /// 4. If an existing entry conflicts with a new one (same index but
    ///    different term), delete the existing entry and all that follow
    /// 5. Append any new entries not already in the log
    /// 6. If leaderCommit > commitIndex, advance commitIndex and apply
    ///
    /// A valid AppendEntries request is also the leader heartbeat. It
    /// refreshes leader identity and suppresses local elections.
    pub fn handle_append_entries_request(
        &mut self,
        req: AppendEntriesRequest,
    ) -> AppendEntriesResponse {
        let now = Instant::now();

        // Figure 2 rule 1: reply false if term < currentTerm.
        if req.term < self.current_term {
            return AppendEntriesResponse {
                term: self.current_term,
                peer_id: self.me.id.clone(),
                success: false,
                match_index: -1,
            };
        }

        // Figure 2 rule 2: if term > currentTerm, or candidate receives
        // AppendEntries, step down to follower.
        if req.term > self.current_term || self.state == NodeState::Candidate {
            self.current_term = req.term;
            self.persistent_state.set_current_term(req.term);
            self.become_follower(now);
        }

        // A valid heartbeat — record the leader and reset the election clock.
        self.known_leader_id = Some(req.leader_id.clone());
        self.refresh_timeout(now);

        // Figure 2 rule 3: log consistency check. Reject if follower's log
        // doesn't contain an entry at prevLogIndex whose term matches
        // prevLogTerm.
        if req.prev_log_index > 0
            && (req.prev_log_index > self.log_store.last_index()
                || self.log_store.term_at(req.prev_log_index) != req.prev_log_term)
        {
            return AppendEntriesResponse {
                term: self.current_term,
                peer_id: self.me.id.clone(),
                success: false,
                match_index: self.log_store.last_index() as i64,
            };
        }

        // Figure 2 rule 4: if an existing entry conflicts with a new one
        // (same index but different term), delete the existing entry and
        // all that follow. Then append new entries.
        let entries_len = req.entries.len() as u64;

        if !req.entries.is_empty() {
            if req.prev_log_index < self.log_store.last_index() {
                self.log_store.truncate_from(req.prev_log_index + 1);
            }

            self.log_store.append(req.entries);
        }

        // Figure 2 rule 5: if leaderCommit > commitIndex, set commitIndex =
        // min(leaderCommit, index of last new entry). Then apply committed
        // entries to the state machine.
        let last_new_entry = req.prev_log_index + entries_len;
        if req.leader_commit > self.commit_index {
            self.commit_index = std::cmp::min(req.leader_commit, last_new_entry);
            self.apply_committed_entries();
        }

        AppendEntriesResponse {
            term: self.current_term,
            peer_id: self.me.id.clone(),
            success: true,
            match_index: last_new_entry as i64,
        }
    }

    // -----------------------------------------------------------------------
    // RequestVote RPC — follower side (Figure 2 "RequestVote RPC" receiver)
    // -----------------------------------------------------------------------

    /// Processes an incoming RequestVote RPC on the receiver side.
    /// Implements the Figure 2 receiver rules:
    ///
    /// 1. Reply false if term < currentTerm
    /// 2. If votedFor is null or candidateId, and candidate's log is at
    ///    least as up-to-date as receiver's log, grant vote (§5.4.1).
    ///
    /// The "log is at least as up-to-date" check (election restriction)
    /// requires the candidate's last log term to be >= receiver's last log
    /// term, and if the terms are equal, the candidate's last log index
    /// must be >= receiver's last log index.
    pub fn handle_vote_request(&mut self, req: VoteRequest) -> VoteResponse {
        let now = Instant::now();

        // Figure 2 "All Servers": if term > currentTerm, update and step down.
        if req.term > self.current_term {
            self.current_term = req.term;
            self.persistent_state.set_current_term(req.term);
            self.become_follower(now);
        }

        // §5.4.1 election restriction: the candidate's log must be at least
        // as up-to-date as the receiver's log.
        let candidate_log_is_up_to_date = req.last_log_term > self.log_store.last_term()
            || (req.last_log_term == self.log_store.last_term()
                && req.last_log_index >= self.log_store.last_index());

        let can_grant = req.term >= self.current_term
            && (self.voted_for.is_none() || self.voted_for.as_deref() == Some(&req.candidate_id))
            && candidate_log_is_up_to_date;

        let mut vote_granted = false;
        if can_grant {
            self.voted_for = Some(req.candidate_id.clone());
            self.persistent_state
                .set_voted_for(Some(req.candidate_id.clone()));
            vote_granted = true;
            // Granting a vote acts as a heartbeat — suppress local election.
            self.refresh_timeout(now);
        }

        VoteResponse {
            peer_id: self.me.id.clone(),
            term: self.current_term,
            vote_granted,
            current_term: self.current_term,
        }
    }

    // -----------------------------------------------------------------------
    // AppendEntries response — leader side
    // -----------------------------------------------------------------------

    /// Processes an AppendEntries response on the leader side.
    ///
    /// - **Higher term**: immediately step down to Follower (Figure 2 rule).
    /// - **Rejection**: decrement `nextIndex` for that follower (conflict
    ///   resolution) and retry. If the rejection indicates the follower is
    ///   behind the snapshot boundary, trigger InstallSnapshot instead.
    /// - **Success**: advance `matchIndex` and `nextIndex`, record contact
    ///   for the read barrier, and check whether the commit index can advance.
    pub fn handle_append_entries_response(
        &mut self,
        resp: AppendEntriesResponse,
        req: AppendEntriesRequest,
        peer_id: &str,
    ) {
        let now = Instant::now();
        if self.state != NodeState::Leader {
            return;
        }

        // Figure 2: higher term → step down.
        if resp.term > self.current_term {
            self.current_term = resp.term;
            self.persistent_state.set_current_term(resp.term);
            self.become_follower(now);
            return;
        }

        // Conflict: follower rejected the AppendEntries.
        if !resp.success {
            if resp.match_index < 0 {
                self.record_follower_replication_failure(peer_id);
                return;
            }
            let snapshot_index = self.log_store.snapshot_index();
            let current_next = self.next_index.get(peer_id).copied().unwrap_or(1);

            // If the follower's matchIndex is at or behind the snapshot,
            // the follower needs a full snapshot catch-up.
            if resp.match_index as u64 <= snapshot_index {
                self.next_index.insert(peer_id.to_string(), snapshot_index);
            } else {
                // Standard Raft conflict resolution: decrement nextIndex
                // and retry. Don't go below snapshot_index + 1.
                let floor = snapshot_index + 1;
                let new_next = std::cmp::max(floor, current_next.saturating_sub(1));
                self.next_index.insert(peer_id.to_string(), new_next);
            }
            return;
        }

        // Success: follower accepted entries up to `advanced`.
        let advanced = req.prev_log_index + req.entries.len() as u64;
        self.next_index.insert(peer_id.to_string(), advanced + 1);
        self.match_index.insert(peer_id.to_string(), advanced);
        self.record_follower_replication_success(peer_id);

        // A standard Raft guard: the leader advances commit by counting
        // replicas for entries from its own term. This indirectly commits
        // older entries (the no-op handles this during election).
        self.advance_commit_index_from_majority();
        self.maybe_auto_finalize_joint_configuration();
    }

    // -- Follower reachability bookkeeping (operational telemetry) --

    pub fn record_follower_replication_success(&mut self, peer_id: &str) {
        self.consecutive_follower_failures
            .insert(peer_id.to_string(), 0);
        let now = now_millis();
        self.last_follower_contact_millis
            .insert(peer_id.to_string(), now);
    }

    pub fn record_follower_replication_failure(&mut self, peer_id: &str) {
        let count = self
            .consecutive_follower_failures
            .get(peer_id)
            .copied()
            .unwrap_or(0)
            + 1;
        self.consecutive_follower_failures
            .insert(peer_id.to_string(), count);
        let now = now_millis();
        self.last_follower_failure_millis
            .insert(peer_id.to_string(), now);
    }

    // -----------------------------------------------------------------------
    // Commit-index advancement (Figure 2 "Leaders" commit rule)
    // -----------------------------------------------------------------------

    /// Advances `commitIndex` by scanning log entries in the current term
    /// and checking whether each has been replicated to a joint-consensus
    /// majority. Only current-term entries are directly committed; entries
    /// from prior terms become committed when a current-term entry commits
    /// (the leader no-op handles this during election).
    ///
    /// Uses `configuration_at(n)` to reconstruct the membership that was
    /// in force at index `n`, so commit decisions during joint consensus
    /// require majorities of **both** old and new configurations.
    pub fn advance_commit_index_from_majority(&mut self) {
        let mut candidate_commit = self.commit_index;
        let last_idx = self.log_store.last_index();

        for n in (self.commit_index + 1)..=last_idx {
            // Only entries from the leader's current term count.
            if self.log_store.term_at(n) != self.current_term {
                continue;
            }

            let mut replicated: HashSet<String> = HashSet::new();
            replicated.insert(self.me.id.clone());

            let config = self.cluster_configuration.clone();
            for peer in self.remote_voting_peers_for(&config) {
                let mi = self.match_index.get(&peer.id).copied().unwrap_or(0);
                if mi >= n {
                    replicated.insert(peer.id.clone());
                }
            }

            // Reconstruct the membership at index n so the quorum check
            // uses the historically-correct configuration.
            let effective_config = self.configuration_at(n);
            if effective_config.has_joint_majority(&replicated) {
                candidate_commit = n;
            }
        }

        if candidate_commit > self.commit_index {
            self.commit_index = candidate_commit;
            self.apply_committed_entries();
        }
    }

    // -----------------------------------------------------------------------
    // Apply committed entries (Figure 3 State Machine Safety)
    // -----------------------------------------------------------------------

    /// Applies all committed-but-unapplied entries to the state machine
    /// in monotonically-increasing index order, preserving the Raft
    /// State Machine Safety property (Figure 3).
    ///
    /// No-op entries (leader-election placeholders with empty data) are
    /// skipped. Configuration commands are applied to the membership
    /// state. Application commands are forwarded to the `StateMachine`.
    pub fn apply_committed_entries(&mut self) {
        while self.last_applied < self.commit_index {
            self.last_applied += 1;
            let entry = match self.log_store.entry_at(self.last_applied) {
                Some(e) => e,
                None => continue,
            };

            // No-op entries anchor the leader's term but have no
            // application-level effect.
            if entry.is_noop() {
                continue;
            }

            // Configuration commands update the cluster membership
            // (JOIN / JOINT / FINALIZE).
            if self.apply_configuration_command(&entry.data) {
                continue;
            }

            // Forward to the application state machine.
            if let Some(ref sm) = self.state_machine {
                let result = sm.apply_with_result(entry.term, &entry.data);
                if !result.is_empty() {
                    self.applied_command_results
                        .insert(self.last_applied, result);
                }
            }
        }

        self.maybe_auto_finalize_joint_configuration();
        self.maybe_compact_local_snapshot();
    }

    // -----------------------------------------------------------------------
    // Configuration tracking
    // -----------------------------------------------------------------------

    /// Reconstructs the cluster configuration that was in force at the
    /// given log index. If the index is ≤ the snapshot boundary, the
    /// snapshot configuration is used. Otherwise, replays configuration
    /// commands from the snapshot boundary to the target index.
    fn configuration_at(&self, index: u64) -> ClusterConfiguration {
        if index <= self.log_store.snapshot_index() {
            return self.snapshot_configuration.clone();
        }

        let mut config = self.snapshot_configuration.clone();
        for i in (self.log_store.snapshot_index() + 1)..=index {
            if let Some(committed) = self.committed_configurations.get(&i) {
                config = committed.clone();
            }
        }
        config
    }

    /// Attempts to interpret `data` as a protobuf-encoded
    /// `InternalRaftCommand` and drive the corresponding membership
    /// transition (JOIN, JOINT, FINALIZE). Returns `true` if the data
    /// was recognized and processed.
    fn apply_configuration_command(&mut self, data: &[u8]) -> bool {
        use graft_proto::raft;
        use graft_proto::raft::internal_raft_command::Command;

        let cmd = match raft::InternalRaftCommand::decode(data) {
            Ok(c) => c,
            Err(_) => return false,
        };

        let command = match cmd.command {
            Some(c) => c,
            None => return false,
        };

        match command {
            Command::Join(join_cmd) => {
                if let Some(ref peer_spec) = join_cmd.member {
                    let ip: std::net::IpAddr = peer_spec
                        .host
                        .parse()
                        .unwrap_or(std::net::IpAddr::from([127, 0, 0, 1]));
                    let addr = std::net::SocketAddr::new(ip, peer_spec.port as u16);
                    let role = match peer_spec.role.to_uppercase().as_str() {
                        "LEARNER" => crate::types::Role::Learner,
                        _ => crate::types::Role::Voter,
                    };
                    let peer = crate::types::Peer::new(peer_spec.id.clone(), addr, role);
                    tracing::info!("{} JOIN: admitting peer {}", self.me.id, peer.id);
                    self.known_peers.insert(peer.id.clone(), peer.clone());
                    self.pending_join_ids.insert(peer.id.clone());
                    return true;
                }
                false
            }
            Command::Joint(joint_cmd) => {
                let proposed: Vec<crate::types::Peer> = joint_cmd
                    .members
                    .iter()
                    .map(|ps| {
                        let ip: std::net::IpAddr = ps
                            .host
                            .parse()
                            .unwrap_or(std::net::IpAddr::from([127, 0, 0, 1]));
                        let addr = std::net::SocketAddr::new(ip, ps.port as u16);
                        let role = match ps.role.to_uppercase().as_str() {
                            "LEARNER" => crate::types::Role::Learner,
                            _ => crate::types::Role::Voter,
                        };
                        crate::types::Peer::new(ps.id.clone(), addr, role)
                    })
                    .collect();

                let proposed_ids: std::collections::HashSet<String> =
                    proposed.iter().map(|p| p.id.clone()).collect();
                tracing::info!(
                    "{} JOINT: entering joint consensus with {} members",
                    self.me.id,
                    proposed.len()
                );

                for m in &proposed {
                    if !self.known_peers.contains_key(&m.id) {
                        self.known_peers.insert(m.id.clone(), m.clone());
                    }
                }

                let old_config = self.cluster_configuration.clone();
                self.cluster_configuration = old_config.transition_to(proposed);
                self.pending_auto_finalize_members = proposed_ids.iter().cloned().collect();
                self.pending_auto_finalize_fence_index = self.commit_index + 1;

                if proposed_ids.contains(&self.me.id) {
                    self.joining = false;
                }
                self.pending_join_ids
                    .retain(|id| !proposed_ids.contains(id));
                self.configuration_transition_started = Some(std::time::Instant::now());

                if self.state == crate::types::NodeState::Leader {
                    let last_idx = self.log_store.last_index();
                    let next = std::cmp::max(self.log_store.snapshot_index() + 1, last_idx + 1);
                    for m in self.cluster_configuration.all_members() {
                        if m.id != self.me.id && !self.next_index.contains_key(&m.id) {
                            self.next_index.insert(m.id.clone(), next);
                            self.match_index
                                .insert(m.id.clone(), self.log_store.snapshot_index());
                        }
                    }
                }
                return true;
            }
            Command::Finalize(_) => {
                tracing::info!("{} FINALIZE: exiting joint consensus", self.me.id);
                let next_ids: std::collections::HashSet<String> = self
                    .cluster_configuration
                    .next_members()
                    .iter()
                    .map(|p| p.id.clone())
                    .collect();

                self.cluster_configuration = self.cluster_configuration.finalize_transition();
                self.configuration_transition_started = None;
                self.joining = false;
                self.pending_join_ids.clear();
                self.clear_pending_auto_finalize();

                if self.state == crate::types::NodeState::Leader {
                    self.next_index.retain(|id, _| next_ids.contains(id));
                    self.match_index.retain(|id, _| next_ids.contains(id));
                }

                let current_ids: std::collections::HashSet<String> = self
                    .cluster_configuration
                    .current_members()
                    .iter()
                    .map(|p| p.id.clone())
                    .collect();
                self.known_peers.retain(|id, _| current_ids.contains(id));

                if !current_ids.contains(&self.me.id) {
                    tracing::info!("{} removed from configuration — stepping down", self.me.id);
                    if self.state == crate::types::NodeState::Leader {
                        self.become_follower(std::time::Instant::now());
                    }
                    self.decommissioned = true;
                }
                true
            }
        }
    }

    /// If the leader has pending auto-finalize members and all of them
    /// have caught up to the fence index (i.e., their matchIndex ≥
    /// fenceIndex and commitIndex ≥ fenceIndex), the leader auto-submits
    /// a FINALIZE configuration command into the log.  This completes the
    /// joint-consensus transition without requiring an explicit
    /// administrator action.
    pub fn maybe_auto_finalize_joint_configuration(&mut self) {
        if self.state != NodeState::Leader
            || self.decommissioned
            || self.pending_auto_finalize_members.is_empty()
            || !self.cluster_configuration.is_joint_consensus()
        {
            return;
        }

        let fence = self.pending_auto_finalize_fence_index;
        if fence == 0 || self.commit_index < fence {
            return;
        }

        let all_caught_up = self
            .pending_auto_finalize_members
            .iter()
            .all(|id| self.match_index.get(id).copied().unwrap_or(0) >= fence);

        if all_caught_up {
            let finalize_cmd = graft_proto::raft::InternalRaftCommand {
                command: Some(graft_proto::raft::internal_raft_command::Command::Finalize(
                    graft_proto::raft::FinalizeConfigurationCommand {},
                )),
            };
            self.log_store.append(vec![LogEntry::new(
                self.current_term,
                self.me.id.clone(),
                finalize_cmd.encode_to_vec(),
            )]);
            self.clear_pending_auto_finalize();
        }
    }

    fn clear_pending_auto_finalize(&mut self) {
        self.pending_auto_finalize_members.clear();
        self.pending_auto_finalize_fence_index = 0;
    }

    // -----------------------------------------------------------------------
    // Snapshot compaction (log prefix truncation)
    // -----------------------------------------------------------------------

    /// Compacts the log prefix when the number of uncompacted entries
    /// exceeds `snapshot_min_entries`. The state machine is asked to
    /// produce a domain snapshot, which is then stored in the log store
    /// alongside the compaction metadata.
    fn maybe_compact_local_snapshot(&mut self) {
        if self.state_machine.is_none() || self.snapshot_min_entries == 0 {
            return;
        }

        let snapshot_index = self.log_store.snapshot_index();
        if self.commit_index == 0 || self.commit_index <= snapshot_index {
            return;
        }

        let uncompacted = self.commit_index - snapshot_index;
        if uncompacted < self.snapshot_min_entries {
            return;
        }

        if let Some(ref sm) = self.state_machine {
            let snapshot_data = sm.snapshot();
            let term = self.log_store.term_at(self.commit_index);
            self.log_store.compact_up_to(self.commit_index);
            self.log_store
                .install_snapshot(self.commit_index, term, snapshot_data);
            self.snapshot_configuration = self.cluster_configuration.clone();
            self.trim_applied_command_results();
        }
    }

    // -----------------------------------------------------------------------
    // InstallSnapshot — follower side (chunked receive)
    // -----------------------------------------------------------------------

    /// Accepts a chunk of a streaming InstallSnapshot RPC on the follower
    /// side. The first chunk (offset=0) creates a `PendingSnapshot`; each
    /// subsequent chunk appends data; the final chunk (`done=true`) installs
    /// the complete snapshot into the log store.
    ///
    /// After installation, the log store drops all compacted entries and
    /// the node's commit/lastApplied indices are implicitly advanced to
    /// the snapshot boundary.
    pub fn accept_snapshot_chunk(
        &mut self,
        req: InstallSnapshotRequest,
    ) -> InstallSnapshotResponse {
        let now = Instant::now();

        // Reject stale snapshots.
        if req.term < self.current_term {
            return InstallSnapshotResponse {
                term: self.current_term,
                peer_id: self.me.id.clone(),
                success: false,
                last_included_index: 0,
            };
        }

        // Higher term → recognize the new leader and become follower.
        if req.term > self.current_term {
            self.current_term = req.term;
            self.persistent_state.set_current_term(req.term);
            self.become_follower(now);
        }

        self.known_leader_id = Some(req.leader_id.clone());
        self.refresh_timeout(now);

        // First chunk: start a new pending snapshot.
        if req.offset == 0 {
            self.pending_snapshot = Some(PendingSnapshot {
                last_included_index: req.last_included_index,
                last_included_term: req.last_included_term,
                offset: 0,
                data: Vec::new(),
            });
        }

        if let Some(ref mut pending) = self.pending_snapshot {
            pending.data.extend_from_slice(&req.data);
            pending.offset += req.data.len() as u64;

            // Final chunk: install the complete snapshot.
            if req.done {
                self.log_store.install_snapshot(
                    pending.last_included_index,
                    pending.last_included_term,
                    std::mem::take(&mut pending.data),
                );
                let installed_index = pending.last_included_index;
                let installed_data = self.log_store.snapshot_data();
                self.pending_snapshot = None;
                self.commit_index = self.commit_index.max(installed_index);
                self.last_applied = self.last_applied.max(installed_index);
                if let Some(ref sm) = self.state_machine {
                    sm.restore(&installed_data);
                }
            }
        }

        InstallSnapshotResponse {
            term: self.current_term,
            peer_id: self.me.id.clone(),
            success: true,
            last_included_index: req.last_included_index,
        }
    }

    // -----------------------------------------------------------------------
    // Decommission / Join
    // -----------------------------------------------------------------------

    /// Updates the decommissioned flag. If `force_false`, the flag is set
    /// to false. Otherwise, the node is marked decommissioned when it is
    /// not a member of the active configuration or is a learner.
    pub fn update_decommissioned(&mut self, force_false: bool) {
        if force_false {
            self.decommissioned = false;
            return;
        }
        if !self.is_peer_in_active_config() || self.cluster_configuration.is_learner(&self.me.id) {
            self.decommissioned = true;
        }
    }

    pub fn is_decommissioned(&self) -> bool {
        self.decommissioned
    }

    /// Enables "joining" mode — the node is a prospective member that
    /// receives log replication but does not start elections.
    pub fn enable_joining_mode(&mut self) {
        self.joining = true;
        self.decommissioned = false;
    }

    pub fn is_joining(&self) -> bool {
        self.joining
    }

    // -----------------------------------------------------------------------
    // Client command submission
    // -----------------------------------------------------------------------

    /// Submits a client command to the Raft log. Only the leader accepts
    /// writes — callers should redirect to the current leader on failure.
    /// Returns the log index where the command was appended.
    ///
    /// The command is not immediately committed; callers must wait for the
    /// commit index to reach the returned index.
    pub fn submit_command(&mut self, command: Vec<u8>) -> Result<u64, String> {
        if self.state != NodeState::Leader {
            return Err("not leader".to_string());
        }

        self.log_store.append(vec![LogEntry::new(
            self.current_term,
            self.me.id.clone(),
            command,
        )]);

        // Immediately attempt to commit — if a majority is reachable,
        // the entry commits without waiting for the next heartbeat.
        self.advance_commit_index_from_majority();
        Ok(self.log_store.last_index())
    }

    // -----------------------------------------------------------------------
    // Linearizable read barrier
    // -----------------------------------------------------------------------

    /// Returns true when the leader can serve a linearizable read without
    /// an additional round-trip to a majority of followers.
    ///
    /// Two conditions are checked:
    /// 1. **Read lease**: the leader has successfully contacted a majority
    ///    within `linearizable_read_lease_millis`, so no other leader could
    ///    have been elected with a higher term.
    /// 2. **Contact majority**: a majority of the active configuration's
    ///    voters have been contacted within `linearizable_read_timeout_millis`.
    ///
    /// The quorum check uses `hasJointMajority()`, so during joint
    /// consensus a read requires contact with majorities of **both** the
    /// old and new configurations.
    pub fn can_serve_linearizable_read(&self) -> bool {
        if self.state != NodeState::Leader {
            return false;
        }

        let now_millis = now_millis();
        let config = self.active_configuration();

        let mut contacted: HashSet<String> = HashSet::new();
        contacted.insert(self.me.id.clone());

        for (peer_id, last_contact) in &self.last_follower_contact_millis {
            let elapsed = if now_millis > *last_contact {
                now_millis - last_contact
            } else {
                0
            };
            if elapsed < self.linearizable_read_timeout_millis {
                contacted.insert(peer_id.clone());
            }
        }

        config.has_joint_majority(&contacted)
    }

    // -- Internal bookkeeping --

    fn trim_applied_command_results(&mut self) {
        let keep_from = self.commit_index.saturating_sub(1024);
        self.applied_command_results = self.applied_command_results.split_off(&keep_from);
    }
}

// -----------------------------------------------------------------------
// Request/Response types (internal domain types, mapped to protobuf by
// the runtime handler)
// -----------------------------------------------------------------------

/// Sent by a candidate to request votes from peers.
#[derive(Debug, Clone)]
pub struct VoteRequest {
    /// Candidate's term.
    pub term: u64,
    /// Candidate's peer id.
    pub candidate_id: String,
    /// Candidate's last log index (for election restriction check).
    pub last_log_index: u64,
    /// Term of candidate's last log entry.
    pub last_log_term: u64,
}

/// A peer's response to a RequestVote RPC.
#[derive(Debug, Clone)]
pub struct VoteResponse {
    /// Responding peer id.
    pub peer_id: String,
    /// Responder's current term (for the candidate to update itself).
    pub term: u64,
    /// True if the vote was granted.
    pub vote_granted: bool,
    /// Responder's current term (redundant with `term`; matches Java layout).
    pub current_term: u64,
}

/// Sent by a leader to replicate log entries and as a heartbeat.
#[derive(Debug, Clone)]
pub struct AppendEntriesRequest {
    /// Leader's term.
    pub term: u64,
    /// Leader's peer id (so followers can redirect clients).
    pub leader_id: String,
    /// Index of log entry immediately preceding new entries.
    pub prev_log_index: u64,
    /// Term of `prev_log_index` entry.
    pub prev_log_term: u64,
    /// Leader's commit index.
    pub leader_commit: u64,
    /// Log entries to append (empty for heartbeat).
    pub entries: Vec<LogEntry>,
}

/// A follower's response to an AppendEntries RPC.
#[derive(Debug, Clone)]
pub struct AppendEntriesResponse {
    /// Follower's current term (for the leader to update itself).
    pub term: u64,
    /// Responding peer id.
    pub peer_id: String,
    /// True if the follower's log matched at `prev_log_index`.
    pub success: bool,
    /// Follower's last log index (used for conflict resolution).
    /// -1 if the rejection was for a reason other than log mismatch.
    pub match_index: i64,
}

/// Streaming snapshot chunk sent from leader to follower.
#[derive(Debug, Clone)]
pub struct InstallSnapshotRequest {
    /// Leader's term.
    pub term: u64,
    /// Leader's peer id.
    pub leader_id: String,
    /// The last log index included in this snapshot.
    pub last_included_index: u64,
    /// Term of `last_included_index`.
    pub last_included_term: u64,
    /// Byte offset of this chunk in the snapshot.
    pub offset: u64,
    /// Snapshot payload bytes for this chunk.
    pub data: Vec<u8>,
    /// True if this is the final chunk.
    pub done: bool,
}

/// A follower's response to an InstallSnapshot chunk.
#[derive(Debug, Clone)]
pub struct InstallSnapshotResponse {
    /// Follower's current term.
    pub term: u64,
    /// Responding peer id.
    pub peer_id: String,
    /// True if the chunk was accepted.
    pub success: bool,
    /// The last included index from the request (echoed back).
    pub last_included_index: u64,
}

/// Returns the current wall-clock epoch millis. Used for follower
/// contact tracking and read-lease calculations.
fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
