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

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use graft_core::raft_node::RaftNode;
use graft_core::types::{NodeState, Peer};
use graft_proto::raft;
use graft_transport::client::RaftClient;
use prost::Message;
use rand::Rng;
use tokio::time::{interval, timeout, MissedTickBehavior};
use tracing::{debug, info};

/// The active Raft runtime — owns the timer-driven event loop that sends
/// outbound RPCs (VoteRequests during elections, AppendEntries as
/// heartbeats/replication), processes responses, and advances the Raft
/// node's commit index.
///
/// This is the Rust equivalent of the C++ `RaftRuntime` class. It
/// bridges the gap between the passive `RaftNode` (which only evaluates
/// incoming RPCs) and a fully-functional cluster member that actively
/// participates in elections and replication.
pub struct RaftRuntime {
    /// Shared Raft consensus state.
    pub node: Arc<parking_lot::Mutex<RaftNode>>,
    /// Outbound RPC client for sending messages to peers.
    client: Arc<RaftClient>,
    /// Copy of the peer address map for fast lookups in the timer loops.
    peer_addrs: Arc<parking_lot::Mutex<HashMap<String, SocketAddr>>>,
    /// Maximum time to wait for an RPC response before considering the
    /// peer unreachable.
    rpc_timeout: Duration,
}

impl RaftRuntime {
    /// Creates a new runtime tied to the given node and transport client.
    /// The peer address map should be populated via `set_peers()` before
    /// calling `run()`.
    pub fn new(
        node: Arc<parking_lot::Mutex<RaftNode>>,
        client: Arc<RaftClient>,
    ) -> Self {
        Self {
            node,
            client,
            peer_addrs: Arc::new(parking_lot::Mutex::new(HashMap::new())),
            rpc_timeout: Duration::from_secs(3),
        }
    }

    /// Updates the peer address book. Called at startup and whenever the
    /// cluster configuration changes (JOIN, JOINT, FINALIZE).
    pub fn set_peers(&self, peers: HashMap<String, SocketAddr>) {
        self.client.set_known_peers(peers.clone());
        let mut addrs = self.peer_addrs.lock();
        *addrs = peers;
    }

    /// Runs the active event loop: election timeout checker and heartbeat
    /// broadcaster. Blocks the calling task forever.
    ///
    /// - Election timer fires at randomized intervals (1500–3000ms base)
    ///   with deadline tracking instead of fixed period, matching the
    ///   C++ `run_active_persistent_server` pattern.
    /// - Heartbeat timer fires every 750ms. If the node is leader, sends
    ///   AppendEntries (or InstallSnapshot) to every remote peer, decodes
    ///   responses, and calls `handle_append_entries_response()` to update
    ///   matchIndex/nextIndex and advance the commit index.
    pub async fn run(&self) {
        let election_base = Duration::from_millis(1500);
        let election_range = Duration::from_millis(1500); // 1500–3000ms total
        let heartbeat_interval = Duration::from_millis(750);

        let mut election_timer = interval(self.random_election_timeout(election_base, election_range));
        election_timer.set_missed_tick_behavior(MissedTickBehavior::Delay);

        let mut heartbeat_timer = interval(heartbeat_interval);
        heartbeat_timer.set_missed_tick_behavior(MissedTickBehavior::Delay);

        info!("RaftRuntime active loop started");

        loop {
            tokio::select! {
                _ = election_timer.tick() => {
                    self.on_election_tick();
                    // Re-randomize the interval for the next round so that
                    // nodes don't synchronize their election timers.
                    election_timer = interval(self.random_election_timeout(election_base, election_range));
                    election_timer.set_missed_tick_behavior(MissedTickBehavior::Delay);
                }
                _ = heartbeat_timer.tick() => {
                    self.on_heartbeat_tick().await;
                }
            }
        }
    }

    // -- Election tick --

    fn on_election_tick(&self) {
        let now = std::time::Instant::now();
        let state;
        let term;
        let peer_id;
        let last_log_index;
        let last_log_term;
        let voting_peers;

        {
            let n = self.node.lock();
            state = n.state;
            term = n.current_term;
            peer_id = n.me.id.clone();
            last_log_index = n.log_store.last_index();
            last_log_term = n.log_store.last_term();
            voting_peers = n.active_configuration()
                .current_voting_members()
                .into_iter()
                .filter(|p| p.id != n.me.id)
                .cloned()
                .collect::<Vec<Peer>>();
        }

        // Only start an election if we're not already leader and the
        // timeout has elapsed.
        if state == NodeState::Leader {
            return;
        }

        {
            let mut n = self.node.lock();
            if !n.has_timed_out(now) {
                return;
            }

            // Start the election: increment term, vote for self, reset timer.
            // Figure 2 "Candidates": on conversion to candidate.
            n.state = NodeState::Candidate;
            n.known_leader_id = None;
            n.current_term += 1;
            n.persistent_state.set_current_term(n.current_term);
            n.voted_for = Some(n.me.id.clone());
            n.persistent_state.set_voted_for(Some(n.me.id.clone()));
            n.election_sequence_counter += 1;
            n.refresh_timeout(now);

            info!("{} starting election for term {}, lastIndex={}, lastTerm={}",
                peer_id, n.current_term, last_log_index, last_log_term);
        }

        let election_term = term + 1; // term was incremented above
        let self_id = peer_id.clone();

        let rt = tokio::runtime::Handle::current();
        let node = self.node.clone();
        let client = self.client.clone();
        let rpc_timeout = self.rpc_timeout;
        let peer_addrs = self.peer_addrs.clone();

        // Send VoteRequests to all remote voting peers concurrently.
        rt.spawn(async move {
            let mut votes_granted: HashSet<String> = HashSet::new();
            votes_granted.insert(self_id.clone());

            let addresses = peer_addrs.lock().clone();

            for peer in &voting_peers {
                let addr = match addresses.get(&peer.id) {
                    Some(a) => *a,
                    None => continue,
                };

                let req = raft::VoteRequest {
                    term: election_term as i64,
                    candidate_id: self_id.clone(),
                    last_log_index: last_log_index as i64,
                    last_log_term: last_log_term as i64,
                };

                let result = timeout(rpc_timeout, client.send_rpc(
                    addr,
                    "VoteRequest",
                    req.encode_to_vec(),
                )).await;

                match result {
                    Ok(Ok(payload)) => {
                        if let Ok(resp) = raft::VoteResponse::decode(&payload[..]) {
                            let mut n = node.lock();
                            // Figure 2 "All Servers": higher term → step down.
                            let max_term = std::cmp::max(n.current_term, resp.current_term as u64);
                            if max_term > n.current_term {
                                n.current_term = max_term;
                                n.persistent_state.set_current_term(max_term);
                                n.become_follower(std::time::Instant::now());
                                return;
                            }
                            if resp.vote_granted && n.current_term == election_term {
                                votes_granted.insert(resp.peer_id.clone());
                            }
                        }
                    }
                    Ok(Err(e)) => {
                        debug!("VoteRequest to {} failed: {}", peer.id, e);
                    }
                    Err(_) => {
                        debug!("VoteRequest to {} timed out", peer.id);
                    }
                }
            }

            // Check if we won the election (joint-majority quorum).
            let mut n = node.lock();
            if n.current_term != election_term || n.state != NodeState::Candidate {
                return; // stepped down during voting
            }
            let config = n.active_configuration().clone();
            if config.has_joint_majority(&votes_granted) {
                info!("{} won election for term {}", self_id, election_term);
                n.state = NodeState::Leader;
                n.known_leader_id = Some(self_id.clone());
                n.election_sequence_counter = 0;
                n.initialize_leader_replication_state();
                n.append_leader_noop_entry();
                n.advance_commit_index_from_majority();
            }
        });
    }

    // -- Heartbeat tick --

    async fn on_heartbeat_tick(&self) {
        let (_state, term, leader_id, commit_index, snapshot_index, snapshot_term, snapshot_data, peer_list) = {
            let n = self.node.lock();
            if n.state != NodeState::Leader {
                return;
            }
            let peers: Vec<(String, u64)> = n.next_index.iter()
                .map(|(id, ni)| (id.clone(), *ni))
                .collect();
            (
                n.state,
                n.current_term,
                n.me.id.clone(),
                n.commit_index,
                n.log_store.snapshot_index(),
                n.log_store.snapshot_term(),
                n.log_store.snapshot_data(),
                peers,
            )
        };

        let addresses = self.peer_addrs.lock().clone();
        let node = self.node.clone();
        let client = self.client.clone();
        let rpc_timeout = self.rpc_timeout;

        for (peer_id, next_idx) in &peer_list {
            let addr = match addresses.get(peer_id) {
                Some(a) => *a,
                None => continue,
            };

            // Check if this follower is currently receiving a snapshot. If so,
            // send the next chunk rather than an AppendEntries.
            let snapshot_active = {
                let n = node.lock();
                n.snapshot_offsets.contains_key(peer_id)
            };

            if snapshot_active {
                self.send_next_snapshot_chunk(
                    peer_id, addr, term, &leader_id,
                    snapshot_index, snapshot_term, &snapshot_data, rpc_timeout,
                ).await;
                continue;
            }

            if *next_idx <= snapshot_index && !snapshot_data.is_empty() {
                // Follower is behind — start snapshot streaming. Send the first
                // chunk (offset=0) and record the offset so we continue on the
                // next heartbeat tick.
                self.start_snapshot_stream(
                    peer_id, addr, term, &leader_id,
                    snapshot_index, snapshot_term, &snapshot_data, rpc_timeout,
                ).await;
                continue;
            } else {
                // Standard AppendEntries heartbeat/replication.
                let prev_log_idx = if *next_idx > 0 { *next_idx - 1 } else { 0 };

                let (prev_log_term, entries) = {
                    let n = node.lock();
                    let plt = if prev_log_idx == 0 { 0 } else { n.log_store.term_at(prev_log_idx) };
                    let ents = n.log_store.entries_from(*next_idx);
                    (plt, ents)
                };

                let proto_entries: Vec<raft::LogEntry> = entries.iter().map(|e| {
                    raft::LogEntry { term: e.term as i64, peer_id: e.peer_id.clone(), data: e.data.clone() }
                }).collect();
                let entry_count = proto_entries.len() as u64;

                let req = raft::AppendEntriesRequest {
                    term: term as i64,
                    leader_id: leader_id.clone(),
                    prev_log_index: prev_log_idx as i64,
                    prev_log_term: prev_log_term as i64,
                    leader_commit: commit_index as i64,
                    entries: proto_entries,
                };
                let payload = req.encode_to_vec();

                let result = timeout(rpc_timeout, client.send_rpc(
                    addr, "AppendEntriesRequest", payload,
                )).await;

                match result {
                    Ok(Ok(resp_bytes)) => {
                        if let Ok(resp) = raft::AppendEntriesResponse::decode(&resp_bytes[..]) {
                            let mut n = node.lock();
                            let now = std::time::Instant::now();

                            // Figure 2: higher term → step down.
                            if resp.term as u64 > n.current_term {
                                n.current_term = resp.term as u64;
                                n.persistent_state.set_current_term(resp.term as u64);
                                n.become_follower(now);
                                return;
                            }

                            if resp.success {
                                let advanced = prev_log_idx + entry_count;
                                n.next_index.insert(peer_id.clone(), advanced + 1);
                                n.match_index.insert(peer_id.clone(), advanced);
                                n.record_follower_replication_success(peer_id);
                                n.advance_commit_index_from_majority();
                                n.maybe_auto_finalize_joint_configuration();
                            } else {
                                n.record_follower_replication_failure(peer_id);
                                // Standard conflict resolution: decrement nextIndex.
                                let floor = n.log_store.snapshot_index() + 1;
                                let new_next = std::cmp::max(floor, next_idx.saturating_sub(1));
                                n.next_index.insert(peer_id.clone(), new_next);
                            }
                        }
                    }
                    Ok(Err(e)) => {
                        debug!("AppendEntries to {} failed: {}", peer_id, e);
                        let mut n = node.lock();
                        n.record_follower_replication_failure(peer_id);
                    }
                    Err(_) => {
                        debug!("AppendEntries to {} timed out", peer_id);
                        let mut n = node.lock();
                        n.record_follower_replication_failure(peer_id);
                    }
                }
            }
        }
    }

    // -- Snapshot streaming (leader side) --

    /// Starts streaming a snapshot to a follower. Sends the first chunk
    /// (offset=0) and records the offset so subsequent heartbeat ticks
    /// continue from where we left off.
    async fn start_snapshot_stream(
        &self,
        peer_id: &str,
        addr: SocketAddr,
        term: u64,
        leader_id: &str,
        snapshot_index: u64,
        snapshot_term: u64,
        snapshot_data: &[u8],
        rpc_timeout: Duration,
    ) {
        let chunk_size = {
            let n = self.node.lock();
            n.snapshot_chunk_bytes as usize
        };
        let total_len = snapshot_data.len();
        let end = std::cmp::min(chunk_size, total_len);
        let done = end >= total_len;
        let chunk = if end <= total_len { &snapshot_data[0..end] } else { &[] };

        let req = raft::InstallSnapshotRequest {
            term: term as i64,
            leader_id: leader_id.to_string(),
            last_included_index: snapshot_index as i64,
            last_included_term: snapshot_term as i64,
            offset: 0,
            snapshot_data: chunk.to_vec(),
            done,
        };
        let payload = req.encode_to_vec();

        let result = timeout(rpc_timeout, self.client.send_rpc(addr, "InstallSnapshotRequest", payload)).await;

        match result {
            Ok(Ok(resp_bytes)) => {
                if let Ok(resp) = raft::InstallSnapshotResponse::decode(&resp_bytes[..]) {
                    let mut n = self.node.lock();
                    let now = std::time::Instant::now();
                    if resp.term as u64 > n.current_term {
                        n.current_term = resp.term as u64;
                        n.persistent_state.set_current_term(resp.term as u64);
                        n.become_follower(now);
                        return;
                    }
                    if resp.success {
                        if done {
                            n.snapshot_offsets.remove(peer_id);
                            n.next_index.insert(peer_id.to_string(), snapshot_index + 1);
                            n.match_index.insert(peer_id.to_string(), snapshot_index);
                            n.advance_commit_index_from_majority();
                        } else {
                            n.snapshot_offsets.insert(peer_id.to_string(), end as u64);
                        }
                    }
                }
            }
            Ok(Err(e)) => {
                debug!("InstallSnapshot to {} failed: {}", peer_id, e);
            }
            Err(_) => {
                debug!("InstallSnapshot to {} timed out", peer_id);
            }
        }
    }

    /// Continues streaming a snapshot chunk to a follower. Reads the
    /// current offset from `snapshot_offsets`, sends the next chunk,
    /// and advances the offset on success.
    async fn send_next_snapshot_chunk(
        &self,
        peer_id: &str,
        addr: SocketAddr,
        term: u64,
        leader_id: &str,
        snapshot_index: u64,
        snapshot_term: u64,
        snapshot_data: &[u8],
        rpc_timeout: Duration,
    ) {
        let (offset, chunk_size, total_len) = {
            let n = self.node.lock();
            let off = n.snapshot_offsets.get(peer_id).copied().unwrap_or(0);
            (off, n.snapshot_chunk_bytes as usize, snapshot_data.len())
        };

        let start = offset as usize;
        if start >= total_len {
            let mut n = self.node.lock();
            n.snapshot_offsets.remove(peer_id);
            n.next_index.insert(peer_id.to_string(), snapshot_index + 1);
            n.match_index.insert(peer_id.to_string(), snapshot_index);
            return;
        }

        let end = std::cmp::min(start + chunk_size, total_len);
        let done = end >= total_len;
        let chunk = &snapshot_data[start..end];

        let req = raft::InstallSnapshotRequest {
            term: term as i64,
            leader_id: leader_id.to_string(),
            last_included_index: snapshot_index as i64,
            last_included_term: snapshot_term as i64,
            offset: offset as i64,
            snapshot_data: chunk.to_vec(),
            done,
        };
        let payload = req.encode_to_vec();

        let result = timeout(rpc_timeout, self.client.send_rpc(addr, "InstallSnapshotRequest", payload)).await;

        match result {
            Ok(Ok(resp_bytes)) => {
                if let Ok(resp) = raft::InstallSnapshotResponse::decode(&resp_bytes[..]) {
                    let mut n = self.node.lock();
                    let now = std::time::Instant::now();
                    if resp.term as u64 > n.current_term {
                        n.current_term = resp.term as u64;
                        n.persistent_state.set_current_term(resp.term as u64);
                        n.become_follower(now);
                        return;
                    }
                    if resp.success {
                        if done {
                            n.snapshot_offsets.remove(peer_id);
                            n.next_index.insert(peer_id.to_string(), snapshot_index + 1);
                            n.match_index.insert(peer_id.to_string(), snapshot_index);
                            n.advance_commit_index_from_majority();
                        } else {
                            n.snapshot_offsets.insert(peer_id.to_string(), end as u64);
                        }
                    }
                }
            }
            Ok(Err(e)) => {
                debug!("InstallSnapshot chunk to {} failed: {}", peer_id, e);
            }
            Err(_) => {
                debug!("InstallSnapshot chunk to {} timed out", peer_id);
            }
        }
    }

    // -- Helpers --

    /// Returns a randomized election timeout in the range
    /// `[base, base + range]`. Each call returns a fresh random duration
    /// so that nodes don't synchronize their election timers.
    fn random_election_timeout(&self, base: Duration, range: Duration) -> Duration {
        let mut rng = rand::thread_rng();
        let extra_ms = rng.gen_range(0..=range.as_millis() as u64);
        base + Duration::from_millis(extra_ms)
    }

    /// Replicates a client command: appends to the log and returns
    /// immediately (the next heartbeat cycle will push entries to peers).
    /// The caller should poll `node.commit_index` until it reaches the
    /// returned index if commit confirmation is needed.
    pub fn submit_command(&self, command: Vec<u8>) -> Result<u64, String> {
        let mut n = self.node.lock();
        n.submit_command(command)
    }

    /// Forces a heartbeat round against all voting peers to refresh the
    /// read lease. Returns true if a joint-consensus quorum was contacted
    /// successfully. Called by the RPC handler before serving a
    /// linearizable read query.
    pub async fn refresh_read_barrier(&self) -> bool {
        // Snapshot the peer list under lock quickly.
        let (peer_list, term, leader_id, commit_index, snapshot_index, snapshot_term, snapshot_data) = {
            let n = self.node.lock();
            if n.state != NodeState::Leader {
                return false;
            }
            let peers: Vec<(String, u64)> = n.next_index.iter()
                .map(|(id, ni)| (id.clone(), *ni))
                .collect();
            (
                peers,
                n.current_term,
                n.me.id.clone(),
                n.commit_index,
                n.log_store.snapshot_index(),
                n.log_store.snapshot_term(),
                n.log_store.snapshot_data(),
            )
        };

        let addresses = self.peer_addrs.lock().clone();
        let node = self.node.clone();
        let client = self.client.clone();
        let rpc_timeout = self.rpc_timeout;

        let mut contacted = HashSet::new();
        {
            let n = node.lock();
            contacted.insert(n.me.id.clone());
        }

        for (peer_id, next_idx) in &peer_list {
            let addr = match addresses.get(peer_id) {
                Some(a) => *a,
                None => continue,
            };

            let req = if *next_idx <= snapshot_index && !snapshot_data.is_empty() {
                raft::InstallSnapshotRequest {
                    term: term as i64, leader_id: leader_id.clone(),
                    last_included_index: snapshot_index as i64, last_included_term: snapshot_term as i64,
                    offset: 0, snapshot_data: snapshot_data.clone(), done: true,
                }.encode_to_vec()
            } else {
                let prev_log_idx = if *next_idx > 0 { *next_idx - 1 } else { 0 };
                let (plt, entries) = {
                    let n = node.lock();
                    let t = if prev_log_idx == 0 { 0 } else { n.log_store.term_at(prev_log_idx) };
                    let e = n.log_store.entries_from(*next_idx);
                    (t, e)
                };
                let proto_entries: Vec<raft::LogEntry> = entries.iter().map(|e| {
                    raft::LogEntry { term: e.term as i64, peer_id: e.peer_id.clone(), data: e.data.clone() }
                }).collect();
                raft::AppendEntriesRequest {
                    term: term as i64, leader_id: leader_id.clone(),
                    prev_log_index: prev_log_idx as i64, prev_log_term: plt as i64,
                    leader_commit: commit_index as i64, entries: proto_entries,
                }.encode_to_vec()
            };

            let result = timeout(rpc_timeout, client.send_rpc(addr, "AppendEntriesRequest", req)).await;
            if let Ok(Ok(_resp_bytes)) = result {
                contacted.insert(peer_id.clone());
                let mut n = node.lock();
                n.record_follower_replication_success(peer_id);
            }
        }

        // Check if we contacted a joint-consensus quorum.
        let n = node.lock();
        n.active_configuration().has_joint_majority(&contacted)
    }

    /// Returns the current leader id, if known. Used by the RPC handler
    /// to redirect clients to the leader.
    pub fn leader_id(&self) -> Option<String> {
        self.node.lock().known_leader_id.clone()
    }

    /// Returns true if the local node is in single-node configuration
    /// (no remote peers in the address book). Used by the RPC handler
    /// to allow self-election when the node is alone.
    pub fn is_single_node(&self) -> bool {
        self.peer_addrs.lock().is_empty()
    }

    /// Blocks the calling task until `commit_index` reaches or exceeds
    /// `target_index`, or until `timeout` elapses. Returns true if the
    /// entry was committed within the timeout.
    ///
    /// Called by the RPC handler after a client command has been appended
    /// to the log, so the handler can return the committed result (or
    /// `ACCEPTED_NOT_COMMITTED` on timeout).
    pub async fn wait_for_commit(&self, target_index: u64, deadline: std::time::Duration) -> bool {
        let start = tokio::time::Instant::now();
        loop {
            let ci = self.node.lock().commit_index;
            if ci >= target_index {
                return true;
            }
            if start.elapsed() >= deadline {
                return false;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }
}
