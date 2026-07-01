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

use std::net::SocketAddr;

/// Membership role of a Raft peer. Voters participate in quorum decisions
/// (elections, commit-index advancement, read barriers); learners receive
/// log replication but are excluded from all majority calculations.
///
/// Role is carried in the cluster configuration and replicated through the
/// log during membership transitions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Role {
    /// Voting member — counted in quorum majorities.
    Voter,
    /// Non-voting learner — receives replication without voting rights.
    Learner,
}

/// A Raft cluster member. Peer identity is the logical Raft id — endpoint
/// resolution (host:port) is a transport-layer concern stored alongside.
///
/// In the configuration, the same logical peer may appear with different
/// addresses across reconfigurations; the `id` is the stable key.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Peer {
    /// Logical Raft identifier, stable across reconfigurations.
    pub id: String,
    /// Network address for TCP transport.
    pub address: SocketAddr,
    /// Voting or learning role in the current configuration.
    pub role: Role,
}

impl Peer {
    pub fn new(id: String, address: SocketAddr, role: Role) -> Self {
        Self { id, address, role }
    }

    /// Convenience constructor for a voting peer.
    pub fn voter(id: String, address: SocketAddr) -> Self {
        Self {
            id,
            address,
            role: Role::Voter,
        }
    }

    pub fn is_voter(&self) -> bool {
        matches!(self.role, Role::Voter)
    }

    pub fn is_learner(&self) -> bool {
        matches!(self.role, Role::Learner)
    }
}

/// One replicated entry in the Raft log. Each entry carries the term when
/// it was created and the peer that originated it, so the commit/apply path
/// can attribute results and detect no-op entries.
///
/// The protobuf `LogEntry` carries term + payload but not an explicit index;
/// the index is implicit in the log position. Rust keeps entries 1-indexed
/// like the Raft paper, with index 0 reserved as "no entry."
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntry {
    /// Term in which this entry was created (leader's currentTerm).
    pub term: u64,
    /// Originating peer id.
    pub peer_id: String,
    /// Opaque command payload. An empty Vec indicates a no-op (leader
    /// election placeholder) entry.
    pub data: Vec<u8>,
}

impl LogEntry {
    pub fn new(term: u64, peer_id: String, data: Vec<u8>) -> Self {
        Self {
            term,
            peer_id,
            data,
        }
    }

    /// Creates a no-op entry for leader-election anchoring (Figure 2 leader
    /// completeness). The leader appends a no-op in its own term after winning
    /// an election so that prior entries become indirectly committed.
    pub fn noop(term: u64, peer_id: String) -> Self {
        Self {
            term,
            peer_id,
            data: Vec::new(),
        }
    }

    /// No-op entries carry empty data and are skipped during commit/apply.
    pub fn is_noop(&self) -> bool {
        self.data.is_empty()
    }
}

/// Node role in the Raft consensus state machine. Transitions follow the
/// Figure 4 state diagram: Follower → Candidate (timeout) → Leader (quorum)
/// and Leader/Candidate → Follower (higher term discovered).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeState {
    Follower,
    Candidate,
    Leader,
}

/// Small durable state that Raft must persist outside the replicated log.
/// Survives restarts so the node cannot vote twice in the same term or
/// accept stale entries after recovery.
#[derive(Debug, Clone)]
pub struct PersistentState {
    /// Latest term the node has seen (initialized to 0, monotonically increases).
    pub current_term: u64,
    /// Candidate peer id this node voted for in `current_term`, or None.
    pub voted_for: Option<String>,
}
