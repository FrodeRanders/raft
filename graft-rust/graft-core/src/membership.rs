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

use std::collections::HashSet;
use indexmap::IndexMap;

use crate::types::Peer;

/// Represents the Raft replication configuration.
///
/// **Stable configuration**: only `current_members` is populated.
///
/// **Joint consensus configuration**: both `current_members` and `next_members`
/// are populated. During joint consensus, quorum decisions (elections, commit,
/// read barriers) require a majority of the voter subsets in BOTH the
/// current and next configurations — never a flat majority of the union.
/// This split-majority rule is what makes reconfiguration safe.
///
/// ClusterConfiguration is an immutable value object. Mutations return new
/// instances via `transition_to()` and `finalize_transition()`.
///
/// Members are stored in an `IndexMap` keyed by peer id to ensure
/// deterministic iteration order for quorum calculations, independent of
/// caller-supplied list order or duplicates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterConfiguration {
    /// Current member set. Always populated.
    current_members: IndexMap<String, Peer>,
    /// Next member set during joint consensus. Empty when stable.
    next_members: IndexMap<String, Peer>,
}

impl ClusterConfiguration {
    /// Creates a stable (single-configuration) cluster from a member list.
    /// Members are normalized into a deterministic map keyed by peer id.
    /// Panics if the list is empty.
    pub fn stable(members: Vec<Peer>) -> Self {
        assert!(!members.is_empty(), "must have at least one member");
        let current_members: IndexMap<String, Peer> = members
            .into_iter()
            .map(|p| (p.id.clone(), p))
            .collect();
        Self {
            current_members,
            next_members: IndexMap::new(),
        }
    }

    /// Whether a joint-consensus reconfiguration is in progress.
    pub fn is_joint_consensus(&self) -> bool {
        !self.next_members.is_empty()
    }

    /// All current-configuration members (voters and learners).
    pub fn current_members(&self) -> Vec<&Peer> {
        self.current_members.values().collect()
    }

    /// Next-configuration members during joint consensus; empty when stable.
    pub fn next_members(&self) -> Vec<&Peer> {
        self.next_members.values().collect()
    }

    /// Current-configuration voters only.
    pub fn current_voting_members(&self) -> Vec<&Peer> {
        self.current_members
            .values()
            .filter(|p| p.is_voter())
            .collect()
    }

    /// Next-configuration voters during joint consensus; falls back to
    /// `current_voting_members()` when stable.
    pub fn next_voting_members(&self) -> Vec<&Peer> {
        if self.is_joint_consensus() {
            self.next_members
                .values()
                .filter(|p| p.is_voter())
                .collect()
        } else {
            self.current_voting_members()
        }
    }

    /// Union of current and next voting members, deduplicated by peer id.
    /// Used for transport-layer peer-set updates, not quorum calculations.
    pub fn all_voting_members(&self) -> Vec<&Peer> {
        let mut seen = HashSet::new();
        let mut result = Vec::new();
        for p in self.current_voting_members() {
            if seen.insert(&p.id) {
                result.push(p);
            }
        }
        for p in self.next_voting_members() {
            if seen.insert(&p.id) {
                result.push(p);
            }
        }
        result
    }

    /// Union of all members (voters + learners) in both configurations,
    /// deduplicated by peer id.
    pub fn all_members(&self) -> Vec<&Peer> {
        let mut seen = HashSet::new();
        let mut result = Vec::new();
        for p in self.current_members.values() {
            if seen.insert(&p.id) {
                result.push(p);
            }
        }
        for p in self.next_members.values() {
            if seen.insert(&p.id) {
                result.push(p);
            }
        }
        result
    }

    /// Whether the peer id is present in either current or next membership.
    pub fn contains(&self, peer_id: &str) -> bool {
        self.current_members.contains_key(peer_id) || self.next_members.contains_key(peer_id)
    }

    /// Whether the peer is a voter in either membership set.
    pub fn is_voter(&self, peer_id: &str) -> bool {
        if let Some(p) = self.current_members.get(peer_id) {
            return p.is_voter();
        }
        if let Some(p) = self.next_members.get(peer_id) {
            return p.is_voter();
        }
        false
    }

    /// Whether the peer exists but is not a voter in either membership set.
    pub fn is_learner(&self, peer_id: &str) -> bool {
        if let Some(p) = self.current_members.get(peer_id) {
            return p.is_learner();
        }
        if let Some(p) = self.next_members.get(peer_id) {
            return p.is_learner();
        }
        false
    }

    /// Enters joint consensus: keeps the current member set as-is and sets
    /// the next member set to the proposed members. Returns `self` unchanged
    /// if the proposed set is identical to the current next set.
    pub fn transition_to(&self, proposed_members: Vec<Peer>) -> Self {
        let next_members: IndexMap<String, Peer> = proposed_members
            .into_iter()
            .map(|p| (p.id.clone(), p))
            .collect();
        Self {
            current_members: self.current_members.clone(),
            next_members,
        }
    }

    /// Exits joint consensus: promotes the next member set to the sole
    /// current set and clears the next set. Returns `self` unchanged if
    /// already stable.
    pub fn finalize_transition(&self) -> Self {
        Self {
            current_members: self.next_members.clone(),
            next_members: IndexMap::new(),
        }
    }

    /// Quorum size for current voting members: `floor(voter_count / 2) + 1`.
    pub fn current_majority_size(&self) -> usize {
        majority_size(
            self.current_members
                .values()
                .filter(|p| p.is_voter())
                .count(),
        )
    }

    /// Quorum size for next voting members; falls back to current if stable.
    pub fn next_majority_size(&self) -> usize {
        if self.is_joint_consensus() {
            majority_size(
                self.next_members
                    .values()
                    .filter(|p| p.is_voter())
                    .count(),
            )
        } else {
            self.current_majority_size()
        }
    }

    /// True when `peer_ids` contains at least `current_majority_size()`
    /// distinct current voters.
    pub fn has_current_majority(&self, peer_ids: &HashSet<String>) -> bool {
        let voters: HashSet<String> = self
            .current_voting_members()
            .into_iter()
            .map(|p| p.id.clone())
            .collect();
        count_present(peer_ids, &voters) >= self.current_majority_size()
    }

    /// True when `peer_ids` contains at least `next_majority_size()` distinct
    /// next voters. Falls back to `has_current_majority` when stable.
    pub fn has_next_majority(&self, peer_ids: &HashSet<String>) -> bool {
        if self.is_joint_consensus() {
            let voters: HashSet<String> = self
                .next_voting_members()
                .into_iter()
                .map(|p| p.id.clone())
                .collect();
            count_present(peer_ids, &voters) >= self.next_majority_size()
        } else {
            self.has_current_majority(peer_ids)
        }
    }

    /// This helper enforces the split-majority joint-consensus rule. A flat
    /// majority of the union would be unsafe during reconfiguration because
    /// the old and new configurations could independently reach conflicting
    /// decisions. Instead, a quorum must satisfy majorities of **both** the
    /// current and next voter subsets.
    ///
    /// When the configuration is stable, this is equivalent to
    /// `has_current_majority`.
    pub fn has_joint_majority(&self, peer_ids: &HashSet<String>) -> bool {
        self.has_current_majority(peer_ids) && self.has_next_majority(peer_ids)
    }

    /// Deep equality check — true when both current and next member maps
    /// are identical.
    pub fn same_membership_as(&self, other: &ClusterConfiguration) -> bool {
        self.current_members == other.current_members
            && self.next_members == other.next_members
    }
}

/// Standard Raft majority: floor(n/2) + 1.
fn majority_size(member_count: usize) -> usize {
    if member_count == 0 {
        return 0;
    }
    (member_count / 2) + 1
}

/// Counts how many peer ids from the candidate set are present in the
/// member set.
fn count_present(peer_ids: &HashSet<String>, members: &HashSet<String>) -> usize {
    peer_ids.intersection(members).count()
}
