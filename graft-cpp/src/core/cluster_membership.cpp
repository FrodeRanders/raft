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

#include "graft/core/cluster_membership.hpp"

#include <algorithm>

namespace graft {
    std::vector<raft::PeerSpec> ClusterMembership::normalize_member_specs(std::vector<raft::PeerSpec> members) {
        // Deterministic ordering matters because membership appears in snapshots,
        // summaries and tests. Sorting also makes duplicate elimination predictable.
        std::sort(members.begin(), members.end(), [](const raft::PeerSpec &left, const raft::PeerSpec &right) {
            return left.id() < right.id();
        });
        std::vector<raft::PeerSpec> normalized;
        normalized.reserve(members.size());
        std::unordered_set<std::string> seen;
        for (auto &member: members) {
            if (member.id().empty()) {
                continue;
            }
            if (member.role().empty()) {
                // Missing role means voter for compatibility with older peer lists that
                // only contained ids/endpoints.
                member.set_role("VOTER");
            }
            if (seen.insert(member.id()).second) {
                normalized.push_back(std::move(member));
            }
        }
        return normalized;
    }

    std::vector<std::string> ClusterMembership::current_voting_members(
        const std::string &local_peer_id,
        bool decommissioned,
        const std::vector<std::string> &voting_peers,
        const std::vector<raft::PeerSpec> &current_members) {
        std::vector<std::string> voters;
        if (current_members.empty()) {
            // Legacy/static configuration path: the local node plus voting_peers form
            // the current voter set.
            voters.reserve(voting_peers.size() + 1);
            if (!decommissioned) {
                voters.push_back(local_peer_id);
            }
            voters.insert(voters.end(), voting_peers.begin(), voting_peers.end());
        } else {
            voters.reserve(current_members.size());
            for (const auto &member: current_members) {
                if (member.role() == "VOTER") {
                    voters.push_back(member.id());
                }
            }
        }
        return sorted_unique(std::move(voters));
    }

    std::vector<std::string> ClusterMembership::next_voting_members(
        bool joint_consensus,
        const std::vector<std::string> &current_voters,
        const std::vector<raft::PeerSpec> &next_members) {
        if (!joint_consensus || next_members.empty()) {
            return current_voters;
        }
        std::vector<std::string> voters;
        voters.reserve(next_members.size());
        for (const auto &member: next_members) {
            if (member.role() == "VOTER") {
                voters.push_back(member.id());
            }
        }
        return sorted_unique(std::move(voters));
    }

    std::vector<std::string> ClusterMembership::active_voting_members(
        const std::vector<std::string> &current_voters,
        const std::vector<std::string> &next_voters) {
        auto voters = current_voters;
        voters.insert(voters.end(), next_voters.begin(), next_voters.end());
        return sorted_unique(std::move(voters));
    }

    bool ClusterMembership::has_joint_majority(const std::unordered_set<std::string> &peer_ids,
                                               const std::vector<std::string> &current_voters,
                                               const std::vector<std::string> &next_voters,
                                               bool joint_consensus) {
        auto has_majority = [&peer_ids](const std::vector<std::string> &voters) {
            if (voters.empty()) {
                return false;
            }
            std::size_t present = 0;
            for (const auto &voter: voters) {
                present += peer_ids.contains(voter) ? 1 : 0;
            }
            return present >= ((voters.size() / 2) + 1);
        };
        if (!has_majority(current_voters)) {
            return false;
        }
        // Stable config: current majority is enough. Joint config: both sides must
        // independently have majorities.
        return !joint_consensus || has_majority(next_voters);
    }

    std::vector<std::string> ClusterMembership::sorted_unique(std::vector<std::string> values) {
        std::sort(values.begin(), values.end());
        values.erase(std::unique(values.begin(), values.end()), values.end());
        return values;
    }
} // namespace graft
