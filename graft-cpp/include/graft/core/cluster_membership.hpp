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
#pragma once

#include <string>
#include <unordered_set>
#include <vector>

#include "raft.pb.h"

namespace graft {
    class ClusterMembership {
    public:
        static std::vector<raft::PeerSpec> normalize_member_specs(std::vector<raft::PeerSpec> members);

        static std::vector<std::string> current_voting_members(const std::string &local_peer_id,
                                                               bool decommissioned,
                                                               const std::vector<std::string> &voting_peers,
                                                               const std::vector<raft::PeerSpec> &current_members);

        static std::vector<std::string> next_voting_members(bool joint_consensus,
                                                            const std::vector<std::string> &current_voters,
                                                            const std::vector<raft::PeerSpec> &next_members);

        static std::vector<std::string> active_voting_members(const std::vector<std::string> &current_voters,
                                                              const std::vector<std::string> &next_voters);

        static bool has_joint_majority(const std::unordered_set<std::string> &peer_ids,
                                       const std::vector<std::string> &current_voters,
                                       const std::vector<std::string> &next_voters,
                                       bool joint_consensus);

    private:
        static std::vector<std::string> sorted_unique(std::vector<std::string> values);
    };
} // namespace graft
