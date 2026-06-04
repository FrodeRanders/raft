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

#include <chrono>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "raft.pb.h"

namespace graft {
    class RaftNode {
    public:
        enum class Role {
            follower,
            candidate,
            leader
        };

        struct Config {
            std::string peer_id;
            std::int64_t current_term{0};
            std::int64_t last_log_index{0};
            std::int64_t last_log_term{0};
            std::int64_t commit_index{0};
            std::int64_t snapshot_index{0};
            std::int64_t snapshot_term{0};
            std::vector<std::string> voting_peers;
        };

        struct PeerProgress {
            std::int64_t next_index{1};
            std::int64_t match_index{0};
        };

        struct LogEntryRecord {
            std::int64_t index{0};
            std::int64_t term{0};
            std::string data;
        };

        struct CommandCommitResult {
            std::int64_t index{0};
            std::string result;
        };

        struct PersistentState {
            std::string peer_id;
            std::int64_t current_term{0};
            std::optional<std::string> voted_for;
            std::optional<std::string> leader_id;
            bool joint_consensus{false};
            std::vector<std::string> pending_join_ids;
            std::vector<std::string> voting_peers;
            std::int64_t last_log_index{0};
            std::int64_t last_log_term{0};
            std::int64_t commit_index{0};
            std::int64_t snapshot_index{0};
            std::int64_t snapshot_term{0};
            std::string snapshot_data;
            std::int64_t last_applied{0};
            std::unordered_map<std::string, std::string> applied_kv;
            std::int64_t previous_log_index{0};
            std::int64_t previous_log_term{0};
            std::string last_entry_data;
            std::vector<LogEntryRecord> log_entries;
            std::unordered_map<std::string, PeerProgress> peer_progress;
        };

        explicit RaftNode(Config config);

        static constexpr std::string_view kInternalCommandPrefix = "raft-internal:";

        static std::string encode_internal_command(const raft::InternalRaftCommand &command);

        raft::VoteResponse handle_vote_request(const raft::VoteRequest &request);

        raft::AppendEntriesResponse handle_append_entries(const raft::AppendEntriesRequest &request);

        raft::InstallSnapshotResponse handle_install_snapshot(const raft::InstallSnapshotRequest &request);

        bool compact_snapshot_to(std::int64_t index, std::string snapshot_data);

        void become_candidate();

        void become_leader();

        void set_voting_peers(std::vector<std::string> voting_peers);

        std::vector<std::string> voting_peers() const;

        bool joint_consensus() const;

        bool has_pending_join(const std::string &peer_id) const;

        std::size_t quorum_size() const;

        std::size_t granted_votes() const;

        std::chrono::steady_clock::time_point last_activity() const;

        std::optional<std::string> voted_for() const;

        raft::VoteRequest start_election();

        bool handle_vote_response(const raft::VoteResponse &response);

        raft::AppendEntriesRequest make_heartbeat_request_for(const std::string &peer_id) const;

        std::vector<raft::AppendEntriesRequest> make_heartbeat_requests() const;

        bool handle_append_entries_response(const std::string &peer_id, const raft::AppendEntriesResponse &response);

        raft::InstallSnapshotRequest make_install_snapshot_request_for(const std::string &peer_id) const;

        bool handle_install_snapshot_response(const std::string &peer_id,
                                              const raft::InstallSnapshotResponse &response);

        std::int64_t append_local_entry(std::string data);

        void observe_local_append(std::int64_t last_log_index, std::int64_t last_log_term);

        raft::AppendEntriesRequest make_replication_request_for(const std::string &peer_id) const;

        std::string peer_id() const;

        std::int64_t current_term() const;

        std::int64_t last_log_index() const;

        std::int64_t last_log_term() const;

        std::int64_t commit_index() const;

        std::int64_t snapshot_index() const;

        std::int64_t snapshot_term() const;

        std::string snapshot_data() const;

        std::int64_t last_applied() const;

        std::unordered_map<std::string, std::string> applied_kv() const;

        std::string applied_command_result(std::int64_t index) const;

        std::optional<CommandCommitResult> append_and_commit_local_command(const std::string &data);

        std::optional<std::string> leader_id() const;

        std::unordered_map<std::string, PeerProgress> peer_progress() const;

        PersistentState persistent_state() const;

        void apply_persistent_state(const PersistentState &state);

        Role role() const;

    private:
        static void append_u32_be(std::string &out, std::uint32_t value);

        static void append_u16_be(std::string &out, std::uint16_t value);

        static bool read_u32_be(const std::string &data, std::size_t &offset, std::uint32_t &value);

        static bool read_u16_be(const std::string &data, std::size_t &offset, std::uint16_t &value);

        static std::string base64_encode(std::string_view input);

        static std::optional<std::string> base64_decode(std::string_view input);

        static std::string quote_json(std::string_view value);

        static std::string unwrap_snapshot_payload(const std::string &payload);

        static std::string serialize_state_machine_snapshot_locked(
            const std::unordered_map<std::string, std::string> &applied_kv);

        std::string wrap_snapshot_payload_locked(const std::string &state_machine_snapshot) const;

        void apply_snapshot_to_state_machine_locked();

        std::string apply_state_machine_command_locked(const std::string &data);

        std::optional<raft::InternalRaftCommand> parse_internal_command_locked(const std::string &data) const;

        bool apply_internal_command_locked(const std::string &data);

        std::string apply_log_entry_locked(const std::string &data);

        void apply_committed_entries_locked();

        void seed_bootstrap_log_locked();

        void normalize_voting_peers_locked();

        void reconfigure_voting_peers_locked(std::vector<std::string> voting_peers);

        std::size_t cluster_size_locked() const;

        std::size_t quorum_size_locked() const;

        void reset_peer_progress_locked();

        void start_election_locked();

        void become_leader_locked();

        void update_commit_index_locked();

        bool candidate_log_is_up_to_date_locked(const raft::VoteRequest &request) const;

        bool prev_log_matches_locked(std::int64_t prev_log_index, std::int64_t prev_log_term) const;

        std::int64_t log_term_at_locked(std::int64_t index) const;

        void truncate_log_from_locked(std::int64_t index);

        void truncate_prefix_up_to_locked(std::int64_t index);

        void step_down_locked(std::int64_t new_term, std::optional<std::string> leader_id);

        std::string applied_command_result_at_locked(std::int64_t index) const;

        mutable std::mutex mu_;
        std::string peer_id_;
        Role role_;
        std::int64_t current_term_;
        std::optional<std::string> voted_for_;
        std::optional<std::string> leader_id_;
        bool joint_consensus_{false};
        std::unordered_set<std::string> pending_join_ids_;
        std::int64_t last_log_index_;
        std::int64_t last_log_term_;
        std::int64_t commit_index_;
        std::int64_t snapshot_index_;
        std::int64_t snapshot_term_;
        std::string snapshot_data_;
        std::int64_t last_applied_{0};
        std::unordered_map<std::string, std::string> applied_kv_;
        std::unordered_map<std::int64_t, std::string> applied_command_results_;
        std::int64_t pending_snapshot_index_{0};
        std::int64_t pending_snapshot_term_{0};
        std::string pending_snapshot_data_;
        std::int64_t previous_log_index_;
        std::int64_t previous_log_term_;
        std::string last_entry_data_;
        std::vector<LogEntryRecord> log_entries_;
        std::chrono::steady_clock::time_point last_activity_;
        std::vector<std::string> voting_peers_;
        std::unordered_map<std::string, PeerProgress> peer_progress_;
        std::unordered_set<std::string> votes_granted_;
        std::unordered_set<std::string> votes_responded_;
    };
} // namespace graft
