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
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "graft/core/application_state_machine.hpp"
#include "raft.pb.h"

namespace graft {
    // RaftNode is deliberately transport-agnostic. It is the consensus state machine:
    // given protobuf RPC requests and locally-triggered actions, it mutates durable Raft
    // state and returns protobuf responses. The runtime layer decides when to send those
    // requests over TCP and when to persist the returned state.
    class RaftNode {
    public:
        enum class Role {
            follower,
            candidate,
            leader
        };

        // Construction-time state. Runtime modes use this for both fresh nodes and
        // nodes rebuilt from PersistentStateStore. Peer ids here are logical Raft ids,
        // not host:port endpoints; endpoint resolution lives in RaftRuntime/RpcHandler.
        struct Config {
            std::string peer_id;
            std::int64_t current_term{0};
            std::int64_t last_log_index{0};
            std::int64_t last_log_term{0};
            std::int64_t commit_index{0};
            std::int64_t snapshot_index{0};
            std::int64_t snapshot_term{0};
            std::vector<std::string> voting_peers;
            std::shared_ptr<ApplicationStateMachine> application;
            std::function<std::int64_t()> time_source;
        };

        // Leader-side view of one peer. next_index/match_index are the classic Raft
        // replication cursors; the reachability fields are operational telemetry and
        // read-lease input, not part of the formal persistent Raft paper state.
        struct PeerProgress {
            std::int64_t next_index{1};
            std::int64_t match_index{0};
            bool reachable{false};
            std::int32_t consecutive_failures{0};
            std::int64_t last_successful_contact_millis{0};
            std::int64_t last_failed_contact_millis{0};
        };

        // In-memory representation of a log entry. The protobuf LogEntry carries
        // term/data but not an explicit index, so the C++ core keeps the index beside it.
        struct LogEntryRecord {
            std::int64_t index{0};
            std::int64_t term{0};
            std::string data;
        };

        // Returned to client-facing code after a single-node local commit or after
        // replicated commit has advanced far enough to apply the command.
        struct CommandCommitResult {
            std::int64_t index{0};
            std::string result;
        };

        // Serializable snapshot of the whole Raft node, including the application
        // snapshot wrapper and membership state. This is intentionally larger than the
        // domain state-machine snapshot: it is what lets a restarted node recover its
        // term, vote, log, membership and compaction boundary.
        struct PersistentState {
            std::string peer_id;
            std::int64_t current_term{0};
            std::optional<std::string> voted_for;
            std::optional<std::string> leader_id;
            bool joint_consensus{false};
            bool pending_decommission{false};
            bool decommissioned{false};
            std::int64_t reconfiguration_started_at_millis{0};
            std::vector<std::string> pending_join_ids;
            std::vector<std::string> voting_peers;
            std::vector<raft::PeerSpec> current_members;
            std::vector<raft::PeerSpec> next_members;
            std::int64_t last_log_index{0};
            std::int64_t last_log_term{0};
            std::int64_t commit_index{0};
            std::int64_t snapshot_index{0};
            std::int64_t snapshot_term{0};
            std::string snapshot_data;
            std::int64_t last_applied{0};
            std::int64_t previous_log_index{0};
            std::int64_t previous_log_term{0};
            std::string last_entry_data;
            std::vector<LogEntryRecord> log_entries;
            std::unordered_map<std::string, PeerProgress> peer_progress;
        };

        explicit RaftNode(Config config);

        // Internal commands are replicated through the same log as application commands
        // but are consumed by Raft itself. Membership change entries are the primary use.
        static std::string encode_internal_command(const raft::InternalRaftCommand &command);

        // RPC receiver side: these methods are called by RpcHandler after an envelope
        // has been decoded. They do not perform network I/O.
        raft::VoteResponse handle_vote_request(const raft::VoteRequest &request);

        raft::AppendEntriesResponse handle_append_entries(const raft::AppendEntriesRequest &request);

        raft::InstallSnapshotResponse handle_install_snapshot(const raft::InstallSnapshotRequest &request);

        bool compact_snapshot_to(std::int64_t index, std::string snapshot_data);

        // Local role transitions used by runtime loops and tests.
        void become_candidate();

        void become_leader();

        void set_voting_peers(std::vector<std::string> voting_peers);

        std::vector<std::string> voting_peers() const;

        std::vector<std::string> current_voting_members() const;

        std::vector<std::string> next_voting_members() const;

        bool has_joint_majority(const std::unordered_set<std::string> &peer_ids) const;

        std::vector<raft::PeerSpec> current_member_specs() const;

        std::vector<raft::PeerSpec> next_member_specs() const;

        bool joint_consensus() const;

        std::int64_t reconfiguration_started_at_millis() const;

        bool decommissioned() const;

        bool has_pending_join(const std::string &peer_id) const;

        std::size_t quorum_size() const;

        std::size_t granted_votes() const;

        std::chrono::steady_clock::time_point last_activity() const;

        std::optional<std::string> voted_for() const;

        raft::VoteRequest start_election();

        bool handle_vote_response(const raft::VoteResponse &response);

        // Leader-side RPC builders. They construct protobuf messages from local Raft
        // state; RaftRuntime is responsible for sending them and feeding responses back.
        raft::AppendEntriesRequest make_heartbeat_request_for(const std::string &peer_id) const;

        std::vector<raft::AppendEntriesRequest> make_heartbeat_requests() const;

        bool handle_append_entries_response(const std::string &peer_id, const raft::AppendEntriesResponse &response);

        bool can_serve_linearizable_read(std::int64_t lease_millis) const;

        raft::InstallSnapshotRequest make_install_snapshot_request_for(const std::string &peer_id) const;

        bool handle_install_snapshot_response(const std::string &peer_id,
                                              const raft::InstallSnapshotResponse &response);

        void record_peer_failure(const std::string &peer_id);

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

        std::string query_application(const std::string &data) const;

        std::string applied_command_result(std::int64_t index) const;

        std::optional<CommandCommitResult> append_and_commit_local_command(const std::string &data);

        std::optional<std::string> leader_id() const;

        std::unordered_map<std::string, PeerProgress> peer_progress() const;

        PersistentState persistent_state() const;

        void apply_persistent_state(const PersistentState &state);

        Role role() const;

    private:
        // All private helpers require mu_ to be held. The _locked suffix is used as a
        // convention so callers do not accidentally call them without the node mutex.
        std::string wrap_snapshot_payload_locked(const std::string &state_machine_snapshot) const;

        void apply_snapshot_to_state_machine_locked();

        std::string apply_state_machine_command_locked(const LogEntryRecord &entry);

        std::optional<raft::InternalRaftCommand> parse_internal_command_locked(const std::string &data) const;

        bool apply_internal_command_locked(const std::string &data);

        std::string apply_log_entry_locked(const LogEntryRecord &entry);

        void apply_committed_entries_locked();

        void seed_bootstrap_log_locked();

        void normalize_voting_peers_locked();

        void seed_current_members_locked();

        void reconfigure_voting_peers_locked(std::vector<std::string> voting_peers);

        std::vector<std::string> current_voting_members_locked() const;

        std::vector<std::string> next_voting_members_locked() const;

        std::vector<std::string> active_voting_members_locked() const;

        void refresh_voting_peers_from_members_locked();

        bool has_joint_majority_locked(const std::unordered_set<std::string> &peer_ids) const;

        std::int64_t current_time_millis() const;

        std::size_t cluster_size_locked() const;

        std::size_t quorum_size_locked() const;

        void reset_peer_progress_locked();

        void start_election_locked();

        void become_leader_locked();

        void update_commit_index_locked();

        bool candidate_log_is_up_to_date_locked(const raft::VoteRequest &request) const;

        bool prev_log_matches_locked(std::int64_t prev_log_index, std::int64_t prev_log_term) const;

        bool is_known_voting_member_locked(const std::string &peer_id) const;

        std::int64_t log_term_at_locked(std::int64_t index) const;

        void truncate_log_from_locked(std::int64_t index);

        void truncate_prefix_up_to_locked(std::int64_t index);

        void step_down_locked(std::int64_t new_term, std::optional<std::string> leader_id);

        std::string applied_command_result_at_locked(std::int64_t index) const;

        std::unordered_map<std::string, std::string> applied_kv_locked() const;

        // One mutex protects the complete Raft state below. This keeps the bounded C++
        // implementation easy to reason about while it is still converging with Java.
        mutable std::mutex mu_;
        // Persistent Raft identity and role state.
        std::string peer_id_;
        Role role_;
        std::int64_t current_term_;
        std::optional<std::string> voted_for_;
        std::optional<std::string> leader_id_;
        // Membership state. current_members_ is always the stable side; next_members_
        // is populated only while joint_consensus_ is true.
        bool joint_consensus_{false};
        bool pending_decommission_{false};
        bool decommissioned_{false};
        std::int64_t reconfiguration_started_at_millis_{0};
        std::unordered_set<std::string> pending_join_ids_;
        // Log and snapshot boundary. snapshot_data_ is a wrapped payload containing
        // application snapshot bytes plus Raft metadata.
        std::int64_t last_log_index_;
        std::int64_t last_log_term_;
        std::int64_t commit_index_;
        std::int64_t snapshot_index_;
        std::int64_t snapshot_term_;
        std::string snapshot_data_;
        std::int64_t last_applied_{0};
        std::shared_ptr<ApplicationStateMachine> application_;
        // Command results are retained long enough for client-facing RPC handlers to
        // return the deterministic result of the committed entry, for example CAS.
        std::unordered_map<std::int64_t, std::string> applied_command_results_;
        // InstallSnapshot chunk assembly state. Followers build the full snapshot here
        // before replacing the active snapshot boundary.
        std::int64_t pending_snapshot_index_{0};
        std::int64_t pending_snapshot_term_{0};
        std::string pending_snapshot_data_;
        // Compatibility fields for bounded smoke/probe modes that start from only
        // previous index/term values rather than a fully materialized log.
        std::int64_t previous_log_index_;
        std::int64_t previous_log_term_;
        std::string last_entry_data_;
        std::vector<LogEntryRecord> log_entries_;
        std::function<std::int64_t()> time_source_;
        std::chrono::steady_clock::time_point last_activity_;
        std::vector<std::string> voting_peers_;
        std::vector<raft::PeerSpec> current_members_;
        std::vector<raft::PeerSpec> next_members_;
        std::unordered_map<std::string, PeerProgress> peer_progress_;
        std::unordered_set<std::string> votes_granted_;
        std::unordered_set<std::string> votes_responded_;
    };
} // namespace graft
