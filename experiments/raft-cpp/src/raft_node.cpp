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

#include "raftcpp/raft_node.hpp"

#include <array>
#include <cstdio>
#include <limits>
#include <stdexcept>
#include <utility>

namespace raftcpp {
    RaftNode::RaftNode(Config config)
        : peer_id_(std::move(config.peer_id)),
          role_(Role::follower),
          current_term_(config.current_term),
          last_log_index_(config.last_log_index),
          last_log_term_(config.last_log_term),
          commit_index_(config.commit_index),
          snapshot_index_(config.snapshot_index),
          snapshot_term_(config.snapshot_term),
          previous_log_index_(config.last_log_index),
          previous_log_term_(config.last_log_term),
          last_activity_(std::chrono::steady_clock::now()),
          voting_peers_(std::move(config.voting_peers)) {
        seed_bootstrap_log_locked();
        normalize_voting_peers_locked();
        reset_peer_progress_locked();
    }

    std::string RaftNode::encode_internal_command(const raft::InternalRaftCommand &command) {
        std::string payload;
        if (!command.SerializeToString(&payload)) {
            throw std::runtime_error("failed to serialize InternalRaftCommand");
        }
        return payload;
    }

    raft::VoteResponse RaftNode::handle_vote_request(const raft::VoteRequest &request) {
        std::scoped_lock lock(mu_);

        if (request.term() > current_term_) {
            step_down_locked(request.term(), std::nullopt);
        }

        bool granted = false;
        if (request.term() == current_term_ && candidate_log_is_up_to_date_locked(request)) {
            if (!voted_for_.has_value() || *voted_for_ == request.candidate_id()) {
                role_ = Role::follower;
                voted_for_ = request.candidate_id();
                last_activity_ = std::chrono::steady_clock::now();
                granted = true;
            }
        }

        raft::VoteResponse response;
        response.set_peer_id(peer_id_);
        response.set_term(request.term());
        response.set_vote_granted(granted);
        response.set_current_term(current_term_);
        return response;
    }

    raft::AppendEntriesResponse RaftNode::handle_append_entries(const raft::AppendEntriesRequest &request) {
        std::scoped_lock lock(mu_);

        if (request.term() > current_term_) {
            step_down_locked(request.term(), request.leader_id());
        }

        bool success = false;
        std::int64_t match_index = 0;
        if (request.term() == current_term_ && prev_log_matches_locked(request.prev_log_index(),
                                                                       request.prev_log_term())) {
            role_ = Role::follower;
            leader_id_ = request.leader_id();
            last_activity_ = std::chrono::steady_clock::now();
            success = true;

            if (request.entries_size() > 0) {
                truncate_log_from_locked(request.prev_log_index() + 1);
                for (int i = 0; i < request.entries_size(); ++i) {
                    const auto &entry = request.entries(i);
                    log_entries_.push_back(LogEntryRecord{
                        .index = request.prev_log_index() + 1 + i,
                        .term = entry.term(),
                        .data = entry.data(),
                    });
                }
                last_log_index_ = log_entries_.back().index;
                last_log_term_ = log_entries_.back().term;
                previous_log_index_ = request.prev_log_index();
                previous_log_term_ = request.prev_log_term();
                last_entry_data_ = log_entries_.back().data;
                match_index = last_log_index_;
            } else {
                match_index = request.prev_log_index();
            }

            commit_index_ = std::max(commit_index_, std::min(request.leader_commit(), match_index));
            apply_committed_entries_locked();
        }

        raft::AppendEntriesResponse response;
        response.set_term(current_term_);
        response.set_peer_id(peer_id_);
        response.set_success(success);
        response.set_match_index(match_index);
        return response;
    }

    raft::InstallSnapshotResponse RaftNode::handle_install_snapshot(const raft::InstallSnapshotRequest &request) {
        std::scoped_lock lock(mu_);

        if (request.term() > current_term_) {
            step_down_locked(request.term(), request.leader_id());
        }

        bool success = false;
        if (request.term() == current_term_) {
            role_ = Role::follower;
            leader_id_ = request.leader_id();
            last_activity_ = std::chrono::steady_clock::now();
            if (request.offset() == 0 ||
                pending_snapshot_index_ != request.last_included_index() ||
                pending_snapshot_term_ != request.last_included_term()) {
                pending_snapshot_index_ = request.last_included_index();
                pending_snapshot_term_ = request.last_included_term();
                pending_snapshot_data_.clear();
            }

            if (request.offset() == static_cast<std::int64_t>(pending_snapshot_data_.size())) {
                pending_snapshot_data_.append(request.snapshot_data());
                success = true;
            }

            if (success && request.done()) {
                snapshot_index_ = request.last_included_index();
                snapshot_term_ = request.last_included_term();
                snapshot_data_ = pending_snapshot_data_;
                pending_snapshot_data_.clear();
                pending_snapshot_index_ = 0;
                pending_snapshot_term_ = 0;
                truncate_prefix_up_to_locked(snapshot_index_);
                if (last_log_index_ < snapshot_index_) {
                    last_log_index_ = snapshot_index_;
                    last_log_term_ = snapshot_term_;
                    previous_log_index_ = snapshot_index_;
                    previous_log_term_ = snapshot_term_;
                    last_entry_data_.clear();
                }
                commit_index_ = std::max(commit_index_, snapshot_index_);
                apply_snapshot_to_state_machine_locked();
                last_applied_ = std::max(last_applied_, snapshot_index_);
            }
        }

        raft::InstallSnapshotResponse response;
        response.set_term(current_term_);
        response.set_peer_id(peer_id_);
        response.set_success(success);
        response.set_last_included_index(request.last_included_index());
        return response;
    }

    bool RaftNode::compact_snapshot_to(std::int64_t index, std::string) {
        std::scoped_lock lock(mu_);
        if (index <= snapshot_index_ || index > commit_index_) {
            return false;
        }

        const auto term = log_term_at_locked(index);
        if (term == 0) {
            return false;
        }

        snapshot_index_ = index;
        snapshot_term_ = term;
        snapshot_data_ = wrap_snapshot_payload_locked(serialize_state_machine_snapshot_locked(applied_kv_));
        truncate_prefix_up_to_locked(snapshot_index_);
        commit_index_ = std::max(commit_index_, snapshot_index_);
        apply_committed_entries_locked();
        return true;
    }

    void RaftNode::become_candidate() {
        std::scoped_lock lock(mu_);
        start_election_locked();
    }

    void RaftNode::become_leader() {
        std::scoped_lock lock(mu_);
        become_leader_locked();
    }

    void RaftNode::set_voting_peers(std::vector<std::string> voting_peers) {
        std::scoped_lock lock(mu_);
        reconfigure_voting_peers_locked(std::move(voting_peers));
    }

    std::vector<std::string> RaftNode::voting_peers() const {
        std::scoped_lock lock(mu_);
        return voting_peers_;
    }

    bool RaftNode::joint_consensus() const {
        std::scoped_lock lock(mu_);
        return joint_consensus_;
    }

    bool RaftNode::has_pending_join(const std::string &peer_id) const {
        std::scoped_lock lock(mu_);
        return pending_join_ids_.contains(peer_id);
    }

    std::size_t RaftNode::quorum_size() const {
        std::scoped_lock lock(mu_);
        return quorum_size_locked();
    }

    std::size_t RaftNode::granted_votes() const {
        std::scoped_lock lock(mu_);
        return votes_granted_.size();
    }

    std::chrono::steady_clock::time_point RaftNode::last_activity() const {
        std::scoped_lock lock(mu_);
        return last_activity_;
    }

    std::optional<std::string> RaftNode::voted_for() const {
        std::scoped_lock lock(mu_);
        return voted_for_;
    }

    raft::VoteRequest RaftNode::start_election() {
        std::scoped_lock lock(mu_);
        start_election_locked();

        raft::VoteRequest request;
        request.set_term(current_term_);
        request.set_candidate_id(peer_id_);
        request.set_last_log_index(last_log_index_);
        request.set_last_log_term(last_log_term_);
        return request;
    }

    bool RaftNode::handle_vote_response(const raft::VoteResponse &response) {
        std::scoped_lock lock(mu_);

        const auto observed_term = std::max(response.current_term(), response.term());
        if (observed_term > current_term_) {
            step_down_locked(observed_term, std::nullopt);
            return false;
        }

        if (role_ != Role::candidate || response.term() != current_term_) {
            return role_ == Role::leader;
        }

        if (!response.peer_id().empty()) {
            votes_responded_.insert(response.peer_id());
            if (response.vote_granted()) {
                votes_granted_.insert(response.peer_id());
            }
        }

        if (votes_granted_.size() >= quorum_size_locked()) {
            become_leader_locked();
            return true;
        }

        return false;
    }

    raft::AppendEntriesRequest RaftNode::make_heartbeat_request_for(const std::string &peer_id) const {
        std::scoped_lock lock(mu_);

        raft::AppendEntriesRequest request;
        request.set_term(current_term_);
        request.set_leader_id(this->peer_id_);
        request.set_leader_commit(commit_index_);

        const auto found = peer_progress_.find(peer_id);
        const auto next_index = found != peer_progress_.end() ? found->second.next_index : (last_log_index_ + 1);
        const auto prev_log_index = std::max<std::int64_t>(0, next_index - 1);

        request.set_prev_log_index(prev_log_index);
        if (prev_log_index == 0) {
            request.set_prev_log_term(0);
        } else if (prev_log_index == snapshot_index_) {
            request.set_prev_log_term(snapshot_term_);
        } else if (prev_log_index == last_log_index_) {
            request.set_prev_log_term(last_log_term_);
        } else {
            request.set_prev_log_term(0);
        }

        return request;
    }

    std::vector<raft::AppendEntriesRequest> RaftNode::make_heartbeat_requests() const {
        std::scoped_lock lock(mu_);

        std::vector<raft::AppendEntriesRequest> requests;
        requests.reserve(voting_peers_.size());
        for (const auto &peer_id: voting_peers_) {
            raft::AppendEntriesRequest request;
            request.set_term(current_term_);
            request.set_leader_id(this->peer_id_);
            request.set_leader_commit(commit_index_);

            const auto found = peer_progress_.find(peer_id);
            const auto next_index = found != peer_progress_.end() ? found->second.next_index : (last_log_index_ + 1);
            const auto prev_log_index = std::max<std::int64_t>(0, next_index - 1);
            request.set_prev_log_index(prev_log_index);

            if (prev_log_index == 0) {
                request.set_prev_log_term(0);
            } else if (prev_log_index == snapshot_index_) {
                request.set_prev_log_term(snapshot_term_);
            } else if (prev_log_index == last_log_index_) {
                request.set_prev_log_term(last_log_term_);
            } else {
                request.set_prev_log_term(0);
            }

            requests.push_back(std::move(request));
        }

        return requests;
    }

    bool RaftNode::handle_append_entries_response(const std::string &peer_id,
                                                  const raft::AppendEntriesResponse &response) {
        std::scoped_lock lock(mu_);

        if (response.term() > current_term_) {
            step_down_locked(response.term(), std::nullopt);
            return false;
        }
        if (role_ != Role::leader || response.term() != current_term_) {
            return false;
        }

        auto &progress = peer_progress_[peer_id];
        if (response.success()) {
            progress.match_index = std::max(progress.match_index, response.match_index());
            progress.next_index = std::max(progress.next_index, response.match_index() + 1);
            update_commit_index_locked();
            apply_committed_entries_locked();
            return true;
        }

        progress.next_index = std::max<std::int64_t>(1, progress.next_index - 1);
        return false;
    }

    raft::InstallSnapshotRequest RaftNode::make_install_snapshot_request_for(const std::string &peer_id) const {
        std::scoped_lock lock(mu_);
        (void) peer_id;

        raft::InstallSnapshotRequest request;
        request.set_term(current_term_);
        request.set_leader_id(this->peer_id_);
        request.set_last_included_index(snapshot_index_);
        request.set_last_included_term(snapshot_term_);
        request.set_offset(0);
        request.set_done(true);
        request.set_snapshot_data(snapshot_data_);
        return request;
    }

    bool RaftNode::handle_install_snapshot_response(const std::string &peer_id,
                                                    const raft::InstallSnapshotResponse &response) {
        std::scoped_lock lock(mu_);

        if (response.term() > current_term_) {
            step_down_locked(response.term(), std::nullopt);
            return false;
        }
        if (role_ != Role::leader || response.term() != current_term_) {
            return false;
        }

        if (!response.success()) {
            return false;
        }

        auto &progress = peer_progress_[peer_id];
        progress.match_index = std::max(progress.match_index, response.last_included_index());
        progress.next_index = std::max(progress.next_index, response.last_included_index() + 1);
        return true;
    }

    std::int64_t RaftNode::append_local_entry(std::string data) {
        std::scoped_lock lock(mu_);
        previous_log_index_ = last_log_index_;
        previous_log_term_ = last_log_term_;
        last_log_index_ += 1;
        last_log_term_ = current_term_;
        log_entries_.push_back(LogEntryRecord{
            .index = last_log_index_,
            .term = last_log_term_,
            .data = data,
        });
        last_entry_data_ = std::move(data);
        peer_progress_[peer_id_].match_index = last_log_index_;
        peer_progress_[peer_id_].next_index = last_log_index_ + 1;
        return last_log_index_;
    }

    void RaftNode::observe_local_append(std::int64_t last_log_index, std::int64_t last_log_term) {
        std::scoped_lock lock(mu_);
        last_log_index_ = std::max(last_log_index_, last_log_index);
        last_log_term_ = last_log_term;
        if (role_ == Role::leader) {
            peer_progress_[peer_id_].match_index = last_log_index_;
            peer_progress_[peer_id_].next_index = last_log_index_ + 1;
            for (auto &[peer_id, progress]: peer_progress_) {
                if (peer_id != peer_id_) {
                    progress.next_index = std::max(progress.next_index, last_log_index_ + 1);
                }
            }
        }
    }

    raft::AppendEntriesRequest RaftNode::make_replication_request_for(const std::string &peer_id) const {
        std::scoped_lock lock(mu_);

        raft::AppendEntriesRequest request;
        request.set_term(current_term_);
        request.set_leader_id(this->peer_id_);
        request.set_leader_commit(commit_index_);

        const auto found = peer_progress_.find(peer_id);
        const auto next_index = found != peer_progress_.end() ? found->second.next_index : (last_log_index_ + 1);
        const auto prev_log_index = std::max<std::int64_t>(0, next_index - 1);
        request.set_prev_log_index(prev_log_index);

        request.set_prev_log_term(log_term_at_locked(prev_log_index));

        for (const auto &local_entry: log_entries_) {
            if (local_entry.index < next_index) {
                continue;
            }
            auto *entry = request.add_entries();
            entry->set_term(local_entry.term);
            entry->set_peer_id(this->peer_id_);
            entry->set_data(local_entry.data);
        }

        return request;
    }

    std::string RaftNode::peer_id() const {
        std::scoped_lock lock(mu_);
        return peer_id_;
    }

    std::int64_t RaftNode::current_term() const {
        std::scoped_lock lock(mu_);
        return current_term_;
    }

    std::int64_t RaftNode::last_log_index() const {
        std::scoped_lock lock(mu_);
        return last_log_index_;
    }

    std::int64_t RaftNode::last_log_term() const {
        std::scoped_lock lock(mu_);
        return last_log_term_;
    }

    std::int64_t RaftNode::commit_index() const {
        std::scoped_lock lock(mu_);
        return commit_index_;
    }

    std::int64_t RaftNode::snapshot_index() const {
        std::scoped_lock lock(mu_);
        return snapshot_index_;
    }

    std::int64_t RaftNode::snapshot_term() const {
        std::scoped_lock lock(mu_);
        return snapshot_term_;
    }

    std::string RaftNode::snapshot_data() const {
        std::scoped_lock lock(mu_);
        return snapshot_data_;
    }

    std::int64_t RaftNode::last_applied() const {
        std::scoped_lock lock(mu_);
        return last_applied_;
    }

    std::unordered_map<std::string, std::string> RaftNode::applied_kv() const {
        std::scoped_lock lock(mu_);
        return applied_kv_;
    }

    std::string RaftNode::applied_command_result(std::int64_t index) const {
        std::scoped_lock lock(mu_);
        return applied_command_result_at_locked(index);
    }

    std::optional<RaftNode::CommandCommitResult> RaftNode::append_and_commit_local_command(const std::string &data) {
        std::scoped_lock lock(mu_);
        if (role_ != Role::leader || quorum_size_locked() != 1) {
            return std::nullopt;
        }

        previous_log_index_ = last_log_index_;
        previous_log_term_ = last_log_term_;
        last_log_index_ += 1;
        last_log_term_ = current_term_;
        log_entries_.push_back(LogEntryRecord{
            .index = last_log_index_,
            .term = last_log_term_,
            .data = data,
        });
        last_entry_data_ = data;
        peer_progress_[peer_id_].match_index = last_log_index_;
        peer_progress_[peer_id_].next_index = last_log_index_ + 1;
        commit_index_ = last_log_index_;
        apply_committed_entries_locked();
        return CommandCommitResult{
            .index = last_log_index_,
            .result = applied_command_result_at_locked(last_log_index_),
        };
    }

    std::optional<std::string> RaftNode::leader_id() const {
        std::scoped_lock lock(mu_);
        return leader_id_;
    }

    std::unordered_map<std::string, RaftNode::PeerProgress> RaftNode::peer_progress() const {
        std::scoped_lock lock(mu_);
        return peer_progress_;
    }

    RaftNode::PersistentState RaftNode::persistent_state() const {
        std::scoped_lock lock(mu_);
        return PersistentState{
            .peer_id = peer_id_,
            .current_term = current_term_,
            .voted_for = voted_for_,
            .leader_id = leader_id_,
            .joint_consensus = joint_consensus_,
            .pending_join_ids = std::vector<std::string>(pending_join_ids_.begin(), pending_join_ids_.end()),
            .voting_peers = voting_peers_,
            .last_log_index = last_log_index_,
            .last_log_term = last_log_term_,
            .commit_index = commit_index_,
            .snapshot_index = snapshot_index_,
            .snapshot_term = snapshot_term_,
            .snapshot_data = snapshot_data_,
            .last_applied = last_applied_,
            .applied_kv = applied_kv_,
            .previous_log_index = previous_log_index_,
            .previous_log_term = previous_log_term_,
            .last_entry_data = last_entry_data_,
            .log_entries = log_entries_,
            .peer_progress = peer_progress_,
        };
    }

    void RaftNode::apply_persistent_state(const PersistentState &state) {
        std::scoped_lock lock(mu_);
        peer_id_ = state.peer_id;
        current_term_ = state.current_term;
        voted_for_ = state.voted_for;
        leader_id_ = state.leader_id;
        joint_consensus_ = state.joint_consensus;
        pending_join_ids_ = std::unordered_set<std::string>(state.pending_join_ids.begin(),
                                                            state.pending_join_ids.end());
        voting_peers_ = state.voting_peers;
        last_log_index_ = state.last_log_index;
        last_log_term_ = state.last_log_term;
        commit_index_ = state.commit_index;
        snapshot_index_ = state.snapshot_index;
        snapshot_term_ = state.snapshot_term;
        snapshot_data_ = state.snapshot_data;
        last_applied_ = state.last_applied;
        applied_kv_ = state.applied_kv;
        applied_command_results_.clear();
        previous_log_index_ = state.previous_log_index;
        previous_log_term_ = state.previous_log_term;
        last_entry_data_ = state.last_entry_data;
        log_entries_ = state.log_entries;
        if (log_entries_.empty()) {
            seed_bootstrap_log_locked();
        }
        role_ = leader_id_.has_value() && *leader_id_ == peer_id_ ? Role::leader : Role::follower;
        last_activity_ = std::chrono::steady_clock::now();
        normalize_voting_peers_locked();
        for (const auto &[peer_id, _]: state.peer_progress) {
            if (peer_id != peer_id_) {
                voting_peers_.push_back(peer_id);
            }
        }
        normalize_voting_peers_locked();
        reset_peer_progress_locked();
        for (const auto &[peer_id, progress]: state.peer_progress) {
            peer_progress_[peer_id] = progress;
        }
    }

    RaftNode::Role RaftNode::role() const {
        std::scoped_lock lock(mu_);
        return role_;
    }

    void RaftNode::append_u32_be(std::string &out, std::uint32_t value) {
        out.push_back(static_cast<char>((value >> 24) & 0xFF));
        out.push_back(static_cast<char>((value >> 16) & 0xFF));
        out.push_back(static_cast<char>((value >> 8) & 0xFF));
        out.push_back(static_cast<char>(value & 0xFF));
    }

    void RaftNode::append_u16_be(std::string &out, std::uint16_t value) {
        out.push_back(static_cast<char>((value >> 8) & 0xFF));
        out.push_back(static_cast<char>(value & 0xFF));
    }

    bool RaftNode::read_u32_be(const std::string &data, std::size_t &offset, std::uint32_t &value) {
        if (offset + 4 > data.size()) {
            return false;
        }
        value = (static_cast<std::uint32_t>(static_cast<unsigned char>(data[offset])) << 24) |
                (static_cast<std::uint32_t>(static_cast<unsigned char>(data[offset + 1])) << 16) |
                (static_cast<std::uint32_t>(static_cast<unsigned char>(data[offset + 2])) << 8) |
                static_cast<std::uint32_t>(static_cast<unsigned char>(data[offset + 3]));
        offset += 4;
        return true;
    }

    bool RaftNode::read_u16_be(const std::string &data, std::size_t &offset, std::uint16_t &value) {
        if (offset + 2 > data.size()) {
            return false;
        }
        value = static_cast<std::uint16_t>(
            (static_cast<std::uint16_t>(static_cast<unsigned char>(data[offset])) << 8) |
            static_cast<std::uint16_t>(static_cast<unsigned char>(data[offset + 1])));
        offset += 2;
        return true;
    }

    std::string RaftNode::base64_encode(std::string_view input) {
        static constexpr char alphabet[] =
                "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                "abcdefghijklmnopqrstuvwxyz"
                "0123456789+/";

        std::string out;
        out.reserve(((input.size() + 2) / 3) * 4);
        std::size_t index = 0;
        while (index + 3 <= input.size()) {
            const auto a = static_cast<unsigned char>(input[index++]);
            const auto b = static_cast<unsigned char>(input[index++]);
            const auto c = static_cast<unsigned char>(input[index++]);
            out.push_back(alphabet[(a >> 2) & 0x3F]);
            out.push_back(alphabet[((a & 0x03) << 4) | ((b >> 4) & 0x0F)]);
            out.push_back(alphabet[((b & 0x0F) << 2) | ((c >> 6) & 0x03)]);
            out.push_back(alphabet[c & 0x3F]);
        }

        const auto remaining = input.size() - index;
        if (remaining == 1) {
            const auto a = static_cast<unsigned char>(input[index]);
            out.push_back(alphabet[(a >> 2) & 0x3F]);
            out.push_back(alphabet[(a & 0x03) << 4]);
            out.push_back('=');
            out.push_back('=');
        } else if (remaining == 2) {
            const auto a = static_cast<unsigned char>(input[index]);
            const auto b = static_cast<unsigned char>(input[index + 1]);
            out.push_back(alphabet[(a >> 2) & 0x3F]);
            out.push_back(alphabet[((a & 0x03) << 4) | ((b >> 4) & 0x0F)]);
            out.push_back(alphabet[(b & 0x0F) << 2]);
            out.push_back('=');
        }
        return out;
    }

    std::optional<std::string> RaftNode::base64_decode(std::string_view input) {
        static constexpr unsigned char invalid = 0xFF;
        static constexpr std::array<unsigned char, 256> decode_table = [] {
            std::array<unsigned char, 256> table{};
            table.fill(invalid);
            for (unsigned char i = 0; i < 26; ++i) {
                table['A' + i] = i;
                table['a' + i] = static_cast<unsigned char>(26 + i);
            }
            for (unsigned char i = 0; i < 10; ++i) {
                table['0' + i] = static_cast<unsigned char>(52 + i);
            }
            table['+'] = 62;
            table['/'] = 63;
            table['='] = 0;
            return table;
        }();

        std::string out;
        out.reserve((input.size() / 4) * 3);
        for (std::size_t i = 0; i < input.size();) {
            unsigned char values[4];
            std::size_t pad = 0;
            for (int j = 0; j < 4; ++j) {
                if (i >= input.size()) {
                    return std::nullopt;
                }
                const unsigned char ch = static_cast<unsigned char>(input[i++]);
                const auto decoded = decode_table[ch];
                if (decoded == invalid) {
                    return std::nullopt;
                }
                if (ch == '=') {
                    ++pad;
                }
                values[j] = decoded;
            }

            out.push_back(static_cast<char>((values[0] << 2) | (values[1] >> 4)));
            if (pad < 2) {
                out.push_back(static_cast<char>((values[1] << 4) | (values[2] >> 2)));
            }
            if (pad < 1) {
                out.push_back(static_cast<char>((values[2] << 6) | values[3]));
            }
        }
        return out;
    }

    std::string RaftNode::quote_json(std::string_view value) {
        std::string escaped;
        escaped.push_back('"');
        for (char c: value) {
            switch (c) {
                case '"':
                    escaped.append("\\\"");
                    break;
                case '\\':
                    escaped.append("\\\\");
                    break;
                case '\b':
                    escaped.append("\\b");
                    break;
                case '\f':
                    escaped.append("\\f");
                    break;
                case '\n':
                    escaped.append("\\n");
                    break;
                case '\r':
                    escaped.append("\\r");
                    break;
                case '\t':
                    escaped.append("\\t");
                    break;
                default:
                    if (static_cast<unsigned char>(c) < 0x20) {
                        char buffer[7];
                        std::snprintf(buffer, sizeof(buffer), "\\u%04x", static_cast<unsigned char>(c));
                        escaped.append(buffer);
                    } else {
                        escaped.push_back(c);
                    }
                    break;
            }
        }
        escaped.push_back('"');
        return escaped;
    }

    std::string RaftNode::wrap_snapshot_payload_locked(const std::string &state_machine_snapshot) const {
        auto append_member_list = [](std::string &out, const std::vector<std::string> &members) {
            out.push_back('[');
            bool first = true;
            for (const auto &member: members) {
                if (!first) {
                    out.push_back(',');
                }
                first = false;
                out.append("{\"id\":");
                out.append(quote_json(member));
                out.append(",\"role\":\"VOTER\",\"address\":null}");
            }
            out.push_back(']');
        };

        std::vector<std::string> current_members;
        current_members.reserve(voting_peers_.size() + 1);
        current_members.push_back(peer_id_);
        for (const auto &peer: voting_peers_) {
            if (peer != peer_id_) {
                current_members.push_back(peer);
            }
        }
        std::sort(current_members.begin(), current_members.end());

        std::string out;
        out.append("{\"version\":1,\"currentMembers\":");
        append_member_list(out, current_members);
        out.append(",\"nextMembers\":");
        if (joint_consensus_) {
            append_member_list(out, current_members);
        } else {
            out.append("[]");
        }
        out.append(",\"stateMachineSnapshot\":");
        out.append(quote_json(base64_encode(state_machine_snapshot)));
        out.push_back('}');
        return out;
    }

    std::string RaftNode::unwrap_snapshot_payload(const std::string &payload) {
        if (payload.empty() || payload.front() != '{') {
            return payload;
        }

        const std::string marker = "\"stateMachineSnapshot\":\"";
        const auto start = payload.find(marker);
        if (start == std::string::npos) {
            return payload;
        }
        const auto value_start = start + marker.size();
        const auto value_end = payload.find('"', value_start);
        if (value_end == std::string::npos) {
            return payload;
        }
        const auto decoded = base64_decode(std::string_view(payload).substr(value_start, value_end - value_start));
        return decoded.value_or(payload);
    }

    std::string RaftNode::serialize_state_machine_snapshot_locked(
        const std::unordered_map<std::string, std::string> &applied_kv) {
        std::string out;
        std::vector<std::pair<std::string, std::string> > ordered(applied_kv.begin(), applied_kv.end());
        std::sort(ordered.begin(), ordered.end(), [](const auto &left, const auto &right) {
            return left.first < right.first;
        });

        append_u32_be(out, static_cast<std::uint32_t>(ordered.size()));
        for (const auto &[key, value]: ordered) {
            if (key.size() > std::numeric_limits<std::uint16_t>::max() ||
                value.size() > std::numeric_limits<std::uint16_t>::max()) {
                throw std::runtime_error("snapshot key or value exceeds Java UTF length limit");
            }
            append_u16_be(out, static_cast<std::uint16_t>(key.size()));
            out.append(key);
            append_u16_be(out, static_cast<std::uint16_t>(value.size()));
            out.append(value);
        }
        return out;
    }

    void RaftNode::apply_snapshot_to_state_machine_locked() {
        if (snapshot_data_.empty()) {
            return;
        }

        const auto state_machine_snapshot = unwrap_snapshot_payload(snapshot_data_);
        std::unordered_map<std::string, std::string> restored;
        std::size_t offset = 0;
        std::uint32_t size = 0;
        if (!read_u32_be(state_machine_snapshot, offset, size)) {
            return;
        }
        for (std::uint32_t i = 0; i < size; ++i) {
            std::uint16_t key_length = 0;
            std::uint16_t value_length = 0;
            if (!read_u16_be(state_machine_snapshot, offset, key_length) ||
                offset + key_length > state_machine_snapshot.size()) {
                return;
            }
            auto key = state_machine_snapshot.substr(offset, key_length);
            offset += key_length;
            if (!read_u16_be(state_machine_snapshot, offset, value_length) ||
                offset + value_length > state_machine_snapshot.size()) {
                return;
            }
            auto value = state_machine_snapshot.substr(offset, value_length);
            offset += value_length;
            restored[std::move(key)] = std::move(value);
        }
        applied_kv_ = std::move(restored);
    }

    std::string RaftNode::apply_state_machine_command_locked(const std::string &data) {
        raft::StateMachineCommand command;
        if (!command.ParseFromString(data)) {
            return {};
        }

        switch (command.command_case()) {
            case raft::StateMachineCommand::kPut:
                applied_kv_[command.put().key()] = command.put().value();
                return {};
            case raft::StateMachineCommand::kDelete:
                applied_kv_.erase(command.delete_().key());
                return {};
            case raft::StateMachineCommand::kClear:
                applied_kv_.clear();
                return {};
            case raft::StateMachineCommand::kCas: {
                const auto &cas = command.cas();
                const auto found = applied_kv_.find(cas.key());
                const bool present = found != applied_kv_.end();
                const bool matched = present == cas.expected_present() &&
                                     (!present || found->second == cas.expected_value());
                bool current_present = present;
                std::string current_value = present ? found->second : "";
                if (matched) {
                    applied_kv_[cas.key()] = cas.new_value();
                    current_present = true;
                    current_value = cas.new_value();
                }

                raft::StateMachineCommandResult result;
                auto *cas_result = result.mutable_cas();
                cas_result->set_key(cas.key());
                cas_result->set_expected_present(cas.expected_present());
                cas_result->set_expected_value(cas.expected_value());
                cas_result->set_new_value(cas.new_value());
                cas_result->set_matched(matched);
                cas_result->set_current_present(current_present);
                cas_result->set_current_value(current_value);

                std::string encoded;
                if (!result.SerializeToString(&encoded)) {
                    throw std::runtime_error("failed to serialize StateMachineCommandResult");
                }
                return encoded;
            }
            case raft::StateMachineCommand::COMMAND_NOT_SET:
                return {};
        }
        return {};
    }

    std::optional<raft::InternalRaftCommand> RaftNode::parse_internal_command_locked(const std::string &data) const {
        raft::InternalRaftCommand command;
        if (data.starts_with(kInternalCommandPrefix)) {
            const auto payload = data.substr(kInternalCommandPrefix.size());
            if (!command.ParseFromString(payload)) {
                return std::nullopt;
            }
            return command;
        }

        if (!command.ParseFromString(data)) {
            return std::nullopt;
        }
        if (command.command_case() == raft::InternalRaftCommand::COMMAND_NOT_SET) {
            return std::nullopt;
        }
        return command;
    }

    bool RaftNode::apply_internal_command_locked(const std::string &data) {
        const auto parsed = parse_internal_command_locked(data);
        if (!parsed.has_value()) {
            return false;
        }

        const auto &command = *parsed;
        switch (command.command_case()) {
            case raft::InternalRaftCommand::kJoin:
                if (!command.join().member().id().empty() && command.join().member().id() != peer_id_) {
                    pending_join_ids_.insert(command.join().member().id());
                    auto &progress = peer_progress_[command.join().member().id()];
                    progress.next_index = std::max<std::int64_t>(1, progress.next_index);
                }
                return true;
            case raft::InternalRaftCommand::kJoint: {
                std::vector<std::string> peer_ids;
                peer_ids.reserve(command.joint().members_size());
                for (const auto &member: command.joint().members()) {
                    if (!member.id().empty() && member.id() != peer_id_) {
                        peer_ids.push_back(member.id());
                        pending_join_ids_.erase(member.id());
                    }
                }
                reconfigure_voting_peers_locked(std::move(peer_ids));
                joint_consensus_ = true;
                return true;
            }
            case raft::InternalRaftCommand::kFinalize:
                joint_consensus_ = false;
                return true;
            case raft::InternalRaftCommand::COMMAND_NOT_SET:
                return false;
        }
        return false;
    }

    std::string RaftNode::apply_log_entry_locked(const std::string &data) {
        if (apply_internal_command_locked(data)) {
            return {};
        }
        return apply_state_machine_command_locked(data);
    }

    void RaftNode::apply_committed_entries_locked() {
        if (snapshot_index_ > last_applied_) {
            apply_snapshot_to_state_machine_locked();
            last_applied_ = snapshot_index_;
        }

        for (const auto &entry: log_entries_) {
            if (entry.index <= last_applied_ || entry.index > commit_index_) {
                continue;
            }
            applied_command_results_.erase(entry.index);
            applied_command_results_[entry.index] = apply_log_entry_locked(entry.data);
            last_applied_ = entry.index;
        }
    }

    void RaftNode::seed_bootstrap_log_locked() {
        if (!log_entries_.empty() || last_log_index_ <= snapshot_index_) {
            return;
        }
        log_entries_.reserve(static_cast<std::size_t>(std::max<std::int64_t>(0, last_log_index_ - snapshot_index_)));
        for (std::int64_t index = snapshot_index_ + 1; index <= last_log_index_; ++index) {
            log_entries_.push_back(LogEntryRecord{
                .index = index,
                .term = last_log_term_,
                .data = "bootstrap-" + std::to_string(index),
            });
        }
        if (!log_entries_.empty()) {
            last_entry_data_ = log_entries_.back().data;
            if (log_entries_.size() >= 2) {
                previous_log_index_ = log_entries_[log_entries_.size() - 2].index;
                previous_log_term_ = log_entries_[log_entries_.size() - 2].term;
            } else {
                previous_log_index_ = snapshot_index_;
                previous_log_term_ = snapshot_term_;
            }
        }
    }

    void RaftNode::normalize_voting_peers_locked() {
        std::vector<std::string> normalized;
        normalized.reserve(voting_peers_.size());
        std::unordered_set<std::string> seen;
        for (const auto &peer_id: voting_peers_) {
            if (peer_id.empty() || peer_id == peer_id_) {
                continue;
            }
            if (seen.insert(peer_id).second) {
                normalized.push_back(peer_id);
            }
        }
        voting_peers_ = std::move(normalized);
    }

    void RaftNode::reconfigure_voting_peers_locked(std::vector<std::string> voting_peers) {
        voting_peers_ = std::move(voting_peers);
        normalize_voting_peers_locked();

        std::unordered_map<std::string, PeerProgress> next_progress;
        next_progress.reserve(voting_peers_.size() + 1);
        const auto self_found = peer_progress_.find(peer_id_);
        next_progress.emplace(
            peer_id_,
            self_found != peer_progress_.end()
                ? self_found->second
                : PeerProgress{.next_index = last_log_index_ + 1, .match_index = last_log_index_});
        next_progress[peer_id_].next_index = std::max(next_progress[peer_id_].next_index, last_log_index_ + 1);
        next_progress[peer_id_].match_index = std::max(next_progress[peer_id_].match_index, last_log_index_);

        for (const auto &peer_id: voting_peers_) {
            const auto found = peer_progress_.find(peer_id);
            if (found != peer_progress_.end()) {
                next_progress.emplace(peer_id, found->second);
                next_progress[peer_id].next_index = std::max(next_progress[peer_id].next_index,
                                                             static_cast<std::int64_t>(1));
            } else {
                next_progress.emplace(peer_id, PeerProgress{.next_index = last_log_index_ + 1, .match_index = 0});
            }
        }

        peer_progress_ = std::move(next_progress);
    }

    std::size_t RaftNode::cluster_size_locked() const {
        return 1 + voting_peers_.size();
    }

    std::size_t RaftNode::quorum_size_locked() const {
        return (cluster_size_locked() / 2) + 1;
    }

    void RaftNode::reset_peer_progress_locked() {
        std::unordered_map<std::string, PeerProgress> progress;
        progress.reserve(voting_peers_.size() + 1);
        progress.emplace(peer_id_, PeerProgress{.next_index = last_log_index_ + 1, .match_index = last_log_index_});
        for (const auto &peer_id: voting_peers_) {
            progress.emplace(peer_id, PeerProgress{.next_index = last_log_index_ + 1, .match_index = 0});
        }
        peer_progress_ = std::move(progress);
    }

    void RaftNode::start_election_locked() {
        role_ = Role::candidate;
        current_term_ += 1;
        voted_for_ = peer_id_;
        leader_id_.reset();
        last_activity_ = std::chrono::steady_clock::now();
        votes_granted_.clear();
        votes_responded_.clear();
        votes_granted_.insert(peer_id_);
        votes_responded_.insert(peer_id_);
    }

    void RaftNode::become_leader_locked() {
        role_ = Role::leader;
        leader_id_ = peer_id_;
        last_activity_ = std::chrono::steady_clock::now();
        votes_granted_.clear();
        votes_responded_.clear();
        reset_peer_progress_locked();
    }

    void RaftNode::update_commit_index_locked() {
        if (last_log_term_ != current_term_) {
            return;
        }

        std::size_t replicated = 0;
        for (const auto &[peer_id, progress]: peer_progress_) {
            (void) peer_id;
            if (progress.match_index >= last_log_index_) {
                replicated += 1;
            }
        }

        if (replicated >= quorum_size_locked()) {
            commit_index_ = std::max(commit_index_, last_log_index_);
        }
    }

    bool RaftNode::candidate_log_is_up_to_date_locked(const raft::VoteRequest &request) const {
        if (request.last_log_term() != last_log_term_) {
            return request.last_log_term() > last_log_term_;
        }
        return request.last_log_index() >= last_log_index_;
    }

    bool RaftNode::prev_log_matches_locked(std::int64_t prev_log_index, std::int64_t prev_log_term) const {
        if (prev_log_index == 0 && prev_log_term == 0) {
            return true;
        }
        return log_term_at_locked(prev_log_index) == prev_log_term;
    }

    std::int64_t RaftNode::log_term_at_locked(std::int64_t index) const {
        if (index == 0) {
            return 0;
        }
        if (index == snapshot_index_) {
            return snapshot_term_;
        }
        for (const auto &entry: log_entries_) {
            if (entry.index == index) {
                return entry.term;
            }
        }
        if (index == last_log_index_) {
            return last_log_term_;
        }
        if (index == previous_log_index_) {
            return previous_log_term_;
        }
        return 0;
    }

    void RaftNode::truncate_log_from_locked(std::int64_t index) {
        if (index <= 0) {
            log_entries_.clear();
            applied_command_results_.clear();
        } else {
            for (auto it = applied_command_results_.begin(); it != applied_command_results_.end();) {
                if (it->first >= index) {
                    it = applied_command_results_.erase(it);
                } else {
                    ++it;
                }
            }
            log_entries_.erase(
                std::remove_if(log_entries_.begin(), log_entries_.end(), [index](const auto &entry) {
                    return entry.index >= index;
                }),
                log_entries_.end());
        }
        if (!log_entries_.empty()) {
            last_log_index_ = log_entries_.back().index;
            last_log_term_ = log_entries_.back().term;
            last_entry_data_ = log_entries_.back().data;
            if (log_entries_.size() >= 2) {
                previous_log_index_ = log_entries_[log_entries_.size() - 2].index;
                previous_log_term_ = log_entries_[log_entries_.size() - 2].term;
            } else {
                previous_log_index_ = snapshot_index_;
                previous_log_term_ = snapshot_term_;
            }
        } else if (last_log_index_ > snapshot_index_) {
            last_log_index_ = snapshot_index_;
            last_log_term_ = snapshot_term_;
            previous_log_index_ = snapshot_index_;
            previous_log_term_ = snapshot_term_;
            last_entry_data_.clear();
        }
    }

    void RaftNode::truncate_prefix_up_to_locked(std::int64_t index) {
        if (index <= 0) {
            return;
        }
        for (auto it = applied_command_results_.begin(); it != applied_command_results_.end();) {
            if (it->first <= index) {
                it = applied_command_results_.erase(it);
            } else {
                ++it;
            }
        }
        log_entries_.erase(
            std::remove_if(log_entries_.begin(), log_entries_.end(), [index](const auto &entry) {
                return entry.index <= index;
            }),
            log_entries_.end());
        if (log_entries_.empty()) {
            last_log_index_ = std::max(last_log_index_, snapshot_index_);
            last_log_term_ = (last_log_index_ == snapshot_index_) ? snapshot_term_ : last_log_term_;
            previous_log_index_ = snapshot_index_;
            previous_log_term_ = snapshot_term_;
            if (last_log_index_ == snapshot_index_) {
                last_entry_data_.clear();
            }
            return;
        }

        if (log_entries_.front().index == snapshot_index_ + 1) {
            previous_log_index_ = snapshot_index_;
            previous_log_term_ = snapshot_term_;
        }
        last_log_index_ = log_entries_.back().index;
        last_log_term_ = log_entries_.back().term;
        last_entry_data_ = log_entries_.back().data;
    }

    void RaftNode::step_down_locked(std::int64_t new_term, std::optional<std::string> leader_id) {
        current_term_ = new_term;
        role_ = Role::follower;
        voted_for_.reset();
        leader_id_ = std::move(leader_id);
        last_activity_ = std::chrono::steady_clock::now();
        votes_granted_.clear();
        votes_responded_.clear();
    }

    std::string RaftNode::applied_command_result_at_locked(std::int64_t index) const {
        const auto found = applied_command_results_.find(index);
        if (found == applied_command_results_.end()) {
            return {};
        }
        return found->second;
    }
} // namespace raftcpp
