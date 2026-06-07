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

#include "graft/core/raft_node.hpp"

#include "graft/core/cluster_membership.hpp"
#include "graft/core/key_value_state_machine.hpp"
#include "graft/core/snapshot_codec.hpp"

#include <algorithm>
#include <stdexcept>
#include <utility>

namespace graft {
    namespace {
        std::int64_t system_time_millis() {
            return std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::system_clock::now().time_since_epoch())
                    .count();
        }
    }

    RaftNode::RaftNode(Config config)
        : peer_id_(std::move(config.peer_id)),
          role_(Role::follower),
          current_term_(config.current_term),
          last_log_index_(config.last_log_index),
          last_log_term_(config.last_log_term),
          commit_index_(config.commit_index),
          snapshot_index_(config.snapshot_index),
          snapshot_term_(config.snapshot_term),
          application_(std::move(config.application)),
          previous_log_index_(config.last_log_index),
          previous_log_term_(config.last_log_term),
          time_source_(std::move(config.time_source)),
          last_activity_(std::chrono::steady_clock::now()),
          voting_peers_(std::move(config.voting_peers)) {
        if (!time_source_) {
            time_source_ = system_time_millis;
        }
        if (!application_) {
            // The KV state machine is the built-in demo/default. Library users should
            // inject their own ApplicationStateMachine through Config.
            application_ = std::make_shared<KeyValueStateMachine>();
        }
        // Normalize immediately so every later majority calculation sees a consistent
        // membership model, even when the node was created from sparse smoke-test input.
        seed_bootstrap_log_locked();
        normalize_voting_peers_locked();
        seed_current_members_locked();
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
            // Observing a newer term invalidates any local leadership/candidacy.
            step_down_locked(request.term(), std::nullopt);
        }

        bool granted = false;
        if (!decommissioned_ &&
            is_known_voting_member_locked(request.candidate_id()) &&
            request.term() == current_term_ &&
            candidate_log_is_up_to_date_locked(request)) {
            if (!voted_for_.has_value() || *voted_for_ == request.candidate_id()) {
                // Grant at most one vote per term, and only to a candidate whose log is
                // at least as up-to-date as ours.
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
        if (!decommissioned_ &&
            is_known_voting_member_locked(request.leader_id()) &&
            request.term() == current_term_ && prev_log_matches_locked(request.prev_log_index(),
                                                                       request.prev_log_term())) {
            // A valid AppendEntries request is also the leader heartbeat. It refreshes
            // leader identity and suppresses local elections.
            role_ = Role::follower;
            leader_id_ = request.leader_id();
            last_activity_ = std::chrono::steady_clock::now();
            success = true;

            if (request.entries_size() > 0) {
                // Raft conflict handling: once the leader proves the previous entry, any local suffix after it
                // is replaced by the leader's entries.
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
        if (!decommissioned_ &&
            is_known_voting_member_locked(request.leader_id()) &&
            request.term() == current_term_) {
            role_ = Role::follower;
            leader_id_ = request.leader_id();
            last_activity_ = std::chrono::steady_clock::now();
            // Snapshot chunks are accepted only in strict offset order. A new index/term or offset zero restarts
            // the pending transfer, which matches the Java side's chunked InstallSnapshot behavior.
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
        // The caller-provided string is ignored intentionally; the authoritative
        // application snapshot must be captured from the state machine at the selected
        // committed index so Raft and domain state stay aligned.
        snapshot_data_ = wrap_snapshot_payload_locked(application_->snapshot());
        truncate_prefix_up_to_locked(snapshot_index_);
        commit_index_ = std::max(commit_index_, snapshot_index_);
        apply_committed_entries_locked();
        return true;
    }

    void RaftNode::become_candidate() {
        std::scoped_lock lock(mu_);
        if (decommissioned_ || !is_known_voting_member_locked(peer_id_)) {
            return;
        }
        start_election_locked();
    }

    void RaftNode::become_leader() {
        std::scoped_lock lock(mu_);
        if (decommissioned_ || !is_known_voting_member_locked(peer_id_)) {
            return;
        }
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

    std::vector<std::string> RaftNode::current_voting_members() const {
        std::scoped_lock lock(mu_);
        return current_voting_members_locked();
    }

    std::vector<std::string> RaftNode::next_voting_members() const {
        std::scoped_lock lock(mu_);
        return next_voting_members_locked();
    }

    bool RaftNode::has_joint_majority(const std::unordered_set<std::string> &peer_ids) const {
        std::scoped_lock lock(mu_);
        return has_joint_majority_locked(peer_ids);
    }

    std::int64_t RaftNode::current_time_millis() const {
        return time_source_ ? time_source_() : system_time_millis();
    }

    std::vector<raft::PeerSpec> RaftNode::current_member_specs() const {
        std::scoped_lock lock(mu_);
        return current_members_;
    }

    std::vector<raft::PeerSpec> RaftNode::next_member_specs() const {
        std::scoped_lock lock(mu_);
        return next_members_;
    }

    bool RaftNode::joint_consensus() const {
        std::scoped_lock lock(mu_);
        return joint_consensus_;
    }

    std::int64_t RaftNode::reconfiguration_started_at_millis() const {
        std::scoped_lock lock(mu_);
        return reconfiguration_started_at_millis_;
    }

    bool RaftNode::decommissioned() const {
        std::scoped_lock lock(mu_);
        return decommissioned_;
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
        if (decommissioned_ || !is_known_voting_member_locked(peer_id_)) {
            raft::VoteRequest request;
            request.set_term(current_term_);
            request.set_candidate_id(peer_id_);
            request.set_last_log_index(last_log_index_);
            request.set_last_log_term(last_log_term_);
            return request;
        }
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

        if (has_joint_majority_locked(votes_granted_)) {
            // This helper enforces the split-majority joint-consensus rule. A flat
            // majority of the union would be unsafe during reconfiguration.
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

        // Heartbeats carry the previous-log position for the follower. That lets a
        // heartbeat double as a consistency/read-barrier probe without sending entries.
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
        progress.reachable = true;
        progress.consecutive_failures = 0;
        if (response.success()) {
            progress.last_successful_contact_millis = current_time_millis();
            progress.match_index = std::max(progress.match_index, response.match_index());
            progress.next_index = std::max(progress.next_index, response.match_index() + 1);
            update_commit_index_locked();
            apply_committed_entries_locked();
            return true;
        }

        progress.next_index = std::max<std::int64_t>(1, progress.next_index - 1);
        return false;
    }

    bool RaftNode::can_serve_linearizable_read(std::int64_t lease_millis) const {
        std::scoped_lock lock(mu_);
        if (role_ != Role::leader || decommissioned_) {
            return false;
        }
        if (last_applied_ < commit_index_) {
            // A leader with unapplied committed entries could answer from stale local
            // application state even if its lease is fresh.
            return false;
        }
        std::unordered_set<std::string> fresh_voters;
        fresh_voters.insert(peer_id_);
        if (has_joint_majority_locked(fresh_voters)) {
            // Single-voter clusters have a quorum with the leader alone.
            return true;
        }

        // The lease is derived from recent successful contact, not from wall-clock
        // promises made by other nodes. Runtime refreshes this by sending heartbeats.
        const auto freshness_cutoff = std::max<std::int64_t>(0, current_time_millis() - std::max<std::int64_t>(1, lease_millis));
        for (const auto &peer_id: voting_peers_) {
            const auto found = peer_progress_.find(peer_id);
            if (found != peer_progress_.end() && found->second.last_successful_contact_millis >= freshness_cutoff) {
                fresh_voters.insert(peer_id);
            }
        }
        return has_joint_majority_locked(fresh_voters);
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

        auto &progress = peer_progress_[peer_id];
        progress.reachable = true;
        progress.consecutive_failures = 0;
        if (!response.success()) {
            return false;
        }

        progress.last_successful_contact_millis = current_time_millis();
        progress.match_index = std::max(progress.match_index, response.last_included_index());
        progress.next_index = std::max(progress.next_index, response.last_included_index() + 1);
        return true;
    }

    void RaftNode::record_peer_failure(const std::string &peer_id) {
        std::scoped_lock lock(mu_);
        auto &progress = peer_progress_[peer_id];
        progress.reachable = false;
        progress.consecutive_failures += 1;
        progress.last_failed_contact_millis = current_time_millis();
    }

    std::int64_t RaftNode::append_local_entry(std::string data) {
        std::scoped_lock lock(mu_);
        // Appending locally is only the first half of a write. Client-facing success is
        // reported later, after replication has advanced commit_index_ and the state
        // machine has applied the entry.
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
        peer_progress_[peer_id_].reachable = true;
        peer_progress_[peer_id_].consecutive_failures = 0;
        return last_log_index_;
    }

    void RaftNode::observe_local_append(std::int64_t last_log_index, std::int64_t last_log_term) {
        std::scoped_lock lock(mu_);
        last_log_index_ = std::max(last_log_index_, last_log_index);
        last_log_term_ = last_log_term;
        if (role_ == Role::leader) {
            peer_progress_[peer_id_].match_index = last_log_index_;
            peer_progress_[peer_id_].next_index = last_log_index_ + 1;
            peer_progress_[peer_id_].reachable = true;
            peer_progress_[peer_id_].consecutive_failures = 0;
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

        // Send every entry the follower is missing according to next_index. The bounded
        // runtime does not batch by size yet; snapshot fallback handles compacted gaps.
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
        return applied_kv_locked();
    }

    std::unordered_map<std::string, std::string> RaftNode::applied_kv_locked() const {
        if (const auto kv = std::dynamic_pointer_cast<KeyValueStateMachine>(application_); kv) {
            return kv->store();
        }
        return {};
    }

    std::string RaftNode::query_application(const std::string &data) const {
        std::scoped_lock lock(mu_);
        return application_->query(data);
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

        // Single-node clusters can commit immediately because the leader is the whole
        // quorum. Multi-node paths must go through RaftRuntime replication.
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
        peer_progress_[peer_id_].reachable = true;
        peer_progress_[peer_id_].consecutive_failures = 0;
        peer_progress_[peer_id_].last_successful_contact_millis = current_time_millis();
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
            .pending_decommission = pending_decommission_,
            .decommissioned = decommissioned_,
            .reconfiguration_started_at_millis = reconfiguration_started_at_millis_,
            .pending_join_ids = std::vector<std::string>(pending_join_ids_.begin(), pending_join_ids_.end()),
            .voting_peers = voting_peers_,
            .current_members = current_members_,
            .next_members = next_members_,
            .last_log_index = last_log_index_,
            .last_log_term = last_log_term_,
            .commit_index = commit_index_,
            .snapshot_index = snapshot_index_,
            .snapshot_term = snapshot_term_,
            .snapshot_data = snapshot_data_,
            .last_applied = last_applied_,
            .previous_log_index = previous_log_index_,
            .previous_log_term = previous_log_term_,
            .last_entry_data = last_entry_data_,
            .log_entries = log_entries_,
            .peer_progress = peer_progress_,
        };
    }

    void RaftNode::apply_persistent_state(const PersistentState &state) {
        std::scoped_lock lock(mu_);
        // Restore the exact durable Raft state first. The role is derived afterward so
        // stale volatile leadership does not accidentally survive a restart.
        peer_id_ = state.peer_id;
        current_term_ = state.current_term;
        voted_for_ = state.voted_for;
        leader_id_ = state.leader_id;
        joint_consensus_ = state.joint_consensus;
        pending_decommission_ = state.pending_decommission;
        decommissioned_ = state.decommissioned;
        reconfiguration_started_at_millis_ = state.reconfiguration_started_at_millis;
        if (joint_consensus_ && reconfiguration_started_at_millis_ <= 0) {
            reconfiguration_started_at_millis_ = current_time_millis();
        }
        if (!joint_consensus_) {
            reconfiguration_started_at_millis_ = 0;
        }
        pending_join_ids_ = std::unordered_set<std::string>(state.pending_join_ids.begin(),
                                                            state.pending_join_ids.end());
        voting_peers_ = state.voting_peers;
        current_members_ = ClusterMembership::normalize_member_specs(state.current_members);
        next_members_ = ClusterMembership::normalize_member_specs(state.next_members);
        last_log_index_ = state.last_log_index;
        last_log_term_ = state.last_log_term;
        commit_index_ = state.commit_index;
        snapshot_index_ = state.snapshot_index;
        snapshot_term_ = state.snapshot_term;
        snapshot_data_ = state.snapshot_data;
        last_applied_ = snapshot_index_;
        if (!snapshot_data_.empty()) {
            // Snapshot data is stored wrapped. The application receives only its own
            // payload; membership metadata has already been restored into Raft fields.
            application_->restore(SnapshotCodec::unwrap_payload(snapshot_data_));
        }
        applied_command_results_.clear();
        previous_log_index_ = state.previous_log_index;
        previous_log_term_ = state.previous_log_term;
        last_entry_data_ = state.last_entry_data;
        log_entries_ = state.log_entries;
        if (log_entries_.empty()) {
            // Some older persisted/smoke states carry only last index/term. Seed a
            // synthetic log suffix so consistency checks have deterministic terms.
            seed_bootstrap_log_locked();
        }
        role_ = !decommissioned_ && leader_id_.has_value() && *leader_id_ == peer_id_ ? Role::leader : Role::follower;
        last_activity_ = std::chrono::steady_clock::now();
        normalize_voting_peers_locked();
        if (current_members_.empty()) {
            seed_current_members_locked();
        }
        if (!joint_consensus_) {
            next_members_.clear();
        } else if (next_members_.empty()) {
            std::vector<raft::PeerSpec> members;
            members.reserve(voting_peers_.size() + 1);
            auto *self = &members.emplace_back();
            self->set_id(peer_id_);
            self->set_role("VOTER");
            for (const auto &peer_id: voting_peers_) {
                auto *member = &members.emplace_back();
                member->set_id(peer_id);
                member->set_role("VOTER");
            }
            next_members_ = ClusterMembership::normalize_member_specs(std::move(members));
        }
        for (const auto &[peer_id, _]: state.peer_progress) {
            if (peer_id != peer_id_) {
                // Peer progress is not authoritative membership, but it may reveal
                // recovered peers from older state files. Normalize afterward.
                voting_peers_.push_back(peer_id);
            }
        }
        normalize_voting_peers_locked();
        reset_peer_progress_locked();
        for (const auto &[peer_id, progress]: state.peer_progress) {
            if (!peer_progress_.contains(peer_id)) {
                continue;
            }
            auto restored = progress;
            restored.last_successful_contact_millis = 0;
            restored.last_failed_contact_millis = 0;
            peer_progress_[peer_id] = restored;
        }
        apply_committed_entries_locked();
    }

    RaftNode::Role RaftNode::role() const {
        std::scoped_lock lock(mu_);
        return role_;
    }

    std::string RaftNode::wrap_snapshot_payload_locked(const std::string &state_machine_snapshot) const {
        std::vector<std::string> current_members;
        current_members.reserve(voting_peers_.size() + 1);
        current_members.push_back(peer_id_);
        for (const auto &peer: voting_peers_) {
            if (peer != peer_id_) {
                current_members.push_back(peer);
            }
        }
        std::sort(current_members.begin(), current_members.end());

        // The snapshot wrapper currently stores member ids. This keeps the C++ snapshot
        // compatible with the bounded mixed-language snapshot tests.
        return SnapshotCodec::wrap_payload(
            current_members,
            joint_consensus_ ? current_members : std::vector<std::string>{},
            state_machine_snapshot);
    }

    void RaftNode::apply_snapshot_to_state_machine_locked() {
        if (snapshot_data_.empty()) {
            return;
        }

        application_->restore(SnapshotCodec::unwrap_payload(snapshot_data_));
    }

    std::string RaftNode::apply_state_machine_command_locked(const LogEntryRecord &entry) {
        return application_->apply(entry.index, entry.term, entry.data);
    }

    std::optional<raft::InternalRaftCommand> RaftNode::parse_internal_command_locked(const std::string &data) const {
        raft::InternalRaftCommand command;
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

        // Internal commands are consumed by Raft and are not delivered to the domain
        // state machine. They still travel through the log so membership changes obey
        // the same ordering and commit rules as ordinary commands.
        const auto &command = *parsed;
        switch (command.command_case()) {
            case raft::InternalRaftCommand::kJoin:
                // Join records a pending peer and initializes progress so the leader
                // can catch it up before voter promotion.
                if (!command.join().member().id().empty() && command.join().member().id() != peer_id_) {
                    pending_join_ids_.insert(command.join().member().id());
                    auto &progress = peer_progress_[command.join().member().id()];
                    progress.next_index = std::max<std::int64_t>(1, progress.next_index);
                }
                return true;
            case raft::InternalRaftCommand::kJoint: {
                // Joint consensus activates both current and next configurations. The
                // node may discover here that it is being removed, but final removal is
                // delayed until the finalize entry commits.
                bool includes_self = false;
                seed_current_members_locked();
                std::vector<raft::PeerSpec> next_members;
                next_members.reserve(command.joint().members_size());
                for (const auto &member: command.joint().members()) {
                    if (!member.id().empty()) {
                        next_members.push_back(member);
                    }
                    if (member.id() == peer_id_) {
                        includes_self = true;
                    } else if (!member.id().empty()) {
                        pending_join_ids_.erase(member.id());
                        auto &progress = peer_progress_[member.id()];
                        progress.next_index = std::max<std::int64_t>(1, progress.next_index);
                    }
                }
                next_members_ = ClusterMembership::normalize_member_specs(std::move(next_members));
                if (!joint_consensus_ || reconfiguration_started_at_millis_ <= 0) {
                    reconfiguration_started_at_millis_ = current_time_millis();
                }
                joint_consensus_ = true;
                pending_decommission_ = !includes_self;
                refresh_voting_peers_from_members_locked();
                return true;
            }
            case raft::InternalRaftCommand::kFinalize:
                // Finalize collapses the next configuration into the stable current
                // configuration. If this node was removed, it steps down permanently.
                joint_consensus_ = false;
                reconfiguration_started_at_millis_ = 0;
                if (!next_members_.empty()) {
                    current_members_ = next_members_;
                }
                next_members_.clear();
                refresh_voting_peers_from_members_locked();
                if (pending_decommission_) {
                    decommissioned_ = true;
                    role_ = Role::follower;
                    leader_id_.reset();
                    voted_for_.reset();
                    votes_granted_.clear();
                    votes_responded_.clear();
                }
                pending_decommission_ = false;
                return true;
            case raft::InternalRaftCommand::COMMAND_NOT_SET:
                return false;
        }
        return false;
    }

    std::string RaftNode::apply_log_entry_locked(const LogEntryRecord &entry) {
        if (apply_internal_command_locked(entry.data)) {
            return {};
        }
        return apply_state_machine_command_locked(entry);
    }

    void RaftNode::apply_committed_entries_locked() {
        if (snapshot_index_ > last_applied_) {
            // Restore snapshot state before replaying post-snapshot entries.
            apply_snapshot_to_state_machine_locked();
            last_applied_ = snapshot_index_;
        }

        for (const auto &entry: log_entries_) {
            if (entry.index <= last_applied_ || entry.index > commit_index_) {
                continue;
            }
            applied_command_results_.erase(entry.index);
            // The result is stored by log index so the client RPC path can return the
            // deterministic application result after commit/apply.
            applied_command_results_[entry.index] = apply_log_entry_locked(entry);
            last_applied_ = entry.index;
        }
    }

    void RaftNode::seed_bootstrap_log_locked() {
        if (!log_entries_.empty() || last_log_index_ <= snapshot_index_) {
            return;
        }
        // Some CLI probes start from only last index/term values. Synthetic entries let prev-log consistency
        // checks and backtracking behave deterministically until real persisted log entries are available.
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

    void RaftNode::seed_current_members_locked() {
        if (!current_members_.empty()) {
            current_members_ = ClusterMembership::normalize_member_specs(std::move(current_members_));
            return;
        }
        std::vector<raft::PeerSpec> members;
        members.reserve(voting_peers_.size() + 1);
        auto *self = &members.emplace_back();
        self->set_id(peer_id_);
        self->set_role("VOTER");
        for (const auto &peer_id: voting_peers_) {
            auto *member = &members.emplace_back();
            member->set_id(peer_id);
            member->set_role("VOTER");
        }
        current_members_ = ClusterMembership::normalize_member_specs(std::move(members));
    }

    void RaftNode::reconfigure_voting_peers_locked(std::vector<std::string> voting_peers) {
        voting_peers_ = std::move(voting_peers);
        normalize_voting_peers_locked();
        if (!joint_consensus_) {
            std::vector<raft::PeerSpec> members;
            members.reserve(voting_peers_.size() + 1);
            auto *self = &members.emplace_back();
            self->set_id(peer_id_);
            self->set_role("VOTER");
            for (const auto &peer_id: voting_peers_) {
                auto *member = &members.emplace_back();
                member->set_id(peer_id);
                member->set_role("VOTER");
            }
            current_members_ = ClusterMembership::normalize_member_specs(std::move(members));
            next_members_.clear();
        }
        refresh_voting_peers_from_members_locked();
    }

    std::vector<std::string> RaftNode::current_voting_members_locked() const {
        return ClusterMembership::current_voting_members(peer_id_, decommissioned_, voting_peers_, current_members_);
    }

    std::vector<std::string> RaftNode::next_voting_members_locked() const {
        return ClusterMembership::next_voting_members(joint_consensus_, current_voting_members_locked(), next_members_);
    }

    std::vector<std::string> RaftNode::active_voting_members_locked() const {
        return ClusterMembership::active_voting_members(current_voting_members_locked(), next_voting_members_locked());
    }

    void RaftNode::refresh_voting_peers_from_members_locked() {
        auto active_voters = active_voting_members_locked();
        voting_peers_.clear();
        voting_peers_.reserve(active_voters.size());
        for (const auto &peer_id: active_voters) {
            if (peer_id != peer_id_) {
                voting_peers_.push_back(peer_id);
            }
        }

        std::unordered_map<std::string, PeerProgress> next_progress;
        next_progress.reserve(voting_peers_.size() + 1);
        const auto self_found = peer_progress_.find(peer_id_);
        next_progress.emplace(
            peer_id_,
            self_found != peer_progress_.end()
                ? self_found->second
                : PeerProgress{.next_index = last_log_index_ + 1, .match_index = last_log_index_, .reachable = true});
        next_progress[peer_id_].next_index = std::max(next_progress[peer_id_].next_index, last_log_index_ + 1);
        next_progress[peer_id_].match_index = std::max(next_progress[peer_id_].match_index, last_log_index_);
        next_progress[peer_id_].reachable = true;
        next_progress[peer_id_].consecutive_failures = 0;

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

    bool RaftNode::has_joint_majority_locked(const std::unordered_set<std::string> &peer_ids) const {
        return ClusterMembership::has_joint_majority(
            peer_ids,
            current_voting_members_locked(),
            next_voting_members_locked(),
            joint_consensus_);
    }

    std::size_t RaftNode::cluster_size_locked() const {
        return active_voting_members_locked().size();
    }

    std::size_t RaftNode::quorum_size_locked() const {
        return (cluster_size_locked() / 2) + 1;
    }

    void RaftNode::reset_peer_progress_locked() {
        std::unordered_map<std::string, PeerProgress> progress;
        const auto active_voters = active_voting_members_locked();
        progress.reserve(active_voters.size());
        progress.emplace(peer_id_, PeerProgress{
                             .next_index = last_log_index_ + 1,
                             .match_index = last_log_index_,
                             .reachable = true,
                         });
        voting_peers_.clear();
        for (const auto &peer_id: active_voters) {
            if (peer_id == peer_id_) {
                continue;
            }
            voting_peers_.push_back(peer_id);
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
        auto candidate_commit = commit_index_;
        for (auto index = commit_index_ + 1; index <= last_log_index_; ++index) {
            if (log_term_at_locked(index) != current_term_) {
                // Standard Raft guard: a leader advances commit by counting replicas for
                // entries from its own term, which indirectly commits older entries.
                continue;
            }
            std::unordered_set<std::string> replicated;
            for (const auto &[peer_id, progress]: peer_progress_) {
                if (progress.match_index >= index) {
                    replicated.insert(peer_id);
                }
            }
            if (has_joint_majority_locked(replicated)) {
                candidate_commit = index;
            }
        }
        commit_index_ = std::max(commit_index_, candidate_commit);
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

    bool RaftNode::is_known_voting_member_locked(const std::string &peer_id) const {
        if (peer_id.empty()) {
            return false;
        }
        const auto active_voters = active_voting_members_locked();
        return std::find(active_voters.begin(), active_voters.end(), peer_id) != active_voters.end();
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
} // namespace graft
