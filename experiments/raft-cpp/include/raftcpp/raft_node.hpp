#pragma once

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "raft.pb.h"

namespace raftcpp {

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

    explicit RaftNode(Config config)
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

    static constexpr std::string_view kInternalCommandPrefix = "raft-internal:";

    static std::string encode_internal_command(const raft::InternalRaftCommand& command) {
        std::string payload;
        if (!command.SerializeToString(&payload)) {
            throw std::runtime_error("failed to serialize InternalRaftCommand");
        }
        return std::string(kInternalCommandPrefix) + payload;
    }

    raft::VoteResponse handle_vote_request(const raft::VoteRequest& request) {
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

    raft::AppendEntriesResponse handle_append_entries(const raft::AppendEntriesRequest& request) {
        std::scoped_lock lock(mu_);

        if (request.term() > current_term_) {
            step_down_locked(request.term(), request.leader_id());
        }

        bool success = false;
        std::int64_t match_index = 0;
        if (request.term() == current_term_ && prev_log_matches_locked(request.prev_log_index(), request.prev_log_term())) {
            role_ = Role::follower;
            leader_id_ = request.leader_id();
            last_activity_ = std::chrono::steady_clock::now();
            success = true;

            if (request.entries_size() > 0) {
                truncate_log_from_locked(request.prev_log_index() + 1);
                for (int i = 0; i < request.entries_size(); ++i) {
                    const auto& entry = request.entries(i);
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

    raft::InstallSnapshotResponse handle_install_snapshot(const raft::InstallSnapshotRequest& request) {
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

    bool compact_snapshot_to(std::int64_t index, std::string snapshot_data) {
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
        snapshot_data_ = serialize_state_machine_snapshot_locked(applied_kv_);
        if (!snapshot_data.empty()) {
            snapshot_data_.insert(0, "__meta__=" + snapshot_data + "\n");
        }
        truncate_prefix_up_to_locked(snapshot_index_);
        commit_index_ = std::max(commit_index_, snapshot_index_);
        apply_committed_entries_locked();
        return true;
    }

    void become_candidate() {
        std::scoped_lock lock(mu_);
        start_election_locked();
    }

    void become_leader() {
        std::scoped_lock lock(mu_);
        become_leader_locked();
    }

    void set_voting_peers(std::vector<std::string> voting_peers) {
        std::scoped_lock lock(mu_);
        reconfigure_voting_peers_locked(std::move(voting_peers));
    }

    std::vector<std::string> voting_peers() const {
        std::scoped_lock lock(mu_);
        return voting_peers_;
    }

    bool joint_consensus() const {
        std::scoped_lock lock(mu_);
        return joint_consensus_;
    }

    bool has_pending_join(const std::string& peer_id) const {
        std::scoped_lock lock(mu_);
        return pending_join_ids_.contains(peer_id);
    }

    std::size_t quorum_size() const {
        std::scoped_lock lock(mu_);
        return quorum_size_locked();
    }

    std::size_t granted_votes() const {
        std::scoped_lock lock(mu_);
        return votes_granted_.size();
    }

    std::chrono::steady_clock::time_point last_activity() const {
        std::scoped_lock lock(mu_);
        return last_activity_;
    }

    std::optional<std::string> voted_for() const {
        std::scoped_lock lock(mu_);
        return voted_for_;
    }

    raft::VoteRequest start_election() {
        std::scoped_lock lock(mu_);
        start_election_locked();

        raft::VoteRequest request;
        request.set_term(current_term_);
        request.set_candidate_id(peer_id_);
        request.set_last_log_index(last_log_index_);
        request.set_last_log_term(last_log_term_);
        return request;
    }

    bool handle_vote_response(const raft::VoteResponse& response) {
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

    raft::AppendEntriesRequest make_heartbeat_request_for(const std::string& peer_id) const {
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

    std::vector<raft::AppendEntriesRequest> make_heartbeat_requests() const {
        std::scoped_lock lock(mu_);

        std::vector<raft::AppendEntriesRequest> requests;
        requests.reserve(voting_peers_.size());
        for (const auto& peer_id : voting_peers_) {
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

    bool handle_append_entries_response(const std::string& peer_id, const raft::AppendEntriesResponse& response) {
        std::scoped_lock lock(mu_);

        if (response.term() > current_term_) {
            step_down_locked(response.term(), std::nullopt);
            return false;
        }
        if (role_ != Role::leader || response.term() != current_term_) {
            return false;
        }

        auto& progress = peer_progress_[peer_id];
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

    raft::InstallSnapshotRequest make_install_snapshot_request_for(const std::string& peer_id) const {
        std::scoped_lock lock(mu_);
        (void)peer_id;

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

    bool handle_install_snapshot_response(const std::string& peer_id, const raft::InstallSnapshotResponse& response) {
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

        auto& progress = peer_progress_[peer_id];
        progress.match_index = std::max(progress.match_index, response.last_included_index());
        progress.next_index = std::max(progress.next_index, response.last_included_index() + 1);
        return true;
    }

    std::int64_t append_local_entry(std::string data) {
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

    void observe_local_append(std::int64_t last_log_index, std::int64_t last_log_term) {
        std::scoped_lock lock(mu_);
        last_log_index_ = std::max(last_log_index_, last_log_index);
        last_log_term_ = last_log_term;
        if (role_ == Role::leader) {
            peer_progress_[peer_id_].match_index = last_log_index_;
            peer_progress_[peer_id_].next_index = last_log_index_ + 1;
            for (auto& [peer_id, progress] : peer_progress_) {
                if (peer_id != peer_id_) {
                    progress.next_index = std::max(progress.next_index, last_log_index_ + 1);
                }
            }
        }
    }

    raft::AppendEntriesRequest make_replication_request_for(const std::string& peer_id) const {
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

        for (const auto& local_entry : log_entries_) {
            if (local_entry.index < next_index) {
                continue;
            }
            auto* entry = request.add_entries();
            entry->set_term(local_entry.term);
            entry->set_peer_id(this->peer_id_);
            entry->set_data(local_entry.data);
        }

        return request;
    }

    std::string peer_id() const {
        std::scoped_lock lock(mu_);
        return peer_id_;
    }

    std::int64_t current_term() const {
        std::scoped_lock lock(mu_);
        return current_term_;
    }

    std::int64_t last_log_index() const {
        std::scoped_lock lock(mu_);
        return last_log_index_;
    }

    std::int64_t last_log_term() const {
        std::scoped_lock lock(mu_);
        return last_log_term_;
    }

    std::int64_t commit_index() const {
        std::scoped_lock lock(mu_);
        return commit_index_;
    }

    std::int64_t snapshot_index() const {
        std::scoped_lock lock(mu_);
        return snapshot_index_;
    }

    std::int64_t snapshot_term() const {
        std::scoped_lock lock(mu_);
        return snapshot_term_;
    }

    std::string snapshot_data() const {
        std::scoped_lock lock(mu_);
        return snapshot_data_;
    }

    std::int64_t last_applied() const {
        std::scoped_lock lock(mu_);
        return last_applied_;
    }

    std::unordered_map<std::string, std::string> applied_kv() const {
        std::scoped_lock lock(mu_);
        return applied_kv_;
    }

    std::string applied_command_result(std::int64_t index) const {
        std::scoped_lock lock(mu_);
        return applied_command_result_at_locked(index);
    }

    std::optional<CommandCommitResult> append_and_commit_local_command(const std::string& data) {
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

    std::optional<std::string> leader_id() const {
        std::scoped_lock lock(mu_);
        return leader_id_;
    }

    std::unordered_map<std::string, PeerProgress> peer_progress() const {
        std::scoped_lock lock(mu_);
        return peer_progress_;
    }

    PersistentState persistent_state() const {
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

    void apply_persistent_state(const PersistentState& state) {
        std::scoped_lock lock(mu_);
        peer_id_ = state.peer_id;
        current_term_ = state.current_term;
        voted_for_ = state.voted_for;
        leader_id_ = state.leader_id;
        joint_consensus_ = state.joint_consensus;
        pending_join_ids_ = std::unordered_set<std::string>(state.pending_join_ids.begin(), state.pending_join_ids.end());
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
        for (const auto& [peer_id, _] : state.peer_progress) {
            if (peer_id != peer_id_) {
                voting_peers_.push_back(peer_id);
            }
        }
        normalize_voting_peers_locked();
        reset_peer_progress_locked();
        for (const auto& [peer_id, progress] : state.peer_progress) {
            peer_progress_[peer_id] = progress;
        }
    }

    Role role() const {
        std::scoped_lock lock(mu_);
        return role_;
    }

private:
    static std::string serialize_state_machine_snapshot_locked(const std::unordered_map<std::string, std::string>& applied_kv) {
        std::string out;
        for (const auto& [key, value] : applied_kv) {
            out.append(key);
            out.push_back('=');
            out.append(value);
            out.push_back('\n');
        }
        return out;
    }

    void apply_snapshot_to_state_machine_locked() {
        if (snapshot_data_.empty()) {
            return;
        }

        std::unordered_map<std::string, std::string> restored;
        bool parsed_any = false;
        std::size_t start = 0;
        while (start < snapshot_data_.size()) {
            const auto end = snapshot_data_.find('\n', start);
            const auto line = snapshot_data_.substr(start, end == std::string::npos ? std::string::npos : end - start);
            if (!line.empty()) {
                const auto split = line.find('=');
                if (split != std::string::npos) {
                    parsed_any = true;
                    const auto key = line.substr(0, split);
                    if (key != "__meta__") {
                        restored[key] = line.substr(split + 1);
                    }
                }
            }
            if (end == std::string::npos) {
                break;
            }
            start = end + 1;
        }
        if (parsed_any) {
            applied_kv_ = std::move(restored);
        }
    }

    std::string apply_state_machine_command_locked(const std::string& data) {
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
            const auto& cas = command.cas();
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
            auto* cas_result = result.mutable_cas();
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

    bool apply_internal_command_locked(const std::string& data) {
        if (!data.starts_with(kInternalCommandPrefix)) {
            return false;
        }

        raft::InternalRaftCommand command;
        const auto payload = data.substr(kInternalCommandPrefix.size());
        if (!command.ParseFromString(payload)) {
            return false;
        }

        switch (command.command_case()) {
        case raft::InternalRaftCommand::kJoin:
            if (!command.join().member().id().empty() && command.join().member().id() != peer_id_) {
                pending_join_ids_.insert(command.join().member().id());
            }
            return true;
        case raft::InternalRaftCommand::kJoint: {
            std::vector<std::string> peer_ids;
            peer_ids.reserve(command.joint().members_size());
            for (const auto& member : command.joint().members()) {
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

    std::string apply_log_entry_locked(const std::string& data) {
        if (apply_internal_command_locked(data)) {
            return {};
        }
        return apply_state_machine_command_locked(data);
    }

    void apply_committed_entries_locked() {
        if (snapshot_index_ > last_applied_) {
            apply_snapshot_to_state_machine_locked();
            last_applied_ = snapshot_index_;
        }

        for (const auto& entry : log_entries_) {
            if (entry.index <= last_applied_ || entry.index > commit_index_) {
                continue;
            }
            applied_command_results_.erase(entry.index);
            applied_command_results_[entry.index] = apply_log_entry_locked(entry.data);
            last_applied_ = entry.index;
        }
    }

    void seed_bootstrap_log_locked() {
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

    void normalize_voting_peers_locked() {
        std::vector<std::string> normalized;
        normalized.reserve(voting_peers_.size());
        std::unordered_set<std::string> seen;
        for (const auto& peer_id : voting_peers_) {
            if (peer_id.empty() || peer_id == peer_id_) {
                continue;
            }
            if (seen.insert(peer_id).second) {
                normalized.push_back(peer_id);
            }
        }
        voting_peers_ = std::move(normalized);
    }

    void reconfigure_voting_peers_locked(std::vector<std::string> voting_peers) {
        voting_peers_ = std::move(voting_peers);
        normalize_voting_peers_locked();

        std::unordered_map<std::string, PeerProgress> next_progress;
        next_progress.reserve(voting_peers_.size() + 1);
        const auto self_found = peer_progress_.find(peer_id_);
        next_progress.emplace(
            peer_id_,
            self_found != peer_progress_.end()
                ? self_found->second
                : PeerProgress{.next_index = last_log_index_ + 1, .match_index = last_log_index_}
        );
        next_progress[peer_id_].next_index = std::max(next_progress[peer_id_].next_index, last_log_index_ + 1);
        next_progress[peer_id_].match_index = std::max(next_progress[peer_id_].match_index, last_log_index_);

        for (const auto& peer_id : voting_peers_) {
            const auto found = peer_progress_.find(peer_id);
            if (found != peer_progress_.end()) {
                next_progress.emplace(peer_id, found->second);
                next_progress[peer_id].next_index = std::max(next_progress[peer_id].next_index, static_cast<std::int64_t>(1));
            } else {
                next_progress.emplace(peer_id, PeerProgress{.next_index = last_log_index_ + 1, .match_index = 0});
            }
        }

        peer_progress_ = std::move(next_progress);
    }

    std::size_t cluster_size_locked() const {
        return 1 + voting_peers_.size();
    }

    std::size_t quorum_size_locked() const {
        return (cluster_size_locked() / 2) + 1;
    }

    void reset_peer_progress_locked() {
        std::unordered_map<std::string, PeerProgress> progress;
        progress.reserve(voting_peers_.size() + 1);
        progress.emplace(peer_id_, PeerProgress{.next_index = last_log_index_ + 1, .match_index = last_log_index_});
        for (const auto& peer_id : voting_peers_) {
            progress.emplace(peer_id, PeerProgress{.next_index = last_log_index_ + 1, .match_index = 0});
        }
        peer_progress_ = std::move(progress);
    }

    void start_election_locked() {
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

    void become_leader_locked() {
        role_ = Role::leader;
        leader_id_ = peer_id_;
        last_activity_ = std::chrono::steady_clock::now();
        votes_granted_.clear();
        votes_responded_.clear();
        reset_peer_progress_locked();
    }

    void update_commit_index_locked() {
        if (last_log_term_ != current_term_) {
            return;
        }

        std::size_t replicated = 0;
        for (const auto& [peer_id, progress] : peer_progress_) {
            (void)peer_id;
            if (progress.match_index >= last_log_index_) {
                replicated += 1;
            }
        }

        if (replicated >= quorum_size_locked()) {
            commit_index_ = std::max(commit_index_, last_log_index_);
        }
    }

    bool candidate_log_is_up_to_date_locked(const raft::VoteRequest& request) const {
        if (request.last_log_term() != last_log_term_) {
            return request.last_log_term() > last_log_term_;
        }
        return request.last_log_index() >= last_log_index_;
    }

    bool prev_log_matches_locked(std::int64_t prev_log_index, std::int64_t prev_log_term) const {
        if (prev_log_index == 0 && prev_log_term == 0) {
            return true;
        }
        return log_term_at_locked(prev_log_index) == prev_log_term;
    }

    std::int64_t log_term_at_locked(std::int64_t index) const {
        if (index == 0) {
            return 0;
        }
        if (index == snapshot_index_) {
            return snapshot_term_;
        }
        for (const auto& entry : log_entries_) {
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

    void truncate_log_from_locked(std::int64_t index) {
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
                std::remove_if(log_entries_.begin(), log_entries_.end(), [index](const auto& entry) {
                    return entry.index >= index;
                }),
                log_entries_.end()
            );
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

    void truncate_prefix_up_to_locked(std::int64_t index) {
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
            std::remove_if(log_entries_.begin(), log_entries_.end(), [index](const auto& entry) {
                return entry.index <= index;
            }),
            log_entries_.end()
        );
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

    void step_down_locked(std::int64_t new_term, std::optional<std::string> leader_id) {
        current_term_ = new_term;
        role_ = Role::follower;
        voted_for_.reset();
        leader_id_ = std::move(leader_id);
        last_activity_ = std::chrono::steady_clock::now();
        votes_granted_.clear();
        votes_responded_.clear();
    }

    std::string applied_command_result_at_locked(std::int64_t index) const {
        const auto found = applied_command_results_.find(index);
        if (found == applied_command_results_.end()) {
            return {};
        }
        return found->second;
    }

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

} // namespace raftcpp
