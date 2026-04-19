#pragma once

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <mutex>
#include <optional>
#include <string>
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

    struct PersistentState {
        std::string peer_id;
        std::int64_t current_term{0};
        std::optional<std::string> voted_for;
        std::optional<std::string> leader_id;
        std::vector<std::string> voting_peers;
        std::int64_t last_log_index{0};
        std::int64_t last_log_term{0};
        std::int64_t commit_index{0};
        std::int64_t snapshot_index{0};
        std::int64_t snapshot_term{0};
        std::int64_t previous_log_index{0};
        std::int64_t previous_log_term{0};
        std::string last_entry_data;
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
        normalize_voting_peers_locked();
        reset_peer_progress_locked();
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
                last_log_index_ = request.prev_log_index() + request.entries_size();
                last_log_term_ = request.entries(request.entries_size() - 1).term();
                match_index = last_log_index_;
            } else {
                match_index = request.prev_log_index();
            }

            commit_index_ = std::max(commit_index_, std::min(request.leader_commit(), match_index));
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
            success = true;
            if (request.done()) {
                snapshot_index_ = request.last_included_index();
                snapshot_term_ = request.last_included_term();
                if (last_log_index_ < snapshot_index_) {
                    last_log_index_ = snapshot_index_;
                    last_log_term_ = snapshot_term_;
                }
                commit_index_ = std::max(commit_index_, snapshot_index_);
            }
        }

        raft::InstallSnapshotResponse response;
        response.set_term(current_term_);
        response.set_peer_id(peer_id_);
        response.set_success(success);
        response.set_last_included_index(request.last_included_index());
        return response;
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
        voting_peers_ = std::move(voting_peers);
        normalize_voting_peers_locked();
        reset_peer_progress_locked();
    }

    std::vector<std::string> voting_peers() const {
        std::scoped_lock lock(mu_);
        return voting_peers_;
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
            return true;
        }

        progress.next_index = std::max<std::int64_t>(1, progress.next_index - 1);
        return false;
    }

    std::int64_t append_local_entry(std::string data) {
        std::scoped_lock lock(mu_);
        previous_log_index_ = last_log_index_;
        previous_log_term_ = last_log_term_;
        last_log_index_ += 1;
        last_log_term_ = current_term_;
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

        if (prev_log_index == 0) {
            request.set_prev_log_term(0);
        } else if (prev_log_index == snapshot_index_) {
            request.set_prev_log_term(snapshot_term_);
        } else if (!last_entry_data_.empty() && prev_log_index == previous_log_index_) {
            request.set_prev_log_term(previous_log_term_);
        } else if (prev_log_index == last_log_index_ && last_entry_data_.empty()) {
            request.set_prev_log_term(last_log_term_);
        } else {
            request.set_prev_log_term(0);
        }

        if (!last_entry_data_.empty() && next_index == last_log_index_) {
            auto* entry = request.add_entries();
            entry->set_term(last_log_term_);
            entry->set_peer_id(this->peer_id_);
            entry->set_data(last_entry_data_);
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
            .voting_peers = voting_peers_,
            .last_log_index = last_log_index_,
            .last_log_term = last_log_term_,
            .commit_index = commit_index_,
            .snapshot_index = snapshot_index_,
            .snapshot_term = snapshot_term_,
            .previous_log_index = previous_log_index_,
            .previous_log_term = previous_log_term_,
            .last_entry_data = last_entry_data_,
            .peer_progress = peer_progress_,
        };
    }

    void apply_persistent_state(const PersistentState& state) {
        std::scoped_lock lock(mu_);
        peer_id_ = state.peer_id;
        current_term_ = state.current_term;
        voted_for_ = state.voted_for;
        leader_id_ = state.leader_id;
        voting_peers_ = state.voting_peers;
        last_log_index_ = state.last_log_index;
        last_log_term_ = state.last_log_term;
        commit_index_ = state.commit_index;
        snapshot_index_ = state.snapshot_index;
        snapshot_term_ = state.snapshot_term;
        previous_log_index_ = state.previous_log_index;
        previous_log_term_ = state.previous_log_term;
        last_entry_data_ = state.last_entry_data;
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
        if (prev_log_index == last_log_index_ && prev_log_term == last_log_term_) {
            return true;
        }
        return prev_log_index == snapshot_index_ && prev_log_term == snapshot_term_;
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

    mutable std::mutex mu_;
    std::string peer_id_;
    Role role_;
    std::int64_t current_term_;
    std::optional<std::string> voted_for_;
    std::optional<std::string> leader_id_;
    std::int64_t last_log_index_;
    std::int64_t last_log_term_;
    std::int64_t commit_index_;
    std::int64_t snapshot_index_;
    std::int64_t snapshot_term_;
    std::int64_t previous_log_index_;
    std::int64_t previous_log_term_;
    std::string last_entry_data_;
    std::chrono::steady_clock::time_point last_activity_;
    std::vector<std::string> voting_peers_;
    std::unordered_map<std::string, PeerProgress> peer_progress_;
    std::unordered_set<std::string> votes_granted_;
    std::unordered_set<std::string> votes_responded_;
};

} // namespace raftcpp
