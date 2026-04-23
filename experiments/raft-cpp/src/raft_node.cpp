#include "raftcpp/raft_node.hpp"

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

std::string RaftNode::encode_internal_command(const raft::InternalRaftCommand& command) {
    std::string payload;
    if (!command.SerializeToString(&payload)) {
        throw std::runtime_error("failed to serialize InternalRaftCommand");
    }
    return payload;
}

raft::VoteResponse RaftNode::handle_vote_request(const raft::VoteRequest& request) {
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

raft::AppendEntriesResponse RaftNode::handle_append_entries(const raft::AppendEntriesRequest& request) {
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

raft::InstallSnapshotResponse RaftNode::handle_install_snapshot(const raft::InstallSnapshotRequest& request) {
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

bool RaftNode::has_pending_join(const std::string& peer_id) const {
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

bool RaftNode::handle_vote_response(const raft::VoteResponse& response) {
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

raft::AppendEntriesRequest RaftNode::make_heartbeat_request_for(const std::string& peer_id) const {
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

bool RaftNode::handle_append_entries_response(const std::string& peer_id, const raft::AppendEntriesResponse& response) {
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

raft::InstallSnapshotRequest RaftNode::make_install_snapshot_request_for(const std::string& peer_id) const {
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

bool RaftNode::handle_install_snapshot_response(const std::string& peer_id, const raft::InstallSnapshotResponse& response) {
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
        for (auto& [peer_id, progress] : peer_progress_) {
            if (peer_id != peer_id_) {
                progress.next_index = std::max(progress.next_index, last_log_index_ + 1);
            }
        }
    }
}

raft::AppendEntriesRequest RaftNode::make_replication_request_for(const std::string& peer_id) const {
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

std::optional<RaftNode::CommandCommitResult> RaftNode::append_and_commit_local_command(const std::string& data) {
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

void RaftNode::apply_persistent_state(const PersistentState& state) {
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

RaftNode::Role RaftNode::role() const {
    std::scoped_lock lock(mu_);
    return role_;
}

} // namespace raftcpp
