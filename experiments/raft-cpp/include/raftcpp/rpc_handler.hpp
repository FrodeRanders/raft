#pragma once

#include <cstdint>
#include <mutex>
#include <memory>
#include <optional>
#include <string>

#include "raft.pb.h"

namespace raftcpp {

class RpcHandler {
public:
    virtual ~RpcHandler() = default;

    virtual std::optional<raft::VoteResponse> on_vote_request(const raft::VoteRequest& request) = 0;
    virtual std::optional<raft::AppendEntriesResponse> on_append_entries_request(const raft::AppendEntriesRequest& request) = 0;
    virtual std::optional<raft::InstallSnapshotResponse> on_install_snapshot_request(const raft::InstallSnapshotRequest& request) = 0;
};

class StubRpcHandler final : public RpcHandler {
public:
    StubRpcHandler(std::string peer_id, std::int64_t current_term)
        : peer_id_(std::move(peer_id)), current_term_(current_term) {
    }

    std::optional<raft::VoteResponse> on_vote_request(const raft::VoteRequest& request) override {
        raft::VoteResponse response;
        response.set_peer_id(peer_id_);
        response.set_term(request.term());
        response.set_vote_granted(false);
        response.set_current_term(current_term_);
        return response;
    }

    std::optional<raft::AppendEntriesResponse> on_append_entries_request(const raft::AppendEntriesRequest&) override {
        raft::AppendEntriesResponse response;
        response.set_term(current_term_);
        response.set_peer_id(peer_id_);
        response.set_success(false);
        response.set_match_index(0);
        return response;
    }

    std::optional<raft::InstallSnapshotResponse> on_install_snapshot_request(const raft::InstallSnapshotRequest& request) override {
        raft::InstallSnapshotResponse response;
        response.set_term(current_term_);
        response.set_peer_id(peer_id_);
        response.set_success(false);
        response.set_last_included_index(request.last_included_index());
        return response;
    }

private:
    std::string peer_id_;
    std::int64_t current_term_;
};

using RpcHandlerPtr = std::shared_ptr<RpcHandler>;

class InMemoryRpcHandler final : public RpcHandler {
public:
    InMemoryRpcHandler(std::string peer_id, std::int64_t current_term, std::int64_t last_log_index, std::int64_t last_log_term)
        : peer_id_(std::move(peer_id)),
          current_term_(current_term),
          last_log_index_(last_log_index),
          last_log_term_(last_log_term),
          snapshot_index_(0),
          snapshot_term_(0) {
    }

    std::optional<raft::VoteResponse> on_vote_request(const raft::VoteRequest& request) override {
        std::scoped_lock lock(mu_);

        if (request.term() > current_term_) {
            current_term_ = request.term();
            voted_for_.clear();
        }

        bool granted = false;
        if (request.term() == current_term_ && candidate_log_is_up_to_date(request)) {
            if (voted_for_.empty() || voted_for_ == request.candidate_id()) {
                voted_for_ = request.candidate_id();
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

    std::optional<raft::AppendEntriesResponse> on_append_entries_request(const raft::AppendEntriesRequest& request) override {
        std::scoped_lock lock(mu_);

        if (request.term() > current_term_) {
            current_term_ = request.term();
            voted_for_.clear();
        }

        bool success = false;
        std::int64_t match_index = 0;
        if (request.term() == current_term_ && prev_log_matches(request.prev_log_index(), request.prev_log_term())) {
            success = true;
            if (request.entries_size() > 0) {
                last_log_index_ = request.prev_log_index() + request.entries_size();
                last_log_term_ = request.entries(request.entries_size() - 1).term();
                match_index = last_log_index_;
            } else {
                match_index = request.prev_log_index();
            }
        }

        raft::AppendEntriesResponse response;
        response.set_term(current_term_);
        response.set_peer_id(peer_id_);
        response.set_success(success);
        response.set_match_index(match_index);
        return response;
    }

    std::optional<raft::InstallSnapshotResponse> on_install_snapshot_request(const raft::InstallSnapshotRequest& request) override {
        std::scoped_lock lock(mu_);

        if (request.term() > current_term_) {
            current_term_ = request.term();
            voted_for_.clear();
        }

        bool success = false;
        if (request.term() == current_term_) {
            success = true;
            if (request.done()) {
                snapshot_index_ = request.last_included_index();
                snapshot_term_ = request.last_included_term();
                if (last_log_index_ < snapshot_index_) {
                    last_log_index_ = snapshot_index_;
                    last_log_term_ = snapshot_term_;
                }
            }
        }

        raft::InstallSnapshotResponse response;
        response.set_term(current_term_);
        response.set_peer_id(peer_id_);
        response.set_success(success);
        response.set_last_included_index(request.last_included_index());
        return response;
    }

private:
    bool candidate_log_is_up_to_date(const raft::VoteRequest& request) const {
        if (request.last_log_term() != last_log_term_) {
            return request.last_log_term() > last_log_term_;
        }
        return request.last_log_index() >= last_log_index_;
    }

    bool prev_log_matches(std::int64_t prev_log_index, std::int64_t prev_log_term) const {
        if (prev_log_index == 0 && prev_log_term == 0) {
            return true;
        }
        if (prev_log_index == last_log_index_ && prev_log_term == last_log_term_) {
            return true;
        }
        return prev_log_index == snapshot_index_ && prev_log_term == snapshot_term_;
    }

    std::mutex mu_;
    std::string peer_id_;
    std::int64_t current_term_;
    std::string voted_for_;
    std::int64_t last_log_index_;
    std::int64_t last_log_term_;
    std::int64_t snapshot_index_;
    std::int64_t snapshot_term_;
};

} // namespace raftcpp
