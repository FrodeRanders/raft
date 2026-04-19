#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>

#include "raft.pb.h"
#include "raftcpp/raft_node.hpp"

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
        : node_(std::make_shared<RaftNode>(RaftNode::Config{
              .peer_id = std::move(peer_id),
              .current_term = current_term,
              .last_log_index = last_log_index,
              .last_log_term = last_log_term,
              .commit_index = 0,
              .snapshot_index = 0,
              .snapshot_term = 0,
          })) {
    }

    explicit InMemoryRpcHandler(std::shared_ptr<RaftNode> node)
        : node_(std::move(node)) {
        if (!node_) {
            throw std::runtime_error("in-memory rpc handler requires a node");
        }
    }

    std::optional<raft::VoteResponse> on_vote_request(const raft::VoteRequest& request) override {
        return node_->handle_vote_request(request);
    }

    std::optional<raft::AppendEntriesResponse> on_append_entries_request(const raft::AppendEntriesRequest& request) override {
        return node_->handle_append_entries(request);
    }

    std::optional<raft::InstallSnapshotResponse> on_install_snapshot_request(const raft::InstallSnapshotRequest& request) override {
        return node_->handle_install_snapshot(request);
    }

    RaftNode& node() { return *node_; }
    const RaftNode& node() const { return *node_; }
    std::shared_ptr<RaftNode> node_ptr() { return node_; }

private:
    std::shared_ptr<RaftNode> node_;
};

} // namespace raftcpp
