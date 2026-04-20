#pragma once

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_set>

#include "raft.pb.h"
#include "raftcpp/persistent_state_store.hpp"
#include "raftcpp/raft_node.hpp"

namespace raftcpp {

class RpcHandler {
public:
    virtual ~RpcHandler() = default;

    virtual std::optional<raft::TelemetryResponse> on_telemetry_request(const raft::TelemetryRequest& request) = 0;
    virtual std::optional<raft::ClusterSummaryResponse> on_cluster_summary_request(const raft::ClusterSummaryRequest& request) = 0;
    virtual std::optional<raft::ClientCommandResponse> on_client_command_request(const raft::ClientCommandRequest& request) = 0;
    virtual std::optional<raft::ClientQueryResponse> on_client_query_request(const raft::ClientQueryRequest& request) = 0;
    virtual std::optional<raft::VoteResponse> on_vote_request(const raft::VoteRequest& request) = 0;
    virtual std::optional<raft::AppendEntriesResponse> on_append_entries_request(const raft::AppendEntriesRequest& request) = 0;
    virtual std::optional<raft::InstallSnapshotResponse> on_install_snapshot_request(const raft::InstallSnapshotRequest& request) = 0;
};

class StubRpcHandler final : public RpcHandler {
public:
    StubRpcHandler(std::string peer_id, std::int64_t current_term)
        : peer_id_(std::move(peer_id)), current_term_(current_term) {
    }

    std::optional<raft::TelemetryResponse> on_telemetry_request(const raft::TelemetryRequest&) override {
        raft::TelemetryResponse response;
        response.set_peer_id(peer_id_);
        response.set_term(current_term_);
        response.set_success(false);
        response.set_status("UNSUPPORTED");
        response.set_state("FOLLOWER");
        response.set_cluster_health("UNKNOWN");
        response.set_cluster_status_reason("stub");
        return response;
    }

    std::optional<raft::ClusterSummaryResponse> on_cluster_summary_request(const raft::ClusterSummaryRequest&) override {
        raft::ClusterSummaryResponse response;
        response.set_peer_id(peer_id_);
        response.set_term(current_term_);
        response.set_success(false);
        response.set_status("UNSUPPORTED");
        response.set_state("FOLLOWER");
        response.set_cluster_health("UNKNOWN");
        response.set_cluster_status_reason("stub");
        return response;
    }

    std::optional<raft::ClientCommandResponse> on_client_command_request(const raft::ClientCommandRequest& request) override {
        raft::ClientCommandResponse response;
        response.set_term(request.term());
        response.set_peer_id(peer_id_);
        response.set_success(false);
        response.set_status("UNSUPPORTED");
        response.set_message("stub");
        return response;
    }

    std::optional<raft::ClientQueryResponse> on_client_query_request(const raft::ClientQueryRequest& request) override {
        raft::ClientQueryResponse response;
        response.set_term(request.term());
        response.set_peer_id(peer_id_);
        response.set_success(false);
        response.set_status("UNSUPPORTED");
        response.set_message("stub");
        return response;
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

    std::optional<raft::TelemetryResponse> on_telemetry_request(const raft::TelemetryRequest& request) override {
        const auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                             std::chrono::system_clock::now().time_since_epoch())
                             .count();
        raft::TelemetryResponse response;
        response.set_observed_at_millis(now);
        response.set_term(node_->current_term());
        response.set_peer_id(node_->peer_id());
        response.set_success(true);
        response.set_status("OK");
        response.set_state(role_to_string(node_->role()));
        response.set_leader_id(node_->leader_id().value_or(""));
        response.set_voted_for(node_->voted_for().value_or(""));
        response.set_joining(false);
        response.set_decommissioned(false);
        response.set_commit_index(node_->commit_index());
        response.set_last_applied(node_->last_applied());
        response.set_last_log_index(node_->last_log_index());
        response.set_last_log_term(node_->last_log_term());
        response.set_snapshot_index(node_->snapshot_index());
        response.set_snapshot_term(node_->snapshot_term());
        response.set_last_heartbeat_millis(0);
        response.set_next_election_deadline_millis(0);
        response.set_joint_consensus(false);
        response.set_cluster_health("HEALTHY");
        response.set_quorum_available(true);
        response.set_current_quorum_available(true);
        response.set_next_quorum_available(true);
        response.set_voting_members(static_cast<std::int32_t>(node_->voting_peers().size() + 1));
        response.set_healthy_voting_members(response.voting_members());
        response.set_reachable_voting_members(response.voting_members());
        response.set_cluster_status_reason("ok");
        add_peer_specs(response);
        if (request.include_peer_stats()) {
            add_replication_status(response);
        }
        add_telemetry_cluster_members(response);
        return response;
    }

    std::optional<raft::ClusterSummaryResponse> on_cluster_summary_request(const raft::ClusterSummaryRequest&) override {
        const auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                             std::chrono::system_clock::now().time_since_epoch())
                             .count();
        raft::ClusterSummaryResponse response;
        response.set_observed_at_millis(now);
        response.set_term(node_->current_term());
        response.set_peer_id(node_->peer_id());
        response.set_success(true);
        response.set_status("OK");
        response.set_state(role_to_string(node_->role()));
        response.set_leader_id(node_->leader_id().value_or(""));
        response.set_joint_consensus(false);
        response.set_cluster_health("HEALTHY");
        response.set_cluster_status_reason("ok");
        response.set_quorum_available(true);
        response.set_current_quorum_available(true);
        response.set_next_quorum_available(true);
        response.set_voting_members(static_cast<std::int32_t>(node_->voting_peers().size() + 1));
        response.set_healthy_voting_members(response.voting_members());
        response.set_reachable_voting_members(response.voting_members());
        add_summary_members(response);
        return response;
    }

    std::optional<raft::VoteResponse> on_vote_request(const raft::VoteRequest& request) override {
        return node_->handle_vote_request(request);
    }

    std::optional<raft::ClientCommandResponse> on_client_command_request(const raft::ClientCommandRequest& request) override {
        raft::ClientCommandResponse response;
        response.set_term(node_->current_term());
        response.set_peer_id(node_->peer_id());
        response.set_leader_id(node_->leader_id().value_or(""));

        raft::StateMachineCommand command;
        if (!command.ParseFromString(request.command())) {
            response.set_success(false);
            response.set_status("BAD_REQUEST");
            response.set_message("failed to parse StateMachineCommand");
            return response;
        }

        if (node_->role() != RaftNode::Role::leader && node_->quorum_size() == 1) {
            node_->become_leader();
            response.set_term(node_->current_term());
            response.set_leader_id(node_->leader_id().value_or(""));
        }

        if (node_->role() != RaftNode::Role::leader) {
            response.set_success(false);
            response.set_status("NOT_LEADER");
            response.set_message("command must target leader");
            return response;
        }

        if (!node_->append_and_commit_local_command(request.command())) {
            response.set_success(false);
            response.set_status("UNSUPPORTED");
            response.set_message("distributed client command replication is not wired in this prototype");
            return response;
        }

        response.set_success(true);
        response.set_status("OK");
        response.set_message("command committed and applied");
        return response;
    }

    std::optional<raft::ClientQueryResponse> on_client_query_request(const raft::ClientQueryRequest& request) override {
        raft::ClientQueryResponse response;
        response.set_term(node_->current_term());
        response.set_peer_id(node_->peer_id());
        response.set_success(false);

        raft::StateMachineQuery query;
        if (!query.ParseFromString(request.query())) {
            response.set_status("BAD_REQUEST");
            response.set_message("failed to parse StateMachineQuery");
            return response;
        }

        if (query.query_case() != raft::StateMachineQuery::kGet) {
            response.set_status("UNSUPPORTED");
            response.set_message("only GET queries are supported");
            return response;
        }

        raft::StateMachineQueryResult result;
        auto* get = result.mutable_get();
        get->set_key(query.get().key());
        const auto applied = node_->applied_kv();
        const auto found = applied.find(query.get().key());
        if (found != applied.end()) {
            get->set_found(true);
            get->set_value(found->second);
        } else {
            get->set_found(false);
        }

        response.set_success(true);
        response.set_status("OK");
        response.set_message("query completed");
        if (!result.SerializeToString(response.mutable_result())) {
            throw std::runtime_error("failed to serialize StateMachineQueryResult");
        }
        return response;
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
    std::vector<std::string> recovered_peer_ids() const {
        std::vector<std::string> peer_ids = node_->voting_peers();
        std::unordered_set<std::string> seen(peer_ids.begin(), peer_ids.end());
        for (const auto& [peer_id, _] : node_->peer_progress()) {
            if (peer_id != node_->peer_id() && seen.insert(peer_id).second) {
                peer_ids.push_back(peer_id);
            }
        }
        return peer_ids;
    }

    static std::string role_to_string(RaftNode::Role role) {
        switch (role) {
        case RaftNode::Role::follower:
            return "FOLLOWER";
        case RaftNode::Role::candidate:
            return "CANDIDATE";
        case RaftNode::Role::leader:
            return "LEADER";
        }
        return "UNKNOWN";
    }

    void add_peer_specs(raft::TelemetryResponse& response) const {
        auto* local = response.add_current_members();
        local->set_id(node_->peer_id());
        local->set_role(node_->role() == RaftNode::Role::leader ? "voter" : "voter");
        auto* local_known = response.add_known_peers();
        *local_known = *local;
        for (const auto& peer_id : recovered_peer_ids()) {
            auto* peer = response.add_current_members();
            peer->set_id(peer_id);
            peer->set_role("voter");
            auto* known = response.add_known_peers();
            *known = *peer;
        }
    }

    void add_replication_status(raft::TelemetryResponse& response) const {
        for (const auto& [peer_id, progress] : node_->peer_progress()) {
            auto* repl = response.add_replication();
            repl->set_peer_id(peer_id);
            repl->set_next_index(progress.next_index);
            repl->set_match_index(progress.match_index);
            repl->set_reachable(true);
            repl->set_last_successful_contact_millis(0);
            repl->set_consecutive_failures(0);
            repl->set_last_failed_contact_millis(0);
        }
    }

    void populate_member(raft::ClusterMemberSummary& member, const std::string& peer_id, bool local, std::int64_t next_index, std::int64_t match_index) const {
        member.set_peer_id(peer_id);
        member.set_local(local);
        member.set_current_member(true);
        member.set_next_member(true);
        member.set_voting(true);
        member.set_role("voter");
        member.set_current_role("voter");
        member.set_next_role("voter");
        member.set_reachable(true);
        member.set_health("healthy");
        member.set_freshness(local ? "current" : "unknown");
        member.set_next_index(next_index);
        member.set_match_index(match_index);
        member.set_lag(std::max<std::int64_t>(0, node_->last_log_index() - match_index));
    }

    void add_telemetry_cluster_members(raft::TelemetryResponse& response) const {
        const auto local_lag = node_->last_log_index() - node_->commit_index();
        auto* local = response.add_cluster_members();
        populate_member(*local, node_->peer_id(), true, node_->last_log_index() + 1, node_->last_log_index());
        local->set_lag(local_lag);

        const auto progress_map = node_->peer_progress();
        for (const auto& peer_id : recovered_peer_ids()) {
            const auto found = progress_map.find(peer_id);
            const auto next_index = found != progress_map.end() ? found->second.next_index : 0;
            const auto match_index = found != progress_map.end() ? found->second.match_index : 0;
            auto* peer = response.add_cluster_members();
            populate_member(*peer, peer_id, false, next_index, match_index);
        }
    }

    void add_summary_members(raft::ClusterSummaryResponse& response) const {
        auto* local = response.add_members();
        populate_member(*local, node_->peer_id(), true, node_->last_log_index() + 1, node_->last_log_index());

        const auto progress_map = node_->peer_progress();
        for (const auto& peer_id : recovered_peer_ids()) {
            const auto found = progress_map.find(peer_id);
            const auto next_index = found != progress_map.end() ? found->second.next_index : 0;
            const auto match_index = found != progress_map.end() ? found->second.match_index : 0;
            auto* peer = response.add_members();
            populate_member(*peer, peer_id, false, next_index, match_index);
        }
    }

    std::shared_ptr<RaftNode> node_;
};

class PersistentRpcHandler final : public RpcHandler {
public:
    PersistentRpcHandler(std::filesystem::path state_path, RaftNode::Config initial_config)
        : store_(std::move(state_path)),
          node_(std::make_shared<RaftNode>(std::move(initial_config))) {
        if (const auto persisted = store_.load(); persisted.has_value()) {
            node_->apply_persistent_state(*persisted);
        } else {
            store_.save(node_->persistent_state());
        }
    }

    std::optional<raft::TelemetryResponse> on_telemetry_request(const raft::TelemetryRequest& request) override {
        return delegate_.on_telemetry_request(request);
    }

    std::optional<raft::ClusterSummaryResponse> on_cluster_summary_request(const raft::ClusterSummaryRequest& request) override {
        return delegate_.on_cluster_summary_request(request);
    }

    std::optional<raft::ClientCommandResponse> on_client_command_request(const raft::ClientCommandRequest& request) override {
        auto response = delegate_.on_client_command_request(request);
        persist();
        return response;
    }

    std::optional<raft::ClientQueryResponse> on_client_query_request(const raft::ClientQueryRequest& request) override {
        return delegate_.on_client_query_request(request);
    }

    std::optional<raft::VoteResponse> on_vote_request(const raft::VoteRequest& request) override {
        auto response = delegate_.on_vote_request(request);
        persist();
        return response;
    }

    std::optional<raft::AppendEntriesResponse> on_append_entries_request(const raft::AppendEntriesRequest& request) override {
        auto response = delegate_.on_append_entries_request(request);
        persist();
        return response;
    }

    std::optional<raft::InstallSnapshotResponse> on_install_snapshot_request(const raft::InstallSnapshotRequest& request) override {
        auto response = delegate_.on_install_snapshot_request(request);
        persist();
        return response;
    }

    std::shared_ptr<RaftNode> node_ptr() { return node_; }
    const PersistentStateStore& store() const { return store_; }

private:
    void persist() {
        store_.save(node_->persistent_state());
    }

    PersistentStateStore store_;
    std::shared_ptr<RaftNode> node_;
    InMemoryRpcHandler delegate_{node_};
};

} // namespace raftcpp
