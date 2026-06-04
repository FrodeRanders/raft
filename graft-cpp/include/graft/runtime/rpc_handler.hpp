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

#include <deque>
#include <filesystem>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "raft.pb.h"
#include "graft/core/raft_node.hpp"
#include "graft/storage/persistent_state_store.hpp"

namespace graft {
    class RpcHandler {
    public:
        virtual ~RpcHandler() = default;

        virtual std::optional<raft::TelemetryResponse> on_telemetry_request(const raft::TelemetryRequest &request) = 0;

        virtual std::optional<raft::ClusterSummaryResponse> on_cluster_summary_request(
            const raft::ClusterSummaryRequest &request) = 0;

        virtual std::optional<raft::ClientCommandResponse> on_client_command_request(
            const raft::ClientCommandRequest &request) = 0;

        virtual std::optional<raft::ClientQueryResponse> on_client_query_request(
            const raft::ClientQueryRequest &request) = 0;

        virtual std::optional<raft::JoinClusterResponse> on_join_cluster_request(
            const raft::JoinClusterRequest &request) = 0;

        virtual std::optional<raft::JoinClusterStatusResponse> on_join_cluster_status_request(
            const raft::JoinClusterStatusRequest &request) = 0;

        virtual std::optional<raft::ReconfigureClusterResponse> on_reconfigure_cluster_request(
            const raft::ReconfigureClusterRequest &request) = 0;

        virtual std::optional<raft::ReconfigurationStatusResponse> on_reconfiguration_status_request(
            const raft::ReconfigurationStatusRequest &request) = 0;

        virtual std::optional<raft::VoteResponse> on_vote_request(const raft::VoteRequest &request) = 0;

        virtual std::optional<raft::AppendEntriesResponse> on_append_entries_request(
            const raft::AppendEntriesRequest &request) = 0;

        virtual std::optional<raft::InstallSnapshotResponse> on_install_snapshot_request(
            const raft::InstallSnapshotRequest &request) = 0;
    };

    class StubRpcHandler final : public RpcHandler {
    public:
        StubRpcHandler(std::string peer_id, std::int64_t current_term);

        std::optional<raft::TelemetryResponse> on_telemetry_request(const raft::TelemetryRequest &request) override;

        std::optional<raft::ClusterSummaryResponse> on_cluster_summary_request(
            const raft::ClusterSummaryRequest &request) override;

        std::optional<raft::ClientCommandResponse>
        on_client_command_request(const raft::ClientCommandRequest &request) override;

        std::optional<raft::ClientQueryResponse>
        on_client_query_request(const raft::ClientQueryRequest &request) override;

        std::optional<raft::JoinClusterResponse>
        on_join_cluster_request(const raft::JoinClusterRequest &request) override;

        std::optional<raft::JoinClusterStatusResponse> on_join_cluster_status_request(
            const raft::JoinClusterStatusRequest &request) override;

        std::optional<raft::ReconfigureClusterResponse> on_reconfigure_cluster_request(
            const raft::ReconfigureClusterRequest &request) override;

        std::optional<raft::ReconfigurationStatusResponse> on_reconfiguration_status_request(
            const raft::ReconfigurationStatusRequest &request) override;

        std::optional<raft::VoteResponse> on_vote_request(const raft::VoteRequest &request) override;

        std::optional<raft::AppendEntriesResponse>
        on_append_entries_request(const raft::AppendEntriesRequest &request) override;

        std::optional<raft::InstallSnapshotResponse> on_install_snapshot_request(
            const raft::InstallSnapshotRequest &request) override;

    private:
        std::string peer_id_;
        std::int64_t current_term_;
    };

    using RpcHandlerPtr = std::shared_ptr<RpcHandler>;

    class InMemoryRpcHandler final : public RpcHandler {
    public:
        using CommandReplicator = std::function<std::optional<std::string>(const std::string &)>;
        using InternalCommandReplicator = std::function<bool(const std::string &)>;
        using ReadBarrier = std::function<bool()>;

        struct AuthenticationFailure {
            std::string status;
            std::string message;
        };

        using Authenticator = std::function<std::optional<AuthenticationFailure>(const std::string &,
                                                                                 const std::string &)>;
        using CommandAuthorizer = std::function<std::optional<AuthenticationFailure>(const std::string &,
                                                                                    const std::string &)>;

        struct Endpoint {
            std::string host;
            std::int32_t port;
        };

        using JoinTracker = std::function<void(const std::string &, const Endpoint &)>;
        using MembershipUpdater = std::function<void(const std::vector<std::string> &, const std::vector<Endpoint> &)>;
        using JoinForwarder = std::function<bool(const Endpoint &, const raft::JoinClusterRequest &)>;
        using ReconfigureForwarder = std::function<bool(const Endpoint &, const raft::ReconfigureClusterRequest &)>;

        struct ClusterHealthSummary {
            std::string health;
            std::string reason;
            bool quorum_available{false};
            bool current_quorum_available{false};
            bool next_quorum_available{false};
            std::int32_t voting_members{0};
            std::int32_t healthy_voting_members{0};
            std::int32_t reachable_voting_members{0};
        };

        InMemoryRpcHandler(std::string peer_id, std::int64_t current_term, std::int64_t last_log_index,
                           std::int64_t last_log_term);

        explicit InMemoryRpcHandler(std::shared_ptr<RaftNode> node);

        void set_command_replicator(CommandReplicator replicator);

        void set_internal_command_replicator(InternalCommandReplicator replicator);

        void set_read_barrier(ReadBarrier read_barrier);

        void set_authenticator(Authenticator authenticator);

        void set_command_authorizer(CommandAuthorizer authorizer);

        void set_telemetry_rate_limit_per_minute(std::int32_t limit);

        void set_join_forwarder(JoinForwarder forwarder);

        void set_reconfigure_forwarder(ReconfigureForwarder forwarder);

        void set_join_tracker(JoinTracker tracker);

        void set_membership_updater(MembershipUpdater updater);

        void set_local_endpoint(std::string host, std::int32_t port);

        void set_known_peer_endpoints(const std::vector<Endpoint> &endpoints_by_position,
                                      const std::vector<std::string> &peer_ids);

        std::optional<raft::TelemetryResponse> on_telemetry_request(const raft::TelemetryRequest &request) override;

        std::optional<raft::ClusterSummaryResponse> on_cluster_summary_request(
            const raft::ClusterSummaryRequest &request) override;

        std::optional<raft::VoteResponse> on_vote_request(const raft::VoteRequest &request) override;

        std::optional<raft::ClientCommandResponse>
        on_client_command_request(const raft::ClientCommandRequest &request) override;

        std::optional<raft::ClientQueryResponse>
        on_client_query_request(const raft::ClientQueryRequest &request) override;

        std::optional<raft::JoinClusterResponse>
        on_join_cluster_request(const raft::JoinClusterRequest &request) override;

        std::optional<raft::JoinClusterStatusResponse> on_join_cluster_status_request(
            const raft::JoinClusterStatusRequest &request) override;

        std::optional<raft::ReconfigureClusterResponse> on_reconfigure_cluster_request(
            const raft::ReconfigureClusterRequest &request) override;

        std::optional<raft::ReconfigurationStatusResponse> on_reconfiguration_status_request(
            const raft::ReconfigurationStatusRequest &request) override;

        std::optional<raft::AppendEntriesResponse>
        on_append_entries_request(const raft::AppendEntriesRequest &request) override;

        std::optional<raft::InstallSnapshotResponse> on_install_snapshot_request(
            const raft::InstallSnapshotRequest &request) override;

        RaftNode &node();

        const RaftNode &node() const;

        std::shared_ptr<RaftNode> node_ptr();

    private:
        static std::string normalize_peer_role(std::string role);

        static std::string role_to_string(RaftNode::Role role);

        std::vector<std::string> recovered_peer_ids() const;

        void add_peer_specs(raft::TelemetryResponse &response) const;

        void add_replication_status(raft::TelemetryResponse &response) const;

        bool is_voting_member(const std::string &peer_id) const;

        ClusterHealthSummary summarize_cluster_health() const;

        void populate_member(raft::ClusterMemberSummary &member, const std::string &peer_id, bool local,
                             const RaftNode::PeerProgress &progress) const;

        void add_telemetry_cluster_members(raft::TelemetryResponse &response) const;

        void add_summary_members(raft::ClusterSummaryResponse &response) const;

        void populate_leader_endpoint(raft::ClientCommandResponse &response) const;

        void populate_leader_endpoint(raft::ClientQueryResponse &response) const;

        std::optional<AuthenticationFailure> authenticate(const std::string &scheme, const std::string &token) const;

        std::optional<AuthenticationFailure> authorize_command(const std::string &requester_id,
                                                               const std::string &command) const;

        bool allow_operational_request(const std::string &requester_id);

        void populate_redirect_leader(raft::ClusterSummaryResponse &response) const;

        void populate_redirect_leader(raft::ReconfigurationStatusResponse &response) const;

        std::optional<Endpoint> current_leader_endpoint() const;

        std::shared_ptr<RaftNode> node_;
        CommandReplicator command_replicator_;
        InternalCommandReplicator internal_command_replicator_;
        ReadBarrier read_barrier_;
        Authenticator authenticator_;
        CommandAuthorizer command_authorizer_;
        std::int32_t telemetry_rate_limit_per_minute_{30};
        std::unordered_map<std::string, std::deque<std::int64_t>> operational_request_history_;
        JoinForwarder join_forwarder_;
        ReconfigureForwarder reconfigure_forwarder_;
        JoinTracker join_tracker_;
        MembershipUpdater membership_updater_;
        std::optional<Endpoint> local_endpoint_;
        std::unordered_map<std::string, Endpoint> known_peer_endpoints_;
    };

    class PersistentRpcHandler final : public RpcHandler {
    public:
        PersistentRpcHandler(std::filesystem::path state_path, RaftNode::Config initial_config);

        std::optional<raft::TelemetryResponse> on_telemetry_request(const raft::TelemetryRequest &request) override;

        std::optional<raft::ClusterSummaryResponse> on_cluster_summary_request(
            const raft::ClusterSummaryRequest &request) override;

        std::optional<raft::ClientCommandResponse>
        on_client_command_request(const raft::ClientCommandRequest &request) override;

        std::optional<raft::ClientQueryResponse>
        on_client_query_request(const raft::ClientQueryRequest &request) override;

        std::optional<raft::JoinClusterResponse>
        on_join_cluster_request(const raft::JoinClusterRequest &request) override;

        std::optional<raft::JoinClusterStatusResponse> on_join_cluster_status_request(
            const raft::JoinClusterStatusRequest &request) override;

        std::optional<raft::ReconfigureClusterResponse> on_reconfigure_cluster_request(
            const raft::ReconfigureClusterRequest &request) override;

        std::optional<raft::ReconfigurationStatusResponse> on_reconfiguration_status_request(
            const raft::ReconfigurationStatusRequest &request) override;

        std::optional<raft::VoteResponse> on_vote_request(const raft::VoteRequest &request) override;

        std::optional<raft::AppendEntriesResponse>
        on_append_entries_request(const raft::AppendEntriesRequest &request) override;

        std::optional<raft::InstallSnapshotResponse> on_install_snapshot_request(
            const raft::InstallSnapshotRequest &request) override;

        std::shared_ptr<RaftNode> node_ptr();

        const PersistentStateStore &store() const;

        InMemoryRpcHandler &delegate();

    private:
        void persist();

        PersistentStateStore store_;
        std::shared_ptr<RaftNode> node_;
        InMemoryRpcHandler delegate_{node_};
    };
} // namespace graft
