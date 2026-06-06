#include "graft/runtime/rpc_handler.hpp"

#include <algorithm>
#include <chrono>
#include <cctype>
#include <stdexcept>
#include <unordered_set>
#include <utility>

namespace graft {
    namespace {
        std::optional<raft::PeerSpec> find_member_spec(const std::vector<raft::PeerSpec> &members,
                                                       const std::string &peer_id) {
            const auto found = std::find_if(members.begin(), members.end(), [&](const raft::PeerSpec &member) {
                return member.id() == peer_id;
            });
            if (found == members.end()) {
                return std::nullopt;
            }
            return *found;
        }

        bool member_is_voter(const std::optional<raft::PeerSpec> &member) {
            return member.has_value() && member->role() == "VOTER";
        }

        std::string member_role(const std::optional<raft::PeerSpec> &member) {
            return member.has_value() ? member->role() : "";
        }

        std::string describe_role_transition(const std::optional<raft::PeerSpec> &current,
                                             const std::optional<raft::PeerSpec> &next) {
            if (!current.has_value() && !next.has_value()) {
                return "known";
            }
            if (!current.has_value()) {
                return "joining";
            }
            if (!next.has_value()) {
                return "removing";
            }
            if (current->role() == next->role()) {
                return "steady";
            }
            return next->role() == "VOTER" ? "promoting" : "demoting";
        }
    }

    StubRpcHandler::StubRpcHandler(std::string peer_id, std::int64_t current_term)
        : peer_id_(std::move(peer_id)), current_term_(current_term) {
    }

    std::optional<raft::TelemetryResponse> StubRpcHandler::on_telemetry_request(const raft::TelemetryRequest &) {
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

    std::optional<raft::ClusterSummaryResponse> StubRpcHandler::on_cluster_summary_request(
        const raft::ClusterSummaryRequest &) {
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

    std::optional<raft::ClientCommandResponse> StubRpcHandler::on_client_command_request(
        const raft::ClientCommandRequest &request) {
        raft::ClientCommandResponse response;
        response.set_term(request.term());
        response.set_peer_id(peer_id_);
        response.set_success(false);
        response.set_status("UNSUPPORTED");
        response.set_message("stub");
        return response;
    }

    std::optional<raft::ClientQueryResponse> StubRpcHandler::on_client_query_request(
        const raft::ClientQueryRequest &request) {
        raft::ClientQueryResponse response;
        response.set_term(request.term());
        response.set_peer_id(peer_id_);
        response.set_success(false);
        response.set_status("UNSUPPORTED");
        response.set_message("stub");
        return response;
    }

    std::optional<raft::JoinClusterResponse> StubRpcHandler::on_join_cluster_request(
        const raft::JoinClusterRequest &request) {
        raft::JoinClusterResponse response;
        response.set_term(request.term());
        response.set_peer_id(peer_id_);
        response.set_success(false);
        response.set_status("UNSUPPORTED");
        response.set_message("stub");
        return response;
    }

    std::optional<raft::JoinClusterStatusResponse> StubRpcHandler::on_join_cluster_status_request(
        const raft::JoinClusterStatusRequest &request) {
        raft::JoinClusterStatusResponse response;
        response.set_term(request.term());
        response.set_peer_id(peer_id_);
        response.set_success(false);
        response.set_status("UNSUPPORTED");
        response.set_message("stub");
        return response;
    }

    std::optional<raft::ReconfigureClusterResponse> StubRpcHandler::on_reconfigure_cluster_request(
        const raft::ReconfigureClusterRequest &request) {
        raft::ReconfigureClusterResponse response;
        response.set_term(request.term());
        response.set_peer_id(peer_id_);
        response.set_success(false);
        response.set_status("UNSUPPORTED");
        response.set_message("stub");
        return response;
    }

    std::optional<raft::ReconfigurationStatusResponse> StubRpcHandler::on_reconfiguration_status_request(
        const raft::ReconfigurationStatusRequest &request) {
        raft::ReconfigurationStatusResponse response;
        response.set_term(request.term());
        response.set_peer_id(peer_id_);
        response.set_success(false);
        response.set_status("UNSUPPORTED");
        response.set_state("FOLLOWER");
        response.set_cluster_health("UNKNOWN");
        response.set_cluster_status_reason("stub");
        return response;
    }

    std::optional<raft::VoteResponse> StubRpcHandler::on_vote_request(const raft::VoteRequest &request) {
        raft::VoteResponse response;
        response.set_peer_id(peer_id_);
        response.set_term(request.term());
        response.set_vote_granted(false);
        response.set_current_term(current_term_);
        return response;
    }

    std::optional<raft::AppendEntriesResponse> StubRpcHandler::on_append_entries_request(
        const raft::AppendEntriesRequest &) {
        raft::AppendEntriesResponse response;
        response.set_term(current_term_);
        response.set_peer_id(peer_id_);
        response.set_success(false);
        response.set_match_index(0);
        return response;
    }

    std::optional<raft::InstallSnapshotResponse> StubRpcHandler::on_install_snapshot_request(
        const raft::InstallSnapshotRequest &request) {
        raft::InstallSnapshotResponse response;
        response.set_term(current_term_);
        response.set_peer_id(peer_id_);
        response.set_success(false);
        response.set_last_included_index(request.last_included_index());
        return response;
    }

    InMemoryRpcHandler::InMemoryRpcHandler(std::string peer_id, std::int64_t current_term, std::int64_t last_log_index,
                                           std::int64_t last_log_term)
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

    InMemoryRpcHandler::InMemoryRpcHandler(std::shared_ptr<RaftNode> node)
        : node_(std::move(node)) {
        if (!node_) {
            throw std::runtime_error("in-memory rpc handler requires a node");
        }
    }

    void InMemoryRpcHandler::set_command_replicator(CommandReplicator replicator) {
        command_replicator_ = std::move(replicator);
    }

    void InMemoryRpcHandler::set_internal_command_replicator(InternalCommandReplicator replicator) {
        internal_command_replicator_ = std::move(replicator);
    }

    void InMemoryRpcHandler::set_read_barrier(ReadBarrier read_barrier) {
        read_barrier_ = std::move(read_barrier);
    }

    void InMemoryRpcHandler::set_linearizable_read_lease_millis(std::int64_t lease_millis) {
        linearizable_read_lease_millis_ = std::max<std::int64_t>(1, lease_millis);
    }

    void InMemoryRpcHandler::set_authenticator(Authenticator authenticator) {
        authenticator_ = std::move(authenticator);
    }

    void InMemoryRpcHandler::set_command_authorizer(CommandAuthorizer authorizer) {
        command_authorizer_ = std::move(authorizer);
    }

    void InMemoryRpcHandler::set_reference_data_admission(bool enabled) {
        reference_data_admission_ = enabled;
    }

    void InMemoryRpcHandler::set_telemetry_rate_limit_per_minute(std::int32_t limit) {
        telemetry_rate_limit_per_minute_ = limit;
    }

    void InMemoryRpcHandler::set_telemetry_reconfiguration_stuck_millis(std::int64_t threshold_millis) {
        telemetry_reconfiguration_stuck_millis_ = threshold_millis;
    }

    void InMemoryRpcHandler::set_join_forwarder(JoinForwarder forwarder) {
        join_forwarder_ = std::move(forwarder);
    }

    void InMemoryRpcHandler::set_reconfigure_forwarder(ReconfigureForwarder forwarder) {
        reconfigure_forwarder_ = std::move(forwarder);
    }

    void InMemoryRpcHandler::set_join_tracker(JoinTracker tracker) {
        join_tracker_ = std::move(tracker);
    }

    void InMemoryRpcHandler::set_membership_updater(MembershipUpdater updater) {
        membership_updater_ = std::move(updater);
    }

    void InMemoryRpcHandler::set_local_endpoint(std::string host, std::int32_t port) {
        local_endpoint_ = Endpoint{std::move(host), port};
    }

    void InMemoryRpcHandler::set_known_peer_endpoints(const std::vector<Endpoint> &endpoints_by_position,
                                                      const std::vector<std::string> &peer_ids) {
        known_peer_endpoints_.clear();
        for (std::size_t i = 0; i < endpoints_by_position.size() && i < peer_ids.size(); ++i) {
            known_peer_endpoints_[peer_ids[i]] = endpoints_by_position[i];
        }
    }

    std::optional<raft::TelemetryResponse> InMemoryRpcHandler::on_telemetry_request(
        const raft::TelemetryRequest &request) {
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
        response.set_decommissioned(node_->decommissioned());
        response.set_commit_index(node_->commit_index());
        response.set_last_applied(node_->last_applied());
        response.set_last_log_index(node_->last_log_index());
        response.set_last_log_term(node_->last_log_term());
        response.set_snapshot_index(node_->snapshot_index());
        response.set_snapshot_term(node_->snapshot_term());
        response.set_last_heartbeat_millis(0);
        response.set_next_election_deadline_millis(0);
        response.set_joint_consensus(node_->joint_consensus());
        const auto cluster = summarize_cluster_health();
        response.set_cluster_health(cluster.health);
        response.set_quorum_available(cluster.quorum_available);
        response.set_current_quorum_available(cluster.current_quorum_available);
        response.set_next_quorum_available(cluster.next_quorum_available);
        response.set_voting_members(cluster.voting_members);
        response.set_healthy_voting_members(cluster.healthy_voting_members);
        response.set_reachable_voting_members(cluster.reachable_voting_members);
        response.set_cluster_status_reason(cluster.reason);
        response.set_reconfiguration_age_millis(cluster.reconfiguration_age_millis);
        add_peer_specs(response);
        if (const auto auth = authenticate(request.auth_scheme(), request.auth_token()); auth.has_value()) {
            response.set_success(false);
            response.set_status(auth->status);
            return response;
        }
        if (!allow_operational_request(request.peer_id())) {
            response.set_success(false);
            response.set_status("RATE_LIMITED");
            return response;
        }
        if (request.require_leader_summary() && node_->role() != RaftNode::Role::leader) {
            response.set_success(false);
            response.set_status(current_leader_endpoint().has_value() ? "REDIRECT" : "NO_LEADER");
            response.set_redirect_leader_id(node_->leader_id().value_or(""));
            response.clear_cluster_health();
            response.clear_cluster_status_reason();
            response.set_quorum_available(false);
            response.set_current_quorum_available(false);
            response.set_next_quorum_available(false);
            response.set_voting_members(0);
            response.set_healthy_voting_members(0);
            response.set_reachable_voting_members(0);
            return response;
        }
        if (request.include_peer_stats()) {
            add_replication_status(response);
        }
        add_telemetry_cluster_members(response);
        return response;
    }

    std::optional<raft::ClusterSummaryResponse> InMemoryRpcHandler::on_cluster_summary_request(
        const raft::ClusterSummaryRequest &request) {
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
        response.set_joint_consensus(node_->joint_consensus());
        const auto cluster = summarize_cluster_health();
        response.set_cluster_health(cluster.health);
        response.set_cluster_status_reason(cluster.reason);
        response.set_quorum_available(cluster.quorum_available);
        response.set_current_quorum_available(cluster.current_quorum_available);
        response.set_next_quorum_available(cluster.next_quorum_available);
        response.set_voting_members(cluster.voting_members);
        response.set_healthy_voting_members(cluster.healthy_voting_members);
        response.set_reachable_voting_members(cluster.reachable_voting_members);
        response.set_reconfiguration_age_millis(cluster.reconfiguration_age_millis);
        if (const auto auth = authenticate(request.auth_scheme(), request.auth_token()); auth.has_value()) {
            response.set_success(false);
            response.set_status(auth->status);
            return response;
        }
        if (!allow_operational_request(request.peer_id())) {
            response.set_success(false);
            response.set_status("RATE_LIMITED");
            return response;
        }
        if (node_->role() != RaftNode::Role::leader) {
            response.set_success(false);
            response.set_status(current_leader_endpoint().has_value() ? "REDIRECT" : "NO_LEADER");
            response.clear_cluster_health();
            response.clear_cluster_status_reason();
            response.set_quorum_available(false);
            response.set_current_quorum_available(false);
            response.set_next_quorum_available(false);
            response.set_voting_members(0);
            response.set_healthy_voting_members(0);
            response.set_reachable_voting_members(0);
            populate_redirect_leader(response);
            return response;
        }
        add_summary_members(response);
        return response;
    }

    std::optional<raft::VoteResponse> InMemoryRpcHandler::on_vote_request(const raft::VoteRequest &request) {
        return node_->handle_vote_request(request);
    }

    std::optional<raft::ClientCommandResponse> InMemoryRpcHandler::on_client_command_request(
        const raft::ClientCommandRequest &request) {
        raft::ClientCommandResponse response;
        response.set_term(node_->current_term());
        response.set_peer_id(node_->peer_id());
        populate_leader_endpoint(response);

        // Authentication and authorization are application-facing policy checks. They
        // happen before the command is offered to Raft and therefore cannot imply commit.
        if (const auto auth = authenticate(request.auth_scheme(), request.auth_token()); auth.has_value()) {
            response.set_success(false);
            response.set_status(auth->status);
            response.set_message(auth->message);
            return response;
        }

        raft::StateMachineCommand command;
        if (!command.ParseFromString(request.command())) {
            // The C++ demo currently understands the shared key-value command payload.
            // Domain applications should replace this with their own stable encoding.
            response.set_success(false);
            response.set_status("INVALID");
            response.set_message("failed to parse StateMachineCommand");
            return response;
        }

        if (node_->role() != RaftNode::Role::leader &&
            node_->quorum_size() == 1 &&
            !node_->leader_id().has_value()) {
            // A single-node local process can safely self-elect on first client traffic. Multi-node deployments
            // still require an explicit election so a minority partition cannot fabricate a leader.
            node_->become_leader();
            response.set_term(node_->current_term());
            populate_leader_endpoint(response);
        }

        if (node_->role() != RaftNode::Role::leader) {
            response.set_success(false);
            if (reference_data_admission_ && local_member_is_learner()) {
                // Reference-data mode intentionally rejects learner writes rather than
                // redirecting, matching the Java domain policy for that application.
                response.set_status("REJECTED");
                response.set_message("Learner nodes never accept or redirect reference-data writes");
                return response;
            }
            if (current_leader_endpoint().has_value()) {
                response.set_status("REDIRECT");
                response.set_message("Node is not leader; send request to current leader");
            } else {
                response.set_status("REJECTED");
                response.set_message("Node is not leader or command could not be applied");
            }
            return response;
        }

        if (const auto authorization = authorize_command(request.peer_id(), request.command());
            authorization.has_value()) {
            response.set_success(false);
            response.set_status(authorization->status);
            response.set_message(authorization->message);
            return response;
        }

        std::optional<std::string> command_result;
        if (command_replicator_) {
            // Active multi-node modes replicate through RaftRuntime and return only
            // after the entry is committed and applied.
            command_result = command_replicator_(request.command());
        } else {
            // Single-node demo mode can commit locally because it is its own quorum.
            const auto committed = node_->append_and_commit_local_command(request.command());
            if (committed.has_value()) {
                command_result = committed->result;
            }
        }

        if (!command_result.has_value()) {
            response.set_success(false);
            response.set_status("UNSUPPORTED");
            response.set_message("client command replication failed");
            return response;
        }

        response.set_success(true);
        response.set_status("ACCEPTED");
        response.set_message("Command committed and applied");
        populate_leader_endpoint(response);
        if (!command_result->empty()) {
            response.set_result(*command_result);
        }
        return response;
    }

    std::optional<raft::ClientQueryResponse> InMemoryRpcHandler::on_client_query_request(
        const raft::ClientQueryRequest &request) {
        raft::ClientQueryResponse response;
        response.set_term(node_->current_term());
        response.set_peer_id(node_->peer_id());
        populate_leader_endpoint(response);
        response.set_success(false);

        if (const auto auth = authenticate(request.auth_scheme(), request.auth_token()); auth.has_value()) {
            response.set_status(auth->status);
            response.set_message(auth->message);
            return response;
        }

        raft::StateMachineQuery query;
        if (!query.ParseFromString(request.query())) {
            response.set_status("INVALID");
            response.set_message("failed to parse StateMachineQuery");
            return response;
        }

        if (query.query_case() != raft::StateMachineQuery::kGet) {
            response.set_status("UNSUPPORTED");
            response.set_message("only GET queries are supported");
            return response;
        }

        if (node_->role() != RaftNode::Role::leader &&
            node_->quorum_size() == 1 &&
            !node_->leader_id().has_value()) {
            // Keep the developer-only single-node path convenient without weakening normal Raft leader checks.
            node_->become_leader();
            response.set_term(node_->current_term());
            populate_leader_endpoint(response);
        }

        if (node_->role() != RaftNode::Role::leader) {
            if (current_leader_endpoint().has_value()) {
                response.set_status("REDIRECT");
                response.set_message("Node is not leader; send query to current leader");
            } else {
                response.set_status("REJECTED");
                response.set_message("Node is not leader or query could not be applied");
            }
            return response;
        }

        if (node_->quorum_size() > 1 && !node_->can_serve_linearizable_read(linearizable_read_lease_millis_)) {
            // A queryable state machine is not enough for a linearizable read. The leader
            // first needs recent quorum evidence; if the lease is stale, ask the runtime
            // to refresh a heartbeat barrier.
            const bool barrier_completed = read_barrier_ && read_barrier_();
            if (!barrier_completed || !node_->can_serve_linearizable_read(linearizable_read_lease_millis_)) {
                response.set_status("RETRY");
                response.set_message("Leader cannot currently guarantee a linearizable read");
                populate_leader_endpoint(response);
                return response;
            }
        }

        const auto result = node_->query_application(request.query());
        if (result.empty()) {
            // Empty result means unsupported/invalid for this bounded demo state machine.
            // A production domain can choose a richer encoded "not found" result.
            response.set_status("UNSUPPORTED");
            response.set_message("query could not be applied");
            return response;
        }

        response.set_success(true);
        response.set_status("OK");
        response.set_message("Query completed");
        populate_leader_endpoint(response);
        response.set_result(result);
        return response;
    }

    std::optional<raft::JoinClusterResponse> InMemoryRpcHandler::on_join_cluster_request(
        const raft::JoinClusterRequest &request) {
        raft::JoinClusterResponse response;
        response.set_term(node_->current_term());
        response.set_peer_id(node_->peer_id());
        response.set_leader_id(node_->leader_id().value_or(""));

        if (const auto auth = authenticate(request.auth_scheme(), request.auth_token()); auth.has_value()) {
            response.set_success(false);
            response.set_status(auth->status);
            response.set_message(auth->message);
            return response;
        }

        if (node_->role() != RaftNode::Role::leader) {
            if (const auto leader_endpoint = current_leader_endpoint(); leader_endpoint.has_value()) {
                // Join and reconfigure requests are leader-only. Followers try to forward
                // when they have enough endpoint information; otherwise clients retry.
                if (join_forwarder_ && join_forwarder_(*leader_endpoint, request)) {
                    response.set_success(true);
                    response.set_status("FORWARDED");
                    response.set_message("Join request forwarded to leader");
                    response.set_leader_id(node_->leader_id().value_or(""));
                    return response;
                }
                response.set_success(false);
                response.set_status("RETRY");
                response.set_message("Join request could not be forwarded to leader");
                return response;
            }
            response.set_success(false);
            response.set_status("REJECTED");
            response.set_message("Node is not leader or join request could not be applied");
            return response;
        }

        if (request.joining_peer_id().empty() || request.host().empty() || request.port() <= 0) {
            response.set_success(false);
            response.set_status("INVALID");
            response.set_message("joining peer id, host, and port are required");
            return response;
        }

        known_peer_endpoints_[request.joining_peer_id()] = Endpoint{request.host(), request.port()};
        if (node_->has_pending_join(request.joining_peer_id())) {
            if (join_tracker_) {
                join_tracker_(request.joining_peer_id(), Endpoint{request.host(), request.port()});
            }
            response.set_success(true);
            response.set_status("PENDING");
            response.set_message("Join request accepted and awaiting joint configuration commit");
            response.set_leader_id(node_->peer_id());
            return response;
        }
        const auto configured_peers = node_->voting_peers();
        if (std::find(configured_peers.begin(), configured_peers.end(), request.joining_peer_id()) != configured_peers.
            end()) {
            response.set_success(true);
            response.set_status("COMPLETED");
            response.set_message("peer is already a configured member");
            response.set_leader_id(node_->peer_id());
            return response;
        }

        raft::InternalRaftCommand internal;
        auto *member = internal.mutable_join()->mutable_member();
        member->set_id(request.joining_peer_id());
        member->set_host(request.host());
        member->set_port(request.port());
        member->set_role(request.role().empty() ? "VOTER" : normalize_peer_role(request.role()));
        // Membership changes are encoded as internal log entries. The local view changes only after the entry is
        // committed and applied, preserving the same safety boundary as user state-machine commands.
        if (!internal_command_replicator_ ||
            !internal_command_replicator_(RaftNode::encode_internal_command(internal))) {
            response.set_success(false);
            response.set_status("REJECTED");
            response.set_message("Join request could not be applied");
            return response;
        }
        if (join_tracker_) {
            join_tracker_(request.joining_peer_id(), Endpoint{request.host(), request.port()});
        }
        response.set_success(true);
        response.set_status("PENDING");
        response.set_message("Join request accepted and awaiting joint configuration commit");
        response.set_leader_id(node_->peer_id());
        return response;
    }

    std::optional<raft::JoinClusterStatusResponse> InMemoryRpcHandler::on_join_cluster_status_request(
        const raft::JoinClusterStatusRequest &request) {
        raft::JoinClusterStatusResponse response;
        response.set_term(node_->current_term());
        response.set_peer_id(node_->peer_id());
        response.set_leader_id(node_->leader_id().value_or(""));

        if (const auto auth = authenticate(request.auth_scheme(), request.auth_token()); auth.has_value()) {
            response.set_success(false);
            response.set_status(auth->status);
            response.set_message(auth->message);
            return response;
        }

        if (request.target_peer_id().empty()) {
            response.set_success(false);
            response.set_status("INVALID");
            response.set_message("target peer id is required");
            return response;
        }

        if (node_->has_pending_join(request.target_peer_id())) {
            response.set_success(true);
            response.set_status("PENDING");
            response.set_message("Join request accepted and awaiting joint configuration commit");
            return response;
        }

        const auto peers = recovered_peer_ids();
        if (std::find(peers.begin(), peers.end(), request.target_peer_id()) != peers.end()) {
            response.set_success(true);
            response.set_status(node_->joint_consensus() ? "IN_JOINT_CONSENSUS" : "COMPLETED");
            response.set_message(node_->joint_consensus()
                                     ? "Peer is present in the active joint configuration"
                                     : "Peer is part of the finalized cluster configuration");
            return response;
        }

        response.set_success(false);
        response.set_status("UNKNOWN");
        response.set_message("No known join request or committed membership for peer");
        return response;
    }

    std::optional<raft::ReconfigureClusterResponse> InMemoryRpcHandler::on_reconfigure_cluster_request(
        const raft::ReconfigureClusterRequest &request) {
        raft::ReconfigureClusterResponse response;
        response.set_term(node_->current_term());
        response.set_peer_id(node_->peer_id());
        response.set_leader_id(node_->leader_id().value_or(""));

        if (const auto auth = authenticate(request.auth_scheme(), request.auth_token()); auth.has_value()) {
            response.set_success(false);
            response.set_status(auth->status);
            response.set_message(auth->message);
            return response;
        }

        if (node_->role() != RaftNode::Role::leader) {
            if (const auto leader_endpoint = current_leader_endpoint(); leader_endpoint.has_value()) {
                if (reconfigure_forwarder_ && reconfigure_forwarder_(*leader_endpoint, request)) {
                    response.set_success(true);
                    response.set_status("FORWARDED");
                    response.set_message("Reconfiguration request forwarded to leader");
                    response.set_leader_id(node_->leader_id().value_or(""));
                    return response;
                }
                response.set_success(false);
                response.set_status("RETRY");
                response.set_message("Reconfiguration request could not be forwarded to leader");
                return response;
            }
            response.set_success(false);
            response.set_status("REJECTED");
            response.set_message("Node is not leader or reconfiguration could not be applied");
            return response;
        }

        const auto action = normalize_peer_role(request.action());
        if (action != "JOINT" && action != "FINALIZE" && action != "PROMOTE" && action != "DEMOTE") {
            response.set_success(false);
            response.set_status("INVALID");
            response.set_message("supported actions are joint, finalize, promote, and demote");
            return response;
        }

        if (action == "JOINT" || action == "PROMOTE" || action == "DEMOTE") {
            if (request.members().empty()) {
                response.set_success(false);
                response.set_status("INVALID");
                response.set_message("reconfigure action requires members");
                return response;
            }
            if ((action == "PROMOTE" || action == "DEMOTE") && request.members_size() != 1) {
                response.set_success(false);
                response.set_status("INVALID");
                response.set_message("promote and demote require exactly one member");
                return response;
            }

            std::vector<raft::PeerSpec> members;
            if (action == "JOINT") {
                // Explicit joint configuration: the requester supplies the next member
                // set. The committed internal command will make current+next active.
                members.reserve(request.members_size());
                for (const auto &member: request.members()) {
                    members.push_back(member);
                }
            } else {
                // Promote/demote are convenience operations. Build a complete next
                // configuration from recovered membership and override one member role.
                const auto &target = request.members(0);
                if (target.id().empty()) {
                    response.set_success(false);
                    response.set_status("INVALID");
                    response.set_message("target member id is required");
                    return response;
                }
                if (!target.host().empty() && target.port() > 0) {
                    known_peer_endpoints_[target.id()] = Endpoint{target.host(), target.port()};
                }

                auto add_member = [&](const std::string &peer_id, const std::string &role) {
                    raft::PeerSpec spec;
                    spec.set_id(peer_id);
                    if (peer_id == node_->peer_id()) {
                        if (local_endpoint_.has_value()) {
                            spec.set_host(local_endpoint_->host);
                            spec.set_port(local_endpoint_->port);
                        }
                    } else if (const auto found = known_peer_endpoints_.find(peer_id); found != known_peer_endpoints_.end()) {
                        spec.set_host(found->second.host);
                        spec.set_port(found->second.port);
                    }
                    spec.set_role(role);
                    members.push_back(std::move(spec));
                };

                add_member(node_->peer_id(), "VOTER");
                for (const auto &peer_id: recovered_peer_ids()) {
                    add_member(peer_id, peer_id == target.id()
                                            ? (action == "PROMOTE" ? "VOTER" : "LEARNER")
                                            : (is_voting_member(peer_id) ? "VOTER" : "LEARNER"));
                }
                if (std::none_of(members.begin(), members.end(), [&](const raft::PeerSpec &member) {
                    return member.id() == target.id();
                })) {
                    raft::PeerSpec spec = target;
                    spec.set_role(action == "PROMOTE" ? "VOTER" : "LEARNER");
                    members.push_back(std::move(spec));
                }
            }

            std::vector<std::string> peer_ids;
            std::vector<Endpoint> endpoints;
            peer_ids.reserve(members.size());
            endpoints.reserve(members.size());
            for (const auto &member: members) {
                if (member.id().empty() || member.id() == node_->peer_id()) {
                    continue;
                }
                peer_ids.push_back(member.id());
                endpoints.push_back(Endpoint{member.host(), member.port()});
                known_peer_endpoints_[member.id()] = Endpoint{member.host(), member.port()};
            }

            raft::InternalRaftCommand internal;
            bool includes_self = false;
            for (const auto &member: members) {
                if (member.id() == node_->peer_id()) {
                    includes_self = true;
                }
                auto *spec = internal.mutable_joint()->add_members();
                *spec = member;
            }
            if (!includes_self) {
                // Keep the leader in the joint entry unless the caller explicitly
                // removes it through finalize. This avoids accidental self-removal when
                // callers provide only remote members.
                auto *self = internal.mutable_joint()->add_members();
                self->set_id(node_->peer_id());
                if (local_endpoint_.has_value()) {
                    self->set_host(local_endpoint_->host);
                    self->set_port(local_endpoint_->port);
                }
                self->set_role("VOTER");
            }
            if (!internal_command_replicator_ ||
                !internal_command_replicator_(RaftNode::encode_internal_command(internal))) {
                response.set_success(false);
                response.set_status("REJECTED");
                response.set_message("Reconfiguration request could not be applied");
                return response;
            }
            if (membership_updater_) {
                membership_updater_(peer_ids, endpoints);
            }

            if (action == "PROMOTE" || action == "DEMOTE") {
                // The bounded C++ runtime auto-finalizes role-only transitions so the
                // CLI/API can expose simple promote/demote commands.
                raft::InternalRaftCommand finalize;
                finalize.mutable_finalize();
                if (!internal_command_replicator_ ||
                    !internal_command_replicator_(RaftNode::encode_internal_command(finalize))) {
                    response.set_success(false);
                    response.set_status("REJECTED");
                    response.set_message("Reconfiguration request could not be applied");
                    return response;
                }
            }

            response.set_success(true);
            response.set_status("ACCEPTED");
            response.set_message(action == "PROMOTE"
                                     ? "Learner promotion accepted"
                                     : action == "DEMOTE"
                                           ? "Voter demotion accepted"
                                           : "Joint configuration accepted");
            response.set_leader_id(node_->peer_id());
            return response;
        }

        raft::InternalRaftCommand internal;
        internal.mutable_finalize();
        if (!internal_command_replicator_ ||
            !internal_command_replicator_(RaftNode::encode_internal_command(internal))) {
            response.set_success(false);
            response.set_status("REJECTED");
            response.set_message("Reconfiguration request could not be applied");
            return response;
        }
        response.set_success(true);
        response.set_status("ACCEPTED");
        response.set_message("Finalize configuration accepted");
        response.set_leader_id(node_->peer_id());
        return response;
    }

    std::optional<raft::ReconfigurationStatusResponse> InMemoryRpcHandler::on_reconfiguration_status_request(
        const raft::ReconfigurationStatusRequest &request) {
        const auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch())
                .count();
        raft::ReconfigurationStatusResponse response;
        response.set_observed_at_millis(now);
        response.set_term(node_->current_term());
        response.set_peer_id(node_->peer_id());
        response.set_success(true);
        response.set_status("OK");
        response.set_state(role_to_string(node_->role()));
        response.set_leader_id(node_->leader_id().value_or(""));
        response.set_reconfiguration_active(node_->joint_consensus());
        response.set_joint_consensus(node_->joint_consensus());
        const auto cluster = summarize_cluster_health();
        response.set_reconfiguration_age_millis(cluster.reconfiguration_age_millis);
        response.set_cluster_health(cluster.health);
        response.set_cluster_status_reason(cluster.reason);
        response.set_quorum_available(cluster.quorum_available);
        response.set_current_quorum_available(cluster.current_quorum_available);
        response.set_next_quorum_available(cluster.next_quorum_available);

        if (const auto auth = authenticate(request.auth_scheme(), request.auth_token()); auth.has_value()) {
            response.set_success(false);
            response.set_status(auth->status);
            return response;
        }

        if (!allow_operational_request(request.peer_id())) {
            response.set_success(false);
            response.set_status("RATE_LIMITED");
            return response;
        }

        if (node_->role() != RaftNode::Role::leader) {
            response.set_success(false);
            response.set_status(current_leader_endpoint().has_value() ? "REDIRECT" : "NO_LEADER");
            response.clear_cluster_health();
            response.clear_cluster_status_reason();
            response.set_quorum_available(false);
            response.set_current_quorum_available(false);
            response.set_next_quorum_available(false);
            populate_redirect_leader(response);
            return response;
        }

        raft::ClusterSummaryResponse summary;
        add_summary_members(summary);
        for (const auto &member: summary.members()) {
            auto *target = response.add_members();
            *target = member;
        }
        return response;
    }

    std::optional<raft::AppendEntriesResponse> InMemoryRpcHandler::on_append_entries_request(
        const raft::AppendEntriesRequest &request) {
        return node_->handle_append_entries(request);
    }

    std::optional<raft::InstallSnapshotResponse> InMemoryRpcHandler::on_install_snapshot_request(
        const raft::InstallSnapshotRequest &request) {
        return node_->handle_install_snapshot(request);
    }

    RaftNode &InMemoryRpcHandler::node() {
        return *node_;
    }

    const RaftNode &InMemoryRpcHandler::node() const {
        return *node_;
    }

    std::shared_ptr<RaftNode> InMemoryRpcHandler::node_ptr() {
        return node_;
    }

    std::string InMemoryRpcHandler::normalize_peer_role(std::string role) {
        std::transform(role.begin(), role.end(), role.begin(), [](unsigned char ch) {
            return static_cast<char>(std::toupper(ch));
        });
        return role;
    }

    std::vector<std::string> InMemoryRpcHandler::recovered_peer_ids() const {
        std::vector<std::string> peer_ids = node_->voting_peers();
        std::unordered_set<std::string> seen(peer_ids.begin(), peer_ids.end());
        for (const auto &member: node_->current_member_specs()) {
            if (member.id() != node_->peer_id() && seen.insert(member.id()).second) {
                peer_ids.push_back(member.id());
            }
        }
        for (const auto &member: node_->next_member_specs()) {
            if (member.id() != node_->peer_id() && seen.insert(member.id()).second) {
                peer_ids.push_back(member.id());
            }
        }
        for (const auto &[peer_id, _]: node_->peer_progress()) {
            if (peer_id != node_->peer_id() && seen.insert(peer_id).second) {
                peer_ids.push_back(peer_id);
            }
        }
        return peer_ids;
    }

    std::string InMemoryRpcHandler::role_to_string(RaftNode::Role role) {
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

    void InMemoryRpcHandler::add_peer_specs(raft::TelemetryResponse &response) const {
        std::unordered_set<std::string> known;
        // Report current, next and known peers separately so clients can distinguish
        // stable membership from joint-consensus transition state.
        for (const auto &member: node_->current_member_specs()) {
            auto *peer = response.add_current_members();
            *peer = member;
            auto *known_peer = response.add_known_peers();
            *known_peer = *peer;
            known.insert(member.id());
        }
        for (const auto &member: node_->next_member_specs()) {
            auto *peer = response.add_next_members();
            *peer = member;
            if (known.insert(member.id()).second) {
                auto *known_peer = response.add_known_peers();
                *known_peer = member;
            }
        }
        for (const auto &peer_id: recovered_peer_ids()) {
            if (known.insert(peer_id).second) {
                auto *peer = response.add_known_peers();
                peer->set_id(peer_id);
                peer->set_role(is_voting_member(peer_id) ? "VOTER" : "LEARNER");
            }
        }
    }

    void InMemoryRpcHandler::add_replication_status(raft::TelemetryResponse &response) const {
        for (const auto &[peer_id, progress]: node_->peer_progress()) {
            auto *repl = response.add_replication();
            repl->set_peer_id(peer_id);
            repl->set_next_index(progress.next_index);
            repl->set_match_index(progress.match_index);
            repl->set_reachable(peer_id == node_->peer_id() || progress.reachable);
            repl->set_last_successful_contact_millis(progress.last_successful_contact_millis);
            repl->set_consecutive_failures(peer_id == node_->peer_id() ? 0 : progress.consecutive_failures);
            repl->set_last_failed_contact_millis(progress.last_failed_contact_millis);
        }
    }

    bool InMemoryRpcHandler::is_voting_member(const std::string &peer_id) const {
        const auto voting = node_->voting_peers();
        return peer_id == node_->peer_id() ||
               std::find(voting.begin(), voting.end(), peer_id) != voting.end();
    }

    InMemoryRpcHandler::ClusterHealthSummary InMemoryRpcHandler::summarize_cluster_health() const {
        const auto voting_peers = node_->voting_peers();
        const auto progress_map = node_->peer_progress();
        const auto commit_index = node_->commit_index();
        const auto current_voters = node_->current_voting_members();
        const auto next_voters = node_->next_voting_members();

        ClusterHealthSummary summary;
        summary.voting_members = static_cast<std::int32_t>(voting_peers.size() + 1);
        summary.reachable_voting_members = 1;
        summary.healthy_voting_members = 1;
        std::unordered_set<std::string> healthy_voters;
        healthy_voters.insert(node_->peer_id());

        // A voter is considered healthy for quorum summaries only if it is reachable
        // and caught up to the leader's committed index.
        for (const auto &peer_id: voting_peers) {
            const auto found = progress_map.find(peer_id);
            const bool reachable = found != progress_map.end() && found->second.reachable;
            const bool healthy = reachable && found->second.match_index >= commit_index;
            summary.reachable_voting_members += reachable ? 1 : 0;
            summary.healthy_voting_members += healthy ? 1 : 0;
            if (healthy) {
                healthy_voters.insert(peer_id);
            }
        }

        auto has_majority = [&healthy_voters](const std::vector<std::string> &voters) {
            std::size_t present = 0;
            for (const auto &voter: voters) {
                present += healthy_voters.contains(voter) ? 1 : 0;
            }
            return !voters.empty() && present >= ((voters.size() / 2) + 1);
        };
        summary.current_quorum_available = has_majority(current_voters);
        // During joint consensus both sides must be healthy. This mirrors the commit
        // and read-barrier majority rule rather than reporting a union majority.
        summary.next_quorum_available = node_->joint_consensus() ? has_majority(next_voters) : summary.current_quorum_available;
        summary.quorum_available = summary.current_quorum_available && summary.next_quorum_available;
        summary.reconfiguration_age_millis = reconfiguration_age_millis();
        const bool reconfiguration_stuck = telemetry_reconfiguration_stuck_millis_ > 0 &&
                                           summary.reconfiguration_age_millis >=
                                           telemetry_reconfiguration_stuck_millis_;
        if (!summary.quorum_available) {
            summary.health = "at-risk";
            summary.reason = "healthy voting members are below quorum";
        } else if (summary.healthy_voting_members < summary.voting_members) {
            summary.health = "degraded";
            summary.reason = "one or more voting members are unreachable or lagging";
        } else if (reconfiguration_stuck) {
            summary.health = "degraded";
            summary.reason = "reconfiguration-stuck";
        } else {
            summary.health = "healthy";
            summary.reason = "ok";
        }
        return summary;
    }

    std::int64_t InMemoryRpcHandler::reconfiguration_age_millis() const {
        const auto started_at = node_->reconfiguration_started_at_millis();
        if (!node_->joint_consensus() || started_at <= 0) {
            return 0;
        }
        const auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch())
                .count();
        return std::max<std::int64_t>(0, now - started_at);
    }

    void InMemoryRpcHandler::populate_member(raft::ClusterMemberSummary &member, const std::string &peer_id, bool local,
                                             const RaftNode::PeerProgress &progress) const {
        // Member summaries intentionally expose both role and transition state. This
        // makes stuck promotions/demotions visible to operators and tests.
        const auto current = find_member_spec(node_->current_member_specs(), peer_id);
        const auto next = find_member_spec(node_->next_member_specs(), peer_id);
        const auto effective = node_->joint_consensus() && next.has_value() ? next : current;
        const bool voting = member_is_voter(effective) || (!effective.has_value() && is_voting_member(peer_id));
        const auto transition = describe_role_transition(current, node_->joint_consensus() ? next : current);
        const bool reachable = local || progress.reachable;
        const auto match_index = local ? node_->last_log_index() : progress.match_index;
        const auto next_index = local ? node_->last_log_index() + 1 : progress.next_index;
        const auto lag = std::max<std::int64_t>(0, node_->last_log_index() - match_index);
        member.set_peer_id(peer_id);
        member.set_local(local);
        member.set_current_member(current.has_value());
        member.set_next_member(node_->joint_consensus() ? next.has_value() : current.has_value());
        member.set_voting(voting);
        member.set_role(effective.has_value() ? effective->role() : (voting ? "VOTER" : "LEARNER"));
        member.set_current_role(member_role(current));
        member.set_next_role(node_->joint_consensus() ? member_role(next) : member_role(current));
        member.set_role_transition(transition);
        member.set_transition_age_millis((transition == "steady" || transition == "known")
                                             ? 0
                                             : reconfiguration_age_millis());
        member.set_reachable(reachable);
        member.set_health(local || (reachable && progress.match_index >= node_->commit_index())
                              ? "healthy"
                              : reachable
                                    ? "lagging"
                                    : "unreachable");
        member.set_freshness(local
                                 ? "current"
                                 : reachable
                                       ? "current"
                                       : "unknown");
        member.set_next_index(next_index);
        member.set_match_index(match_index);
        member.set_lag(lag);
        member.set_consecutive_failures(local ? 0 : progress.consecutive_failures);
        const bool blocking = voting && !local && (!reachable || progress.match_index < node_->commit_index());
        member.set_blocking_quorums(blocking
                                        ? (node_->joint_consensus() && current.has_value() && next.has_value()
                                               ? "current,next"
                                               : current.has_value()
                                                     ? "current"
                                                     : "next")
                                        : "");
        member.set_blocking_reason(member.blocking_quorums().empty()
                                       ? ""
                                       : !reachable
                                             ? "unreachable"
                                             : "lagging");
    }

    void InMemoryRpcHandler::add_telemetry_cluster_members(raft::TelemetryResponse &response) const {
        const auto local_lag = node_->last_log_index() - node_->commit_index();
        auto *local = response.add_cluster_members();
        populate_member(*local, node_->peer_id(), true, RaftNode::PeerProgress{
                            .next_index = node_->last_log_index() + 1,
                            .match_index = node_->last_log_index(),
                            .reachable = true,
                        });
        local->set_lag(local_lag);

        const auto progress_map = node_->peer_progress();
        for (const auto &peer_id: recovered_peer_ids()) {
            const auto found = progress_map.find(peer_id);
            const auto progress = found != progress_map.end() ? found->second : RaftNode::PeerProgress{};
            auto *peer = response.add_cluster_members();
            populate_member(*peer, peer_id, false, progress);
        }
    }

    void InMemoryRpcHandler::add_summary_members(raft::ClusterSummaryResponse &response) const {
        auto *local = response.add_members();
        populate_member(*local, node_->peer_id(), true, RaftNode::PeerProgress{
                            .next_index = node_->last_log_index() + 1,
                            .match_index = node_->last_log_index(),
                            .reachable = true,
                        });

        const auto progress_map = node_->peer_progress();
        for (const auto &peer_id: recovered_peer_ids()) {
            const auto found = progress_map.find(peer_id);
            const auto progress = found != progress_map.end() ? found->second : RaftNode::PeerProgress{};
            auto *peer = response.add_members();
            populate_member(*peer, peer_id, false, progress);
        }
    }

    void InMemoryRpcHandler::populate_leader_endpoint(raft::ClientCommandResponse &response) const {
        response.set_leader_id(node_->leader_id().value_or(""));
        if (const auto endpoint = current_leader_endpoint(); endpoint.has_value()) {
            response.set_leader_host(endpoint->host);
            response.set_leader_port(endpoint->port);
        } else {
            response.clear_leader_host();
            response.clear_leader_port();
        }
    }

    void InMemoryRpcHandler::populate_leader_endpoint(raft::ClientQueryResponse &response) const {
        response.set_leader_id(node_->leader_id().value_or(""));
        if (const auto endpoint = current_leader_endpoint(); endpoint.has_value()) {
            response.set_leader_host(endpoint->host);
            response.set_leader_port(endpoint->port);
        } else {
            response.clear_leader_host();
            response.clear_leader_port();
        }
    }

    std::optional<InMemoryRpcHandler::AuthenticationFailure> InMemoryRpcHandler::authenticate(
        const std::string &scheme,
        const std::string &token
    ) const {
        if (!authenticator_) {
            return std::nullopt;
        }
        return authenticator_(scheme, token);
    }

    std::optional<InMemoryRpcHandler::AuthenticationFailure> InMemoryRpcHandler::authorize_command(
        const std::string &requester_id,
        const std::string &command
    ) const {
        if (!command_authorizer_) {
            return std::nullopt;
        }
        return command_authorizer_(requester_id, command);
    }

    bool InMemoryRpcHandler::local_member_is_learner() const {
        const auto current = find_member_spec(node_->current_member_specs(), node_->peer_id());
        if (current.has_value()) {
            return current->role() == "LEARNER";
        }
        const auto next = find_member_spec(node_->next_member_specs(), node_->peer_id());
        return next.has_value() && next->role() == "LEARNER";
    }

    bool InMemoryRpcHandler::allow_operational_request(const std::string &requester_id) {
        if (telemetry_rate_limit_per_minute_ <= 0) {
            return true;
        }
        // Simple per-requester sliding window. It protects high-frequency operational
        // polling without affecting Raft protocol RPCs.
        const auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch())
                .count();
        const auto cutoff = now - 60'000;
        const auto key = requester_id.empty() ? std::string{"anonymous"} : requester_id;
        auto &timestamps = operational_request_history_[key];
        while (!timestamps.empty() && timestamps.front() < cutoff) {
            timestamps.pop_front();
        }
        if (timestamps.size() >= static_cast<std::size_t>(telemetry_rate_limit_per_minute_)) {
            return false;
        }
        timestamps.push_back(now);
        return true;
    }

    void InMemoryRpcHandler::populate_redirect_leader(raft::ClusterSummaryResponse &response) const {
        response.set_redirect_leader_id(node_->leader_id().value_or(""));
        if (const auto endpoint = current_leader_endpoint(); endpoint.has_value()) {
            response.set_redirect_leader_host(endpoint->host);
            response.set_redirect_leader_port(endpoint->port);
        } else {
            response.clear_redirect_leader_host();
            response.clear_redirect_leader_port();
        }
    }

    void InMemoryRpcHandler::populate_redirect_leader(raft::ReconfigurationStatusResponse &response) const {
        response.set_redirect_leader_id(node_->leader_id().value_or(""));
        if (const auto endpoint = current_leader_endpoint(); endpoint.has_value()) {
            response.set_redirect_leader_host(endpoint->host);
            response.set_redirect_leader_port(endpoint->port);
        } else {
            response.clear_redirect_leader_host();
            response.clear_redirect_leader_port();
        }
    }

    std::optional<InMemoryRpcHandler::Endpoint> InMemoryRpcHandler::current_leader_endpoint() const {
        const auto leader_id = node_->leader_id();
        if (!leader_id.has_value()) {
            return std::nullopt;
        }
        // Redirect responses need endpoint data, not just leader id. The leader may be
        // local or one of the known peers learned from CLI input or membership messages.
        if (*leader_id == node_->peer_id()) {
            return local_endpoint_;
        }
        const auto found = known_peer_endpoints_.find(*leader_id);
        if (found == known_peer_endpoints_.end()) {
            return std::nullopt;
        }
        return found->second;
    }

    PersistentRpcHandler::PersistentRpcHandler(std::filesystem::path state_path, RaftNode::Config initial_config)
        : store_(std::move(state_path)),
          node_(std::make_shared<RaftNode>(std::move(initial_config))),
          delegate_(node_) {
        if (const auto persisted = store_.load(); persisted.has_value()) {
            // Persistent handlers recover Raft state before serving traffic. This is
            // where term/vote/log/membership state returns to the in-memory node.
            node_->apply_persistent_state(*persisted);
        } else {
            // Fresh stores are initialized immediately so later startup failures still
            // leave an inspectable baseline file.
            store_.save(node_->persistent_state());
        }
    }

    std::optional<raft::TelemetryResponse> PersistentRpcHandler::on_telemetry_request(
        const raft::TelemetryRequest &request) {
        return delegate_.on_telemetry_request(request);
    }

    std::optional<raft::ClusterSummaryResponse> PersistentRpcHandler::on_cluster_summary_request(
        const raft::ClusterSummaryRequest &request) {
        return delegate_.on_cluster_summary_request(request);
    }

    std::optional<raft::ClientCommandResponse> PersistentRpcHandler::on_client_command_request(
        const raft::ClientCommandRequest &request) {
        auto response = delegate_.on_client_command_request(request);
        persist();
        return response;
    }

    std::optional<raft::ClientQueryResponse> PersistentRpcHandler::on_client_query_request(
        const raft::ClientQueryRequest &request) {
        return delegate_.on_client_query_request(request);
    }

    std::optional<raft::JoinClusterResponse> PersistentRpcHandler::on_join_cluster_request(
        const raft::JoinClusterRequest &request) {
        auto response = delegate_.on_join_cluster_request(request);
        persist();
        return response;
    }

    std::optional<raft::JoinClusterStatusResponse> PersistentRpcHandler::on_join_cluster_status_request(
        const raft::JoinClusterStatusRequest &request) {
        return delegate_.on_join_cluster_status_request(request);
    }

    std::optional<raft::ReconfigureClusterResponse> PersistentRpcHandler::on_reconfigure_cluster_request(
        const raft::ReconfigureClusterRequest &request) {
        auto response = delegate_.on_reconfigure_cluster_request(request);
        persist();
        return response;
    }

    std::optional<raft::ReconfigurationStatusResponse> PersistentRpcHandler::on_reconfiguration_status_request(
        const raft::ReconfigurationStatusRequest &request) {
        return delegate_.on_reconfiguration_status_request(request);
    }

    std::optional<raft::VoteResponse> PersistentRpcHandler::on_vote_request(const raft::VoteRequest &request) {
        auto response = delegate_.on_vote_request(request);
        persist();
        return response;
    }

    std::optional<raft::AppendEntriesResponse> PersistentRpcHandler::on_append_entries_request(
        const raft::AppendEntriesRequest &request) {
        auto response = delegate_.on_append_entries_request(request);
        persist();
        return response;
    }

    std::optional<raft::InstallSnapshotResponse> PersistentRpcHandler::on_install_snapshot_request(
        const raft::InstallSnapshotRequest &request) {
        auto response = delegate_.on_install_snapshot_request(request);
        persist();
        return response;
    }

    std::shared_ptr<RaftNode> PersistentRpcHandler::node_ptr() {
        return node_;
    }

    const PersistentStateStore &PersistentRpcHandler::store() const {
        return store_;
    }

    InMemoryRpcHandler &PersistentRpcHandler::delegate() {
        return delegate_;
    }

    void PersistentRpcHandler::persist() {
        store_.save(node_->persistent_state());
    }
} // namespace graft
