#include <catch2/catch_test_macros.hpp>

#include <memory>
#include <string>
#include <vector>

#include "raft.pb.h"
#include "graft/core/raft_node.hpp"
#include "graft/runtime/rpc_handler.hpp"

TEST_CASE("RaftNode applies committed membership commands", "[raft-node][membership]") {
    graft::RaftNode node(graft::RaftNode::Config{
        .peer_id = "n1",
        .current_term = 1,
        .last_log_index = 0,
        .last_log_term = 0,
        .commit_index = 0,
        .snapshot_index = 0,
        .snapshot_term = 0,
        .voting_peers = {},
    });

    raft::InternalRaftCommand join;
    auto* joining_member = join.mutable_join()->mutable_member();
    joining_member->set_id("n2");
    joining_member->set_host("127.0.0.1");
    joining_member->set_port(10082);
    joining_member->set_role("VOTER");

    raft::InternalRaftCommand joint;
    auto* n1 = joint.mutable_joint()->add_members();
    n1->set_id("n1");
    n1->set_role("VOTER");
    auto* n2 = joint.mutable_joint()->add_members();
    n2->set_id("n2");
    n2->set_role("VOTER");

    raft::InternalRaftCommand finalize;
    finalize.mutable_finalize();

    raft::AppendEntriesRequest append;
    append.set_term(1);
    append.set_leader_id("n2");
    append.set_prev_log_index(0);
    append.set_prev_log_term(0);
    append.set_leader_commit(1);
    auto* join_entry = append.add_entries();
    join_entry->set_term(1);
    join_entry->set_peer_id("n2");
    join_entry->set_data(graft::RaftNode::encode_internal_command(join));

    const auto join_response = node.handle_append_entries(append);
    REQUIRE(join_response.success());
    REQUIRE(node.has_pending_join("n2"));

    append.clear_entries();
    append.set_prev_log_index(1);
    append.set_prev_log_term(1);
    append.set_leader_commit(2);
    auto* joint_entry = append.add_entries();
    joint_entry->set_term(1);
    joint_entry->set_peer_id("n2");
    joint_entry->set_data(graft::RaftNode::encode_internal_command(joint));

    const auto joint_response = node.handle_append_entries(append);
    REQUIRE(joint_response.success());
    REQUIRE_FALSE(node.has_pending_join("n2"));
    REQUIRE(node.joint_consensus());
    REQUIRE(node.voting_peers() == std::vector<std::string>{"n2"});

    append.clear_entries();
    append.set_prev_log_index(2);
    append.set_prev_log_term(1);
    append.set_leader_commit(3);
    auto* finalize_entry = append.add_entries();
    finalize_entry->set_term(1);
    finalize_entry->set_peer_id("n2");
    finalize_entry->set_data(graft::RaftNode::encode_internal_command(finalize));

    const auto finalize_response = node.handle_append_entries(append);
    REQUIRE(finalize_response.success());
    REQUIRE_FALSE(node.joint_consensus());
}

TEST_CASE("Follower forwards membership RPCs to known leader", "[rpc][membership]") {
    auto node = std::make_shared<graft::RaftNode>(graft::RaftNode::Config{
        .peer_id = "n1",
        .current_term = 1,
        .last_log_index = 0,
        .last_log_term = 0,
        .commit_index = 0,
        .snapshot_index = 0,
        .snapshot_term = 0,
        .voting_peers = {"leader"},
    });

    raft::AppendEntriesRequest heartbeat;
    heartbeat.set_term(1);
    heartbeat.set_leader_id("leader");
    heartbeat.set_prev_log_index(0);
    heartbeat.set_prev_log_term(0);
    heartbeat.set_leader_commit(0);
    REQUIRE(node->handle_append_entries(heartbeat).success());

    graft::InMemoryRpcHandler handler(node);
    handler.set_known_peer_endpoints({graft::InMemoryRpcHandler::Endpoint{"127.0.0.1", 10080}}, {"leader"});

    bool join_forwarded = false;
    handler.set_join_forwarder(
        [&](const graft::InMemoryRpcHandler::Endpoint &endpoint, const raft::JoinClusterRequest &request) {
            join_forwarded = true;
            REQUIRE(endpoint.host == "127.0.0.1");
            REQUIRE(endpoint.port == 10080);
            REQUIRE(request.joining_peer_id() == "n2");
            return true;
        });

    raft::JoinClusterRequest join;
    join.set_term(1);
    join.set_peer_id("client");
    join.set_joining_peer_id("n2");
    join.set_host("127.0.0.1");
    join.set_port(10082);
    join.set_role("VOTER");

    const auto join_response = handler.on_join_cluster_request(join);
    REQUIRE(join_response.has_value());
    REQUIRE(join_response->success());
    REQUIRE(join_response->status() == "FORWARDED");
    REQUIRE(join_response->leader_id() == "leader");
    REQUIRE(join_forwarded);

    bool reconfigure_forwarded = false;
    handler.set_reconfigure_forwarder(
        [&](const graft::InMemoryRpcHandler::Endpoint &endpoint, const raft::ReconfigureClusterRequest &request) {
            reconfigure_forwarded = true;
            REQUIRE(endpoint.host == "127.0.0.1");
            REQUIRE(endpoint.port == 10080);
            REQUIRE(request.action() == "FINALIZE");
            return true;
        });

    raft::ReconfigureClusterRequest reconfigure;
    reconfigure.set_term(1);
    reconfigure.set_peer_id("client");
    reconfigure.set_action("FINALIZE");

    const auto reconfigure_response = handler.on_reconfigure_cluster_request(reconfigure);
    REQUIRE(reconfigure_response.has_value());
    REQUIRE(reconfigure_response->success());
    REQUIRE(reconfigure_response->status() == "FORWARDED");
    REQUIRE(reconfigure_response->leader_id() == "leader");
    REQUIRE(reconfigure_forwarded);
}

TEST_CASE("Follower redirects operational summary RPCs to known leader", "[rpc][operational]") {
    auto node = std::make_shared<graft::RaftNode>(graft::RaftNode::Config{
        .peer_id = "n1",
        .current_term = 1,
        .last_log_index = 0,
        .last_log_term = 0,
        .commit_index = 0,
        .snapshot_index = 0,
        .snapshot_term = 0,
        .voting_peers = {"leader"},
    });

    raft::AppendEntriesRequest heartbeat;
    heartbeat.set_term(1);
    heartbeat.set_leader_id("leader");
    heartbeat.set_prev_log_index(0);
    heartbeat.set_prev_log_term(0);
    heartbeat.set_leader_commit(0);
    REQUIRE(node->handle_append_entries(heartbeat).success());

    graft::InMemoryRpcHandler handler(node);
    handler.set_known_peer_endpoints({graft::InMemoryRpcHandler::Endpoint{"127.0.0.1", 10080}}, {"leader"});

    raft::TelemetryRequest telemetry;
    telemetry.set_peer_id("client");
    telemetry.set_require_leader_summary(true);

    const auto telemetry_response = handler.on_telemetry_request(telemetry);
    REQUIRE(telemetry_response.has_value());
    REQUIRE_FALSE(telemetry_response->success());
    REQUIRE(telemetry_response->status() == "REDIRECT");
    REQUIRE(telemetry_response->redirect_leader_id() == "leader");
    REQUIRE(telemetry_response->state() == "FOLLOWER");

    raft::ClusterSummaryRequest cluster_summary;
    cluster_summary.set_peer_id("client");

    const auto cluster_summary_response = handler.on_cluster_summary_request(cluster_summary);
    REQUIRE(cluster_summary_response.has_value());
    REQUIRE_FALSE(cluster_summary_response->success());
    REQUIRE(cluster_summary_response->status() == "REDIRECT");
    REQUIRE(cluster_summary_response->redirect_leader_id() == "leader");
    REQUIRE(cluster_summary_response->redirect_leader_host() == "127.0.0.1");
    REQUIRE(cluster_summary_response->redirect_leader_port() == 10080);
    REQUIRE(cluster_summary_response->state() == "FOLLOWER");

    raft::ReconfigurationStatusRequest reconfiguration_status;
    reconfiguration_status.set_peer_id("client");

    const auto reconfiguration_status_response =
            handler.on_reconfiguration_status_request(reconfiguration_status);
    REQUIRE(reconfiguration_status_response.has_value());
    REQUIRE_FALSE(reconfiguration_status_response->success());
    REQUIRE(reconfiguration_status_response->status() == "REDIRECT");
    REQUIRE(reconfiguration_status_response->redirect_leader_id() == "leader");
    REQUIRE(reconfiguration_status_response->redirect_leader_host() == "127.0.0.1");
    REQUIRE(reconfiguration_status_response->redirect_leader_port() == 10080);
    REQUIRE(reconfiguration_status_response->state() == "FOLLOWER");
}

TEST_CASE("Leader operational summaries report degraded and at-risk quorum health", "[rpc][operational]") {
    auto node = std::make_shared<graft::RaftNode>(graft::RaftNode::Config{
        .peer_id = "n1",
        .current_term = 1,
        .last_log_index = 0,
        .last_log_term = 0,
        .commit_index = 0,
        .snapshot_index = 0,
        .snapshot_term = 0,
        .voting_peers = {"n2", "n3"},
    });
    node->become_leader();

    graft::InMemoryRpcHandler handler(node);

    raft::ClusterSummaryRequest cluster_summary;
    cluster_summary.set_peer_id("client");

    const auto at_risk_response = handler.on_cluster_summary_request(cluster_summary);
    REQUIRE(at_risk_response.has_value());
    REQUIRE(at_risk_response->success());
    REQUIRE(at_risk_response->status() == "OK");
    REQUIRE(at_risk_response->cluster_health() == "at-risk");
    REQUIRE_FALSE(at_risk_response->quorum_available());
    REQUIRE(at_risk_response->healthy_voting_members() == 1);
    REQUIRE(at_risk_response->reachable_voting_members() == 1);

    raft::AppendEntriesResponse n2_response;
    n2_response.set_term(1);
    n2_response.set_peer_id("n2");
    n2_response.set_success(true);
    n2_response.set_match_index(0);
    REQUIRE(node->handle_append_entries_response("n2", n2_response));

    const auto degraded_response = handler.on_cluster_summary_request(cluster_summary);
    REQUIRE(degraded_response.has_value());
    REQUIRE(degraded_response->success());
    REQUIRE(degraded_response->status() == "OK");
    REQUIRE(degraded_response->cluster_health() == "degraded");
    REQUIRE(degraded_response->quorum_available());
    REQUIRE(degraded_response->healthy_voting_members() == 2);
    REQUIRE(degraded_response->reachable_voting_members() == 2);
}

TEST_CASE("RaftNode decommissions itself after committed finalized removal", "[raft-node][membership]") {
    graft::RaftNode node(graft::RaftNode::Config{
        .peer_id = "n1",
        .current_term = 1,
        .last_log_index = 0,
        .last_log_term = 0,
        .commit_index = 0,
        .snapshot_index = 0,
        .snapshot_term = 0,
        .voting_peers = {"n2", "n3"},
    });

    raft::InternalRaftCommand joint;
    auto* n2 = joint.mutable_joint()->add_members();
    n2->set_id("n2");
    n2->set_role("VOTER");
    auto* n3 = joint.mutable_joint()->add_members();
    n3->set_id("n3");
    n3->set_role("VOTER");

    raft::AppendEntriesRequest append;
    append.set_term(1);
    append.set_leader_id("n2");
    append.set_prev_log_index(0);
    append.set_prev_log_term(0);
    append.set_leader_commit(1);
    auto* joint_entry = append.add_entries();
    joint_entry->set_term(1);
    joint_entry->set_peer_id("n2");
    joint_entry->set_data(graft::RaftNode::encode_internal_command(joint));

    REQUIRE(node.handle_append_entries(append).success());
    REQUIRE(node.joint_consensus());
    REQUIRE_FALSE(node.decommissioned());

    raft::InternalRaftCommand finalize;
    finalize.mutable_finalize();

    append.clear_entries();
    append.set_prev_log_index(1);
    append.set_prev_log_term(1);
    append.set_leader_commit(2);
    auto* finalize_entry = append.add_entries();
    finalize_entry->set_term(1);
    finalize_entry->set_peer_id("n2");
    finalize_entry->set_data(graft::RaftNode::encode_internal_command(finalize));

    REQUIRE(node.handle_append_entries(append).success());
    REQUIRE_FALSE(node.joint_consensus());
    REQUIRE(node.decommissioned());
    REQUIRE(node.role() == graft::RaftNode::Role::follower);

    node.become_candidate();
    REQUIRE(node.role() == graft::RaftNode::Role::follower);

    const auto persisted = node.persistent_state();
    REQUIRE(persisted.decommissioned);
    REQUIRE_FALSE(persisted.pending_decommission);
}
