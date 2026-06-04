#include <catch2/catch_test_macros.hpp>

#include <memory>

#include "raft.pb.h"
#include "graft/core/raft_node.hpp"
#include "graft/runtime/rpc_handler.hpp"
#include "test_commands.hpp"

TEST_CASE("RaftNode applies key/value state machine commands", "[raft-node][state-machine]") {
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
    node.become_leader();

    const auto put = graft::test::put_command("k", "v1");
    const auto put_result = node.append_and_commit_local_command(put.SerializeAsString());
    REQUIRE(put_result.has_value());
    REQUIRE(node.applied_kv().at("k") == "v1");

    const auto matching_cas = graft::test::cas_command("k", true, "v1", "v2");
    const auto cas_result = node.append_and_commit_local_command(matching_cas.SerializeAsString());
    REQUIRE(cas_result.has_value());

    raft::StateMachineCommandResult decoded;
    REQUIRE(decoded.ParseFromString(cas_result->result));
    REQUIRE(decoded.result_case() == raft::StateMachineCommandResult::kCas);
    REQUIRE(decoded.cas().matched());
    REQUIRE(decoded.cas().current_value() == "v2");
    REQUIRE(node.applied_kv().at("k") == "v2");

    const auto failing_cas = graft::test::cas_command("k", true, "v1", "v3");
    const auto failed_result = node.append_and_commit_local_command(failing_cas.SerializeAsString());
    REQUIRE(failed_result.has_value());
    REQUIRE(decoded.ParseFromString(failed_result->result));
    REQUIRE_FALSE(decoded.cas().matched());
    REQUIRE(decoded.cas().current_value() == "v2");
    REQUIRE(node.applied_kv().at("k") == "v2");
}

TEST_CASE("Client command RPC reports accepted after commit", "[rpc-handler][state-machine]") {
    graft::InMemoryRpcHandler handler("n1", 1, 0, 0);
    handler.set_local_endpoint("127.0.0.1", 10080);

    const auto put = graft::test::put_command("k", "v1");
    raft::ClientCommandRequest request;
    request.set_term(1);
    request.set_peer_id("client");
    request.set_command(put.SerializeAsString());

    const auto response = handler.on_client_command_request(request);
    REQUIRE(response.has_value());
    REQUIRE(response->success());
    REQUIRE(response->status() == "ACCEPTED");
    REQUIRE(response->message() == "Command committed and applied");
    REQUIRE(response->leader_id() == "n1");
    REQUIRE(response->leader_host() == "127.0.0.1");
    REQUIRE(response->leader_port() == 10080);
}

TEST_CASE("Client query RPC requires read barrier in multi-node leader mode", "[rpc-handler][state-machine]") {
    auto node = std::make_shared<graft::RaftNode>(graft::RaftNode::Config{
        .peer_id = "n1",
        .current_term = 1,
        .last_log_index = 0,
        .last_log_term = 0,
        .commit_index = 0,
        .snapshot_index = 0,
        .snapshot_term = 0,
        .voting_peers = {"n2"},
    });
    node->become_leader();

    graft::InMemoryRpcHandler handler(node);
    handler.set_local_endpoint("127.0.0.1", 10080);

    raft::StateMachineQuery query;
    query.mutable_get()->set_key("k");

    raft::ClientQueryRequest request;
    request.set_term(1);
    request.set_peer_id("client");
    request.set_query(query.SerializeAsString());

    const auto no_barrier_response = handler.on_client_query_request(request);
    REQUIRE(no_barrier_response.has_value());
    REQUIRE_FALSE(no_barrier_response->success());
    REQUIRE(no_barrier_response->status() == "RETRY");
    REQUIRE(no_barrier_response->message() == "Leader cannot currently guarantee a linearizable read");

    handler.set_read_barrier([] {
        return true;
    });

    const auto barrier_response = handler.on_client_query_request(request);
    REQUIRE(barrier_response.has_value());
    REQUIRE(barrier_response->success());
    REQUIRE(barrier_response->status() == "OK");
    REQUIRE(barrier_response->message() == "Query completed");
}
