#include <catch2/catch_test_macros.hpp>

#include <memory>
#include <optional>
#include <string>

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

TEST_CASE("Client command and query RPCs report invalid payload status", "[rpc-handler][state-machine]") {
    graft::InMemoryRpcHandler handler("n1", 1, 0, 0);

    raft::ClientCommandRequest command;
    command.set_term(1);
    command.set_peer_id("client");
    command.set_command("not-a-state-machine-command");

    const auto command_response = handler.on_client_command_request(command);
    REQUIRE(command_response.has_value());
    REQUIRE_FALSE(command_response->success());
    REQUIRE(command_response->status() == "INVALID");

    raft::ClientQueryRequest query;
    query.set_term(1);
    query.set_peer_id("client");
    query.set_query("not-a-state-machine-query");

    const auto query_response = handler.on_client_query_request(query);
    REQUIRE(query_response.has_value());
    REQUIRE_FALSE(query_response->success());
    REQUIRE(query_response->status() == "INVALID");
}

TEST_CASE("Client command and query RPCs enforce configured shared-secret auth", "[rpc-handler][auth]") {
    graft::InMemoryRpcHandler handler("n1", 1, 0, 0);
    handler.set_local_endpoint("127.0.0.1", 10080);
    handler.set_authenticator([](const std::string &scheme, const std::string &token) {
        if (scheme != "shared-secret") {
            return std::optional<graft::InMemoryRpcHandler::AuthenticationFailure>{
                graft::InMemoryRpcHandler::AuthenticationFailure{
                    .status = "UNAUTHENTICATED",
                    .message = "Client command authentication requires scheme 'shared-secret'",
                }
            };
        }
        if (token != "top-secret") {
            return std::optional<graft::InMemoryRpcHandler::AuthenticationFailure>{
                graft::InMemoryRpcHandler::AuthenticationFailure{
                    .status = "UNAUTHENTICATED",
                    .message = "Client command authentication failed",
                }
            };
        }
        return std::optional<graft::InMemoryRpcHandler::AuthenticationFailure>{};
    });

    const auto put = graft::test::put_command("k", "v1");
    raft::ClientCommandRequest command;
    command.set_term(1);
    command.set_peer_id("client");
    command.set_command(put.SerializeAsString());

    const auto unauthenticated_command = handler.on_client_command_request(command);
    REQUIRE(unauthenticated_command.has_value());
    REQUIRE_FALSE(unauthenticated_command->success());
    REQUIRE(unauthenticated_command->status() == "UNAUTHENTICATED");
    REQUIRE(unauthenticated_command->message() == "Client command authentication requires scheme 'shared-secret'");

    command.set_auth_scheme("shared-secret");
    command.set_auth_token("top-secret");
    const auto authenticated_command = handler.on_client_command_request(command);
    REQUIRE(authenticated_command.has_value());
    REQUIRE(authenticated_command->success());
    REQUIRE(authenticated_command->status() == "ACCEPTED");

    raft::StateMachineQuery query;
    query.mutable_get()->set_key("k");
    raft::ClientQueryRequest query_request;
    query_request.set_term(1);
    query_request.set_peer_id("client");
    query_request.set_query(query.SerializeAsString());

    const auto unauthenticated_query = handler.on_client_query_request(query_request);
    REQUIRE(unauthenticated_query.has_value());
    REQUIRE_FALSE(unauthenticated_query->success());
    REQUIRE(unauthenticated_query->status() == "UNAUTHENTICATED");

    query_request.set_auth_scheme("shared-secret");
    query_request.set_auth_token("top-secret");
    const auto authenticated_query = handler.on_client_query_request(query_request);
    REQUIRE(authenticated_query.has_value());
    REQUIRE(authenticated_query->success());
    REQUIRE(authenticated_query->status() == "OK");
}

TEST_CASE("Client command RPC enforces configured requester allow-list", "[rpc-handler][authorization]") {
    graft::InMemoryRpcHandler handler("n1", 1, 0, 0);
    handler.set_command_authorizer([](const std::string &requester_id, const std::string &) {
        if (requester_id == "reference-admin") {
            return std::optional<graft::InMemoryRpcHandler::AuthenticationFailure>{};
        }
        return std::optional<graft::InMemoryRpcHandler::AuthenticationFailure>{
            graft::InMemoryRpcHandler::AuthenticationFailure{
                .status = "FORBIDDEN",
                .message = "Requester '" + requester_id + "' is not authorized to modify cluster state",
            }
        };
    });

    const auto put = graft::test::put_command("k", "v1");
    raft::ClientCommandRequest denied;
    denied.set_term(1);
    denied.set_peer_id("ordinary-client");
    denied.set_command(put.SerializeAsString());

    const auto denied_response = handler.on_client_command_request(denied);
    REQUIRE(denied_response.has_value());
    REQUIRE_FALSE(denied_response->success());
    REQUIRE(denied_response->status() == "FORBIDDEN");
    REQUIRE(denied_response->message() == "Requester 'ordinary-client' is not authorized to modify cluster state");
    REQUIRE(handler.node().applied_kv().find("k") == handler.node().applied_kv().end());

    raft::ClientCommandRequest allowed = denied;
    allowed.set_peer_id("reference-admin");

    const auto allowed_response = handler.on_client_command_request(allowed);
    REQUIRE(allowed_response.has_value());
    REQUIRE(allowed_response->success());
    REQUIRE(allowed_response->status() == "ACCEPTED");
    REQUIRE(handler.node().applied_kv().at("k") == "v1");
}

TEST_CASE("Reference-data admission rejects learner writes without redirect", "[rpc-handler][admission]") {
    auto node = std::make_shared<graft::RaftNode>(graft::RaftNode::Config{
        .peer_id = "learner",
        .current_term = 1,
        .last_log_index = 0,
        .last_log_term = 0,
        .commit_index = 0,
        .snapshot_index = 0,
        .snapshot_term = 0,
        .voting_peers = {"leader"},
    });
    auto persisted = node->persistent_state();
    persisted.current_members.clear();
    auto* self = &persisted.current_members.emplace_back();
    self->set_id("learner");
    self->set_role("LEARNER");
    auto* leader = &persisted.current_members.emplace_back();
    leader->set_id("leader");
    leader->set_role("VOTER");
    node->apply_persistent_state(persisted);

    raft::AppendEntriesRequest heartbeat;
    heartbeat.set_term(1);
    heartbeat.set_leader_id("leader");
    heartbeat.set_prev_log_index(0);
    heartbeat.set_prev_log_term(0);
    heartbeat.set_leader_commit(0);
    REQUIRE(node->handle_append_entries(heartbeat).success());

    const auto put = graft::test::put_command("k", "v1");
    raft::ClientCommandRequest command;
    command.set_term(1);
    command.set_peer_id("reference-admin");
    command.set_command(put.SerializeAsString());

    graft::InMemoryRpcHandler default_handler(node);
    default_handler.set_known_peer_endpoints({graft::InMemoryRpcHandler::Endpoint{"127.0.0.1", 10080}}, {"leader"});
    const auto default_response = default_handler.on_client_command_request(command);
    REQUIRE(default_response.has_value());
    REQUIRE_FALSE(default_response->success());
    REQUIRE(default_response->status() == "REDIRECT");

    graft::InMemoryRpcHandler reference_handler(node);
    reference_handler.set_reference_data_admission(true);
    reference_handler.set_known_peer_endpoints({graft::InMemoryRpcHandler::Endpoint{"127.0.0.1", 10080}}, {"leader"});
    const auto reference_response = reference_handler.on_client_command_request(command);
    REQUIRE(reference_response.has_value());
    REQUIRE_FALSE(reference_response->success());
    REQUIRE(reference_response->status() == "REJECTED");
    REQUIRE(reference_response->message() == "Learner nodes never accept or redirect reference-data writes");
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
