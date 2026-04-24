#include <catch2/catch_test_macros.hpp>

#include "raft.pb.h"
#include "graft/core/raft_node.hpp"
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
