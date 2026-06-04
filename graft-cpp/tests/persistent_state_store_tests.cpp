#include <catch2/catch_test_macros.hpp>

#include <filesystem>

#include "graft/core/raft_node.hpp"
#include "graft/storage/persistent_state_store.hpp"
#include "test_commands.hpp"

TEST_CASE("PersistentStateStore round-trips node state", "[raft-node][persistence]") {
    const auto path = std::filesystem::temp_directory_path() / "graft-unit-state.txt";
    std::filesystem::remove(path);

    graft::PersistentStateStore store(path);
    graft::RaftNode node(graft::RaftNode::Config{
        .peer_id = "n1",
        .current_term = 7,
        .last_log_index = 0,
        .last_log_term = 0,
        .commit_index = 0,
        .snapshot_index = 0,
        .snapshot_term = 0,
        .voting_peers = {},
    });
    node.become_leader();
    const auto put = graft::test::put_command("persisted", "value");
    REQUIRE(node.append_and_commit_local_command(put.SerializeAsString()).has_value());
    auto persisted = node.persistent_state();
    persisted.reconfiguration_started_at_millis = 12345;
    raft::PeerSpec next;
    next.set_id("n2");
    next.set_host("127.0.0.1");
    next.set_port(10082);
    next.set_role("LEARNER");
    persisted.next_members.push_back(next);

    store.save(persisted);
    const auto loaded = store.load();

    REQUIRE(loaded.has_value());
    REQUIRE(loaded->peer_id == "n1");
    REQUIRE(loaded->leader_id == "n1");
    REQUIRE(loaded->reconfiguration_started_at_millis == 12345);
    REQUIRE(loaded->current_members.size() == 1);
    REQUIRE(loaded->current_members.front().id() == "n1");
    REQUIRE(loaded->next_members.size() == 1);
    REQUIRE(loaded->next_members.front().id() == "n2");
    REQUIRE(loaded->next_members.front().role() == "LEARNER");
    REQUIRE(loaded->applied_kv.at("persisted") == "value");

    std::filesystem::remove(path);
}
