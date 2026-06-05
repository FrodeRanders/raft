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
    persisted.peer_progress["n2"] = graft::RaftNode::PeerProgress{
        .next_index = 4,
        .match_index = 3,
        .reachable = true,
        .consecutive_failures = 2,
    };

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
    REQUIRE(loaded->peer_progress.at("n2").next_index == 4);
    REQUIRE(loaded->peer_progress.at("n2").match_index == 3);
    REQUIRE(loaded->peer_progress.at("n2").reachable);
    REQUIRE(loaded->peer_progress.at("n2").consecutive_failures == 2);
    REQUIRE(loaded->peer_progress.at("n2").last_successful_contact_millis == 0);
    REQUIRE(loaded->peer_progress.at("n2").last_failed_contact_millis == 0);
    graft::RaftNode restored(graft::RaftNode::Config{
        .peer_id = "n1",
        .current_term = 1,
        .last_log_index = 0,
        .last_log_term = 0,
        .commit_index = 0,
        .snapshot_index = 0,
        .snapshot_term = 0,
        .voting_peers = {},
    });
    restored.apply_persistent_state(*loaded);
    REQUIRE(restored.applied_kv().at("persisted") == "value");

    std::filesystem::remove(path);
}
