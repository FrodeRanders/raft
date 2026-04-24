#include <catch2/catch_test_macros.hpp>

#include <string>
#include <vector>

#include "raft.pb.h"
#include "graft/core/raft_node.hpp"

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
    append.set_leader_id("leader");
    append.set_prev_log_index(0);
    append.set_prev_log_term(0);
    append.set_leader_commit(1);
    auto* join_entry = append.add_entries();
    join_entry->set_term(1);
    join_entry->set_peer_id("leader");
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
    joint_entry->set_peer_id("leader");
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
    finalize_entry->set_peer_id("leader");
    finalize_entry->set_data(graft::RaftNode::encode_internal_command(finalize));

    const auto finalize_response = node.handle_append_entries(append);
    REQUIRE(finalize_response.success());
    REQUIRE_FALSE(node.joint_consensus());
}
