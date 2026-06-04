#include <catch2/catch_test_macros.hpp>

#include "raft.pb.h"
#include "graft/core/raft_node.hpp"

TEST_CASE("RaftNode grants votes only to eligible candidates", "[raft-node][election]") {
    SECTION("grants vote to up-to-date candidate") {
        graft::RaftNode node(graft::RaftNode::Config{
            .peer_id = "n1",
            .current_term = 1,
            .last_log_index = 5,
            .last_log_term = 2,
            .commit_index = 0,
            .snapshot_index = 0,
            .snapshot_term = 0,
            .voting_peers = {"n2", "n3"},
        });

        raft::VoteRequest request;
        request.set_term(2);
        request.set_candidate_id("n2");
        request.set_last_log_index(5);
        request.set_last_log_term(2);

        const auto response = node.handle_vote_request(request);

        REQUIRE(response.vote_granted());
        REQUIRE(response.current_term() == 2);
        REQUIRE(node.voted_for() == "n2");
    }

    SECTION("rejects candidate with stale log term") {
        graft::RaftNode node(graft::RaftNode::Config{
            .peer_id = "n1",
            .current_term = 3,
            .last_log_index = 5,
            .last_log_term = 4,
            .commit_index = 0,
            .snapshot_index = 0,
            .snapshot_term = 0,
            .voting_peers = {"n2", "n3"},
        });

        raft::VoteRequest request;
        request.set_term(3);
        request.set_candidate_id("n2");
        request.set_last_log_index(9);
        request.set_last_log_term(3);

        const auto response = node.handle_vote_request(request);

        REQUIRE_FALSE(response.vote_granted());
        REQUIRE_FALSE(node.voted_for().has_value());
    }
}
