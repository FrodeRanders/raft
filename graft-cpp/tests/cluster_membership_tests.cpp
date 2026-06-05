#include <catch2/catch_test_macros.hpp>

#include <string>
#include <unordered_set>
#include <vector>

#include "graft/core/cluster_membership.hpp"

namespace {
    raft::PeerSpec member(std::string id, std::string role = "VOTER") {
        raft::PeerSpec spec;
        spec.set_id(std::move(id));
        spec.set_role(std::move(role));
        return spec;
    }
}

TEST_CASE("ClusterMembership normalizes member specs", "[cluster-membership]") {
    std::vector<raft::PeerSpec> members;
    members.push_back(member("n2", ""));
    members.push_back(member(""));
    members.push_back(member("n1", "LEARNER"));
    members.push_back(member("n2", "LEARNER"));

    const auto normalized = graft::ClusterMembership::normalize_member_specs(std::move(members));

    REQUIRE(normalized.size() == 2);
    REQUIRE(normalized[0].id() == "n1");
    REQUIRE(normalized[0].role() == "LEARNER");
    REQUIRE(normalized[1].id() == "n2");
    REQUIRE(normalized[1].role() == "VOTER");
}

TEST_CASE("ClusterMembership computes stable and joint voting sets", "[cluster-membership]") {
    const auto current = graft::ClusterMembership::current_voting_members(
        "n1",
        false,
        std::vector<std::string>{"n3", "n2", "n2"},
        std::vector<raft::PeerSpec>{});

    REQUIRE(current == std::vector<std::string>{"n1", "n2", "n3"});

    const auto next = graft::ClusterMembership::next_voting_members(
        true,
        current,
        std::vector<raft::PeerSpec>{member("n2"), member("n3"), member("n4", "LEARNER")});

    REQUIRE(next == std::vector<std::string>{"n2", "n3"});

    const auto active = graft::ClusterMembership::active_voting_members(current, next);
    REQUIRE(active == std::vector<std::string>{"n1", "n2", "n3"});
}

TEST_CASE("ClusterMembership requires current and next majorities in joint consensus", "[cluster-membership]") {
    const std::vector<std::string> current{"n1", "n2", "n3"};
    const std::vector<std::string> next{"n2", "n3", "n4"};

    REQUIRE(graft::ClusterMembership::has_joint_majority(
        std::unordered_set<std::string>{"n1", "n2", "n3"},
        current,
        next,
        true));

    REQUIRE_FALSE(graft::ClusterMembership::has_joint_majority(
        std::unordered_set<std::string>{"n1", "n2"},
        current,
        next,
        true));
}
