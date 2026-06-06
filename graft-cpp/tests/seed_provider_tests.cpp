#include <catch2/catch_test_macros.hpp>

#include <vector>

#include "graft/runtime/seed_provider.hpp"

TEST_CASE("DnsSrvSeedProvider derives peer ids from SRV targets", "[seed-provider]") {
    graft::DnsSrvSeedProvider provider("_raft._tcp.raft.local", [](const std::string &) {
        return std::vector<graft::DnsSrvRecord>{
            graft::DnsSrvRecord{10, 10, 7000, "raft-2.raft.local."},
            graft::DnsSrvRecord{10, 10, 7000, "raft-1.raft.local."},
        };
    });

    const auto seeds = provider.seeds();

    REQUIRE(seeds.size() == 2);
    REQUIRE(seeds[0].peer_id == "raft-1");
    REQUIRE(seeds[0].host == "raft-1.raft.local.");
    REQUIRE(seeds[0].port == 7000);
    REQUIRE(seeds[1].peer_id == "raft-2");
}

TEST_CASE("StaticSeedProvider preserves explicit peer endpoints", "[seed-provider]") {
    std::vector<graft::PeerEndpoint> peers{
        graft::PeerEndpoint{.peer_id = "n1", .host = "127.0.0.1", .port = 10080, .role = "VOTER"},
        graft::PeerEndpoint{.peer_id = "n2", .host = "127.0.0.1", .port = 10081, .role = "LEARNER"},
    };

    graft::StaticSeedProvider provider(peers);
    const auto seeds = provider.seeds();

    REQUIRE(seeds.size() == 2);
    REQUIRE(seeds[0].peer_id == peers[0].peer_id);
    REQUIRE(seeds[0].host == peers[0].host);
    REQUIRE(seeds[0].port == peers[0].port);
    REQUIRE(seeds[0].role == peers[0].role);
    REQUIRE(seeds[1].peer_id == peers[1].peer_id);
    REQUIRE(seeds[1].host == peers[1].host);
    REQUIRE(seeds[1].port == peers[1].port);
    REQUIRE(seeds[1].role == peers[1].role);
}
