#include <catch2/catch_test_macros.hpp>

#include <string>
#include <unordered_map>
#include <vector>

#include "graft/core/snapshot_codec.hpp"

TEST_CASE("SnapshotCodec round-trips Java-compatible key value snapshots", "[snapshot-codec]") {
    const std::unordered_map<std::string, std::string> applied{
        {"b", "two"},
        {"a", "one"},
    };

    const auto encoded = graft::SnapshotCodec::serialize_key_value_snapshot(applied);
    const auto decoded = graft::SnapshotCodec::deserialize_key_value_snapshot(encoded);

    REQUIRE(decoded.has_value());
    REQUIRE(decoded->at("a") == "one");
    REQUIRE(decoded->at("b") == "two");
}

TEST_CASE("SnapshotCodec unwraps membership snapshot envelopes", "[snapshot-codec]") {
    const std::unordered_map<std::string, std::string> applied{
        {"k", "v"},
    };
    const auto state_machine_snapshot = graft::SnapshotCodec::serialize_key_value_snapshot(applied);
    const auto wrapped = graft::SnapshotCodec::wrap_payload(
        std::vector<std::string>{"n1", "n2"},
        std::vector<std::string>{},
        state_machine_snapshot);

    const auto unwrapped = graft::SnapshotCodec::unwrap_payload(wrapped);
    const auto decoded = graft::SnapshotCodec::deserialize_key_value_snapshot(unwrapped);

    REQUIRE(decoded.has_value());
    REQUIRE(decoded->at("k") == "v");
}
