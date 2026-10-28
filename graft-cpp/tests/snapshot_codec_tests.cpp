#include <catch2/catch_test_macros.hpp>

#include <string>
#include <unordered_map>
#include <vector>

#include "graft/core/snapshot_codec.hpp"
#include "graft/core/key_value_state_machine.hpp"

TEST_CASE("SnapshotCodec matches Java modified UTF including NUL and supplementary characters", "[snapshot-codec]") {
    const std::unordered_map<std::string, std::string> applied{{"k", std::string("\0\xf0\x9f\x98\x80", 5)}};
    const std::string fixture("\0\0\0\x01\0\x01k\0\x08\xc0\x80\xed\xa0\xbd\xed\xb8\x80", 17);
    REQUIRE(graft::SnapshotCodec::serialize_key_value_snapshot(applied) == fixture);
    REQUIRE(graft::SnapshotCodec::deserialize_key_value_snapshot(fixture) == applied);
    REQUIRE_FALSE(graft::SnapshotCodec::deserialize_key_value_snapshot(fixture + "x"));
    REQUIRE_THROWS(graft::SnapshotCodec::serialize_key_value_snapshot({{"k", std::string(65536, 'x')}}));
    graft::KeyValueStateMachine state;
    REQUIRE_THROWS(state.restore("invalid"));
}

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
