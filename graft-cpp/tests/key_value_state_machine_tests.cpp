#include <catch2/catch_test_macros.hpp>

#include <string>

#include "graft/core/key_value_state_machine.hpp"
#include "test_commands.hpp"

TEST_CASE("KeyValueStateMachine applies put delete clear and get", "[key-value-state-machine]") {
    graft::KeyValueStateMachine::Store store;

    graft::KeyValueStateMachine::apply_command(store, graft::test::put_command("k", "v1"));
    REQUIRE(store.at("k") == "v1");

    auto result = graft::KeyValueStateMachine::get(store, "k");
    REQUIRE(result.get().found());
    REQUIRE(result.get().value() == "v1");

    raft::StateMachineCommand delete_command;
    delete_command.mutable_delete_()->set_key("k");
    graft::KeyValueStateMachine::apply_command(store, delete_command);
    REQUIRE(store.find("k") == store.end());

    graft::KeyValueStateMachine::apply_command(store, graft::test::put_command("a", "one"));
    graft::KeyValueStateMachine::apply_command(store, graft::test::put_command("b", "two"));
    raft::StateMachineCommand clear_command;
    clear_command.mutable_clear();
    graft::KeyValueStateMachine::apply_command(store, clear_command);
    REQUIRE(store.empty());
}

TEST_CASE("KeyValueStateMachine reports CAS result payload", "[key-value-state-machine]") {
    graft::KeyValueStateMachine::Store store{{"k", "v1"}};

    const auto encoded = graft::KeyValueStateMachine::apply_command(
        store,
        graft::test::cas_command("k", true, "v1", "v2"));

    raft::StateMachineCommandResult result;
    REQUIRE(result.ParseFromString(encoded));
    REQUIRE(result.cas().matched());
    REQUIRE(result.cas().current_present());
    REQUIRE(result.cas().current_value() == "v2");
    REQUIRE(store.at("k") == "v2");
}
