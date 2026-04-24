#pragma once

#include <string>

#include "raft.pb.h"

namespace graft::test {

inline raft::StateMachineCommand put_command(const std::string& key, const std::string& value) {
    raft::StateMachineCommand command;
    command.mutable_put()->set_key(key);
    command.mutable_put()->set_value(value);
    return command;
}

inline raft::StateMachineCommand cas_command(
    const std::string& key,
    bool expected_present,
    const std::string& expected_value,
    const std::string& new_value
) {
    raft::StateMachineCommand command;
    auto* cas = command.mutable_cas();
    cas->set_key(key);
    cas->set_expected_present(expected_present);
    cas->set_expected_value(expected_value);
    cas->set_new_value(new_value);
    return command;
}

} // namespace graft::test
