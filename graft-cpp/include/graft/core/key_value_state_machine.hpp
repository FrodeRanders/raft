/*
 * Copyright (C) 2026 Frode Randers
 * All rights reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <string>
#include <unordered_map>

#include "graft/core/application_state_machine.hpp"
#include "raft.pb.h"

namespace graft {
    // Basic/demo application for C++.
    //
    // This class demonstrates how a domain state machine plugs into Raft:
    // - apply handles committed writes
    // - query handles safe reads
    // - snapshot/restore handle only domain state
    //
    // It deliberately does not know about members, quorum, leadership, or log replication.
    class KeyValueStateMachine final : public ApplicationStateMachine {
    public:
        using Store = std::unordered_map<std::string, std::string>;

        std::string apply(std::int64_t index, std::int64_t term, std::string_view command) override;

        std::string query(std::string_view request) const override;

        std::string snapshot() const override;

        void restore(std::string_view snapshot) override;

        const Store &store() const;

        void replace_store(Store store);

        static std::string apply_command(Store &store, const raft::StateMachineCommand &command);

        static raft::StateMachineQueryResult get(const Store &store, const std::string &key);

    private:
        Store store_;
    };
} // namespace graft
