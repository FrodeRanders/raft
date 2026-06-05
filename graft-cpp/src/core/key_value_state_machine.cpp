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

#include "graft/core/key_value_state_machine.hpp"

#include "graft/core/snapshot_codec.hpp"

#include <stdexcept>
#include <utility>

namespace graft {
    std::string KeyValueStateMachine::apply(std::int64_t, std::int64_t, std::string_view data) {
        // The runtime calls apply only after the command has been appended, replicated, and committed.
        // Invalid demo payloads return an empty result; real applications can choose stricter validation
        // before submission or encode rejection in the command result.
        raft::StateMachineCommand command;
        if (!command.ParseFromArray(data.data(), static_cast<int>(data.size()))) {
            return {};
        }
        return apply_command(store_, command);
    }

    std::string KeyValueStateMachine::query(std::string_view data) const {
        // Raft read safety has already been handled by the runtime. This method is just a domain lookup.
        raft::StateMachineQuery query;
        if (!query.ParseFromArray(data.data(), static_cast<int>(data.size())) ||
            query.query_case() != raft::StateMachineQuery::kGet) {
            return {};
        }

        const auto result = get(store_, query.get().key());
        std::string encoded;
        if (!result.SerializeToString(&encoded)) {
            throw std::runtime_error("failed to serialize StateMachineQueryResult");
        }
        return encoded;
    }

    std::string KeyValueStateMachine::snapshot() const {
        // SnapshotCodec serializes the demo map. Raft will wrap these bytes with current/next members.
        return SnapshotCodec::serialize_key_value_snapshot(store_);
    }

    void KeyValueStateMachine::restore(std::string_view snapshot) {
        // The incoming bytes are already unwrapped application bytes, not the full Raft snapshot.
        if (auto restored = SnapshotCodec::deserialize_key_value_snapshot(std::string(snapshot)); restored.has_value()) {
            store_ = std::move(*restored);
        }
    }

    const KeyValueStateMachine::Store &KeyValueStateMachine::store() const {
        return store_;
    }

    void KeyValueStateMachine::replace_store(Store store) {
        store_ = std::move(store);
    }

    std::string KeyValueStateMachine::apply_command(Store &store, const raft::StateMachineCommand &command) {
        // These protobuf command types are the demo application's command language. They are distinct
        // from InternalRaftCommand, which the Raft node consumes for membership changes.
        switch (command.command_case()) {
            case raft::StateMachineCommand::kPut:
                store[command.put().key()] = command.put().value();
                return {};
            case raft::StateMachineCommand::kDelete:
                store.erase(command.delete_().key());
                return {};
            case raft::StateMachineCommand::kClear:
                store.clear();
                return {};
            case raft::StateMachineCommand::kCas: {
                const auto &cas = command.cas();
                const auto found = store.find(cas.key());
                const bool present = found != store.end();
                const bool matched = present == cas.expected_present() &&
                                     (!present || found->second == cas.expected_value());
                bool current_present = present;
                std::string current_value = present ? found->second : "";
                if (matched) {
                    store[cas.key()] = cas.new_value();
                    current_present = true;
                    current_value = cas.new_value();
                }

                raft::StateMachineCommandResult result;
                auto *cas_result = result.mutable_cas();
                cas_result->set_key(cas.key());
                cas_result->set_expected_present(cas.expected_present());
                cas_result->set_expected_value(cas.expected_value());
                cas_result->set_new_value(cas.new_value());
                cas_result->set_matched(matched);
                cas_result->set_current_present(current_present);
                cas_result->set_current_value(current_value);

                std::string encoded;
                if (!result.SerializeToString(&encoded)) {
                    throw std::runtime_error("failed to serialize StateMachineCommandResult");
                }
                return encoded;
            }
            case raft::StateMachineCommand::COMMAND_NOT_SET:
                return {};
        }
        return {};
    }

    raft::StateMachineQueryResult KeyValueStateMachine::get(const Store &store, const std::string &key) {
        raft::StateMachineQueryResult result;
        auto *get = result.mutable_get();
        get->set_key(key);
        const auto found = store.find(key);
        get->set_found(found != store.end());
        if (found != store.end()) {
            get->set_value(found->second);
        }
        return result;
    }
} // namespace graft
