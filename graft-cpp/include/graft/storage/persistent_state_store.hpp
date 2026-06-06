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

#include <filesystem>
#include <optional>
#include <string>

#include "graft/core/raft_node.hpp"

namespace graft {
    // Simple file-backed store for RaftNode::PersistentState. This is intentionally
    // not a general storage engine; it is enough to preserve Raft safety state across
    // smoke-test restarts and to document what a production store must persist.
    class PersistentStateStore {
    public:
        explicit PersistentStateStore(std::filesystem::path path);

        const std::filesystem::path &path() const;

        bool exists() const;

        void save(const RaftNode::PersistentState &state) const;

        std::optional<RaftNode::PersistentState> load() const;

    private:
        // The on-disk format is line-oriented, so string fields are escaped rather than
        // serialized with protobuf. That keeps the file inspectable during debugging.
        static std::string escape(const std::string &value);

        static std::string unescape(const std::string &value);

        std::filesystem::path path_;
    };
} // namespace graft
