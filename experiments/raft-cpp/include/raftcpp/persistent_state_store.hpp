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

#include "raftcpp/raft_node.hpp"

namespace raftcpp {
    class PersistentStateStore {
    public:
        explicit PersistentStateStore(std::filesystem::path path);

        const std::filesystem::path &path() const;

        bool exists() const;

        void save(const RaftNode::PersistentState &state) const;

        std::optional<RaftNode::PersistentState> load() const;

    private:
        static std::string escape(const std::string &value);

        static std::string unescape(const std::string &value);

        std::filesystem::path path_;
    };
} // namespace raftcpp
