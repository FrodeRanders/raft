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

#include <cstdint>
#include <string>
#include <string_view>

namespace graft {
    // ApplicationStateMachine is the C++ domain/Raft boundary.
    //
    // Raft owns elections, log replication, commit, membership, snapshots as transport/storage units,
    // and read safety. A domain application owns only opaque command/query payloads and domain
    // snapshot bytes. Cluster configuration must not be stored in the domain state machine.
    class ApplicationStateMachine {
    public:
        virtual ~ApplicationStateMachine() = default;

        // Called after Raft has committed a log entry. The index and term identify the committed entry
        // for audit/idempotency purposes; they are not an invitation for the application to influence
        // consensus behavior.
        virtual std::string apply(std::int64_t index, std::int64_t term, std::string_view command) = 0;

        // Called only after the runtime has established read safety for the leader. Implementations
        // should answer from local domain state and should not contact Raft peers.
        virtual std::string query(std::string_view request) const = 0;

        // Return only application state. Raft wraps these bytes with cluster membership metadata.
        virtual std::string snapshot() const = 0;

        // Restore only application state. Raft has already restored its own metadata from the wrapper.
        virtual void restore(std::string_view snapshot) = 0;
    };
} // namespace graft
