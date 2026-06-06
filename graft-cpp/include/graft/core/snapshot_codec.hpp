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
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace graft {
    // SnapshotCodec separates application bytes from Raft metadata. The wrapped
    // snapshot carries membership state so a follower or restarted node can restore
    // the correct configuration at the compaction boundary.
    class SnapshotCodec {
    public:
        // Wrap a domain snapshot with current/next membership. next_members is empty
        // for stable configurations and populated for joint consensus.
        static std::string wrap_payload(const std::vector<std::string> &current_members,
                                        const std::vector<std::string> &next_members,
                                        const std::string &state_machine_snapshot);

        // Return only the domain payload for ApplicationStateMachine::restore().
        static std::string unwrap_payload(const std::string &payload);

        // Java-compatible key-value snapshot helpers used by the demo state machine
        // and mixed Java/C++ snapshot tests.
        static std::string serialize_key_value_snapshot(
            const std::unordered_map<std::string, std::string> &applied_kv);

        static std::optional<std::unordered_map<std::string, std::string> > deserialize_key_value_snapshot(
            const std::string &snapshot);

    private:
        // The binary helper routines keep the snapshot format deterministic and avoid
        // dependence on platform endianness.
        static void append_u32_be(std::string &out, std::uint32_t value);

        static void append_u16_be(std::string &out, std::uint16_t value);

        static bool read_u32_be(const std::string &data, std::size_t &offset, std::uint32_t &value);

        static bool read_u16_be(const std::string &data, std::size_t &offset, std::uint16_t &value);

        static std::string base64_encode(std::string_view input);

        static std::optional<std::string> base64_decode(std::string_view input);

        static std::string quote_json(std::string_view value);
    };
} // namespace graft
