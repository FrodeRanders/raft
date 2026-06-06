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

#include "graft/core/snapshot_codec.hpp"

#include <algorithm>
#include <array>
#include <cstdio>
#include <limits>
#include <stdexcept>

namespace graft {
    std::string SnapshotCodec::wrap_payload(const std::vector<std::string> &current_members,
                                            const std::vector<std::string> &next_members,
                                            const std::string &state_machine_snapshot) {
        auto append_member_list = [](std::string &out, const std::vector<std::string> &members) {
            out.push_back('[');
            bool first = true;
            for (const auto &member: members) {
                if (!first) {
                    out.push_back(',');
                }
                first = false;
                out.append("{\"id\":");
                out.append(quote_json(member));
                out.append(",\"role\":\"VOTER\",\"address\":null}");
            }
            out.push_back(']');
        };

        // Java snapshots wrap opaque state-machine bytes with cluster configuration metadata.
        std::string out;
        out.append("{\"version\":1,\"currentMembers\":");
        append_member_list(out, current_members);
        out.append(",\"nextMembers\":");
        append_member_list(out, next_members);
        out.append(",\"stateMachineSnapshot\":");
        out.append(quote_json(base64_encode(state_machine_snapshot)));
        out.push_back('}');
        return out;
    }

    std::string SnapshotCodec::unwrap_payload(const std::string &payload) {
        if (payload.empty() || payload.front() != '{') {
            // Older/local snapshots may be raw application bytes. Preserve backward
            // compatibility by returning them unchanged.
            return payload;
        }

        const std::string marker = "\"stateMachineSnapshot\":\"";
        const auto start = payload.find(marker);
        if (start == std::string::npos) {
            return payload;
        }
        const auto value_start = start + marker.size();
        const auto value_end = payload.find('"', value_start);
        if (value_end == std::string::npos) {
            return payload;
        }
        const auto decoded = base64_decode(std::string_view(payload).substr(value_start, value_end - value_start));
        return decoded.value_or(payload);
    }

    std::string SnapshotCodec::serialize_key_value_snapshot(
        const std::unordered_map<std::string, std::string> &applied_kv) {
        std::string out;
        std::vector<std::pair<std::string, std::string> > ordered(applied_kv.begin(), applied_kv.end());
        std::sort(ordered.begin(), ordered.end(), [](const auto &left, const auto &right) {
            return left.first < right.first;
        });

        append_u32_be(out, static_cast<std::uint32_t>(ordered.size()));
        for (const auto &[key, value]: ordered) {
            if (key.size() > std::numeric_limits<std::uint16_t>::max() ||
                value.size() > std::numeric_limits<std::uint16_t>::max()) {
                throw std::runtime_error("snapshot key or value exceeds Java UTF length limit");
            }
            // Java DataOutput.writeUTF uses big-endian unsigned 16-bit lengths.
            append_u16_be(out, static_cast<std::uint16_t>(key.size()));
            out.append(key);
            append_u16_be(out, static_cast<std::uint16_t>(value.size()));
            out.append(value);
        }
        return out;
    }

    std::optional<std::unordered_map<std::string, std::string> > SnapshotCodec::deserialize_key_value_snapshot(
        const std::string &snapshot) {
        std::unordered_map<std::string, std::string> restored;
        std::size_t offset = 0;
        std::uint32_t size = 0;
        if (!read_u32_be(snapshot, offset, size)) {
            return std::nullopt;
        }
        for (std::uint32_t i = 0; i < size; ++i) {
            // The format is strict: a malformed length or truncated key/value makes the
            // whole snapshot invalid rather than partially restored.
            std::uint16_t key_length = 0;
            std::uint16_t value_length = 0;
            if (!read_u16_be(snapshot, offset, key_length) || offset + key_length > snapshot.size()) {
                return std::nullopt;
            }
            auto key = snapshot.substr(offset, key_length);
            offset += key_length;
            if (!read_u16_be(snapshot, offset, value_length) || offset + value_length > snapshot.size()) {
                return std::nullopt;
            }
            auto value = snapshot.substr(offset, value_length);
            offset += value_length;
            restored[std::move(key)] = std::move(value);
        }
        return restored;
    }

    void SnapshotCodec::append_u32_be(std::string &out, std::uint32_t value) {
        out.push_back(static_cast<char>((value >> 24) & 0xFF));
        out.push_back(static_cast<char>((value >> 16) & 0xFF));
        out.push_back(static_cast<char>((value >> 8) & 0xFF));
        out.push_back(static_cast<char>(value & 0xFF));
    }

    void SnapshotCodec::append_u16_be(std::string &out, std::uint16_t value) {
        out.push_back(static_cast<char>((value >> 8) & 0xFF));
        out.push_back(static_cast<char>(value & 0xFF));
    }

    bool SnapshotCodec::read_u32_be(const std::string &data, std::size_t &offset, std::uint32_t &value) {
        if (offset + 4 > data.size()) {
            return false;
        }
        value = (static_cast<std::uint32_t>(static_cast<unsigned char>(data[offset])) << 24) |
                (static_cast<std::uint32_t>(static_cast<unsigned char>(data[offset + 1])) << 16) |
                (static_cast<std::uint32_t>(static_cast<unsigned char>(data[offset + 2])) << 8) |
                static_cast<std::uint32_t>(static_cast<unsigned char>(data[offset + 3]));
        offset += 4;
        return true;
    }

    bool SnapshotCodec::read_u16_be(const std::string &data, std::size_t &offset, std::uint16_t &value) {
        if (offset + 2 > data.size()) {
            return false;
        }
        value = static_cast<std::uint16_t>(
            (static_cast<std::uint16_t>(static_cast<unsigned char>(data[offset])) << 8) |
            static_cast<std::uint16_t>(static_cast<unsigned char>(data[offset + 1])));
        offset += 2;
        return true;
    }

    std::string SnapshotCodec::base64_encode(std::string_view input) {
        static constexpr char alphabet[] =
                "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                "abcdefghijklmnopqrstuvwxyz"
                "0123456789+/";

        std::string out;
        out.reserve(((input.size() + 2) / 3) * 4);
        std::size_t index = 0;
        while (index + 3 <= input.size()) {
            const auto a = static_cast<unsigned char>(input[index++]);
            const auto b = static_cast<unsigned char>(input[index++]);
            const auto c = static_cast<unsigned char>(input[index++]);
            out.push_back(alphabet[(a >> 2) & 0x3F]);
            out.push_back(alphabet[((a & 0x03) << 4) | ((b >> 4) & 0x0F)]);
            out.push_back(alphabet[((b & 0x0F) << 2) | ((c >> 6) & 0x03)]);
            out.push_back(alphabet[c & 0x3F]);
        }

        const auto remaining = input.size() - index;
        if (remaining == 1) {
            const auto a = static_cast<unsigned char>(input[index]);
            out.push_back(alphabet[(a >> 2) & 0x3F]);
            out.push_back(alphabet[(a & 0x03) << 4]);
            out.push_back('=');
            out.push_back('=');
        } else if (remaining == 2) {
            const auto a = static_cast<unsigned char>(input[index]);
            const auto b = static_cast<unsigned char>(input[index + 1]);
            out.push_back(alphabet[(a >> 2) & 0x3F]);
            out.push_back(alphabet[((a & 0x03) << 4) | ((b >> 4) & 0x0F)]);
            out.push_back(alphabet[(b & 0x0F) << 2]);
            out.push_back('=');
        }
        return out;
    }

    std::optional<std::string> SnapshotCodec::base64_decode(std::string_view input) {
        static constexpr unsigned char invalid = 0xFF;
        static constexpr std::array<unsigned char, 256> decode_table = [] {
            std::array<unsigned char, 256> table{};
            table.fill(invalid);
            for (unsigned char i = 0; i < 26; ++i) {
                table['A' + i] = i;
                table['a' + i] = static_cast<unsigned char>(26 + i);
            }
            for (unsigned char i = 0; i < 10; ++i) {
                table['0' + i] = static_cast<unsigned char>(52 + i);
            }
            table['+'] = 62;
            table['/'] = 63;
            table['='] = 0;
            return table;
        }();

        std::string out;
        out.reserve((input.size() / 4) * 3);
        for (std::size_t i = 0; i < input.size();) {
            unsigned char values[4];
            std::size_t pad = 0;
            for (int j = 0; j < 4; ++j) {
                // Whitespace is not expected in generated snapshots. Rejecting invalid
                // characters keeps unwrap failures visible and deterministic.
                if (i >= input.size()) {
                    return std::nullopt;
                }
                const unsigned char ch = static_cast<unsigned char>(input[i++]);
                const auto decoded = decode_table[ch];
                if (decoded == invalid) {
                    return std::nullopt;
                }
                if (ch == '=') {
                    ++pad;
                }
                values[j] = decoded;
            }

            out.push_back(static_cast<char>((values[0] << 2) | (values[1] >> 4)));
            if (pad < 2) {
                out.push_back(static_cast<char>((values[1] << 4) | (values[2] >> 2)));
            }
            if (pad < 1) {
                out.push_back(static_cast<char>((values[2] << 6) | values[3]));
            }
        }
        return out;
    }

    std::string SnapshotCodec::quote_json(std::string_view value) {
        std::string escaped;
        escaped.push_back('"');
        for (char c: value) {
            switch (c) {
                case '"':
                    escaped.append("\\\"");
                    break;
                case '\\':
                    escaped.append("\\\\");
                    break;
                case '\b':
                    escaped.append("\\b");
                    break;
                case '\f':
                    escaped.append("\\f");
                    break;
                case '\n':
                    escaped.append("\\n");
                    break;
                case '\r':
                    escaped.append("\\r");
                    break;
                case '\t':
                    escaped.append("\\t");
                    break;
                default:
                    if (static_cast<unsigned char>(c) < 0x20) {
                        char buffer[7];
                        std::snprintf(buffer, sizeof(buffer), "\\u%04x", static_cast<unsigned char>(c));
                        escaped.append(buffer);
                    } else {
                        escaped.push_back(c);
                    }
                    break;
            }
        }
        escaped.push_back('"');
        return escaped;
    }
} // namespace graft
