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

#include <boost/asio.hpp>
#include <cstdint>
#include <stdexcept>
#include <vector>

#include "raft.pb.h"

namespace raftcpp {
    inline std::vector<std::uint8_t> encode_varint32(std::uint32_t value) {
        std::vector<std::uint8_t> out;
        while ((value & ~0x7Fu) != 0) {
            out.push_back(static_cast<std::uint8_t>((value & 0x7Fu) | 0x80u));
            value >>= 7;
        }
        out.push_back(static_cast<std::uint8_t>(value));
        return out;
    }

    template<typename SyncReadStream>
    std::uint32_t read_varint32(SyncReadStream &stream) {
        std::uint32_t result = 0;
        int shift = 0;

        while (shift < 35) {
            std::uint8_t byte = 0;
            boost::asio::read(stream, boost::asio::buffer(&byte, 1));
            result |= static_cast<std::uint32_t>(byte & 0x7Fu) << shift;
            if ((byte & 0x80u) == 0) {
                return result;
            }
            shift += 7;
        }

        throw std::runtime_error("malformed varint32 length prefix");
    }

    inline std::vector<std::uint8_t> encode_envelope(const raft::Envelope &envelope) {
        const auto payload_size = envelope.ByteSizeLong();
        if (payload_size > static_cast<std::size_t>(std::numeric_limits<std::uint32_t>::max())) {
            throw std::runtime_error("envelope too large");
        }

        std::vector<std::uint8_t> frame = encode_varint32(static_cast<std::uint32_t>(payload_size));
        const auto start = frame.size();
        frame.resize(start + payload_size);
        if (!envelope.SerializeToArray(frame.data() + start, static_cast<int>(payload_size))) {
            throw std::runtime_error("failed to serialize envelope");
        }
        return frame;
    }

    template<typename SyncWriteStream>
    void write_envelope(SyncWriteStream &stream, const raft::Envelope &envelope) {
        const auto frame = encode_envelope(envelope);
        boost::asio::write(stream, boost::asio::buffer(frame.data(), frame.size()));
    }

    template<typename SyncReadStream>
    raft::Envelope read_envelope(SyncReadStream &stream) {
        const auto length = read_varint32(stream);
        std::vector<std::uint8_t> buffer(length);
        boost::asio::read(stream, boost::asio::buffer(buffer.data(), buffer.size()));

        raft::Envelope envelope;
        if (!envelope.ParseFromArray(buffer.data(), static_cast<int>(buffer.size()))) {
            throw std::runtime_error("failed to parse envelope");
        }
        return envelope;
    }
} // namespace raftcpp
