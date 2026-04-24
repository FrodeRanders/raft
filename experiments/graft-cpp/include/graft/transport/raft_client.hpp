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
#include <chrono>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>

#include "raft.pb.h"
#include "graft/wire/envelope_codec.hpp"

namespace graft {
    class RaftClient {
    public:
        explicit RaftClient(boost::asio::io_context &io_context)
            : io_context_(io_context) {
        }

        template<typename Request, typename Response>
        Response call(
            const std::string &host,
            std::uint16_t port,
            const std::string &type,
            const Request &request,
            const std::string &expected_response_type
        ) {
            boost::asio::ip::tcp::resolver resolver(io_context_);
            boost::asio::ip::tcp::socket socket(io_context_);

            auto endpoints = resolver.resolve(host, std::to_string(port));
            boost::asio::connect(socket, endpoints);

            raft::Envelope envelope;
            envelope.set_correlation_id(make_correlation_id());
            envelope.set_type(type);
            if (!request.SerializeToString(envelope.mutable_payload())) {
                throw std::runtime_error("failed to serialize request payload");
            }

            write_envelope(socket, envelope);

            raft::Envelope response_envelope = read_envelope(socket);
            if (response_envelope.correlation_id() != envelope.correlation_id()) {
                throw std::runtime_error("correlation id mismatch in response");
            }
            if (response_envelope.type() != expected_response_type) {
                throw std::runtime_error("unexpected response type: " + response_envelope.type());
            }

            Response response;
            if (!response.ParseFromString(response_envelope.payload())) {
                throw std::runtime_error("failed to parse response payload");
            }
            return response;
        }

    private:
        static std::string make_correlation_id() {
            static thread_local std::mt19937_64 rng{std::random_device{}()};
            const auto now = std::chrono::steady_clock::now().time_since_epoch().count();
            std::ostringstream out;
            out << "cpp-" << now << "-" << rng();
            return out.str();
        }

        boost::asio::io_context &io_context_;
    };
} // namespace graft
