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
#include <memory>
#include <string>

#include "graft/runtime/rpc_handler.hpp"

namespace graft {
    // TCP server for the Java-compatible Raft wire protocol.
    //
    // RaftServer owns only the listening socket and connection lifecycle.  It
    // does not know about elections, logs, state machines, or domain commands;
    // every decoded request is handed to RpcHandler, which is the boundary from
    // transport concerns into Raft/runtime/domain behavior.
    class RaftServer {
    public:
        RaftServer(boost::asio::io_context &io_context, std::string bind_host, std::uint16_t port,
                   RpcHandlerPtr handler);

        // Start accepting asynchronously.  The caller still owns running the
        // io_context; tests can call start() and drive the event loop manually.
        void start();

        // Convenience for the CLI/server process: start accepting and run the
        // io_context until the process is stopped.
        [[noreturn]] void serve_forever();

    private:
        // One Connection instance represents one accepted TCP stream.  The
        // implementation reads one or more framed protobuf envelopes from that
        // stream and lets RpcHandler produce response envelopes.
        class Connection;

        void start_accept();

        boost::asio::io_context &io_context_;
        boost::asio::ip::tcp::endpoint endpoint_;
        boost::asio::ip::tcp::acceptor acceptor_;
        RpcHandlerPtr handler_;
    };
} // namespace graft
