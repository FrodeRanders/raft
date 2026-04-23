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

#include "raftcpp/rpc_handler.hpp"

namespace raftcpp {
    class RaftServer {
    public:
        RaftServer(boost::asio::io_context &io_context, std::string bind_host, std::uint16_t port,
                   RpcHandlerPtr handler);

        void start();

        [[noreturn]] void serve_forever();

    private:
        class Connection;

        void start_accept();

        boost::asio::io_context &io_context_;
        boost::asio::ip::tcp::endpoint endpoint_;
        boost::asio::ip::tcp::acceptor acceptor_;
        RpcHandlerPtr handler_;
    };
} // namespace raftcpp
