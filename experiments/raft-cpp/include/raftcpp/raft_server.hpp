#pragma once

#include <boost/asio.hpp>
#include <cstdint>
#include <memory>
#include <string>

#include "raftcpp/rpc_handler.hpp"

namespace raftcpp {

class RaftServer {
public:
    RaftServer(boost::asio::io_context& io_context, std::string bind_host, std::uint16_t port, RpcHandlerPtr handler);

    void start();
    [[noreturn]] void serve_forever();

private:
    class Connection;

    void start_accept();

    boost::asio::io_context& io_context_;
    boost::asio::ip::tcp::endpoint endpoint_;
    boost::asio::ip::tcp::acceptor acceptor_;
    RpcHandlerPtr handler_;
};

} // namespace raftcpp
