#pragma once

#include <boost/asio.hpp>
#include <cstdint>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <string>

#include "raft.pb.h"
#include "raftcpp/envelope_codec.hpp"
#include "raftcpp/rpc_handler.hpp"

namespace raftcpp {

class RaftServer {
public:
    RaftServer(boost::asio::io_context& io_context, std::string bind_host, std::uint16_t port, RpcHandlerPtr handler)
        : io_context_(io_context),
          endpoint_(boost::asio::ip::make_address(bind_host), port),
          acceptor_(io_context_, endpoint_),
          handler_(std::move(handler)) {
        if (!handler_) {
            throw std::runtime_error("raft server requires a handler");
        }
    }

    [[noreturn]] void serve_forever() {
        std::cout
            << "raft_cpp_smoke server listening on "
            << endpoint_.address().to_string() << ":" << endpoint_.port()
            << '\n';

        for (;;) {
            boost::asio::ip::tcp::socket socket(io_context_);
            acceptor_.accept(socket);
            serve_connection(socket);
        }
    }

private:
    void serve_connection(boost::asio::ip::tcp::socket& socket) {
        for (;;) {
            if (!socket.is_open()) {
                return;
            }

            raft::Envelope request_envelope;
            try {
                request_envelope = read_envelope(socket);
            } catch (const std::exception&) {
                return;
            }

            const auto response = dispatch(request_envelope);
            if (response.has_value()) {
                write_envelope(socket, *response);
            }
        }
    }

    std::optional<raft::Envelope> dispatch(const raft::Envelope& request_envelope) {
        std::cout
            << "received type=" << request_envelope.type()
            << " correlation_id=" << request_envelope.correlation_id()
            << '\n';

        if (request_envelope.type() == "VoteRequest") {
            raft::VoteRequest request;
            if (!request.ParseFromString(request_envelope.payload())) {
                throw std::runtime_error("failed to parse VoteRequest");
            }

            if (auto response = handler_->on_vote_request(request); response.has_value()) {
                return wrap(request_envelope, "VoteResponse", *response);
            }
            return std::nullopt;
        }

        if (request_envelope.type() == "AppendEntriesRequest") {
            raft::AppendEntriesRequest request;
            if (!request.ParseFromString(request_envelope.payload())) {
                throw std::runtime_error("failed to parse AppendEntriesRequest");
            }

            if (auto response = handler_->on_append_entries_request(request); response.has_value()) {
                return wrap(request_envelope, "AppendEntriesResponse", *response);
            }
            return std::nullopt;
        }

        if (request_envelope.type() == "InstallSnapshotRequest") {
            raft::InstallSnapshotRequest request;
            if (!request.ParseFromString(request_envelope.payload())) {
                throw std::runtime_error("failed to parse InstallSnapshotRequest");
            }

            if (auto response = handler_->on_install_snapshot_request(request); response.has_value()) {
                return wrap(request_envelope, "InstallSnapshotResponse", *response);
            }
            return std::nullopt;
        }

        std::cout << "ignoring unsupported inbound type=" << request_envelope.type() << '\n';
        return std::nullopt;
    }

    template <typename Response>
    static raft::Envelope wrap(const raft::Envelope& request_envelope, const std::string& response_type, const Response& response) {
        raft::Envelope envelope;
        envelope.set_correlation_id(request_envelope.correlation_id());
        envelope.set_type(response_type);
        if (!response.SerializeToString(envelope.mutable_payload())) {
            throw std::runtime_error("failed to serialize response payload");
        }
        return envelope;
    }

    boost::asio::io_context& io_context_;
    boost::asio::ip::tcp::endpoint endpoint_;
    boost::asio::ip::tcp::acceptor acceptor_;
    RpcHandlerPtr handler_;
};

} // namespace raftcpp
