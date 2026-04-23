#include "raftcpp/raft_server.hpp"

#include <array>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <utility>
#include <vector>

#include "raft.pb.h"
#include "raftcpp/envelope_codec.hpp"

namespace raftcpp {

class RaftServer::Connection final : public std::enable_shared_from_this<Connection> {
public:
    Connection(boost::asio::ip::tcp::socket socket, RpcHandlerPtr handler)
        : socket_(std::move(socket)),
          handler_(std::move(handler)) {
    }

    void start() {
        read_length_byte();
    }

private:
    void read_length_byte() {
        auto self = shared_from_this();
        boost::asio::async_read(socket_, boost::asio::buffer(length_byte_), [self](const boost::system::error_code& error, std::size_t) {
            if (error) {
                self->close();
                return;
            }
            self->on_length_byte(self->length_byte_[0]);
        });
    }

    void on_length_byte(std::uint8_t byte) {
        frame_length_ |= static_cast<std::uint32_t>(byte & 0x7Fu) << length_shift_;
        if ((byte & 0x80u) == 0) {
            read_payload();
            return;
        }

        length_shift_ += 7;
        if (length_shift_ >= 35) {
            throw std::runtime_error("malformed varint32 length prefix");
        }
        read_length_byte();
    }

    void read_payload() {
        payload_.assign(frame_length_, 0);
        auto self = shared_from_this();
        boost::asio::async_read(socket_, boost::asio::buffer(payload_), [self](const boost::system::error_code& error, std::size_t) {
            if (error) {
                self->close();
                return;
            }
            self->on_payload();
        });
    }

    void on_payload() {
        raft::Envelope request_envelope;
        if (!request_envelope.ParseFromArray(payload_.data(), static_cast<int>(payload_.size()))) {
            throw std::runtime_error("failed to parse envelope");
        }

        reset_frame_state();
        const auto response = dispatch(request_envelope);
        if (!response.has_value()) {
            read_length_byte();
            return;
        }

        write_buffer_ = encode_envelope(*response);
        auto self = shared_from_this();
        boost::asio::async_write(socket_, boost::asio::buffer(write_buffer_), [self](const boost::system::error_code& error, std::size_t) {
            if (error) {
                self->close();
                return;
            }
            self->write_buffer_.clear();
            self->read_length_byte();
        });
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

        if (request_envelope.type() == "TelemetryRequest") {
            raft::TelemetryRequest request;
            if (!request.ParseFromString(request_envelope.payload())) {
                throw std::runtime_error("failed to parse TelemetryRequest");
            }

            if (auto response = handler_->on_telemetry_request(request); response.has_value()) {
                return wrap(request_envelope, "TelemetryResponse", *response);
            }
            return std::nullopt;
        }

        if (request_envelope.type() == "ClusterSummaryRequest") {
            raft::ClusterSummaryRequest request;
            if (!request.ParseFromString(request_envelope.payload())) {
                throw std::runtime_error("failed to parse ClusterSummaryRequest");
            }

            if (auto response = handler_->on_cluster_summary_request(request); response.has_value()) {
                return wrap(request_envelope, "ClusterSummaryResponse", *response);
            }
            return std::nullopt;
        }

        if (request_envelope.type() == "ClientCommandRequest") {
            raft::ClientCommandRequest request;
            if (!request.ParseFromString(request_envelope.payload())) {
                throw std::runtime_error("failed to parse ClientCommandRequest");
            }

            if (auto response = handler_->on_client_command_request(request); response.has_value()) {
                return wrap(request_envelope, "ClientCommandResponse", *response);
            }
            return std::nullopt;
        }

        if (request_envelope.type() == "ClientQueryRequest") {
            raft::ClientQueryRequest request;
            if (!request.ParseFromString(request_envelope.payload())) {
                throw std::runtime_error("failed to parse ClientQueryRequest");
            }

            if (auto response = handler_->on_client_query_request(request); response.has_value()) {
                return wrap(request_envelope, "ClientQueryResponse", *response);
            }
            return std::nullopt;
        }

        if (request_envelope.type() == "JoinClusterRequest") {
            raft::JoinClusterRequest request;
            if (!request.ParseFromString(request_envelope.payload())) {
                throw std::runtime_error("failed to parse JoinClusterRequest");
            }

            if (auto response = handler_->on_join_cluster_request(request); response.has_value()) {
                return wrap(request_envelope, "JoinClusterResponse", *response);
            }
            return std::nullopt;
        }

        if (request_envelope.type() == "JoinClusterStatusRequest") {
            raft::JoinClusterStatusRequest request;
            if (!request.ParseFromString(request_envelope.payload())) {
                throw std::runtime_error("failed to parse JoinClusterStatusRequest");
            }

            if (auto response = handler_->on_join_cluster_status_request(request); response.has_value()) {
                return wrap(request_envelope, "JoinClusterStatusResponse", *response);
            }
            return std::nullopt;
        }

        if (request_envelope.type() == "ReconfigureClusterRequest") {
            raft::ReconfigureClusterRequest request;
            if (!request.ParseFromString(request_envelope.payload())) {
                throw std::runtime_error("failed to parse ReconfigureClusterRequest");
            }

            if (auto response = handler_->on_reconfigure_cluster_request(request); response.has_value()) {
                return wrap(request_envelope, "ReconfigureClusterResponse", *response);
            }
            return std::nullopt;
        }

        if (request_envelope.type() == "AppendEntriesRequest") {
            raft::AppendEntriesRequest request;
            if (!request.ParseFromString(request_envelope.payload())) {
                throw std::runtime_error("failed to parse AppendEntriesRequest");
            }
            std::cout
                << "append leader_commit=" << request.leader_commit()
                << " prev_log_index=" << request.prev_log_index()
                << " entries=" << request.entries_size()
                << '\n';

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

    void reset_frame_state() {
        frame_length_ = 0;
        length_shift_ = 0;
        payload_.clear();
    }

    void close() {
        boost::system::error_code ignored;
        socket_.shutdown(boost::asio::ip::tcp::socket::shutdown_both, ignored);
        socket_.close(ignored);
    }

    boost::asio::ip::tcp::socket socket_;
    RpcHandlerPtr handler_;
    std::array<std::uint8_t, 1> length_byte_{};
    std::uint32_t frame_length_{0};
    int length_shift_{0};
    std::vector<std::uint8_t> payload_;
    std::vector<std::uint8_t> write_buffer_;
};

RaftServer::RaftServer(boost::asio::io_context& io_context, std::string bind_host, std::uint16_t port, RpcHandlerPtr handler)
    : io_context_(io_context),
      endpoint_(boost::asio::ip::make_address(bind_host), port),
      acceptor_(io_context_, endpoint_),
      handler_(std::move(handler)) {
    if (!handler_) {
        throw std::runtime_error("raft server requires a handler");
    }
}

void RaftServer::start() {
    std::cout
        << "raft_cpp_smoke server listening on "
        << endpoint_.address().to_string() << ":" << endpoint_.port()
        << '\n';
    start_accept();
}

[[noreturn]] void RaftServer::serve_forever() {
    start();
    io_context_.run();
    throw std::runtime_error("raft server io_context stopped unexpectedly");
}

void RaftServer::start_accept() {
    acceptor_.async_accept([this](const boost::system::error_code& error, boost::asio::ip::tcp::socket socket) {
        if (!error) {
            std::make_shared<Connection>(std::move(socket), handler_)->start();
        }
        start_accept();
    });
}

} // namespace raftcpp
