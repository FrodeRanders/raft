#include <boost/asio.hpp>
#include <cstdint>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>

#include "raft.pb.h"
#include "raftcpp/raft_client.hpp"
#include "raftcpp/rpc_handler.hpp"
#include "raftcpp/raft_server.hpp"

namespace {

void print_usage() {
    std::cerr
        << "Usage:\n"
        << "  raft_cpp_smoke cluster-summary <host> <port> [peer-id]\n"
        << "  raft_cpp_smoke telemetry <host> <port> [peer-id]\n"
        << "  raft_cpp_smoke vote-request <host> <port> <candidate-id> <last-log-index> <last-log-term> [term]\n"
        << "  raft_cpp_smoke append-entries <host> <port> <leader-id> <prev-log-index> <prev-log-term> <leader-commit> [term]\n"
        << "  raft_cpp_smoke install-snapshot <host> <port> <leader-id> <last-included-index> <last-included-term> [term]\n"
        << "  raft_cpp_smoke serve <bind-host> <port> <peer-id> [current-term]\n"
        << "  raft_cpp_smoke serve-stateful <bind-host> <port> <peer-id> [current-term] [last-log-index] [last-log-term]\n";
}

std::uint16_t parse_port(const std::string& text) {
    const auto value = std::stoul(text);
    if (value > 65535u) {
        throw std::runtime_error("port out of range");
    }
    return static_cast<std::uint16_t>(value);
}

std::int64_t parse_int64(const std::string& text, const std::string& field_name) {
    const auto value = std::stoll(text);
    if (value < std::numeric_limits<std::int64_t>::min() || value > std::numeric_limits<std::int64_t>::max()) {
        throw std::runtime_error(field_name + " out of range");
    }
    return static_cast<std::int64_t>(value);
}

std::string peer_id_or_default(int argc, char** argv, int index) {
    if (argc > index) {
        return argv[index];
    }
    return "cpp-cli";
}

int run_cluster_summary(const std::string& host, std::uint16_t port, const std::string& peer_id) {
    boost::asio::io_context io_context;
    raftcpp::RaftClient client(io_context);

    raft::ClusterSummaryRequest request;
    request.set_term(0);
    request.set_peer_id(peer_id);

    const auto response = client.call<raft::ClusterSummaryRequest, raft::ClusterSummaryResponse>(
        host,
        port,
        "ClusterSummaryRequest",
        request,
        "ClusterSummaryResponse"
    );

    std::cout
        << "peer_id: " << response.peer_id() << '\n'
        << "status: " << response.status() << '\n'
        << "success: " << (response.success() ? "true" : "false") << '\n'
        << "leader_id: " << response.leader_id() << '\n'
        << "state: " << response.state() << '\n'
        << "cluster_health: " << response.cluster_health() << '\n'
        << "cluster_status_reason: " << response.cluster_status_reason() << '\n'
        << "quorum_available: " << (response.quorum_available() ? "true" : "false") << '\n'
        << "joint_consensus: " << (response.joint_consensus() ? "true" : "false") << '\n'
        << "members: " << response.members_size() << '\n';

    return response.success() ? 0 : 2;
}

int run_telemetry(const std::string& host, std::uint16_t port, const std::string& peer_id) {
    boost::asio::io_context io_context;
    raftcpp::RaftClient client(io_context);

    raft::TelemetryRequest request;
    request.set_term(0);
    request.set_peer_id(peer_id);
    request.set_include_peer_stats(true);
    request.set_require_leader_summary(true);

    const auto response = client.call<raft::TelemetryRequest, raft::TelemetryResponse>(
        host,
        port,
        "TelemetryRequest",
        request,
        "TelemetryResponse"
    );

    std::cout
        << "peer_id: " << response.peer_id() << '\n'
        << "status: " << response.status() << '\n'
        << "success: " << (response.success() ? "true" : "false") << '\n'
        << "state: " << response.state() << '\n'
        << "leader_id: " << response.leader_id() << '\n'
        << "term: " << response.term() << '\n'
        << "commit_index: " << response.commit_index() << '\n'
        << "last_applied: " << response.last_applied() << '\n'
        << "cluster_health: " << response.cluster_health() << '\n'
        << "cluster_status_reason: " << response.cluster_status_reason() << '\n'
        << "quorum_available: " << (response.quorum_available() ? "true" : "false") << '\n';

    return response.success() ? 0 : 2;
}

int run_vote_request(
    const std::string& host,
    std::uint16_t port,
    const std::string& candidate_id,
    std::int64_t last_log_index,
    std::int64_t last_log_term,
    std::int64_t term
) {
    boost::asio::io_context io_context;
    raftcpp::RaftClient client(io_context);

    raft::VoteRequest request;
    request.set_term(term);
    request.set_candidate_id(candidate_id);
    request.set_last_log_index(last_log_index);
    request.set_last_log_term(last_log_term);

    const auto response = client.call<raft::VoteRequest, raft::VoteResponse>(
        host,
        port,
        "VoteRequest",
        request,
        "VoteResponse"
    );

    std::cout
        << "peer_id: " << response.peer_id() << '\n'
        << "term: " << response.term() << '\n'
        << "current_term: " << response.current_term() << '\n'
        << "vote_granted: " << (response.vote_granted() ? "true" : "false") << '\n';

    return 0;
}

int run_append_entries(
    const std::string& host,
    std::uint16_t port,
    const std::string& leader_id,
    std::int64_t prev_log_index,
    std::int64_t prev_log_term,
    std::int64_t leader_commit,
    std::int64_t term
) {
    boost::asio::io_context io_context;
    raftcpp::RaftClient client(io_context);

    raft::AppendEntriesRequest request;
    request.set_term(term);
    request.set_leader_id(leader_id);
    request.set_prev_log_index(prev_log_index);
    request.set_prev_log_term(prev_log_term);
    request.set_leader_commit(leader_commit);

    const auto response = client.call<raft::AppendEntriesRequest, raft::AppendEntriesResponse>(
        host,
        port,
        "AppendEntriesRequest",
        request,
        "AppendEntriesResponse"
    );

    std::cout
        << "peer_id: " << response.peer_id() << '\n'
        << "term: " << response.term() << '\n'
        << "success: " << (response.success() ? "true" : "false") << '\n'
        << "match_index: " << response.match_index() << '\n';

    return 0;
}

int run_install_snapshot(
    const std::string& host,
    std::uint16_t port,
    const std::string& leader_id,
    std::int64_t last_included_index,
    std::int64_t last_included_term,
    std::int64_t term
) {
    boost::asio::io_context io_context;
    raftcpp::RaftClient client(io_context);

    raft::InstallSnapshotRequest request;
    request.set_term(term);
    request.set_leader_id(leader_id);
    request.set_last_included_index(last_included_index);
    request.set_last_included_term(last_included_term);
    request.set_offset(0);
    request.set_done(true);
    request.set_snapshot_data(std::string{});

    const auto response = client.call<raft::InstallSnapshotRequest, raft::InstallSnapshotResponse>(
        host,
        port,
        "InstallSnapshotRequest",
        request,
        "InstallSnapshotResponse"
    );

    std::cout
        << "peer_id: " << response.peer_id() << '\n'
        << "term: " << response.term() << '\n'
        << "success: " << (response.success() ? "true" : "false") << '\n'
        << "last_included_index: " << response.last_included_index() << '\n';

    return 0;
}

[[noreturn]] void run_server(
    const std::string& bind_host,
    std::uint16_t port,
    const std::string& peer_id,
    std::int64_t current_term
) {
    boost::asio::io_context io_context;
    auto handler = std::make_shared<raftcpp::StubRpcHandler>(peer_id, current_term);
    raftcpp::RaftServer server(io_context, bind_host, port, std::move(handler));
    server.serve_forever();
}

[[noreturn]] void run_stateful_server(
    const std::string& bind_host,
    std::uint16_t port,
    const std::string& peer_id,
    std::int64_t current_term,
    std::int64_t last_log_index,
    std::int64_t last_log_term
) {
    boost::asio::io_context io_context;
    auto handler = std::make_shared<raftcpp::InMemoryRpcHandler>(peer_id, current_term, last_log_index, last_log_term);
    raftcpp::RaftServer server(io_context, bind_host, port, std::move(handler));
    server.serve_forever();
}

} // namespace

int main(int argc, char** argv) {
    GOOGLE_PROTOBUF_VERIFY_VERSION;

    try {
        if (argc < 4) {
            print_usage();
            return 1;
        }

        const std::string command = argv[1];
        const std::string host = argv[2];
        const auto port = parse_port(argv[3]);

        int exit_code = 1;
        if (command == "cluster-summary") {
            exit_code = run_cluster_summary(host, port, peer_id_or_default(argc, argv, 4));
        } else if (command == "telemetry") {
            exit_code = run_telemetry(host, port, peer_id_or_default(argc, argv, 4));
        } else if (command == "vote-request") {
            if (argc < 7) {
                print_usage();
                google::protobuf::ShutdownProtobufLibrary();
                return 1;
            }
            exit_code = run_vote_request(
                host,
                port,
                argv[4],
                parse_int64(argv[5], "last-log-index"),
                parse_int64(argv[6], "last-log-term"),
                argc > 7 ? parse_int64(argv[7], "term") : 0
            );
        } else if (command == "append-entries") {
            if (argc < 8) {
                print_usage();
                google::protobuf::ShutdownProtobufLibrary();
                return 1;
            }
            exit_code = run_append_entries(
                host,
                port,
                argv[4],
                parse_int64(argv[5], "prev-log-index"),
                parse_int64(argv[6], "prev-log-term"),
                parse_int64(argv[7], "leader-commit"),
                argc > 8 ? parse_int64(argv[8], "term") : 0
            );
        } else if (command == "install-snapshot") {
            if (argc < 7) {
                print_usage();
                google::protobuf::ShutdownProtobufLibrary();
                return 1;
            }
            exit_code = run_install_snapshot(
                host,
                port,
                argv[4],
                parse_int64(argv[5], "last-included-index"),
                parse_int64(argv[6], "last-included-term"),
                argc > 7 ? parse_int64(argv[7], "term") : 0
            );
        } else if (command == "serve") {
            if (argc < 5) {
                print_usage();
                google::protobuf::ShutdownProtobufLibrary();
                return 1;
            }
            run_server(
                host,
                port,
                argv[4],
                argc > 5 ? parse_int64(argv[5], "current-term") : 0
            );
        } else if (command == "serve-stateful") {
            if (argc < 5) {
                print_usage();
                google::protobuf::ShutdownProtobufLibrary();
                return 1;
            }
            run_stateful_server(
                host,
                port,
                argv[4],
                argc > 5 ? parse_int64(argv[5], "current-term") : 0,
                argc > 6 ? parse_int64(argv[6], "last-log-index") : 0,
                argc > 7 ? parse_int64(argv[7], "last-log-term") : 0
            );
        } else {
            print_usage();
        }

        google::protobuf::ShutdownProtobufLibrary();
        return exit_code;
    } catch (const std::exception& e) {
        std::cerr << "raft_cpp_smoke error: " << e.what() << '\n';
        google::protobuf::ShutdownProtobufLibrary();
        return 1;
    }
}
