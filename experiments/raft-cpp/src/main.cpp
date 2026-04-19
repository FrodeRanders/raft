#include <boost/asio.hpp>
#include <cstdint>
#include <chrono>
#include <functional>
#include <iostream>
#include <memory>
#include <random>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "raft.pb.h"
#include "raftcpp/raft_client.hpp"
#include "raftcpp/rpc_handler.hpp"
#include "raftcpp/raft_runtime.hpp"
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
        << "  raft_cpp_smoke serve-stateful <bind-host> <port> <peer-id> [current-term] [last-log-index] [last-log-term]\n"
        << "  raft_cpp_smoke serve-active <bind-host> <port> <peer-id> <current-term> <last-log-index> <last-log-term> <peer-spec>...\n"
        << "  raft_cpp_smoke election-round <peer-id> [current-term] [last-log-index] [last-log-term] <peer-spec>...\n"
        << "  raft_cpp_smoke heartbeat-round <peer-id> <term> [last-log-index] [last-log-term] <peer-spec>...\n"
        << "  raft_cpp_smoke replicate-once <peer-id> <term> <data> [last-log-index] [last-log-term] <peer-spec>...\n"
        << "\n"
        << "  peer-spec format: <peer-id>@<host>:<port>\n";
}

std::uint16_t parse_port(const std::string& text) {
    const auto value = std::stoul(text);
    if (value > 65535u) {
        throw std::runtime_error("port out of range");
    }
    return static_cast<std::uint16_t>(value);
}

std::int64_t parse_int64(const std::string& text, const std::string&) {
    const auto value = std::stoll(text);
    return static_cast<std::int64_t>(value);
}

std::vector<raftcpp::PeerEndpoint> parse_peer_specs(char** argv, int start_index, int argc) {
    std::vector<raftcpp::PeerEndpoint> peers;
    for (int i = start_index; i < argc; ++i) {
        const std::string spec = argv[i];
        const auto at = spec.find('@');
        const auto colon = spec.rfind(':');
        if (at == std::string::npos || colon == std::string::npos || at == 0 || colon <= at + 1 || colon == spec.size() - 1) {
            throw std::runtime_error("invalid peer spec: " + spec);
        }

        peers.push_back(raftcpp::PeerEndpoint{
            .peer_id = spec.substr(0, at),
            .host = spec.substr(at + 1, colon - at - 1),
            .port = parse_port(spec.substr(colon + 1)),
        });
    }
    if (peers.empty()) {
        throw std::runtime_error("at least one peer spec is required");
    }
    return peers;
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

[[noreturn]] void run_active_server(
    const std::string& bind_host,
    std::uint16_t port,
    const std::string& peer_id,
    std::int64_t current_term,
    std::int64_t last_log_index,
    std::int64_t last_log_term,
    std::vector<raftcpp::PeerEndpoint> peers
) {
    auto node = std::make_shared<raftcpp::RaftNode>(raftcpp::RaftNode::Config{
        .peer_id = peer_id,
        .current_term = current_term,
        .last_log_index = last_log_index,
        .last_log_term = last_log_term,
        .commit_index = 0,
        .snapshot_index = 0,
        .snapshot_term = 0,
        .voting_peers = {},
    });

    boost::asio::io_context server_io_context;
    boost::asio::io_context client_io_context;
    auto handler = std::make_shared<raftcpp::InMemoryRpcHandler>(node);
    raftcpp::RaftRuntime runtime(client_io_context, node, std::move(peers));
    raftcpp::RaftServer server(server_io_context, bind_host, port, std::move(handler));

    std::jthread server_thread([&server]() {
        server.serve_forever();
    });

    constexpr auto heartbeat_interval = std::chrono::milliseconds(750);
    constexpr auto election_timeout_min = std::chrono::milliseconds(1500);
    constexpr auto election_timeout_max = std::chrono::milliseconds(3000);

    std::mt19937 rng{std::random_device{}()};
    std::uniform_int_distribution<int> election_jitter_ms(
        static_cast<int>(election_timeout_min.count()),
        static_cast<int>(election_timeout_max.count())
    );
    auto next_election_timeout = [&]() {
        return std::chrono::milliseconds(election_jitter_ms(rng));
    };
    auto heartbeat_timer = std::make_shared<boost::asio::steady_timer>(client_io_context);
    auto election_timer = std::make_shared<boost::asio::steady_timer>(client_io_context);

    std::function<void()> schedule_heartbeat;
    std::function<void(std::chrono::milliseconds)> schedule_election;

    schedule_heartbeat = [&]() {
        heartbeat_timer->expires_after(heartbeat_interval);
        heartbeat_timer->async_wait([&, heartbeat_timer](const boost::system::error_code& error) {
            if (error) {
                return;
            }
            if (runtime.node().role() == raftcpp::RaftNode::Role::leader) {
                runtime.send_heartbeats_once();
            }
            schedule_heartbeat();
        });
    };

    schedule_election = [&](std::chrono::milliseconds timeout) {
        election_timer->expires_after(timeout);
        election_timer->async_wait([&, timeout, election_timer](const boost::system::error_code& error) {
            if (error) {
                return;
            }

            if (runtime.node().role() == raftcpp::RaftNode::Role::leader) {
                schedule_election(next_election_timeout());
                return;
            }

            const auto idle_for = std::chrono::steady_clock::now() - runtime.node().last_activity();
            if (idle_for < timeout) {
                const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(timeout - idle_for);
                schedule_election(std::max<std::chrono::milliseconds>(std::chrono::milliseconds(1), remaining));
                return;
            }

            const auto became_leader = runtime.run_election_round();
            if (became_leader) {
                heartbeat_timer->cancel();
                heartbeat_timer->expires_after(std::chrono::milliseconds(0));
                heartbeat_timer->async_wait([&, heartbeat_timer](const boost::system::error_code& timer_error) {
                    if (timer_error) {
                        return;
                    }
                    if (runtime.node().role() == raftcpp::RaftNode::Role::leader) {
                        runtime.send_heartbeats_once();
                    }
                    schedule_heartbeat();
                });
            }

            schedule_election(next_election_timeout());
        });
    };

    schedule_heartbeat();
    schedule_election(next_election_timeout());
    client_io_context.run();

    for (;;) {
        std::this_thread::sleep_for(std::chrono::hours(24));
    }
}

int run_election_round(
    const std::string& peer_id,
    std::int64_t current_term,
    std::int64_t last_log_index,
    std::int64_t last_log_term,
    std::vector<raftcpp::PeerEndpoint> peers
) {
    boost::asio::io_context io_context;
    raftcpp::RaftRuntime runtime(
        io_context,
        raftcpp::RaftNode::Config{
            .peer_id = peer_id,
            .current_term = current_term,
            .last_log_index = last_log_index,
            .last_log_term = last_log_term,
            .commit_index = 0,
            .snapshot_index = 0,
            .snapshot_term = 0,
            .voting_peers = {},
        },
        std::move(peers)
    );

    const auto became_leader = runtime.run_election_round();
    std::cout
        << "role: " << (runtime.node().role() == raftcpp::RaftNode::Role::leader ? "leader" : "candidate-or-follower") << '\n'
        << "granted_votes: " << runtime.node().granted_votes() << '\n'
        << "quorum: " << runtime.node().quorum_size() << '\n';
    return became_leader ? 0 : 2;
}

int run_heartbeat_round(
    const std::string& peer_id,
    std::int64_t term,
    std::int64_t last_log_index,
    std::int64_t last_log_term,
    std::vector<raftcpp::PeerEndpoint> peers
) {
    boost::asio::io_context io_context;
    raftcpp::RaftRuntime runtime(
        io_context,
        raftcpp::RaftNode::Config{
            .peer_id = peer_id,
            .current_term = term,
            .last_log_index = last_log_index,
            .last_log_term = last_log_term,
            .commit_index = last_log_index,
            .snapshot_index = 0,
            .snapshot_term = 0,
            .voting_peers = {},
        },
        std::move(peers)
    );
    runtime.node().become_leader();

    const auto successes = runtime.send_heartbeats_once();
    std::cout
        << "role: " << (runtime.node().role() == raftcpp::RaftNode::Role::leader ? "leader" : "not-leader") << '\n'
        << "heartbeat_successes: " << successes << '\n';
    return successes > 0 ? 0 : 2;
}

int run_replicate_once(
    const std::string& peer_id,
    std::int64_t term,
    const std::string& data,
    std::int64_t last_log_index,
    std::int64_t last_log_term,
    std::vector<raftcpp::PeerEndpoint> peers
) {
    boost::asio::io_context io_context;
    raftcpp::RaftRuntime runtime(
        io_context,
        raftcpp::RaftNode::Config{
            .peer_id = peer_id,
            .current_term = term,
            .last_log_index = last_log_index,
            .last_log_term = last_log_term,
            .commit_index = last_log_index,
            .snapshot_index = 0,
            .snapshot_term = 0,
            .voting_peers = {},
        },
        std::move(peers)
    );
    runtime.node().become_leader();

    const auto successes = runtime.replicate_entry_once(data);
    std::cout
        << "role: " << (runtime.node().role() == raftcpp::RaftNode::Role::leader ? "leader" : "not-leader") << '\n'
        << "replication_successes: " << successes << '\n'
        << "last_log_index: " << runtime.node().last_log_index() << '\n';
    return successes > 0 ? 0 : 2;
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
        } else if (command == "serve-active") {
            if (argc < 9) {
                print_usage();
                google::protobuf::ShutdownProtobufLibrary();
                return 1;
            }
            run_active_server(
                host,
                port,
                argv[4],
                parse_int64(argv[5], "current-term"),
                parse_int64(argv[6], "last-log-index"),
                parse_int64(argv[7], "last-log-term"),
                parse_peer_specs(argv, 8, argc)
            );
        } else if (command == "election-round") {
            if (argc < 7) {
                print_usage();
                google::protobuf::ShutdownProtobufLibrary();
                return 1;
            }
            exit_code = run_election_round(
                argv[2],
                parse_int64(argv[3], "current-term"),
                parse_int64(argv[4], "last-log-index"),
                parse_int64(argv[5], "last-log-term"),
                parse_peer_specs(argv, 6, argc)
            );
        } else if (command == "heartbeat-round") {
            if (argc < 7) {
                print_usage();
                google::protobuf::ShutdownProtobufLibrary();
                return 1;
            }
            exit_code = run_heartbeat_round(
                argv[2],
                parse_int64(argv[3], "term"),
                parse_int64(argv[4], "last-log-index"),
                parse_int64(argv[5], "last-log-term"),
                parse_peer_specs(argv, 6, argc)
            );
        } else if (command == "replicate-once") {
            if (argc < 7) {
                print_usage();
                google::protobuf::ShutdownProtobufLibrary();
                return 1;
            }
            exit_code = run_replicate_once(
                argv[2],
                parse_int64(argv[3], "term"),
                argv[4],
                parse_int64(argv[5], "last-log-index"),
                parse_int64(argv[6], "last-log-term"),
                parse_peer_specs(argv, 7, argc)
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
