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

#include <boost/asio.hpp>
#include <chrono>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <memory>
#include <optional>
#include <random>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <thread>
#include <vector>

#include "raft.pb.h"
#include "graft/runtime/raft_runtime.hpp"
#include "graft/runtime/rpc_handler.hpp"
#include "graft/transport/raft_client.hpp"
#include "graft/transport/raft_server.hpp"

namespace {
    // The CLI is intentionally a broad integration surface: it drives direct
    // request/response probes, passive servers, active Raft nodes, persistence
    // exercises, and mixed Java/C++ smoke tests.  The core library does not
    // depend on this file; application developers should read it as a worked
    // example of how to wire the library pieces together.
    void print_usage() {
        std::cerr
                << "Usage:\n"
                << "  graft_smoke cluster-summary <host> <port> [peer-id]\n"
                << "  graft_smoke reconfiguration-status <host> <port> [peer-id]\n"
                << "  graft_smoke telemetry <host> <port> [peer-id]\n"
                << "  graft_smoke client-put <host> <port> <key> <value> [peer-id]\n"
                << "  graft_smoke client-cas <host> <port> <key> <expected-present> <expected-value> <new-value> [peer-id]\n"
                << "  graft_smoke client-get <host> <port> <key> [peer-id]\n"
                << "  graft_smoke join-cluster <host> <port> <joining-peer-id> <join-host> <join-port> [role] [peer-id]\n"
                << "  graft_smoke join-status <host> <port> <target-peer-id> [peer-id]\n"
                << "  graft_smoke reconfigure <host> <port> <action> <peer-spec>... [peer-id]\n"
                << "  graft_smoke vote-request <host> <port> <candidate-id> <last-log-index> <last-log-term> [term]\n"
                << "  graft_smoke append-entries <host> <port> <leader-id> <prev-log-index> <prev-log-term> <leader-commit> [term]\n"
                << "  graft_smoke install-snapshot <host> <port> <leader-id> <last-included-index> <last-included-term> [term] [snapshot-data]\n"
                << "  graft_smoke serve <bind-host> <port> <peer-id> [current-term]\n"
                << "  graft_smoke serve-stateful <bind-host> <port> <peer-id> [current-term] [last-log-index] [last-log-term]\n"
                << "  graft_smoke serve-persistent <bind-host> <port> <peer-id> <state-file> [current-term] [last-log-index] [last-log-term] [peer-spec]...\n"
                << "  graft_smoke serve-active <bind-host> <port> <peer-id> <current-term> <last-log-index> <last-log-term> <peer-spec>...\n"
                << "  graft_smoke serve-active-persistent <bind-host> <port> <peer-id> <state-file> <current-term> <last-log-index> <last-log-term> <peer-spec>...\n"
                <<
                "  graft_smoke serve-active-persistent-workload <bind-host> <port> <peer-id> <state-file> <current-term> <last-log-index> <last-log-term> <replicate-interval-ms> [snapshot-threshold] <peer-spec>...\n"
                << "  graft_smoke election-round <peer-id> [current-term] [last-log-index] [last-log-term] <peer-spec>...\n"
                << "  graft_smoke heartbeat-round <peer-id> <term> [last-log-index] [last-log-term] <peer-spec>...\n"
                << "  graft_smoke replicate-once <peer-id> <term> <data> [last-log-index] [last-log-term] <peer-spec>...\n"
                << "  graft_smoke replicate-once-persistent <peer-id> <state-file> <term> <data> [last-log-index] [last-log-term] <peer-spec>...\n"
                << "  graft_smoke replicate-put-persistent <peer-id> <state-file> <term> <key> <value> [last-log-index] [last-log-term] <peer-spec>...\n"
                << "  graft_smoke compact-snapshot <peer-id> <state-file> <index> <snapshot-data> [current-term] [last-log-index] [last-log-term]\n"
                << "  graft_smoke dump-state <state-file>\n"
                << "\n"
                << "  peer-spec format: <peer-id>@<host>:<port>\n";
    }

    std::optional<std::string> env_value(const char *name) {
        const char *value = std::getenv(name);
        if (value == nullptr || *value == '\0') {
            return std::nullopt;
        }
        return std::string(value);
    }

    std::string lower_ascii(std::string value) {
        std::transform(value.begin(), value.end(), value.begin(), [](unsigned char ch) {
            return static_cast<char>(std::tolower(ch));
        });
        return value;
    }

    std::optional<graft::InMemoryRpcHandler::Authenticator> shared_secret_authenticator_from_env() {
        // Authentication is a domain/integration concern, not a Raft primitive.
        // The handler exposes a callback so an application can plug in shared
        // secrets, mTLS identity, JWT validation, or any other local policy.
        const auto mode = lower_ascii(env_value("RAFT_REQUEST_AUTH_MODE").value_or("none"));
        if (mode == "none") {
            return std::nullopt;
        }
        if (mode != "shared-secret") {
            throw std::runtime_error("unsupported RAFT_REQUEST_AUTH_MODE: " + mode);
        }
        const auto shared_secret = env_value("RAFT_REQUEST_AUTH_SHARED_SECRET");
        if (!shared_secret.has_value()) {
            throw std::runtime_error("RAFT_REQUEST_AUTH_SHARED_SECRET is required for shared-secret auth");
        }
        return graft::InMemoryRpcHandler::Authenticator{[shared_secret = *shared_secret](const std::string &scheme,
                                                                                         const std::string &token) {
            if (lower_ascii(scheme) != "shared-secret") {
                return std::optional<graft::InMemoryRpcHandler::AuthenticationFailure>{
                    graft::InMemoryRpcHandler::AuthenticationFailure{
                        .status = "UNAUTHENTICATED",
                        .message = "Client command authentication requires scheme 'shared-secret'",
                    }
                };
            }
            if (token != shared_secret) {
                return std::optional<graft::InMemoryRpcHandler::AuthenticationFailure>{
                    graft::InMemoryRpcHandler::AuthenticationFailure{
                        .status = "UNAUTHENTICATED",
                        .message = "Client command authentication failed",
                    }
                };
            }
            return std::optional<graft::InMemoryRpcHandler::AuthenticationFailure>{};
        }};
    }

    std::vector<std::string> parse_csv_env(const char *name) {
        std::vector<std::string> values;
        const auto csv = env_value(name);
        if (!csv.has_value()) {
            return values;
        }
        std::size_t start = 0;
        while (start <= csv->size()) {
            const auto comma = csv->find(',', start);
            auto value = csv->substr(start, comma == std::string::npos ? std::string::npos : comma - start);
            const auto first = value.find_first_not_of(" \t\r\n");
            if (first != std::string::npos) {
                const auto last = value.find_last_not_of(" \t\r\n");
                values.push_back(value.substr(first, last - first + 1));
            }
            if (comma == std::string::npos) {
                break;
            }
            start = comma + 1;
        }
        return values;
    }

    std::optional<graft::InMemoryRpcHandler::CommandAuthorizer> command_authorizer_from_env() {
        // Authorization is checked before a client command is appended to the
        // Raft log.  Once an entry is committed, every node must apply the same
        // bytes, so admission decisions belong at the edge of the leader.
        auto allowed = parse_csv_env("RAFT_COMMAND_AUTHORIZER_ALLOW_LIST");
        if (allowed.empty()) {
            return std::nullopt;
        }
        return graft::InMemoryRpcHandler::CommandAuthorizer{[allowed = std::move(allowed)](
                                                                const std::string &requester_id,
                                                                const std::string &) {
            if (std::find(allowed.begin(), allowed.end(), requester_id) != allowed.end()) {
                return std::optional<graft::InMemoryRpcHandler::AuthenticationFailure>{};
            }
            const auto id = requester_id.empty() ? std::string{"unknown"} : requester_id;
            return std::optional<graft::InMemoryRpcHandler::AuthenticationFailure>{
                graft::InMemoryRpcHandler::AuthenticationFailure{
                    .status = "FORBIDDEN",
                    .message = "Requester '" + id + "' is not authorized to modify cluster state",
                }
            };
        }};
    }

    bool reference_data_admission_from_env() {
        // This switch demonstrates a domain-specific admission policy.  It is
        // deliberately outside RaftNode so the consensus library can be reused
        // for applications other than reference-data distribution.
        const auto mode = lower_ascii(env_value("RAFT_ADAPTER_MODE").value_or("basic"));
        return mode == "reference-data" || mode == "reference" || mode == "refdata";
    }

    template<typename Request>
    void apply_request_auth(Request &request) {
        // Client-side helper for the demo CLI.  Real applications would normally
        // populate these fields from their own identity/session infrastructure.
        if (const auto scheme = env_value("RAFT_REQUEST_AUTH_CLIENT_SCHEME"); scheme.has_value()) {
            request.set_auth_scheme(*scheme);
        }
        if (const auto token = env_value("RAFT_REQUEST_AUTH_CLIENT_TOKEN"); token.has_value()) {
            request.set_auth_token(*token);
        }
    }

    std::uint16_t parse_port(const std::string &text) {
        const auto value = std::stoul(text);
        if (value > 65535u) {
            throw std::runtime_error("port out of range");
        }
        return static_cast<std::uint16_t>(value);
    }

    std::int64_t parse_int64(const std::string &text, const std::string &) {
        const auto value = std::stoll(text);
        return static_cast<std::int64_t>(value);
    }

    std::optional<std::int32_t> parse_int32_env(const char *name) {
        const auto value = env_value(name);
        if (!value.has_value()) {
            return std::nullopt;
        }
        return static_cast<std::int32_t>(std::stol(*value));
    }

    std::optional<std::int64_t> parse_int64_env(const char *name) {
        const auto value = env_value(name);
        if (!value.has_value()) {
            return std::nullopt;
        }
        return static_cast<std::int64_t>(std::stoll(*value));
    }

    std::chrono::milliseconds linearizable_read_lease_from_env() {
        const auto value = parse_int64_env("RAFT_LINEARIZABLE_READ_LEASE_MILLIS").value_or(750);
        return std::chrono::milliseconds(std::max<std::int64_t>(1, value));
    }

    std::chrono::milliseconds linearizable_read_timeout_from_env() {
        const auto value = parse_int64_env("RAFT_LINEARIZABLE_READ_TIMEOUT_MILLIS").value_or(500);
        return std::chrono::milliseconds(std::max<std::int64_t>(1, value));
    }

    bool parse_bool(const std::string &text, const std::string &field_name) {
        if (text == "true" || text == "1") {
            return true;
        }
        if (text == "false" || text == "0") {
            return false;
        }
        throw std::runtime_error("invalid boolean for " + field_name + ": " + text);
    }

    std::string normalize_peer_role(std::string role) {
        std::transform(role.begin(), role.end(), role.begin(), [](unsigned char ch) {
            return static_cast<char>(std::toupper(ch));
        });
        return role;
    }

    std::vector<graft::PeerEndpoint> parse_peer_specs(char **argv, int start_index, int argc) {
        // peer-id@host:port[/ROLE] is the compact notation used by smoke tests.
        // ROLE is part of the cluster configuration model; learners are tracked
        // and replicated to, but are excluded from quorum calculations.
        std::vector<graft::PeerEndpoint> peers;
        for (int i = start_index; i < argc; ++i) {
            const std::string spec = argv[i];
            const auto at = spec.find('@');
            const auto colon = spec.rfind(':');
            const auto slash = spec.find('/', colon == std::string::npos ? 0 : colon);
            if (at == std::string::npos || colon == std::string::npos || at == 0 || colon <= at + 1 || colon == spec.
                size() - 1 || (slash != std::string::npos && slash == spec.size() - 1)) {
                throw std::runtime_error("invalid peer spec: " + spec);
            }

            peers.push_back(graft::PeerEndpoint{
                .peer_id = spec.substr(0, at),
                .host = spec.substr(at + 1, colon - at - 1),
                .port = parse_port(spec.substr(colon + 1, slash == std::string::npos
                                                            ? std::string::npos
                                                            : slash - colon - 1)),
                .role = slash == std::string::npos ? "VOTER" : normalize_peer_role(spec.substr(slash + 1)),
            });
        }
        if (peers.empty()) {
            throw std::runtime_error("at least one peer spec is required");
        }
        return peers;
    }

    std::vector<graft::InMemoryRpcHandler::Endpoint> endpoints_from_peers(
        const std::vector<graft::PeerEndpoint> &peers) {
        std::vector<graft::InMemoryRpcHandler::Endpoint> endpoints;
        endpoints.reserve(peers.size());
        for (const auto &peer: peers) {
            endpoints.push_back({peer.host, static_cast<std::int32_t>(peer.port)});
        }
        return endpoints;
    }

    std::vector<std::string> peer_ids_from_peers(const std::vector<graft::PeerEndpoint> &peers) {
        std::vector<std::string> peer_ids;
        peer_ids.reserve(peers.size());
        for (const auto &peer: peers) {
            peer_ids.push_back(peer.peer_id);
        }
        return peer_ids;
    }

    template<typename HandlerPtr>
    void configure_handler_endpoints(
        const HandlerPtr &handler,
        const std::string &bind_host,
        std::uint16_t port,
        const std::vector<graft::PeerEndpoint> &peers = {}
    ) {
        // Operational APIs such as redirects, summaries, and join forwarding
        // need network endpoints in addition to Raft peer ids.  RaftNode itself
        // only reasons about ids and log state.
        if constexpr (std::is_same_v<typename HandlerPtr::element_type, graft::InMemoryRpcHandler>) {
            handler->set_local_endpoint(bind_host, static_cast<std::int32_t>(port));
            handler->set_known_peer_endpoints(endpoints_from_peers(peers), peer_ids_from_peers(peers));
        } else if constexpr (std::is_same_v<typename HandlerPtr::element_type, graft::PersistentRpcHandler>) {
            handler->delegate().set_local_endpoint(bind_host, static_cast<std::int32_t>(port));
            handler->delegate().set_known_peer_endpoints(endpoints_from_peers(peers), peer_ids_from_peers(peers));
        }
    }

    template<typename HandlerPtr>
    void configure_handler_auth(const HandlerPtr &handler) {
        // Shared helper keeps persistent and in-memory handler setup identical.
        // PersistentRpcHandler is a decorator around InMemoryRpcHandler, so most
        // knobs are applied to its delegate.
        const auto authenticator = shared_secret_authenticator_from_env();
        if (!authenticator.has_value()) {
            return;
        }
        if constexpr (std::is_same_v<typename HandlerPtr::element_type, graft::InMemoryRpcHandler>) {
            handler->set_authenticator(*authenticator);
        } else if constexpr (std::is_same_v<typename HandlerPtr::element_type, graft::PersistentRpcHandler>) {
            handler->delegate().set_authenticator(*authenticator);
        }
    }

    template<typename HandlerPtr>
    void configure_handler_authorization(const HandlerPtr &handler) {
        const auto authorizer = command_authorizer_from_env();
        if (!authorizer.has_value()) {
            return;
        }
        if constexpr (std::is_same_v<typename HandlerPtr::element_type, graft::InMemoryRpcHandler>) {
            handler->set_command_authorizer(*authorizer);
        } else if constexpr (std::is_same_v<typename HandlerPtr::element_type, graft::PersistentRpcHandler>) {
            handler->delegate().set_command_authorizer(*authorizer);
        }
    }

    template<typename HandlerPtr>
    void configure_handler_admission(const HandlerPtr &handler) {
        const bool reference_data_admission = reference_data_admission_from_env();
        if constexpr (std::is_same_v<typename HandlerPtr::element_type, graft::InMemoryRpcHandler>) {
            handler->set_reference_data_admission(reference_data_admission);
        } else if constexpr (std::is_same_v<typename HandlerPtr::element_type, graft::PersistentRpcHandler>) {
            handler->delegate().set_reference_data_admission(reference_data_admission);
        }
    }

    template<typename HandlerPtr>
    void configure_handler_operational_policy(const HandlerPtr &handler) {
        // These knobs affect observability and read behavior, not Raft safety:
        // telemetry rate limiting protects the operational surface, while the
        // read lease duration controls how recent leader contact must be before
        // a local read can be treated as linearizable.
        const auto limit = parse_int32_env("RAFT_TELEMETRY_RATE_LIMIT_PER_MINUTE");
        const auto stuck_millis = parse_int64_env("RAFT_TELEMETRY_RECONFIGURATION_STUCK_MILLIS");
        const auto read_lease_millis = linearizable_read_lease_from_env().count();
        if (!limit.has_value() && !stuck_millis.has_value() && read_lease_millis == 750) {
            return;
        }
        auto configure = [&](graft::InMemoryRpcHandler &target) {
            target.set_linearizable_read_lease_millis(read_lease_millis);
            if (limit.has_value()) {
                target.set_telemetry_rate_limit_per_minute(*limit);
            }
            if (stuck_millis.has_value()) {
                target.set_telemetry_reconfiguration_stuck_millis(*stuck_millis);
            }
        };
        if constexpr (std::is_same_v<typename HandlerPtr::element_type, graft::InMemoryRpcHandler>) {
            configure(*handler);
        } else if constexpr (std::is_same_v<typename HandlerPtr::element_type, graft::PersistentRpcHandler>) {
            configure(handler->delegate());
        }
    }

    bool forward_join_request(
        const graft::InMemoryRpcHandler::Endpoint &endpoint,
        const raft::JoinClusterRequest &request
    ) {
        // Followers can receive administrative requests.  Forwarding lets users
        // point tooling at any node while still ensuring that configuration
        // changes are proposed by the current leader.
        try {
            boost::asio::io_context io_context;
            graft::RaftClient client(io_context);
            (void) client.call<raft::JoinClusterRequest, raft::JoinClusterResponse>(
                endpoint.host,
                static_cast<std::uint16_t>(endpoint.port),
                "JoinClusterRequest",
                request,
                "JoinClusterResponse"
            );
            return true;
        } catch (const std::exception &) {
            return false;
        }
    }

    bool forward_reconfigure_request(
        const graft::InMemoryRpcHandler::Endpoint &endpoint,
        const raft::ReconfigureClusterRequest &request
    ) {
        try {
            boost::asio::io_context io_context;
            graft::RaftClient client(io_context);
            (void) client.call<raft::ReconfigureClusterRequest, raft::ReconfigureClusterResponse>(
                endpoint.host,
                static_cast<std::uint16_t>(endpoint.port),
                "ReconfigureClusterRequest",
                request,
                "ReconfigureClusterResponse"
            );
            return true;
        } catch (const std::exception &) {
            return false;
        }
    }

    std::string peer_id_or_default(int argc, char **argv, int index) {
        if (argc > index) {
            return argv[index];
        }
        return "cpp-cli";
    }

    int run_cluster_summary(const std::string &host, std::uint16_t port, const std::string &peer_id) {
        boost::asio::io_context io_context;
        graft::RaftClient client(io_context);

        raft::ClusterSummaryRequest request;
        request.set_term(0);
        request.set_peer_id(peer_id);
        apply_request_auth(request);

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

        for (const auto &member: response.members()) {
            std::cout
                    << "member[" << member.peer_id() << "]:"
                    << " local=" << (member.local() ? "true" : "false")
                    << " voting=" << (member.voting() ? "true" : "false")
                    << " next_index=" << member.next_index()
                    << " match_index=" << member.match_index()
                    << " lag=" << member.lag()
                    << '\n';
        }

        return response.success() ? 0 : 2;
    }

    int run_telemetry(const std::string &host, std::uint16_t port, const std::string &peer_id) {
        boost::asio::io_context io_context;
        graft::RaftClient client(io_context);

        raft::TelemetryRequest request;
        request.set_term(0);
        request.set_peer_id(peer_id);
        request.set_include_peer_stats(true);
        request.set_require_leader_summary(true);
        apply_request_auth(request);

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
                << "last_log_index: " << response.last_log_index() << '\n'
                << "last_log_term: " << response.last_log_term() << '\n'
                << "cluster_health: " << response.cluster_health() << '\n'
                << "cluster_status_reason: " << response.cluster_status_reason() << '\n'
                << "quorum_available: " << (response.quorum_available() ? "true" : "false") << '\n';

        for (const auto &repl: response.replication()) {
            std::cout
                    << "replication[" << repl.peer_id() << "]:"
                    << " next_index=" << repl.next_index()
                    << " match_index=" << repl.match_index()
                    << " reachable=" << (repl.reachable() ? "true" : "false")
                    << '\n';
        }

        for (const auto &member: response.cluster_members()) {
            std::cout
                    << "member[" << member.peer_id() << "]:"
                    << " local=" << (member.local() ? "true" : "false")
                    << " voting=" << (member.voting() ? "true" : "false")
                    << " next_index=" << member.next_index()
                    << " match_index=" << member.match_index()
                    << " lag=" << member.lag()
                    << '\n';
        }

        return response.success() ? 0 : 2;
    }

    int run_reconfiguration_status(const std::string &host, std::uint16_t port, const std::string &peer_id) {
        boost::asio::io_context io_context;
        graft::RaftClient client(io_context);

        raft::ReconfigurationStatusRequest request;
        request.set_term(0);
        request.set_peer_id(peer_id);
        apply_request_auth(request);

        const auto response = client.call<raft::ReconfigurationStatusRequest, raft::ReconfigurationStatusResponse>(
            host,
            port,
            "ReconfigurationStatusRequest",
            request,
            "ReconfigurationStatusResponse"
        );

        std::cout
                << "peer_id: " << response.peer_id() << '\n'
                << "status: " << response.status() << '\n'
                << "success: " << (response.success() ? "true" : "false") << '\n'
                << "leader_id: " << response.leader_id() << '\n'
                << "state: " << response.state() << '\n'
                << "reconfiguration_active: " << (response.reconfiguration_active() ? "true" : "false") << '\n'
                << "joint_consensus: " << (response.joint_consensus() ? "true" : "false") << '\n'
                << "cluster_health: " << response.cluster_health() << '\n'
                << "cluster_status_reason: " << response.cluster_status_reason() << '\n'
                << "quorum_available: " << (response.quorum_available() ? "true" : "false") << '\n'
                << "current_quorum_available: " << (response.current_quorum_available() ? "true" : "false") << '\n'
                << "next_quorum_available: " << (response.next_quorum_available() ? "true" : "false") << '\n'
                << "members: " << response.members_size() << '\n';

        for (const auto &member: response.members()) {
            std::cout
                    << "member[" << member.peer_id() << "]:"
                    << " local=" << (member.local() ? "true" : "false")
                    << " voting=" << (member.voting() ? "true" : "false")
                    << " role=" << member.role()
                    << " current_role=" << member.current_role()
                    << " next_role=" << member.next_role()
                    << " transition=" << member.role_transition()
                    << " next_index=" << member.next_index()
                    << " match_index=" << member.match_index()
                    << " lag=" << member.lag()
                    << '\n';
        }

        return response.success() ? 0 : 2;
    }

    int run_client_put(
        const std::string &host,
        std::uint16_t port,
        const std::string &key,
        const std::string &value,
        const std::string &peer_id
    ) {
        boost::asio::io_context io_context;
        graft::RaftClient client(io_context);

        raft::StateMachineCommand command;
        command.mutable_put()->set_key(key);
        command.mutable_put()->set_value(value);

        raft::ClientCommandRequest request;
        request.set_term(0);
        request.set_peer_id(peer_id);
        request.set_command(command.SerializeAsString());
        apply_request_auth(request);

        const auto response = client.call<raft::ClientCommandRequest, raft::ClientCommandResponse>(
            host,
            port,
            "ClientCommandRequest",
            request,
            "ClientCommandResponse"
        );

        std::cout
                << "peer_id: " << response.peer_id() << '\n'
                << "status: " << response.status() << '\n'
                << "success: " << (response.success() ? "true" : "false") << '\n'
                << "leader_id: " << response.leader_id() << '\n'
                << "leader_host: " << response.leader_host() << '\n'
                << "leader_port: " << response.leader_port() << '\n'
                << "message: " << response.message() << '\n';

        if (response.success() && !response.result().empty()) {
            raft::StateMachineCommandResult result;
            if (!result.ParseFromString(response.result())) {
                throw std::runtime_error("failed to parse StateMachineCommandResult");
            }
            if (result.result_case() == raft::StateMachineCommandResult::kCas) {
                std::cout
                        << "cas.key: " << result.cas().key() << '\n'
                        << "cas.matched: " << (result.cas().matched() ? "true" : "false") << '\n'
                        << "cas.expected_present: " << (result.cas().expected_present() ? "true" : "false") << '\n'
                        << "cas.expected_value: " << result.cas().expected_value() << '\n'
                        << "cas.new_value: " << result.cas().new_value() << '\n'
                        << "cas.current_present: " << (result.cas().current_present() ? "true" : "false") << '\n'
                        << "cas.current_value: " << result.cas().current_value() << '\n';
            }
        }

        return response.success() ? 0 : 2;
    }

    int run_client_cas(
        const std::string &host,
        std::uint16_t port,
        const std::string &key,
        bool expected_present,
        const std::string &expected_value,
        const std::string &new_value,
        const std::string &peer_id
    ) {
        boost::asio::io_context io_context;
        graft::RaftClient client(io_context);

        raft::StateMachineCommand command;
        auto *cas = command.mutable_cas();
        cas->set_key(key);
        cas->set_expected_present(expected_present);
        cas->set_expected_value(expected_value);
        cas->set_new_value(new_value);

        raft::ClientCommandRequest request;
        request.set_term(0);
        request.set_peer_id(peer_id);
        request.set_command(command.SerializeAsString());
        apply_request_auth(request);

        const auto response = client.call<raft::ClientCommandRequest, raft::ClientCommandResponse>(
            host,
            port,
            "ClientCommandRequest",
            request,
            "ClientCommandResponse"
        );

        std::cout
                << "peer_id: " << response.peer_id() << '\n'
                << "status: " << response.status() << '\n'
                << "success: " << (response.success() ? "true" : "false") << '\n'
                << "leader_id: " << response.leader_id() << '\n'
                << "leader_host: " << response.leader_host() << '\n'
                << "leader_port: " << response.leader_port() << '\n'
                << "message: " << response.message() << '\n';

        if (response.success() && !response.result().empty()) {
            raft::StateMachineCommandResult result;
            if (!result.ParseFromString(response.result())) {
                throw std::runtime_error("failed to parse StateMachineCommandResult");
            }
            if (result.result_case() == raft::StateMachineCommandResult::kCas) {
                std::cout
                        << "cas.key: " << result.cas().key() << '\n'
                        << "cas.matched: " << (result.cas().matched() ? "true" : "false") << '\n'
                        << "cas.expected_present: " << (result.cas().expected_present() ? "true" : "false") << '\n'
                        << "cas.expected_value: " << result.cas().expected_value() << '\n'
                        << "cas.new_value: " << result.cas().new_value() << '\n'
                        << "cas.current_present: " << (result.cas().current_present() ? "true" : "false") << '\n'
                        << "cas.current_value: " << result.cas().current_value() << '\n';
            }
        }

        return response.success() ? 0 : 2;
    }

    int run_client_get(
        const std::string &host,
        std::uint16_t port,
        const std::string &key,
        const std::string &peer_id
    ) {
        boost::asio::io_context io_context;
        graft::RaftClient client(io_context);

        raft::StateMachineQuery query;
        query.mutable_get()->set_key(key);

        raft::ClientQueryRequest request;
        request.set_term(0);
        request.set_peer_id(peer_id);
        request.set_query(query.SerializeAsString());
        apply_request_auth(request);

        const auto response = client.call<raft::ClientQueryRequest, raft::ClientQueryResponse>(
            host,
            port,
            "ClientQueryRequest",
            request,
            "ClientQueryResponse"
        );

        std::cout
                << "peer_id: " << response.peer_id() << '\n'
                << "status: " << response.status() << '\n'
                << "success: " << (response.success() ? "true" : "false") << '\n'
                << "leader_id: " << response.leader_id() << '\n'
                << "leader_host: " << response.leader_host() << '\n'
                << "leader_port: " << response.leader_port() << '\n'
                << "message: " << response.message() << '\n';

        if (response.success() && !response.result().empty()) {
            raft::StateMachineQueryResult result;
            if (!result.ParseFromString(response.result())) {
                throw std::runtime_error("failed to parse StateMachineQueryResult");
            }
            if (result.result_case() == raft::StateMachineQueryResult::kGet) {
                std::cout
                        << "found: " << (result.get().found() ? "true" : "false") << '\n'
                        << "value: " << result.get().value() << '\n';
            }
        }

        return response.success() ? 0 : 2;
    }

    int run_join_cluster(
        const std::string &host,
        std::uint16_t port,
        const std::string &joining_peer_id,
        const std::string &join_host,
        std::uint16_t join_port,
        const std::string &role,
        const std::string &peer_id
    ) {
        boost::asio::io_context io_context;
        graft::RaftClient client(io_context);

        raft::JoinClusterRequest request;
        request.set_term(0);
        request.set_peer_id(peer_id);
        request.set_joining_peer_id(joining_peer_id);
        request.set_host(join_host);
        request.set_port(join_port);
        request.set_role(normalize_peer_role(role));
        apply_request_auth(request);

        const auto response = client.call<raft::JoinClusterRequest, raft::JoinClusterResponse>(
            host,
            port,
            "JoinClusterRequest",
            request,
            "JoinClusterResponse"
        );

        std::cout
                << "peer_id: " << response.peer_id() << '\n'
                << "status: " << response.status() << '\n'
                << "success: " << (response.success() ? "true" : "false") << '\n'
                << "leader_id: " << response.leader_id() << '\n'
                << "message: " << response.message() << '\n';

        return response.success() ? 0 : 2;
    }

    int run_join_status(
        const std::string &host,
        std::uint16_t port,
        const std::string &target_peer_id,
        const std::string &peer_id
    ) {
        boost::asio::io_context io_context;
        graft::RaftClient client(io_context);

        raft::JoinClusterStatusRequest request;
        request.set_term(0);
        request.set_peer_id(peer_id);
        request.set_target_peer_id(target_peer_id);
        apply_request_auth(request);

        const auto response = client.call<raft::JoinClusterStatusRequest, raft::JoinClusterStatusResponse>(
            host,
            port,
            "JoinClusterStatusRequest",
            request,
            "JoinClusterStatusResponse"
        );

        std::cout
                << "peer_id: " << response.peer_id() << '\n'
                << "status: " << response.status() << '\n'
                << "success: " << (response.success() ? "true" : "false") << '\n'
                << "leader_id: " << response.leader_id() << '\n'
                << "message: " << response.message() << '\n';

        return response.success() ? 0 : 2;
    }

    int run_reconfigure(
        const std::string &host,
        std::uint16_t port,
        const std::string &action,
        std::vector<graft::PeerEndpoint> members,
        const std::string &peer_id
    ) {
        boost::asio::io_context io_context;
        graft::RaftClient client(io_context);

        raft::ReconfigureClusterRequest request;
        request.set_term(0);
        request.set_peer_id(peer_id);
        request.set_action(action);
        apply_request_auth(request);
        for (const auto &member: members) {
            auto *spec = request.add_members();
            spec->set_id(member.peer_id);
            spec->set_host(member.host);
            spec->set_port(member.port);
            spec->set_role(member.role.empty() ? "VOTER" : normalize_peer_role(member.role));
        }

        const auto response = client.call<raft::ReconfigureClusterRequest, raft::ReconfigureClusterResponse>(
            host,
            port,
            "ReconfigureClusterRequest",
            request,
            "ReconfigureClusterResponse"
        );

        std::cout
                << "peer_id: " << response.peer_id() << '\n'
                << "status: " << response.status() << '\n'
                << "success: " << (response.success() ? "true" : "false") << '\n'
                << "leader_id: " << response.leader_id() << '\n'
                << "message: " << response.message() << '\n';

        return response.success() ? 0 : 2;
    }

    int run_vote_request(
        const std::string &host,
        std::uint16_t port,
        const std::string &candidate_id,
        std::int64_t last_log_index,
        std::int64_t last_log_term,
        std::int64_t term
    ) {
        boost::asio::io_context io_context;
        graft::RaftClient client(io_context);

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
        const std::string &host,
        std::uint16_t port,
        const std::string &leader_id,
        std::int64_t prev_log_index,
        std::int64_t prev_log_term,
        std::int64_t leader_commit,
        std::int64_t term
    ) {
        boost::asio::io_context io_context;
        graft::RaftClient client(io_context);

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
        const std::string &host,
        std::uint16_t port,
        const std::string &leader_id,
        std::int64_t last_included_index,
        std::int64_t last_included_term,
        std::int64_t term,
        const std::string &snapshot_data
    ) {
        boost::asio::io_context io_context;
        graft::RaftClient client(io_context);

        raft::InstallSnapshotRequest request;
        request.set_term(term);
        request.set_leader_id(leader_id);
        request.set_last_included_index(last_included_index);
        request.set_last_included_term(last_included_term);
        request.set_offset(0);
        request.set_done(true);
        request.set_snapshot_data(snapshot_data);

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
                << "last_included_index: " << response.last_included_index() << '\n'
                << "snapshot_data_size: " << snapshot_data.size() << '\n';

        return 0;
    }

    [[noreturn]] void run_server(
        const std::string &bind_host,
        std::uint16_t port,
        const std::string &peer_id,
        std::int64_t current_term
    ) {
        // Minimal passive server: useful for protocol probes, but it does not
        // run elections or maintain a replicated log.
        boost::asio::io_context io_context;
        auto handler = std::make_shared<graft::StubRpcHandler>(peer_id, current_term);
        graft::RaftServer server(io_context, bind_host, port, std::move(handler));
        server.serve_forever();
    }

    [[noreturn]] void run_stateful_server(
        const std::string &bind_host,
        std::uint16_t port,
        const std::string &peer_id,
        std::int64_t current_term,
        std::int64_t last_log_index,
        std::int64_t last_log_term
    ) {
        // Stateful passive server: answers Raft RPCs through InMemoryRpcHandler
        // but still lacks the active runtime loops that initiate elections,
        // heartbeats, or outgoing replication.
        boost::asio::io_context io_context;
        auto handler = std::make_shared<graft::InMemoryRpcHandler>(peer_id, current_term, last_log_index,
                                                                     last_log_term);
        configure_handler_endpoints(handler, bind_host, port);
        configure_handler_auth(handler);
        configure_handler_authorization(handler);
        configure_handler_admission(handler);
        configure_handler_operational_policy(handler);
        graft::RaftServer server(io_context, bind_host, port, std::move(handler));
        server.serve_forever();
    }

    [[noreturn]] void run_persistent_server(
        const std::string &bind_host,
        std::uint16_t port,
        const std::string &peer_id,
        const std::string &state_file,
        std::int64_t current_term,
        std::int64_t last_log_index,
        std::int64_t last_log_term,
        std::vector<graft::PeerEndpoint> peers
    ) {
        // Passive persistent server: mainly used to verify that incoming RPCs
        // update durable Raft state correctly across restarts.
        boost::asio::io_context io_context;
        std::vector<std::string> peer_ids;
        peer_ids.reserve(peers.size());
        for (const auto &peer: peers) {
            peer_ids.push_back(peer.peer_id);
        }
        auto handler = std::make_shared<graft::PersistentRpcHandler>(
            state_file,
            graft::RaftNode::Config{
                .peer_id = peer_id,
                .current_term = current_term,
                .last_log_index = last_log_index,
                .last_log_term = last_log_term,
                .commit_index = 0,
                .snapshot_index = 0,
                .snapshot_term = 0,
                .voting_peers = std::move(peer_ids),
            }
        );
        configure_handler_endpoints(handler, bind_host, port, peers);
        configure_handler_auth(handler);
        configure_handler_authorization(handler);
        configure_handler_admission(handler);
        configure_handler_operational_policy(handler);
        graft::RaftServer server(io_context, bind_host, port, std::move(handler));
        server.serve_forever();
    }

    [[noreturn]] void run_active_server(
        const std::string &bind_host,
        std::uint16_t port,
        const std::string &,
        std::shared_ptr<graft::RaftNode> node,
        std::shared_ptr<graft::RpcHandler> handler,
        std::vector<graft::PeerEndpoint> peers,
        std::function<void(const graft::RaftNode &)> persist_callback = {},
        std::optional<std::chrono::milliseconds> replicate_interval = std::nullopt,
        std::string replicate_prefix = "synthetic",
        std::optional<std::int64_t> snapshot_threshold = std::nullopt
    ) {
        // Active server wiring:
        //
        // * RaftServer owns inbound TCP traffic and calls RpcHandler.
        // * RpcHandler translates protobuf requests to RaftNode operations and
        //   domain callbacks.
        // * RaftRuntime owns outbound RPCs: elections, heartbeats, replication,
        //   read barriers, and dynamic peer endpoint tracking.
        //
        // This is the composition an application would normally reproduce,
        // replacing CLI parsing and demo policies with application services.
        boost::asio::io_context server_io_context;
        boost::asio::io_context client_io_context;
        graft::RaftRuntime runtime(client_io_context, node, std::move(peers), std::move(persist_callback));
        const auto peer_endpoints = runtime.peers();

        if (auto in_memory = std::dynamic_pointer_cast<graft::InMemoryRpcHandler>(handler)) {
            // Domain commands enter Raft through handler callbacks.  The handler
            // performs edge validation; the runtime appends and replicates the
            // command; RaftNode applies it only after commitment.
            configure_handler_endpoints(in_memory, bind_host, port, peer_endpoints);
            configure_handler_auth(in_memory);
            configure_handler_authorization(in_memory);
            configure_handler_admission(in_memory);
            configure_handler_operational_policy(in_memory);
            in_memory->set_command_replicator([&runtime](const std::string &command) {
                return runtime.replicate_entry_once_with_result(command);
            });
            in_memory->set_internal_command_replicator([&runtime](const std::string &command) {
                return runtime.replicate_entry_once(command) > 0;
            });
            const auto read_lease = linearizable_read_lease_from_env();
            const auto read_timeout = linearizable_read_timeout_from_env();
            // Queries do not append log entries, so the handler asks the runtime
            // for a read barrier before serving from local state.  The runtime
            // confirms recent leader authority using the read-lease model.
            in_memory->set_read_barrier([&runtime, read_lease, read_timeout]() {
                return runtime.await_linearizable_read(read_lease, read_timeout);
            });
            in_memory->set_join_forwarder(forward_join_request);
            in_memory->set_reconfigure_forwarder(forward_reconfigure_request);
            in_memory->set_join_tracker(
                [&runtime](const std::string &peer_id, const graft::InMemoryRpcHandler::Endpoint &endpoint) {
                    // Track the endpoint immediately so the leader can start
                    // contacting a joining peer even before the configuration
                    // entry has been committed everywhere.
                    runtime.track_peer(graft::PeerEndpoint{
                        .peer_id = peer_id,
                        .host = endpoint.host,
                        .port = static_cast<std::uint16_t>(endpoint.port),
                    });
                });
            in_memory->set_membership_updater(
                [&runtime](const std::vector<std::string> &peer_ids,
                           const std::vector<graft::InMemoryRpcHandler::Endpoint> &endpoints) {
                    // Once a configuration entry is accepted, keep the runtime's
                    // address book in sync with the ids stored in Raft state.
                    std::vector<graft::PeerEndpoint> peers;
                    peers.reserve(peer_ids.size());
                    for (std::size_t i = 0; i < peer_ids.size() && i < endpoints.size(); ++i) {
                        peers.push_back(graft::PeerEndpoint{
                            .peer_id = peer_ids[i],
                            .host = endpoints[i].host,
                            .port = static_cast<std::uint16_t>(endpoints[i].port),
                        });
                    }
                    runtime.configure_peers(std::move(peers));
                });
        }
        if (auto persistent = std::dynamic_pointer_cast<graft::PersistentRpcHandler>(handler)) {
            // Same active wiring as the in-memory case, with the important
            // difference that committed Raft state is saved through the store
            // callback supplied by the persistent wrapper.
            configure_handler_endpoints(persistent, bind_host, port, peer_endpoints);
            configure_handler_auth(persistent);
            configure_handler_authorization(persistent);
            configure_handler_admission(persistent);
            configure_handler_operational_policy(persistent);
            persistent->delegate().set_command_replicator([&runtime](const std::string &command) {
                return runtime.replicate_entry_once_with_result(command);
            });
            persistent->delegate().set_internal_command_replicator([&runtime](const std::string &command) {
                return runtime.replicate_entry_once(command) > 0;
            });
            const auto read_lease = linearizable_read_lease_from_env();
            const auto read_timeout = linearizable_read_timeout_from_env();
            persistent->delegate().set_read_barrier([&runtime, read_lease, read_timeout]() {
                return runtime.await_linearizable_read(read_lease, read_timeout);
            });
            persistent->delegate().set_join_forwarder(forward_join_request);
            persistent->delegate().set_reconfigure_forwarder(forward_reconfigure_request);
            persistent->delegate().set_join_tracker(
                [&runtime](const std::string &peer_id, const graft::InMemoryRpcHandler::Endpoint &endpoint) {
                    runtime.track_peer(graft::PeerEndpoint{
                        .peer_id = peer_id,
                        .host = endpoint.host,
                        .port = static_cast<std::uint16_t>(endpoint.port),
                    });
                });
            persistent->delegate().set_membership_updater(
                [&runtime](const std::vector<std::string> &peer_ids,
                           const std::vector<graft::InMemoryRpcHandler::Endpoint> &endpoints) {
                    std::vector<graft::PeerEndpoint> peers;
                    peers.reserve(peer_ids.size());
                    for (std::size_t i = 0; i < peer_ids.size() && i < endpoints.size(); ++i) {
                        peers.push_back(graft::PeerEndpoint{
                            .peer_id = peer_ids[i],
                            .host = endpoints[i].host,
                            .port = static_cast<std::uint16_t>(endpoints[i].port),
                        });
                    }
                    runtime.configure_peers(std::move(peers));
                });
        }

        graft::RaftServer server(server_io_context, bind_host, port, std::move(handler));

        // Inbound and outbound work use separate io_context instances.  That
        // keeps server accepts/reads independent from outbound client RPCs and
        // timer callbacks used by the Raft runtime.
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

        // These timers implement the background behavior that the paper treats
        // as "servers do periodically": leaders send heartbeats; followers and
        // candidates start elections after randomized inactivity timeouts.
        std::function<void()> schedule_heartbeat;
        std::function<void(std::chrono::milliseconds)> schedule_election;

        schedule_heartbeat = [&]() {
            heartbeat_timer->expires_after(heartbeat_interval);
            heartbeat_timer->async_wait([&, heartbeat_timer](const boost::system::error_code &error) {
                if (error) {
                    return;
                }
                if (runtime.node().role() == graft::RaftNode::Role::leader) {
                    // Heartbeats are empty AppendEntries RPCs.  They also carry
                    // commit index and leader identity, and they refresh the
                    // successful-contact timestamps used by read leases.
                    runtime.send_heartbeats_once();
                }
                schedule_heartbeat();
            });
        };

        schedule_election = [&](std::chrono::milliseconds timeout) {
            election_timer->expires_after(timeout);
            election_timer->async_wait([&, timeout, election_timer](const boost::system::error_code &error) {
                if (error) {
                    return;
                }

                if (runtime.node().role() == graft::RaftNode::Role::leader) {
                    schedule_election(next_election_timeout());
                    return;
                }

                const auto idle_for = std::chrono::steady_clock::now() - runtime.node().last_activity();
                if (idle_for < timeout) {
                    // Any valid leader/candidate contact resets the election
                    // clock.  Rescheduling the remaining time avoids starting an
                    // election just because the timer was already armed.
                    const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(timeout - idle_for);
                    schedule_election(std::max<std::chrono::milliseconds>(std::chrono::milliseconds(1), remaining));
                    return;
                }

                const auto became_leader = runtime.run_election_round();
                if (became_leader) {
                    // A new leader sends an immediate heartbeat.  This announces
                    // leadership quickly and initializes follower progress for
                    // later log replication.
                    heartbeat_timer->cancel();
                    heartbeat_timer->expires_after(std::chrono::milliseconds(0));
                    heartbeat_timer->async_wait([&, heartbeat_timer](const boost::system::error_code &timer_error) {
                        if (timer_error) {
                            return;
                        }
                        if (runtime.node().role() == graft::RaftNode::Role::leader) {
                            runtime.send_heartbeats_once();
                        }
                        schedule_heartbeat();
                    });
                }

                schedule_election(next_election_timeout());
            });
        };

        std::jthread replicate_thread;
        if (replicate_interval.has_value()) {
            // Synthetic workload mode is for demos and Jepsen-style smoke tests:
            // it continuously appends commands so replication, commitment, and
            // snapshot compaction can be observed without an external client.
            replicate_thread = std::jthread([&, replicate_interval, replicate_prefix, snapshot_threshold]() {
                std::size_t replicate_counter = 0;
                for (;;) {
                    std::this_thread::sleep_for(*replicate_interval);
                    if (runtime.node().role() == graft::RaftNode::Role::leader) {
                        runtime.replicate_entry_once(replicate_prefix + "-" + std::to_string(++replicate_counter));
                        if (snapshot_threshold.has_value()) {
                            const auto commit_index = runtime.node().commit_index();
                            const auto snapshot_index = runtime.node().snapshot_index();
                            if (commit_index > snapshot_index && (commit_index - snapshot_index) >= *
                                snapshot_threshold) {
                                // The core node snapshots the application state
                                // machine, not the synthetic string passed here;
                                // the label remains only as CLI output context.
                                const auto compacted = runtime.node().compact_snapshot_to(
                                    commit_index,
                                    "auto-snapshot-" + std::to_string(commit_index)
                                );
                                if (compacted && persist_callback) {
                                    persist_callback(runtime.node());
                                }
                                std::cout
                                        << "auto-compact snapshot_index=" << runtime.node().snapshot_index()
                                        << " compacted=" << (compacted ? "true" : "false")
                                        << '\n';
                            }
                        }
                    }
                }
            });
        }

        schedule_heartbeat();
        schedule_election(next_election_timeout());
        client_io_context.run();

        for (;;) {
            std::this_thread::sleep_for(std::chrono::hours(24));
        }
    }

    [[noreturn]] void run_active_server(
        const std::string &bind_host,
        std::uint16_t port,
        const std::string &peer_id,
        std::int64_t current_term,
        std::int64_t last_log_index,
        std::int64_t last_log_term,
        std::vector<graft::PeerEndpoint> peers
    ) {
        // Non-persistent active mode is convenient for short-lived tests.  It is
        // still a real Raft participant, but restart loses term/vote/log state.
        auto node = std::make_shared<graft::RaftNode>(graft::RaftNode::Config{
            .peer_id = peer_id,
            .current_term = current_term,
            .last_log_index = last_log_index,
            .last_log_term = last_log_term,
            .commit_index = 0,
            .snapshot_index = 0,
            .snapshot_term = 0,
            .voting_peers = {},
        });
        auto handler = std::make_shared<graft::InMemoryRpcHandler>(node);
        run_active_server(bind_host, port, peer_id, std::move(node), std::move(handler), std::move(peers));
    }

    [[noreturn]] void run_active_persistent_server(
        const std::string &bind_host,
        std::uint16_t port,
        const std::string &peer_id,
        const std::string &state_file,
        std::int64_t current_term,
        std::int64_t last_log_index,
        std::int64_t last_log_term,
        std::vector<graft::PeerEndpoint> peers
    ) {
        // Persistent active mode is the shape expected for real deployments:
        // Raft metadata, log entries, snapshots, and cluster configuration are
        // restored before the node rejoins the cluster.
        auto handler = std::make_shared<graft::PersistentRpcHandler>(
            state_file,
            graft::RaftNode::Config{
                .peer_id = peer_id,
                .current_term = current_term,
                .last_log_index = last_log_index,
                .last_log_term = last_log_term,
                .commit_index = 0,
                .snapshot_index = 0,
                .snapshot_term = 0,
                .voting_peers = {},
            }
        );
        run_active_server(
            bind_host,
            port,
            peer_id,
            handler->node_ptr(),
            handler,
            std::move(peers),
            [handler](const graft::RaftNode &current_node) {
                handler->store().save(current_node.persistent_state());
            }
        );
    }

    [[noreturn]] void run_active_persistent_workload_server(
        const std::string &bind_host,
        std::uint16_t port,
        const std::string &peer_id,
        const std::string &state_file,
        std::int64_t current_term,
        std::int64_t last_log_index,
        std::int64_t last_log_term,
        std::chrono::milliseconds replicate_interval,
        std::optional<std::int64_t> snapshot_threshold,
        std::vector<graft::PeerEndpoint> peers
    ) {
        // Adds only the synthetic workload/compaction loop to persistent active
        // mode.  Application code should normally drive commands through the
        // client API instead.
        auto handler = std::make_shared<graft::PersistentRpcHandler>(
            state_file,
            graft::RaftNode::Config{
                .peer_id = peer_id,
                .current_term = current_term,
                .last_log_index = last_log_index,
                .last_log_term = last_log_term,
                .commit_index = 0,
                .snapshot_index = 0,
                .snapshot_term = 0,
                .voting_peers = {},
            }
        );
        run_active_server(
            bind_host,
            port,
            peer_id,
            handler->node_ptr(),
            handler,
            std::move(peers),
            [handler](const graft::RaftNode &current_node) {
                handler->store().save(current_node.persistent_state());
            },
            replicate_interval,
            "workload",
            snapshot_threshold
        );
    }

    int run_election_round(
        const std::string &peer_id,
        std::int64_t current_term,
        std::int64_t last_log_index,
        std::int64_t last_log_term,
        std::vector<graft::PeerEndpoint> peers
    ) {
        // One-shot commands are useful when testing the Raft mechanics in
        // isolation.  They construct a runtime, execute a single protocol step,
        // print the resulting state, and then exit.
        boost::asio::io_context io_context;
        graft::RaftRuntime runtime(
            io_context,
            graft::RaftNode::Config{
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
                << "role: " << (runtime.node().role() == graft::RaftNode::Role::leader
                                    ? "leader"
                                    : "candidate-or-follower") << '\n'
                << "granted_votes: " << runtime.node().granted_votes() << '\n'
                << "quorum: " << runtime.node().quorum_size() << '\n';
        return became_leader ? 0 : 2;
    }

    int run_heartbeat_round(
        const std::string &peer_id,
        std::int64_t term,
        std::int64_t last_log_index,
        std::int64_t last_log_term,
        std::vector<graft::PeerEndpoint> peers
    ) {
        boost::asio::io_context io_context;
        graft::RaftRuntime runtime(
            io_context,
            graft::RaftNode::Config{
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
                << "role: " << (runtime.node().role() == graft::RaftNode::Role::leader ? "leader" : "not-leader") <<
                '\n'
                << "heartbeat_successes: " << successes << '\n';
        return successes > 0 ? 0 : 2;
    }

    int run_replicate_once(
        const std::string &peer_id,
        std::int64_t term,
        const std::string &data,
        std::int64_t last_log_index,
        std::int64_t last_log_term,
        std::vector<graft::PeerEndpoint> peers
    ) {
        boost::asio::io_context io_context;
        graft::RaftRuntime runtime(
            io_context,
            graft::RaftNode::Config{
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
                << "role: " << (runtime.node().role() == graft::RaftNode::Role::leader ? "leader" : "not-leader") <<
                '\n'
                << "replication_successes: " << successes << '\n'
                << "last_log_index: " << runtime.node().last_log_index() << '\n'
                << "commit_index: " << runtime.node().commit_index() << '\n';
        return successes > 0 ? 0 : 2;
    }

    int run_replicate_once_persistent(
        const std::string &peer_id,
        const std::string &state_file,
        std::int64_t term,
        const std::string &data,
        std::int64_t last_log_index,
        std::int64_t last_log_term,
        std::vector<graft::PeerEndpoint> peers
    ) {
        boost::asio::io_context io_context;
        graft::PersistentStateStore store(state_file);
        auto node = std::make_shared<graft::RaftNode>(graft::RaftNode::Config{
            .peer_id = peer_id,
            .current_term = term,
            .last_log_index = last_log_index,
            .last_log_term = last_log_term,
            .commit_index = last_log_index,
            .snapshot_index = 0,
            .snapshot_term = 0,
            .voting_peers = {},
        });

        if (const auto persisted = store.load(); persisted.has_value()) {
            node->apply_persistent_state(*persisted);
        } else {
            store.save(node->persistent_state());
        }

        node->become_leader();
        store.save(node->persistent_state());

        graft::RaftRuntime runtime(
            io_context,
            node,
            std::move(peers),
            [&store](const graft::RaftNode &current_node) {
                store.save(current_node.persistent_state());
            }
        );

        const auto successes = runtime.replicate_entry_once(data);
        std::cout
                << "role: " << (runtime.node().role() == graft::RaftNode::Role::leader ? "leader" : "not-leader") <<
                '\n'
                << "replication_successes: " << successes << '\n'
                << "last_log_index: " << runtime.node().last_log_index() << '\n'
                << "commit_index: " << runtime.node().commit_index() << '\n';
        return successes > 0 ? 0 : 2;
    }

    int run_compact_snapshot(
        const std::string &peer_id,
        const std::string &state_file,
        std::int64_t index,
        const std::string &snapshot_data,
        std::int64_t current_term,
        std::int64_t last_log_index,
        std::int64_t last_log_term
    ) {
        graft::PersistentStateStore store(state_file);
        auto node = std::make_shared<graft::RaftNode>(graft::RaftNode::Config{
            .peer_id = peer_id,
            .current_term = current_term,
            .last_log_index = last_log_index,
            .last_log_term = last_log_term,
            .commit_index = last_log_index,
            .snapshot_index = 0,
            .snapshot_term = 0,
            .voting_peers = {},
        });

        if (const auto persisted = store.load(); persisted.has_value()) {
            node->apply_persistent_state(*persisted);
        } else {
            store.save(node->persistent_state());
        }

        const auto compacted = node->compact_snapshot_to(index, snapshot_data);
        store.save(node->persistent_state());

        std::cout
                << "compacted: " << (compacted ? "true" : "false") << '\n'
                << "snapshot_index: " << node->snapshot_index() << '\n'
                << "snapshot_term: " << node->snapshot_term() << '\n'
                << "snapshot_data_size: " << node->snapshot_data().size() << '\n'
                << "last_log_index: " << node->last_log_index() << '\n'
                << "commit_index: " << node->commit_index() << '\n';

        return compacted ? 0 : 2;
    }

    int run_replicate_put_persistent(
        const std::string &peer_id,
        const std::string &state_file,
        std::int64_t term,
        const std::string &key,
        const std::string &value,
        std::int64_t last_log_index,
        std::int64_t last_log_term,
        std::vector<graft::PeerEndpoint> peers
    ) {
        raft::StateMachineCommand command;
        command.mutable_put()->set_key(key);
        command.mutable_put()->set_value(value);
        return run_replicate_once_persistent(
            peer_id,
            state_file,
            term,
            command.SerializeAsString(),
            last_log_index,
            last_log_term,
            std::move(peers)
        );
    }

    int run_dump_state(const std::string &state_file) {
        graft::PersistentStateStore store(state_file);
        const auto persisted = store.load();
        if (!persisted.has_value()) {
            throw std::runtime_error("state file does not exist or is empty");
        }

        std::cout
                << "peer_id: " << persisted->peer_id << '\n'
                << "current_term: " << persisted->current_term << '\n'
                << "commit_index: " << persisted->commit_index << '\n'
                << "last_applied: " << persisted->last_applied << '\n'
                << "snapshot_index: " << persisted->snapshot_index << '\n'
                << "snapshot_term: " << persisted->snapshot_term << '\n'
                << "snapshot_data_size: " << persisted->snapshot_data.size() << '\n';
        return 0;
    }
} // namespace

namespace graft {
int run_cli(int argc, char **argv) {
    try {
        if (argc < 2) {
            print_usage();
            return 1;
        }

        const std::string command = argv[1];
        // Commands that address a running node share the leading host/port
        // arguments; purely local one-shot commands parse their own arguments.
        const auto uses_host_port =
                command == "cluster-summary" ||
                command == "telemetry" ||
                command == "reconfiguration-status" ||
                command == "client-put" ||
                command == "client-cas" ||
                command == "client-get" ||
                command == "join-cluster" ||
                command == "join-status" ||
                command == "reconfigure" ||
                command == "vote-request" ||
                command == "append-entries" ||
                command == "install-snapshot" ||
                command == "serve" ||
                command == "serve-stateful" ||
                command == "serve-persistent" ||
                command == "serve-active" ||
                command == "serve-active-persistent" ||
                command == "serve-active-persistent-workload";

        std::string host;
        std::uint16_t port = 0;
        if (uses_host_port) {
            if (argc < 4) {
                print_usage();
                return 1;
            }
            host = argv[2];
            port = parse_port(argv[3]);
        }

        int exit_code = 1;
        if (command == "cluster-summary") {
            exit_code = run_cluster_summary(host, port, peer_id_or_default(argc, argv, 4));
        } else if (command == "telemetry") {
            exit_code = run_telemetry(host, port, peer_id_or_default(argc, argv, 4));
        } else if (command == "reconfiguration-status") {
            exit_code = run_reconfiguration_status(host, port, peer_id_or_default(argc, argv, 4));
        } else if (command == "client-put") {
            if (argc < 6) {
                print_usage();
                return 1;
            }
            exit_code = run_client_put(host, port, argv[4], argv[5], peer_id_or_default(argc, argv, 6));
        } else if (command == "client-cas") {
            if (argc < 8) {
                print_usage();
                return 1;
            }
            exit_code = run_client_cas(
                host,
                port,
                argv[4],
                parse_bool(argv[5], "expected-present"),
                argv[6],
                argv[7],
                peer_id_or_default(argc, argv, 8)
            );
        } else if (command == "client-get") {
            if (argc < 5) {
                print_usage();
                return 1;
            }
            exit_code = run_client_get(host, port, argv[4], peer_id_or_default(argc, argv, 5));
        } else if (command == "join-cluster") {
            if (argc < 7) {
                print_usage();
                return 1;
            }
            exit_code = run_join_cluster(
                host,
                port,
                argv[4],
                argv[5],
                parse_port(argv[6]),
                argc > 7 ? argv[7] : "VOTER",
                peer_id_or_default(argc, argv, 8)
            );
        } else if (command == "join-status") {
            if (argc < 5) {
                print_usage();
                return 1;
            }
            exit_code = run_join_status(host, port, argv[4], peer_id_or_default(argc, argv, 5));
        } else if (command == "reconfigure") {
            if (argc < 6) {
                print_usage();
                return 1;
            }
            exit_code = run_reconfigure(
                host,
                port,
                argv[4],
                parse_peer_specs(argv, 5, argc),
                "cpp-cli"
            );
        } else if (command == "vote-request") {
            if (argc < 7) {
                print_usage();
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
                return 1;
            }
            exit_code = run_install_snapshot(
                host,
                port,
                argv[4],
                parse_int64(argv[5], "last-included-index"),
                parse_int64(argv[6], "last-included-term"),
                argc > 7 ? parse_int64(argv[7], "term") : 0,
                argc > 8 ? argv[8] : std::string{}
            );
        } else if (command == "serve") {
            if (argc < 5) {
                print_usage();
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
        } else if (command == "serve-persistent") {
            if (argc < 6) {
                print_usage();
                return 1;
            }
            run_persistent_server(
                host,
                port,
                argv[4],
                argv[5],
                argc > 6 ? parse_int64(argv[6], "current-term") : 0,
                argc > 7 ? parse_int64(argv[7], "last-log-index") : 0,
                argc > 8 ? parse_int64(argv[8], "last-log-term") : 0,
                argc > 9 ? parse_peer_specs(argv, 9, argc) : std::vector<graft::PeerEndpoint>{}
            );
        } else if (command == "serve-active") {
            if (argc < 9) {
                print_usage();
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
        } else if (command == "serve-active-persistent") {
            if (argc < 10) {
                print_usage();
                return 1;
            }
            run_active_persistent_server(
                host,
                port,
                argv[4],
                argv[5],
                parse_int64(argv[6], "current-term"),
                parse_int64(argv[7], "last-log-index"),
                parse_int64(argv[8], "last-log-term"),
                parse_peer_specs(argv, 9, argc)
            );
        } else if (command == "serve-active-persistent-workload") {
            if (argc < 11) {
                print_usage();
                return 1;
            }
            const bool has_snapshot_threshold =
                    argc > 10 && std::string_view(argv[10]).find('@') == std::string_view::npos;
            run_active_persistent_workload_server(
                host,
                port,
                argv[4],
                argv[5],
                parse_int64(argv[6], "current-term"),
                parse_int64(argv[7], "last-log-index"),
                parse_int64(argv[8], "last-log-term"),
                std::chrono::milliseconds(parse_int64(argv[9], "replicate-interval-ms")),
                has_snapshot_threshold
                    ? std::optional<std::int64_t>{parse_int64(argv[10], "snapshot-threshold")}
                    : std::nullopt,
                parse_peer_specs(argv, has_snapshot_threshold ? 11 : 10, argc)
            );
        } else if (command == "election-round") {
            if (argc < 7) {
                print_usage();
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
        } else if (command == "replicate-once-persistent") {
            if (argc < 8) {
                print_usage();
                return 1;
            }
            exit_code = run_replicate_once_persistent(
                argv[2],
                argv[3],
                parse_int64(argv[4], "term"),
                argv[5],
                parse_int64(argv[6], "last-log-index"),
                parse_int64(argv[7], "last-log-term"),
                parse_peer_specs(argv, 8, argc)
            );
        } else if (command == "replicate-put-persistent") {
            if (argc < 9) {
                print_usage();
                return 1;
            }
            exit_code = run_replicate_put_persistent(
                argv[2],
                argv[3],
                parse_int64(argv[4], "term"),
                argv[5],
                argv[6],
                parse_int64(argv[7], "last-log-index"),
                parse_int64(argv[8], "last-log-term"),
                parse_peer_specs(argv, 9, argc)
            );
        } else if (command == "compact-snapshot") {
            if (argc < 6) {
                print_usage();
                return 1;
            }
            exit_code = run_compact_snapshot(
                argv[2],
                argv[3],
                parse_int64(argv[4], "index"),
                argv[5],
                argc > 6 ? parse_int64(argv[6], "current-term") : 0,
                argc > 7 ? parse_int64(argv[7], "last-log-index") : 0,
                argc > 8 ? parse_int64(argv[8], "last-log-term") : 0
            );
        } else if (command == "dump-state") {
            if (argc < 3) {
                print_usage();
                google::protobuf::ShutdownProtobufLibrary();
                return 1;
            }
            exit_code = run_dump_state(argv[2]);
        } else {
            print_usage();
        }

        return exit_code;
    } catch (const std::exception &e) {
        std::cerr << "graft_smoke error: " << e.what() << '\n';
        return 1;
    }
}
} // namespace graft
