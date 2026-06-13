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

#include "graft/telemetry/telemetry_exporter.hpp"
#include "graft/core/raft_node.hpp"

#include <cstdlib>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>

namespace graft {

    // -------------------------------------------------------------------
    // Environment variable helpers
    // -------------------------------------------------------------------

    static std::string env_str(const char *key, const std::string &default_val) {
        const char *val = std::getenv(key);
        return val ? std::string(val) : default_val;
    }

    static std::uint16_t env_uint16(const char *key, std::uint16_t default_val) {
        const char *val = std::getenv(key);
        if (!val) return default_val;
        try {
            auto ival = std::stoi(val);
            if (ival < 0 || ival > 65535) return default_val;
            return static_cast<std::uint16_t>(ival);
        } catch (...) {
            return default_val;
        }
    }

    static std::int64_t env_int64(const char *key, std::int64_t default_val) {
        const char *val = std::getenv(key);
        if (!val) return default_val;
        try {
            return std::stoll(val);
        } catch (...) {
            return default_val;
        }
    }

    // -------------------------------------------------------------------
    // Implementation (PIMPL to keep Asio out of the public header)
    // -------------------------------------------------------------------

    struct TelemetryExporter::Impl : std::enable_shared_from_this<Impl> {
        boost::asio::ip::tcp::acceptor prometheus_acceptor;
        boost::asio::steady_timer otlp_timer;
        SnapshotProvider snapshot_provider;
        std::string prometheus_path;
        std::string otlp_endpoint;
        std::vector<std::pair<std::string, std::string>> otlp_headers;
        std::chrono::milliseconds otlp_timeout;
        std::chrono::seconds export_interval;
        bool running{false};

        Impl(boost::asio::io_context &io) : prometheus_acceptor(io), otlp_timer(io) {}

        void do_accept();
        void handle_connection(boost::asio::ip::tcp::socket sock);
        void do_otlp_push();
        void do_otel_log_push();
    };

    // -------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------

    TelemetryExporter::TelemetryExporter() = default;
    TelemetryExporter::~TelemetryExporter() = default;

    TelemetryExporter TelemetryExporter::from_env() {
        TelemetryExporter ex;
        std::string exporter_type = env_str("RAFT_TELEMETRY_EXPORTER", "none");

        if (exporter_type == "prometheus") {
            ex.type_ = Type::Prometheus;
            ex.prometheus_.host = env_str("RAFT_TELEMETRY_PROMETHEUS_HOST", "127.0.0.1");
            ex.prometheus_.port = env_uint16("RAFT_TELEMETRY_PROMETHEUS_PORT", 9108);
            ex.prometheus_.path = env_str("RAFT_TELEMETRY_PROMETHEUS_PATH", "/metrics");
            ex.export_interval_ = std::chrono::seconds(
                env_int64("RAFT_TELEMETRY_EXPORT_INTERVAL_SECONDS", 15));
        } else if (exporter_type == "opentelemetry" || exporter_type == "otlp") {
            ex.type_ = Type::Otlp;
            ex.otlp_.endpoint = env_str("RAFT_TELEMETRY_OTLP_ENDPOINT",
                                         "http://127.0.0.1:4318/v1/metrics");
            ex.otlp_.timeout = std::chrono::milliseconds(
                env_int64("RAFT_TELEMETRY_OTLP_TIMEOUT_MILLIS", 2000));
            ex.export_interval_ = std::chrono::seconds(
                env_int64("RAFT_TELEMETRY_EXPORT_INTERVAL_SECONDS", 15));

            std::string headers_str = env_str("RAFT_TELEMETRY_OTLP_HEADERS", "");
            if (!headers_str.empty()) {
                std::istringstream iss(headers_str);
                std::string token;
                while (std::getline(iss, token, ',')) {
                    auto eq = token.find('=');
                    if (eq != std::string::npos) {
                        std::string key = token.substr(0, eq);
                        std::string value = token.substr(eq + 1);
                        key.erase(0, key.find_first_not_of(" \t"));
                        key.erase(key.find_last_not_of(" \t") + 1);
                        value.erase(0, value.find_first_not_of(" \t"));
                        value.erase(value.find_last_not_of(" \t") + 1);
                        ex.otlp_.headers.emplace_back(key, value);
                    }
                }
            }
        } else if (exporter_type == "otel-log" || exporter_type == "opentelemetry-log") {
            ex.type_ = Type::OtelJsonLog;
            ex.export_interval_ = std::chrono::seconds(
                env_int64("RAFT_TELEMETRY_EXPORT_INTERVAL_SECONDS", 15));
        }

        return ex;
    }

    bool TelemetryExporter::enabled() const {
        return type_ != Type::None;
    }

    void TelemetryExporter::start(boost::asio::io_context &io, SnapshotProvider provider) {
        if (type_ == Type::None) return;

        impl_ = std::make_shared<Impl>(io);
        impl_->snapshot_provider = std::move(provider);
        impl_->running = true;

        if (type_ == Type::Prometheus) {
            impl_->prometheus_path = prometheus_.path;

            boost::asio::ip::tcp::endpoint ep(
                boost::asio::ip::make_address(prometheus_.host), prometheus_.port);

            boost::system::error_code ec;
            impl_->prometheus_acceptor.open(ep.protocol(), ec);
            if (ec) {
                std::cerr << "[graft-telemetry] Failed to open Prometheus acceptor: "
                          << ec.message() << std::endl;
                return;
            }
            impl_->prometheus_acceptor.set_option(
                boost::asio::ip::tcp::acceptor::reuse_address(true), ec);
            impl_->prometheus_acceptor.bind(ep, ec);
            if (ec) {
                std::cerr << "[graft-telemetry] Failed to bind Prometheus on " << ep << ": "
                          << ec.message() << std::endl;
                return;
            }
            impl_->prometheus_acceptor.listen(
                boost::asio::socket_base::max_listen_connections, ec);
            if (ec) {
                std::cerr << "[graft-telemetry] Failed to listen: " << ec.message() << std::endl;
                return;
            }

            std::cout << "[graft-smoke] telemetry exporter: prometheus http://" << ep
                      << prometheus_.path << std::endl;

            impl_->do_accept();
        } else if (type_ == Type::Otlp) {
            impl_->otlp_endpoint = otlp_.endpoint;
            impl_->otlp_headers = otlp_.headers;
            impl_->otlp_timeout = otlp_.timeout;
            impl_->export_interval = export_interval_;

            std::cout << "[graft-smoke] telemetry exporter: otlp " << otlp_.endpoint << std::endl;

            impl_->do_otlp_push();
        } else if (type_ == Type::OtelJsonLog) {
            impl_->export_interval = export_interval_;
            impl_->snapshot_provider = std::move(provider);

            std::cout << "[graft-smoke] telemetry exporter: otel-log (interval="
                      << export_interval_.count() << "s)" << std::endl;

            impl_->do_otel_log_push();
        }
    }

    void TelemetryExporter::Impl::do_accept() {
        if (!running) return;
        auto self = shared_from_this();
        prometheus_acceptor.async_accept(
            [self](boost::system::error_code ec, boost::asio::ip::tcp::socket sock) {
                if (!ec && self->running) {
                    self->handle_connection(std::move(sock));
                }
                self->do_accept();
            });
    }

    void TelemetryExporter::Impl::handle_connection(boost::asio::ip::tcp::socket sock) {
        auto buf = std::make_shared<std::vector<char>>(4096);
        auto sock_ptr = std::make_shared<boost::asio::ip::tcp::socket>(std::move(sock));
        auto self = shared_from_this();

        sock_ptr->async_read_some(
            boost::asio::buffer(*buf),
            [self, buf, sock_ptr](boost::system::error_code ec, std::size_t n) {
                if (ec || n == 0) return;
                std::string req(buf->data(), n);
                std::istringstream iss(req);
                std::string first_line;
                std::getline(iss, first_line);

                std::istringstream lp(first_line);
                std::string method, path, _ver;
                lp >> method >> path >> _ver;

                std::string response;
                if (method == "GET" && path == self->prometheus_path) {
                    std::string body = self->snapshot_provider();
                    std::ostringstream oss;
                    oss << "HTTP/1.1 200 OK\r\n"
                        << "Content-Type: text/plain; charset=utf-8\r\n"
                        << "Content-Length: " << body.size() << "\r\n"
                        << "Connection: close\r\n\r\n"
                        << body;
                    response = oss.str();
                } else if (method == "GET") {
                    response = "HTTP/1.1 404 Not Found\r\n"
                               "Content-Length: 10\r\n"
                               "Connection: close\r\n"
                               "\r\n"
                               "Not Found\n";
                } else {
                    response = "HTTP/1.1 405 Method Not Allowed\r\n"
                               "Content-Length: 18\r\n"
                               "Connection: close\r\n"
                               "\r\n"
                               "Method Not Allowed\n";
                }

                boost::asio::async_write(
                    *sock_ptr, boost::asio::buffer(response),
                    [sock_ptr](boost::system::error_code, std::size_t) {});
            });
    }

    void TelemetryExporter::Impl::do_otlp_push() {
        if (!running) return;
        auto self = shared_from_this();
        otlp_timer.expires_after(export_interval);
        otlp_timer.async_wait([self](boost::system::error_code ec) {
            if (ec || !self->running) return;
            // TODO: full OTLP HTTP push via Boost.Beast.
            self->do_otlp_push();
        });
    }

    void TelemetryExporter::Impl::do_otel_log_push() {
        if (!running || !snapshot_provider) return;
        auto self = shared_from_this();
        otlp_timer.expires_after(export_interval);
        otlp_timer.async_wait([self](boost::system::error_code ec) {
            if (ec || !self->running || !self->snapshot_provider) return;
            // Emit a single-line JSON log entry via std::cout,
            // matching Java's OpenTelemetryJsonExporter pattern
            // of writing to a dedicated OTEL logger.
            std::string body = self->snapshot_provider();
            std::cout << body << std::endl;
            self->do_otel_log_push();
        });
    }

    // -------------------------------------------------------------------
    // Prometheus text formatting
    // -------------------------------------------------------------------

    static void append_gauge(std::ostringstream &oss, const std::string &name,
                              const std::string &help, const std::string &labels,
                              std::int64_t value) {
        oss << "# HELP " << name << " " << help << "\n";
        oss << "# TYPE " << name << " gauge\n";
        oss << name << labels << " " << value << "\n";
    }

    std::string TelemetryExporter::format_prometheus_metrics(const RaftNode &node) {
        std::ostringstream oss;
        std::string peer_id = node.peer_id();
        std::string labels = "{peer_id=\"" + peer_id + "\"}";

        int state_num = 0;
        switch (node.role()) {
            case RaftNode::Role::follower:  state_num = 0; break;
            case RaftNode::Role::candidate: state_num = 1; break;
            case RaftNode::Role::leader:    state_num = 2; break;
        }

        auto joint = node.joint_consensus() ? 1 : 0;
        auto decommissioned = node.decommissioned() ? 1 : 0;
        auto voting = node.current_member_specs().size();
        auto total = voting;

        // Count followers caught up
        std::int64_t followers_replicating = 0;
        auto progress = node.peer_progress();
        auto ci = node.commit_index();
        for (const auto &[pid, pp] : progress) {
            if (pp.match_index >= ci) followers_replicating++;
        }

        // Count pending joins from persistent state
        auto ps = node.persistent_state();
        auto pending_joins = static_cast<std::int64_t>(ps.pending_join_ids.size());

        auto reconfig_age = node.reconfiguration_started_at_millis();

        append_gauge(oss, "raft_term", "Current Raft term.", labels, node.current_term());
        append_gauge(oss, "raft_state", "Node state (0=follower,1=candidate,2=leader).", labels, state_num);
        append_gauge(oss, "raft_commit_index", "Latest committed log index.", labels, node.commit_index());
        append_gauge(oss, "raft_last_applied", "Last applied log index.", labels, node.last_applied());
        append_gauge(oss, "raft_last_log_index", "Last log index.", labels, node.last_log_index());
        append_gauge(oss, "raft_last_log_term", "Last log term.", labels, node.last_log_term());
        append_gauge(oss, "raft_snapshot_index", "Snapshot index.", labels, node.snapshot_index());
        append_gauge(oss, "raft_snapshot_term", "Snapshot term.", labels, node.snapshot_term());
        append_gauge(oss, "raft_members_total", "Total cluster members.", labels, static_cast<std::int64_t>(total));
        append_gauge(oss, "raft_voting_members", "Voting members.", labels, static_cast<std::int64_t>(voting));
        append_gauge(oss, "raft_joint_consensus", "Joint consensus active (1=yes).", labels, joint);
        append_gauge(oss, "raft_followers_replicating", "Followers caught up to commit index.", labels, followers_replicating);
        append_gauge(oss, "raft_pending_joins", "Number of pending join requests.", labels, pending_joins);
        append_gauge(oss, "raft_decommissioned", "Node is decommissioned (1=yes).", labels, decommissioned);
        append_gauge(oss, "raft_reconfiguration_age_millis", "Milliseconds since reconfig started.", labels, reconfig_age);

        return oss.str();
    }

    // -------------------------------------------------------------------
    // OTEL JSON log formatting
    // -------------------------------------------------------------------

    static std::string json_escape(const std::string &s) {
        std::string out;
        out.reserve(s.size());
        for (char c : s) {
            switch (c) {
                case '"':  out += "\\\""; break;
                case '\\': out += "\\\\"; break;
                case '\n': out += "\\n";  break;
                case '\r': out += "\\r";  break;
                case '\t': out += "\\t";  break;
                default:   out += c;      break;
            }
        }
        return out;
    }

    static std::string role_to_string(RaftNode::Role role) {
        switch (role) {
            case RaftNode::Role::follower:  return "Follower";
            case RaftNode::Role::candidate: return "Candidate";
            case RaftNode::Role::leader:    return "Leader";
        }
        return "Follower";
    }

    std::string TelemetryExporter::format_otel_json_log(const RaftNode &node) {
        auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::system_clock::now().time_since_epoch())
                       .count();

        auto member_specs = node.current_member_specs();
        auto next_specs = node.next_member_specs();
        auto ps = node.persistent_state();

        std::ostringstream json;
        json << "{";
        json << "\"exporter\":\"otel-log\",";
        json << "\"observedAtMillis\":" << now << ",";
        json << "\"peerId\":\"" << json_escape(node.peer_id()) << "\",";
        json << "\"state\":\"" << role_to_string(node.role()) << "\",";
        json << "\"term\":" << node.current_term() << ",";
        json << "\"leaderId\":\"" << json_escape(node.leader_id().value_or("")) << "\",";
        json << "\"commitIndex\":" << node.commit_index() << ",";
        json << "\"lastApplied\":" << node.last_applied() << ",";
        json << "\"lastLogIndex\":" << node.last_log_index() << ",";
        json << "\"lastLogTerm\":" << node.last_log_term() << ",";
        json << "\"snapshotIndex\":" << node.snapshot_index() << ",";
        json << "\"snapshotTerm\":" << node.snapshot_term() << ",";
        json << "\"jointConsensus\":" << (node.joint_consensus() ? "true" : "false") << ",";
        json << "\"decommissioned\":" << (node.decommissioned() ? "true" : "false") << ",";
        json << "\"knownPeers\":" << member_specs.size() << ",";
        json << "\"currentMembers\":" << member_specs.size() << ",";
        json << "\"nextMembers\":" << next_specs.size() << ",";
        json << "\"pendingJoinIds\":" << ps.pending_join_ids.size() << ",";
        json << "\"reconfigurationAgeMillis\":" << node.reconfiguration_started_at_millis();
        json << "}";

        return json.str();
    }

} // namespace graft
