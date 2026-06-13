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
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace graft {

    class RaftNode;

    // -----------------------------------------------------------------------
    // TelemetryExporter — Prometheus scrape endpoint and OTLP push export.
    //
    // Reads configuration from RAFT_TELEMETRY_* environment variables,
    // matching the Java raft.telemetry.* property pattern and the Rust
    // TelemetryPublisher.
    //
    // Usage:
    //   auto exporter = TelemetryExporter::from_env();
    //   exporter.start(io_context, [&node] {
    //       return TelemetryExporter::format_prometheus_metrics(node);
    //   });
    // -----------------------------------------------------------------------
    class TelemetryExporter {
    public:
        enum class Type { None, Prometheus, Otlp, OtelJsonLog };

        struct PrometheusConfig {
            std::string host{"127.0.0.1"};
            std::uint16_t port{9108};
            std::string path{"/metrics"};
        };

        struct OtlpConfig {
            std::string endpoint{"http://127.0.0.1:4318/v1/metrics"};
            std::vector<std::pair<std::string, std::string>> headers;
            std::chrono::milliseconds timeout{2000};
        };

        // Callback that returns a current telemetry snapshot. Called on
        // each Prometheus scrape or OTLP push interval.
        using SnapshotProvider = std::function<std::string()>;

        // Reads configuration from environment variables. Returns a
        // no-op exporter when RAFT_TELEMETRY_EXPORTER is unset or "none".
        static TelemetryExporter from_env();

        // Copyable wrapper around the implementation.
        TelemetryExporter();
        ~TelemetryExporter();

        TelemetryExporter(const TelemetryExporter &) = default;
        TelemetryExporter &operator=(const TelemetryExporter &) = default;
        TelemetryExporter(TelemetryExporter &&) noexcept = default;
        TelemetryExporter &operator=(TelemetryExporter &&) noexcept = default;

        // Starts the exporter. If type is Prometheus, starts an
        // HTTP listener on the configured port. If OTLP, starts a
        // periodic push timer. io_context must stay alive.
        void start(boost::asio::io_context &io, SnapshotProvider provider);

        // Returns true if telemetry export is enabled.
        [[nodiscard]] bool enabled() const;

        [[nodiscard]] Type type() const { return type_; }
        [[nodiscard]] const PrometheusConfig &prometheus_config() const { return prometheus_; }

        // Formats a Prometheus exposition text representation of the
        // current Raft node state. Called by snapshot providers to
        // serialize metrics for scraping.
        static std::string format_prometheus_metrics(const RaftNode &node);

        // Formats a single-line OTEL-compatible JSON string from the
        // current Raft node state.  Matches the Java
        // OpenTelemetryJsonExporter structure.
        static std::string format_otel_json_log(const RaftNode &node);

    private:
        Type type_{Type::None};
        PrometheusConfig prometheus_;
        OtlpConfig otlp_;
        std::chrono::seconds export_interval_{15};

        struct Impl;
        std::shared_ptr<Impl> impl_;
    };

} // namespace graft
