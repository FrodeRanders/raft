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

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[cfg(feature = "otlp")]
use std::time::{SystemTime, UNIX_EPOCH};

use graft_core::raft_node::RaftNode;
use graft_core::types::NodeState;
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tracing::{info, warn};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Supported telemetry exporters, matching the Java `raft-telemetry`
/// module configuration pattern.
pub enum TelemetryExporter {
    /// No telemetry export (default).
    None,
    /// Prometheus scrape endpoint.
    Prometheus {
        host: String,
        port: u16,
        path: String,
    },
    /// OTLP HTTP push (requires `otlp` feature).
    #[cfg(feature = "otlp")]
    Otlp {
        endpoint: String,
        headers: Vec<(String, String)>,
        timeout: Duration,
    },
    /// OTEL JSON log — writes single-line JSON to the tracing INFO
    /// level, matching the Java OpenTelemetryJsonExporter pattern.
    OtelJsonLog,
}

/// Reads telemetry configuration from environment variables.
///
/// Java property → env var mapping:
/// - `raft.telemetry.exporter` → `RAFT_TELEMETRY_EXPORTER`
/// - `raft.telemetry.prometheus.host` → `RAFT_TELEMETRY_PROMETHEUS_HOST`
/// - `raft.telemetry.prometheus.port` → `RAFT_TELEMETRY_PROMETHEUS_PORT`
/// - `raft.telemetry.prometheus.path` → `RAFT_TELEMETRY_PROMETHEUS_PATH`
/// - `raft.telemetry.otlp.endpoint` → `RAFT_TELEMETRY_OTLP_ENDPOINT`
/// - `raft.telemetry.otlp.headers` → `RAFT_TELEMETRY_OTLP_HEADERS`
/// - `raft.telemetry.otlp.timeout.millis` → `RAFT_TELEMETRY_OTLP_TIMEOUT_MILLIS`
/// - `raft.telemetry.export.interval.seconds` → `RAFT_TELEMETRY_EXPORT_INTERVAL_SECONDS`
pub fn telemetry_exporter_from_env() -> TelemetryExporter {
    let exporter = std::env::var("RAFT_TELEMETRY_EXPORTER")
        .unwrap_or_else(|_| "none".to_string())
        .to_lowercase();

    match exporter.as_str() {
        "prometheus" => {
            let host = std::env::var("RAFT_TELEMETRY_PROMETHEUS_HOST")
                .unwrap_or_else(|_| "127.0.0.1".to_string());
            let port: u16 = std::env::var("RAFT_TELEMETRY_PROMETHEUS_PORT")
                .unwrap_or_else(|_| "9108".to_string())
                .parse()
                .unwrap_or(9108);
            let path = std::env::var("RAFT_TELEMETRY_PROMETHEUS_PATH")
                .unwrap_or_else(|_| "/metrics".to_string());
            TelemetryExporter::Prometheus { host, port, path }
        }
        #[cfg(feature = "otlp")]
        "opentelemetry" | "otlp" => {
            let endpoint = std::env::var("RAFT_TELEMETRY_OTLP_ENDPOINT")
                .unwrap_or_else(|_| "http://127.0.0.1:4318/v1/metrics".to_string());
            let timeout_ms: u64 = std::env::var("RAFT_TELEMETRY_OTLP_TIMEOUT_MILLIS")
                .unwrap_or_else(|_| "2000".to_string())
                .parse()
                .unwrap_or(2000);
            let headers = std::env::var("RAFT_TELEMETRY_OTLP_HEADERS")
                .unwrap_or_default()
                .split(',')
                .filter_map(|s| {
                    let parts: Vec<&str> = s.trim().splitn(2, '=').collect();
                    if parts.len() == 2 {
                        Some((parts[0].trim().to_string(), parts[1].trim().to_string()))
                    } else {
                        None
                    }
                })
                .collect();
            TelemetryExporter::Otlp {
                endpoint,
                headers,
                timeout: Duration::from_millis(timeout_ms),
            }
        }
        "otel-log" | "opentelemetry-log" => TelemetryExporter::OtelJsonLog,
        _ => TelemetryExporter::None,
    }
}

fn export_interval_from_env() -> Duration {
    let seconds: u64 = std::env::var("RAFT_TELEMETRY_EXPORT_INTERVAL_SECONDS")
        .unwrap_or_else(|_| "15".to_string())
        .parse()
        .unwrap_or(15);
    Duration::from_secs(seconds)
}

// ---------------------------------------------------------------------------
// Prometheus text formatting
// ---------------------------------------------------------------------------

/// Formats a telemetry snapshot as Prometheus exposition format.
fn format_prometheus_metrics(node: &RaftNode) -> String {
    let peer_id = &node.me.id;
    let state_num = match node.state {
        NodeState::Follower => 0,
        NodeState::Candidate => 1,
        NodeState::Leader => 2,
    };
    let joint = if node.cluster_configuration.is_joint_consensus() {
        1
    } else {
        0
    };
    let _voted_for = node.voted_for.as_deref().unwrap_or("");
    let _leader_id = node.known_leader_id.as_deref().unwrap_or("");

    let ci = node.commit_index;
    let la = node.last_applied;
    let li = node.log_store.last_index();
    let lt = node.log_store.last_term();
    let si = node.log_store.snapshot_index();
    let st = node.log_store.snapshot_term();
    let voting = node
        .cluster_configuration
        .current_voting_members()
        .len();
    let total = node.cluster_configuration.current_members().len();
    let joined = node.joining;
    let decommissioned = node.decommissioned;
    let pending_joins = node.pending_join_ids.len();

    // Count followers caught up to commit index
    let mut followers_replicating: i64 = 0;
    for (_pid, mi) in node.match_index.iter() {
        if *mi >= ci {
            followers_replicating += 1;
        }
    }

    let reconfig_age = node
        .configuration_transition_started
        .map(|s| s.elapsed().as_millis() as i64)
        .unwrap_or(0);

    let mut buf = String::new();

    macro_rules! gauge {
        ($name:expr, $help:expr, $value:expr) => {
            buf.push_str(&format!(
                "# HELP {} {}\n# TYPE {} gauge\n{}{{peer_id=\"{}\"}} {}\n",
                $name, $help, $name, $name, peer_id, $value
            ));
        };
    }

    // Raft state metrics (matching Java Prometheus exporter)
    gauge!("raft_term", "Current Raft term.", node.current_term);
    gauge!("raft_state", "Node state (0=follower, 1=candidate, 2=leader).", state_num);
    gauge!("raft_commit_index", "Latest committed log index.", ci);
    gauge!("raft_last_applied", "Last applied log index.", la);
    gauge!("raft_last_log_index", "Last log index.", li);
    gauge!("raft_last_log_term", "Last log term.", lt);
    gauge!("raft_snapshot_index", "Snapshot index.", si);
    gauge!("raft_snapshot_term", "Snapshot term.", st);
    gauge!("raft_members_total", "Total cluster members.", total);
    gauge!("raft_voting_members", "Voting members.", voting);
    gauge!("raft_joint_consensus", "Joint consensus active (1=yes, 0=no).", joint);
    gauge!("raft_followers_replicating", "Number of followers caught up to commit index.", followers_replicating);
    gauge!("raft_pending_joins", "Number of pending join requests.", pending_joins);
    gauge!("raft_joining", "Node is joining (1=yes, 0=no).", if joined { 1 } else { 0 });
    gauge!("raft_decommissioned", "Node is decommissioned (1=yes, 0=no).", if decommissioned { 1 } else { 0 });
    gauge!("raft_reconfiguration_age_millis", "Milliseconds since reconfiguration started.", reconfig_age);

    buf
}

// ---------------------------------------------------------------------------
// Prometheus HTTP scrape endpoint
// ---------------------------------------------------------------------------

/// Formats a telemetry snapshot as a single-line JSON string matching
/// the Java OpenTelemetryJsonExporter structure.
fn format_otel_json_log(node: &RaftNode) -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;

    let state = format!("{:?}", node.state);
    let leader_id = node.known_leader_id.as_deref().unwrap_or("");
    let voting = node.cluster_configuration.current_voting_members().len();
    let total = node.cluster_configuration.current_members().len();

    let json = serde_json::json!({
        "exporter": "otel-log",
        "observedAtMillis": now,
        "peerId": node.me.id,
        "state": state,
        "term": node.current_term,
        "leaderId": leader_id,
        "commitIndex": node.commit_index,
        "lastApplied": node.last_applied,
        "lastLogIndex": node.log_store.last_index(),
        "lastLogTerm": node.log_store.last_term(),
        "snapshotIndex": node.log_store.snapshot_index(),
        "snapshotTerm": node.log_store.snapshot_term(),
        "jointConsensus": node.cluster_configuration.is_joint_consensus(),
        "joining": node.joining,
        "decommissioned": node.decommissioned,
        "knownPeers": total,
        "currentMembers": voting,
        "nextMembers": node.cluster_configuration.next_members().len(),
        "pendingJoinIds": node.pending_join_ids.len(),
        "reconfigurationAgeMillis": node
            .configuration_transition_started
            .map(|s| s.elapsed().as_millis() as i64)
            .unwrap_or(0),
    });

    json.to_string()
}

/// Serves Prometheus metrics over HTTP using a raw TCP listener.
/// This avoids pulling in a full HTTP framework — tokio is already a
/// dependency and the Prometheus scrape protocol is trivial.
async fn serve_prometheus(
    node: Arc<Mutex<RaftNode>>,
    bind_addr: SocketAddr,
    metrics_path: &str,
) {
    let listener = match TcpListener::bind(bind_addr).await {
        Ok(l) => l,
        Err(e) => {
            warn!("Prometheus listener bind failed on {}: {}", bind_addr, e);
            return;
        }
    };
    info!(
        "Prometheus metrics endpoint listening on http://{}{}",
        bind_addr, metrics_path
    );

    let metrics_path = metrics_path.to_string();

    loop {
        let (mut socket, _peer_addr) = match listener.accept().await {
            Ok(conn) => conn,
            Err(e) => {
                warn!("Prometheus accept error: {}", e);
                continue;
            }
        };

        let node = node.clone();
        let path = metrics_path.clone();

        tokio::spawn(async move {
            let mut buf = vec![0u8; 4096];
            let read_result =
                tokio::time::timeout(Duration::from_secs(5), socket.read(&mut buf)).await;

            let (status, body) = match read_result {
                Ok(Ok(n)) if n > 0 => {
                    let request = String::from_utf8_lossy(&buf[..n]);
                    let first_line = request.lines().next().unwrap_or("");
                    let parts: Vec<&str> = first_line.split_whitespace().collect();

                    if parts.len() >= 2 && parts[0] == "GET" && parts[1] == path {
                        let metrics = format_prometheus_metrics(&node.lock());
                        ("200 OK", metrics)
                    } else if parts.len() >= 2 && parts[0] == "GET" {
                        ("404 Not Found", "Not Found\n".to_string())
                    } else {
                        ("405 Method Not Allowed", "Method Not Allowed\n".to_string())
                    }
                }
                _ => return,
            };

            let response = format!(
                "HTTP/1.1 {}\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                status,
                body.len(),
                body
            );
            let _ = socket.write_all(response.as_bytes()).await;
            let _ = socket.shutdown().await;
        });
    }
}

// ---------------------------------------------------------------------------
// OTLP HTTP push (optional, behind `otlp` feature)
// ---------------------------------------------------------------------------

#[cfg(feature = "otlp")]
fn format_otlp_json(node: &RaftNode) -> String {
    let now_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .to_string();

    let peer_id = &node.me.id;
    let state_num = match node.state {
        NodeState::Follower => 0,
        NodeState::Candidate => 1,
        NodeState::Leader => 2,
    };

    let joint = if node.cluster_configuration.is_joint_consensus() {
        1
    } else {
        0
    };

    let reconfig_age = node
        .configuration_transition_started
        .map(|s| s.elapsed().as_millis() as i64)
        .unwrap_or(0);

    let voting = node
        .cluster_configuration
        .current_voting_members()
        .len();
    let total = node.cluster_configuration.current_members().len();

    serde_json::json!({
        "resourceMetrics": [{
            "resource": {
                "attributes": [
                    {"key": "service.name", "value": {"stringValue": "graft"}},
                    {"key": "service.instance.id", "value": {"stringValue": peer_id}}
                ]
            },
            "scopeMetrics": [{
                "scope": {"name": "raft"},
                "metrics": [
                    {"name": "raft.term", "gauge": {"dataPoints": [{"timeUnixNano": now_ns, "asInt": node.current_term}]}},
                    {"name": "raft.state", "gauge": {"dataPoints": [{"timeUnixNano": now_ns, "asInt": state_num}]}},
                    {"name": "raft.commit.index", "gauge": {"dataPoints": [{"timeUnixNano": now_ns, "asInt": node.commit_index}]}},
                    {"name": "raft.last.applied", "gauge": {"dataPoints": [{"timeUnixNano": now_ns, "asInt": node.last_applied}]}},
                    {"name": "raft.last.log.index", "gauge": {"dataPoints": [{"timeUnixNano": now_ns, "asInt": node.log_store.last_index()}]}},
                    {"name": "raft.snapshot.index", "gauge": {"dataPoints": [{"timeUnixNano": now_ns, "asInt": node.log_store.snapshot_index()}]}},
                    {"name": "raft.members.total", "gauge": {"dataPoints": [{"timeUnixNano": now_ns, "asInt": total}]}},
                    {"name": "raft.members.voting", "gauge": {"dataPoints": [{"timeUnixNano": now_ns, "asInt": voting}]}},
                    {"name": "raft.members.joining", "gauge": {"dataPoints": [{"timeUnixNano": now_ns, "asInt": if node.joining {1} else {0}}]}},
                    {"name": "raft.members.decommissioned", "gauge": {"dataPoints": [{"timeUnixNano": now_ns, "asInt": if node.decommissioned {1} else {0}}]}},
                    {"name": "raft.reconfiguration.age.millis", "gauge": {"dataPoints": [{"timeUnixNano": now_ns, "asInt": reconfig_age}]}}
                ]
            }]
        }]
    })
    .to_string()
}

#[cfg(feature = "otlp")]
async fn push_otlp(
    node: Arc<Mutex<RaftNode>>,
    endpoint: &str,
    headers: &[(String, String)],
    timeout: Duration,
    interval: Duration,
) {
    let client = match reqwest::Client::builder().timeout(timeout).build() {
        Ok(c) => c,
        Err(e) => {
            warn!("Failed to create OTLP HTTP client: {}", e);
            return;
        }
    };

    info!("OTLP telemetry export to {}", endpoint);

    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        ticker.tick().await;

        let json = format_otlp_json(&node.lock());

        let mut req = client
            .post(endpoint)
            .header("Content-Type", "application/json");
        for (key, value) in headers {
            req = req.header(key.as_str(), value.as_str());
        }

        match req.body(json).send().await {
            Ok(resp) => {
                if !resp.status().is_success() {
                    warn!("OTLP export returned HTTP {}", resp.status());
                }
            }
            Err(e) => {
                warn!("OTLP export failed: {}", e);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// OTEL JSON log export
// ---------------------------------------------------------------------------

/// Periodically formats a telemetry snapshot as a single-line JSON string
/// and writes it to the tracing INFO level.  This matches the Java
/// `OpenTelemetryJsonExporter` pattern of emitting to a named logger.
async fn publish_otel_log(node: Arc<Mutex<RaftNode>>, interval: Duration) {
    info!("OTEL JSON log telemetry export started (interval={}s)", interval.as_secs());

    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        ticker.tick().await;
        let json = format_otel_json_log(&node.lock());
        tracing::info!(target: "OTEL", "{}", json);
    }
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Background telemetry publisher. Starts Prometheus scrape endpoint
/// or OTLP push loop depending on the configured exporter.
///
/// Usage:
/// ```ignore
/// let exporter = graft_telemetry::telemetry_exporter_from_env();
/// let publisher = graft_telemetry::TelemetryPublisher::new(node.clone(), exporter);
/// publisher.start();
/// ```
pub struct TelemetryPublisher {
    node: Arc<Mutex<RaftNode>>,
    exporter: TelemetryExporter,
}

impl TelemetryPublisher {
    pub fn new(node: Arc<Mutex<RaftNode>>, exporter: TelemetryExporter) -> Self {
        Self { node, exporter }
    }

    /// Returns the Prometheus bind address if the exporter is configured
    /// for Prometheus. Useful for logging at startup.
    pub fn prometheus_bind_addr(&self) -> Option<SocketAddr> {
        match &self.exporter {
            TelemetryExporter::Prometheus { host, port, .. } => Some(SocketAddr::new(
                host.parse().unwrap_or_else(|_| "127.0.0.1".parse().unwrap()),
                *port,
            )),
            _ => None,
        }
    }

    /// Starts the telemetry export. Spawns background tokio tasks as
    /// needed. Does not block the caller.
    pub fn start(self) {
        match &self.exporter {
            TelemetryExporter::None => {
                info!("Telemetry export disabled (RAFT_TELEMETRY_EXPORTER=none)");
            }
            TelemetryExporter::Prometheus { host, port, path } => {
                let bind_addr = SocketAddr::new(
                    host.parse().unwrap_or_else(|_| "127.0.0.1".parse().unwrap()),
                    *port,
                );
                let node = self.node.clone();
                let path = path.clone();
                tokio::spawn(async move {
                    serve_prometheus(node, bind_addr, &path).await;
                });
            }
            #[cfg(feature = "otlp")]
            TelemetryExporter::Otlp {
                endpoint,
                headers,
                timeout,
            } => {
                let node = self.node.clone();
                let interval = export_interval_from_env();
                tokio::spawn(async move {
                    push_otlp(node, &endpoint, &headers, timeout, interval).await;
                });
            }
            TelemetryExporter::OtelJsonLog => {
                let node = self.node.clone();
                let interval = export_interval_from_env();
                tokio::spawn(async move {
                    publish_otel_log(node, interval).await;
                });
            }
        }
    }
}
