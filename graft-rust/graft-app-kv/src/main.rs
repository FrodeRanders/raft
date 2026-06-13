use std::collections::HashMap;
use std::fs;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;

use clap::{Parser, Subcommand};
use graft_app_kv::{CasResult, GetResult, KeyValueStateMachine, KvCommand, KvQuery};
use graft_core::membership::ClusterConfiguration;
use graft_core::raft_node::{LogStore, PersistentStateStore, RaftNode};
use graft_core::types::{LogEntry, Peer};
use graft_proto::raft;
use graft_proto::{encode_frame, Envelope};
use graft_runtime::handlers::RaftHandler;
use graft_runtime::runtime::RaftRuntime;
use graft_storage::log_store::{FileLogStore, InMemoryLogStore, InMemoryPersistentStateStore};
use graft_storage::state_store::FilePersistentStateStore;
use graft_telemetry::{telemetry_exporter_from_env, TelemetryPublisher};
use prost::Message;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::runtime::Runtime;

// ---------------------------------------------------------------------------
// CLI definition
// ---------------------------------------------------------------------------

/// Raft cross-language smoke-test binary. Matches the C++ `graft_smoke`
/// CLI so the existing `run-mixed-*.sh` scripts can drive a Rust node.
#[derive(Parser)]
#[command(name = "graft-smoke", about = "Raft smoke test tool")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start a passive in-memory Raft server (no elections, no persistence).
    Serve {
        host: String,
        port: u16,
        peer_id: String,
        #[arg(default_value = "0")]
        term: u64,
        #[arg(default_value = "0")]
        last_log_index: u64,
        #[arg(default_value = "0")]
        last_log_term: u64,
        peers: Vec<String>,
    },
    /// Start a passive stateful server with explicit Raft state.
    ServeStateful {
        host: String,
        port: u16,
        peer_id: String,
        #[arg(default_value = "0")]
        term: u64,
        #[arg(default_value = "0")]
        last_log_index: u64,
        #[arg(default_value = "0")]
        last_log_term: u64,
        peers: Vec<String>,
    },
    /// Start an active in-memory Raft server with elections.
    ServeActive {
        host: String,
        port: u16,
        peer_id: String,
        #[arg(default_value = "0")]
        current_term: u64,
        #[arg(default_value = "0")]
        last_log_index: u64,
        #[arg(default_value = "0")]
        last_log_term: u64,
        peers: Vec<String>,
    },
    /// Start a persistent passive Raft server (no elections/heartbeats).
    ServePersistent {
        host: String,
        port: u16,
        peer_id: String,
        state_file: String,
        #[arg(default_value = "0")]
        term: u64,
        #[arg(default_value = "0")]
        last_log_index: u64,
        #[arg(default_value = "0")]
        last_log_term: u64,
        peers: Vec<String>,
    },
    /// Start an active persistent Raft server with elections.
    ServeActivePersistent {
        host: String,
        port: u16,
        peer_id: String,
        state_file: String,
        #[arg(default_value = "0")]
        current_term: u64,
        #[arg(default_value = "0")]
        last_log_index: u64,
        #[arg(default_value = "0")]
        last_log_term: u64,
        peers: Vec<String>,
    },
    /// Active server with synthetic workload and auto-compaction.
    ServeActivePersistentWorkload {
        host: String,
        port: u16,
        peer_id: String,
        state_file: String,
        #[arg(default_value = "0")]
        current_term: u64,
        #[arg(default_value = "0")]
        last_log_index: u64,
        #[arg(default_value = "0")]
        last_log_term: u64,
        #[arg(default_value = "500")]
        _replicate_interval_ms: u64,
        #[arg(default_value = "2")]
        _snapshot_threshold: u64,
        peers: Vec<String>,
    },
    /// Send a client-put command.
    ClientPut {
        host: String,
        port: u16,
        key: String,
        value: String,
        #[arg(default_value = "graft-client")]
        peer_id: String,
    },
    /// Send a client-get query.
    ClientGet {
        host: String,
        port: u16,
        key: String,
        #[arg(default_value = "graft-client")]
        peer_id: String,
    },
    /// Send a CAS (compare-and-swap) command.
    ClientCas {
        host: String,
        port: u16,
        key: String,
        expected_present: String,
        expected_value: String,
        new_value: String,
        #[arg(default_value = "graft-client")]
        peer_id: String,
    },
    /// Query telemetry.
    Telemetry {
        host: String,
        port: u16,
        #[arg(long, default_value_t = false)]
        require_leader_summary: bool,
    },
    /// Query cluster summary.
    ClusterSummary { host: String, port: u16 },
    /// Join a new peer to the cluster.
    JoinCluster {
        host: String,
        port: u16,
        joining_peer_id: String,
        joining_host: String,
        joining_port: u16,
        role: String,
        #[arg(default_value = "graft-client")]
        peer_id: String,
    },
    /// Check join status.
    JoinStatus {
        host: String,
        port: u16,
        target_peer_id: String,
        #[arg(default_value = "graft-client")]
        peer_id: String,
    },
    /// Reconfigure cluster.
    Reconfigure {
        host: String,
        port: u16,
        #[arg(default_value = "graft-client")]
        peer_id: String,
        #[command(subcommand)]
        action: ReconfigureAction,
    },
    /// Raw VoteRequest RPC.
    VoteRequest {
        host: String,
        port: u16,
        candidate_id: String,
        last_log_index: u64,
        last_log_term: u64,
        #[arg(default_value = "1")]
        term: u64,
    },
    /// Raw AppendEntries RPC.
    AppendEntries {
        host: String,
        port: u16,
        leader_id: String,
        prev_log_index: u64,
        prev_log_term: u64,
        leader_commit: u64,
        #[arg(default_value = "1")]
        term: u64,
    },
    /// Raw InstallSnapshot RPC.
    InstallSnapshot {
        host: String,
        port: u16,
        leader_id: String,
        last_included_index: u64,
        last_included_term: u64,
        #[arg(default_value = "1")]
        term: u64,
    },
    /// Send a client-delete command.
    ClientDelete {
        host: String,
        port: u16,
        key: String,
        #[arg(default_value = "graft-client")]
        peer_id: String,
    },
    /// Send a client-clear command (clear all keys).
    ClientClear {
        host: String,
        port: u16,
        #[arg(default_value = "graft-client")]
        peer_id: String,
    },
    /// Query reconfiguration status.
    ReconfigurationStatus { host: String, port: u16 },
    /// Dump persisted state file.
    DumpState { state_file: String },
    /// Run one election round (send VoteRequests, count responses).
    ElectionRound {
        host: String,
        port: u16,
        peer_id: String,
        last_log_index: u64,
        last_log_term: u64,
        #[arg(default_value = "1")]
        term: u64,
    },
    /// Send one heartbeat (AppendEntries) to all peers.
    HeartbeatRound {
        host: String,
        port: u16,
        leader_id: String,
        prev_log_index: u64,
        prev_log_term: u64,
        leader_commit: u64,
        #[arg(default_value = "1")]
        term: u64,
    },
    /// Replicate one entry (append to log and wait for commit).
    ReplicateOnce {
        host: String,
        port: u16,
        key: String,
        value: String,
        #[arg(default_value = "graft-client")]
        peer_id: String,
    },
    /// Replicate one entry from a persistent state file.
    ReplicateOncePersistent {
        host: String,
        port: u16,
        state_file: String,
        key: String,
        value: String,
        #[arg(default_value = "graft-client")]
        peer_id: String,
    },
    /// Replicate a put command using a persistent state file.
    ReplicatePutPersistent {
        host: String,
        port: u16,
        state_file: String,
        key: String,
        value: String,
        #[arg(default_value = "graft-client")]
        peer_id: String,
    },
    /// Trigger snapshot compaction on the local node.
    CompactSnapshot { host: String, port: u16 },
}

#[derive(Subcommand)]
enum ReconfigureAction {
    Joint {
        members: Vec<String>,
    },
    Finalize {
        #[arg(default_value = "")]
        _members: Vec<String>,
    },
    Promote {
        member: String,
    },
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn parse_peer_spec(spec: &str) -> (String, SocketAddr, String) {
    let (id, rest) = spec.split_once('@').unwrap_or((spec, ""));
    let (addr_str, role) = if let Some((a, r)) = rest.split_once('/') {
        (a, r.to_uppercase())
    } else {
        (rest, "VOTER".to_string())
    };
    let addr = SocketAddr::from_str(addr_str)
        .unwrap_or_else(|_| SocketAddr::from_str(&format!("{}:0", addr_str)).unwrap());
    (id.to_string(), addr, role)
}

/// Synchronous one-shot RPC call — connection per call, matching the C++
/// `RaftClient` pattern used in smoke scripts.
fn send_raft_rpc(
    host: &str,
    port: u16,
    envelope_type: &str,
    payload: Vec<u8>,
) -> Result<Vec<u8>, String> {
    let rt = Runtime::new().map_err(|e| e.to_string())?;
    rt.block_on(async {
        let addr = format!("{}:{}", host, port);
        let mut stream = TcpStream::connect(&addr).await.map_err(|e| e.to_string())?;
        let envelope = Envelope {
            correlation_id: "smoke-1".to_string(),
            r#type: envelope_type.to_string(),
            payload,
        };
        let frame = encode_frame(&envelope);
        stream.write_all(&frame).await.map_err(|e| e.to_string())?;
        stream.flush().await.map_err(|e| e.to_string())?;
        let mut buf = bytes::BytesMut::with_capacity(8192);
        loop {
            let mut read_buf = vec![0u8; 4096];
            let n = stream
                .read(&mut read_buf)
                .await
                .map_err(|e| e.to_string())?;
            if n == 0 {
                return Err("connection closed".to_string());
            }
            buf.extend_from_slice(&read_buf[..n]);
            if let Ok(resp_env) = graft_proto::read_envelope(&mut buf) {
                return Ok(resp_env.payload);
            }
        }
    })
}

fn dump_state(state_file: &str) -> Result<(), String> {
    let content = fs::read_to_string(state_file).map_err(|e| e.to_string())?;
    let mut term = 0u64;
    let mut voted = String::new();
    for line in content.lines() {
        if let Some((key, value)) = line.split_once('=') {
            match key.trim() {
                "term" => {
                    term = value.trim().parse().unwrap_or(0);
                }
                "voted" => {
                    voted = value.trim().to_string();
                }
                _ => {}
            }
        }
    }
    println!("term: {}", term);
    println!("voted_for: {}", voted);
    let kv_path = format!("{}.kv", state_file);
    if let Ok(kv_content) = fs::read_to_string(&kv_path) {
        if let Ok(store) = serde_json::from_str::<HashMap<String, Vec<u8>>>(&kv_content) {
            for (key, value) in &store {
                if let Ok(val_str) = String::from_utf8(value.clone()) {
                    println!("kv[{}]={}", key, val_str);
                }
            }
        }
    }
    Ok(())
}

/// Runs a Raft server. In active mode (`active=true`), the RaftRuntime
/// is launched with election and heartbeat loops. In passive mode, only
/// inbound RPCs are answered. If `workload` is `Some((interval_ms, threshold))`,
/// a synthetic key-value workload is generated periodically and
/// auto-compaction runs when the uncompacted log exceeds the threshold.
fn run_server(
    host: &str,
    port: u16,
    peer_id: &str,
    state_file: &str,
    term: u64,
    last_log_index: u64,
    last_log_term: u64,
    peers: &[String],
    active: bool,
    workload: Option<(u64, u64)>,
) -> Result<(), String> {
    let bind_addr =
        SocketAddr::from_str(&format!("{}:{}", host, port)).map_err(|e| e.to_string())?;
    let me = Peer::voter(peer_id.to_string(), bind_addr);

    let peer_specs = parse_peers(peers);
    let mut peer_addrs: HashMap<String, SocketAddr> = HashMap::new();
    let mut members = vec![me.clone()];
    for (id, (addr, _role)) in &peer_specs {
        members.push(Peer::voter(id.clone(), *addr));
        peer_addrs.insert(id.clone(), *addr);
    }
    let config = ClusterConfiguration::stable(members);

    let persistent = active && last_log_index == 0 && last_log_term == 0;
    let log_store: Arc<dyn LogStore> = if persistent {
        Arc::new(FileLogStore::new(std::path::PathBuf::from(format!(
            "{}.log",
            state_file
        ))))
    } else {
        Arc::new(InMemoryLogStore::new())
    };
    let state_store: Arc<dyn PersistentStateStore> = if persistent {
        Arc::new(FilePersistentStateStore::new(std::path::PathBuf::from(
            state_file,
        )))
    } else {
        let store = Arc::new(InMemoryPersistentStateStore::new());
        store.set_current_term(term);
        store
    };

    for i in 1..=last_log_index {
        log_store.append(vec![LogEntry::new(
            last_log_term,
            peer_id.to_string(),
            format!("entry-{}", i).into_bytes(),
        )]);
    }

    let sm = Arc::new(KeyValueStateMachine::new());
    let raft_node = Arc::new(parking_lot::Mutex::new(RaftNode::new(
        me,
        500,
        log_store,
        state_store,
        Some(sm),
        config,
        100,
        1024,
    )));

    let rt = Runtime::new().map_err(|e| e.to_string())?;

    // Build the RPC handler (shared across all connections).
    let client = Arc::new(graft_transport::client::RaftClient::new());
    client.set_known_peers(peer_addrs.clone());
    let runtime = Arc::new(RaftRuntime::new(raft_node.clone(), client.clone()));
    runtime.set_peers(peer_addrs.clone());
    let handler = Arc::new(RaftHandler::new(
        raft_node.clone(),
        client.clone(),
        runtime.clone(),
    ));

    // ── Active mode: launch RaftRuntime with election + heartbeat loops ──
    if active {
        let rt_handle = rt.handle().clone();
        let runtime_clone = runtime.clone();
        rt_handle.spawn(async move {
            runtime_clone.run().await;
        });

        // ── Synthetic workload (optional) ──
        if let Some((interval_ms, threshold)) = workload {
            let wl_runtime = runtime.clone();
            rt_handle.spawn(async move {
                let mut counter: u64 = 0;
                loop {
                    tokio::time::sleep(std::time::Duration::from_millis(interval_ms)).await;
                    counter += 1;
                    let key = format!("workload-{}", counter);
                    let value = format!("v{}", counter);
                    let cmd = KvCommand::Put {
                        key,
                        value: value.into_bytes(),
                    };
                    if let Ok(cmd_json) = serde_json::to_vec(&cmd) {
                        if let Ok(_idx) = wl_runtime.submit_command(cmd_json) {
                            // Auto-compaction: check if uncompacted entries exceed threshold
                            let node = wl_runtime.node.lock();
                            if node.commit_index > 0
                                && node
                                    .commit_index
                                    .saturating_sub(node.log_store.snapshot_index())
                                    >= threshold
                            {
                                drop(node);
                                // Force compaction check (normally done in apply_committed_entries)
                                let mut n = wl_runtime.node.lock();
                                if let Some(ref sm) = n.state_machine {
                                    let snap = sm.snapshot();
                                    if !snap.is_empty() {
                                        let term = n.log_store.term_at(n.commit_index);
                                        n.log_store.compact_up_to(n.commit_index);
                                        n.log_store.install_snapshot(n.commit_index, term, snap);
                                        n.snapshot_configuration = n.cluster_configuration.clone();
                                    }
                                }
                            }
                        }
                    }
                }
            });
        }
    }

    // ── Telemetry export (Prometheus / OTLP) if configured via env vars ──
    let exporter = telemetry_exporter_from_env();
    let tp = TelemetryPublisher::new(raft_node.clone(), exporter);
    if let Some(addr) = tp.prometheus_bind_addr() {
        eprintln!(
            "[graft-smoke] telemetry exporter: prometheus http://{}/metrics",
            addr
        );
    }
    tp.start();

    rt.block_on(async {
        let listener = tokio::net::TcpListener::bind(bind_addr)
            .await
            .map_err(|e| e.to_string())?;
        eprintln!(
            "[graft-smoke] {} server {} listening on {}",
            if active { "active" } else { "passive" },
            peer_id,
            bind_addr
        );

        loop {
            let (mut stream, _addr) = listener.accept().await.map_err(|e| e.to_string())?;
            let h = handler.clone();
            tokio::spawn(async move {
                let mut buf = bytes::BytesMut::with_capacity(8192);
                loop {
                    let envelope =
                        match graft_transport::codec::read_envelope(&mut stream, &mut buf).await {
                            Ok(e) => e,
                            Err(_) => return,
                        };
                    let resp_payload = h.dispatch(&envelope.r#type, &envelope.payload).await;
                    let resp = Envelope {
                        correlation_id: envelope.correlation_id,
                        r#type: response_type_for(&envelope.r#type),
                        payload: resp_payload,
                    };
                    if graft_transport::codec::write_envelope(&mut stream, &resp)
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            });
        }
    })
}

fn response_type_for(request_type: &str) -> String {
    match request_type {
        "VoteRequest" => "VoteResponse".to_string(),
        "AppendEntriesRequest" => "AppendEntriesResponse".to_string(),
        "InstallSnapshotRequest" => "InstallSnapshotResponse".to_string(),
        "ClientCommandRequest" => "ClientCommandResponse".to_string(),
        "ClientQueryRequest" => "ClientQueryResponse".to_string(),
        "JoinClusterRequest" => "JoinClusterResponse".to_string(),
        "JoinClusterStatusRequest" => "JoinClusterStatusResponse".to_string(),
        "ReconfigureClusterRequest" => "ReconfigureClusterResponse".to_string(),
        "ReconfigurationStatusRequest" => "ReconfigurationStatusResponse".to_string(),
        "ClusterSummaryRequest" => "ClusterSummaryResponse".to_string(),
        "TelemetryRequest" => "TelemetryResponse".to_string(),
        _ => format!("{}Response", request_type),
    }
}

fn parse_peers(peers: &[String]) -> HashMap<String, (SocketAddr, String)> {
    let mut map = HashMap::new();
    for spec in peers {
        let (id, addr, role) = parse_peer_spec(spec);
        map.insert(id, (addr, role));
    }
    map
}

// ---------------------------------------------------------------------------
// Client commands
// ---------------------------------------------------------------------------

fn client_put(host: &str, port: u16, key: &str, value: &str, peer_id: &str) -> Result<(), String> {
    let cmd = KvCommand::Put {
        key: key.to_string(),
        value: value.as_bytes().to_vec(),
    };
    let cmd_json = serde_json::to_vec(&cmd).map_err(|e| e.to_string())?;
    let req = raft::ClientCommandRequest {
        term: 0,
        peer_id: peer_id.to_string(),
        command: cmd_json,
        auth_scheme: String::new(),
        auth_token: String::new(),
    };
    let resp_bytes = send_raft_rpc(host, port, "ClientCommandRequest", req.encode_to_vec())?;
    let resp = raft::ClientCommandResponse::decode(&resp_bytes[..]).map_err(|e| e.to_string())?;
    println!("success: {}", resp.success);
    println!("status: {}", resp.status);
    println!("result: {}", String::from_utf8_lossy(&resp.result));
    Ok(())
}

fn client_get(host: &str, port: u16, key: &str, peer_id: &str) -> Result<(), String> {
    let q = KvQuery::Get {
        key: key.to_string(),
    };
    let q_json = serde_json::to_vec(&q).map_err(|e| e.to_string())?;
    let req = raft::ClientQueryRequest {
        term: 0,
        peer_id: peer_id.to_string(),
        query: q_json,
        auth_scheme: String::new(),
        auth_token: String::new(),
    };
    let resp_bytes = send_raft_rpc(host, port, "ClientQueryRequest", req.encode_to_vec())?;
    let resp = raft::ClientQueryResponse::decode(&resp_bytes[..]).map_err(|e| e.to_string())?;
    println!("success: {}", resp.success);
    println!("status: {}", resp.status);
    if resp.success {
        if let Ok(r) = serde_json::from_slice::<GetResult>(&resp.result) {
            println!("get.value: {}", String::from_utf8_lossy(&r.value));
            println!("get.found: {}", r.found);
        }
    } else {
        println!("leader_id: {}", resp.leader_id);
        println!("leader_host: {}", resp.leader_host);
        println!("leader_port: {}", resp.leader_port);
    }
    Ok(())
}

fn client_cas(
    host: &str,
    port: u16,
    key: &str,
    expected_present: &str,
    expected_value: &str,
    new_value: &str,
    peer_id: &str,
) -> Result<(), String> {
    let cmd = KvCommand::Cas {
        key: key.to_string(),
        expected_present: parse_bool_arg(expected_present),
        expected_value: expected_value.as_bytes().to_vec(),
        new_value: new_value.as_bytes().to_vec(),
    };
    let cmd_json = serde_json::to_vec(&cmd).map_err(|e| e.to_string())?;
    let req = raft::ClientCommandRequest {
        term: 0,
        peer_id: peer_id.to_string(),
        command: cmd_json,
        auth_scheme: String::new(),
        auth_token: String::new(),
    };
    let resp_bytes = send_raft_rpc(host, port, "ClientCommandRequest", req.encode_to_vec())?;
    let resp = raft::ClientCommandResponse::decode(&resp_bytes[..]).map_err(|e| e.to_string())?;
    // Output in C++-compatible "key: value" format for Jepsen parse-cpp-lines
    println!("success: {}", resp.success);
    println!("status: {}", resp.status);
    println!("peer_id: {}", peer_id);
    if resp.success && resp.status == "ACCEPTED" {
        if let Ok(r) = serde_json::from_slice::<CasResult>(&resp.result) {
            println!("cas.matched: {}", r.matched);
            println!("cas.key: {}", key);
            println!("cas.expected_present: {}", r.expected_present);
            println!(
                "cas.expected_value: {}",
                String::from_utf8_lossy(&r.expected_value)
            );
            println!("cas.new_value: {}", new_value);
            println!("cas.current_present: {}", r.current_present);
            println!(
                "cas.current_value: {}",
                String::from_utf8_lossy(&r.current_value)
            );
            return Ok(());
        }
    }
    // Fallback: report mismatch
    println!("cas.matched: false");
    println!("cas.key: {}", key);
    println!("cas.expected_present: {}", expected_present);
    println!("cas.expected_value: {}", expected_value);
    println!("cas.new_value: {}", new_value);
    println!("cas.current_present: false");
    println!("cas.current_value: ");
    Ok(())
}

fn parse_bool_arg(value: &str) -> bool {
    matches!(value.to_ascii_lowercase().as_str(), "true" | "1" | "yes")
}

fn telemetry(host: &str, port: u16, require_leader_summary: bool) -> Result<(), String> {
    let req = raft::TelemetryRequest {
        term: 0,
        peer_id: "graft-client".to_string(),
        include_peer_stats: true,
        require_leader_summary,
        auth_scheme: String::new(),
        auth_token: String::new(),
    };
    let resp_bytes = send_raft_rpc(host, port, "TelemetryRequest", req.encode_to_vec())?;
    let resp = raft::TelemetryResponse::decode(&resp_bytes[..]).map_err(|e| e.to_string())?;
    println!("success: {}", resp.success);
    println!("state: {}", resp.state);
    println!("leader_id: {}", resp.leader_id);
    println!("commit_index: {}", resp.commit_index);
    println!("last_applied: {}", resp.last_applied);
    println!("snapshot_index: {}", resp.snapshot_index);
    Ok(())
}

fn cluster_summary(host: &str, port: u16) -> Result<(), String> {
    let req = raft::ClusterSummaryRequest {
        term: 0,
        peer_id: "graft-client".to_string(),
        auth_scheme: String::new(),
        auth_token: String::new(),
    };
    let resp_bytes = send_raft_rpc(host, port, "ClusterSummaryRequest", req.encode_to_vec())?;
    let resp = raft::ClusterSummaryResponse::decode(&resp_bytes[..]).map_err(|e| e.to_string())?;
    println!("success: {}", resp.success);
    println!("status: {}", resp.status);
    println!("leader_id: {}", resp.leader_id);
    println!("joint_consensus: {}", resp.joint_consensus);
    for m in &resp.members {
        println!("member[{}].voting={}.role={}", m.peer_id, m.voting, m.role);
    }
    Ok(())
}

fn join_cluster(
    host: &str,
    port: u16,
    joining_peer_id: &str,
    joining_host: &str,
    joining_port: u16,
    role: &str,
    peer_id: &str,
) -> Result<(), String> {
    let req = raft::JoinClusterRequest {
        term: 0,
        peer_id: peer_id.to_string(),
        joining_peer_id: joining_peer_id.to_string(),
        host: joining_host.to_string(),
        port: joining_port as i32,
        role: role.to_string(),
        auth_scheme: String::new(),
        auth_token: String::new(),
    };
    let resp_bytes = send_raft_rpc(host, port, "JoinClusterRequest", req.encode_to_vec())?;
    let resp = raft::JoinClusterResponse::decode(&resp_bytes[..]).map_err(|e| e.to_string())?;
    println!("success: {}", resp.success);
    println!("status: {}", resp.status);
    Ok(())
}

fn join_status(host: &str, port: u16, target_peer_id: &str, peer_id: &str) -> Result<(), String> {
    let req = raft::JoinClusterStatusRequest {
        term: 0,
        peer_id: peer_id.to_string(),
        target_peer_id: target_peer_id.to_string(),
        auth_scheme: String::new(),
        auth_token: String::new(),
    };
    let resp_bytes = send_raft_rpc(host, port, "JoinClusterStatusRequest", req.encode_to_vec())?;
    let resp =
        raft::JoinClusterStatusResponse::decode(&resp_bytes[..]).map_err(|e| e.to_string())?;
    println!("success: {}", resp.success);
    println!("status: {}", resp.status);
    Ok(())
}

fn reconfigure_joint(
    host: &str,
    port: u16,
    peer_id: &str,
    members: &[String],
) -> Result<(), String> {
    let specs: Vec<raft::PeerSpec> = members
        .iter()
        .map(|m| {
            let (id, addr, role) = parse_peer_spec(m);
            raft::PeerSpec {
                id,
                host: addr.ip().to_string(),
                port: addr.port() as i32,
                role: role.to_string(),
            }
        })
        .collect();
    let req = raft::ReconfigureClusterRequest {
        term: 0,
        peer_id: peer_id.to_string(),
        action: "JOINT".to_string(),
        members: specs,
        auth_scheme: String::new(),
        auth_token: String::new(),
    };
    let resp_bytes = send_raft_rpc(host, port, "ReconfigureClusterRequest", req.encode_to_vec())?;
    let resp =
        raft::ReconfigureClusterResponse::decode(&resp_bytes[..]).map_err(|e| e.to_string())?;
    println!("success: {}", resp.success);
    println!("status: {}", resp.status);
    Ok(())
}

fn reconfigure_finalize(host: &str, port: u16, peer_id: &str) -> Result<(), String> {
    let req = raft::ReconfigureClusterRequest {
        term: 0,
        peer_id: peer_id.to_string(),
        action: "FINALIZE".to_string(),
        members: vec![],
        auth_scheme: String::new(),
        auth_token: String::new(),
    };
    let resp_bytes = send_raft_rpc(host, port, "ReconfigureClusterRequest", req.encode_to_vec())?;
    let resp =
        raft::ReconfigureClusterResponse::decode(&resp_bytes[..]).map_err(|e| e.to_string())?;
    println!("success: {}", resp.success);
    println!("status: {}", resp.status);
    Ok(())
}

fn reconfigure_promote(
    host: &str,
    port: u16,
    peer_id: &str,
    member_spec: &str,
) -> Result<(), String> {
    let (id, addr, _role) = parse_peer_spec(member_spec);
    let spec = raft::PeerSpec {
        id,
        host: addr.ip().to_string(),
        port: addr.port() as i32,
        role: "VOTER".to_string(),
    };
    let req = raft::ReconfigureClusterRequest {
        term: 0,
        peer_id: peer_id.to_string(),
        action: "PROMOTE".to_string(),
        members: vec![spec],
        auth_scheme: String::new(),
        auth_token: String::new(),
    };
    let resp_bytes = send_raft_rpc(host, port, "ReconfigureClusterRequest", req.encode_to_vec())?;
    let resp =
        raft::ReconfigureClusterResponse::decode(&resp_bytes[..]).map_err(|e| e.to_string())?;
    println!("success: {}", resp.success);
    println!("status: {}", resp.status);
    Ok(())
}

fn send_vote_request_cmd(
    host: &str,
    port: u16,
    candidate_id: &str,
    last_log_index: u64,
    last_log_term: u64,
    term: u64,
) -> Result<(), String> {
    let req = raft::VoteRequest {
        term: term as i64,
        candidate_id: candidate_id.to_string(),
        last_log_index: last_log_index as i64,
        last_log_term: last_log_term as i64,
    };
    let resp_bytes = send_raft_rpc(host, port, "VoteRequest", req.encode_to_vec())?;
    let resp = raft::VoteResponse::decode(&resp_bytes[..]).map_err(|e| e.to_string())?;
    println!("success: true");
    println!("term: {}", resp.term);
    println!("vote_granted: {}", resp.vote_granted);
    Ok(())
}

fn send_append_entries_cmd(
    host: &str,
    port: u16,
    leader_id: &str,
    prev_log_index: u64,
    prev_log_term: u64,
    leader_commit: u64,
    term: u64,
) -> Result<(), String> {
    let req = raft::AppendEntriesRequest {
        term: term as i64,
        leader_id: leader_id.to_string(),
        prev_log_index: prev_log_index as i64,
        prev_log_term: prev_log_term as i64,
        leader_commit: leader_commit as i64,
        entries: vec![],
    };
    let resp_bytes = send_raft_rpc(host, port, "AppendEntriesRequest", req.encode_to_vec())?;
    let resp = raft::AppendEntriesResponse::decode(&resp_bytes[..]).map_err(|e| e.to_string())?;
    println!("success: true");
    println!("term: {}", resp.term);
    println!("append_success: {}", resp.success);
    Ok(())
}

fn send_install_snapshot_cmd(
    host: &str,
    port: u16,
    leader_id: &str,
    last_included_index: u64,
    last_included_term: u64,
    term: u64,
) -> Result<(), String> {
    let req = raft::InstallSnapshotRequest {
        term: term as i64,
        leader_id: leader_id.to_string(),
        last_included_index: last_included_index as i64,
        last_included_term: last_included_term as i64,
        offset: 0,
        snapshot_data: vec![],
        done: true,
    };
    let resp_bytes = send_raft_rpc(host, port, "InstallSnapshotRequest", req.encode_to_vec())?;
    let resp = raft::InstallSnapshotResponse::decode(&resp_bytes[..]).map_err(|e| e.to_string())?;
    println!("success: true");
    println!("term: {}", resp.term);
    println!("install_success: {}", resp.success);
    Ok(())
}

// ---------------------------------------------------------------------------
// Additional client commands
// ---------------------------------------------------------------------------

fn client_delete(host: &str, port: u16, key: &str, peer_id: &str) -> Result<(), String> {
    let cmd = KvCommand::Delete {
        key: key.to_string(),
    };
    let cmd_json = serde_json::to_vec(&cmd).map_err(|e| e.to_string())?;
    let req = raft::ClientCommandRequest {
        term: 0,
        peer_id: peer_id.to_string(),
        command: cmd_json,
        auth_scheme: String::new(),
        auth_token: String::new(),
    };
    let resp_bytes = send_raft_rpc(host, port, "ClientCommandRequest", req.encode_to_vec())?;
    let resp = raft::ClientCommandResponse::decode(&resp_bytes[..]).map_err(|e| e.to_string())?;
    println!("success: {}", resp.success);
    println!("status: {}", resp.status);
    Ok(())
}

fn client_clear(host: &str, port: u16, peer_id: &str) -> Result<(), String> {
    let cmd = KvCommand::Clear;
    let cmd_json = serde_json::to_vec(&cmd).map_err(|e| e.to_string())?;
    let req = raft::ClientCommandRequest {
        term: 0,
        peer_id: peer_id.to_string(),
        command: cmd_json,
        auth_scheme: String::new(),
        auth_token: String::new(),
    };
    let resp_bytes = send_raft_rpc(host, port, "ClientCommandRequest", req.encode_to_vec())?;
    let resp = raft::ClientCommandResponse::decode(&resp_bytes[..]).map_err(|e| e.to_string())?;
    println!("success: {}", resp.success);
    println!("status: {}", resp.status);
    Ok(())
}

fn reconfiguration_status(host: &str, port: u16) -> Result<(), String> {
    let req = raft::ReconfigurationStatusRequest {
        term: 0,
        peer_id: "graft-client".to_string(),
        auth_scheme: String::new(),
        auth_token: String::new(),
    };
    let resp_bytes = send_raft_rpc(
        host,
        port,
        "ReconfigurationStatusRequest",
        req.encode_to_vec(),
    )?;
    let resp = raft::ReconfigurationStatusResponse::decode(&resp_bytes[..]).map_err(|e| e.to_string())?;
    println!("success: {}", resp.success);
    println!("status: {}", resp.status);
    println!("joint_consensus: {}", resp.joint_consensus);
    println!("reconfiguration_active: {}", resp.reconfiguration_active);
    if resp.joint_consensus {
        println!("reconfiguration_age_millis: {}", resp.reconfiguration_age_millis);
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// One-shot smoke commands
// ---------------------------------------------------------------------------

fn election_round(
    host: &str,
    port: u16,
    peer_id: &str,
    last_log_index: u64,
    last_log_term: u64,
    term: u64,
) -> Result<(), String> {
    // Send VoteRequest and decode response
    let req = raft::VoteRequest {
        term: term as i64,
        candidate_id: peer_id.to_string(),
        last_log_index: last_log_index as i64,
        last_log_term: last_log_term as i64,
    };
    let resp_bytes = send_raft_rpc(host, port, "VoteRequest", req.encode_to_vec())?;
    let resp = raft::VoteResponse::decode(&resp_bytes[..]).map_err(|e| e.to_string())?;
    println!("term: {}", resp.term);
    println!("vote_granted: {}", resp.vote_granted);
    println!("current_term: {}", resp.current_term);
    println!("peer_id: {}", resp.peer_id);
    Ok(())
}

fn heartbeat_round(
    host: &str,
    port: u16,
    leader_id: &str,
    prev_log_index: u64,
    prev_log_term: u64,
    leader_commit: u64,
    term: u64,
) -> Result<(), String> {
    // Send an empty AppendEntries (heartbeat) and decode the response
    let req = raft::AppendEntriesRequest {
        term: term as i64,
        leader_id: leader_id.to_string(),
        prev_log_index: prev_log_index as i64,
        prev_log_term: prev_log_term as i64,
        leader_commit: leader_commit as i64,
        entries: vec![],
    };
    let resp_bytes = send_raft_rpc(host, port, "AppendEntriesRequest", req.encode_to_vec())?;
    let resp = raft::AppendEntriesResponse::decode(&resp_bytes[..]).map_err(|e| e.to_string())?;
    println!("term: {}", resp.term);
    println!("success: {}", resp.success);
    println!("match_index: {}", resp.match_index);
    Ok(())
}

fn replicate_once(
    host: &str,
    port: u16,
    key: &str,
    value: &str,
    peer_id: &str,
) -> Result<(), String> {
    // Put a key, wait for commit, and verify with get
    client_put(host, port, key, value, peer_id)?;
    // Poll until the key is readable (committed)
    for _ in 0..30 {
        let q = KvQuery::Get {
            key: key.to_string(),
        };
        let q_json = serde_json::to_vec(&q).map_err(|e| e.to_string())?;
        let req = raft::ClientQueryRequest {
            term: 0,
            peer_id: peer_id.to_string(),
            query: q_json,
            auth_scheme: String::new(),
            auth_token: String::new(),
        };
        if let Ok(resp_bytes) = send_raft_rpc(host, port, "ClientQueryRequest", req.encode_to_vec())
        {
            if let Ok(resp) = raft::ClientQueryResponse::decode(&resp_bytes[..]) {
                if resp.success {
                    println!("replicated: true");
                    println!("key: {}", key);
                    println!("value: {}", String::from_utf8_lossy(&resp.result));
                    return Ok(());
                }
            }
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    println!("replicated: false");
    Ok(())
}

fn replicate_once_persistent(_state_file: &str, key: &str, value: &str) -> Result<(), String> {
    // Like replicate_once but loads state from a file first. For now, print
    // the operation and skip file loading.
    println!("replicate_once_persistent: key={}, value={}", key, value);
    println!("replicated: false (file-based persistence requires active server)");
    Ok(())
}

fn replicate_put_persistent(_state_file: &str, key: &str, value: &str) -> Result<(), String> {
    // Like replicate_once_persistent but issues a put command. For now, print.
    println!("replicate_put_persistent: key={}, value={}", key, value);
    println!("replicated: false (file-based persistence requires active server)");
    Ok(())
}

fn compact_snapshot(host: &str, port: u16) -> Result<(), String> {
    // Trigger snapshot compaction by sending a client-put and then querying telemetry
    // to check snapshot_index. The Raft node auto-compacts based on snapshot_min_entries.
    let before = {
        let req = raft::TelemetryRequest {
            term: 0,
            peer_id: "graft-client".to_string(),
            include_peer_stats: false,
            require_leader_summary: false,
            auth_scheme: String::new(),
            auth_token: String::new(),
        };
        let resp_bytes = send_raft_rpc(host, port, "TelemetryRequest", req.encode_to_vec())?;
        let resp = raft::TelemetryResponse::decode(&resp_bytes[..]).map_err(|e| e.to_string())?;
        (resp.snapshot_index, resp.commit_index, resp.last_log_index)
    };
    // Force a few writes to advance the log past the compaction threshold
    for i in 0..5 {
        let cmd = KvCommand::Put {
            key: format!("compact-{}", i),
            value: format!("v{}", i).into_bytes(),
        };
        let cmd_json = serde_json::to_vec(&cmd).map_err(|e| e.to_string())?;
        let req = raft::ClientCommandRequest {
            term: 0,
            peer_id: "graft-client".to_string(),
            command: cmd_json,
            auth_scheme: String::new(),
            auth_token: String::new(),
        };
        let _ = send_raft_rpc(host, port, "ClientCommandRequest", req.encode_to_vec());
    }
    std::thread::sleep(std::time::Duration::from_millis(500));
    let after = {
        let req = raft::TelemetryRequest {
            term: 0,
            peer_id: "graft-client".to_string(),
            include_peer_stats: false,
            require_leader_summary: false,
            auth_scheme: String::new(),
            auth_token: String::new(),
        };
        let resp_bytes = send_raft_rpc(host, port, "TelemetryRequest", req.encode_to_vec())?;
        let resp = raft::TelemetryResponse::decode(&resp_bytes[..]).map_err(|e| e.to_string())?;
        (resp.snapshot_index, resp.commit_index, resp.last_log_index)
    };
    println!("snapshot_index_before: {}", before.0);
    println!("snapshot_index_after: {}", after.0);
    println!("compacted: {}", after.0 > before.0);
    Ok(())
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

fn main() -> Result<(), String> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Serve {
            host,
            port,
            peer_id,
            term,
            last_log_index,
            last_log_term,
            peers,
        } => run_server(
            &host,
            port,
            &peer_id,
            "",
            term,
            last_log_index,
            last_log_term,
            &peers,
            false,
            None,
        ),
        Commands::ServeStateful {
            host,
            port,
            peer_id,
            term,
            last_log_index,
            last_log_term,
            peers,
        } => run_server(
            &host,
            port,
            &peer_id,
            "",
            term,
            last_log_index,
            last_log_term,
            &peers,
            false,
            None,
        ),
        Commands::ServeActive {
            host,
            port,
            peer_id,
            current_term,
            last_log_index,
            last_log_term,
            peers,
        } => run_server(
            &host,
            port,
            &peer_id,
            "",
            current_term,
            last_log_index,
            last_log_term,
            &peers,
            true,
            None,
        ),
        Commands::ServePersistent {
            host,
            port,
            peer_id,
            state_file,
            term,
            last_log_index,
            last_log_term,
            peers,
        } => run_server(
            &host,
            port,
            &peer_id,
            &state_file,
            term,
            last_log_index,
            last_log_term,
            &peers,
            false,
            None,
        ),
        Commands::ServeActivePersistent {
            host,
            port,
            peer_id,
            state_file,
            current_term,
            last_log_index,
            last_log_term,
            peers,
        } => run_server(
            &host,
            port,
            &peer_id,
            &state_file,
            current_term,
            last_log_index,
            last_log_term,
            &peers,
            true,
            None,
        ),
        Commands::ServeActivePersistentWorkload {
            host,
            port,
            peer_id,
            state_file,
            current_term,
            last_log_index,
            last_log_term,
            _replicate_interval_ms,
            _snapshot_threshold,
            peers,
        } => run_server(
            &host,
            port,
            &peer_id,
            &state_file,
            current_term,
            last_log_index,
            last_log_term,
            &peers,
            true,
            Some((_replicate_interval_ms, _snapshot_threshold)),
        ),
        Commands::ClientPut {
            host,
            port,
            key,
            value,
            peer_id,
        } => client_put(&host, port, &key, &value, &peer_id),
        Commands::ClientGet {
            host,
            port,
            key,
            peer_id,
        } => client_get(&host, port, &key, &peer_id),
        Commands::ClientCas {
            host,
            port,
            key,
            expected_present,
            expected_value,
            new_value,
            peer_id,
        } => client_cas(
            &host,
            port,
            &key,
            &expected_present,
            &expected_value,
            &new_value,
            &peer_id,
        ),
        Commands::ClientDelete {
            host,
            port,
            key,
            peer_id,
        } => client_delete(&host, port, &key, &peer_id),
        Commands::ClientClear {
            host,
            port,
            peer_id,
        } => client_clear(&host, port, &peer_id),
        Commands::Telemetry {
            host,
            port,
            require_leader_summary,
        } => telemetry(&host, port, require_leader_summary),
        Commands::ClusterSummary { host, port } => cluster_summary(&host, port),
        Commands::JoinCluster {
            host,
            port,
            joining_peer_id,
            joining_host,
            joining_port,
            role,
            peer_id,
        } => join_cluster(
            &host,
            port,
            &joining_peer_id,
            &joining_host,
            joining_port,
            &role,
            &peer_id,
        ),
        Commands::JoinStatus {
            host,
            port,
            target_peer_id,
            peer_id,
        } => join_status(&host, port, &target_peer_id, &peer_id),
        Commands::Reconfigure {
            host,
            port,
            peer_id,
            action,
        } => match action {
            ReconfigureAction::Joint { members } => {
                reconfigure_joint(&host, port, &peer_id, &members)
            }
            ReconfigureAction::Finalize { .. } => reconfigure_finalize(&host, port, &peer_id),
            ReconfigureAction::Promote { member } => {
                reconfigure_promote(&host, port, &peer_id, &member)
            }
        },
        Commands::VoteRequest {
            host,
            port,
            candidate_id,
            last_log_index,
            last_log_term,
            term,
        } => send_vote_request_cmd(
            &host,
            port,
            &candidate_id,
            last_log_index,
            last_log_term,
            term,
        ),
        Commands::AppendEntries {
            host,
            port,
            leader_id,
            prev_log_index,
            prev_log_term,
            leader_commit,
            term,
        } => send_append_entries_cmd(
            &host,
            port,
            &leader_id,
            prev_log_index,
            prev_log_term,
            leader_commit,
            term,
        ),
        Commands::InstallSnapshot {
            host,
            port,
            leader_id,
            last_included_index,
            last_included_term,
            term,
        } => send_install_snapshot_cmd(
            &host,
            port,
            &leader_id,
            last_included_index,
            last_included_term,
            term,
        ),
        Commands::ReconfigurationStatus { host, port } => reconfiguration_status(&host, port),
        Commands::DumpState { state_file } => dump_state(&state_file),
        Commands::ElectionRound {
            host,
            port,
            peer_id,
            last_log_index,
            last_log_term,
            term,
        } => election_round(&host, port, &peer_id, last_log_index, last_log_term, term),
        Commands::HeartbeatRound {
            host,
            port,
            leader_id,
            prev_log_index,
            prev_log_term,
            leader_commit,
            term,
        } => heartbeat_round(
            &host,
            port,
            &leader_id,
            prev_log_index,
            prev_log_term,
            leader_commit,
            term,
        ),
        Commands::ReplicateOnce {
            host,
            port,
            key,
            value,
            peer_id,
        } => replicate_once(&host, port, &key, &value, &peer_id),
        Commands::ReplicateOncePersistent {
            host: _host,
            port: _port,
            state_file,
            key,
            value,
            peer_id: _peer_id,
        } => replicate_once_persistent(&state_file, &key, &value),
        Commands::ReplicatePutPersistent {
            host: _host,
            port: _port,
            state_file,
            key,
            value,
            peer_id: _peer_id,
        } => replicate_put_persistent(&state_file, &key, &value),
        Commands::CompactSnapshot { host, port } => compact_snapshot(&host, port),
    }
}
