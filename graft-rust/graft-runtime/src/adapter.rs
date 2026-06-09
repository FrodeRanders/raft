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

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use graft_core::raft_node::RaftNode;
use graft_transport::client::RaftClient;

// ---------------------------------------------------------------------------
// RuntimeConfiguration — operational tunables
// ---------------------------------------------------------------------------

/// Holds all runtime configuration loaded from CLI arguments and/or
/// environment variables. This is the Rust equivalent of the Java
/// `RuntimeConfiguration` record.
pub struct RuntimeConfiguration {
    /// This node's logical Raft identifier.
    pub node_id: String,
    /// Address this node should bind its TCP server to.
    pub bind_addr: SocketAddr,
    /// Known peer addresses, keyed by peer id. May be loaded from a
    /// static list or resolved from DNS SRV records.
    pub peers: HashMap<String, SocketAddr>,
    /// Directory for persistent state files (log, term/vote). None for
    /// in-memory-only operation.
    pub data_dir: Option<String>,
    /// When true, this node bootstraps a new single-node cluster.
    pub bootstrap: bool,
    /// Base election timeout (ms). The actual deadline includes randomized
    /// jitter and per-round backoff.
    pub timeout_millis: u64,
    /// Minimum uncompacted log entries before triggering local snapshot
    /// compaction.
    pub snapshot_min_entries: u64,
    /// Maximum bytes per InstallSnapshot chunk for streaming transfer.
    pub snapshot_chunk_bytes: u64,
}

// ---------------------------------------------------------------------------
// RuntimeAdapter — wiring point for application modules
// ---------------------------------------------------------------------------

/// The runtime adapter bundles a shared `RaftNode` (behind a `Mutex` for
/// thread safety), a `RaftClient` for outbound RPCs, and the runtime
/// configuration. Application modules receive an adapter and use it to
/// submit commands, query state, and access cluster metadata.
///
/// This is the Rust equivalent of the Java `BasicAdapter` — it wires the
/// core consensus engine to the transport layer.
pub struct RuntimeAdapter {
    /// Shared Raft consensus state machine, protected by a mutex so the
    /// transport server and timer tasks can access it concurrently.
    pub node: Arc<parking_lot::Mutex<RaftNode>>,
    /// Outbound RPC client for sending AppendEntries, VoteRequests,
    /// InstallSnapshots, etc.
    pub client: Arc<RaftClient>,
    /// Immutable runtime configuration.
    pub config: RuntimeConfiguration,
}
