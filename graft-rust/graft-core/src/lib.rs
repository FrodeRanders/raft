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

//! Core Raft consensus engine — the transport-agnostic state machine.
//!
//! `graft-core` provides the consensus logic that drives a Raft node:
//! elections, log replication, commit-index advancement, snapshot
//! installation, joint-consensus membership transitions, and read
//! barriers. It is deliberately free of any transport or I/O dependency:
//! the runtime layer (graft-runtime / graft-transport) decides when to
//! send RPCs over TCP and when to persist state to disk.
//!
//! # Module overview
//!
//! - **[`types`]**: Peer identity, log entries, role/state enums, and
//!   persistent state.
//! - **[`state_machine`]**: Traits that define the boundary between Raft
//!   consensus and domain application logic.
//! - **[`membership`]**: `ClusterConfiguration` — stable and
//!   joint-consensus configurations with split-majority quorum checks.
//! - **[`raft_node`]**: `RaftNode` — the full consensus state machine
//!   with election, replication, commit/apply, and snapshot logic.

pub mod membership;
pub mod state_machine;
pub mod raft_node;
pub mod types;
