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

//! Demo key-value state machine built on the Raft consensus engine.
//!
//! `KeyValueStateMachine` implements the `StateMachine` and
//! `QueryableStateMachine` traits from `graft-core`. It stores
//! key-value pairs in an in-memory `HashMap`, supports Put/Delete/
//! Clear/CAS writes and Get reads, and serializes its state for
//! snapshot/restore using JSON.
//!
//! This is the Rust equivalent of the Java `raft-app-kv` module and
//! the C++ `KeyValueStateMachine`. It demonstrates how domain
//! applications plug into the Raft consensus layer.

use std::collections::HashMap;
use std::sync::Mutex;

use graft_core::state_machine::{QueryableStateMachine, StateMachine};

// ---------------------------------------------------------------------------
// KeyValueStateMachine
// ---------------------------------------------------------------------------

/// A simple in-memory key-value store that participates in Raft
/// consensus. All writes (Put, Delete, Clear, CAS) are applied via
/// `StateMachine::apply`, which is only called after Raft has committed
/// the entry — guaranteeing linearizability. Reads (Get) go through
/// `QueryableStateMachine::query` and are only dispatched after the
/// leader has established read safety via a read lease or contact-
/// majority barrier.
///
/// Snapshot and restore use JSON serialization of the full store,
/// suitable for small-to-medium datasets.
pub struct KeyValueStateMachine {
    store: Mutex<HashMap<String, Vec<u8>>>,
}

impl KeyValueStateMachine {
    /// Creates an empty key-value store.
    pub fn new() -> Self {
        Self {
            store: Mutex::new(HashMap::new()),
        }
    }
}

impl StateMachine for KeyValueStateMachine {
    /// Applies a committed command. The command is deserialized from
    /// JSON; unrecognized commands are silently ignored (the command
    /// was validated before submission).
    ///
    /// Determinism is guaranteed by Raft: the same command at the same
    /// log index produces the same state on every node.
    fn apply(&self, _term: u64, command: &[u8]) {
        if let Ok(cmd) = serde_json::from_slice::<KvCommand>(command) {
            let mut store = self.store.lock().unwrap();
            match cmd {
                KvCommand::Put { key, value } => {
                    store.insert(key, value);
                }
                KvCommand::Delete { key } => {
                    store.remove(&key);
                }
                KvCommand::Clear => {
                    store.clear();
                }
                KvCommand::Cas {
                    key,
                    expected_value,
                    new_value,
                } => {
                    // Compare-and-swap: only update if the current value
                    // matches the expected value. This is an atomic
                    // operation because apply is called under Raft
                    // commitment — no concurrent writes can interleave.
                    if store.get(&key) == Some(&expected_value) {
                        store.insert(key, new_value);
                    }
                }
            }
        }
    }

    /// Returns a JSON-serialized snapshot of the entire store. Raft
    /// wraps these bytes with cluster membership metadata before storage.
    fn snapshot(&self) -> Vec<u8> {
        let store = self.store.lock().unwrap();
        serde_json::to_vec(&*store).unwrap_or_default()
    }

    /// Restores the store from a previously-produced JSON snapshot.
    /// Raft has already restored its own metadata before calling this.
    fn restore(&self, snapshot_data: &[u8]) {
        if let Ok(data) = serde_json::from_slice::<HashMap<String, Vec<u8>>>(snapshot_data) {
            let mut store = self.store.lock().unwrap();
            *store = data;
        }
    }

    fn as_queryable(&self) -> Option<&dyn QueryableStateMachine> {
        Some(self)
    }
}

impl QueryableStateMachine for KeyValueStateMachine {
    /// Executes a read query against local state. Only called after
    /// the leader has confirmed it still holds leadership (read lease
    /// or contact-majority barrier), so the result is linearizable.
    fn query(&self, request: &[u8]) -> Vec<u8> {
        if let Ok(q) = serde_json::from_slice::<KvQuery>(request) {
            let store = self.store.lock().unwrap();
            match q {
                KvQuery::Get { key } => {
                    let found = store.contains_key(&key);
                    let value = store.get(&key).cloned().unwrap_or_default();
                    serde_json::to_vec(&GetResult {
                        key,
                        found,
                        value,
                    })
                    .unwrap_or_default()
                }
            }
        } else {
            Vec::new()
        }
    }
}

// -- Command and query types (JSON-serialized, carried in Raft log entries) --

/// A write command submitted by a client. Serialized as JSON in the
/// Raft log entry's data field.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum KvCommand {
    /// Store a value under a key.
    Put { key: String, value: Vec<u8> },
    /// Remove a key (succeeds even if the key is absent).
    Delete { key: String },
    /// Remove all keys.
    Clear,
    /// Conditional write: only update if the current value matches
    /// `expected_value`.
    Cas { key: String, expected_value: Vec<u8>, new_value: Vec<u8> },
}

/// A read query submitted by a client.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum KvQuery {
    /// Read the current value for a key.
    Get { key: String },
}

/// Result of a Get query.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct GetResult {
    pub key: String,
    /// True if the key was present.
    pub found: bool,
    /// The value, or empty if `found` is false.
    pub value: Vec<u8>,
}
