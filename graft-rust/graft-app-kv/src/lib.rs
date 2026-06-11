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
    fn apply(&self, term: u64, command: &[u8]) {
        let _ = self.apply_with_result(term, command);
    }

    fn apply_with_result(&self, _term: u64, command: &[u8]) -> Vec<u8> {
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
                    expected_present,
                    expected_value,
                    new_value,
                } => {
                    let current = store.get(&key).cloned();
                    let current_present = current.is_some();
                    let matched = current_present == expected_present
                        && (!expected_present || current.as_ref() == Some(&expected_value));
                    if matched {
                        store.insert(key, new_value);
                    }
                    return serde_json::to_vec(&CasResult {
                        matched,
                        expected_present,
                        expected_value,
                        current_present,
                        current_value: current.unwrap_or_default(),
                    })
                    .unwrap_or_default();
                }
            }
        }
        Vec::new()
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
                    serde_json::to_vec(&GetResult { key, found, value }).unwrap_or_default()
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
    /// Conditional write: only update if presence and value match the expected
    /// state.
    Cas {
        key: String,
        expected_present: bool,
        expected_value: Vec<u8>,
        new_value: Vec<u8>,
    },
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

/// Result of a committed CAS command.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct CasResult {
    pub matched: bool,
    pub expected_present: bool,
    pub expected_value: Vec<u8>,
    pub current_present: bool,
    pub current_value: Vec<u8>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use graft_core::state_machine::StateMachine;

    #[test]
    fn cas_missing_expected_succeeds_and_reports_result() {
        let sm = KeyValueStateMachine::new();
        let command = serde_json::to_vec(&KvCommand::Cas {
            key: "k".to_string(),
            expected_present: false,
            expected_value: Vec::new(),
            new_value: b"v1".to_vec(),
        })
        .unwrap();

        let result = sm.apply_with_result(1, &command);
        let result: CasResult = serde_json::from_slice(&result).unwrap();

        assert!(result.matched);
        assert!(!result.current_present);

        let query = serde_json::to_vec(&KvQuery::Get {
            key: "k".to_string(),
        })
        .unwrap();
        let get: GetResult = serde_json::from_slice(&sm.query(&query)).unwrap();
        assert!(get.found);
        assert_eq!(get.value, b"v1");
    }

    #[test]
    fn cas_present_mismatch_reports_false_without_updating() {
        let sm = KeyValueStateMachine::new();
        let put = serde_json::to_vec(&KvCommand::Put {
            key: "k".to_string(),
            value: b"old".to_vec(),
        })
        .unwrap();
        sm.apply(1, &put);

        let command = serde_json::to_vec(&KvCommand::Cas {
            key: "k".to_string(),
            expected_present: true,
            expected_value: b"expected".to_vec(),
            new_value: b"new".to_vec(),
        })
        .unwrap();
        let result: CasResult = serde_json::from_slice(&sm.apply_with_result(1, &command)).unwrap();

        assert!(!result.matched);
        assert!(result.current_present);
        assert_eq!(result.current_value, b"old");
    }
}
