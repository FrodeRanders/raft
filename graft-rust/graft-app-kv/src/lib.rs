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
//! Uses protobuf `StateMachineCommand` / `StateMachineQuery` for
//! wire-compatibility with the Java and C++ implementations.
//! Internal snapshot serialization uses JSON (deterministic key order).

use std::collections::HashMap;
use std::sync::Mutex;

use graft_core::state_machine::{QueryableStateMachine, StateMachine};
use prost::Message;

pub use graft_proto::raft;

// ---------------------------------------------------------------------------
// KeyValueStateMachine
// ---------------------------------------------------------------------------

/// A simple in-memory key-value store that participates in Raft
/// consensus using the shared protobuf command/query wire format.
pub struct KeyValueStateMachine {
    store: Mutex<HashMap<String, Vec<u8>>>,
}

impl KeyValueStateMachine {
    pub fn new() -> Self {
        Self {
            store: Mutex::new(HashMap::new()),
        }
    }

    /// Applies a protobuf `StateMachineCommand` to an external KV store.
    /// Used by offline tools (e.g. dump-state) to reconstruct state.
    pub fn apply_command(store: &mut HashMap<String, Vec<u8>>, cmd: &raft::StateMachineCommand) {
        use raft::state_machine_command::Command;
        match &cmd.command {
            Some(Command::Put(put)) => {
                store.insert(put.key.clone(), put.value.clone().into_bytes());
            }
            Some(Command::Delete(del)) => {
                store.remove(&del.key);
            }
            Some(Command::Clear(_)) => {
                store.clear();
            }
            Some(Command::Cas(cas)) => {
                let current = store.get(&cas.key).cloned();
                let current_present = current.is_some();
                let matched = current_present == cas.expected_present
                    && (!cas.expected_present
                        || current.as_deref() == Some(cas.expected_value.as_bytes()));
                if matched {
                    store.insert(cas.key.clone(), cas.new_value.clone().into_bytes());
                }
            }
            None => {}
        }
    }
}

impl StateMachine for KeyValueStateMachine {
    fn apply(&self, term: u64, command: &[u8]) {
        let _ = self.apply_with_result(term, command);
    }

    fn apply_with_result(&self, _term: u64, data: &[u8]) -> Vec<u8> {
        let cmd = match raft::StateMachineCommand::decode(data) {
            Ok(c) => c,
            Err(_) => return Vec::new(),
        };

        use raft::state_machine_command::Command;
        let mut store = self.store.lock().unwrap();

        match cmd.command {
            Some(Command::Put(put)) => {
                store.insert(put.key, put.value.into_bytes());
            }
            Some(Command::Delete(del)) => {
                store.remove(&del.key);
            }
            Some(Command::Clear(_)) => {
                store.clear();
            }
            Some(Command::Cas(cas)) => {
                let current = store.get(&cas.key).cloned();
                let current_present = current.is_some();
                let current_value =
                    String::from_utf8(current.clone().unwrap_or_default()).unwrap_or_default();

                let matched = current_present == cas.expected_present
                    && (!cas.expected_present
                        || current.as_deref() == Some(cas.expected_value.as_bytes()));

                if matched {
                    store.insert(cas.key.clone(), cas.new_value.clone().into_bytes());
                }

                let result = raft::StateMachineCommandResult {
                    result: Some(raft::state_machine_command_result::Result::Cas(
                        raft::CasCommandResult {
                            key: cas.key,
                            matched,
                            expected_present: cas.expected_present,
                            expected_value: cas.expected_value,
                            new_value: cas.new_value,
                            current_present,
                            current_value,
                        },
                    )),
                };
                return result.encode_to_vec();
            }
            None => {}
        }
        Vec::new()
    }

    fn snapshot(&self) -> Vec<u8> {
        let store = self.store.lock().unwrap();
        // Use sorted keys for deterministic output
        let mut keys: Vec<&String> = store.keys().collect();
        keys.sort();
        let ordered: Vec<(&String, &Vec<u8>)> = keys.iter().map(|k| (*k, &store[*k])).collect();
        serde_json::to_vec(&ordered).unwrap_or_default()
    }

    fn restore(&self, snapshot_data: &[u8]) {
        if let Ok(pairs) = serde_json::from_slice::<Vec<(String, Vec<u8>)>>(snapshot_data) {
            let mut store = self.store.lock().unwrap();
            store.clear();
            for (key, value) in pairs {
                store.insert(key, value);
            }
        }
    }

    fn as_queryable(&self) -> Option<&dyn QueryableStateMachine> {
        Some(self)
    }
}

impl QueryableStateMachine for KeyValueStateMachine {
    fn query(&self, data: &[u8]) -> Vec<u8> {
        let q = match raft::StateMachineQuery::decode(data) {
            Ok(q) => q,
            Err(_) => return Vec::new(),
        };

        use raft::state_machine_query::Query;
        let store = self.store.lock().unwrap();

        match q.query {
            Some(Query::Get(get)) => {
                let found = store.contains_key(&get.key);
                let value = if found {
                    String::from_utf8(store.get(&get.key).cloned().unwrap_or_default())
                        .unwrap_or_default()
                } else {
                    String::new()
                };

                let result = raft::StateMachineQueryResult {
                    result: Some(raft::state_machine_query_result::Result::Get(
                        raft::GetValueResult {
                            key: get.key,
                            found,
                            value,
                        },
                    )),
                };
                result.encode_to_vec()
            }
            None => Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use graft_core::state_machine::StateMachine;

    #[test]
    fn put_and_get() {
        let sm = KeyValueStateMachine::new();

        let put_cmd = raft::StateMachineCommand {
            command: Some(raft::state_machine_command::Command::Put(
                raft::PutCommand {
                    key: "k".to_string(),
                    value: "v1".to_string(),
                },
            )),
        };
        sm.apply(1, &put_cmd.encode_to_vec());

        let get_q = raft::StateMachineQuery {
            query: Some(raft::state_machine_query::Query::Get(raft::GetValueQuery {
                key: "k".to_string(),
            })),
        };
        let get_result =
            raft::StateMachineQueryResult::decode(&sm.query(&get_q.encode_to_vec())[..]).unwrap();
        if let Some(raft::state_machine_query_result::Result::Get(r)) = get_result.result {
            assert!(r.found);
            assert_eq!(r.value, "v1");
        } else {
            panic!("expected Get result");
        }
    }

    #[test]
    fn cas_missing_expected_succeeds() {
        let sm = KeyValueStateMachine::new();
        let cas_cmd = raft::StateMachineCommand {
            command: Some(raft::state_machine_command::Command::Cas(
                raft::CasCommand {
                    key: "k".to_string(),
                    expected_present: false,
                    expected_value: String::new(),
                    new_value: "v1".to_string(),
                },
            )),
        };
        let result_bytes = sm.apply_with_result(1, &cas_cmd.encode_to_vec());
        let result = raft::StateMachineCommandResult::decode(&result_bytes[..]).unwrap();
        if let Some(raft::state_machine_command_result::Result::Cas(r)) = result.result {
            assert!(r.matched);
            assert!(!r.current_present);
        } else {
            panic!("expected Cas result");
        }
    }

    #[test]
    fn cas_present_mismatch_reports_false() {
        let sm = KeyValueStateMachine::new();
        let put_cmd = raft::StateMachineCommand {
            command: Some(raft::state_machine_command::Command::Put(
                raft::PutCommand {
                    key: "k".to_string(),
                    value: "old".to_string(),
                },
            )),
        };
        sm.apply(1, &put_cmd.encode_to_vec());

        let cas_cmd = raft::StateMachineCommand {
            command: Some(raft::state_machine_command::Command::Cas(
                raft::CasCommand {
                    key: "k".to_string(),
                    expected_present: true,
                    expected_value: "expected".to_string(),
                    new_value: "new".to_string(),
                },
            )),
        };
        let result_bytes = sm.apply_with_result(1, &cas_cmd.encode_to_vec());
        let result = raft::StateMachineCommandResult::decode(&result_bytes[..]).unwrap();
        if let Some(raft::state_machine_command_result::Result::Cas(r)) = result.result {
            assert!(!r.matched);
            assert!(r.current_present);
            assert_eq!(r.current_value, "old");
        } else {
            panic!("expected Cas result");
        }
    }

    #[test]
    fn snapshot_roundtrip() {
        let sm = KeyValueStateMachine::new();
        let put = raft::StateMachineCommand {
            command: Some(raft::state_machine_command::Command::Put(
                raft::PutCommand {
                    key: "k".to_string(),
                    value: "v1".to_string(),
                },
            )),
        };
        sm.apply(1, &put.encode_to_vec());

        let snap = sm.snapshot();
        let sm2 = KeyValueStateMachine::new();
        sm2.restore(&snap);

        let get = raft::StateMachineQuery {
            query: Some(raft::state_machine_query::Query::Get(raft::GetValueQuery {
                key: "k".to_string(),
            })),
        };
        let result =
            raft::StateMachineQueryResult::decode(&sm2.query(&get.encode_to_vec())[..]).unwrap();
        if let Some(raft::state_machine_query_result::Result::Get(r)) = result.result {
            assert!(r.found);
            assert_eq!(r.value, "v1");
        } else {
            panic!("expected Get result");
        }
    }
}
