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

use graft_core::raft_node::PersistentStateStore as PersistentStateStoreTrait;
use std::{fs, io::ErrorKind, path::PathBuf, sync::Mutex};

#[derive(Default)]
struct State {
    term: u64,
    vote: Option<String>,
}

/// Mutations are serialized through durable publication; failure is fail-stop.
pub struct FilePersistentStateStore {
    path: PathBuf,
    state: Mutex<State>,
}

impl FilePersistentStateStore {
    pub fn new(path: PathBuf) -> Self {
        let state = match fs::read_to_string(&path) {
            Err(e) if e.kind() == ErrorKind::NotFound => State::default(),
            Err(e) => panic!("cannot open Raft state {}: {e}", path.display()),
            Ok(content) => {
                let mut term = None;
                let mut vote = None;
                for line in content.lines() {
                    let (key, value) = line.split_once('=').expect("malformed Raft state");
                    match key {
                        "term" if term.is_none() => {
                            term = Some(value.parse().expect("invalid Raft term"))
                        }
                        "voted" if vote.is_none() => {
                            vote = Some(if value.is_empty() {
                                None
                            } else {
                                Some(value.to_owned())
                            })
                        }
                        _ => panic!("unknown or duplicate Raft state field"),
                    }
                }
                State {
                    term: term.expect("missing Raft term"),
                    vote: vote.expect("missing Raft vote"),
                }
            }
        };
        Self {
            path,
            state: Mutex::new(state),
        }
    }

    fn save(&self, state: &State) {
        crate::durable::persist(
            &self.path,
            format!(
                "term={}\nvoted={}\n",
                state.term,
                state.vote.as_deref().unwrap_or("")
            )
            .as_bytes(),
        );
    }
}

impl PersistentStateStoreTrait for FilePersistentStateStore {
    fn current_term(&self) -> u64 {
        self.state.lock().unwrap().term
    }
    fn voted_for(&self) -> Option<String> {
        self.state.lock().unwrap().vote.clone()
    }
    fn set_current_term(&self, term: u64) {
        let mut state = self.state.lock().unwrap();
        assert!(term >= state.term, "Raft term cannot decrease");
        if term > state.term {
            state.vote = None;
        }
        state.term = term;
        self.save(&state);
    }
    fn set_voted_for(&self, peer_id: Option<String>) {
        assert!(
            !peer_id
                .as_ref()
                .is_some_and(|id| id.contains(['\n', '\r']) || id.is_empty()),
            "invalid Raft voter id"
        );
        let mut state = self.state.lock().unwrap();
        state.vote = peer_id;
        self.save(&state);
    }
}
