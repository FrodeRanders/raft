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

use std::fs;
use std::path::PathBuf;
use std::sync::Mutex;

use graft_core::raft_node::PersistentStateStore as PersistentStateStoreTrait;

// ---------------------------------------------------------------------------
// FilePersistentStateStore
// ---------------------------------------------------------------------------

/// A file-backed persistent state store that survives restarts. Stores
/// `current_term` and `voted_for` in a simple key-value properties file
/// (one `key=value` pair per line). The file is written on every mutation
/// so that a crash between writes never leaves stale state on disk.
///
/// The format is intentionally human-readable for smoke-test debugging:
/// ```text
/// term=7
/// voted=n2
/// ```
pub struct FilePersistentStateStore {
    /// Path to the state file.
    path: PathBuf,
    /// In-memory cache of the current term.
    term: Mutex<u64>,
    /// In-memory cache of the voted-for peer id.
    voted_for: Mutex<Option<String>>,
}

impl FilePersistentStateStore {
    pub fn new(path: PathBuf) -> Self {
        let store = Self {
            path,
            term: Mutex::new(0),
            voted_for: Mutex::new(None),
        };
        store.load();
        store
    }

    /// Reads the state file and populates the in-memory caches. If the
    /// file does not exist, the defaults (term=0, no vote) are kept.
    fn load(&self) {
        if !self.path.exists() {
            return;
        }

        if let Ok(content) = fs::read_to_string(&self.path) {
            for line in content.lines() {
                if let Some((key, value)) = line.split_once('=') {
                    match key.trim() {
                        "term" => {
                            if let Ok(t) = value.trim().parse::<u64>() {
                                *self.term.lock().unwrap() = t;
                            }
                        }
                        "voted" => {
                            let v = value.trim().to_string();
                            *self.voted_for.lock().unwrap() = if v.is_empty() { None } else { Some(v) };
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    /// Persists the in-memory state to the file. Called after every
    /// `set_current_term` and `set_voted_for`.
    fn save(&self) {
        let term = *self.term.lock().unwrap();
        let voted = self.voted_for.lock().unwrap().clone().unwrap_or_default();
        let content = format!("term={}\nvoted={}\n", term, voted);
        let _ = fs::write(&self.path, content);
    }
}

impl PersistentStateStoreTrait for FilePersistentStateStore {
    fn current_term(&self) -> u64 {
        *self.term.lock().unwrap()
    }

    fn set_current_term(&self, term: u64) {
        *self.term.lock().unwrap() = term;
        self.save();
    }

    fn voted_for(&self) -> Option<String> {
        self.voted_for.lock().unwrap().clone()
    }

    fn set_voted_for(&self, peer_id: Option<String>) {
        *self.voted_for.lock().unwrap() = peer_id;
        self.save();
    }
}
