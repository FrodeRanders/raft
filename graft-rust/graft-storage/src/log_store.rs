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

use graft_core::raft_node::LogStore as LogStoreTrait;
use graft_core::raft_node::PersistentStateStore as PersistentStateStoreTrait;
use graft_core::types::LogEntry;
use std::{fs, io::ErrorKind, path::PathBuf, sync::Mutex};

/// Snapshot metadata, bytes and retained entries share one atomic durable record.
pub struct FileLogStore {
    path: PathBuf,
    inner: InMemoryLogStore,
    mutation: Mutex<()>,
}

impl FileLogStore {
    pub fn new(path: PathBuf) -> Self {
        let store = Self {
            path,
            inner: InMemoryLogStore::new(),
            mutation: Mutex::new(()),
        };
        match fs::read_to_string(&store.path) {
            Err(e) if e.kind() == ErrorKind::NotFound => {}
            Err(e) => panic!("cannot open Raft log {}: {e}", store.path.display()),
            Ok(content) => {
                let mut index = None;
                let mut term = None;
                let mut data = None;
                let mut entries = Vec::new();
                for line in content.lines() {
                    let (key, value) = line.split_once('=').expect("malformed Raft log");
                    match key {
                        "snapshot_index" if index.is_none() => {
                            index = Some(value.parse::<u64>().expect("invalid snapshot index"))
                        }
                        "snapshot_term" if term.is_none() => {
                            term = Some(value.parse::<u64>().expect("invalid snapshot term"))
                        }
                        "snapshot_data" if data.is_none() => data = Some(decode_checked(value)),
                        "log_entry" => {
                            let fields: Vec<_> = value.split(',').collect();
                            assert!(
                                fields.len() == 3 || fields.len() == 4,
                                "invalid Raft log entry"
                            );
                            let expected = index
                                .expect("snapshot metadata must precede log entries")
                                + entries.len() as u64
                                + 1;
                            assert_eq!(
                                fields[0].parse::<u64>().expect("invalid entry index"),
                                expected,
                                "noncontiguous Raft log"
                            );
                            let peer = if fields.len() == 4 {
                                String::from_utf8(decode_checked(fields[3]))
                                    .expect("invalid peer id")
                            } else {
                                String::new()
                            };
                            entries.push(LogEntry::new(
                                fields[1].parse().expect("invalid entry term"),
                                peer,
                                decode_checked(fields[2]),
                            ));
                        }
                        _ => panic!("unknown or duplicate Raft log field"),
                    }
                }
                store.inner.install_snapshot(
                    index.expect("missing snapshot index"),
                    term.expect("missing snapshot term"),
                    data.expect("missing snapshot bytes"),
                );
                store.inner.append(entries);
            }
        }
        store
    }

    fn save(&self) {
        let index = self.inner.snapshot_index();
        let mut record = format!(
            "snapshot_index={}\nsnapshot_term={}\nsnapshot_data={}\n",
            index,
            self.inner.snapshot_term(),
            base64_encode(&self.inner.snapshot_data())
        );
        for (offset, entry) in self.inner.entries_from(index + 1).iter().enumerate() {
            record.push_str(&format!(
                "log_entry={},{},{},{}\n",
                index + offset as u64 + 1,
                entry.term,
                base64_encode(&entry.data),
                base64_encode(entry.peer_id.as_bytes())
            ));
        }
        crate::durable::persist(&self.path, record.as_bytes());
    }
}

fn decode_checked(value: &str) -> Vec<u8> {
    let data = base64_decode(value);
    assert_eq!(
        base64_encode(&data).trim_end_matches('='),
        value.trim_end_matches('='),
        "invalid Raft base64"
    );
    data
}

impl LogStoreTrait for FileLogStore {
    fn snapshot_index(&self) -> u64 {
        self.inner.snapshot_index()
    }
    fn snapshot_term(&self) -> u64 {
        self.inner.snapshot_term()
    }
    fn last_index(&self) -> u64 {
        self.inner.last_index()
    }
    fn last_term(&self) -> u64 {
        self.inner.last_term()
    }
    fn term_at(&self, index: u64) -> u64 {
        self.inner.term_at(index)
    }
    fn entry_at(&self, index: u64) -> Option<LogEntry> {
        self.inner.entry_at(index)
    }
    fn entries_from(&self, index: u64) -> Vec<LogEntry> {
        self.inner.entries_from(index)
    }
    fn snapshot_data(&self) -> Vec<u8> {
        self.inner.snapshot_data()
    }
    fn append(&self, entries: Vec<LogEntry>) {
        let _guard = self.mutation.lock().unwrap();
        self.inner.append(entries);
        self.save();
    }
    fn truncate_from(&self, index: u64) {
        let _guard = self.mutation.lock().unwrap();
        self.inner.truncate_from(index);
        self.save();
    }
    fn compact_up_to(&self, index: u64) {
        let _guard = self.mutation.lock().unwrap();
        self.inner.compact_up_to(index);
        self.save();
    }
    fn install_snapshot(&self, index: u64, term: u64, data: Vec<u8>) {
        let _guard = self.mutation.lock().unwrap();
        self.inner.install_snapshot(index, term, data);
        self.save();
    }
}

// -- Simple base64 encoding (no external dep) --

pub fn base64_encode(data: &[u8]) -> String {
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
        let triple = (b0 << 16) | (b1 << 8) | b2;
        out.push(CHARS[((triple >> 18) & 0x3F) as usize] as char);
        out.push(CHARS[((triple >> 12) & 0x3F) as usize] as char);
        if chunk.len() > 1 {
            out.push(CHARS[((triple >> 6) & 0x3F) as usize] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(CHARS[(triple & 0x3F) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
}

pub fn base64_decode(s: &str) -> Vec<u8> {
    let s = s.trim();
    if s.is_empty() {
        return vec![];
    }
    let mut out = Vec::new();
    let mut buffer: u32 = 0;
    let mut bits = 0;
    for b in s.bytes() {
        let val = match b {
            b'A'..=b'Z' => b - b'A',
            b'a'..=b'z' => b - b'a' + 26,
            b'0'..=b'9' => b - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            b'=' => break,
            _ => continue,
        };
        buffer = (buffer << 6) | val as u32;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((buffer >> bits) as u8);
            buffer &= (1 << bits) - 1;
        }
    }
    out
}

// ---------------------------------------------------------------------------
// InMemoryLogStore (for tests and ephemeral runs)
// ---------------------------------------------------------------------------

/// A purely in-memory Raft log store.
pub struct InMemoryLogStore {
    entries: Mutex<Vec<LogEntry>>,
    snapshot_index: Mutex<u64>,
    snapshot_term: Mutex<u64>,
    snapshot_data: Mutex<Vec<u8>>,
}

impl InMemoryLogStore {
    pub fn new() -> Self {
        Self {
            entries: Mutex::new(Vec::new()),
            snapshot_index: Mutex::new(0),
            snapshot_term: Mutex::new(0),
            snapshot_data: Mutex::new(Vec::new()),
        }
    }
}

impl LogStoreTrait for InMemoryLogStore {
    fn snapshot_index(&self) -> u64 {
        *self.snapshot_index.lock().unwrap()
    }
    fn snapshot_term(&self) -> u64 {
        *self.snapshot_term.lock().unwrap()
    }
    fn last_index(&self) -> u64 {
        let entries = self.entries.lock().unwrap();
        let si = *self.snapshot_index.lock().unwrap();
        if entries.is_empty() {
            si
        } else {
            si + entries.len() as u64
        }
    }
    fn last_term(&self) -> u64 {
        let entries = self.entries.lock().unwrap();
        entries
            .last()
            .map(|e| e.term)
            .unwrap_or_else(|| *self.snapshot_term.lock().unwrap())
    }
    fn term_at(&self, index: u64) -> u64 {
        if index == 0 {
            return 0;
        }
        let si = *self.snapshot_index.lock().unwrap();
        if index == si {
            return *self.snapshot_term.lock().unwrap();
        }
        if index < si {
            return 0;
        }
        let entries = self.entries.lock().unwrap();
        let local = (index - si - 1) as usize;
        entries.get(local).map(|e| e.term).unwrap_or(0)
    }
    fn entry_at(&self, index: u64) -> Option<LogEntry> {
        let si = *self.snapshot_index.lock().unwrap();
        if index <= si {
            return None;
        }
        let entries = self.entries.lock().unwrap();
        let local = (index - si - 1) as usize;
        entries.get(local).cloned()
    }
    fn append(&self, new_entries: Vec<LogEntry>) {
        self.entries.lock().unwrap().extend(new_entries);
    }
    fn truncate_from(&self, index: u64) {
        let si = *self.snapshot_index.lock().unwrap();
        if index <= si {
            return;
        }
        let mut entries = self.entries.lock().unwrap();
        let local = (index - si - 1) as usize;
        if local < entries.len() {
            entries.truncate(local);
        }
    }
    fn entries_from(&self, index: u64) -> Vec<LogEntry> {
        let si = *self.snapshot_index.lock().unwrap();
        if index <= si {
            return vec![];
        }
        let entries = self.entries.lock().unwrap();
        let local = (index - si - 1) as usize;
        if local >= entries.len() {
            return vec![];
        }
        entries[local..].to_vec()
    }
    fn compact_up_to(&self, index: u64) {
        if index <= self.snapshot_index() {
            return;
        }
        assert!(index <= self.last_index(), "snapshot exceeds log");
        self.install_snapshot(index, self.term_at(index), self.snapshot_data());
    }
    fn snapshot_data(&self) -> Vec<u8> {
        self.snapshot_data.lock().unwrap().clone()
    }
    fn install_snapshot(&self, index: u64, term: u64, data: Vec<u8>) {
        let base = self.snapshot_index();
        if index < base {
            return;
        }
        let matching = self.term_at(index) == term;
        let mut entries = self.entries.lock().unwrap();
        if matching && index <= base + entries.len() as u64 {
            entries.drain(..(index - base) as usize);
        } else {
            entries.clear();
        }
        *self.snapshot_index.lock().unwrap() = index;
        *self.snapshot_term.lock().unwrap() = term;
        *self.snapshot_data.lock().unwrap() = data;
    }
}

/// A purely in-memory persistent state store.
pub struct InMemoryPersistentStateStore {
    current_term: Mutex<u64>,
    voted_for: Mutex<Option<String>>,
}

impl InMemoryPersistentStateStore {
    pub fn new() -> Self {
        Self {
            current_term: Mutex::new(0),
            voted_for: Mutex::new(None),
        }
    }
}

impl PersistentStateStoreTrait for InMemoryPersistentStateStore {
    fn current_term(&self) -> u64 {
        *self.current_term.lock().unwrap()
    }
    fn set_current_term(&self, term: u64) {
        *self.current_term.lock().unwrap() = term;
    }
    fn voted_for(&self) -> Option<String> {
        self.voted_for.lock().unwrap().clone()
    }
    fn set_voted_for(&self, peer_id: Option<String>) {
        *self.voted_for.lock().unwrap() = peer_id;
    }
}
