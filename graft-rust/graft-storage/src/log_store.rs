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
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::sync::Mutex;

use graft_core::raft_node::{LogStore as LogStoreTrait, PersistentStateStore as PersistentStateStoreTrait};
use graft_core::types::LogEntry;

/// A file-backed Raft log store that persists log entries and snapshot
/// metadata to disk. Uses a simple line-oriented key=value format for
/// debuggability, mirroring the C++ `PersistentStateStore`.
///
/// ## File format
///
/// ```text
/// snapshot_index=10
/// snapshot_term=3
/// snapshot_data=<base64>
/// log_entry=<index>,<term>,<base64-data>
/// ...
/// ```
///
/// The file is rewritten on every mutation. This is intentionally simple,
/// not a production-grade append-only log. It captures enough state for
/// restart recovery and mixed-language smoke validation.
pub struct FileLogStore {
    path: PathBuf,
    /// In-memory copy of the log entries. The file is the source of truth;
    /// this is the working copy.
    entries: Mutex<Vec<LogEntry>>,
    snapshot_index: Mutex<u64>,
    snapshot_term: Mutex<u64>,
    snapshot_data: Mutex<Vec<u8>>,
}

impl FileLogStore {
    /// Opens or creates a log store at the given file path. If the file
    /// already exists, the stored state is loaded.
    pub fn new(path: PathBuf) -> Self {
        let store = Self {
            path,
            entries: Mutex::new(Vec::new()),
            snapshot_index: Mutex::new(0),
            snapshot_term: Mutex::new(0),
            snapshot_data: Mutex::new(Vec::new()),
        };
        store.load();
        store
    }

    fn load(&self) {
        if !self.path.exists() {
            return;
        }
        let file = match fs::File::open(&self.path) {
            Ok(f) => f,
            Err(_) => return,
        };
        let reader = BufReader::new(file);

        let mut snapshot_index = 0u64;
        let mut snapshot_term = 0u64;
        let mut snapshot_data = Vec::new();
        let mut entries = Vec::new();

        for line in reader.lines() {
            let line = match line { Ok(l) => l, Err(_) => continue };
            if let Some((key, value)) = line.split_once('=') {
                match key.trim() {
                    "snapshot_index" => {
                        snapshot_index = value.trim().parse().unwrap_or(0);
                    }
                    "snapshot_term" => {
                        snapshot_term = value.trim().parse().unwrap_or(0);
                    }
                    "snapshot_data" => {
                        snapshot_data = base64_decode(value.trim());
                    }
                    "log_entry" => {
                        // Format: index,term,base64-data
                        let parts: Vec<&str> = value.splitn(3, ',').collect();
                        if parts.len() >= 2 {
                            let term = parts[1].trim().parse().unwrap_or(0);
                            let data = if parts.len() > 2 {
                                base64_decode(parts[2].trim())
                            } else {
                                Vec::new()
                            };
                            // Peer id is lost across restarts in this simple
                            // format. Use empty string.
                            entries.push(LogEntry::new(term, String::new(), data));
                        }
                    }
                    _ => {}
                }
            }
        }

        *self.snapshot_index.lock().unwrap() = snapshot_index;
        *self.snapshot_term.lock().unwrap() = snapshot_term;
        *self.snapshot_data.lock().unwrap() = snapshot_data;
        *self.entries.lock().unwrap() = entries;
    }

    fn save(&self) {
        if let Some(parent) = self.path.parent() {
            let _ = fs::create_dir_all(parent);
        }
        let mut f = match fs::File::create(&self.path) {
            Ok(f) => f,
            Err(_) => return,
        };

        let si = *self.snapshot_index.lock().unwrap();
        let st = *self.snapshot_term.lock().unwrap();
        let sd = self.snapshot_data.lock().unwrap().clone();
        let entries = self.entries.lock().unwrap();

        writeln!(f, "snapshot_index={}", si).ok();
        writeln!(f, "snapshot_term={}", st).ok();
        writeln!(f, "snapshot_data={}", base64_encode(&sd)).ok();
        for (i, entry) in entries.iter().enumerate() {
            let logical_idx = si + i as u64 + 1;
            writeln!(
                f,
                "log_entry={},{},{}",
                logical_idx,
                entry.term,
                base64_encode(&entry.data)
            ).ok();
        }
        f.flush().ok();
    }
}

impl LogStoreTrait for FileLogStore {
    fn snapshot_index(&self) -> u64 {
        *self.snapshot_index.lock().unwrap()
    }
    fn snapshot_term(&self) -> u64 {
        *self.snapshot_term.lock().unwrap()
    }
    fn last_index(&self) -> u64 {
        let entries = self.entries.lock().unwrap();
        let si = *self.snapshot_index.lock().unwrap();
        if entries.is_empty() { si } else { si + entries.len() as u64 }
    }
    fn last_term(&self) -> u64 {
        let entries = self.entries.lock().unwrap();
        entries.last().map(|e| e.term).unwrap_or_else(|| {
            *self.snapshot_term.lock().unwrap()
        })
    }
    fn term_at(&self, index: u64) -> u64 {
        if index == 0 { return 0; }
        let si = *self.snapshot_index.lock().unwrap();
        if index == si { return *self.snapshot_term.lock().unwrap(); }
        if index < si { return 0; }
        let entries = self.entries.lock().unwrap();
        let local = (index - si - 1) as usize;
        entries.get(local).map(|e| e.term).unwrap_or(0)
    }
    fn entry_at(&self, index: u64) -> Option<LogEntry> {
        let si = *self.snapshot_index.lock().unwrap();
        if index <= si { return None; }
        let entries = self.entries.lock().unwrap();
        let local = (index - si - 1) as usize;
        entries.get(local).cloned()
    }
    fn append(&self, new_entries: Vec<LogEntry>) {
        self.entries.lock().unwrap().extend(new_entries);
        self.save();
    }
    fn truncate_from(&self, index: u64) {
        let si = *self.snapshot_index.lock().unwrap();
        if index <= si { return; }
        let mut entries = self.entries.lock().unwrap();
        let local = (index - si - 1) as usize;
        if local < entries.len() {
            entries.truncate(local);
        }
        self.save();
    }
    fn entries_from(&self, index: u64) -> Vec<LogEntry> {
        let si = *self.snapshot_index.lock().unwrap();
        if index <= si { return vec![]; }
        let entries = self.entries.lock().unwrap();
        let local = (index - si - 1) as usize;
        if local >= entries.len() { return vec![]; }
        entries[local..].to_vec()
    }
    fn compact_up_to(&self, index: u64) {
        let si = *self.snapshot_index.lock().unwrap();
        if index <= si { return; }
        let remove_count = (index - si) as usize;
        let mut entries = self.entries.lock().unwrap();
        if remove_count <= entries.len() {
            entries.drain(0..remove_count);
        } else {
            entries.clear();
        }
        *self.snapshot_index.lock().unwrap() = index;
        self.save();
    }
    fn snapshot_data(&self) -> Vec<u8> {
        self.snapshot_data.lock().unwrap().clone()
    }
    fn install_snapshot(&self, last_included_index: u64, last_included_term: u64, snapshot_data: Vec<u8>) {
        *self.snapshot_index.lock().unwrap() = last_included_index;
        *self.snapshot_term.lock().unwrap() = last_included_term;
        *self.snapshot_data.lock().unwrap() = snapshot_data;
        self.entries.lock().unwrap().clear();
        self.save();
    }
}

// -- Simple base64 encoding (no external dep) --

fn base64_encode(data: &[u8]) -> String {
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

fn base64_decode(s: &str) -> Vec<u8> {
    let s = s.trim();
    if s.is_empty() { return vec![]; }
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
    fn snapshot_index(&self) -> u64 { *self.snapshot_index.lock().unwrap() }
    fn snapshot_term(&self) -> u64 { *self.snapshot_term.lock().unwrap() }
    fn last_index(&self) -> u64 {
        let entries = self.entries.lock().unwrap();
        let si = *self.snapshot_index.lock().unwrap();
        if entries.is_empty() { si } else { si + entries.len() as u64 }
    }
    fn last_term(&self) -> u64 {
        let entries = self.entries.lock().unwrap();
        entries.last().map(|e| e.term).unwrap_or_else(|| *self.snapshot_term.lock().unwrap())
    }
    fn term_at(&self, index: u64) -> u64 {
        if index == 0 { return 0; }
        let si = *self.snapshot_index.lock().unwrap();
        if index == si { return *self.snapshot_term.lock().unwrap(); }
        if index < si { return 0; }
        let entries = self.entries.lock().unwrap();
        let local = (index - si - 1) as usize;
        entries.get(local).map(|e| e.term).unwrap_or(0)
    }
    fn entry_at(&self, index: u64) -> Option<LogEntry> {
        let si = *self.snapshot_index.lock().unwrap();
        if index <= si { return None; }
        let entries = self.entries.lock().unwrap();
        let local = (index - si - 1) as usize;
        entries.get(local).cloned()
    }
    fn append(&self, new_entries: Vec<LogEntry>) { self.entries.lock().unwrap().extend(new_entries); }
    fn truncate_from(&self, index: u64) {
        let si = *self.snapshot_index.lock().unwrap();
        if index <= si { return; }
        let mut entries = self.entries.lock().unwrap();
        let local = (index - si - 1) as usize;
        if local < entries.len() { entries.truncate(local); }
    }
    fn entries_from(&self, index: u64) -> Vec<LogEntry> {
        let si = *self.snapshot_index.lock().unwrap();
        if index <= si { return vec![]; }
        let entries = self.entries.lock().unwrap();
        let local = (index - si - 1) as usize;
        if local >= entries.len() { return vec![]; }
        entries[local..].to_vec()
    }
    fn compact_up_to(&self, index: u64) {
        let si = *self.snapshot_index.lock().unwrap();
        if index <= si { return; }
        let remove_count = (index - si) as usize;
        let mut entries = self.entries.lock().unwrap();
        if remove_count <= entries.len() { entries.drain(0..remove_count); } else { entries.clear(); }
        *self.snapshot_index.lock().unwrap() = index;
    }
    fn snapshot_data(&self) -> Vec<u8> { self.snapshot_data.lock().unwrap().clone() }
    fn install_snapshot(&self, li: u64, lt: u64, sd: Vec<u8>) {
        *self.snapshot_index.lock().unwrap() = li;
        *self.snapshot_term.lock().unwrap() = lt;
        *self.snapshot_data.lock().unwrap() = sd;
        self.entries.lock().unwrap().clear();
    }
}

/// A purely in-memory persistent state store.
pub struct InMemoryPersistentStateStore {
    current_term: Mutex<u64>,
    voted_for: Mutex<Option<String>>,
}

impl InMemoryPersistentStateStore {
    pub fn new() -> Self {
        Self { current_term: Mutex::new(0), voted_for: Mutex::new(None) }
    }
}

impl PersistentStateStoreTrait for InMemoryPersistentStateStore {
    fn current_term(&self) -> u64 { *self.current_term.lock().unwrap() }
    fn set_current_term(&self, term: u64) { *self.current_term.lock().unwrap() = term; }
    fn voted_for(&self) -> Option<String> { self.voted_for.lock().unwrap().clone() }
    fn set_voted_for(&self, peer_id: Option<String>) { *self.voted_for.lock().unwrap() = peer_id; }
}
