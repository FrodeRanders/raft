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

/// Encodes arbitrary bytes as a Base64 string (standard alphabet, no padding).
fn base64_encode(data: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity((data.len() + 2) / 3 * 4);
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
        let triple = (b0 << 16) | (b1 << 8) | b2;
        out.push(ALPHABET[((triple >> 18) & 0x3F) as usize] as char);
        out.push(ALPHABET[((triple >> 12) & 0x3F) as usize] as char);
        if chunk.len() > 1 {
            out.push(ALPHABET[((triple >> 6) & 0x3F) as usize] as char);
        }
        if chunk.len() > 2 {
            out.push(ALPHABET[(triple & 0x3F) as usize] as char);
        }
    }
    out
}

/// Decodes a Base64 string into bytes.
fn base64_decode(s: &str) -> Option<Vec<u8>> {
    const DECODE: [i8; 128] = {
        let mut table = [-1i8; 128];
        let alphabet = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let mut i = 0;
        while i < 64 {
            table[alphabet[i] as usize] = i as i8;
            i += 1;
        }
        table
    };
    let mut out = Vec::with_capacity(s.len() * 3 / 4);
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'=' {
            break;
        }
        let v0 = DECODE.get(bytes[i] as usize).copied().unwrap_or(-1);
        let v1 = DECODE.get(bytes.get(i + 1).copied().unwrap_or(0) as usize).copied().unwrap_or(-1);
        let v2 = DECODE.get(bytes.get(i + 2).copied().unwrap_or(0) as usize).copied().unwrap_or(-1);
        let v3 = DECODE.get(bytes.get(i + 3).copied().unwrap_or(0) as usize).copied().unwrap_or(-1);
        if v0 < 0 || v1 < 0 {
            return None;
        }
        out.push(((v0 as u32) << 2 | (v1 as u32) >> 4) as u8);
        if v2 >= 0 {
            out.push(((v1 as u32) << 4 | (v2 as u32) >> 2) as u8);
        }
        if v3 >= 0 {
            out.push(((v2 as u32) << 6 | v3 as u32) as u8);
        }
        i += 4;
    }
    Some(out)
}

fn quote_json(value: &str) -> String {
    let escaped = value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\t', "\\t");
    format!("\"{}\"", escaped)
}

// ---------------------------------------------------------------------------
// Public API — Java / C++ compatible snapshot wrapping
// ---------------------------------------------------------------------------

/// Wraps a domain snapshot with current/next membership so a follower or
/// restarted node can restore the correct configuration at the compaction
/// boundary.  Format is a JSON envelope identical to Java's
/// `ClusterConfigurationSnapshotCodec` and C++'s `SnapshotCodec::wrap_payload`.
pub fn wrap_snapshot_payload(
    current_members: &[String],
    next_members: &[String],
    state_machine_snapshot: &[u8],
) -> Vec<u8> {
    let mut out = String::from("{\"version\":1,\"currentMembers\":[");

    let mut first = true;
    for id in current_members {
        if !first {
            out.push(',');
        }
        first = false;
        out.push_str(&format!(
            "{{\"id\":{},\"role\":\"VOTER\",\"address\":null}}",
            quote_json(id)
        ));
    }

    out.push_str("],\"nextMembers\":[");
    first = true;
    for id in next_members {
        if !first {
            out.push(',');
        }
        first = false;
        out.push_str(&format!(
            "{{\"id\":{},\"role\":\"VOTER\",\"address\":null}}",
            quote_json(id)
        ));
    }

    out.push_str("],\"stateMachineSnapshot\":");
    out.push_str(&quote_json(&base64_encode(state_machine_snapshot)));
    out.push('}');
    out.into_bytes()
}

/// Returns only the domain payload for `ApplicationStateMachine::restore()`.
/// Handles the wrapped JSON envelope (newer format) and passes through
/// raw application bytes unchanged (older/local snapshots).
pub fn unwrap_snapshot_payload(payload: &[u8]) -> Vec<u8> {
    let s = match std::str::from_utf8(payload) {
        Ok(s) => s,
        Err(_) => return payload.to_vec(),
    };
    if !s.starts_with('{') {
        return payload.to_vec();
    }
    let marker = "\"stateMachineSnapshot\":\"";
    let start = match s.find(marker) {
        Some(p) => p + marker.len(),
        None => return payload.to_vec(),
    };
    let end = match s[start..].find('"') {
        Some(p) => start + p,
        None => return payload.to_vec(),
    };
    base64_decode(&s[start..end]).unwrap_or_else(|| payload.to_vec())
}

// ---------------------------------------------------------------------------
// Java-compatible key-value binary snapshot (big-endian, deterministic)
// ---------------------------------------------------------------------------

fn append_u32_be(out: &mut Vec<u8>, value: u32) {
    out.push((value >> 24) as u8);
    out.push((value >> 16) as u8);
    out.push((value >> 8) as u8);
    out.push(value as u8);
}

fn append_u16_be(out: &mut Vec<u8>, value: u16) {
    out.push((value >> 8) as u8);
    out.push(value as u8);
}

fn read_u32_be(data: &[u8], offset: &mut usize) -> Option<u32> {
    if *offset + 4 > data.len() {
        return None;
    }
    let v = ((data[*offset] as u32) << 24)
        | ((data[*offset + 1] as u32) << 16)
        | ((data[*offset + 2] as u32) << 8)
        | (data[*offset + 3] as u32);
    *offset += 4;
    Some(v)
}

fn read_u16_be(data: &[u8], offset: &mut usize) -> Option<u16> {
    if *offset + 2 > data.len() {
        return None;
    }
    let v = ((data[*offset] as u16) << 8) | (data[*offset + 1] as u16);
    *offset += 2;
    Some(v)
}

/// Serializes a key-value map into a deterministic binary format
/// compatible with Java's `SnapshotCodec.serializeKeyValueSnapshot`.
pub fn serialize_key_value_snapshot(kv: &HashMap<String, Vec<u8>>) -> Vec<u8> {
    let mut out = Vec::new();
    let mut ordered: Vec<(&String, &Vec<u8>)> = kv.iter().collect();
    ordered.sort_by(|a, b| a.0.cmp(b.0));

    append_u32_be(&mut out, ordered.len() as u32);
    for (key, value) in &ordered {
        append_u16_be(&mut out, key.len() as u16);
        out.extend_from_slice(key.as_bytes());
        append_u16_be(&mut out, value.len() as u16);
        out.extend_from_slice(value);
    }
    out
}

/// Deserializes a binary key-value snapshot produced by
/// `serialize_key_value_snapshot` (or the equivalent Java/C++ format).
pub fn deserialize_key_value_snapshot(data: &[u8]) -> Option<HashMap<String, Vec<u8>>> {
    let mut offset = 0;
    let count = read_u32_be(data, &mut offset)? as usize;
    let mut kv = HashMap::with_capacity(count);
    for _ in 0..count {
        let key_len = read_u16_be(data, &mut offset)? as usize;
        if offset + key_len > data.len() {
            return None;
        }
        let key = String::from_utf8(data[offset..offset + key_len].to_vec()).ok()?;
        offset += key_len;

        let val_len = read_u16_be(data, &mut offset)? as usize;
        if offset + val_len > data.len() {
            return None;
        }
        let value = data[offset..offset + val_len].to_vec();
        offset += val_len;

        kv.insert(key, value);
    }
    Some(kv)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_kv_snapshot() {
        let mut kv = HashMap::new();
        kv.insert("k".to_string(), b"v1".to_vec());
        kv.insert("a".to_string(), b"b".to_vec());
        let data = serialize_key_value_snapshot(&kv);
        let restored = deserialize_key_value_snapshot(&data).unwrap();
        assert_eq!(restored.get("k").unwrap(), b"v1");
        assert_eq!(restored.get("a").unwrap(), b"b");
    }

    #[test]
    fn wrap_and_unwrap_payload() {
        let sm_data = b"hello";
        let current = vec!["n1".to_string(), "n2".to_string()];
        let next: Vec<String> = vec![];
        let wrapped = wrap_snapshot_payload(&current, &next, sm_data);
        let unwrapped = unwrap_snapshot_payload(&wrapped);
        assert_eq!(unwrapped, sm_data);
    }

    #[test]
    fn wrap_and_unwrap_joint_consensus() {
        let sm_data = b"world";
        let current = vec!["a".to_string()];
        let next = vec!["a".to_string(), "b".to_string()];
        let wrapped = wrap_snapshot_payload(&current, &next, sm_data);
        let unwrapped = unwrap_snapshot_payload(&wrapped);
        assert_eq!(unwrapped, sm_data);
    }

    #[test]
    fn unwrap_raw_bytes_passthrough() {
        let raw = b"not-json";
        assert_eq!(unwrap_snapshot_payload(raw), raw);
    }

    #[test]
    fn base64_roundtrip() {
        let data = b"hello world";
        let enc = base64_encode(data);
        let dec = base64_decode(&enc).unwrap();
        assert_eq!(dec, data);
    }
}
