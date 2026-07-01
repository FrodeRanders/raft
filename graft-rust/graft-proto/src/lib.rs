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

//! Protobuf-generated Raft wire types plus varint32 envelope framing.
//!
//! The generated code comes from the shared `raft.proto` file at
//! `raft-wire/src/main/proto/raft.proto`. This is the same `.proto`
//! used by the Java and C++ implementations, ensuring byte-level
//! wire compatibility across all three languages.
//!
//! # Generated module
//!
//! The `raft` module contains all 31 protobuf message types: Envelope,
//! VoteRequest/Response, AppendEntriesRequest/Response,
//! InstallSnapshotRequest/Response, client command/query types,
//! membership commands, cluster summary/telemetry types, and more.
//!
//! # Envelope framing
//!
//! The free functions `encode_varint32`, `decode_varint32`,
//! `encode_frame`, `decode_frame`, `read_envelope`, and
//! `write_envelope` implement the varint32 length-prefix framing
//! used by all three implementations. The encoding is identical to
//! `CodedOutputStream.writeRawVarint32` (Java protobuf-lite) and
//! `graft::encode_varint32` (C++).

/// Generated protobuf message types from `raft.proto`.
pub mod raft {
    include!(concat!(env!("OUT_DIR"), "/raft.rs"));
}

use bytes::{Buf, BufMut, BytesMut};
use prost::Message;
use std::io;

pub use raft::*;

/// Encodes a 32-bit unsigned integer as a base-128 varint into the
/// buffer. Equivalent to protobuf's `writeRawVarint32` and the C++
/// `graft::encode_varint32`.
pub fn encode_varint32(mut value: u32, buf: &mut BytesMut) {
    loop {
        if (value & !0x7F) == 0 {
            buf.put_u8(value as u8);
            break;
        }
        buf.put_u8(((value & 0x7F) | 0x80) as u8);
        value >>= 7;
    }
}

/// Decodes a base-128 varint32 from the buffer. Returns
/// `Err(UnexpectedEof)` if the buffer is exhausted mid-varint, and
/// `Err(InvalidData)` if the varint exceeds 5 bytes (would overflow
/// 32 bits).
pub fn decode_varint32(buf: &mut impl Buf) -> io::Result<u32> {
    let mut result: u32 = 0;
    let mut shift = 0;

    loop {
        if shift >= 35 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "malformed varint32",
            ));
        }
        if !buf.has_remaining() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "incomplete varint32",
            ));
        }
        let byte = buf.get_u8();
        result |= ((byte & 0x7F) as u32) << shift;
        if (byte & 0x80) == 0 {
            return Ok(result);
        }
        shift += 7;
    }
}

/// Wraps a protobuf message in a varint-length-prefixed frame buffer.
/// The resulting `BytesMut` can be written directly to a TCP stream
/// — it contains the varint header and the serialized message bytes
/// in a single contiguous allocation.
pub fn encode_frame(message: &impl Message) -> BytesMut {
    let payload = message.encode_to_vec();
    let mut frame = BytesMut::with_capacity(5 + payload.len());
    encode_varint32(payload.len() as u32, &mut frame);
    frame.extend_from_slice(&payload);
    frame
}

/// Extracts a complete length-prefixed protobuf message from the buffer.
/// Returns `Err(UnexpectedEof)` if the buffer contains an incomplete
/// frame (partial varint or partial payload).
pub fn decode_frame<T: Message + Default>(buf: &mut BytesMut) -> io::Result<T> {
    let len = decode_varint32(buf)? as usize;
    if buf.remaining() < len {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "incomplete frame",
        ));
    }
    let payload = buf.split_to(len);
    T::decode(&payload[..]).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

/// Convenience: decodes a varint-framed `Envelope` from the buffer.
pub fn read_envelope(buf: &mut BytesMut) -> io::Result<Envelope> {
    decode_frame::<Envelope>(buf)
}

/// Convenience: encodes an `Envelope` with a varint length prefix
/// into a `BytesMut` ready for `write_all`.
pub fn write_envelope(envelope: &Envelope) -> BytesMut {
    encode_frame(envelope)
}
