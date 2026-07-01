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

//! Varint32 length-prefixed protobuf envelope framing.
//!
//! The Rust transport uses the same length-delimited protobuf framing
//! style as the Java Netty pipeline (`ProtobufLiteEncoder` /
//! `ProtobufLiteDecoder`) and the C++ `envelope_codec.hpp`:
//! a base-128 varint32 length prefix followed by a serialized `Envelope`
//! protobuf message. The three implementations are byte-identical on the
//! wire, validated by the mixed Java/C++/Rust smoke suite.
//!
//! ## Wire format
//!
//! ```text
//! [varint32 length (1–5 bytes)] [serialized protobuf Envelope (length bytes)]
//! ```
//!
//! The `Envelope` contains a `correlation_id` (for request/response
//! matching), a `type` field (string dispatch key, e.g. "VoteRequest"),
//! and a `payload` (serialized inner RPC message).

use bytes::{Buf, BytesMut};
use graft_proto::Envelope;
use prost::Message;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::error::TransportError;

/// Reads a complete framed Envelope from the TCP stream. Buffers partial
/// data internally until a full frame is available, then decodes it.
///
/// Returns `TransportError::ConnectionClosed` when the peer cleanly shuts
/// down the connection (read returns 0 bytes).
pub async fn read_envelope(
    stream: &mut TcpStream,
    buf: &mut BytesMut,
) -> Result<Envelope, TransportError> {
    loop {
        // Try to parse a complete frame from the buffer first.
        if let Some(envelope) = try_parse_envelope(buf)? {
            return Ok(envelope);
        }

        // Read more data from the socket.
        let mut read_buf = vec![0u8; 4096];
        let n = stream.read(&mut read_buf).await?;
        if n == 0 {
            return Err(TransportError::ConnectionClosed);
        }
        buf.extend_from_slice(&read_buf[..n]);
    }
}

/// Attempts to extract a complete Envelope from the front of the buffer
/// without consuming partial frames. Returns:
/// - `Ok(Some(envelope))` when a complete frame is available.
/// - `Ok(None)` when more data is needed (incomplete varint or payload).
/// - `Err(...)` when the data is malformed.
fn try_parse_envelope(buf: &mut BytesMut) -> Result<Option<Envelope>, TransportError> {
    if buf.is_empty() {
        return Ok(None);
    }

    // The varint decoder needs to peek without consuming — use a cursor
    // so the original buffer is untouched if the varint is incomplete.
    let mut cursor = buf.clone();
    let length = match graft_proto::decode_varint32(&mut cursor) {
        Ok(l) => l as usize,
        Err(_) => return Ok(None),
    };

    let header_bytes = buf.len() - cursor.len();
    if buf.len() < header_bytes + length {
        return Ok(None); // Incomplete payload — wait for more data.
    }

    // Consume the header bytes and extract the payload.
    buf.advance(header_bytes);
    let payload = buf.split_to(length);
    let envelope = Envelope::decode(&payload[..])?;

    Ok(Some(envelope))
}

/// Serializes an Envelope and writes it to the TCP stream with a varint32
/// length prefix. The frame is written as a single `write_all` to avoid
/// fragmentation.
pub async fn write_envelope(
    stream: &mut TcpStream,
    envelope: &Envelope,
) -> Result<(), TransportError> {
    let payload = envelope.encode_to_vec();
    let mut frame = BytesMut::with_capacity(5 + payload.len());
    graft_proto::encode_varint32(payload.len() as u32, &mut frame);
    frame.extend_from_slice(&payload);

    stream.write_all(&frame).await?;
    stream.flush().await?;
    Ok(())
}
