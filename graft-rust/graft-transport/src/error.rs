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

use std::io;
use prost::DecodeError;
use tokio::sync::oneshot;

/// Unified error type covering all transport-layer failure modes:
/// I/O errors, protobuf decode failures, connection loss, timeouts,
/// and application-level send failures.
#[derive(Debug)]
pub enum TransportError {
    /// Underlying TCP I/O error.
    Io(io::Error),
    /// Protobuf decode failure (malformed or truncated message).
    Decode(DecodeError),
    /// The remote peer cleanly closed the connection (read returned 0).
    ConnectionClosed,
    /// A request did not receive a response within the configured timeout.
    Timeout,
    /// Application-level send failure (e.g., unknown peer, transport rejected).
    SendError(String),
    /// The response oneshot channel was dropped before a response arrived.
    RecvError(oneshot::error::RecvError),
}

impl std::fmt::Display for TransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TransportError::Io(e) => write!(f, "IO error: {}", e),
            TransportError::Decode(e) => write!(f, "Decode error: {}", e),
            TransportError::ConnectionClosed => write!(f, "Connection closed"),
            TransportError::Timeout => write!(f, "Timeout"),
            TransportError::SendError(msg) => write!(f, "Send error: {}", msg),
            TransportError::RecvError(e) => write!(f, "Recv error: {}", e),
        }
    }
}

impl std::error::Error for TransportError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            TransportError::Io(e) => Some(e),
            TransportError::Decode(e) => Some(e),
            TransportError::RecvError(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for TransportError {
    fn from(e: io::Error) -> Self {
        TransportError::Io(e)
    }
}

impl From<DecodeError> for TransportError {
    fn from(e: DecodeError) -> Self {
        TransportError::Decode(e)
    }
}

impl From<oneshot::error::RecvError> for TransportError {
    fn from(e: oneshot::error::RecvError) -> Self {
        TransportError::RecvError(e)
    }
}
