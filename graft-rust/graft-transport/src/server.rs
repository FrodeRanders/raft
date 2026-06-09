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

use std::net::SocketAddr;
use std::sync::Arc;

use bytes::BytesMut;
use graft_proto::Envelope;
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info};

use crate::codec;
use crate::error::TransportError;

// ---------------------------------------------------------------------------
// MessageHandler — inbound RPC dispatch
// ---------------------------------------------------------------------------

/// Callback interface for dispatching decoded protobuf envelopes to the
/// Raft runtime. The transport layer decodes the varint frame and the
/// `Envelope` message, then calls `handle()` with the `correlation_id`,
/// envelope type string, and raw payload bytes.
///
/// The handler returns the response payload bytes (serialized protobuf
/// response message). The transport wraps this in an `Envelope` response
/// and writes it back to the TCP stream.
///
/// A zero-length envelope is technically parseable as an empty protobuf
/// message, but callers will reject it because type/correlation/payload
/// are absent.
pub trait MessageHandler: Send + Sync {
    fn handle(&self, correlation_id: &str, envelope_type: &str, payload: &[u8])
        -> Result<Vec<u8>, String>;
}

// ---------------------------------------------------------------------------
// RaftServer — inbound TCP listener
// ---------------------------------------------------------------------------

/// Async TCP server that accepts connections, reads framed protobuf
/// envelopes, dispatches them to a `MessageHandler`, and writes back
/// response envelopes.
///
/// The server uses tokio's multi-threaded runtime — each accepted
/// connection is handled in its own spawned task. This matches the
/// Java `RaftServer` (Netty ServerBootstrap) and the C++ `RaftServer`
/// (Boost.Asio acceptor).
pub struct RaftServer {
    handler: Arc<dyn MessageHandler>,
}

impl RaftServer {
    pub fn new(handler: Arc<dyn MessageHandler>) -> Self {
        Self { handler }
    }

    /// Starts the server on `bind_addr` and blocks the current task
    /// (typically the main tokio task) in an accept loop. Spawns a
    /// new task for each incoming connection.
    pub async fn start(&self, bind_addr: SocketAddr) -> Result<(), TransportError> {
        let listener = TcpListener::bind(bind_addr).await?;
        info!("Raft server listening on {}", bind_addr);

        loop {
            let (stream, addr) = match listener.accept().await {
                Ok(conn) => conn,
                Err(e) => {
                    error!("Accept error: {}", e);
                    continue;
                }
            };

            debug!("Accepted connection from {}", addr);
            let handler = self.handler.clone();

            tokio::spawn(async move {
                if let Err(e) = handle_connection(stream, handler).await {
                    error!("Connection error from {}: {}", addr, e);
                }
            });
        }
    }
}

/// Handles a single TCP connection: reads framed envelopes in a loop,
/// dispatches each to the handler, and writes back the response. The
/// loop exits when the peer closes the connection or an unrecoverable
/// error occurs.
async fn handle_connection(
    mut stream: TcpStream,
    handler: Arc<dyn MessageHandler>,
) -> Result<(), TransportError> {
    let mut buf = BytesMut::with_capacity(8192);

    loop {
        let envelope = codec::read_envelope(&mut stream, &mut buf).await?;

        let correlation_id = envelope.correlation_id.clone();
        let envelope_type = envelope.r#type.clone();

        // Dispatch to the runtime handler. The handler is responsible for
        // parsing the inner protobuf message, calling into RaftNode, and
        // returning serialized response bytes.
        let response_payload = match handler.handle(&correlation_id, &envelope_type, &envelope.payload) {
            Ok(payload) => payload,
            Err(e) => {
                error!("Handler error for type {}: {}", envelope_type, e);
                continue;
            }
        };

        // Wrap the response in an Envelope and send it back.
        let response = Envelope {
            correlation_id: format!("{}_resp", correlation_id),
            r#type: format!("{}Response", envelope_type),
            payload: response_payload,
        };

        codec::write_envelope(&mut stream, &response).await?;
    }
}
