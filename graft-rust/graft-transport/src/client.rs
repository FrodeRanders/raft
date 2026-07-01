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
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use graft_proto::Envelope;
use parking_lot::Mutex;
use tokio::net::TcpStream;
use tokio::sync::{oneshot, Mutex as TokioMutex};
use tokio::time::timeout;

use crate::codec;
use crate::error::TransportError;

/// Maximum time to wait for a response to an outbound RPC. If the peer
/// does not respond within this window, the request future is failed with
/// `TransportError::Timeout`.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

/// A pending outbound request waiting for a response. The `tx` oneshot
/// sender is completed when the response envelope arrives.
struct PendingRequest {
    tx: oneshot::Sender<Result<Vec<u8>, TransportError>>,
}

// ---------------------------------------------------------------------------
// RaftClient — outbound RPC sender
// ---------------------------------------------------------------------------

/// Async outbound RPC client that manages per-peer TCP connections and
/// matches response envelopes to pending request futures via correlation
/// IDs. Connections are lazily established and pooled for reuse.
///
/// The client uses a `DashMap<correlationId, PendingRequest>` to track
/// in-flight requests, mirroring the Java `RaftClient`'s
/// `ConcurrentHashMap<correlationId, CompletableFuture>`.
pub struct RaftClient {
    /// Per-peer TCP connections, keyed by peer id. Pooled for reuse
    /// across multiple requests.
    connections: DashMap<String, Arc<TokioMutex<TcpStream>>>,
    /// In-flight requests awaiting a response.
    pending: DashMap<String, PendingRequest>,
    /// Monotonically-increasing counter for generating unique correlation
    /// IDs.
    counter: Mutex<u64>,
    /// Known peer addresses, updated when the cluster configuration changes.
    peer_addrs: Mutex<HashMap<String, SocketAddr>>,
}

impl RaftClient {
    pub fn new() -> Self {
        Self {
            connections: DashMap::new(),
            pending: DashMap::new(),
            counter: Mutex::new(0),
            peer_addrs: Mutex::new(HashMap::new()),
        }
    }

    /// Updates the peer address map. Called by the runtime when the
    /// cluster configuration changes (JOIN, JOINT, FINALIZE).
    pub fn set_known_peers(&self, peers: HashMap<String, SocketAddr>) {
        let mut addrs = self.peer_addrs.lock();
        *addrs = peers;
    }

    /// Returns a new unique correlation ID for a pending request.
    fn next_correlation_id(&self) -> String {
        let mut counter = self.counter.lock();
        *counter += 1;
        format!("{}", *counter)
    }

    /// Returns a pooled TCP connection to the given peer, establishing one
    /// if no cached connection exists.
    async fn get_connection(
        &self,
        peer_id: &str,
    ) -> Result<Arc<TokioMutex<TcpStream>>, TransportError> {
        if let Some(conn) = self.connections.get(peer_id) {
            return Ok(conn.clone());
        }

        let addr = {
            let addrs = self.peer_addrs.lock();
            addrs
                .get(peer_id)
                .copied()
                .ok_or_else(|| TransportError::SendError(format!("unknown peer: {}", peer_id)))?
        };

        let stream = TcpStream::connect(addr).await?;
        let conn = Arc::new(TokioMutex::new(stream));
        self.connections.insert(peer_id.to_string(), conn.clone());
        Ok(conn)
    }

    /// Sends a request envelope to a peer and waits for the response.
    /// The request is framed with a correlation ID that the peer echoes
    /// back in its response envelope. Returns the raw response payload
    /// bytes.
    ///
    /// Internally, this:
    /// 1. Registers a `PendingRequest` in the pending map.
    /// 2. Writes the framed envelope to the peer's TCP connection.
    /// 3. Waits for the response oneshot to fire (up to `REQUEST_TIMEOUT`).
    ///
    /// On timeout or send failure, the pending entry is cleaned up.
    pub async fn send_request(
        &self,
        peer_id: &str,
        envelope_type: &str,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>, TransportError> {
        let correlation_id = self.next_correlation_id();
        let (tx, rx) = oneshot::channel();

        self.pending
            .insert(correlation_id.clone(), PendingRequest { tx });

        let envelope = Envelope {
            correlation_id: correlation_id.clone(),
            r#type: envelope_type.to_string(),
            payload,
        };

        let conn = self.get_connection(peer_id).await?;

        {
            let mut stream = conn.lock().await;
            if let Err(e) = codec::write_envelope(&mut *stream, &envelope).await {
                self.pending.remove(&correlation_id);
                self.connections.remove(peer_id);
                return Err(e);
            }
        }

        // Wait for the response, bounded by REQUEST_TIMEOUT.
        let response = timeout(REQUEST_TIMEOUT, rx).await.map_err(|_| {
            self.pending.remove(&correlation_id);
            TransportError::Timeout
        })??;

        response
    }

    /// Completes a pending request with the response payload. Called by
    /// the response-receiving path (e.g., a background reader task) when
    /// a response envelope arrives with a matching correlation ID.
    pub async fn handle_response(&self, correlation_id: &str, payload: Vec<u8>) {
        if let Some((_, pending)) = self.pending.remove(correlation_id) {
            let _ = pending.tx.send(Ok(payload));
        }
    }

    /// Sends a protobuf envelope and synchronously waits for the response
    /// on the same TCP connection. Opens a fresh connection per call (no
    /// pooling) — matching the C++ `RaftClient` connection-per-call pattern
    /// used in the active runtime.
    ///
    /// This is the method the `RaftRuntime` uses for election votes,
    /// heartbeats, and snapshot chunks. It avoids the background-reader
    /// complexity of the pooled `send_request` path.
    pub async fn send_rpc(
        &self,
        addr: SocketAddr,
        envelope_type: &str,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>, TransportError> {
        let mut stream = TcpStream::connect(addr).await?;

        let envelope = Envelope {
            correlation_id: self.next_correlation_id(),
            r#type: envelope_type.to_string(),
            payload,
        };
        codec::write_envelope(&mut stream, &envelope).await?;

        let mut buf = bytes::BytesMut::with_capacity(8192);
        let resp = timeout(REQUEST_TIMEOUT, codec::read_envelope(&mut stream, &mut buf))
            .await
            .map_err(|_| TransportError::Timeout)??;
        Ok(resp.payload)
    }

    /// Look up a peer's address from the address book. Returns an error
    /// if the peer is unknown.
    pub fn peer_addr(&self, peer_id: &str) -> Result<SocketAddr, TransportError> {
        let addrs = self.peer_addrs.lock();
        addrs
            .get(peer_id)
            .copied()
            .ok_or_else(|| TransportError::SendError(format!("unknown peer: {}", peer_id)))
    }
}
