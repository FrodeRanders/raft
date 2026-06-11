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

/// Defines the application state-machine contract used by a Raft node.
///
/// Implementations own only domain/application state. Raft owns log
/// replication, membership, terms, votes, commit tracking, and snapshot
/// wrapping. Cluster configuration must not be stored in the domain
/// state machine.
///
/// The trait is deliberately `Send + Sync` so it can be shared across
/// threads behind an `Arc` — the Raft runtime calls `apply` from the
/// commit path and `snapshot`/`restore` from log compaction.
pub trait StateMachine: Send + Sync {
    /// Called after Raft has committed a log entry. The term identifies the
    /// committed entry for audit/idempotency purposes; it is not an invitation
    /// for the application to influence consensus behavior.
    ///
    /// Implementations must be deterministic: applying the same command at
    /// the same index must produce the same domain state.
    fn apply(&self, term: u64, command: &[u8]);

    /// Applies a committed command and returns optional client-visible result
    /// bytes. Implementations that do not produce command results can use the
    /// default, which delegates to `apply` and returns an empty payload.
    fn apply_with_result(&self, term: u64, command: &[u8]) -> Vec<u8> {
        self.apply(term, command);
        Vec::new()
    }

    /// Returns encoded domain snapshot bytes. The returned bytes are later
    /// wrapped by Raft with cluster membership metadata before being stored
    /// or transferred. Return only application state, not Raft metadata.
    fn snapshot(&self) -> Vec<u8> {
        Vec::new()
    }

    /// Restores domain state from previously-produced snapshot bytes. Raft
    /// has already restored its own metadata (term, vote, log, membership)
    /// from the wrapper before calling this method.
    fn restore(&self, _snapshot_data: &[u8]) {}

    /// If this state machine also implements `QueryableStateMachine`, returns
    /// a reference to it for read queries. Returns `None` for write-only
    /// state machines. The default returns `None`; implementations of
    /// `QueryableStateMachine` should override this to return `Some(self)`.
    fn as_queryable(&self) -> Option<&dyn QueryableStateMachine> {
        None
    }
}

/// Extension trait for state machines that return client-visible results
/// from committed commands.
///
/// When a client submits a write command, the leader applies it only after
/// commitment. The `apply_with_result` variant lets the application return
/// an opaque byte array (e.g., CAS success/failure, generated key) that
/// is sent back to the client.
pub trait ResultStateMachine: StateMachine {
    /// Commits a command and returns an application result to the client.
    /// Returns an empty array when no result is needed.
    fn apply_with_result(&self, term: u64, command: &[u8]) -> Vec<u8>;
}

/// Extension trait for state machines that answer read queries against
/// locally-consistent state.
///
/// Queries are only dispatched after the runtime has established read
/// safety for the leader (via a read lease or contact-majority barrier).
/// Implementations should answer from local domain state and should
/// not contact Raft peers.
pub trait QueryableStateMachine: StateMachine {
    /// Executes a read query against local state. Raft handles leader
    /// checks, read leases, and read barriers before calling this.
    /// Returns an empty array when the query is unsupported.
    fn query(&self, request: &[u8]) -> Vec<u8>;
}
