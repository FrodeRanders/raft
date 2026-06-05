/*
 * Copyright (C) 2025-2026 Frode Randers
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
package org.gautelis.raft.statemachine;

/**
 * Defines the application state-machine contract used by a Raft node.
 *
 * <p>Implementations own only domain/application state. Raft owns log replication,
 * membership, terms, votes, commit tracking, and snapshot wrapping. The bytes passed to
 * and returned from this interface are application-owned payloads.</p>
 */
public interface SnapshotStateMachine {
    /**
     * Applies one committed application command.
     *
     * <p>Raft calls this method only after the command has been committed. Implementations
     * should be deterministic: every replica that applies the same command at the same log
     * position must reach the same state.</p>
     *
     * @param term Raft term of the committed log entry
     * @param command application command bytes
     */
    void apply(long term, byte[] command);

    /**
     * Creates an application snapshot.
     *
     * <p>The returned bytes must contain only domain state. Raft wraps these bytes with
     * its own metadata, including cluster configuration at the snapshot boundary.</p>
     *
     * @return encoded application snapshot bytes
     */
    byte[] snapshot();

    /**
     * Restores application state from a previously produced application snapshot.
     *
     * <p>The input is already unwrapped by Raft. It does not include Raft membership or
     * log metadata.</p>
     *
     * @param snapshotData encoded application snapshot bytes
     */
    void restore(byte[] snapshotData);
}
