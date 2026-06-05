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
 * Extends a snapshot state machine with typed command-result support.
 */
public interface ResultSnapshotStateMachine extends SnapshotStateMachine {
    /**
     * Applies one committed application command and returns an application result.
     *
     * <p>The result is returned to the client after Raft commit. Implementations that
     * need idempotent retries should include stable command identifiers in their
     * command payloads and return the same result for duplicate identifiers.</p>
     *
     * @param term Raft term of the committed log entry
     * @param command application command bytes
     * @return encoded application command result, or an empty array when no result is needed
     */
    byte[] applyWithResult(long term, byte[] command);

    /**
     * Applies one committed command while discarding the returned result.
     *
     * @param term Raft term of the committed log entry
     * @param command application command bytes
     */
    @Override
    default void apply(long term, byte[] command) {
        applyWithResult(term, command);
    }
}
