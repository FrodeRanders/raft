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
package org.gautelis.raft.protocol;

/**
 * Reports follower progress while receiving snapshot chunks from the leader.
 */
public class InstallSnapshotResponse {
    private long term;
    private String peerId;
    private boolean success;
    private long lastIncludedIndex;

    /**
     * Creates an install-snapshot response.
     *
     * @param term responder term
     * @param peerId responder peer identifier
     * @param success whether the chunk was accepted
     * @param lastIncludedIndex last snapshot index acknowledged by the follower
     */
    public InstallSnapshotResponse(long term, String peerId, boolean success, long lastIncludedIndex) {
        this.term = term;
        this.peerId = peerId;
        this.success = success;
        this.lastIncludedIndex = lastIncludedIndex;
    }

    /**
     * Returns the responder term.
     *
     * @return responder term
     */
    public long getTerm() { return term; }
    /**
     * Returns the responder peer identifier.
     *
     * @return responder peer identifier
     */
    public String getPeerId() { return peerId; }
    /**
     * Indicates whether the chunk was accepted.
     *
     * @return {@code true} when the chunk was accepted
     */
    public boolean isSuccess() { return success; }
    /**
     * Returns the last included index acknowledged by the follower.
     *
     * @return last included index acknowledged by the follower
     */
    public long getLastIncludedIndex() { return lastIncludedIndex; }
}
