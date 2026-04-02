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
 * Reports whether a follower accepted a leader's AppendEntries request.
 */
public class AppendEntriesResponse {
    private long term;
    private String peerId;
    private boolean success;
    private long matchIndex;

    /**
     * Creates an append-entries response.
     *
     * @param term responder term
     * @param peerId responder peer identifier
     * @param success whether the append was accepted
     * @param matchIndex highest replicated or known matching index on the responder
     */
    public AppendEntriesResponse(long term, String peerId, boolean success, long matchIndex) {
        this.term = term;
        this.peerId = peerId;
        this.success = success;
        this.matchIndex = matchIndex;
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
     * @return responder peer id
     */
    public String getPeerId() { return peerId; }

    /**
     * Indicates whether the append succeeded.
     *
     * @return {@code true} when the request was accepted
     */
    public boolean isSuccess() { return success; }

    /**
     * Returns the follower's matching index.
     *
     * @return highest matching or replicated log index
     */
    public long getMatchIndex() { return matchIndex; }
}
