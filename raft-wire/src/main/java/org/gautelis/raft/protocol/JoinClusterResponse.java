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
 * Reports the immediate outcome of a join request, including redirect or pending status.
 */
public class JoinClusterResponse {
    private final long term;
    private final String peerId;
    private final boolean success;
    private final String status;
    private final String message;
    private final String leaderId;

    /**
     * Creates a join-cluster response.
     *
     * @param term responder term
     * @param peerId responding peer identifier
     * @param success whether the request succeeded
     * @param status machine-readable status
     * @param message human-readable detail
     * @param leaderId known leader identifier for redirects
     */
    public JoinClusterResponse(long term, String peerId, boolean success, String status, String message, String leaderId) {
        this.term = term;
        this.peerId = peerId;
        this.success = success;
        this.status = status;
        this.message = message;
        this.leaderId = leaderId;
    }

    /**
     * Returns the responder term.
     *
     * @return responder term
     */
    public long getTerm() {
        return term;
    }

    /**
     * Returns the responding peer identifier.
     *
     * @return responding peer identifier
     */
    public String getPeerId() {
        return peerId;
    }

    /**
     * Indicates whether the request succeeded.
     *
     * @return {@code true} when the request succeeded
     */
    public boolean isSuccess() {
        return success;
    }

    /**
     * Returns the machine-readable status.
     *
     * @return machine-readable status
     */
    public String getStatus() {
        return status;
    }

    /**
     * Returns the human-readable detail.
     *
     * @return human-readable detail
     */
    public String getMessage() {
        return message;
    }

    /**
     * Returns the known leader identifier for redirects.
     *
     * @return known leader identifier for redirects
     */
    public String getLeaderId() {
        return leaderId;
    }
}
