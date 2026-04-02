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
 * Reports the immediate outcome of an explicit cluster membership change request.
 */
public class ReconfigureClusterResponse {
    private final long term;
    private final String peerId;
    private final boolean success;
    private final String status;
    private final String message;
    private final String leaderId;

    /**
     * Creates a reconfigure-cluster response.
     *
     * @param term responder term
     * @param peerId responding peer identifier
     * @param success whether the request succeeded
     * @param status machine-readable status
     * @param message human-readable detail
     * @param leaderId known leader identifier for redirects
     */
    public ReconfigureClusterResponse(long term, String peerId, boolean success, String status, String message, String leaderId) {
        this.term = term;
        this.peerId = peerId;
        this.success = success;
        this.status = status;
        this.message = message;
        this.leaderId = leaderId;
    }

    /** @return responder term */
    public long getTerm() {
        return term;
    }

    /** @return responding peer identifier */
    public String getPeerId() {
        return peerId;
    }

    /** @return {@code true} when the request succeeded */
    public boolean isSuccess() {
        return success;
    }

    /** @return machine-readable status */
    public String getStatus() {
        return status;
    }

    /** @return human-readable detail */
    public String getMessage() {
        return message;
    }

    /** @return known leader identifier for redirects */
    public String getLeaderId() {
        return leaderId;
    }
}
