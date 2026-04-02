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
 * Queries the lifecycle state of an earlier join request.
 */
public class JoinClusterStatusRequest {
    private final long term;
    private final String peerId;
    private final String targetPeerId;
    private final String authScheme;
    private final String authToken;

    /**
     * Creates an unauthenticated join-status request.
     *
     * @param term sender term
     * @param peerId requesting peer identifier
     * @param targetPeerId joining peer identifier being queried
     */
    public JoinClusterStatusRequest(long term, String peerId, String targetPeerId) {
        this(term, peerId, targetPeerId, "", "");
    }

    /**
     * Creates a join-status request.
     *
     * @param term sender term
     * @param peerId requesting peer identifier
     * @param targetPeerId joining peer identifier being queried
     * @param authScheme authentication scheme name
     * @param authToken authentication token or credential
     */
    public JoinClusterStatusRequest(long term, String peerId, String targetPeerId, String authScheme, String authToken) {
        this.term = term;
        this.peerId = peerId;
        this.targetPeerId = targetPeerId;
        this.authScheme = authScheme == null ? "" : authScheme;
        this.authToken = authToken == null ? "" : authToken;
    }

    /**
     * Returns the sender term.
     *
     * @return sender term
     */
    public long getTerm() {
        return term;
    }

    /**
     * Returns the requesting peer identifier.
     *
     * @return requesting peer identifier
     */
    public String getPeerId() {
        return peerId;
    }

    /**
     * Returns the joining peer identifier being queried.
     *
     * @return joining peer identifier being queried
     */
    public String getTargetPeerId() {
        return targetPeerId;
    }

    /**
     * Returns the authentication scheme.
     *
     * @return authentication scheme, or an empty string when absent
     */
    public String getAuthScheme() {
        return authScheme;
    }

    /**
     * Returns the authentication token.
     *
     * @return authentication token, or an empty string when absent
     */
    public String getAuthToken() {
        return authToken;
    }
}
