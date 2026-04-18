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
 * Reports the outcome of a typed client write, including redirect information when needed.
 */
public final class ClientCommandResponse {
    private final long term;
    private final String peerId;
    private final boolean success;
    private final String status;
    private final String message;
    private final String leaderId;
    private final String leaderHost;
    private final int leaderPort;
    private final byte[] result;

    /**
     * Creates a client command response.
     *
     * @param term responder term
     * @param peerId responding peer identifier
     * @param success whether the command succeeded
     * @param status machine-readable status
     * @param message human-readable detail
     * @param leaderId known leader identifier for redirects
     * @param leaderHost known leader host for redirects
     * @param leaderPort known leader port for redirects
     */
    public ClientCommandResponse(long term, String peerId, boolean success, String status, String message, String leaderId, String leaderHost, int leaderPort) {
        this(term, peerId, success, status, message, leaderId, leaderHost, leaderPort, new byte[0]);
    }

    /**
     * Creates a client command response with an optional typed command result payload.
     *
     * @param term responder term
     * @param peerId responding peer identifier
     * @param success whether the command succeeded
     * @param status machine-readable status
     * @param message human-readable detail
     * @param leaderId known leader identifier for redirects
     * @param leaderHost known leader host for redirects
     * @param leaderPort known leader port for redirects
     * @param result encoded typed command result, or empty when absent
     */
    public ClientCommandResponse(long term, String peerId, boolean success, String status, String message, String leaderId, String leaderHost, int leaderPort, byte[] result) {
        this.term = term;
        this.peerId = peerId;
        this.success = success;
        this.status = status;
        this.message = message;
        this.leaderId = leaderId;
        this.leaderHost = leaderHost == null ? "" : leaderHost;
        this.leaderPort = Math.max(0, leaderPort);
        this.result = result == null ? new byte[0] : result.clone();
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
     * Indicates whether the command succeeded.
     *
     * @return {@code true} when the command succeeded
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
     * Returns the human-readable status detail.
     *
     * @return human-readable detail
     */
    public String getMessage() {
        return message;
    }

    /**
     * Returns the known leader identifier for redirects.
     *
     * @return redirect leader identifier
     */
    public String getLeaderId() {
        return leaderId;
    }

    /**
     * Returns the known leader host for redirects.
     *
     * @return redirect leader host
     */
    public String getLeaderHost() {
        return leaderHost;
    }

    /**
     * Returns the known leader port for redirects.
     *
     * @return redirect leader port
     */
    public int getLeaderPort() {
        return leaderPort;
    }

    /**
     * Returns the encoded typed command result payload, or an empty array when absent.
     *
     * @return encoded command result payload
     */
    public byte[] getResult() {
        return result.clone();
    }
}
