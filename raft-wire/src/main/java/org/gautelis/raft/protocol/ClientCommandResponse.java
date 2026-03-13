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

    public ClientCommandResponse(long term, String peerId, boolean success, String status, String message, String leaderId, String leaderHost, int leaderPort) {
        this.term = term;
        this.peerId = peerId;
        this.success = success;
        this.status = status;
        this.message = message;
        this.leaderId = leaderId;
        this.leaderHost = leaderHost == null ? "" : leaderHost;
        this.leaderPort = Math.max(0, leaderPort);
    }

    public long getTerm() {
        return term;
    }

    public String getPeerId() {
        return peerId;
    }

    public boolean isSuccess() {
        return success;
    }

    public String getStatus() {
        return status;
    }

    public String getMessage() {
        return message;
    }

    public String getLeaderId() {
        return leaderId;
    }

    public String getLeaderHost() {
        return leaderHost;
    }

    public int getLeaderPort() {
        return leaderPort;
    }
}
