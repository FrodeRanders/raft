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

import java.util.Arrays;

/**
 * Submits one typed client write command to the cluster leader.
 */
public final class ClientCommandRequest {
    private final long term;
    private final String peerId;
    private final byte[] command;
    private final String authScheme;
    private final String authToken;

    public ClientCommandRequest(long term, String peerId, byte[] command) {
        this(term, peerId, command, "", "");
    }

    public ClientCommandRequest(long term, String peerId, byte[] command, String authScheme, String authToken) {
        this.term = term;
        this.peerId = peerId;
        this.command = command == null ? new byte[0] : Arrays.copyOf(command, command.length);
        this.authScheme = authScheme == null ? "" : authScheme;
        this.authToken = authToken == null ? "" : authToken;
    }

    public long getTerm() {
        return term;
    }

    public String getPeerId() {
        return peerId;
    }

    public byte[] getCommand() {
        return Arrays.copyOf(command, command.length);
    }

    public String getAuthScheme() {
        return authScheme;
    }

    public String getAuthToken() {
        return authToken;
    }
}
