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

import java.util.List;

/**
 * Submits an explicit cluster membership change such as joint, finalize, promote, or demote.
 */
public class ReconfigureClusterRequest {
    public enum Action {
        JOINT,
        FINALIZE,
        PROMOTE,
        DEMOTE
    }

    private final long term;
    private final String peerId;
    private final Action action;
    private final List<Peer> members;
    private final String authScheme;
    private final String authToken;

    public ReconfigureClusterRequest(long term, String peerId, Action action, List<Peer> members) {
        this(term, peerId, action, members, "", "");
    }

    public ReconfigureClusterRequest(long term, String peerId, Action action, List<Peer> members, String authScheme, String authToken) {
        this.term = term;
        this.peerId = peerId;
        this.action = action;
        this.members = members == null ? List.of() : List.copyOf(members);
        this.authScheme = authScheme == null ? "" : authScheme;
        this.authToken = authToken == null ? "" : authToken;
    }

    public long getTerm() {
        return term;
    }

    public String getPeerId() {
        return peerId;
    }

    public Action getAction() {
        return action;
    }

    public List<Peer> getMembers() {
        return members;
    }

    public String getAuthScheme() {
        return authScheme;
    }

    public String getAuthToken() {
        return authToken;
    }
}
