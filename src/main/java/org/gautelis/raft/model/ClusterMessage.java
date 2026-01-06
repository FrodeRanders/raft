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
package org.gautelis.raft.model;

public class ClusterMessage {
    private long term;
    private String peerId;  // for heartbeat messages

    private String message;

    // Default constructor needed for Jackson
    protected ClusterMessage() {
    }

    public ClusterMessage(long term, String peerId, String message) {
        this.term = term;
        this.peerId = peerId;
        this.message = message;
    }

    public long getTerm() {
        return term;
    }

    public String getPeerId() {
        return peerId;
    }

    public String getMessage() {
        return message;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("ClusterMessage{");
        sb.append("term=").append(term);
        sb.append(", peerId=").append(peerId);
        sb.append(", message='").append(message).append('\'');
        sb.append('}');
        return sb.toString();
    }
}