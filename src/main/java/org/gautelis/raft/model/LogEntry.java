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

import com.fasterxml.jackson.databind.JsonNode;

public class LogEntry {
    private long term;
    private String peerId;  // for heartbeat messages

    private JsonNode data; // byte[]??

    // Default constructor needed for Jackson
    protected LogEntry() {}

    public LogEntry(long term, String peerId) {
        this.term = term;
        this.peerId = peerId;
    }

    public long getTerm() { return term; }
    public String getPeerId() { return peerId; }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("LogEntry{");
        sb.append("term=").append(term);
        sb.append(", peerId=").append(peerId);
        sb.append('}');
        return sb.toString();
    }
}
