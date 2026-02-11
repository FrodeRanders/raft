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

public class LogEntry {
    private long term;
    private String peerId;
    private byte[] data;
    public LogEntry(long term, String peerId) {
        this(term, peerId, new byte[0]);
    }

    public LogEntry(long term, String peerId, byte[] data) {
        this.term = term;
        this.peerId = peerId;
        this.data = data == null ? new byte[0] : Arrays.copyOf(data, data.length);
    }

    public long getTerm() { return term; }
    public String getPeerId() { return peerId; }
    public byte[] getData() { return Arrays.copyOf(data, data.length); }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("LogEntry{");
        sb.append("term=").append(term);
        sb.append(", peerId=").append(peerId);
        sb.append(", dataBytes=").append(data == null ? 0 : data.length);
        sb.append('}');
        return sb.toString();
    }
}
