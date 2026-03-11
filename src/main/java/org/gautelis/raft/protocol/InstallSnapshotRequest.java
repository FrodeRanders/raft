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

public class InstallSnapshotRequest {
    private long term;
    private String leaderId;
    private long lastIncludedIndex;
    private long lastIncludedTerm;
    private long offset;
    private byte[] snapshotData;
    private boolean done;

    public InstallSnapshotRequest(long term, String leaderId, long lastIncludedIndex, long lastIncludedTerm, byte[] snapshotData) {
        this(term, leaderId, lastIncludedIndex, lastIncludedTerm, 0L, snapshotData, true);
    }

    public InstallSnapshotRequest(long term, String leaderId, long lastIncludedIndex, long lastIncludedTerm, long offset, byte[] snapshotData, boolean done) {
        this.term = term;
        this.leaderId = leaderId;
        this.lastIncludedIndex = lastIncludedIndex;
        this.lastIncludedTerm = lastIncludedTerm;
        this.offset = offset;
        this.snapshotData = snapshotData == null ? new byte[0] : Arrays.copyOf(snapshotData, snapshotData.length);
        this.done = done;
    }

    public long getTerm() { return term; }
    public String getLeaderId() { return leaderId; }
    public long getLastIncludedIndex() { return lastIncludedIndex; }
    public long getLastIncludedTerm() { return lastIncludedTerm; }
    public long getOffset() { return offset; }
    public byte[] getSnapshotData() { return Arrays.copyOf(snapshotData, snapshotData.length); }
    public boolean isDone() { return done; }
}
