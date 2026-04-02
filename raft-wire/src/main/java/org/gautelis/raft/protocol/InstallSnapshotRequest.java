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
 * Streams a snapshot chunk from the leader to a follower that is too far behind for log replay.
 */
public class InstallSnapshotRequest {
    private long term;
    private String leaderId;
    private long lastIncludedIndex;
    private long lastIncludedTerm;
    private long offset;
    private byte[] snapshotData;
    private boolean done;

    /**
     * Creates a complete snapshot request carried in a single message.
     *
     * @param term leader term
     * @param leaderId leader peer identifier
     * @param lastIncludedIndex snapshot index
     * @param lastIncludedTerm snapshot term
     * @param snapshotData encoded snapshot bytes
     */
    public InstallSnapshotRequest(long term, String leaderId, long lastIncludedIndex, long lastIncludedTerm, byte[] snapshotData) {
        this(term, leaderId, lastIncludedIndex, lastIncludedTerm, 0L, snapshotData, true);
    }

    /**
     * Creates a snapshot chunk request.
     *
     * @param term leader term
     * @param leaderId leader peer identifier
     * @param lastIncludedIndex snapshot index
     * @param lastIncludedTerm snapshot term
     * @param offset byte offset within the snapshot stream
     * @param snapshotData snapshot chunk bytes
     * @param done whether this is the final chunk
     */
    public InstallSnapshotRequest(long term, String leaderId, long lastIncludedIndex, long lastIncludedTerm, long offset, byte[] snapshotData, boolean done) {
        this.term = term;
        this.leaderId = leaderId;
        this.lastIncludedIndex = lastIncludedIndex;
        this.lastIncludedTerm = lastIncludedTerm;
        this.offset = offset;
        this.snapshotData = snapshotData == null ? new byte[0] : Arrays.copyOf(snapshotData, snapshotData.length);
        this.done = done;
    }

    /**
     * Returns the leader term.
     *
     * @return leader term
     */
    public long getTerm() { return term; }
    /**
     * Returns the leader peer identifier.
     *
     * @return leader peer identifier
     */
    public String getLeaderId() { return leaderId; }
    /**
     * Returns the snapshot index.
     *
     * @return snapshot index
     */
    public long getLastIncludedIndex() { return lastIncludedIndex; }
    /**
     * Returns the snapshot term.
     *
     * @return snapshot term
     */
    public long getLastIncludedTerm() { return lastIncludedTerm; }
    /**
     * Returns the byte offset within the snapshot stream.
     *
     * @return byte offset within the snapshot stream
     */
    public long getOffset() { return offset; }
    /**
     * Returns the snapshot bytes.
     *
     * @return defensive copy of the snapshot bytes
     */
    public byte[] getSnapshotData() { return Arrays.copyOf(snapshotData, snapshotData.length); }
    /**
     * Indicates whether this is the final chunk.
     *
     * @return {@code true} when this is the final chunk
     */
    public boolean isDone() { return done; }
}
