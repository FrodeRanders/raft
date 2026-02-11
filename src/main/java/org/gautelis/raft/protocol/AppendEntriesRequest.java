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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class AppendEntriesRequest {
    private long term;
    private String leaderId;
    private long prevLogIndex;
    private long prevLogTerm;
    private long leaderCommit;
    private List<LogEntry> entries;
    public AppendEntriesRequest(
            long term,
            String leaderId,
            long prevLogIndex,
            long prevLogTerm,
            long leaderCommit,
            List<LogEntry> entries
    ) {
        // Raft AppendEntries semantics:
        // - empty entries == heartbeat
        // - prevLogIndex/prevLogTerm anchor consistency check on follower
        // - leaderCommit advertises leader's committed index for follower apply progress
        this.term = term;
        this.leaderId = leaderId;
        this.prevLogIndex = prevLogIndex;
        this.prevLogTerm = prevLogTerm;
        this.leaderCommit = leaderCommit;
        this.entries = entries == null ? List.of() : new ArrayList<>(entries);
    }

    public long getTerm() { return term; }
    public String getLeaderId() { return leaderId; }
    public long getPrevLogIndex() { return prevLogIndex; }
    public long getPrevLogTerm() { return prevLogTerm; }
    public long getLeaderCommit() { return leaderCommit; }
    public List<LogEntry> getEntries() { return Collections.unmodifiableList(entries == null ? List.of() : entries); }
}
