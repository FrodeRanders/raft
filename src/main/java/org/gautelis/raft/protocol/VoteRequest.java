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

public class VoteRequest {
    private long term;
    private String candidateId;

    private long lastLogIndex;
    private long lastLogTerm;

    public VoteRequest(long term, String candidateId) {
        this(term, candidateId, 0L, 0L);
    }

    public VoteRequest(long term, String candidateId, long lastLogIndex, long lastLogTerm) {
        this.term = term;
        this.candidateId = candidateId;
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm = lastLogTerm;
    }

    public long getTerm() { return term; }
    public String getCandidateId() { return candidateId; }

    public long getLastLogIndex() { return lastLogIndex; }
    public long getLastLogTerm() { return lastLogTerm; }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("VoteRequest{");
        sb.append("term=").append(term);
        sb.append(", candidate-id='").append(candidateId).append('\'');
        sb.append(", last-log-index=").append(lastLogIndex);
        sb.append(", last-log-term=").append(lastLogTerm);
        sb.append('}');
        return sb.toString();
    }
}
