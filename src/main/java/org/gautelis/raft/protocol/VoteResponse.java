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

public class VoteResponse {
    private String peerId;
    private long term;
    private boolean voteGranted;
    private long currentTerm;
    public VoteResponse(VoteRequest request, String peerId, boolean voteGranted, long currentTerm) {
        // term: the election term from the corresponding VoteRequest.
        // currentTerm: responder's local term at response time (used to detect newer terms).
        this.term = request.getTerm();
        this.peerId = peerId;
        this.voteGranted = voteGranted;
        this.currentTerm = currentTerm;
    }

    public String getPeerId() { return peerId; }
    public long getTerm() { return term; }
    public boolean isVoteGranted() { return voteGranted; }
    public long getCurrentTerm() { return currentTerm; }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("VoteResponse{");
        sb.append("peer-id=").append(peerId);
        sb.append(", term=").append(term);
        sb.append(", voteGranted=").append(voteGranted);
        sb.append(", currentTerm=").append(currentTerm);
        sb.append('}');
        return sb.toString();
    }
}
