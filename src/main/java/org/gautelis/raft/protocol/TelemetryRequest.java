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

public final class TelemetryRequest {
    private final long term;
    private final String peerId;
    private final boolean includePeerStats;
    private final boolean requireLeaderSummary;

    public TelemetryRequest(long term, String peerId, boolean includePeerStats, boolean requireLeaderSummary) {
        this.term = term;
        this.peerId = peerId;
        this.includePeerStats = includePeerStats;
        this.requireLeaderSummary = requireLeaderSummary;
    }

    public long getTerm() {
        return term;
    }

    public String getPeerId() {
        return peerId;
    }

    public boolean isIncludePeerStats() {
        return includePeerStats;
    }

    public boolean isRequireLeaderSummary() {
        return requireLeaderSummary;
    }
}
