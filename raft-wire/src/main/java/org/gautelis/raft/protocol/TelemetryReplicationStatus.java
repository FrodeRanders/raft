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

/**
 * Summarizes the leader's replication state and recent contact history for one follower.
 *
 * @param peerId follower identifier
 * @param nextIndex next log index the leader plans to send
 * @param matchIndex highest index known replicated on the follower
 * @param reachable whether the follower is currently considered reachable
 * @param lastSuccessfulContactMillis timestamp of last successful contact
 * @param consecutiveFailures number of consecutive replication failures
 * @param lastFailedContactMillis timestamp of last failed contact
 */
public record TelemetryReplicationStatus(
        String peerId,
        long nextIndex,
        long matchIndex,
        boolean reachable,
        long lastSuccessfulContactMillis,
        int consecutiveFailures,
        long lastFailedContactMillis
) {}
