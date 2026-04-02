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
 * Describes one member in the leader's cluster-health and reconfiguration view.
 *
 * @param peerId peer identifier
 * @param local whether this summary describes the local node
 * @param currentMember whether the peer is in the current committed configuration
 * @param nextMember whether the peer is in the proposed next configuration
 * @param voting whether the peer currently participates in quorum decisions
 * @param role effective role label presented to clients
 * @param currentRole role in the current configuration
 * @param nextRole role in the next configuration when transitioning
 * @param roleTransition textual description of any in-flight role change
 * @param transitionAgeMillis age of the current transition in milliseconds
 * @param blockingQuorums textual summary of quorums blocked by this member's state
 * @param blockingReason human-readable explanation for blocked progress
 * @param reachable whether the leader currently considers the peer reachable
 * @param freshness recency classification for successful communication
 * @param health health classification presented by telemetry
 * @param nextIndex leader-side next index for replication
 * @param matchIndex leader-side highest known replicated index
 * @param lag estimated replication lag
 * @param consecutiveFailures number of consecutive replication failures
 * @param lastSuccessfulContactMillis timestamp of last successful contact
 * @param lastFailedContactMillis timestamp of last failed contact
 */
public record ClusterMemberSummary(
        String peerId,
        boolean local,
        boolean currentMember,
        boolean nextMember,
        boolean voting,
        String role,
        String currentRole,
        String nextRole,
        String roleTransition,
        long transitionAgeMillis,
        String blockingQuorums,
        String blockingReason,
        boolean reachable,
        String freshness,
        String health,
        long nextIndex,
        long matchIndex,
        long lag,
        int consecutiveFailures,
        long lastSuccessfulContactMillis,
        long lastFailedContactMillis
) {}
