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
package org.gautelis.raft.bootstrap;

import org.gautelis.raft.protocol.Peer;

/**
 * Reference-data admission policy: learners never redirect writes, voters may redirect to the leader.
 */
public final class ReferenceDataWriteAdmissionPolicy implements ClientWriteAdmissionPolicy {
    public static final ReferenceDataWriteAdmissionPolicy INSTANCE = new ReferenceDataWriteAdmissionPolicy();

    private ReferenceDataWriteAdmissionPolicy() {
    }

    @Override
    public ClientWriteAdmissionDecision evaluate(ClientWriteAdmissionContext context) {
        if (context == null || context.decommissioned()) {
            return ClientWriteAdmissionDecision.reject("REJECTED", "Node is not allowed to modify reference data");
        }
        if (context.leader()) {
            return ClientWriteAdmissionDecision.accept("ACCEPT", "Leader may evaluate the request locally");
        }
        if (context.localMemberRole() == Peer.Role.LEARNER) {
            return ClientWriteAdmissionDecision.reject("REJECTED", "Learner nodes never accept or redirect reference-data writes");
        }
        if (context.knownLeader() != null) {
            return ClientWriteAdmissionDecision.redirect("REDIRECT", "Node is not leader; send request to current leader");
        }
        return ClientWriteAdmissionDecision.reject("REJECTED", "Node is not leader or command could not be applied");
    }
}
