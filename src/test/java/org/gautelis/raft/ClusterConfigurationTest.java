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
package org.gautelis.raft;

import org.gautelis.raft.protocol.Peer;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClusterConfigurationTest {

    private static Peer peer(String id) {
        return new Peer(id, null);
    }

    private static Peer learner(String id) {
        return new Peer(id, null, Peer.Role.LEARNER);
    }

    @Test
    void stableConfigurationUsesSingleMajority() {
        ClusterConfiguration configuration = ClusterConfiguration.stable(List.of(peer("A"), peer("B"), peer("C")));

        assertTrue(configuration.hasJointMajority(List.of("A", "B")));
        assertFalse(configuration.hasJointMajority(List.of("A")));
    }

    @Test
    void jointConsensusRequiresBothOldAndNewMajorities() {
        ClusterConfiguration configuration = ClusterConfiguration
                .stable(List.of(peer("A"), peer("B"), peer("C")))
                .transitionTo(List.of(peer("B"), peer("C"), peer("D")));

        assertFalse(configuration.hasJointMajority(List.of("A", "B")));
        assertFalse(configuration.hasJointMajority(List.of("B", "D")));
        assertTrue(configuration.hasJointMajority(List.of("A", "B", "D")));
        assertTrue(configuration.hasJointMajority(List.of("B", "C", "D")));
    }

    @Test
    void finalizedConfigurationUsesNewMembershipOnly() {
        ClusterConfiguration configuration = ClusterConfiguration
                .stable(List.of(peer("A"), peer("B"), peer("C")))
                .transitionTo(List.of(peer("B"), peer("C"), peer("D")))
                .finalizeTransition();

        assertFalse(configuration.hasJointMajority(List.of("A", "B")));
        assertTrue(configuration.hasJointMajority(List.of("B", "D")));
    }

    @Test
    void learnerMembersDoNotAffectVotingMajorities() {
        ClusterConfiguration configuration = ClusterConfiguration.stable(List.of(peer("A"), peer("B"), learner("L")));

        assertTrue(configuration.contains("L"));
        assertTrue(configuration.isLearner("L"));
        assertFalse(configuration.isVoter("L"));
        assertTrue(configuration.hasJointMajority(List.of("A", "B")));
        assertFalse(configuration.hasJointMajority(List.of("A")));
    }
}
