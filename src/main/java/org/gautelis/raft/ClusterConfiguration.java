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

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Represents the Raft voting configuration.
 *
 * Stable configuration:
 * - only {@code currentMembers} is populated
 *
 * Joint consensus configuration:
 * - both {@code currentMembers} and {@code nextMembers} are populated
 * - quorums must satisfy majorities of both sets
 */
public final class ClusterConfiguration {
    private final LinkedHashMap<String, Peer> currentMembers;
    private final LinkedHashMap<String, Peer> nextMembers;

    private ClusterConfiguration(Map<String, Peer> currentMembers, Map<String, Peer> nextMembers) {
        this.currentMembers = new LinkedHashMap<>(currentMembers);
        this.nextMembers = new LinkedHashMap<>(nextMembers);
    }

    public static ClusterConfiguration stable(Collection<Peer> members) {
        LinkedHashMap<String, Peer> normalized = normalize(members);
        if (normalized.isEmpty()) {
            throw new IllegalArgumentException("Cluster configuration must contain at least one member");
        }
        return new ClusterConfiguration(normalized, Map.of());
    }

    public boolean isJointConsensus() {
        return !nextMembers.isEmpty();
    }

    public Collection<Peer> currentMembers() {
        return Collections.unmodifiableCollection(currentMembers.values());
    }

    public Collection<Peer> nextMembers() {
        return Collections.unmodifiableCollection(nextMembers.values());
    }

    public Collection<Peer> allMembers() {
        LinkedHashMap<String, Peer> all = new LinkedHashMap<>(currentMembers);
        all.putAll(nextMembers);
        return Collections.unmodifiableCollection(all.values());
    }

    public boolean contains(String peerId) {
        return currentMembers.containsKey(peerId) || nextMembers.containsKey(peerId);
    }

    public ClusterConfiguration transitionTo(Collection<Peer> proposedMembers) {
        LinkedHashMap<String, Peer> normalized = normalize(proposedMembers);
        if (normalized.isEmpty()) {
            throw new IllegalArgumentException("Next cluster configuration must contain at least one member");
        }
        if (currentMembers.equals(normalized)) {
            return this;
        }
        return new ClusterConfiguration(currentMembers, normalized);
    }

    public ClusterConfiguration finalizeTransition() {
        if (!isJointConsensus()) {
            return this;
        }
        return new ClusterConfiguration(nextMembers, Map.of());
    }

    public int currentMajoritySize() {
        return majoritySize(currentMembers.size());
    }

    public int nextMajoritySize() {
        if (!isJointConsensus()) {
            return currentMajoritySize();
        }
        return majoritySize(nextMembers.size());
    }

    public boolean hasCurrentMajority(Collection<String> peerIds) {
        return countPresent(peerIds, currentMembers.keySet()) >= currentMajoritySize();
    }

    public boolean hasNextMajority(Collection<String> peerIds) {
        if (!isJointConsensus()) {
            return hasCurrentMajority(peerIds);
        }
        return countPresent(peerIds, nextMembers.keySet()) >= nextMajoritySize();
    }

    public boolean hasJointMajority(Collection<String> peerIds) {
        if (!isJointConsensus()) {
            return hasCurrentMajority(peerIds);
        }
        return hasCurrentMajority(peerIds) && hasNextMajority(peerIds);
    }

    public boolean sameMembershipAs(ClusterConfiguration other) {
        if (other == null) {
            return false;
        }
        return currentMembers.equals(other.currentMembers) && nextMembers.equals(other.nextMembers);
    }

    private static int majoritySize(int members) {
        return (members / 2) + 1;
    }

    private static int countPresent(Collection<String> peerIds, Set<String> members) {
        Set<String> unique = new LinkedHashSet<>(peerIds);
        int matches = 0;
        for (String peerId : unique) {
            if (members.contains(peerId)) {
                matches++;
            }
        }
        return matches;
    }

    private static LinkedHashMap<String, Peer> normalize(Collection<Peer> members) {
        if (members == null) {
            throw new IllegalArgumentException("Cluster members cannot be null");
        }

        LinkedHashMap<String, Peer> normalized = new LinkedHashMap<>();
        for (Peer peer : members) {
            if (peer == null) {
                continue;
            }
            String peerId = peer.getId();
            if (peerId == null || peerId.isBlank()) {
                throw new IllegalArgumentException("Cluster peer must have non-null, non-blank id");
            }

            Peer existing = normalized.get(peerId);
            if (existing != null && !sameAddress(existing.getAddress(), peer.getAddress())) {
                throw new IllegalArgumentException("Conflicting peer configuration for id " + peerId + ": " + existing.getAddress() + " vs " + peer.getAddress());
            }
            normalized.putIfAbsent(peerId, peer);
        }
        return normalized;
    }

    private static boolean sameAddress(InetSocketAddress left, InetSocketAddress right) {
        return Objects.equals(left, right);
    }
}
