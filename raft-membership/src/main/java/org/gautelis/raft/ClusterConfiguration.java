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
 * Represents the Raft replication configuration.
 *
 * Stable configuration:
 * - only {@code currentMembers} is populated
 *
 * Joint consensus configuration:
 * - both {@code currentMembers} and {@code nextMembers} are populated
 * - quorums must satisfy majorities of the voter subsets in both sets
 *
 * <p>ClusterConfiguration is deliberately a small immutable value object. The
 * Raft node stores and snapshots it, while application code should treat it as
 * consensus metadata rather than domain state.</p>
 */
public final class ClusterConfiguration {
    private final LinkedHashMap<String, Peer> currentMembers;
    private final LinkedHashMap<String, Peer> nextMembers;

    private ClusterConfiguration(Map<String, Peer> currentMembers, Map<String, Peer> nextMembers) {
        this.currentMembers = new LinkedHashMap<>(currentMembers);
        this.nextMembers = new LinkedHashMap<>(nextMembers);
    }

    public static ClusterConfiguration stable(Collection<Peer> members) {
        // Normalize once at the boundary so quorum calculations are based on a
        // deterministic peer-id map, not caller list order or duplicate entries.
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

    public Collection<Peer> currentVotingMembers() {
        return currentMembers.values().stream()
                .filter(Peer::isVoter)
                .toList();
    }

    public Collection<Peer> nextVotingMembers() {
        if (!isJointConsensus()) {
            // In stable mode the next configuration is implicitly the current
            // one; callers can use nextVotingMembers() without branching.
            return currentVotingMembers();
        }
        return nextMembers.values().stream()
                .filter(Peer::isVoter)
                .toList();
    }

    public Collection<Peer> allVotingMembers() {
        // During joint consensus this is the union of current and next voters.
        // It is useful for replication/telemetry, but not itself a quorum rule.
        LinkedHashMap<String, Peer> all = new LinkedHashMap<>();
        for (Peer peer : currentVotingMembers()) {
            all.put(peer.getId(), peer);
        }
        for (Peer peer : nextVotingMembers()) {
            all.put(peer.getId(), peer);
        }
        return Collections.unmodifiableCollection(all.values());
    }

    public Collection<Peer> allMembers() {
        LinkedHashMap<String, Peer> all = new LinkedHashMap<>(currentMembers);
        all.putAll(nextMembers);
        return Collections.unmodifiableCollection(all.values());
    }

    public boolean contains(String peerId) {
        return currentMembers.containsKey(peerId) || nextMembers.containsKey(peerId);
    }

    public boolean isVoter(String peerId) {
        if (peerId == null) {
            return false;
        }
        Peer current = currentMembers.get(peerId);
        if (current != null && current.isVoter()) {
            return true;
        }
        Peer next = nextMembers.get(peerId);
        return next != null && next.isVoter();
    }

    public boolean isLearner(String peerId) {
        return contains(peerId) && !isVoter(peerId);
    }

    public ClusterConfiguration transitionTo(Collection<Peer> proposedMembers) {
        // Enter joint consensus by keeping the committed current set and adding
        // the proposed next set. Finalization is a separate committed command.
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
        // Once the finalize entry commits, the next set becomes the only stable
        // membership. Old-only voters no longer participate in quorums.
        return new ClusterConfiguration(nextMembers, Map.of());
    }

    public int currentMajoritySize() {
        return majoritySize((int) currentVotingMembers().stream().count());
    }

    public int nextMajoritySize() {
        if (!isJointConsensus()) {
            return currentMajoritySize();
        }
        return majoritySize((int) nextVotingMembers().stream().count());
    }

    public boolean hasCurrentMajority(Collection<String> peerIds) {
        return countPresent(peerIds, currentVotingMembers().stream().map(Peer::getId).collect(java.util.stream.Collectors.toSet())) >= currentMajoritySize();
    }

    public boolean hasNextMajority(Collection<String> peerIds) {
        if (!isJointConsensus()) {
            return hasCurrentMajority(peerIds);
        }
        return countPresent(peerIds, nextVotingMembers().stream().map(Peer::getId).collect(java.util.stream.Collectors.toSet())) >= nextMajoritySize();
    }

    public boolean hasJointMajority(Collection<String> peerIds) {
        if (!isJointConsensus()) {
            return hasCurrentMajority(peerIds);
        }
        // Joint consensus safety hinges on this conjunction: an entry or read
        // lease must be acknowledged by majorities of both configurations.
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
            if (existing != null && (!sameAddress(existing.getAddress(), peer.getAddress()) || existing.getRole() != peer.getRole())) {
                // Duplicate ids are allowed only when they describe the exact
                // same endpoint and role. Conflicting duplicates would make
                // quorum and transport behavior ambiguous.
                throw new IllegalArgumentException("Conflicting peer configuration for id " + peerId + ": " + existing + " vs " + peer);
            }
            normalized.putIfAbsent(peerId, peer);
        }
        return normalized;
    }

    private static boolean sameAddress(InetSocketAddress left, InetSocketAddress right) {
        return Objects.equals(left, right);
    }
}
