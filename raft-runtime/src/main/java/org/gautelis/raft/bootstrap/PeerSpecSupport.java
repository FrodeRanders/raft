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

import org.gautelis.raft.RaftNode;
import org.gautelis.raft.protocol.Peer;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Locale;

final class PeerSpecSupport {
    private PeerSpecSupport() {
    }

    static String describe(Peer peer) {
        if (peer == null) {
            throw new IllegalArgumentException("Peer must not be null");
        }
        String roleSuffix = peer.getRole() == Peer.Role.VOTER ? "" : "/" + peer.getRole().name().toLowerCase(Locale.ROOT);
        if (peer.getAddress() == null) {
            return peer.getId() + roleSuffix;
        }
        return peer.getId() + "@" + peer.getAddress().getHostString() + ":" + peer.getAddress().getPort() + roleSuffix;
    }

    static Peer resolve(String spec, Peer me, List<Peer> peers, RaftNode stateMachine) {
        String trimmed = spec.trim();
        Peer.Role role = Peer.Role.VOTER;
        int slash = trimmed.lastIndexOf('/');
        if (slash > 0 && slash < trimmed.length() - 1) {
            role = Peer.Role.valueOf(trimmed.substring(slash + 1).trim().toUpperCase(Locale.ROOT));
            trimmed = trimmed.substring(0, slash).trim();
        }
        int at = trimmed.indexOf('@');
        if (at < 0) {
            if (me.getId().equals(trimmed)) {
                return new Peer(me.getId(), me.getAddress(), role == Peer.Role.VOTER ? me.getRole() : role);
            }
            for (Peer peer : peers) {
                if (peer.getId().equals(trimmed)) {
                    return new Peer(peer.getId(), peer.getAddress(), role == Peer.Role.VOTER ? peer.getRole() : role);
                }
            }
            if (stateMachine != null) {
                Peer peer = stateMachine.getPeerById(trimmed);
                if (peer != null) {
                    return new Peer(peer.getId(), peer.getAddress(), role == Peer.Role.VOTER ? peer.getRole() : role);
                }
            }
            throw new IllegalArgumentException("Unknown peer id '" + trimmed + "'; use id@host:port for new members");
        }

        String id = trimmed.substring(0, at).trim();
        String addressSpec = trimmed.substring(at + 1).trim();
        int colon = addressSpec.lastIndexOf(':');
        if (id.isBlank() || colon <= 0 || colon == addressSpec.length() - 1) {
            throw new IllegalArgumentException("Invalid peer specification '" + spec + "'");
        }
        String host = addressSpec.substring(0, colon).trim();
        int port = Integer.parseInt(addressSpec.substring(colon + 1).trim());
        return new Peer(id, new InetSocketAddress(host, port), role);
    }
}
