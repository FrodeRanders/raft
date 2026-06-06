/*
 * Copyright (C) 2026 Frode Randers
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

import java.net.InetSocketAddress;
import java.util.Locale;

/**
 * One discovered Raft seed endpoint.
 *
 * <p>The endpoint carries both logical identity and contact address. DNS SRV
 * records provide target host and port; the peer id is derived from the target
 * name unless a caller supplies a stronger identity through another mechanism.</p>
 */
public record SeedEndpoint(String peerId, String host, int port, Peer.Role role) {
    public SeedEndpoint {
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("seed endpoint host must not be blank");
        }
        if (port <= 0 || port > 65_535) {
            throw new IllegalArgumentException("seed endpoint port out of range: " + port);
        }
        if (peerId == null || peerId.isBlank()) {
            peerId = derivePeerId(host);
        }
        role = role == null ? Peer.Role.VOTER : role;
    }

    public Peer toPeer() {
        return new Peer(peerId, new InetSocketAddress(host, port), role);
    }

    public static String derivePeerId(String host) {
        String normalized = host == null ? "" : host.trim();
        while (normalized.endsWith(".")) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        int dot = normalized.indexOf('.');
        String firstLabel = dot >= 0 ? normalized.substring(0, dot) : normalized;
        return firstLabel.toLowerCase(Locale.ROOT);
    }
}
