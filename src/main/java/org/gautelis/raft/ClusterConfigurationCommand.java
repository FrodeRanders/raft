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
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Optional;

final class ClusterConfigurationCommand {
    // Internal log encoding for membership changes.
    // These entries are not user commands; RaftNode intercepts and applies them on commit.
    private static final String PREFIX = "__raft_config__:";
    private static final String JOINT = "joint:";
    private static final String FINALIZE = "finalize";

    enum Type {
        JOINT,
        FINALIZE
    }

    record Parsed(Type type, List<Peer> members) {
    }

    private ClusterConfigurationCommand() {
    }

    static String joint(List<Peer> members) {
        // Base64 keeps ids/hosts delimiter-safe without introducing another protobuf
        // type just for internal log entries.
        List<String> encoded = new ArrayList<>();
        for (Peer peer : members) {
            String host = "";
            int port = -1;
            if (peer.getAddress() != null) {
                host = peer.getAddress().getHostString();
                port = peer.getAddress().getPort();
            }
            encoded.add(encode(peer.getId()) + "," + encode(host) + "," + port);
        }
        return PREFIX + JOINT + String.join(";", encoded);
    }

    static String finalizeTransition() {
        return PREFIX + FINALIZE;
    }

    static Optional<Parsed> parse(String command) {
        if (command == null || !command.startsWith(PREFIX)) {
            return Optional.empty();
        }
        String body = command.substring(PREFIX.length());
        if (body.equals(FINALIZE)) {
            return Optional.of(new Parsed(Type.FINALIZE, List.of()));
        }
        if (!body.startsWith(JOINT)) {
            return Optional.empty();
        }

        String payload = body.substring(JOINT.length());
        if (payload.isBlank()) {
            return Optional.empty();
        }

        List<Peer> members = new ArrayList<>();
        for (String token : payload.split(";")) {
            // Each member is encoded as id,host,port where id/host are Base64url.
            String[] parts = token.split(",", 3);
            if (parts.length != 3) {
                return Optional.empty();
            }
            String id = decode(parts[0]);
            String host = decode(parts[1]);
            int port = Integer.parseInt(parts[2]);
            InetSocketAddress address = port < 0 || host.isBlank() ? null : new InetSocketAddress(host, port);
            members.add(new Peer(id, address));
        }
        return Optional.of(new Parsed(Type.JOINT, members));
    }

    private static String encode(String value) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    private static String decode(String value) {
        return new String(Base64.getUrlDecoder().decode(value), java.nio.charset.StandardCharsets.UTF_8);
    }
}
