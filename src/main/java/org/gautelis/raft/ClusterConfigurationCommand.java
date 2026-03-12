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

import com.google.protobuf.InvalidProtocolBufferException;
import org.gautelis.raft.protocol.Peer;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

final class ClusterConfigurationCommand {
    enum Type {
        JOINT,
        FINALIZE
    }

    record Parsed(Type type, List<Peer> members) {
    }

    private ClusterConfigurationCommand() {
    }

    static byte[] joint(List<Peer> members) {
        var builder = org.gautelis.raft.proto.JointConfigurationCommand.newBuilder();
        for (Peer peer : members) {
            var peerBuilder = org.gautelis.raft.proto.PeerSpec.newBuilder()
                    .setId(peer.getId())
                    .setRole(peer.getRole().name());
            if (peer.getAddress() != null) {
                peerBuilder.setHost(peer.getAddress().getHostString())
                        .setPort(peer.getAddress().getPort());
            }
            builder.addMembers(peerBuilder);
        }
        return org.gautelis.raft.proto.InternalRaftCommand.newBuilder()
                .setJoint(builder.build())
                .build()
                .toByteArray();
    }

    static byte[] finalizeTransition() {
        return org.gautelis.raft.proto.InternalRaftCommand.newBuilder()
                .setFinalize(org.gautelis.raft.proto.FinalizeConfigurationCommand.newBuilder().build())
                .build()
                .toByteArray();
    }

    static Optional<Parsed> parse(byte[] command) {
        if (command == null || command.length == 0) {
            return Optional.empty();
        }
        final org.gautelis.raft.proto.InternalRaftCommand parsed;
        try {
            parsed = org.gautelis.raft.proto.InternalRaftCommand.parseFrom(command);
        } catch (InvalidProtocolBufferException e) {
            return Optional.empty();
        }
        return switch (parsed.getCommandCase()) {
            case JOINT -> {
                List<Peer> members = new ArrayList<>();
                for (org.gautelis.raft.proto.PeerSpec member : parsed.getJoint().getMembersList()) {
                    InetSocketAddress address = member.getHost().isBlank() || member.getPort() <= 0
                            ? null
                            : new InetSocketAddress(member.getHost(), member.getPort());
                    Peer.Role role = member.getRole().isBlank() ? Peer.Role.VOTER : Peer.Role.valueOf(member.getRole());
                    members.add(new Peer(member.getId(), address, role));
                }
                yield Optional.of(new Parsed(Type.JOINT, members));
            }
            case FINALIZE -> Optional.of(new Parsed(Type.FINALIZE, List.of()));
            case COMMAND_NOT_SET -> Optional.empty();
        };
    }

    static boolean isInternalCommand(byte[] payload) {
        return parse(payload).isPresent();
    }

    private static String encode(String value) {
        throw new UnsupportedOperationException("No longer uses string encoding");
    }

    private static String decode(String value) {
        throw new UnsupportedOperationException("No longer uses string encoding");
    }
}
