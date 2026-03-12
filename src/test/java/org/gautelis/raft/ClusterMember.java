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

import org.gautelis.raft.storage.*;
import org.gautelis.raft.statemachine.*;
import org.gautelis.raft.transport.netty.*;
import org.gautelis.raft.serialization.ProtoMapper;

import io.netty.channel.ChannelHandlerContext;
import org.gautelis.raft.protocol.ClientCommandRequest;
import org.gautelis.raft.protocol.Peer;
import org.gautelis.raft.protocol.StateMachineCommand;
import org.gautelis.raft.bootstrap.BasicAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ClusterMember extends BasicAdapter {
    protected static final Logger log = LoggerFactory.getLogger(ClusterMember.class);

    public ClusterMember(long timeoutMillis, Peer me, List<Peer> peers) {
        super(timeoutMillis, me, peers);
    }

    @Override
    public void handleMessage(String correlationId, String type, byte[] payload, ChannelHandlerContext ctx) {
        switch (type) {
            case "ClientCommandRequest" -> {
                var parsed = ProtoMapper.parseClientCommandRequest(payload);
                if (parsed.isEmpty()) {
                    log.info("Received invalid client command payload");
                    return;
                }
                ClientCommandRequest command = ProtoMapper.fromProto(parsed.get());
                log.info("Received client command ({} bytes)", command.getCommand().length);
            }
        }
    }

    public void inform(StateMachineCommand message) {
        final String type = "ClientCommandRequest";
        ClientCommandRequest command = new ClientCommandRequest(
                stateMachine.getTerm(), stateMachine.getId(), message.encode()
        );

        byte[] payload = ProtoMapper.toProto(command).toByteString().toByteArray();
        stateMachine.getRaftClient().broadcast(type, stateMachine.getTerm(), payload);
    }
}
