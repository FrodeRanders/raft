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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.channel.ChannelHandlerContext;
import org.gautelis.raft.model.ClusterMessage;
import org.gautelis.raft.model.LogEntry;
import org.gautelis.raft.model.Message;
import org.gautelis.raft.model.Peer;
import org.gautelis.raft.utilities.BasicAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ClusterMember extends BasicAdapter {
    protected static final Logger log = LoggerFactory.getLogger(ClusterMember.class);

    private final ObjectMapper mapper = new ObjectMapper();

    public ClusterMember(long timeoutMillis, Peer me, List<Peer> peers) {
        super(timeoutMillis, me, peers);
    }

    @Override
    public void handleMessage(String correlationId, String type, JsonNode node, ChannelHandlerContext ctx) throws JsonProcessingException {
        switch (type) {
            case "ClusterMessage" -> {
                ClusterMessage command = mapper.treeToValue(node, ClusterMessage.class);
                log.info("Received cluster message {}", command.getMessage());
            }
        }
    }

    public void inform(String message) {
        final String type = "ClusterMessage";
        ClusterMessage command = new ClusterMessage(
                stateMachine.getTerm(), stateMachine.getId(), message
        );

        try {
            Message msg = new Message(type, command);
            String json = mapper.writeValueAsString(msg);

            stateMachine.getRaftClient().broadcast(type, stateMachine.getTerm(), json);

        } catch (JsonProcessingException jpe) {
            throw new RuntimeException(jpe);
        }
    }
}
