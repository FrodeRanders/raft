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
package org.gautelis.raft.utilities;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import io.netty.channel.ChannelHandlerContext;
import org.gautelis.raft.*;
import org.gautelis.raft.model.LogEntry;
import org.gautelis.raft.model.Peer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class BasicAdapter {
    protected static final Logger log = LoggerFactory.getLogger(BasicAdapter.class);

    protected final long timeoutMillis;
    protected final Peer me;
    protected final List<Peer> peers;

    protected RaftNode stateMachine;

    public BasicAdapter(long timeoutMillis, Peer me, List<Peer> peers) {
        this.timeoutMillis = timeoutMillis;
        this.me = me;
        this.peers = peers;
    }

    public void start() {
        LogStore logStore = new InMemoryLogStore();

        MessageHandler messageHandler = this::handleMessage;

        stateMachine = new RaftNode(
                me, peers, timeoutMillis, messageHandler, new RaftClient(messageHandler), logStore
        );

        try {
            RaftServer server = new RaftServer(stateMachine, me.getAddress().getPort());
            server.start();
        }
        catch (InterruptedException ie) {
            log.info("Interrupted!", ie);
        }
        finally {
            stateMachine.shutdown();
        }
    }

    public void handleMessage(String correlationId, String type, JsonNode node, ChannelHandlerContext ctx) throws JsonProcessingException {
        log.debug(
                "Received '{}' message {}",
                type, node
        );
    }
}
