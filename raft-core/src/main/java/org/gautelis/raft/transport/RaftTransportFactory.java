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
package org.gautelis.raft.transport;

import org.gautelis.raft.MessageHandler;
import org.gautelis.raft.RaftNode;

/**
 * Factory for transport client and server implementations used by a {@link RaftNode}.
 */
public interface RaftTransportFactory {
    /**
     * Creates a transport client bound to the local peer identity.
     *
     * @param localPeerId local peer identifier
     * @param messageHandler handler for inbound responses or messages
     * @return transport client
     */
    RaftTransportClient createClient(String localPeerId, MessageHandler messageHandler);

    /**
     * Creates a transport server for a node.
     *
     * @param stateMachine node instance that will receive inbound RPCs
     * @param port local listening port
     * @return transport server
     */
    RaftTransportServer createServer(RaftNode stateMachine, int port);
}
