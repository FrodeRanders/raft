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

import org.gautelis.raft.transport.MessageResponder;

/**
 * Receives decoded envelopes after the transport layer has identified their message type.
 */
public interface MessageHandler {
    /**
     * Handles a decoded request or response in either server or client mode.
     *
     * <p>In server mode, a request initiated by a peer normally results in a response written
     * through the supplied responder. In client mode, the payload typically represents a response
     * to an earlier request issued by the local peer.</p>
     *
     * @param correlationId request or response correlation identifier
     * @param type logical message type
     * @param payload raw payload bytes, for example protobuf-encoded data
     * @param responder callback used to write a response when the transport supports one
     * @throws Exception if the message cannot be processed
     */
    void handle(String correlationId, String type, byte[] payload, MessageResponder responder) throws Exception;
}
