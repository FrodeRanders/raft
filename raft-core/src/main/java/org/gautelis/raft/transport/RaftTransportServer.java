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

/**
 * Server-side transport abstraction that accepts inbound Raft RPC traffic.
 */
public interface RaftTransportServer extends AutoCloseable {
    /**
     * Starts the server and begins accepting requests.
     *
     * @throws InterruptedException if startup is interrupted
     */
    void start() throws InterruptedException;

    /** Closes the server and releases transport resources. */
    @Override
    void close();
}
