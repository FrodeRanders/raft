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
package org.gautelis.raft.app.compensator;

import org.gautelis.raft.bootstrap.BasicAdapter;
import org.gautelis.raft.bootstrap.RaftApplicationFactory;
import org.gautelis.raft.bootstrap.RuntimeConfiguration;
import org.gautelis.raft.protocol.Peer;

import java.util.List;

/**
 * Factory for the compensator work-partitioning demo application wiring.
 */
public final class CompensatorApplicationFactory implements RaftApplicationFactory {

    @Override
    public BasicAdapter create(long timeoutMillis, Peer me, List<Peer> peers, Peer joinSeed, RuntimeConfiguration configuration) {
        return CompensatorAdapter.builder(me)
                .withTimeoutMillis(timeoutMillis)
                .withPeers(peers)
                .withJoinSeed(joinSeed)
                .withRuntimeConfiguration(configuration)
                .build();
    }
}
