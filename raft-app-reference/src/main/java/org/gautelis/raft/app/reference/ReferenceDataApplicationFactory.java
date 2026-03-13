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
package org.gautelis.raft.app.reference;

import org.gautelis.raft.bootstrap.BasicAdapter;
import org.gautelis.raft.bootstrap.ClientCommandAuthenticator;
import org.gautelis.raft.bootstrap.RaftApplicationFactory;
import org.gautelis.raft.bootstrap.RuntimeConfiguration;
import org.gautelis.raft.protocol.Peer;
import org.gautelis.raft.transport.RaftTransportFactory;

import java.util.List;
import java.util.Set;

/**
 * Factory for the reference-data application wiring.
 */
public class ReferenceDataApplicationFactory implements RaftApplicationFactory {
    private final Set<String> allowList;
    private final ClientCommandAuthenticator authenticator;
    private final RaftTransportFactory transportFactory;

    public ReferenceDataApplicationFactory(Set<String> allowList, ClientCommandAuthenticator authenticator, RaftTransportFactory transportFactory) {
        this.allowList = allowList == null ? Set.of() : Set.copyOf(allowList);
        this.authenticator = authenticator == null ? ClientCommandAuthenticator.none() : authenticator;
        this.transportFactory = transportFactory;
    }

    @Override
    public BasicAdapter create(long timeoutMillis, Peer me, List<Peer> peers, Peer joinSeed, RuntimeConfiguration configuration) {
        return ReferenceDataAdapter.builder(me)
                .withTimeoutMillis(timeoutMillis)
                .withPeers(peers)
                .withJoinSeed(joinSeed)
                .withRuntimeConfiguration(configuration)
                .withTransportFactory(transportFactory)
                .withAllowedRequesterIds(allowList)
                .withAuthenticator(authenticator)
                .build();
    }
}
