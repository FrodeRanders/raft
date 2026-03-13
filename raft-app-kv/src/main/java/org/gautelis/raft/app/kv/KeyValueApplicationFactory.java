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
package org.gautelis.raft.app.kv;

import org.gautelis.raft.bootstrap.AdapterSpec;
import org.gautelis.raft.bootstrap.AllowListCommandAuthorizer;
import org.gautelis.raft.bootstrap.BasicAdapter;
import org.gautelis.raft.bootstrap.ClientCommandAuthenticator;
import org.gautelis.raft.bootstrap.ClientCommandAuthorizer;
import org.gautelis.raft.bootstrap.ClientWriteAdmissionPolicy;
import org.gautelis.raft.bootstrap.LeaderRedirectWriteAdmissionPolicy;
import org.gautelis.raft.bootstrap.PolicyBasedAdapter;
import org.gautelis.raft.bootstrap.RaftApplicationFactory;
import org.gautelis.raft.bootstrap.RuntimeConfiguration;
import org.gautelis.raft.transport.RaftTransportFactory;
import org.gautelis.raft.protocol.Peer;

import java.util.List;
import java.util.Set;

/**
 * Factory for the key-value demo application wiring.
 */
public final class KeyValueApplicationFactory implements RaftApplicationFactory {
    private final Set<String> allowList;
    private final ClientCommandAuthenticator authenticator;
    private final String authMode;
    private final RaftTransportFactory transportFactory;

    public KeyValueApplicationFactory(Set<String> allowList, ClientCommandAuthenticator authenticator, String authMode, RaftTransportFactory transportFactory) {
        this.allowList = allowList == null ? Set.of() : Set.copyOf(allowList);
        this.authenticator = authenticator == null ? ClientCommandAuthenticator.none() : authenticator;
        this.authMode = authMode == null ? "none" : authMode;
        this.transportFactory = transportFactory;
    }

    @Override
    public BasicAdapter create(long timeoutMillis, Peer me, List<Peer> peers, Peer joinSeed, RuntimeConfiguration configuration) {
        if (allowList.isEmpty() && "none".equals(authMode)) {
            return KeyValueDemoAdapter.builder(me)
                    .withTimeoutMillis(timeoutMillis)
                    .withPeers(peers)
                    .withJoinSeed(joinSeed)
                    .withRuntimeConfiguration(configuration)
                    .withTransportFactory(transportFactory)
                    .build();
        }
        return new KeyValueDemoPolicyAdapter(
                timeoutMillis,
                me,
                peers,
                joinSeed,
                configuration,
                transportFactory,
                LeaderRedirectWriteAdmissionPolicy.INSTANCE,
                allowList.isEmpty() ? ClientCommandAuthorizer.allowAll() : new AllowListCommandAuthorizer(allowList),
                authenticator
        );
    }

    private static final class KeyValueDemoPolicyAdapter extends PolicyBasedAdapter {
        private KeyValueDemoPolicyAdapter(long timeoutMillis, Peer me, List<Peer> peers, Peer joinSeed,
                                          RuntimeConfiguration runtimeConfiguration,
                                          RaftTransportFactory transportFactory,
                                          ClientWriteAdmissionPolicy writeAdmissionPolicy,
                                          ClientCommandAuthorizer commandAuthorizer,
                                          ClientCommandAuthenticator commandAuthenticator) {
            super(AdapterSpec.forPeer(me)
                            .withTimeoutMillis(timeoutMillis)
                            .withPeers(peers)
                            .withJoinSeed(joinSeed)
                            .withRuntimeConfiguration(runtimeConfiguration)
                            .withTransportFactory(transportFactory)
                            .build(),
                    writeAdmissionPolicy,
                    commandAuthorizer,
                    commandAuthenticator);
        }

        @Override
        protected org.gautelis.raft.statemachine.SnapshotStateMachine createSnapshotStateMachine() {
            return new KeyValueStateMachine();
        }
    }
}
