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

import org.gautelis.raft.bootstrap.AdapterSpec;
import org.gautelis.raft.bootstrap.AllowListCommandAuthorizer;
import org.gautelis.raft.bootstrap.ClientCommandAuthenticator;
import org.gautelis.raft.bootstrap.ClientCommandAuthorizer;
import org.gautelis.raft.bootstrap.PolicyBasedAdapter;
import org.gautelis.raft.bootstrap.ReferenceDataWriteAdmissionPolicy;
import org.gautelis.raft.bootstrap.RuntimeConfiguration;
import org.gautelis.raft.protocol.Peer;
import org.gautelis.raft.statemachine.SnapshotStateMachine;
import org.gautelis.raft.transport.RaftTransportFactory;

import java.util.List;
import java.util.Set;

/**
 * Application adapter for centrally managed reference data.
 */
public class ReferenceDataAdapter extends PolicyBasedAdapter {
    public static Builder builder(Peer me) {
        return new Builder(me);
    }

    public ReferenceDataAdapter(long timeoutMillis, Peer me, List<Peer> peers, Peer joinSeed, Set<String> allowedRequesterIds) {
        this(timeoutMillis, me, peers, joinSeed, allowedRequesterIds, ClientCommandAuthenticator.none());
    }

    public ReferenceDataAdapter(long timeoutMillis, Peer me, List<Peer> peers, Peer joinSeed, Set<String> allowedRequesterIds,
                                ClientCommandAuthenticator commandAuthenticator) {
        super(
                timeoutMillis,
                me,
                peers,
                joinSeed,
                RuntimeConfiguration.load(),
                ReferenceDataWriteAdmissionPolicy.INSTANCE,
                allowedRequesterIds == null || allowedRequesterIds.isEmpty()
                        ? ClientCommandAuthorizer.allowAll()
                        : new AllowListCommandAuthorizer(allowedRequesterIds),
                commandAuthenticator
        );
    }

    public ReferenceDataAdapter(long timeoutMillis, Peer me, List<Peer> peers, Peer joinSeed, Set<String> allowedRequesterIds,
                                ClientCommandAuthenticator commandAuthenticator, RuntimeConfiguration runtimeConfiguration) {
        this(AdapterSpec.forPeer(me)
                        .withTimeoutMillis(timeoutMillis)
                        .withPeers(peers)
                        .withJoinSeed(joinSeed)
                        .withRuntimeConfiguration(runtimeConfiguration)
                        .build(),
                allowedRequesterIds,
                commandAuthenticator);
    }

    public ReferenceDataAdapter(long timeoutMillis, Peer me, List<Peer> peers, Set<String> allowedRequesterIds) {
        this(timeoutMillis, me, peers, null, allowedRequesterIds);
    }

    public ReferenceDataAdapter(AdapterSpec spec, Set<String> allowedRequesterIds, ClientCommandAuthenticator commandAuthenticator) {
        super(
                spec,
                ReferenceDataWriteAdmissionPolicy.INSTANCE,
                allowedRequesterIds == null || allowedRequesterIds.isEmpty()
                        ? ClientCommandAuthorizer.allowAll()
                        : new AllowListCommandAuthorizer(allowedRequesterIds),
                commandAuthenticator
        );
    }

    @Override
    protected boolean isValidClientCommand(byte[] command) {
        return ReferenceDataCommand.decode(command).isPresent();
    }

    @Override
    protected boolean isValidClientQuery(byte[] query) {
        var decoded = ReferenceDataQuery.decode(query);
        return decoded.isPresent() && !decoded.get().productId().isBlank();
    }

    @Override
    protected SnapshotStateMachine createSnapshotStateMachine() {
        return new ReferenceDataStateMachine();
    }

    public static final class Builder {
        private final AdapterSpec.Builder specBuilder;
        private Set<String> allowedRequesterIds = Set.of();
        private ClientCommandAuthenticator commandAuthenticator = ClientCommandAuthenticator.none();

        private Builder(Peer me) {
            this.specBuilder = AdapterSpec.forPeer(me);
        }

        public Builder withTimeoutMillis(long timeoutMillis) {
            specBuilder.withTimeoutMillis(timeoutMillis);
            return this;
        }

        public Builder withPeers(List<Peer> peers) {
            specBuilder.withPeers(peers);
            return this;
        }

        public Builder withJoinSeed(Peer joinSeed) {
            specBuilder.withJoinSeed(joinSeed);
            return this;
        }

        public Builder withRuntimeConfiguration(RuntimeConfiguration runtimeConfiguration) {
            specBuilder.withRuntimeConfiguration(runtimeConfiguration);
            return this;
        }

        public Builder withTransportFactory(RaftTransportFactory transportFactory) {
            specBuilder.withTransportFactory(transportFactory);
            return this;
        }

        public Builder withAllowedRequesterIds(Set<String> allowedRequesterIds) {
            this.allowedRequesterIds = allowedRequesterIds == null ? Set.of() : Set.copyOf(allowedRequesterIds);
            return this;
        }

        public Builder withAuthenticator(ClientCommandAuthenticator commandAuthenticator) {
            this.commandAuthenticator = commandAuthenticator == null ? ClientCommandAuthenticator.none() : commandAuthenticator;
            return this;
        }

        public ReferenceDataAdapter build() {
            return new ReferenceDataAdapter(specBuilder.build(), allowedRequesterIds, commandAuthenticator);
        }
    }
}
