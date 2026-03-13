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
package org.gautelis.raft.bootstrap;

import org.gautelis.raft.protocol.Peer;
import org.gautelis.raft.transport.RaftTransportFactory;

import java.util.List;

/**
 * Fluent adapter construction spec with sensible runtime defaults.
 */
public record AdapterSpec(
        long timeoutMillis,
        Peer me,
        List<Peer> peers,
        Peer joinSeed,
        RuntimeConfiguration runtimeConfiguration,
        RaftTransportFactory transportFactory
) {
    public AdapterSpec {
        if (me == null) {
            throw new IllegalArgumentException("me must not be null");
        }
        peers = peers == null ? List.of() : List.copyOf(peers);
        runtimeConfiguration = runtimeConfiguration == null ? RuntimeConfiguration.load() : runtimeConfiguration;
    }

    public static Builder forPeer(Peer me) {
        return new Builder(me);
    }

    public static final class Builder {
        private final Peer me;
        private long timeoutMillis = 2_000L;
        private List<Peer> peers = List.of();
        private Peer joinSeed;
        private RuntimeConfiguration runtimeConfiguration = RuntimeConfiguration.load();
        private RaftTransportFactory transportFactory;

        private Builder(Peer me) {
            this.me = me;
        }

        public Builder withTimeoutMillis(long timeoutMillis) {
            this.timeoutMillis = timeoutMillis;
            return this;
        }

        public Builder withPeers(List<Peer> peers) {
            this.peers = peers == null ? List.of() : List.copyOf(peers);
            return this;
        }

        public Builder withJoinSeed(Peer joinSeed) {
            this.joinSeed = joinSeed;
            return this;
        }

        public Builder withRuntimeConfiguration(RuntimeConfiguration runtimeConfiguration) {
            this.runtimeConfiguration = runtimeConfiguration == null ? RuntimeConfiguration.load() : runtimeConfiguration;
            return this;
        }

        public Builder withTransportFactory(RaftTransportFactory transportFactory) {
            this.transportFactory = transportFactory;
            return this;
        }

        public AdapterSpec build() {
            if (transportFactory == null) {
                throw new IllegalStateException("transportFactory must not be null for a runnable adapter");
            }
            return buildDetached();
        }

        public AdapterSpec buildDetached() {
            return new AdapterSpec(timeoutMillis, me, peers, joinSeed, runtimeConfiguration, transportFactory);
        }
    }
}
