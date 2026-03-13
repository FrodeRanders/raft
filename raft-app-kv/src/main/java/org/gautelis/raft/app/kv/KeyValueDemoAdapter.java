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
import org.gautelis.raft.bootstrap.BasicAdapter;
import org.gautelis.raft.bootstrap.RuntimeConfiguration;
import org.gautelis.raft.protocol.Peer;
import org.gautelis.raft.statemachine.SnapshotStateMachine;
import org.gautelis.raft.transport.RaftTransportFactory;

import java.util.List;

/**
 * Demo/runtime adapter for the key-value sample application.
 */
public class KeyValueDemoAdapter extends BasicAdapter {
    public static Builder builder(Peer me) {
        return new Builder(me);
    }

    public KeyValueDemoAdapter(long timeoutMillis, Peer me, List<Peer> peers, Peer joinSeed) {
        super(timeoutMillis, me, peers, joinSeed);
    }

    public KeyValueDemoAdapter(long timeoutMillis, Peer me, List<Peer> peers, Peer joinSeed, RuntimeConfiguration runtimeConfiguration) {
        super(timeoutMillis, me, peers, joinSeed, runtimeConfiguration);
    }

    public KeyValueDemoAdapter(long timeoutMillis, Peer me, List<Peer> peers) {
        super(timeoutMillis, me, peers);
    }

    public KeyValueDemoAdapter(AdapterSpec spec) {
        super(spec);
    }

    @Override
    protected SnapshotStateMachine createSnapshotStateMachine() {
        return new KeyValueStateMachine();
    }

    public static final class Builder {
        private final AdapterSpec.Builder specBuilder;

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

        public KeyValueDemoAdapter build() {
            return new KeyValueDemoAdapter(specBuilder.build());
        }
    }
}
