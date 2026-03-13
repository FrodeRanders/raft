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

import java.util.List;

/**
 * Adapter variant that injects explicit admission and authorization policies for client writes.
 */
public class PolicyBasedAdapter extends BasicAdapter {
    private final ClientWriteAdmissionPolicy writeAdmissionPolicy;
    private final ClientCommandAuthorizer commandAuthorizer;
    private final ClientCommandAuthenticator commandAuthenticator;

    public PolicyBasedAdapter(long timeoutMillis, Peer me, List<Peer> peers,
                              ClientWriteAdmissionPolicy writeAdmissionPolicy,
                              ClientCommandAuthorizer commandAuthorizer,
                              ClientCommandAuthenticator commandAuthenticator) {
        this(AdapterSpec.forPeer(me)
                        .withTimeoutMillis(timeoutMillis)
                        .withPeers(peers)
                        .build(),
                writeAdmissionPolicy,
                commandAuthorizer,
                commandAuthenticator);
    }

    public PolicyBasedAdapter(long timeoutMillis, Peer me, List<Peer> peers, Peer joinSeed,
                              ClientWriteAdmissionPolicy writeAdmissionPolicy,
                              ClientCommandAuthorizer commandAuthorizer,
                              ClientCommandAuthenticator commandAuthenticator) {
        this(AdapterSpec.forPeer(me)
                        .withTimeoutMillis(timeoutMillis)
                        .withPeers(peers)
                        .withJoinSeed(joinSeed)
                        .build(),
                writeAdmissionPolicy,
                commandAuthorizer,
                commandAuthenticator);
    }

    public PolicyBasedAdapter(long timeoutMillis, Peer me, List<Peer> peers, Peer joinSeed, RuntimeConfiguration runtimeConfiguration,
                              ClientWriteAdmissionPolicy writeAdmissionPolicy,
                              ClientCommandAuthorizer commandAuthorizer,
                              ClientCommandAuthenticator commandAuthenticator) {
        this(AdapterSpec.forPeer(me)
                        .withTimeoutMillis(timeoutMillis)
                        .withPeers(peers)
                        .withJoinSeed(joinSeed)
                        .withRuntimeConfiguration(runtimeConfiguration)
                        .build(),
                writeAdmissionPolicy,
                commandAuthorizer,
                commandAuthenticator);
    }

    public PolicyBasedAdapter(AdapterSpec spec,
                              ClientWriteAdmissionPolicy writeAdmissionPolicy,
                              ClientCommandAuthorizer commandAuthorizer,
                              ClientCommandAuthenticator commandAuthenticator) {
        super(spec);
        this.writeAdmissionPolicy = writeAdmissionPolicy == null ? LeaderRedirectWriteAdmissionPolicy.INSTANCE : writeAdmissionPolicy;
        this.commandAuthorizer = commandAuthorizer == null ? ClientCommandAuthorizer.allowAll() : commandAuthorizer;
        this.commandAuthenticator = commandAuthenticator == null ? ClientCommandAuthenticator.none() : commandAuthenticator;
    }

    @Override
    protected ClientWriteAdmissionPolicy clientWriteAdmissionPolicy() {
        return writeAdmissionPolicy;
    }

    @Override
    protected ClientCommandAuthorizer clientCommandAuthorizer() {
        return commandAuthorizer;
    }

    @Override
    protected ClientCommandAuthenticator clientCommandAuthenticator() {
        return commandAuthenticator;
    }
}
