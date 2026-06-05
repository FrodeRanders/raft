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

/**
 * Optional application-facing authentication hook for client writes.
 */
@FunctionalInterface
public interface ClientCommandAuthenticator {
    /**
     * Authenticates a client command request.
     *
     * @param context authentication context
     * @return authentication result
     */
    ClientCommandAuthenticationResult authenticate(ClientCommandAuthenticationContext context);

    /**
     * Convenience adapter for older call sites that pass authentication fields separately.
     *
     * @param requesterId requester identity from the request
     * @param authenticationScheme authentication scheme
     * @param authenticationToken authentication token
     * @param localPeer local peer
     * @param localMemberRole local membership role
     * @param leader whether the local peer is leader
     * @param decommissioned whether the local peer is decommissioned
     * @return authentication result
     */
    default ClientCommandAuthenticationResult authenticate(String requesterId, String authenticationScheme, String authenticationToken,
                                                           org.gautelis.raft.protocol.Peer localPeer,
                                                           org.gautelis.raft.protocol.Peer.Role localMemberRole,
                                                           boolean leader, boolean decommissioned) {
        return authenticate(new ClientCommandAuthenticationContext(
                requesterId,
                authenticationScheme,
                authenticationToken,
                null,
                localPeer,
                localMemberRole,
                leader,
                decommissioned
        ));
    }

    /**
     * Creates an authenticator that accepts every request and maps it to a default principal.
     *
     * @return permissive authenticator
     */
    static ClientCommandAuthenticator none() {
        return context -> ClientCommandAuthenticationResult.authenticated(
                new AuthenticatedPrincipal(
                        context == null || context.requesterId() == null || context.requesterId().isBlank()
                                ? "unknown"
                                : context.requesterId(),
                        "none"
                )
        );
    }
}
