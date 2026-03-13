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

import org.gautelis.raft.RaftNode;
import org.gautelis.raft.protocol.ClientCommandRequest;
import org.gautelis.raft.protocol.ClientCommandResponse;
import org.gautelis.raft.protocol.ClientQueryRequest;
import org.gautelis.raft.protocol.ClientQueryResponse;
import org.gautelis.raft.protocol.Peer;

final class ClientRequestHandler {
    private final AdapterHandlerContext context;

    ClientRequestHandler(AdapterHandlerContext context) {
        this.context = context;
    }

    ClientCommandResponse handleClientCommandRequest(ClientCommandRequest request) {
        RaftNode stateMachine = context.stateMachineSupplier().get();
        if (stateMachine == null || request == null) {
            return new ClientCommandResponse(0L, context.me().getId(), false, "INVALID", "Client command request is invalid", "", "", 0);
        }
        byte[] command = request.getCommand();
        if (!context.commandValidator().test(command)) {
            Peer leader = stateMachine.getKnownLeaderPeer();
            return new ClientCommandResponse(stateMachine.getTerm(), context.me().getId(), false, "INVALID", "Command payload is invalid", leaderId(leader), leaderHost(leader), leaderPort(leader));
        }
        Peer leader = stateMachine.getKnownLeaderPeer();
        Peer.Role localMemberRole = stateMachine.getLocalMemberRole();
        ClientCommandAuthenticationResult authentication = context.commandAuthenticator().get().authenticate(
                new ClientCommandAuthenticationContext(
                        request.getPeerId(),
                        request.getAuthScheme(),
                        request.getAuthToken(),
                        command,
                        context.me(),
                        localMemberRole,
                        stateMachine.isLeader(),
                        stateMachine.isDecommissioned()
                )
        );
        if (!authentication.authenticated()) {
            return new ClientCommandResponse(stateMachine.getTerm(), context.me().getId(), false, authentication.status(), authentication.message(), leaderId(leader), leaderHost(leader), leaderPort(leader));
        }
        String principalId = authentication.principal() == null ? request.getPeerId() : authentication.principal().principalId();
        ClientWriteAdmissionDecision admissionDecision = context.writeAdmissionPolicy().get().evaluate(
                new ClientWriteAdmissionContext(
                        principalId,
                        command,
                        context.me(),
                        localMemberRole,
                        stateMachine.isLeader(),
                        stateMachine.isDecommissioned(),
                        leader
                )
        );
        if (admissionDecision.action() != ClientWriteAdmissionDecision.Action.ACCEPT) {
            return new ClientCommandResponse(stateMachine.getTerm(), context.me().getId(), false, admissionDecision.status(), admissionDecision.message(), leaderId(leader), leaderHost(leader), leaderPort(leader));
        }
        ClientCommandAuthorizationResult authorization = context.commandAuthorizer().get().authorize(
                new ClientCommandAuthorizationContext(
                        principalId,
                        command,
                        context.me(),
                        localMemberRole,
                        stateMachine.isLeader(),
                        stateMachine.isDecommissioned()
                )
        );
        if (!authorization.allowed()) {
            return new ClientCommandResponse(stateMachine.getTerm(), context.me().getId(), false, authorization.status(), authorization.message(), leaderId(leader), leaderHost(leader), leaderPort(leader));
        }
        if (stateMachine.isLeader()) {
            if (!context.commandSubmitter().apply(command)) {
                return new ClientCommandResponse(stateMachine.getTerm(), context.me().getId(), false, "REJECTED", "Command could not be applied", leaderId(leader), leaderHost(leader), leaderPort(leader));
            }
            context.log().info("Accepted typed client command from {}", request.getPeerId());
            return new ClientCommandResponse(stateMachine.getTerm(), context.me().getId(), true, "ACCEPTED", "Command accepted for replication", leaderId(leader), leaderHost(leader), leaderPort(leader));
        }
        return new ClientCommandResponse(stateMachine.getTerm(), context.me().getId(), false, "REJECTED", "Node is not leader or command could not be applied", "", "", 0);
    }

    ClientQueryResponse handleClientQueryRequest(ClientQueryRequest request) {
        RaftNode stateMachine = context.stateMachineSupplier().get();
        if (stateMachine == null || request == null) {
            return new ClientQueryResponse(0L, context.me().getId(), false, "INVALID", "Client query request is invalid", "", "", 0, new byte[0]);
        }
        byte[] query = request.getQuery();
        if (!context.queryValidator().test(query)) {
            Peer leader = stateMachine.getKnownLeaderPeer();
            return new ClientQueryResponse(stateMachine.getTerm(), context.me().getId(), false, "INVALID", "Query payload is invalid", leaderId(leader), leaderHost(leader), leaderPort(leader), new byte[0]);
        }
        ClientCommandAuthenticationResult authentication = context.commandAuthenticator().get().authenticate(
                request.getPeerId(),
                request.getAuthScheme(),
                request.getAuthToken(),
                context.me(),
                stateMachine.getLocalMemberRole(),
                stateMachine.isLeader(),
                stateMachine.isDecommissioned()
        );
        if (!authentication.authenticated()) {
            Peer leader = stateMachine.getKnownLeaderPeer();
            return new ClientQueryResponse(stateMachine.getTerm(), context.me().getId(), false, authentication.status(), authentication.message(), leaderId(leader), leaderHost(leader), leaderPort(leader), new byte[0]);
        }
        if (stateMachine.isLeader()) {
            if (!stateMachine.canServeLinearizableRead() && !stateMachine.awaitLinearizableRead()) {
                Peer leader = stateMachine.getKnownLeaderPeer();
                return new ClientQueryResponse(stateMachine.getTerm(), context.me().getId(), false, "RETRY", "Leader cannot currently guarantee a linearizable read", leaderId(leader), leaderHost(leader), leaderPort(leader), new byte[0]);
            }
            byte[] result = stateMachine.queryStateMachine(query);
            if (result.length == 0) {
                Peer leader = stateMachine.getKnownLeaderPeer();
                return new ClientQueryResponse(stateMachine.getTerm(), context.me().getId(), false, "UNSUPPORTED", "State machine does not support queries", leaderId(leader), leaderHost(leader), leaderPort(leader), new byte[0]);
            }
            Peer leader = stateMachine.getKnownLeaderPeer();
            return new ClientQueryResponse(stateMachine.getTerm(), context.me().getId(), true, "OK", "Query completed", leaderId(leader), leaderHost(leader), leaderPort(leader), result);
        }
        Peer leader = stateMachine.getKnownLeaderPeer();
        if (leader != null && !stateMachine.isDecommissioned()) {
            return new ClientQueryResponse(stateMachine.getTerm(), context.me().getId(), false, "REDIRECT", "Node is not leader; send query to current leader", leaderId(leader), leaderHost(leader), leaderPort(leader), new byte[0]);
        }
        return new ClientQueryResponse(stateMachine.getTerm(), context.me().getId(), false, "REJECTED", "Node is not leader or query could not be applied", "", "", 0, new byte[0]);
    }

    private static String leaderId(Peer leader) {
        return leader == null ? "" : leader.getId();
    }

    private static String leaderHost(Peer leader) {
        return leader == null || leader.getAddress() == null ? "" : leader.getAddress().getHostString();
    }

    private static int leaderPort(Peer leader) {
        return leader == null || leader.getAddress() == null ? 0 : leader.getAddress().getPort();
    }
}
