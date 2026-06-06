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

/**
 * Handles application-facing client command and query requests.
 *
 * <p>This class is deliberately outside {@link RaftNode}: authentication,
 * authorization, payload validation, leader redirects, and read-query response
 * formatting are application/API concerns.  The Raft node is only asked to
 * replicate commands or prove that a leader-local read is currently safe.</p>
 */
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
        // Validate bytes before they enter the Raft log.  Once committed, every
        // peer must apply the exact same command, so invalid domain payloads are
        // rejected at the leader/API boundary rather than during log replay.
        if (!context.commandValidator().test(command)) {
            Peer leader = stateMachine.getKnownLeaderPeer();
            return new ClientCommandResponse(stateMachine.getTerm(), context.me().getId(), false, "INVALID", "Command payload is invalid", leaderId(leader), leaderHost(leader), leaderPort(leader));
        }
        Peer leader = stateMachine.getKnownLeaderPeer();
        Peer.Role localMemberRole = stateMachine.getLocalMemberRole();
        // Authentication happens before admission/authorization.  The resulting
        // principal is what downstream policies should reason about; the peer id
        // in the request is just untrusted client input until this succeeds.
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
        // Admission answers "may this node accept this write now?"  The default
        // policy accepts only on the leader and redirects followers, but domain
        // adapters can also reject learner writes or decommissioned nodes.
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
        // Authorization answers "may this principal perform this command?"
        // Keeping it after admission avoids authorizing commands that this node
        // would not be allowed to propose anyway.
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
            // The response is ACCEPTED only after the entry has committed and
            // been applied locally.  Merely appending to the leader log is not a
            // client-visible success condition.
            RaftNode.CommandApplication application = stateMachine.submitCommandWithResult(command);
            if (!application.success()) {
                return new ClientCommandResponse(stateMachine.getTerm(), context.me().getId(), false, "RETRY", "Command was not committed and applied before acknowledgement", leaderId(leader), leaderHost(leader), leaderPort(leader));
            }
            context.log().info("Accepted typed client command from {}", request.getPeerId());
            return new ClientCommandResponse(stateMachine.getTerm(), context.me().getId(), true, "ACCEPTED", "Command committed and applied", leaderId(leader), leaderHost(leader), leaderPort(leader), application.result());
        }
        return new ClientCommandResponse(stateMachine.getTerm(), context.me().getId(), false, "REJECTED", "Node is not leader or command could not be applied", "", "", 0);
    }

    ClientQueryResponse handleClientQueryRequest(ClientQueryRequest request) {
        RaftNode stateMachine = context.stateMachineSupplier().get();
        if (stateMachine == null || request == null) {
            return new ClientQueryResponse(0L, context.me().getId(), false, "INVALID", "Client query request is invalid", "", "", 0, new byte[0]);
        }
        byte[] query = request.getQuery();
        // Queries are not written to the Raft log, so they need their own domain
        // validation path and a read barrier before local state can be exposed.
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
            // Linearizable reads rely on recent leader authority.  If the current
            // lease is not fresh enough, awaitLinearizableRead sends/awaits a
            // heartbeat barrier before the state machine is queried.
            if (!stateMachine.canServeLinearizableRead() && !stateMachine.awaitLinearizableRead()) {
                Peer leader = stateMachine.getKnownLeaderPeer();
                return new ClientQueryResponse(stateMachine.getTerm(), context.me().getId(), false, "RETRY", "Leader cannot currently guarantee a linearizable read", leaderId(leader), leaderHost(leader), leaderPort(leader), new byte[0]);
            }
            byte[] result = stateMachine.queryStateMachine(query);
            if (result.length == 0) {
                // Empty result is reserved here as "query API unsupported" for
                // the adapter boundary.  Domain query protocols that need empty
                // values should encode an explicit result envelope.
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
