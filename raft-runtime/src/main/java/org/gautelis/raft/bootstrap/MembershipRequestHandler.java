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
import org.gautelis.raft.protocol.JoinClusterRequest;
import org.gautelis.raft.protocol.JoinClusterResponse;
import org.gautelis.raft.protocol.JoinClusterStatusRequest;
import org.gautelis.raft.protocol.JoinClusterStatusResponse;
import org.gautelis.raft.protocol.Peer;
import org.gautelis.raft.protocol.ReconfigureClusterRequest;
import org.gautelis.raft.protocol.ReconfigureClusterResponse;

final class MembershipRequestHandler {
    private final AdapterHandlerContext context;

    MembershipRequestHandler(AdapterHandlerContext context) {
        this.context = context;
    }

    JoinClusterResponse handleJoinClusterRequest(JoinClusterRequest request) {
        RaftNode stateMachine = context.stateMachineSupplier().get();
        if (stateMachine == null || request == null || request.getJoiningPeer() == null) {
            return new JoinClusterResponse(0L, context.me().getId(), false, "INVALID", "Join request is invalid", "");
        }
        ClientCommandAuthenticationResult authentication = context.authenticator().apply(new AdapterRequestAuth(
                request.getPeerId(), request.getAuthScheme(), request.getAuthToken()
        ));
        if (!authentication.authenticated()) {
            return new JoinClusterResponse(stateMachine.getTerm(), context.me().getId(), false, authentication.status(), authentication.message(), knownLeaderId(stateMachine));
        }
        try {
            context.peerRegistrar().accept(request.getJoiningPeer());
        } catch (IllegalArgumentException e) {
            return new JoinClusterResponse(stateMachine.getTerm(), context.me().getId(), false, "INVALID", e.getMessage(), knownLeaderId(stateMachine));
        }
        if (stateMachine.isLeader()) {
            boolean accepted = stateMachine.submitJoinConfigurationChange(request.getJoiningPeer());
            if (!accepted) {
                return new JoinClusterResponse(stateMachine.getTerm(), context.me().getId(), false, "REJECTED", "Join request could not be applied", knownLeaderId(stateMachine));
            }
            RaftNode.JoinStatus joinStatus = stateMachine.getJoinStatus(request.getJoiningPeer().getId());
            return new JoinClusterResponse(stateMachine.getTerm(), context.me().getId(), true, joinStatus.status(), joinStatus.message(), knownLeaderId(stateMachine));
        }
        Peer leader = stateMachine.getKnownLeaderPeer();
        if (leader != null && !stateMachine.isDecommissioned()) {
            stateMachine.getRaftClient().sendJoinClusterRequest(leader, request);
            return new JoinClusterResponse(stateMachine.getTerm(), context.me().getId(), true, "FORWARDED", "Join request forwarded to leader", leader.getId());
        }
        return new JoinClusterResponse(stateMachine.getTerm(), context.me().getId(), false, "REJECTED", "Node is not leader or join request could not be applied", knownLeaderId(stateMachine));
    }

    JoinClusterStatusResponse handleJoinClusterStatusRequest(JoinClusterStatusRequest request) {
        RaftNode stateMachine = context.stateMachineSupplier().get();
        if (stateMachine == null || request == null) {
            return new JoinClusterStatusResponse(0L, context.me().getId(), false, "INVALID", "Join status request is invalid", "");
        }
        ClientCommandAuthenticationResult authentication = context.authenticator().apply(new AdapterRequestAuth(
                request.getPeerId(), request.getAuthScheme(), request.getAuthToken()
        ));
        if (!authentication.authenticated()) {
            return new JoinClusterStatusResponse(stateMachine.getTerm(), context.me().getId(), false, authentication.status(), authentication.message(), knownLeaderId(stateMachine));
        }
        RaftNode.JoinStatus joinStatus = stateMachine.getJoinStatus(request.getTargetPeerId());
        return new JoinClusterStatusResponse(
                stateMachine.getTerm(),
                context.me().getId(),
                joinStatus.success(),
                joinStatus.status(),
                joinStatus.message(),
                knownLeaderId(stateMachine)
        );
    }

    ReconfigureClusterResponse handleReconfigureClusterRequest(ReconfigureClusterRequest request) {
        RaftNode stateMachine = context.stateMachineSupplier().get();
        if (stateMachine == null || request == null || request.getAction() == null) {
            return new ReconfigureClusterResponse(0L, context.me().getId(), false, "INVALID", "Reconfiguration request is invalid", "");
        }
        ClientCommandAuthenticationResult authentication = context.authenticator().apply(new AdapterRequestAuth(
                request.getPeerId(), request.getAuthScheme(), request.getAuthToken()
        ));
        if (!authentication.authenticated()) {
            return new ReconfigureClusterResponse(stateMachine.getTerm(), context.me().getId(), false, authentication.status(), authentication.message(), knownLeaderId(stateMachine));
        }
        try {
            for (Peer member : request.getMembers()) {
                context.peerRegistrar().accept(member);
            }
        } catch (IllegalArgumentException e) {
            return new ReconfigureClusterResponse(stateMachine.getTerm(), context.me().getId(), false, "INVALID", e.getMessage(), knownLeaderId(stateMachine));
        }
        if (stateMachine.isLeader()) {
            boolean accepted = switch (request.getAction()) {
                case JOINT -> stateMachine.submitJointConfigurationChange(request.getMembers());
                case FINALIZE -> stateMachine.submitFinalizeConfigurationChange();
                case PROMOTE -> request.getMembers().size() == 1 && stateMachine.submitPromoteLearnerChange(request.getMembers().getFirst());
                case DEMOTE -> request.getMembers().size() == 1 && stateMachine.submitDemoteVoterChange(request.getMembers().getFirst());
            };
            if (!accepted) {
                return new ReconfigureClusterResponse(stateMachine.getTerm(), context.me().getId(), false, "REJECTED", "Reconfiguration request could not be applied", knownLeaderId(stateMachine));
            }
            String message = switch (request.getAction()) {
                case JOINT -> "Joint configuration accepted";
                case FINALIZE -> "Finalize configuration accepted";
                case PROMOTE -> "Learner promotion accepted";
                case DEMOTE -> "Voter demotion accepted";
            };
            return new ReconfigureClusterResponse(stateMachine.getTerm(), context.me().getId(), true, "ACCEPTED", message, knownLeaderId(stateMachine));
        }
        Peer leader = stateMachine.getKnownLeaderPeer();
        if (leader != null && !stateMachine.isDecommissioned()) {
            stateMachine.getRaftClient().sendReconfigureClusterRequest(leader, request);
            return new ReconfigureClusterResponse(stateMachine.getTerm(), context.me().getId(), true, "FORWARDED", "Reconfiguration request forwarded to leader", leader.getId());
        }
        return new ReconfigureClusterResponse(stateMachine.getTerm(), context.me().getId(), false, "REJECTED", "Node is not leader or reconfiguration request could not be applied", knownLeaderId(stateMachine));
    }

    private static String knownLeaderId(RaftNode stateMachine) {
        Peer leader = stateMachine == null ? null : stateMachine.getKnownLeaderPeer();
        return leader == null ? "" : leader.getId();
    }
}
