/*
 * Copyright (C) 2026 Frode Randers
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

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use graft_core::raft_node::{AppendEntriesRequest, InstallSnapshotRequest, RaftNode, VoteRequest};
use graft_core::types::{LogEntry, NodeState};
use graft_proto::raft;
use graft_transport::client::RaftClient;
use prost::Message;

use crate::policy::{
    AdmissionContext, AdmissionPolicy, AllowAllAuthenticator, AllowAllAuthorizer, Authenticator,
    Authorizer, RateLimiter, StuckDetector,
};
use crate::runtime::RaftRuntime;

pub struct RaftHandler {
    pub node: Arc<parking_lot::Mutex<RaftNode>>,
    pub client: Arc<RaftClient>,
    pub runtime: Arc<RaftRuntime>,
    pub authenticator: Arc<dyn Authenticator>,
    pub authorizer: Arc<dyn Authorizer>,
    pub admission: Arc<dyn AdmissionPolicy>,
    pub telemetry_rate_limiter: RateLimiter,
    pub stuck_detector: StuckDetector,
}

impl RaftHandler {
    pub fn new(
        node: Arc<parking_lot::Mutex<RaftNode>>,
        client: Arc<RaftClient>,
        runtime: Arc<RaftRuntime>,
    ) -> Self {
        Self {
            node,
            client,
            runtime,
            authenticator: Arc::new(AllowAllAuthenticator),
            authorizer: Arc::new(AllowAllAuthorizer),
            admission: Arc::new(crate::policy::AcceptAllAdmissionPolicy),
            telemetry_rate_limiter: RateLimiter::new(30),
            stuck_detector: StuckDetector::new(60_000),
        }
    }

    pub async fn dispatch(&self, envelope_type: &str, payload: &[u8]) -> Vec<u8> {
        let now_millis = now_millis();
        match envelope_type {
            "VoteRequest" => handle_vote(&self.node, payload),
            "AppendEntriesRequest" => handle_append(&self.node, payload),
            "InstallSnapshotRequest" => handle_snapshot(&self.node, payload),
            "ClientCommandRequest" => handle_client_cmd(self, payload).await,
            "ClientQueryRequest" => handle_client_query(self, payload).await,
            "TelemetryRequest" => handle_telemetry_req(self, now_millis, payload),
            "ClusterSummaryRequest" => handle_summary_req(self, now_millis, payload),
            "JoinClusterRequest" => handle_join(&self.node, &self.runtime, payload),
            "JoinClusterStatusRequest" => handle_join_status(&self.node, payload),
            "ReconfigureClusterRequest" => handle_reconfig(&self.node, &self.runtime, payload),
            "ReconfigurationStatusRequest" => handle_reconfig_status(self, now_millis, payload),
            _ => {
                tracing::warn!("Unknown envelope type: {}", envelope_type);
                vec![]
            }
        }
    }
}

// -- Core Raft RPCs --

fn handle_vote(node: &Arc<parking_lot::Mutex<RaftNode>>, payload: &[u8]) -> Vec<u8> {
    let req = match raft::VoteRequest::decode(payload) {
        Ok(r) => r,
        _ => return vec![],
    };
    let mut n = node.lock();
    let r = n.handle_vote_request(VoteRequest {
        term: req.term as u64,
        candidate_id: req.candidate_id,
        last_log_index: req.last_log_index as u64,
        last_log_term: req.last_log_term as u64,
    });
    raft::VoteResponse {
        peer_id: r.peer_id,
        term: r.term as i64,
        vote_granted: r.vote_granted,
        current_term: r.current_term as i64,
    }
    .encode_to_vec()
}

fn handle_append(node: &Arc<parking_lot::Mutex<RaftNode>>, payload: &[u8]) -> Vec<u8> {
    let req = match raft::AppendEntriesRequest::decode(payload) {
        Ok(r) => r,
        _ => return vec![],
    };
    let entries = req
        .entries
        .into_iter()
        .map(|e| LogEntry::new(e.term as u64, e.peer_id, e.data))
        .collect();
    let mut n = node.lock();
    let r = n.handle_append_entries_request(AppendEntriesRequest {
        term: req.term as u64,
        leader_id: req.leader_id,
        prev_log_index: req.prev_log_index as u64,
        prev_log_term: req.prev_log_term as u64,
        leader_commit: req.leader_commit as u64,
        entries,
    });
    raft::AppendEntriesResponse {
        term: r.term as i64,
        peer_id: r.peer_id,
        success: r.success,
        match_index: r.match_index,
    }
    .encode_to_vec()
}

fn handle_snapshot(node: &Arc<parking_lot::Mutex<RaftNode>>, payload: &[u8]) -> Vec<u8> {
    let req = match raft::InstallSnapshotRequest::decode(payload) {
        Ok(r) => r,
        _ => return vec![],
    };
    let mut n = node.lock();
    let r = n.accept_snapshot_chunk(InstallSnapshotRequest {
        term: req.term as u64,
        leader_id: req.leader_id,
        last_included_index: req.last_included_index as u64,
        last_included_term: req.last_included_term as u64,
        offset: req.offset as u64,
        data: req.snapshot_data,
        done: req.done,
    });
    raft::InstallSnapshotResponse {
        term: r.term as i64,
        peer_id: r.peer_id,
        success: r.success,
        last_included_index: r.last_included_index as i64,
    }
    .encode_to_vec()
}

// -- Client command (with auth + admission) --

async fn handle_client_cmd(handler: &RaftHandler, payload: &[u8]) -> Vec<u8> {
    let req = match raft::ClientCommandRequest::decode(payload) {
        Ok(r) => r,
        _ => return vec![],
    };
    let (is_leader, me_id, leader_id, current_term) = {
        let n = handler.node.lock();
        (
            n.state == NodeState::Leader,
            n.me.id.clone(),
            n.known_leader_id.clone(),
            n.current_term,
        )
    };

    // Authenticate
    let auth_ctx = crate::policy::AuthenticationContext {
        requester_id: req.peer_id.clone(),
        authentication_scheme: req.auth_scheme.clone(),
        authentication_token: req.auth_token.clone(),
        local_peer_id: me_id.clone(),
        is_leader,
        decommissioned: false,
    };
    let auth_result = handler.authenticator.authenticate(&auth_ctx);
    if !auth_result.authenticated {
        return raft::ClientCommandResponse {
            term: current_term as i64,
            peer_id: me_id,
            success: false,
            status: auth_result.status,
            message: auth_result.message,
            leader_id: leader_id.unwrap_or_default(),
            leader_host: String::new(),
            leader_port: 0,
            result: vec![],
        }
        .encode_to_vec();
    }

    // Authorize
    let authz_ctx = crate::policy::AuthorizationContext {
        principal: auth_result.principal.clone(),
        command: req.command.clone(),
    };
    let authz_result = handler.authorizer.authorize(&authz_ctx);
    if !authz_result.allowed {
        return raft::ClientCommandResponse {
            term: current_term as i64,
            peer_id: me_id,
            success: false,
            status: authz_result.status,
            message: authz_result.message,
            leader_id: leader_id.unwrap_or_default(),
            leader_host: String::new(),
            leader_port: 0,
            result: vec![],
        }
        .encode_to_vec();
    }

    // Admission
    let adm_ctx = AdmissionContext {
        local_peer_id: me_id.clone(),
        leader_id: leader_id.clone(),
        leader_host: String::new(),
        leader_port: 0,
        principal: auth_result.principal,
        decommissioned: false,
        is_leader,
    };
    let adm = handler.admission.evaluate(&adm_ctx);
    match adm.action {
        crate::policy::AdmissionAction::Reject => {
            return raft::ClientCommandResponse {
                term: current_term as i64,
                peer_id: me_id,
                success: false,
                status: adm.status,
                message: adm.message,
                leader_id: String::new(),
                leader_host: String::new(),
                leader_port: 0,
                result: vec![],
            }
            .encode_to_vec();
        }
        crate::policy::AdmissionAction::Redirect => {
            let leader = leader_id.clone().unwrap_or_default();
            return raft::ClientCommandResponse {
                term: current_term as i64,
                peer_id: me_id,
                success: false,
                status: "REDIRECT".to_string(),
                message: format!("Redirect to leader {}", leader),
                leader_id: leader,
                leader_host: String::new(),
                leader_port: 0,
                result: vec![],
            }
            .encode_to_vec();
        }
        crate::policy::AdmissionAction::Accept => {}
    }

    if !is_leader {
        return raft::ClientCommandResponse {
            term: current_term as i64,
            peer_id: me_id,
            success: false,
            status: "NOT_LEADER".to_string(),
            message: String::new(),
            leader_id: leader_id.unwrap_or_default(),
            leader_host: String::new(),
            leader_port: 0,
            result: vec![],
        }
        .encode_to_vec();
    }

    let submitted = {
        let mut n = handler.node.lock();
        let term = n.current_term;
        n.submit_command(req.command).map(|index| (index, term))
    };
    match submitted {
        Ok((log_index, submitted_term)) => {
            let committed = handler
                .runtime
                .wait_for_commit(log_index, submitted_term, std::time::Duration::from_secs(5))
                .await;
            let n = handler.node.lock();
            let committed = committed
                && n.state == NodeState::Leader
                && n.current_term == submitted_term
                && n.commit_index >= log_index
                && n.last_applied >= log_index;
            raft::ClientCommandResponse {
                term: n.current_term as i64,
                peer_id: n.me.id.clone(),
                success: true,
                status: if committed {
                    "ACCEPTED"
                } else {
                    "ACCEPTED_NOT_COMMITTED"
                }
                .to_string(),
                message: String::new(),
                leader_id: String::new(),
                leader_host: String::new(),
                leader_port: 0,
                result: n
                    .applied_command_results
                    .get(&log_index)
                    .cloned()
                    .unwrap_or_default(),
            }
            .encode_to_vec()
        }
        Err(e) => {
            let n = handler.node.lock();
            raft::ClientCommandResponse {
                term: n.current_term as i64,
                peer_id: n.me.id.clone(),
                success: false,
                status: "ERROR".to_string(),
                message: e,
                leader_id: String::new(),
                leader_host: String::new(),
                leader_port: 0,
                result: vec![],
            }
            .encode_to_vec()
        }
    }
}

// -- Client query (with read barrier) --

async fn handle_client_query(handler: &RaftHandler, payload: &[u8]) -> Vec<u8> {
    let req = match raft::ClientQueryRequest::decode(payload) {
        Ok(r) => r,
        _ => return vec![],
    };
    let (is_leader, current_term, leader_id, peer_id, can_serve) = {
        let n = handler.node.lock();
        (
            n.state == NodeState::Leader,
            n.current_term,
            n.known_leader_id.clone(),
            n.me.id.clone(),
            n.can_serve_linearizable_read(),
        )
    };
    if !is_leader {
        return raft::ClientQueryResponse {
            term: current_term as i64,
            peer_id,
            success: false,
            status: "NOT_LEADER".to_string(),
            message: String::new(),
            leader_id: leader_id.unwrap_or_default(),
            leader_host: String::new(),
            leader_port: 0,
            result: vec![],
        }
        .encode_to_vec();
    }
    if !can_serve && !handler.runtime.refresh_read_barrier().await {
        let n = handler.node.lock();
        return raft::ClientQueryResponse {
            term: n.current_term as i64,
            peer_id: n.me.id.clone(),
            success: false,
            status: "RETRY".to_string(),
            message: "Read barrier quorum unavailable".to_string(),
            leader_id: n.known_leader_id.clone().unwrap_or_default(),
            leader_host: String::new(),
            leader_port: 0,
            result: vec![],
        }
        .encode_to_vec();
    }
    let n = handler.node.lock();
    if n.state != NodeState::Leader
        || n.current_term != current_term
        || n.decommissioned
        || n.joining
    {
        return raft::ClientQueryResponse {
            term: n.current_term as i64,
            peer_id: n.me.id.clone(),
            success: false,
            status: "RETRY".into(),
            message: "Leadership changed during read".into(),
            leader_id: n.known_leader_id.clone().unwrap_or_default(),
            leader_host: String::new(),
            leader_port: 0,
            result: vec![],
        }
        .encode_to_vec();
    }
    if let Some(ref sm) = n.state_machine {
        if let Some(qs) = sm.as_queryable() {
            let result = qs.query(&req.query);
            return raft::ClientQueryResponse {
                term: n.current_term as i64,
                peer_id: n.me.id.clone(),
                success: true,
                status: "OK".to_string(),
                message: String::new(),
                leader_id: String::new(),
                leader_host: String::new(),
                leader_port: 0,
                result,
            }
            .encode_to_vec();
        }
    }
    raft::ClientQueryResponse {
        term: n.current_term as i64,
        peer_id: n.me.id.clone(),
        success: false,
        status: "UNSUPPORTED".to_string(),
        message: String::new(),
        leader_id: String::new(),
        leader_host: String::new(),
        leader_port: 0,
        result: vec![],
    }
    .encode_to_vec()
}

// -- Operational RPCs (with rate limiting) --

fn handle_telemetry_req(handler: &RaftHandler, now_millis: i64, payload: &[u8]) -> Vec<u8> {
    let req = match raft::TelemetryRequest::decode(payload) {
        Ok(r) => r,
        _ => return vec![],
    };
    if !handler.telemetry_rate_limiter.check(&req.peer_id) {
        return empty_telemetry(now_millis, "RATE_LIMITED");
    }
    let n = handler.node.lock();
    let voters = n.cluster_configuration.current_voting_members().len() as i32;
    let is_stuck = handler.stuck_detector.is_stuck(&n);
    let reconfig_age = n
        .configuration_transition_started
        .map(|s| s.elapsed().as_millis() as i64)
        .unwrap_or(0);
    raft::TelemetryResponse {
        observed_at_millis: now_millis,
        term: n.current_term as i64,
        peer_id: n.me.id.clone(),
        success: true,
        status: "OK".to_string(),
        redirect_leader_id: String::new(),
        state: format!("{:?}", n.state).to_uppercase(),
        leader_id: n.known_leader_id.clone().unwrap_or_default(),
        voted_for: n.voted_for.clone().unwrap_or_default(),
        joining: n.joining,
        decommissioned: n.decommissioned,
        commit_index: n.commit_index as i64,
        last_applied: n.last_applied as i64,
        last_log_index: n.log_store.last_index() as i64,
        last_log_term: n.log_store.last_term() as i64,
        snapshot_index: n.log_store.snapshot_index() as i64,
        snapshot_term: n.log_store.snapshot_term() as i64,
        last_heartbeat_millis: 0,
        next_election_deadline_millis: 0,
        joint_consensus: n.cluster_configuration.is_joint_consensus(),
        current_members: specs_from_config(&n, true),
        next_members: specs_from_config(&n, false),
        known_peers: known_specs(&n),
        pending_join_ids: n.pending_join_ids.iter().cloned().collect(),
        replication: repl_status(&n),
        peer_stats: vec![],
        cluster_health: if is_stuck { "DEGRADED" } else { "OK" }.to_string(),
        cluster_status_reason: if is_stuck {
            "Joint consensus transition has not progressed"
        } else {
            ""
        }
        .to_string(),
        quorum_available: true,
        current_quorum_available: true,
        next_quorum_available: !n.cluster_configuration.is_joint_consensus(),
        voting_members: voters,
        healthy_voting_members: voters,
        reachable_voting_members: 0,
        reconfiguration_age_millis: reconfig_age,
        blocking_current_quorum_peer_ids: vec![],
        blocking_next_quorum_peer_ids: vec![],
        cluster_members: member_vec(&n),
    }
    .encode_to_vec()
}

fn handle_summary_req(handler: &RaftHandler, now_millis: i64, payload: &[u8]) -> Vec<u8> {
    let req = match raft::ClusterSummaryRequest::decode(payload) {
        Ok(r) => r,
        _ => return vec![],
    };
    if !handler.telemetry_rate_limiter.check(&req.peer_id) {
        return empty_summary(now_millis);
    }
    let n = handler.node.lock();
    let voters = n.cluster_configuration.current_voting_members().len() as i32;
    raft::ClusterSummaryResponse {
        observed_at_millis: now_millis,
        term: n.current_term as i64,
        peer_id: n.me.id.clone(),
        success: true,
        status: "OK".to_string(),
        redirect_leader_id: String::new(),
        redirect_leader_host: String::new(),
        redirect_leader_port: 0,
        state: format!("{:?}", n.state).to_uppercase(),
        leader_id: n.known_leader_id.clone().unwrap_or_default(),
        joint_consensus: n.cluster_configuration.is_joint_consensus(),
        cluster_health: "OK".to_string(),
        cluster_status_reason: String::new(),
        quorum_available: true,
        current_quorum_available: true,
        next_quorum_available: !n.cluster_configuration.is_joint_consensus(),
        voting_members: voters,
        healthy_voting_members: voters,
        reachable_voting_members: 0,
        reconfiguration_age_millis: 0,
        blocking_current_quorum_peer_ids: vec![],
        blocking_next_quorum_peer_ids: vec![],
        members: member_vec(&n),
    }
    .encode_to_vec()
}

fn empty_telemetry(now_millis: i64, status: &str) -> Vec<u8> {
    raft::TelemetryResponse {
        observed_at_millis: now_millis,
        term: 0,
        peer_id: String::new(),
        success: false,
        status: status.to_string(),
        redirect_leader_id: String::new(),
        state: String::new(),
        leader_id: String::new(),
        voted_for: String::new(),
        joining: false,
        decommissioned: false,
        commit_index: 0,
        last_applied: 0,
        last_log_index: 0,
        last_log_term: 0,
        snapshot_index: 0,
        snapshot_term: 0,
        last_heartbeat_millis: 0,
        next_election_deadline_millis: 0,
        joint_consensus: false,
        current_members: vec![],
        next_members: vec![],
        known_peers: vec![],
        pending_join_ids: vec![],
        replication: vec![],
        peer_stats: vec![],
        cluster_health: String::new(),
        cluster_status_reason: String::new(),
        quorum_available: false,
        current_quorum_available: false,
        next_quorum_available: false,
        voting_members: 0,
        healthy_voting_members: 0,
        reachable_voting_members: 0,
        reconfiguration_age_millis: 0,
        blocking_current_quorum_peer_ids: vec![],
        blocking_next_quorum_peer_ids: vec![],
        cluster_members: vec![],
    }
    .encode_to_vec()
}

fn empty_summary(now_millis: i64) -> Vec<u8> {
    raft::ClusterSummaryResponse {
        observed_at_millis: now_millis,
        term: 0,
        peer_id: String::new(),
        success: false,
        status: "RATE_LIMITED".to_string(),
        redirect_leader_id: String::new(),
        redirect_leader_host: String::new(),
        redirect_leader_port: 0,
        state: String::new(),
        leader_id: String::new(),
        joint_consensus: false,
        cluster_health: String::new(),
        cluster_status_reason: String::new(),
        quorum_available: false,
        current_quorum_available: false,
        next_quorum_available: false,
        voting_members: 0,
        healthy_voting_members: 0,
        reachable_voting_members: 0,
        reconfiguration_age_millis: 0,
        blocking_current_quorum_peer_ids: vec![],
        blocking_next_quorum_peer_ids: vec![],
        members: vec![],
    }
    .encode_to_vec()
}

// -- Membership RPCs --

fn handle_join(
    node: &Arc<parking_lot::Mutex<RaftNode>>,
    runtime: &Arc<RaftRuntime>,
    payload: &[u8],
) -> Vec<u8> {
    let req = match raft::JoinClusterRequest::decode(payload) {
        Ok(r) => r,
        _ => return vec![],
    };
    let (is_leader, current_term, leader_id, peer_id) = {
        let n = node.lock();
        (
            n.state == NodeState::Leader,
            n.current_term,
            n.known_leader_id.clone(),
            n.me.id.clone(),
        )
    };
    if !is_leader {
        return raft::JoinClusterResponse {
            term: current_term as i64,
            peer_id,
            success: false,
            status: "NOT_LEADER".to_string(),
            message: String::new(),
            leader_id: leader_id.unwrap_or_default(),
        }
        .encode_to_vec();
    }
    let join_spec = raft::PeerSpec {
        id: req.joining_peer_id,
        host: req.host,
        port: req.port,
        role: req.role,
    };
    let cmd = crate::membership::encode_join_command(&join_spec);
    match runtime.submit_command(cmd) {
        Ok(_) => {
            let n = node.lock();
            raft::JoinClusterResponse {
                term: n.current_term as i64,
                peer_id: n.me.id.clone(),
                success: true,
                status: "ACCEPTED".to_string(),
                message: String::new(),
                leader_id: n.me.id.clone(),
            }
            .encode_to_vec()
        }
        Err(e) => {
            let n = node.lock();
            raft::JoinClusterResponse {
                term: n.current_term as i64,
                peer_id: n.me.id.clone(),
                success: false,
                status: "ERROR".to_string(),
                message: e,
                leader_id: n.me.id.clone(),
            }
            .encode_to_vec()
        }
    }
}

fn handle_join_status(node: &Arc<parking_lot::Mutex<RaftNode>>, payload: &[u8]) -> Vec<u8> {
    let req = match raft::JoinClusterStatusRequest::decode(payload) {
        Ok(r) => r,
        _ => return vec![],
    };
    let n = node.lock();
    let s = if n.pending_join_ids.contains(&req.target_peer_id) {
        "PENDING"
    } else if n.cluster_configuration.contains(&req.target_peer_id) {
        if n.cluster_configuration.is_joint_consensus() {
            "IN_JOINT_CONSENSUS"
        } else {
            "COMPLETED"
        }
    } else {
        "UNKNOWN"
    };
    raft::JoinClusterStatusResponse {
        term: n.current_term as i64,
        peer_id: n.me.id.clone(),
        success: true,
        status: s.to_string(),
        message: String::new(),
        leader_id: n.known_leader_id.clone().unwrap_or_default(),
    }
    .encode_to_vec()
}

fn handle_reconfig(
    node: &Arc<parking_lot::Mutex<RaftNode>>,
    runtime: &Arc<RaftRuntime>,
    payload: &[u8],
) -> Vec<u8> {
    let req = match raft::ReconfigureClusterRequest::decode(payload) {
        Ok(r) => r,
        _ => return vec![],
    };
    let (is_leader, current_term, leader_id, peer_id) = {
        let n = node.lock();
        (
            n.state == NodeState::Leader,
            n.current_term,
            n.known_leader_id.clone(),
            n.me.id.clone(),
        )
    };
    if !is_leader {
        return raft::ReconfigureClusterResponse {
            term: current_term as i64,
            peer_id,
            success: false,
            status: "NOT_LEADER".to_string(),
            message: String::new(),
            leader_id: leader_id.unwrap_or_default(),
        }
        .encode_to_vec();
    }
    if !req.action.eq_ignore_ascii_case("FINALIZE")
        && node.lock().active_configuration().is_joint_consensus()
    {
        return raft::ReconfigureClusterResponse {
            term: current_term as i64,
            peer_id,
            success: false,
            status: "REJECTED".into(),
            message: "reconfiguration already active".into(),
            leader_id: leader_id.unwrap_or_default(),
        }
        .encode_to_vec();
    }
    let cmd = match req.action.to_uppercase().as_str() {
        "JOINT" => crate::membership::encode_joint_command(&req.members),
        "FINALIZE" => {
            let n = node.lock();
            if !n.cluster_configuration.is_joint_consensus() {
                return raft::ReconfigureClusterResponse {
                    term: n.current_term as i64,
                    peer_id: n.me.id.clone(),
                    success: true,
                    status: "ACCEPTED".to_string(),
                    message: "No active joint consensus to finalize".to_string(),
                    leader_id: n.me.id.clone(),
                }
                .encode_to_vec();
            }
            crate::membership::encode_finalize_command()
        }
        "PROMOTE" | "DEMOTE" => {
            let n = node.lock();
            if req.members.len() != 1 || !n.known_peers.contains_key(&req.members[0].id) {
                return raft::ReconfigureClusterResponse {
                    term: n.current_term as i64,
                    peer_id: n.me.id.clone(),
                    success: false,
                    status: "INVALID".into(),
                    message: "role change requires exactly one known member".into(),
                    leader_id: n.me.id.clone(),
                }
                .encode_to_vec();
            }
            let mut requested = req.members.clone();
            requested[0].role = if req.action.eq_ignore_ascii_case("PROMOTE") {
                "VOTER"
            } else {
                "LEARNER"
            }
            .into();
            let mut members: Vec<raft::PeerSpec> = Vec::new();
            for p in n.cluster_configuration.current_members().iter() {
                if let Some(ps) = requested.iter().find(|m| m.id == p.id) {
                    members.push(raft::PeerSpec {
                        id: ps.id.clone(),
                        host: ps.host.clone(),
                        port: ps.port,
                        role: ps.role.clone(),
                    });
                } else {
                    members.push(raft::PeerSpec {
                        id: p.id.clone(),
                        host: p.address.ip().to_string(),
                        port: p.address.port() as i32,
                        role: if p.is_voter() { "VOTER" } else { "LEARNER" }.to_string(),
                    });
                }
            }
            for ps in &requested {
                if !n.cluster_configuration.contains(&ps.id) {
                    members.push(raft::PeerSpec {
                        id: ps.id.clone(),
                        host: ps.host.clone(),
                        port: ps.port,
                        role: ps.role.clone(),
                    });
                }
            }
            crate::membership::encode_joint_command(&members)
        }
        _ => {
            return raft::ReconfigureClusterResponse {
                term: current_term as i64,
                peer_id,
                success: false,
                status: "INVALID".into(),
                message: "unknown reconfiguration action".into(),
                leader_id: leader_id.unwrap_or_default(),
            }
            .encode_to_vec()
        }
    };
    match runtime.submit_command(cmd) {
        Ok(_) => {
            let n = node.lock();
            raft::ReconfigureClusterResponse {
                term: n.current_term as i64,
                peer_id: n.me.id.clone(),
                success: true,
                status: "ACCEPTED".to_string(),
                message: String::new(),
                leader_id: n.me.id.clone(),
            }
            .encode_to_vec()
        }
        Err(e) => {
            let n = node.lock();
            raft::ReconfigureClusterResponse {
                term: n.current_term as i64,
                peer_id: n.me.id.clone(),
                success: false,
                status: "ERROR".to_string(),
                message: e,
                leader_id: n.me.id.clone(),
            }
            .encode_to_vec()
        }
    }
}

fn handle_reconfig_status(handler: &RaftHandler, now_millis: i64, payload: &[u8]) -> Vec<u8> {
    let req = match raft::ReconfigurationStatusRequest::decode(payload) {
        Ok(r) => r,
        _ => return vec![],
    };
    if !handler.telemetry_rate_limiter.check(&req.peer_id) {
        return empty_reconfig_status(now_millis, "RATE_LIMITED");
    }
    let (is_leader, current_term, peer_id, leader_id, state_str) = {
        let n = handler.node.lock();
        (
            n.state == NodeState::Leader,
            n.current_term,
            n.me.id.clone(),
            n.known_leader_id.clone().unwrap_or_default(),
            format!("{:?}", n.state).to_uppercase(),
        )
    };
    if !is_leader {
        let redirect_id = if leader_id.is_empty() {
            String::new()
        } else {
            leader_id.clone()
        };
        let (redirect_host, redirect_port) = redirect_id
            .is_empty()
            .then(|| (String::new(), 0))
            .unwrap_or_else(|| {
                handler
                    .client
                    .peer_addr(&redirect_id)
                    .map(|a| (a.ip().to_string(), a.port() as i32))
                    .unwrap_or((String::new(), 0))
            });
        return raft::ReconfigurationStatusResponse {
            observed_at_millis: now_millis,
            term: current_term as i64,
            peer_id,
            success: false,
            status: if redirect_id.is_empty() {
                "NO_LEADER"
            } else {
                "REDIRECT"
            }
            .to_string(),
            redirect_leader_id: redirect_id,
            redirect_leader_host: redirect_host,
            redirect_leader_port: redirect_port,
            state: state_str,
            leader_id,
            reconfiguration_active: false,
            joint_consensus: false,
            reconfiguration_age_millis: 0,
            cluster_health: String::new(),
            cluster_status_reason: String::new(),
            quorum_available: false,
            current_quorum_available: false,
            next_quorum_available: false,
            blocking_current_quorum_peer_ids: vec![],
            blocking_next_quorum_peer_ids: vec![],
            members: vec![],
        }
        .encode_to_vec();
    }
    let n = handler.node.lock();
    let is_stuck = handler.stuck_detector.is_stuck(&n);
    let joint = n.cluster_configuration.is_joint_consensus();
    let reconfig_age = n
        .configuration_transition_started
        .map(|s| s.elapsed().as_millis() as i64)
        .unwrap_or(0);
    raft::ReconfigurationStatusResponse {
        observed_at_millis: now_millis,
        term: n.current_term as i64,
        peer_id: n.me.id.clone(),
        success: true,
        status: "OK".to_string(),
        redirect_leader_id: String::new(),
        redirect_leader_host: String::new(),
        redirect_leader_port: 0,
        state: state_str,
        leader_id: leader_id.clone(),
        reconfiguration_active: joint,
        joint_consensus: joint,
        reconfiguration_age_millis: reconfig_age,
        cluster_health: if is_stuck { "DEGRADED" } else { "OK" }.to_string(),
        cluster_status_reason: if is_stuck {
            "Joint consensus transition has not progressed"
        } else {
            ""
        }
        .to_string(),
        quorum_available: true,
        current_quorum_available: true,
        next_quorum_available: !joint,
        blocking_current_quorum_peer_ids: vec![],
        blocking_next_quorum_peer_ids: vec![],
        members: member_vec(&n),
    }
    .encode_to_vec()
}

fn empty_reconfig_status(now_millis: i64, status: &str) -> Vec<u8> {
    raft::ReconfigurationStatusResponse {
        observed_at_millis: now_millis,
        term: 0,
        peer_id: String::new(),
        success: false,
        status: status.to_string(),
        redirect_leader_id: String::new(),
        redirect_leader_host: String::new(),
        redirect_leader_port: 0,
        state: String::new(),
        leader_id: String::new(),
        reconfiguration_active: false,
        joint_consensus: false,
        reconfiguration_age_millis: 0,
        cluster_health: String::new(),
        cluster_status_reason: String::new(),
        quorum_available: false,
        current_quorum_available: false,
        next_quorum_available: false,
        blocking_current_quorum_peer_ids: vec![],
        blocking_next_quorum_peer_ids: vec![],
        members: vec![],
    }
    .encode_to_vec()
}

// -- Shared helpers --

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn specs_from_config(n: &parking_lot::MutexGuard<RaftNode>, current: bool) -> Vec<raft::PeerSpec> {
    let members = if current {
        n.cluster_configuration.current_members()
    } else {
        n.cluster_configuration.next_members()
    };
    members
        .into_iter()
        .map(|p| raft::PeerSpec {
            id: p.id.clone(),
            host: p.address.ip().to_string(),
            port: p.address.port() as i32,
            role: if p.is_voter() { "VOTER" } else { "LEARNER" }.to_string(),
        })
        .collect()
}

fn known_specs(n: &parking_lot::MutexGuard<RaftNode>) -> Vec<raft::PeerSpec> {
    n.known_peers
        .values()
        .map(|p| raft::PeerSpec {
            id: p.id.clone(),
            host: p.address.ip().to_string(),
            port: p.address.port() as i32,
            role: if p.is_voter() { "VOTER" } else { "LEARNER" }.to_string(),
        })
        .collect()
}

fn repl_status(n: &parking_lot::MutexGuard<RaftNode>) -> Vec<raft::TelemetryReplicationStatus> {
    n.match_index
        .iter()
        .map(|(id, mi)| raft::TelemetryReplicationStatus {
            peer_id: id.clone(),
            next_index: n.next_index.get(id).copied().unwrap_or(0) as i64,
            match_index: *mi as i64,
            reachable: n.last_follower_contact_millis.get(id).is_some(),
            last_successful_contact_millis: n
                .last_follower_contact_millis
                .get(id)
                .copied()
                .unwrap_or(0) as i64,
            consecutive_failures: n
                .consecutive_follower_failures
                .get(id)
                .copied()
                .unwrap_or(0) as i32,
            last_failed_contact_millis: n.last_follower_failure_millis.get(id).copied().unwrap_or(0)
                as i64,
        })
        .collect()
}

fn member_vec(n: &parking_lot::MutexGuard<RaftNode>) -> Vec<raft::ClusterMemberSummary> {
    n.known_peers
        .iter()
        .map(|(id, peer)| {
            let is_local = id == &n.me.id;
            let current = n
                .cluster_configuration
                .current_members()
                .into_iter()
                .find(|p| p.id == *id);
            let next = if n.cluster_configuration.is_joint_consensus() {
                n.cluster_configuration
                    .next_members()
                    .into_iter()
                    .find(|p| p.id == *id)
            } else {
                current
            };
            let role = |p: &graft_core::types::Peer| if p.is_voter() { "VOTER" } else { "LEARNER" };
            let current_role = current.map(role).unwrap_or("");
            let next_role = next.map(role).unwrap_or("");
            let transition = match (current_role, next_role) {
                ("", "") => "known",
                ("", _) => "joining",
                (_, "") => "leaving",
                ("LEARNER", "VOTER") => "promoting",
                ("VOTER", "LEARNER") => "demoting",
                _ => "steady",
            };
            let is_voter = n.cluster_configuration.is_voter(id);
            let mi = if is_local {
                n.log_store.last_index()
            } else {
                n.match_index.get(id).copied().unwrap_or(0)
            };
            let ni = if is_local {
                mi + 1
            } else {
                n.next_index.get(id).copied().unwrap_or(0)
            };
            let lag = n.log_store.last_index().saturating_sub(mi);
            let reachable = is_local || n.last_follower_contact_millis.contains_key(id);
            raft::ClusterMemberSummary {
                peer_id: id.clone(),
                local: is_local,
                current_member: current.is_some(),
                next_member: next.is_some(),
                voting: is_voter,
                role: next
                    .or(current)
                    .map(role)
                    .unwrap_or_else(|| role(peer))
                    .into(),
                current_role: current_role.into(),
                next_role: next_role.into(),
                role_transition: transition.into(),
                transition_age_millis: 0,
                blocking_quorums: String::new(),
                blocking_reason: String::new(),
                reachable,
                freshness: if reachable { "current" } else { "unknown" }.into(),
                health: if !reachable {
                    "unreachable"
                } else if lag > 0 {
                    "lagging"
                } else {
                    "healthy"
                }
                .into(),
                next_index: ni as i64,
                match_index: mi as i64,
                consecutive_failures: n
                    .consecutive_follower_failures
                    .get(id)
                    .copied()
                    .unwrap_or(0) as i32,
                lag: lag as i64,
                last_failed_contact_millis: n
                    .last_follower_failure_millis
                    .get(id)
                    .copied()
                    .unwrap_or(0) as i64,
                last_successful_contact_millis: n
                    .last_follower_contact_millis
                    .get(id)
                    .copied()
                    .unwrap_or(0) as i64,
            }
        })
        .collect()
}

#[cfg(test)]
mod membership_tests {
    use super::*;
    use graft_core::{
        membership::ClusterConfiguration,
        types::{Peer, Role},
    };
    use graft_storage::log_store::{InMemoryLogStore, InMemoryPersistentStateStore};

    fn fixture() -> (Arc<parking_lot::Mutex<RaftNode>>, Arc<RaftRuntime>) {
        let members: Vec<_> = ["a", "b", "c", "d"]
            .into_iter()
            .map(|id| {
                Peer::new(
                    id.into(),
                    "127.0.0.1:1234".parse().unwrap(),
                    if id == "d" {
                        Role::Learner
                    } else {
                        Role::Voter
                    },
                )
            })
            .collect();
        let mut n = RaftNode::new(
            members[0].clone(),
            1500,
            Arc::new(InMemoryLogStore::new()),
            Arc::new(InMemoryPersistentStateStore::new()),
            None,
            ClusterConfiguration::stable(members),
            100,
            1024,
        );
        n.current_term = 1;
        n.state = NodeState::Leader;
        let node = Arc::new(parking_lot::Mutex::new(n));
        let runtime = Arc::new(RaftRuntime::new(node.clone(), Arc::new(RaftClient::new())));
        (node, runtime)
    }

    #[test]
    fn role_actions_override_the_callers_existing_role() {
        for (action, target, supplied_role, desired_role) in [
            ("PROMOTE", "d", "LEARNER", "VOTER"),
            ("DEMOTE", "b", "VOTER", "LEARNER"),
        ] {
            let (node, runtime) = fixture();
            let req = raft::ReconfigureClusterRequest {
                action: action.into(),
                members: vec![raft::PeerSpec {
                    id: target.into(),
                    host: "127.0.0.1".into(),
                    port: 1234,
                    role: supplied_role.into(),
                }],
                ..Default::default()
            };
            let response = raft::ReconfigureClusterResponse::decode(
                handle_reconfig(&node, &runtime, &req.encode_to_vec()).as_slice(),
            )
            .unwrap();
            assert!(response.success);
            let n = node.lock();
            let entry = n.log_store.entry_at(n.log_store.last_index()).unwrap();
            let command = raft::InternalRaftCommand::decode(entry.data.as_slice()).unwrap();
            let Some(raft::internal_raft_command::Command::Joint(joint)) = command.command else {
                panic!("not a joint entry")
            };
            assert_eq!(
                joint.members.iter().find(|m| m.id == target).unwrap().role,
                desired_role
            );
            assert_eq!(joint.members.len(), 4);
        }
    }

    #[test]
    fn invalid_action_does_not_append_a_joint_entry() {
        let (node, runtime) = fixture();
        let request = raft::ReconfigureClusterRequest {
            action: "TYPO".into(),
            ..Default::default()
        };
        let response = raft::ReconfigureClusterResponse::decode(
            handle_reconfig(&node, &runtime, &request.encode_to_vec()).as_slice(),
        )
        .unwrap();
        assert_eq!(response.status, "INVALID");
        assert_eq!(node.lock().log_store.last_index(), 0);
    }

    #[test]
    fn member_status_preserves_both_roles_during_promotion() {
        let (node, _) = fixture();
        let mut n = node.lock();
        let mut next: Vec<_> = n
            .cluster_configuration
            .current_members()
            .into_iter()
            .cloned()
            .collect();
        next.iter_mut().find(|p| p.id == "d").unwrap().role = Role::Voter;
        n.cluster_configuration = n.cluster_configuration.transition_to(next);
        let members = member_vec(&n);
        let d = members.iter().find(|m| m.peer_id == "d").unwrap();
        assert_eq!(
            (&*d.current_role, &*d.next_role, &*d.role_transition),
            ("LEARNER", "VOTER", "promoting")
        );
        assert_eq!(
            members.iter().find(|m| m.peer_id == "a").unwrap().health,
            "healthy"
        );
    }

    #[test]
    fn committed_join_is_a_replication_target_before_promotion() {
        let (node, _) = fixture();
        let mut n = node.lock();
        n.state = NodeState::Follower;
        let command = crate::membership::encode_join_command(&raft::PeerSpec {
            id: "e".into(),
            host: "127.0.0.1".into(),
            port: 1235,
            role: "LEARNER".into(),
        });
        assert!(
            n.handle_append_entries_request(AppendEntriesRequest {
                term: 1,
                leader_id: "b".into(),
                prev_log_index: 0,
                prev_log_term: 0,
                leader_commit: 1,
                entries: vec![LogEntry::new(1, "b".into(), command)]
            })
            .success
        );
        assert!(n.pending_join_ids.contains("e"));
        assert_eq!(n.next_index.get("e"), Some(&1));
        assert_eq!(n.match_index.get("e"), Some(&0));
        assert!(!n.cluster_configuration.is_voter("e"));
    }
}
