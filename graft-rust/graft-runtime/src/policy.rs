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

//! Security and operational policy hooks for the Raft runtime.
//!
//! Mirrors the Java `raft-runtime` policy interfaces:
//!
//! - `Authenticator` — authenticates a client request and returns a principal.
//! - `Authorizer` — authorizes a principal to execute a command.
//! - `AdmissionPolicy` — decides whether a client write should be admitted
//!   (accept, redirect to leader, or reject).
//! - `RateLimiter` — throttles telemetry/cluster-summary requests per requester.
//! - `StuckDetector` — detects prolonged joint-consensus transitions.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use graft_core::raft_node::RaftNode;

// ---------------------------------------------------------------------------
// AuthenticatedPrincipal
// ---------------------------------------------------------------------------

/// An identity established by authentication. Carried through the request
/// pipeline so the authorizer and admission policy can make decisions.
#[derive(Debug, Clone)]
pub struct AuthenticatedPrincipal {
    /// Stable identifier for the authenticated entity (user, service, etc.).
    pub principal_id: String,
    /// The authentication scheme used (e.g., "shared-secret", "token").
    pub authentication_scheme: String,
}

// ---------------------------------------------------------------------------
// Authentication
// ---------------------------------------------------------------------------

/// Context for authenticating a client request. Mirrors the Java
/// `ClientCommandAuthenticationContext`.
#[derive(Debug, Clone)]
pub struct AuthenticationContext {
    /// The peer id claimed by the requester.
    pub requester_id: String,
    /// The authentication scheme (e.g., "shared-secret").
    pub authentication_scheme: String,
    /// The authentication token/credential.
    pub authentication_token: String,
    /// This node's peer id.
    pub local_peer_id: String,
    /// Whether this node is currently the leader.
    pub is_leader: bool,
    /// Whether this node is decommissioned.
    pub decommissioned: bool,
}

/// Result of authenticating a client request.
#[derive(Debug, Clone)]
pub struct AuthenticationResult {
    /// True if the requester was successfully authenticated.
    pub authenticated: bool,
    /// Status code for the response.
    pub status: String,
    /// Human-readable message.
    pub message: String,
    /// The authenticated principal, or None if authentication failed.
    pub principal: Option<AuthenticatedPrincipal>,
}

impl AuthenticationResult {
    pub fn authenticated(principal: AuthenticatedPrincipal) -> Self {
        Self {
            authenticated: true,
            status: "AUTHENTICATED".to_string(),
            message: "Request authenticated".to_string(),
            principal: Some(principal),
        }
    }

    pub fn reject(status: &str, message: &str) -> Self {
        Self {
            authenticated: false,
            status: status.to_string(),
            message: message.to_string(),
            principal: None,
        }
    }
}

/// Authenticates client requests. The runtime calls this before processing
/// any client command, query, or administration RPC.
///
/// The default implementation (`AllowAllAuthenticator`) accepts all requests
/// with an anonymous principal.
pub trait Authenticator: Send + Sync {
    fn authenticate(&self, context: &AuthenticationContext) -> AuthenticationResult;
}

/// An authenticator that accepts all requests unconditionally. Used when
/// no authentication is configured.
pub struct AllowAllAuthenticator;

impl Authenticator for AllowAllAuthenticator {
    fn authenticate(&self, _context: &AuthenticationContext) -> AuthenticationResult {
        AuthenticationResult::authenticated(AuthenticatedPrincipal {
            principal_id: "anonymous".to_string(),
            authentication_scheme: "none".to_string(),
        })
    }
}

/// Authenticates requests using a pre-shared secret token. If the
/// provided token matches the configured secret, the requester is
/// authenticated with the given principal id.
pub struct SharedSecretAuthenticator {
    secret: String,
}

impl SharedSecretAuthenticator {
    pub fn new(secret: String) -> Self {
        Self { secret }
    }
}

impl Authenticator for SharedSecretAuthenticator {
    fn authenticate(&self, context: &AuthenticationContext) -> AuthenticationResult {
        if context.authentication_token == self.secret {
            AuthenticationResult::authenticated(AuthenticatedPrincipal {
                principal_id: context.requester_id.clone(),
                authentication_scheme: context.authentication_scheme.clone(),
            })
        } else {
            AuthenticationResult::reject("UNAUTHENTICATED", "Invalid shared secret")
        }
    }
}

// ---------------------------------------------------------------------------
// Authorization
// ---------------------------------------------------------------------------

/// Context for authorizing a command.
#[derive(Debug, Clone)]
pub struct AuthorizationContext {
    /// The authenticated principal (None if auth was skipped).
    pub principal: Option<AuthenticatedPrincipal>,
    /// The raw command bytes.
    pub command: Vec<u8>,
}

/// Result of authorizing a command.
#[derive(Debug, Clone)]
pub struct AuthorizationResult {
    /// True if the command is authorized.
    pub allowed: bool,
    /// Status code for the response.
    pub status: String,
    /// Human-readable message.
    pub message: String,
    /// The principal that was authorized (may be modified).
    pub principal: Option<AuthenticatedPrincipal>,
}

impl AuthorizationResult {
    pub fn allow(principal: Option<AuthenticatedPrincipal>) -> Self {
        Self {
            allowed: true,
            status: "AUTHORIZED".to_string(),
            message: "Command authorized".to_string(),
            principal,
        }
    }

    pub fn deny(status: &str, message: &str) -> Self {
        Self {
            allowed: false,
            status: status.to_string(),
            message: message.to_string(),
            principal: None,
        }
    }
}

/// Authorizes client commands against a principal. Called after
/// authentication and before admission.
pub trait Authorizer: Send + Sync {
    fn authorize(&self, context: &AuthorizationContext) -> AuthorizationResult;
}

/// An authorizer that allows all commands.
pub struct AllowAllAuthorizer;

impl Authorizer for AllowAllAuthorizer {
    fn authorize(&self, _context: &AuthorizationContext) -> AuthorizationResult {
        AuthorizationResult::allow(None)
    }
}

// ---------------------------------------------------------------------------
// Write Admission
// ---------------------------------------------------------------------------

/// Context for evaluating whether a client write should be admitted.
#[derive(Debug, Clone)]
pub struct AdmissionContext {
    /// This node's peer id.
    pub local_peer_id: String,
    /// The current leader id (None if unknown).
    pub leader_id: Option<String>,
    /// The leader's host.
    pub leader_host: String,
    /// The leader's port.
    pub leader_port: u16,
    /// The authenticated principal (None if auth was skipped).
    pub principal: Option<AuthenticatedPrincipal>,
    /// Whether this node is decommissioned.
    pub decommissioned: bool,
    /// Whether this node is currently the leader.
    pub is_leader: bool,
}

/// Decision returned by an admission policy.
#[derive(Debug, Clone)]
pub struct AdmissionDecision {
    pub action: AdmissionAction,
    pub status: String,
    pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionAction {
    /// Accept the write and submit it to the log.
    Accept,
    /// Redirect the client to the current leader.
    Redirect,
    /// Reject the write (e.g., node is decommissioned).
    Reject,
}

impl AdmissionDecision {
    pub fn accept() -> Self {
        Self {
            action: AdmissionAction::Accept,
            status: "ADMITTED".to_string(),
            message: String::new(),
        }
    }
    pub fn redirect(status: &str, message: &str) -> Self {
        Self {
            action: AdmissionAction::Redirect,
            status: status.to_string(),
            message: message.to_string(),
        }
    }
    pub fn reject(status: &str, message: &str) -> Self {
        Self {
            action: AdmissionAction::Reject,
            status: status.to_string(),
            message: message.to_string(),
        }
    }
}

/// Decides whether a client write should be admitted.
///
/// Called after authentication and authorization, before the command is
/// submitted to the Raft log. A follower can use this to redirect the
/// client to the leader, or a decommissioned node can reject writes.
pub trait AdmissionPolicy: Send + Sync {
    fn evaluate(&self, context: &AdmissionContext) -> AdmissionDecision;
}

/// Accepts all writes unconditionally.
pub struct AcceptAllAdmissionPolicy;

impl AdmissionPolicy for AcceptAllAdmissionPolicy {
    fn evaluate(&self, _context: &AdmissionContext) -> AdmissionDecision {
        AdmissionDecision::accept()
    }
}

/// Redirects all writes to the current leader. Useful for read-only
/// nodes or passive followers that should not accept writes locally.
pub struct LeaderRedirectAdmissionPolicy;

impl AdmissionPolicy for LeaderRedirectAdmissionPolicy {
    fn evaluate(&self, context: &AdmissionContext) -> AdmissionDecision {
        if context.is_leader {
            AdmissionDecision::accept()
        } else if let Some(ref leader) = context.leader_id {
            AdmissionDecision::redirect("REDIRECT", &format!("Redirect to leader {}", leader))
        } else {
            AdmissionDecision::reject("UNKNOWN_LEADER", "No known leader to redirect to")
        }
    }
}

// ---------------------------------------------------------------------------
// Rate Limiter (telemetry / cluster-summary)
// ---------------------------------------------------------------------------

/// Tracks per-requester request timestamps and enforces a configurable
/// rate limit. Requests exceeding the limit are rejected.
///
/// The default limit is 30 requests per minute (matching the Java
/// `RAFT_TELEMETRY_RATE_LIMIT_PER_MINUTE`).
pub struct RateLimiter {
    /// Per-requester ring buffer of recent request timestamps.
    windows: Mutex<HashMap<String, Vec<Instant>>>,
    /// Maximum requests allowed per minute.
    rate_per_minute: u32,
}

impl RateLimiter {
    pub fn new(rate_per_minute: u32) -> Self {
        Self {
            windows: Mutex::new(HashMap::new()),
            rate_per_minute,
        }
    }

    /// Checks whether the given requester is allowed to make a request.
    /// Returns true if the request is within the rate limit.
    ///
    /// The window is a sliding 60-second ring: timestamps older than
    /// 60 seconds are pruned before the count is checked.
    pub fn check(&self, requester_id: &str) -> bool {
        let mut windows = self.windows.lock().unwrap();
        let now = Instant::now();
        let window = windows.entry(requester_id.to_string()).or_default();

        // Prune entries older than 60 seconds.
        window.retain(|t| now.duration_since(*t) < Duration::from_secs(60));

        if window.len() < self.rate_per_minute as usize {
            window.push(now);
            true
        } else {
            false
        }
    }
}

// ---------------------------------------------------------------------------
// Stuck Detector (reconfiguration health)
// ---------------------------------------------------------------------------

/// Monitors joint-consensus transitions and reports whether a
/// reconfiguration appears stuck (no progress for longer than
/// `stuck_threshold` millis).
pub struct StuckDetector {
    /// Millis after which a joint-consensus transition is considered stuck.
    stuck_threshold_millis: u64,
}

impl StuckDetector {
    pub fn new(stuck_threshold_millis: u64) -> Self {
        Self {
            stuck_threshold_millis,
        }
    }

    /// Returns true if the node is in joint consensus and the transition
    /// has been active longer than the stuck threshold.
    pub fn is_stuck(&self, node: &RaftNode) -> bool {
        if !node.cluster_configuration.is_joint_consensus() {
            return false;
        }
        if let Some(start) = node.configuration_transition_started {
            start.elapsed().as_millis() as u64 > self.stuck_threshold_millis
        } else {
            false
        }
    }
}
