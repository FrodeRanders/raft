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

use std::net::SocketAddr;
use std::process::Command;
use std::str::FromStr;

/// A resolved bootstrap peer endpoint. Seeds are used during cluster
/// startup so a node can contact existing members and join (or discover
/// the current leader).
#[derive(Debug, Clone)]
pub struct SeedEndpoint {
    pub peer_id: String,
    pub host: String,
    pub port: u16,
}

impl SeedEndpoint {
    pub fn to_socket_addr(&self) -> Option<SocketAddr> {
        SocketAddr::from_str(&format!("{}:{}", self.host, self.port)).ok()
    }
}

/// Abstraction for bootstrap peer discovery.
pub trait SeedProvider: Send + Sync {
    fn seeds(&self) -> Vec<SeedEndpoint>;
}

/// A seed provider backed by a fixed, hard-coded list of endpoints.
pub struct StaticSeedProvider {
    seeds: Vec<SeedEndpoint>,
}

impl StaticSeedProvider {
    pub fn new(seeds: Vec<SeedEndpoint>) -> Self {
        Self { seeds }
    }
}

impl SeedProvider for StaticSeedProvider {
    fn seeds(&self) -> Vec<SeedEndpoint> {
        self.seeds.clone()
    }
}

/// Resolves cluster bootstrap peers from DNS SRV records. Uses the
/// system's `dig` command (or `nslookup` as fallback) to query SRV
/// records for the given service name.
///
/// The service name format follows RFC 2782:
/// `_raft._tcp.<domain>` — e.g., `_raft._tcp.cluster.example.com`.
///
/// This mirrors the C++ `DnsSrvSeedProvider` which uses `res_query`.
pub struct DnsSrvSeedProvider {
    service_name: String,
}

impl DnsSrvSeedProvider {
    pub fn new(service_name: String) -> Self {
        Self { service_name }
    }

    /// Resolves SRV records using `dig +short SRV <service>`.
    /// Falls back to `nslookup -type=SRV <service>` if `dig` is unavailable.
    fn resolve(&self) -> Vec<SeedEndpoint> {
        // Try `dig` first (more reliable SRV output parsing)
        if let Ok(output) = Command::new("dig")
            .args(["+short", "SRV", &self.service_name])
            .output()
        {
            if output.status.success() {
                return self.parse_dig_output(&output.stdout);
            }
        }

        // Fallback to nslookup
        if let Ok(output) = Command::new("nslookup")
            .args(["-type=SRV", &self.service_name])
            .output()
        {
            if output.status.success() {
                return self.parse_nslookup_output(&output.stdout);
            }
        }

        tracing::warn!("Failed to resolve SRV records for {}", self.service_name);
        vec![]
    }

    /// Parses `dig +short SRV` output:
    /// ```text
    /// 10 5 5001 n1.example.com.
    /// 10 5 5002 n2.example.com.
    /// ```
    fn parse_dig_output(&self, stdout: &[u8]) -> Vec<SeedEndpoint> {
        let text = String::from_utf8_lossy(stdout);
        text.lines()
            .filter(|l| !l.trim().is_empty())
            .filter_map(|line| {
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() >= 4 {
                    let port: u16 = parts[2].parse().ok()?;
                    let host = parts[3].trim_end_matches('.');
                    // Use the hostname as the peer id (stripping domain suffix)
                    let peer_id = host.split('.').next().unwrap_or(host).to_string();
                    Some(SeedEndpoint { peer_id, host: host.to_string(), port })
                } else {
                    None
                }
            })
            .collect()
    }

    /// Parses `nslookup -type=SRV` output.
    fn parse_nslookup_output(&self, stdout: &[u8]) -> Vec<SeedEndpoint> {
        let text = String::from_utf8_lossy(stdout);
        text.lines()
            .filter(|l| l.contains("service =") || l.contains("SRV service"))
            .filter_map(|line| {
                // nslookup format: "_raft._tcp.cluster.example.com service = 10 5 5001 n1.example.com."
                let parts: Vec<&str> = line.split_whitespace().collect();
                // Find the position after "service ="
                if let Some(pos) = parts.iter().position(|&p| p == "=") {
                    if parts.len() > pos + 4 {
                        let port: u16 = parts[pos + 3].parse().ok()?;
                        let host = parts[pos + 4].trim_end_matches('.');
                        let peer_id = host.split('.').next().unwrap_or(host).to_string();
                        return Some(SeedEndpoint { peer_id, host: host.to_string(), port });
                    }
                }
                None
            })
            .collect()
    }
}

impl SeedProvider for DnsSrvSeedProvider {
    fn seeds(&self) -> Vec<SeedEndpoint> {
        self.resolve()
    }
}
