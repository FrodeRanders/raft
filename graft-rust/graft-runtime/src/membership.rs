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

//! Encoder functions for internal Raft membership commands.
//!
//! Membership changes (JOIN, JOINT, FINALIZE) are encoded as protobuf
//! `InternalRaftCommand` messages and replicated through the Raft log.
//! The runtime layer encodes them; `RaftNode::apply_configuration_command`
//! decodes and applies them.

use graft_proto::raft;
use graft_proto::raft::internal_raft_command::Command;
use prost::Message;

/// Encodes a JOIN command: admit a single peer (typically as learner).
pub fn encode_join_command(peer: &raft::PeerSpec) -> Vec<u8> {
    raft::InternalRaftCommand {
        command: Some(Command::Join(raft::JoinPeerCommand {
            member: Some(peer.clone()),
        })),
    }
    .encode_to_vec()
}

/// Encodes a JOINT command: enter joint consensus with the given members.
pub fn encode_joint_command(members: &[raft::PeerSpec]) -> Vec<u8> {
    raft::InternalRaftCommand {
        command: Some(Command::Joint(raft::JointConfigurationCommand {
            members: members.to_vec(),
        })),
    }
    .encode_to_vec()
}

/// Encodes a FINALIZE command: exit joint consensus.
pub fn encode_finalize_command() -> Vec<u8> {
    raft::InternalRaftCommand {
        command: Some(Command::Finalize(raft::FinalizeConfigurationCommand {})),
    }
    .encode_to_vec()
}
