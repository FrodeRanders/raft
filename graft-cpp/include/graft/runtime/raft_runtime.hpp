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
#pragma once

#include <boost/asio.hpp>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "graft/core/raft_node.hpp"
#include "graft/transport/raft_client.hpp"

namespace graft {
    // Runtime-level endpoint metadata. RaftNode only knows peer ids; the runtime
    // translates those ids into host/port destinations for outbound RPCs.
    struct PeerEndpoint {
        std::string peer_id;
        std::string host;
        std::uint16_t port;
        std::string role;
    };

    // RaftRuntime is the active side of a node. It drives elections, heartbeats,
    // replication, snapshot fallback and read barriers by calling the pure RaftNode
    // methods and sending the resulting protobuf messages through RaftClient.
    class RaftRuntime {
    public:
        // The bounded runtime retries AppendEntries backtracking a fixed number of
        // times. Production implementations would usually keep running asynchronously.
        static constexpr std::size_t kMaxReplicationAttempts = 16;
        // Small chunks make mixed-language snapshot smoke tests exercise chunk assembly.
        static constexpr std::size_t kSnapshotChunkBytes = 8;

        RaftRuntime(boost::asio::io_context &io_context, RaftNode::Config config, std::vector<PeerEndpoint> peers);

        RaftRuntime(boost::asio::io_context &io_context, std::shared_ptr<RaftNode> node,
                    std::vector<PeerEndpoint> peers);

        RaftRuntime(
            boost::asio::io_context &io_context,
            std::shared_ptr<RaftNode> node,
            std::vector<PeerEndpoint> peers,
            std::function<void(const RaftNode &)> persist_callback
        );

        RaftNode &node();

        const RaftNode &node() const;

        std::shared_ptr<RaftNode> node_ptr();

        std::vector<PeerEndpoint> peers() const;

        void configure_peers(std::vector<PeerEndpoint> peers);

        // Track a peer endpoint without necessarily making it a voter. This is used
        // when membership messages carry endpoint data before the node is promoted.
        void track_peer(PeerEndpoint peer);

        // One-shot operations used by CLI modes and tests. The caller owns scheduling.
        bool run_election_round();

        std::size_t send_heartbeats_once();

        bool refresh_read_barrier_once();

        bool await_linearizable_read(std::chrono::milliseconds lease, std::chrono::milliseconds timeout);

        std::size_t replicate_entry_once(const std::string &data);

        std::optional<std::string> replicate_entry_once_with_result(const std::string &data);

    private:
        // Only voting peers participate in elections, commit majorities and read barriers.
        std::vector<PeerEndpoint> voting_peers() const;

        bool sync_peer_once(const PeerEndpoint &peer);

        bool replicate_peer_until_caught_up(const PeerEndpoint &peer);

        bool send_snapshot_to_peer(const PeerEndpoint &peer, std::size_t attempt);

        void refresh_configured_peers();

        void persist();

        RaftClient client_;
        // Endpoint membership can be updated by join/reconfigure handling while runtime
        // loops are active, so the vector has its own mutex separate from RaftNode.
        mutable std::mutex peers_mu_;
        std::vector<PeerEndpoint> peers_;
        std::shared_ptr<RaftNode> node_;
        // Persistence is injected so tests, smoke modes and future library users can
        // decide how durable state is stored without changing the runtime algorithm.
        std::function<void(const RaftNode &)> persist_callback_;
    };
} // namespace graft
