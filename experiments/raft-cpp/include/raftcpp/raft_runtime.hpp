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
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "raftcpp/raft_client.hpp"
#include "raftcpp/raft_node.hpp"

namespace raftcpp {
    struct PeerEndpoint {
        std::string peer_id;
        std::string host;
        std::uint16_t port;
    };

    class RaftRuntime {
    public:
        static constexpr std::size_t kMaxReplicationAttempts = 16;
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

        void track_peer(PeerEndpoint peer);

        bool run_election_round();

        std::size_t send_heartbeats_once();

        std::size_t replicate_entry_once(const std::string &data);

        std::optional<std::string> replicate_entry_once_with_result(const std::string &data);

    private:
        std::vector<PeerEndpoint> voting_peers() const;

        bool sync_peer_once(const PeerEndpoint &peer);

        bool replicate_peer_until_caught_up(const PeerEndpoint &peer);

        bool send_snapshot_to_peer(const PeerEndpoint &peer, std::size_t attempt);

        void persist();

        RaftClient client_;
        mutable std::mutex peers_mu_;
        std::vector<PeerEndpoint> peers_;
        std::shared_ptr<RaftNode> node_;
        std::function<void(const RaftNode &)> persist_callback_;
    };
} // namespace raftcpp
