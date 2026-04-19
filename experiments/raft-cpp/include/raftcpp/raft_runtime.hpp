#pragma once

#include <boost/asio.hpp>
#include <cstdint>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "raft.pb.h"
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
    RaftRuntime(boost::asio::io_context& io_context, RaftNode::Config config, std::vector<PeerEndpoint> peers)
        : RaftRuntime(io_context, std::make_shared<RaftNode>(std::move(config)), std::move(peers)) {
    }

    RaftRuntime(boost::asio::io_context& io_context, std::shared_ptr<RaftNode> node, std::vector<PeerEndpoint> peers)
        : client_(io_context),
          peers_(std::move(peers)),
          node_(std::move(node)) {
        if (!node_) {
            throw std::runtime_error("raft runtime requires a node");
        }
        std::vector<std::string> peer_ids;
        peer_ids.reserve(peers_.size());
        for (const auto& peer : peers_) {
            if (peer.peer_id.empty()) {
                throw std::runtime_error("peer id is required");
            }
            peer_ids.push_back(peer.peer_id);
        }
        node_->set_voting_peers(std::move(peer_ids));
    }

    RaftNode& node() { return *node_; }
    const RaftNode& node() const { return *node_; }
    std::shared_ptr<RaftNode> node_ptr() { return node_; }

    bool run_election_round() {
        const auto request = node_->start_election();

        std::cout
            << "starting election term=" << request.term()
            << " candidate=" << request.candidate_id()
            << " quorum=" << node_->quorum_size()
            << '\n';

        for (const auto& peer : peers_) {
            try {
                const auto response = client_.call<raft::VoteRequest, raft::VoteResponse>(
                    peer.host,
                    peer.port,
                    "VoteRequest",
                    request,
                    "VoteResponse"
                );
                const auto became_leader = node_->handle_vote_response(response);
                std::cout
                    << "vote-response peer=" << peer.peer_id
                    << " current_term=" << response.current_term()
                    << " granted=" << (response.vote_granted() ? "true" : "false")
                    << " leader=" << (became_leader ? "true" : "false")
                    << '\n';
            } catch (const std::exception& e) {
                std::cout
                    << "vote-response peer=" << peer.peer_id
                    << " error=" << e.what()
                    << '\n';
            }
        }

        return node_->role() == RaftNode::Role::leader;
    }

    std::size_t send_heartbeats_once() {
        if (node_->role() != RaftNode::Role::leader) {
            return 0;
        }

        std::size_t successes = 0;
        for (const auto& peer : peers_) {
            const auto request = node_->make_heartbeat_request_for(peer.peer_id);
            try {
                const auto response = client_.call<raft::AppendEntriesRequest, raft::AppendEntriesResponse>(
                    peer.host,
                    peer.port,
                    "AppendEntriesRequest",
                    request,
                    "AppendEntriesResponse"
                );
                if (node_->handle_append_entries_response(peer.peer_id, response)) {
                    successes += 1;
                }
                std::cout
                    << "heartbeat-response peer=" << peer.peer_id
                    << " success=" << (response.success() ? "true" : "false")
                    << " match_index=" << response.match_index()
                    << '\n';
            } catch (const std::exception& e) {
                std::cout
                    << "heartbeat-response peer=" << peer.peer_id
                    << " error=" << e.what()
                    << '\n';
            }
        }

        return successes;
    }

    std::size_t replicate_entry_once(const std::string& data) {
        if (node_->role() != RaftNode::Role::leader) {
            return 0;
        }

        const auto initial_commit_index = node_->commit_index();
        node_->append_local_entry(data);

        std::size_t successes = 0;
        for (const auto& peer : peers_) {
            const auto request = node_->make_replication_request_for(peer.peer_id);
            try {
                const auto response = client_.call<raft::AppendEntriesRequest, raft::AppendEntriesResponse>(
                    peer.host,
                    peer.port,
                    "AppendEntriesRequest",
                    request,
                    "AppendEntriesResponse"
                );
                if (node_->handle_append_entries_response(peer.peer_id, response)) {
                    successes += 1;
                }
                std::cout
                    << "replication-response peer=" << peer.peer_id
                    << " success=" << (response.success() ? "true" : "false")
                    << " match_index=" << response.match_index()
                    << '\n';
            } catch (const std::exception& e) {
                std::cout
                    << "replication-response peer=" << peer.peer_id
                    << " error=" << e.what()
                    << '\n';
            }
        }

        if (node_->commit_index() > initial_commit_index) {
            send_heartbeats_once();
        }

        return successes;
    }

private:
    RaftClient client_;
    std::vector<PeerEndpoint> peers_;
    std::shared_ptr<RaftNode> node_;
};

} // namespace raftcpp
