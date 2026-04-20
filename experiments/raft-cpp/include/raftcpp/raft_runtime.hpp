#pragma once

#include <boost/asio.hpp>
#include <cstdint>
#include <functional>
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
    static constexpr std::size_t kMaxReplicationAttempts = 16;
    static constexpr std::size_t kSnapshotChunkBytes = 8;

    RaftRuntime(boost::asio::io_context& io_context, RaftNode::Config config, std::vector<PeerEndpoint> peers)
        : RaftRuntime(io_context, std::make_shared<RaftNode>(std::move(config)), std::move(peers), {}) {
    }

    RaftRuntime(boost::asio::io_context& io_context, std::shared_ptr<RaftNode> node, std::vector<PeerEndpoint> peers)
        : RaftRuntime(io_context, std::move(node), std::move(peers), {}) {
    }

    RaftRuntime(
        boost::asio::io_context& io_context,
        std::shared_ptr<RaftNode> node,
        std::vector<PeerEndpoint> peers,
        std::function<void(const RaftNode&)> persist_callback
    )
        : client_(io_context),
          peers_(std::move(peers)),
          node_(std::move(node)),
          persist_callback_(std::move(persist_callback)) {
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
        persist();

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
                persist();
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
            successes += sync_peer_once(peer) ? 1 : 0;
        }

        return successes;
    }

    std::size_t replicate_entry_once(const std::string& data) {
        if (node_->role() != RaftNode::Role::leader) {
            return 0;
        }

        const auto initial_commit_index = node_->commit_index();
        node_->append_local_entry(data);
        persist();

        std::size_t successes = 0;
        for (const auto& peer : peers_) {
            successes += replicate_peer_until_caught_up(peer) ? 1 : 0;
        }

        if (node_->commit_index() > initial_commit_index) {
            send_heartbeats_once();
        }

        return successes;
    }

private:
    bool sync_peer_once(const PeerEndpoint& peer) {
        const auto progress = node_->peer_progress();
        const auto found = progress.find(peer.peer_id);
        const auto next_index = found != progress.end() ? found->second.next_index : (node_->last_log_index() + 1);

        if (node_->snapshot_index() > 0 && next_index <= node_->snapshot_index()) {
            return send_snapshot_to_peer(peer, 1);
        }

        const bool needs_append = next_index <= node_->last_log_index();
        const auto request = needs_append
            ? node_->make_replication_request_for(peer.peer_id)
            : node_->make_heartbeat_request_for(peer.peer_id);
        try {
            const auto response = client_.call<raft::AppendEntriesRequest, raft::AppendEntriesResponse>(
                peer.host,
                peer.port,
                "AppendEntriesRequest",
                request,
                "AppendEntriesResponse"
            );
            const auto advanced = node_->handle_append_entries_response(peer.peer_id, response);
            persist();
            std::cout
                << (needs_append ? "catchup-response peer=" : "heartbeat-response peer=") << peer.peer_id
                << " success=" << (response.success() ? "true" : "false")
                << " match_index=" << response.match_index()
                << '\n';
            return advanced;
        } catch (const std::exception& e) {
            std::cout
                << (needs_append ? "catchup-response peer=" : "heartbeat-response peer=") << peer.peer_id
                << " error=" << e.what()
                << '\n';
            return false;
        }
    }

    bool replicate_peer_until_caught_up(const PeerEndpoint& peer) {
        const auto target_index = node_->last_log_index();
        for (std::size_t attempt = 1; attempt <= kMaxReplicationAttempts; ++attempt) {
            const auto progress = node_->peer_progress();
            const auto found = progress.find(peer.peer_id);
            const auto next_index = found != progress.end() ? found->second.next_index : (target_index + 1);
            if (node_->snapshot_index() > 0 && next_index <= node_->snapshot_index()) {
                if (!send_snapshot_to_peer(peer, attempt)) {
                    return false;
                }
                continue;
            }

            const auto request = node_->make_replication_request_for(peer.peer_id);
            try {
                const auto response = client_.call<raft::AppendEntriesRequest, raft::AppendEntriesResponse>(
                    peer.host,
                    peer.port,
                    "AppendEntriesRequest",
                    request,
                    "AppendEntriesResponse"
                );
                const auto advanced = node_->handle_append_entries_response(peer.peer_id, response);
                persist();
                std::cout
                    << "replication-response peer=" << peer.peer_id
                    << " attempt=" << attempt
                    << " success=" << (response.success() ? "true" : "false")
                    << " match_index=" << response.match_index()
                    << '\n';

                if (response.success() && response.match_index() >= target_index) {
                    return true;
                }
                if (!response.success() && !advanced) {
                    continue;
                }
            } catch (const std::exception& e) {
                std::cout
                    << "replication-response peer=" << peer.peer_id
                    << " attempt=" << attempt
                    << " error=" << e.what()
                    << '\n';
                return false;
            }
        }

        const auto progress = node_->peer_progress();
        const auto found = progress.find(peer.peer_id);
        return found != progress.end() && found->second.match_index >= target_index;
    }

    bool send_snapshot_to_peer(const PeerEndpoint& peer, std::size_t attempt) {
        const auto full_request = node_->make_install_snapshot_request_for(peer.peer_id);
        const auto full_data = full_request.snapshot_data();
        const auto total_size = std::max<std::size_t>(1, full_data.size());

        for (std::size_t offset = 0; offset < total_size; offset += kSnapshotChunkBytes) {
            raft::InstallSnapshotRequest chunk_request;
            chunk_request.set_term(full_request.term());
            chunk_request.set_leader_id(full_request.leader_id());
            chunk_request.set_last_included_index(full_request.last_included_index());
            chunk_request.set_last_included_term(full_request.last_included_term());
            chunk_request.set_offset(static_cast<std::int64_t>(offset));

            const auto remaining = full_data.size() > offset ? full_data.size() - offset : 0;
            const auto chunk_size = full_data.empty() ? 0 : std::min<std::size_t>(kSnapshotChunkBytes, remaining);
            chunk_request.set_snapshot_data(full_data.substr(offset, chunk_size));
            chunk_request.set_done(offset + chunk_size >= full_data.size());

            try {
                const auto response = client_.call<raft::InstallSnapshotRequest, raft::InstallSnapshotResponse>(
                    peer.host,
                    peer.port,
                    "InstallSnapshotRequest",
                    chunk_request,
                    "InstallSnapshotResponse"
                );
                const auto advanced = chunk_request.done() && node_->handle_install_snapshot_response(peer.peer_id, response);
                persist();
                std::cout
                    << "snapshot-response peer=" << peer.peer_id
                    << " attempt=" << attempt
                    << " offset=" << offset
                    << " bytes=" << chunk_size
                    << " success=" << (response.success() ? "true" : "false")
                    << " last_included_index=" << response.last_included_index()
                    << '\n';
                if (!response.success()) {
                    return false;
                }
                if (chunk_request.done()) {
                    return advanced;
                }
            } catch (const std::exception& e) {
                std::cout
                    << "snapshot-response peer=" << peer.peer_id
                    << " attempt=" << attempt
                    << " offset=" << offset
                    << " error=" << e.what()
                    << '\n';
                return false;
            }
        }
        return false;
    }

    void persist() {
        if (persist_callback_) {
            persist_callback_(*node_);
        }
    }

    RaftClient client_;
    std::vector<PeerEndpoint> peers_;
    std::shared_ptr<RaftNode> node_;
    std::function<void(const RaftNode&)> persist_callback_;
};

} // namespace raftcpp
