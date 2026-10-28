#include <catch2/catch_test_macros.hpp>
#include <filesystem>
#include <fstream>
#include "graft/core/raft_node.hpp"
#include "graft/core/snapshot_codec.hpp"
#include "graft/storage/persistent_state_store.hpp"
#include "graft/runtime/raft_runtime.hpp"
#include "graft/runtime/rpc_handler.hpp"

namespace {
    struct RecordingApp : graft::ApplicationStateMachine {
        std::string value;
        std::string apply(std::int64_t, std::int64_t, std::string_view data) override { value=data; return {}; }
        std::string query(std::string_view) const override { return value; }
        std::string snapshot() const override { return value; }
        void restore(std::string_view data) override { value=data; }
    };
    raft::AppendEntriesResponse acknowledgement(const std::string &peer, std::int64_t term, std::int64_t index) {
        raft::AppendEntriesResponse reply;
        reply.set_peer_id(peer); reply.set_term(term); reply.set_success(true); reply.set_match_index(index);
        return reply;
    }
}

TEST_CASE("Short append retry preserves an already committed suffix", "[safety]") {
    graft::RaftNode node({.peer_id="a", .voting_peers={"b","c"}});
    raft::AppendEntriesRequest request;
    request.set_term(1); request.set_leader_id("b"); request.set_leader_commit(2);
    request.add_entries()->set_term(1); request.add_entries()->set_term(1);
    REQUIRE(node.handle_append_entries(request).success());
    request.mutable_entries()->RemoveLast();
    REQUIRE(node.handle_append_entries(request).success());
    REQUIRE(node.last_log_index() == 2);
    REQUIRE(node.commit_index() == 2);
    REQUIRE(node.last_applied() == 2);
}

TEST_CASE("Stale snapshot cannot roll back applied application state", "[safety]") {
    auto app = std::make_shared<RecordingApp>();
    graft::RaftNode node({.peer_id="a", .voting_peers={"b","c"}, .application=app});
    raft::InstallSnapshotRequest snapshot;
    snapshot.set_term(3); snapshot.set_leader_id("b"); snapshot.set_last_included_index(2);
    snapshot.set_last_included_term(1); snapshot.set_snapshot_data("old"); snapshot.set_done(true);
    REQUIRE(node.handle_install_snapshot(snapshot).success());
    raft::AppendEntriesRequest request;
    request.set_term(3); request.set_leader_id("b"); request.set_prev_log_index(2); request.set_prev_log_term(1); request.set_leader_commit(3);
    auto *entry = request.add_entries(); entry->set_term(3); entry->set_data("new");
    REQUIRE(node.handle_append_entries(request).success());
    REQUIRE(node.handle_install_snapshot(snapshot).success());
    REQUIRE(app->value == "new");
    REQUIRE(node.last_applied() == 3);
}

TEST_CASE("Delayed quorum reply cannot authorize a later read after replacement leadership", "[safety][read]") {
    std::int64_t now=10000;
    auto aa=std::make_shared<RecordingApp>(), ba=std::make_shared<RecordingApp>(), ca=std::make_shared<RecordingApp>();
    graft::RaftNode a({.peer_id="a",.voting_peers={"b","c"},.application=aa,.time_source=[&]{return now;}});
    graft::RaftNode b({.peer_id="b",.voting_peers={"a","c"},.application=ba,.time_source=[&]{return now;}});
    graft::RaftNode c({.peer_id="c",.voting_peers={"a","b"},.application=ca,.time_source=[&]{return now;}});
    auto vote=a.start_election();
    REQUIRE(a.handle_vote_response(b.handle_vote_request(vote)));
    a.append_local_entry("one");
    REQUIRE(a.handle_append_entries_response("b", b.handle_append_entries(a.make_replication_request_for("b"))));
    REQUIRE(a.handle_append_entries_response("c", c.handle_append_entries(a.make_replication_request_for("c"))));
    auto delayed=b.handle_append_entries(a.make_replication_request_for("b"));
    now=12000;
    vote=b.start_election();
    REQUIRE(b.handle_vote_response(c.handle_vote_request(vote)));
    b.append_local_entry("two");
    REQUIRE(b.handle_append_entries_response("c", c.handle_append_entries(b.make_replication_request_for("c"))));
    REQUIRE(ba->value == "two");
    REQUIRE(a.handle_append_entries_response("b", delayed));
    REQUIRE(aa->value == "one");
    REQUIRE_FALSE(a.can_serve_linearizable_read(750));
    const auto fence=a.append_local_entry({});
    REQUIRE(a.handle_append_entries_response("b", delayed));
    REQUIRE(a.commit_index() < fence);
}

TEST_CASE("Uncommitted JOINT entry requires a new voter acknowledgement", "[safety][membership]") {
    graft::RaftNode node({.peer_id="a",.current_term=1,.voting_peers={"b","c"}});
    node.become_leader();
    node.handle_append_entries_response("b", acknowledgement("b",1,1));
    raft::InternalRaftCommand joint;
    for (const auto *id: {"a","d","e"}) { auto *p=joint.mutable_joint()->add_members(); p->set_id(id); p->set_role("VOTER"); }
    const auto index=node.append_local_entry(joint.SerializeAsString());
    node.handle_append_entries_response("b", acknowledgement("b",1,index));
    REQUIRE(node.commit_index() < index);
    node.handle_append_entries_response("d", acknowledgement("d",1,index));
    REQUIRE(node.commit_index() == index);
}

TEST_CASE("Snapshots preserve learner roles and addresses", "[safety][snapshot]") {
    raft::PeerSpec voter, learner;
    voter.set_id("a"); voter.set_role("VOTER"); voter.set_host("127.0.0.1"); voter.set_port(9000);
    learner.set_id("b"); learner.set_role("LEARNER"); learner.set_host("127.0.0.2"); learner.set_port(9001);
    auto bytes=graft::SnapshotCodec::wrap_member_specs({voter,learner},{},"app!");
    auto decoded=graft::SnapshotCodec::decode_payload(bytes);
    REQUIRE(decoded.has_value());
    REQUIRE(decoded->current_member_specs.at(1).role() == "LEARNER");
    REQUIRE(decoded->current_member_specs.at(1).host() == "127.0.0.2");
    REQUIRE(decoded->current_member_specs.at(1).port() == 9001);
    REQUIRE(decoded->state_machine_snapshot == "app!");
    bytes.replace(bytes.find("YXBwIQ=="), 8, "YXBwIQ");
    REQUIRE(graft::SnapshotCodec::decode_payload(bytes)->state_machine_snapshot == "app!");
}

TEST_CASE("Restart restores followers with unconfirmed replication progress", "[safety][persistence]") {
    graft::RaftNode original({.peer_id="a",.current_term=3,.voting_peers={"b","c"}});
    original.become_leader();
    original.handle_append_entries_response("b",acknowledgement("b",3,1));
    auto state=original.persistent_state();
    graft::RaftNode recovered({.peer_id="a"});
    recovered.apply_persistent_state(state);
    REQUIRE(recovered.role() == graft::RaftNode::Role::follower);
    recovered.become_leader();
    REQUIRE(recovered.peer_progress().at("b").match_index == 0);
}

TEST_CASE("Runtime startup endpoints cannot overwrite recovered voting membership", "[safety][membership][runtime]") {
    graft::RaftNode original({.peer_id="a",.current_term=3,.voting_peers={"d","e"}});
    auto recovered=std::make_shared<graft::RaftNode>(graft::RaftNode::Config{.peer_id="a"});
    recovered->apply_persistent_state(original.persistent_state());
    boost::asio::io_context context;
    graft::RaftRuntime runtime(context,recovered,{{.peer_id="b",.host="127.0.0.1",.port=1234}});
    const auto voters=recovered->current_voting_members();
    REQUIRE(std::find(voters.begin(),voters.end(),"d") != voters.end());
    REQUIRE(std::find(voters.begin(),voters.end(),"e") != voters.end());
    REQUIRE(std::find(voters.begin(),voters.end(),"b") == voters.end());
}

TEST_CASE("Client completion requires the submitted term and a committed applied index", "[safety][write]") {
    auto app = std::make_shared<RecordingApp>();
    graft::RaftNode node({.peer_id="a", .current_term=1, .voting_peers={"b","c","d","e"}, .application=app});
    node.become_leader();
    const auto index = node.append_local_entry("original");
    node.handle_append_entries_response("b", acknowledgement("b", 1, index));
    REQUIRE(node.commit_index() < index);
    REQUIRE_FALSE(node.committed_command_result(index, 1));
    node.handle_append_entries_response("c", acknowledgement("c", 1, index));
    REQUIRE(node.committed_command_result(index, 1).has_value());
    REQUIRE(app->value == "original");
    auto vote = node.start_election();
    REQUIRE(vote.term() > 1);
    REQUIRE_FALSE(node.committed_command_result(index, 1));
}

TEST_CASE("Singleton runtime reports its own committed write without remote replies", "[safety][write][runtime]") {
    auto app = std::make_shared<RecordingApp>();
    auto node = std::make_shared<graft::RaftNode>(graft::RaftNode::Config{.peer_id="a", .current_term=1, .application=app});
    boost::asio::io_context context;
    graft::RaftRuntime runtime(context, node, {});
    node->become_leader();
    REQUIRE(runtime.replicate_entry_once_with_result("value").has_value());
    REQUIRE(app->value == "value");
    REQUIRE(node->commit_index() == node->last_log_index());
}

TEST_CASE("Joint reconfiguration preserves an explicit leader removal", "[safety][membership][runtime]") {
    auto node = std::make_shared<graft::RaftNode>(graft::RaftNode::Config{.peer_id="a", .current_term=1, .voting_peers={"b","c"}});
    node->become_leader();
    graft::InMemoryRpcHandler handler(node);
    std::string proposed;
    handler.set_internal_command_replicator([&](const std::string &command) { proposed = command; return true; });
    raft::ReconfigureClusterRequest request;
    request.set_action("JOINT");
    for (const auto *id: {"b", "c"}) {
        auto *member = request.add_members(); member->set_id(id); member->set_role("VOTER");
        member->set_host("127.0.0.1"); member->set_port(1234);
    }
    REQUIRE(handler.on_reconfigure_cluster_request(request)->success());
    raft::InternalRaftCommand command;
    REQUIRE(command.ParseFromString(proposed));
    REQUIRE(command.joint().members_size() == 2);
    for (const auto &member: command.joint().members()) REQUIRE(member.id() != "a");
}
