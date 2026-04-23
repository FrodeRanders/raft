#pragma once

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdio>
#include <cstdint>
#include <limits>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "raft.pb.h"

namespace raftcpp {

class RaftNode {
public:
    enum class Role {
        follower,
        candidate,
        leader
    };

    struct Config {
        std::string peer_id;
        std::int64_t current_term{0};
        std::int64_t last_log_index{0};
        std::int64_t last_log_term{0};
        std::int64_t commit_index{0};
        std::int64_t snapshot_index{0};
        std::int64_t snapshot_term{0};
        std::vector<std::string> voting_peers;
    };

    struct PeerProgress {
        std::int64_t next_index{1};
        std::int64_t match_index{0};
    };

    struct LogEntryRecord {
        std::int64_t index{0};
        std::int64_t term{0};
        std::string data;
    };

    struct CommandCommitResult {
        std::int64_t index{0};
        std::string result;
    };

    struct PersistentState {
        std::string peer_id;
        std::int64_t current_term{0};
        std::optional<std::string> voted_for;
        std::optional<std::string> leader_id;
        bool joint_consensus{false};
        std::vector<std::string> pending_join_ids;
        std::vector<std::string> voting_peers;
        std::int64_t last_log_index{0};
        std::int64_t last_log_term{0};
        std::int64_t commit_index{0};
        std::int64_t snapshot_index{0};
        std::int64_t snapshot_term{0};
        std::string snapshot_data;
        std::int64_t last_applied{0};
        std::unordered_map<std::string, std::string> applied_kv;
        std::int64_t previous_log_index{0};
        std::int64_t previous_log_term{0};
        std::string last_entry_data;
        std::vector<LogEntryRecord> log_entries;
        std::unordered_map<std::string, PeerProgress> peer_progress;
    };

    explicit RaftNode(Config config);

    static constexpr std::string_view kInternalCommandPrefix = "raft-internal:";

    static std::string encode_internal_command(const raft::InternalRaftCommand& command);

    raft::VoteResponse handle_vote_request(const raft::VoteRequest& request);
    raft::AppendEntriesResponse handle_append_entries(const raft::AppendEntriesRequest& request);
    raft::InstallSnapshotResponse handle_install_snapshot(const raft::InstallSnapshotRequest& request);

    bool compact_snapshot_to(std::int64_t index, std::string snapshot_data);
    void become_candidate();
    void become_leader();
    void set_voting_peers(std::vector<std::string> voting_peers);
    std::vector<std::string> voting_peers() const;
    bool joint_consensus() const;
    bool has_pending_join(const std::string& peer_id) const;
    std::size_t quorum_size() const;
    std::size_t granted_votes() const;
    std::chrono::steady_clock::time_point last_activity() const;
    std::optional<std::string> voted_for() const;

    raft::VoteRequest start_election();
    bool handle_vote_response(const raft::VoteResponse& response);
    raft::AppendEntriesRequest make_heartbeat_request_for(const std::string& peer_id) const;
    std::vector<raft::AppendEntriesRequest> make_heartbeat_requests() const;
    bool handle_append_entries_response(const std::string& peer_id, const raft::AppendEntriesResponse& response);
    raft::InstallSnapshotRequest make_install_snapshot_request_for(const std::string& peer_id) const;
    bool handle_install_snapshot_response(const std::string& peer_id, const raft::InstallSnapshotResponse& response);
    std::int64_t append_local_entry(std::string data);
    void observe_local_append(std::int64_t last_log_index, std::int64_t last_log_term);
    raft::AppendEntriesRequest make_replication_request_for(const std::string& peer_id) const;

    std::string peer_id() const;
    std::int64_t current_term() const;
    std::int64_t last_log_index() const;
    std::int64_t last_log_term() const;
    std::int64_t commit_index() const;
    std::int64_t snapshot_index() const;
    std::int64_t snapshot_term() const;
    std::string snapshot_data() const;
    std::int64_t last_applied() const;
    std::unordered_map<std::string, std::string> applied_kv() const;
    std::string applied_command_result(std::int64_t index) const;
    std::optional<CommandCommitResult> append_and_commit_local_command(const std::string& data);
    std::optional<std::string> leader_id() const;
    std::unordered_map<std::string, PeerProgress> peer_progress() const;
    PersistentState persistent_state() const;
    void apply_persistent_state(const PersistentState& state);
    Role role() const;

private:
    static void append_u32_be(std::string& out, std::uint32_t value) {
        out.push_back(static_cast<char>((value >> 24) & 0xFF));
        out.push_back(static_cast<char>((value >> 16) & 0xFF));
        out.push_back(static_cast<char>((value >> 8) & 0xFF));
        out.push_back(static_cast<char>(value & 0xFF));
    }

    static void append_u16_be(std::string& out, std::uint16_t value) {
        out.push_back(static_cast<char>((value >> 8) & 0xFF));
        out.push_back(static_cast<char>(value & 0xFF));
    }

    static bool read_u32_be(const std::string& data, std::size_t& offset, std::uint32_t& value) {
        if (offset + 4 > data.size()) {
            return false;
        }
        value = (static_cast<std::uint32_t>(static_cast<unsigned char>(data[offset])) << 24) |
                (static_cast<std::uint32_t>(static_cast<unsigned char>(data[offset + 1])) << 16) |
                (static_cast<std::uint32_t>(static_cast<unsigned char>(data[offset + 2])) << 8) |
                static_cast<std::uint32_t>(static_cast<unsigned char>(data[offset + 3]));
        offset += 4;
        return true;
    }

    static bool read_u16_be(const std::string& data, std::size_t& offset, std::uint16_t& value) {
        if (offset + 2 > data.size()) {
            return false;
        }
        value = static_cast<std::uint16_t>(
            (static_cast<std::uint16_t>(static_cast<unsigned char>(data[offset])) << 8) |
            static_cast<std::uint16_t>(static_cast<unsigned char>(data[offset + 1]))
        );
        offset += 2;
        return true;
    }

    static std::string base64_encode(std::string_view input) {
        static constexpr char alphabet[] =
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
            "abcdefghijklmnopqrstuvwxyz"
            "0123456789+/";

        std::string out;
        out.reserve(((input.size() + 2) / 3) * 4);
        std::size_t index = 0;
        while (index + 3 <= input.size()) {
            const auto a = static_cast<unsigned char>(input[index++]);
            const auto b = static_cast<unsigned char>(input[index++]);
            const auto c = static_cast<unsigned char>(input[index++]);
            out.push_back(alphabet[(a >> 2) & 0x3F]);
            out.push_back(alphabet[((a & 0x03) << 4) | ((b >> 4) & 0x0F)]);
            out.push_back(alphabet[((b & 0x0F) << 2) | ((c >> 6) & 0x03)]);
            out.push_back(alphabet[c & 0x3F]);
        }

        const auto remaining = input.size() - index;
        if (remaining == 1) {
            const auto a = static_cast<unsigned char>(input[index]);
            out.push_back(alphabet[(a >> 2) & 0x3F]);
            out.push_back(alphabet[(a & 0x03) << 4]);
            out.push_back('=');
            out.push_back('=');
        } else if (remaining == 2) {
            const auto a = static_cast<unsigned char>(input[index]);
            const auto b = static_cast<unsigned char>(input[index + 1]);
            out.push_back(alphabet[(a >> 2) & 0x3F]);
            out.push_back(alphabet[((a & 0x03) << 4) | ((b >> 4) & 0x0F)]);
            out.push_back(alphabet[(b & 0x0F) << 2]);
            out.push_back('=');
        }
        return out;
    }

    static std::optional<std::string> base64_decode(std::string_view input) {
        static constexpr unsigned char invalid = 0xFF;
        static constexpr std::array<unsigned char, 256> decode_table = [] {
            std::array<unsigned char, 256> table{};
            table.fill(invalid);
            for (unsigned char i = 0; i < 26; ++i) {
                table['A' + i] = i;
                table['a' + i] = static_cast<unsigned char>(26 + i);
            }
            for (unsigned char i = 0; i < 10; ++i) {
                table['0' + i] = static_cast<unsigned char>(52 + i);
            }
            table['+'] = 62;
            table['/'] = 63;
            table['='] = 0;
            return table;
        }();

        std::string out;
        out.reserve((input.size() / 4) * 3);
        for (std::size_t i = 0; i < input.size();) {
            unsigned char values[4];
            std::size_t pad = 0;
            for (int j = 0; j < 4; ++j) {
                if (i >= input.size()) {
                    return std::nullopt;
                }
                const unsigned char ch = static_cast<unsigned char>(input[i++]);
                const auto decoded = decode_table[ch];
                if (decoded == invalid) {
                    return std::nullopt;
                }
                if (ch == '=') {
                    ++pad;
                }
                values[j] = decoded;
            }

            out.push_back(static_cast<char>((values[0] << 2) | (values[1] >> 4)));
            if (pad < 2) {
                out.push_back(static_cast<char>((values[1] << 4) | (values[2] >> 2)));
            }
            if (pad < 1) {
                out.push_back(static_cast<char>((values[2] << 6) | values[3]));
            }
        }
        return out;
    }

    static std::string quote_json(std::string_view value) {
        std::string escaped;
        escaped.push_back('"');
        for (char c : value) {
            switch (c) {
            case '"':
                escaped.append("\\\"");
                break;
            case '\\':
                escaped.append("\\\\");
                break;
            case '\b':
                escaped.append("\\b");
                break;
            case '\f':
                escaped.append("\\f");
                break;
            case '\n':
                escaped.append("\\n");
                break;
            case '\r':
                escaped.append("\\r");
                break;
            case '\t':
                escaped.append("\\t");
                break;
            default:
                if (static_cast<unsigned char>(c) < 0x20) {
                    char buffer[7];
                    std::snprintf(buffer, sizeof(buffer), "\\u%04x", static_cast<unsigned char>(c));
                    escaped.append(buffer);
                } else {
                    escaped.push_back(c);
                }
                break;
            }
        }
        escaped.push_back('"');
        return escaped;
    }

    std::string wrap_snapshot_payload_locked(const std::string& state_machine_snapshot) const {
        auto append_member_list = [](std::string& out, const std::vector<std::string>& members) {
            out.push_back('[');
            bool first = true;
            for (const auto& member : members) {
                if (!first) {
                    out.push_back(',');
                }
                first = false;
                out.append("{\"id\":");
                out.append(quote_json(member));
                out.append(",\"role\":\"VOTER\",\"address\":null}");
            }
            out.push_back(']');
        };

        std::vector<std::string> current_members;
        current_members.reserve(voting_peers_.size() + 1);
        current_members.push_back(peer_id_);
        for (const auto& peer : voting_peers_) {
            if (peer != peer_id_) {
                current_members.push_back(peer);
            }
        }
        std::sort(current_members.begin(), current_members.end());

        std::string out;
        out.append("{\"version\":1,\"currentMembers\":");
        append_member_list(out, current_members);
        out.append(",\"nextMembers\":");
        if (joint_consensus_) {
            append_member_list(out, current_members);
        } else {
            out.append("[]");
        }
        out.append(",\"stateMachineSnapshot\":");
        out.append(quote_json(base64_encode(state_machine_snapshot)));
        out.push_back('}');
        return out;
    }

    static std::string unwrap_snapshot_payload(const std::string& payload) {
        if (payload.empty() || payload.front() != '{') {
            return payload;
        }

        const std::string marker = "\"stateMachineSnapshot\":\"";
        const auto start = payload.find(marker);
        if (start == std::string::npos) {
            return payload;
        }
        const auto value_start = start + marker.size();
        const auto value_end = payload.find('"', value_start);
        if (value_end == std::string::npos) {
            return payload;
        }
        const auto decoded = base64_decode(std::string_view(payload).substr(value_start, value_end - value_start));
        return decoded.value_or(payload);
    }

    static std::string serialize_state_machine_snapshot_locked(const std::unordered_map<std::string, std::string>& applied_kv) {
        std::string out;
        std::vector<std::pair<std::string, std::string>> ordered(applied_kv.begin(), applied_kv.end());
        std::sort(ordered.begin(), ordered.end(), [](const auto& left, const auto& right) {
            return left.first < right.first;
        });

        append_u32_be(out, static_cast<std::uint32_t>(ordered.size()));
        for (const auto& [key, value] : ordered) {
            if (key.size() > std::numeric_limits<std::uint16_t>::max() ||
                value.size() > std::numeric_limits<std::uint16_t>::max()) {
                throw std::runtime_error("snapshot key or value exceeds Java UTF length limit");
            }
            append_u16_be(out, static_cast<std::uint16_t>(key.size()));
            out.append(key);
            append_u16_be(out, static_cast<std::uint16_t>(value.size()));
            out.append(value);
        }
        return out;
    }

    void apply_snapshot_to_state_machine_locked() {
        if (snapshot_data_.empty()) {
            return;
        }

        const auto state_machine_snapshot = unwrap_snapshot_payload(snapshot_data_);
        std::unordered_map<std::string, std::string> restored;
        std::size_t offset = 0;
        std::uint32_t size = 0;
        if (!read_u32_be(state_machine_snapshot, offset, size)) {
            return;
        }
        for (std::uint32_t i = 0; i < size; ++i) {
            std::uint16_t key_length = 0;
            std::uint16_t value_length = 0;
            if (!read_u16_be(state_machine_snapshot, offset, key_length) ||
                offset + key_length > state_machine_snapshot.size()) {
                return;
            }
            auto key = state_machine_snapshot.substr(offset, key_length);
            offset += key_length;
            if (!read_u16_be(state_machine_snapshot, offset, value_length) ||
                offset + value_length > state_machine_snapshot.size()) {
                return;
            }
            auto value = state_machine_snapshot.substr(offset, value_length);
            offset += value_length;
            restored[std::move(key)] = std::move(value);
        }
        applied_kv_ = std::move(restored);
    }

    std::string apply_state_machine_command_locked(const std::string& data) {
        raft::StateMachineCommand command;
        if (!command.ParseFromString(data)) {
            return {};
        }

        switch (command.command_case()) {
        case raft::StateMachineCommand::kPut:
            applied_kv_[command.put().key()] = command.put().value();
            return {};
        case raft::StateMachineCommand::kDelete:
            applied_kv_.erase(command.delete_().key());
            return {};
        case raft::StateMachineCommand::kClear:
            applied_kv_.clear();
            return {};
        case raft::StateMachineCommand::kCas: {
            const auto& cas = command.cas();
            const auto found = applied_kv_.find(cas.key());
            const bool present = found != applied_kv_.end();
            const bool matched = present == cas.expected_present() &&
                                 (!present || found->second == cas.expected_value());
            bool current_present = present;
            std::string current_value = present ? found->second : "";
            if (matched) {
                applied_kv_[cas.key()] = cas.new_value();
                current_present = true;
                current_value = cas.new_value();
            }

            raft::StateMachineCommandResult result;
            auto* cas_result = result.mutable_cas();
            cas_result->set_key(cas.key());
            cas_result->set_expected_present(cas.expected_present());
            cas_result->set_expected_value(cas.expected_value());
            cas_result->set_new_value(cas.new_value());
            cas_result->set_matched(matched);
            cas_result->set_current_present(current_present);
            cas_result->set_current_value(current_value);

            std::string encoded;
            if (!result.SerializeToString(&encoded)) {
                throw std::runtime_error("failed to serialize StateMachineCommandResult");
            }
            return encoded;
        }
        case raft::StateMachineCommand::COMMAND_NOT_SET:
            return {};
        }
        return {};
    }

    std::optional<raft::InternalRaftCommand> parse_internal_command_locked(const std::string& data) const {
        raft::InternalRaftCommand command;
        if (data.starts_with(kInternalCommandPrefix)) {
            const auto payload = data.substr(kInternalCommandPrefix.size());
            if (!command.ParseFromString(payload)) {
                return std::nullopt;
            }
            return command;
        }

        if (!command.ParseFromString(data)) {
            return std::nullopt;
        }
        if (command.command_case() == raft::InternalRaftCommand::COMMAND_NOT_SET) {
            return std::nullopt;
        }
        return command;
    }

    bool apply_internal_command_locked(const std::string& data) {
        const auto parsed = parse_internal_command_locked(data);
        if (!parsed.has_value()) {
            return false;
        }

        const auto& command = *parsed;
        switch (command.command_case()) {
        case raft::InternalRaftCommand::kJoin:
            if (!command.join().member().id().empty() && command.join().member().id() != peer_id_) {
                pending_join_ids_.insert(command.join().member().id());
                auto& progress = peer_progress_[command.join().member().id()];
                progress.next_index = std::max<std::int64_t>(1, progress.next_index);
            }
            return true;
        case raft::InternalRaftCommand::kJoint: {
            std::vector<std::string> peer_ids;
            peer_ids.reserve(command.joint().members_size());
            for (const auto& member : command.joint().members()) {
                if (!member.id().empty() && member.id() != peer_id_) {
                    peer_ids.push_back(member.id());
                    pending_join_ids_.erase(member.id());
                }
            }
            reconfigure_voting_peers_locked(std::move(peer_ids));
            joint_consensus_ = true;
            return true;
        }
        case raft::InternalRaftCommand::kFinalize:
            joint_consensus_ = false;
            return true;
        case raft::InternalRaftCommand::COMMAND_NOT_SET:
            return false;
        }
        return false;
    }

    std::string apply_log_entry_locked(const std::string& data) {
        if (apply_internal_command_locked(data)) {
            return {};
        }
        return apply_state_machine_command_locked(data);
    }

    void apply_committed_entries_locked() {
        if (snapshot_index_ > last_applied_) {
            apply_snapshot_to_state_machine_locked();
            last_applied_ = snapshot_index_;
        }

        for (const auto& entry : log_entries_) {
            if (entry.index <= last_applied_ || entry.index > commit_index_) {
                continue;
            }
            applied_command_results_.erase(entry.index);
            applied_command_results_[entry.index] = apply_log_entry_locked(entry.data);
            last_applied_ = entry.index;
        }
    }

    void seed_bootstrap_log_locked() {
        if (!log_entries_.empty() || last_log_index_ <= snapshot_index_) {
            return;
        }
        log_entries_.reserve(static_cast<std::size_t>(std::max<std::int64_t>(0, last_log_index_ - snapshot_index_)));
        for (std::int64_t index = snapshot_index_ + 1; index <= last_log_index_; ++index) {
            log_entries_.push_back(LogEntryRecord{
                .index = index,
                .term = last_log_term_,
                .data = "bootstrap-" + std::to_string(index),
            });
        }
        if (!log_entries_.empty()) {
            last_entry_data_ = log_entries_.back().data;
            if (log_entries_.size() >= 2) {
                previous_log_index_ = log_entries_[log_entries_.size() - 2].index;
                previous_log_term_ = log_entries_[log_entries_.size() - 2].term;
            } else {
                previous_log_index_ = snapshot_index_;
                previous_log_term_ = snapshot_term_;
            }
        }
    }

    void normalize_voting_peers_locked() {
        std::vector<std::string> normalized;
        normalized.reserve(voting_peers_.size());
        std::unordered_set<std::string> seen;
        for (const auto& peer_id : voting_peers_) {
            if (peer_id.empty() || peer_id == peer_id_) {
                continue;
            }
            if (seen.insert(peer_id).second) {
                normalized.push_back(peer_id);
            }
        }
        voting_peers_ = std::move(normalized);
    }

    void reconfigure_voting_peers_locked(std::vector<std::string> voting_peers) {
        voting_peers_ = std::move(voting_peers);
        normalize_voting_peers_locked();

        std::unordered_map<std::string, PeerProgress> next_progress;
        next_progress.reserve(voting_peers_.size() + 1);
        const auto self_found = peer_progress_.find(peer_id_);
        next_progress.emplace(
            peer_id_,
            self_found != peer_progress_.end()
                ? self_found->second
                : PeerProgress{.next_index = last_log_index_ + 1, .match_index = last_log_index_}
        );
        next_progress[peer_id_].next_index = std::max(next_progress[peer_id_].next_index, last_log_index_ + 1);
        next_progress[peer_id_].match_index = std::max(next_progress[peer_id_].match_index, last_log_index_);

        for (const auto& peer_id : voting_peers_) {
            const auto found = peer_progress_.find(peer_id);
            if (found != peer_progress_.end()) {
                next_progress.emplace(peer_id, found->second);
                next_progress[peer_id].next_index = std::max(next_progress[peer_id].next_index, static_cast<std::int64_t>(1));
            } else {
                next_progress.emplace(peer_id, PeerProgress{.next_index = last_log_index_ + 1, .match_index = 0});
            }
        }

        peer_progress_ = std::move(next_progress);
    }

    std::size_t cluster_size_locked() const {
        return 1 + voting_peers_.size();
    }

    std::size_t quorum_size_locked() const {
        return (cluster_size_locked() / 2) + 1;
    }

    void reset_peer_progress_locked() {
        std::unordered_map<std::string, PeerProgress> progress;
        progress.reserve(voting_peers_.size() + 1);
        progress.emplace(peer_id_, PeerProgress{.next_index = last_log_index_ + 1, .match_index = last_log_index_});
        for (const auto& peer_id : voting_peers_) {
            progress.emplace(peer_id, PeerProgress{.next_index = last_log_index_ + 1, .match_index = 0});
        }
        peer_progress_ = std::move(progress);
    }

    void start_election_locked() {
        role_ = Role::candidate;
        current_term_ += 1;
        voted_for_ = peer_id_;
        leader_id_.reset();
        last_activity_ = std::chrono::steady_clock::now();
        votes_granted_.clear();
        votes_responded_.clear();
        votes_granted_.insert(peer_id_);
        votes_responded_.insert(peer_id_);
    }

    void become_leader_locked() {
        role_ = Role::leader;
        leader_id_ = peer_id_;
        last_activity_ = std::chrono::steady_clock::now();
        votes_granted_.clear();
        votes_responded_.clear();
        reset_peer_progress_locked();
    }

    void update_commit_index_locked() {
        if (last_log_term_ != current_term_) {
            return;
        }

        std::size_t replicated = 0;
        for (const auto& [peer_id, progress] : peer_progress_) {
            (void)peer_id;
            if (progress.match_index >= last_log_index_) {
                replicated += 1;
            }
        }

        if (replicated >= quorum_size_locked()) {
            commit_index_ = std::max(commit_index_, last_log_index_);
        }
    }

    bool candidate_log_is_up_to_date_locked(const raft::VoteRequest& request) const {
        if (request.last_log_term() != last_log_term_) {
            return request.last_log_term() > last_log_term_;
        }
        return request.last_log_index() >= last_log_index_;
    }

    bool prev_log_matches_locked(std::int64_t prev_log_index, std::int64_t prev_log_term) const {
        if (prev_log_index == 0 && prev_log_term == 0) {
            return true;
        }
        return log_term_at_locked(prev_log_index) == prev_log_term;
    }

    std::int64_t log_term_at_locked(std::int64_t index) const {
        if (index == 0) {
            return 0;
        }
        if (index == snapshot_index_) {
            return snapshot_term_;
        }
        for (const auto& entry : log_entries_) {
            if (entry.index == index) {
                return entry.term;
            }
        }
        if (index == last_log_index_) {
            return last_log_term_;
        }
        if (index == previous_log_index_) {
            return previous_log_term_;
        }
        return 0;
    }

    void truncate_log_from_locked(std::int64_t index) {
        if (index <= 0) {
            log_entries_.clear();
            applied_command_results_.clear();
        } else {
            for (auto it = applied_command_results_.begin(); it != applied_command_results_.end();) {
                if (it->first >= index) {
                    it = applied_command_results_.erase(it);
                } else {
                    ++it;
                }
            }
            log_entries_.erase(
                std::remove_if(log_entries_.begin(), log_entries_.end(), [index](const auto& entry) {
                    return entry.index >= index;
                }),
                log_entries_.end()
            );
        }
        if (!log_entries_.empty()) {
            last_log_index_ = log_entries_.back().index;
            last_log_term_ = log_entries_.back().term;
            last_entry_data_ = log_entries_.back().data;
            if (log_entries_.size() >= 2) {
                previous_log_index_ = log_entries_[log_entries_.size() - 2].index;
                previous_log_term_ = log_entries_[log_entries_.size() - 2].term;
            } else {
                previous_log_index_ = snapshot_index_;
                previous_log_term_ = snapshot_term_;
            }
        } else if (last_log_index_ > snapshot_index_) {
            last_log_index_ = snapshot_index_;
            last_log_term_ = snapshot_term_;
            previous_log_index_ = snapshot_index_;
            previous_log_term_ = snapshot_term_;
            last_entry_data_.clear();
        }
    }

    void truncate_prefix_up_to_locked(std::int64_t index) {
        if (index <= 0) {
            return;
        }
        for (auto it = applied_command_results_.begin(); it != applied_command_results_.end();) {
            if (it->first <= index) {
                it = applied_command_results_.erase(it);
            } else {
                ++it;
            }
        }
        log_entries_.erase(
            std::remove_if(log_entries_.begin(), log_entries_.end(), [index](const auto& entry) {
                return entry.index <= index;
            }),
            log_entries_.end()
        );
        if (log_entries_.empty()) {
            last_log_index_ = std::max(last_log_index_, snapshot_index_);
            last_log_term_ = (last_log_index_ == snapshot_index_) ? snapshot_term_ : last_log_term_;
            previous_log_index_ = snapshot_index_;
            previous_log_term_ = snapshot_term_;
            if (last_log_index_ == snapshot_index_) {
                last_entry_data_.clear();
            }
            return;
        }

        if (log_entries_.front().index == snapshot_index_ + 1) {
            previous_log_index_ = snapshot_index_;
            previous_log_term_ = snapshot_term_;
        }
        last_log_index_ = log_entries_.back().index;
        last_log_term_ = log_entries_.back().term;
        last_entry_data_ = log_entries_.back().data;
    }

    void step_down_locked(std::int64_t new_term, std::optional<std::string> leader_id) {
        current_term_ = new_term;
        role_ = Role::follower;
        voted_for_.reset();
        leader_id_ = std::move(leader_id);
        last_activity_ = std::chrono::steady_clock::now();
        votes_granted_.clear();
        votes_responded_.clear();
    }

    std::string applied_command_result_at_locked(std::int64_t index) const {
        const auto found = applied_command_results_.find(index);
        if (found == applied_command_results_.end()) {
            return {};
        }
        return found->second;
    }

    mutable std::mutex mu_;
    std::string peer_id_;
    Role role_;
    std::int64_t current_term_;
    std::optional<std::string> voted_for_;
    std::optional<std::string> leader_id_;
    bool joint_consensus_{false};
    std::unordered_set<std::string> pending_join_ids_;
    std::int64_t last_log_index_;
    std::int64_t last_log_term_;
    std::int64_t commit_index_;
    std::int64_t snapshot_index_;
    std::int64_t snapshot_term_;
    std::string snapshot_data_;
    std::int64_t last_applied_{0};
    std::unordered_map<std::string, std::string> applied_kv_;
    std::unordered_map<std::int64_t, std::string> applied_command_results_;
    std::int64_t pending_snapshot_index_{0};
    std::int64_t pending_snapshot_term_{0};
    std::string pending_snapshot_data_;
    std::int64_t previous_log_index_;
    std::int64_t previous_log_term_;
    std::string last_entry_data_;
    std::vector<LogEntryRecord> log_entries_;
    std::chrono::steady_clock::time_point last_activity_;
    std::vector<std::string> voting_peers_;
    std::unordered_map<std::string, PeerProgress> peer_progress_;
    std::unordered_set<std::string> votes_granted_;
    std::unordered_set<std::string> votes_responded_;
};

} // namespace raftcpp
