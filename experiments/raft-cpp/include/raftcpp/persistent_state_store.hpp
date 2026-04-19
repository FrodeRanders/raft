#pragma once

#include <filesystem>
#include <fstream>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>

#include "raftcpp/raft_node.hpp"

namespace raftcpp {

class PersistentStateStore {
public:
    explicit PersistentStateStore(std::filesystem::path path)
        : path_(std::move(path)) {
    }

    const std::filesystem::path& path() const { return path_; }

    bool exists() const {
        return std::filesystem::exists(path_);
    }

    void save(const RaftNode::PersistentState& state) const {
        std::filesystem::create_directories(path_.parent_path());
        std::ofstream out(path_, std::ios::trunc);
        if (!out) {
            throw std::runtime_error("failed to open state file for write: " + path_.string());
        }

        out << "peer_id=" << escape(state.peer_id) << '\n';
        out << "current_term=" << state.current_term << '\n';
        out << "voted_for=" << escape(state.voted_for.value_or("")) << '\n';
        out << "leader_id=" << escape(state.leader_id.value_or("")) << '\n';
        out << "last_log_index=" << state.last_log_index << '\n';
        out << "last_log_term=" << state.last_log_term << '\n';
        out << "commit_index=" << state.commit_index << '\n';
        out << "snapshot_index=" << state.snapshot_index << '\n';
        out << "snapshot_term=" << state.snapshot_term << '\n';
        out << "previous_log_index=" << state.previous_log_index << '\n';
        out << "previous_log_term=" << state.previous_log_term << '\n';
        out << "last_entry_data=" << escape(state.last_entry_data) << '\n';
    }

    std::optional<RaftNode::PersistentState> load() const {
        if (!exists()) {
            return std::nullopt;
        }

        std::ifstream in(path_);
        if (!in) {
            throw std::runtime_error("failed to open state file for read: " + path_.string());
        }

        RaftNode::PersistentState state;
        std::string line;
        while (std::getline(in, line)) {
            const auto pos = line.find('=');
            if (pos == std::string::npos) {
                continue;
            }
            const auto key = line.substr(0, pos);
            const auto value = unescape(line.substr(pos + 1));
            if (key == "peer_id") {
                state.peer_id = value;
            } else if (key == "current_term") {
                state.current_term = std::stoll(value);
            } else if (key == "voted_for") {
                state.voted_for = value.empty() ? std::nullopt : std::optional<std::string>{value};
            } else if (key == "leader_id") {
                state.leader_id = value.empty() ? std::nullopt : std::optional<std::string>{value};
            } else if (key == "last_log_index") {
                state.last_log_index = std::stoll(value);
            } else if (key == "last_log_term") {
                state.last_log_term = std::stoll(value);
            } else if (key == "commit_index") {
                state.commit_index = std::stoll(value);
            } else if (key == "snapshot_index") {
                state.snapshot_index = std::stoll(value);
            } else if (key == "snapshot_term") {
                state.snapshot_term = std::stoll(value);
            } else if (key == "previous_log_index") {
                state.previous_log_index = std::stoll(value);
            } else if (key == "previous_log_term") {
                state.previous_log_term = std::stoll(value);
            } else if (key == "last_entry_data") {
                state.last_entry_data = value;
            }
        }

        if (state.peer_id.empty()) {
            throw std::runtime_error("state file missing peer_id: " + path_.string());
        }
        return state;
    }

private:
    static std::string escape(const std::string& value) {
        std::string out;
        out.reserve(value.size());
        for (const char ch : value) {
            if (ch == '\\' || ch == '\n' || ch == '=') {
                out.push_back('\\');
            }
            out.push_back(ch == '\n' ? 'n' : ch);
        }
        return out;
    }

    static std::string unescape(const std::string& value) {
        std::string out;
        out.reserve(value.size());
        bool escaped = false;
        for (const char ch : value) {
            if (escaped) {
                out.push_back(ch == 'n' ? '\n' : ch);
                escaped = false;
            } else if (ch == '\\') {
                escaped = true;
            } else {
                out.push_back(ch);
            }
        }
        return out;
    }

    std::filesystem::path path_;
};

} // namespace raftcpp
