#pragma once

#include <filesystem>
#include <optional>
#include <string>

#include "raftcpp/raft_node.hpp"

namespace raftcpp {

class PersistentStateStore {
public:
    explicit PersistentStateStore(std::filesystem::path path);

    const std::filesystem::path& path() const;
    bool exists() const;
    void save(const RaftNode::PersistentState& state) const;
    std::optional<RaftNode::PersistentState> load() const;

private:
    static std::string escape(const std::string& value);
    static std::string unescape(const std::string& value);

    std::filesystem::path path_;
};

} // namespace raftcpp
