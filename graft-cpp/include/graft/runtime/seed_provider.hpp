#pragma once

#include <cstdint>
#include <functional>
#include <string>
#include <vector>

#include "graft/runtime/raft_runtime.hpp"

namespace graft {
    struct DnsSrvRecord {
        std::int32_t priority;
        std::int32_t weight;
        std::uint16_t port;
        std::string target;
    };

    using DnsSrvResolver = std::function<std::vector<DnsSrvRecord>(const std::string &service_name)>;

    // Startup discovery helpers. Their output seeds the runtime address book,
    // but committed Raft membership remains authoritative after startup.
    class SeedProvider {
    public:
        virtual ~SeedProvider() = default;

        virtual std::vector<PeerEndpoint> seeds() const = 0;
    };

    class StaticSeedProvider final : public SeedProvider {
    public:
        explicit StaticSeedProvider(std::vector<PeerEndpoint> peers);

        std::vector<PeerEndpoint> seeds() const override;

    private:
        std::vector<PeerEndpoint> peers_;
    };

    class DnsSrvSeedProvider final : public SeedProvider {
    public:
        explicit DnsSrvSeedProvider(std::string service_name);
        DnsSrvSeedProvider(std::string service_name, DnsSrvResolver resolver);

        std::vector<PeerEndpoint> seeds() const override;

        static std::string derive_peer_id(std::string host);

    private:
        std::string service_name_;
        DnsSrvResolver resolver_;
    };

    std::vector<DnsSrvRecord> resolve_dns_srv_records(const std::string &service_name);
} // namespace graft
