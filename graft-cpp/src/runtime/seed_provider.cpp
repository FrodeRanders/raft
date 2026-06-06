#include "graft/runtime/seed_provider.hpp"

#include <algorithm>
#include <cctype>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <utility>

#if defined(__APPLE__) || defined(__linux__) || defined(__unix__)
#include <arpa/nameser.h>
#include <resolv.h>
#endif

namespace graft {
    namespace {
        std::string strip_trailing_dot(std::string value) {
            while (!value.empty() && value.back() == '.') {
                value.pop_back();
            }
            return value;
        }
    }

    StaticSeedProvider::StaticSeedProvider(std::vector<PeerEndpoint> peers)
        : peers_(std::move(peers)) {
    }

    std::vector<PeerEndpoint> StaticSeedProvider::seeds() const {
        return peers_;
    }

    DnsSrvSeedProvider::DnsSrvSeedProvider(std::string service_name)
        : DnsSrvSeedProvider(std::move(service_name), resolve_dns_srv_records) {
    }

    DnsSrvSeedProvider::DnsSrvSeedProvider(std::string service_name, DnsSrvResolver resolver)
        : service_name_(std::move(service_name)),
          resolver_(std::move(resolver)) {
        if (service_name_.empty()) {
            throw std::invalid_argument("DNS SRV service name must not be empty");
        }
        if (!resolver_) {
            throw std::invalid_argument("DNS SRV resolver must be provided");
        }
    }

    std::vector<PeerEndpoint> DnsSrvSeedProvider::seeds() const {
        auto records = resolver_(service_name_);
        std::sort(records.begin(), records.end(), [](const DnsSrvRecord &left, const DnsSrvRecord &right) {
            if (left.priority != right.priority) {
                return left.priority < right.priority;
            }
            if (left.weight != right.weight) {
                return left.weight < right.weight;
            }
            return left.target < right.target;
        });

        std::vector<PeerEndpoint> endpoints;
        endpoints.reserve(records.size());
        for (const auto &record: records) {
            if (record.target.empty() || record.port == 0) {
                continue;
            }
            endpoints.push_back(PeerEndpoint{
                .peer_id = derive_peer_id(record.target),
                .host = record.target,
                .port = record.port,
                .role = "VOTER",
            });
        }
        return endpoints;
    }

    std::string DnsSrvSeedProvider::derive_peer_id(std::string host) {
        host = strip_trailing_dot(std::move(host));
        const auto dot = host.find('.');
        auto id = dot == std::string::npos ? host : host.substr(0, dot);
        std::transform(id.begin(), id.end(), id.begin(), [](unsigned char ch) {
            return static_cast<char>(std::tolower(ch));
        });
        return id;
    }

    std::vector<DnsSrvRecord> resolve_dns_srv_records(const std::string &service_name) {
#if defined(__APPLE__) || defined(__linux__) || defined(__unix__)
        unsigned char answer[NS_PACKETSZ * 4];
        const int length = res_query(service_name.c_str(), ns_c_in, ns_t_srv, answer, sizeof(answer));
        if (length < 0) {
            return {};
        }

        ns_msg message;
        if (ns_initparse(answer, length, &message) < 0) {
            throw std::runtime_error("failed parsing DNS SRV response for " + service_name);
        }

        std::vector<DnsSrvRecord> records;
        const int count = ns_msg_count(message, ns_s_an);
        for (int i = 0; i < count; ++i) {
            ns_rr record;
            if (ns_parserr(&message, ns_s_an, i, &record) < 0) {
                throw std::runtime_error("failed parsing DNS SRV answer for " + service_name);
            }
            if (ns_rr_type(record) != ns_t_srv) {
                continue;
            }
            const auto *rdata = ns_rr_rdata(record);
            const auto rdlen = ns_rr_rdlen(record);
            if (rdlen < 7) {
                continue;
            }
            const auto priority = static_cast<std::int32_t>(ns_get16(rdata));
            const auto weight = static_cast<std::int32_t>(ns_get16(rdata + 2));
            const auto port = ns_get16(rdata + 4);

            char target[NS_MAXDNAME];
            const int expanded = ns_name_uncompress(
                ns_msg_base(message),
                ns_msg_end(message),
                rdata + 6,
                target,
                sizeof(target)
            );
            if (expanded < 0) {
                throw std::runtime_error("failed expanding DNS SRV target for " + service_name);
            }
            records.push_back(DnsSrvRecord{
                .priority = priority,
                .weight = weight,
                .port = static_cast<std::uint16_t>(port),
                .target = strip_trailing_dot(target),
            });
        }
        return records;
#else
        throw std::runtime_error("DNS SRV resolution is not supported on this platform for service " + service_name);
#endif
    }
} // namespace graft
