#pragma once

// ── PeerRegistry — multi-master peer discovery and registration via etcd ─────
//
// PeerInfo describes a single multi-master node.  PeerRegistryData holds the
// full peer topology stored in etcd.  Both serialize to/from JSON with
// deterministic (alphabetically sorted) key ordering via nlohmann::json.
//
// PeerRegistry manages the lifecycle of a node in the cluster: registration,
// lease keep-alive, topology watching, and deregistration.
//
// Requirements: 3.1, 3.2, 3.6, 8.2, 13.1, 13.2, 13.3, 13.4, 13.5

#include "orderbook/coordinator.hpp"
#include "orderbook/hlc.hpp"
#include "orderbook/log_episode.hpp"
#include "orderbook/metrics.hpp"

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>

namespace ob {

// ── Peer information ──────────────────────────────────────────────────────────

struct PeerInfo {
    uint16_t    node_id{0};
    std::string address;           // host:port (replication address)
    std::string status;            // "active", "joining", "leaving"
    HLCTimestamp last_hlc;         // last known HLC timestamp
    uint32_t    wal_file_index{0}; // WAL position (for anti-entropy)
    size_t      wal_byte_offset{0};

    bool operator==(const PeerInfo& o) const {
        return node_id == o.node_id &&
               address == o.address &&
               status == o.status &&
               last_hlc == o.last_hlc &&
               wal_file_index == o.wal_file_index &&
               wal_byte_offset == o.wal_byte_offset;
    }

    bool operator!=(const PeerInfo& o) const { return !(*this == o); }

    /// Serialize to JSON (deterministic key order).
    std::string to_json() const;

    /// Deserialize from JSON.  Returns true on success.
    static bool from_json(std::string_view json, PeerInfo& out);

    /// Deserialize from JSON with descriptive error message on failure.
    static bool from_json(std::string_view json, PeerInfo& out,
                          std::string& error);
};

// ── Peer Registry data (full structure in etcd) ───────────────────────────────

struct PeerRegistryData {
    uint64_t version{0};
    std::unordered_map<uint16_t, PeerInfo> peers;  // node_id → PeerInfo
    std::string topology{"full-mesh"};

    bool operator==(const PeerRegistryData& o) const {
        return version == o.version &&
               peers == o.peers &&
               topology == o.topology;
    }

    bool operator!=(const PeerRegistryData& o) const { return !(*this == o); }

    /// Serialize to JSON (deterministic key order).
    std::string to_json() const;

    /// Pretty-print JSON (indented, human-readable).
    std::string to_json_pretty() const;

    /// Deserialize from JSON.  Returns true on success.
    static bool from_json(std::string_view json, PeerRegistryData& out);

    /// Deserialize from JSON with descriptive error message on failure.
    static bool from_json(std::string_view json, PeerRegistryData& out,
                          std::string& error);
};

// ── Topology change callback ──────────────────────────────────────────────────

using TopologyChangeCallback = std::function<void(const std::vector<PeerInfo>& peers)>;

// ── etcd key layout helpers ───────────────────────────────────────────────────
//
// Without sharding:
//   <cluster_prefix>mm_peers/<node_id>  →  PeerInfo JSON
//
// With sharding:
//   <cluster_prefix>shards/<shard_id>/mm_peers/<node_id>  →  PeerInfo JSON

/// Build the peer key path (without sharding).
std::string mm_peer_key(const std::string& prefix, uint16_t node_id);

/// Build the peer key path (with sharding).
std::string mm_peer_key(const std::string& prefix, const std::string& shard_id,
                        uint16_t node_id);

/// Build the range-end for watching all peer keys (without sharding).
std::string mm_peers_range_end(const std::string& prefix);

/// Build the range-end for watching all peer keys (with sharding).
std::string mm_peers_range_end(const std::string& prefix,
                               const std::string& shard_id);

// ── PeerRegistry ──────────────────────────────────────────────────────────────

class PeerRegistry {
public:
    /// The registry is taken by reference and is **not** defaulted, for the reason
    /// `FailoverManager`'s is (#112): a default would let every construction site leave the
    /// counter below unfed, which is #117 exactly - and #117 is why the counter is here at all.
    /// Three sites, one of them production.
    explicit PeerRegistry(CoordinatorConfig config, uint16_t local_node_id,
                          const std::string& replication_address,
                          MetricsRegistry& registry,
                          const std::string& shard_id = "");
    ~PeerRegistry();

    PeerRegistry(const PeerRegistry&) = delete;
    PeerRegistry& operator=(const PeerRegistry&) = delete;

    /// Register this node in etcd (with lease).
    ///
    /// Called once at start, and again by `lease_loop()` only when the lease is gone **and** the
    /// key is confirmed absent (#132). The status is remembered so the second call says what the
    /// first one said.
    bool register_self(const std::string& status = "active");

    /// Deregister this node from etcd.
    bool deregister_self();

    /// Get all known peers (excluding self).
    std::vector<PeerInfo> get_peers() const;

    /// Get a specific peer by node_id.
    std::optional<PeerInfo> get_peer(uint16_t node_id) const;

    /// Start watching for topology changes.
    void start_watch(TopologyChangeCallback cb);

    /// Stop watching.
    void stop_watch();

    /// Refresh lease (keep-alive).  Returns false if lease expired.
    bool refresh_lease();

    /// Get the lease TTL remaining.
    int64_t lease_ttl_remaining() const;

private:
    CoordinatorConfig config_;
    uint16_t local_node_id_;
    std::string replication_address_;
    std::string shard_id_;

    std::unique_ptr<CoordinatorClient> coordinator_;
    MetricsRegistry& registry_;
    int64_t lease_id_{0};

    /// What `register_self()` last wrote as this node's status, so a re-registration says the same
    /// thing rather than a hardcoded default. `register_self()` is the only writer of this key
    /// there has ever been, so this is the only status the registry has ever held for us - which is
    /// why it is one string and not a pair.
    std::string registered_status_{"active"};

    mutable std::mutex mtx_;
    std::unordered_map<uint16_t, PeerInfo> peers_;

    std::thread watch_thread_;
    std::thread lease_thread_;
    std::atomic<bool> running_{false};

    /// What makes the lease loop's wait interruptible.
    ///
    /// That loop slept `max(1, lease_ttl/3)` **seconds** in a plain `sleep_for`, and
    /// `stop_watch()` joins it — so shutdown waited out whatever remained of the current sleep.
    /// Measured on this machine with nothing connected: a standalone node exits in 0.22 s, a mesh
    /// node with the default 10 s TTL in 2.94 s, and one with a 30 s TTL in 4.02 s, which is the
    /// remainder of a 10 s sleep entered six seconds earlier — so the cost is the rest of the
    /// current sleep and only its bound is a function of the TTL (#129).
    ///
    /// The same shape as `Engine::flush_loop()`, whose comment says why a `sleep_for` cannot be
    /// interrupted by `join()`, and as the mesh's `wakeup_fd_`. Third place, same answer.
    std::mutex              lease_stop_mtx_;
    std::condition_variable lease_stop_cv_;
    TopologyChangeCallback change_cb_;

    /// Whether the lease loop is in an episode of caught exceptions.
    ///
    /// Touched only by the lease thread, which is what `LogEpisode` asks of its users.
    LogEpisode lease_errors_;

    /// Whether the lease is currently *refusing* to refresh, which is a different condition from
    /// the one above: it threw, against it said no. Loud-once is still right, and the reason
    /// changed with #132 rather than going away: of the three answers `read_self_key()` can give,
    /// two leave the refusal **permanent** (the key is still there, or etcd cannot be reached) and
    /// only `Absent` ends it — by granting a new lease, which is what the recovery line reports.
    /// Measured before this existed: eleven WARN lines in 33 s, one per interval, unbounded
    /// (#133).
    LogEpisode lease_refusals_;

    /// Whether the topology poll is currently unable to run because no coordinator endpoint has
    /// answered. Loud once, for the reason the two above are: the condition lasts as long as the
    /// coordinator is unreachable, and this loop polls every 100 ms (#133).
    ///
    /// Touched only by the watch thread.
    LogEpisode no_endpoint_;

    /// Whether this node's own entry is in the registry. **Three answers, not two**, for the
    /// reason #82 gave `read_leader()` the same shape one class away: a read that failed and a key
    /// that is gone ask for opposite things, and a `bool` makes them the same answer.
    ///
    /// `Unavailable` is what keeps the recovery below from flooding. While etcd is unreachable the
    /// answer is never `Absent`, so no grant is attempted and no line is written - the condition
    /// is already reported once by the episode above.
    enum class SelfKey { Present, Absent, Unavailable };
    SelfKey read_self_key() const;

    /// One POST to etcd's HTTP gateway, which is the only door this class uses.
    ///
    /// Extracted when `read_self_key()` needed a third copy of the same twenty lines; the two it
    /// replaced differed only in their timeout, and a third copy is how the three of them would
    /// have come to disagree about anything else.
    bool etcd_post(const std::string& url, const std::string& body, long timeout_seconds,
                   std::string& out) const;

    void watch_loop();
    void lease_loop();
    std::string build_key() const;
    std::string build_prefix() const;
};

} // namespace ob
