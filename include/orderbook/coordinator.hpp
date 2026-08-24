#pragma once

// ── CoordinatorClient — thin etcd v3 REST client ─────────────────────────────
//
// Provides lease management, key-value operations, and leader watching against
// an etcd v3 REST gateway using libcurl.  Used by FailoverManager for leader
// election and cluster topology discovery.

#include "orderbook/epoch.hpp"

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace ob {

// ── Configuration ────────────────────────────────────────────────────────────

struct CoordinatorConfig {
    std::vector<std::string> endpoints;   // e.g. {"http://etcd1:2379"}
    int64_t  lease_ttl_seconds{10};
    std::string node_id;                  // unique node identifier
    std::string cluster_prefix{"/ob/"};   // etcd key prefix
};

// ── Cluster state stored in etcd ─────────────────────────────────────────────

struct ClusterState {
    std::string leader_node_id;
    std::string leader_address;   // host:port of current primary
    EpochValue  epoch;
    int64_t     lease_id{0};

    /// Serialize to JSON string (deterministic alphabetical field ordering).
    std::string to_json() const;

    /// Parse from JSON string.  Returns true on success.
    static bool from_json(std::string_view json, ClusterState& out);
};

// ── WAL position published by replicas during election ───────────────────────

struct PublishedPosition {
    std::string node_id;
    uint32_t    wal_file_index{0};
    size_t      wal_byte_offset{0};

    /// Serialize to JSON string.
    std::string to_json() const;

    /// Parse from JSON string.  Returns true on success.
    static bool from_json(std::string_view json, PublishedPosition& out);
};

// ── Handover intent: announced target of a graceful failover ─────────────────

/// Announcement that the current primary is handing its role to a named node.
///
/// The outgoing primary cannot install the successor itself: the leader key is
/// held under the successor's lease, and a lease belongs to its own session. So
/// it publishes this intent, then revokes its lease and steps aside. Replicas
/// read it while the leader key is empty: the named target campaigns
/// immediately, everyone else waits until the deadline passes.
///
/// Stored without a lease, so that it survives the revocation that follows it.
/// After `deadline_ns` it is ignored, so an unreachable target cannot deadlock
/// the cluster, and a stale key does no harm.
struct HandoverIntent {
    std::string target_node_id;   // node that should take over
    std::string from_node_id;     // node handing the role away
    uint64_t    deadline_ns{0};   // wall clock; intent is void afterwards

    /// Serialize to JSON string (deterministic alphabetical field ordering).
    std::string to_json() const;

    /// Parse from JSON string.  Returns true on success.
    static bool from_json(std::string_view json, HandoverIntent& out);

    /// Whether the intent still applies, against a wall-clock reading.
    ///
    /// Clocks differ between nodes, so this is a hint rather than a guarantee:
    /// the worst case is a shorter or longer preference window. Promotion still
    /// goes through a CAS on the leader key, so a skewed clock cannot produce
    /// two primaries.
    bool is_active(uint64_t now_ns) const;
};

// ── Callback for lease expiry / leader change events ─────────────────────────

using LeaseEventCallback = std::function<void(const ClusterState&)>;

// ── etcd key layout helpers ──────────────────────────────────────────────────

/// Build the leader key path:  <prefix>leader
std::string coordinator_leader_key(const std::string& prefix);

/// Build the epoch key path:   <prefix>epoch
std::string coordinator_epoch_key(const std::string& prefix);

/// Build a per-node key path:  <prefix>nodes/<node_id>
std::string coordinator_node_key(const std::string& prefix,
                                 const std::string& node_id);

/// Build the range-end for all node keys (prefix + "nodes" + '\x01').
std::string coordinator_nodes_range_end(const std::string& prefix);

/// Build the handover intent key path:  <prefix>handover
std::string coordinator_handover_key(const std::string& prefix);

// ── Base64 helpers (etcd v3 REST requires base64 keys/values) ────────────────

std::string base64_encode(const std::string& input);
std::string base64_decode(const std::string& input);

// ── CoordinatorClient ────────────────────────────────────────────────────────

class CoordinatorClient {
public:
    explicit CoordinatorClient(CoordinatorConfig config);
    ~CoordinatorClient();

    CoordinatorClient(const CoordinatorClient&) = delete;
    CoordinatorClient& operator=(const CoordinatorClient&) = delete;

    /// Connect to etcd (tries each endpoint in order).  Returns true on success.
    bool connect();

    /// Disconnect from etcd.
    void disconnect();

    /// Check if connected to etcd.
    bool is_connected() const;

    /// Grant a lease with the configured TTL.  Returns lease_id or 0 on failure.
    int64_t grant_lease();

    /// Refresh (keep-alive) an existing lease.  Returns true on success.
    bool refresh_lease(int64_t lease_id);

    /// Revoke a lease explicitly.  Returns true on success.
    bool revoke_lease(int64_t lease_id);

    /// Try to acquire leadership via CAS on the leader key.
    /// Writes node_id + address + epoch under the lease.
    /// Returns true if this node became leader.
    bool try_acquire_leadership(int64_t lease_id, const EpochValue& epoch,
                                const std::string& address);

    /// Read current cluster state from etcd.
    std::optional<ClusterState> get_cluster_state();

    /// Publish this node's WAL position for election comparison.
    /// Publish this node's WAL position.
    ///
    /// `lease_id` ties the key to a lease, so the position disappears on its own when this node
    /// stops refreshing it. That is what makes "is that replica further ahead, or merely dead?"
    /// answerable without comparing wall clocks across machines (#72). Passing 0 publishes a key
    /// that outlives the node, which is the behaviour this parameter replaced.
    bool publish_wal_position(uint32_t file_index, size_t byte_offset, int64_t lease_id = 0);

    /// Read all published WAL positions.
    std::vector<PublishedPosition> get_published_positions();

    /// Publish a graceful-failover handover intent.
    ///
    /// Written WITHOUT a lease on purpose: the outgoing primary revokes its
    /// lease immediately afterwards, and a leased key would disappear with it.
    /// Returns true on success.
    bool publish_handover_intent(const HandoverIntent& intent);

    /// Read the current handover intent.
    /// Returns nullopt when the key is absent or its contents do not parse.
    std::optional<HandoverIntent> get_handover_intent();

    /// Delete the handover intent key.
    /// Returns true when the key is gone afterwards, including when it was
    /// never there.
    bool clear_handover_intent();

    /// Start watching the leader key for changes.  Calls cb on change.
    void watch_leader(LeaseEventCallback cb);

    /// Stop watching.
    void stop_watch();

private:
    CoordinatorConfig config_;
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

} // namespace ob
