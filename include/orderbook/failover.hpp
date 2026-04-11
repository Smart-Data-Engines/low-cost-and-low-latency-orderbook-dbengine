#pragma once

// ── FailoverManager — role transitions and monitoring ────────────────────────
//
// Orchestrates automatic failover using an external coordinator (etcd).
// Runs a background thread that monitors the coordinator lease and triggers
// promotion/demotion as needed.  The Engine implements RoleTransitionHandler
// to perform the actual state changes.

#include "orderbook/coordinator.hpp"
#include "orderbook/epoch.hpp"

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

namespace ob {

class Engine;  // forward

// ── Node role ────────────────────────────────────────────────────────────────

enum class NodeRole : uint8_t {
    STANDALONE = 0,
    PRIMARY    = 1,
    REPLICA    = 2,
};

// ── Failover configuration ──────────────────────────────────────────────────

struct FailoverConfig {
    CoordinatorConfig coordinator;
    bool              failover_enabled{true};
    std::string       replication_address;  // host:port for replication
    uint16_t          replication_port{0};
};

// ── Callback interface for Engine to implement role transitions ──────────────

/// The Engine implements this interface so that FailoverManager can trigger
/// role changes without depending on the full Engine class.
struct RoleTransitionHandler {
    virtual ~RoleTransitionHandler() = default;

    /// Called when this node should become primary.
    /// Must: stop ReplicationClient, increment epoch, write Epoch_Record,
    ///       start ReplicationManager, disable read-only.
    virtual void promote_to_primary(const EpochValue& new_epoch) = 0;

    /// Called when this node should become replica.
    /// Must: stop ReplicationManager, start ReplicationClient, enable read-only.
    virtual void demote_to_replica(const std::string& new_primary_address) = 0;

    /// Called to get current WAL position for election comparison.
    virtual std::pair<uint32_t, size_t> get_wal_position() const = 0;

    /// Called to get current epoch.
    virtual EpochValue get_current_epoch() const = 0;

    /// Called to truncate stale WAL records and re-bootstrap from new primary.
    virtual void truncate_and_rebootstrap(const EpochValue& new_epoch,
                                          const std::string& primary_address) = 0;
};

// ── FailoverManager ─────────────────────────────────────────────────────────

class FailoverManager {
public:
    explicit FailoverManager(FailoverConfig config, RoleTransitionHandler& handler);
    ~FailoverManager();

    FailoverManager(const FailoverManager&) = delete;
    FailoverManager& operator=(const FailoverManager&) = delete;

    /// Start the failover manager (connect to coordinator, begin monitoring).
    void start();

    /// Stop the failover manager.
    void stop();

    /// Get current node role.
    NodeRole role() const;

    /// Get current epoch.
    EpochValue epoch() const;

    /// Initiate graceful failover to a target node.
    /// Only works if we are PRIMARY.  Revokes lease so replicas detect expiry.
    /// Returns true if handover initiated successfully.
    bool initiate_graceful_failover(const std::string& target_node_id);

    /// Get the current primary address (from coordinator).
    std::string primary_address() const;

    /// Get coordinator lease TTL remaining in seconds (for STATUS).
    int64_t lease_ttl_remaining() const;

private:
    FailoverConfig          config_;
    RoleTransitionHandler&  handler_;
    std::unique_ptr<CoordinatorClient> coordinator_;

    std::atomic<NodeRole>   role_{NodeRole::STANDALONE};
    mutable std::mutex      mtx_;
    EpochValue              epoch_;
    std::atomic<int64_t>    lease_id_{0};
    std::string             primary_address_;
    std::chrono::steady_clock::time_point last_lease_refresh_;

    std::thread             monitor_thread_;
    std::atomic<bool>       running_{false};

    void monitor_loop();
    void handle_lease_expiry();
    void attempt_promotion();
    void handle_primary_lease_lost();
    void reconcile_epoch(const ClusterState& state);
};

// ── Election helper (exposed for testing) ────────────────────────────────────

/// Given a set of published positions, return the election winner:
/// highest WAL position (file_index, byte_offset), tie-break by lowest node_id.
/// Returns nullptr if positions is empty.
const PublishedPosition* elect_winner(const std::vector<PublishedPosition>& positions);

} // namespace ob
