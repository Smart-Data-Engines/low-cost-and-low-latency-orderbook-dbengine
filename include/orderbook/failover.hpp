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
    STANDALONE   = 0,
    PRIMARY      = 1,
    REPLICA      = 2,
    MULTI_MASTER = 3,
};

// ── Failover configuration ──────────────────────────────────────────────────

struct FailoverConfig {
    CoordinatorConfig coordinator;
    bool              failover_enabled{true};
    std::string       replication_address;  // host:port for replication
    uint16_t          replication_port{0};

    /// How long the named successor gets to take over during a graceful
    /// failover, before the cluster falls back to an ordinary election.
    /// Shorter than the lease TTL, so a handover completes faster than a
    /// failure would be detected.
    int64_t handover_grace_seconds{5};

    /// How long the outgoing primary refrains from standing for election after
    /// giving up the role. Must be >= handover_grace_seconds, otherwise the
    /// node could win the very race it announced. Longer than the lease TTL, so
    /// it does not come back before the new primary has settled.
    int64_t handover_cooldown_seconds{15};
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

    /// Set node role externally (used by Engine to set MULTI_MASTER).
    void set_role(NodeRole role);

    /// Get current epoch.
    EpochValue epoch() const;

    /// Outcome of an attempted graceful failover.
    ///
    /// Distinguishing the causes matters to the operator: "unknown target"
    /// usually means a typo in a node id, while "coordinator error" means the
    /// node is still primary and the handover never started.
    enum class HandoverResult {
        OK,                 ///< intent published, lease revoked, role given up
        NOT_PRIMARY,        ///< this node is not the primary
        NOT_CONFIGURED,     ///< no coordinator, or no lease held
        INVALID_TARGET,     ///< target empty, or naming this node itself
        UNKNOWN_TARGET,     ///< target not known to the coordinator
        COORDINATOR_ERROR,  ///< could not publish the intent; still primary
    };

    /// Hand the primary role to a named node.
    ///
    /// Publishes a handover intent, blocks itself from standing for election for
    /// handover_cooldown_seconds, then revokes its lease so the target can take
    /// over. Only works if we are PRIMARY.
    ///
    /// On anything other than OK the node keeps its role and its lease, so a
    /// rejected handover is not a partial one.
    HandoverResult initiate_graceful_failover(const std::string& target_node_id);

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

    /// Until when this node declines to stand for election, after handing the
    /// role away. steady_clock, because this measures elapsed time locally and
    /// must not be affected by wall-clock adjustments.
    std::chrono::steady_clock::time_point election_blocked_until_{};

    std::thread             monitor_thread_;
    std::chrono::steady_clock::time_point last_position_publish_{};
    /// The primary address this node has told the engine to follow, so a leader change is
    /// adopted once and an unchanged leader does not restart replication every second.
    std::string             adopted_primary_address_;
    std::atomic<bool>       running_{false};

    void monitor_loop();

    /// Publish this node's WAL position to the coordinator, at most once per second.
    ///
    /// Nothing did this before: `publish_wal_position()` was called from tests and from one
    /// connectivity check, so `get_published_positions()` was always empty on a real cluster and
    /// `FAILOVER <target>` answered ERR unknown_target every time (roadmap #60). The positions are
    /// also what `elect_winner()` was written to compare, though nothing calls that yet.
    void publish_position_if_due();
    void handle_lease_expiry();
    void attempt_promotion();
    void handle_primary_lease_lost();

    /// True while a graceful handover intent names another node, so this node
    /// should not compete for the leader key yet.
    bool should_defer_to_handover_target();
    void reconcile_epoch(const ClusterState& state);
};

// ── Election helper (exposed for testing) ────────────────────────────────────

/// Given a set of published positions, return the election winner:
/// highest WAL position (file_index, byte_offset), tie-break by lowest node_id.
/// Returns nullptr if positions is empty.
const PublishedPosition* elect_winner(const std::vector<PublishedPosition>& positions);

} // namespace ob
