#pragma once

// ── ShardCoordinator — server-side shard management ──────────────────────────
//
// Manages shard registration in etcd, maintains the ShardMap, coordinates
// symbol migrations between shards, and handles wire protocol commands
// (SHARD_MAP, SHARD_INFO, MIGRATE).  Runs on every sharded node.

#include "orderbook/coordinator.hpp"
#include "orderbook/shard_map.hpp"

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

namespace ob {

class Engine;  // forward

// ── Shard coordinator configuration ───────────────────────────────────────────

struct ShardCoordinatorConfig {
    std::string shard_id;                    // unique shard identifier
    uint32_t    vnodes{150};                 // --shard-vnodes
    CoordinatorConfig coordinator;           // etcd endpoints, prefix, node_id
};

// ── Callback for shard map changes ────────────────────────────────────────────

using ShardMapChangeCallback = std::function<void(const ShardMap& new_map)>;

// ── ShardCoordinator ──────────────────────────────────────────────────────────

/// Server-side component managing shard registration in etcd,
/// ShardMap maintenance, and symbol migration coordination.
/// Runs on every sharded node.
class ShardCoordinator {
public:
    explicit ShardCoordinator(ShardCoordinatorConfig config, Engine& engine);
    ~ShardCoordinator();

    ShardCoordinator(const ShardCoordinator&) = delete;
    ShardCoordinator& operator=(const ShardCoordinator&) = delete;

    /// Start the coordinator: register shard in etcd, fetch/create ShardMap.
    void start();

    /// Stop the coordinator: deregister shard (if draining complete).
    void stop();

    /// Get the current ShardMap (thread-safe).
    ShardMap shard_map() const;

    /// Get this node's shard_id.
    const std::string& shard_id() const;

    /// Get this shard's status.
    ShardStatus status() const;

    /// Get the number of symbols assigned to this shard.
    size_t local_symbol_count() const;

    /// Initiate migration of a symbol to the target shard.
    /// Returns true if migration was initiated.
    bool initiate_migration(const std::string& symbol_key,
                            const std::string& target_shard_id);

    /// Initiate draining (deregistration) of this shard.
    bool initiate_draining();

    /// Check if a symbol is owned by this shard.
    bool owns_symbol(const std::string& symbol_key) const;

    /// Check if a symbol is currently being migrated.
    bool is_migrating(const std::string& symbol_key) const;

    /// Register a callback for ShardMap changes.
    void on_shard_map_change(ShardMapChangeCallback cb);

    /// Pin a symbol to this shard (exclude from rebalancing).
    bool pin_symbol(const std::string& symbol_key);

    /// Unpin a symbol (restore consistent hashing).
    bool unpin_symbol(const std::string& symbol_key);

    /// Handle SHARD_MAP command — return JSON.
    std::string handle_shard_map_command() const;

    /// Handle SHARD_INFO command — return shard info as TSV.
    std::string handle_shard_info_command() const;

    /// Handle MIGRATE command — initiate migration.
    std::string handle_migrate_command(const std::string& symbol_key,
                                       const std::string& target_shard_id);

    /// Migration metrics (for STATUS).
    struct MigrationMetrics {
        bool        in_progress{false};
        std::string symbol;
        std::string target_shard;
        uint8_t     progress_pct{0};
    };
    MigrationMetrics migration_metrics() const;

    /// Routing error counter (for STATUS).
    uint64_t routing_errors() const;

    /// Increment routing error counter.
    void increment_routing_errors();

private:
    ShardCoordinatorConfig config_;
    Engine& engine_;
    std::unique_ptr<CoordinatorClient> coordinator_;

    mutable std::mutex mtx_;
    ShardMap shard_map_;
    ConsistentHashRing hash_ring_;
    std::atomic<ShardStatus> status_{ShardStatus::JOINING};
    std::atomic<uint64_t> routing_errors_{0};

    ShardMapChangeCallback change_cb_;

    // Background threads
    std::thread watch_thread_;
    std::thread migration_thread_;
    std::atomic<bool> running_{false};

    // Lease
    int64_t lease_id_{0};

    // Shard registration in etcd
    bool register_shard();
    void deregister_shard();

    // Watch loop on shard_map
    void watch_loop();

    // Update ShardMap in etcd (CAS)
    bool update_shard_map(const ShardMap& new_map);

    // Rebalancing after topology change
    void rebalance();

    // Execute symbol migration
    void execute_migration(const std::string& symbol_key,
                           const std::string& target_shard_id);

    // Rollback migration
    void rollback_migration(const std::string& symbol_key);
};

} // namespace ob
