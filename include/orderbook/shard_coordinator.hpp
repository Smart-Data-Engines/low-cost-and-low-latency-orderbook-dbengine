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
    /// "host:port" the shard's clients connect to - what the map says of it (#175): --advertise-host
    /// and --port.
    std::string advertise_address;
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

    /// The target's half of a migration (#196), sent by the shard the symbol moves from. BEGIN
    /// adopts it - this shard takes its writes from the connection that sent BEGIN, although the
    /// map still names the source - when this node holds none of its rows, so a migration tried
    /// again stores nothing twice; `*adoption` is then the adoption's number, which that connection
    /// keeps (`Session::set_adoption()`). END says the map names this shard now: from it the
    /// symbol's writes are taken from any connection, and the adoption ends once the map this shard
    /// reads says so. ABANDON ends an adoption END has not, and drops what was adopted - or, with
    /// none, what a migration that failed left of a symbol this shard does not own.
    std::string handle_adopt_command(const std::string& symbol_key, const std::string& action,
                                     const std::string& source_shard_id,
                                     uint64_t* adoption = nullptr);

    /// Whether the symbol is being adopted here (#196).
    bool is_adopting(const std::string& symbol_key) const;

    /// The number of the symbol's adoption, which the connection that began it writes on until END
    /// makes the symbol this shard's (owns_symbol()); 0 without one (#196).
    uint64_t adoption_number(const std::string& symbol_key) const;

    /// Install `map` as a read of etcd would, without etcd: what a test of the map's consequences
    /// needs - this shard's status, its ring, the adoptions a map ends.
    void adopt_map_for_test(ShardMap map) { adopt_map(std::move(map), ++test_map_revision_); }

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
    /// Symbols adopted from another shard (#196): symbol key -> the shard it moves from, the
    /// adoption's number (Engine::begin_adoption()), and whether END came - after it, the entry goes
    /// once a map this shard reads names it. Under mtx_.
    struct Adoption {
        std::string source;
        uint64_t    number{0};
        bool        ended{false};
    };
    std::map<std::string, Adoption> adopting_;
    /// One ADOPT at a time (#196): each is a check and then an act. Its own lock rather than the
    /// server's for commands that run alone (see serialised_across_reactors()), and taken before mtx_.
    std::mutex adopt_command_mtx_;
    int64_t test_map_revision_{0};   ///< see adopt_map_for_test()
    std::atomic<ShardStatus> status_{ShardStatus::JOINING};
    std::atomic<uint64_t> routing_errors_{0};

    ShardMapChangeCallback change_cb_;

    // Background threads
    std::thread watch_thread_;
    std::thread migration_thread_;
    std::atomic<bool> running_{false};

    // Lease
    int64_t lease_id_{0};

    /// In the map in etcd, with its node key under the lease (#175). Touched by start() and then only
    /// by the watch thread.
    bool joined_{false};
    /// The revision of the map this shard last took, so a poll that finds it again does nothing.
    /// Guarded by mtx_.
    int64_t map_revision_{0};
    /// Said once until a registration succeeds: a shard etcd cannot reach retries every pass.
    bool join_warned_{false};

    // Shard registration in etcd
    bool register_shard();
    void deregister_shard();

    /// Connect, take a lease, write the node key and join the map; what start() tries once and the
    /// watch loop until it succeeds.
    bool connect_and_join();
    /// Read the map, put this shard in it, and write it back only on the revision read - read again
    /// on a conflict (#175). A replica of the shard's group takes the map as it is: its primary is
    /// the address the map names.
    bool join_map();
    /// Take `map` as this shard's: ring, ownership, status, callback.
    void adopt_map(ShardMap map, int64_t revision);
    /// Read the map, and take it when it changed; join it again when it is gone or says something
    /// else of this shard than this node would.
    void poll_map();
    /// This shard as it says itself in the map.
    ShardNode self_node() const;
    /// Whether this node is the one the map names for its shard: not a replica (#175).
    bool advertises() const;

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

    // Propagate multi-master topology from etcd to ShardMap
    void propagate_mm_topology();
};

} // namespace ob
