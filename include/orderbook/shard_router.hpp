#pragma once

// ── ShardRouter — client-side shard routing ──────────────────────────────────
//
// Routes operations (INSERT, MINSERT, SELECT, FLUSH) to the correct shard
// based on a locally-cached ShardMap fetched from etcd.  Maintains TCP
// connections to all shards and handles migration retries transparently.

#include "orderbook/client.hpp"
#include "orderbook/coordinator.hpp"
#include "orderbook/shard_map.hpp"

#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>

namespace ob {

// ── Shard router configuration ────────────────────────────────────────────────

struct ShardRouterConfig {
    std::vector<std::string> coordinator_endpoints;  // etcd endpoints
    std::string cluster_prefix{"/ob/"};
    double connect_timeout_sec{5.0};
    double read_timeout_sec{10.0};
    double health_check_interval_sec{2.0};
    bool   compress{false};
};

// ── ShardRouter ───────────────────────────────────────────────────────────────

/// Client-side component responsible for routing operations to the correct
/// shard.  Maintains a local ShardMap copy and TCP connections to all shards.
class ShardRouter {
public:
    explicit ShardRouter(ShardRouterConfig config);
    ~ShardRouter();

    ShardRouter(const ShardRouter&) = delete;
    ShardRouter& operator=(const ShardRouter&) = delete;

    /// Initialize: fetch ShardMap from etcd, build hash ring, connect to shards.
    Result<void> initialize();

    /// Close all connections and stop background threads.
    void close();

    // ── Routing ───────────────────────────────────────────────────────

    /// Resolve the shard owning a symbol.  ~200ns (unordered_map lookup).
    std::string resolve_shard(std::string_view symbol,
                              std::string_view exchange) const;

    /// Get the TCP client for a given shard.  Returns nullptr if unknown.
    OrderbookClient* get_client(const std::string& shard_id);

    // ── Routed operations ─────────────────────────────────────────────

    Result<void> insert(std::string_view symbol, std::string_view exchange,
                        Side side, int64_t price, uint64_t qty,
                        uint32_t count = 1);

    Result<void> minsert(std::string_view symbol, std::string_view exchange,
                         Side side, const Level* levels, size_t n_levels);

    Result<void> flush();

    Result<QueryResult> query(std::string_view sql);

    Result<bool> ping();

    // ── Map access ────────────────────────────────────────────────────

    /// Return a snapshot of the current ShardMap.
    ShardMap shard_map() const;

    /// Force-refresh the ShardMap from etcd.
    Result<void> refresh_shard_map();

private:
    ShardRouterConfig config_;
    std::unique_ptr<CoordinatorClient> coordinator_;

    mutable std::mutex mtx_;
    ShardMap shard_map_;
    ConsistentHashRing hash_ring_;

    /// TCP connections per shard: shard_id → OrderbookClient
    std::unordered_map<std::string, std::unique_ptr<OrderbookClient>> clients_;

    // Background watch thread
    std::thread watch_thread_;
    std::atomic<bool> running_{false};

    // Internal helpers
    void watch_loop();
    void update_connections(const ShardMap& new_map);
    std::string build_symbol_key(std::string_view symbol,
                                 std::string_view exchange) const;

    /// Parse symbol(s) from a SQL query's FROM clause.
    std::vector<std::string> extract_symbols_from_sql(std::string_view sql) const;

    /// Send operation, handle ERR SYMBOL_MIGRATED with one retry.
    template<typename F>
    auto execute_with_migration_retry(const std::string& symbol_key, F&& fn)
        -> decltype(fn(std::declval<OrderbookClient&>()));

    /// Assign an unknown symbol via consistent hashing.
    std::string assign_unknown_symbol(const std::string& symbol_key);
};

} // namespace ob
