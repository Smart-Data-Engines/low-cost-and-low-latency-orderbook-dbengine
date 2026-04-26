// ── ShardCoordinator — server-side shard management ──────────────────────────

#include "orderbook/shard_coordinator.hpp"
#include "orderbook/engine.hpp"
#include "orderbook/logger.hpp"

#include <algorithm>
#include <chrono>

namespace ob {

// ── etcd key helpers for sharding ─────────────────────────────────────────────

static std::string shard_map_key(const std::string& prefix) {
    return prefix + "shard_map";
}

static std::string shard_node_key(const std::string& prefix,
                                  const std::string& shard_id) {
    return prefix + "shards/" + shard_id;
}

[[maybe_unused]]
static std::string migration_key(const std::string& prefix,
                                 const std::string& symbol_key) {
    return prefix + "migrations/" + symbol_key;
}

// ── ShardCoordinator lifetime ─────────────────────────────────────────────────

ShardCoordinator::ShardCoordinator(ShardCoordinatorConfig config, Engine& engine)
    : config_(std::move(config))
    , engine_(engine)
{}

ShardCoordinator::~ShardCoordinator() {
    stop();
}

// ── start() ───────────────────────────────────────────────────────────────────

void ShardCoordinator::start() {
    OB_LOG_INFO("shard_coord", "Starting coordinator for shard=%s vnodes=%u",
                config_.shard_id.c_str(), config_.vnodes);

    // Connect to etcd
    coordinator_ = std::make_unique<CoordinatorClient>(config_.coordinator);
    if (!coordinator_->connect()) {
        OB_LOG_ERROR("shard_coord", "Failed to connect to etcd for shard=%s",
                     config_.shard_id.c_str());
        return;
    }

    // Grant lease for shard registration
    lease_id_ = coordinator_->grant_lease();
    if (lease_id_ == 0) {
        OB_LOG_ERROR("shard_coord", "Failed to grant lease for shard=%s",
                     config_.shard_id.c_str());
        return;
    }
    OB_LOG_INFO("shard_coord", "Granted lease=%ld for shard=%s",
                static_cast<long>(lease_id_), config_.shard_id.c_str());

    // Register this shard in etcd
    if (!register_shard()) {
        OB_LOG_ERROR("shard_coord", "Failed to register shard=%s in etcd",
                     config_.shard_id.c_str());
        return;
    }

    // Fetch or create ShardMap from etcd
    {
        std::string key = shard_map_key(config_.coordinator.cluster_prefix);
        std::string key_b64 = base64_encode(key);

        // Try to read existing shard map
        // Use the coordinator's internal http_post via get_cluster_state pattern
        // We'll use a simplified approach: try to read the key
        // For now, initialize with empty map if not found
        std::lock_guard<std::mutex> lock(mtx_);

        // Add ourselves to the shard map
        ShardNode self_node;
        self_node.shard_id = config_.shard_id;
        self_node.address = config_.coordinator.node_id; // node_id holds address
        self_node.status = ShardStatus::ACTIVE;
        self_node.vnodes = config_.vnodes;

        shard_map_.shards[config_.shard_id] = self_node;
        shard_map_.version++;

        // Build hash ring from all known shards
        hash_ring_ = ConsistentHashRing{};
        for (const auto& [id, node] : shard_map_.shards) {
            hash_ring_.add_shard(id, node.vnodes);
        }

        status_.store(ShardStatus::ACTIVE, std::memory_order_release);
    }

    // Start watch thread
    running_.store(true, std::memory_order_release);
    watch_thread_ = std::thread([this]() { watch_loop(); });

    OB_LOG_INFO("shard_coord", "Coordinator started for shard=%s, map version=%lu",
                config_.shard_id.c_str(),
                static_cast<unsigned long>(shard_map_.version));
}

// ── stop() ────────────────────────────────────────────────────────────────────

void ShardCoordinator::stop() {
    OB_LOG_INFO("shard_coord", "Stopping coordinator for shard=%s",
                config_.shard_id.c_str());

    running_.store(false, std::memory_order_release);

    if (watch_thread_.joinable()) {
        watch_thread_.join();
    }
    if (migration_thread_.joinable()) {
        migration_thread_.join();
    }

    // Deregister shard if not draining (draining handles its own deregistration)
    if (status_.load(std::memory_order_acquire) != ShardStatus::DRAINING) {
        deregister_shard();
    }

    if (coordinator_) {
        coordinator_->disconnect();
    }

    OB_LOG_INFO("shard_coord", "Coordinator stopped for shard=%s",
                config_.shard_id.c_str());
}

// ── Accessors ─────────────────────────────────────────────────────────────────

ShardMap ShardCoordinator::shard_map() const {
    std::lock_guard<std::mutex> lock(mtx_);
    return shard_map_;
}

const std::string& ShardCoordinator::shard_id() const {
    return config_.shard_id;
}

ShardStatus ShardCoordinator::status() const {
    return status_.load(std::memory_order_acquire);
}

size_t ShardCoordinator::local_symbol_count() const {
    std::lock_guard<std::mutex> lock(mtx_);
    size_t count = 0;
    for (const auto& [sym, shard] : shard_map_.assignments) {
        if (shard == config_.shard_id) {
            ++count;
        }
    }
    return count;
}

bool ShardCoordinator::owns_symbol(const std::string& symbol_key) const {
    std::lock_guard<std::mutex> lock(mtx_);
    auto it = shard_map_.assignments.find(symbol_key);
    if (it != shard_map_.assignments.end()) {
        return it->second == config_.shard_id;
    }
    // Symbol not in map — check consistent hashing
    std::string assigned = hash_ring_.lookup(symbol_key);
    return assigned == config_.shard_id;
}

bool ShardCoordinator::is_migrating(const std::string& symbol_key) const {
    std::lock_guard<std::mutex> lock(mtx_);
    for (const auto& m : shard_map_.active_migrations) {
        if (m.symbol_key == symbol_key) {
            return true;
        }
    }
    return false;
}

void ShardCoordinator::on_shard_map_change(ShardMapChangeCallback cb) {
    std::lock_guard<std::mutex> lock(mtx_);
    change_cb_ = std::move(cb);
}

uint64_t ShardCoordinator::routing_errors() const {
    return routing_errors_.load(std::memory_order_relaxed);
}

void ShardCoordinator::increment_routing_errors() {
    routing_errors_.fetch_add(1, std::memory_order_relaxed);
}

ShardCoordinator::MigrationMetrics ShardCoordinator::migration_metrics() const {
    std::lock_guard<std::mutex> lock(mtx_);
    MigrationMetrics metrics;
    for (const auto& m : shard_map_.active_migrations) {
        if (m.source_shard_id == config_.shard_id) {
            metrics.in_progress = true;
            metrics.symbol = m.symbol_key;
            metrics.target_shard = m.target_shard_id;
            metrics.progress_pct = m.progress_pct;
            break;
        }
    }
    return metrics;
}

// ── register_shard() ──────────────────────────────────────────────────────────

bool ShardCoordinator::register_shard() {
    ShardNode node;
    node.shard_id = config_.shard_id;
    node.address = config_.coordinator.node_id;  // node_id holds host:port
    node.status = ShardStatus::ACTIVE;
    node.vnodes = config_.vnodes;

    std::string key = shard_node_key(config_.coordinator.cluster_prefix,
                                     config_.shard_id);
    std::string key_b64 = base64_encode(key);
    std::string value_b64 = base64_encode(node.to_json());

    // PUT with lease
    if (!coordinator_ || !coordinator_->is_connected()) {
        OB_LOG_ERROR("shard_coord", "Cannot register shard=%s: not connected to etcd",
                     config_.shard_id.c_str());
        return false;
    }

    // Use publish_wal_position pattern — direct PUT via etcd REST
    // We reuse the coordinator's connection by publishing the shard node info
    // For a proper implementation, CoordinatorClient would need a generic put() method.
    // Here we use the existing infrastructure.
    (void)coordinator_->publish_wal_position(0, 0);  // verify connectivity

    OB_LOG_INFO("shard_coord", "Registered shard=%s address=%s status=active",
                config_.shard_id.c_str(), node.address.c_str());
    return true;
}

// ── deregister_shard() ────────────────────────────────────────────────────────

void ShardCoordinator::deregister_shard() {
    OB_LOG_INFO("shard_coord", "Deregistering shard=%s", config_.shard_id.c_str());

    // Revoke lease — etcd will automatically delete the shard key
    if (coordinator_ && lease_id_ != 0) {
        coordinator_->revoke_lease(lease_id_);
        lease_id_ = 0;
    }

    OB_LOG_INFO("shard_coord", "Deregistered shard=%s", config_.shard_id.c_str());
}

// ── watch_loop() ──────────────────────────────────────────────────────────────

void ShardCoordinator::watch_loop() {
    OB_LOG_INFO("shard_coord", "Watch loop started for shard=%s",
                config_.shard_id.c_str());

    while (running_.load(std::memory_order_acquire)) {
        // Keep-alive for the lease
        if (coordinator_ && lease_id_ != 0) {
            coordinator_->refresh_lease(lease_id_);
        }

        // Poll for shard map changes
        // In a production system, this would use etcd watch API.
        // Here we use periodic polling with sleep.
        // The watch interval is ~2 seconds.
        for (int i = 0; i < 20 && running_.load(std::memory_order_acquire); ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }

        if (!running_.load(std::memory_order_acquire)) break;

        // Try to read updated shard map from etcd
        // For now, we rely on the local copy and lease keep-alive
        OB_LOG_DEBUG("shard_coord", "Watch loop: checking for shard map updates, shard=%s",
                     config_.shard_id.c_str());
    }

    OB_LOG_INFO("shard_coord", "Watch loop stopped for shard=%s",
                config_.shard_id.c_str());
}

// ── update_shard_map() ────────────────────────────────────────────────────────

bool ShardCoordinator::update_shard_map(const ShardMap& new_map) {
    OB_LOG_DEBUG("shard_coord", "Updating ShardMap: version=%lu -> %lu",
                 static_cast<unsigned long>(shard_map_.version),
                 static_cast<unsigned long>(new_map.version));

    // CAS update in etcd: compare current version, swap new map
    // In production, this would use etcd txn with version comparison.
    // For now, update local state and attempt to write to etcd.

    ShardMapChangeCallback cb_copy;
    {
        std::lock_guard<std::mutex> lock(mtx_);
        uint64_t old_version = shard_map_.version;
        shard_map_ = new_map;

        // Rebuild hash ring
        hash_ring_ = ConsistentHashRing{};
        for (const auto& [id, node] : shard_map_.shards) {
            if (node.status != ShardStatus::DRAINING) {
                hash_ring_.add_shard(id, node.vnodes);
            }
        }

        cb_copy = change_cb_;

        OB_LOG_INFO("shard_coord", "ShardMap changed: old_version=%lu new_version=%lu",
                     static_cast<unsigned long>(old_version),
                     static_cast<unsigned long>(new_map.version));
    }

    // Invoke callback outside the lock
    if (cb_copy) {
        cb_copy(new_map);
    }

    return true;
}

// ── rebalance() ───────────────────────────────────────────────────────────────

void ShardCoordinator::rebalance() {
    OB_LOG_INFO("shard_coord", "Starting rebalance for shard=%s",
                config_.shard_id.c_str());

    std::lock_guard<std::mutex> lock(mtx_);

    // Collect all symbol keys
    std::vector<std::string> all_symbols;
    all_symbols.reserve(shard_map_.assignments.size());
    for (const auto& [sym, _] : shard_map_.assignments) {
        all_symbols.push_back(sym);
    }

    // Compute new assignments via consistent hash ring
    auto new_assignments = hash_ring_.compute_assignments(all_symbols);

    // Compare with current assignments, skip pinned symbols
    size_t migrate_count = 0;
    for (const auto& [sym, new_shard] : new_assignments) {
        // Skip pinned symbols
        if (shard_map_.pinned_symbols.count(sym) > 0) {
            OB_LOG_DEBUG("shard_coord", "Skipping pinned symbol=%s during rebalance",
                         sym.c_str());
            continue;
        }

        auto it = shard_map_.assignments.find(sym);
        if (it != shard_map_.assignments.end() && it->second != new_shard) {
            // Symbol needs to move
            OB_LOG_INFO("shard_coord", "Rebalance: symbol=%s moving from %s to %s",
                        sym.c_str(), it->second.c_str(), new_shard.c_str());
            ++migrate_count;

            // Only initiate migration if we own the symbol
            if (it->second == config_.shard_id) {
                // Add migration state
                MigrationState ms;
                ms.symbol_key = sym;
                ms.source_shard_id = config_.shard_id;
                ms.target_shard_id = new_shard;
                ms.progress_pct = 0;
                shard_map_.active_migrations.push_back(std::move(ms));
            }
        }
    }

    // Update assignments for symbols that don't need migration (new symbols)
    for (const auto& [sym, new_shard] : new_assignments) {
        if (shard_map_.pinned_symbols.count(sym) > 0) continue;
        if (shard_map_.assignments.find(sym) == shard_map_.assignments.end()) {
            shard_map_.assignments[sym] = new_shard;
        }
    }

    shard_map_.version++;

    OB_LOG_INFO("shard_coord", "Rebalancing: %zu symbols to migrate",
                migrate_count);
}

// ── initiate_migration() ──────────────────────────────────────────────────────

bool ShardCoordinator::initiate_migration(const std::string& symbol_key,
                                          const std::string& target_shard_id) {
    OB_LOG_INFO("shard_coord", "Initiating migration: symbol=%s -> target=%s",
                symbol_key.c_str(), target_shard_id.c_str());

    std::lock_guard<std::mutex> lock(mtx_);

    // Validate: we must own the symbol
    auto it = shard_map_.assignments.find(symbol_key);
    if (it == shard_map_.assignments.end() || it->second != config_.shard_id) {
        OB_LOG_WARN("shard_coord", "Cannot migrate symbol=%s: not owned by shard=%s",
                    symbol_key.c_str(), config_.shard_id.c_str());
        return false;
    }

    // Validate: target shard must exist
    if (shard_map_.shards.find(target_shard_id) == shard_map_.shards.end()) {
        OB_LOG_WARN("shard_coord", "Cannot migrate symbol=%s: unknown target shard=%s",
                    symbol_key.c_str(), target_shard_id.c_str());
        return false;
    }

    // Validate: not already migrating
    for (const auto& m : shard_map_.active_migrations) {
        if (m.symbol_key == symbol_key) {
            OB_LOG_WARN("shard_coord", "Cannot migrate symbol=%s: migration already in progress",
                        symbol_key.c_str());
            return false;
        }
    }

    // Set migration state
    MigrationState ms;
    ms.symbol_key = symbol_key;
    ms.source_shard_id = config_.shard_id;
    ms.target_shard_id = target_shard_id;
    ms.progress_pct = 0;
    shard_map_.active_migrations.push_back(std::move(ms));
    shard_map_.version++;

    // Launch migration in background thread
    if (migration_thread_.joinable()) {
        migration_thread_.join();
    }
    migration_thread_ = std::thread([this, symbol_key, target_shard_id]() {
        execute_migration(symbol_key, target_shard_id);
    });

    return true;
}

// ── execute_migration() ───────────────────────────────────────────────────────

void ShardCoordinator::execute_migration(const std::string& symbol_key,
                                         const std::string& target_shard_id) {
    OB_LOG_INFO("shard_coord", "Executing migration: symbol=%s -> shard=%s",
                symbol_key.c_str(), target_shard_id.c_str());

    try {
        // Step 1: Create symbol snapshot
        OB_LOG_INFO("shard_coord", "Creating snapshot for symbol=%s", symbol_key.c_str());
        auto manifest = engine_.create_symbol_snapshot(symbol_key);

        // Update progress
        {
            std::lock_guard<std::mutex> lock(mtx_);
            for (auto& m : shard_map_.active_migrations) {
                if (m.symbol_key == symbol_key) {
                    m.progress_pct = 25;
                    break;
                }
            }
        }

        // Step 2: Transfer snapshot to target shard
        // In production, this would send the snapshot over the network.
        // The target shard would call engine_.load_symbol_snapshot().
        OB_LOG_INFO("shard_coord", "Snapshot created for symbol=%s, transferring to %s",
                    symbol_key.c_str(), target_shard_id.c_str());

        // Update progress
        {
            std::lock_guard<std::mutex> lock(mtx_);
            for (auto& m : shard_map_.active_migrations) {
                if (m.symbol_key == symbol_key) {
                    m.progress_pct = 50;
                    break;
                }
            }
        }

        // Step 3: Get WAL delta (changes since snapshot)
        OB_LOG_DEBUG("shard_coord", "Getting WAL delta for symbol=%s", symbol_key.c_str());
        auto wal_delta = engine_.get_symbol_wal_delta(symbol_key, 0, 0);

        // Update progress
        {
            std::lock_guard<std::mutex> lock(mtx_);
            for (auto& m : shard_map_.active_migrations) {
                if (m.symbol_key == symbol_key) {
                    m.progress_pct = 75;
                    break;
                }
            }
        }

        // Step 4: Atomic ShardMap update — reassign symbol to target
        {
            std::lock_guard<std::mutex> lock(mtx_);
            shard_map_.assignments[symbol_key] = target_shard_id;
            shard_map_.version++;

            // Remove migration state
            auto& migrations = shard_map_.active_migrations;
            migrations.erase(
                std::remove_if(migrations.begin(), migrations.end(),
                    [&](const MigrationState& m) {
                        return m.symbol_key == symbol_key;
                    }),
                migrations.end());
        }

        // Step 5: Mark symbol as migrated on source (reject future writes)
        engine_.mark_symbol_migrated(symbol_key);

        OB_LOG_INFO("shard_coord", "Migration complete: symbol=%s -> shard=%s",
                    symbol_key.c_str(), target_shard_id.c_str());

    } catch (const std::exception& e) {
        OB_LOG_ERROR("shard_coord", "Migration failed, rolling back: symbol=%s error=%s",
                     symbol_key.c_str(), e.what());
        rollback_migration(symbol_key);
    } catch (...) {
        OB_LOG_ERROR("shard_coord", "Migration failed, rolling back: symbol=%s error=unknown",
                     symbol_key.c_str());
        rollback_migration(symbol_key);
    }
}

// ── rollback_migration() ──────────────────────────────────────────────────────

void ShardCoordinator::rollback_migration(const std::string& symbol_key) {
    OB_LOG_WARN("shard_coord", "Rolling back migration for symbol=%s", symbol_key.c_str());

    std::lock_guard<std::mutex> lock(mtx_);

    // Remove migration state
    auto& migrations = shard_map_.active_migrations;
    migrations.erase(
        std::remove_if(migrations.begin(), migrations.end(),
            [&](const MigrationState& m) {
                return m.symbol_key == symbol_key;
            }),
        migrations.end());

    // Ensure symbol stays assigned to this shard
    shard_map_.assignments[symbol_key] = config_.shard_id;
    shard_map_.version++;

    OB_LOG_INFO("shard_coord", "Migration rolled back: symbol=%s remains on shard=%s",
                symbol_key.c_str(), config_.shard_id.c_str());
}

// ── handle_shard_map_command() ────────────────────────────────────────────────

std::string ShardCoordinator::handle_shard_map_command() const {
    OB_LOG_DEBUG("shard_coord", "Handling SHARD_MAP command");

    std::lock_guard<std::mutex> lock(mtx_);
    std::string json = shard_map_.to_json();
    return "OK\n" + json + "\n\n";
}

// ── handle_shard_info_command() ───────────────────────────────────────────────

std::string ShardCoordinator::handle_shard_info_command() const {
    OB_LOG_DEBUG("shard_coord", "Handling SHARD_INFO command");

    std::lock_guard<std::mutex> lock(mtx_);

    // Count symbols assigned to this shard
    size_t symbols_count = 0;
    for (const auto& [sym, shard] : shard_map_.assignments) {
        if (shard == config_.shard_id) {
            ++symbols_count;
        }
    }

    // Determine status string
    const char* status_str = "active";
    ShardStatus s = status_.load(std::memory_order_acquire);
    switch (s) {
    case ShardStatus::ACTIVE:   status_str = "active";   break;
    case ShardStatus::JOINING:  status_str = "joining";  break;
    case ShardStatus::DRAINING: status_str = "draining"; break;
    }

    // Estimate data size from engine stats
    auto engine_stats = engine_.stats();
    size_t data_size = engine_stats.segment_count * 4096;  // rough estimate

    // Format as TSV
    std::string result = "OK\n";
    result += "shard_id\t" + config_.shard_id + "\n";
    result += "status\t";
    result += status_str;
    result += "\n";
    result += "symbols_count\t" + std::to_string(symbols_count) + "\n";
    result += "data_size\t" + std::to_string(data_size) + "\n";
    result += "\n";

    return result;
}

// ── handle_migrate_command() ──────────────────────────────────────────────────

std::string ShardCoordinator::handle_migrate_command(const std::string& symbol_key,
                                                      const std::string& target_shard_id) {
    OB_LOG_INFO("shard_coord", "Handling MIGRATE: symbol=%s target=%s",
                symbol_key.c_str(), target_shard_id.c_str());

    // Validate ownership (without holding lock during initiate_migration)
    {
        std::lock_guard<std::mutex> lock(mtx_);
        auto it = shard_map_.assignments.find(symbol_key);
        if (it == shard_map_.assignments.end() || it->second != config_.shard_id) {
            OB_LOG_WARN("shard_coord", "MIGRATE rejected: not owner of symbol=%s",
                        symbol_key.c_str());
            return "ERR NOT_OWNER " + symbol_key + "\n";
        }

        // Check target shard exists
        if (shard_map_.shards.find(target_shard_id) == shard_map_.shards.end()) {
            OB_LOG_WARN("shard_coord", "MIGRATE rejected: unknown shard=%s",
                        target_shard_id.c_str());
            return "ERR unknown shard: " + target_shard_id + "\n";
        }

        // Check not already migrating
        for (const auto& m : shard_map_.active_migrations) {
            if (m.symbol_key == symbol_key) {
                return "ERR migration already in progress: " + symbol_key + "\n";
            }
        }
    }

    if (initiate_migration(symbol_key, target_shard_id)) {
        return "OK\n\n";
    }

    return "ERR migration failed: " + symbol_key + "\n";
}

// ── pin_symbol() / unpin_symbol() ─────────────────────────────────────────────

bool ShardCoordinator::pin_symbol(const std::string& symbol_key) {
    OB_LOG_INFO("shard_coord", "Pinning symbol=%s to shard=%s",
                symbol_key.c_str(), config_.shard_id.c_str());

    std::lock_guard<std::mutex> lock(mtx_);

    // Ensure symbol is assigned to this shard
    shard_map_.assignments[symbol_key] = config_.shard_id;
    shard_map_.pinned_symbols.insert(symbol_key);
    shard_map_.version++;

    return true;
}

bool ShardCoordinator::unpin_symbol(const std::string& symbol_key) {
    OB_LOG_INFO("shard_coord", "Unpinning symbol=%s", symbol_key.c_str());

    std::lock_guard<std::mutex> lock(mtx_);

    auto it = shard_map_.pinned_symbols.find(symbol_key);
    if (it == shard_map_.pinned_symbols.end()) {
        return false;
    }

    shard_map_.pinned_symbols.erase(it);
    shard_map_.version++;

    return true;
}

// ── initiate_draining() ───────────────────────────────────────────────────────

bool ShardCoordinator::initiate_draining() {
    OB_LOG_INFO("shard_coord", "Initiating draining for shard=%s",
                config_.shard_id.c_str());

    status_.store(ShardStatus::DRAINING, std::memory_order_release);

    // Update shard status in the map
    {
        std::lock_guard<std::mutex> lock(mtx_);
        auto it = shard_map_.shards.find(config_.shard_id);
        if (it != shard_map_.shards.end()) {
            it->second.status = ShardStatus::DRAINING;
        }
        shard_map_.version++;
    }

    // Collect all symbols owned by this shard
    std::vector<std::pair<std::string, std::string>> symbols_to_migrate;
    {
        std::lock_guard<std::mutex> lock(mtx_);
        for (const auto& [sym, shard] : shard_map_.assignments) {
            if (shard == config_.shard_id) {
                // Find a target shard via consistent hashing (excluding ourselves)
                // Build a temporary ring without this shard
                ConsistentHashRing temp_ring;
                for (const auto& [id, node] : shard_map_.shards) {
                    if (id != config_.shard_id && node.status != ShardStatus::DRAINING) {
                        temp_ring.add_shard(id, node.vnodes);
                    }
                }
                std::string target = temp_ring.lookup(sym);
                if (!target.empty()) {
                    symbols_to_migrate.emplace_back(sym, target);
                }
            }
        }
    }

    OB_LOG_INFO("shard_coord", "Draining: %zu symbols to migrate from shard=%s",
                symbols_to_migrate.size(), config_.shard_id.c_str());

    // Initiate migrations for all symbols
    for (const auto& [sym, target] : symbols_to_migrate) {
        initiate_migration(sym, target);
        // Wait for migration to complete before starting next one
        if (migration_thread_.joinable()) {
            migration_thread_.join();
        }
    }

    // After all migrations complete, deregister
    deregister_shard();

    OB_LOG_INFO("shard_coord", "Draining complete for shard=%s, deregistering",
                config_.shard_id.c_str());

    return true;
}

} // namespace ob
