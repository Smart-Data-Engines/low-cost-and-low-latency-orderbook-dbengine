// ── ShardCoordinator — server-side shard management ──────────────────────────

#include "orderbook/shard_coordinator.hpp"
#include "orderbook/loop_guard.hpp"
#include "orderbook/thread_boundary.hpp"
#include "orderbook/engine.hpp"
#include "orderbook/logger.hpp"
#include "orderbook/update_assembler.hpp"

#include <algorithm>
#include <chrono>
#include <cstdio>

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
    OB_LOG_INFO("shard_coord", "Starting coordinator for shard=%s vnodes=%u address=%s",
                config_.shard_id.c_str(), config_.vnodes, config_.advertise_address.c_str());

    coordinator_ = std::make_unique<CoordinatorClient>(config_.coordinator);
    // Once here, and in the watch loop until it succeeds: a shard that could not reach etcd at its
    // start stayed JOINING for good, refusing every write as not its own (#175).
    joined_ = connect_and_join();

    running_.store(true, std::memory_order_release);
    watch_thread_ = std::thread([this]() {
        run_thread_body("shard_coord", "watch_loop", [this] { watch_loop(); });
    });

    OB_LOG_INFO("shard_coord", "Coordinator started for shard=%s: %s, map version=%lu",
                config_.shard_id.c_str(), joined_ ? "in the shard map" : "not registered yet",
                static_cast<unsigned long>(shard_map().version));
}

bool ShardCoordinator::connect_and_join() {
    const auto give_up = [this](const char* why) {
        if (!join_warned_) {
            OB_LOG_WARN("shard_coord", "Shard %s is not registered yet (%s): every write is refused "
                                       "as not its own until it is - trying again every 2 s",
                        config_.shard_id.c_str(), why);
            join_warned_ = true;
        } else {
            OB_LOG_DEBUG("shard_coord", "Shard %s still not registered: %s", config_.shard_id.c_str(),
                         why);
        }
        return false;
    };
    if (!coordinator_->is_connected() && !coordinator_->connect()) return give_up("etcd unreachable");
    if (lease_id_ == 0) {
        lease_id_ = coordinator_->grant_lease();
        if (lease_id_ == 0) return give_up("no lease granted");
        OB_LOG_INFO("shard_coord", "Granted lease=%ld for shard=%s", static_cast<long>(lease_id_),
                    config_.shard_id.c_str());
    }
    if (!register_shard()) return give_up("its node key was not written");
    if (!join_map()) return give_up("the shard map was not written");
    if (join_warned_) {
        OB_LOG_INFO("shard_coord", "Shard %s is registered now", config_.shard_id.c_str());
        join_warned_ = false;
    }
    return true;
}

ShardNode ShardCoordinator::self_node() const {
    ShardNode node;
    node.shard_id = config_.shard_id;
    node.address  = config_.advertise_address;
    node.status   = ShardStatus::ACTIVE;
    node.vnodes   = config_.vnodes;
    return node;
}

bool ShardCoordinator::advertises() const {
    return engine_.node_role() != NodeRole::REPLICA;
}

bool ShardCoordinator::join_map() {
    // Two shards starting together read the same map; each writes it back only on the revision it
    // read, and the one that loses reads it again - with the other already in it.
    constexpr int kAttempts = 16;
    const std::string key = shard_map_key(config_.coordinator.cluster_prefix);
    for (int attempt = 1; attempt <= kAttempts; ++attempt) {
        CoordinatorClient::KeyValue kv;
        const auto read = coordinator_->get(key, kv);
        if (read == CoordinatorClient::KeyRead::Unavailable) return false;
        ShardMap map;
        if (read == CoordinatorClient::KeyRead::Present) {
            std::string error;
            if (!ShardMap::from_json(kv.value, map, error)) {
                OB_LOG_ERROR("shard_coord", "The shard map in etcd (%s) does not parse: %s - shard %s "
                                            "leaves it as it is", key.c_str(), error.c_str(),
                             config_.shard_id.c_str());
                return false;
            }
        }
        if (!advertises()) {
            // A replica of this shard's group: its primary is the one the map names.
            if (read == CoordinatorClient::KeyRead::Absent) return false;
            adopt_map(std::move(map), kv.mod_revision);
            return true;
        }
        if (!upsert_shard(map, self_node())) {
            adopt_map(std::move(map), kv.mod_revision);
            return true;
        }
        const auto cas = coordinator_->compare_and_put(
            key, read == CoordinatorClient::KeyRead::Present ? kv.mod_revision : 0, map.to_json());
        if (cas == CoordinatorClient::CasOutcome::Swapped) {
            OB_LOG_INFO("shard_coord", "Shard %s joined the shard map at %s (version %lu, %zu "
                                       "shard(s))", config_.shard_id.c_str(),
                        config_.advertise_address.c_str(), static_cast<unsigned long>(map.version),
                        map.shards.size());
            // The revision of this write is the next poll's to learn: it reads the map again once.
            adopt_map(std::move(map), 0);
            return true;
        }
        if (cas == CoordinatorClient::CasOutcome::Unavailable) return false;
        OB_LOG_WARN("shard_coord", "The shard map changed under shard %s since revision %lld - "
                                   "reading it again (%d of %d)", config_.shard_id.c_str(),
                    static_cast<long long>(kv.mod_revision), attempt, kAttempts);
        std::this_thread::sleep_for(std::chrono::milliseconds(10 * attempt));
    }
    OB_LOG_WARN("shard_coord", "The shard map kept changing under shard %s for %d attempts",
                config_.shard_id.c_str(), kAttempts);
    return false;
}

void ShardCoordinator::adopt_map(ShardMap map, int64_t revision) {
    ShardMapChangeCallback cb;
    ShardMap copy;
    bool moved = false;
    bool member = false;
    size_t shards = 0;
    uint64_t version = 0;
    {
        std::lock_guard<std::mutex> lock(mtx_);
        moved         = map.version != shard_map_.version;
        shard_map_    = std::move(map);
        map_revision_ = revision;
        hash_ring_    = ring_of(shard_map_);
        member        = shard_map_.shards.count(config_.shard_id) != 0;
        shards        = shard_map_.shards.size();
        // An adoption ends once the map names this shard for the symbol (#196) - after END or
        // without it: the source writes the map only once the rows are here, and a migration whose
        // END was lost has written it too.
        for (auto it = adopting_.begin(); it != adopting_.end();) {
            const auto named = shard_map_.assignments.find(it->first);
            if (named != shard_map_.assignments.end() && named->second == config_.shard_id) {
                OB_LOG_INFO("shard_coord", "Adopted %s from shard %s: the map names shard %s now",
                            it->first.c_str(), it->second.source.c_str(), config_.shard_id.c_str());
                engine_.end_adoption(it->first);
                it = adopting_.erase(it);
            } else {
                ++it;
            }
        }
        version       = shard_map_.version;
        cb            = change_cb_;
        copy          = shard_map_;
    }
    // A shard the map does not name owns nothing, and says so as it did before it joined.
    status_.store(member ? ShardStatus::ACTIVE : ShardStatus::JOINING, std::memory_order_release);
    if (moved) {
        OB_LOG_INFO("shard_coord", "Shard map version %lu: %zu shard(s), the ring rebuilt - shard %s "
                                   "%s", static_cast<unsigned long>(version), shards,
                    config_.shard_id.c_str(), member ? "is in it" : "is not in it");
    }
    if (cb) cb(copy);
}

void ShardCoordinator::poll_map() {
    const std::string key = shard_map_key(config_.coordinator.cluster_prefix);
    CoordinatorClient::KeyValue kv;
    const auto read = coordinator_->get(key, kv);
    if (read == CoordinatorClient::KeyRead::Unavailable) return;
    if (read == CoordinatorClient::KeyRead::Absent) {
        OB_LOG_WARN("shard_coord", "The shard map is gone from etcd (%s); shard %s writes it again",
                    key.c_str(), config_.shard_id.c_str());
        joined_ = join_map();
        return;
    }
    {
        std::lock_guard<std::mutex> lock(mtx_);
        if (kv.mod_revision == map_revision_) return;          // nothing new
    }
    ShardMap map;
    std::string error;
    if (!ShardMap::from_json(kv.value, map, error)) {
        OB_LOG_WARN("shard_coord", "The shard map in etcd no longer parses (%s): shard %s keeps its "
                                   "own copy", error.c_str(), config_.shard_id.c_str());
        return;
    }
    // The map says something else of this shard than this node would - another node of its group
    // wrote it, or the map was written without it - and this node is the one it should name.
    if (advertises()) {
        ShardMap mine = map;
        if (upsert_shard(mine, self_node())) {
            joined_ = join_map();
            return;
        }
    }
    adopt_map(std::move(map), kv.mod_revision);
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
    // Adopted, and ended: the map in etcd names this shard, and the one here does not yet (#196).
    // An adoption END has not ended is its connection's alone (adoption_number()).
    if (const auto a = adopting_.find(symbol_key); a != adopting_.end() && a->second.ended) return true;
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
    return migrating_ && migration_.symbol == symbol_key;
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
    metrics.in_progress  = migrating_;
    metrics.symbol       = migration_.symbol;
    metrics.target_shard = migration_.target;
    metrics.progress_pct = migration_.phase == "done" ? 100 : 0;
    return metrics;
}

ShardCoordinator::MigrationStatus ShardCoordinator::migration_status() const {
    std::lock_guard<std::mutex> lock(mtx_);
    return migration_;
}

void ShardCoordinator::set_migration_status(const std::function<void(MigrationStatus&)>& change) {
    std::lock_guard<std::mutex> lock(mtx_);
    change(migration_);
}

std::string ShardCoordinator::owner_in(const ShardMap& map, const std::string& symbol_key) {
    const auto named = map.assignments.find(symbol_key);
    return named != map.assignments.end() ? named->second : ring_of(map).lookup(symbol_key);
}

// ── register_shard() ──────────────────────────────────────────────────────────

bool ShardCoordinator::register_shard() {
    // This node, under its lease: gone on its own when the node stops keeping the lease alive, which
    // the map's entry - the shard's, and its symbols' - is not.
    const ShardNode node = self_node();
    const std::string key = shard_node_key(config_.coordinator.cluster_prefix, config_.shard_id);
    if (!coordinator_->put(key, node.to_json(), lease_id_)) return false;
    OB_LOG_INFO("shard_coord", "Registered shard=%s address=%s under lease %ld", config_.shard_id.c_str(),
                node.address.c_str(), static_cast<long>(lease_id_));
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
    // One pass is the unit, and it is the only one of the seven whose wait sits *inside* the guard:
    // this loop naps between its two halves - the lease keepalive and the topology propagation -
    // so a failing pass is still paced at two seconds. Without this the thread ends and shard
    // ownership is never re-read.
    LoopGuard guard{"shard_coord", "a shard-map poll", &engine_.registry()};

    while (running_.load(std::memory_order_acquire)) {
        try {
            // Keep-alive for the lease. One that etcd no longer knows - the node was away longer
            // than its TTL - is granted again, and the node key written again, below.
            if (coordinator_ && lease_id_ != 0 && !coordinator_->refresh_lease(lease_id_)) {
                OB_LOG_WARN("shard_coord", "Shard %s lost its lease %ld; registering again",
                            config_.shard_id.c_str(), static_cast<long>(lease_id_));
                lease_id_ = 0;
                joined_   = false;
            }

            for (int i = 0; i < 20 && running_.load(std::memory_order_acquire); ++i) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
            if (!running_.load(std::memory_order_acquire)) break;

            // Not in the map yet: try again. In it: read it, and take what changed - a shard that
            // joined since is in this shard's ring from here on (#175).
            if (!joined_) {
                joined_ = connect_and_join();
            } else {
                poll_map();
            }

            propagate_mm_topology();
            guard.ok();
        } catch (const std::exception& e) {
            guard.caught(e);
        }
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
    {
        // One at a time: the driver's state is one migration's.
        std::lock_guard<std::mutex> lock(mtx_);
        if (migrating_) {
            OB_LOG_WARN("shard_coord", "Not moving %s: moving %s to shard %s is under way",
                        symbol_key.c_str(), migration_.symbol.c_str(), migration_.target.c_str());
            return false;
        }
        migrating_        = true;
        migration_        = MigrationStatus{};
        migration_.symbol = symbol_key;
        migration_.target = target_shard_id;
        migration_.phase  = "adopting";
    }
    if (migration_thread_.joinable()) migration_thread_.join();   // the last one's, finished
    migration_thread_ = std::thread([this, symbol_key, target_shard_id]() {
        run_thread_body("shard_coord", "execute_migration", [&] {
            execute_migration(symbol_key, target_shard_id);
        });
        // A move whose switch has an unknown outcome leaves its symbol frozen, and one begun now
        // could thaw it: none begins until a restart reads the map.
        std::lock_guard<std::mutex> lock(mtx_);
        if (migration_.phase != "unknown") migrating_ = false;
    });
    return true;
}

// ── execute_migration() ───────────────────────────────────────────────────────

namespace {

/// Rounds of copying while the symbol's writes are taken, each copying what came during the one
/// before, until one copies fewer updates than this - what the second freeze then copies with the
/// symbol's writes refused - or there have been kMaxCopyRounds.
constexpr uint64_t kFinalRoundUpdates = 2000;
constexpr uint32_t kMaxCopyRounds     = 8;
/// The longest the last round may have taken when it copied more than kFinalRoundUpdates: the freeze
/// copies about as much again with the symbol's writes refused.
constexpr double kFreezeBudgetMs = 1000.0;

/// An answer as a log line and an error can carry it: without its newlines.
std::string one_line(std::string answer) {
    while (!answer.empty() && (answer.back() == '\n' || answer.back() == '\r')) answer.pop_back();
    return answer;
}

double ms_since(std::chrono::steady_clock::time_point t) {
    return std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - t).count();
}

}  // namespace

void ShardCoordinator::execute_migration(const std::string& symbol_key,
                                         const std::string& target_shard_id) {
    const auto started = std::chrono::steady_clock::now();
    OB_LOG_INFO("shard_coord", "Moving %s to shard %s with its rows (#196)", symbol_key.c_str(),
                target_shard_id.c_str());
    const auto fail = [&](const std::string& why) {
        OB_LOG_ERROR("shard_coord", "Moving %s to shard %s failed: %s - it stays on shard %s, which "
                                    "takes its writes", symbol_key.c_str(), target_shard_id.c_str(),
                     why.c_str(), config_.shard_id.c_str());
        set_migration_status([&](MigrationStatus& m) {
            m.phase = "failed";
            m.error = why;
        });
    };

    std::string address;
    {
        std::lock_guard<std::mutex> lock(mtx_);
        const auto t = shard_map_.shards.find(target_shard_id);
        if (t != shard_map_.shards.end()) address = t->second.address;
    }
    if (address.empty()) return fail("the map names no address for shard " + target_shard_id);
    TargetConnection target(address, config_.migration_access);
    if (const std::string why = target.connect(); !why.empty()) {
        return fail("shard " + target_shard_id + " at " + address + " cannot be reached: " + why);
    }
    std::string answer;
    if (const std::string why =
            target.command("ADOPT " + symbol_key + " BEGIN " + config_.shard_id, answer);
        !why.empty()) {
        return fail("shard " + target_shard_id + " did not answer ADOPT BEGIN: " + why);
    }
    if (answer != "OK\n\n") {
        return fail("shard " + target_shard_id + " did not adopt it: " + one_line(answer));
    }

    // Adopted: from here a failure before the map names the target abandons the adoption, and one
    // after it does not - what the target holds is then the symbol's only copy that takes writes.
    bool frozen = false;
    bool switched_map = false;
    std::shared_ptr<const void> pin;
    try {
        std::unordered_set<std::string> copied;
        set_migration_status([](MigrationStatus& m) { m.phase = "copying"; });
        // The files pinned first, so that no merge or retention sweep changes what the rounds copy
        // from: a merge between the first seal and the pin would replace segments the list names -
        // gone when read, or their rows sent again as the merged segment's. A merge under way when
        // the pin is taken finishes before the seal, which waits for the flush lock it holds.
        pin = engine_.pin_segment_files();
        // No round but the last needs the symbol's writes refused: a row taken after a round's seal
        // drained the queue is in a segment a later seal lists, and the last seal is made frozen.
        auto round_started = std::chrono::steady_clock::now();
        uint64_t moved = copy_segments(target, engine_.seal_symbol(symbol_key), copied);
        double round_ms = ms_since(round_started);
        uint32_t rounds = 1;
        // Converged: the last round copied a small remainder, which the freeze copies about as much
        // of again. The rounds also stop after kMaxCopyRounds, converged or not.
        bool converged = moved < kFinalRoundUpdates;
        while (!converged && rounds < kMaxCopyRounds) {
            if (!running_.load(std::memory_order_acquire)) {
                throw std::runtime_error("shard " + config_.shard_id + " is stopping");
            }
            round_started = std::chrono::steady_clock::now();
            moved = copy_segments(target, engine_.seal_symbol(symbol_key), copied);
            round_ms = ms_since(round_started);
            ++rounds;
            converged = moved < kFinalRoundUpdates;
        }
        // Rounds that never came down to a small remainder: the symbol's writes arrive about as fast
        // as they are copied, and the freeze would refuse them for about as long as the last round
        // took - past what a client tries again for. Refused, and the symbol stays where it is.
        if (!converged && round_ms > kFreezeBudgetMs) {
            char detail[160];
            std::snprintf(detail, sizeof(detail),
                          "the last of %u rounds copied %llu update(s) in %.0f ms, and the switch "
                          "would refuse the symbol's writes for about as long",
                          rounds, static_cast<unsigned long long>(moved), round_ms);
            throw std::runtime_error(std::string("its writes arrive about as fast as they are copied: ") +
                                     detail);
        }
        set_migration_status([&](MigrationStatus& m) {
            m.phase  = "switching";
            m.rounds = rounds;
        });

        // The switch. The symbol's writes are refused SYMBOL_MOVING from here to the thaw: what
        // came during the last round is copied, and then the map names the target.
        engine_.freeze_symbol(symbol_key);
        frozen = true;
        const auto freeze_started = std::chrono::steady_clock::now();
        copy_segments(target, engine_.seal_symbol(symbol_key), copied);
        std::string why;
        const MapSwitch switched = switch_owner_in_map(symbol_key, target_shard_id, why);
        if (switched == MapSwitch::Refused) throw std::runtime_error("the map was not switched: " + why);
        if (switched == MapSwitch::Unknown) {
            // Neither thawed nor abandoned, and no other MIGRATE until a restart (migrating_ stays).
            // The map may name the target, and then a write taken here would be stored where no
            // client reads it, and what the target adopted would be the symbol's only copy.
            OB_LOG_ERROR("shard_coord", "Moving %s to shard %s: whether the map names shard %s for it "
                                        "is unknown (%s) - its writes stay refused here until a "
                                        "restart of shard %s reads the map from etcd",
                         symbol_key.c_str(), target_shard_id.c_str(), target_shard_id.c_str(),
                         why.c_str(), config_.shard_id.c_str());
            set_migration_status([&](MigrationStatus& m) {
                m.phase = "unknown";
                m.error = why;
            });
            return;
        }

        // The map names the target. END lets it take the symbol's writes from every client before
        // its own read of the map says so; were END lost, that read ends the adoption all the same.
        switched_map = true;
        if (const std::string e = target.command("ADOPT " + symbol_key + " END", answer);
            !e.empty() || answer != "OK\n\n") {
            OB_LOG_WARN("shard_coord", "Moving %s: shard %s did not take END (%s) - it takes the "
                                       "symbol's writes once it reads the map",
                        symbol_key.c_str(), target_shard_id.c_str(),
                        e.empty() ? one_line(answer).c_str() : e.c_str());
        }
        engine_.mark_symbol_migrated(symbol_key);
        engine_.thaw_symbol(symbol_key);
        frozen = false;
        const double freeze_ms = ms_since(freeze_started);
        MigrationStatus done;
        set_migration_status([&](MigrationStatus& m) {
            m.phase     = "done";
            m.freeze_ms = freeze_ms;
            done        = m;
        });
        OB_LOG_INFO("shard_coord", "Moved %s to shard %s: %u round(s), %llu update(s) of %llu row(s), "
                                   "its writes refused for %.1f ms, %.1f s in all",
                    symbol_key.c_str(), target_shard_id.c_str(), done.rounds,
                    static_cast<unsigned long long>(done.updates),
                    static_cast<unsigned long long>(done.rows), freeze_ms, ms_since(started) / 1000.0);
    } catch (const std::exception& e) {
        if (switched_map) {
            // The map names the target: the move is done whatever failed after it, and the symbol
            // is refused here as migrated.
            engine_.mark_symbol_migrated(symbol_key);
            if (frozen) engine_.thaw_symbol(symbol_key);
            OB_LOG_ERROR("shard_coord", "Moved %s to shard %s, the map names it, and then: %s",
                         symbol_key.c_str(), target_shard_id.c_str(), e.what());
            set_migration_status([&](MigrationStatus& m) {
                m.phase = "done";
                m.error = e.what();
            });
            return;
        }
        abandon_on_target(target, symbol_key);
        if (frozen) engine_.thaw_symbol(symbol_key);
        fail(e.what());
    }
}

uint64_t ShardCoordinator::copy_segments(TargetConnection& target,
                                         const std::vector<SegmentMeta>& sealed,
                                         std::unordered_set<std::string>& copied) {
    // Sent in batches, each one write answered in order: one round trip a write bounded the copy
    // below the rate one writer writes at, and a copy slower than its symbol's writers never ends.
    constexpr size_t kBatchUpdates = 512;
    constexpr size_t kBatchLevels  = 8192;
    const auto started = std::chrono::steady_clock::now();
    uint64_t updates = 0;
    uint64_t rows = 0;
    uint64_t counted_updates = 0;   // what the status says already
    uint64_t counted_rows = 0;
    size_t segments = 0;
    std::vector<MovedUpdate> batch;
    std::vector<MovedWrite> writes;
    size_t batch_levels = 0;
    for (const SegmentMeta& meta : sealed) {
        if (!copied.insert(meta.dir_path).second) continue;
        ++segments;
        std::string refused;
        const auto send = [&]() {
            if (batch.empty() || !refused.empty()) return;
            if (!running_.load(std::memory_order_acquire)) {
                refused = "shard " + config_.shard_id + " is stopping";
                return;
            }
            writes.resize(batch.size());
            for (size_t i = 0; i < batch.size(); ++i) {
                writes[i] = MovedWrite{batch[i].side, batch[i].timestamp_ns, batch[i].levels.data(),
                                       batch[i].levels.size()};
            }
            refused = target.send_updates(meta.symbol, meta.exchange, writes);
            if (refused.empty()) {
                updates += batch.size();
                rows += batch_levels;
            }
            batch.clear();
            batch_levels = 0;
        };
        UpdateAssembler assembler;
        MovedUpdate done;
        const auto take = [&](MovedUpdate&& u) {
            batch_levels += u.levels.size();
            batch.push_back(std::move(u));
            if (batch.size() >= kBatchUpdates || batch_levels >= kBatchLevels) send();
        };
        const bool whole = engine_.read_symbol_segment(meta, [&](const SnapshotRow& row) {
            if (!refused.empty()) return;   // the read goes on to its end; nothing more is sent
            if (assembler.add(row, done)) take(std::move(done));
        });
        if (refused.empty() && assembler.finish(done)) take(std::move(done));
        send();
        // Progress by the segment, so that SHARD_INFO moves during a round.
        set_migration_status([&](MigrationStatus& m) {
            m.updates += updates - counted_updates;
            m.rows += rows - counted_rows;
        });
        counted_updates = updates;
        counted_rows    = rows;
        if (!refused.empty()) {
            throw std::runtime_error("shard at " + target.address() + " did not store a write of " +
                                     meta.dir_path + ": " + refused);
        }
        if (!whole) throw std::runtime_error("segment " + meta.dir_path + " could not be read whole");
    }
    OB_LOG_INFO("shard_coord", "Copied %zu segment(s) to %s: %llu update(s) of %llu row(s) in %.1f ms",
                segments, target.address().c_str(), static_cast<unsigned long long>(updates),
                static_cast<unsigned long long>(rows), ms_since(started));
    return updates;
}

void ShardCoordinator::abandon_on_target(TargetConnection& target, const std::string& symbol_key) {
    const std::string line = "ADOPT " + symbol_key + " ABANDON";
    std::string answer;
    std::string why = target.connected() ? target.command(line, answer) : std::string("not connected");
    if (!why.empty()) {
        // The copy's connection broke: a new one says it.
        TargetConnection again(target.address(), config_.migration_access);
        why = again.connect();
        if (why.empty()) why = again.command(line, answer);
    }
    if (why.empty() && answer.rfind("OK", 0) == 0) {
        OB_LOG_WARN("shard_coord", "Moving %s abandoned: the target at %s dropped what it adopted (%s)",
                    symbol_key.c_str(), target.address().c_str(), one_line(answer).c_str());
        return;
    }
    OB_LOG_ERROR("shard_coord", "Moving %s abandoned, and the target at %s keeps what it adopted: %s - "
                                "ADOPT %s ABANDON on it drops that, and the next MIGRATE of the "
                                "symbol is refused until it does",
                 symbol_key.c_str(), target.address().c_str(),
                 why.empty() ? one_line(answer).c_str() : why.c_str(), symbol_key.c_str());
}

ShardCoordinator::MapSwitch ShardCoordinator::switch_owner_in_map(const std::string& symbol_key,
                                                                  const std::string& target_shard_id,
                                                                  std::string& why) {
    // A compare-and-swap that gets no answer may have landed, and then the map names the target:
    // from then on this gives up only once a read says which, or after kUnknownFor. Before one,
    // nothing was written, and an unreachable etcd is a refusal.
    constexpr int kAttempts = 16;
    constexpr auto kUnknownFor = std::chrono::seconds(60);
    const std::string key = shard_map_key(config_.coordinator.cluster_prefix);
    bool maybe_landed = false;
    std::chrono::steady_clock::time_point give_up{};
    int attempts = 0;
    auto pause = std::chrono::milliseconds(10);
    for (;;) {
        if (!running_.load(std::memory_order_acquire)) {
            why = "shard " + config_.shard_id + " is stopping";
            return maybe_landed ? MapSwitch::Unknown : MapSwitch::Refused;
        }
        if (maybe_landed && std::chrono::steady_clock::now() >= give_up) {
            why = "a compare-and-swap of the map got no answer, and no read since has said whether it "
                  "was written";
            return MapSwitch::Unknown;
        }
        if (!maybe_landed && ++attempts > kAttempts) {
            why = "etcd did not take the map in " + std::to_string(kAttempts) + " attempts";
            return MapSwitch::Refused;
        }
        if (attempts > 1 || maybe_landed) {
            std::this_thread::sleep_for(pause);
            pause = std::min(pause * 2, std::chrono::milliseconds(2000));
        }
        CoordinatorClient::KeyValue kv;
        const auto read = coordinator_ ? coordinator_->get(key, kv) : CoordinatorClient::KeyRead::Unavailable;
        if (read == CoordinatorClient::KeyRead::Unavailable) continue;
        if (read == CoordinatorClient::KeyRead::Absent) {
            why = "etcd holds no shard map";
            return MapSwitch::Refused;
        }
        ShardMap map;
        std::string error;
        if (!ShardMap::from_json(kv.value, map, error)) {
            why = "the map in etcd does not parse: " + error;
            return MapSwitch::Refused;
        }
        const std::string owner = owner_in(map, symbol_key);
        if (owner == target_shard_id) {
            // This switch, or an earlier attempt's that landed.
            OB_LOG_INFO("shard_coord", "The map names shard %s for %s (revision %lld)",
                        target_shard_id.c_str(), symbol_key.c_str(),
                        static_cast<long long>(kv.mod_revision));
            adopt_map(std::move(map), kv.mod_revision);
            return MapSwitch::Switched;
        }
        if (owner != config_.shard_id) {
            why = "the map names shard " + owner + " for it";
            return MapSwitch::Refused;
        }
        map.assignments[symbol_key] = target_shard_id;
        ++map.version;
        const auto cas = coordinator_->compare_and_put(key, kv.mod_revision, map.to_json());
        if (cas == CoordinatorClient::CasOutcome::Swapped) {
            OB_LOG_INFO("shard_coord", "The map names shard %s for %s now (version %lu)",
                        target_shard_id.c_str(), symbol_key.c_str(),
                        static_cast<unsigned long>(map.version));
            // The revision of this write is the next poll's to learn, as join_map() leaves it.
            adopt_map(std::move(map), 0);
            return MapSwitch::Switched;
        }
        if (cas == CoordinatorClient::CasOutcome::Unavailable && !maybe_landed) {
            maybe_landed = true;
            give_up = std::chrono::steady_clock::now() + kUnknownFor;
            OB_LOG_WARN("shard_coord", "A compare-and-swap of the map for %s got no answer: reading it "
                                       "until it says whether it was written", symbol_key.c_str());
        } else if (cas == CoordinatorClient::CasOutcome::Conflict) {
            OB_LOG_DEBUG("shard_coord", "The map changed since revision %lld: reading it again",
                         static_cast<long long>(kv.mod_revision));
        }
    }
}

// ── rollback_migration() ──────────────────────────────────────────────────────

void ShardCoordinator::rollback_migration(const std::string& symbol_key) {
    // What the stub's failure did to this shard's copy of the map; a migration now changes nothing
    // here that a failure has to take back, and the map in etcd is written once, by its switch.
    OB_LOG_DEBUG("shard_coord", "Nothing to roll back for %s", symbol_key.c_str());
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
    // The migration this shard runs, or ran last (#196): what MIGRATE, which answers once it has
    // begun, leaves an operator to read.
    if (!migration_.phase.empty()) {
        char freeze[32];
        std::snprintf(freeze, sizeof(freeze), "%.1f", migration_.freeze_ms);
        result += "migration_symbol\t" + migration_.symbol + "\n";
        result += "migration_target\t" + migration_.target + "\n";
        result += "migration_phase\t" + migration_.phase + "\n";
        result += "migration_rounds\t" + std::to_string(migration_.rounds) + "\n";
        result += "migration_updates\t" + std::to_string(migration_.updates) + "\n";
        result += "migration_rows\t" + std::to_string(migration_.rows) + "\n";
        result += "migration_freeze_ms\t" + std::string(freeze) + "\n";
        if (!migration_.error.empty()) result += "migration_error\t" + migration_.error + "\n";
    }
    result += "\n";

    return result;
}

// ── handle_migrate_command() ──────────────────────────────────────────────────

std::string ShardCoordinator::handle_migrate_command(const std::string& symbol_key,
                                                      const std::string& target_shard_id) {
    OB_LOG_INFO("shard_coord", "Handling MIGRATE: symbol=%s target=%s",
                symbol_key.c_str(), target_shard_id.c_str());
    {
        std::lock_guard<std::mutex> lock(mtx_);
        // Owned as a write is: by the map's assignment, or by the ring (#196, requirement 5).
        if (owner_in(shard_map_, symbol_key) != config_.shard_id) {
            OB_LOG_WARN("shard_coord", "MIGRATE rejected: not owner of symbol=%s",
                        symbol_key.c_str());
            return "ERR NOT_OWNER " + symbol_key + "\n";
        }
        const auto target = shard_map_.shards.find(target_shard_id);
        if (target == shard_map_.shards.end()) {
            OB_LOG_WARN("shard_coord", "MIGRATE rejected: unknown shard=%s",
                        target_shard_id.c_str());
            return "ERR unknown shard: " + target_shard_id + "\n";
        }
        if (target_shard_id == config_.shard_id) {
            return "ERR shard " + config_.shard_id + " owns " + symbol_key + " already\n";
        }
        if (target->second.status != ShardStatus::ACTIVE) {
            return "ERR shard " + target_shard_id + " is not active\n";
        }
        if (migrating_ && migration_.phase == "unknown") {
            return "ERR whether moving " + migration_.symbol + " to shard " + migration_.target +
                   " switched the map is unknown: restart shard " + config_.shard_id +
                   ", which reads the map, before another MIGRATE\n";
        }
        if (migrating_) {
            return "ERR migration already in progress: " + migration_.symbol + "\n";
        }
    }
    if (!initiate_migration(symbol_key, target_shard_id)) {
        return "ERR migration already in progress\n";
    }
    // Under way: SHARD_INFO says how it goes.
    return "OK\n\n";
}

// ── handle_adopt_command() ────────────────────────────────────────────────────

bool ShardCoordinator::is_adopting(const std::string& symbol_key) const {
    std::lock_guard<std::mutex> lock(mtx_);
    return adopting_.count(symbol_key) > 0;
}

uint64_t ShardCoordinator::adoption_number(const std::string& symbol_key) const {
    std::lock_guard<std::mutex> lock(mtx_);
    const auto a = adopting_.find(symbol_key);
    return a == adopting_.end() ? 0 : a->second.number;
}

std::string ShardCoordinator::handle_adopt_command(const std::string& symbol_key,
                                                    const std::string& action,
                                                    const std::string& source_shard_id,
                                                    uint64_t* adoption) {
    std::lock_guard<std::mutex> one_at_a_time(adopt_command_mtx_);
    const auto owned_locked = [&] {
        const auto named = shard_map_.assignments.find(symbol_key);
        return named != shard_map_.assignments.end() ? named->second == config_.shard_id
                                                     : hash_ring_.lookup(symbol_key) == config_.shard_id;
    };
    if (action == "BEGIN") {
        {
            std::lock_guard<std::mutex> lock(mtx_);
            if (status_.load(std::memory_order_acquire) != ShardStatus::ACTIVE) {
                return "ERR shard " + config_.shard_id + " is not active\n";
            }
            const auto a = adopting_.find(symbol_key);
            if (a != adopting_.end()) {
                return "ERR already adopting " + symbol_key + " from shard " + a->second.source + "\n";
            }
            if (owned_locked()) {
                return "ERR shard " + config_.shard_id + " owns " + symbol_key + " already\n";
            }
        }
        // Checked under the engine's lock with the adoption's start: nothing writes the symbol here
        // - this shard does not own it and does not adopt it yet - so what it holds stays held.
        const uint64_t number = engine_.begin_adoption(symbol_key);
        if (number == 0) {
            OB_LOG_WARN("shard_coord", "ADOPT %s refused: shard %s holds rows of it already - a "
                                       "migration that failed left them, and ADOPT %s ABANDON drops them",
                        symbol_key.c_str(), config_.shard_id.c_str(), symbol_key.c_str());
            return "ERR shard " + config_.shard_id + " holds rows of " + symbol_key +
                   " already: ADOPT " + symbol_key + " ABANDON drops them\n";
        }
        {
            std::lock_guard<std::mutex> lock(mtx_);
            adopting_[symbol_key] = Adoption{source_shard_id, number, false};
        }
        if (adoption != nullptr) *adoption = number;
        OB_LOG_INFO("shard_coord", "Adopting %s from shard %s (adoption %llu): the writes of the "
                                   "connection that began it are taken here until the map names this "
                                   "shard",
                    symbol_key.c_str(), source_shard_id.c_str(),
                    static_cast<unsigned long long>(number));
        return "OK\n\n";
    }
    if (action == "END") {
        std::lock_guard<std::mutex> lock(mtx_);
        const auto a = adopting_.find(symbol_key);
        if (a == adopting_.end()) return "ERR not adopting " + symbol_key + "\n";
        a->second.ended = true;
        const auto named = shard_map_.assignments.find(symbol_key);
        if (named != shard_map_.assignments.end() && named->second == config_.shard_id) {
            OB_LOG_INFO("shard_coord", "Adopted %s from shard %s", symbol_key.c_str(),
                        a->second.source.c_str());
            engine_.end_adoption(symbol_key);
            adopting_.erase(a);
        } else {
            OB_LOG_INFO("shard_coord", "Adoption of %s ended: its writes are taken from any connection, "
                                       "and it is this shard's once the map it reads says so",
                        symbol_key.c_str());
        }
        return "OK\n\n";
    }
    // ABANDON: out of the adopted first, so that nothing writes it while it is dropped.
    bool adopted = false;
    {
        std::lock_guard<std::mutex> lock(mtx_);
        // A shard drops only what it does not own - by its map, whether it adopts the symbol or not:
        // the rows of a symbol it serves are not a migration's to throw away, and a migration whose
        // END never came has written the map to name this shard all the same.
        if (owned_locked()) return "ERR shard " + config_.shard_id + " owns " + symbol_key + "\n";
        const auto a = adopting_.find(symbol_key);
        if (a == adopting_.end()) {
            // What a migration that failed left of a symbol this shard does not own.
        } else if (a->second.ended) {
            // END came after the map in etcd was changed to name this shard: what it adopted is the
            // symbol's only copy that takes writes.
            return "ERR the adoption of " + symbol_key + " has ended: the map names shard " +
                   config_.shard_id + "\n";
        } else {
            adopting_.erase(a);
            adopted = true;
        }
    }
    try {
        const size_t dropped =
            adopted ? engine_.abandon_adoption(symbol_key) : engine_.drop_symbol(symbol_key);
        OB_LOG_WARN("shard_coord", "%s %s: %zu segment(s) of it dropped",
                    adopted ? "Adoption abandoned of" : "Rows dropped of", symbol_key.c_str(), dropped);
        return "OK " + std::to_string(dropped) + "\n\n";
    } catch (const std::exception& e) {
        // The adoption is over either way; what stays is rows no connection writes, which a later
        // BEGIN refuses to adopt over and ABANDON drops.
        OB_LOG_ERROR("shard_coord", "ADOPT %s ABANDON: the rows of it stay: %s", symbol_key.c_str(),
                     e.what());
        return "ERR the rows of " + symbol_key + " stay: " + e.what() + "\n";
    }
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

// ── propagate_mm_topology() ───────────────────────────────────────────────────

void ShardCoordinator::propagate_mm_topology() {
    // Read mm_peers from etcd for each shard and propagate to ShardMap.
    // In production, this would use etcd range query on
    // <prefix>shards/<shard_id>/mm_peers/ for each shard.
    // For now, we check if the coordinator has mm_peers data available
    // and update the local ShardMap accordingly.

    if (!coordinator_ || !coordinator_->is_connected()) {
        return;
    }

    std::lock_guard<std::mutex> lock(mtx_);

    // For each shard in the map, attempt to read mm_peers from etcd.
    // The mm_peers are stored under:
    //   <prefix>shards/<shard_id>/mm_peers/<node_id>
    // Since CoordinatorClient doesn't expose a generic range query,
    // we rely on the watch mechanism to receive topology updates.
    // The mm_nodes in ShardMap are populated when the shard map is
    // fetched/updated from etcd (via update_shard_map).

    // Log that we're checking for mm topology updates
    bool has_mm_nodes = false;
    for (const auto& [shard_id, node] : shard_map_.shards) {
        if (!node.mm_nodes.empty()) {
            has_mm_nodes = true;
            break;
        }
    }

    if (has_mm_nodes) {
        OB_LOG_DEBUG("shard_coord",
                     "MM topology propagated: shard_map version=%lu has mm_nodes",
                     static_cast<unsigned long>(shard_map_.version));
    }

    // Invoke change callback so clients (ShardRouter) get updated mm_nodes
    ShardMapChangeCallback cb_copy = change_cb_;
    if (cb_copy && has_mm_nodes) {
        // Release lock before callback
        ShardMap map_copy = shard_map_;
        mtx_.unlock();
        cb_copy(map_copy);
        mtx_.lock();
    }
}

} // namespace ob
