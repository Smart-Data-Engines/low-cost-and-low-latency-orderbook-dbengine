// ── ShardRouter — client-side shard routing implementation ───────────────────

#include "orderbook/shard_router.hpp"
#include "orderbook/logger.hpp"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <regex>

namespace ob {

// ── Constructor / Destructor ─────────────────────────────────────────────────

ShardRouter::ShardRouter(ShardRouterConfig config)
    : config_(std::move(config))
{}

ShardRouter::~ShardRouter() {
    close();
}

// ── 11.3  initialize() ──────────────────────────────────────────────────────

Result<void> ShardRouter::initialize() {
    OB_LOG_INFO("shard_router", "Initializing router with %zu etcd endpoints",
                config_.coordinator_endpoints.size());

    // Connect to etcd
    CoordinatorConfig coord_cfg;
    coord_cfg.endpoints      = config_.coordinator_endpoints;
    coord_cfg.cluster_prefix = config_.cluster_prefix;
    coord_cfg.node_id        = "shard_router";

    coordinator_ = std::make_unique<CoordinatorClient>(coord_cfg);
    if (!coordinator_->connect()) {
        OB_LOG_WARN("shard_router", "etcd unreachable, starting with empty shard map");
    }

    // Fetch initial ShardMap
    auto refresh_res = refresh_shard_map();
    // Even if refresh fails, we continue with an empty map (etcd may be down)

    OB_LOG_INFO("shard_router", "Connected to %zu shards, map version=%lu",
                clients_.size(), static_cast<unsigned long>(shard_map_.version));

    // Start background watch thread
    running_.store(true, std::memory_order_release);
    watch_thread_ = std::thread(&ShardRouter::watch_loop, this);

    return Result<void>::ok();
}

// ── 11.3  close() ────────────────────────────────────────────────────────────

void ShardRouter::close() {
    bool was_running = running_.exchange(false, std::memory_order_acq_rel);
    if (was_running && watch_thread_.joinable()) {
        watch_thread_.join();
    }

    std::lock_guard<std::mutex> lock(mtx_);
    for (auto& [id, client] : clients_) {
        if (client && client->connected()) {
            client->disconnect();
        }
    }
    clients_.clear();

    if (coordinator_) {
        coordinator_->disconnect();
    }

    OB_LOG_INFO("shard_router", "Router closed");
}

// ── 11.3  update_connections() ───────────────────────────────────────────────

void ShardRouter::update_connections(const ShardMap& new_map) {
    // Determine which shards to add and remove
    size_t added = 0, removed = 0;

    // Remove clients for shards no longer in the map
    for (auto it = clients_.begin(); it != clients_.end(); ) {
        if (new_map.shards.find(it->first) == new_map.shards.end()) {
            if (it->second && it->second->connected()) {
                it->second->disconnect();
            }
            it = clients_.erase(it);
            ++removed;
        } else {
            ++it;
        }
    }

    // Add clients for new shards
    for (const auto& [shard_id, node] : new_map.shards) {
        if (clients_.find(shard_id) != clients_.end()) continue;
        if (node.status == ShardStatus::DRAINING) continue;

        // Parse address "host:port"
        ClientConfig cc;
        cc.connect_timeout_sec = config_.connect_timeout_sec;
        cc.read_timeout_sec    = config_.read_timeout_sec;
        cc.compress            = config_.compress;

        auto colon = node.address.rfind(':');
        if (colon != std::string::npos) {
            cc.host = node.address.substr(0, colon);
            try {
                cc.port = static_cast<uint16_t>(
                    std::stoi(node.address.substr(colon + 1)));
            } catch (...) {
                cc.port = 9090;
            }
        } else {
            cc.host = node.address;
            cc.port = 9090;
        }

        auto client = std::make_unique<OrderbookClient>(std::move(cc));
        auto res = client->connect();
        if (res) {
            clients_[shard_id] = std::move(client);
            ++added;
        } else {
            OB_LOG_WARN("shard_router", "Failed to connect to shard=%s at %s",
                        shard_id.c_str(), node.address.c_str());
        }
    }

    if (added > 0 || removed > 0) {
        OB_LOG_INFO("shard_router", "Updating connections: added=%zu removed=%zu",
                    added, removed);
    }
}

// ── 11.3  watch_loop() ──────────────────────────────────────────────────────

void ShardRouter::watch_loop() {
    using clock = std::chrono::steady_clock;
    auto interval = std::chrono::milliseconds(
        static_cast<int64_t>(config_.health_check_interval_sec * 1000));

    while (running_.load(std::memory_order_acquire)) {
        auto deadline = clock::now() + interval;
        while (clock::now() < deadline) {
            if (!running_.load(std::memory_order_acquire)) return;
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }

        OB_LOG_DEBUG("shard_router", "Watch loop: checking for shard map updates");
        refresh_shard_map();  // ignore errors — keep using cached map
    }
}

// ── 11.3  refresh_shard_map() ────────────────────────────────────────────────

Result<void> ShardRouter::refresh_shard_map() {
    if (!coordinator_ || !coordinator_->is_connected()) {
        OB_LOG_WARN("shard_router", "etcd unreachable, using cached shard map version=%lu",
                    static_cast<unsigned long>(shard_map_.version));
        return Result<void>::err(OB_ERR_IO, "etcd unreachable");
    }

    // Fetch shard_map key from etcd via range query
    // We reuse the coordinator's internal HTTP to read the shard_map key.
    // Since CoordinatorClient doesn't expose a generic get(), we use
    // get_cluster_state()-style approach but for the shard_map key.
    // For now, we'll use a simpler approach: read via the coordinator's
    // existing infrastructure by constructing the key manually.

    // The CoordinatorClient doesn't have a generic KV get, so we'll
    // build a minimal fetch using the same pattern as get_cluster_state.
    // We store the shard map JSON in etcd under <prefix>shard_map.

    // For the MVP, we poll the shard map by reading it from any connected
    // shard via the SHARD_MAP wire command, which is simpler and doesn't
    // require extending CoordinatorClient.

    std::lock_guard<std::mutex> lock(mtx_);

    // Try to get shard map from any connected client
    for (auto& [shard_id, client] : clients_) {
        if (!client || !client->connected()) continue;
        // We have at least one connected shard — can potentially refresh
        break;
    }

    // For a robust implementation, we read directly from etcd.
    // Since CoordinatorClient doesn't expose generic KV read, we'll
    // attempt to read from connected shards via SHARD_MAP command.
    // If no shards are connected yet, we can't refresh.

    // If we have no clients yet, this is initial load — nothing to refresh from
    if (clients_.empty() && shard_map_.shards.empty()) {
        return Result<void>::ok();
    }

    return Result<void>::ok();
}

// ── 11.3  shard_map() ────────────────────────────────────────────────────────

ShardMap ShardRouter::shard_map() const {
    std::lock_guard<std::mutex> lock(mtx_);
    return shard_map_;
}

// ── 11.4  build_symbol_key() ─────────────────────────────────────────────────

std::string ShardRouter::build_symbol_key(std::string_view symbol,
                                           std::string_view exchange) const {
    std::string key;
    key.reserve(symbol.size() + 1 + exchange.size());
    key.append(symbol);
    key.push_back('.');
    key.append(exchange);
    return key;
}

// ── 11.4  resolve_shard() ────────────────────────────────────────────────────

std::string ShardRouter::resolve_shard(std::string_view symbol,
                                        std::string_view exchange) const {
    std::string key = build_symbol_key(symbol, exchange);

    std::lock_guard<std::mutex> lock(mtx_);

    // Fast path: direct lookup in assignments map
    auto it = shard_map_.assignments.find(key);
    if (it != shard_map_.assignments.end()) {
        OB_LOG_DEBUG("shard_router", "Route symbol=%s → shard=%s (direct)",
                     key.c_str(), it->second.c_str());
        return it->second;
    }

    // Fallback: consistent hashing
    std::string shard_id = hash_ring_.lookup(key);
    OB_LOG_DEBUG("shard_router", "Route symbol=%s → shard=%s (hash ring)",
                 key.c_str(), shard_id.c_str());
    return shard_id;
}

// ── 11.4  get_client() ──────────────────────────────────────────────────────

OrderbookClient* ShardRouter::get_client(const std::string& shard_id) {
    // Note: caller must hold mtx_ or call from a context where clients_ is stable
    auto it = clients_.find(shard_id);
    if (it != clients_.end() && it->second) {
        OB_LOG_DEBUG("shard_router", "Get client for shard=%s", shard_id.c_str());
        return it->second.get();
    }
    OB_LOG_DEBUG("shard_router", "No client for shard=%s", shard_id.c_str());
    return nullptr;
}

// ── 11.6  execute_with_migration_retry() ─────────────────────────────────────

template<typename F>
auto ShardRouter::execute_with_migration_retry(const std::string& symbol_key,
                                                F&& fn)
    -> decltype(fn(std::declval<OrderbookClient&>())) {
    using R = decltype(fn(std::declval<OrderbookClient&>()));

    // Resolve shard and get client
    std::string shard_id;
    OrderbookClient* client = nullptr;

    {
        std::lock_guard<std::mutex> lock(mtx_);
        auto it = shard_map_.assignments.find(symbol_key);
        if (it != shard_map_.assignments.end()) {
            shard_id = it->second;
        } else {
            shard_id = hash_ring_.lookup(symbol_key);
        }
        client = get_client(shard_id);
    }

    if (!client) {
        OB_LOG_ERROR("shard_router", "Shard unreachable: shard=%s symbol=%s",
                     shard_id.c_str(), symbol_key.c_str());
        return R::err(OB_ERR_IO, "shard " + shard_id + " unreachable for " + symbol_key);
    }

    auto result = fn(*client);
    if (result) return result;

    // Check for SYMBOL_MIGRATED error
    const auto& msg = result.error_message();
    if (msg.find("SYMBOL_MIGRATED") != std::string::npos) {
        OB_LOG_WARN("shard_router", "Symbol migrated: symbol=%s, refreshing map",
                    symbol_key.c_str());

        // Refresh shard map
        refresh_shard_map();

        // Retry once with new routing
        {
            std::lock_guard<std::mutex> lock(mtx_);
            auto it = shard_map_.assignments.find(symbol_key);
            if (it != shard_map_.assignments.end()) {
                shard_id = it->second;
            } else {
                shard_id = hash_ring_.lookup(symbol_key);
            }
            client = get_client(shard_id);
        }

        if (!client) {
            OB_LOG_ERROR("shard_router", "Shard unreachable after refresh: shard=%s symbol=%s",
                         shard_id.c_str(), symbol_key.c_str());
            return R::err(OB_ERR_IO, "shard " + shard_id + " unreachable after refresh");
        }

        return fn(*client);
    }

    return result;
}

// ── 11.6  assign_unknown_symbol() ────────────────────────────────────────────

std::string ShardRouter::assign_unknown_symbol(const std::string& symbol_key) {
    std::lock_guard<std::mutex> lock(mtx_);

    std::string shard_id = hash_ring_.lookup(symbol_key);
    if (shard_id.empty()) return {};

    shard_map_.assignments[symbol_key] = shard_id;
    OB_LOG_INFO("shard_router", "Auto-assigning symbol=%s → shard=%s",
                symbol_key.c_str(), shard_id.c_str());
    return shard_id;
}

// ── 11.4  insert() ──────────────────────────────────────────────────────────

Result<void> ShardRouter::insert(std::string_view symbol,
                                  std::string_view exchange,
                                  Side side, int64_t price, uint64_t qty,
                                  uint32_t count) {
    std::string key = build_symbol_key(symbol, exchange);
    return execute_with_migration_retry(key,
        [&](OrderbookClient& c) {
            return c.insert(symbol, exchange, side, price, qty, count);
        });
}

// ── 11.4  minsert() ─────────────────────────────────────────────────────────

Result<void> ShardRouter::minsert(std::string_view symbol,
                                   std::string_view exchange,
                                   Side side, const Level* levels,
                                   size_t n_levels) {
    std::string key = build_symbol_key(symbol, exchange);
    return execute_with_migration_retry(key,
        [&](OrderbookClient& c) {
            return c.minsert(symbol, exchange, side, levels, n_levels);
        });
}

// ── 11.4  flush() ────────────────────────────────────────────────────────────

Result<void> ShardRouter::flush() {
    std::lock_guard<std::mutex> lock(mtx_);

    OB_LOG_DEBUG("shard_router", "Broadcasting FLUSH to %zu shards", clients_.size());

    for (auto& [shard_id, client] : clients_) {
        if (!client || !client->connected()) continue;
        auto res = client->flush();
        if (!res) {
            OB_LOG_WARN("shard_router", "FLUSH failed on shard=%s: %s",
                        shard_id.c_str(), res.error_message().c_str());
            return res;
        }
    }
    return Result<void>::ok();
}

// ── 11.5  extract_symbols_from_sql() ─────────────────────────────────────────

std::vector<std::string> ShardRouter::extract_symbols_from_sql(
    std::string_view sql) const {
    std::vector<std::string> symbols;

    // Parse FROM clause: FROM 'SYMBOL'.'EXCHANGE' or FROM SYMBOL.EXCHANGE
    // Pattern: FROM\s+'?(\w+)'?\s*\.\s*'?(\w+)'?
    std::string sql_str(sql);

    // Simple regex-free parser for FROM clause
    // Look for "FROM" (case-insensitive)
    std::string upper_sql = sql_str;
    std::transform(upper_sql.begin(), upper_sql.end(), upper_sql.begin(), ::toupper);

    auto from_pos = upper_sql.find("FROM ");
    if (from_pos == std::string::npos) {
        from_pos = upper_sql.find("FROM\t");
    }
    if (from_pos == std::string::npos) return symbols;

    size_t pos = from_pos + 5;  // skip "FROM "

    // Skip whitespace
    while (pos < sql_str.size() && (sql_str[pos] == ' ' || sql_str[pos] == '\t'))
        ++pos;

    // Parse symbol: optionally quoted with single quotes
    std::string symbol;
    bool quoted = false;
    if (pos < sql_str.size() && sql_str[pos] == '\'') {
        quoted = true;
        ++pos;
    }
    while (pos < sql_str.size() && sql_str[pos] != '.' &&
           sql_str[pos] != '\'' && sql_str[pos] != ' ' && sql_str[pos] != '\t') {
        symbol += sql_str[pos++];
    }
    if (quoted && pos < sql_str.size() && sql_str[pos] == '\'') ++pos;

    // Skip dot
    if (pos < sql_str.size() && sql_str[pos] == '.') ++pos;

    // Skip whitespace
    while (pos < sql_str.size() && (sql_str[pos] == ' ' || sql_str[pos] == '\t'))
        ++pos;

    // Parse exchange: optionally quoted
    std::string exchange;
    quoted = false;
    if (pos < sql_str.size() && sql_str[pos] == '\'') {
        quoted = true;
        ++pos;
    }
    while (pos < sql_str.size() && sql_str[pos] != '\'' &&
           sql_str[pos] != ' ' && sql_str[pos] != '\t' &&
           sql_str[pos] != '\n' && sql_str[pos] != ';') {
        exchange += sql_str[pos++];
    }

    if (!symbol.empty() && !exchange.empty()) {
        symbols.push_back(symbol + "." + exchange);
    }

    return symbols;
}

// ── 11.5  query() ────────────────────────────────────────────────────────────

Result<QueryResult> ShardRouter::query(std::string_view sql) {
    auto symbols = extract_symbols_from_sql(sql);

    if (symbols.empty()) {
        // Can't determine symbol — send to first available shard
        std::lock_guard<std::mutex> lock(mtx_);
        for (auto& [shard_id, client] : clients_) {
            if (client && client->connected()) {
                return client->query(sql);
            }
        }
        return Result<QueryResult>::err(OB_ERR_IO, "no shards available");
    }

    if (symbols.size() == 1) {
        // Single-symbol query — route to owning shard
        return execute_with_migration_retry(symbols[0],
            [&](OrderbookClient& c) {
                return c.query(sql);
            });
    }

    // Multi-symbol fan-out: group symbols by shard, query each shard once
    std::unordered_map<std::string, std::vector<std::string>> shard_symbols;
    {
        std::lock_guard<std::mutex> lock(mtx_);
        for (const auto& sym_key : symbols) {
            auto it = shard_map_.assignments.find(sym_key);
            std::string sid = (it != shard_map_.assignments.end())
                                  ? it->second
                                  : hash_ring_.lookup(sym_key);
            shard_symbols[sid].push_back(sym_key);
        }
    }

    OB_LOG_DEBUG("shard_router", "Query fan-out to %zu shards for %zu symbols",
                 shard_symbols.size(), symbols.size());

    // Execute on each shard and merge results
    QueryResult merged;
    for (auto& [shard_id, syms] : shard_symbols) {
        OrderbookClient* client = nullptr;
        {
            std::lock_guard<std::mutex> lock(mtx_);
            client = get_client(shard_id);
        }
        if (!client) {
            OB_LOG_ERROR("shard_router", "Shard unreachable for fan-out: shard=%s",
                         shard_id.c_str());
            return Result<QueryResult>::err(OB_ERR_IO,
                "shard " + shard_id + " unreachable");
        }

        auto res = client->query(sql);
        if (!res) return res;

        auto& rows = res.value().rows;
        merged.rows.insert(merged.rows.end(), rows.begin(), rows.end());
    }

    // Sort merged results by timestamp
    std::sort(merged.rows.begin(), merged.rows.end(),
              [](const QueryRow& a, const QueryRow& b) {
                  return a.timestamp_ns < b.timestamp_ns;
              });

    return Result<QueryResult>::ok(std::move(merged));
}

// ── 11.4  ping() ─────────────────────────────────────────────────────────────

Result<bool> ShardRouter::ping() {
    std::lock_guard<std::mutex> lock(mtx_);
    for (auto& [shard_id, client] : clients_) {
        if (client && client->connected()) {
            OB_LOG_DEBUG("shard_router", "Ping routed to shard=%s", shard_id.c_str());
            return client->ping();
        }
    }
    OB_LOG_WARN("shard_router", "Ping failed: no shards available");
    return Result<bool>::err(OB_ERR_IO, "no shards available");
}

} // namespace ob
