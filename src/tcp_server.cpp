#include "orderbook/tcp_server.hpp"
#include "orderbook/logger.hpp"
#include "orderbook/metrics.hpp"
#include "orderbook/metrics_server.hpp"
#include "orderbook/shard_coordinator.hpp"

#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <stdexcept>
#include <charconv>
#include <limits>
#include <type_traits>
#include <string_view>
#include <span>
#include <string>
#include <vector>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

namespace ob {

// ── execute_command ───────────────────────────────────────────────────────────

std::string execute_command(const Command& cmd,
                            Engine& engine,
                            Session& session,
                            ServerStats& stats,
                            bool read_only,
                            MetricsRegistry* registry,
                            ShardCoordinator* shard_coord) {
    switch (cmd.type) {

    case CommandType::COMPRESS: {
        if (session.commands_executed() > 0) {
            return format_error("compress_must_be_first");
        }
        // NOTE: Do NOT call session.set_compressed(true) here!
        // The "OK COMPRESS LZ4\n\n" response must be sent as plain text.
        // The caller (epoll/io_uring loop) enables compression AFTER
        // sending this response.
        // Double newline required — client uses \n\n as OK terminator.
        return "OK COMPRESS LZ4\n\n";
    }

    case CommandType::SELECT: {
        session.increment_commands();
        // Reject queries during snapshot bootstrap.
        if (engine.is_bootstrapping()) return format_error("bootstrapping");
        auto t0_select = std::chrono::steady_clock::now();
        std::vector<QueryResult> rows;
        try {
            std::string err = engine.execute(cmd.raw_sql, [&](const QueryResult& r) {
                rows.push_back(r);
            });
            if (!err.empty()) {
                return format_error(err);
            }
        } catch (const std::exception& e) {
            return format_error(e.what());
        }
        if (registry) {
            double secs = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0_select).count();
            registry->observe_histogram("ob_query_latency_seconds", secs);
            registry->increment_counter("ob_total_queries");
        }
        session.increment_queries();
        stats.total_queries.fetch_add(1, std::memory_order_relaxed);

        // An aggregate query calls the callback exactly once, with agg_values set
        // and the row fields left at zero. Formatting that as a data row is what
        // made SPREAD, MID_PRICE, IMBALANCE and VWAP return zeros to every network
        // client while the engine computed them correctly.
        if (!rows.empty() && !rows.front().agg_values.empty()) {
            return format_agg_response(rows.front().agg_values);
        }
        return format_query_response(rows);
    }

    case CommandType::INSERT: {
        session.increment_commands();
        if (read_only || engine.node_role() == NodeRole::REPLICA) return format_error("read-only replica");
        // Covers the multi-master bootstrap too, which used to need its own block here and
        // answered with a second spelling of the same error.
        if (engine.is_bootstrapping()) return format_error("bootstrapping");
        // Shard ownership check: reject writes for symbols not owned by this shard
        if (shard_coord) {
            const std::string symbol_key = cmd.insert_args.symbol + "." + cmd.insert_args.exchange;
            if (engine.is_symbol_migrated(symbol_key)) {
                shard_coord->increment_routing_errors();
                OB_LOG_WARN("tcp_server", "Rejecting INSERT for migrated symbol=%s", symbol_key.c_str());
                return format_error("SYMBOL_MIGRATED");
            }
            if (!shard_coord->owns_symbol(symbol_key)) {
                shard_coord->increment_routing_errors();
                OB_LOG_WARN("tcp_server", "Rejecting INSERT for non-owned symbol=%s", symbol_key.c_str());
                return format_error("NOT_OWNER " + symbol_key);
            }
        }
        auto t0_insert = std::chrono::steady_clock::now();
        try {
            const auto& a = cmd.insert_args;

            DeltaUpdate delta{};
            std::strncpy(delta.symbol,   a.symbol.c_str(),   sizeof(delta.symbol)   - 1);
            std::strncpy(delta.exchange, a.exchange.c_str(), sizeof(delta.exchange) - 1);
            // 0 means "unassigned": Engine::stamp_sequence() gives this write the next
            // number for its symbol. The comment here used to claim the engine handled
            // sequencing while nothing did, so every stored row carried a zero.
            delta.sequence_number = 0;
            delta.timestamp_ns = static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(
                    std::chrono::system_clock::now().time_since_epoch())
                    .count());
            delta.side     = a.side;
            delta.n_levels = 1;

            Level level{};
            level.price = a.price;
            level.qty   = a.qty;
            level.cnt   = a.count;

            ob_status_t rc = engine.is_multi_master()
                ? engine.apply_delta_mm(delta, &level)
                : engine.apply_delta(delta, &level);
            if (rc != OB_OK) {
                return format_error("apply_delta failed with code " + std::to_string(rc));
            }
        } catch (const std::exception& e) {
            return format_error(e.what());
        }
        if (registry) {
            double secs = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0_insert).count();
            registry->observe_histogram("ob_insert_latency_seconds", secs);
            registry->increment_counter("ob_total_inserts");
        }
        session.increment_inserts();
        stats.total_inserts.fetch_add(1, std::memory_order_relaxed);
        return format_ok();
    }

    case CommandType::MINSERT: {
        session.increment_commands();
        if (read_only || engine.node_role() == NodeRole::REPLICA) return format_error("read-only replica");
        // Covers the multi-master bootstrap too, which used to need its own block here and
        // answered with a second spelling of the same error.
        if (engine.is_bootstrapping()) return format_error("bootstrapping");
        // Shard ownership check: reject writes for symbols not owned by this shard
        if (shard_coord) {
            const std::string symbol_key = cmd.minsert_args.symbol + "." + cmd.minsert_args.exchange;
            if (engine.is_symbol_migrated(symbol_key)) {
                shard_coord->increment_routing_errors();
                OB_LOG_WARN("tcp_server", "Rejecting MINSERT for migrated symbol=%s", symbol_key.c_str());
                return format_error("SYMBOL_MIGRATED");
            }
            if (!shard_coord->owns_symbol(symbol_key)) {
                shard_coord->increment_routing_errors();
                OB_LOG_WARN("tcp_server", "Rejecting MINSERT for non-owned symbol=%s", symbol_key.c_str());
                return format_error("NOT_OWNER " + symbol_key);
            }
        }
        auto t0_minsert = std::chrono::steady_clock::now();
        try {
            const auto& a = cmd.minsert_args;

            DeltaUpdate delta{};
            std::strncpy(delta.symbol,   a.symbol.c_str(),   sizeof(delta.symbol)   - 1);
            std::strncpy(delta.exchange, a.exchange.c_str(), sizeof(delta.exchange) - 1);
            delta.sequence_number = 0;   // unassigned; the engine numbers it (see INSERT above)
            delta.timestamp_ns = static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(
                    std::chrono::system_clock::now().time_since_epoch())
                    .count());
            delta.side     = a.side;
            delta.n_levels = a.n_levels;

            std::vector<Level> levels(a.n_levels);
            for (uint16_t i = 0; i < a.n_levels; ++i) {
                levels[i].price = a.levels[i].price;
                levels[i].qty   = a.levels[i].qty;
                levels[i].cnt   = a.levels[i].count;
                levels[i]._pad  = 0;
            }

            ob_status_t status = engine.is_multi_master()
                ? engine.apply_delta_mm(delta, levels.data())
                : engine.apply_delta(delta, levels.data());
            if (status != OB_OK) {
                return format_error("apply_delta failed with code " + std::to_string(status));
            }
        } catch (const std::exception& e) {
            return format_error(e.what());
        }
        if (registry) {
            double secs = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0_minsert).count();
            registry->observe_histogram("ob_insert_latency_seconds", secs);
            registry->increment_counter("ob_total_inserts");
        }
        session.increment_inserts();
        stats.total_inserts.fetch_add(1, std::memory_order_relaxed);
        return format_ok();
    }

    case CommandType::FLUSH: {
        session.increment_commands();
        if (read_only || engine.node_role() == NodeRole::REPLICA) return format_error("read-only replica");
        if (engine.is_bootstrapping()) return format_error("bootstrapping");
        auto t0_flush = std::chrono::steady_clock::now();
        try {
            engine.flush_incremental();
        } catch (const std::exception& e) {
            return format_error(e.what());
        }
        if (registry) {
            double secs = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0_flush).count();
            registry->observe_histogram("ob_flush_latency_seconds", secs);
            registry->increment_counter("ob_total_flushes");
        }
        return format_ok();
    }

    case CommandType::PING:
        session.increment_commands();
        return format_pong();

    case CommandType::STATUS: {
        session.increment_commands();
        auto es = engine.stats();
        stats.engine_metrics.pending_rows   = es.pending_rows;
        stats.engine_metrics.wal_file_index = es.wal_file_index;
        stats.engine_metrics.segment_count  = es.segment_count;
        stats.engine_metrics.symbol_count   = es.symbol_count;

        // Copy replication metrics
        stats.replicas.clear();
        for (const auto& r : es.replicas) {
            stats.replicas.push_back({r.address, r.confirmed_file, r.confirmed_offset, r.lag_bytes});
        }
        stats.is_replica            = es.is_replica;
        stats.repl_confirmed_file   = es.repl_confirmed_file;
        stats.repl_confirmed_offset = es.repl_confirmed_offset;
        stats.repl_records_replayed = es.repl_records_replayed;
        stats.repl_connected        = es.repl_connected;
        stats.bootstrapping         = es.bootstrapping;
        stats.snapshot_bytes_received = es.snapshot_bytes_received;
        stats.snapshot_bytes_total  = es.snapshot_bytes_total;
        stats.snapshot_active       = es.snapshot_active;

        // Failover state
        stats.node_role           = static_cast<uint8_t>(es.node_role);
        stats.current_epoch       = es.current_epoch;
        stats.primary_address     = es.primary_address;
        stats.lease_ttl_remaining = es.lease_ttl_remaining;

        // Compression metrics from the requesting session
        stats.compress_bytes_in  = session.compress_bytes_in();
        stats.compress_bytes_out = session.compress_bytes_out();

        // TTL / data retention metrics
        stats.ttl_hours            = es.ttl_hours;
        stats.ttl_segments_deleted = es.ttl_segments_deleted;
        stats.ttl_bytes_reclaimed  = es.ttl_bytes_reclaimed;

        // Flush integrity
        stats.segment_merge_refused = es.segment_merge_refused;

        // Sharding metrics
        stats.shard_id              = es.shard_id;
        stats.shard_status          = es.shard_status;
        stats.shard_symbols_count   = es.shard_symbols_count;
        stats.shard_map_version     = es.shard_map_version;
        stats.migration_in_progress = es.migration_in_progress;
        stats.migration_symbol      = es.migration_symbol;
        stats.migration_target_shard = es.migration_target_shard;
        stats.migration_progress_pct = es.migration_progress_pct;
        stats.shard_routing_errors  = es.shard_routing_errors;

        // Multi-master metrics
        stats.mm_node_role = static_cast<uint8_t>(es.node_role);
        if (es.node_role == NodeRole::MULTI_MASTER) {
            stats.mm_node_id           = es.mm_node_id;
            stats.mm_peer_count        = es.mm_peer_count;
            stats.mm_connected_peers   = es.mm_connected_peers;
            stats.mm_conflicts_total   = es.mm_conflicts_total;
            stats.mm_anti_entropy_runs = es.mm_anti_entropy_runs;
            stats.mm_anti_entropy_repairs = es.mm_anti_entropy_repairs;
            stats.mm_hlc_physical_ns   = es.mm_hlc_physical_ns;
            stats.mm_hlc_logical       = es.mm_hlc_logical;
            stats.mm_hlc_drift_ns      = es.mm_hlc_drift_ns;
            stats.mm_replication_lag_per_peer = es.mm_replication_lag_per_peer;
        }

        return format_status(stats);
    }

    case CommandType::ROLE:
        session.increment_commands();
        return engine.handle_role_command();

    case CommandType::FAILOVER:
        session.increment_commands();
        return engine.handle_failover_command(cmd.target_node_id);

    case CommandType::SHARD_MAP: {
        session.increment_commands();
        OB_LOG_DEBUG("tcp_server", "Handling SHARD_MAP command");
        if (!shard_coord) return format_error("sharding not enabled");
        return shard_coord->handle_shard_map_command();
    }

    case CommandType::SHARD_INFO: {
        session.increment_commands();
        OB_LOG_DEBUG("tcp_server", "Handling SHARD_INFO command");
        if (!shard_coord) return format_error("sharding not enabled");
        return shard_coord->handle_shard_info_command();
    }

    case CommandType::MIGRATE: {
        session.increment_commands();
        OB_LOG_INFO("tcp_server", "Handling MIGRATE command: symbol=%s target=%s",
                    cmd.migrate_symbol.c_str(), cmd.migrate_target_shard.c_str());
        if (!shard_coord) return format_error("sharding not enabled");
        if (read_only) return format_error("read-only mode");
        return shard_coord->handle_migrate_command(
            cmd.migrate_symbol, cmd.migrate_target_shard);
    }

    case CommandType::MM_PEERS: {
        session.increment_commands();
        OB_LOG_DEBUG("tcp_server", "Handling MM_PEERS command");
        if (!engine.is_multi_master()) return format_error("not in multi-master mode");
        return engine.multi_master_manager()->handle_mm_peers_command();
    }

    case CommandType::MM_CONFLICTS: {
        session.increment_commands();
        OB_LOG_DEBUG("tcp_server", "Handling MM_CONFLICTS command limit=%zu", cmd.mm_conflicts_limit);
        if (!engine.is_multi_master()) return format_error("not in multi-master mode");
        return engine.multi_master_manager()->handle_mm_conflicts_command(cmd.mm_conflicts_limit);
    }

    case CommandType::QUIT:
        session.increment_commands();
        return ""; // empty string signals session close

    case CommandType::UNKNOWN:
    default:
        session.increment_commands();
        return format_error("unknown command");
    }
}

// ── ArgCursor ─────────────────────────────────────────────────────────────────
//
// A cursor over argv, so consuming a flag's value does not mean modifying the loop variable.
// The old parser did `argv[++i]` inside `for (int i = 1; i < argc; ++i)` 29 times, which is 29
// instances of cpp/loop-variable-changed and a review thread on every PR that added a flag
// (roadmap #36). It also hid three real defects, all measured before this rewrite:
//
//   ob_tcp_server --port abc      → terminate called after throwing std::invalid_argument, core
//                                   dumped: stoi threw and nothing caught it
//   ob_tcp_server --port          → started anyway, on the default port. The guard was
//                                   `arg == "--port" && i + 1 < argc`, so a flag with no value
//                                   simply fell through
//   ob_tcp_server --prot 5599     → started anyway, on the default port, ignoring both the typo
//                                   and its value, because there was no unknown-argument branch
//
// All three are now errors with a message naming the flag. That is stricter than before: an
// invocation carrying an unknown flag used to start a server and now refuses to. A correct
// invocation behaves exactly as it did.

namespace {

class ArgCursor {
public:
    /// A span rather than (int, char*[]): the cursor then carries its own bounds, and the bounds
    /// are what every check below is against. main()'s signature is fixed, so the conversion
    /// happens once, at the call site.
    explicit ArgCursor(std::span<char* const> args) : args_(args) {}

    /// Advance to the next argument. False when there are none left.
    bool next() { return ++index_ < args_.size(); }

    /// The argument the cursor is on.
    std::string_view arg() const { return args_[index_]; }

    /// The value belonging to the current flag. Missing values are fatal, not ignored.
    std::string_view value() {
        if (index_ + 1 >= args_.size()) {
            std::fprintf(stderr, "Error: %s requires a value\n", args_[index_]);
            std::exit(1);
        }
        return args_[++index_];
    }

    /// The value parsed as a number, with the flag named in the error rather than a stoi throw.
    template <typename T>
    T value_as() {
        const char* flag = args_[index_];
        const std::string_view text = value();

        if constexpr (std::is_signed_v<T>) {
            long long parsed = 0;
            const auto result = std::from_chars(text.data(), text.data() + text.size(), parsed);
            if (result.ec != std::errc{} || result.ptr != text.data() + text.size()) {
                fail(flag, text, "an integer");
            }
            if (parsed < static_cast<long long>(std::numeric_limits<T>::min()) ||
                parsed > static_cast<long long>(std::numeric_limits<T>::max())) {
                fail(flag, text, "a value in range");
            }
            return static_cast<T>(parsed);
        } else {
            unsigned long long parsed = 0;
            const auto result = std::from_chars(text.data(), text.data() + text.size(), parsed);
            if (result.ec != std::errc{} || result.ptr != text.data() + text.size()) {
                fail(flag, text, "a non-negative integer");
            }
            if (parsed > static_cast<unsigned long long>(std::numeric_limits<T>::max())) {
                fail(flag, text, "a value in range");
            }
            return static_cast<T>(parsed);
        }
    }

private:
    [[noreturn]] static void fail(const char* flag, std::string_view text, const char* expected) {
        std::fprintf(stderr, "Error: %s expects %s, got '%.*s'\n", flag, expected,
                     static_cast<int>(text.size()), text.data());
        std::exit(1);
    }

    std::span<char* const> args_;
    std::size_t            index_{0};
};

}  // namespace

// ── parse_cli_args ────────────────────────────────────────────────────────────

ServerConfig parse_cli_args(int argc, char* argv[]) {
    ServerConfig config;

    ArgCursor cursor(std::span<char* const>(argv, static_cast<std::size_t>(argc)));
    while (cursor.next()) {
        const std::string arg{cursor.arg()};

        if (arg == "--port") {
            config.port = cursor.value_as<uint16_t>();
        } else if (arg == "--data-dir") {
            config.data_dir = std::string{cursor.value()};
        } else if (arg == "--max-sessions") {
            config.max_sessions = cursor.value_as<int>();
        } else if (arg == "--workers") {
            config.worker_threads = cursor.value_as<int>();
        } else if (arg == "--read-only") {
            config.read_only = true;
        } else if (arg == "--replication-port") {
            config.replication_port = cursor.value_as<uint16_t>();
        } else if (arg == "--replication-compress") {
            config.replication_compress = true;
        } else if (arg == "--primary-host") {
            config.primary_host = std::string{cursor.value()};
        } else if (arg == "--primary-port") {
            config.primary_port = cursor.value_as<uint16_t>();
        } else if (arg == "--snapshot-chunk-size") {
            config.snapshot_chunk_size = cursor.value_as<size_t>();
        } else if (arg == "--snapshot-staging-dir") {
            config.snapshot_staging_dir = std::string{cursor.value()};
        } else if (arg == "--coordinator-endpoints") {
            // Comma-separated list; empty entries are skipped rather than stored.
            const std::string endpoints{cursor.value()};
            std::string ep;
            for (char c : endpoints) {
                if (c == ',') {
                    if (!ep.empty()) config.coordinator_endpoints.push_back(ep);
                    ep.clear();
                } else {
                    ep.push_back(c);
                }
            }
            if (!ep.empty()) config.coordinator_endpoints.push_back(ep);
        } else if (arg == "--coordinator-lease-ttl") {
            config.coordinator_lease_ttl = cursor.value_as<int64_t>();
        } else if (arg == "--handover-grace-seconds") {
            config.handover_grace_seconds = cursor.value_as<int64_t>();
        } else if (arg == "--handover-cooldown-seconds") {
            config.handover_cooldown_seconds = cursor.value_as<int64_t>();
        } else if (arg == "--election-deference-ms") {
            config.election_deference_ms = cursor.value_as<int64_t>();
        } else if (arg == "--node-id") {
            config.node_id = std::string{cursor.value()};
        } else if (arg == "--failover-enabled") {
            const std::string val{cursor.value()};
            config.failover_enabled = (val == "true" || val == "1" || val == "yes");
        } else if (arg == "--ttl-hours") {
            config.ttl_hours = cursor.value_as<uint64_t>();
        } else if (arg == "--ttl-scan-interval-seconds") {
            config.ttl_scan_interval_seconds = cursor.value_as<uint64_t>();
        } else if (arg == "--metrics-port") {
            config.metrics_port = cursor.value_as<uint16_t>();
        } else if (arg == "--flush-interval-ms") {
            config.flush_interval_ms = cursor.value_as<uint64_t>();
        } else if (arg == "--log-level") {
            const std::string level{cursor.value()};
            if (!StructuredLogger::parse_level(level).has_value()) {
                std::fprintf(stderr,
                    "Error: invalid log level '%s'. Valid values: ERROR, WARN, INFO, DEBUG\n",
                    level.c_str());
                std::exit(1);
            }
            config.log_level = level;
        } else if (arg == "--sqpoll-idle-ms") {
            config.uring_sqpoll_idle_ms = cursor.value_as<uint32_t>();
        } else if (arg == "--ring-size") {
            config.uring_ring_size = cursor.value_as<uint32_t>();
        } else if (arg == "--no-sqpoll") {
            config.uring_no_sqpoll = true;
        } else if (arg == "--shard-id") {
            config.shard_id = std::string{cursor.value()};
        } else if (arg == "--shard-vnodes") {
            config.shard_vnodes = cursor.value_as<uint32_t>();
        } else if (arg == "--multi-master") {
            config.multi_master = true;
        } else if (arg == "--mm-node-id") {
            config.mm_node_id = cursor.value_as<uint16_t>();
        } else if (arg == "--mm-replication-port") {
            config.mm_replication_port = cursor.value_as<uint16_t>();
        } else if (arg == "--anti-entropy-interval-seconds") {
            config.anti_entropy_interval_sec = cursor.value_as<uint32_t>();
        } else if (arg == "--mm-max-peer-send-buffer") {
            config.mm_max_peer_send_buf_bytes = cursor.value_as<size_t>();
        } else if (arg == "--mm-max-catchup-bytes") {
            config.mm_max_catchup_bytes = cursor.value_as<size_t>();
        } else {
            // Previously ignored in silence, which meant a typo started a server on the default
            // port: `--prot 5599` was accepted, and so was `--port` with no value at all.
            std::fprintf(stderr, "Error: unknown argument '%s'\n", arg.c_str());
            std::exit(1);
        }
    }

    // Validation: handover windows must be sane. A cooldown shorter than the
    // grace window would let the outgoing primary win the race it announced,
    // which is the bug this configuration exists to prevent.
    if (config.handover_grace_seconds <= 0 || config.handover_cooldown_seconds <= 0) {
        std::fprintf(stderr,
                     "Error: --handover-grace-seconds and --handover-cooldown-seconds "
                     "must be positive\n");
        std::exit(1);
    }
    if (config.handover_cooldown_seconds < config.handover_grace_seconds) {
        std::fprintf(stderr,
                     "Error: --handover-cooldown-seconds (%ld) must be >= "
                     "--handover-grace-seconds (%ld)\n",
                     static_cast<long>(config.handover_cooldown_seconds),
                     static_cast<long>(config.handover_grace_seconds));
        std::exit(1);
    }

    // Validation: --shard-id requires --coordinator-endpoints
    if (!config.shard_id.empty() && config.coordinator_endpoints.empty()) {
        std::fprintf(stderr,
            "Error: --shard-id requires --coordinator-endpoints to be specified. "
            "Shard mode needs etcd for shard map coordination.\n");
        std::exit(1);
    }

    if (!config.coordinator_endpoints.empty()) {
        OB_LOG_INFO("tcp_server", "Handover: grace=%lds cooldown=%lds",
                    static_cast<long>(config.handover_grace_seconds),
                    static_cast<long>(config.handover_cooldown_seconds));
    }

    if (!config.shard_id.empty()) {
        OB_LOG_INFO("tcp_server", "Shard mode enabled: shard_id=%s vnodes=%u",
                    config.shard_id.c_str(), config.shard_vnodes);
    }

    // Validation: --multi-master requires --mm-node-id
    if (config.multi_master && config.mm_node_id == 0) {
        std::fprintf(stderr,
            "Error: --multi-master requires --mm-node-id <uint16>\n");
        std::exit(1);
    }

    // Validation: --multi-master requires --coordinator-endpoints
    if (config.multi_master && config.coordinator_endpoints.empty()) {
        std::fprintf(stderr,
            "Error: --multi-master requires --coordinator-endpoints for peer discovery\n");
        std::exit(1);
    }

    // Validation: --multi-master requires --mm-replication-port
    if (config.multi_master && config.mm_replication_port == 0) {
        std::fprintf(stderr,
            "Error: --multi-master requires --mm-replication-port <port>\n");
        std::exit(1);
    }

    // Validation: --multi-master is incompatible with --read-only
    if (config.multi_master && config.read_only) {
        std::fprintf(stderr,
            "Error: --multi-master is incompatible with --read-only\n");
        std::exit(1);
    }

    // Validation: --multi-master is incompatible with --primary-host/--primary-port
    if (config.multi_master && (!config.primary_host.empty() || config.primary_port > 0)) {
        std::fprintf(stderr,
            "Error: --multi-master is incompatible with --primary-host/--primary-port "
            "(single-primary replication)\n");
        std::exit(1);
    }

    // Validation: warn if --replication-port is specified in MM mode (it will be ignored)
    if (config.multi_master && config.replication_port > 0) {
        OB_LOG_WARN("cli",
            "replication-port is ignored in multi-master mode");
    }

    // Validation: --mm-replication-port and --replication-port must be different ports
    if (config.multi_master && config.replication_port > 0 &&
        config.mm_replication_port == config.replication_port) {
        std::fprintf(stderr,
            "Error: --mm-replication-port and --replication-port must be different ports\n");
        std::exit(1);
    }

    // Validation: --mm-replication-port requires --multi-master mode
    if (!config.multi_master && config.mm_replication_port > 0) {
        std::fprintf(stderr,
            "Error: --mm-replication-port requires --multi-master mode\n");
        std::exit(1);
    }

    if (config.multi_master) {
        OB_LOG_INFO("cli", "Multi-master mode: node_id=%u replication_port=%u anti_entropy=%us",
                    config.mm_node_id, config.mm_replication_port,
                    config.anti_entropy_interval_sec);
    }

    return config;
}

// ── TcpServer ─────────────────────────────────────────────────────────────────

TcpServer::TcpServer(ServerConfig config)
    : config_(std::move(config))
    , read_only_(config_.read_only)
{
    ReplicationConfig repl_config{};
    if (!config_.multi_master) {
        repl_config.port = config_.replication_port;
        repl_config.compress = config_.replication_compress;
    }
    // In MM mode, repl_config.port stays 0 → Engine won't create ReplicationManager

    ReplicationClientConfig repl_client_config{};
    repl_client_config.primary_host = config_.primary_host;
    repl_client_config.primary_port = config_.primary_port;
    repl_client_config.state_file   = config_.data_dir + "/repl_state.txt";
    repl_client_config.snapshot_chunk_size = config_.snapshot_chunk_size;
    repl_client_config.snapshot_staging_dir = config_.snapshot_staging_dir;

    FailoverConfig failover_config{};
    if (!config_.coordinator_endpoints.empty()) {
        failover_config.coordinator.endpoints = config_.coordinator_endpoints;
        failover_config.coordinator.lease_ttl_seconds = config_.coordinator_lease_ttl;
        failover_config.handover_grace_seconds    = config_.handover_grace_seconds;
        failover_config.election_deference_ms      = config_.election_deference_ms;
        failover_config.handover_cooldown_seconds = config_.handover_cooldown_seconds;
        failover_config.coordinator.node_id = config_.node_id;
        failover_config.failover_enabled = config_.failover_enabled;
        failover_config.replication_port = config_.replication_port;
        failover_config.replication_address = "127.0.0.1:" + std::to_string(config_.replication_port);
    }

    engine_ = std::make_unique<Engine>(config_.data_dir,
                                      config_.flush_interval_ms * 1'000'000ULL,
                                      FsyncPolicy::INTERVAL,
                                       repl_config, repl_client_config, failover_config,
                                       TTLConfig{config_.ttl_hours,
                                                 config_.ttl_scan_interval_seconds},
                                       // Designated initialisers: this was positional, and adding
                                       // a field in the middle of MultiMasterConfig silently
                                       // shifted every argument after it.
                                       MultiMasterConfig{
                                           .node_id = config_.mm_node_id,
                                           .replication_port = config_.mm_replication_port,
                                           .enabled = config_.multi_master,
                                           .compress = config_.replication_compress,
                                           .max_catchup_bytes = config_.mm_max_catchup_bytes,
                                           .anti_entropy_interval_sec =
                                               config_.anti_entropy_interval_sec,
                                           .max_peer_send_buf_bytes =
                                               config_.mm_max_peer_send_buf_bytes,
                                           .shard_id = config_.shard_id,
                                           .coordinator_config = CoordinatorConfig{
                                               config_.coordinator_endpoints,
                                               config_.coordinator_lease_ttl,
                                               config_.node_id,
                                               "/ob/"
                                           }
                                       });
}

TcpServer::~TcpServer() {
    if (epoll_fd_ >= 0) ::close(epoll_fd_);
    if (listen_fd_ >= 0) ::close(listen_fd_);
}

void TcpServer::run() {
    // Wire up the dynamic read-only flag so failover transitions toggle it.
    engine_->set_read_only_flag(&read_only_);

    // Open the engine (replay WAL, start flush thread).
    engine_->open();

    // Set structured log level from config.
    auto parsed_level = StructuredLogger::parse_level(config_.log_level);
    if (parsed_level.has_value()) {
        StructuredLogger::instance().set_level(*parsed_level);
    }

    // Start MetricsServer if configured.
    if (config_.metrics_port > 0) {
        metrics_server_ = std::make_unique<MetricsServer>(config_.metrics_port, engine_->registry());
        metrics_server_->start();
    }

    // Initialize ShardCoordinator if shard mode is enabled.
    std::unique_ptr<ShardCoordinator> shard_coord;
    if (!config_.shard_id.empty()) {
        OB_LOG_INFO("tcp_server", "Shard mode: shard_id=%s", config_.shard_id.c_str());

        ShardCoordinatorConfig sc_config;
        sc_config.shard_id = config_.shard_id;
        sc_config.vnodes = config_.shard_vnodes;
        sc_config.coordinator.endpoints = config_.coordinator_endpoints;
        sc_config.coordinator.lease_ttl_seconds = config_.coordinator_lease_ttl;
        sc_config.coordinator.node_id = config_.node_id;
        sc_config.coordinator.cluster_prefix = "/ob/";

        shard_coord = std::make_unique<ShardCoordinator>(sc_config, *engine_);
        shard_coord->start();
    }

    // 1. Create non-blocking TCP socket.
    listen_fd_ = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    if (listen_fd_ < 0) {
        throw std::runtime_error(std::string("socket() failed: ") + std::strerror(errno));
    }

    // 2. Set SO_REUSEADDR.
    int opt = 1;
    if (::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        throw std::runtime_error(std::string("setsockopt() failed: ") + std::strerror(errno));
    }

    // 3. Bind to 0.0.0.0:port.
    struct sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons(config_.port);

    if (::bind(listen_fd_, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
        throw std::runtime_error(std::string("bind() failed on port ")
                                 + std::to_string(config_.port) + ": " + std::strerror(errno));
    }

    // 4. Listen with backlog 128.
    if (::listen(listen_fd_, 128) < 0) {
        throw std::runtime_error(std::string("listen() failed: ") + std::strerror(errno));
    }

    // 5. Create epoll instance.
    epoll_fd_ = ::epoll_create1(0);
    if (epoll_fd_ < 0) {
        throw std::runtime_error(std::string("epoll_create1() failed: ") + std::strerror(errno));
    }

    // 6. Add listen_fd_ to epoll.
    struct epoll_event ev{};
    ev.events  = EPOLLIN;
    ev.data.fd = listen_fd_;
    if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, listen_fd_, &ev) < 0) {
        throw std::runtime_error(std::string("epoll_ctl() failed: ") + std::strerror(errno));
    }

    // 7. Create SessionManager and ServerStats.
    SessionManager session_mgr(config_.max_sessions);

    ServerStats stats;
    // One place that closes a session, so every close carries a reason in the log.
    // This used to be five copies of the same four lines, none of them logging,
    // which is why a session dying in the middle of a large response left no trace.
    auto close_session = [&](int fd, const char* reason) {
        size_t pending = 0;
        if (Session* s = session_mgr.get_session(fd)) {
            pending = s->pending_output_bytes();
        }
        OB_LOG_INFO("tcp_server",
                    "Closing session: fd=%d reason=%s pending_bytes=%zu",
                    fd, reason, pending);
        ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
        session_mgr.remove_session(fd);
        stats.active_sessions.fetch_sub(1, std::memory_order_relaxed);
        engine_->registry().increment_gauge("ob_active_sessions", -1);
    };

    // Store pointers for use in accept_connection / handle_client_data.
    // We use a local lambda-based epoll loop so these are captured by reference.

    static constexpr int MAX_EVENTS = 64;
    struct epoll_event events[MAX_EVENTS];

    running_.store(true, std::memory_order_relaxed);

    // 8. Epoll loop.
    while (running_.load(std::memory_order_relaxed)) {
        int nfds = ::epoll_wait(epoll_fd_, events, MAX_EVENTS, 100 /*ms timeout*/);
        if (nfds < 0) {
            if (errno == EINTR) continue;
            break; // fatal epoll error
        }

        // Once per loop iteration, not per event: the sum walks the session map, and
        // there is nothing to learn from updating it several times per wake-up.
        engine_->registry().set_gauge(
            "ob_session_pending_bytes",
            static_cast<int64_t>(session_mgr.total_pending_output_bytes()));

        for (int i = 0; i < nfds; ++i) {
            int fd = events[i].data.fd;

            if (fd == listen_fd_) {
                // Draining: stop accepting new connections.
                if (draining_.load(std::memory_order_relaxed)) {
                    // Reject all pending connections.
                    while (true) {
                        int reject_fd = ::accept4(listen_fd_, nullptr, nullptr, SOCK_NONBLOCK);
                        if (reject_fd < 0) break;
                        const char* msg = "ERR server shutting down\n";
                        auto wr = ::send(reject_fd, msg, std::strlen(msg), MSG_NOSIGNAL);
                        (void)wr;
                        ::close(reject_fd);
                    }
                    continue;
                }

                // Accept new connection(s).
                while (true) {
                    struct sockaddr_in client_addr{};
                    socklen_t client_len = sizeof(client_addr);
                    int client_fd = ::accept4(listen_fd_,
                                              reinterpret_cast<struct sockaddr*>(&client_addr),
                                              &client_len,
                                              SOCK_NONBLOCK);
                    if (client_fd < 0) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                        break; // accept error, continue loop
                    }

                    if (!session_mgr.add_session(client_fd)) {
                        // Server full — reject.
                        const char* msg = "ERR server full\n";
                        auto wr = ::send(client_fd, msg, std::strlen(msg), MSG_NOSIGNAL);
                        (void)wr;
                        ::close(client_fd);
                        continue;
                    }

                    // Add to epoll (edge-triggered).
                    struct epoll_event cev{};
                    cev.events  = EPOLLIN | EPOLLET;
                    cev.data.fd = client_fd;
                    if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, client_fd, &cev) < 0) {
                        session_mgr.remove_session(client_fd);
                        continue;
                    }

                    stats.active_sessions.fetch_add(1, std::memory_order_relaxed);
                    engine_->registry().increment_gauge("ob_active_sessions");

                    // Send welcome message.
                    Session* s = session_mgr.get_session(client_fd);
                    if (s) {
                        s->send_response("OK ob_tcp_server v0.1.0\n\n");
                    }
                }
            } else {
                // Writable first: a session with queued output is waiting on this,
                // and draining it may be the only thing that lets the client send
                // its next command.
                if (events[i].events & EPOLLOUT) {
                    Session* session = session_mgr.get_session(fd);
                    if (session) {
                        if (!session->flush_output()) {
                            close_session(fd, "flush failed");
                            continue;
                        }
                        if (!session->has_pending_output()) {
                            disarm_epollout(fd);
                            if (session->close_requested()) {
                                close_session(fd, "quit after flush");
                                continue;
                            }
                        }
                    }
                }

                if (!(events[i].events & EPOLLIN)) continue;

                // Client data ready.
                // Edge-triggered: read until EAGAIN.
                char buf[4096];
                while (true) {
                    ssize_t n = ::read(fd, buf, sizeof(buf));
                    if (n == 0) {
                        // Client disconnected.
                        close_session(fd, "peer closed or read error");
                        break;
                    }
                    if (n < 0) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                        // Read error — disconnect.
                        close_session(fd, "peer closed or read error");
                        break;
                    }

                    Session* session = session_mgr.get_session(fd);
                    if (!session) break;

                    auto lines = session->feed(buf, static_cast<size_t>(n));
                    for (const auto& line : lines) {
                        // Check line length.
                        if (line.size() > config_.max_line_length) {
                            session->send_response(format_error("line too long"));
                            close_session(fd, "line too long");
                            goto next_event; // break out of both loops
                        }

                        // Multi-line blocks (containing \n) are MINSERT — use parse_minsert()
                        Command cmd = (line.find('\n') != std::string::npos)
                                          ? parse_minsert(line)
                                          : parse_command(line);
                        std::string response = execute_command(cmd, *engine_, *session, stats, read_only_.load(std::memory_order_acquire), &engine_->registry(), shard_coord.get());

                        if (response.empty()) {
                            // QUIT. If a previous response is still draining, let it
                            // finish first: closing now would truncate data the
                            // client already asked for and is still reading.
                            if (session->has_pending_output()) {
                                session->request_close_after_flush();
                                arm_epollout(fd);
                                goto next_event;
                            }
                            close_session(fd, "quit");
                            goto next_event;
                        }

                        if (!session->send_response(response)) {
                            // A real error: EPIPE, ECONNRESET or the buffer cap. A
                            // full socket buffer is not one of these — it leaves
                            // bytes queued and is handled just below.
                            close_session(fd, "send failed");
                            goto next_event;
                        }

                        if (session->has_pending_output()) {
                            // The socket took part of the response. The rest goes out
                            // on EPOLLOUT; closing here is what truncated every
                            // response larger than the socket buffer.
                            arm_epollout(fd);
                        }

                        // Enable compression AFTER sending the plain-text ack.
                        if (cmd.type == CommandType::COMPRESS) {
                            session->set_compressed(true);
                        }
                    }
                }
                next_event:;
            }
        }

        // Drain phase: if draining and all sessions are closed, stop the loop.
        if (draining_.load(std::memory_order_relaxed) &&
            stats.active_sessions.load(std::memory_order_relaxed) <= 0) {
            running_.store(false, std::memory_order_relaxed);
        }
    }

    // Shutdown: close all sessions, close epoll, close listen socket.
    session_mgr.close_all();

    if (epoll_fd_ >= 0) {
        ::close(epoll_fd_);
        epoll_fd_ = -1;
    }
    // listen_fd_ may already be closed by shutdown() during drain.
    if (listen_fd_ >= 0) {
        ::close(listen_fd_);
        listen_fd_ = -1;
    }

    // Stop ShardCoordinator before closing engine.
    if (shard_coord) {
        shard_coord->stop();
        shard_coord.reset();
    }

    engine_->close();
}

void TcpServer::arm_epollout(int fd) {
    if (epoll_fd_ < 0 || fd < 0) return;

    struct epoll_event ev{};
    ev.events  = EPOLLIN | EPOLLOUT | EPOLLET;
    ev.data.fd = fd;
    if (::epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd, &ev) < 0) {
        OB_LOG_WARN("tcp_server", "arm_epollout failed: fd=%d errno=%s",
                    fd, std::strerror(errno));
    }
}

void TcpServer::disarm_epollout(int fd) {
    if (epoll_fd_ < 0 || fd < 0) return;

    // Back to read-only interest. Leaving EPOLLOUT armed on an edge-triggered fd
    // makes epoll_wait return immediately whenever the socket is writable, which is
    // almost always, and the loop spins on a core for nothing.
    struct epoll_event ev{};
    ev.events  = EPOLLIN | EPOLLET;
    ev.data.fd = fd;
    if (::epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd, &ev) < 0) {
        OB_LOG_WARN("tcp_server", "disarm_epollout failed: fd=%d errno=%s",
                    fd, std::strerror(errno));
    }
}

void TcpServer::shutdown() {
    // Stop MetricsServer if running.
    if (metrics_server_) {
        metrics_server_->stop();
    }

    // Initiate graceful drain: stop accepting new connections,
    // let in-flight commands finish, then stop the epoll loop.
    draining_.store(true, std::memory_order_relaxed);

    // Close the listen socket so the OS rejects new TCP connections immediately.
    if (listen_fd_ >= 0) {
        ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, listen_fd_, nullptr);
        ::close(listen_fd_);
        listen_fd_ = -1;
    }
}

} // namespace ob
