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
        return format_query_response(rows);
    }

    case CommandType::INSERT: {
        session.increment_commands();
        if (read_only || engine.node_role() == NodeRole::REPLICA) return format_error("read-only replica");
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
            delta.sequence_number = 0; // server-assigned; engine handles sequencing
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

            ob_status_t rc = engine.apply_delta(delta, &level);
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
            delta.sequence_number = 0;
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

            ob_status_t status = engine.apply_delta(delta, levels.data());
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

    case CommandType::QUIT:
        session.increment_commands();
        return ""; // empty string signals session close

    case CommandType::UNKNOWN:
    default:
        session.increment_commands();
        return format_error("unknown command");
    }
}

// ── parse_cli_args ────────────────────────────────────────────────────────────

ServerConfig parse_cli_args(int argc, char* argv[]) {
    ServerConfig config;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];

        if (arg == "--port" && i + 1 < argc) {
            config.port = static_cast<uint16_t>(std::stoi(argv[++i]));
        } else if (arg == "--data-dir" && i + 1 < argc) {
            config.data_dir = argv[++i];
        } else if (arg == "--max-sessions" && i + 1 < argc) {
            config.max_sessions = std::stoi(argv[++i]);
        } else if (arg == "--workers" && i + 1 < argc) {
            config.worker_threads = std::stoi(argv[++i]);
        } else if (arg == "--read-only") {
            config.read_only = true;
        } else if (arg == "--replication-port" && i + 1 < argc) {
            config.replication_port = static_cast<uint16_t>(std::stoi(argv[++i]));
        } else if (arg == "--replication-compress") {
            config.replication_compress = true;
        } else if (arg == "--primary-host" && i + 1 < argc) {
            config.primary_host = argv[++i];
        } else if (arg == "--primary-port" && i + 1 < argc) {
            config.primary_port = static_cast<uint16_t>(std::stoi(argv[++i]));
        } else if (arg == "--snapshot-chunk-size" && i + 1 < argc) {
            config.snapshot_chunk_size = static_cast<size_t>(std::stoul(argv[++i]));
        } else if (arg == "--snapshot-staging-dir" && i + 1 < argc) {
            config.snapshot_staging_dir = argv[++i];
        } else if (arg == "--coordinator-endpoints" && i + 1 < argc) {
            // Parse comma-separated list of endpoints.
            std::string endpoints_str = argv[++i];
            std::string ep;
            for (char c : endpoints_str) {
                if (c == ',') {
                    if (!ep.empty()) config.coordinator_endpoints.push_back(ep);
                    ep.clear();
                } else {
                    ep += c;
                }
            }
            if (!ep.empty()) config.coordinator_endpoints.push_back(ep);
        } else if (arg == "--coordinator-lease-ttl" && i + 1 < argc) {
            config.coordinator_lease_ttl = std::stoll(argv[++i]);
        } else if (arg == "--node-id" && i + 1 < argc) {
            config.node_id = argv[++i];
        } else if (arg == "--failover-enabled" && i + 1 < argc) {
            std::string val = argv[++i];
            config.failover_enabled = (val == "true" || val == "1" || val == "yes");
        } else if (arg == "--ttl-hours" && i + 1 < argc) {
            config.ttl_hours = std::stoull(argv[++i]);
        } else if (arg == "--ttl-scan-interval-seconds" && i + 1 < argc) {
            config.ttl_scan_interval_seconds = std::stoull(argv[++i]);
        } else if (arg == "--metrics-port" && i + 1 < argc) {
            config.metrics_port = static_cast<uint16_t>(std::stoi(argv[++i]));
        } else if (arg == "--log-level" && i + 1 < argc) {
            std::string level_str = argv[++i];
            auto parsed = StructuredLogger::parse_level(level_str);
            if (!parsed.has_value()) {
                std::fprintf(stderr,
                    "Error: invalid log level '%s'. Valid values: ERROR, WARN, INFO, DEBUG\n",
                    level_str.c_str());
                std::exit(1);
            }
            config.log_level = level_str;
        } else if (arg == "--sqpoll-idle-ms" && i + 1 < argc) {
            config.uring_sqpoll_idle_ms = std::stoul(argv[++i]);
        } else if (arg == "--ring-size" && i + 1 < argc) {
            config.uring_ring_size = std::stoul(argv[++i]);
        } else if (arg == "--no-sqpoll") {
            config.uring_no_sqpoll = true;
        } else if (arg == "--shard-id" && i + 1 < argc) {
            config.shard_id = argv[++i];
        } else if (arg == "--shard-vnodes" && i + 1 < argc) {
            config.shard_vnodes = static_cast<uint32_t>(std::stoul(argv[++i]));
        }
    }

    // Validation: --shard-id requires --coordinator-endpoints
    if (!config.shard_id.empty() && config.coordinator_endpoints.empty()) {
        std::fprintf(stderr,
            "Error: --shard-id requires --coordinator-endpoints to be specified. "
            "Shard mode needs etcd for shard map coordination.\n");
        std::exit(1);
    }

    if (!config.shard_id.empty()) {
        OB_LOG_INFO("tcp_server", "Shard mode enabled: shard_id=%s vnodes=%u",
                    config.shard_id.c_str(), config.shard_vnodes);
    }

    return config;
}

// ── TcpServer ─────────────────────────────────────────────────────────────────

TcpServer::TcpServer(ServerConfig config)
    : config_(std::move(config))
    , read_only_(config_.read_only)
{
    ReplicationConfig repl_config{};
    repl_config.port = config_.replication_port;
    repl_config.compress = config_.replication_compress;

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
        failover_config.coordinator.node_id = config_.node_id;
        failover_config.failover_enabled = config_.failover_enabled;
        failover_config.replication_port = config_.replication_port;
        failover_config.replication_address = "127.0.0.1:" + std::to_string(config_.replication_port);
    }

    engine_ = std::make_unique<Engine>(config_.data_dir, 100'000'000ULL, FsyncPolicy::INTERVAL,
                                       repl_config, repl_client_config, failover_config,
                                       TTLConfig{config_.ttl_hours,
                                                 config_.ttl_scan_interval_seconds});
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
                        auto wr = ::write(reject_fd, msg, std::strlen(msg));
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
                        auto wr = ::write(client_fd, msg, std::strlen(msg));
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
                // Client data ready.
                // Edge-triggered: read until EAGAIN.
                char buf[4096];
                while (true) {
                    ssize_t n = ::read(fd, buf, sizeof(buf));
                    if (n == 0) {
                        // Client disconnected.
                        ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
                        session_mgr.remove_session(fd);
                        stats.active_sessions.fetch_sub(1, std::memory_order_relaxed);
                        engine_->registry().increment_gauge("ob_active_sessions", -1);
                        break;
                    }
                    if (n < 0) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                        // Read error — disconnect.
                        ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
                        session_mgr.remove_session(fd);
                        stats.active_sessions.fetch_sub(1, std::memory_order_relaxed);
                        engine_->registry().increment_gauge("ob_active_sessions", -1);
                        break;
                    }

                    Session* session = session_mgr.get_session(fd);
                    if (!session) break;

                    auto lines = session->feed(buf, static_cast<size_t>(n));
                    for (const auto& line : lines) {
                        // Check line length.
                        if (line.size() > config_.max_line_length) {
                            session->send_response(format_error("line too long"));
                            ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
                            session_mgr.remove_session(fd);
                            stats.active_sessions.fetch_sub(1, std::memory_order_relaxed);
                            engine_->registry().increment_gauge("ob_active_sessions", -1);
                            goto next_event; // break out of both loops
                        }

                        // Multi-line blocks (containing \n) are MINSERT — use parse_minsert()
                        Command cmd = (line.find('\n') != std::string::npos)
                                          ? parse_minsert(line)
                                          : parse_command(line);
                        std::string response = execute_command(cmd, *engine_, *session, stats, read_only_.load(std::memory_order_acquire), &engine_->registry(), shard_coord.get());

                        if (response.empty()) {
                            // QUIT — close session.
                            ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
                            session_mgr.remove_session(fd);
                            stats.active_sessions.fetch_sub(1, std::memory_order_relaxed);
                            engine_->registry().increment_gauge("ob_active_sessions", -1);
                            goto next_event;
                        }

                        if (!session->send_response(response)) {
                            // Write failed (client gone).
                            ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
                            session_mgr.remove_session(fd);
                            stats.active_sessions.fetch_sub(1, std::memory_order_relaxed);
                            engine_->registry().increment_gauge("ob_active_sessions", -1);
                            goto next_event;
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
