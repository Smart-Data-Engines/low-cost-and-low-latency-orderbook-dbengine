#include "orderbook/tcp_server.hpp"

#include <cerrno>
#include <chrono>
#include <cstdio>
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
                            bool read_only) {
    switch (cmd.type) {

    case CommandType::SELECT: {
        // Reject queries during snapshot bootstrap.
        if (engine.is_bootstrapping()) return format_error("bootstrapping");
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
        session.increment_queries();
        stats.total_queries.fetch_add(1, std::memory_order_relaxed);
        return format_query_response(rows);
    }

    case CommandType::INSERT: {
        if (read_only) return format_error("read-only replica");
        if (engine.is_bootstrapping()) return format_error("bootstrapping");
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
        session.increment_inserts();
        stats.total_inserts.fetch_add(1, std::memory_order_relaxed);
        return format_ok();
    }

    case CommandType::FLUSH: {
        if (read_only) return format_error("read-only replica");
        if (engine.is_bootstrapping()) return format_error("bootstrapping");
        try {
            // Flush pattern: close engine (flushes all data) then reopen.
            // NOTE: This is called on the TcpServer's engine, which is shared.
            // The caller must ensure thread-safety (Engine has internal mutex).
            engine.close();
            engine.open();
        } catch (const std::exception& e) {
            return format_error(e.what());
        }
        return format_ok();
    }

    case CommandType::PING:
        return format_pong();

    case CommandType::STATUS: {
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

        return format_status(stats);
    }

    case CommandType::QUIT:
        return ""; // empty string signals session close

    case CommandType::UNKNOWN:
    default:
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
        } else if (arg == "--primary-host" && i + 1 < argc) {
            config.primary_host = argv[++i];
        } else if (arg == "--primary-port" && i + 1 < argc) {
            config.primary_port = static_cast<uint16_t>(std::stoi(argv[++i]));
        } else if (arg == "--snapshot-chunk-size" && i + 1 < argc) {
            config.snapshot_chunk_size = static_cast<size_t>(std::stoul(argv[++i]));
        } else if (arg == "--snapshot-staging-dir" && i + 1 < argc) {
            config.snapshot_staging_dir = argv[++i];
        }
    }

    return config;
}

// ── TcpServer ─────────────────────────────────────────────────────────────────

TcpServer::TcpServer(ServerConfig config)
    : config_(std::move(config))
{
    ReplicationConfig repl_config{};
    repl_config.port = config_.replication_port;

    ReplicationClientConfig repl_client_config{};
    repl_client_config.primary_host = config_.primary_host;
    repl_client_config.primary_port = config_.primary_port;
    repl_client_config.state_file   = config_.data_dir + "/repl_state.txt";
    repl_client_config.snapshot_chunk_size = config_.snapshot_chunk_size;
    repl_client_config.snapshot_staging_dir = config_.snapshot_staging_dir;

    engine_ = std::make_unique<Engine>(config_.data_dir, 100'000'000ULL, FsyncPolicy::INTERVAL,
                                       repl_config, repl_client_config);
}

TcpServer::~TcpServer() {
    if (epoll_fd_ >= 0) ::close(epoll_fd_);
    if (listen_fd_ >= 0) ::close(listen_fd_);
}

void TcpServer::run() {
    // Open the engine (replay WAL, start flush thread).
    engine_->open();

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

                    // Send welcome message.
                    Session* s = session_mgr.get_session(client_fd);
                    if (s) {
                        s->send_response("OK ob_tcp_server v0.1.0\n");
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
                        break;
                    }
                    if (n < 0) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                        // Read error — disconnect.
                        ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
                        session_mgr.remove_session(fd);
                        stats.active_sessions.fetch_sub(1, std::memory_order_relaxed);
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
                            goto next_event; // break out of both loops
                        }

                        Command cmd = parse_command(line);
                        std::string response = execute_command(cmd, *engine_, *session, stats, config_.read_only);

                        if (response.empty()) {
                            // QUIT — close session.
                            ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
                            session_mgr.remove_session(fd);
                            stats.active_sessions.fetch_sub(1, std::memory_order_relaxed);
                            goto next_event;
                        }

                        if (!session->send_response(response)) {
                            // Write failed (client gone).
                            ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
                            session_mgr.remove_session(fd);
                            stats.active_sessions.fetch_sub(1, std::memory_order_relaxed);
                            goto next_event;
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

    engine_->close();
}

void TcpServer::shutdown() {
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
