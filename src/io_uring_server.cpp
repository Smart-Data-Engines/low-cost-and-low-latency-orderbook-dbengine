#ifdef OB_USE_IO_URING

#include "orderbook/io_uring_server.hpp"
#include "orderbook/tcp_server.hpp"  // for execute_command(), parse_cli_args()
#include "orderbook/compression.hpp"
#include "orderbook/logger.hpp"
#include "orderbook/metrics.hpp"
#include "orderbook/metrics_server.hpp"

#include <liburing.h>

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
#include <sys/socket.h>
#include <unistd.h>

namespace ob {

// ── Constructor ──────────────────────────────────────────────────────────────

IoUringServer::IoUringServer(ServerConfig config)
    : config_(std::move(config))
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

    // Allocate buffer pool
    buffer_pool_.resize(static_cast<size_t>(config_.max_sessions) * BUFFER_SIZE);
    buffer_in_use_.resize(static_cast<size_t>(config_.max_sessions), false);
}

// ── Destructor ───────────────────────────────────────────────────────────────

IoUringServer::~IoUringServer() {
    if (ring_) {
        io_uring_queue_exit(ring_);
        delete ring_;
        ring_ = nullptr;
    }
    if (listen_fd_ >= 0) {
        ::close(listen_fd_);
        listen_fd_ = -1;
    }
}


// ── user_data encoding/decoding ──────────────────────────────────────────────

uint64_t IoUringServer::encode_user_data(IoOp op, int fd) {
    return (static_cast<uint64_t>(op) << 56) | static_cast<uint64_t>(fd);
}

IoOp IoUringServer::decode_op(uint64_t user_data) {
    return static_cast<IoOp>(user_data >> 56);
}

int IoUringServer::decode_fd(uint64_t user_data) {
    return static_cast<int>(user_data & 0x00FFFFFFFFFFFFFFULL);
}

// ── Buffer pool management ───────────────────────────────────────────────────

int IoUringServer::alloc_buffer() {
    for (size_t i = 0; i < buffer_in_use_.size(); ++i) {
        if (!buffer_in_use_[i]) {
            buffer_in_use_[i] = true;
            return static_cast<int>(i);
        }
    }
    return -1; // pool exhausted
}

void IoUringServer::free_buffer(int idx) {
    if (idx >= 0 && idx < static_cast<int>(buffer_in_use_.size())) {
        buffer_in_use_[idx] = false;
    }
}

char* IoUringServer::buffer_ptr(int idx) {
    return buffer_pool_.data() + static_cast<size_t>(idx) * BUFFER_SIZE;
}

// ── init_ring() — SQPOLL fallback ───────────────────────────────────────────

void IoUringServer::init_ring() {
    ring_ = new io_uring();
    std::memset(ring_, 0, sizeof(*ring_));

    struct io_uring_params params{};
    int ret = -1;

    // Try with SQPOLL unless explicitly disabled
    if (!config_.uring_no_sqpoll) {
        params.flags = IORING_SETUP_SQPOLL;
        params.sq_thread_idle = config_.uring_sqpoll_idle_ms;

        ret = io_uring_queue_init_params(config_.uring_ring_size, ring_, &params);

        if (ret == 0) {
            sqpoll_enabled_ = true;
            return;
        }

        // SQPOLL failed — log and fall through to retry without it
        OB_LOG_WARN("io_uring", "SQPOLL not available (error=%d), falling back to standard io_uring", -ret);
    }

    // Retry without SQPOLL
    std::memset(&params, 0, sizeof(params));
    ret = io_uring_queue_init_params(config_.uring_ring_size, ring_, &params);

    if (ret < 0) {
        delete ring_;
        ring_ = nullptr;
        throw std::runtime_error(std::string("io_uring_queue_init failed: ") + std::strerror(-ret));
    }

    sqpoll_enabled_ = false;
}

// ── register_buffers() — fallback ───────────────────────────────────────────

void IoUringServer::register_buffers() {
    int max_sessions = config_.max_sessions;
    std::vector<struct iovec> iovs(static_cast<size_t>(max_sessions));

    for (int i = 0; i < max_sessions; ++i) {
        iovs[static_cast<size_t>(i)].iov_base = buffer_ptr(i);
        iovs[static_cast<size_t>(i)].iov_len  = BUFFER_SIZE;
    }

    int ret = io_uring_register_buffers(ring_, iovs.data(), static_cast<unsigned>(max_sessions));
    if (ret < 0) {
        buffers_registered_ = false;
        OB_LOG_WARN("io_uring", "Buffer registration failed (error=%d), using unregistered buffers", -ret);
    } else {
        buffers_registered_ = true;
    }
}


// ── submit_accept() ──────────────────────────────────────────────────────────

void IoUringServer::submit_accept() {
    struct io_uring_sqe* sqe = io_uring_get_sqe(ring_);
    if (!sqe) return; // SQ full — will retry on next iteration

    io_uring_prep_accept(sqe, listen_fd_, nullptr, nullptr, 0);
    io_uring_sqe_set_data64(sqe, encode_user_data(IoOp::ACCEPT, listen_fd_));
}

// ── submit_read() ────────────────────────────────────────────────────────────

void IoUringServer::submit_read(int fd) {
    struct io_uring_sqe* sqe = io_uring_get_sqe(ring_);
    if (!sqe) return;

    if (buffers_registered_) {
        // Find buffer index for this fd
        int idx = -1;
        if (fd < static_cast<int>(fd_to_buf_idx_.size())) {
            idx = fd_to_buf_idx_[static_cast<size_t>(fd)];
        }
        if (idx >= 0) {
            io_uring_prep_read_fixed(sqe, fd, buffer_ptr(idx), BUFFER_SIZE, 0,
                                     static_cast<unsigned>(idx));
        } else {
            // Fallback: no registered buffer for this fd
            io_uring_prep_recv(sqe, fd, buffer_ptr(0), BUFFER_SIZE, 0);
        }
    } else {
        // Unregistered buffers — use recv with fd's assigned buffer
        int idx = -1;
        if (fd < static_cast<int>(fd_to_buf_idx_.size())) {
            idx = fd_to_buf_idx_[static_cast<size_t>(fd)];
        }
        if (idx >= 0) {
            io_uring_prep_recv(sqe, fd, buffer_ptr(idx), BUFFER_SIZE, 0);
        } else {
            // Should not happen, but fallback to first buffer
            io_uring_prep_recv(sqe, fd, buffer_ptr(0), BUFFER_SIZE, 0);
        }
    }

    io_uring_sqe_set_data64(sqe, encode_user_data(IoOp::READ, fd));
}

// ── submit_write() ───────────────────────────────────────────────────────────

void IoUringServer::submit_write(int fd, const char* data, size_t len) {
    struct io_uring_sqe* sqe = io_uring_get_sqe(ring_);
    if (!sqe) return;

    // Copy data into pending_writes_ so it stays alive until the WRITE CQE.
    // io_uring_prep_send takes a pointer — the data must outlive the SQE submission.
    auto& buf = pending_writes_[fd];
    buf.assign(data, len);

    io_uring_prep_send(sqe, fd, buf.data(), buf.size(), 0);
    io_uring_sqe_set_data64(sqe, encode_user_data(IoOp::WRITE, fd));
}

// ── handle_accept() ──────────────────────────────────────────────────────────

void IoUringServer::handle_accept(int client_fd) {
    if (client_fd < 0) {
        OB_LOG_WARN("io_uring", "accept failed: %s", std::strerror(-client_fd));
        submit_accept(); // re-arm
        return;
    }

    // Draining: reject new connections
    if (draining_.load(std::memory_order_relaxed)) {
        static const char drain_msg[] = "ERR server shutting down\n";
        auto wr = ::write(client_fd, drain_msg, sizeof(drain_msg) - 1);
        (void)wr;
        ::close(client_fd);
        submit_accept(); // re-arm
        return;
    }

    // Check session limit
    if (!session_mgr_->add_session(client_fd)) {
        static const char full_msg[] = "ERR server full\n";
        auto wr = ::write(client_fd, full_msg, sizeof(full_msg) - 1);
        (void)wr;
        ::close(client_fd);
        submit_accept(); // re-arm
        return;
    }

    // Allocate buffer for this connection
    int buf_idx = alloc_buffer();
    if (buf_idx < 0) {
        // No buffers available — reject
        session_mgr_->remove_session(client_fd);
        static const char full_msg[] = "ERR server full\n";
        auto wr = ::write(client_fd, full_msg, sizeof(full_msg) - 1);
        (void)wr;
        ::close(client_fd);
        submit_accept(); // re-arm
        return;
    }

    // Ensure fd_to_buf_idx_ is large enough
    if (client_fd >= static_cast<int>(fd_to_buf_idx_.size())) {
        fd_to_buf_idx_.resize(static_cast<size_t>(client_fd) + 1, -1);
    }
    fd_to_buf_idx_[static_cast<size_t>(client_fd)] = buf_idx;

    // Update stats
    stats_.active_sessions.fetch_add(1, std::memory_order_relaxed);
    engine_->registry().increment_gauge("ob_active_sessions");

    // Send welcome banner
    static const char welcome[] = "OK ob_tcp_server v0.1.0\n";
    submit_write(client_fd, welcome, sizeof(welcome) - 1);

    // Submit read for incoming data
    submit_read(client_fd);

    // Re-arm accept
    submit_accept();
}


// ── handle_read() ────────────────────────────────────────────────────────────

void IoUringServer::handle_read(int fd, int bytes_read) {
    if (bytes_read == 0) {
        // Client disconnected
        OB_LOG_DEBUG("io_uring", "client disconnected: fd=%d", fd);
        session_mgr_->remove_session(fd);
        if (fd < static_cast<int>(fd_to_buf_idx_.size()) && fd_to_buf_idx_[static_cast<size_t>(fd)] >= 0) {
            free_buffer(fd_to_buf_idx_[static_cast<size_t>(fd)]);
            fd_to_buf_idx_[static_cast<size_t>(fd)] = -1;
        }
        pending_writes_.erase(fd);
        ::close(fd);
        stats_.active_sessions.fetch_sub(1, std::memory_order_relaxed);
        engine_->registry().increment_gauge("ob_active_sessions", -1);
        return;
    }

    if (bytes_read < 0) {
        // Read error
        OB_LOG_WARN("io_uring", "read error on fd=%d: %s", fd, std::strerror(-bytes_read));
        session_mgr_->remove_session(fd);
        if (fd < static_cast<int>(fd_to_buf_idx_.size()) && fd_to_buf_idx_[static_cast<size_t>(fd)] >= 0) {
            free_buffer(fd_to_buf_idx_[static_cast<size_t>(fd)]);
            fd_to_buf_idx_[static_cast<size_t>(fd)] = -1;
        }
        pending_writes_.erase(fd);
        ::close(fd);
        stats_.active_sessions.fetch_sub(1, std::memory_order_relaxed);
        engine_->registry().increment_gauge("ob_active_sessions", -1);
        return;
    }

    // Get session
    Session* session = session_mgr_->get_session(fd);
    if (!session) {
        pending_writes_.erase(fd);
        ::close(fd);
        return;
    }

    // Get buffer pointer for this fd
    int buf_idx = -1;
    if (fd < static_cast<int>(fd_to_buf_idx_.size())) {
        buf_idx = fd_to_buf_idx_[static_cast<size_t>(fd)];
    }
    char* buf = (buf_idx >= 0) ? buffer_ptr(buf_idx) : nullptr;
    if (!buf) {
        pending_writes_.erase(fd);
        ::close(fd);
        return;
    }

    // Feed data to session and process lines
    auto lines = session->feed(buf, static_cast<size_t>(bytes_read));
    for (const auto& line : lines) {
        // Check line length
        if (line.size() > config_.max_line_length) {
            OB_LOG_WARN("io_uring", "line too long from fd=%d", fd);
            std::string err_response = format_error("line too long");
            submit_write(fd, err_response.c_str(), err_response.size());
            // Close session after sending error
            session_mgr_->remove_session(fd);
            if (buf_idx >= 0) {
                free_buffer(buf_idx);
                fd_to_buf_idx_[static_cast<size_t>(fd)] = -1;
            }
            // Don't erase pending_writes_ here — submit_write just added data
            ::close(fd);
            stats_.active_sessions.fetch_sub(1, std::memory_order_relaxed);
            engine_->registry().increment_gauge("ob_active_sessions", -1);
            return;
        }

        // Parse and execute command
        Command cmd = (line.find('\n') != std::string::npos)
                          ? parse_minsert(line)
                          : parse_command(line);

        std::string response = execute_command(cmd, *engine_, *session, stats_,
                                               config_.read_only, &engine_->registry());

        if (response.empty()) {
            // QUIT — close session
            session_mgr_->remove_session(fd);
            if (buf_idx >= 0) {
                free_buffer(buf_idx);
                fd_to_buf_idx_[static_cast<size_t>(fd)] = -1;
            }
            pending_writes_.erase(fd);
            ::close(fd);
            stats_.active_sessions.fetch_sub(1, std::memory_order_relaxed);
            engine_->registry().increment_gauge("ob_active_sessions", -1);
            return;
        }

        // If session is compressed, wrap response in [4-byte BE len][LZ4 frame].
        if (session->is_compressed()) {
            auto compressed = lz4_compress(response.data(), response.size());
            uint32_t frame_len = static_cast<uint32_t>(compressed.size());
            std::string framed;
            framed.reserve(4 + compressed.size());
            framed.push_back(static_cast<char>((frame_len >> 24) & 0xFF));
            framed.push_back(static_cast<char>((frame_len >> 16) & 0xFF));
            framed.push_back(static_cast<char>((frame_len >> 8) & 0xFF));
            framed.push_back(static_cast<char>(frame_len & 0xFF));
            framed.append(reinterpret_cast<const char*>(compressed.data()),
                          compressed.size());
            submit_write(fd, framed.c_str(), framed.size());
        } else {
            submit_write(fd, response.c_str(), response.size());
        }

        // Enable compression AFTER sending the plain-text ack.
        if (cmd.type == CommandType::COMPRESS) {
            session->set_compressed(true);
        }
    }

    // Re-arm read
    submit_read(fd);
}

// ── handle_write() ───────────────────────────────────────────────────────────

void IoUringServer::handle_write(int fd, int bytes_written) {
    if (bytes_written < 0) {
        // Write error — disconnect
        OB_LOG_WARN("io_uring", "write error on fd=%d: %s", fd, std::strerror(-bytes_written));
        session_mgr_->remove_session(fd);
        if (fd < static_cast<int>(fd_to_buf_idx_.size()) && fd_to_buf_idx_[static_cast<size_t>(fd)] >= 0) {
            free_buffer(fd_to_buf_idx_[static_cast<size_t>(fd)]);
            fd_to_buf_idx_[static_cast<size_t>(fd)] = -1;
        }
        pending_writes_.erase(fd);
        ::close(fd);
        stats_.active_sessions.fetch_sub(1, std::memory_order_relaxed);
        engine_->registry().increment_gauge("ob_active_sessions", -1);
        return;
    }

    // Write completed — free the pending write buffer.
    pending_writes_.erase(fd);
}


// ── run() — io_uring event loop ──────────────────────────────────────────────

void IoUringServer::run() {
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

    // Initialize io_uring ring
    init_ring();
    register_buffers();

    // Create TCP listen socket
    listen_fd_ = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    if (listen_fd_ < 0) {
        throw std::runtime_error(std::string("socket() failed: ") + std::strerror(errno));
    }

    int opt = 1;
    if (::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        throw std::runtime_error(std::string("setsockopt() failed: ") + std::strerror(errno));
    }

    struct sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons(config_.port);

    if (::bind(listen_fd_, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
        throw std::runtime_error(std::string("bind() failed on port ")
                                 + std::to_string(config_.port) + ": " + std::strerror(errno));
    }

    if (::listen(listen_fd_, 128) < 0) {
        throw std::runtime_error(std::string("listen() failed: ") + std::strerror(errno));
    }

    // Create SessionManager
    session_mgr_ = std::make_unique<SessionManager>(config_.max_sessions);

    // Log startup configuration
    OB_LOG_INFO("io_uring", "IoUringServer starting: port=%d, ring_size=%u, sqpoll=%s, registered_buffers=%s",
                config_.port, config_.uring_ring_size,
                sqpoll_enabled_ ? "enabled" : "disabled",
                buffers_registered_ ? "yes" : "no");

    // Submit initial accept and kick the ring
    submit_accept();
    io_uring_submit(ring_);

    running_.store(true, std::memory_order_relaxed);

    // Event loop
    struct __kernel_timespec timeout{};
    timeout.tv_sec  = 0;
    timeout.tv_nsec = 100'000'000; // 100ms

    while (running_.load(std::memory_order_relaxed)) {
        struct io_uring_cqe* cqe = nullptr;
        int ret = io_uring_wait_cqe_timeout(ring_, &cqe, &timeout);

        if (ret == -ETIME || ret == -EINTR) {
            // Timeout or interrupted — check drain condition and continue
            if (draining_.load(std::memory_order_relaxed) &&
                stats_.active_sessions.load(std::memory_order_relaxed) <= 0) {
                running_.store(false, std::memory_order_relaxed);
            }
            continue;
        }

        if (ret < 0) {
            OB_LOG_WARN("io_uring", "io_uring_wait_cqe_timeout error: %s", std::strerror(-ret));
            continue;
        }

        // Process all available CQEs
        unsigned head = 0;
        unsigned count = 0;
        io_uring_for_each_cqe(ring_, head, cqe) {
            uint64_t user_data = cqe->user_data;
            IoOp op = decode_op(user_data);
            int  fd = decode_fd(user_data);

            switch (op) {
                case IoOp::ACCEPT:
                    handle_accept(cqe->res);
                    break;
                case IoOp::READ:
                    handle_read(fd, cqe->res);
                    break;
                case IoOp::WRITE:
                    handle_write(fd, cqe->res);
                    break;
            }
            ++count;
        }
        io_uring_cq_advance(ring_, count);

        // Batch submit all new SQEs generated by handlers
        io_uring_submit(ring_);

        // Update io_uring metrics
        engine_->registry().increment_counter("ob_iouring_cqe_processed", count);
        engine_->registry().increment_counter("ob_iouring_sqe_submitted", count);

        // Drain check
        if (draining_.load(std::memory_order_relaxed) &&
            stats_.active_sessions.load(std::memory_order_relaxed) <= 0) {
            running_.store(false, std::memory_order_relaxed);
        }
    }

    // Cleanup
    session_mgr_->close_all();

    if (buffers_registered_) {
        io_uring_unregister_buffers(ring_);
    }

    if (ring_) {
        io_uring_queue_exit(ring_);
        delete ring_;
        ring_ = nullptr;
    }

    if (listen_fd_ >= 0) {
        ::close(listen_fd_);
        listen_fd_ = -1;
    }

    engine_->close();
}

// ── shutdown() ───────────────────────────────────────────────────────────────

void IoUringServer::shutdown() {
    // Stop MetricsServer if running.
    if (metrics_server_) {
        metrics_server_->stop();
    }

    // Initiate graceful drain
    draining_.store(true, std::memory_order_relaxed);

    // Close the listen socket so the OS rejects new TCP connections immediately.
    if (listen_fd_ >= 0) {
        ::close(listen_fd_);
        listen_fd_ = -1;
    }
}

} // namespace ob

#endif // OB_USE_IO_URING
