// WAL Streaming Replication — ReplicationManager (primary) + ReplicationClient (replica).
//
// The ReplicationManager runs its own epoll thread to accept replica connections,
// stream WAL records, handle ACKs, and send heartbeats.
//
// Wire protocol (text+binary hybrid, newline-delimited control messages):
//   Handshake:  REPLICATE <file_index> <byte_offset>\n
//   WAL record: WAL <file_index> <byte_offset> <total_len>\n<WALRecord(24)><payload>
//   ACK:        ACK <file_index> <byte_offset>\n
//   Heartbeat:  HEARTBEAT\n
//   Error:      ERR <message>\n
//
// Requirements: 1.1, 1.2, 1.3, 1.4, 4.1, 4.2, 4.3, 4.4, 4.5, 6.2, 6.3

#include "orderbook/replication.hpp"
#include "orderbook/engine.hpp"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <stdexcept>
#include <string>
#include <vector>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

namespace ob {

// ── Helpers ───────────────────────────────────────────────────────────────────

namespace {

/// Build WAL filename for a given index: wal_000000.bin
static std::string wal_filename(const std::string& dir, uint32_t index) {
    char buf[32];
    std::snprintf(buf, sizeof(buf), "wal_%06u.bin", index);
    return dir + "/" + buf;
}

/// Send all bytes, handling partial writes. Returns true on success.
static bool send_all(int fd, const void* data, size_t len) {
    const auto* ptr = static_cast<const uint8_t*>(data);
    size_t remaining = len;
    while (remaining > 0) {
        ssize_t n = ::send(fd, ptr, remaining, MSG_NOSIGNAL);
        if (n <= 0) {
            if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                return false;
            }
            return false;
        }
        ptr += n;
        remaining -= static_cast<size_t>(n);
    }
    return true;
}

/// Read exactly `len` bytes from fd. Returns true on success, false on error/disconnect.
static bool recv_all(int fd, void* data, size_t len) {
    auto* ptr = static_cast<uint8_t*>(data);
    size_t remaining = len;
    while (remaining > 0) {
        ssize_t n = ::recv(fd, ptr, remaining, 0);
        if (n <= 0) return false;
        ptr += n;
        remaining -= static_cast<size_t>(n);
    }
    return true;
}

/// Read a newline-terminated line from fd into buf.
/// Returns number of bytes read (excluding newline), 0 on EAGAIN, -1 on error/disconnect.
static ssize_t read_line(int fd, char* buf, size_t buf_size) {
    size_t pos = 0;
    while (pos < buf_size - 1) {
        ssize_t n = ::recv(fd, buf + pos, 1, 0);
        if (n == 0) return -1; // disconnect
        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                return (pos > 0) ? static_cast<ssize_t>(pos) : 0;
            }
            return -1;
        }
        if (buf[pos] == '\n') {
            buf[pos] = '\0';
            return static_cast<ssize_t>(pos);
        }
        ++pos;
    }
    buf[pos] = '\0';
    return static_cast<ssize_t>(pos);
}

// ── Portable CRC32C (software lookup table, same as wal.cpp) ──────────────────
static constexpr uint32_t CRC32C_POLY = 0x82F63B78u;
static uint32_t crc32c_table[256];
static bool     crc32c_table_init = false;

static void init_crc32c_table() {
    for (uint32_t i = 0; i < 256; ++i) {
        uint32_t crc = i;
        for (int j = 0; j < 8; ++j) {
            crc = (crc >> 1) ^ ((crc & 1u) ? CRC32C_POLY : 0u);
        }
        crc32c_table[i] = crc;
    }
    crc32c_table_init = true;
}

static uint32_t crc32c(const void* data, size_t len) {
    if (!crc32c_table_init) init_crc32c_table();
    const auto* p = static_cast<const uint8_t*>(data);
    uint32_t crc = 0xFFFFFFFFu;
    for (size_t i = 0; i < len; ++i) {
        crc = (crc >> 8) ^ crc32c_table[(crc ^ p[i]) & 0xFFu];
    }
    return crc ^ 0xFFFFFFFFu;
}

} // anonymous namespace

// ── ReplicationManager ────────────────────────────────────────────────────────

ReplicationManager::ReplicationManager(ReplicationConfig config, WALWriter& wal)
    : config_(std::move(config))
    , wal_(wal)
{}

ReplicationManager::~ReplicationManager() {
    stop();
}

void ReplicationManager::start() {
    if (config_.port == 0) return;
    if (running_.load(std::memory_order_relaxed)) return;

    // 1. Create non-blocking TCP socket for replication port.
    listen_fd_ = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    if (listen_fd_ < 0) {
        throw std::runtime_error(std::string("ReplicationManager: socket() failed: ") +
                                 std::strerror(errno));
    }

    int opt = 1;
    ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    // 2. Bind to 0.0.0.0:replication_port.
    struct sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons(config_.port);

    if (::bind(listen_fd_, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
        ::close(listen_fd_);
        listen_fd_ = -1;
        throw std::runtime_error(std::string("ReplicationManager: bind() failed on port ") +
                                 std::to_string(config_.port) + ": " + std::strerror(errno));
    }

    if (::listen(listen_fd_, 16) < 0) {
        ::close(listen_fd_);
        listen_fd_ = -1;
        throw std::runtime_error(std::string("ReplicationManager: listen() failed: ") +
                                 std::strerror(errno));
    }

    // 3. Create epoll instance.
    epoll_fd_ = ::epoll_create1(0);
    if (epoll_fd_ < 0) {
        ::close(listen_fd_);
        listen_fd_ = -1;
        throw std::runtime_error(std::string("ReplicationManager: epoll_create1() failed: ") +
                                 std::strerror(errno));
    }

    // 4. Add listen socket to epoll.
    struct epoll_event ev{};
    ev.events  = EPOLLIN;
    ev.data.fd = listen_fd_;
    ::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, listen_fd_, &ev);

    // 5. Start epoll thread.
    running_.store(true, std::memory_order_release);
    thread_ = std::thread([this]() { run_loop(); });
}

void ReplicationManager::stop() {
    if (!running_.load(std::memory_order_relaxed)) return;

    running_.store(false, std::memory_order_release);

    if (thread_.joinable()) {
        thread_.join();
    }

    // Close all replica fds.
    {
        std::lock_guard<std::mutex> lock(mtx_);
        for (auto& r : replicas_) {
            if (r.fd >= 0) {
                ::close(r.fd);
                r.fd = -1;
            }
        }
        replicas_.clear();
    }

    if (epoll_fd_ >= 0) {
        ::close(epoll_fd_);
        epoll_fd_ = -1;
    }
    if (listen_fd_ >= 0) {
        ::close(listen_fd_);
        listen_fd_ = -1;
    }
}

void ReplicationManager::broadcast(const WALRecord& hdr, const void* payload,
                                    size_t payload_len) {
    // Format: WAL <file_index> <byte_offset> <total_len>\n<WALRecord(24)><payload>
    // total_len = sizeof(WALRecord) + payload_len
    const uint32_t file_index = wal_.current_file_index();
    const size_t total_len = sizeof(WALRecord) + payload_len;

    // Build the text header line.
    char line[128];
    int line_len = std::snprintf(line, sizeof(line), "WAL %u %zu %zu\n",
                                  file_index, static_cast<size_t>(0), total_len);

    // Build the complete message: text header + WALRecord bytes + payload bytes.
    std::vector<uint8_t> msg(static_cast<size_t>(line_len) + total_len);
    std::memcpy(msg.data(), line, static_cast<size_t>(line_len));
    std::memcpy(msg.data() + line_len, &hdr, sizeof(WALRecord));
    if (payload_len > 0) {
        std::memcpy(msg.data() + line_len + sizeof(WALRecord), payload, payload_len);
    }

    std::lock_guard<std::mutex> lock(mtx_);
    for (auto it = replicas_.begin(); it != replicas_.end(); ) {
        if (!send_all(it->fd, msg.data(), msg.size())) {
            // Write failed — replica disconnected or too slow.
            ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, it->fd, nullptr);
            ::close(it->fd);
            it = replicas_.erase(it);
        } else {
            ++it;
        }
    }
}

std::vector<ReplicaInfo> ReplicationManager::replica_states() const {
    std::lock_guard<std::mutex> lock(mtx_);
    return replicas_;
}

void ReplicationManager::run_loop() {
    static constexpr int MAX_EVENTS = 32;
    struct epoll_event events[MAX_EVENTS];

    auto last_heartbeat = std::chrono::steady_clock::now();

    while (running_.load(std::memory_order_acquire)) {
        int nfds = ::epoll_wait(epoll_fd_, events, MAX_EVENTS, 100 /*ms timeout*/);
        if (nfds < 0) {
            if (errno == EINTR) continue;
            break; // fatal epoll error
        }

        for (int i = 0; i < nfds; ++i) {
            int fd = events[i].data.fd;

            if (fd == listen_fd_) {
                accept_replica();
            } else {
                handle_replica_data(fd);
            }
        }

        // Send heartbeat every 5 seconds when idle.
        auto now = std::chrono::steady_clock::now();
        if (std::chrono::duration_cast<std::chrono::seconds>(now - last_heartbeat).count() >= 5) {
            last_heartbeat = now;
            const char* hb = "HEARTBEAT\n";
            const size_t hb_len = 10;

            std::lock_guard<std::mutex> lock(mtx_);
            for (auto it = replicas_.begin(); it != replicas_.end(); ) {
                if (!send_all(it->fd, hb, hb_len)) {
                    ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, it->fd, nullptr);
                    ::close(it->fd);
                    it = replicas_.erase(it);
                } else {
                    ++it;
                }
            }
        }
    }
}

void ReplicationManager::accept_replica() {
    while (true) {
        struct sockaddr_in client_addr{};
        socklen_t client_len = sizeof(client_addr);
        int client_fd = ::accept4(listen_fd_,
                                   reinterpret_cast<struct sockaddr*>(&client_addr),
                                   &client_len,
                                   SOCK_NONBLOCK);
        if (client_fd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            break;
        }

        // Check max replicas limit (Requirement 1.4).
        {
            std::lock_guard<std::mutex> lock(mtx_);
            if (static_cast<int>(replicas_.size()) >= config_.max_replicas) {
                const char* msg = "ERR max_replicas_reached\n";
                auto wr = ::write(client_fd, msg, std::strlen(msg));
                (void)wr;
                ::close(client_fd);
                continue;
            }
        }

        // Build address string for logging.
        char addr_str[64];
        ::inet_ntop(AF_INET, &client_addr.sin_addr, addr_str, sizeof(addr_str));
        std::string address = std::string(addr_str) + ":" +
                              std::to_string(ntohs(client_addr.sin_port));

        // Add to epoll (edge-triggered for client data).
        struct epoll_event ev{};
        ev.events  = EPOLLIN | EPOLLET;
        ev.data.fd = client_fd;
        if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, client_fd, &ev) < 0) {
            ::close(client_fd);
            continue;
        }

        // Add to replicas list.
        ReplicaInfo info;
        info.fd               = client_fd;
        info.address          = std::move(address);
        info.confirmed_file   = 0;
        info.confirmed_offset = 0;

        std::lock_guard<std::mutex> lock(mtx_);
        replicas_.push_back(std::move(info));
    }
}

void ReplicationManager::handle_replica_data(int fd) {
    // Edge-triggered: read until EAGAIN.
    char buf[512];
    while (true) {
        ssize_t n = read_line(fd, buf, sizeof(buf));
        if (n < 0) {
            // Disconnect — remove replica (Requirement 1.3).
            ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
            std::lock_guard<std::mutex> lock(mtx_);
            for (auto it = replicas_.begin(); it != replicas_.end(); ++it) {
                if (it->fd == fd) {
                    ::close(fd);
                    replicas_.erase(it);
                    break;
                }
            }
            return;
        }
        if (n == 0) break; // EAGAIN, no more data

        std::string line(buf);

        // Parse REPLICATE handshake: REPLICATE <file_index> <byte_offset>
        if (line.rfind("REPLICATE ", 0) == 0) {
            uint32_t from_file = 0;
            size_t from_offset = 0;
            if (std::sscanf(line.c_str(), "REPLICATE %u %zu", &from_file, &from_offset) == 2) {
                std::lock_guard<std::mutex> lock(mtx_);
                for (auto& r : replicas_) {
                    if (r.fd == fd) {
                        r.confirmed_file   = from_file;
                        r.confirmed_offset = from_offset;
                        // Perform catchup from the requested position.
                        handle_catchup(r, from_file, from_offset);
                        break;
                    }
                }
            }
            continue;
        }

        // Parse ACK: ACK <file_index> <byte_offset>
        if (line.rfind("ACK ", 0) == 0) {
            uint32_t ack_file = 0;
            size_t ack_offset = 0;
            if (std::sscanf(line.c_str(), "ACK %u %zu", &ack_file, &ack_offset) == 2) {
                std::lock_guard<std::mutex> lock(mtx_);
                for (auto& r : replicas_) {
                    if (r.fd == fd) {
                        r.confirmed_file   = ack_file;
                        r.confirmed_offset = ack_offset;
                        break;
                    }
                }
            }
            continue;
        }

        // Unknown message — ignore.
    }
}

void ReplicationManager::send_to_replica(ReplicaInfo& replica, const WALRecord& hdr,
                                          const void* payload, size_t payload_len) {
    // Format: WAL <file_index> <byte_offset> <total_len>\n<WALRecord(24)><payload>
    const size_t total_len = sizeof(WALRecord) + payload_len;

    char line[128];
    int line_len = std::snprintf(line, sizeof(line), "WAL %u %zu %zu\n",
                                  replica.confirmed_file,
                                  replica.confirmed_offset,
                                  total_len);

    // Build complete message.
    std::vector<uint8_t> msg(static_cast<size_t>(line_len) + total_len);
    std::memcpy(msg.data(), line, static_cast<size_t>(line_len));
    std::memcpy(msg.data() + line_len, &hdr, sizeof(WALRecord));
    if (payload_len > 0) {
        std::memcpy(msg.data() + line_len + sizeof(WALRecord), payload, payload_len);
    }

    if (!send_all(replica.fd, msg.data(), msg.size())) {
        // Mark fd as -1; caller should clean up.
        ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, replica.fd, nullptr);
        ::close(replica.fd);
        replica.fd = -1;
    }
}

void ReplicationManager::handle_catchup(ReplicaInfo& replica, uint32_t from_file,
                                         size_t from_offset) {
    // Read historical WAL files starting from from_file and stream records to
    // the replica. Uses WALReplayer-like logic: open file, read WALRecord
    // headers + payloads, send each as a WAL message.

    const uint32_t current_file = wal_.current_file_index();
    const std::string& wal_dir = wal_.dir();

    // Stream records from from_file to current_file.
    for (uint32_t fi = from_file; fi <= current_file; ++fi) {
        std::string path = wal_filename(wal_dir, fi);
        int fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            // File doesn't exist — WAL has been truncated (Requirement 6.3).
            if (fi == from_file) {
                const char* err = "ERR WAL_TRUNCATED\n";
                send_all(replica.fd, err, std::strlen(err));
                return;
            }
            // Later files missing is unexpected but not fatal — stop catchup.
            break;
        }

        // If this is the first file and we have an offset, seek to it.
        if (fi == from_file && from_offset > 0) {
            off_t pos = ::lseek(fd, static_cast<off_t>(from_offset), SEEK_SET);
            if (pos < 0) {
                ::close(fd);
                const char* err = "ERR WAL_TRUNCATED\n";
                send_all(replica.fd, err, std::strlen(err));
                return;
            }
        }

        // Read records and stream them.
        size_t file_offset = (fi == from_file) ? from_offset : 0;
        while (true) {
            WALRecord hdr{};
            ssize_t n = ::read(fd, &hdr, sizeof(WALRecord));
            if (n == 0) break; // EOF
            if (n != static_cast<ssize_t>(sizeof(WALRecord))) break; // truncated

            // Read payload.
            std::vector<uint8_t> payload(hdr.payload_len);
            if (hdr.payload_len > 0) {
                size_t remaining = hdr.payload_len;
                uint8_t* ptr = payload.data();
                while (remaining > 0) {
                    ssize_t r = ::read(fd, ptr, remaining);
                    if (r <= 0) goto done_file;
                    ptr += r;
                    remaining -= static_cast<size_t>(r);
                }
            }

            // Skip ROTATE records — they're internal to WAL file management.
            if (hdr.record_type == WAL_RECORD_ROTATE) {
                break; // move to next file
            }

            // Send this record to the replica.
            send_to_replica(replica, hdr, payload.data(), hdr.payload_len);
            if (replica.fd < 0) {
                // Replica disconnected during catchup.
                ::close(fd);
                return;
            }

            file_offset += sizeof(WALRecord) + hdr.payload_len;
        }

        done_file:
        ::close(fd);
    }
}

// ── ReplicationClient (Requirements: 2.1, 2.2, 2.3, 2.4, 4.2, 4.3, 4.4) ────

ReplicationClient::ReplicationClient(ReplicationClientConfig config, Engine& engine)
    : config_(std::move(config))
    , engine_(engine)
{}

ReplicationClient::~ReplicationClient() {
    stop();
}

void ReplicationClient::start() {
    if (config_.primary_port == 0) return;
    if (running_.load(std::memory_order_relaxed)) return;

    // Load last confirmed offset from state file (Requirement 6.1, 6.2).
    load_state();

    running_.store(true, std::memory_order_release);
    thread_ = std::thread([this]() { run_loop(); });
}

void ReplicationClient::stop() {
    if (!running_.load(std::memory_order_relaxed)) return;

    running_.store(false, std::memory_order_release);

    // Shutdown the socket to unblock any blocking recv() in the receive thread.
    if (fd_ >= 0) {
        ::shutdown(fd_, SHUT_RDWR);
    }

    if (thread_.joinable()) {
        thread_.join();
    }

    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }

    save_state();
}

ReplicationClient::State ReplicationClient::state() const {
    return State{confirmed_file_, confirmed_offset_, (fd_ >= 0), records_replayed_};
}

void ReplicationClient::run_loop() {
    // Reconnection loop with exponential backoff (Requirement 6.2).
    int backoff_sec = 5;
    static constexpr int MAX_BACKOFF_SEC = 60;

    while (running_.load(std::memory_order_acquire)) {
        try {
            connect_to_primary();
            backoff_sec = 5; // Reset backoff on successful connect.
            receive_and_replay();
        } catch (...) {
            // Connection failed or receive error — will retry.
        }

        // Clean up socket on disconnect.
        if (fd_ >= 0) {
            ::close(fd_);
            fd_ = -1;
        }

        // Wait before reconnecting, checking running_ periodically.
        for (int i = 0; i < backoff_sec * 10 && running_.load(std::memory_order_acquire); ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }

        // Exponential backoff: 5 → 10 → 20 → 40 → 60 (capped).
        backoff_sec = std::min(backoff_sec * 2, MAX_BACKOFF_SEC);
    }
}

void ReplicationClient::connect_to_primary() {
    // Create a blocking TCP socket (Requirement 4.1).
    fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd_ < 0) {
        throw std::runtime_error(std::string("ReplicationClient: socket() failed: ") +
                                 std::strerror(errno));
    }

    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(config_.primary_port);

    if (::inet_pton(AF_INET, config_.primary_host.c_str(), &addr.sin_addr) <= 0) {
        ::close(fd_);
        fd_ = -1;
        throw std::runtime_error("ReplicationClient: invalid primary_host: " +
                                 config_.primary_host);
    }

    if (::connect(fd_, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
        ::close(fd_);
        fd_ = -1;
        throw std::runtime_error(std::string("ReplicationClient: connect() failed: ") +
                                 std::strerror(errno));
    }

    // Set a receive timeout so we can periodically check running_ flag.
    struct timeval tv{};
    tv.tv_sec  = 5;
    tv.tv_usec = 0;
    ::setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    // Send REPLICATE handshake (Requirement 4.2).
    char handshake[128];
    int len = std::snprintf(handshake, sizeof(handshake), "REPLICATE %u %zu\n",
                            confirmed_file_, confirmed_offset_);
    if (!send_all(fd_, handshake, static_cast<size_t>(len))) {
        ::close(fd_);
        fd_ = -1;
        throw std::runtime_error("ReplicationClient: failed to send handshake");
    }
}

void ReplicationClient::receive_and_replay() {
    char line_buf[512];
    auto last_save = std::chrono::steady_clock::now();

    while (running_.load(std::memory_order_acquire)) {
        // Periodic state save every 10 seconds (Requirement 6.1).
        auto now = std::chrono::steady_clock::now();
        if (std::chrono::duration_cast<std::chrono::seconds>(now - last_save).count() >= 10) {
            save_state();
            last_save = now;
        }
        // Read a text header line.
        ssize_t n = read_line(fd_, line_buf, sizeof(line_buf));
        if (n < 0) {
            // Disconnect or error.
            return;
        }
        if (n == 0) {
            // Timeout (EAGAIN from SO_RCVTIMEO) — check running_ and continue.
            continue;
        }

        // Parse WAL record: WAL <file_index> <byte_offset> <total_len>
        if (std::strncmp(line_buf, "WAL ", 4) == 0) {
            uint32_t file_index = 0;
            size_t byte_offset = 0;
            size_t total_len = 0;
            if (std::sscanf(line_buf, "WAL %u %zu %zu",
                            &file_index, &byte_offset, &total_len) != 3) {
                continue; // Malformed — skip.
            }

            if (total_len < sizeof(WALRecord) || total_len > 1024 * 1024) {
                // Sanity check: total_len must be at least WALRecord header size
                // and not absurdly large.
                return; // Protocol error — disconnect.
            }

            // Read exactly total_len binary bytes: WALRecord header + payload.
            std::vector<uint8_t> buf(total_len);
            if (!recv_all(fd_, buf.data(), total_len)) {
                return; // Disconnect.
            }

            // Extract WALRecord header.
            WALRecord hdr{};
            std::memcpy(&hdr, buf.data(), sizeof(WALRecord));

            const size_t payload_len = total_len - sizeof(WALRecord);
            const uint8_t* payload = buf.data() + sizeof(WALRecord);

            // Verify CRC32C of payload (Requirement 2.2).
            if (payload_len != hdr.payload_len) {
                // Length mismatch — protocol error.
                return;
            }

            const uint32_t computed_crc = crc32c(payload, payload_len);
            if (computed_crc != hdr.checksum) {
                // CRC32C mismatch — disconnect and log error (Requirement 2.3).
                return;
            }

            // Skip non-DELTA records (GAP, ROTATE, etc.) — only replay DELTA.
            if (hdr.record_type == WAL_RECORD_DELTA && payload_len >= sizeof(DeltaUpdate)) {
                // Decode DeltaUpdate + Levels from payload.
                // Payload format: DeltaUpdate struct followed by n_levels * Level.
                DeltaUpdate delta{};
                std::memcpy(&delta, payload, sizeof(DeltaUpdate));

                const size_t levels_bytes = delta.n_levels * sizeof(Level);
                if (sizeof(DeltaUpdate) + levels_bytes <= payload_len) {
                    const auto* levels = reinterpret_cast<const Level*>(
                        payload + sizeof(DeltaUpdate));

                    // Replay via Engine::apply_delta() (Requirement 2.1).
                    engine_.apply_delta(delta, levels);
                }
            }

            // Update confirmed position.
            confirmed_file_   = file_index;
            confirmed_offset_ = byte_offset + total_len;
            ++records_replayed_;

            // Send ACK (Requirement 2.4).
            send_ack();
            continue;
        }

        // Handle HEARTBEAT: respond with current ACK (Requirement 4.5).
        if (std::strncmp(line_buf, "HEARTBEAT", 9) == 0) {
            send_ack();
            continue;
        }

        // Handle ERR: log and disconnect (Requirement 6.3).
        if (std::strncmp(line_buf, "ERR ", 4) == 0) {
            // Error from primary — disconnect. The run_loop will handle reconnection.
            return;
        }

        // Unknown message — ignore.
    }
}

void ReplicationClient::send_ack() {
    // Format: ACK <file_index> <byte_offset>\n (Requirement 4.4).
    char ack[128];
    int len = std::snprintf(ack, sizeof(ack), "ACK %u %zu\n",
                            confirmed_file_, confirmed_offset_);
    send_all(fd_, ack, static_cast<size_t>(len));
}

void ReplicationClient::save_state() {
    if (config_.state_file.empty()) return;

    std::FILE* f = std::fopen(config_.state_file.c_str(), "w");
    if (!f) return;

    std::fprintf(f, "file_index=%u\nbyte_offset=%zu\n",
                 confirmed_file_, confirmed_offset_);
    std::fclose(f);
}

void ReplicationClient::load_state() {
    if (config_.state_file.empty()) return;

    std::FILE* f = std::fopen(config_.state_file.c_str(), "r");
    if (!f) {
        // No state file — start from beginning.
        confirmed_file_   = 0;
        confirmed_offset_ = 0;
        return;
    }

    uint32_t file_index = 0;
    size_t byte_offset = 0;
    char line[256];

    while (std::fgets(line, sizeof(line), f)) {
        if (std::sscanf(line, "file_index=%u", &file_index) == 1) {
            confirmed_file_ = file_index;
        } else if (std::sscanf(line, "byte_offset=%zu", &byte_offset) == 1) {
            confirmed_offset_ = byte_offset;
        }
    }

    std::fclose(f);
}

} // namespace ob
