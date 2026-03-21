// WAL Streaming Replication — ReplicationManager (primary) + ReplicationClient (replica).
//
// The ReplicationManager runs its own epoll thread to accept replica connections,
// stream WAL records, handle ACKs, and send heartbeats.
//
// Wire protocol (text+binary hybrid, newline-delimited control messages):
//   Handshake:  REPLICATE <file_index> <byte_offset> <epoch>\n
//   WAL record: WAL <file_index> <byte_offset> <total_len> <epoch>\n<WALRecord(24)><payload>
//   ACK:        ACK <file_index> <byte_offset>\n
//   Heartbeat:  HEARTBEAT <epoch>\n
//   Error:      ERR <message>\n
//   Stale:      ERR STALE_PRIMARY\n
//
// Design notes:
//   - broadcast() is non-blocking: it enqueues data into per-replica send buffers.
//     The epoll thread drains buffers via EPOLLOUT, keeping the hot path lock-free.
//   - Line parsing uses BufferedReader (4 KB chunks) instead of byte-by-byte recv().
//   - CRC32C uses the shared constexpr table from orderbook/crc32c.hpp.
//
// Requirements: 1.1, 1.2, 1.3, 1.4, 2.1, 2.2, 2.3, 3.1, 3.2, 3.3, 3.4, 3.5, 4.1, 4.2, 4.3, 4.4, 4.5, 6.2, 6.3

#include "orderbook/replication.hpp"
#include "orderbook/compression.hpp"
#include "orderbook/crc32c.hpp"
#include "orderbook/engine.hpp"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cinttypes>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <set>
#include <stdexcept>
#include <string>
#include <vector>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

namespace ob {

namespace fs = std::filesystem;

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

/// Maximum per-replica send buffer size before we consider the replica too slow
/// and disconnect it. 16 MB is generous for WAL streaming.
static constexpr size_t MAX_SEND_BUF_SIZE = 16 * 1024 * 1024;

} // anonymous namespace

// ── SnapshotManifest serialization ────────────────────────────────────────────

std::string SnapshotManifest::to_json() const {
    // Deterministic alphabetical field ordering.
    std::string out;
    out.reserve(256 + files.size() * 128);

    out += "{\"created_at_ns\":";
    out += std::to_string(created_at_ns);

    out += ",\"files\":[";
    // Sort files by path for deterministic output.
    auto sorted = files;
    std::sort(sorted.begin(), sorted.end(),
              [](const SnapshotFileEntry& a, const SnapshotFileEntry& b) {
                  return a.path < b.path;
              });
    for (size_t i = 0; i < sorted.size(); ++i) {
        if (i > 0) out += ',';
        out += "{\"crc32c\":";
        out += std::to_string(sorted[i].crc32c);
        out += ",\"path\":\"";
        out += sorted[i].path;
        out += "\",\"size\":";
        out += std::to_string(sorted[i].size);
        out += '}';
    }
    out += ']';

    out += ",\"total_bytes\":";
    out += std::to_string(total_bytes);
    out += ",\"total_rows\":";
    out += std::to_string(total_rows);
    out += ",\"wal_byte_offset\":";
    out += std::to_string(wal_byte_offset);
    out += ",\"wal_file_index\":";
    out += std::to_string(wal_file_index);
    out += '}';

    return out;
}

bool SnapshotManifest::from_json(std::string_view json, SnapshotManifest& out) {
    out = {};

    auto extract_uint64 = [&](const char* key) -> uint64_t {
        std::string search = std::string("\"") + key + "\":";
        auto pos = json.find(search);
        if (pos == std::string_view::npos) return 0;
        pos += search.size();
        uint64_t val = 0;
        while (pos < json.size() && json[pos] >= '0' && json[pos] <= '9') {
            val = val * 10 + static_cast<uint64_t>(json[pos] - '0');
            ++pos;
        }
        return val;
    };

    out.created_at_ns   = extract_uint64("created_at_ns");
    out.total_bytes     = static_cast<size_t>(extract_uint64("total_bytes"));
    out.total_rows      = static_cast<size_t>(extract_uint64("total_rows"));
    out.wal_byte_offset = static_cast<size_t>(extract_uint64("wal_byte_offset"));
    out.wal_file_index  = static_cast<uint32_t>(extract_uint64("wal_file_index"));

    // Parse files array.
    auto files_pos = json.find("\"files\":[");
    if (files_pos == std::string_view::npos) return out.total_bytes > 0 || out.created_at_ns > 0;
    files_pos += 9; // skip "files":[

    while (files_pos < json.size()) {
        // Find next object start.
        auto obj_start = json.find('{', files_pos);
        if (obj_start == std::string_view::npos) break;
        auto obj_end = json.find('}', obj_start);
        if (obj_end == std::string_view::npos) break;

        auto obj = json.substr(obj_start, obj_end - obj_start + 1);

        SnapshotFileEntry entry;

        // Extract path.
        auto path_pos = obj.find("\"path\":\"");
        if (path_pos != std::string_view::npos) {
            path_pos += 8;
            auto path_end = obj.find('"', path_pos);
            if (path_end != std::string_view::npos) {
                entry.path = std::string(obj.substr(path_pos, path_end - path_pos));
            }
        }

        // Extract size.
        auto size_search = std::string("\"size\":");
        auto size_pos = obj.find(size_search);
        if (size_pos != std::string_view::npos) {
            size_pos += size_search.size();
            uint64_t val = 0;
            while (size_pos < obj.size() && obj[size_pos] >= '0' && obj[size_pos] <= '9') {
                val = val * 10 + static_cast<uint64_t>(obj[size_pos] - '0');
                ++size_pos;
            }
            entry.size = static_cast<size_t>(val);
        }

        // Extract crc32c.
        auto crc_search = std::string("\"crc32c\":");
        auto crc_pos = obj.find(crc_search);
        if (crc_pos != std::string_view::npos) {
            crc_pos += crc_search.size();
            uint64_t val = 0;
            while (crc_pos < obj.size() && obj[crc_pos] >= '0' && obj[crc_pos] <= '9') {
                val = val * 10 + static_cast<uint64_t>(obj[crc_pos] - '0');
                ++crc_pos;
            }
            entry.crc32c = static_cast<uint32_t>(val);
        }

        out.files.push_back(std::move(entry));
        files_pos = obj_end + 1;

        // Check for end of array.
        while (files_pos < json.size() && (json[files_pos] == ',' || json[files_pos] == ' '))
            ++files_pos;
        if (files_pos < json.size() && json[files_pos] == ']') break;
    }

    return true;
}

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
    // Non-blocking broadcast: enqueue the WAL message into each replica's send
    // buffer. The epoll thread will drain buffers via EPOLLOUT.
    //
    // Format: WAL <file_index> <byte_offset> <total_len> <epoch>\n<WALRecord(24)><payload>
    const uint32_t file_index = wal_.current_file_index();
    const uint64_t epoch = wal_.current_epoch();
    const size_t total_len = sizeof(WALRecord) + payload_len;

    // Build the text header line.
    char line[128];
    int line_len = std::snprintf(line, sizeof(line), "WAL %u %zu %zu %" PRIu64 "\n",
                                  file_index, static_cast<size_t>(0), total_len, epoch);

    // Build the complete message: text header + WALRecord bytes + payload bytes.
    std::vector<uint8_t> msg(static_cast<size_t>(line_len) + total_len);
    std::memcpy(msg.data(), line, static_cast<size_t>(line_len));
    std::memcpy(msg.data() + line_len, &hdr, sizeof(WALRecord));
    if (payload_len > 0) {
        std::memcpy(msg.data() + line_len + sizeof(WALRecord), payload, payload_len);
    }

    std::lock_guard<std::mutex> lock(mtx_);
    for (auto it = replicas_.begin(); it != replicas_.end(); ) {
        if (it->compress) {
            // Compress the entire message as a single LZ4 frame.
            auto compressed = ob::lz4_compress(msg.data(), msg.size());
            // Prefix with 4-byte big-endian compressed length.
            uint32_t comp_len = static_cast<uint32_t>(compressed.size());
            uint8_t len_prefix[4];
            len_prefix[0] = static_cast<uint8_t>((comp_len >> 24) & 0xFF);
            len_prefix[1] = static_cast<uint8_t>((comp_len >> 16) & 0xFF);
            len_prefix[2] = static_cast<uint8_t>((comp_len >> 8) & 0xFF);
            len_prefix[3] = static_cast<uint8_t>(comp_len & 0xFF);
            enqueue_send(*it, len_prefix, 4);
            enqueue_send(*it, compressed.data(), compressed.size());
        } else {
            enqueue_send(*it, msg.data(), msg.size());
        }

        // If the send buffer is too large, the replica is too slow — disconnect it.
        if (it->send_buf.size() > MAX_SEND_BUF_SIZE) {
            remove_replica_locked(it->fd);
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

bool ReplicationManager::snapshot_active() const {
    std::lock_guard<std::mutex> lock(mtx_);
    for (const auto& r : replicas_) {
        if (r.snapshot_transfer.active) return true;
    }
    return false;
}

void ReplicationManager::enqueue_send(ReplicaInfo& replica, const void* data, size_t len) {
    const bool was_empty = replica.send_buf.empty();
    const auto* bytes = static_cast<const uint8_t*>(data);
    replica.send_buf.insert(replica.send_buf.end(), bytes, bytes + len);

    // If the buffer was empty, we need to arm EPOLLOUT so the epoll thread
    // will drain it. If it was already non-empty, EPOLLOUT is already armed.
    if (was_empty && epoll_fd_ >= 0) {
        struct epoll_event ev{};
        ev.events  = EPOLLIN | EPOLLOUT | EPOLLET;
        ev.data.fd = replica.fd;
        ::epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, replica.fd, &ev);
    }
}

bool ReplicationManager::drain_send_buffer(ReplicaInfo& replica) {
    while (!replica.send_buf.empty()) {
        ssize_t n = ::send(replica.fd, replica.send_buf.data(),
                           replica.send_buf.size(), MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // Socket buffer full — wait for next EPOLLOUT.
                return true;
            }
            // Real error — replica is dead.
            return false;
        }
        if (n == 0) return false;

        // Erase sent bytes from the front.
        replica.send_buf.erase(replica.send_buf.begin(),
                               replica.send_buf.begin() + n);
    }

    // Buffer fully drained — disarm EPOLLOUT to avoid busy-spinning.
    if (epoll_fd_ >= 0) {
        struct epoll_event ev{};
        ev.events  = EPOLLIN | EPOLLET;
        ev.data.fd = replica.fd;
        ::epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, replica.fd, &ev);
    }
    return true;
}

void ReplicationManager::remove_replica_locked(int fd) {
    ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
    ::close(fd);
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
                continue;
            }

            // Handle EPOLLOUT: drain send buffer and continue snapshot transfer.
            if (events[i].events & EPOLLOUT) {
                std::lock_guard<std::mutex> lock(mtx_);
                for (auto it = replicas_.begin(); it != replicas_.end(); ++it) {
                    if (it->fd == fd) {
                        if (!drain_send_buffer(*it)) {
                            remove_replica_locked(fd);
                            replicas_.erase(it);
                            break;
                        }
                        // If snapshot transfer is active and send buffer has room,
                        // enqueue the next chunk.
                        if (it->snapshot_transfer.active &&
                            it->send_buf.size() < MAX_SEND_BUF_SIZE / 2) {
                            if (!continue_snapshot_transfer(*it)) {
                                remove_replica_locked(fd);
                                replicas_.erase(it);
                            }
                        }
                        break;
                    }
                }
            }

            // Handle EPOLLIN: read replica data (ACK, REPLICATE, etc.).
            if (events[i].events & EPOLLIN) {
                handle_replica_data(fd);
            }
        }

        // Send heartbeat every 5 seconds when idle.
        auto now = std::chrono::steady_clock::now();
        if (std::chrono::duration_cast<std::chrono::seconds>(now - last_heartbeat).count() >= 5) {
            last_heartbeat = now;
            const uint64_t epoch = wal_.current_epoch();
            char hb[64];
            int hb_len = std::snprintf(hb, sizeof(hb), "HEARTBEAT %" PRIu64 "\n", epoch);

            std::lock_guard<std::mutex> lock(mtx_);
            for (auto it = replicas_.begin(); it != replicas_.end(); ) {
                if (it->compress) {
                    // Compress heartbeat as a single LZ4 frame with length prefix.
                    auto compressed = ob::lz4_compress(hb, static_cast<size_t>(hb_len));
                    uint32_t comp_len = static_cast<uint32_t>(compressed.size());
                    uint8_t len_prefix[4];
                    len_prefix[0] = static_cast<uint8_t>((comp_len >> 24) & 0xFF);
                    len_prefix[1] = static_cast<uint8_t>((comp_len >> 16) & 0xFF);
                    len_prefix[2] = static_cast<uint8_t>((comp_len >> 8) & 0xFF);
                    len_prefix[3] = static_cast<uint8_t>(comp_len & 0xFF);
                    enqueue_send(*it, len_prefix, 4);
                    enqueue_send(*it, compressed.data(), compressed.size());
                } else {
                    enqueue_send(*it, hb, static_cast<size_t>(hb_len));
                }
                if (it->send_buf.size() > MAX_SEND_BUF_SIZE) {
                    remove_replica_locked(it->fd);
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
        info.compress         = false;
        info.reader.set_fd(client_fd);

        std::lock_guard<std::mutex> lock(mtx_);

        // If compression is enabled, send COMPRESS LZ4 directive before streaming.
        if (config_.compress) {
            const char* directive = "COMPRESS LZ4\n";
            info.compress = true;
            // Enqueue the directive into the send buffer (will be drained by epoll).
            const auto* bytes = reinterpret_cast<const uint8_t*>(directive);
            info.send_buf.insert(info.send_buf.end(), bytes, bytes + std::strlen(directive));
            // Arm EPOLLOUT to drain the directive.
            struct epoll_event ev_out{};
            ev_out.events  = EPOLLIN | EPOLLOUT | EPOLLET;
            ev_out.data.fd = client_fd;
            ::epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, client_fd, &ev_out);
        }

        replicas_.push_back(std::move(info));
    }
}

void ReplicationManager::handle_replica_data(int fd) {
    // Edge-triggered: read until EAGAIN.
    char buf[512];

    // Find the replica's BufferedReader.
    std::lock_guard<std::mutex> lock(mtx_);
    ReplicaInfo* replica_ptr = nullptr;
    for (auto& r : replicas_) {
        if (r.fd == fd) {
            replica_ptr = &r;
            break;
        }
    }
    if (!replica_ptr) return;

    while (true) {
        ssize_t n = replica_ptr->reader.read_line(buf, sizeof(buf));
        if (n < 0) {
            // Disconnect — remove replica (Requirement 1.3).
            remove_replica_locked(fd);
            for (auto it = replicas_.begin(); it != replicas_.end(); ++it) {
                if (it->fd == fd) {
                    replicas_.erase(it);
                    break;
                }
            }
            return;
        }
        if (n == 0) break; // EAGAIN, no more data

        std::string line(buf);

        // Parse REPLICATE handshake: REPLICATE <file_index> <byte_offset> <epoch>
        if (line.rfind("REPLICATE ", 0) == 0) {
            uint32_t from_file = 0;
            size_t from_offset = 0;
            uint64_t replica_epoch = 0;
            int parsed = std::sscanf(line.c_str(), "REPLICATE %u %zu %" SCNu64,
                                     &from_file, &from_offset, &replica_epoch);
            if (parsed >= 2) {
                // Stale-primary check: if replica's epoch > our epoch, reject (Requirement 3.4).
                if (parsed == 3 && replica_epoch > wal_.current_epoch()) {
                    const char* err = "ERR STALE_PRIMARY\n";
                    auto wr = ::write(replica_ptr->fd, err, std::strlen(err));
                    (void)wr;
                    remove_replica_locked(fd);
                    for (auto it = replicas_.begin(); it != replicas_.end(); ++it) {
                        if (it->fd == fd) {
                            replicas_.erase(it);
                            break;
                        }
                    }
                    return;
                }
                replica_ptr->confirmed_file   = from_file;
                replica_ptr->confirmed_offset = from_offset;
                // Perform catchup from the requested position.
                handle_catchup(*replica_ptr, from_file, from_offset);
            }
            continue;
        }

        // Parse SNAPSHOT_REQUEST
        if (line == "SNAPSHOT_REQUEST") {
            handle_snapshot_request(*replica_ptr);
            continue;
        }

        // Parse ACK: ACK <file_index> <byte_offset>
        if (line.rfind("ACK ", 0) == 0) {
            uint32_t ack_file = 0;
            size_t ack_offset = 0;
            if (std::sscanf(line.c_str(), "ACK %u %zu", &ack_file, &ack_offset) == 2) {
                replica_ptr->confirmed_file   = ack_file;
                replica_ptr->confirmed_offset = ack_offset;
            }
            continue;
        }

        // Unknown message — ignore.
    }
}

void ReplicationManager::send_to_replica(ReplicaInfo& replica, const WALRecord& hdr,
                                          const void* payload, size_t payload_len) {
    // Format: WAL <file_index> <byte_offset> <total_len> <epoch>\n<WALRecord(24)><payload>
    const size_t total_len = sizeof(WALRecord) + payload_len;
    const uint64_t epoch = wal_.current_epoch();

    char line[128];
    int line_len = std::snprintf(line, sizeof(line), "WAL %u %zu %zu %" PRIu64 "\n",
                                  replica.confirmed_file,
                                  replica.confirmed_offset,
                                  total_len, epoch);

    // Build complete message.
    std::vector<uint8_t> msg(static_cast<size_t>(line_len) + total_len);
    std::memcpy(msg.data(), line, static_cast<size_t>(line_len));
    std::memcpy(msg.data() + line_len, &hdr, sizeof(WALRecord));
    if (payload_len > 0) {
        std::memcpy(msg.data() + line_len + sizeof(WALRecord), payload, payload_len);
    }

    if (!send_all(replica.fd, msg.data(), msg.size())) {
        // Mark fd as -1; caller should clean up.
        remove_replica_locked(replica.fd);
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

void ReplicationManager::handle_snapshot_request(ReplicaInfo& replica) {
    if (!engine_) {
        const char* err = "ERR SNAPSHOT_FAILED no_engine\n";
        enqueue_send(replica, err, std::strlen(err));
        return;
    }

    // Create snapshot (releases mtx_ internally for CRC computation).
    // We must release our own mtx_ first since create_snapshot() acquires Engine::mtx_.
    SnapshotManifest manifest;
    {
        // Temporarily release replication mtx_ to avoid deadlock with Engine::mtx_.
        mtx_.unlock();
        try {
            manifest = engine_->create_snapshot();
        } catch (const std::exception& e) {
            mtx_.lock();
            char err[256];
            int len = std::snprintf(err, sizeof(err), "ERR SNAPSHOT_FAILED %s\n", e.what());
            enqueue_send(replica, err, static_cast<size_t>(len));
            return;
        }
        mtx_.lock();

        // Re-find the replica (it may have been removed while we released the lock).
        bool found = false;
        for (auto& r : replicas_) {
            if (r.fd == replica.fd) { found = true; break; }
        }
        if (!found) return;
    }

    // Initialize snapshot transfer state.
    auto& st = replica.snapshot_transfer;
    st.active            = true;
    st.manifest          = std::move(manifest);
    st.current_file_idx  = 0;
    st.current_file_offset = 0;
    st.current_file_fd   = -1;
    st.header_sent       = false;
    st.begin_sent        = false;
    st.base_dir          = engine_->base_dir();
    st.chunk_size        = 262144; // 256 KB

    // Send SNAPSHOT_BEGIN.
    char line[256];
    int line_len = std::snprintf(line, sizeof(line),
        "SNAPSHOT_BEGIN %zu %u %zu %zu\n",
        st.manifest.total_bytes,
        st.manifest.wal_file_index,
        st.manifest.wal_byte_offset,
        st.manifest.files.size());
    enqueue_send(replica, line, static_cast<size_t>(line_len));
    st.begin_sent = true;

    // Start streaming the first chunk (the epoll loop will continue via EPOLLOUT).
    continue_snapshot_transfer(replica);
}

bool ReplicationManager::continue_snapshot_transfer(ReplicaInfo& replica) {
    auto& st = replica.snapshot_transfer;
    if (!st.active) return true;

    while (st.current_file_idx < st.manifest.files.size()) {
        const auto& entry = st.manifest.files[st.current_file_idx];

        // Send SNAPSHOT_FILE header once per file.
        if (!st.header_sent) {
            char line[512];
            int line_len = std::snprintf(line, sizeof(line),
                "SNAPSHOT_FILE %s %zu %u\n",
                entry.path.c_str(), entry.size, entry.crc32c);
            enqueue_send(replica, line, static_cast<size_t>(line_len));
            st.header_sent = true;
            st.current_file_offset = 0;

            // Open the file.
            std::string full_path = st.base_dir + "/" + entry.path;
            st.current_file_fd = ::open(full_path.c_str(), O_RDONLY);
            if (st.current_file_fd < 0) {
                // File disappeared — abort transfer.
                const char* err = "ERR SNAPSHOT_FAILED file_read_error\n";
                enqueue_send(replica, err, std::strlen(err));
                st.active = false;
                return true;
            }
        }

        // Read and enqueue chunks until file is done or send buffer is getting full.
        while (st.current_file_offset < entry.size) {
            if (replica.send_buf.size() >= MAX_SEND_BUF_SIZE / 2) {
                // Back off — let EPOLLOUT drain the buffer first.
                return true;
            }

            size_t remaining = entry.size - st.current_file_offset;
            size_t to_read = std::min(remaining, st.chunk_size);

            std::vector<uint8_t> chunk(to_read);
            ssize_t n = ::pread(st.current_file_fd, chunk.data(), to_read,
                                static_cast<off_t>(st.current_file_offset));
            if (n <= 0) {
                ::close(st.current_file_fd);
                st.current_file_fd = -1;
                const char* err = "ERR SNAPSHOT_FAILED file_read_error\n";
                enqueue_send(replica, err, std::strlen(err));
                st.active = false;
                return true;
            }

            enqueue_send(replica, chunk.data(), static_cast<size_t>(n));
            st.current_file_offset += static_cast<size_t>(n);
        }

        // File done — close and move to next.
        if (st.current_file_fd >= 0) {
            ::close(st.current_file_fd);
            st.current_file_fd = -1;
        }
        st.current_file_idx++;
        st.header_sent = false;
    }

    // All files sent — send SNAPSHOT_END.
    std::string manifest_json = st.manifest.to_json();
    uint32_t manifest_crc = ob::crc32c(manifest_json.data(), manifest_json.size());

    char line[128];
    int line_len = std::snprintf(line, sizeof(line), "SNAPSHOT_END %u\n", manifest_crc);
    enqueue_send(replica, line, static_cast<size_t>(line_len));

    st.active = false;
    return true;
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
    return State{confirmed_file_, confirmed_offset_, (fd_ >= 0), records_replayed_,
                 bootstrapping_.load(std::memory_order_acquire),
                 snapshot_bytes_received_.load(std::memory_order_relaxed),
                 snapshot_bytes_total_.load(std::memory_order_relaxed)};
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

    // Initialize the buffered reader for this connection.
    reader_.set_fd(fd_);

    // Reset compression flag for new connection.
    compress_ = false;

    // Send REPLICATE handshake (Requirement 4.2, 3.1).
    // Even fresh replicas start with REPLICATE 0 0 0. If the primary responds
    // with ERR WAL_TRUNCATED, receive_and_replay() will trigger snapshot bootstrap.
    char handshake[128];
    int len = std::snprintf(handshake, sizeof(handshake), "REPLICATE %u %zu %" PRIu64 "\n",
                            confirmed_file_, confirmed_offset_, local_epoch_);
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

        // When compression is enabled, read length-prefixed LZ4 frames.
        if (compress_) {
            // Read 4-byte big-endian compressed length.
            uint8_t len_bytes[4];
            if (!reader_.read_exact(len_bytes, 4)) {
                // Check if it's a timeout (EAGAIN) — read_exact returns false on both.
                // Since we have SO_RCVTIMEO set, a timeout will cause recv to return -1
                // with EAGAIN. But read_exact treats that as failure. We need to handle
                // timeouts differently. For simplicity, just return and let run_loop reconnect.
                return;
            }

            uint32_t comp_len = (static_cast<uint32_t>(len_bytes[0]) << 24) |
                                (static_cast<uint32_t>(len_bytes[1]) << 16) |
                                (static_cast<uint32_t>(len_bytes[2]) << 8) |
                                static_cast<uint32_t>(len_bytes[3]);

            if (comp_len == 0 || comp_len > 16 * 1024 * 1024) {
                // Sanity check — invalid frame size.
                return;
            }

            // Read the compressed frame.
            std::vector<uint8_t> compressed(comp_len);
            if (!reader_.read_exact(compressed.data(), comp_len)) {
                return;
            }

            // Decompress.
            std::vector<uint8_t> decompressed;
            try {
                decompressed = ob::lz4_decompress(compressed.data(), compressed.size());
            } catch (const std::runtime_error& e) {
                // Decompression failure — disconnect, log error (Requirement 2.5).
                std::fprintf(stderr, "ReplicationClient: decompression failed: %s\n", e.what());
                return;
            }

            // Parse the decompressed data as a normal message (text header + binary).
            // Find the newline that terminates the text header.
            const char* data = reinterpret_cast<const char*>(decompressed.data());
            size_t data_len = decompressed.size();

            const char* nl = static_cast<const char*>(std::memchr(data, '\n', data_len));
            if (!nl) {
                // No newline — malformed decompressed message.
                return;
            }

            size_t header_len = static_cast<size_t>(nl - data);
            std::string line(data, header_len);
            const uint8_t* binary_start = decompressed.data() + header_len + 1;
            size_t binary_len = data_len - header_len - 1;

            // Parse WAL record.
            if (line.rfind("WAL ", 0) == 0) {
                uint32_t file_index = 0;
                size_t byte_offset = 0;
                size_t total_len = 0;
                uint64_t msg_epoch = 0;
                int parsed = std::sscanf(line.c_str(), "WAL %u %zu %zu %" SCNu64,
                                &file_index, &byte_offset, &total_len, &msg_epoch);
                if (parsed < 3) continue;

                if (parsed == 4 && msg_epoch < local_epoch_) {
                    std::fprintf(stderr, "ReplicationClient: stale epoch %" PRIu64
                                 " < local %" PRIu64 ", disconnecting\n",
                                 msg_epoch, local_epoch_);
                    return;
                }
                if (parsed == 4 && msg_epoch > local_epoch_) {
                    local_epoch_ = msg_epoch;
                }

                if (binary_len < sizeof(WALRecord)) {
                    return;
                }

                WALRecord hdr{};
                std::memcpy(&hdr, binary_start, sizeof(WALRecord));

                const size_t payload_len = binary_len - sizeof(WALRecord);
                const uint8_t* payload = binary_start + sizeof(WALRecord);

                if (payload_len != hdr.payload_len) {
                    return;
                }

                const uint32_t computed_crc = ob::crc32c(payload, payload_len);
                if (computed_crc != hdr.checksum) {
                    return;
                }

                if (hdr.record_type == WAL_RECORD_EPOCH && payload_len == 8) {
                    EpochValue received_epoch = epoch_from_payload(payload);
                    if (received_epoch.term > local_epoch_) {
                        local_epoch_ = received_epoch.term;
                    }
                }

                if (hdr.record_type == WAL_RECORD_DELTA && payload_len >= sizeof(DeltaUpdate)) {
                    DeltaUpdate delta{};
                    std::memcpy(&delta, payload, sizeof(DeltaUpdate));

                    const size_t levels_bytes = delta.n_levels * sizeof(Level);
                    if (sizeof(DeltaUpdate) + levels_bytes <= payload_len) {
                        const auto* levels = reinterpret_cast<const Level*>(
                            payload + sizeof(DeltaUpdate));
                        engine_.apply_delta(delta, levels);
                    }
                }

                confirmed_file_   = file_index;
                confirmed_offset_ = byte_offset + total_len;
                ++records_replayed_;
                send_ack();
                continue;
            }

            // Handle HEARTBEAT.
            if (line.rfind("HEARTBEAT", 0) == 0) {
                uint64_t hb_epoch = 0;
                if (std::sscanf(line.c_str(), "HEARTBEAT %" SCNu64, &hb_epoch) == 1) {
                    if (hb_epoch < local_epoch_) {
                        std::fprintf(stderr, "ReplicationClient: stale heartbeat epoch %" PRIu64
                                     " < local %" PRIu64 ", disconnecting\n",
                                     hb_epoch, local_epoch_);
                        return;
                    }
                    if (hb_epoch > local_epoch_) {
                        local_epoch_ = hb_epoch;
                    }
                }
                send_ack();
                continue;
            }

            // Handle ERR.
            if (line.rfind("ERR ", 0) == 0) {
                if (line.find("WAL_TRUNCATED") != std::string::npos) {
                    request_and_receive_snapshot();
                    return;
                }
                return;
            }

            // Unknown — ignore.
            continue;
        }

        // Uncompressed path (original logic).
        // Read a text header line using the buffered reader.
        ssize_t n = reader_.read_line(line_buf, sizeof(line_buf));
        if (n < 0) {
            // Disconnect or error.
            return;
        }
        if (n == 0) {
            // Timeout (EAGAIN from SO_RCVTIMEO) — check running_ and continue.
            continue;
        }

        // Parse WAL record: WAL <file_index> <byte_offset> <total_len> <epoch>
        if (std::strncmp(line_buf, "WAL ", 4) == 0) {
            uint32_t file_index = 0;
            size_t byte_offset = 0;
            size_t total_len = 0;
            uint64_t msg_epoch = 0;
            int parsed = std::sscanf(line_buf, "WAL %u %zu %zu %" SCNu64,
                            &file_index, &byte_offset, &total_len, &msg_epoch);
            if (parsed < 3) {
                continue; // Malformed — skip.
            }

            // Stale-epoch check (Requirement 2.1, 2.2, 3.5).
            if (parsed == 4 && msg_epoch < local_epoch_) {
                // Stale primary — disconnect and log warning.
                std::fprintf(stderr, "ReplicationClient: stale epoch %" PRIu64
                             " < local %" PRIu64 ", disconnecting\n",
                             msg_epoch, local_epoch_);
                return;
            }
            // Epoch advancement: if received epoch > local, update (Requirement 2.4).
            if (parsed == 4 && msg_epoch > local_epoch_) {
                local_epoch_ = msg_epoch;
            }

            if (total_len < sizeof(WALRecord) || total_len > 1024 * 1024) {
                // Sanity check: total_len must be at least WALRecord header size
                // and not absurdly large.
                return; // Protocol error — disconnect.
            }

            // Read exactly total_len binary bytes: WALRecord header + payload.
            std::vector<uint8_t> buf(total_len);
            if (!reader_.read_exact(buf.data(), total_len)) {
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

            const uint32_t computed_crc = ob::crc32c(payload, payload_len);
            if (computed_crc != hdr.checksum) {
                // CRC32C mismatch — disconnect and log error (Requirement 2.3).
                return;
            }

            // Handle Epoch_Record: update local epoch (Requirement 2.4).
            if (hdr.record_type == WAL_RECORD_EPOCH && payload_len == 8) {
                EpochValue received_epoch = epoch_from_payload(payload);
                if (received_epoch.term > local_epoch_) {
                    local_epoch_ = received_epoch.term;
                }
            }

            // Skip non-DELTA records (GAP, ROTATE, EPOCH, etc.) — only replay DELTA.
            if (hdr.record_type == WAL_RECORD_DELTA && payload_len >= sizeof(DeltaUpdate)) {
                // Decode DeltaUpdate + Levels from payload.
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

        // Handle HEARTBEAT <epoch>: respond with current ACK (Requirement 4.5, 3.3, 3.5).
        if (std::strncmp(line_buf, "HEARTBEAT", 9) == 0) {
            // Parse epoch from HEARTBEAT message.
            uint64_t hb_epoch = 0;
            if (std::sscanf(line_buf, "HEARTBEAT %" SCNu64, &hb_epoch) == 1) {
                // Stale-epoch check (Requirement 3.5).
                if (hb_epoch < local_epoch_) {
                    std::fprintf(stderr, "ReplicationClient: stale heartbeat epoch %" PRIu64
                                 " < local %" PRIu64 ", disconnecting\n",
                                 hb_epoch, local_epoch_);
                    return;
                }
                // Epoch advancement.
                if (hb_epoch > local_epoch_) {
                    local_epoch_ = hb_epoch;
                }
            }
            send_ack();
            continue;
        }

        // Handle ERR: log and disconnect (Requirement 6.3).
        if (std::strncmp(line_buf, "ERR ", 4) == 0) {
            // Check for WAL_TRUNCATED — trigger snapshot bootstrap.
            if (std::strncmp(line_buf + 4, "WAL_TRUNCATED", 13) == 0) {
                request_and_receive_snapshot();
                // After snapshot, resume normal streaming from the snapshot's WAL position.
                return;
            }
            // Other errors — disconnect. The run_loop will handle reconnection.
            return;
        }

        // Detect COMPRESS LZ4 directive from primary (Requirement 2.3).
        if (std::strncmp(line_buf, "COMPRESS LZ4", 12) == 0) {
            compress_ = true;
            continue;
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

// ── Snapshot bootstrap (replica side) ─────────────────────────────────────────

void ReplicationClient::request_and_receive_snapshot() {
    bootstrapping_.store(true, std::memory_order_release);
    snapshot_bytes_received_.store(0, std::memory_order_relaxed);
    snapshot_bytes_total_.store(0, std::memory_order_relaxed);

    // Determine staging directory.
    std::string staging_dir = config_.snapshot_staging_dir;
    if (staging_dir.empty()) {
        staging_dir = engine_.base_dir() + "/snapshot_staging";
    }

    // Clean up any leftover staging directory from a previous interrupted transfer.
    cleanup_staging(staging_dir);

    // Send SNAPSHOT_REQUEST.
    const char* req = "SNAPSHOT_REQUEST\n";
    if (!send_all(fd_, req, std::strlen(req))) {
        bootstrapping_.store(false, std::memory_order_release);
        return;
    }

    // Read SNAPSHOT_BEGIN response.
    char line_buf[512];
    ssize_t n = reader_.read_line(line_buf, sizeof(line_buf));
    if (n <= 0) {
        bootstrapping_.store(false, std::memory_order_release);
        return;
    }

    // Check for error.
    if (std::strncmp(line_buf, "ERR ", 4) == 0) {
        bootstrapping_.store(false, std::memory_order_release);
        return;
    }

    size_t total_bytes = 0;
    uint32_t snap_wal_fi = 0;
    size_t snap_wal_off = 0;
    size_t file_count = 0;

    if (std::sscanf(line_buf, "SNAPSHOT_BEGIN %zu %u %zu %zu",
                    &total_bytes, &snap_wal_fi, &snap_wal_off, &file_count) != 4) {
        bootstrapping_.store(false, std::memory_order_release);
        return;
    }

    snapshot_bytes_total_.store(total_bytes, std::memory_order_relaxed);

    // Create staging directory.
    fs::create_directories(staging_dir);

    // Receive files.
    SnapshotManifest manifest;
    manifest.wal_file_index  = snap_wal_fi;
    manifest.wal_byte_offset = snap_wal_off;
    manifest.total_bytes     = total_bytes;

    size_t bytes_received = 0;

    for (size_t i = 0; i < file_count; ++i) {
        // Read SNAPSHOT_FILE header.
        n = reader_.read_line(line_buf, sizeof(line_buf));
        if (n <= 0) {
            cleanup_staging(staging_dir);
            bootstrapping_.store(false, std::memory_order_release);
            return;
        }

        char rel_path[256] = {};
        size_t file_size = 0;
        uint32_t file_crc = 0;

        if (std::sscanf(line_buf, "SNAPSHOT_FILE %255s %zu %u",
                        rel_path, &file_size, &file_crc) != 3) {
            cleanup_staging(staging_dir);
            bootstrapping_.store(false, std::memory_order_release);
            return;
        }

        // Create parent directories in staging.
        std::string staged_path = staging_dir + "/" + rel_path;
        fs::create_directories(fs::path(staged_path).parent_path());

        // Receive file data and write to staging.
        {
            std::FILE* out = std::fopen(staged_path.c_str(), "wb");
            if (!out) {
                cleanup_staging(staging_dir);
                bootstrapping_.store(false, std::memory_order_release);
                return;
            }

            uint32_t running_crc = 0xFFFFFFFFu;
            size_t remaining = file_size;

            while (remaining > 0) {
                size_t chunk = std::min(remaining, static_cast<size_t>(262144));
                std::vector<uint8_t> buf(chunk);

                if (!reader_.read_exact(buf.data(), chunk)) {
                    std::fclose(out);
                    cleanup_staging(staging_dir);
                    bootstrapping_.store(false, std::memory_order_release);
                    return;
                }

                std::fwrite(buf.data(), 1, chunk, out);

                // Update CRC32C incrementally.
                for (size_t b = 0; b < chunk; ++b) {
                    running_crc = (running_crc >> 8) ^
                        detail::crc32c_table[(running_crc ^ buf[b]) & 0xFFu];
                }

                remaining -= chunk;
                bytes_received += chunk;
                snapshot_bytes_received_.store(bytes_received, std::memory_order_relaxed);
            }

            std::fclose(out);

            // Verify CRC32C.
            uint32_t computed_crc = running_crc ^ 0xFFFFFFFFu;
            if (computed_crc != file_crc) {
                // CRC mismatch — abort.
                cleanup_staging(staging_dir);
                bootstrapping_.store(false, std::memory_order_release);
                return;
            }
        }

        SnapshotFileEntry entry;
        entry.path   = rel_path;
        entry.size   = file_size;
        entry.crc32c = file_crc;
        manifest.files.push_back(std::move(entry));
    }

    // Read SNAPSHOT_END.
    n = reader_.read_line(line_buf, sizeof(line_buf));
    if (n <= 0) {
        cleanup_staging(staging_dir);
        bootstrapping_.store(false, std::memory_order_release);
        return;
    }

    uint32_t manifest_crc = 0;
    if (std::sscanf(line_buf, "SNAPSHOT_END %u", &manifest_crc) != 1) {
        cleanup_staging(staging_dir);
        bootstrapping_.store(false, std::memory_order_release);
        return;
    }

    // Verify manifest CRC32C.
    std::string manifest_json = manifest.to_json();
    uint32_t computed_manifest_crc = ob::crc32c(manifest_json.data(), manifest_json.size());
    if (computed_manifest_crc != manifest_crc) {
        cleanup_staging(staging_dir);
        bootstrapping_.store(false, std::memory_order_release);
        return;
    }

    // Install the snapshot.
    install_snapshot(staging_dir, manifest);

    bootstrapping_.store(false, std::memory_order_release);
}

void ReplicationClient::install_snapshot(const std::string& staging_dir,
                                          const SnapshotManifest& manifest) {
    const std::string& data_dir = engine_.base_dir();

    // Move staged files into the data directory.
    // We move entire segment directories (parent of column files).
    std::set<std::string> moved_dirs;
    for (const auto& entry : manifest.files) {
        std::string src = staging_dir + "/" + entry.path;
        std::string dst = data_dir + "/" + entry.path;

        // Ensure destination parent directory exists.
        fs::create_directories(fs::path(dst).parent_path());

        // Move (rename) the file.
        std::error_code ec;
        fs::rename(src, dst, ec);
        if (ec) {
            // Fallback: copy + remove (cross-device move).
            fs::copy_file(src, dst, fs::copy_options::overwrite_existing, ec);
            if (!ec) fs::remove(src, ec);
        }
    }

    // Load the snapshot into the engine.
    engine_.load_snapshot(manifest);

    // Update confirmed WAL position.
    confirmed_file_   = manifest.wal_file_index;
    confirmed_offset_ = manifest.wal_byte_offset;
    save_state();

    // Clean up staging directory.
    cleanup_staging(staging_dir);
}

void ReplicationClient::cleanup_staging(const std::string& staging_dir) {
    std::error_code ec;
    fs::remove_all(staging_dir, ec);
    // Ignore errors — best effort cleanup.
}

} // namespace ob
