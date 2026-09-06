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
#include "orderbook/logger.hpp"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cinttypes>
#include <cstdio>
#include <cstdlib>
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

// ── Snapshot path validation ──────────────────────────────────────────────────

bool is_safe_snapshot_path(std::string_view rel) {
    if (rel.empty() || rel.size() > kMaxSnapshotPathLen) {
        return false;
    }

    // Absolute paths would ignore the base directory entirely.
    if (rel.front() == '/') {
        return false;
    }

    // Walk components manually rather than through fs::path, so the rules are
    // explicit and do not depend on platform path semantics.
    size_t start = 0;
    while (start <= rel.size()) {
        size_t slash = rel.find('/', start);
        std::string_view comp = (slash == std::string_view::npos)
                                    ? rel.substr(start)
                                    : rel.substr(start, slash - start);

        // Rejects a trailing slash, a leading slash and any `a//b`.
        if (comp.empty()) {
            return false;
        }
        // `..` escapes the base directory; `.` is pointless and worth rejecting
        // so that only one spelling of a path is ever accepted.
        if (comp == ".." || comp == ".") {
            return false;
        }
        for (char c : comp) {
            const bool allowed = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
                                 (c >= '0' && c <= '9') ||
                                 c == '.' || c == '_' || c == '-';
            if (!allowed) {
                return false;
            }
        }

        if (slash == std::string_view::npos) {
            break;
        }
        start = slash + 1;
    }

    return true;
}

bool path_stays_within(const std::string& base, std::string_view rel) {
    std::error_code ec;

    // weakly_canonical resolves symlinks and `..` for the parts that exist,
    // and lexically normalises the rest.
    const fs::path canonical_base = fs::weakly_canonical(fs::path(base), ec);
    if (ec) {
        return false;
    }
    const fs::path canonical_target =
        fs::weakly_canonical(fs::path(base) / fs::path(std::string(rel)), ec);
    if (ec) {
        return false;
    }

    // Compare component-wise so that /data/foo is not treated as being inside
    // /data/foobar.
    auto b = canonical_base.begin();
    auto t = canonical_target.begin();
    for (; b != canonical_base.end(); ++b, ++t) {
        if (t == canonical_target.end() || *t != *b) {
            return false;
        }
    }
    return true;
}

// ── Helpers ───────────────────────────────────────────────────────────────────

namespace {

/// Opens a file for writing with mode 0640, bypassing the process umask.
///
/// std::fopen creates files with 0666 & ~umask, so the resulting permissions
/// depend on how the server was started. Under a systemd unit without an
/// explicit UMask= that yields world-writable database files. Returns nullptr
/// on failure, with errno set by open().
static std::FILE* open_file_private(const std::string& path, const char* mode) {
    int flags = O_WRONLY | O_CREAT;
    flags |= (std::strchr(mode, 'a') != nullptr) ? O_APPEND : O_TRUNC;

    const int fd = ::open(path.c_str(), flags, S_IRUSR | S_IWUSR | S_IRGRP);
    if (fd < 0) {
        return nullptr;
    }
    std::FILE* f = ::fdopen(fd, mode);
    if (!f) {
        const int saved = errno;
        ::close(fd);
        errno = saved;
    }
    return f;
}

/// Creates a uniquely named temporary file inside `dir` and returns it opened
/// for writing, with the final path in `out_path`.
///
/// The name is generated locally by mkstemp(), so nothing derived from the
/// network is used to open a file: a peer-supplied name only ever reaches
/// rename(), after validation. mkstemp creates with 0600; we relax to 0640 to
/// match the rest of the data directory, and never to anything group-writable.
/// Returns nullptr on failure.
static std::FILE* open_temp_file_private(const std::string& dir,
                                         std::string& out_path) {
    std::string tmpl = dir + "/.incoming_XXXXXX";
    std::vector<char> buf(tmpl.begin(), tmpl.end());
    buf.push_back('\0');

    const int fd = ::mkstemp(buf.data());
    if (fd < 0) {
        OB_LOG_ERROR("repl_client", "mkstemp failed in %s: %s",
                     dir.c_str(), std::strerror(errno));
        return nullptr;
    }
    if (::fchmod(fd, S_IRUSR | S_IWUSR | S_IRGRP) != 0) {
        OB_LOG_WARN("repl_client", "fchmod on temp snapshot file failed: %s",
                    std::strerror(errno));
    }

    std::FILE* f = ::fdopen(fd, "wb");
    if (!f) {
        const int saved = errno;
        ::close(fd);
        ::unlink(buf.data());
        errno = saved;
        return nullptr;
    }

    out_path.assign(buf.data());
    return f;
}

/// Build WAL filename for a given index: wal_000000.bin
static std::string wal_filename(const std::string& dir, uint32_t index) {
    char buf[32];
    std::snprintf(buf, sizeof(buf), "wal_%06u.bin", index);
    return dir + "/" + buf;
}

/// Send all bytes on a **blocking** socket, handling partial writes. True on success.
///
/// `ReplicationClient` only, and the qualifier is the whole point. On a blocking socket EAGAIN means
/// the `SO_RCVTIMEO`/`SO_SNDTIMEO` deadline expired, which is a genuine failure; on the *primary's*
/// non-blocking replica sockets it means "come back later", and treating it as failure dropped a
/// replica in the middle of catch-up (series D §16). The primary side therefore has no
/// write-it-now helper at all: everything it sends goes through `enqueue_send()` and the EPOLLOUT
/// drain, which is the only shape in which a socket saying "later" has somewhere to say it - and the
/// same shape TLS needs for `SSL_ERROR_WANT_WRITE`.
static bool blocking_send_all(int fd, TlsChannel* tls, const void* data, size_t len) {
    if (tls != nullptr) {
        std::string why;
        const int rc = tls_blocking_write_all(tls->raw(), static_cast<const char*>(data), len, &why);
        if (rc != 1) {
            OB_LOG_WARN("repl_client", "TLS write of %zu bytes failed: %s", len, why.c_str());
            return false;
        }
        return true;
    }
    const auto* ptr = static_cast<const uint8_t*>(data);
    size_t remaining = len;
    while (remaining > 0) {
        ssize_t n = ::send(fd, ptr, remaining, MSG_NOSIGNAL);
        if (n <= 0) {
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

/// How much catch-up output accumulates before it is pushed towards the socket.
///
/// Roughly one default socket send buffer, so a replica that is reading keeps the queue at this
/// order of magnitude rather than at the size of the requested WAL range.
static constexpr size_t kCatchupDrainThreshold = 256 * 1024;

/// How much one pass of a catch-up may queue before it yields (#93).
///
/// The bound the cursor exists for is the send queue; this is the other one, and it is about the
/// mutex. `broadcast()` needs `mtx_` and runs under the engine's write lock, so a catch-up holding
/// `mtx_` for the length of its range stalls every client write for that long - the shape roadmap
/// #97 measured at 135 s on the mesh side. A megabyte is read out of the page cache and copied in
/// about a millisecond, and the loop comes straight back for the next one.
static constexpr size_t kCatchupBatchBytes = 1024 * 1024;

/// Everything queued for a replica: what is on its way out, plus what is waiting behind an
/// unfinished catch-up.
///
/// The ceiling is a statement about how much this process is holding for one replica, so it has to
/// count both. A function rather than the sum written out at each of the four sites: the ceiling
/// that forgets the second buffer is a ceiling that can be walked past.
static size_t queued_bytes(const ReplicaInfo& replica) {
    return replica.send_buf.size() + replica.catchup.pending.size();
}

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
    // No notification: run_loop() already comes back every 100 ms and polls. See
    // poll_snapshot_preparation() in the header for why that is enough here.
    , snapshot_builder_([] {})
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
    // Serialised, and the guard is now an exchange, so a second caller waits here and then returns
    // knowing the stop is *finished*. It used to return knowing only that one had begun: the old
    // code stored `false` before joining, so caller two skipped the join, destroyed this object, and
    // its destructor's `stop()` hit the same guard — destroying a joinable `std::thread`, which
    // calls `std::terminate`. The node died with SIGABRT on a graceful FAILOVER.
    std::lock_guard<std::mutex> serialise(stop_mtx_);
    if (!running_.exchange(false, std::memory_order_acq_rel)) return;

    if (thread_.joinable()) {
        thread_.join();
    }

    // And the snapshot worker, before any descriptor it might be about to be handed goes away. A
    // snapshot in flight is waited for rather than cancelled — abandoning a flush half-way is worse
    // than waiting for a result that is about to be discarded.
    snapshot_builder_.shutdown();
    snapshot_prepare_ = ReplicaSnapshotPrepare{};

    // Close all replica fds.
    {
        std::lock_guard<std::mutex> lock(mtx_);
        for (auto& r : replicas_) {
            if (r.fd >= 0) {
                if (r.tls != nullptr) r.tls->shutdown();
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
            queue_to_replica(*it, len_prefix, 4);
            queue_to_replica(*it, compressed.data(), compressed.size());
        } else {
            queue_to_replica(*it, msg.data(), msg.size());
        }

        // If the send buffer is too large, the replica is too slow — disconnect it.
        if (queued_bytes(*it) > MAX_SEND_BUF_SIZE) {
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

ssize_t BufferedReader::pull(void* dst, size_t len) {
    if (tls_ == nullptr) return ::recv(fd_, dst, len, 0);

    size_t got = 0;
    switch (tls_->read(dst, len, got)) {
    case TlsChannel::Io::Data:
        return static_cast<ssize_t>(got);
    case TlsChannel::Io::Closed:
        return 0;
    case TlsChannel::Io::Again:
        // Both wants collapse to EAGAIN here, which is what makes this a drop-in for `::recv`. The
        // one that does not collapse is `io_want()`, and an edge-triggered caller has to read it:
        // a TLS *read* waiting to write needs EPOLLOUT, not readability.
        errno = EAGAIN;
        return -1;
    case TlsChannel::Io::Error:
        break;
    }
    errno = EIO;
    return -1;
}

void ReplicationManager::arm_epollout(const ReplicaInfo& replica) {
    if (epoll_fd_ < 0 || replica.fd < 0) return;
    struct epoll_event ev{};
    ev.events  = EPOLLIN | EPOLLOUT | EPOLLET;
    ev.data.fd = replica.fd;
    ::epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, replica.fd, &ev);
}

void ReplicationManager::disarm_epollout(const ReplicaInfo& replica) {
    if (epoll_fd_ < 0 || replica.fd < 0) return;
    struct epoll_event ev{};
    ev.events  = EPOLLIN | EPOLLET;
    ev.data.fd = replica.fd;
    ::epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, replica.fd, &ev);
}

ReplicaInfo* ReplicationManager::find_replica_locked(int fd) {
    for (auto& r : replicas_) {
        if (r.fd == fd) return &r;
    }
    return nullptr;
}

bool ReplicationManager::advance_tls_handshake(ReplicaInfo& replica) {
    if (replica.tls == nullptr || !replica.tls->handshaking()) return true;

    if (!replica.tls->continue_handshake()) return false;
    if (replica.tls->handshaking()) {
        // Not finished. Arm what OpenSSL asked for rather than what the event was: a handshake with
        // ServerHello to send needs writability even though this side never called write.
        if (replica.tls->io_want() == IoWant::Write) arm_epollout(replica);
        else                                        disarm_epollout(replica);
        return true;
    }

    replica.identity = replica.tls->identity();
    OB_LOG_INFO("repl_mgr", "replica fd=%d from %s authenticated by certificate: %s",
                replica.fd, replica.address.c_str(), replica.identity.c_str());
    publish_replica_gauges();
    // Whatever was queued before the handshake - the challenge, on an authenticating link - goes out
    // now. This is the flush the accept path deliberately did not perform.
    return drain_send_buffer(replica);
}

void ReplicationManager::publish_replica_gauges() {
    if (engine_ == nullptr) return;
    size_t connected = 0;
    size_t verified  = 0;
    for (const auto& r : replicas_) {
        if (r.fd < 0) continue;
        ++connected;
        if (r.tls != nullptr && !r.tls->handshaking()) ++verified;
    }
    engine_->registry().set_gauge("ob_replicas_connected",    static_cast<int64_t>(connected));
    engine_->registry().set_gauge("ob_replicas_tls_verified", static_cast<int64_t>(verified));
}

void ReplicationManager::enqueue_and_flush(ReplicaInfo& replica, const void* data, size_t len) {
    enqueue_send(replica, data, len);
    drain_send_buffer(replica);
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
    return replica.tls == nullptr ? drain_send_buffer_plain(replica)
                                  : drain_send_buffer_tls(replica);
}

bool ReplicationManager::drain_send_buffer_plain(ReplicaInfo& replica) {
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
    disarm_epollout(replica);
    return true;
}

bool ReplicationManager::drain_send_buffer_tls(ReplicaInfo& replica) {
    // The handshake has one owner, and it is advance_tls_handshake(). Writing here while it runs
    // would work — OpenSSL lets a write drive a handshake — and that is exactly the accident series
    // C shipped a comment about (pitfall 130): two functions advancing one state machine.
    if (replica.tls->handshaking()) return true;

    while (!replica.send_buf.empty()) {
        size_t sent = 0;
        const TlsChannel::Io r =
            replica.tls->write(replica.send_buf.data(), replica.send_buf.size(), sent);
        if (r == TlsChannel::Io::Data) {
            // This erase moves the *pending* bytes to a lower address in the same allocation, which
            // is a different address — so a retry after WANT_WRITE presents a pointer OpenSSL did
            // not see before, and refuses it with `bad write retry` unless
            // SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER is set. Every context this engine builds sets it.
            replica.send_buf.erase(replica.send_buf.begin(),
                                   replica.send_buf.begin() + static_cast<ptrdiff_t>(sent));
            continue;
        }
        if (r == TlsChannel::Io::Again) {
            // WANT_WRITE arms EPOLLOUT. WANT_READ must not: the socket is writable and OpenSSL is
            // waiting to read, so arming it spins the loop (pitfall 5). handle_replica_data() retries
            // the drain for that case, which is the fourth of the four combinations.
            if (replica.tls->io_want() == IoWant::Write) arm_epollout(replica);
            return true;
        }
        return false;   // Closed or Error, both already logged by the channel
    }

    disarm_epollout(replica);
    return true;
}

void ReplicationManager::remove_replica_locked(int fd) {
    // A snapshot being created for this replica is now pointless. It is not cancellable, so the
    // request is marked dead and poll_snapshot_preparation() discards the result when it lands.
    if (snapshot_prepare_.active && snapshot_prepare_.fd == fd) {
        OB_LOG_WARN("repl_mgr",
                    "replica fd=%d left while its snapshot was being created (token %llu); the "
                    "work will finish and the result will be discarded",
                    fd, static_cast<unsigned long long>(snapshot_prepare_.token));
        snapshot_prepare_.active = false;
    }

    // close_notify before the descriptor goes, so the replica's read reports a clean close rather
    // than a truncation it cannot tell from a network fault. Best effort: on a socket that is
    // already gone this does nothing, which is why it is not checked.
    if (ReplicaInfo* r = find_replica_locked(fd); r != nullptr && r->tls != nullptr) {
        r->tls->shutdown();
    }

    ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
    ::close(fd);
}

void ReplicationManager::disconnect_replica_locked(int fd, const char* reason) {
    OB_LOG_INFO("repl_mgr", "disconnecting replica fd=%d: %s", fd, reason);
    remove_replica_locked(fd);
    for (auto it = replicas_.begin(); it != replicas_.end(); ++it) {
        if (it->fd == fd) {
            replicas_.erase(it);
            break;
        }
    }
}

void ReplicationManager::run_loop() {
    static constexpr int MAX_EVENTS = 32;
    struct epoll_event events[MAX_EVENTS];

    auto last_heartbeat = std::chrono::steady_clock::now();

    // How long this loop is allowed to sleep. 100 ms when there is nothing in hand; zero while a
    // catch-up has room to queue more, because that cursor is work this thread already owns and
    // sleeping on it would cap a catch-up at one batch per tick. A cursor whose queue is *full* is
    // not work in hand - waiting on that would be the busy-spin of pitfall 5, and EPOLLOUT is what
    // says the socket drained. Recomputed at the end of every pass, after this pass's drains.
    int wait_ms = 100;

    while (running_.load(std::memory_order_acquire)) {
        int nfds = ::epoll_wait(epoll_fd_, events, MAX_EVENTS, wait_ms);
        if (nfds < 0) {
            if (errno == EINTR) continue;
            break; // fatal epoll error
        }

        // Has a snapshot worker finished? Before dispatching events, and on the timeout path too.
        poll_snapshot_preparation();

        // Both replica gauges, once per pass. This tick is the mechanism: publishing them only
        // where a link is established - which is what the verified count did - leaves a dropped
        // replica counted until the next handshake, so the gauge could claim more verified links
        // than there were links at all.
        {
            std::lock_guard<std::mutex> lock(mtx_);
            publish_replica_gauges();
        }

        for (int i = 0; i < nfds; ++i) {
            int fd = events[i].data.fd;

            if (fd == listen_fd_) {
                accept_replica();
                continue;
            }

            // A handshake in progress consumes this event and nothing else. Not one byte of
            // application data may be read before it finishes: a frame arriving earlier would be a
            // frame from a transport that has not proved who it is, and the cluster-secret gate is a
            // different mechanism that knows nothing about TLS.
            {
                std::lock_guard<std::mutex> lock(mtx_);
                ReplicaInfo* r = find_replica_locked(fd);
                if (r != nullptr && r->tls != nullptr && r->tls->handshaking()) {
                    if (!advance_tls_handshake(*r)) {
                        disconnect_replica_locked(fd, "tls handshake failed");
                    }
                    continue;
                }
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

        // Advance every catch-up that has room, once per pass and *after* this pass's EPOLLOUT
        // drains have made that room (#93).
        //
        // One site, and not also inside the EPOLLOUT branch beside the snapshot transfer's: the
        // cursor has to be resumed from three different situations - the socket drained, the batch
        // budget ran out with the queue already empty, and the first batch was queued by
        // `handle_catchup()` - and only one of those arrives as an event. Two places advancing one
        // cursor is the shape series C shipped a comment about.
        {
            std::lock_guard<std::mutex> lock(mtx_);
            for (auto& r : replicas_) {
                if (r.fd >= 0 && r.catchup.active) continue_catchup(r);
            }
            wait_ms = catchup_can_progress_locked() ? 0 : 100;
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
                    queue_to_replica(*it, len_prefix, 4);
                    queue_to_replica(*it, compressed.data(), compressed.size());
                } else {
                    queue_to_replica(*it, hb, static_cast<size_t>(hb_len));
                }
                if (queued_bytes(*it) > MAX_SEND_BUF_SIZE) {
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
            OB_LOG_WARN("repl_mgr", "accept4 failed: %s", std::strerror(errno));
            break;
        }

        // Build address string for logging.
        char addr_str[64];
        ::inet_ntop(AF_INET, &client_addr.sin_addr, addr_str, sizeof(addr_str));
        std::string address = std::string(addr_str) + ":" +
                              std::to_string(ntohs(client_addr.sin_port));

        OB_LOG_INFO("repl_mgr", "new replica connection from %s, fd=%d", address.c_str(), client_fd);

        // Check max replicas limit (Requirement 1.4).
        {
            std::lock_guard<std::mutex> lock(mtx_);
            if (static_cast<int>(replicas_.size()) >= config_.max_replicas) {
                OB_LOG_WARN("repl_mgr", "max replicas reached (%d), rejecting fd=%d",
                            config_.max_replicas, client_fd);
                // The one raw socket write left on this side, and only when TLS is off. The peer
                // has negotiated nothing yet, so a plaintext line would arrive where a ServerHello
                // is expected and the replica would report `wrong version number` - the reason
                // rather than a red herring, but a worse message than none. Completing a handshake
                // in order to refuse would also be a handshake performed on demand for an
                // unauthenticated peer. The reason is in this node's log, which is where an
                // operator looks for it.
                if (config_.tls_server == nullptr) {
                    const char* msg = "ERR max_replicas_reached\n";
                    auto wr = ::write(client_fd, msg, std::strlen(msg));
                    (void)wr;
                }
                ::close(client_fd);
                continue;
            }
        }

        // Add to epoll (edge-triggered for client data).
        struct epoll_event ev{};
        ev.events  = EPOLLIN | EPOLLET;
        ev.data.fd = client_fd;
        if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, client_fd, &ev) < 0) {
            OB_LOG_WARN("repl_mgr", "epoll_ctl ADD failed for fd=%d: %s", client_fd, std::strerror(errno));
            ::close(client_fd);
            continue;
        }

        // Add to replicas list.
        ReplicaInfo info;
        info.fd               = client_fd;
        info.conn_id          = next_conn_id_.fetch_add(1, std::memory_order_relaxed);
        info.address          = std::move(address);
        info.confirmed_file   = 0;
        info.confirmed_offset = 0;
        info.compress         = false;
        info.reader.set_fd(client_fd);

        // TLS before the record joins the list, so a channel that cannot even be created never
        // becomes a replica. `set_tls` after `set_fd`, because `set_fd` clears the channel - it is
        // the "this reader now belongs to a different connection" call.
        if (config_.tls_server != nullptr) {
            try {
                info.tls = config_.tls_server->open_channel(client_fd, /*server_side=*/true,
                                                            info.address);
                info.reader.set_tls(info.tls);
                OB_LOG_DEBUG("repl_mgr", "tls handshake started for replica fd=%d from %s",
                             client_fd, info.address.c_str());
            } catch (const std::exception& e) {
                OB_LOG_WARN("repl_mgr", "cannot start a TLS handshake for fd=%d from %s: %s",
                            client_fd, info.address.c_str(), e.what());
                ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, client_fd, nullptr);
                ::close(client_fd);
                continue;
            }
        }

        std::lock_guard<std::mutex> lock(mtx_);

        // NOTE: We only set the compress flag here. The actual COMPRESS LZ4 directive
        // is sent AFTER catchup in handle_replica_data(), because catchup
        // sends plain text and the replica must not switch to LZ4 mode yet.
        if (config_.compress) {
            info.compress = true;
            OB_LOG_INFO("repl_mgr", "compression enabled for replica fd=%d (will send COMPRESS LZ4 after catchup)", client_fd);
        }

        replicas_.push_back(std::move(info));
        OB_LOG_INFO("repl_mgr", "replica added on connection %llu, total replicas=%zu",
                    static_cast<unsigned long long>(replicas_.back().conn_id), replicas_.size());

        // Challenge first, before the replica has said anything (#30 part two). This side speaks
        // first because it accepted the connection; the replica challenges back, and neither
        // proceeds to REPLICATE until both have answered.
        if (!config_.cluster_secret.empty()) {
            ReplicaInfo& r = replicas_.back();
            r.auth_nonce = generate_nonce_hex();
            const std::string line = "CHALLENGE " + r.auth_nonce + "\n";
            // Queued, not written. Under TLS the handshake has not finished at this point, so the
            // drain returns early and these bytes go out with the first flush afterwards - exactly
            // how the client port's banner works. Without TLS the drain sends them here.
            enqueue_send(r, line.data(), line.size());
            drain_send_buffer(r);
            OB_LOG_INFO("repl_mgr", "challenged replica fd=%d from %s", r.fd, r.address.c_str());
        }
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

    // Nothing may reach the parser while the handshake is running; run_loop() consumes that event
    // before it gets here, and this is the second half of the same statement, for a caller that
    // reaches this function some other way.
    if (replica_ptr->tls != nullptr && replica_ptr->tls->handshaking()) return;

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

        // ── Authentication (#30 part two) ─────────────────────────────────────
        //
        // Answering a challenge is unconditional: the replica sends its challenge before its own
        // response, and by the time the response has been verified `peer_proved` is already true -
        // so a branch gated on `!peer_proved` would leave the second message unanswered and the
        // replica waiting for a proof that never comes.
        if (!config_.cluster_secret.empty() && line.rfind("CHALLENGE ", 0) == 0) {
            const std::string nonce = line.substr(std::strlen("CHALLENGE "));
            if (!is_auth_hex(nonce)) {
                OB_LOG_WARN("repl_mgr", "replica fd=%d from %s sent a malformed challenge",
                            fd, replica_ptr->address.c_str());
                disconnect_replica_locked(fd, "malformed challenge");
                return;
            }
            // Answered as the acceptor, which is what makes a reflected nonce useless: the
            // attacker needs an initiator-side response and this is not one.
            const std::string answer =
                "AUTH " + auth_response(config_.cluster_secret.sole().secret,
                                        AuthSurface::Replication, AuthRole::Acceptor, "",
                                        nonce) + "\n";
            enqueue_and_flush(*replica_ptr, answer.data(), answer.size());
            continue;
        }

        if (!config_.cluster_secret.empty() && !replica_ptr->peer_proved) {
            if (line.rfind("AUTH ", 0) == 0) {
                const std::string got = line.substr(std::strlen("AUTH "));
                const std::string expected =
                    replica_ptr->auth_nonce.empty()
                        ? std::string{}
                        : auth_response(config_.cluster_secret.sole().secret,
                                        AuthSurface::Replication, AuthRole::Initiator, "",
                                        replica_ptr->auth_nonce);
                // Single-use: whether it verified or not, this nonce is spent.
                replica_ptr->auth_nonce.clear();
                if (!responses_equal(expected, got)) {
                    OB_LOG_ERROR("repl_mgr", "replica fd=%d from %s failed authentication",
                                 fd, replica_ptr->address.c_str());
                    const char* err = "ERR unauthenticated\n";
                    enqueue_and_flush(*replica_ptr, err, std::strlen(err));
                    disconnect_replica_locked(fd, "authentication failed");
                    return;
                }
                replica_ptr->peer_proved = true;
                const char* ok = "OK AUTH\n";
                enqueue_and_flush(*replica_ptr, ok, std::strlen(ok));
                OB_LOG_INFO("repl_mgr", "replica fd=%d from %s authenticated",
                            fd, replica_ptr->address.c_str());
                continue;
            }
            // Anything else, REPLICATE included, and the error goes on the wire before the close:
            // a replica that is simply missing its secret would otherwise see a reconnect loop
            // with no message.
            OB_LOG_ERROR("repl_mgr",
                         "replica fd=%d from %s sent '%s' before authenticating - disconnecting",
                         fd, replica_ptr->address.c_str(),
                         sanitise_for_log(line, 32).c_str());
            const char* err = "ERR unauthenticated\n";
            enqueue_and_flush(*replica_ptr, err, std::strlen(err));
            disconnect_replica_locked(fd, "unauthenticated");
            return;
        }

        // Parse REPLICATE handshake: REPLICATE <file_index> <byte_offset> <epoch>
        if (line.rfind("REPLICATE ", 0) == 0) {
            uint32_t from_file = 0;
            size_t from_offset = 0;
            uint64_t replica_epoch = 0;
            int parsed = std::sscanf(line.c_str(), "REPLICATE %u %zu %" SCNu64,
                                     &from_file, &from_offset, &replica_epoch);
            OB_LOG_INFO("repl_mgr", "received REPLICATE from fd=%d: file=%u offset=%zu epoch=%" PRIu64 " (parsed=%d)",
                        fd, from_file, from_offset, replica_epoch, parsed);
            if (parsed >= 2) {
                // Stale-primary check: if replica's epoch > our epoch, reject (Requirement 3.4).
                if (parsed == 3 && replica_epoch > wal_.current_epoch()) {
                    const char* err = "ERR STALE_PRIMARY\n";
                    enqueue_and_flush(*replica_ptr, err, std::strlen(err));
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
                // Start the catch-up from the requested position. Since #93 this queues the
                // first batch and returns; the run loop streams the rest. The COMPRESS LZ4
                // directive left with it - "after the last plain byte" is the cursor's moment to
                // pick, and it is no longer this one.
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

    // The fourth of the four combinations, and the one whose absence looks like a wedged peer.
    // A TLS *read* can leave OpenSSL wanting to write - a key update - and a TLS *write* can leave
    // it wanting to read, in which case the drain deliberately did not arm EPOLLOUT because the
    // socket is already writable. Either way the retry has to happen on a readable event, which is
    // this one, and there is no other path back into the drain.
    replica_ptr = find_replica_locked(fd);
    if (replica_ptr == nullptr || replica_ptr->tls == nullptr) return;
    if (replica_ptr->reader.io_want() == IoWant::Write) {
        arm_epollout(*replica_ptr);
    }
    if (!replica_ptr->send_buf.empty() && !drain_send_buffer(*replica_ptr)) {
        disconnect_replica_locked(fd, "tls write failed");
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

    // Queued rather than written, and this is the fix for a defect older than TLS (series D §16).
    // This used to be `send_all()` on a **non-blocking** socket, so the first EAGAIN - which arrives
    // as soon as the socket send buffer fills, measured at about 208 kB - was read as a dead replica
    // and dropped it mid catch-up. It then reconnected, asked for the same range and was dropped
    // again. Measured before the change: 17 270 of 40 000 records delivered, then
    // `send_to_replica failed`.
    enqueue_send(replica, msg.data(), msg.size());

    // The ceiling still applies, and reaching it still drops the replica - but since #93 it is no
    // longer reachable by asking for a large range. The cursor stops at half of it and comes back,
    // so a queue this deep now means the socket is not moving: the replica is slow, which is the
    // case this ceiling was written for.
    if (queued_bytes(replica) > MAX_SEND_BUF_SIZE) {
        OB_LOG_WARN("repl_mgr",
                    "replica fd=%d is not draining: queued=%zu > %zu - dropping the connection. "
                    "Since #93 a catch-up stops at half this ceiling and resumes, so reaching it "
                    "means the replica itself is not keeping up",
                    replica.fd, queued_bytes(replica), MAX_SEND_BUF_SIZE);
        remove_replica_locked(replica.fd);
        replica.fd = -1;
        return;
    }

    // Push what the socket will take, but not once per record: `enqueue_send()` arms EPOLLOUT when
    // the queue was empty and a completed drain disarms it, so draining after every record would
    // cost two `epoll_ctl` calls per WAL record - worse than the one `send` this replaced. A
    // threshold of about one socket buffer keeps the queue at a few hundred kilobytes against a
    // replica that is reading, and lets it grow to the 16 MB ceiling only against one that is not.
    //
    // Without any drain here the whole requested range would be queued before a byte moved, so the
    // ceiling above would be reached by any catch-up over 16 MB even against a fast replica.
    if (replica.send_buf.size() < kCatchupDrainThreshold) return;
    if (!drain_send_buffer(replica)) {
        OB_LOG_WARN("repl_mgr", "replica fd=%d failed during catch-up, marking disconnected",
                    replica.fd);
        remove_replica_locked(replica.fd);
        replica.fd = -1;
    }
}

void ReplicationManager::handle_catchup(ReplicaInfo& replica, uint32_t from_file,
                                         size_t from_offset) {
    const WalPosition through = wal_.current_position();
    const std::string& wal_dir = wal_.dir();

    OB_LOG_INFO("repl_mgr",
                "catchup for fd=%d: from_file=%u, from_offset=%zu, through_file=%u, "
                "through_offset=%zu, wal_dir=%s",
                replica.fd, from_file, from_offset, through.file_index,
                static_cast<size_t>(through.offset), wal_dir.c_str());

    // The first file has to exist, and that is the one refusal which has to happen before a byte is
    // streamed: a replica asking from a position retention has already removed belongs on the
    // snapshot-bootstrap path (Requirement 6.3), not on this one. Checked here rather than inside
    // the cursor because it is the answer to the *request*, and the cursor's own failures are all
    // "stop where you are".
    {
        const std::string path = wal_filename(wal_dir, from_file);
        int probe = ::open(path.c_str(), O_RDONLY);
        if (probe < 0) {
            OB_LOG_WARN("repl_mgr", "catchup: cannot open WAL file %s: %s", path.c_str(),
                        std::strerror(errno));
            const char* err = "ERR WAL_TRUNCATED\n";
            enqueue_and_flush(replica, err, std::strlen(err));
            OB_LOG_INFO("repl_mgr", "sent ERR WAL_TRUNCATED to replica fd=%d", replica.fd);
            return;
        }
        ::close(probe);
    }

    CatchupCursor& cur = replica.catchup;
    cur.active         = true;
    cur.file           = from_file;
    cur.offset         = from_offset;
    cur.through_file   = through.file_index;
    cur.through_offset = through.offset;
    cur.pending.clear();
    // The directive travels with the cursor rather than with this function, because "after the
    // last plain byte of the catch-up" is now a moment the cursor decides.
    cur.compress_after = replica.compress;

    continue_catchup(replica);
}

void ReplicationManager::continue_catchup(ReplicaInfo& replica) {
    CatchupCursor& cur = replica.catchup;
    if (!cur.active) return;

    const std::string& wal_dir = wal_.dir();
    size_t   queued_this_pass = 0;
    int      fd        = -1;
    uint32_t open_file = 0;

    // One `goto`-free way to be sure the descriptor is closed on all eleven exits.
    struct FdGuard {
        int& fd;
        ~FdGuard() { if (fd >= 0) ::close(fd); }
    } guard{fd};

    const auto next_file = [&]() {
        if (fd >= 0) { ::close(fd); fd = -1; }
        ++cur.file;
        cur.offset = 0;
    };

    while (true) {
        // A record that failed to send took the replica with it; `send_to_replica()` has already
        // removed it and set `fd` to -1.
        if (replica.fd < 0) {
            cur.active = false;
            cur.pending.clear();
            return;
        }

        // The end, and the only place this cursor finishes on purpose. `>` and not just `==`
        // because a WAL file shorter than the position recorded for it - a torn tail - walks the
        // cursor past `through_file` rather than leaving it reading forever.
        if (cur.file > cur.through_file ||
            (cur.file == cur.through_file && cur.offset >= cur.through_offset)) {
            if (fd >= 0) { ::close(fd); fd = -1; }
            finish_catchup(replica);
            return;
        }

        // Two bounds, and they are different promises. The queue is about how much of this range
        // the process is holding for one replica; the batch is about how long the write path waits
        // for `mtx_`. Reaching the first means EPOLLOUT will say when there is room; reaching the
        // second means the loop comes straight back.
        if (replica.send_buf.size() >= MAX_SEND_BUF_SIZE / 2) return;
        if (queued_this_pass >= kCatchupBatchBytes) return;

        if (fd < 0 || open_file != cur.file) {
            if (fd >= 0) { ::close(fd); fd = -1; }
            const std::string path = wal_filename(wal_dir, cur.file);
            fd = ::open(path.c_str(), O_RDONLY);
            if (fd < 0) {
                // A file in the middle of the range is missing. Unexpected but not fatal, and the
                // same answer the synchronous pass gave: stop here and go live. The first file is
                // the only one whose absence is an answer to the replica, and that was checked
                // before this cursor existed.
                OB_LOG_WARN("repl_mgr",
                            "catchup for fd=%d: cannot open WAL file %s: %s - stopping at file %u",
                            replica.fd, path.c_str(), std::strerror(errno), cur.file);
                finish_catchup(replica);
                return;
            }
            open_file = cur.file;
        }

        WALRecord hdr{};
        const ssize_t n = ::pread(fd, &hdr, sizeof(WALRecord), static_cast<off_t>(cur.offset));
        if (n != static_cast<ssize_t>(sizeof(WALRecord))) {
            next_file();   // EOF, or a header too short to be one
            continue;
        }

        std::vector<uint8_t> payload(hdr.payload_len);
        if (hdr.payload_len > 0) {
            const ssize_t r = ::pread(fd, payload.data(), hdr.payload_len,
                                      static_cast<off_t>(cur.offset + sizeof(WALRecord)));
            if (r != static_cast<ssize_t>(hdr.payload_len)) {
                next_file();   // a record whose payload is not all there yet is the end of this file
                continue;
            }
        }

        const size_t record_bytes = sizeof(WALRecord) + hdr.payload_len;

        // ROTATE is internal to WAL file management, and it is the last thing in its file.
        if (hdr.record_type == WAL_RECORD_ROTATE) {
            next_file();
            continue;
        }

        send_to_replica(replica, hdr, payload.data(), hdr.payload_len);
        cur.offset       += record_bytes;
        queued_this_pass += record_bytes;
    }
}

void ReplicationManager::finish_catchup(ReplicaInfo& replica) {
    CatchupCursor& cur = replica.catchup;
    cur.active = false;

    if (replica.fd < 0) {
        cur.pending.clear();
        return;
    }

    // Plain text up to here, LZ4 from here: the directive is the seam, so it goes out after the
    // last catch-up byte and before the first live one. The records in `pending` were framed at
    // broadcast time and are already compressed if this replica asked for that, which is the other
    // half of the same ordering.
    if (cur.compress_after) {
        const char* directive = "COMPRESS LZ4\n";
        enqueue_send(replica, directive, std::strlen(directive));
        OB_LOG_INFO("repl_mgr", "sent COMPRESS LZ4 to replica fd=%d after catchup", replica.fd);
    }
    cur.compress_after = false;

    if (!cur.pending.empty()) {
        OB_LOG_INFO("repl_mgr",
                    "catchup complete for fd=%d at file=%u offset=%zu; releasing %zu bytes of live "
                    "records that arrived while it streamed",
                    replica.fd, cur.file, cur.offset, cur.pending.size());
        enqueue_send(replica, cur.pending.data(), cur.pending.size());
        cur.pending.clear();
        cur.pending.shrink_to_fit();
    } else {
        OB_LOG_INFO("repl_mgr", "catchup complete for fd=%d at file=%u offset=%zu", replica.fd,
                    cur.file, cur.offset);
    }

    if (!replica.send_buf.empty() && !drain_send_buffer(replica)) {
        OB_LOG_WARN("repl_mgr", "replica fd=%d failed while flushing the catch-up tail", replica.fd);
        remove_replica_locked(replica.fd);
        replica.fd = -1;
    }
}

void ReplicationManager::queue_to_replica(ReplicaInfo& replica, const void* data, size_t len) {
    if (!replica.catchup.active) {
        enqueue_send(replica, data, len);
        return;
    }
    // A live record may not overtake the history in front of it. It waits here, framed exactly as
    // it would have been queued, and `finish_catchup()` releases it in arrival order - which is WAL
    // order, because both happen under the engine's write lock.
    const auto* bytes = static_cast<const uint8_t*>(data);
    replica.catchup.pending.insert(replica.catchup.pending.end(), bytes, bytes + len);
}

bool ReplicationManager::catchup_can_progress_locked() const {
    for (const auto& r : replicas_) {
        if (r.fd < 0 || !r.catchup.active) continue;
        if (r.send_buf.size() < MAX_SEND_BUF_SIZE / 2) return true;
    }
    return false;
}

void ReplicationManager::handle_snapshot_request(ReplicaInfo& replica) {
    if (!engine_) {
        const char* err = "ERR SNAPSHOT_FAILED no_engine\n";
        enqueue_send(replica, err, std::strlen(err));
        return;
    }

    // Since #79 the flush and the checksum pass happen on a worker thread. What that removes here is
    // not only the stall — this function used to unlock mtx_ in the middle, create the snapshot, lock
    // it again and then re-find the replica, because the state it was holding a reference to could
    // have been removed while the lock was down. Nothing is released and nothing has to be re-found
    // now: the wait happens elsewhere.
    if (replica.snapshot_transfer.active) {
        const char* err = "ERR SNAPSHOT_FAILED busy\n";
        enqueue_send(replica, err, std::strlen(err));
        return;
    }
    if (snapshot_prepare_.active || snapshot_builder_.busy()) {
        OB_LOG_WARN("repl_mgr",
                    "refusing snapshot for fd=%d: one is already being created (token %llu)",
                    replica.fd, static_cast<unsigned long long>(snapshot_prepare_.token));
        const char* err = "ERR SNAPSHOT_FAILED busy\n";
        enqueue_send(replica, err, std::strlen(err));
        return;
    }

    const uint64_t token = next_snapshot_token_++;

    // The pointer is captured by value rather than read from the member on the worker, so the
    // worker never touches state this manager can change under it.
    Engine* engine = engine_;
    if (!snapshot_builder_.start(token, [engine] {
            SnapshotWithSequenceState out;
            out.manifest = engine->create_snapshot();
            return out;
        })) {
        OB_LOG_ERROR("repl_mgr", "could not start a snapshot worker for fd=%d (token %llu)",
                     replica.fd, static_cast<unsigned long long>(token));
        const char* err = "ERR SNAPSHOT_FAILED worker_unavailable\n";
        enqueue_send(replica, err, std::strlen(err));
        return;
    }

    snapshot_prepare_            = ReplicaSnapshotPrepare{};
    snapshot_prepare_.active     = true;
    snapshot_prepare_.fd         = replica.fd;
    snapshot_prepare_.conn_id    = replica.conn_id;
    snapshot_prepare_.token      = token;
    snapshot_prepare_.started_at = std::chrono::steady_clock::now();

    OB_LOG_INFO("repl_mgr",
                "snapshot for replica fd=%d (connection %llu) is being created on a worker thread "
                "(token %llu)",
                replica.fd, static_cast<unsigned long long>(replica.conn_id),
                static_cast<unsigned long long>(token));
}

void ReplicationManager::poll_snapshot_preparation() {
    // Collected without mtx_: the worker has published by the time take_result() returns anything,
    // so this joins a thread that is already on its way out.
    auto result = snapshot_builder_.take_result();
    if (!result) return;

    std::lock_guard<std::mutex> lock(mtx_);

    auto& prep = snapshot_prepare_;
    const double prepare_ms = std::chrono::duration<double, std::milli>(
                                  std::chrono::steady_clock::now() - prep.started_at).count();

    auto discard = [&](const char* why) {
        OB_LOG_WARN("repl_mgr", "discarding a finished snapshot (token %llu) after %.1f ms: %s",
                    static_cast<unsigned long long>(result->token), prepare_ms, why);
        prep = ReplicaSnapshotPrepare{};
    };

    if (!prep.active) {
        discard("the replica that asked for it is gone");
        return;
    }
    if (prep.token != result->token) {
        discard("it answers a request nobody is waiting for");
        return;
    }

    // Matched on the connection, not the descriptor alone: a closed fd can be reissued to the next
    // replica to connect, which has asked for nothing.
    ReplicaInfo* replica = nullptr;
    for (auto& r : replicas_) {
        if (r.fd == prep.fd && r.conn_id == prep.conn_id) {
            replica = &r;
            break;
        }
    }
    if (replica == nullptr) {
        discard("connection to the replica that asked no longer exists");
        return;
    }

    if (!result->ok) {
        OB_LOG_ERROR("repl_mgr", "snapshot creation failed for fd=%d (token %llu): %s",
                     replica->fd, static_cast<unsigned long long>(result->token),
                     result->error.c_str());
        char err[256];
        int len = std::snprintf(err, sizeof(err), "ERR SNAPSHOT_FAILED %s\n",
                                result->error.c_str());
        prep = ReplicaSnapshotPrepare{};
        enqueue_send(*replica, err, static_cast<size_t>(len));
        return;
    }

    OB_LOG_INFO("repl_mgr", "snapshot for fd=%d ready after %.1f ms on the worker (token %llu)",
                replica->fd, prepare_ms, static_cast<unsigned long long>(result->token));

    prep = ReplicaSnapshotPrepare{};
    begin_snapshot_transfer(*replica, std::move(result->snap.manifest));
}

void ReplicationManager::begin_snapshot_transfer(ReplicaInfo& replica,
                                                 SnapshotManifest&& manifest_in) {
    SnapshotManifest manifest = std::move(manifest_in);

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
    if (config_.primary_port == 0) {
        OB_LOG_INFO("repl_client", "not starting — primary_port=0");
        return;
    }
    if (running_.load(std::memory_order_relaxed)) {
        OB_LOG_WARN("repl_client", "already running, skipping start()");
        return;
    }

    // Load last confirmed offset from state file (Requirement 6.1, 6.2).
    load_state();

    const uint32_t cf = confirmed_file_.load(std::memory_order_relaxed);
    const size_t   co = confirmed_offset_.load(std::memory_order_relaxed);
    OB_LOG_INFO("repl_client",
                "starting replication client, primary=%s:%u, confirmed_file=%u, "
                "confirmed_offset=%lu",
                config_.primary_host.c_str(), config_.primary_port,
                cf, static_cast<unsigned long>(co));

    running_.store(true, std::memory_order_release);
    thread_ = std::thread([this]() { run_loop(); });
}

void ReplicationClient::stop() {
    // Same change as `ReplicationManager::stop()`, and made at the same time rather than after this
    // one is observed to abort as well: identical guard, identical destructor calling it, and the
    // demotion path stops both objects in one function.
    std::lock_guard<std::mutex> serialise(stop_mtx_);
    if (!running_.exchange(false, std::memory_order_acq_rel)) return;

    // Shutdown the socket to unblock any blocking recv() in the receive thread.
    //
    // Under fd_mtx_, and that is not decoration: reading the descriptor and then calling shutdown()
    // on it leaves a window in which the receive thread closes it and the kernel hands the number to
    // something else — so the call would land on an unrelated socket. shutdown() itself is the right
    // tool here and cannot be replaced by a flag, because unlike close() it actually wakes a blocked
    // recv().
    {
        std::lock_guard<std::mutex> lk(fd_mtx_);
        const int fd = fd_.load(std::memory_order_acquire);
        if (fd >= 0) {
            ::shutdown(fd, SHUT_RDWR);
        }
    }

    if (thread_.joinable()) {
        thread_.join();
    }

    close_socket();

    save_state();
}

void ReplicationClient::close_socket() {
    std::lock_guard<std::mutex> lk(fd_mtx_);
    const int fd = fd_.exchange(-1, std::memory_order_acq_rel);
    if (fd >= 0) {
        // close_notify first, so the primary's read reports a clean close. Under the same mutex as
        // the descriptor: the channel holds the `SSL` bound to it, and shutting one down after the
        // number has been reused would write into somebody else's connection.
        if (tls_ != nullptr) tls_->shutdown();
        ::close(fd);
    }
    // The channel belongs to the connection, not to this object, so it goes with it. Left in place
    // it would be handed to the next connection's reader and decrypt with the previous session's
    // keys - a failure that reads as corruption rather than as a lifetime mistake.
    tls_.reset();
}

ReplicationClient::State ReplicationClient::state() const {
    return State{confirmed_file_.load(std::memory_order_relaxed),
                 confirmed_offset_.load(std::memory_order_relaxed),
                 (fd_.load(std::memory_order_acquire) >= 0),
                 records_replayed_.load(std::memory_order_relaxed),
                 bootstrapping_.load(std::memory_order_acquire),
                 snapshot_bytes_received_.load(std::memory_order_relaxed),
                 snapshot_bytes_total_.load(std::memory_order_relaxed)};
}

void ReplicationClient::run_loop() {
    // Reconnection loop with exponential backoff (Requirement 6.2).
    int backoff_sec = 5;
    static constexpr int MAX_BACKOFF_SEC = 60;

    OB_LOG_INFO("repl_client", "run_loop started, connecting to %s:%u",
                config_.primary_host.c_str(), config_.primary_port);

    while (running_.load(std::memory_order_acquire)) {
        try {
            connect_to_primary();
            OB_LOG_INFO("repl_client", "connected to primary %s:%u",
                        config_.primary_host.c_str(), config_.primary_port);
            backoff_sec = 5; // Reset backoff on successful connect.
            receive_and_replay();
            OB_LOG_INFO("repl_client", "receive_and_replay() returned, will reconnect");
        } catch (const std::exception& ex) {
            OB_LOG_WARN("repl_client", "connection/replay error: %s", ex.what());
        } catch (...) {
            OB_LOG_WARN("repl_client", "unknown error in run_loop");
        }

        // Clean up socket on disconnect. Through close_socket() so it cannot race stop()'s
        // shutdown() on the same descriptor.
        close_socket();

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
        close_socket();
        throw std::runtime_error("ReplicationClient: invalid primary_host: " +
                                 config_.primary_host);
    }

    if (::connect(fd_, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
        close_socket();
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

    // ── TLS (#30 part three, series D) ────────────────────────────────────────
    //
    // Before authentication and before REPLICATE, because everything after this point is
    // application data. The handshake is blocking here - this client has its own thread and a
    // 5-second `SO_RCVTIMEO`, so a primary that accepts TCP and then says nothing is a transient
    // failure that `run_loop()` retries, not a stalled event loop.
    if (config_.tls_client != nullptr) {
        tls_ = config_.tls_client->open_channel(fd_, /*server_side=*/false, config_.primary_host);
        // The name check, and it is not what SSL_VERIFY_PEER does. Without it any certificate this
        // CA signed authenticates any host, so with a private CA signing the cluster another node's
        // certificate would be accepted here and the relay in SECURITY.md would still work
        // (pitfall 124).
        if (!tls_expect_host(tls_->raw(), config_.primary_host)) {
            close_socket();
            throw std::runtime_error("ReplicationClient: cannot require the primary's certificate "
                                     "to cover " + config_.primary_host);
        }
        std::string why;
        if (!tls_->blocking_handshake(&why)) {
            close_socket();
            throw std::runtime_error("ReplicationClient: TLS handshake with " +
                                     config_.primary_host + " failed: " + why);
        }
        reader_.set_tls(tls_);
        OB_LOG_INFO("repl_client", "TLS established with primary %s:%u, certificate cn=%s",
                    config_.primary_host.c_str(), config_.primary_port,
                    tls_->identity().c_str());
    }

    // Reset compression flag for new connection.
    compress_ = false;

    // ── Authentication (#30 part two) ─────────────────────────────────────────
    //
    // Mutual, and both directions complete before REPLICATE goes out. The order is fixed so that
    // neither side ever has to handle an out-of-order message: the primary challenges on accept,
    // this side answers *after* sending its own challenge, and the primary's two replies therefore
    // arrive as `AUTH <hmac>` then `OK AUTH`.
    if (!config_.cluster_secret.empty()) {
        if (!authenticate_with_primary()) {
            close_socket();
            throw std::runtime_error("ReplicationClient: authentication with the primary failed");
        }
    }

    // Send REPLICATE handshake (Requirement 4.2, 3.1).
    // Even fresh replicas start with REPLICATE 0 0 0. If the primary responds
    // with ERR WAL_TRUNCATED, receive_and_replay() will trigger snapshot bootstrap.
    char handshake[128];
    int len = std::snprintf(handshake, sizeof(handshake), "REPLICATE %u %zu %" PRIu64 "\n",
                            confirmed_file_.load(std::memory_order_relaxed),
                            confirmed_offset_.load(std::memory_order_relaxed),
                            local_epoch_.load(std::memory_order_relaxed));
    if (!blocking_send_all(fd_, tls_.get(), handshake, static_cast<size_t>(len))) {
        close_socket();
        throw std::runtime_error("ReplicationClient: failed to send handshake");
    }
    OB_LOG_INFO("repl_client", "handshake sent: %.*s", len - 1, handshake);
}

bool ReplicationClient::authenticate_with_primary() {
    const std::string& secret = config_.cluster_secret.sole().secret;
    char line_buf[512];

    auto read_line = [&]() -> std::string {
        const ssize_t n = reader_.read_line(line_buf, sizeof(line_buf));
        if (n <= 0) return {};
        return std::string(line_buf);
    };

    // 1. The primary challenges first, because it accepted the connection.
    const std::string challenge = read_line();
    if (challenge.rfind("CHALLENGE ", 0) != 0) {
        // A primary without a cluster secret says nothing here, so this is also the message an
        // operator sees when only one side has been configured. Naming what arrived instead is what
        // distinguishes that from a network fault.
        OB_LOG_ERROR("repl_client",
                     "expected a challenge from the primary, got '%s' - is the primary running "
                     "with --cluster-secret-file?",
                     sanitise_for_log(challenge, 32).c_str());
        return false;
    }
    const std::string primary_nonce = challenge.substr(std::strlen("CHALLENGE "));
    if (!is_auth_hex(primary_nonce)) {
        OB_LOG_ERROR("repl_client", "primary sent a malformed challenge");
        return false;
    }

    // 2. Our challenge first, then our answer. This ordering is what makes the primary's replies
    //    arrive in a known order.
    const std::string our_nonce = generate_nonce_hex();
    const std::string our_challenge = "CHALLENGE " + our_nonce + "\n";
    if (!blocking_send_all(fd_, tls_.get(), our_challenge.data(), our_challenge.size())) return false;

    const std::string answer =
        "AUTH " + auth_response(secret, AuthSurface::Replication, AuthRole::Initiator, "",
                                primary_nonce) + "\n";
    if (!blocking_send_all(fd_, tls_.get(), answer.data(), answer.size())) return false;

    // 3. The primary's answer to our challenge. Verified before REPLICATE goes out: a primary that
    //    cannot prove itself must not be handed our WAL position, and must not be trusted with the
    //    records it would send back.
    const std::string their_answer = read_line();
    if (their_answer.rfind("AUTH ", 0) != 0) {
        OB_LOG_ERROR("repl_client", "primary did not answer our challenge, got '%s'",
                     sanitise_for_log(their_answer, 32).c_str());
        return false;
    }
    const std::string expected =
        auth_response(secret, AuthSurface::Replication, AuthRole::Acceptor, "", our_nonce);
    if (!responses_equal(expected, their_answer.substr(std::strlen("AUTH ")))) {
        OB_LOG_ERROR("repl_client", "primary failed authentication - refusing to replicate from it");
        return false;
    }

    // 4. And its verdict on ours.
    const std::string verdict = read_line();
    if (verdict.rfind("OK AUTH", 0) != 0) {
        OB_LOG_ERROR("repl_client", "primary refused our credentials: '%s'",
                     sanitise_for_log(verdict, 48).c_str());
        return false;
    }

    OB_LOG_INFO("repl_client", "authenticated with primary %s:%u",
                config_.primary_host.c_str(), config_.primary_port);
    return true;
}

void ReplicationClient::receive_and_replay() {
    OB_LOG_INFO("repl_client", "entering receive_and_replay loop, running=%d, compress=%d",
                running_.load(std::memory_order_acquire), compress_);
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
                OB_LOG_WARN("repl_client", "compressed read_exact(4) failed (timeout or disconnect)");
                return;
            }

            uint32_t comp_len = (static_cast<uint32_t>(len_bytes[0]) << 24) |
                                (static_cast<uint32_t>(len_bytes[1]) << 16) |
                                (static_cast<uint32_t>(len_bytes[2]) << 8) |
                                static_cast<uint32_t>(len_bytes[3]);

            if (comp_len == 0 || comp_len > 16 * 1024 * 1024) {
                OB_LOG_WARN("repl_client", "invalid compressed frame size: %u", comp_len);
                return;
            }

            OB_LOG_INFO("repl_client", "reading compressed frame: %u bytes", comp_len);

            // Read the compressed frame.
            std::vector<uint8_t> compressed(comp_len);
            if (!reader_.read_exact(compressed.data(), comp_len)) {
                OB_LOG_WARN("repl_client", "compressed read_exact(%u) failed", comp_len);
                return;
            }

            // Decompress.
            std::vector<uint8_t> decompressed;
            try {
                decompressed = ob::lz4_decompress(compressed.data(), compressed.size());
            } catch (const std::runtime_error& e) {
                // Decompression failure — disconnect, log error (Requirement 2.5).
                OB_LOG_ERROR("replication", "decompression failed: %s", e.what());
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

                if (parsed == 4 && msg_epoch < local_epoch_.load(std::memory_order_relaxed)) {
                    OB_LOG_WARN("replication", "stale epoch %" PRIu64
                                 " < local %" PRIu64 ", disconnecting",
                                 msg_epoch, local_epoch_.load(std::memory_order_relaxed));
                    return;
                }
                if (parsed == 4 && msg_epoch > local_epoch_.load(std::memory_order_relaxed)) {
                    local_epoch_.store(msg_epoch, std::memory_order_relaxed);
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
                    if (received_epoch.term > local_epoch_.load(std::memory_order_relaxed)) {
                        local_epoch_.store(received_epoch.term, std::memory_order_relaxed);
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

                confirmed_file_.store(file_index, std::memory_order_relaxed);
                confirmed_offset_.store(byte_offset + total_len, std::memory_order_relaxed);
                records_replayed_.fetch_add(1, std::memory_order_relaxed);
                send_ack();
                continue;
            }

            // Handle HEARTBEAT.
            if (line.rfind("HEARTBEAT", 0) == 0) {
                uint64_t hb_epoch = 0;
                if (std::sscanf(line.c_str(), "HEARTBEAT %" SCNu64, &hb_epoch) == 1) {
                    if (hb_epoch < local_epoch_.load(std::memory_order_relaxed)) {
                        OB_LOG_WARN("replication", "stale heartbeat epoch %" PRIu64
                                     " < local %" PRIu64 ", disconnecting",
                                     hb_epoch, local_epoch_.load(std::memory_order_relaxed));
                        return;
                    }
                    if (hb_epoch > local_epoch_.load(std::memory_order_relaxed)) {
                        local_epoch_.store(hb_epoch, std::memory_order_relaxed);
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
        OB_LOG_INFO("repl_client", "about to call read_line, running=%d",
                    running_.load(std::memory_order_acquire));
        ssize_t n = reader_.read_line(line_buf, sizeof(line_buf));
        if (n < 0) {
            // Disconnect or error.
            OB_LOG_WARN("repl_client", "read_line returned %zd (disconnect/error), errno=%d (%s)",
                        n, errno, std::strerror(errno));
            return;
        }
        if (n == 0) {
            // Timeout (EAGAIN from SO_RCVTIMEO) — check running_ and continue.
            continue;
        }

        OB_LOG_INFO("repl_client", "received line: %.*s", static_cast<int>(n), line_buf);

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
            if (parsed == 4 && msg_epoch < local_epoch_.load(std::memory_order_relaxed)) {
                // Stale primary — disconnect and log warning.
                OB_LOG_WARN("replication", "stale epoch %" PRIu64
                             " < local %" PRIu64 ", disconnecting",
                             msg_epoch, local_epoch_.load(std::memory_order_relaxed));
                return;
            }
            // Epoch advancement: if received epoch > local, update (Requirement 2.4).
            if (parsed == 4 && msg_epoch > local_epoch_.load(std::memory_order_relaxed)) {
                local_epoch_.store(msg_epoch, std::memory_order_relaxed);
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
                if (received_epoch.term > local_epoch_.load(std::memory_order_relaxed)) {
                    local_epoch_.store(received_epoch.term, std::memory_order_relaxed);
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
            confirmed_file_.store(file_index, std::memory_order_relaxed);
            confirmed_offset_.store(byte_offset + total_len, std::memory_order_relaxed);
            records_replayed_.fetch_add(1, std::memory_order_relaxed);

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
                if (hb_epoch < local_epoch_.load(std::memory_order_relaxed)) {
                    OB_LOG_WARN("replication", "stale heartbeat epoch %" PRIu64
                                 " < local %" PRIu64 ", disconnecting",
                                 hb_epoch, local_epoch_.load(std::memory_order_relaxed));
                    return;
                }
                // Epoch advancement.
                if (hb_epoch > local_epoch_.load(std::memory_order_relaxed)) {
                    local_epoch_.store(hb_epoch, std::memory_order_relaxed);
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
                            confirmed_file_.load(std::memory_order_relaxed),
                            confirmed_offset_.load(std::memory_order_relaxed));
    blocking_send_all(fd_, tls_.get(), ack, static_cast<size_t>(len));
}

void ReplicationClient::save_state() {
    if (config_.state_file.empty()) return;

    std::FILE* f = open_file_private(config_.state_file, "w");
    if (!f) return;

    std::fprintf(f, "file_index=%u\nbyte_offset=%zu\n",
                 confirmed_file_.load(std::memory_order_relaxed),
                 confirmed_offset_.load(std::memory_order_relaxed));
    std::fclose(f);
}

void ReplicationClient::load_state() {
    if (config_.state_file.empty()) return;

    std::FILE* f = std::fopen(config_.state_file.c_str(), "r");
    if (!f) {
        // No state file — start from beginning.
        confirmed_file_.store(0, std::memory_order_relaxed);
        confirmed_offset_.store(0, std::memory_order_relaxed);
        return;
    }

    uint32_t file_index = 0;
    size_t byte_offset = 0;
    char line[256];

    while (std::fgets(line, sizeof(line), f)) {
        if (std::sscanf(line, "file_index=%u", &file_index) == 1) {
            confirmed_file_.store(file_index, std::memory_order_relaxed);
        } else if (std::sscanf(line, "byte_offset=%zu", &byte_offset) == 1) {
            confirmed_offset_.store(byte_offset, std::memory_order_relaxed);
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
    if (!blocking_send_all(fd_, tls_.get(), req, std::strlen(req))) {
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

        // The peer controls this path. Reject anything that could escape the
        // staging directory before it reaches the filesystem.
        if (!is_safe_snapshot_path(rel_path) ||
            !path_stays_within(staging_dir, rel_path)) {
            OB_LOG_ERROR("repl_client",
                         "Rejecting snapshot: unsafe file path from peer: '%s'",
                         rel_path);
            cleanup_staging(staging_dir);
            bootstrapping_.store(false, std::memory_order_release);
            return;
        }

        // Create parent directories in staging.
        std::string staged_path = staging_dir + "/" + rel_path;
        fs::create_directories(fs::path(staged_path).parent_path());

        // Receive file data into a temporary file whose name we generate, then
        // rename it into place once the content is complete and its CRC checks
        // out. Two reasons:
        //   - a partially received or corrupt file never carries the final name,
        //     so an interrupted transfer cannot be mistaken for a good segment
        //   - nothing derived from the network reaches open(); the peer-supplied
        //     name is only ever used by rename(), after validation
        {
            std::string temp_path;
            std::FILE* out = open_temp_file_private(staging_dir, temp_path);
            if (!out) {
                cleanup_staging(staging_dir);
                bootstrapping_.store(false, std::memory_order_release);
                return;
            }

            uint32_t running_crc = crc32c_init;
            size_t remaining = file_size;

            while (remaining > 0) {
                size_t chunk = std::min(remaining, static_cast<size_t>(262144));
                std::vector<uint8_t> buf(chunk);

                if (!reader_.read_exact(buf.data(), chunk)) {
                    std::fclose(out);
                    std::error_code rm_ec;
                    fs::remove(temp_path, rm_ec);
                    cleanup_staging(staging_dir);
                    bootstrapping_.store(false, std::memory_order_release);
                    return;
                }

                std::fwrite(buf.data(), 1, chunk, out);

                running_crc = crc32c_update(running_crc, buf.data(), chunk);

                remaining -= chunk;
                bytes_received += chunk;
                snapshot_bytes_received_.store(bytes_received, std::memory_order_relaxed);
            }

            std::fclose(out);

            // Verify CRC32C.
            uint32_t computed_crc = crc32c_finish(running_crc);
            if (computed_crc != file_crc) {
                OB_LOG_ERROR("repl_client",
                             "Snapshot file CRC mismatch: path=%s expected=%u got=%u",
                             rel_path, file_crc, computed_crc);
                std::error_code rm_ec;
                fs::remove(temp_path, rm_ec);
                cleanup_staging(staging_dir);
                bootstrapping_.store(false, std::memory_order_release);
                return;
            }

            // Content is complete and verified: give it its final name.
            // staged_path was validated above, and rename within one directory
            // is atomic.
            std::error_code mv_ec;
            fs::rename(temp_path, staged_path, mv_ec);
            if (mv_ec) {
                OB_LOG_ERROR("repl_client",
                             "Failed to move staged snapshot file into place: "
                             "path=%s error=%s",
                             rel_path, mv_ec.message().c_str());
                std::error_code rm_ec;
                fs::remove(temp_path, rm_ec);
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
        // Defence in depth. request_and_receive_snapshot() already validates
        // every path before writing to staging, but this is the call that
        // overwrites files inside the live data directory, so it re-checks
        // rather than trusting the manifest it was handed.
        if (!is_safe_snapshot_path(entry.path) ||
            !path_stays_within(data_dir, entry.path)) {
            OB_LOG_ERROR("repl_client",
                         "Refusing to install snapshot entry with unsafe path: '%s'",
                         entry.path.c_str());
            continue;
        }

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
    confirmed_file_.store(manifest.wal_file_index, std::memory_order_relaxed);
    confirmed_offset_.store(manifest.wal_byte_offset, std::memory_order_relaxed);
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
