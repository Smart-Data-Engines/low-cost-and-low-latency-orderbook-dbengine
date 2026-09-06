// ── MultiMasterManager implementation ─────────────────────────────────────────
//
// Epoll-based peer networking, WAL broadcast, origin-based loop prevention,
// bootstrap state management, and diagnostic commands.
//
// Requirements: 4.1–4.8, 9.1–9.6

#include "orderbook/multi_master.hpp"

#include "orderbook/crc32c.hpp"
#include "orderbook/engine.hpp"
#include "orderbook/logger.hpp"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cmath>
#include <cstring>
#include <random>
#include <sstream>
#include <stdexcept>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>

namespace ob {

// ── Helpers ───────────────────────────────────────────────────────────────────

namespace {

/// Set a socket to non-blocking mode.
bool set_nonblocking(int fd) {
    int flags = ::fcntl(fd, F_GETFL, 0);
    if (flags < 0) return false;
    return ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0;
}

/// Parse "host:port" into components.
bool parse_address(const std::string& addr, std::string& host, uint16_t& port) {
    auto colon = addr.rfind(':');
    if (colon == std::string::npos) return false;
    host = addr.substr(0, colon);
    try {
        unsigned long p = std::stoul(addr.substr(colon + 1));
        if (p > 65535) return false;
        port = static_cast<uint16_t>(p);
    } catch (...) {
        return false;
    }
    return true;
}

} // anonymous namespace

// ── HandshakeMessage implementation ───────────────────────────────────────────

void HandshakeMessage::serialize(uint8_t out[MM_HANDSHAKE_SIZE]) const {
    std::memcpy(out + 0, &node_id, sizeof(node_id));
    std::memcpy(out + 2, &protocol_version, sizeof(protocol_version));
    std::memcpy(out + 4, &compression_preference, sizeof(compression_preference));
    std::memcpy(out + 5, &wal_file_index, sizeof(wal_file_index));
    std::memcpy(out + 9, &wal_byte_offset, sizeof(wal_byte_offset));
}

bool HandshakeMessage::deserialize(const uint8_t* data, size_t len,
                                   HandshakeMessage& out) {
    if (len < MM_HANDSHAKE_SIZE) return false;
    std::memcpy(&out.node_id, data + 0, sizeof(out.node_id));
    std::memcpy(&out.protocol_version, data + 2, sizeof(out.protocol_version));
    std::memcpy(&out.compression_preference, data + 4, sizeof(out.compression_preference));
    std::memcpy(&out.wal_file_index, data + 5, sizeof(out.wal_file_index));
    std::memcpy(&out.wal_byte_offset, data + 9, sizeof(out.wal_byte_offset));
    return true;
}

std::string HandshakeMessage::to_string() const {
    std::ostringstream oss;
    oss << "HandshakeMessage{"
        << "node_id=" << node_id
        << " protocol_version=" << protocol_version
        << " compression_preference=" << static_cast<unsigned>(compression_preference)
        << " wal_file_index=" << wal_file_index
        << " wal_byte_offset=" << wal_byte_offset
        << "}";
    return oss.str();
}

bool HandshakeMessage::operator==(const HandshakeMessage& o) const {
    return node_id == o.node_id &&
           protocol_version == o.protocol_version &&
           compression_preference == o.compression_preference &&
           wal_file_index == o.wal_file_index &&
           wal_byte_offset == o.wal_byte_offset;
}

bool HandshakeMessage::operator!=(const HandshakeMessage& o) const {
    return !(*this == o);
}

// ── ReconnectBackoff implementation ───────────────────────────────────────────

uint32_t ReconnectBackoff::next_delay_ms() {
    // base_delay = min(initial_delay * 2^attempt, max_delay)
    double base = std::min(initial_delay_s * std::pow(multiplier, static_cast<double>(attempt)),
                           max_delay_s);
    double jitter_range = base * jitter_fraction;

    // Thread-local RNG for jitter.
    thread_local std::mt19937 rng{std::random_device{}()};
    std::uniform_real_distribution<double> dist(-jitter_range, jitter_range);
    double actual = base + dist(rng);

    // Clamp to non-negative.
    if (actual < 0.0) actual = 0.0;

    ++attempt;
    return static_cast<uint32_t>(actual * 1000.0);
}

// ── Frame encode/decode implementation ─────────────────────────────────────────

void encode_frame(const void* payload, size_t len, std::vector<uint8_t>& out) {
    // Append 4-byte LE length header.
    uint32_t length = static_cast<uint32_t>(len);
    const auto* len_bytes = reinterpret_cast<const uint8_t*>(&length);
    out.insert(out.end(), len_bytes, len_bytes + sizeof(uint32_t));

    // Append payload bytes.
    if (payload && len > 0) {
        const auto* pl = static_cast<const uint8_t*>(payload);
        out.insert(out.end(), pl, pl + len);
    }
}

/// Monotonic milliseconds. Used for the version-vector grace window; steady_clock because a
/// wall-clock step must not shorten or extend it.
static uint64_t now_ms() {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count());
}

int parse_frames(std::vector<uint8_t>& recv_buf,
                 std::vector<std::pair<size_t, size_t>>& frames_out) {
    frames_out.clear();

    size_t offset = 0;
    while (offset + MM_FRAME_HEADER_SIZE <= recv_buf.size()) {
        // Read 4-byte LE length from current position.
        uint32_t length = 0;
        std::memcpy(&length, recv_buf.data() + offset, sizeof(uint32_t));

        // Validate: length must not exceed MM_MAX_FRAME_PAYLOAD.
        if (length > MM_MAX_FRAME_PAYLOAD) {
            return -1;  // Protocol error: frame too large.
        }

        // Check if the full frame (header + payload) is available.
        if (offset + MM_FRAME_HEADER_SIZE + length > recv_buf.size()) {
            break;  // Incomplete frame — wait for more data.
        }

        // Record payload position: offset past the 4B header, with payload length.
        frames_out.emplace_back(offset + MM_FRAME_HEADER_SIZE, static_cast<size_t>(length));
        offset += MM_FRAME_HEADER_SIZE + length;
    }

    // Erase consumed bytes from recv_buf.
    if (offset > 0) {
        recv_buf.erase(recv_buf.begin(), recv_buf.begin() + static_cast<std::ptrdiff_t>(offset));
    }

    return 0;  // Success.
}

// ── Constructor / Destructor ──────────────────────────────────────────────────

MultiMasterManager::MultiMasterManager(MultiMasterConfig config, Engine& engine,
                                       WALWriter& wal, HybridLogicalClock& hlc)
    : config_(std::move(config))
    , engine_(engine)
    , wal_(wal)
    , hlc_(hlc)
    // The worker's only job when it finishes is to make io_loop() come back and look.
    , snapshot_builder_([this] { wake_io_loop(); }) {
    conflict_resolver_ = std::make_unique<ConflictResolver>();

    OB_LOG_DEBUG("mm", "MultiMasterManager created: node_id=%u port=%u",
                 config_.node_id, config_.replication_port);
}

MultiMasterManager::~MultiMasterManager() {
    stop();
}

// ── Start / Stop ──────────────────────────────────────────────────────────────

void MultiMasterManager::wake_io_loop() {
    const int fd = wakeup_fd_;
    if (fd < 0) return;
    const uint64_t one = 1;
    const ssize_t wr = ::write(fd, &one, sizeof(one));
    (void)wr;   // a full eventfd counter still means the loop will wake
}

void MultiMasterManager::start() {
    if (running_.load(std::memory_order_acquire)) return;

    OB_LOG_INFO("mm", "Starting MultiMasterManager: node_id=%u port=%u",
                config_.node_id, config_.replication_port);

    // Create epoll instance.
    epoll_fd_ = ::epoll_create1(0);
    if (epoll_fd_ < 0) {
        OB_LOG_ERROR("mm", "epoll_create1 failed: %s", std::strerror(errno));
        throw std::runtime_error("epoll_create1 failed");
    }

    // A descriptor whose only job is to end epoll_wait() on demand. Registered below, written by
    // stop(). Without it, shutdown waits out the 500 ms epoll timeout and stop() has to interfere
    // with descriptors the io thread is still using.
    wakeup_fd_ = ::eventfd(0, EFD_NONBLOCK);
    if (wakeup_fd_ < 0) {
        OB_LOG_ERROR("mm", "eventfd failed: %s", std::strerror(errno));
        ::close(epoll_fd_);
        epoll_fd_ = -1;
        throw std::runtime_error("eventfd failed");
    }
    {
        struct epoll_event ev{};
        ev.events  = EPOLLIN;
        ev.data.fd = wakeup_fd_;
        if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, wakeup_fd_, &ev) < 0) {
            OB_LOG_ERROR("mm", "epoll_ctl(wakeup) failed: %s", std::strerror(errno));
            ::close(wakeup_fd_);
            ::close(epoll_fd_);
            wakeup_fd_ = -1;
            epoll_fd_  = -1;
            throw std::runtime_error("epoll_ctl(wakeup) failed");
        }
    }

    // Bind listen socket.
    listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_ < 0) {
        OB_LOG_ERROR("mm", "socket() failed: %s", std::strerror(errno));
        throw std::runtime_error("socket() failed");
    }

    int opt = 1;
    ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    set_nonblocking(listen_fd_);

    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(config_.replication_port);

    if (::bind(listen_fd_, reinterpret_cast<struct sockaddr*>(&addr),
               sizeof(addr)) < 0) {
        OB_LOG_ERROR("mm", "bind() failed on port %u: %s",
                     config_.replication_port, std::strerror(errno));
        ::close(listen_fd_);
        listen_fd_ = -1;
        // Don't throw — allow tests to work without binding
        OB_LOG_WARN("mm", "Continuing without listen socket (port bind failed)");
    } else {
        if (::listen(listen_fd_, 16) < 0) {
            OB_LOG_ERROR("mm", "listen() failed: %s", std::strerror(errno));
            ::close(listen_fd_);
            listen_fd_ = -1;
        } else {
            // Add listen_fd to epoll.
            struct epoll_event ev{};
            ev.events = EPOLLIN;
            ev.data.fd = listen_fd_;
            ::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, listen_fd_, &ev);
        }
    }

    running_.store(true, std::memory_order_release);

    // Initialize PeerRegistry for etcd-based peer discovery.
    if (!config_.coordinator_config.endpoints.empty()) {
        std::string repl_addr = "127.0.0.1:" + std::to_string(config_.replication_port);
        peer_registry_ = std::make_unique<PeerRegistry>(
            config_.coordinator_config, config_.node_id, repl_addr, config_.shard_id);

        // Register this node in etcd and start watching for peers.
        peer_registry_->register_self("active");
        peer_registry_->start_watch([this](const std::vector<PeerInfo>& peers) {
            handle_topology_change(peers);
        });
    }

    // Start unified I/O thread and reconnect thread.
    // Anti-entropy: construct it here, which until now nobody did anywhere. The pointer was
    // declared, stop() checked it, and anti_entropy() handed out a reference to it regardless —
    // so the first caller of Engine::stats() on a multi-master node took the process down with
    // it (roadmap #68). The scheduler the roadmap described as working had never run.
    if (peer_registry_) {
        AntiEntropyConfig ae_config{};
        ae_config.interval_seconds = config_.anti_entropy_interval_sec;
        anti_entropy_ = std::make_unique<AntiEntropyManager>(ae_config, engine_, *peer_registry_);
        // The work itself, injected rather than reached for: this object owns the manager, so a
        // reference back would be a cycle, and a function makes a pass testable with a fake.
        anti_entropy_->set_reconciler([this] { return reconcile_with_peers(); });
        anti_entropy_->start();
        OB_LOG_INFO("mm", "Anti-entropy scheduler started: interval=%us",
                    config_.anti_entropy_interval_sec);
    } else {
        OB_LOG_INFO("mm", "Anti-entropy scheduler not started: no peer registry (no "
                          "coordinator endpoints configured)");
    }

    io_thread_ = std::thread([this] { io_loop(); });
    reconnect_thread_ = std::thread([this] { reconnect_loop(); });

    OB_LOG_INFO("mm", "MultiMasterManager started: node_id=%u port=%u",
                config_.node_id, config_.replication_port);
}

void MultiMasterManager::stop() {
    if (!running_.exchange(false, std::memory_order_acq_rel)) return;

    OB_LOG_INFO("mm", "Stopping MultiMasterManager: node_id=%u", config_.node_id);

    // Stop peer registry (deregister from etcd, stop watching).
    if (peer_registry_) {
        peer_registry_->stop_watch();
        peer_registry_->deregister_self();
    }

    // Wake the io thread, then join it, and only then close anything it might be holding.
    //
    // The order matters and the previous one was wrong: closing listen_fd_ and epoll_fd_ here was
    // described as unblocking the threads, but closing an epoll descriptor does not wake a thread
    // inside epoll_wait() on Linux — so shutdown waited out the 500 ms timeout anyway, while the io
    // loop could call epoll_wait() on a descriptor number the kernel had already reassigned.
    // ThreadSanitizer reported it as a data race on file descriptor 4 between stop() and io_loop().
    wake_io_loop();

    // Join threads. Both check running_, and the io thread is now woken at once rather than in up
    // to half a second.
    if (io_thread_.joinable()) io_thread_.join();
    if (reconnect_thread_.joinable()) reconnect_thread_.join();

    // And the snapshot worker, before anything closes wakeup_fd_ — the worker writes to it when it
    // finishes, so closing first would hand it a descriptor number the kernel has reassigned. That
    // is pitfall 41 and #80, in a path that did not exist when either was written. A snapshot in
    // flight is waited for, not cancelled: abandoning a flush half-way is worse than waiting for
    // work whose result is about to be discarded.
    snapshot_builder_.shutdown();
    snapshot_prepare_ = MMSnapshotPrepare{};

    // Only now that the io thread is gone is it safe to touch the transfer state it owns. An
    // in-flight snapshot holds an open descriptor and a staging directory; a shutdown mid-transfer
    // must leave neither, and must not leave the node believing it is still bootstrapping.
    if (snapshot_send_.active) finish_snapshot_send(false, "shutting_down");
    if (snapshot_recv_.active) abort_bootstrap("shutting_down");

    // Nobody is reading these any more.
    if (listen_fd_ >= 0) {
        ::close(listen_fd_);
        listen_fd_ = -1;
    }
    if (wakeup_fd_ >= 0) {
        ::close(wakeup_fd_);
        wakeup_fd_ = -1;
    }
    if (epoll_fd_ >= 0) {
        ::close(epoll_fd_);
        epoll_fd_ = -1;
    }

    // Disconnect all peers.
    {
        std::lock_guard<std::mutex> lock(mtx_);
        for (auto& [nid, peer] : peers_) {
            if (peer.fd >= 0) {
                release_tls(peer);
                ::close(peer.fd);
                peer.fd = -1;
                peer.connected = false;
            }
        }
        peers_.clear();
    }

    // Stop anti-entropy if running.
    if (anti_entropy_) {
        anti_entropy_->stop();
    }

    OB_LOG_INFO("mm", "MultiMasterManager stopped: node_id=%u", config_.node_id);
}

// ── Broadcast ─────────────────────────────────────────────────────────────────

void MultiMasterManager::broadcast_local(const WALRecordV2& hdr,
                                         const void* payload,
                                         size_t payload_len) {
    std::lock_guard<std::mutex> lock(mtx_);

    size_t peer_count = 0;
    for (auto& [nid, peer] : peers_) {
        if (peer.connected) {
            send_to_peer(peer, hdr, payload, payload_len);
            ++peer_count;
        }
    }

    OB_LOG_DEBUG("mm", "broadcast_local: seq=%lu to %zu peers (send_buf sizes: ",
                 static_cast<unsigned long>(hdr.sequence_number), peer_count);
    for (auto& [nid, peer] : peers_) {
        if (peer.connected) {
            OB_LOG_DEBUG("mm", "  peer %u: send_buf=%zu bytes", nid, peer.send_buf.size());
        }
    }
}

// ── Handle remote record ──────────────────────────────────────────────────────

bool MultiMasterManager::handle_remote_record(uint16_t /*peer_node_id*/,
                                              const WALRecordV2& hdr,
                                              const void* payload,
                                              size_t payload_len) {
    // Extract origin from the WAL record header.
    uint16_t origin = hdr.origin_node_id;

    // Nothing may be applied while a snapshot is being installed, and — just as important —
    // nothing may be *recorded as seen*. load_snapshot() discards the in-memory buffers, so a
    // delta applied now can vanish while its number stays in the tracker: a frontier claiming a
    // row that does not exist, which no later catch-up will ever fill. Left unmarked, the record
    // comes back on the next vector exchange, because our frontier will not cover it.
    if (bootstrapping_.load(std::memory_order_acquire)) {
        OB_LOG_DEBUG("mm",
                     "Dropping record from origin=%u seq=%lu: bootstrapping, so it would be "
                     "discarded by the install and remembered anyway",
                     origin, static_cast<unsigned long>(hdr.sequence_number));
        engine_.registry().increment_counter("ob_mm_records_dropped_bootstrapping_total");
        return false;
    }

    // Only process DELTA records — skip GAP, ROTATE, EPOCH, etc.
    if (hdr.record_type != WAL_RECORD_DELTA) {
        OB_LOG_DEBUG("mm", "Skipping non-DELTA record type=%u from origin=%u",
                     hdr.record_type, origin);
        return false;
    }

    // Empty payload (payload_len == 0) is valid — means zero levels.
    // Accept the record but nothing to apply.
    if (payload_len == 0 || !payload) {
        OB_LOG_DEBUG("mm", "handle_remote_record: accepted empty DELTA from origin=%u seq=%lu",
                     origin, static_cast<unsigned long>(hdr.sequence_number));
        return true;
    }

    // Validate payload size: must contain at least a DeltaUpdate header.
    if (payload_len < sizeof(DeltaUpdate)) {
        OB_LOG_WARN("mm", "handle_remote_record: payload too short (%zu < %zu)",
                    payload_len, sizeof(DeltaUpdate));
        return false;
    }

    // Deserialize DeltaUpdate from payload.
    DeltaUpdate delta{};
    std::memcpy(&delta, payload, sizeof(DeltaUpdate));

    // Validate levels fit in payload.
    const size_t levels_bytes = delta.n_levels * sizeof(Level);
    if (sizeof(DeltaUpdate) + levels_bytes > payload_len) {
        OB_LOG_WARN("mm", "handle_remote_record: payload_len mismatch (need %zu, have %zu)",
                    sizeof(DeltaUpdate) + levels_bytes, payload_len);
        return false;
    }

    const auto* levels = reinterpret_cast<const Level*>(
        static_cast<const uint8_t*>(payload) + sizeof(DeltaUpdate));

    // Extract HLC timestamp from WAL header.
    HLCTimestamp remote_hlc = HLCTimestamp::deserialize(hdr.hlc_data);

    OB_LOG_DEBUG("mm", "handle_remote_record: origin=%u seq=%lu hlc={%lu,%u,%u}",
                 origin, static_cast<unsigned long>(hdr.sequence_number),
                 static_cast<unsigned long>(remote_hlc.physical_ns),
                 remote_hlc.logical, remote_hlc.node_id);

    // Apply to engine with conflict resolution.
    // Loop prevention is handled by apply_remote_delta (skips WAL write for
    // records from self) and by broadcast_local (only broadcasts local writes).
    engine_.apply_remote_delta(delta, levels, origin, remote_hlc);

    OB_LOG_DEBUG("mm", "handle_remote_record: APPLIED origin=%u seq=%lu symbol=%.*s n_levels=%u",
                 origin, static_cast<unsigned long>(hdr.sequence_number),
                 static_cast<int>(sizeof(delta.symbol)), delta.symbol,
                 delta.n_levels);

    return true;
}

// ── Peer state queries ────────────────────────────────────────────────────────

std::vector<PeerConnection> MultiMasterManager::peer_states() const {
    std::lock_guard<std::mutex> lock(mtx_);
    std::vector<PeerConnection> result;
    result.reserve(peers_.size());
    for (const auto& [nid, peer] : peers_) {
        result.push_back(peer);
    }
    return result;
}

size_t MultiMasterManager::connected_peer_count() const {
    std::lock_guard<std::mutex> lock(mtx_);
    size_t count = 0;
    for (const auto& [nid, peer] : peers_) {
        if (peer.connected) ++count;
    }
    return count;
}

// ── Bootstrap ─────────────────────────────────────────────────────────────────

void MultiMasterManager::start_bootstrap() {
    bootstrapping_.store(true, std::memory_order_release);
    OB_LOG_INFO("mm",
                "Bootstrap started for node %u — writes are refused with ERR BOOTSTRAPPING until "
                "finish_bootstrap() is called",
                config_.node_id);
}

void MultiMasterManager::finish_bootstrap(bool succeeded) {
    const bool was = bootstrapping_.exchange(false, std::memory_order_acq_rel);
    if (!was) {
        OB_LOG_DEBUG("mm", "finish_bootstrap() with no bootstrap in progress on node %u",
                     config_.node_id);
        return;
    }
    if (succeeded) {
        OB_LOG_INFO("mm", "Bootstrap finished for node %u — accepting writes", config_.node_id);
    } else {
        OB_LOG_ERROR("mm",
                     "Bootstrap FAILED for node %u — accepting writes anyway rather than refusing "
                     "them for ever. This node may be missing data its peers hold; reads can be "
                     "incomplete until anti-entropy catches up",
                     config_.node_id);
    }
}

// ── Diagnostic commands ───────────────────────────────────────────────────────

std::string MultiMasterManager::handle_mm_peers_command() const {
    std::lock_guard<std::mutex> lock(mtx_);

    std::ostringstream oss;
    oss << "node_id\taddress\tstatus\thlc_timestamp\tlag_bytes\n";

    // An accepted connection lands in peers_ under a temporary key with node_id 0 and no address,
    // and stays there until its handshake says who it is. Those are not peers, and listing them made
    // MM_PEERS answer "0, (no address), disconnected" — which an operator reads as a peer that has
    // fallen over, and which anything comparing the row count against the cluster size reads as one
    // node too many. Both readings are wrong: it is an inbound connection mid-handshake.
    //
    // Skipped rather than reported differently, because these rows are parsed — by the integration
    // harness among others — so a trailing summary line would be counted as a peer by anything
    // splitting on newlines. The count goes to the log, so nothing is hidden by being dropped.
    size_t unidentified = 0;
    for (const auto& [nid, peer] : peers_) {
        if (peer.node_id == 0) {
            ++unidentified;
            continue;
        }
        oss << peer.node_id << '\t'
            << peer.address << '\t'
            << (peer.connected ? "connected" : "disconnected") << '\t'
            << peer.last_hlc.to_string() << '\t'
            << peer.send_buf.size() << '\n';
    }

    if (unidentified > 0) {
        OB_LOG_DEBUG("mm",
                     "MM_PEERS: %zu inbound connection(s) have not completed a handshake and are "
                     "not listed as peers",
                     unidentified);
    }

    return oss.str();
}

std::string MultiMasterManager::handle_mm_conflicts_command(size_t limit) const {
    auto entries = conflict_resolver_->get_log(limit);

    std::ostringstream oss;
    oss << "symbol\texchange\tside\tprice\tlocal_hlc\tremote_hlc\tlocal_origin\tremote_origin\tresult\n";

    for (const auto& entry : entries) {
        oss << entry.key.symbol << '\t'
            << entry.key.exchange << '\t'
            << static_cast<int>(entry.key.side) << '\t'
            << entry.key.price << '\t'
            << entry.local_hlc.to_string() << '\t'
            << entry.remote_hlc.to_string() << '\t'
            << entry.local_origin << '\t'
            << entry.remote_origin << '\t'
            << (entry.result == ConflictEntry::LOCAL_WINS ? "local_wins" : "remote_wins")
            << '\n';
    }

    return oss.str();
}

// ── Networking internals ──────────────────────────────────────────────────────

void MultiMasterManager::io_loop() {
    OB_LOG_DEBUG("mm", "io_loop started");

    while (running_.load(std::memory_order_acquire)) {
        if (epoll_fd_ < 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            continue;
        }

        struct epoll_event events[64];
        int nfds = ::epoll_wait(epoll_fd_, events, 64, 500 /*ms timeout*/);
        if (nfds < 0) {
            if (errno == EINTR) continue;
            break;
        }

        // Before dispatching anything: has a snapshot worker finished? Checked here rather than in
        // the wakeup_fd_ branch on purpose, so the plain 500 ms timeout picks a result up too. The
        // notification is what makes it prompt; this is what makes a lost notification cost half a
        // second instead of a stuck bootstrap.
        poll_snapshot_preparation();

        for (int i = 0; i < nfds; ++i) {
            int ev_fd = events[i].data.fd;
            uint32_t ev_events = events[i].events;

            if (ev_fd == wakeup_fd_) {
                // Drain it and re-check running_ at the top of the loop. Nothing else to do: the
                // event carries no information beyond "look again".
                uint64_t drained = 0;
                const ssize_t rd = ::read(wakeup_fd_, &drained, sizeof(drained));
                (void)rd;
                continue;
            }

            if (ev_fd == listen_fd_) {
                // ── Accept new connections (level-triggered EPOLLIN) ──────────
                while (true) {
                    struct sockaddr_in peer_addr{};
                    socklen_t addr_len = sizeof(peer_addr);
                    int client_fd = ::accept(listen_fd_,
                                             reinterpret_cast<struct sockaddr*>(&peer_addr),
                                             &addr_len);
                    if (client_fd < 0) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                        break;
                    }

                    set_nonblocking(client_fd);
                    int tcp_nodelay = 1;
                    ::setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY,
                                 &tcp_nodelay, sizeof(tcp_nodelay));

                    // Add to epoll with edge-triggered EPOLLIN.
                    struct epoll_event ev{};
                    ev.events = EPOLLIN | EPOLLET;
                    ev.data.fd = client_fd;
                    ::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, client_fd, &ev);

                    OB_LOG_INFO("mm", "Accepted peer connection fd=%d", client_fd);

                    // Create a temporary PeerConnection for this accepted fd.
                    // The peer will identify itself via handshake.
                    // Use node_id=0 as placeholder until handshake completes.
                    std::lock_guard<std::mutex> lock(mtx_);

                    // Find an unused temporary node_id slot for accepted connections.
                    // We use fd as a temporary key in a separate lookup, but store
                    // in peers_ with a placeholder. After handshake, we'll re-key.
                    // For simplicity, use a high node_id range (fd + 10000) as temp key.
                    uint16_t temp_id = static_cast<uint16_t>(client_fd);

                    PeerConnection conn{};
                    conn.node_id = 0;  // unknown until handshake
                    conn.fd = client_fd;
                    conn.conn_id = next_conn_id_++;
                    conn.connected = true;
                    conn.handshake_done = false;
                    conn.peer_proved = false;
                    conn.auth_nonce.clear();
                    conn.we_accepted = true;
                    conn.compress = config_.compress;

                    // Store with temp key — will be re-keyed after handshake.
                    peers_[temp_id] = std::move(conn);

                    // TLS before a byte is queued. What follows is queued rather than written: the
                    // drain returns early while the handshake runs, so these frames go out with the
                    // first flush afterwards - the same shape as the client port's banner.
                    if (!attach_tls(peers_[temp_id])) {
                        ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, client_fd, nullptr);
                        ::close(client_fd);
                        peers_.erase(temp_id);
                        continue;
                    }

                    // With a cluster secret, challenge first and let the *handshake* be the
                    // acceptance; without one, handshake straight away as before.
                    if (!config_.cluster_secret.empty()) {
                        send_auth_challenge(peers_[temp_id]);
                    } else {
                        send_handshake(peers_[temp_id]);
                    }
                }
            } else {
                // ── Peer fd event ────────────────────────────────────────────
                // Find the peer by fd.
                std::lock_guard<std::mutex> lock(mtx_);

                PeerConnection* peer_ptr = nullptr;
                uint16_t peer_key = 0;
                for (auto& [nid, p] : peers_) {
                    if (p.fd == ev_fd) {
                        peer_ptr = &p;
                        peer_key = nid;
                        break;
                    }
                }

                if (!peer_ptr) {
                    // Unknown fd — remove from epoll.
                    ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, ev_fd, nullptr);
                    ::close(ev_fd);
                    continue;
                }

                // A handshake in progress consumes this event and nothing else. Not one frame may
                // be parsed before it finishes: a frame arriving earlier would come from a
                // transport that has not proved who it is, and the cluster-secret gate is a
                // different mechanism that knows nothing about TLS.
                if (peer_ptr->tls != nullptr && peer_ptr->tls->handshaking()) {
                    if (!advance_tls_handshake(*peer_ptr)) {
                        ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, ev_fd, nullptr);
                        release_tls(*peer_ptr);
                        ::close(ev_fd);
                        peer_ptr->fd        = -1;
                        peer_ptr->connected = false;
                        peer_ptr->recv_buf.clear();
                        peer_ptr->send_buf.clear();
                        on_peer_disconnected(*peer_ptr);
                        if (peer_ptr->node_id != 0) {
                            const uint32_t delay_ms = peer_ptr->backoff.next_delay_ms();
                            peer_ptr->next_reconnect_time =
                                std::chrono::steady_clock::now() +
                                std::chrono::milliseconds(delay_ms);
                        }
                        publish_peer_gauges();
                    }
                    continue;
                }

                // Handle EPOLLIN — recv data.
                if (ev_events & EPOLLIN) {
                    // Edge-triggered: read in a loop until EAGAIN.
                    bool disconnected = false;
                    while (true) {
                        uint8_t buf[8192];
                        // Read until the *TLS layer* says it has nothing, not until the socket does.
                        // OpenSSL reads a whole record - up to 16 kB - decrypts it into its own
                        // buffer and hands back what was asked for, so on an edge-triggered loop a
                        // socket-level EAGAIN can arrive with decrypted bytes still pending and no
                        // further event coming. `Again` here is `WANT_*`, which cannot.
                        ssize_t n;
                        if (peer_ptr->tls != nullptr) {
                            size_t got = 0;
                            switch (peer_ptr->tls->read(buf, sizeof(buf), got)) {
                            case TlsChannel::Io::Data:   n = static_cast<ssize_t>(got); break;
                            case TlsChannel::Io::Closed: n = 0; break;
                            case TlsChannel::Io::Again:  n = -1; errno = EAGAIN; break;
                            case TlsChannel::Io::Error:  n = -1; errno = EIO; break;
                            }
                        } else {
                            n = ::recv(ev_fd, buf, sizeof(buf), 0);
                        }
                        if (n > 0) {
                            peer_ptr->recv_buf.insert(peer_ptr->recv_buf.end(),
                                                     buf, buf + n);
                        } else if (n == 0) {
                            // Peer closed connection.
                            OB_LOG_INFO("mm", "Peer fd=%d closed connection", ev_fd);
                            disconnected = true;
                            break;
                        } else {
                            int err = errno;
                            if (err == EAGAIN || err == EWOULDBLOCK) {
                                break;  // No more data available.
                            }
                            // Error — disconnect.
                            OB_LOG_WARN("mm", "Peer fd=%d recv error: %s",
                                        ev_fd, std::strerror(err));
                            disconnected = true;
                            break;
                        }
                    }

                    if (disconnected) {
                        ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, ev_fd, nullptr);
                        release_tls(*peer_ptr);
                        ::close(ev_fd);
                        peer_ptr->fd = -1;
                        peer_ptr->connected = false;
                        peer_ptr->recv_buf.clear();
                        peer_ptr->send_buf.clear();
                        on_peer_disconnected(*peer_ptr);

                        uint16_t node_to_reconnect = peer_ptr->node_id;
                        if (node_to_reconnect != 0) {
                            // Schedule reconnect (outside lock would be ideal,
                            // but schedule_reconnect acquires its own lock — 
                            // we handle it inline here).
                            uint32_t delay_ms = peer_ptr->backoff.next_delay_ms();
                            peer_ptr->next_reconnect_time =
                                std::chrono::steady_clock::now() +
                                std::chrono::milliseconds(delay_ms);
                            OB_LOG_INFO("mm", "Scheduled reconnect for peer %u (delay %u ms)",
                                        node_to_reconnect, delay_ms);
                        }
                        continue;
                    }

                    // Process received data — parse frames.
                    process_recv_buf(*peer_ptr);

                    // The fourth of the four combinations, and the one whose absence looks like a
                    // wedged peer. A TLS write can leave OpenSSL wanting to *read*, in which case
                    // the drain deliberately did not arm EPOLLOUT because the socket is already
                    // writable - so a readable event is the only way back in.
                    if (peer_ptr->tls != nullptr && peer_ptr->connected) {
                        if (peer_ptr->tls->io_want() == IoWant::Write) arm_epollout(*peer_ptr);
                        if (!peer_ptr->send_buf.empty()) try_drain_send_buf(*peer_ptr);
                    }

                    // After handshake, re-key the peer if needed. The whole record moves, so
                    // conn_id moves with it: the connection accepted under a temporary key keeps
                    // its identity once the handshake names the node behind it.
                    if (peer_ptr->handshake_done && peer_ptr->node_id != 0 &&
                        peer_key != peer_ptr->node_id) {
                        // Move peer to correct key.
                        uint16_t real_id = peer_ptr->node_id;
                        PeerConnection moved = std::move(*peer_ptr);
                        peers_.erase(peer_key);
                        peers_[real_id] = std::move(moved);
                    }
                }

                // Handle EPOLLOUT — drain send buffer, then push more of any snapshot.
                if ((ev_events & EPOLLOUT) && peer_ptr && peer_ptr->connected) {
                    try_drain_send_buf(*peer_ptr);
                    // This is what keeps a snapshot from being enqueued all at once: chunks are
                    // added only as the socket makes room, so live deltas queued between them go
                    // out promptly and the buffer never reaches the size that drops the peer.
                    if (peer_ptr->connected) advance_snapshot_send(*peer_ptr);
                }

                // Handle errors/hangup.
                if (ev_events & (EPOLLERR | EPOLLHUP)) {
                    if (peer_ptr && peer_ptr->connected) {
                        OB_LOG_WARN("mm", "Peer fd=%d EPOLLERR/HUP — disconnecting", ev_fd);
                        ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, ev_fd, nullptr);
                        release_tls(*peer_ptr);
                        ::close(ev_fd);
                        peer_ptr->fd = -1;
                        peer_ptr->connected = false;
                        peer_ptr->recv_buf.clear();
                        peer_ptr->send_buf.clear();
                        on_peer_disconnected(*peer_ptr);

                        if (peer_ptr->node_id != 0) {
                            uint32_t delay_ms = peer_ptr->backoff.next_delay_ms();
                            peer_ptr->next_reconnect_time =
                                std::chrono::steady_clock::now() +
                                std::chrono::milliseconds(delay_ms);
                        }
                    }
                }
            }
        }

        // epoll_wait above returns at least every 500 ms, so this runs regularly without a
        // timer of its own: a peer that completed the handshake and never sent a version
        // vector (protocol 1, or a version it could not state) must still get its catch-up.
        {
            std::lock_guard<std::mutex> lock(mtx_);
            start_overdue_catchups();
        }
    }

    OB_LOG_DEBUG("mm", "io_loop exited");
}

void MultiMasterManager::connect_to_peer(const PeerInfo& peer) {
    if (peer.node_id == config_.node_id) return;  // don't connect to self

    std::lock_guard<std::mutex> lock(mtx_);

    // Check if already connected.
    auto it = peers_.find(peer.node_id);
    if (it != peers_.end() && it->second.connected) {
        OB_LOG_DEBUG("mm", "Already connected to peer %u", peer.node_id);
        return;
    }

    std::string host;
    uint16_t port = 0;
    if (!parse_address(peer.address, host, port)) {
        OB_LOG_WARN("mm", "Invalid peer address: %s", peer.address.c_str());
        return;
    }

    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        OB_LOG_ERROR("mm", "socket() failed for peer %u: %s",
                     peer.node_id, std::strerror(errno));
        return;
    }

    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (::inet_pton(AF_INET, host.c_str(), &addr.sin_addr) <= 0) {
        // Try hostname resolution.
        struct addrinfo hints{}, *res = nullptr;
        hints.ai_family = AF_INET;
        hints.ai_socktype = SOCK_STREAM;
        if (::getaddrinfo(host.c_str(), nullptr, &hints, &res) == 0 && res) {
            addr.sin_addr = reinterpret_cast<struct sockaddr_in*>(res->ai_addr)->sin_addr;
            ::freeaddrinfo(res);
        } else {
            OB_LOG_WARN("mm", "Cannot resolve peer %u address: %s",
                        peer.node_id, host.c_str());
            ::close(fd);
            return;
        }
    }

    if (::connect(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
        OB_LOG_WARN("mm", "connect() to peer %u at %s failed: %s",
                    peer.node_id, peer.address.c_str(), std::strerror(errno));
        ::close(fd);

        // Store as disconnected peer.
        PeerConnection conn{};
        conn.node_id = peer.node_id;
        conn.address = peer.address;
        conn.fd = -1;
        conn.connected = false;
        conn.compress = config_.compress;
        peers_[peer.node_id] = std::move(conn);
        return;
    }

    set_nonblocking(fd);
    int tcp_nodelay = 1;
    ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &tcp_nodelay, sizeof(tcp_nodelay));

    PeerConnection conn{};
    conn.node_id = peer.node_id;
    conn.address = peer.address;
    conn.fd = fd;
    conn.conn_id = next_conn_id_++;
    conn.connected = true;
    conn.compress = config_.compress;
    OB_LOG_INFO("mm", "Connected to peer %u at %s (fd=%d, connection %llu)",
                conn.node_id, conn.address.c_str(), fd,
                static_cast<unsigned long long>(conn.conn_id));
    peers_[peer.node_id] = std::move(conn);

    // Add peer fd to epoll (edge-triggered EPOLLIN).
    if (epoll_fd_ >= 0) {
        struct epoll_event ev{};
        ev.events = EPOLLIN | EPOLLET;
        ev.data.fd = fd;
        ::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &ev);
    }

    OB_LOG_INFO("mm", "Connected to peer %u at %s", peer.node_id,
                peer.address.c_str());

    // TLS before anything is queued. The handshake itself is *not* driven here: this function runs
    // on the topology-change path and the reconnect thread, and io_loop() owns the epoll set - so
    // arming here and stepping there is what keeps one `SSL` state machine to one thread.
    if (!attach_tls(peers_[peer.node_id])) {
        if (epoll_fd_ >= 0) ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
        ::close(fd);
        peers_[peer.node_id].fd        = -1;
        peers_[peer.node_id].connected = false;
        return;
    }

    // Send handshake to initiate protocol exchange - or, with a cluster secret, a challenge, and
    // the handshake follows once this peer has proved itself.
    if (!config_.cluster_secret.empty()) {
        send_auth_challenge(peers_[peer.node_id]);
    } else {
        send_handshake(peers_[peer.node_id]);
    }

    publish_peer_gauges();
}

void MultiMasterManager::disconnect_peer(uint16_t node_id) {
    std::lock_guard<std::mutex> lock(mtx_);

    auto it = peers_.find(node_id);
    if (it == peers_.end()) return;

    if (it->second.fd >= 0) {
        release_tls(it->second);
        ::close(it->second.fd);
    }
    peers_.erase(it);

    OB_LOG_INFO("mm", "Peer %u disconnected", node_id);

    publish_peer_gauges();
}

void MultiMasterManager::handle_peer_data(uint16_t node_id) {
    // In full implementation, this would read WAL records from the peer's fd
    // and call handle_remote_record(). Placeholder for task 12 integration.
    OB_LOG_DEBUG("mm", "handle_peer_data: node_id=%u", node_id);
}

void MultiMasterManager::send_to_peer(PeerConnection& peer,
                                      const WALRecordV2& hdr,
                                      const void* payload,
                                      size_t payload_len) {
    if (!peer.connected || peer.fd < 0) return;

    // Build a contiguous buffer: WALRecordV2 header (38B) + payload.
    // This becomes the Frame payload (length = 38 + payload_len).
    std::vector<uint8_t> frame_payload;
    frame_payload.reserve(MM_WALRECORD_V2_SIZE + payload_len);

    const auto* hdr_bytes = reinterpret_cast<const uint8_t*>(&hdr);
    frame_payload.insert(frame_payload.end(), hdr_bytes,
                         hdr_bytes + MM_WALRECORD_V2_SIZE);
    if (payload && payload_len > 0) {
        const auto* pl_bytes = static_cast<const uint8_t*>(payload);
        frame_payload.insert(frame_payload.end(), pl_bytes,
                             pl_bytes + payload_len);
    }

    // Use Frame encoding: 4B LE length + frame_payload.
    enqueue_frame(peer, frame_payload.data(), frame_payload.size());
}

// ── Frame-based send methods (task 5.1) ───────────────────────────────────────

void MultiMasterManager::enqueue_frame(PeerConnection& peer,
                                       const void* payload, size_t len) {
    if (!peer.connected || peer.fd < 0) return;

    // Encode frame (4B LE length + payload) into peer.send_buf.
    encode_frame(payload, len, peer.send_buf);

    // Attempt to drain immediately.
    try_drain_send_buf(peer);

    // And refuse to hold an unbounded backlog for a peer that is not reading. Before this check
    // existed, one unreachable peer grew the writer at about 113 MB/s at the rate this engine
    // advertises, with nothing to stop it: check_backpressure() only ever ran inside the catch-up
    // loop.
    drop_peer_if_send_buf_too_large(peer);
}

bool MultiMasterManager::drop_peer_if_send_buf_too_large(PeerConnection& peer) {
    engine_.registry().set_gauge("ob_mm_peer_send_buf_bytes",
                                 static_cast<int64_t>(peer.send_buf.size()));
    if (peer.send_buf.size() <= config_.max_peer_send_buf_bytes) return false;

    OB_LOG_WARN("mm",
                "Peer %u is not draining: send_buf=%zu > %zu — dropping the connection so it "
                "reconnects and catches up",
                peer.node_id, peer.send_buf.size(), config_.max_peer_send_buf_bytes);
    engine_.registry().increment_counter("ob_mm_peer_dropped_slow_total");

    // Deliberately not send_buf.clear() while keeping the socket: after a partial write the buffer
    // can start mid-frame, and abandoning half a frame desynchronises the peer's parser.
    if (peer.fd >= 0) {
        release_tls(peer);
        ::close(peer.fd);
        peer.fd = -1;
    }
    peer.connected      = false;
    peer.handshake_done = false;
    peer.peer_proved = false;
    peer.auth_nonce.clear();
    peer.we_accepted = false;
    peer.catching_up    = false;
    peer.needs_snapshot = true;
    peer.send_buf.clear();          // safe now: nobody is reading this socket any more
    peer.recv_buf.clear();
    on_peer_disconnected(peer);
    return true;
}

bool MultiMasterManager::attach_tls(PeerConnection& peer) {
    if (peer.we_accepted) {
        if (config_.tls_server == nullptr) return true;
    } else if (config_.tls_client == nullptr) {
        return true;
    }

    const TlsContext& ctx = peer.we_accepted ? *config_.tls_server : *config_.tls_client;
    try {
        peer.tls = ctx.open_channel(peer.fd, /*server_side=*/peer.we_accepted,
                                    peer.address.empty() ? std::string("(accepted)") : peer.address);
    } catch (const std::exception& e) {
        OB_LOG_ERROR("mm", "cannot start a TLS handshake on fd=%d (%s): %s", peer.fd,
                     peer.address.c_str(), e.what());
        return false;
    }

    if (!peer.we_accepted) {
        // The name check, and it is not what SSL_VERIFY_PEER does: without it any certificate this
        // CA signed authenticates any peer, so another node's certificate would be accepted here
        // and the relay in SECURITY.md would still work (pitfall 124). The accepting end has no
        // name to expect and uses --tls-peer-names instead (requirements §6.3).
        std::string host;
        uint16_t    port = 0;
        if (!parse_address(peer.address, host, port)) {
            OB_LOG_ERROR("mm", "cannot require a certificate name for peer %u: unparsable address "
                               "'%s'", peer.node_id, peer.address.c_str());
            peer.tls.reset();
            return false;
        }
        if (!tls_expect_host(peer.tls->raw(), host)) {
            OB_LOG_ERROR("mm", "cannot require peer %u's certificate to cover %s", peer.node_id,
                         host.c_str());
            peer.tls.reset();
            return false;
        }
    }

    // Both events armed: the dialling end has a ClientHello to send and the accepting end has one
    // to read, and the loop that steps the handshake needs to be woken either way.
    if (epoll_fd_ >= 0 && peer.fd >= 0) {
        struct epoll_event ev{};
        ev.events  = EPOLLIN | EPOLLOUT | EPOLLET;
        ev.data.fd = peer.fd;
        ::epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, peer.fd, &ev);
    }
    OB_LOG_DEBUG("mm", "tls handshake started on fd=%d (%s, %s)", peer.fd,
                 peer.address.c_str(), peer.we_accepted ? "accepted" : "dialled");
    return true;
}

bool MultiMasterManager::advance_tls_handshake(PeerConnection& peer) {
    if (peer.tls == nullptr || !peer.tls->handshaking()) return true;

    if (!peer.tls->continue_handshake()) return false;
    if (peer.tls->handshaking()) {
        if (peer.tls->io_want() == IoWant::Write) arm_epollout(peer);
        else                                      disarm_epollout(peer);
        return true;
    }

    peer.identity = peer.tls->identity();
    OB_LOG_INFO("mm", "peer at %s (fd=%d) authenticated by certificate: %s",
                peer.address.c_str(), peer.fd, peer.identity.c_str());
    publish_peer_gauges();
    // Whatever was queued before the handshake - the challenge, or the handshake frame on a mesh
    // without a cluster secret - goes out now. This is the flush the connect paths did not perform.
    return try_drain_send_buf(peer);
}

void MultiMasterManager::release_tls(PeerConnection& peer) {
    if (peer.tls == nullptr) return;
    // close_notify before the descriptor goes, so the peer's read reports a clean close rather than
    // a truncation it cannot tell from a network fault.
    peer.tls->shutdown();
    peer.tls.reset();
    peer.identity.clear();
}

void MultiMasterManager::publish_peer_gauges() {
    size_t connected = 0;
    size_t verified  = 0;
    for (const auto& [nid, p] : peers_) {
        // The same denominator as MM_PEERS: a connection accepted but not yet named by its
        // handshake is not a peer (#84). Counting it would make the gauge disagree with the view
        // an operator reads beside it, and would count a connection that may still be refused.
        if (p.node_id == 0 || !p.connected) continue;
        ++connected;
        if (p.tls != nullptr && !p.tls->handshaking()) ++verified;
    }
    engine_.registry().set_gauge("ob_mm_peers_connected",     static_cast<int64_t>(connected));
    engine_.registry().set_gauge("ob_mm_peers_tls_verified",  static_cast<int64_t>(verified));
}

bool MultiMasterManager::try_drain_send_buf(PeerConnection& peer) {
    return peer.tls == nullptr ? try_drain_send_buf_plain(peer) : try_drain_send_buf_tls(peer);
}

bool MultiMasterManager::try_drain_send_buf_tls(PeerConnection& peer) {
    // The handshake has one owner, advance_tls_handshake(). Writing here while it runs would work -
    // OpenSSL lets a write drive a handshake - and that is the accident series C shipped a comment
    // about (pitfall 130): two functions advancing one state machine.
    if (peer.tls->handshaking()) return true;
    if (!peer.connected || peer.fd < 0) return false;

    while (!peer.send_buf.empty()) {
        size_t sent = 0;
        const TlsChannel::Io r = peer.tls->write(peer.send_buf.data(), peer.send_buf.size(), sent);
        if (r == TlsChannel::Io::Data) {
            // Moves the pending bytes to a lower address in the same allocation, so a retry after
            // WANT_WRITE presents a pointer OpenSSL has not seen - refused with `bad write retry`
            // without SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER, which every context here sets.
            peer.send_buf.erase(peer.send_buf.begin(),
                                peer.send_buf.begin() + static_cast<ptrdiff_t>(sent));
            continue;
        }
        if (r == TlsChannel::Io::Again) {
            // WANT_WRITE arms EPOLLOUT; WANT_READ must not, because the socket is writable and
            // OpenSSL is waiting to read, so arming it spins (pitfall 5). The EPOLLIN branch of
            // io_loop() retries the drain for that case - the fourth of the four combinations.
            if (peer.tls->io_want() == IoWant::Write) arm_epollout(peer);
            return true;
        }
        // Closed or Error, both already logged by the channel.
        OB_LOG_WARN("mm", "Peer %u: TLS write failed - disconnecting", peer.node_id);
        peer.connected = false;
        if (peer.fd >= 0) {
            release_tls(peer);
            ::close(peer.fd);
            peer.fd = -1;
        }
        peer.send_buf.clear();
        return false;
    }

    disarm_epollout(peer);
    return true;
}

bool MultiMasterManager::try_drain_send_buf_plain(PeerConnection& peer) {
    if (peer.send_buf.empty()) {
        disarm_epollout(peer);
        return true;
    }

    if (!peer.connected || peer.fd < 0) return false;

    while (!peer.send_buf.empty()) {
        ssize_t sent = ::send(peer.fd, peer.send_buf.data(),
                              peer.send_buf.size(), MSG_NOSIGNAL);

        if (sent > 0) {
            peer.send_buf.erase(peer.send_buf.begin(),
                                peer.send_buf.begin() + sent);
            if (peer.send_buf.empty()) {
                // Fully drained — disarm EPOLLOUT.
                disarm_epollout(peer);
                return true;
            }
            // Partial write — loop to try sending more.
            continue;
        }

        if (sent == 0) {
            // Neither progress nor an error. A stream socket should not answer a non-empty send()
            // this way, and before this branch existed the loop simply spun on it — a wedged io
            // thread with no log line to say why. Treat it as "come back later", which is the
            // only interpretation that cannot lose the buffer.
            OB_LOG_WARN("mm", "Peer %u: send() returned 0 for %zu pending bytes — waiting for "
                              "EPOLLOUT", peer.node_id, peer.send_buf.size());
            arm_epollout(peer);
            return true;
        }

        if (sent < 0) {
            int err = errno;
            if (err == EAGAIN || err == EWOULDBLOCK) {
                // Socket buffer full — arm EPOLLOUT and wait.
                arm_epollout(peer);
                return true;
            }

            if (err == EPIPE || err == ECONNRESET) {
                // Connection lost — disconnect and schedule reconnect.
                OB_LOG_WARN("mm", "Peer %u send error: %s — disconnecting",
                            peer.node_id, std::strerror(err));
                peer.connected = false;
                if (peer.fd >= 0) {
                    release_tls(peer);
                    ::close(peer.fd);
                    peer.fd = -1;
                }
                peer.send_buf.clear();
                // Schedule reconnect (will be fully implemented in task 10.1).
                return false;
            }

            // Other error — treat as disconnect.
            OB_LOG_ERROR("mm", "Peer %u send error: %s — disconnecting",
                         peer.node_id, std::strerror(err));
            peer.connected = false;
            if (peer.fd >= 0) {
                release_tls(peer);
                ::close(peer.fd);
                peer.fd = -1;
            }
            peer.send_buf.clear();
            return false;
        }

        // sent == 0: unusual but possible — treat as EAGAIN.
        arm_epollout(peer);
        return true;
    }

    return true;
}

void MultiMasterManager::arm_epollout(PeerConnection& peer) {
    if (epoll_fd_ < 0 || peer.fd < 0) return;

    struct epoll_event ev{};
    ev.events = EPOLLIN | EPOLLOUT | EPOLLET;
    ev.data.fd = peer.fd;
    ::epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, peer.fd, &ev);
}

void MultiMasterManager::disarm_epollout(PeerConnection& peer) {
    if (epoll_fd_ < 0 || peer.fd < 0) return;

    struct epoll_event ev{};
    ev.events = EPOLLIN | EPOLLET;
    ev.data.fd = peer.fd;
    ::epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, peer.fd, &ev);
}

// ── Frame receive/parse methods (task 6.1) ────────────────────────────────────

void MultiMasterManager::process_recv_buf(PeerConnection& peer) {
    while (true) {
        // Need at least 4 bytes for the frame header.
        if (peer.recv_buf.size() < MM_FRAME_HEADER_SIZE) {
            break;
        }

        // Read length (uint32 LE) from first 4 bytes.
        uint32_t length = 0;
        std::memcpy(&length, peer.recv_buf.data(), sizeof(uint32_t));

        // Validate: length must not exceed MM_MAX_FRAME_PAYLOAD.
        if (length > MM_MAX_FRAME_PAYLOAD) {
            OB_LOG_ERROR("mm", "Peer %u: frame too large (%u bytes > %zu max) — disconnecting",
                         peer.node_id, length, MM_MAX_FRAME_PAYLOAD);
            if (peer.fd >= 0) {
                release_tls(peer);
                ::close(peer.fd);
                peer.fd = -1;
            }
            peer.connected = false;
            peer.recv_buf.clear();
            return;
        }

        // Check if the full frame (header + payload) is available.
        if (peer.recv_buf.size() < MM_FRAME_HEADER_SIZE + length) {
            break;  // Incomplete frame — wait for more data.
        }

        // Extract payload pointer and call handle_frame.
        const uint8_t* payload_ptr = peer.recv_buf.data() + MM_FRAME_HEADER_SIZE;
        handle_frame(peer, payload_ptr, static_cast<size_t>(length));

        // If peer was disconnected during handle_frame, stop processing.
        if (!peer.connected) {
            return;
        }

        // Remove processed bytes (header + payload) from recv_buf.
        size_t consumed = MM_FRAME_HEADER_SIZE + length;
        peer.recv_buf.erase(peer.recv_buf.begin(),
                            peer.recv_buf.begin() + static_cast<std::ptrdiff_t>(consumed));
    }
}

void MultiMasterManager::handle_frame(PeerConnection& peer,
                                      const uint8_t* data, size_t len) {
    // Authentication comes before the handshake, so an unauthenticated peer cannot even tell us
    // which node it claims to be (#30 part two).
    if (!config_.cluster_secret.empty() && !peer.peer_proved) {
        handle_auth_frame(peer, data, len);
        return;
    }

    if (!peer.handshake_done) {
        // First frame on connection must be a handshake.
        process_handshake(peer, data, len);
        return;
    }

    // Subsequent frames are WAL records: WALRecordV2 header (38B) + payload.
    if (len < MM_WALRECORD_V2_SIZE) {
        OB_LOG_ERROR("mm", "Peer %u: frame too short for WAL record (%zu < %zu) — disconnecting",
                     peer.node_id, len, MM_WALRECORD_V2_SIZE);
        if (peer.fd >= 0) {
            release_tls(peer);
            ::close(peer.fd);
            peer.fd = -1;
        }
        peer.connected = false;
        peer.recv_buf.clear();
        return;
    }

    // Parse WALRecordV2 header.
    WALRecordV2 hdr{};
    std::memcpy(&hdr, data, MM_WALRECORD_V2_SIZE);

    // Validate: payload_len should match (frame_len - 38).
    size_t expected_payload_len = len - MM_WALRECORD_V2_SIZE;
    if (hdr.payload_len != expected_payload_len) {
        OB_LOG_ERROR("mm", "Peer %u: WAL payload_len mismatch (hdr=%u, frame=%zu) — disconnecting",
                     peer.node_id, hdr.payload_len,
                     expected_payload_len);
        if (peer.fd >= 0) {
            release_tls(peer);
            ::close(peer.fd);
            peer.fd = -1;
        }
        peer.connected = false;
        peer.recv_buf.clear();
        return;
    }

    // Extract payload pointer (bytes after the 38B header).
    const void* payload_ptr = (expected_payload_len > 0)
                              ? static_cast<const void*>(data + MM_WALRECORD_V2_SIZE)
                              : nullptr;

    // Snapshot bootstrap (#76). Wire-only record types, dispatched before the WAL types so a
    // chunk is never mistaken for something to replay.
    switch (hdr.record_type) {
        case MM_MSG_SNAPSHOT_REQUEST:
            handle_snapshot_request(peer);
            return;
        case MM_MSG_SNAPSHOT_BEGIN:
            handle_snapshot_begin(peer, static_cast<const uint8_t*>(payload_ptr),
                                  expected_payload_len);
            return;
        case MM_MSG_SNAPSHOT_CHUNK:
            handle_snapshot_chunk(peer, static_cast<const uint8_t*>(payload_ptr),
                                  expected_payload_len);
            return;
        case MM_MSG_SNAPSHOT_END:
            handle_snapshot_end(peer, static_cast<const uint8_t*>(payload_ptr),
                                expected_payload_len);
            return;
        case MM_MSG_SNAPSHOT_ABORT: {
            const std::string reason = decode_snapshot_abort(
                static_cast<const uint8_t*>(payload_ptr), expected_payload_len);
            OB_LOG_WARN("mm", "Peer %u aborted the snapshot: %s", peer.node_id, reason.c_str());
            if (snapshot_recv_.active && snapshot_recv_.source_node_id == peer.node_id) {
                abort_bootstrap("peer_aborted");
            }
            return;
        }
        default:
            break;
    }

    if (hdr.record_type == WAL_RECORD_VERSION_VECTOR) {
        // The peer told us what it holds. This is what replaced the byte-offset comparison
        // that #61 was built on.
        if (peer.peer_vector.deserialize(static_cast<const uint8_t*>(payload_ptr),
                                         expected_payload_len)) {
            OB_LOG_INFO("mm", "Peer %u version vector: entries=%zu truncated=%d",
                        peer.node_id, peer.peer_vector.entry_count(),
                        peer.peer_vector.truncated() ? 1 : 0);
        } else {
            OB_LOG_WARN("mm", "Peer %u sent an unusable version vector — sending everything",
                        peer.node_id);
        }
        // Only scan the WAL if this peer is actually missing something. Reconciliation (#57)
        // sends a vector to every peer on a timer, and a vector arriving used to start a full
        // scan of the retained WAL — measured in the harness as `scanned=543 (9662010 bytes)
        // sent=0`, repeated per peer per interval, on the io_loop thread that also carries live
        // traffic. A 1 GB WAL at the 94 MB/s this scan runs at would spend most of every interval
        // reading itself to discover there was nothing to send.
        //
        // Skipping is safe because every route by which a peer can be missing data leaves
        // evidence in the comparison or forces a reconnect: a peer that was disconnected comes
        // back through the handshake, a backlog dropped for not draining now closes the
        // connection (#69), and a record the receiver refused leaves its own frontier behind, so
        // peer_lacks is not empty. If a future change can drop a record while both sides stay
        // connected and both frontiers keep moving, this shortcut has to go with it.
        // A node that holds nothing cannot be caught up honestly: it will see sequence 5000
        // before it ever sees 1, so it can never claim contiguity for a foreign origin and its
        // peers keep resending records it already has (#67). A snapshot carries the sender's own
        // frontiers, which is a base it may legitimately declare. Gated inside
        // request_snapshot_from() on holding nothing at all, because the install discards
        // whatever is here.
        if (!peer.peer_vector.wants_everything() && peer.peer_vector.entry_count() > 0 &&
            request_snapshot_from(peer)) {
            return;
        }

        bool truncated = false;
        const auto ours = engine_.export_version_vector(MM_MAX_VV_ENTRIES, truncated);
        const VectorDiff diff = compare_vectors(ours, peer.peer_vector, peer.node_id);

        if (!peer.peer_vector.wants_everything() && diff.peer_lacks.empty()) {
            peer.catchup_started = true;
            OB_LOG_DEBUG("mm",
                         "Peer %u holds everything we do (%zu entries compared) — no scan",
                         peer.node_id, ours.size());
            return;
        }

        OB_LOG_INFO("mm", "Peer %u is missing %zu (symbol, origin) ranges — scanning",
                    peer.node_id, diff.peer_lacks.size());
        start_catchup_to_peer(peer);
        return;
    }

    // Remember what we last heard from this peer. PeerConnection::last_hlc is
    // reported by MM_PEERS, and before this it was written nowhere in the codebase:
    // one read site, zero write sites, so the hlc_timestamp column showed 0.0.0 for
    // every peer no matter how much data had flowed.
    peer.last_hlc = HLCTimestamp::deserialize(hdr.hlc_data);

    // Dispatch to handle_remote_record.
    handle_remote_record(peer.node_id, hdr, payload_ptr, expected_payload_len);
}

// ── Authentication (#30 part two) ─────────────────────────────────────────────

void MultiMasterManager::send_auth_challenge(PeerConnection& peer) {
    peer.auth_nonce = generate_nonce_hex();

    WALRecordV2 hdr{};
    hdr.sequence_number = 0;
    hdr.timestamp_ns    = 0;
    hdr.checksum        = crc32c(peer.auth_nonce.data(), peer.auth_nonce.size());
    hdr.payload_len     = static_cast<uint16_t>(peer.auth_nonce.size());
    hdr.record_type     = MM_MSG_AUTH_CHALLENGE;
    hdr.version         = 1;
    hdr.origin_node_id  = config_.node_id;
    std::memset(hdr.hlc_data, 0, sizeof(hdr.hlc_data));

    std::vector<uint8_t> frame;
    frame.reserve(MM_WALRECORD_V2_SIZE + peer.auth_nonce.size());
    const auto* hdr_bytes = reinterpret_cast<const uint8_t*>(&hdr);
    frame.insert(frame.end(), hdr_bytes, hdr_bytes + MM_WALRECORD_V2_SIZE);
    frame.insert(frame.end(), peer.auth_nonce.begin(), peer.auth_nonce.end());

    enqueue_frame(peer, frame.data(), frame.size());
    OB_LOG_INFO("mm", "Challenged peer %u at %s", peer.node_id, peer.address.c_str());
}

void MultiMasterManager::handle_auth_frame(PeerConnection& peer,
                                           const uint8_t* data, size_t len) {
    auto disconnect = [&](const char* reason) {
        OB_LOG_ERROR("mm", "Peer %u at %s: %s - disconnecting",
                     peer.node_id, peer.address.c_str(), reason);
        if (peer.fd >= 0) {
            ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, peer.fd, nullptr);
            release_tls(peer);
            ::close(peer.fd);
            peer.fd = -1;
        }
        peer.connected = false;
        peer.peer_proved = false;
        peer.auth_nonce.clear();
        peer.recv_buf.clear();
        peer.send_buf.clear();
    };

    if (len == MM_HANDSHAKE_SIZE) {
        // The one case worth naming: a peer that sent its handshake straight away is a peer running
        // without --cluster-secret-file. Reported as that rather than as a malformed frame, because
        // the fix is a configuration change on the other node and nothing about this one.
        disconnect("sent a handshake before authenticating, so it is running without a cluster "
                   "secret; there is no mixed mode");
        return;
    }
    if (len < MM_WALRECORD_V2_SIZE) {
        disconnect("frame too short to be an authentication message");
        return;
    }

    WALRecordV2 hdr{};
    std::memcpy(&hdr, data, MM_WALRECORD_V2_SIZE);
    const size_t payload_len = len - MM_WALRECORD_V2_SIZE;
    if (hdr.payload_len != payload_len) {
        disconnect("authentication frame length disagrees with its header");
        return;
    }
    const std::string payload(reinterpret_cast<const char*>(data + MM_WALRECORD_V2_SIZE),
                              payload_len);
    const std::string& secret = config_.cluster_secret.sole().secret;

    if (hdr.record_type == MM_MSG_AUTH_CHALLENGE) {
        if (!is_auth_hex(payload)) {
            disconnect("malformed challenge");
            return;
        }
        // Answered as *our* role. The peer verifies it as the other one, so a nonce reflected
        // back at us produces a response for the wrong role and does not verify.
        const std::string answer =
            auth_response(secret, AuthSurface::MultiMaster,
                          peer.we_accepted ? AuthRole::Acceptor : AuthRole::Initiator,
                          "", payload);

        WALRecordV2 out{};
        out.sequence_number = 0;
        out.timestamp_ns    = 0;
        out.checksum        = crc32c(answer.data(), answer.size());
        out.payload_len     = static_cast<uint16_t>(answer.size());
        out.record_type     = MM_MSG_AUTH_RESPONSE;
        out.version         = 1;
        out.origin_node_id  = config_.node_id;
        std::memset(out.hlc_data, 0, sizeof(out.hlc_data));

        std::vector<uint8_t> frame;
        frame.reserve(MM_WALRECORD_V2_SIZE + answer.size());
        const auto* out_bytes = reinterpret_cast<const uint8_t*>(&out);
        frame.insert(frame.end(), out_bytes, out_bytes + MM_WALRECORD_V2_SIZE);
        frame.insert(frame.end(), answer.begin(), answer.end());
        enqueue_frame(peer, frame.data(), frame.size());
        OB_LOG_DEBUG("mm", "Answered challenge from peer %u", peer.node_id);
        return;
    }

    if (hdr.record_type == MM_MSG_AUTH_RESPONSE) {
        const std::string expected =
            peer.auth_nonce.empty()
                ? std::string{}
                : auth_response(secret, AuthSurface::MultiMaster,
                                peer.we_accepted ? AuthRole::Initiator : AuthRole::Acceptor,
                                "", peer.auth_nonce);
        // Spent either way, so a captured response cannot be replayed on this connection.
        peer.auth_nonce.clear();
        if (!responses_equal(expected, payload)) {
            disconnect("failed authentication");
            return;
        }
        peer.peer_proved = true;
        OB_LOG_INFO("mm", "Peer %u at %s authenticated", peer.node_id, peer.address.c_str());
        // The handshake is the acceptance: sending it only now is what makes this mutual, because
        // the peer applies the same rule to us.
        send_handshake(peer);
        return;
    }

    disconnect("unexpected frame type before authentication");
}

void MultiMasterManager::send_handshake(PeerConnection& peer) {
    HandshakeMessage msg{};
    msg.node_id = config_.node_id;
    msg.protocol_version = MM_PROTOCOL_VERSION;
    msg.compression_preference = config_.compress ? uint8_t(1) : uint8_t(0);
    // One load. These were two adjacent lines, and a rotation between them told the peer to resume
    // from an offset belonging to the file that had just been closed - see WalPosition (#85).
    const WalPosition wal_pos = wal_.current_position();
    msg.wal_file_index = wal_pos.file_index;
    msg.wal_byte_offset = wal_pos.offset;

    uint8_t buf[MM_HANDSHAKE_SIZE];
    msg.serialize(buf);

    enqueue_frame(peer, buf, MM_HANDSHAKE_SIZE);

    OB_LOG_DEBUG("mm", "Sent handshake to peer %u: %s",
                 peer.node_id, msg.to_string().c_str());
}

void MultiMasterManager::process_handshake(PeerConnection& peer,
                                           const uint8_t* data, size_t len) {
    // Validate minimum size.
    if (len < MM_HANDSHAKE_SIZE) {
        OB_LOG_ERROR("mm", "Peer %u: handshake too short (%zu < %zu) — disconnecting",
                     peer.node_id, len, MM_HANDSHAKE_SIZE);
        if (peer.fd >= 0) {
            release_tls(peer);
            ::close(peer.fd);
            peer.fd = -1;
        }
        peer.connected = false;
        peer.recv_buf.clear();
        return;
    }

    // Deserialize handshake message.
    HandshakeMessage msg{};
    if (!HandshakeMessage::deserialize(data, len, msg)) {
        OB_LOG_ERROR("mm", "Peer %u: handshake deserialization failed — disconnecting",
                     peer.node_id);
        if (peer.fd >= 0) {
            release_tls(peer);
            ::close(peer.fd);
            peer.fd = -1;
        }
        peer.connected = false;
        peer.recv_buf.clear();
        return;
    }

    // Verify protocol version.
    if (msg.protocol_version != MM_PROTOCOL_VERSION) {
        OB_LOG_WARN("mm", "Peer %u: unsupported protocol version %u (expected %u) — disconnecting",
                    peer.node_id, msg.protocol_version, MM_PROTOCOL_VERSION);
        if (peer.fd >= 0) {
            release_tls(peer);
            ::close(peer.fd);
            peer.fd = -1;
        }
        peer.connected = false;
        peer.recv_buf.clear();
        return;
    }

    // Update peer state from handshake.
    peer.node_id = msg.node_id;
    peer.confirmed_file = msg.wal_file_index;
    peer.confirmed_offset = msg.wal_byte_offset;
    peer.handshake_done = true;

    // An accepted connection carries no address: the socket's source port is
    // ephemeral, not the port the peer listens on, so MM_PEERS showed a blank
    // address for every inbound peer — half the mesh in a three-node cluster. The
    // registry knows the advertised address, and the handshake has just told us
    // which node this is.
    if (peer.address.empty() && peer_registry_) {
        for (const auto& known : peer_registry_->get_peers()) {
            if (known.node_id == peer.node_id) {
                peer.address = known.address;
                break;
            }
        }
        if (peer.address.empty()) {
            OB_LOG_WARN("mm",
                        "Peer %u completed handshake but is not in the registry, "
                        "so its address stays unknown", peer.node_id);
        }
    }

    OB_LOG_INFO("mm", "Handshake complete with peer %u: %s",
                peer.node_id, msg.to_string().c_str());

    // No catch-up decision here any more. This is where #61 lived: the peer's WAL position
    // was compared with ours, and two independent logs have no common scale — every node
    // writes its own records plus copies of foreign ones, so the same data yields different
    // offsets. Measured consequence: a node reporting offset 846 against a local 870 was
    // judged "behind by 24 bytes" and sent one empty checkpoint record, while the rows it had
    // missed sat earlier in the log.
    //
    // Instead we tell the peer what we hold, and wait for it to tell us the same. Catch-up
    // starts when its vector arrives, or when MM_VV_GRACE_MS passes and we assume it holds
    // nothing.
    send_version_vector(peer);
    peer.vector_deadline_ms = now_ms() + MM_VV_GRACE_MS;
    peer.catchup_started    = false;

    if (msg.protocol_version < 2) {
        OB_LOG_WARN("mm",
                    "Peer %u speaks protocol %u, which does not send a version vector — it "
                    "will receive everything retained in our WAL",
                    peer.node_id, msg.protocol_version);
    }
}

void MultiMasterManager::send_version_vector(PeerConnection& peer) {
    bool truncated = false;
    const auto entries = engine_.export_version_vector(MM_MAX_VV_ENTRIES, truncated);
    const auto payload = serialize_version_vector(entries, truncated);

    WALRecordV2 hdr{};
    hdr.sequence_number = 0;
    hdr.timestamp_ns    = 0;
    hdr.checksum        = crc32c(payload.data(), payload.size());
    hdr.payload_len     = static_cast<uint16_t>(payload.size());
    hdr.record_type     = WAL_RECORD_VERSION_VECTOR;
    hdr.version         = 1;
    hdr.origin_node_id  = config_.node_id;
    std::memset(hdr.hlc_data, 0, sizeof(hdr.hlc_data));

    std::vector<uint8_t> frame;
    frame.reserve(MM_WALRECORD_V2_SIZE + payload.size());
    const auto* hdr_bytes = reinterpret_cast<const uint8_t*>(&hdr);
    frame.insert(frame.end(), hdr_bytes, hdr_bytes + MM_WALRECORD_V2_SIZE);
    frame.insert(frame.end(), payload.begin(), payload.end());

    enqueue_frame(peer, frame.data(), frame.size());
    OB_LOG_INFO("mm", "Sent version vector to peer %u: entries=%zu truncated=%d bytes=%zu",
                peer.node_id, entries.size(), truncated ? 1 : 0, frame.size());
}

ReconcileReport MultiMasterManager::reconcile_with_peers() {
    ReconcileReport report{};

    // The vector snapshot comes from the engine's cache, so this does not touch the engine mutex
    // while holding MM's — the cycle that deadlocked the flush thread once already.
    bool truncated = false;
    const auto ours = engine_.export_version_vector(MM_MAX_VV_ENTRIES, truncated);

    std::lock_guard<std::mutex> lock(mtx_);
    for (auto& [node_id, peer] : peers_) {
        if (!peer.connected || !peer.handshake_done) continue;
        ++report.peers_contacted;

        // What the two sides disagree about, as far as we know from the peer's last vector. This
        // is reporting, not the repair: the repair is the send below.
        const VectorDiff diff = compare_vectors(ours, peer.peer_vector, peer.node_id);
        report.we_lack.insert(report.we_lack.end(), diff.we_lack.begin(), diff.we_lack.end());
        report.peer_lacks.insert(report.peer_lacks.end(),
                                 diff.peer_lacks.begin(), diff.peer_lacks.end());

        // Send ours again and let the peer's returning vector re-run our filter, so both
        // directions get closed by one exchange.
        send_version_vector(peer);
        peer.catchup_started    = false;
        peer.vector_deadline_ms = now_ms() + MM_VV_GRACE_MS;
        ++report.vectors_sent;
    }

    OB_LOG_DEBUG("mm", "Reconcile pass: peers=%zu vectors_sent=%zu we_lack=%zu peer_lacks=%zu",
                 report.peers_contacted, report.vectors_sent,
                 report.we_lack.size(), report.peer_lacks.size());
    return report;
}

void MultiMasterManager::start_overdue_catchups() {
    const uint64_t now = now_ms();
    for (auto& [node_id, peer] : peers_) {
        if (!peer.connected || !peer.handshake_done) continue;
        if (peer.catchup_started || peer.catching_up) continue;
        if (peer.vector_deadline_ms == 0 || now < peer.vector_deadline_ms) continue;

        OB_LOG_WARN("mm",
                    "Peer %u sent no version vector within %llu ms — treating it as holding "
                    "nothing and sending everything retained",
                    peer.node_id, static_cast<unsigned long long>(MM_VV_GRACE_MS));
        start_catchup_to_peer(peer);
    }
}

// ── Reconnect logic (task 10.1) ────────────────────────────────────────────────

void MultiMasterManager::schedule_reconnect(uint16_t node_id) {
    std::lock_guard<std::mutex> lock(mtx_);

    auto it = peers_.find(node_id);
    if (it == peers_.end()) return;

    auto& peer = it->second;

    // Close the socket if still open.
    if (peer.fd >= 0) {
        release_tls(peer);
        ::close(peer.fd);
        peer.fd = -1;
    }

    peer.connected = false;
    peer.handshake_done = false;
    peer.peer_proved = false;
    peer.auth_nonce.clear();
    peer.we_accepted = false;
    peer.recv_buf.clear();
    peer.send_buf.clear();

    // Schedule next reconnect attempt using backoff delay.
    uint32_t delay_ms = peer.backoff.next_delay_ms();
    peer.next_reconnect_time = std::chrono::steady_clock::now() +
                               std::chrono::milliseconds(delay_ms);

    OB_LOG_INFO("mm", "Scheduled reconnect for peer %u (attempt #%u, delay %u ms)",
                node_id, peer.backoff.attempt, delay_ms);
}

void MultiMasterManager::reconnect_loop() {
    OB_LOG_DEBUG("mm", "reconnect_loop started");

    while (running_.load(std::memory_order_acquire)) {
        {
            std::lock_guard<std::mutex> lock(mtx_);
            auto now = std::chrono::steady_clock::now();

            // A connection we accepted and never identified is not a peer, and once it is down it
            // cannot become one: the port it arrived on is the peer's ephemeral source port, so
            // there is no address to dial and no node behind the record yet. Keeping it left one
            // dead entry in peers_ per refused inbound connection - and, because the dial below
            // then had nothing to parse, put `Reconnect: invalid peer address:` in the log ten
            // times a second for the rest of the process's life. The peer that dialled us will
            // dial again by itself; that is the only way this connection can come back.
            for (auto it = peers_.begin(); it != peers_.end();) {
                const PeerConnection& dead = it->second;
                if (dead.node_id == 0 && !dead.connected && dead.fd < 0) {
                    OB_LOG_DEBUG("mm", "Dropping the record of an inbound connection that closed "
                                       "before its handshake named a node (key=%u)", it->first);
                    it = peers_.erase(it);
                } else {
                    ++it;
                }
            }

            for (auto& [nid, peer] : peers_) {
                if (peer.connected) continue;

                // Check if it's time to attempt reconnect.
                if (now < peer.next_reconnect_time) continue;

                // Every failure branch in this loop has to move next_reconnect_time. This one did
                // not, so a permanent failure was retried at loop frequency and said so in the log
                // at the same rate; backoff is what makes a failure that will not clear legible.
                if (peer.address.empty()) {
                    const uint32_t delay_ms = peer.backoff.next_delay_ms();
                    peer.next_reconnect_time = now + std::chrono::milliseconds(delay_ms);
                    OB_LOG_DEBUG("mm", "Reconnect: peer %u advertises no address (it is not in the "
                                       "registry), so it has to dial us; next look in %u ms",
                                 nid, delay_ms);
                    continue;
                }

                // Attempt to connect.
                std::string host;
                uint16_t port = 0;
                if (!parse_address(peer.address, host, port)) {
                    const uint32_t delay_ms = peer.backoff.next_delay_ms();
                    peer.next_reconnect_time = now + std::chrono::milliseconds(delay_ms);
                    OB_LOG_WARN("mm", "Reconnect: invalid peer address for peer %u: '%s', next "
                                      "attempt in %u ms", nid, peer.address.c_str(), delay_ms);
                    continue;
                }

                int fd = ::socket(AF_INET, SOCK_STREAM, 0);
                if (fd < 0) {
                    OB_LOG_ERROR("mm", "Reconnect: socket() failed for peer %u: %s",
                                 nid, std::strerror(errno));
                    // Schedule next attempt.
                    uint32_t delay_ms = peer.backoff.next_delay_ms();
                    peer.next_reconnect_time = now + std::chrono::milliseconds(delay_ms);
                    OB_LOG_INFO("mm", "Reconnect attempt #%u to peer %u failed, next in %u ms",
                                peer.backoff.attempt, nid, delay_ms);
                    continue;
                }

                struct sockaddr_in addr{};
                addr.sin_family = AF_INET;
                addr.sin_port = htons(port);
                if (::inet_pton(AF_INET, host.c_str(), &addr.sin_addr) <= 0) {
                    // Try hostname resolution.
                    struct addrinfo hints{}, *res = nullptr;
                    hints.ai_family = AF_INET;
                    hints.ai_socktype = SOCK_STREAM;
                    if (::getaddrinfo(host.c_str(), nullptr, &hints, &res) == 0 && res) {
                        addr.sin_addr = reinterpret_cast<struct sockaddr_in*>(res->ai_addr)->sin_addr;
                        ::freeaddrinfo(res);
                    } else {
                        OB_LOG_WARN("mm", "Reconnect: cannot resolve peer %u address: %s",
                                    nid, host.c_str());
                        ::close(fd);
                        uint32_t delay_ms = peer.backoff.next_delay_ms();
                        peer.next_reconnect_time = now + std::chrono::milliseconds(delay_ms);
                        OB_LOG_INFO("mm", "Reconnect attempt #%u to peer %u failed, next in %u ms",
                                    peer.backoff.attempt, nid, delay_ms);
                        continue;
                    }
                }

                if (::connect(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
                    ::close(fd);
                    uint32_t delay_ms = peer.backoff.next_delay_ms();
                    peer.next_reconnect_time = now + std::chrono::milliseconds(delay_ms);
                    OB_LOG_INFO("mm", "Reconnect attempt #%u to peer %u at %s failed: %s, next in %u ms",
                                peer.backoff.attempt, nid, peer.address.c_str(),
                                std::strerror(errno), delay_ms);
                    continue;
                }

                // Connection successful!
                set_nonblocking(fd);
                int tcp_nodelay = 1;
                ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY,
                             &tcp_nodelay, sizeof(tcp_nodelay));

                peer.fd = fd;
                peer.connected = true;
                peer.handshake_done = false;
                peer.peer_proved = false;
                peer.auth_nonce.clear();
                peer.we_accepted = false;
                peer.backoff.reset();

                // Add peer fd to epoll (edge-triggered EPOLLIN).
                if (epoll_fd_ >= 0) {
                    struct epoll_event ev{};
                    ev.events = EPOLLIN | EPOLLET;
                    ev.data.fd = fd;
                    ::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &ev);
                }

                OB_LOG_INFO("mm", "Reconnected to peer %u at %s", nid,
                            peer.address.c_str());

                // A reconnect starts the TLS handshake again too: the channel belongs to the
                // connection, which is why `release_tls()` cleared it when the last one went.
                if (!attach_tls(peer)) {
                    if (epoll_fd_ >= 0) ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
                    ::close(fd);
                    peer.fd        = -1;
                    peer.connected = false;
                    const uint32_t delay_ms = peer.backoff.next_delay_ms();
                    peer.next_reconnect_time = now + std::chrono::milliseconds(delay_ms);
                    continue;
                }

                // Challenge first when a cluster secret is configured; the handshake follows
                // once the peer has proved itself on this *new* socket - a reconnect starts the
                // exchange again, which is why peer_proved was just cleared.
                if (!config_.cluster_secret.empty()) {
                    send_auth_challenge(peer);
                } else {
                    send_handshake(peer);
                }
            }

            // Both gauges, once per pass, from the one place that computes them. This tick is the
            // mechanism and the call sites are only latency: whichever of the twenty-odd places
            // that move a peer's state ran since the last pass - including accept(), which none of
            // the old inline copies covered - the gauges are right again within 100 ms.
            publish_peer_gauges();
        }

        // Sleep 100ms between iterations.
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    OB_LOG_DEBUG("mm", "reconnect_loop exited");
}

// ── Catch-up streaming (task 8.1) ─────────────────────────────────────────────

void MultiMasterManager::start_catchup_to_peer(PeerConnection& peer) {
    peer.catching_up    = true;
    peer.catchup_started = true;
    peer.needs_snapshot = false;

    const PeerVector& pv = peer.peer_vector;
    OB_LOG_INFO("mm",
                "Starting catch-up to peer %u: vector entries=%zu received=%d truncated=%d",
                peer.node_id, pv.entry_count(), pv.received() ? 1 : 0,
                pv.truncated() ? 1 : 0);

    // Read the whole retained WAL through the same parser everything else uses, and decide
    // per record. Two things this deliberately does not do:
    //
    //   - it does not seek to a byte offset derived from the peer's position. That was #61:
    //     the offsets belong to different logs, and a seek into the middle of a record reads
    //     a header out of payload bytes.
    //   - it does not restrict itself to records this node originated. If the peer is missing
    //     records from a third origin that is currently unreachable, we have them and it does
    //     not, so we send them. That is what makes this a version vector rather than a
    //     per-link cursor.
    uint64_t scanned = 0, sent = 0, skipped_have = 0, skipped_type = 0;
    size_t bytes_sent = 0, bytes_scanned = 0;

    WALReplayer replayer(wal_.dir());
    replayer.replay_v2([&](const WALReplayContext& ctx) {
        ++scanned;
        bytes_scanned += MM_WALRECORD_V2_SIZE + ctx.payload_len;

        if (peer.needs_snapshot) return;                       // backpressure already gave up

        // This scan runs on the io_loop thread, which also carries live traffic to every other
        // peer. Measured on machine B: 94 MB/s, so a 1 GB WAL would stall the loop for about
        // ten seconds. A catch-up that has to read more than the send-buffer ceiling is a
        // snapshot, not a catch-up.
        if (bytes_scanned > config_.max_catchup_bytes) {
            OB_LOG_WARN("mm",
                        "Catch-up scan for peer %u exceeded %zu bytes — falling back to "
                        "snapshot sync instead of holding the io_loop",
                        peer.node_id, config_.max_catchup_bytes);
            peer.needs_snapshot = true;
            peer.send_buf.clear();
            return;
        }
        if (ctx.header.record_type != WAL_RECORD_DELTA) {       // GAP, EPOCH, CHECKPOINT, vector
            ++skipped_type;
            return;
        }
        if (ctx.payload_len < sizeof(DeltaUpdate)) {
            ++skipped_type;
            return;
        }

        DeltaUpdate delta{};
        std::memcpy(&delta, ctx.payload, sizeof(DeltaUpdate));
        const std::string key = std::string(delta.symbol) + "." + delta.exchange;

        // 0 for anything the peer never mentioned, which reads as "it holds nothing here".
        const uint64_t peer_frontier = pv.wants_everything()
                                       ? 0
                                       : pv.frontier_for(key, ctx.origin_node_id);
        if (ctx.header.sequence_number <= peer_frontier) {
            ++skipped_have;
            return;
        }

        // Rebuild the V2 header for the wire: the replay context carries the legacy header
        // plus the origin and HLC that the V2 envelope needs.
        WALRecordV2 hdr{};
        hdr.sequence_number = ctx.header.sequence_number;
        hdr.timestamp_ns    = ctx.header.timestamp_ns;
        hdr.checksum        = ctx.header.checksum;
        hdr.payload_len     = static_cast<uint16_t>(ctx.payload_len);
        hdr.record_type     = WAL_RECORD_DELTA;
        hdr.version         = 1;
        hdr.origin_node_id  = ctx.origin_node_id;
        ctx.hlc.serialize(hdr.hlc_data);

        std::vector<uint8_t> frame;
        frame.reserve(MM_WALRECORD_V2_SIZE + ctx.payload_len);
        const auto* hdr_bytes = reinterpret_cast<const uint8_t*>(&hdr);
        frame.insert(frame.end(), hdr_bytes, hdr_bytes + MM_WALRECORD_V2_SIZE);
        frame.insert(frame.end(), ctx.payload, ctx.payload + ctx.payload_len);

        enqueue_frame(peer, frame.data(), frame.size());
        ++sent;
        bytes_sent += frame.size();

        OB_LOG_DEBUG("mm",
                     "Catch-up: sent %s origin=%u seq=%lu (peer had %lu) to peer %u",
                     key.c_str(), ctx.origin_node_id,
                     static_cast<unsigned long>(ctx.header.sequence_number),
                     static_cast<unsigned long>(peer_frontier), peer.node_id);

        check_backpressure(peer);
    });

    // These numbers are the whole point of the log line: a catch-up that sends nothing looks
    // identical to one that had nothing to send, and #61 lived in exactly that ambiguity.
    OB_LOG_INFO("mm",
                "Catch-up to peer %u finished: scanned=%llu (%zu bytes) sent=%llu "
                "skipped_peer_has=%llu skipped_type=%llu bytes_sent=%zu snapshot_needed=%d",
                peer.node_id,
                static_cast<unsigned long long>(scanned), bytes_scanned,
                static_cast<unsigned long long>(sent),
                static_cast<unsigned long long>(skipped_have),
                static_cast<unsigned long long>(skipped_type),
                bytes_sent, peer.needs_snapshot ? 1 : 0);

    peer.catching_up = false;
}

// ── Backpressure check (task 11.1) ────────────────────────────────────────────

void MultiMasterManager::check_backpressure(PeerConnection& peer) {
    if (peer.send_buf.size() > config_.max_catchup_bytes) {
        OB_LOG_WARN("mm",
                    "Backpressure: peer %u send_buf=%zu > max_catchup_bytes=%zu — dropping the "
                    "connection instead of abandoning a partial frame",
                    peer.node_id, peer.send_buf.size(), config_.max_catchup_bytes);
        engine_.registry().increment_counter("ob_mm_backpressure_snapshot_total");

        // This used to clear the buffer and keep the socket. After a partial write the buffer can
        // begin mid-frame, so that left the peer waiting for the rest of a frame nobody would send
        // and reading the next frames as its tail. Closing the connection is the only answer that
        // does not lie about the state of the stream.
        if (peer.fd >= 0) {
            release_tls(peer);
            ::close(peer.fd);
            peer.fd = -1;
        }
        peer.connected      = false;
        peer.handshake_done = false;
        peer.peer_proved = false;
        peer.auth_nonce.clear();
        peer.we_accepted = false;
        peer.send_buf.clear();
        peer.recv_buf.clear();
        peer.needs_snapshot = true;
        peer.catching_up    = false;
    }
}

void MultiMasterManager::handle_catchup_request(PeerConnection& peer,
                                                uint32_t from_file,
                                                size_t from_offset) {
    OB_LOG_INFO("mm", "Catchup request from peer %u: file=%u offset=%zu",
                peer.node_id, from_file, from_offset);
    // Full implementation in task 12 — replay WAL from position to peer.
}

void MultiMasterManager::handle_topology_change(
    const std::vector<PeerInfo>& new_peers) {
    OB_LOG_INFO("mm", "Topology change: %zu peers", new_peers.size());

    // Build set of new peer node_ids.
    std::unordered_map<uint16_t, PeerInfo> new_map;
    for (const auto& p : new_peers) {
        if (p.node_id != config_.node_id) {
            new_map[p.node_id] = p;
        }
    }

    // Disconnect peers that are no longer in the topology.
    {
        std::vector<uint16_t> to_remove;
        {
            std::lock_guard<std::mutex> lock(mtx_);
            for (const auto& [nid, conn] : peers_) {
                if (new_map.find(nid) == new_map.end()) {
                    to_remove.push_back(nid);
                }
            }
        }
        for (uint16_t nid : to_remove) {
            disconnect_peer(nid);
        }
    }

    // Fill in addresses we could not know earlier. A peer that dialled us arrives
    // over an accepted socket whose source port is ephemeral, so the connection has
    // no usable address until the registry tells us what the node advertises. This
    // is the moment it does.
    {
        std::lock_guard<std::mutex> lock(mtx_);
        for (auto& [nid, conn] : peers_) {
            if (!conn.address.empty()) continue;
            auto it = new_map.find(conn.node_id);
            if (it != new_map.end() && !it->second.address.empty()) {
                conn.address = it->second.address;
                OB_LOG_DEBUG("mm", "Learned address for inbound peer %u: %s",
                             conn.node_id, conn.address.c_str());
            }
        }
    }

    // Connect to new peers.
    for (const auto& [nid, info] : new_map) {
        connect_to_peer(info);
    }
}

} // namespace ob
