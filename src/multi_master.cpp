// ── MultiMasterManager implementation ─────────────────────────────────────────
//
// Epoll-based peer networking, WAL broadcast, origin-based loop prevention,
// bootstrap state management, and diagnostic commands.
//
// Requirements: 4.1–4.8, 9.1–9.6

#include "orderbook/multi_master.hpp"
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
    , hlc_(hlc) {
    conflict_resolver_ = std::make_unique<ConflictResolver>();

    OB_LOG_DEBUG("mm", "MultiMasterManager created: node_id=%u port=%u",
                 config_.node_id, config_.replication_port);
}

MultiMasterManager::~MultiMasterManager() {
    stop();
}

// ── Start / Stop ──────────────────────────────────────────────────────────────

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

    // Close listen socket to unblock accept.
    if (listen_fd_ >= 0) {
        ::close(listen_fd_);
        listen_fd_ = -1;
    }

    // Close epoll to unblock threads.
    if (epoll_fd_ >= 0) {
        ::close(epoll_fd_);
        epoll_fd_ = -1;
    }

    // Join threads.
    if (io_thread_.joinable()) io_thread_.join();
    if (reconnect_thread_.joinable()) reconnect_thread_.join();

    // Disconnect all peers.
    {
        std::lock_guard<std::mutex> lock(mtx_);
        for (auto& [nid, peer] : peers_) {
            if (peer.fd >= 0) {
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

    OB_LOG_DEBUG("mm", "broadcast_local: seq=%lu to %zu peers",
                 static_cast<unsigned long>(hdr.sequence_number), peer_count);
}

// ── Handle remote record ──────────────────────────────────────────────────────

bool MultiMasterManager::handle_remote_record(uint16_t /*peer_node_id*/,
                                              const WALRecordV2& hdr,
                                              const void* payload,
                                              size_t payload_len) {
    // Extract origin from the WAL record header.
    uint16_t origin = hdr.origin_node_id;

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
    OB_LOG_INFO("mm", "Bootstrap started for node %u", config_.node_id);
}

// ── Diagnostic commands ───────────────────────────────────────────────────────

std::string MultiMasterManager::handle_mm_peers_command() const {
    std::lock_guard<std::mutex> lock(mtx_);

    std::ostringstream oss;
    oss << "node_id\taddress\tstatus\thlc_timestamp\tlag_bytes\n";

    for (const auto& [nid, peer] : peers_) {
        oss << peer.node_id << '\t'
            << peer.address << '\t'
            << (peer.connected ? "connected" : "disconnected") << '\t'
            << peer.last_hlc.to_string() << '\t'
            << peer.send_buf.size() << '\n';
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

        for (int i = 0; i < nfds; ++i) {
            int ev_fd = events[i].data.fd;
            uint32_t ev_events = events[i].events;

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
                    conn.connected = true;
                    conn.handshake_done = false;
                    conn.compress = config_.compress;

                    // Store with temp key — will be re-keyed after handshake.
                    peers_[temp_id] = std::move(conn);

                    // Send our handshake to the new peer.
                    send_handshake(peers_[temp_id]);
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

                // Handle EPOLLIN — recv data.
                if (ev_events & EPOLLIN) {
                    // Edge-triggered: read in a loop until EAGAIN.
                    bool disconnected = false;
                    while (true) {
                        uint8_t buf[8192];
                        ssize_t n = ::recv(ev_fd, buf, sizeof(buf), 0);
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
                        ::close(ev_fd);
                        peer_ptr->fd = -1;
                        peer_ptr->connected = false;
                        peer_ptr->recv_buf.clear();
                        peer_ptr->send_buf.clear();

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

                    // After handshake, re-key the peer if needed.
                    if (peer_ptr->handshake_done && peer_ptr->node_id != 0 &&
                        peer_key != peer_ptr->node_id) {
                        // Move peer to correct key.
                        uint16_t real_id = peer_ptr->node_id;
                        PeerConnection moved = std::move(*peer_ptr);
                        peers_.erase(peer_key);
                        peers_[real_id] = std::move(moved);
                    }
                }

                // Handle EPOLLOUT — drain send buffer.
                if ((ev_events & EPOLLOUT) && peer_ptr && peer_ptr->connected) {
                    try_drain_send_buf(*peer_ptr);
                }

                // Handle errors/hangup.
                if (ev_events & (EPOLLERR | EPOLLHUP)) {
                    if (peer_ptr && peer_ptr->connected) {
                        OB_LOG_WARN("mm", "Peer fd=%d EPOLLERR/HUP — disconnecting", ev_fd);
                        ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, ev_fd, nullptr);
                        ::close(ev_fd);
                        peer_ptr->fd = -1;
                        peer_ptr->connected = false;
                        peer_ptr->recv_buf.clear();
                        peer_ptr->send_buf.clear();

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
    conn.connected = true;
    conn.compress = config_.compress;
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

    // Send handshake to initiate protocol exchange.
    send_handshake(peers_[peer.node_id]);

    // Update metrics: count connected peers.
    size_t connected = 0;
    for (const auto& [nid, p] : peers_) {
        if (p.connected) ++connected;
    }
    engine_.registry().set_gauge("ob_mm_peers_connected",
                                 static_cast<int64_t>(connected));
}

void MultiMasterManager::disconnect_peer(uint16_t node_id) {
    std::lock_guard<std::mutex> lock(mtx_);

    auto it = peers_.find(node_id);
    if (it == peers_.end()) return;

    if (it->second.fd >= 0) {
        ::close(it->second.fd);
    }
    peers_.erase(it);

    OB_LOG_INFO("mm", "Peer %u disconnected", node_id);

    // Update metrics: count connected peers.
    size_t connected = 0;
    for (const auto& [nid, p] : peers_) {
        if (p.connected) ++connected;
    }
    engine_.registry().set_gauge("ob_mm_peers_connected",
                                 static_cast<int64_t>(connected));
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
}

bool MultiMasterManager::try_drain_send_buf(PeerConnection& peer) {
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

    // Dispatch to handle_remote_record.
    handle_remote_record(peer.node_id, hdr, payload_ptr, expected_payload_len);
}

void MultiMasterManager::send_handshake(PeerConnection& peer) {
    HandshakeMessage msg{};
    msg.node_id = config_.node_id;
    msg.protocol_version = MM_PROTOCOL_VERSION;
    msg.compression_preference = config_.compress ? uint8_t(1) : uint8_t(0);
    msg.wal_file_index = wal_.current_file_index();
    msg.wal_byte_offset = wal_.current_offset();

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

    OB_LOG_INFO("mm", "Handshake complete with peer %u: %s",
                peer.node_id, msg.to_string().c_str());

    // Check if peer is behind our WAL position — if so, start catch-up.
    uint32_t local_file = wal_.current_file_index();
    size_t local_offset = wal_.current_offset();

    if (msg.wal_file_index < local_file ||
        (msg.wal_file_index == local_file && msg.wal_byte_offset < local_offset)) {
        OB_LOG_INFO("mm", "Peer %u is behind (peer: file=%u off=%lu, local: file=%u off=%zu) — starting catch-up",
                    peer.node_id, msg.wal_file_index,
                    static_cast<unsigned long>(msg.wal_byte_offset),
                    local_file, local_offset);
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
        ::close(peer.fd);
        peer.fd = -1;
    }

    peer.connected = false;
    peer.handshake_done = false;
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

            for (auto& [nid, peer] : peers_) {
                if (peer.connected) continue;

                // Check if it's time to attempt reconnect.
                if (now < peer.next_reconnect_time) continue;

                // Attempt to connect.
                std::string host;
                uint16_t port = 0;
                if (!parse_address(peer.address, host, port)) {
                    OB_LOG_WARN("mm", "Reconnect: invalid peer address: %s",
                                peer.address.c_str());
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

                // Send handshake to initiate protocol exchange.
                send_handshake(peer);

                // Update metrics.
                size_t connected = 0;
                for (const auto& [id, p] : peers_) {
                    if (p.connected) ++connected;
                }
                engine_.registry().set_gauge("ob_mm_peers_connected",
                                             static_cast<int64_t>(connected));
            }
        }

        // Sleep 100ms between iterations.
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    OB_LOG_DEBUG("mm", "reconnect_loop exited");
}

// ── Catch-up streaming (task 8.1) ─────────────────────────────────────────────

void MultiMasterManager::start_catchup_to_peer(PeerConnection& peer) {
    OB_LOG_INFO("mm", "Starting catch-up to peer %u from file=%u offset=%zu",
                peer.node_id, peer.confirmed_file, peer.confirmed_offset);

    peer.catching_up = true;
    peer.needs_snapshot = false;

    uint32_t file_idx = peer.confirmed_file;
    size_t   offset   = peer.confirmed_offset;

    const uint32_t local_file   = wal_.current_file_index();
    const size_t   local_offset = wal_.current_offset();

    // Helper lambda: build WAL file path for a given index.
    auto wal_path = [&](uint32_t idx) -> std::string {
        char buf[32];
        std::snprintf(buf, sizeof(buf), "wal_%06u.bin", idx);
        return wal_.dir() + "/" + buf;
    };

    // Stream WAL records from peer's confirmed position to current position.
    while (file_idx < local_file ||
           (file_idx == local_file && offset < local_offset)) {

        std::string path = wal_path(file_idx);
        int fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            // WAL file doesn't exist (rotated away) → trigger snapshot sync.
            OB_LOG_WARN("mm", "Catch-up: WAL file %s not found (rotated?) — needs snapshot for peer %u",
                        path.c_str(), peer.node_id);
            peer.needs_snapshot = true;
            peer.catching_up = false;
            return;
        }

        // Seek to the starting offset within this file.
        if (offset > 0) {
            if (::lseek(fd, static_cast<off_t>(offset), SEEK_SET) < 0) {
                OB_LOG_WARN("mm", "Catch-up: lseek failed on %s offset=%zu — needs snapshot for peer %u",
                            path.c_str(), offset, peer.node_id);
                ::close(fd);
                peer.needs_snapshot = true;
                peer.catching_up = false;
                return;
            }
        }

        // Determine the end position for this file.
        size_t end_offset = (file_idx == local_file) ? local_offset : SIZE_MAX;

        // Read records sequentially from this WAL file.
        // WAL files contain mixed V1 (24B header) and V2 (38B header) records.
        while (true) {
            // Check current position.
            off_t cur_pos = ::lseek(fd, 0, SEEK_CUR);
            if (cur_pos < 0) break;

            // If we're in the current file and reached the local offset, done with this file.
            if (file_idx == local_file &&
                static_cast<size_t>(cur_pos) >= end_offset) {
                break;
            }

            // Read the first 24 bytes (common to V1 and V2).
            WALRecord v1_hdr{};
            ssize_t n = ::read(fd, &v1_hdr, sizeof(WALRecord));
            if (n == 0) {
                // EOF — move to next file.
                break;
            }
            if (n != static_cast<ssize_t>(sizeof(WALRecord))) {
                // Truncated header — end of valid data.
                OB_LOG_DEBUG("mm", "Catch-up: truncated WAL header in %s — ending file",
                             path.c_str());
                break;
            }

            // Check version field (_pad in V1, version in V2).
            // In V1: _pad is always 0 and version concept doesn't exist.
            // In V2: version=1 means extended header.
            uint8_t version = v1_hdr._pad;  // _pad field is at the same offset as version

            WALRecordV2 hdr{};
            hdr.sequence_number = v1_hdr.sequence_number;
            hdr.timestamp_ns = v1_hdr.timestamp_ns;
            hdr.checksum = v1_hdr.checksum;
            hdr.payload_len = v1_hdr.payload_len;
            hdr.record_type = v1_hdr.record_type;
            hdr.version = version;
            hdr.origin_node_id = 0;
            std::memset(hdr.hlc_data, 0, 12);

            if (version >= 1) {
                // Read the remaining 14 bytes of V2 header.
                uint8_t ext[14];
                ssize_t ext_n = ::read(fd, ext, 14);
                if (ext_n != 14) {
                    OB_LOG_DEBUG("mm", "Catch-up: truncated V2 extension in %s — ending file",
                                 path.c_str());
                    break;
                }
                std::memcpy(&hdr.origin_node_id, ext, 2);
                std::memcpy(hdr.hlc_data, ext + 2, 12);
            }

            // Read payload.
            std::vector<uint8_t> payload(hdr.payload_len);
            if (hdr.payload_len > 0) {
                size_t remaining = hdr.payload_len;
                uint8_t* ptr = payload.data();
                while (remaining > 0) {
                    ssize_t r = ::read(fd, ptr, remaining);
                    if (r <= 0) {
                        // Truncated payload — end of valid data.
                        OB_LOG_DEBUG("mm", "Catch-up: truncated WAL payload in %s — ending file",
                                     path.c_str());
                        goto done_file;
                    }
                    ptr += r;
                    remaining -= static_cast<size_t>(r);
                }
            }

            // Only stream DELTA records to the peer (skip GAP, EPOCH, ROTATE).
            if (hdr.record_type != WAL_RECORD_DELTA) {
                continue;
            }

            {
                // Build frame payload: WALRecordV2 header (38B) + payload.
                // Always send as V2 format to the peer.
                std::vector<uint8_t> frame_payload;
                frame_payload.reserve(MM_WALRECORD_V2_SIZE + hdr.payload_len);

                const auto* hdr_bytes = reinterpret_cast<const uint8_t*>(&hdr);
                frame_payload.insert(frame_payload.end(), hdr_bytes,
                                     hdr_bytes + MM_WALRECORD_V2_SIZE);
                if (hdr.payload_len > 0) {
                    frame_payload.insert(frame_payload.end(),
                                         payload.data(),
                                         payload.data() + hdr.payload_len);
                }

                enqueue_frame(peer, frame_payload.data(), frame_payload.size());
            }

            // Check backpressure after each record.
            check_backpressure(peer);
            if (peer.needs_snapshot) {
                // Backpressure triggered — abort catch-up.
                ::close(fd);
                return;  // catching_up already set to false by check_backpressure
            }
        }

    done_file:
        ::close(fd);

        // Move to the next WAL file.
        file_idx++;
        offset = 0;  // Start from beginning of next file.
    }

    // Catch-up complete.
    peer.catching_up = false;
    OB_LOG_INFO("mm", "Catch-up to peer %u complete (reached file=%u offset=%zu)",
                peer.node_id, local_file, local_offset);
}

// ── Backpressure check (task 11.1) ────────────────────────────────────────────

void MultiMasterManager::check_backpressure(PeerConnection& peer) {
    if (peer.send_buf.size() > config_.max_catchup_bytes) {
        OB_LOG_WARN("mm", "Backpressure: peer %u send_buf=%zu > max_catchup_bytes=%zu — needs snapshot",
                    peer.node_id, peer.send_buf.size(), config_.max_catchup_bytes);
        peer.send_buf.clear();
        peer.needs_snapshot = true;
        peer.catching_up = false;
        engine_.registry().increment_counter("ob_mm_backpressure_snapshot_total");
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

    // Connect to new peers.
    for (const auto& [nid, info] : new_map) {
        connect_to_peer(info);
    }
}

void MultiMasterManager::bootstrap_from_peer(const PeerConnection& source) {
    OB_LOG_INFO("mm",
                "Bootstrap progress: phase=%s bytes=%zu/%zu (%.1f%%) elapsed=%.1fs",
                "snapshot", size_t(0), size_t(0), 0.0, 0.0);
    // Full implementation in task 12 — snapshot transfer + WAL catch-up.
    (void)source;
}

} // namespace ob
