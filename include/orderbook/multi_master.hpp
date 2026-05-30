#pragma once

// ── MultiMasterManager — peer networking, WAL broadcast, loop prevention ─────
//
// Central component managing multi-master replication: epoll-based peer
// connections, WAL record broadcast, origin-based loop prevention, bootstrap
// state management, and diagnostic commands (MM_PEERS, MM_CONFLICTS).
//
// Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 4.7, 4.8, 9.1, 9.2, 9.3, 9.4

#include "orderbook/anti_entropy.hpp"
#include "orderbook/conflict_resolver.hpp"
#include "orderbook/hlc.hpp"
#include "orderbook/peer_registry.hpp"
#include "orderbook/replication.hpp"
#include "orderbook/wal.hpp"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace ob {

// ── Protocol constants ────────────────────────────────────────────────────────

inline constexpr uint16_t MM_PROTOCOL_VERSION   = 1;
inline constexpr size_t   MM_FRAME_HEADER_SIZE  = 4;            // uint32 LE length
inline constexpr size_t   MM_HANDSHAKE_SIZE     = 17;           // HandshakeMessage wire size
inline constexpr size_t   MM_MAX_FRAME_PAYLOAD  = 64ULL << 20;  // 64 MB
inline constexpr size_t   MM_WALRECORD_V2_SIZE  = 38;           // WALRecordV2 header size

// ── HandshakeMessage ──────────────────────────────────────────────────────────
//
// Binary layout (17 bytes, little-endian):
//   offset 0:  node_id                (uint16 LE)
//   offset 2:  protocol_version       (uint16 LE)
//   offset 4:  compression_preference (uint8)
//   offset 5:  wal_file_index         (uint32 LE)
//   offset 9:  wal_byte_offset        (uint64 LE)

struct HandshakeMessage {
    uint16_t node_id{0};
    uint16_t protocol_version{1};
    uint8_t  compression_preference{0};  // 0=none, 1=LZ4
    uint32_t wal_file_index{0};
    uint64_t wal_byte_offset{0};

    /// Serialize to 17-byte LE buffer.
    void serialize(uint8_t out[MM_HANDSHAKE_SIZE]) const;

    /// Deserialize from buffer. Returns false if buffer too short (<17 bytes).
    static bool deserialize(const uint8_t* data, size_t len, HandshakeMessage& out);

    /// Pretty-print for diagnostics.
    std::string to_string() const;

    /// Equality operators.
    bool operator==(const HandshakeMessage& o) const;
    bool operator!=(const HandshakeMessage& o) const;
};

// ── ReconnectBackoff ──────────────────────────────────────────────────────────
//
// Exponential backoff with jitter for reconnect attempts.
// Formula: base_delay = min(initial_delay * 2^attempt, max_delay)
//          actual_delay = base_delay + uniform_random(-jitter_range, +jitter_range)
//          where jitter_range = base_delay * jitter_fraction

struct ReconnectBackoff {
    uint32_t attempt{0};

    static constexpr double initial_delay_s  = 1.0;
    static constexpr double max_delay_s      = 30.0;
    static constexpr double jitter_fraction  = 0.25;
    static constexpr double multiplier       = 2.0;

    /// Calculate next delay in milliseconds (includes jitter).
    /// Increments attempt counter.
    uint32_t next_delay_ms();

    /// Reset after successful connection.
    void reset() { attempt = 0; }
};

// ── Frame encode/decode ────────────────────────────────────────────────────────
//
// Length-prefixed framing: 4-byte LE uint32 length header + payload bytes.
// encode_frame appends a complete frame to the output vector.
// parse_frames scans recv_buf for complete frames, returns payload offsets,
// and erases consumed bytes from recv_buf.

/// Encode a single frame: appends [4B LE length | payload] to `out`.
void encode_frame(const void* payload, size_t len, std::vector<uint8_t>& out);

/// Parse complete frames from recv_buf.
/// On success, fills frames_out with (offset, length) pairs pointing to payload
/// positions within recv_buf BEFORE erasure, then erases consumed bytes.
/// Returns 0 on success, -1 on protocol error (frame length > MM_MAX_FRAME_PAYLOAD).
int parse_frames(std::vector<uint8_t>& recv_buf,
                 std::vector<std::pair<size_t, size_t>>& frames_out);

class Engine;  // forward — full integration comes in task 12

// ── Multi-master configuration ────────────────────────────────────────────────

struct MultiMasterConfig {
    uint16_t    node_id{0};                       // --mm-node-id (required)
    uint16_t    replication_port{0};              // --mm-replication-port
    bool        enabled{false};                   // --multi-master
    bool        compress{false};                  // --replication-compress
    size_t      max_catchup_bytes{512ULL << 20};  // --mm-max-catchup-bytes (512MB)
    uint32_t    anti_entropy_interval_sec{30};    // --anti-entropy-interval-seconds
    std::string shard_id;                         // optional, if sharding active
    CoordinatorConfig coordinator_config;         // etcd endpoints for peer discovery
};

// ── Peer connection state ─────────────────────────────────────────────────────

struct PeerConnection {
    uint16_t     node_id{0};
    std::string  address;            // host:port
    int          fd{-1};             // socket fd (-1 = disconnected)
    bool         connected{false};
    bool         handshake_done{false};  // handshake completed
    bool         compress{false};    // LZ4 negotiated
    uint32_t     confirmed_file{0};
    size_t       confirmed_offset{0};
    HLCTimestamp last_hlc;           // last HLC received from this peer

    // Send buffer (non-blocking)
    std::vector<uint8_t> send_buf;

    // Receive buffer (simple byte buffer for incoming data)
    std::vector<uint8_t> recv_buf;

    // Reconnect state
    ReconnectBackoff backoff;
    std::chrono::steady_clock::time_point next_reconnect_time{};

    // Catch-up state
    bool catching_up{false};
    bool needs_snapshot{false};
};

// ── MultiMasterManager ────────────────────────────────────────────────────────

class MultiMasterManager {
public:
    explicit MultiMasterManager(MultiMasterConfig config, Engine& engine,
                                WALWriter& wal, HybridLogicalClock& hlc);
    ~MultiMasterManager();

    MultiMasterManager(const MultiMasterManager&) = delete;
    MultiMasterManager& operator=(const MultiMasterManager&) = delete;

    /// Start: bind replication port, register in etcd, connect to peers.
    void start();

    /// Stop: disconnect peers, deregister from etcd.
    void stop();

    /// Broadcast a locally-originated WAL record to all connected peers.
    /// Called by Engine after apply_delta + WAL append.
    /// NOTE: does NOT broadcast records received from replication (origin != local).
    void broadcast_local(const WALRecordV2& hdr, const void* payload,
                         size_t payload_len);

    /// Handle an incoming WAL record from a peer.
    /// Returns true if the record was applied (not a duplicate/loop).
    bool handle_remote_record(uint16_t peer_node_id,
                              const WALRecordV2& hdr,
                              const void* payload, size_t payload_len);

    /// Get peer connection states (for STATUS/MM_PEERS commands).
    std::vector<PeerConnection> peer_states() const;

    /// Get the number of connected peers.
    size_t connected_peer_count() const;

    /// Get conflict resolver (for MM_CONFLICTS command).
    const ConflictResolver& conflict_resolver() const { return *conflict_resolver_; }

    /// Get anti-entropy manager.
    AntiEntropyManager& anti_entropy() { return *anti_entropy_; }

    /// Check if this manager is in bootstrap state.
    bool is_bootstrapping() const { return bootstrapping_.load(std::memory_order_acquire); }

    /// Initiate bootstrap from a peer.
    void start_bootstrap();

    /// Handle MM_PEERS command — return TSV response.
    std::string handle_mm_peers_command() const;

    /// Handle MM_CONFLICTS command — return TSV response.
    std::string handle_mm_conflicts_command(size_t limit = 100) const;

    /// Get the config (for testing).
    const MultiMasterConfig& config() const { return config_; }

private:
    MultiMasterConfig config_;
    Engine& engine_;
    WALWriter& wal_;
    HybridLogicalClock& hlc_;

    std::unique_ptr<PeerRegistry> peer_registry_;
    std::unique_ptr<ConflictResolver> conflict_resolver_;
    std::unique_ptr<AntiEntropyManager> anti_entropy_;

    mutable std::mutex mtx_;
    std::unordered_map<uint16_t, PeerConnection> peers_;

    // Networking
    int listen_fd_{-1};
    int epoll_fd_{-1};
    std::thread io_thread_;
    std::thread reconnect_thread_;
    std::atomic<bool> running_{false};
    std::atomic<bool> bootstrapping_{false};

    // Internal methods
    void io_loop();
    void connect_to_peer(const PeerInfo& peer);
    void disconnect_peer(uint16_t node_id);
    void handle_peer_data(uint16_t node_id);
    void send_to_peer(PeerConnection& peer, const WALRecordV2& hdr,
                      const void* payload, size_t payload_len);
    void handle_catchup_request(PeerConnection& peer, uint32_t from_file,
                                size_t from_offset);
    void handle_topology_change(const std::vector<PeerInfo>& new_peers);
    void bootstrap_from_peer(const PeerConnection& source);

    // Frame-based send methods (task 5.1)
    /// Encode payload into a Frame and append to peer.send_buf, then try to drain.
    void enqueue_frame(PeerConnection& peer, const void* payload, size_t len);

    /// Attempt to drain peer.send_buf via send(MSG_NOSIGNAL).
    /// Handles partial write (erase sent bytes), EAGAIN (arm EPOLLOUT),
    /// EPIPE/ECONNRESET (disconnect + reconnect).
    /// Returns false if peer was disconnected.
    bool try_drain_send_buf(PeerConnection& peer);

    /// Arm EPOLLOUT for a peer's fd (when send_buf is non-empty after EAGAIN).
    void arm_epollout(PeerConnection& peer);

    /// Disarm EPOLLOUT for a peer's fd (when send_buf is fully drained).
    void disarm_epollout(PeerConnection& peer);

    // Frame receive/parse methods (task 6.1)
    /// Process all complete frames in peer.recv_buf.
    /// Parses length-prefixed frames, dispatches to handle_frame, removes consumed bytes.
    void process_recv_buf(PeerConnection& peer);

    /// Handle a single parsed frame payload.
    /// If handshake not done: calls process_handshake.
    /// Otherwise: parses WALRecordV2 header + payload, calls handle_remote_record.
    void handle_frame(PeerConnection& peer, const uint8_t* data, size_t len);

    /// Process incoming handshake from peer.
    void process_handshake(PeerConnection& peer, const uint8_t* data, size_t len);

    // Handshake send (task 7.1)
    /// Send our handshake message to a peer (called after connect or accept).
    void send_handshake(PeerConnection& peer);

    // Reconnect logic (task 10.1)
    /// Mark peer as disconnected, close fd, log INFO. Schedules reconnect.
    void schedule_reconnect(uint16_t node_id);

    /// Reconnect thread loop: periodically attempts to reconnect disconnected peers.
    void reconnect_loop();

    // Catch-up streaming (task 8.1)
    /// Start streaming WAL records from peer's confirmed position to current.
    void start_catchup_to_peer(PeerConnection& peer);

    // Backpressure (task 11.1)
    /// Check if peer's send_buf exceeds max_catchup_bytes threshold.
    /// If so: clear send_buf, set needs_snapshot = true, set catching_up = false.
    void check_backpressure(PeerConnection& peer);
};

} // namespace ob
