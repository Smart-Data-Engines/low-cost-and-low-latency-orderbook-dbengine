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
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace ob {

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
};

// ── Peer connection state ─────────────────────────────────────────────────────

struct PeerConnection {
    uint16_t     node_id{0};
    std::string  address;            // host:port
    int          fd{-1};             // socket fd (-1 = disconnected)
    bool         connected{false};
    bool         compress{false};    // LZ4 negotiated
    uint32_t     confirmed_file{0};
    size_t       confirmed_offset{0};
    HLCTimestamp last_hlc;           // last HLC received from this peer

    // Send buffer (non-blocking)
    std::vector<uint8_t> send_buf;

    // Receive buffer (simple byte buffer for incoming data)
    std::vector<uint8_t> recv_buf;
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
    std::thread accept_thread_;
    std::thread receive_thread_;
    std::atomic<bool> running_{false};
    std::atomic<bool> bootstrapping_{false};

    // Internal methods
    void accept_loop();
    void receive_loop();
    void connect_to_peer(const PeerInfo& peer);
    void disconnect_peer(uint16_t node_id);
    void handle_peer_data(uint16_t node_id);
    void send_to_peer(PeerConnection& peer, const WALRecordV2& hdr,
                      const void* payload, size_t payload_len);
    void handle_catchup_request(PeerConnection& peer, uint32_t from_file,
                                size_t from_offset);
    void handle_topology_change(const std::vector<PeerInfo>& new_peers);
    void bootstrap_from_peer(const PeerConnection& source);
};

} // namespace ob
