#pragma once

#include "orderbook/wal.hpp"

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

namespace ob {

// Forward declaration — Engine is defined in engine.hpp.
class Engine;

// ── Replication configuration ─────────────────────────────────────────────────

struct ReplicationConfig {
    uint16_t port{0};           // 0 = disabled
    int      max_replicas{4};
};

struct ReplicationClientConfig {
    std::string primary_host;
    uint16_t    primary_port{0};  // 0 = no replication
    std::string state_file{"repl_state.txt"};
};

// ── ReplicaInfo ───────────────────────────────────────────────────────────────

struct ReplicaInfo {
    int         fd{-1};
    std::string address;
    uint32_t    confirmed_file{0};
    size_t      confirmed_offset{0};
};

// ── ReplicationManager (primary side) ─────────────────────────────────────────

class ReplicationManager {
public:
    explicit ReplicationManager(ReplicationConfig config, WALWriter& wal);
    ~ReplicationManager();

    ReplicationManager(const ReplicationManager&) = delete;
    ReplicationManager& operator=(const ReplicationManager&) = delete;

    /// Start the replication server (binds port, starts epoll thread).
    void start();

    /// Stop the replication server.
    void stop();

    /// Broadcast a WAL record to all connected replicas.
    /// Called by Engine after WALWriter::append().
    void broadcast(const WALRecord& hdr, const void* payload, size_t payload_len);

    /// Get current replica states (for STATUS command).
    std::vector<ReplicaInfo> replica_states() const;

private:
    ReplicationConfig config_;
    WALWriter&        wal_;
    std::thread       thread_;
    std::atomic<bool> running_{false};
    int               listen_fd_{-1};
    int               epoll_fd_{-1};

    mutable std::mutex         mtx_;
    std::vector<ReplicaInfo>   replicas_;

    void run_loop();
    void accept_replica();
    void handle_replica_data(int fd);
    void send_to_replica(ReplicaInfo& replica, const WALRecord& hdr,
                         const void* payload, size_t payload_len);
    void handle_catchup(ReplicaInfo& replica, uint32_t from_file, size_t from_offset);
};

// ── ReplicationClient (replica side) ──────────────────────────────────────────

class ReplicationClient {
public:
    explicit ReplicationClient(ReplicationClientConfig config, Engine& engine);
    ~ReplicationClient();

    ReplicationClient(const ReplicationClient&) = delete;
    ReplicationClient& operator=(const ReplicationClient&) = delete;

    /// Start the replication client (connects to primary, starts receive thread).
    void start();

    /// Stop the replication client.
    void stop();

    /// Get current replication state.
    struct State {
        uint32_t confirmed_file;
        size_t   confirmed_offset;
        bool     connected;
        uint64_t records_replayed;
    };
    State state() const;

private:
    ReplicationClientConfig config_;
    Engine&                 engine_;
    std::thread             thread_;
    std::atomic<bool>       running_{false};
    int                     fd_{-1};

    uint32_t confirmed_file_{0};
    size_t   confirmed_offset_{0};
    uint64_t records_replayed_{0};

    void run_loop();
    void connect_to_primary();
    void receive_and_replay();
    void send_ack();
    void save_state();
    void load_state();
};

} // namespace ob
