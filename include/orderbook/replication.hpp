#pragma once

#include "orderbook/wal.hpp"

#include <atomic>
#include <cerrno>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <deque>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include <sys/socket.h>

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

    // Snapshot bootstrap configuration
    size_t      snapshot_chunk_size{262144};     // --snapshot-chunk-size (default 256 KB)
    std::string snapshot_staging_dir;            // --snapshot-staging-dir (default: <data_dir>/snapshot_staging)
};

// ── Snapshot manifest ─────────────────────────────────────────────────────────

struct SnapshotFileEntry {
    std::string path;       // relative to data dir
    size_t      size{0};    // file size in bytes
    uint32_t    crc32c{0};  // CRC32C of file contents
};

struct SnapshotManifest {
    uint32_t    wal_file_index{0};
    size_t      wal_byte_offset{0};
    size_t      total_bytes{0};
    size_t      total_rows{0};
    uint64_t    created_at_ns{0};
    std::vector<SnapshotFileEntry> files;

    /// Serialize to JSON string (deterministic alphabetical field ordering).
    std::string to_json() const;

    /// Parse from JSON string. Returns true on success.
    static bool from_json(std::string_view json, SnapshotManifest& out);
};

// ── Snapshot transfer state (per-replica, primary side) ───────────────────────

struct SnapshotTransferState {
    bool                active{false};
    SnapshotManifest    manifest;
    size_t              current_file_idx{0};
    size_t              current_file_offset{0};
    int                 current_file_fd{-1};
    bool                header_sent{false};     // SNAPSHOT_FILE header sent for current file
    bool                begin_sent{false};
    std::string         base_dir;
    size_t              chunk_size{262144};
};

// ── BufferedReader ────────────────────────────────────────────────────────────
// Reads from a socket in chunks (default 4 KB) and provides line-oriented
// access. Eliminates the byte-by-byte recv() overhead of the old read_line().

class BufferedReader {
public:
    static constexpr size_t DEFAULT_BUF_SIZE = 4096;

    explicit BufferedReader(size_t buf_size = DEFAULT_BUF_SIZE)
        : buf_(buf_size), pos_(0), end_(0) {}

    /// Set the file descriptor to read from.
    void set_fd(int fd) { fd_ = fd; pos_ = 0; end_ = 0; }

    /// Read exactly `len` bytes into `out`. Uses buffered data first, then reads
    /// from the socket for the remainder. Returns true on success, false on error/disconnect.
    bool read_exact(void* out, size_t len) {
        auto* dst = static_cast<uint8_t*>(out);
        size_t written = 0;

        // 1. Drain from internal buffer first.
        const size_t buffered = end_ - pos_;
        if (buffered > 0) {
            const size_t take = (buffered < len) ? buffered : len;
            std::memcpy(dst, buf_.data() + pos_, take);
            pos_ += take;
            written += take;
        }

        // 2. Read remainder directly from socket (bypass buffer for large reads).
        while (written < len) {
            ssize_t n = ::recv(fd_, dst + written, len - written, 0);
            if (n <= 0) return false;
            written += static_cast<size_t>(n);
        }
        return true;
    }

    /// Read a newline-terminated line into `out` (without the trailing '\n').
    /// Returns:  >0 = line length,  0 = EAGAIN (no complete line yet),  -1 = error/disconnect.
    ssize_t read_line(char* out, size_t out_size) {
        // Scan existing buffer for a newline.
        for (;;) {
            for (size_t i = pos_; i < end_; ++i) {
                if (buf_[i] == '\n') {
                    const size_t line_len = i - pos_;
                    const size_t copy_len = (line_len < out_size - 1) ? line_len : out_size - 1;
                    std::memcpy(out, buf_.data() + pos_, copy_len);
                    out[copy_len] = '\0';
                    pos_ = i + 1; // skip past '\n'
                    return static_cast<ssize_t>(copy_len);
                }
            }

            // No newline found — compact and refill.
            if (pos_ > 0) {
                const size_t remaining = end_ - pos_;
                if (remaining > 0) {
                    std::memmove(buf_.data(), buf_.data() + pos_, remaining);
                }
                end_ = remaining;
                pos_ = 0;
            }

            // Buffer full without a newline — protocol error.
            if (end_ >= buf_.size()) {
                // Return what we have as a line (truncated).
                const size_t copy_len = (end_ < out_size - 1) ? end_ : out_size - 1;
                std::memcpy(out, buf_.data(), copy_len);
                out[copy_len] = '\0';
                pos_ = 0;
                end_ = 0;
                return static_cast<ssize_t>(copy_len);
            }

            // Read a chunk from the socket.
            ssize_t n = ::recv(fd_, buf_.data() + end_, buf_.size() - end_, 0);
            if (n == 0) return -1; // disconnect
            if (n < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    return 0; // no data available
                }
                return -1; // error
            }
            end_ += static_cast<size_t>(n);
        }
    }

private:
    int fd_{-1};
    std::vector<char> buf_;
    size_t pos_;
    size_t end_;
};

// ── ReplicaInfo ───────────────────────────────────────────────────────────────

struct ReplicaInfo {
    int         fd{-1};
    std::string address;
    uint32_t    confirmed_file{0};
    size_t      confirmed_offset{0};

    // Per-replica send buffer for non-blocking broadcast (EPOLLOUT drain).
    std::vector<uint8_t> send_buf;

    // Per-replica buffered reader for efficient line parsing.
    BufferedReader reader;

    // Per-replica snapshot transfer state (active during SNAPSHOT_REQUEST handling).
    SnapshotTransferState snapshot_transfer;
};

// ── ReplicationManager (primary side) ─────────────────────────────────────────

class ReplicationManager {
public:
    explicit ReplicationManager(ReplicationConfig config, WALWriter& wal);
    ~ReplicationManager();

    ReplicationManager(const ReplicationManager&) = delete;
    ReplicationManager& operator=(const ReplicationManager&) = delete;

    /// Set the Engine pointer so the manager can call create_snapshot().
    void set_engine(Engine* engine) { engine_ = engine; }

    /// Start the replication server (binds port, starts epoll thread).
    void start();

    /// Stop the replication server.
    void stop();

    /// Broadcast a WAL record to all connected replicas (non-blocking enqueue).
    /// Called by Engine after WALWriter::append().
    void broadcast(const WALRecord& hdr, const void* payload, size_t payload_len);

    /// Get current replica states (for STATUS command).
    std::vector<ReplicaInfo> replica_states() const;

    /// Returns true if any replica is currently receiving a snapshot.
    bool snapshot_active() const;

private:
    ReplicationConfig config_;
    WALWriter&        wal_;
    Engine*           engine_{nullptr};
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

    /// Handle a SNAPSHOT_REQUEST from a replica: create snapshot and begin streaming.
    void handle_snapshot_request(ReplicaInfo& replica);

    /// Continue streaming snapshot data to a replica (called on EPOLLOUT).
    /// Returns false if the transfer failed and the replica should be removed.
    bool continue_snapshot_transfer(ReplicaInfo& replica);

    /// Enqueue bytes into a replica's send buffer and arm EPOLLOUT if needed.
    void enqueue_send(ReplicaInfo& replica, const void* data, size_t len);

    /// Drain a replica's send buffer. Returns false if the replica should be removed.
    bool drain_send_buffer(ReplicaInfo& replica);

    /// Remove a replica by fd (closes fd, removes from epoll and replicas_ list).
    /// Caller must hold mtx_.
    void remove_replica_locked(int fd);
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

    /// Returns true if the client is currently bootstrapping from a snapshot.
    bool is_bootstrapping() const { return bootstrapping_.load(std::memory_order_acquire); }

    /// Get current replication state.
    struct State {
        uint32_t confirmed_file;
        size_t   confirmed_offset;
        bool     connected;
        uint64_t records_replayed;
        bool     bootstrapping;
        size_t   snapshot_bytes_received;
        size_t   snapshot_bytes_total;
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

    // Snapshot bootstrap state
    std::atomic<bool> bootstrapping_{false};
    std::atomic<size_t> snapshot_bytes_received_{0};
    std::atomic<size_t> snapshot_bytes_total_{0};

    // Client-side buffered reader for efficient line parsing from primary.
    BufferedReader reader_;

    void run_loop();
    void connect_to_primary();
    void receive_and_replay();
    void send_ack();
    void save_state();
    void load_state();

    /// Handle snapshot bootstrap: send SNAPSHOT_REQUEST, receive files, verify, load.
    void request_and_receive_snapshot();

    /// Move staged files into data directory and load columnar index.
    void install_snapshot(const std::string& staging_dir, const SnapshotManifest& manifest);

    /// Clean up the staging directory.
    void cleanup_staging(const std::string& staging_dir);
};

} // namespace ob
