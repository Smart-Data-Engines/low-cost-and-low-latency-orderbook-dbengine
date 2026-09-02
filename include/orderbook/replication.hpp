#pragma once

#include "orderbook/async_snapshot.hpp"
#include "orderbook/snapshot.hpp"
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
    bool     compress{false};   // --replication-compress
};

struct ReplicationClientConfig {
    std::string primary_host;
    uint16_t    primary_port{0};  // 0 = no replication
    std::string state_file{"repl_state.txt"};

    // Snapshot bootstrap configuration
    size_t      snapshot_chunk_size{262144};     // --snapshot-chunk-size (default 256 KB)
    std::string snapshot_staging_dir;            // --snapshot-staging-dir (default: <data_dir>/snapshot_staging)
};

// ── Snapshot path validation ──────────────────────────────────────────────────

/// Checks whether `rel` is safe to append to a base directory.
///
/// Snapshot file names arrive from the network, from whichever peer we are
/// bootstrapping against, and are used to build both the staging path and the
/// final destination inside the data directory. Without validation a peer can
/// send `../../../../home/user/.ssh/authorized_keys` and have the replica write
/// a file of the peer's choosing anywhere the process can reach.
///
/// A path is accepted only when all of the following hold:
///   - it is not empty and no longer than `kMaxSnapshotPathLen`
///   - it is relative, not absolute
///   - it contains no `..` component and no `.` component
///   - it contains no empty component (rejects `a//b`) and does not end in `/`
///   - every character is in [A-Za-z0-9._-] or is the `/` separator
///
/// The character allowlist is deliberately narrow: snapshot entries are segment
/// directories and column files that we generate ourselves, so anything outside
/// that set is either a bug or an attack.
[[nodiscard]] bool is_safe_snapshot_path(std::string_view rel);

/// Upper bound on a snapshot entry path, matching the receive buffer.
inline constexpr size_t kMaxSnapshotPathLen = 255;

/// Verifies that `base / rel` resolves to a location inside `base`.
///
/// Defence in depth behind is_safe_snapshot_path(): this one also catches the
/// case where a component of `base` itself is a symlink pointing elsewhere.
/// Returns false if the path escapes, or if the check cannot be performed.
[[nodiscard]] bool path_stays_within(const std::string& base, std::string_view rel);

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
    /// Identifies this connection, not this replica: assigned from a counter on accept, never
    /// reused. A snapshot is now created on a worker thread (#79), so the result can land after the
    /// requester has gone — and a descriptor number alone cannot tell "still here" from "closed and
    /// handed to somebody else".
    uint64_t    conn_id{0};
    std::string address;
    uint32_t    confirmed_file{0};
    size_t      confirmed_offset{0};
    bool        compress{false};  // true after COMPRESS LZ4 directive sent

    // Per-replica send buffer for non-blocking broadcast (EPOLLOUT drain).
    std::vector<uint8_t> send_buf;

    // Per-replica buffered reader for efficient line parsing.
    BufferedReader reader;

    // Per-replica snapshot transfer state (active during SNAPSHOT_REQUEST handling).
    SnapshotTransferState snapshot_transfer;
};

/// A snapshot being created on a worker thread for a replica that asked for one (#79).
struct ReplicaSnapshotPrepare {
    bool     active{false};
    int      fd{-1};
    /// Which connection asked. A descriptor number on its own cannot say that.
    uint64_t conn_id{0};
    uint64_t token{0};
    std::chrono::steady_clock::time_point started_at{};
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

    /// Whether the replication loop is running.
    ///
    /// Exposed because it is the state `stop()`'s early return reads, and that return used to mean
    /// *stopping* rather than *stopped* - a distinction a test cannot make without seeing this.
    bool is_running() const { return running_.load(std::memory_order_acquire); }

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

    /// Serialises `stop()`, so that its early return means *stopped* rather than *stopping*.
    ///
    /// The old guard was `if (!running_) return;` followed by `running_ = false;` and only then the
    /// join. A second caller therefore saw `false` and returned **without joining** — and its next
    /// act was usually destroying this object, whose destructor calls `stop()` and hits the same
    /// guard. So a joinable `std::thread` was destroyed, which calls `std::terminate`: the node died
    /// with `SIGABRT` and printed `terminate called without an active exception`.
    ///
    /// Reached by a graceful `FAILOVER`: the outgoing primary revokes its own lease, so the
    /// unconditional lease-lost demotion from #82 fires while the handover's own demotion is still
    /// running, and both call `stop()` on this manager. Diagnosed from the node's own log in #86,
    /// fixed as #88.
    ///
    /// Held across the join on purpose. Releasing it first would let the second caller return while
    /// the first is still inside this object, which is the same hazard wearing a smaller window.
    /// Not `mtx_`: the epoll thread takes that one, so holding it across a join is the deadlock
    /// pitfall 41 came from.
    ///
    /// Lock order is `stop_mtx_` → `mtx_`, and only that: `stop()` takes this one and then `mtx_`
    /// to close the replica descriptors, while `run_loop()` and `broadcast()` take `mtx_` and never
    /// this one. Verified rather than assumed, because holding a mutex across a `join()` is only
    /// safe while the joined thread cannot want it — neither worker calls `stop()` nor stores
    /// `running_`. A future path taking `mtx_` and then this one would invert that and deadlock.
    std::mutex stop_mtx_;


    /// Creating a snapshot is a flush plus a checksum of the whole store, which used to happen on
    /// this manager's own epoll thread (#79).
    AsyncSnapshotBuilder   snapshot_builder_;
    ReplicaSnapshotPrepare snapshot_prepare_;
    uint64_t               next_snapshot_token_{1};

    /// Source of ReplicaInfo::conn_id. Atomic rather than mutex-protected because it is read on the
    /// accept path before the record joins `replicas_`, where the mutex starts applying.
    std::atomic<uint64_t>      next_conn_id_{1};

    void run_loop();
    void accept_replica();
    void handle_replica_data(int fd);
    void send_to_replica(ReplicaInfo& replica, const WALRecord& hdr,
                         const void* payload, size_t payload_len);
    void handle_catchup(ReplicaInfo& replica, uint32_t from_file, size_t from_offset);

    /// Handle a SNAPSHOT_REQUEST from a replica: hand the creation to a worker thread.
    void handle_snapshot_request(ReplicaInfo& replica);

    /// Begin streaming a snapshot a worker has finished creating.
    void begin_snapshot_transfer(ReplicaInfo& replica, SnapshotManifest&& manifest);

    /// Collect a finished snapshot, if there is one, and act on it.
    ///
    /// Called once per pass of run_loop(), which has a 100 ms timeout — so unlike the multi-master
    /// side there is no notification and none is needed: a result is picked up within 100 ms of
    /// being ready, against a creation measured in milliseconds to seconds. Adding an eventfd to a
    /// manager that has none would buy that back and cost a descriptor and a wake-up path. Written
    /// down so the absence reads as a decision rather than an oversight.
    void poll_snapshot_preparation();

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

    /// The socket to the primary. Atomic because two threads read it and one writes it: the receive
    /// thread owns its lifecycle, while `stop()` and `state()` are called from elsewhere.
    ///
    /// ThreadSanitizer reported the write in `run_loop()` against the read in `stop()` as soon as
    /// this library was actually instrumented (#83). A torn `int` is the smaller half of it — the
    /// larger is that `stop()` read the descriptor and then called `shutdown()` on it, so the
    /// receive thread could close and the kernel reassign the number in between. That is the third
    /// appearance of the same shape in this codebase (pitfalls 41 and 49).
    std::atomic<int>        fd_{-1};

    /// Serialises "close the socket and forget it" against "shut the socket down to wake the read".
    ///
    /// `shutdown()` is the right call in `stop()` and cannot be replaced by a flag: unlike
    /// `close()`, it genuinely wakes a blocked `recv()`. What it needs is for the descriptor not to
    /// be closed underneath it, which one mutex on the lifecycle gives — on the connect and shutdown
    /// paths only, never on the receive path.
    mutable std::mutex      fd_mtx_;

    /// Serialises `stop()`, for the reason `ReplicationManager::stop_mtx_` documents at length: the
    /// early return used to mean *stopping* rather than *stopped*, so a second caller skipped the
    /// join and then destroyed a joinable thread. The same shape, one class away — which is why both
    /// were changed together rather than the one that was observed to abort.
    std::mutex              stop_mtx_;

    /// Where replay has got to, and how much of it there has been.
    ///
    /// Atomic because `state()` reads them for `STATUS` and `/metrics` while the receive thread
    /// advances them — which ThreadSanitizer reported as three data races per run once this library
    /// was instrumented (#83). The consequence was mild, since these are diagnostics rather than
    /// decisions, but "mild" was not established anywhere: an unsynchronised `size_t` is a torn read
    /// by the language's rules whatever the hardware does, and the three were reported as a triple
    /// nobody had made consistent.
    ///
    /// `snapshot_bytes_received_` below was already atomic for exactly this reason. These four were
    /// missed.
    ///
    /// Relaxed ordering throughout: the receive thread is the only writer, and a reader wants a
    /// recent value rather than a synchronised one.
    std::atomic<uint32_t> confirmed_file_{0};
    std::atomic<size_t>   confirmed_offset_{0};
    std::atomic<uint64_t> records_replayed_{0};
    std::atomic<uint64_t> local_epoch_{0};

    // Snapshot bootstrap state
    std::atomic<bool> bootstrapping_{false};
    std::atomic<size_t> snapshot_bytes_received_{0};
    std::atomic<size_t> snapshot_bytes_total_{0};

    // Compression flag — set when primary sends COMPRESS LZ4 directive.
    bool compress_{false};

    // Client-side buffered reader for efficient line parsing from primary.
    BufferedReader reader_;

    void run_loop();
    void connect_to_primary();
    void receive_and_replay();

    /// Close the socket and forget it, once, under fd_mtx_.
    void close_socket();
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
