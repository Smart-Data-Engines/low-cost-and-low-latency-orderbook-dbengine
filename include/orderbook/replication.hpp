#pragma once

#include "orderbook/auth.hpp"
#include "orderbook/tls.hpp"

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

    /// Cluster secret (#30 part two). Empty = this link does not authenticate.
    ///
    /// A credential in a config struct, which `ServerConfig` deliberately refuses - the difference
    /// is that nothing renders this one. `format_config()` walks every field of `ServerConfig` and
    /// prints it, so a secret there would be printed by the command that exists to be pasted into a
    /// ticket. If anything ever renders this struct, the secret moves out of it.
    SecretStore cluster_secret;

    /// TLS for accepted replica connections (#30 part three, series D). Null = plaintext.
    ///
    /// A `node_server` context: it presents this node's certificate and **requires** one from the
    /// replica, so there is no flag here saying whether to verify. Built once at startup, so a
    /// certificate that does not load stops the process rather than every handshake.
    std::shared_ptr<TlsContext> tls_server;
};

struct ReplicationClientConfig {
    std::string primary_host;
    uint16_t    primary_port{0};  // 0 = no replication
    std::string state_file{"repl_state.txt"};

    /// Cluster secret (#30 part two). Empty = do not authenticate, and then a primary that *does*
    /// authenticate refuses this replica before `REPLICATE`.
    SecretStore cluster_secret;

    /// TLS for the connection to the primary (#30 part three, series D). Null = plaintext.
    ///
    /// A `node_client` context. The name this side requires is `primary_host`, pinned through
    /// `tls_expect_host()` - and since `connect_to_primary()` resolves nothing (`inet_pton` only),
    /// that name is always an address, so the primary's certificate needs an `IP:` SAN.
    std::shared_ptr<TlsContext> tls_client;

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
    void set_fd(int fd) { fd_ = fd; pos_ = 0; end_ = 0; tls_.reset(); }

    /// Read through TLS instead of straight off the descriptor. Call after `set_fd`.
    ///
    /// Holds the channel by `shared_ptr` and not a pointer to the record that owns it: `ReplicaInfo`
    /// lives in a `std::vector` whose `push_back` moves its elements, so a pointer into the enclosing
    /// record dangles from the first reallocation - and the symptom would be corrupt bytes on the
    /// sixth replica rather than anything that reads as a lifetime bug.
    void set_tls(std::shared_ptr<TlsChannel> tls) { tls_ = std::move(tls); }

    /// What OpenSSL wanted after the last attempt that produced nothing, or `Read` on a plaintext
    /// reader. The caller arms this: a TLS *read* can need the socket to become writable.
    IoWant io_want() const { return tls_ ? tls_->io_want() : IoWant::Read; }

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
            ssize_t n = pull(dst + written, len - written);
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
            ssize_t n = pull(buf_.data() + end_, buf_.size() - end_);
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
    /// One receive attempt, with `::recv`'s convention: >0 bytes, 0 = the peer closed, -1 with
    /// `errno == EAGAIN` = nothing available now, -1 otherwise = error.
    ///
    /// Out of line because the TLS branch needs OpenSSL's headers and this one is included by
    /// everything that holds a replication config. Keeping `read_exact` and `read_line` inline keeps
    /// the per-record path free of a call: `read_line` reaches here only when it needs bytes.
    ///
    /// The errno channel is deliberate - it makes the TLS transport a drop-in for `::recv`, so the
    /// two readers above keep their logic unchanged. What it cannot carry is *which* want OpenSSL
    /// has, and an edge-triggered caller needs that: a TLS read wanting to **write** must arm
    /// EPOLLOUT rather than wait for readability that is not coming. Hence `io_want()`.
    ssize_t pull(void* dst, size_t len);

    int fd_{-1};
    std::shared_ptr<TlsChannel> tls_;
    std::vector<char> buf_;
    size_t pos_;
    size_t end_;
};

// ── CatchupCursor ─────────────────────────────────────────────────────────────

/// How far a catch-up has streamed, so the pass can stop and be resumed (#93).
///
/// `handle_catchup()` used to stream the whole requested range in one synchronous pass, which put
/// the weight of that range into the send queue: past the 16 MB ceiling the replica was dropped, it
/// reconnected, asked again and was dropped again. This is the shape `SnapshotTransferState` already
/// gives the snapshot stream, one function away in this file.
struct CatchupCursor {
    bool     active{false};

    /// The next byte to send: which WAL file, and where in it. A real position, unlike the one the
    /// wire carries (#98).
    uint32_t file{0};
    size_t   offset{0};

    /// Where the stream ends. Fixed when the cursor is created rather than chased, and that is what
    /// keeps a record from being sent twice.
    ///
    /// The end is the WAL's append position at that moment. A record appended afterwards sits at or
    /// past it, so the cursor never reads it - and its broadcast waits in `pending` instead, because
    /// a live record may not overtake the history in front of it. A cursor that chased the live end
    /// would instead read a record whose own `broadcast()` was still blocked on `mtx_`, and that
    /// record would go out twice: once from the file, once from the call that was waiting.
    uint32_t through_file{0};
    size_t   through_offset{0};

    /// Live records that arrived while this cursor was streaming, framed exactly as `broadcast()`
    /// would have queued them - compressed too, if this replica asked for that. Appended to
    /// `send_buf` when the cursor reaches its end.
    std::vector<uint8_t> pending;

    /// Send `COMPRESS LZ4` when the cursor finishes. The catch-up stream is plain text, so the
    /// directive cannot go out before its last byte - the reason it was sent after the synchronous
    /// pass, kept as the reason it is sent from the cursor's end.
    bool     compress_after{false};
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

    /// Authentication state (#30 part two), meaningful only when a cluster secret is configured.
    ///
    /// One flag, not two, and the asymmetry is deliberate: this side refuses to serve `REPLICATE`
    /// until the replica has proved itself, and the replica refuses to *send* `REPLICATE` until we
    /// have proved ourselves. Mutual authentication falls out of both sides applying that rule, so
    /// neither has to track its own proof.
    bool        peer_proved{false};
    /// The nonce we challenged this connection with. Single-use: cleared when it is answered.
    std::string auth_nonce;

    /// TLS state for this connection (#30 part three, series D). Null = plaintext.
    std::shared_ptr<TlsChannel> tls;

    /// Who the replica's certificate says it is, once the handshake completes. Empty otherwise.
    ///
    /// The field requirement 8.4 of part one asked for: the cluster form of a secret file carries no
    /// identity, because a node's identity was its `node_id` from a handshake that authentication
    /// precedes - so mTLS is the first thing on this link that has one. Read by the log line today
    /// and by the ACLs of #31 when they exist, from here rather than from a second source.
    std::string identity;

    // Per-replica send buffer for non-blocking broadcast (EPOLLOUT drain).
    std::vector<uint8_t> send_buf;

    // Per-replica buffered reader for efficient line parsing.
    BufferedReader reader;

    // Per-replica snapshot transfer state (active during SNAPSHOT_REQUEST handling).
    SnapshotTransferState snapshot_transfer;

    // Per-replica catch-up state (active while a requested WAL range is being streamed).
    CatchupCursor catchup;
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

    /// Stream the next batch of a replica's catch-up.
    ///
    /// Bounded twice over: it stops once `kCatchupBatchBytes` have been queued in this pass, so the
    /// mutex the write path needs is not held for the length of the range, and it stops at half the
    /// send-buffer ceiling, so the queue is never grown to the size of what was asked for.
    ///
    /// Returns nothing, deliberately: a replica that dies mid-batch has already been removed by
    /// `send_to_replica()`, and the caller can read that off `replica.fd`. A bool here would be a
    /// second account of a removal that has happened either way.
    void continue_catchup(ReplicaInfo& replica);

    /// Hand the replica back to live streaming: the directive, then the records that waited.
    void finish_catchup(ReplicaInfo& replica);

    /// Queue a live message: into the send buffer, or behind an unfinished catch-up.
    ///
    /// One function because the choice is a property of the replica rather than of the caller, and
    /// two callers that each decide it are two callers that can disagree - which here would put a
    /// live record in front of the history it belongs after.
    void queue_to_replica(ReplicaInfo& replica, const void* data, size_t len);

    /// Whether some replica's catch-up could queue more bytes right now. Requires `mtx_`.
    ///
    /// This is the run loop's timeout: a cursor with room to write is work in hand, so the loop
    /// must not sit in `epoll_wait` for 100 ms holding it. A cursor with a full queue is *not* work
    /// in hand - polling that would be the busy-spin of pitfall 5 - and EPOLLOUT is what says the
    /// socket drained.
    bool catchup_can_progress_locked() const;

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

    /// Queue bytes and push what the socket takes now.
    ///
    /// For a short message that precedes a disconnect - `ERR unauthenticated`, `OK AUTH`,
    /// `ERR WAL_TRUNCATED`. If the socket will not take them the bytes are lost, which is what the
    /// old direct write did too; the difference is that they now go through TLS when TLS is on, and
    /// that a full socket buffer no longer reads as a failure.
    void enqueue_and_flush(ReplicaInfo& replica, const void* data, size_t len);

    /// Step this replica's TLS handshake and arm what OpenSSL asked for. False = fatal, and the
    /// caller disconnects. True with `tls->handshaking()` still set means "not finished yet".
    bool advance_tls_handshake(ReplicaInfo& replica);

    /// Publish how many connected replicas presented a verified certificate.
    ///
    /// The readable form of the guarantee (requirement 6.6): a guarantee whose state cannot be read
    /// on a live node is a guarantee on our word. A count and not a label, because a label fed by a
    /// peer is an unbounded label set (pitfall 116). Through `engine_`, which is null in the unit
    /// tests that construct this class directly - so the absence of the metric there is a property
    /// of the fixture, not a branch anybody has to remember.
    /// Publish both replica gauges from `replicas_`. Requires `mtx_` held.
    ///
    /// Both, from one loop, and from the run loop's every-pass tick rather than only from the
    /// handshake: the verified count used to be published *only* where it goes up, so a replica
    /// that dropped left its contribution behind and the gauge could report more verified links
    /// than there were links. `ob_replicas_connected` is here because the guarantee is the
    /// *comparison* - a count of verified links means nothing without the count it is measured
    /// against, and telling an operator to read that one off `STATUS` means it cannot be alerted
    /// on. Same defect from the other side as roadmap #94.
    void publish_replica_gauges();

    /// Arm or disarm EPOLLOUT for one replica. Extracted because the TLS paths need it from four
    /// places, and an inline `epoll_ctl` in each is how the two event masks drift apart.
    void arm_epollout(const ReplicaInfo& replica);
    void disarm_epollout(const ReplicaInfo& replica);

    /// Drain a replica's send buffer. Returns false if the replica should be removed.
    ///
    /// One pointer test and then one of two loops. The TLS half is a separate function rather than a
    /// branch inside the plaintext one, for the reason series C measured on `Session`: an inlined
    /// TLS loop changes the *plaintext* function's prologue, so the unencrypted path pays for a
    /// branch it never takes.
    bool drain_send_buffer(ReplicaInfo& replica);
    bool drain_send_buffer_plain(ReplicaInfo& replica);
    [[gnu::noinline]] bool drain_send_buffer_tls(ReplicaInfo& replica);

    /// The replica record for `fd`, or null. Caller must hold `mtx_`.
    ///
    /// There were four copies of this loop before it existed, and the handshake path needed a fifth.
    ReplicaInfo* find_replica_locked(int fd);

    /// Remove a replica by fd (closes fd, removes from epoll and replicas_ list).
    /// Caller must hold mtx_.
    void remove_replica_locked(int fd);

    /// Close a replica connection and drop its record, with the reason logged.
    ///
    /// The three-line "remove, then find and erase" sequence appeared three times in
    /// handle_replica_data() before this existed, and forgetting the erase leaves a record pointing
    /// at a closed descriptor - which the broadcast path then writes to.
    void disconnect_replica_locked(int fd, const char* reason);
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

    /// TLS state for the current connection to the primary. Null = plaintext, and reset on every
    /// reconnect because it belongs to the connection rather than to this object.
    std::shared_ptr<TlsChannel> tls_;

    void run_loop();
    void connect_to_primary();
    /// Complete the mutual challenge-response with the primary (#30 part two).
    ///
    /// Returns false on any failure, including a primary that says nothing - which is what a
    /// primary without a cluster secret does, and the log line says so rather than reporting a
    /// timeout.
    bool authenticate_with_primary();

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
