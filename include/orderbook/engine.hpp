#pragma once

#include "orderbook/aggregation.hpp"
#include "orderbook/columnar_store.hpp"
#include "orderbook/data_model.hpp"
#include "orderbook/epoch.hpp"
#include "orderbook/failover.hpp"
#include "orderbook/hlc.hpp"
#include "orderbook/log_episode.hpp"
#include "orderbook/metrics.hpp"
#include "orderbook/multi_master.hpp"
#include "orderbook/query_engine.hpp"
#include "orderbook/snapshot.hpp"
#include "orderbook/replication.hpp"
#include "orderbook/soa_buffer.hpp"
#include "orderbook/sequence_tracker.hpp"
#include "orderbook/version_vector.hpp"
#include "orderbook/wal.hpp"

#include <optional>
#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace ob {

/// TTL / data retention configuration.
struct TTLConfig {
    uint64_t ttl_hours{0};                  // 0 = disabled
    uint64_t scan_interval_seconds{300};    // default 5 minutes
};

/// One write for `Engine::apply_deltas()`: the update and its levels, which are the caller's and
/// must outlive the call.
struct ClientWrite {
    DeltaUpdate  update{};
    const Level* levels{nullptr};
};

/// What happened to one write of a batch: the status `apply_delta()` returns for it, or - where
/// `apply_delta()` would have thrown - the message of what it threw, which is the text a client is
/// sent. `error` is empty exactly when nothing was thrown.
struct WriteOutcome {
    ob_status_t status{OB_OK};
    std::string error;
};

/// Top-level facade that owns and coordinates all subsystems.
///
/// Subsystem ownership order (construction/destruction):
///   wal_ → agg_ → buffers_ → stores_ → combined_store_ → query_engine_
///
/// Requirements: 7.3, 7.4, 7.5, 8.1, 8.3
class Engine : public RoleTransitionHandler {
public:
    explicit Engine(std::string_view base_dir,
                    uint64_t flush_interval_ns = 100'000'000ULL,
                    FsyncPolicy fsync_policy = FsyncPolicy::INTERVAL,
                    ReplicationConfig repl_config = {},
                    ReplicationClientConfig repl_client_config = {},
                    FailoverConfig failover_config = {},
                    TTLConfig ttl_config = {},
                    MultiMasterConfig mm_config = {},
                    size_t wal_rotate_bytes = 512ULL << 20);

    ~Engine();

    // Non-copyable, non-movable
    Engine(const Engine&)            = delete;
    Engine& operator=(const Engine&) = delete;
    Engine(Engine&&)                 = delete;
    Engine& operator=(Engine&&)      = delete;

    /// Open the engine: replay WAL + rebuild columnar index + start flush thread.
    void open();

    /// Close the engine: flush all dirty data + stop background thread.
    void close();

    /// Incremental two-phase flush: drain pending rows (Phase A, under mutex)
    /// then write segments to disk and merge index (Phase B, no mutex).
    void flush_incremental();

    /// Apply a delta update: WAL → SoA buffer (gap detection) → enqueue for columnar flush.
    /// Returns OB_OK on success, error code on failure.
    ob_status_t apply_delta(const DeltaUpdate& delta, const Level* levels);

    /// Apply a record streamed from a primary on the replication link.
    ///
    /// Identical to `apply_delta()` except that a record whose sequence number this node has
    /// already seen for that symbol is **dropped** rather than applied. Storage is append-only, so
    /// applying a duplicate appends its rows a second time (#101, and the half of #100 that was
    /// left unmeasured).
    ///
    /// A separate entry point rather than a condition inside `apply_delta()`, because the fact that
    /// decides it is *where the record came from* and only the caller knows that. The obvious
    /// version — drop when `sequence_number != 0`, since a client write carries zero — is false of
    /// the embedded path: `ob_apply_delta()` takes `seq` as a caller parameter and the Python
    /// client's `insert()` has it as a required argument, so an embedded user numbering their own
    /// records from 1 per symbol would have had every write after the first silently dropped.
    ///
    /// The engine already separates apply paths by origin — this one, `apply_remote_delta()` for
    /// the mesh, `apply_delta_replayed()` for WAL recovery — so this completes the set rather than
    /// adding an exception to it.
    ob_status_t apply_delta_replicated(const DeltaUpdate& delta, const Level* levels);

    /// Apply several writes - the ones a client sent in one read - under **one** acquisition of
    /// `mtx_`, with their WAL records written by one `write()` per run between rotations (stage 2b
    /// of #151, roadmap #155).
    ///
    /// Each write is treated as `apply_delta()` treats it - refused for a migrated symbol,
    /// numbered, written to the WAL before it changes anything, sent to the replicas with its
    /// position (#98), applied to the book, queued for the columnar flush, pushed to subscribers -
    /// and `outcomes[i]` says what `apply_delta()` would have returned or thrown for `writes[i]`.
    /// `apply_delta()` is this with one write, so there is one write path in this engine, not two.
    ///
    /// What one acquisition changes, and nothing else: the wait for room in the pending queue is
    /// taken **once for the batch**, with the predicate a single write uses - so the queue can pass
    /// its ceiling by the batch's rows, as one `MINSERT` passes it by its own - and a WAL write
    /// that fails refuses the write it stopped at while the ones behind it are tried again, as the
    /// next command after a refused one was (`WALWriter::append_batch()`).
    /// `outcomes` must be at least as long as `writes`.
    void apply_deltas(std::span<const ClientWrite> writes, std::span<WriteOutcome> outcomes);

    /// Execute a SQL query.
    std::string execute(std::string_view sql, RowCallback cb);

    /// As above, and report the shape of the answer in `shape` before the first row.
    std::string execute(std::string_view sql, RowCallback cb, QueryShape& shape);

    /// The live book for one symbol, as rows. Error string on failure, empty on success.
    ///
    /// Delegates to `QueryEngine`, which owns the live-buffer lookup. Deliberately not resolving
    /// the buffer here: a second supplier of that lookup is what the static test in
    /// `tests/test_query_live_buffer_race.cpp` refuses, because the C API had the identical race
    /// one file away and fixing only the server would have left it (#91, #92).
    std::string read_book(const std::string& symbol, const std::string& exchange,
                          uint32_t depth, RowCallback cb);

    /// Parse a SQL query.
    std::string parse(std::string_view sql, QueryAST& out);

    /// Format a QueryAST to canonical SQL.
    std::string format(const QueryAST& ast);

    /// Register a streaming subscription; returns subscription id.
    uint64_t subscribe(std::string_view sql, RowCallback cb);

    /// Unregister a streaming subscription.
    void unsubscribe(uint64_t id);

    /// Access the query engine (for advanced use).
    QueryEngine& query_engine() { return *query_engine_; }

    /// Access the metrics registry.
    MetricsRegistry& registry() { return registry_; }

    /// Engine-level statistics for monitoring.
    struct Stats {
        size_t   pending_rows;       ///< rows waiting for columnar flush
        size_t   wal_file_index;     ///< current WAL file index
        size_t   segment_count;      ///< total columnar segments
        size_t   symbol_count;       ///< number of tracked symbols
        uint64_t flush_interval_ns;  ///< configured flush interval

        // Replication (primary) — Requirements 5.1, 5.2
        struct ReplicaMetrics {
            std::string address;
            uint32_t    confirmed_file;
            size_t      confirmed_offset;
            /// Bytes this replica is behind, valid only when `lag_known`.
            ///
            /// The pair rather than a sentinel: zero is a real answer here — a replica that is
            /// caught up is zero bytes behind — so a zero standing in for "cannot be measured"
            /// would be the defect #123 is about, said a second way. `bytes_since()` cannot
            /// answer when a WAL file between the two positions is gone, which retention should
            /// never allow for a connected replica and which therefore means that replica can no
            /// longer catch up from this log.
            uint64_t    lag_bytes;
            bool        lag_known;
        };
        std::vector<ReplicaMetrics> replicas;

        // Replication (replica) — Requirements 5.3
        bool     is_replica{false};
        uint32_t repl_confirmed_file{0};
        size_t   repl_confirmed_offset{0};
        uint64_t repl_records_replayed{0};
        bool     repl_connected{false};

        // Snapshot bootstrap state
        bool     bootstrapping{false};
        size_t   snapshot_bytes_received{0};
        size_t   snapshot_bytes_total{0};
        bool     snapshot_active{false};  // primary: snapshot transfer in progress

        // Failover state
        NodeRole    node_role{NodeRole::STANDALONE};
        uint64_t    current_epoch{0};
        std::string primary_address;
        int64_t     lease_ttl_remaining{0};

        // Compression metrics
        uint64_t compress_bytes_in{0};   // total pre-compression bytes
        uint64_t compress_bytes_out{0};  // total post-compression bytes

        // TTL / data retention metrics
        uint64_t ttl_hours{0};              // configured TTL (0 = disabled)
        uint64_t ttl_segments_deleted{0};   // cumulative segments deleted
        uint64_t ttl_bytes_reclaimed{0};    // cumulative bytes reclaimed

        // Flush integrity: segments refused as already indexed. Non-zero means two
        // flush paths raced; rows would have been scanned twice.
        uint64_t segment_merge_refused{0};

        // Sharding metrics
        std::string shard_id;                // empty = non-sharded
        std::string shard_status;            // "active", "joining", "draining"
        size_t      shard_symbols_count{0};
        uint64_t    shard_map_version{0};

        // Migration metrics
        bool        migration_in_progress{false};
        std::string migration_symbol;
        std::string migration_target_shard;
        uint8_t     migration_progress_pct{0};

        // Routing errors
        uint64_t    shard_routing_errors{0};

        // Multi-master metrics
        uint16_t    mm_node_id{0};
        size_t      mm_peer_count{0};
        size_t      mm_connected_peers{0};
        uint64_t    mm_conflicts_total{0};
        uint64_t    mm_anti_entropy_runs{0};
        /// Repairs the scheduler actually completed. Reported alongside the run count so a
        /// reader can tell "ran and found nothing" from "never ran" — tcp_server.cpp used to
        /// hardcode this to 0 with a comment saying it was unavailable.
        uint64_t    mm_anti_entropy_repairs{0};
        uint64_t    mm_hlc_physical_ns{0};
        uint16_t    mm_hlc_logical{0};
        int64_t     mm_hlc_drift_ns{0};
    };

    /// Collect current engine statistics (thread-safe, acquires mtx_).
    /// Bytes the WAL has written since `from`, or `nullopt` when a file in between is gone.
    ///
    /// Exposed so that `ReplicationManager` can publish the same number `stats()` reports rather
    /// than keeping a second answer beside it — two ways of computing one quantity is how #118
    /// produced a lag that was not one, and #123 is the same question one layer down.
    std::optional<uint64_t> wal_bytes_since(WalPosition from) const {
        return wal_.bytes_since(from);
    }

    Stats stats();

    /// Create a consistent snapshot: flush pending rows, capture WAL position,
    /// enumerate segment files with CRC32C checksums.
    /// Returns the manifest. Writes snapshot_manifest.json to data dir.
    SnapshotManifest create_snapshot();

    /// A snapshot plus what the sender holds, captured together. Defined in snapshot.hpp so
    /// that multi_master.hpp and replication.hpp can hold fields of it; the alias keeps every
    /// existing `Engine::SnapshotWithSequenceState` spelling working.
    using SnapshotWithSequenceState = ob::SnapshotWithSequenceState;

    /// The same snapshot, with the sequence state a receiver needs to declare frontiers from it.
    ///
    /// Both halves come out of the *same* critical section as the flush, and that is the whole
    /// point of a separate method. A vector exported afterwards can claim a number that landed
    /// after the flush and is therefore in no snapshot file — the receiver would then declare a
    /// frontier over a hole. Exported before, it claims less than the files hold, and a redelivery
    /// of the difference appends the rows a second time. The held set closes the remaining gap
    /// exactly: the numbers above the frontier that we do hold are listed, so a redelivery of any
    /// of them meets `has_seen()`.
    SnapshotWithSequenceState create_snapshot_with_sequence_state();

    /// Replace this node's sequence state with a snapshot sender's.
    ///
    /// Only legitimate straight after `install_snapshot()` or `adopt_store_on_disk()`, because it
    /// resets: our contents are now the sender's contents, so our frontiers must be the sender's
    /// frontiers and nothing else.
    void adopt_snapshot_sequence_state(const std::vector<SequenceTracker::VectorEntry>& vector,
                                       const std::vector<SequenceTracker::HeldRanges>& held);

    /// Install a received snapshot: the staged files **replace** this node's columnar store, and
    /// the in-memory state that described the old one is discarded.
    ///
    /// This is the whole of what "bootstrap from a snapshot" means, in one call, because the
    /// defect it closes was that it used to be two and the first one did not exist (#142). Each
    /// installer renamed the received files into the data directory and removed nothing, then
    /// called a function named `load_snapshot` which cleared **memory** and rebuilt the index
    /// from whatever was on disk — so a replica that had flushed a *prefix* of a symbol kept its
    /// own segment beside the arriving one and answered with both. The name described the
    /// caller's intention rather than what the function did.
    [[nodiscard]] bool install_snapshot(const std::string& staging_dir,
                                        const SnapshotManifest& manifest);

    /// Discard the in-memory store and re-read the index from whatever is on disk.
    ///
    /// Note what this does **not** do: it does not touch the files. It is the second half of
    /// `install_snapshot()`, exposed because a test that wants the buffers cleared has no
    /// snapshot to install — and named for what it does, so that no caller can mistake it for an
    /// install again.
    void adopt_store_on_disk();

    /// Returns true if the replica is currently bootstrapping from a snapshot.
    bool is_bootstrapping() const;

    /// True when this node holds nothing at all: no sequence state, no stored segment, no
    /// pending row.
    ///
    /// The gate on requesting a multi-master snapshot, and deliberately stricter than "we are
    /// behind": installing a snapshot *discards* local contents, so a node with data of its own
    /// must never ask for one automatically. Reads the tracker rather than
    /// `export_version_vector()`, whose cache is a flush interval stale — long enough for a node
    /// that has just accepted writes to look empty.
    /// Not const: `mtx_` is not mutable, the same reason `above_frontier_size()` is not const.
    bool holds_no_data();

    // ── Symbol migration (sharding) ───────────────────────────────────────────

    /// Create a snapshot containing only data for one symbol.
    /// Used during symbol migration between shards.
    SnapshotManifest create_symbol_snapshot(const std::string& symbol_key);

    /// Load a symbol snapshot received from another shard.
    void load_symbol_snapshot(const std::string& symbol_key,
                              const SnapshotManifest& manifest);

    /// Get WAL delta for a symbol from a given position.
    std::vector<uint8_t> get_symbol_wal_delta(const std::string& symbol_key,
                                               uint32_t from_file,
                                               size_t from_offset);

    /// Check if a symbol has been migrated (reject writes after switchover).
    bool is_symbol_migrated(const std::string& symbol_key) const;

    /// Mark a symbol as migrated (after atomic ShardMap update).
    void mark_symbol_migrated(const std::string& symbol_key);

    /// Access the base data directory path.
    const std::string& base_dir() const { return base_dir_; }

    // ── Failover / role management ────────────────────────────────────────────

    /// RoleTransitionHandler overrides.
    void promote_to_primary(const EpochValue& new_epoch) override;
    void demote_to_replica(const std::string& new_primary_address) override;

    /// Throw away everything this node holds, so a stream can be replayed into it from zero.
    ///
    /// Clears the buffers and the pending queue, closes the columnar store, deletes every segment
    /// directory on disk and reopens the store empty. The WAL files are left alone.
    ///
    /// **Caller must hold neither `flush_mtx_` nor `mtx_`**: this takes both, in that order
    /// (pitfall 10). Called from the replication client's own thread, which is the only place that
    /// knows whether it has to happen - `demote_to_replica()` used to call it and no longer does
    /// (#101), because a role change is not evidence about what this node holds.
    ///
    /// It is only correct where the position this node saved is discarded with it. Doing one
    /// without the other leaves a replica asking to resume from a position whose data it has just
    /// deleted - so the one caller zeroes the position and writes it down before asking for
    /// anything.
    void discard_local_data_for_resync();

    std::pair<uint32_t, size_t> get_wal_position() const override;

    /// Identity of the WAL this node writes (#101). Non-zero from the moment `open()` returns.
    ///
    /// No lock: written once in `open()`, before any thread that could read it exists. A replica
    /// compares the one its primary announces against the one it saved, and a data directory
    /// restored from scratch has a different one even at the same address - which is the case
    /// requirement 4.3 exists for.
    uint64_t wal_identity() const { return wal_identity_; }

    EpochValue get_current_epoch() const override;
    void truncate_and_rebootstrap(const EpochValue& new_epoch,
                                  const std::string& primary_address) override;

    /// Get current node role.
    NodeRole node_role() const;

    /// Get current epoch value.
    uint64_t current_epoch() const;

    /// Remember an epoch a primary announced, if it is newer than the one this node knows (#103).
    ///
    /// **Never lowers.** The epoch is a fact about the cluster and it only ever moves forward, so
    /// the guards that stand on it - `ERR STALE_PRIMARY` on the primary, the replica's own record
    /// filter - are only worth anything if this number cannot be talked down. The consequence is
    /// named rather than avoided: a data directory carrying a higher epoch than the cluster it is
    /// pointed at refuses to follow it, and `docs/operations.md` covers the two ways to read that.
    ///
    /// Called from the replication client's receive thread; the store is a CAS loop rather than a
    /// compare-then-store so a concurrent promotion cannot be overwritten by a lower number.
    void note_primary_epoch(uint64_t epoch);

    /// Handle ROLE command — returns wire-protocol response.
    std::string handle_role_command() const;

    /// Handle FAILOVER command — returns wire-protocol response.
    std::string handle_failover_command(const std::string& target_node_id);

    /// Set external read-only flag pointer (toggled during role transitions).
    void set_read_only_flag(std::atomic<bool>* flag);

    // ── Multi-master replication ──────────────────────────────────────────────

    /// Apply a delta update in multi-master mode: HLC tick → WAL append with
    /// origin → conflict resolver update → SoA buffer apply → broadcast → enqueue.
    ob_status_t apply_delta_mm(const DeltaUpdate& delta, const Level* levels);

    /// `apply_deltas()` in multi-master mode: `apply_delta_mm()` per write - each with its own HLC
    /// tick, in order - and the peers told after `mtx_` is released, in the order of the writes
    /// (#80). `apply_delta_mm()` is this with one write.
    void apply_deltas_mm(std::span<const ClientWrite> writes, std::span<WriteOutcome> outcomes);

    /// Apply a remote delta received from a peer node.  Performs loop prevention,
    /// HLC merge, per-level conflict resolution, and WAL append with original origin.
    /// Does NOT re-broadcast (single-hop propagation).
    ob_status_t apply_remote_delta(const DeltaUpdate& delta, const Level* levels,
                                   uint16_t origin_node_id,
                                   const HLCTimestamp& remote_hlc);

    /// What this node holds, per (symbol, origin), for a peer's catch-up decision.
    ///
    /// Served from a cache refreshed at flush time, and deliberately not from the tracker:
    /// MultiMasterManager calls this from its io_loop while holding its own mutex, and the
    /// write path takes the engine mutex before MM's. Reaching into the tracker here would
    /// close that cycle — measured as a node that stopped answering writes entirely.
    ///
    /// A stale cache understates what we hold, so a peer sends more than it needs to and the
    /// duplicates are dropped on arrival. The staleness window is one flush interval.
    /// How many sequence numbers from `origin` are held above the frontier for this symbol key.
    ///
    /// A test seam, and a diagnostic: a non-zero count means this node has seen records it cannot
    /// yet claim contiguity for, which is exactly the state a restart used to lose (#75).
    std::size_t above_frontier_size(const std::string& key, uint16_t origin);

    std::vector<SequenceTracker::VectorEntry> export_version_vector(std::size_t limit,
                                                                    bool& truncated) const;

    /// Get the HLC clock (nullptr if multi-master is not enabled).
    HybridLogicalClock* hlc() const { return hlc_.get(); }

    /// Get the MultiMasterManager (nullptr if multi-master is not enabled).
    MultiMasterManager* multi_master_manager() const { return mm_mgr_.get(); }

    /// Check if this engine is running in multi-master mode.
    bool is_multi_master() const { return mm_config_.enabled; }

private:
    std::string base_dir_;
    uint64_t    flush_interval_ns_;

    // Subsystems (order matters for construction/destruction)
    WALWriter         wal_;
    AggregationEngine agg_;
    MetricsRegistry   registry_;

    // Per-symbol SoABuffers
    // `shared_ptr` and not `unique_ptr`, so a query can hold a buffer alive across the snapshot
    // install that clears this map (#92). `live_ptrs_` used to sit beside it as a raw-pointer
    // index of the same keys, populated and cleared in the same three places; one map cannot
    // disagree with itself.
    std::unordered_map<std::string, std::shared_ptr<SoABuffer>> buffers_;

    // Per-symbol ColumnarStores
    std::unordered_map<std::string, std::unique_ptr<ColumnarStore>> stores_;

    // Combined store used by QueryEngine for scanning
    ColumnarStore combined_store_;

    std::unique_ptr<QueryEngine> query_engine_;

    // Replication (optional, disabled when port/primary_port == 0)
    ReplicationConfig                    repl_config_;
    ReplicationClientConfig              repl_client_config_;
    std::unique_ptr<ReplicationManager>  repl_mgr_;
    std::unique_ptr<ReplicationClient>   repl_client_;

    // Failover (optional, disabled when coordinator endpoints are empty)
    FailoverConfig                       failover_config_;
    std::unique_ptr<FailoverManager>     failover_mgr_;
    std::atomic<NodeRole>                node_role_{NodeRole::STANDALONE};
    std::atomic<uint64_t>                current_epoch_{0};

    // External read-only flag (owned by TcpServer, toggled during role transitions)
    std::atomic<bool>*                   read_only_flag_{nullptr};

    // TTL / data retention
    TTLConfig ttl_config_;
    std::atomic<uint64_t> ttl_segments_deleted_{0};
    std::atomic<uint64_t> ttl_bytes_reclaimed_{0};

    // Segments refused by merge_segments() because their directory was already in
    // the index. Should stay at zero: any increment means two flush paths raced and
    // the duplicate was caught by the index check rather than prevented by
    // flush_mtx_. Worth alerting on, and it is what the concurrency test asserts.
    std::atomic<uint64_t> segment_merge_refused_{0};
    uint64_t last_ttl_scan_ns_{0};

    // Sharding: symbols that have been migrated away from this shard
    std::unordered_set<std::string> migrated_symbols_;

    // Multi-master replication (optional, disabled when mm_config_.enabled == false)
    MultiMasterConfig                    mm_config_;

    /// Per-symbol sequence counters and per-origin high-water marks. Guarded by mtx_.
    SequenceTracker                      seq_tracker_;
    /// Fingerprint of the frontiers as last written to the WAL, so an unchanged vector is not
    /// rewritten ten times a second.
    uint64_t                             vector_fingerprint_written_{0};

    /// Snapshot of the vector for MM to read without touching mtx_. Its own small mutex,
    /// because the point is to be reachable from the MM io_loop under MM's lock.
    mutable std::mutex                   vector_cache_mtx_;
    std::vector<SequenceTracker::VectorEntry> vector_cache_;
    bool                                 vector_cache_truncated_{false};

    /// Refresh the snapshot above from the tracker. Caller must hold mtx_.
    void refresh_version_vector_cache();
    std::unique_ptr<HybridLogicalClock>  hlc_;
    std::unique_ptr<MultiMasterManager>  mm_mgr_;

    // Background flush thread
    std::thread       flush_thread_;
    std::atomic<bool> stop_flush_{false};
    std::mutex        mtx_;

    // Serialises every path that writes segments or mutates stores_.
    //
    // Separate from mtx_ on purpose: segment I/O must not block writers, so Phase B
    // deliberately runs outside mtx_. But two flushers in Phase B at once each saw
    // the same active segment, wrote the same directory and merged the same meta,
    // so every row in it came back from SELECT twice. A concurrent stores_.clear()
    // during a role transition freed stores mid-iteration on top of that.
    //
    // LOCK ORDER: flush_mtx_ → mtx_ → ColumnarStore::index_mtx_. Never the reverse.
    std::mutex        flush_mtx_;

    // Shutdown signalling for the flush thread. Kept separate from pending_cv_
    // so that backpressure traffic cannot interfere with it, and so that close()
    // does not have to wait out a full flush interval before join() returns.
    std::mutex              flush_stop_mtx_;
    std::condition_variable flush_stop_cv_;

    /// Set by a writer that has run out of room, cleared by the flush loop when it wakes.
    ///
    /// Without it a writer at the ceiling waits for `--flush-interval-ms` to elapse, because
    /// nothing asks the flush loop to run on account of a writer waiting — and the work it is
    /// waiting for takes **73 ms** for a full million rows, measured on an m9g.xlarge under a
    /// load average of 2.95, linear at 0.068 µs a row. At a one-second interval that is a
    /// thirteenfold wait for the same work (#137).
    ///
    /// **Lock order.** `request_flush()` takes `flush_stop_mtx_` while the caller holds `mtx_`,
    /// which adds `mtx_ → flush_stop_mtx_` to the order documented above. It is safe because
    /// `flush_stop_mtx_` is a leaf: it is taken in exactly two places — the wait in
    /// `flush_loop()` and the wake in `close()` — and neither holds `mtx_` while doing so.
    /// Taking it is not optional: setting the flag without it loses the wake-up when the flush
    /// loop has evaluated its predicate and not yet slept.
    std::atomic<bool> flush_now_{false};

    /// Loud once when writers start waiting, loud once when they stop (#116's mechanism).
    /// Guarded by `mtx_`, which every waiter already holds.
    LogEpisode backpressure_;

    /// How long a writer waits for room before the write is refused.
    ///
    /// Asking for a flush removes the dependency on the operator's interval; it does not help
    /// when the flush itself cannot make progress, which is a full disk or #113's `EIO`. Sixty
    /// times the 73 ms a full ceiling takes to flush, so a healthy flush never reaches it even on
    /// a machine an order of magnitude slower — a deadline a healthy write can touch is a gate on
    /// a clock, and those teach operators to ignore refusals.
    ///
    /// **What it bounds, exactly.** `wait_for` times the *condition* wait and not the
    /// reacquisition of `mtx_` afterwards, and the flush holds `mtx_` for the whole of its first
    /// phase — measured at **1.2 s** for a million rows on the development machine, against
    /// 73 ms for the segment write that follows it outside the lock. So a writer parked behind
    /// that phase is not bounded by this deadline and will not be refused: it waits for the
    /// mutex, gets it, finds room, and proceeds. That is the right outcome and it is not what
    /// the constant's name suggests, which is why it is written here. A mutation shortening this
    /// to one millisecond therefore **survives** the unit tests, and the case it does bite —
    /// a flush that throws, releases `mtx_` and leaves the queue full — is the fault-injector
    /// test this item has not written yet.
    static constexpr std::chrono::seconds kBackpressureDeadline{5};

    /// Ask the flush loop to run now. Caller may hold `mtx_`; see `flush_now_`.
    void request_flush();

    /// Wait for room in the pending queue. False means the deadline passed and the caller must
    /// refuse the write rather than accept one it cannot store.
    [[nodiscard]] bool await_pending_room(std::unique_lock<std::mutex>& lock);

    // Pending rows for columnar flush
    struct PendingRow {
        std::string symbol;
        std::string exchange;
        SnapshotRow row;
    };
    std::vector<PendingRow> pending_rows_;

    // Backpressure: maximum number of pending rows before apply_delta blocks.
    // Default 1M rows ≈ ~100 MB memory. Prevents OOM under sustained ingestion.
    static constexpr size_t MAX_PENDING_ROWS = 1'000'000;
    std::condition_variable pending_cv_;  // signalled when pending_rows_ is drained

    // Helpers
    /// Buffer for a symbol whose "SYMBOL.EXCHANGE" key the caller already built.
    ///
    /// The write path has that key in hand — it needs it for the migrated-symbol check and for the
    /// sequence tracker — and the two-argument overload below rebuilt it from scratch: two
    /// std::string temporaries from the char arrays plus the concatenation, three allocations per
    /// write on the hottest path in the engine (roadmap #66).
    SoABuffer&     get_or_create_buffer(const std::string& key, const char* symbol,
                                        const char* exchange);

    /// Convenience for callers that do not have the key yet.
    SoABuffer&     get_or_create_buffer(const std::string& symbol, const std::string& exchange);

    /// Stamp `delta` with a sequence number and record a GAP if its origin's stream skipped.
    ///
    /// Called under mtx_ immediately before the WAL append, so the numbers a symbol receives
    /// are in WAL order. A delta arriving with a non-zero number keeps it: that number was
    /// minted by whoever originated the record, and renumbering it here would make catch-up
    /// compare numbers from different nodes.
    void stamp_sequence(DeltaUpdate& delta, uint16_t origin, const std::string& key);

    /// `stamp_sequence()` without the GAP record: the number is assigned, a gap is counted and
    /// logged, and whether there was one is returned, for a caller that writes the GAP itself - a
    /// batch, which puts it into the WAL directly in front of its DELTA (`WalDelta::gap_before`),
    /// the place `stamp_sequence()` puts it. Caller holds `mtx_`.
    [[nodiscard]] bool observe_sequence(DeltaUpdate& delta, uint16_t origin,
                                        const std::string& key);

    /// Cap on what gets written down. Above it the node relearns by over-asking, which costs
    /// traffic and duplicate drops, never data.
    static constexpr std::size_t kMaxPersistedVectorEntries = 4096;
    /// Held ranges written down per persist. The WAL payload length is 16-bit, so this is a hard
    /// ceiling rather than a preference: 3000 ranges is ~48 KB of payload plus entry headers.
    static constexpr std::size_t kMaxPersistedHeldRanges = 3000;

    /// Read size when checksumming snapshot files. One buffer, reused for every file, rather
    /// than an allocation the size of each file — which for a large segment meant a large
    /// transient allocation on whichever thread asked for the snapshot (#79).
    static constexpr std::size_t kSnapshotReadChunk = 256u * 1024u;

    /// Write the version vector to the WAL if any frontier moved since it was last written.
    /// **Caller must hold mtx_** — it is called from inside the flush's merge block.
    void persist_version_vector_if_changed();

    /// Whether a record already seen for this (symbol, origin) is applied again or dropped.
    /// Named rather than a bool at the call site: `apply_delta_impl(delta, levels, true)` says
    /// nothing about which way true goes.
    enum class DuplicatePolicy { Apply, DropIfSeen };

    /// `apply_delta()`, `apply_delta_replicated()` and `apply_delta_mm()`: `apply_local_writes()`
    /// with one write, and what it could not apply thrown with the text it always had. One
    /// acquisition of `mtx_` either way: a wrapper that checked `has_seen()`, released the lock and
    /// delegated would leave a window between the check and the append.
    ob_status_t apply_delta_impl(const DeltaUpdate& delta_in, const Level* levels,
                                 DuplicatePolicy policy, bool multi_master);

    /// The one local write path: `apply_delta()`, `apply_delta_replicated()`, `apply_delta_mm()`
    /// and both batch entry points are this, with one write or with several. One acquisition of
    /// `mtx_` for the whole batch; see `apply_deltas()` for what that changes and what it keeps.
    void apply_local_writes(std::span<const ClientWrite> writes, std::span<WriteOutcome> outcomes,
                            DuplicatePolicy policy, bool multi_master);

    /// Hand a record the WAL has just written to the replicas, with the position the append
    /// returned (#98). Caller holds `mtx_` and calls this in WAL order: the replicas apply in the
    /// order they are sent.
    void broadcast_to_replicas(const DeltaUpdate& delta, const Level* levels, WalPosition at);

    /// The in-memory half of a write the WAL holds: the live book, the rows for the columnar
    /// flush, and the subscribers. Caller holds `mtx_`. Returns the book's status.
    ob_status_t apply_in_memory(const std::string& key, const DeltaUpdate& delta,
                                const Level* levels);

    /// Multi-master: note each level's HLC with the conflict resolver. Caller holds `mtx_`.
    void note_local_hlcs(const DeltaUpdate& delta, const Level* levels, const HLCTimestamp& hlc);

    /// Multi-master: send a local write to the peers. Caller must **not** hold `mtx_` (#80).
    void broadcast_to_peers(const DeltaUpdate& delta, const Level* levels,
                            const HLCTimestamp& hlc);

    /// Path of the file holding the replication position, or empty when nothing saves one.
    ///
    /// Read from `repl_client_config_.state_file` rather than rebuilt from `base_dir_`. The
    /// rebuilt form happened to be right in production, where `tcp_server.cpp` sets the config to
    /// `<data_dir>/repl_state.txt` — and silently deleted nothing anywhere the path is configured
    /// differently, which every unit test does.
    std::string replication_state_path() const;

    /// Forget where we were in a primary's stream, because this node's data is no longer a prefix
    /// of it.
    ///
    /// Called from `promote_to_primary()` and **not** from `demote_to_replica()`, which is the
    /// inversion #101 is about: the position used to be deleted on the way *into* replication,
    /// which is exactly when it is needed. The moment that matters is the one where this node
    /// starts writing records of its own.
    void discard_saved_replication_position();

    /// Identity of this data directory's WAL, so a position recorded in a segment can be told
    /// apart from one that arrived with a snapshot. Read from `<base_dir>/wal_identity`, generated
    /// on first open. Deliberately outside every segment directory: a snapshot ships segment
    /// directories, and an identity that travelled with them would defeat its own purpose.
    void load_or_create_wal_identity();
    uint64_t wal_identity_{0};

    void restore_version_vector();

    /// Restore the numbers held above the frontiers from the last HELD_SEQUENCES record.
    ///
    /// Separate from the vector restore, and called even when there is no usable vector: held
    /// numbers are independently useful, and importing them can only raise what this node claims
    /// to have seen.
    void restore_held_sequences();
    ColumnarStore& get_or_create_store(const std::string& symbol, const std::string& exchange);
    void flush_loop();

    /// One flush: drain, segment I/O, gauge, WAL truncation and the TTL scan.
    ///
    /// Separated from the loop so the loop can put an exception boundary **around one tick** and
    /// run the next one. Without that, the first `ENOSPC` from the WAL ends the flush thread for
    /// the life of the process - which stops the node dying (#112) and leaves it never flushing
    /// again, a guarantee absent in production rather than a crash. The tick contains no
    /// `continue`, `break` or `return`, which is what made moving it out of the loop mechanical.
    void flush_tick();

    /// Failed `fsync` counts already published, so the counter is fed a delta rather than a total.
    ///
    /// The registry's counter API takes an increment, and `WALWriter` keeps a running total - so
    /// publishing the total each tick would count every past failure again. The writer owns the
    /// number because the failure happens there; the engine owns the publishing because the writer
    /// has no registry (#113).
    uint64_t published_fsync_failures_{0};

    /// WAL records already published, and replayed records already published.
    ///
    /// Two more of the shape above, and the second one is why they all go through
    /// `publish_counter_delta()` rather than three copies of a subtraction: `repl_client_` is
    /// **rebuilt on every role change** (`demote_to_replica()` constructs a new one), so its total
    /// restarts at zero while the registry's counter must not. A bare `total - published` freezes
    /// the metric from that moment until the new client passes the old total — which is precisely
    /// the window an operator is watching, a node that has just become a replica and is catching
    /// up. `WALWriter` and `HybridLogicalClock` live as long as the engine, so the naive form
    /// happens to be correct for them; that is a property of those two objects and not of the
    /// pattern (#117).
    uint64_t published_wal_records_{0};
    uint64_t published_repl_replayed_{0};

    /// Feed `name` the difference between a counter owned elsewhere and what has been published,
    /// tolerating the source restarting from zero.
    void publish_counter_delta(const char* name, uint64_t total, uint64_t& published);

    /// HLC drift excursions already published, for the same reason and by the same shape as the
    /// field above: the clock keeps a running total and the registry's counters take an increment.
    /// The clock owns the number because the excursion is observed there, and it has no registry
    /// to publish to — `liborderbook_hlc` links nothing, deliberately (#120).
    uint64_t published_drift_excursions_{0};

    /// Consecutive failed ticks, so the log reports an episode rather than one line per interval.
    ///
    /// A permanently full disk would otherwise write an ERROR every flush interval for ever -
    /// #95's shape, where a reconnect failure logged at loop frequency until the process died.
    uint64_t consecutive_flush_failures_{0};

    /// Apply a DELTA record read back from the WAL during open().
    ///
    /// Does two things from apply_delta(): the SoA update and the pending-row enqueue.
    /// Deliberately not: writing to the WAL (the record is already there, and writing
    /// it again would grow the log on every restart), broadcasting to replicas or
    /// peers (each node replays its own WAL, so re-sending duplicates on the other
    /// side), notifying subscribers (nothing is subscribed before open() returns), or
    /// waiting on backpressure (nobody is competing for the buffer yet).
    void apply_delta_replayed(const DeltaUpdate& delta, const Level* levels);

    /// Replay the WAL tail into memory. Returns records applied.
    ///
    /// Requires combined_store_.open_existing() to have run: rows already covered by
    /// a segment are skipped by timestamp, which needs the segment index.
    uint64_t replay_wal_tail();
    void flush_drain_pending();    // Phase A: drain pending_rows_ → per-symbol append (must hold mtx_)
    void flush_write_and_merge();  // Phase B: segment I/O + merge index (must hold flush_mtx_, not mtx_)
};

} // namespace ob
