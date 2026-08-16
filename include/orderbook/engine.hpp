#pragma once

#include "orderbook/aggregation.hpp"
#include "orderbook/columnar_store.hpp"
#include "orderbook/data_model.hpp"
#include "orderbook/epoch.hpp"
#include "orderbook/failover.hpp"
#include "orderbook/hlc.hpp"
#include "orderbook/metrics.hpp"
#include "orderbook/multi_master.hpp"
#include "orderbook/query_engine.hpp"
#include "orderbook/replication.hpp"
#include "orderbook/soa_buffer.hpp"
#include "orderbook/sequence_tracker.hpp"
#include "orderbook/wal.hpp"

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
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

/// Top-level facade that owns and coordinates all subsystems.
///
/// Subsystem ownership order (construction/destruction):
///   wal_ → agg_ → buffers_/live_ptrs_ → stores_ → combined_store_ → query_engine_
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
                    MultiMasterConfig mm_config = {});

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

    /// Execute a SQL query.
    std::string execute(std::string_view sql, RowCallback cb);

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
            size_t      lag_bytes;
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
        uint64_t    mm_hlc_physical_ns{0};
        uint16_t    mm_hlc_logical{0};
        int64_t     mm_hlc_drift_ns{0};
        std::vector<std::pair<uint16_t, size_t>> mm_replication_lag_per_peer;
    };

    /// Collect current engine statistics (thread-safe, acquires mtx_).
    Stats stats();

    /// Create a consistent snapshot: flush pending rows, capture WAL position,
    /// enumerate segment files with CRC32C checksums.
    /// Returns the manifest. Writes snapshot_manifest.json to data dir.
    SnapshotManifest create_snapshot();

    /// Load a snapshot received from the primary: replace the columnar store
    /// index with the snapshot's segments.
    void load_snapshot(const SnapshotManifest& manifest);

    /// Returns true if the replica is currently bootstrapping from a snapshot.
    bool is_bootstrapping() const;

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
    std::pair<uint32_t, size_t> get_wal_position() const override;
    EpochValue get_current_epoch() const override;
    void truncate_and_rebootstrap(const EpochValue& new_epoch,
                                  const std::string& primary_address) override;

    /// Get current node role.
    NodeRole node_role() const;

    /// Get current epoch value.
    uint64_t current_epoch() const;

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

    /// Apply a remote delta received from a peer node.  Performs loop prevention,
    /// HLC merge, per-level conflict resolution, and WAL append with original origin.
    /// Does NOT re-broadcast (single-hop propagation).
    ob_status_t apply_remote_delta(const DeltaUpdate& delta, const Level* levels,
                                   uint16_t origin_node_id,
                                   const HLCTimestamp& remote_hlc);

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
    std::unordered_map<std::string, std::unique_ptr<SoABuffer>> buffers_;
    std::unordered_map<std::string, SoABuffer*>                 live_ptrs_;

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
    SoABuffer&     get_or_create_buffer(const std::string& symbol, const std::string& exchange);

    /// Stamp `delta` with a sequence number and record a GAP if its origin's stream skipped.
    ///
    /// Called under mtx_ immediately before the WAL append, so the numbers a symbol receives
    /// are in WAL order. A delta arriving with a non-zero number keeps it: that number was
    /// minted by whoever originated the record, and renumbering it here would make catch-up
    /// compare numbers from different nodes.
    void stamp_sequence(DeltaUpdate& delta, uint16_t origin);
    ColumnarStore& get_or_create_store(const std::string& symbol, const std::string& exchange);
    void flush_loop();

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
