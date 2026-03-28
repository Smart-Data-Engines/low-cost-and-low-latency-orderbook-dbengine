#pragma once

#include "orderbook/aggregation.hpp"
#include "orderbook/columnar_store.hpp"
#include "orderbook/data_model.hpp"
#include "orderbook/epoch.hpp"
#include "orderbook/failover.hpp"
#include "orderbook/query_engine.hpp"
#include "orderbook/replication.hpp"
#include "orderbook/soa_buffer.hpp"
#include "orderbook/wal.hpp"

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
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
                    TTLConfig ttl_config = {});

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

private:
    std::string base_dir_;
    uint64_t    flush_interval_ns_;

    // Subsystems (order matters for construction/destruction)
    WALWriter         wal_;
    AggregationEngine agg_;

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

    // TTL / data retention
    TTLConfig ttl_config_;
    std::atomic<uint64_t> ttl_segments_deleted_{0};
    std::atomic<uint64_t> ttl_bytes_reclaimed_{0};
    uint64_t last_ttl_scan_ns_{0};

    // Background flush thread
    std::thread       flush_thread_;
    std::atomic<bool> stop_flush_{false};
    std::mutex        mtx_;

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
    ColumnarStore& get_or_create_store(const std::string& symbol, const std::string& exchange);
    void flush_loop();
    void flush_drain_pending();    // Phase A: drain pending_rows_ → per-symbol append (must hold mtx_)
    void flush_write_and_merge();  // Phase B: flush segments to disk + merge index (no mutex)
};

} // namespace ob
