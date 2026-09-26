#pragma once

#include "orderbook/aggregation.hpp"
#include "orderbook/chunked_queue.hpp"
#include "orderbook/columnar_store.hpp"
#include "orderbook/compaction.hpp"
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
#include <deque>
#include <chrono>
#include <condition_variable>
#include <limits>
#include <map>
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

/// The event time before which a segment has expired under a retention of `ttl_hours`: a segment
/// whose newest row is older than this goes. 0 - nothing has expired - when the retention is 0,
/// which is the flag's "keep everything", or when it reaches back past the epoch (#163).
///
/// `wall_now_ns` is **the wall clock**, `wall_clock_ns()`, because a segment's times are event
/// times - nanoseconds since the Unix epoch, whether the server stamped a row on arrival or the
/// client gave its time (#105) - and a cutoff is a comparison with them only on the same clock. The
/// sweep used to read `steady_clock`, which counts from boot. On a machine up for less than the
/// retention the subtraction wrapped, the cutoff came out past every timestamp and the first sweep
/// deleted every segment: measured, a restart with `--ttl-hours 24` on a machine up 21.5 hours
/// took 200 rows and both their segment directories to none. On a machine up for longer the cutoff
/// was a few hours into 1970, so nothing ever expired.
///
/// Saturating, and without a product that can overflow: `ttl_hours` comes from a flag that takes
/// any `uint64_t`, and a retention of a million years is a request with an answer, not a wrap.
constexpr uint64_t ttl_cutoff_ns(uint64_t wall_now_ns, uint64_t ttl_hours) {
    constexpr uint64_t kNsPerHour = 3600ULL * 1'000'000'000ULL;
    if (ttl_hours == 0 || ttl_hours > wall_now_ns / kNsPerHour) return 0;
    return wall_now_ns - ttl_hours * kNsPerHour;
}

/// One write for `Engine::apply_deltas()`: the update and its levels, both the caller's, which must
/// outlive the call. Pointers, not a copy of the update: the engine copies it once, into the batch
/// it numbers, and a second copy here was 88 bytes per write for a batch of one (measured with the
/// rest of that path's overhead: 390 instructions per single in-process write, #155).
struct ClientWrite {
    const DeltaUpdate* update{nullptr};
    const Level*       levels{nullptr};
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

    /// Whether the flush tick merges small segments (#165 part 2b): on unless `--compaction off`.
    /// Not a tuning knob - a valve on a process that rewrites what is stored. Before open().
    void set_compaction_enabled(bool enabled) { compaction_enabled_ = enabled; }

    /// Keep every segment's files where they are for as long as the returned handle lives (#165 part
    /// 2b): while one is held, the tick neither merges segments nor sweeps retention. A snapshot
    /// takes one before its flush, and whoever sends it holds it until the transfer ends - the
    /// manifest names files, and a merge or a sweep removing one mid-transfer failed it, and the
    /// replica started again.
    std::shared_ptr<const void> pin_segment_files();

    /// Whether the last checkpoint vouches for a segment found at open (#160, #165 part 2a); one it
    /// does not is removed there and its rows rebuilt from the WAL. Pure, so the rule is tested
    /// without staging a crash:
    ///   - a segment of another WAL - a snapshot's, a migration's - is not this log's to judge;
    ///   - a checkpoint naming a seal epoch vouches for the segments sealed at or before it;
    ///   - one naming only a position vouches for the segments at or before that position;
    ///   - one that says nothing, or a log whose first files retention took, vouches for every one.
    static bool segment_vouched_for(const SegmentMeta& meta,
                                    const WALReplayer::LastCheckpoint& last,
                                    uint64_t local_wal_identity);

    /// When a store's drained rows are sealed into a segment (#165 part 2a): enough rows, or old
    /// enough, oldest first, and a tick takes **its share** - at most `kSealsPerTick` stores, and
    /// after the first no more rows than the tick's share, which is a quarter more than it drained
    /// or `kSealRows`, whichever is more - so stores that come due together are spread over ticks
    /// rather than sealed in one; and, while every store's rows together are over the budget, the
    /// oldest of the rest past both limits until they are not. Constants, not flags: nothing yet
    /// says an operator has a reason to turn them.
    ///
    /// A quarter more than it drained, because a share of exactly that keeps any backlog it finds:
    /// what comes due each tick is what was drained, so stores the first wave deferred stayed
    /// deferred - sealed four or five ticks late, 2.5-3.2 M rows waiting - for as long as it ran.
    ///
    /// The share is measured. Without it, sixteen stores taking ~62 500 rows a tick at four
    /// pipelining connections on the m9g.xlarge came due together every other tick - 39 sealing
    /// ticks of 79, strictly alternating, read from the tick's DEBUG lines - and a sealing tick,
    /// ~1.45 M rows written in 21 ms and synced in 39, took about as long as the four connections
    /// take to fill the pending queue's million rows.
    static constexpr size_t kSealRows = 65'536;
    static constexpr std::chrono::milliseconds kSealAge{10'000};
    static constexpr size_t kUnsealedRowsBudget = 4'000'000;
    static constexpr size_t kSealsPerTick = 64;
    /// What FLUSH, close(), a snapshot and a store's own seal say they drained: they take every
    /// store, and no share limits them.
    static constexpr size_t kNoRowLimit = std::numeric_limits<size_t>::max();

    /// One store's standing for a seal: its rows in blocks, and when its oldest block was published.
    struct SealCandidate {
        size_t rows{0};
        std::chrono::steady_clock::time_point oldest{};
    };
    enum class SealReason { kAll, kRows, kAge, kBudget };
    struct SealPick {
        size_t index{0};
        SealReason why{SealReason::kAll};
    };
    /// The policy above, as a function of its inputs alone, so it is tested without a clock:
    /// `candidates` oldest first, and the picks in the same order. `seal_all` is FLUSH, close() and
    /// a snapshot, which take every one. `drained_rows` is what the tick drained, and its share is
    /// a quarter more rows or `kSealRows`, whichever is more - so a tick that drained little still seals
    /// small stores due by age in bulk. Below the budget a due store after the first is taken only
    /// while the rows picked stay within the share - a younger one that fits after an older one
    /// that does not, so the share is filled - and the oldest due store is always taken, so none
    /// waits behind a share it is bigger than.
    static std::vector<SealPick> pick_seals(const std::vector<SealCandidate>& candidates,
                                            std::chrono::steady_clock::time_point now,
                                            bool seal_all, size_t drained_rows);

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
    /// The policy the WAL was given, kept here as well because it decides a second thing: whether a
    /// flush syncs the segments it wrote (#160). `none` promises nothing after a power cut, so it
    /// does not pay for a sync; `every` and `interval` do, or a checkpoint would claim rows the
    /// device never received.
    const FsyncPolicy fsync_policy_;

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

    /// Rows drained and published as blocks, waiting for their store's seal (#165 part 2a), oldest
    /// first. A tick used to write a segment for every symbol with a row since the last one - at 256
    /// symbols ~2 000 files and a syncfs() a tick, 84.7% of the flush thread in the kernel - so a
    /// node gained a segment per active symbol per tick. Keyed by the store, whose pointer is stable
    /// under `flush_mtx_`, which every path that drains, seals or replaces the stores holds; only
    /// they touch this. `unsealed_rows_` is its total, for the readers that hold only mtx_.
    struct UnsealedBlock {
        std::shared_ptr<const RowBlock> block;
        /// Every row's record is at or after `wal_from` and before `wal_to`, the drain's position.
        /// A checkpoint may not claim `wal_from` while the block waits; the segment it seals into
        /// records `wal_to`, like every segment's position since #63.
        WalPosition wal_from{};
        WalPosition wal_to{};
        std::chrono::steady_clock::time_point published{};
    };
    struct Unsealed {
        std::deque<UnsealedBlock> blocks;
        size_t rows{0};
    };
    std::unordered_map<ColumnarStore*, Unsealed> unsealed_;
    std::atomic<size_t> unsealed_rows_{0};
    /// How many rows each store's last block held, reserved for its next one, so a busy store's
    /// block is not grown - its rows copied again each time - on the way to its size. Under
    /// `flush_mtx_`, like `unsealed_`, and cleared wherever the stores are.
    std::unordered_map<ColumnarStore*, size_t> block_rows_hint_;
    /// Where a sealed block's rows go back to and a drain takes them from (#165 part 2a); a
    /// drain takes about the pending queue's ceiling at most, so that is what is kept spare.
    std::shared_ptr<RowBufferPool> block_rows_pool_ = std::make_shared<RowBufferPool>(MAX_PENDING_ROWS);
    /// The column buffers every seal writes through, lent to its store for the seal (#165 part 2a).
    /// Under `flush_mtx_`, which every seal holds - and so does every merge (#165 part 2b).
    ColumnarStore::ColumnBuffers seal_buffers_;
    LogEpisode unsealed_budget_episode_{};

    // ── Compaction (#165 part 2b) ─────────────────────────────────────────────
    //
    // Every field below is the flush thread's, under `flush_mtx_`, unless it says otherwise. A merge
    // moves through five steps across ticks - written, synced, published, synced, inputs removed -
    // and each field is one step's list.
    bool compaction_enabled_{true};
    /// A symbol's segments whose end falls in one period: what a merge may take from.
    struct CompactionKey {
        uint64_t    period_start{0};
        std::string symbol;
        std::string exchange;
        bool operator<(const CompactionKey& o) const {
            if (period_start != o.period_start) return period_start < o.period_start;
            if (symbol != o.symbol) return symbol < o.symbol;
            return exchange < o.exchange;
        }
    };
    struct CompactionPartition {
        /// When a seal last added a segment, which a partition must be quiet since to settle.
        std::chrono::steady_clock::time_point last_added{};
        /// When to look at it again: now after a change, its settling time after a look that found
        /// nothing to merge.
        std::chrono::steady_clock::time_point next_look{};
    };
    std::map<CompactionKey, CompactionPartition> compaction_partitions_;
    /// Where the last tick's looks stopped, so the next resumes after it.
    CompactionKey compaction_cursor_{};
    /// Written into a working directory, waiting for a sync before it is published.
    struct StagedMerge {
        std::vector<SegmentMeta> inputs;
        SegmentMeta output;
        uint64_t    synced_before{0};   ///< `segment_syncs_` when written
    };
    std::vector<StagedMerge> staged_merges_;
    /// Published, their inputs waiting for a sync - which takes the rename to the device - and for
    /// every scan that copied them before the swap.
    struct RetiredInputs {
        std::vector<std::string>  dirs;
        std::weak_ptr<const void> readers;
        uint64_t                  synced_before{0};   ///< `segment_syncs_` when published
    };
    std::vector<RetiredInputs> retired_inputs_;
    /// Segments a merge could not read whole, left as they are for the life of the process.
    std::unordered_set<std::string> unmergeable_;
    std::chrono::steady_clock::time_point last_merge_{};
    /// When a tick at the write ceiling last looked for a merge (compaction_step()).
    std::chrono::steady_clock::time_point last_heavy_look_{};
    /// When a merge's own sync last ran: under `none`, the only thing that puts a checkpoint on the
    /// device.
    std::chrono::steady_clock::time_point last_merge_sync_{};
    /// Numbers the merges' working directories, so no two share one.
    uint64_t merge_seq_{0};
    std::chrono::steady_clock::time_point merge_backoff_until_{};
    LogEpisode merge_failures_{};
    /// Syncs of the data directory that ran - a seal's under a policy that syncs, and every one of the
    /// merges' own: what a step compares with the count it recorded to know that a sync has come
    /// since. Not `none`'s, which sync nothing.
    uint64_t segment_syncs_{0};
    /// The seal epoch the last checkpoint appended vouches for, and the one the last checkpoint known
    /// to be on the device does: a merge takes a segment of this WAL only at or below the second.
    /// Under `mtx_`, like the checkpoint's position.
    uint64_t checkpoint_seal_epoch_{0};
    uint64_t durable_seal_epoch_{0};
    /// Held snapshots' pins (`pin_segment_files()`); any thread. Shared with the handles, which a
    /// sender may release after this engine is gone.
    std::shared_ptr<std::atomic<int>> segment_file_pins_ = std::make_shared<std::atomic<int>>(0);


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
    /// When the last TTL sweep ran, for the sweep's **cadence** - on the monotonic clock, because
    /// "every five minutes" must not stretch or shrink when the wall clock is stepped. The sweep's
    /// **cutoff** is on the wall clock, because it is compared with event times (`ttl_cutoff_ns()`).
    /// Two clocks for two questions, and a `time_point` here rather than a count of nanoseconds, so
    /// the one cannot be compared with the other: a count from this clock read as a time is #163.
    /// Default-constructed until the first sweep, which runs at the first tick.
    std::chrono::steady_clock::time_point last_ttl_scan_{};

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
    /// A queue of fixed-size chunks rather than a vector (stage 5 of #151): a flush tick takes
    /// every row queued so far in one short hold of mtx_, drains them without it, and gives each
    /// chunk back as it is done. `chunked_queue.hpp` says what two swapped vectors did to memory
    /// instead. The chunks it keeps are bounded by the ceiling's worth of rows, plus two.
    using PendingQueue = ChunkedQueue<PendingRow>;
    PendingQueue pending_rows_{MAX_PENDING_ROWS / PendingQueue::chunk_rows() + 2};

    /// How far into the WAL the last completed drain reached: the position it stamped into its
    /// stores. **The engine's answer to "how much of the log is in segment files"** once a flush's
    /// segments are written (#159) - which survives a killed process and not a power cut, so since
    /// #160 its reader is `durable_up_to_`, which takes this value once a sync has covered those
    /// files. Both used to read the log's end instead, later by whatever writers appended while
    /// the flush wrote segments without `mtx_`. Written and read under `mtx_`, with `flush_mtx_`
    /// held from the drain to the checkpoint, so no other drain comes between them. Zero until a
    /// first drain, which reads as covering nothing - the direction in which a reader may be wrong.
    WalPosition drained_up_to_{};

    /// How far the WAL is durable in segments **on the device** (#160): the drain position of the
    /// last flush whose segments a successful `syncfs()` covered. This, not `drained_up_to_`, is
    /// what the checkpoint claims and what WAL retention reads - `drained_up_to_` says the rows are
    /// in segment files, which survives a killed process and not a power cut: measured, a cut after
    /// a flush brought back eight empty segment files under a checkpoint that claimed them. Equal to
    /// `drained_up_to_` after a flush that synced; it stops at the first failed sync of any kind, for
    /// the rest of the process (`checkpoints_frozen_`). Under `--fsync-policy none` it follows
    /// `drained_up_to_` without a sync. Written and read under `mtx_`, like `drained_up_to_`.
    WalPosition durable_up_to_{};

    /// What the newest checkpoint **known to be on the device** claims (#160) - the one WAL
    /// retention reads. A checkpoint is appended without a sync of its own, and the next WAL sync
    /// (the next tick's, or the next write's under `every`) is what makes it durable. Retention in
    /// the same tick that appended it would otherwise delete WAL files on the strength of a record
    /// a power cut could still take, while the unlink survives: the segments that checkpoint
    /// vouched for would then be rebuilt at startup from records that are gone. So this takes
    /// `durable_up_to_` once a WAL sync has covered the checkpoint - one tick later, which is what
    /// retention pays - and never after a failed one (`checkpoints_frozen_`). Under
    /// `--fsync-policy none` it follows `durable_up_to_` at once. Under `mtx_`.
    WalPosition retention_floor_{};

    /// The data directory, open for `syncfs()` from `open()` to `close()` (#160). -1 until then.
    int data_dir_fd_{-1};

    /// Set by the first failed sync of any kind - a flush's `syncfs()` or a WAL `fsync` - and never
    /// cleared in this process (#160). Linux reports a failed sync once and marks the pages it could
    /// not write clean, so the next sync succeeds without writing them: a checkpoint after it would
    /// vouch for segments, or for a checkpoint, the device may not have. So from the first failure
    /// no checkpoint is appended and the retention floor stays where it was; the restart, which
    /// replays from the last checkpoint synced before the failure, is what rebuilds the segments
    /// written since, and the WAL still holds every record they need. Under `mtx_`.
    bool checkpoints_frozen_{false};

    /// Rows a flush tick has taken out of `pending_rows_` and not yet drained into their stores
    /// (stage 5 of #151). The tick takes them under mtx_, syncs their WAL records and drains them
    /// **without** it, and lowers this chunk by chunk as each is drained - so a writer waiting at
    /// the ceiling waits for one chunk, not for the tick. Zero outside a tick: the tick holds
    /// `flush_mtx_` from taking the rows to giving the last chunk back, so nothing else that drains
    /// or replaces the queue runs in between.
    ///
    /// Atomic, and lowered **without** mtx_: the first version took mtx_ for each chunk, and at four
    /// pipelining connections on the m9g.xlarge the drain, queueing ~250 times a tick behind every
    /// writer, fell behind them - 10.6 -> 9.0 M levels/s - and under `--fsync-policy every`, where a
    /// writer holds mtx_ through its fsync, it could not give chunks back as fast as they filled.
    /// Set, and zeroed on a failure, under mtx_; read under it by everything that counts the queue.
    ///
    /// Everything that counts the queue counts these as well (`queued_rows()`): they are not in a
    /// segment yet, a reader of `STATUS` or `holds_no_data()` must not see them vanish for the
    /// length of an `fsync`, and the room a writer waits for is room in both - otherwise a tick
    /// that starts at the ceiling would let the queue grow to twice it while the sync runs.
    std::atomic<size_t> detached_rows_{0};

    /// Rows not yet in a segment: the queue, plus what a flush tick has taken. Caller holds mtx_.
    size_t queued_rows() const {
        return pending_rows_.size() + detached_rows_.load(std::memory_order_relaxed);
    }

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
    /// The store for `symbol.exchange`, created if it is new. Caller holds `flush_mtx_`, which every
    /// mutator of `stores_` holds, so a lookup needs nothing more; a creation - the one insertion
    /// into `stores_` - also takes mtx_ unless `mtx_held`, because `holds_no_data()` reads the map
    /// under mtx_ alone.
    ColumnarStore& get_or_create_store(const std::string& symbol, const std::string& exchange,
                                       bool mtx_held);
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
    /// Requires combined_store_.open_existing() to have run, and the segments no surviving
    /// checkpoint vouches for to have been removed (#160): a record a remaining segment already
    /// holds is skipped by the WAL position that segment recorded (#63), which needs the index.
    uint64_t replay_wal_tail(WALReplayer& replayer, const WALReplayer::LastCheckpoint& last);
    void flush_drain_pending();    // Phase A: drain pending_rows_ → per-symbol append (must hold mtx_)
    /// The drain itself: `batch` into its per-symbol stores, each store stamped with `covered` -
    /// the WAL position every one of these rows' records is at or before, and which a sync has
    /// reached - and `drained_up_to_` set to it once every row is in. Each chunk is given back to
    /// `pending_rows_` as soon as it is drained, lowering `detached_rows_`, which the caller set
    /// to the batch's rows - neither under mtx_. With `mtx_held` the caller holds mtx_ throughout;
    /// without it, the caller holds only `flush_mtx_`, and mtx_ is taken just to create a store and
    /// at the end (stage 5 of #151). On a throw - a segment write the disk refused (#161) - the rows
    /// it appended stay in their stores and the rest go back in front of the queue, and no drain
    /// position is recorded.
    void drain_batch(PendingQueue::Batch& batch, WalPosition covered, bool mtx_held);
    /// Phase B: segment I/O, the segment sync, and the index merge (hold `flush_mtx_`, not `mtx_`).
    /// Returns 0, or the `errno` the segment sync failed with - in which case the segments are
    /// merged and visible, and no checkpoint claims them or anything after them in this process
    /// (#160). The flush tick counts and goes on; `FLUSH` answers `ERR`, because a client that
    /// asked is told.
    ///
    /// `drained_rows` is what the tick drained (`pick_seals()`), and `kNoRowLimit` for the rest.
    int flush_write_and_merge(bool seal_all, size_t drained_rows);

    /// One store's seal: its first `count` blocks written into `metas` (#165 part 2a).
    struct Seal {
        ColumnarStore* store{nullptr};
        size_t count{0};
        size_t rows{0};
        std::vector<SegmentMeta> metas;
    };
    /// Which stores to seal, oldest first: every store with blocks, `only`'s, or what is due within
    /// the share of a tick that drained `drained_rows` (`pick_seals()`). Holds `flush_mtx_`.
    std::vector<Seal> choose_seals(bool seal_all, ColumnarStore* only, size_t drained_rows);
    /// Write each seal's segments from its blocks. A store whose write fails keeps its blocks and is
    /// dropped from `seals`, with what it wrote removed. Returns the first failure, or null. Holds
    /// `flush_mtx_`; `mtx_` or not.
    std::exception_ptr write_seals(std::vector<Seal>& seals);
    /// Replace the sealed blocks with their segments, in the index and in `unsealed_`. Returns the
    /// segments refused as already indexed. Holds `flush_mtx_` and `mtx_`.
    size_t merge_seals_locked(const std::vector<Seal>& seals);
    /// Where replay starts: the drain's position, or the oldest unsealed block's start if that is
    /// earlier - claiming a waiting block's rows would lose them in a crash. **With blocks waiting
    /// this position alone does not say which segments are durable** (#165 part 2a): a seal writes
    /// several drains into one segment positioned at the last of them, so a start judging by it
    /// removed segments whose earlier rows no replay from here brings back. The checkpoint names
    /// the seal epoch too, and `segment_vouched_for()` reads it. Holds `flush_mtx_` and `mtx_`.
    WalPosition claim_locked() const;
    /// Forget every unsealed block, for the paths that discard the stores. Holds both locks.
    void drop_unsealed_locked();
    /// Sync what this flush wrote, before anything claims it (#160): one `syncfs()` on the data
    /// directory, without `mtx_`. Returns 0, or the `errno`; always 0 under `--fsync-policy none`.
    int sync_segments();
    /// The WAL was just synced to its end, so the last checkpoint appended is on the device and
    /// retention may follow its claim - unless a sync has failed, after which no success vouches for
    /// anything (#160). Caller holds `mtx_`.
    void note_wal_synced();
    /// Whether a sync has failed in this process, so that no checkpoint may claim anything
    /// (`checkpoints_frozen_`). A WAL `fsync` that failed counts too, read from the writer's own
    /// count, so the freeze does not depend on which path saw it. Caller holds `mtx_`.
    bool checkpoints_frozen();
    /// Freeze the checkpoints and say so, once (#160). Caller holds `mtx_`.
    void freeze_checkpoints(const std::string& what_failed);
    /// At startup, before replay: remove the segments no surviving checkpoint vouches for (#160).
    void remove_unvouched_segments(const WALReplayer::LastCheckpoint& last);

    /// The flush tick's merges (#165 part 2b), after retention: a sync if something written or
    /// published since the last one waits for it, the inputs whose replacement is on the device and
    /// that no scan reads removed, what is on the device published, and more merged - in a tick
    /// that drained no more than `kSealRows` rows, for `compaction::kTickBudget`. Holds
    /// `flush_mtx_`, not `mtx_`.
    void compaction_step(size_t drained_rows);
    /// Read `inputs` whole and write them as one segment into a working directory beside them.
    bool stage_merge(const std::vector<SegmentMeta>& inputs);
    void publish_staged_merges();
    void remove_retired_inputs();
    /// Rename a merge's working directory to a segment's name no directory has.
    bool publish_merged_dir(SegmentMeta& output);
    /// A syncfs() of the data directory whatever `--fsync-policy` says: a merge removes segments
    /// that were on the device, so it may do so only once what replaces them is (#165 part 2b).
    /// Under `none` it is also what makes the last checkpoint durable. Holds `flush_mtx_`.
    int sync_for_merge();
    /// What a partition may merge now - up to `max_merges` in all, within `budget_end` - and when
    /// to look at it again.
    void look_at_partition(std::map<CompactionKey, CompactionPartition>::iterator it,
                           std::chrono::steady_clock::time_point now, uint64_t vouched_epoch,
                           size_t& merges, size_t max_merges,
                           std::chrono::steady_clock::time_point budget_end);
    /// A segment a seal (`arrived`), a merge or a rebuild added: its partition is looked at next.
    void note_segment_for_compaction(const SegmentMeta& meta,
                                     std::chrono::steady_clock::time_point now, bool arrived);
    /// Every indexed segment's partition, after open() and an install replaced the store.
    void note_store_for_compaction();
    /// Forget every merge in flight, for the paths that replace or discard the store: a name a
    /// retired input had may come back with the new store. Holds `flush_mtx_`.
    void drop_compaction_locked();
    /// At close(), after the final flush: remove every merge's working directory, and every
    /// replaced input no query still reads once the rename that replaced it is on the device, so a
    /// clean stop leaves nothing a build before part 2b would take for a segment and hold twice.
    /// Holds `flush_mtx_`, not `mtx_`.
    void finish_compaction_on_close();
    /// The directories of replaced inputs whose files are still on the disk, for a snapshot to
    /// leave out: a replica of any build then holds each row once. Holds `flush_mtx_`.
    std::vector<std::string> replaced_input_dirs() const;

    /// The seal epoch (#165 part 2a): bumped by every `write_seals()` that has something to write,
    /// stamped on the segments it writes, and named by a checkpoint written while rows wait in
    /// blocks. Restored at open to the highest the store or the last checkpoint knows, so a segment
    /// sealed after a restart can never be taken for one an old checkpoint vouched for. Under
    /// flush_mtx_.
    uint64_t seal_epoch_{0};
};

} // namespace ob
