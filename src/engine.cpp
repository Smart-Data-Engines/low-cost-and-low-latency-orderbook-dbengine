// Feature: orderbook-dbengine — Engine facade (Requirements 7.3, 7.4, 7.5, 8.1, 8.3)
//
// The Engine owns and coordinates all subsystems:
//   WALWriter, SoABuffer map, ColumnarStore map, AggregationEngine, QueryEngine.
//
// apply_delta flow: WAL write → SoA buffer apply (gap detection) → enqueue for columnar flush.
// open():  replay WAL + rebuild columnar index + start background flush thread.
// close(): stop flush thread + final flush + flush_segment + WAL flush.

#include "orderbook/engine.hpp"
#include "orderbook/crc32c.hpp"
#include "orderbook/logger.hpp"

#include <chrono>
#include <cinttypes>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <random>

namespace ob {

namespace fs = std::filesystem;

Engine::Engine(std::string_view base_dir, uint64_t flush_interval_ns,
               FsyncPolicy fsync_policy,
               ReplicationConfig repl_config,
               ReplicationClientConfig repl_client_config,
               FailoverConfig failover_config,
               TTLConfig ttl_config,
               MultiMasterConfig mm_config)
    : base_dir_(base_dir)
    , flush_interval_ns_(flush_interval_ns)
    , wal_(base_dir, 512ULL << 20, fsync_policy)
    , combined_store_(base_dir)
    , query_engine_(std::make_unique<QueryEngine>(combined_store_, live_ptrs_, agg_))
    , repl_config_(std::move(repl_config))
    , repl_client_config_(std::move(repl_client_config))
    , failover_config_(std::move(failover_config))
    , ttl_config_(ttl_config)
    , mm_config_(std::move(mm_config))
{}

Engine::~Engine() {
    close();
}

void Engine::open() {
    // Rebuild the columnar segment index first: the replay below needs it to tell
    // which records are already durable.
    combined_store_.open_existing();

    // Restore the sequence counters from what is already durable in segments, before the
    // replay below adds what is durable only in the WAL. Both only ever raise, so the order
    // between them does not matter; skipping either hands out a number twice.
    {
        size_t raised = 0;
        for (const auto& meta : combined_store_.index()) {
            if (meta.max_sequence_number == 0) continue;   // written before numbers existed
            const std::string key = meta.symbol + "." + meta.exchange;
            seq_tracker_.raise_local(key, meta.max_sequence_number);
            // Everything this node minted up to that number is held: it assigned and applied
            // those records itself. Without saying so, a hole in a remote origin's stream
            // would hold the frontier down and every local write after a restart would be
            // reported as a gap.
            seq_tracker_.declare_frontier(key, mm_config_.node_id, meta.max_sequence_number);
            ++raised;
        }
        OB_LOG_INFO("engine", "Sequence counters restored from segments: segments=%zu symbols=%zu",
                    raised, seq_tracker_.symbol_count());
    }

    // Which WAL these segments' positions refer to. Before the replay, which needs it.
    load_or_create_wal_identity();

    // What this node holds, from the last vector it wrote down. Before the tail replay, so
    // the tail can only raise it.
    restore_version_vector();
    restore_held_sequences();

    // Replay the WAL tail — the records written after the last flush. Until this
    // existed, the replay callback was empty and every write acknowledged but not yet
    // flushed was lost on a crash, despite being in a fsynced WAL.
    const uint64_t replayed = replay_wal_tail();

    // Persist what was recovered before serving anything. Two reasons, and the first
    // is not optional: SELECT reads the columnar store and never the live SoA buffer,
    // so rows recovered into memory alone would be invisible to every query. The
    // second is that this flush appends a checkpoint, so the next open() has nothing
    // left to replay.
    if (replayed > 0) {
        OB_LOG_INFO("engine", "Persisting %llu recovered records before serving",
                    static_cast<unsigned long long>(replayed));
        flush_incremental();
    }

    // Restore epoch from WAL replay.
    {
        WALReplayer epoch_replayer(base_dir_);
        epoch_replayer.replay([](const WALRecord&, const uint8_t*) {});
        current_epoch_.store(epoch_replayer.last_epoch(), std::memory_order_relaxed);
    }

    // Mutual exclusivity gate: MM mode and Replication mode are mutually exclusive.
    // In MM mode, ONLY MultiMasterManager is created.
    // In non-MM mode, ONLY ReplicationManager/ReplicationClient are created.
    if (mm_config_.enabled) {
        // Multi-master mode: ONLY create MultiMasterManager
        OB_LOG_INFO("engine", "Multi-master mode enabled: node_id=%u replication_port=%u",
                    mm_config_.node_id, mm_config_.replication_port);

        hlc_ = std::make_unique<HybridLogicalClock>(mm_config_.node_id);
        wal_.set_origin_node_id(mm_config_.node_id);
        mm_mgr_ = std::make_unique<MultiMasterManager>(mm_config_, *this, wal_, *hlc_);
        node_role_.store(NodeRole::MULTI_MASTER, std::memory_order_release);

        // Multi-master nodes always accept writes — reset read-only flag
        // that may have been set by FailoverManager's initial election.
        if (read_only_flag_) {
            read_only_flag_->store(false, std::memory_order_release);
        }

        // Tell FailoverManager to skip election logic for multi-master nodes.
        if (failover_mgr_) {
            failover_mgr_->set_role(NodeRole::MULTI_MASTER);
        }

        mm_mgr_->start();

        // NOTE: ReplicationManager is NOT created in MM mode.
        // NOTE: ReplicationClient is NOT created in MM mode.
    } else {
        // Non-MM mode: create ReplicationManager if port configured (Requirement 7.4).
        if (repl_config_.port > 0) {
            repl_mgr_ = std::make_unique<ReplicationManager>(repl_config_, wal_);
            repl_mgr_->set_engine(this);
            repl_mgr_->start();
        }

        // Start ReplicationClient if configured as replica (Requirement 7.4).
        if (repl_client_config_.primary_port > 0) {
            OB_LOG_INFO("engine", "starting ReplicationClient to %s:%u",
                        repl_client_config_.primary_host.c_str(), repl_client_config_.primary_port);
            repl_client_ = std::make_unique<ReplicationClient>(repl_client_config_, *this);
            repl_client_->start();
        } else {
            OB_LOG_INFO("engine", "ReplicationClient not started (primary_port=0)");
        }
    }

    // FailoverManager is independent but only used in non-MM mode.
    if (!failover_config_.coordinator.endpoints.empty() && !mm_config_.enabled) {
        OB_LOG_INFO("engine", "starting FailoverManager, node_id=%s",
                    failover_config_.coordinator.node_id.c_str());
        failover_mgr_ = std::make_unique<FailoverManager>(failover_config_, *this);
        failover_mgr_->start();
        node_role_.store(failover_mgr_->role(), std::memory_order_relaxed);
    }

    // Start background flush thread.
    stop_flush_.store(false, std::memory_order_relaxed);
    flush_thread_ = std::thread([this]() { flush_loop(); });
}

void Engine::close() {
    // Stop multi-master manager first (it broadcasts, so must stop before WAL/flush).
    if (mm_mgr_) {
        mm_mgr_->stop();
    }

    // Stop failover manager first (it may trigger role transitions).
    if (failover_mgr_) {
        failover_mgr_->stop();
    }

    // Stop replication client first (it calls apply_delta, so must stop before flush thread).
    if (repl_client_) {
        repl_client_->stop();
    }

    // Stop replication manager (no more broadcasts after this).
    if (repl_mgr_) {
        repl_mgr_->stop();
    }

    // Stop background flush thread.
    stop_flush_.store(true, std::memory_order_relaxed);
    // Wake any writers blocked on backpressure so they can exit.
    pending_cv_.notify_all();
    // Wake the flush thread itself, so join() does not wait out the interval.
    {
        std::lock_guard<std::mutex> lock(flush_stop_mtx_);
        flush_stop_cv_.notify_all();
    }
    if (flush_thread_.joinable()) {
        flush_thread_.join();
    }

    // flush_mtx_ is taken after join(), never before: the flush thread holds it for
    // the duration of a tick, so taking it first would make this thread wait on the
    // very thread it is about to join.
    {
        std::lock_guard<std::mutex> flush_lock(flush_mtx_);

        // Final flush of all pending rows under the lock (Phase A).
        {
            std::unique_lock<std::mutex> lock(mtx_);
            // Group commit: sync any remaining WAL records.
            wal_.sync();
            flush_drain_pending();
        }

        // Phase B: segment I/O + merge. This closes every active segment, so the
        // second flush_segment() loop that used to follow was dead code.
        flush_write_and_merge();
    }

    // Flush WAL to disk.
    wal_.flush();
}

std::size_t Engine::above_frontier_size(const std::string& key, uint16_t origin) {
    std::lock_guard<std::mutex> lock(mtx_);
    return seq_tracker_.above_frontier_size(key, origin);
}

std::vector<SequenceTracker::VectorEntry> Engine::export_version_vector(std::size_t limit,
                                                                       bool& truncated) const {
    std::lock_guard<std::mutex> lock(vector_cache_mtx_);
    truncated = vector_cache_truncated_ || vector_cache_.size() > limit;
    if (truncated) {
        OB_LOG_DEBUG("engine", "Version vector not exportable: cached=%zu limit=%zu",
                     vector_cache_.size(), limit);
        return {};
    }
    return vector_cache_;
}

void Engine::refresh_version_vector_cache() {
    // Caller holds mtx_.
    bool truncated = false;
    auto entries = seq_tracker_.export_vector(kMaxPersistedVectorEntries, truncated);
    {
        std::lock_guard<std::mutex> lock(vector_cache_mtx_);
        vector_cache_           = std::move(entries);
        vector_cache_truncated_ = truncated;
    }
}

void Engine::persist_version_vector_if_changed() {
    // Caller holds mtx_ — this runs from flush_write_and_merge() inside the block that merges
    // segments and appends the checkpoint. Taking mtx_ here instead deadlocked the flush
    // thread against itself: std::mutex is not recursive, and the stack showed the flush
    // thread waiting on a mutex it already held while every client write queued behind it.
    const uint64_t fp = seq_tracker_.fingerprint();
    if (fp == vector_fingerprint_written_) return;      // nothing moved

    bool truncated = false;
    auto entries = seq_tracker_.export_vector(kMaxPersistedVectorEntries, truncated);
    refresh_version_vector_cache();

    if (truncated) {
        // Too many entries to write down. A node with that many symbols will relearn by
        // over-asking after a restart, which costs traffic and drops duplicates.
        OB_LOG_WARN("engine",
                    "Version vector too large to persist (limit=%zu) — a restart will ask "
                    "peers for more than it needs", kMaxPersistedVectorEntries);
        vector_fingerprint_written_ = fp;
        return;
    }

    const auto payload = serialize_version_vector(entries, /*truncated=*/false);
    wal_.append_version_vector(payload.data(), payload.size());

    // The held set goes with it. The frontier alone describes a node that followed every origin's
    // stream from its first record; anything that arrived out of order lives above the frontier,
    // and forgetting it across a restart turns catch-up's deliberate over-delivery back into
    // duplicate rows (#75).
    bool held_truncated = false;
    const auto held = seq_tracker_.export_held(kMaxPersistedHeldRanges, held_truncated);
    if (held_truncated) {
        OB_LOG_WARN("engine",
                    "Held sequence set too large to persist in full (limit=%zu ranges) — a "
                    "restart may store duplicates for the numbers left out",
                    kMaxPersistedHeldRanges);
    }
    if (!held.empty()) {
        const auto held_payload = serialize_held_ranges(held);
        wal_.append_held_sequences(held_payload.data(), held_payload.size());
        OB_LOG_DEBUG("engine", "Persisted held sequences: entries=%zu bytes=%zu",
                     held.size(), held_payload.size());
    }
    vector_fingerprint_written_ = fp;

    OB_LOG_DEBUG("engine", "Persisted version vector: entries=%zu bytes=%zu",
                 entries.size(), payload.size());
}

void Engine::restore_held_sequences() {
    std::vector<uint8_t> last;
    WALReplayer replayer(base_dir_);
    replayer.replay_v2([&last](const WALReplayContext& ctx) {
        if (ctx.header.record_type != WAL_RECORD_HELD_SEQUENCES) return;
        last.assign(ctx.payload, ctx.payload + ctx.payload_len);
    });

    if (last.empty()) return;   // nothing was held when this node last wrote its state down

    std::vector<SequenceTracker::HeldRanges> held;
    if (!deserialize_held_ranges(last.data(), last.size(), held)) {
        OB_LOG_WARN("engine",
                    "Held sequence record unusable — a redelivered out-of-order record may be "
                    "stored twice");
        return;
    }

    std::size_t numbers = 0;
    for (const auto& entry : held) {
        for (const auto& [first, last_seq] : entry.ranges) numbers += last_seq - first + 1;
    }

    {
        std::unique_lock<std::mutex> lock(mtx_);
        seq_tracker_.import_held(held);
        vector_fingerprint_written_ = seq_tracker_.fingerprint();
    }
    OB_LOG_INFO("engine",
                "Restored held sequences from WAL: entries=%zu numbers=%zu — these are the "
                "out-of-order records a redelivery must not apply again",
                held.size(), numbers);
}

void Engine::load_or_create_wal_identity() {
    const std::string path = base_dir_ + "/wal_identity";

    std::ifstream in(path);
    if (in.is_open()) {
        uint64_t value = 0;
        in >> value;
        if (value != 0) {
            wal_identity_ = value;
            OB_LOG_INFO("engine", "WAL identity %llu",
                        static_cast<unsigned long long>(wal_identity_));
            return;
        }
        OB_LOG_WARN("engine", "wal_identity file present but unusable — generating a new one, so "
                              "positions recorded by the previous run are ignored");
    }

    // Random rather than derived from the path or the clock: two data directories restored from the
    // same backup must not agree, or one would trust the other's positions.
    std::random_device rd;
    uint64_t value = (static_cast<uint64_t>(rd()) << 32) ^ rd();
    if (value == 0) value = 1;   // 0 means "unknown" everywhere else
    wal_identity_ = value;

    std::ofstream out(path, std::ios::trunc);
    if (!out.is_open()) {
        OB_LOG_WARN("engine",
                    "cannot write %s — recovery will fall back to comparing timestamps, which is "
                    "exact only while a symbol's timestamps increase", path.c_str());
        return;
    }
    out << wal_identity_;
    out.flush();
    OB_LOG_INFO("engine", "WAL identity %llu generated",
                static_cast<unsigned long long>(wal_identity_));
}

void Engine::restore_version_vector() {
    // Caller holds nothing: this runs from open() before the flush thread exists.
    // A full pass, like the epoch restore: the vector is written next to a checkpoint, so
    // replay_after_checkpoint() would usually skip it. Keep the last one seen.
    std::vector<uint8_t> last;
    WALReplayer replayer(base_dir_);
    replayer.replay_v2([&last](const WALReplayContext& ctx) {
        if (ctx.header.record_type != WAL_RECORD_VERSION_VECTOR) return;
        last.assign(ctx.payload, ctx.payload + ctx.payload_len);
    });

    if (last.empty()) {
        OB_LOG_INFO("engine", "No version vector in the WAL — this node will ask peers for "
                              "everything they have");
        return;
    }

    PeerVector own;
    if (!own.deserialize(last.data(), last.size()) || own.truncated()) {
        OB_LOG_WARN("engine", "Persisted version vector unusable — asking peers for everything");
        return;
    }

    std::vector<SequenceTracker::VectorEntry> entries = own.entries();
    {
        std::unique_lock<std::mutex> lock(mtx_);
        seq_tracker_.import_own_vector(entries);
        vector_fingerprint_written_ = seq_tracker_.fingerprint();
        refresh_version_vector_cache();   // safe: refresh does not touch mtx_
    }
    OB_LOG_INFO("engine", "Restored version vector from WAL: entries=%zu", entries.size());
}

void Engine::stamp_sequence(DeltaUpdate& delta, uint16_t origin, const std::string& key) {
    // Caller holds mtx_, and passes the "SYMBOL.EXCHANGE" key it already built. Building it
    // again here would add a heap allocation to every write for nothing.
    const SequenceTracker::Decision d = seq_tracker_.observe(key, origin,
                                                             delta.sequence_number);
    delta.sequence_number = d.sequence_number;

    if (d.gap) {
        // First time this engine has ever written one of these: the record type is as old as
        // the WAL format, but nothing assigned sequence numbers, so the check that produces it
        // could never fire.
        OB_LOG_WARN("engine", "Sequence gap: symbol=%s.%s origin=%u expected=%llu got=%llu",
                    delta.symbol, delta.exchange, static_cast<unsigned>(origin),
                    static_cast<unsigned long long>(d.expected),
                    static_cast<unsigned long long>(d.sequence_number));
        registry_.increment_counter("ob_sequence_gaps_detected");
        wal_.append_gap(delta.sequence_number, delta.timestamp_ns);
    }
}

ob_status_t Engine::apply_delta(const DeltaUpdate& delta_in, const Level* levels) {
    // Local copy, because the sequence number is stamped below and the public signature
    // takes a const reference — a caller's DeltaUpdate is not ours to modify.
    DeltaUpdate delta = delta_in;

    std::unique_lock<std::mutex> lock(mtx_);

    // Built once and reused by the migrated-symbol check and stamp_sequence() below. One
    // string per write, not two.
    const std::string symbol_key = std::string(delta.symbol) + "." + delta.exchange;

    // Reject writes to migrated symbols (Requirement 6.6).
    if (migrated_symbols_.count(symbol_key)) {
        OB_LOG_WARN("engine", "Rejecting write to migrated symbol: symbol_key=%s",
                    symbol_key.c_str());
        return OB_ERR_MIGRATED;
    }

    // Backpressure: wait until pending queue has room.
    // This blocks the writer if the flush thread can't keep up.
    pending_cv_.wait(lock, [this]() {
        return pending_rows_.size() < MAX_PENDING_ROWS ||
               stop_flush_.load(std::memory_order_relaxed);
    });

    // 1. Assign the sequence number, then write to WAL before any state mutation
    //    (Requirement 8.1). No fsync here — group commit via flush_loop() or close().
    //    Origin 0 outside multi-master; a record streamed from a primary arrives here with
    //    the primary's number already set and keeps it.
    stamp_sequence(delta, mm_config_.node_id, symbol_key);
    wal_.append(delta, levels);

    // 1b. Broadcast to replicas if replication is enabled (Requirement 1.2).
    //     Must be within the same mutex lock to maintain WAL ordering.
    if (repl_mgr_) {
        const size_t levels_bytes = delta.n_levels * sizeof(Level);
        const size_t payload_len  = sizeof(DeltaUpdate) + levels_bytes;

        alignas(8) uint8_t payload[sizeof(DeltaUpdate) + MAX_LEVELS * sizeof(Level)];
        std::memcpy(payload, &delta, sizeof(DeltaUpdate));
        if (levels_bytes > 0) {
            std::memcpy(payload + sizeof(DeltaUpdate), levels, levels_bytes);
        }

        WALRecord hdr{};
        hdr.sequence_number = delta.sequence_number;
        hdr.timestamp_ns    = delta.timestamp_ns;
        hdr.checksum        = crc32c(payload, payload_len);
        hdr.payload_len     = static_cast<uint16_t>(payload_len);
        hdr.record_type     = WAL_RECORD_DELTA;
        hdr._pad            = 0;

        repl_mgr_->broadcast(hdr, payload, payload_len);
    }

    // 2. Apply to SoA buffer using seqlock writer protocol.
    SoABuffer& buf = get_or_create_buffer(symbol_key, delta.symbol, delta.exchange);
    bool gap_detected = false;   // unused: gaps are decided per origin in stamp_sequence()
    ob_status_t status = ob::apply_delta(buf, delta, levels, gap_detected);

    // 4. Enqueue SnapshotRows for background columnar flush + notify subscribers.
    for (uint16_t i = 0; i < delta.n_levels; ++i) {
        SnapshotRow row{};
        row.timestamp_ns    = delta.timestamp_ns;
        row.sequence_number = delta.sequence_number;
        row.side            = delta.side;
        row.level_index     = i;
        row.price           = levels[i].price;
        row.quantity        = levels[i].qty;
        row.order_count     = levels[i].cnt;

        pending_rows_.push_back({delta.symbol, delta.exchange, row});

        // 5. Notify streaming subscribers synchronously (within 1 µs budget, Requirement 10.9).
        query_engine_->notify_subscribers(delta.symbol, delta.exchange, row);
    }

    // Update gauge: pending rows after enqueue.
    registry_.set_gauge("ob_pending_rows", static_cast<int64_t>(pending_rows_.size()));

    return status;
}

ob_status_t Engine::apply_delta_mm(const DeltaUpdate& delta_in, const Level* levels) {
    DeltaUpdate delta = delta_in;   // see apply_delta() for why this is copied

    std::unique_lock<std::mutex> lock(mtx_);

    // One key string per write; see apply_delta().
    const std::string symbol_key = std::string(delta.symbol) + "." + delta.exchange;

    // Reject writes to migrated symbols (Requirement 6.6).
    if (migrated_symbols_.count(symbol_key)) {
        OB_LOG_WARN("engine", "Rejecting write to migrated symbol: symbol_key=%s",
                    symbol_key.c_str());
        return OB_ERR_MIGRATED;
    }

    // Backpressure: wait until pending queue has room.
    pending_cv_.wait(lock, [this]() {
        return pending_rows_.size() < MAX_PENDING_ROWS ||
               stop_flush_.load(std::memory_order_relaxed);
    });

    // 1. Tick local HLC to get a timestamp for this write.
    HLCTimestamp hlc_ts = hlc_->tick_local();

    OB_LOG_DEBUG("engine", "apply_delta_mm: sym=%s exch=%s hlc={%lu,%u,%u}",
                 delta.symbol, delta.exchange,
                 static_cast<unsigned long>(hlc_ts.physical_ns),
                 hlc_ts.logical, hlc_ts.node_id);

    // 2. Assign the sequence number for this node's stream, then write to WAL with origin
    //    and HLC (Requirement 2.1, 2.2). Each node numbers only its own stream, which is
    //    what makes (origin, sequence) comparable across a cluster.
    stamp_sequence(delta, mm_config_.node_id, symbol_key);
    wal_.append_with_origin(delta, levels, mm_config_.node_id, hlc_ts);

    // 3. Update conflict resolver HLC for each level.
    auto& resolver = const_cast<ConflictResolver&>(mm_mgr_->conflict_resolver());
    for (uint16_t i = 0; i < delta.n_levels; ++i) {
        ConflictKey ck{delta.symbol, delta.exchange, delta.side, levels[i].price};
        resolver.update_hlc(ck, hlc_ts, mm_config_.node_id);
    }

    // 4. Apply to SoA buffer (same logic as apply_delta).
    SoABuffer& buf = get_or_create_buffer(symbol_key, delta.symbol, delta.exchange);
    bool gap_detected = false;   // unused: gaps are decided per origin in stamp_sequence()
    ob_status_t status = ob::apply_delta(buf, delta, levels, gap_detected);

    // 5. Enqueue SnapshotRows for background columnar flush + notify subscribers.
    for (uint16_t i = 0; i < delta.n_levels; ++i) {
        SnapshotRow row{};
        row.timestamp_ns    = delta.timestamp_ns;
        row.sequence_number = delta.sequence_number;
        row.side            = delta.side;
        row.level_index     = i;
        row.price           = levels[i].price;
        row.quantity        = levels[i].qty;
        row.order_count     = levels[i].cnt;

        pending_rows_.push_back({delta.symbol, delta.exchange, row});
        query_engine_->notify_subscribers(delta.symbol, delta.exchange, row);
    }

    registry_.set_gauge("ob_pending_rows", static_cast<int64_t>(pending_rows_.size()));

    // 6. Broadcast to peers — with mtx_ released.
    //
    // This is the last step, and the unlock is the point of it rather than tidiness.
    // `broadcast_local()` takes MultiMasterManager's mutex, and the io loop holds that mutex
    // across the whole peer-fd branch — including `apply_remote_delta()`, which takes this one.
    // Holding mtx_ here made the two orders opposite: a client write going Engine → MM against a
    // received delta going MM → Engine. ThreadSanitizer reports that cycle within seconds of a
    // three-node cluster doing both, which is what every multi-master node does (#80).
    //
    // The cost of moving it out is that two concurrent writers can now reach the wire in an order
    // that differs from their WAL order. That is already the normal case for a receiver: catch-up
    // over-delivers and delivers out of order on purpose, records above the frontier are held
    // rather than rejected, and conflicts are resolved by HLC rather than by arrival. Nothing on
    // the receiving side reads arrival order as meaning anything.
    lock.unlock();

    if (mm_mgr_) {
        const size_t levels_bytes = delta.n_levels * sizeof(Level);
        const size_t payload_len  = sizeof(DeltaUpdate) + levels_bytes;

        alignas(8) uint8_t payload[sizeof(DeltaUpdate) + MAX_LEVELS * sizeof(Level)];
        std::memcpy(payload, &delta, sizeof(DeltaUpdate));
        if (levels_bytes > 0) {
            std::memcpy(payload + sizeof(DeltaUpdate), levels, levels_bytes);
        }

        WALRecordV2 hdr{};
        hdr.sequence_number = delta.sequence_number;
        hdr.timestamp_ns    = delta.timestamp_ns;
        hdr.checksum        = crc32c(payload, payload_len);
        hdr.payload_len     = static_cast<uint16_t>(payload_len);
        hdr.record_type     = WAL_RECORD_DELTA;
        hdr.version         = 1;
        hdr.origin_node_id  = mm_config_.node_id;
        hlc_ts.serialize(hdr.hlc_data);

        mm_mgr_->broadcast_local(hdr, payload, payload_len);
    }

    return status;
}

ob_status_t Engine::apply_remote_delta(const DeltaUpdate& delta_in, const Level* levels,
                                       uint16_t origin_node_id,
                                       const HLCTimestamp& remote_hlc) {
    // Only MultiMasterManager calls this, and it exists only in multi-master mode — but this
    // is a public method on a library type, and hlc_ / mm_mgr_ below are null without it.
    // Answering an error beats taking the process down.
    if (!hlc_ || !mm_mgr_) {
        OB_LOG_ERROR("engine",
                     "apply_remote_delta called with multi-master disabled: origin=%u sym=%s.%s",
                     static_cast<unsigned>(origin_node_id), delta_in.symbol, delta_in.exchange);
        return OB_ERR_INVALID_ARG;
    }

    DeltaUpdate delta = delta_in;   // see apply_delta() for why this is copied

    // Determine if this record originated from self (for WAL write decision).
    bool from_self = (origin_node_id == mm_config_.node_id);

    std::unique_lock<std::mutex> lock(mtx_);

    const std::string symbol_key = std::string(delta.symbol) + "." + delta.exchange;

    // Drop what we already applied, before the WAL append and before any state changes.
    // Catch-up over-delivers on purpose — it would rather send a record twice than lose it —
    // and storage is append-only, so applying a duplicate appends its rows a second time.
    // Measured without this check: four outage cycles turned 9 written rows into 25 stored
    // ones, trading #61's data loss for #26's duplicates.
    if (delta.sequence_number != 0 &&
        seq_tracker_.has_seen(symbol_key, origin_node_id, delta.sequence_number)) {
        OB_LOG_DEBUG("engine",
                     "Dropping duplicate remote record: sym=%s origin=%u seq=%llu",
                     symbol_key.c_str(), static_cast<unsigned>(origin_node_id),
                     static_cast<unsigned long long>(delta.sequence_number));
        registry_.increment_counter("ob_mm_duplicates_dropped");
        return OB_OK;
    }

    // The peer's number stays the peer's number: stamp_sequence() only assigns when the
    // record carries 0, which happens if the peer predates sequence numbering.
    stamp_sequence(delta, origin_node_id, symbol_key);

    OB_LOG_DEBUG("engine", "apply_remote_delta: origin=%u sym=%s exch=%s remote_hlc={%lu,%u,%u}",
                 origin_node_id, delta.symbol, delta.exchange,
                 static_cast<unsigned long>(remote_hlc.physical_ns),
                 remote_hlc.logical, remote_hlc.node_id);

    // 2. Merge remote HLC into local clock.
    hlc_->tick_receive(remote_hlc);

    // Update HLC drift metric.
    registry_.set_gauge("ob_mm_hlc_drift_ns", hlc_->max_drift_ns());

    // 3. Per-level conflict resolution.
    auto& resolver = const_cast<ConflictResolver&>(mm_mgr_->conflict_resolver());

    // Build a list of levels that should be applied (not rejected by conflict resolution).
    std::vector<uint16_t> winning_levels;
    winning_levels.reserve(delta.n_levels);

    for (uint16_t i = 0; i < delta.n_levels; ++i) {
        ConflictKey ck{delta.symbol, delta.exchange, delta.side, levels[i].price};
        ConflictResolution result = resolver.resolve(ck, remote_hlc, origin_node_id);

        if (result == ConflictResolution::REJECT_REMOTE) {
            OB_LOG_DEBUG("engine", "Conflict resolved: local_wins for %s/%s/%d/%ld",
                         delta.symbol, delta.exchange, delta.side,
                         static_cast<long>(levels[i].price));
            registry_.increment_counter("ob_mm_conflicts_total");
            continue;  // Skip this level — local is newer.
        }

        if (result == ConflictResolution::APPLY_REMOTE) {
            OB_LOG_DEBUG("engine", "Conflict resolved: remote_wins for %s/%s/%d/%ld",
                         delta.symbol, delta.exchange, delta.side,
                         static_cast<long>(levels[i].price));
            registry_.increment_counter("ob_mm_conflicts_total");
        }

        // NO_CONFLICT or APPLY_REMOTE → apply this level.
        resolver.update_hlc(ck, remote_hlc, origin_node_id);
        winning_levels.push_back(i);
    }

    // 4. Apply winning levels to SoA buffer.
    if (!winning_levels.empty()) {
        SoABuffer& buf = get_or_create_buffer(symbol_key, delta.symbol, delta.exchange);

        // Build a filtered DeltaUpdate + Level array for winning levels only.
        if (winning_levels.size() == static_cast<size_t>(delta.n_levels)) {
            // All levels won — apply directly.
            bool gap_detected = false;
            ob::apply_delta(buf, delta, levels, gap_detected);
            if (gap_detected) {
                wal_.append_gap(delta.sequence_number, delta.timestamp_ns);
            }
        } else {
            // Partial apply: build a new DeltaUpdate with only winning levels.
            DeltaUpdate filtered = delta;
            filtered.n_levels = static_cast<uint16_t>(winning_levels.size());

            Level filtered_levels[MAX_LEVELS];
            for (size_t j = 0; j < winning_levels.size(); ++j) {
                filtered_levels[j] = levels[winning_levels[j]];
            }

            bool gap_detected = false;
            ob::apply_delta(buf, filtered, filtered_levels, gap_detected);
            if (gap_detected) {
                wal_.append_gap(delta.sequence_number, delta.timestamp_ns);
            }
        }

        // Enqueue for columnar flush.
        for (uint16_t idx : winning_levels) {
            SnapshotRow row{};
            row.timestamp_ns    = delta.timestamp_ns;
            row.sequence_number = delta.sequence_number;
            row.side            = delta.side;
            row.level_index     = idx;
            row.price           = levels[idx].price;
            row.quantity        = levels[idx].qty;
            row.order_count     = levels[idx].cnt;

            pending_rows_.push_back({delta.symbol, delta.exchange, row});
            query_engine_->notify_subscribers(delta.symbol, delta.exchange, row);
        }
    }

    // 5. Write to WAL with original origin (preserve WAL_Origin for anti-entropy).
    // Skip WAL write for records that originated from self (they're already in our WAL).
    if (!from_self) {
        wal_.append_with_origin(delta, levels, origin_node_id, remote_hlc);
    }

    // 6. Do NOT broadcast further (single-hop propagation in full-mesh).

    registry_.set_gauge("ob_pending_rows", static_cast<int64_t>(pending_rows_.size()));

    return OB_OK;
}

std::string Engine::execute(std::string_view sql, RowCallback cb) {
    return query_engine_->execute(sql, std::move(cb));
}

std::string Engine::parse(std::string_view sql, QueryAST& out) {
    return query_engine_->parse(sql, out);
}

std::string Engine::format(const QueryAST& ast) {
    return query_engine_->format(ast);
}

uint64_t Engine::subscribe(std::string_view sql, RowCallback cb) {
    return query_engine_->subscribe(sql, std::move(cb));
}

void Engine::unsubscribe(uint64_t id) {
    query_engine_->unsubscribe(id);
}

Engine::Stats Engine::stats() {
    // Ask multi-master first, before taking mtx_.
    //
    // `peer_states()` and `connected_peer_count()` take MultiMasterManager's mutex, and the io
    // loop holds that one across `apply_remote_delta()`, which takes this one — the cycle #80 is
    // about. STATUS and every /metrics scrape come through here, so this was the second way into
    // it after the write path. Collected into locals, then copied in below.
    std::vector<PeerConnection> mm_peers;
    size_t   mm_connected  = 0;
    uint64_t mm_conflicts  = 0;
    uint64_t mm_ae_runs    = 0;
    uint64_t mm_ae_repairs = 0;
    bool     mm_ae_present = false;
    if (mm_config_.enabled && mm_mgr_) {
        mm_peers      = mm_mgr_->peer_states();
        mm_connected  = mm_mgr_->connected_peer_count();
        mm_conflicts  = mm_mgr_->conflict_resolver().total_conflicts();
        if (auto* ae = mm_mgr_->anti_entropy()) {
            mm_ae_present = true;
            mm_ae_runs    = ae->total_runs();
            mm_ae_repairs = ae->total_repairs();
        }
    }

    std::unique_lock<std::mutex> lock(mtx_);
    Stats s{};
    s.pending_rows      = pending_rows_.size();
    s.wal_file_index    = wal_.current_file_index();
    s.segment_count     = combined_store_.segment_count();
    s.symbol_count      = buffers_.size();
    s.flush_interval_ns = flush_interval_ns_;

    // Replication (primary): populate per-replica metrics (Requirements 5.1, 5.2).
    if (repl_mgr_) {
        const size_t current_offset = wal_.current_offset();
        for (const auto& r : repl_mgr_->replica_states()) {
            Stats::ReplicaMetrics rm;
            rm.address          = r.address;
            rm.confirmed_file   = r.confirmed_file;
            rm.confirmed_offset = r.confirmed_offset;
            rm.lag_bytes        = (current_offset > r.confirmed_offset)
                                    ? (current_offset - r.confirmed_offset) : 0;
            s.replicas.push_back(std::move(rm));
        }
    }

    // Replication (replica): populate client state (Requirement 5.3).
    if (repl_client_) {
        auto st = repl_client_->state();
        s.is_replica             = true;
        s.repl_confirmed_file    = st.confirmed_file;
        s.repl_confirmed_offset  = st.confirmed_offset;
        s.repl_records_replayed  = st.records_replayed;
        s.repl_connected         = st.connected;
        s.bootstrapping          = st.bootstrapping;
        s.snapshot_bytes_received = st.snapshot_bytes_received;
        s.snapshot_bytes_total   = st.snapshot_bytes_total;
    }

    // Snapshot transfer active on primary.
    if (repl_mgr_) {
        s.snapshot_active = repl_mgr_->snapshot_active();
    }

    // Failover state.
    s.node_role = node_role_.load(std::memory_order_relaxed);
    s.current_epoch = current_epoch_.load(std::memory_order_relaxed);
    if (failover_mgr_) {
        s.primary_address = failover_mgr_->primary_address();
        s.lease_ttl_remaining = failover_mgr_->lease_ttl_remaining();
    }

    // TTL / data retention metrics.
    s.ttl_hours            = ttl_config_.ttl_hours;
    s.ttl_segments_deleted = ttl_segments_deleted_.load(std::memory_order_relaxed);
    s.ttl_bytes_reclaimed  = ttl_bytes_reclaimed_.load(std::memory_order_relaxed);

    // Flush integrity.
    s.segment_merge_refused = segment_merge_refused_.load(std::memory_order_relaxed);

    // Sharding metrics: populated when ShardCoordinator is available (Task 8).
    // shard_id, shard_status, shard_symbols_count, shard_map_version,
    // migration_in_progress, migration_symbol, migration_target_shard,
    // migration_progress_pct, and shard_routing_errors are left at defaults
    // until ShardCoordinator integration.

    // Multi-master metrics.
    if (mm_config_.enabled) {
        s.mm_node_id = mm_config_.node_id;
        if (mm_mgr_) {
            s.mm_peer_count      = mm_peers.size();
            s.mm_connected_peers = mm_connected;
            s.mm_conflicts_total = mm_conflicts;
            // Only when the scheduler exists. Zero here would otherwise mean "no scheduler",
            // which is not the same statement as "it ran and found nothing" — the distinction
            // that made an earlier crash invisible.
            if (mm_ae_present) {
                s.mm_anti_entropy_runs    = mm_ae_runs;
                s.mm_anti_entropy_repairs = mm_ae_repairs;
            }
        }
        if (hlc_) {
            auto cur = hlc_->current();
            s.mm_hlc_physical_ns = cur.physical_ns;
            s.mm_hlc_logical = cur.logical;
            s.mm_hlc_drift_ns = hlc_->max_drift_ns();
        }
        // Per-peer replication lag, against the WAL offset as of this call.
        const size_t current_offset = wal_.current_offset();
        for (const auto& peer : mm_peers) {
            size_t lag = (current_offset > peer.confirmed_offset)
                             ? (current_offset - peer.confirmed_offset) : 0;
            s.mm_replication_lag_per_peer.emplace_back(peer.node_id, lag);
        }
    }

    return s;
}

// ── Snapshot operations ───────────────────────────────────────────────────────

SnapshotManifest Engine::create_snapshot() {
    return create_snapshot_with_sequence_state().manifest;
}

Engine::SnapshotWithSequenceState Engine::create_snapshot_with_sequence_state() {
    const auto t_start = std::chrono::steady_clock::now();

    SnapshotWithSequenceState out;
    SnapshotManifest& manifest = out.manifest;

    // Phase 1: flush + capture under lock (< 100ms).
    {
        // flush_mtx_ before mtx_: this path writes segments, so it must not run
        // alongside flush_loop() or a client FLUSH.
        std::lock_guard<std::mutex> flush_lock(flush_mtx_);
        std::unique_lock<std::mutex> lock(mtx_);

        // Flush all pending rows to columnar stores.
        wal_.sync();
        flush_drain_pending();

        // Flush all per-symbol columnar store active segments. The returned metas
        // must be merged, not dropped: SELECT reads combined_store_ only, so a
        // snapshot that flushed rows without merging them made the rows it had just
        // persisted disappear from every query until the next open_existing().
        std::vector<SegmentMeta> flushed;
        for (auto& [key, store] : stores_) {
            for (auto& rolled : store->take_rolled_segments()) {
                flushed.push_back(std::move(rolled));
            }
            auto meta = store->flush_segment();
            if (meta.has_value()) {
                flushed.push_back(std::move(meta.value()));
            }
        }
        segment_merge_refused_.fetch_add(combined_store_.merge_segments(flushed),
                                         std::memory_order_relaxed);

        // Capture WAL position atomically with the flush.
        manifest.wal_file_index  = wal_.current_file_index();
        manifest.wal_byte_offset = wal_.current_offset();

        // And the sequence state, in the same critical section. See the header for why the
        // boundary has to be exactly here and not a line later.
        out.vector = seq_tracker_.export_vector(kMaxPersistedVectorEntries, out.vector_truncated);
        out.held   = seq_tracker_.export_held(kMaxPersistedHeldRanges, out.held_truncated);
    }

    // Phase 2: enumerate files and compute CRC32C (lock-free, read-only).
    auto now = std::chrono::steady_clock::now().time_since_epoch();
    manifest.created_at_ns = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(now).count());

    size_t total_bytes = 0;
    size_t total_rows = 0;

    // Walk the data directory for segment files.
    if (fs::exists(base_dir_)) {
        for (auto& entry : fs::recursive_directory_iterator(base_dir_)) {
            if (!entry.is_regular_file()) continue;

            const auto& path = entry.path();
            const auto filename = path.filename().string();

            // Include every columnar file plus its metadata.
            //
            // Matched by extension rather than by an allowlist of names: the
            // allowlist version silently dropped side.col, level.col and seq.col
            // when they were added, which would have shipped replicas segments
            // the reader then rejects as incomplete. One place to forget is
            // better than two.
            const bool is_column_file = path.extension() == ".col";
            if (!is_column_file && filename != "meta.json") {
                continue;
            }

            // Skip WAL files and snapshot manifests.
            if (filename.find("wal_") == 0 || filename == "snapshot_manifest.json") {
                continue;
            }

            // Compute relative path from base_dir_.
            auto rel = fs::relative(path, base_dir_).string();
            auto file_size = static_cast<size_t>(entry.file_size());

            // Compute CRC32C.
            uint32_t crc = 0;
            {
                std::ifstream f(path.string(), std::ios::binary);
                if (f.is_open()) {
                    std::vector<uint8_t> buf(file_size);
                    f.read(reinterpret_cast<char*>(buf.data()),
                           static_cast<std::streamsize>(file_size));
                    crc = ob::crc32c(buf.data(), file_size);
                }
            }

            SnapshotFileEntry fe;
            fe.path   = std::move(rel);
            fe.size   = file_size;
            fe.crc32c = crc;
            manifest.files.push_back(std::move(fe));

            total_bytes += file_size;

            // Count rows from meta.json files.
            if (filename == "meta.json") {
                std::ifstream mf(path.string());
                if (mf.is_open()) {
                    std::string content((std::istreambuf_iterator<char>(mf)),
                                         std::istreambuf_iterator<char>());
                    // Extract row_count from meta.json.
                    auto rc_pos = content.find("\"row_count\":");
                    if (rc_pos != std::string::npos) {
                        rc_pos += 12;
                        uint64_t rc = 0;
                        while (rc_pos < content.size() && content[rc_pos] >= '0' && content[rc_pos] <= '9') {
                            rc = rc * 10 + static_cast<uint64_t>(content[rc_pos] - '0');
                            ++rc_pos;
                        }
                        total_rows += static_cast<size_t>(rc);
                    }
                }
            }
        }
    }

    manifest.total_bytes = total_bytes;
    manifest.total_rows  = total_rows;

    // Write snapshot_manifest.json (at-most-one policy: overwrite previous).
    {
        std::string manifest_path = base_dir_ + "/snapshot_manifest.json";
        std::ofstream f(manifest_path, std::ios::out | std::ios::trunc);
        if (f.is_open()) {
            f << manifest.to_json();
            f.flush();
        }
    }

    out.create_ms = std::chrono::duration<double, std::milli>(
                        std::chrono::steady_clock::now() - t_start).count();
    OB_LOG_INFO("engine",
                "Snapshot created: files=%zu bytes=%zu rows=%zu wal=%u:%zu vector=%zu%s "
                "held=%zu%s in %.1f ms",
                manifest.files.size(), manifest.total_bytes, manifest.total_rows,
                manifest.wal_file_index, manifest.wal_byte_offset,
                out.vector.size(), out.vector_truncated ? " (truncated)" : "",
                out.held.size(), out.held_truncated ? " (truncated)" : "",
                out.create_ms);

    return out;
}

void Engine::adopt_snapshot_sequence_state(
        const std::vector<SequenceTracker::VectorEntry>& vector,
        const std::vector<SequenceTracker::HeldRanges>& held) {
    std::lock_guard<std::mutex> lock(mtx_);

    seq_tracker_.reset();
    seq_tracker_.import_own_vector(vector);
    seq_tracker_.import_held(held);

    // A frontier for *our own* origin has to move the local counter with it. A node whose data
    // directory was wiped keeps its node id, so a peer can still hold records this node minted
    // before the wipe: minting from 1 again would hand out numbers the cluster has already seen,
    // and every peer would drop the new records as duplicates of rows this node no longer holds.
    const uint16_t self = mm_config_.node_id;
    for (const auto& e : vector) {
        if (e.origin == self) {
            seq_tracker_.raise_local(e.key, e.frontier);
        }
    }

    // Write it down before returning. Until the vector is in the WAL, a restart before the next
    // flush would come back with the frontiers of the contents that were just discarded.
    persist_version_vector_if_changed();
    refresh_version_vector_cache();

    OB_LOG_INFO("engine",
                "Adopted snapshot sequence state: entries=%zu held_entries=%zu symbols=%zu",
                vector.size(), held.size(), seq_tracker_.symbol_count());
}

void Engine::load_snapshot(const SnapshotManifest& /*manifest*/) {
    // flush_mtx_ first: clearing stores_ destroys the ColumnarStore objects that a
    // concurrent Phase B may be iterating over.
    std::lock_guard<std::mutex> flush_lock(flush_mtx_);
    std::unique_lock<std::mutex> lock(mtx_);

    // Clear all in-memory state.
    stores_.clear();
    buffers_.clear();
    live_ptrs_.clear();
    pending_rows_.clear();

    // Rebuild columnar index from the new files on disk.
    combined_store_.close();
    combined_store_.open_existing();
}

bool Engine::holds_no_data() {
    std::lock_guard<std::mutex> lock(mtx_);
    const bool empty = seq_tracker_.symbol_count() == 0 &&
                       pending_rows_.empty() &&
                       stores_.empty() &&
                       combined_store_.segment_count() == 0;
    OB_LOG_DEBUG("engine",
                 "holds_no_data=%d (symbols=%zu pending=%zu stores=%zu segments=%zu)",
                 empty ? 1 : 0, seq_tracker_.symbol_count(), pending_rows_.size(),
                 stores_.size(), combined_store_.segment_count());
    return empty;
}

bool Engine::is_bootstrapping() const {
    if (repl_client_) {
        return repl_client_->is_bootstrapping();
    }
    // And the multi-master path, which this used to miss entirely. ReplicationClient does not
    // exist in MM mode, so every caller of this — SELECT, FLUSH, and the write commands through
    // their own duplicated check — read "not bootstrapping" while an MM bootstrap was under way.
    // FLUSH is the one that mattered: it writes segments into the directory an install is about
    // to rename files into.
    if (mm_mgr_) {
        return mm_mgr_->is_bootstrapping();
    }
    return false;
}

// ── Symbol migration (sharding) ──────────────────────────────────────────────

SnapshotManifest Engine::create_symbol_snapshot(const std::string& symbol_key) {
    OB_LOG_INFO("engine", "Creating symbol snapshot: symbol_key=%s", symbol_key.c_str());

    SnapshotManifest manifest;

    {
        // flush_mtx_ before mtx_, same reason as create_snapshot().
        std::lock_guard<std::mutex> flush_lock(flush_mtx_);
        std::unique_lock<std::mutex> lock(mtx_);

        // Flush pending rows for this symbol to columnar stores.
        wal_.sync();
        flush_drain_pending();

        // Flush the per-symbol columnar store segment if it exists, merging the
        // metas for the same reason as create_snapshot(): a dropped meta hides the
        // rows it just wrote from every SELECT.
        auto it = stores_.find(symbol_key);
        if (it != stores_.end()) {
            std::vector<SegmentMeta> flushed = it->second->take_rolled_segments();
            auto meta = it->second->flush_segment();
            if (meta.has_value()) {
                flushed.push_back(std::move(meta.value()));
            }
            segment_merge_refused_.fetch_add(combined_store_.merge_segments(flushed),
                                         std::memory_order_relaxed);
        }

        // Capture WAL position atomically with the flush.
        manifest.wal_file_index  = wal_.current_file_index();
        manifest.wal_byte_offset = wal_.current_offset();
    }

    auto now = std::chrono::steady_clock::now().time_since_epoch();
    manifest.created_at_ns = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(now).count());

    // Stub: in a full implementation, enumerate segment files for this symbol
    // and populate manifest.files with per-symbol data.
    // For now, return the manifest with WAL position but no files.

    return manifest;
}

void Engine::load_symbol_snapshot(const std::string& symbol_key,
                                  const SnapshotManifest& manifest) {
    OB_LOG_INFO("engine", "Loading symbol snapshot: symbol_key=%s files=%zu",
                symbol_key.c_str(), manifest.files.size());

    // Stub: in a full implementation, load segments into per-symbol ColumnarStore
    // and restore SoA buffer from the snapshot data.
    // Requires ShardCoordinator (Task 8) for the full migration flow.
    (void)symbol_key;
    (void)manifest;
}

std::vector<uint8_t> Engine::get_symbol_wal_delta(const std::string& symbol_key,
                                                   uint32_t from_file,
                                                   size_t from_offset) {
    OB_LOG_DEBUG("engine", "Getting WAL delta: symbol_key=%s from_file=%u from_offset=%zu",
                 symbol_key.c_str(), from_file, from_offset);

    // Stub: in a full implementation, filter WAL records for the given symbol
    // from the specified position and return serialized delta bytes.
    // Requires WAL reader with symbol filtering (Task 8 migration flow).
    (void)symbol_key;
    (void)from_file;
    (void)from_offset;
    return {};
}

bool Engine::is_symbol_migrated(const std::string& symbol_key) const {
    // Note: caller should hold mtx_ or this should be called from a context
    // where migrated_symbols_ is not being concurrently modified.
    return migrated_symbols_.count(symbol_key) > 0;
}

void Engine::mark_symbol_migrated(const std::string& symbol_key) {
    std::unique_lock<std::mutex> lock(mtx_);
    migrated_symbols_.insert(symbol_key);
    OB_LOG_INFO("engine", "Marking symbol as migrated: symbol_key=%s", symbol_key.c_str());
}

// ── RoleTransitionHandler implementation ──────────────────────────────────────

void Engine::promote_to_primary(const EpochValue& new_epoch) {
    std::unique_lock<std::mutex> lock(mtx_);

    // Stop ReplicationClient if running.
    if (repl_client_) {
        lock.unlock();
        repl_client_->stop();
        repl_client_.reset();
        lock.lock();
    }

    // Increment epoch and write Epoch_Record to WAL.
    current_epoch_.store(new_epoch.term, std::memory_order_release);
    wal_.set_epoch(new_epoch.term);
    wal_.append_epoch(new_epoch);

    // Update gauge: current epoch after promotion.
    registry_.set_gauge("ob_current_epoch", static_cast<int64_t>(new_epoch.term));

    // Start ReplicationManager if not already running.
    if (!repl_mgr_ && repl_config_.port > 0) {
        repl_mgr_ = std::make_unique<ReplicationManager>(repl_config_, wal_);
        repl_mgr_->set_engine(this);
        lock.unlock();
        repl_mgr_->start();
        lock.lock();
    }

    node_role_.store(NodeRole::PRIMARY, std::memory_order_release);

    // Toggle dynamic read-only flag: PRIMARY accepts writes.
    if (read_only_flag_) {
        read_only_flag_->store(false, std::memory_order_release);
    }

    // Update metrics registry node role.
    registry_.set_node_role("primary");

    OB_LOG_INFO("engine", "promoted to PRIMARY, epoch=%" PRIu64, new_epoch.term);
}

void Engine::demote_to_replica(const std::string& new_primary_address) {
    std::unique_lock<std::mutex> lock(mtx_);

    // Stop ReplicationManager if running.
    if (repl_mgr_) {
        lock.unlock();
        repl_mgr_->stop();
        lock.lock();
        repl_mgr_.reset();
    }

    node_role_.store(NodeRole::REPLICA, std::memory_order_release);

    // Toggle dynamic read-only flag: REPLICA rejects writes.
    if (read_only_flag_) {
        read_only_flag_->store(true, std::memory_order_release);
    }

    // Update metrics registry node role and epoch gauge.
    registry_.set_node_role("replica");
    registry_.set_gauge("ob_current_epoch",
                        static_cast<int64_t>(current_epoch_.load(std::memory_order_relaxed)));

    // Start ReplicationClient to new primary.
    if (!new_primary_address.empty()) {
        // Stop existing ReplicationClient if running (may have been started
        // from static --primary-host config or a previous demote).
        if (repl_client_) {
            lock.unlock();
            repl_client_->stop();
            lock.lock();
            repl_client_.reset();
        }

        // Clear in-memory state to avoid data duplication during catchup.
        // The ReplicationClient will replay WAL records from the primary,
        // rebuilding the data from scratch. Without this, records that
        // already exist locally would be duplicated.
        OB_LOG_INFO("engine", "clearing local data before starting replication from %s",
                    new_primary_address.c_str());

        // flush_mtx_ guards this block: clearing stores_ destroys the ColumnarStore
        // objects a concurrent Phase B may be iterating. Taken here and not at the
        // top of the function on purpose — repl_mgr_->stop() above joins a thread
        // that can be inside create_snapshot() waiting for this very lock, so
        // holding it across the stop would deadlock the demotion.
        // Lock order is flush_mtx_ → mtx_, hence the release and reacquire.
        lock.unlock();
        std::unique_lock<std::mutex> flush_lock(flush_mtx_);
        lock.lock();

        stores_.clear();
        buffers_.clear();
        live_ptrs_.clear();
        pending_rows_.clear();

        // Close and wipe columnar store to prevent stale data from appearing in queries.
        combined_store_.close();

        // Delete all columnar segment directories on disk.
        // This is necessary because the node was previously PRIMARY with its own data,
        // and the new primary may have different data. Catchup will rebuild everything.
        {
            namespace fs = std::filesystem;
            std::error_code ec;
            for (auto& entry : fs::directory_iterator(base_dir_, ec)) {
                if (entry.is_directory() && entry.path().filename().string() != "." &&
                    entry.path().filename().string() != "..") {
                    // Skip WAL files (wal_*.bin) — only delete columnar segment dirs
                    auto name = entry.path().filename().string();
                    if (name.find("wal_") != 0) {
                        fs::remove_all(entry.path(), ec);
                    }
                }
            }
        }

        // Reopen empty columnar store.
        combined_store_.open_existing();

        // The store list is consistent again; release flush_mtx_ before starting the
        // ReplicationClient so a catch-up flush does not wait on this function.
        flush_lock.unlock();

        // Delete replication state file so catchup starts from position 0.
        {
            std::string state_path = base_dir_ + "/repl_state.txt";
            std::error_code ec;
            std::filesystem::remove(state_path, ec);
        }

        // Parse host:port from address.
        auto colon = new_primary_address.rfind(':');
        if (colon != std::string::npos) {
            ReplicationClientConfig cfg = repl_client_config_;
            cfg.primary_host = new_primary_address.substr(0, colon);
            cfg.primary_port = static_cast<uint16_t>(
                std::stoi(new_primary_address.substr(colon + 1)));
            repl_client_ = std::make_unique<ReplicationClient>(cfg, *this);
            lock.unlock();
            repl_client_->start();
            lock.lock();
        }
    }

    OB_LOG_INFO("engine", "demoted to REPLICA, primary=%s",
                new_primary_address.c_str());
}

std::pair<uint32_t, size_t> Engine::get_wal_position() const {
    return {wal_.current_file_index(), wal_.current_offset()};
}

EpochValue Engine::get_current_epoch() const {
    return EpochValue{current_epoch_.load(std::memory_order_acquire)};
}

void Engine::truncate_and_rebootstrap(const EpochValue& new_epoch,
                                      const std::string& primary_address) {
    current_epoch_.store(new_epoch.term, std::memory_order_release);

    // Request snapshot from new primary via ReplicationClient.
    OB_LOG_INFO("engine", "re-bootstrapping from %s, epoch=%" PRIu64,
                primary_address.c_str(), new_epoch.term);
}

NodeRole Engine::node_role() const {
    return node_role_.load(std::memory_order_acquire);
}

void Engine::set_read_only_flag(std::atomic<bool>* flag) {
    read_only_flag_ = flag;
}

uint64_t Engine::current_epoch() const {
    return current_epoch_.load(std::memory_order_acquire);
}

std::string Engine::handle_role_command() const {
    NodeRole role = node_role_.load(std::memory_order_acquire);
    uint64_t epoch = current_epoch_.load(std::memory_order_acquire);

    switch (role) {
    case NodeRole::PRIMARY:
        return "PRIMARY " + std::to_string(epoch) + "\n";
    case NodeRole::REPLICA: {
        std::string addr;
        if (failover_mgr_) {
            addr = failover_mgr_->primary_address();
        }
        return "REPLICA " + addr + " " + std::to_string(epoch) + "\n";
    }
    case NodeRole::MULTI_MASTER: {
        std::string hlc_str = "0.0.0";
        size_t peer_count = 0;
        if (hlc_) {
            hlc_str = hlc_->current().to_string();
        }
        if (mm_mgr_) {
            peer_count = mm_mgr_->connected_peer_count();
        }
        return "MULTI_MASTER " + std::to_string(mm_config_.node_id) + " " +
               hlc_str + " " + std::to_string(peer_count) + "\n";
    }
    case NodeRole::STANDALONE:
    default:
        return "STANDALONE\n";
    }
}

std::string Engine::handle_failover_command(const std::string& target_node_id) {
    if (node_role_.load(std::memory_order_acquire) != NodeRole::PRIMARY) {
        return "ERR not_primary\n";
    }
    if (!failover_mgr_) {
        return "ERR failover_not_configured\n";
    }

    using HandoverResult = FailoverManager::HandoverResult;
    const HandoverResult result =
        failover_mgr_->initiate_graceful_failover(target_node_id);

    OB_LOG_INFO("engine", "FAILOVER command: target=%s result=%d",
                target_node_id.c_str(), static_cast<int>(result));

    // Distinct codes so an operator can tell a typo in a node id from a
    // coordinator problem. Previously every failure was "failover_failed".
    switch (result) {
    case HandoverResult::OK:
        // Note: OK means the handover was initiated, not that it finished.
        // Confirmation is ROLE reporting PRIMARY on the target node.
        return "OK\n";
    case HandoverResult::NOT_PRIMARY:
        return "ERR not_primary\n";
    case HandoverResult::NOT_CONFIGURED:
        return "ERR failover_not_configured\n";
    case HandoverResult::INVALID_TARGET:
        return "ERR invalid_target " + target_node_id + "\n";
    case HandoverResult::UNKNOWN_TARGET:
        return "ERR unknown_target " + target_node_id + "\n";
    case HandoverResult::COORDINATOR_ERROR:
        break;
    }
    return "ERR failover_failed\n";
}

// ── Private helpers ───────────────────────────────────────────────────────────

SoABuffer& Engine::get_or_create_buffer(const std::string& symbol,
                                         const std::string& exchange) {
    return get_or_create_buffer(symbol + "." + exchange, symbol.c_str(), exchange.c_str());
}

SoABuffer& Engine::get_or_create_buffer(const std::string& key, const char* symbol,
                                        const char* exchange) {
    auto it = buffers_.find(key);
    if (it != buffers_.end()) return *it->second;

    auto buf = std::make_unique<SoABuffer>();
    buf->bid.depth = 0;
    buf->ask.depth = 0;
    buf->bid.version.store(0, std::memory_order_relaxed);
    buf->ask.version.store(0, std::memory_order_relaxed);
    buf->sequence_number.store(0, std::memory_order_relaxed);
    buf->last_timestamp_ns = 0;
    std::strncpy(buf->symbol,   symbol,   sizeof(buf->symbol)   - 1);
    std::strncpy(buf->exchange, exchange, sizeof(buf->exchange) - 1);

    live_ptrs_[key] = buf.get();
    auto& ref = *buf;
    buffers_[key] = std::move(buf);

    // Update gauge: symbol count after adding new symbol.
    registry_.set_gauge("ob_symbol_count", static_cast<int64_t>(buffers_.size()));

    return ref;
}

ColumnarStore& Engine::get_or_create_store(const std::string& symbol,
                                            const std::string& exchange) {
    const std::string key = symbol + "." + exchange;
    auto it = stores_.find(key);
    if (it != stores_.end()) return *it->second;

    auto store = std::make_unique<ColumnarStore>(base_dir_);
    store->set_symbol_exchange(symbol, exchange);
    auto& ref = *store;
    stores_[key] = std::move(store);
    return ref;
}

void Engine::flush_loop() {
    const auto interval = std::chrono::nanoseconds(flush_interval_ns_);
    while (!stop_flush_.load(std::memory_order_relaxed)) {
        // Interruptible wait. A plain sleep_for() here made close() block until
        // the current interval elapsed, because join() cannot interrupt a
        // sleeping thread: shutdown took up to flush_interval_ns_ for no reason,
        // and tests that open and close an Engine per case paid it every time.
        {
            std::unique_lock<std::mutex> lock(flush_stop_mtx_);
            const bool stop_requested = flush_stop_cv_.wait_for(
                lock, interval,
                [this]() { return stop_flush_.load(std::memory_order_relaxed); });
            if (stop_requested) {
                // close() performs the final drain, sync, segment flush and WAL
                // flush itself, so leaving now loses nothing.
                OB_LOG_DEBUG("engine", "flush_loop: stop requested, exiting");
                break;
            }
        }

        // The whole tick is one flush, so a client FLUSH cannot interleave with it.
        std::lock_guard<std::mutex> flush_lock(flush_mtx_);

        // Phase A: drain pending rows under mutex.
        {
            std::unique_lock<std::mutex> lock(mtx_);
            if (wal_.pending_sync_count() > 0) {
                wal_.sync();
            }
            flush_drain_pending();
        }

        // Phase B: segment I/O + merge, outside mtx_ so writers are not blocked.
        flush_write_and_merge();

        // Update gauge: WAL file index.
        registry_.set_gauge("ob_wal_file_index",
                            static_cast<int64_t>(wal_.current_file_index()));

        // WAL truncation and TTL scan under mutex.
        {
            std::unique_lock<std::mutex> lock(mtx_);

            // WAL truncation: only truncate files that ALL replicas have confirmed
            // past, so lagging replicas can still catch up (Requirement 6.3).
            uint32_t safe_truncate = wal_.current_file_index();
            if (repl_mgr_) {
                for (const auto& r : repl_mgr_->replica_states()) {
                    safe_truncate = std::min(safe_truncate, r.confirmed_file);
                }
            }
            if (safe_truncate > 0) {
                wal_.truncate_before(safe_truncate);
            }

            // TTL retention scan: delete expired segments periodically.
            if (ttl_config_.ttl_hours > 0) {
                auto now = std::chrono::steady_clock::now().time_since_epoch();
                uint64_t now_ns = static_cast<uint64_t>(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(now).count());
                uint64_t scan_interval_ns = ttl_config_.scan_interval_seconds * 1'000'000'000ULL;
                if (now_ns - last_ttl_scan_ns_ >= scan_interval_ns) {
                    uint64_t cutoff_ns = now_ns - ttl_config_.ttl_hours * 3600ULL * 1'000'000'000ULL;
                    auto [deleted, reclaimed] = combined_store_.delete_expired_segments(cutoff_ns);
                    ttl_segments_deleted_.fetch_add(deleted, std::memory_order_relaxed);
                    ttl_bytes_reclaimed_.fetch_add(reclaimed, std::memory_order_relaxed);
                    last_ttl_scan_ns_ = now_ns;
                }
            }
        }
    }
}

void Engine::flush_drain_pending() {
    // Phase A: drain pending_rows_ into per-symbol columnar stores.
    // Must be called with mtx_ held.
    //
    // The WAL position is read once, here, and stamped into every store this drain touches. It is
    // exact: `wal_.sync()` has just run and every row about to be appended came from a record at or
    // before this point, so a segment closed from these rows covers that symbol's WAL up to here.
    // That is the fact replay needs, and the reason it no longer has to guess from timestamps
    // (#63).
    const uint32_t wal_file   = wal_.current_file_index();
    const uint64_t wal_offset = static_cast<uint64_t>(wal_.current_offset());

    for (const auto& pr : pending_rows_) {
        ColumnarStore& store = get_or_create_store(pr.symbol, pr.exchange);
        store.set_wal_position(wal_identity_, wal_file, wal_offset);
        store.append(pr.row);
    }
    pending_rows_.clear();

    // Update gauge: pending rows is now 0 after drain.
    registry_.set_gauge("ob_pending_rows", 0);

    // Wake up any writers blocked on backpressure.
    pending_cv_.notify_all();
}

void Engine::flush_write_and_merge() {
    // Phase B: flush segments to disk and merge into combined_store_.
    // Caller holds flush_mtx_. Runs WITHOUT mtx_ (except the brief merge at the end)
    // so that disk I/O does not block writers.

    // Snapshot the store pointers under mtx_. stores_ is mutated by
    // get_or_create_store(), load_snapshot() and the REPLICA transition; iterating
    // it unlocked risked an invalidated iterator on insert and a use-after-free on
    // clear(). The raw pointers stay valid because every mutator of stores_ holds
    // flush_mtx_, which this caller holds too.
    std::vector<ColumnarStore*> stores;
    {
        std::unique_lock<std::mutex> lock(mtx_);
        stores.reserve(stores_.size());
        for (auto& [key, store] : stores_) {
            stores.push_back(store.get());
        }
    }

    std::vector<SegmentMeta> new_segments;
    for (ColumnarStore* store : stores) {
        // Segments closed by a rollover inside append() come first: append() has no
        // reference to the query index, so it parks their metas here. Left
        // uncollected, those rows are on disk and invisible to SELECT.
        for (auto& rolled : store->take_rolled_segments()) {
            new_segments.push_back(std::move(rolled));
        }
        auto meta = store->flush_segment();
        if (meta.has_value()) {
            new_segments.push_back(std::move(meta.value()));
        }
    }

    if (!new_segments.empty()) {
        std::unique_lock<std::mutex> lock(mtx_);
        const size_t refused = combined_store_.merge_segments(new_segments);
        if (refused > 0) {
            segment_merge_refused_.fetch_add(refused, std::memory_order_relaxed);
            registry_.set_gauge("ob_segment_merge_refused",
                                static_cast<int64_t>(
                                    segment_merge_refused_.load(std::memory_order_relaxed)));
        }

        // Update gauge: segment count after merge.
        registry_.set_gauge("ob_segment_count",
                            static_cast<int64_t>(combined_store_.segment_count()));

        // Record that everything written before now is durable in segments, so the
        // next open() does not replay it. Appended AFTER the segments are on disk,
        // never before: a checkpoint that claims more than is durable turns a crash
        // into data loss, while one that claims less only costs a replay that the
        // timestamp guard in replay_wal_tail() filters.
        wal_.append_checkpoint(static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count()));

        // And what this node holds, so a restart does not have to relearn it. Written next to
        // the checkpoint because that is where the WAL tail is cut: a vector after the last
        // checkpoint is one the next replay would find anyway.
        persist_version_vector_if_changed();
    }
}

void Engine::apply_delta_replayed(const DeltaUpdate& delta, const Level* levels) {
    // Caller holds mtx_.
    //
    // No number is assigned here: this record was written once already and carries its
    // number. seed() restores the counters from it without reporting a gap — the gap, if
    // there was one, was recorded when the records were first written, and re-reporting it
    // would append a second GAP for the same hole on every restart.
    const std::string symbol_key = std::string(delta.symbol) + "." + delta.exchange;
    seq_tracker_.seed(symbol_key, /*origin=*/mm_config_.node_id, delta.sequence_number);

    SoABuffer& buf = get_or_create_buffer(symbol_key, delta.symbol, delta.exchange);
    bool gap_detected = false;
    (void)ob::apply_delta(buf, delta, levels, gap_detected);

    for (uint16_t i = 0; i < delta.n_levels; ++i) {
        SnapshotRow row{};
        row.timestamp_ns    = delta.timestamp_ns;
        row.sequence_number = delta.sequence_number;
        row.side            = delta.side;
        row.level_index     = i;
        row.price           = levels[i].price;
        row.quantity        = levels[i].qty;
        row.order_count     = levels[i].cnt;

        pending_rows_.push_back({delta.symbol, delta.exchange, row});
    }
}

uint64_t Engine::replay_wal_tail() {
    // What each symbol already has on disk, so a record a segment covers is not applied twice.
    // This closes the window between writing segments and appending the checkpoint: a crash in
    // there replays records that are already durable, and duplicated rows are as wrong as lost
    // ones.
    //
    // Two answers, and the first one is a fact. Every segment records the WAL position its rows
    // came from, so "this record is already stored" is a position comparison — and a per-symbol
    // one, which is sound even when a crash left one symbol's segment written and another's not.
    // The timestamp comparison is the fallback for segments written before positions were
    // recorded; it is exact only while timestamps for a symbol increase, which a single node
    // guarantees and multi-master does not, because a peer's record carries the origin's clock
    // (#63).
    struct DurableUpTo {
        uint32_t wal_file_index{0};
        uint64_t wal_byte_offset{0};
        bool     has_position{false};
        uint64_t end_ts_ns{0};
    };
    std::unordered_map<std::string, DurableUpTo> durable;
    for (const auto& meta : combined_store_.index()) {
        const std::string key = meta.symbol + "." + meta.exchange;
        auto& d = durable[key];
        if (meta.end_ts_ns > d.end_ts_ns) d.end_ts_ns = meta.end_ts_ns;
        // Trust a position only if it was written against this WAL. A snapshot or a shard
        // migration ships whole segment directories, so a received segment carries the sender's
        // position — believing it would skip records this node never stored. A zero position means
        // the segment predates this record-keeping, not offset zero.
        const bool has_pos = meta.wal_identity != 0 && meta.wal_identity == wal_identity_ &&
                             (meta.wal_file_index != 0 || meta.wal_byte_offset != 0);
        if (has_pos) {
            const bool later = !d.has_position ||
                               meta.wal_file_index > d.wal_file_index ||
                               (meta.wal_file_index == d.wal_file_index &&
                                meta.wal_byte_offset > d.wal_byte_offset);
            if (later) {
                d.wal_file_index  = meta.wal_file_index;
                d.wal_byte_offset = meta.wal_byte_offset;
                d.has_position    = true;
            }
        }
    }

    uint64_t applied = 0;
    uint64_t skipped = 0;
    uint64_t skipped_by_timestamp = 0;
    uint64_t records = 0;

    WALReplayer replayer(base_dir_);
    replayer.replay_after_checkpoint([&](const WALReplayContext& ctx) {
        ++records;
        if (ctx.header.record_type != WAL_RECORD_DELTA) return;
        if (ctx.payload_len < sizeof(DeltaUpdate)) {
            OB_LOG_WARN("engine", "WAL replay: DELTA payload too short (%zu < %zu), skipping",
                        ctx.payload_len, sizeof(DeltaUpdate));
            return;
        }

        DeltaUpdate delta{};
        std::memcpy(&delta, ctx.payload, sizeof(DeltaUpdate));

        const size_t levels_bytes = static_cast<size_t>(delta.n_levels) * sizeof(Level);
        if (sizeof(DeltaUpdate) + levels_bytes > ctx.payload_len) {
            OB_LOG_WARN("engine",
                        "WAL replay: payload holds %zu bytes but %u levels need %zu, skipping",
                        ctx.payload_len, delta.n_levels, sizeof(DeltaUpdate) + levels_bytes);
            return;
        }

        const std::string key = std::string(delta.symbol) + "." + delta.exchange;
        auto it = durable.find(key);
        if (it != durable.end()) {
            const auto& d = it->second;
            if (d.has_position) {
                const bool already_stored =
                    ctx.wal_file_index < d.wal_file_index ||
                    (ctx.wal_file_index == d.wal_file_index &&
                     ctx.wal_byte_offset < d.wal_byte_offset);
                if (already_stored) {
                    ++skipped;
                    return;
                }
            } else if (delta.timestamp_ns <= d.end_ts_ns) {
                // Legacy segment with no recorded position. Same behaviour as before, and the same
                // assumption: it holds on a single node and can misfire in multi-master.
                ++skipped_by_timestamp;
                return;
            }
        }

        const auto* levels = reinterpret_cast<const Level*>(
            ctx.payload + sizeof(DeltaUpdate));

        std::unique_lock<std::mutex> lock(mtx_);
        apply_delta_replayed(delta, levels);
        ++applied;
    });

    // skipped should be zero on a clean checkpoint; anything else means the crash
    // landed between writing segments and recording that fact.
    OB_LOG_INFO("engine",
                "WAL replay: records=%llu applied=%llu skipped_by_position=%llu "
                "skipped_by_timestamp=%llu",
                static_cast<unsigned long long>(records),
                static_cast<unsigned long long>(applied),
                static_cast<unsigned long long>(skipped),
                static_cast<unsigned long long>(skipped_by_timestamp));
    if (skipped_by_timestamp > 0) {
        OB_LOG_WARN("engine",
                    "%llu records were skipped by timestamp because their symbol's segments carry "
                    "no WAL position — pre-#63 data, where an out-of-order timestamp can hide a "
                    "record that was never stored",
                    static_cast<unsigned long long>(skipped_by_timestamp));
    }

    registry_.set_gauge("ob_pending_rows", static_cast<int64_t>(pending_rows_.size()));
    return applied;
}

void Engine::flush_incremental() {
    // One flush at a time, whichever thread asks. A client FLUSH arriving while
    // flush_loop() was mid-tick used to produce two Phase B passes over the same
    // active segment, and the segment ended up in the query index twice.
    std::lock_guard<std::mutex> flush_lock(flush_mtx_);

    // Phase A: lock → WAL sync → drain pending rows → unlock
    {
        std::unique_lock<std::mutex> lock(mtx_);
        wal_.sync();
        flush_drain_pending();
    }
    // Phase B: segment I/O + merge, outside mtx_ so writers are not blocked.
    flush_write_and_merge();
}

} // namespace ob
