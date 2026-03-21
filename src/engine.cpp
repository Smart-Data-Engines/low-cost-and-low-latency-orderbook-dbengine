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

#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>

namespace ob {

namespace fs = std::filesystem;

Engine::Engine(std::string_view base_dir, uint64_t flush_interval_ns,
               FsyncPolicy fsync_policy,
               ReplicationConfig repl_config,
               ReplicationClientConfig repl_client_config)
    : base_dir_(base_dir)
    , flush_interval_ns_(flush_interval_ns)
    , wal_(base_dir, 512ULL << 20, fsync_policy)
    , combined_store_(base_dir)
    , query_engine_(std::make_unique<QueryEngine>(combined_store_, live_ptrs_, agg_))
    , repl_config_(std::move(repl_config))
    , repl_client_config_(std::move(repl_client_config))
{}

Engine::~Engine() {
    close();
}

void Engine::open() {
    // Replay WAL to restore any updates not yet in the columnar store.
    // (Simplified: we rebuild the columnar index from persisted segments;
    //  a full implementation would re-apply WAL records to the SoA buffers.)
    WALReplayer replayer(base_dir_);
    replayer.replay([this](const WALRecord& /*rec*/, const uint8_t* /*payload*/) {
        // WAL replay: in a full implementation, reconstruct DeltaUpdate from
        // the payload and re-apply to the SoA buffer / columnar store.
        // For now we rely on the columnar store's persisted segments.
    });

    // Rebuild columnar segment index from persisted meta.json files.
    combined_store_.open_existing();

    // Start ReplicationManager if configured as primary (Requirement 7.4).
    if (repl_config_.port > 0) {
        repl_mgr_ = std::make_unique<ReplicationManager>(repl_config_, wal_);
        repl_mgr_->set_engine(this);
        repl_mgr_->start();
    }

    // Start ReplicationClient if configured as replica (Requirement 7.4).
    if (repl_client_config_.primary_port > 0) {
        repl_client_ = std::make_unique<ReplicationClient>(repl_client_config_, *this);
        repl_client_->start();
    }

    // Start background flush thread.
    stop_flush_.store(false, std::memory_order_relaxed);
    flush_thread_ = std::thread([this]() { flush_loop(); });
}

void Engine::close() {
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
    if (flush_thread_.joinable()) {
        flush_thread_.join();
    }

    // Final flush of all pending rows under the lock.
    {
        std::unique_lock<std::mutex> lock(mtx_);
        // Group commit: sync any remaining WAL records.
        wal_.sync();
        flush_pending();
    }

    // Flush each per-symbol columnar store.
    for (auto& [key, store] : stores_) {
        store->flush_segment();
    }

    // Flush WAL to disk.
    wal_.flush();
}

ob_status_t Engine::apply_delta(const DeltaUpdate& delta, const Level* levels) {
    std::unique_lock<std::mutex> lock(mtx_);

    // Backpressure: wait until pending queue has room.
    // This blocks the writer if the flush thread can't keep up.
    pending_cv_.wait(lock, [this]() {
        return pending_rows_.size() < MAX_PENDING_ROWS ||
               stop_flush_.load(std::memory_order_relaxed);
    });

    // 1. Write to WAL before any state mutation (Requirement 8.1).
    //    No fsync here — group commit via flush_loop() or close().
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
    SoABuffer& buf = get_or_create_buffer(delta.symbol, delta.exchange);
    bool gap_detected = false;
    ob_status_t status = ob::apply_delta(buf, delta, levels, gap_detected);

    // 3. Record gap event in WAL if sequence number was non-consecutive (Requirement 1.5).
    if (gap_detected) {
        wal_.append_gap(delta.sequence_number, delta.timestamp_ns);
    }

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

    return status;
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

    return s;
}

// ── Snapshot operations ───────────────────────────────────────────────────────

SnapshotManifest Engine::create_snapshot() {
    SnapshotManifest manifest;

    // Phase 1: flush + capture under lock (< 100ms).
    {
        std::unique_lock<std::mutex> lock(mtx_);

        // Flush all pending rows to columnar stores.
        wal_.sync();
        flush_pending();

        // Flush all per-symbol columnar store active segments.
        for (auto& [key, store] : stores_) {
            store->flush_segment();
        }

        // Capture WAL position atomically with the flush.
        manifest.wal_file_index  = wal_.current_file_index();
        manifest.wal_byte_offset = wal_.current_offset();
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

            // Only include columnar segment files and meta.json.
            if (filename != "price.col" && filename != "qty.col" &&
                filename != "ts.col" && filename != "cnt.col" &&
                filename != "meta.json") {
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

    return manifest;
}

void Engine::load_snapshot(const SnapshotManifest& /*manifest*/) {
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

bool Engine::is_bootstrapping() const {
    if (repl_client_) {
        return repl_client_->is_bootstrapping();
    }
    return false;
}

// ── Private helpers ───────────────────────────────────────────────────────────

SoABuffer& Engine::get_or_create_buffer(const std::string& symbol,
                                         const std::string& exchange) {
    const std::string key = symbol + "." + exchange;
    auto it = buffers_.find(key);
    if (it != buffers_.end()) return *it->second;

    auto buf = std::make_unique<SoABuffer>();
    buf->bid.depth = 0;
    buf->ask.depth = 0;
    buf->bid.version.store(0, std::memory_order_relaxed);
    buf->ask.version.store(0, std::memory_order_relaxed);
    buf->sequence_number.store(0, std::memory_order_relaxed);
    buf->last_timestamp_ns = 0;
    std::strncpy(buf->symbol,   symbol.c_str(),   sizeof(buf->symbol)   - 1);
    std::strncpy(buf->exchange, exchange.c_str(), sizeof(buf->exchange) - 1);

    live_ptrs_[key] = buf.get();
    auto& ref = *buf;
    buffers_[key] = std::move(buf);
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
        std::this_thread::sleep_for(interval);
        std::unique_lock<std::mutex> lock(mtx_);
        // Group commit: sync WAL to disk periodically instead of per-record.
        if (wal_.pending_sync_count() > 0) {
            wal_.sync();
        }
        flush_pending();

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
    }
}

void Engine::flush_pending() {
    // Drain pending_rows_ into per-symbol columnar stores.
    for (const auto& pr : pending_rows_) {
        ColumnarStore& store = get_or_create_store(pr.symbol, pr.exchange);
        store.append(pr.row);
    }
    pending_rows_.clear();

    // Wake up any writers blocked on backpressure.
    pending_cv_.notify_all();
}

} // namespace ob
