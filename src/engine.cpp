// Feature: orderbook-dbengine — Engine facade (Requirements 7.3, 7.4, 7.5, 8.1, 8.3)
//
// The Engine owns and coordinates all subsystems:
//   WALWriter, SoABuffer map, ColumnarStore map, AggregationEngine, QueryEngine.
//
// apply_delta flow: WAL write → SoA buffer apply (gap detection) → enqueue for columnar flush.
// open():  replay WAL + rebuild columnar index + start background flush thread.
// close(): stop flush thread + final flush + flush_segment + WAL flush.

#include "orderbook/engine.hpp"

#include <chrono>
#include <cstring>

namespace ob {

// ── Portable CRC32C (software lookup table, same as wal.cpp) ──────────────────
namespace {

static constexpr uint32_t CRC32C_POLY = 0x82F63B78u;
static uint32_t crc32c_table[256];
static bool     crc32c_table_init = false;

static void init_crc32c_table() {
    for (uint32_t i = 0; i < 256; ++i) {
        uint32_t crc = i;
        for (int j = 0; j < 8; ++j) {
            crc = (crc >> 1) ^ ((crc & 1u) ? CRC32C_POLY : 0u);
        }
        crc32c_table[i] = crc;
    }
    crc32c_table_init = true;
}

static uint32_t crc32c(const void* data, size_t len) {
    if (!crc32c_table_init) init_crc32c_table();
    const auto* p = static_cast<const uint8_t*>(data);
    uint32_t crc = 0xFFFFFFFFu;
    for (size_t i = 0; i < len; ++i) {
        crc = (crc >> 8) ^ crc32c_table[(crc ^ p[i]) & 0xFFu];
    }
    return crc ^ 0xFFFFFFFFu;
}

} // anonymous namespace

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
    }

    return s;
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
