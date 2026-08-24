#pragma once

#include "orderbook/data_model.hpp"
#include "orderbook/mmap_store.hpp"

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace ob {

/// Metadata for a single columnar segment.
/// Version of the on-disk segment layout.
///
/// Version 1 stored only ts/price/qty/cnt and silently zeroed side, level_index
/// and sequence_number on read, which lost the order side of every row that made
/// it past a flush. Version 2 stores all seven columns.
inline constexpr uint32_t kColumnarFormatVersion = 2;

struct SegmentMeta {
    uint32_t format_version{kColumnarFormatVersion};
    uint64_t start_ts_ns;   ///< earliest timestamp in this segment
    uint64_t end_ts_ns;     ///< latest timestamp in this segment
    uint64_t row_count;     ///< number of rows stored
    uint64_t first_price;   ///< absolute price anchor for delta decoding (zigzag-encoded)
    bool     has_raw_qty;   ///< true if any qty used raw uint64 fallback
    uint64_t max_sequence_number{0};  ///< highest sequence number in this segment; 0 in
                                      ///< segments written before numbers were assigned,
                                      ///< which is the truth about that data. Read at
                                      ///< startup so the next number cannot repeat one
                                      ///< already durable.
    /// WAL position whose records are all durable in this segment, for this symbol.
    ///
    /// Every row here came from a record at or before this position, because a flush drains all
    /// pending rows first and only then writes segments. That makes the pair below an exact,
    /// per-symbol answer to "is this replayed record already stored?" — which timestamps cannot
    /// give in multi-master, where a peer's record carries the origin's clock and can sit below a
    /// segment's `end_ts_ns` for the same symbol (#63).
    ///
    /// Both zero in segments written before this was recorded, which is the truth about that data:
    /// replay falls back to the timestamp comparison for those.
    /// Identity of the WAL the position below refers to. A snapshot and a shard migration ship
    /// whole segment directories, `meta.json` included, so the position in a received segment is
    /// the *sender's* — meaningless here, and dangerous if believed, because skipping by a foreign
    /// position would drop records this node never stored. Recovery trusts the position only when
    /// this matches the local WAL's identity; 0 means "written before identities existed".
    uint64_t wal_identity{0};
    uint32_t wal_file_index{0};
    uint64_t wal_byte_offset{0};
    std::string symbol;     ///< symbol this segment belongs to
    std::string exchange;   ///< exchange this segment belongs to
    std::string dir_path;   ///< full path to the segment directory
};

/// Columnar storage engine for SnapshotRow data.
///
/// Directory layout:
///   <base_dir>/<symbol>/<exchange>/<start_ts_ns>_<end_ts_ns>/
///     price.col  — zigzag(delta(price)) encoded uint64 values
///     qty.col    — Simple8b encoded uint64 quantities
///     ts.col     — raw uint64 nanosecond timestamps
///     cnt.col    — raw uint32 order counts
///     meta.json  — SegmentMeta as JSON
///
/// Thread safety: index_mtx_ guards the segment index and rolled_segments_, so
/// scan() and merge_segments() are safe against each other. append() and
/// flush_segment() are NOT: they touch the column buffers and the active-segment
/// flags with no lock, deliberately, because append() sits on the drain hot path.
/// Callers must serialise them — Engine does so with flush_mtx_. Two unsynchronised
/// flush_segment() calls each write the same directory and each return a valid meta,
/// which is how the same segment once landed in the query index twice.
class ColumnarStore {
public:
    explicit ColumnarStore(std::string_view base_dir,
                           uint64_t segment_duration_ns = 3600ULL * 1'000'000'000ULL);

    ~ColumnarStore() { close(); }

    // Non-copyable, non-movable
    ColumnarStore(const ColumnarStore&)            = delete;
    ColumnarStore& operator=(const ColumnarStore&) = delete;
    ColumnarStore(ColumnarStore&&)                 = delete;
    ColumnarStore& operator=(ColumnarStore&&)      = delete;

    /// Append a row to the active segment, rolling over if needed.
    void append(const SnapshotRow& row);

    /// Set the symbol and exchange for this store (used by C API wrapper).
    /// Must be called before the first append if symbol/exchange metadata is needed.
    void set_symbol_exchange(std::string_view symbol, std::string_view exchange) {
        symbol_   = std::string(symbol);
        exchange_ = std::string(exchange);
    }

    /// Record the WAL position whose records are covered by the rows appended from here on.
    ///
    /// Called at the start of a drain, when everything the engine is about to append came from a
    /// record at or before this position. Every segment closed afterwards — by `flush_segment()` or
    /// by a rollover inside `append()` — is stamped with it, which is what lets replay decide by
    /// position instead of by timestamp (#63).
    void set_wal_position(uint64_t wal_identity, uint32_t file_index, uint64_t byte_offset) {
        wal_identity_    = wal_identity;
        wal_file_index_  = file_index;
        wal_byte_offset_ = byte_offset;
    }

    /// Flush the active segment: encode buffers, write column files, write meta.json.
    /// Returns the SegmentMeta of the flushed segment, or std::nullopt if no active segment.
    std::optional<SegmentMeta> flush_segment();

    /// Return and clear the metas of segments closed by a rollover inside append().
    ///
    /// append() cannot merge them itself: it has no reference to the index that
    /// queries read. Whoever owns this store must collect them and merge them, or
    /// those rows sit on disk invisible to every SELECT until the next
    /// open_existing().
    std::vector<SegmentMeta> take_rolled_segments();

    /// Time-range scan; calls cb for each decoded row in [start_ns, end_ns].
    void scan(uint64_t start_ns, uint64_t end_ns,
              std::string_view symbol, std::string_view exchange,
              std::function<void(const SnapshotRow&)> cb) const;

    /// Called on startup to rebuild segment index from persisted meta.json files.
    void open_existing();

    /// Flush active segment and release resources.
    void close();

    /// Delete segments whose end_ts_ns < cutoff_ns.
    /// Returns {segments_deleted, bytes_reclaimed}.
    /// Deletes in chronological order (oldest first).
    /// Logs each deletion to stderr.
    /// Skips segments that fail to delete (logs error, continues).
    std::pair<size_t, size_t> delete_expired_segments(uint64_t cutoff_ns);

    /// Number of segments in the index (including active if flushed).
    size_t segment_count() const {
        std::shared_lock<std::shared_mutex> lock(index_mtx_);
        return index_.size();
    }

    /// Access the segment index (read-only snapshot).
    /// NOTE: Returns a copy for thread safety. Use scan() for iteration.
    std::vector<SegmentMeta> index() const {
        std::shared_lock<std::shared_mutex> lock(index_mtx_);
        return index_;
    }

    /// Merge new segments into the index, maintaining sort order by start_ts_ns.
    ///
    /// Returns the number of segments refused because their directory was already
    /// indexed. A non-zero return means two flush paths raced: the duplicate rows
    /// were kept out of the index, but the race itself still needs fixing, so the
    /// caller should surface the count rather than ignore it.
    size_t merge_segments(const std::vector<SegmentMeta>& new_segments);

private:
    std::string base_dir_;
    uint64_t    segment_duration_ns_;

    // Active segment state
    uint64_t    wal_identity_{0};
    uint32_t    wal_file_index_{0};
    uint64_t    wal_byte_offset_{0};
    std::string symbol_;
    std::string exchange_;
    uint64_t    active_segment_start_{0};
    uint64_t    active_row_count_{0};
    bool        active_has_raw_qty_{false};
    bool        has_active_segment_{false};

    // Accumulation buffers for the active segment
    std::vector<int64_t>  price_buf_;
    std::vector<uint64_t> qty_buf_;
    std::vector<uint64_t> ts_buf_;
    std::vector<uint32_t> cnt_buf_;
    std::vector<uint8_t>  side_buf_;
    std::vector<uint16_t> level_buf_;
    // int64 rather than uint64 because encode_prices() takes int64. Sequence
    // numbers never approach 2^63 in practice, and zigzag handles the sign, which
    // matters in multi-master mode where sequences from different nodes can land
    // in one segment out of order.
    std::vector<int64_t>  seq_buf_;

    /// Segments closed by a rollover inside append(), waiting to be collected by
    /// take_rolled_segments(). Guarded by index_mtx_ because it crosses the
    /// append → flush boundary.
    std::vector<SegmentMeta> rolled_segments_;

    // Segment index (rebuilt from meta.json on open_existing)
    std::vector<SegmentMeta> index_;

    // Protects index_ for concurrent scan() (shared) vs merge_segments()/open_existing() (exclusive)
    mutable std::shared_mutex index_mtx_;

    // Helpers
    std::string segment_dir(const std::string& symbol, const std::string& exchange,
                            uint64_t start_ts, uint64_t end_ts) const;
    void ensure_dirs(const std::string& path) const;
    void write_meta_json(const std::string& dir, const SegmentMeta& meta) const;
    bool parse_meta_json(const std::string& path, SegmentMeta& out) const;
};

} // namespace ob
