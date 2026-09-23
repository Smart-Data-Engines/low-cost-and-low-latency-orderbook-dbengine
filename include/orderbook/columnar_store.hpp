#pragma once

#include "orderbook/data_model.hpp"
#include "orderbook/query_columns.hpp"

#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
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
    /// The timestamp of the row appended **last**, which is not the latest when rows reach a flush
    /// out of time order. Replay's fallback, for a symbol with no trusted WAL position, compares a
    /// record's time with this (#63) - and #166 must not change what that fallback reads: before
    /// #166 `end_ts_ns` held exactly this number, so a segment written then carries it over.
    uint64_t last_row_ts_ns{0};
    /// Whether the two above are the minimum and the maximum of the rows' timestamps, which is what
    /// their comments have always said. False only for a segment written before #166 and not yet
    /// repaired: its range was the start of the period its first row fell in and the time of its
    /// last row, so a row that reached a flush out of time order was outside it - skipped by a
    /// query that asked for it, and deleted by retention with a segment it thought was old.
    bool time_range_is_rows{false};
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

/// The rows one drain gave one symbol, held in memory until the flush tick seals them into a
/// segment (#165 part 2a).
///
/// A tick used to write a segment for every symbol that had received a row since the last one: at
/// 256 symbols about two thousand files and a syncfs() each tick, with 84.7% of the flush thread's
/// CPU in the kernel creating them, and a node gained one segment per active symbol per tick. Rows
/// now wait in blocks until their symbol has enough of them or they are old enough, and a query
/// reads them here meanwhile.
///
/// Immutable once published, so a query reads one with no lock held; shared, so an index that
/// drops it - a seal, a snapshot install - does not free what a query is still reading (#92).
struct RowBlock {
    std::string symbol;
    std::string exchange;
    std::vector<SnapshotRow> rows;   ///< in the order they were drained
    uint64_t min_ts_ns{0};
    uint64_t max_ts_ns{0};

    /// A block of these rows, its range computed from them; null for none, because a symbol that
    /// received no row this drain has no block.
    static std::shared_ptr<const RowBlock> make(std::string symbol, std::string exchange,
                                                std::vector<SnapshotRow> rows);
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
    static constexpr uint64_t kDefaultSegmentDurationNs = 3600ULL * 1'000'000'000ULL;

    /// Whether this store indexes the segments it writes itself (#165).
    ///
    /// A store on its own - a test, a tool - has to: it is the only index there is. A store whose
    /// segments go to someone else's index must not. The engine's and the C API's stores keep one
    /// symbol's rows each and hand every segment they write to a combined store, which is the one
    /// every query reads and the one retention prunes; the copy each of them also kept was read by
    /// nothing, pruned by nothing, and grew by one `SegmentMeta` per segment for the life of the
    /// process.
    enum class OwnIndex { kYes, kNo };

    explicit ColumnarStore(std::string_view base_dir,
                           uint64_t segment_duration_ns = kDefaultSegmentDurationNs,
                           OwnIndex own_index = OwnIndex::kYes);

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
    /// The symbol and exchange this store holds, for a message that has to name them.
    const std::string& symbol() const { return symbol_; }
    const std::string& exchange() const { return exchange_; }

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
    ///
    /// `columns` is what to read. A column file outside it is not opened, not decoded, and the
    /// field it fills is left at its default in every row - so a caller that reads a field it did
    /// not ask for gets a plausible zero rather than a value. The set is the caller's statement
    /// of what it will look at.
    ///
    /// `TimestampNs` is added to the set whichever way it arrives: the scan compares every row's
    /// timestamp against the range, so a set without it would filter against a column it never
    /// read.
    ///
    /// **Required, not defaulted.** A default would be `all()`, which is correct for every
    /// caller and therefore never wrong enough for anyone to notice they had not chosen.
    ///
    /// Returns what the scan looked at in the index (#165): the segments of this symbol it
    /// compared with the range, and the candidates among them - those whose recorded range meets
    /// it, whose files it then opens. The first is what the index costs a query; what it compared
    /// and did not need is bounded per width tier (`WidthTier`).
    struct ScanCost {
        size_t compared{0};
        size_t candidates{0};
        /// Published blocks whose range met the query, read from memory (#165 part 2a).
        size_t blocks{0};
    };
    ScanCost scan(uint64_t start_ns, uint64_t end_ns,
                  std::string_view symbol, std::string_view exchange,
                  ColumnSet columns,
                  std::function<void(const SnapshotRow&)> cb) const;

    /// Called on startup to rebuild segment index from persisted meta.json files.
    void open_existing();

    /// Replace everything this store holds with the files staged under `staging_dir` at
    /// `relative_paths`, which are paths relative to this store's base directory.
    ///
    /// **One exclusive hold for the whole swap**, which is the point of putting it here rather
    /// than in the caller. A scan takes the same mutex shared, so it waits and then sees the new
    /// store — it cannot see a half of each. Before #142 the installers renamed the received
    /// files in and removed nothing, so a replica that had flushed a *prefix* of a symbol kept
    /// its own segment beside the arriving one: overlapping event-time ranges, different
    /// directory names, nothing for the duplicate-directory guard to refuse, and the rows were
    /// returned twice. Measured then: 122 rows where the primary held 100.
    ///
    /// Spares anything whose name begins with `wal_` (the WAL files and `wal_identity`), every
    /// plain file in the base directory (`repl_state.txt`), and the staging directory itself,
    /// which the callers put **inside** the data directory.
    ///
    /// Returns false if any staged file could not be moved, having already cleared — the caller
    /// is bootstrapping and the recovery is to bootstrap again, which is what its saved position
    /// forces anyway.
    [[nodiscard]] bool replace_from_staging(const std::string& staging_dir,
                                            const std::vector<std::string>& relative_paths);

    /// Flush active segment and release resources.
    void close();

    /// Delete segments whose end_ts_ns < cutoff_ns.
    /// Returns {segments_deleted, bytes_reclaimed}.
    /// Deletes in chronological order (oldest first).
    /// Logs each deletion to stderr.
    /// Skips segments that fail to delete (logs error, continues).
    std::pair<size_t, size_t> delete_expired_segments(uint64_t cutoff_ns);

    /// Remove the segments whose directories are named, from the disk and from the index.
    /// Returns how many were removed; one whose directory cannot be removed stays indexed.
    ///
    /// For recovery (#160): a segment no surviving checkpoint vouches for may be what a power cut
    /// left of it, and the WAL holds every one of its rows, so the engine removes it before replay
    /// rather than trust a directory the device may not have received in full.
    size_t remove_segments(const std::vector<std::string>& dirs);

    /// How many segments the last index rebuild had to read `ts.col` of, because their
    /// `meta.json` predates #166 and does not say its range is the rows'. The next rebuild of the
    /// same directory reads none of them, once the repair reached the disk.
    size_t last_rebuild_ranges_read() const {
        std::shared_lock<std::shared_mutex> lock(index_mtx_);
        return last_rebuild_ranges_read_;
    }

    /// Number of segments in the index (including active if flushed).
    size_t segment_count() const {
        std::shared_lock<std::shared_mutex> lock(index_mtx_);
        return indexed_count_;
    }

    /// Every indexed segment, in `segment_order_less` order across all symbols - a copy, for the
    /// paths that need the whole index: startup's recovery, replay's filter and snapshots. Not for
    /// anything on a query's or a tick's path: that is the whole-index walk #165 took out of them.
    std::vector<SegmentMeta> index() const;

    /// Whether any segment or published block of this symbol is indexed. What a query asks before
    /// it answers "not found" - which used to copy the whole index to find out (#165).
    bool holds(std::string_view symbol, std::string_view exchange) const;

    /// Publish drained rows, so a query reads them before they are sealed (#165 part 2a). A symbol's
    /// blocks are read after its segments, in the order they were published - the order they were
    /// written, which is what a tie between two rows of one level resolves by (#168).
    void publish_blocks(const std::vector<std::shared_ptr<const RowBlock>>& blocks);

    /// Replace the first `count` blocks published for a symbol with the segments written from them,
    /// **in one step** under the index's lock: a query sees the blocks or the segments, never both
    /// and never neither. Returns how many of `segments` were refused as already indexed, like
    /// merge_segments().
    size_t seal_blocks(const std::string& symbol, const std::string& exchange, size_t count,
                       const std::vector<SegmentMeta>& segments);

    /// Rows in published blocks that no seal has replaced yet.
    size_t unsealed_rows() const {
        std::shared_lock<std::shared_mutex> lock(index_mtx_);
        return unsealed_rows_;
    }

    /// How many symbols, each with its exchange, the index holds. Every one of them holds a
    /// segment or a published block: retention and remove_segments() erase a symbol whose last
    /// segment leaves and which has no block, or a store whose symbols come and go - a contract per
    /// expiry, an options chain - would keep an entry for every symbol it ever saw. holds() cannot
    /// tell: it answers by what the entry holds.
    size_t symbols_indexed() const {
        std::shared_lock<std::shared_mutex> lock(index_mtx_);
        return by_symbol_.size();
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
    OwnIndex    own_index_;

    // Active segment state
    uint64_t    wal_identity_{0};
    uint32_t    wal_file_index_{0};
    uint64_t    wal_byte_offset_{0};
    std::string symbol_;
    std::string exchange_;
    uint64_t    active_segment_start_{0};
    // The active segment's rows' earliest and latest timestamps (#166). The start above is the
    // period the segment belongs to, which decides when it rolls over; these are what it records.
    uint64_t    active_min_ts_{0};
    uint64_t    active_max_ts_{0};
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

    /// One width tier of one symbol's segments (#165): those whose `end - start` has this bit
    /// width, sorted by `segment_order_less`. A segment can overlap a query's [s, e] only if its
    /// start is in [s - widest_ns, e], so a scan searches that window of each tier.
    ///
    /// Tiers, because one window per symbol made one wide segment every query's cost. A flush that
    /// closes a batch mixing current rows with old ones - a peer's backlog applied after a
    /// partition, a client's late correction (#105) - writes a segment as wide as the gap between
    /// them, and with one window every later query of that symbol walked back over each of its
    /// segments that start within that width of the range. Measured with
    /// benchmarks/segment_index_cost on the m9g.xlarge, a scan that found nothing past one wide
    /// segment: 0.0031 ms at 10 000 segments of the symbol, 0.020-0.025 at 50 000, 0.048-0.052 at
    /// 100 000 - linear in them, as the flat index was in every symbol's. With tiers, 0.0001 ms at
    /// each of those sizes.
    ///
    /// Within a tier every segment is more than half the widest, so what a scan compares there and
    /// does not need holds one instant: at most the tier's segments that contain
    /// `s - 2^(width_bits - 1)`, which is one or none when a symbol's segments follow each other,
    /// and nothing at all in the tier of width zero, whose window starts at `s`.
    struct WidthTier {
        unsigned width_bits{0};
        std::vector<SegmentMeta> segments;
        /// The widest `end - start` here. Removals do not lower it, and it cannot need to: it is
        /// less than twice the width of any segment the tier holds, which keeps the bound above.
        uint64_t widest_ns{0};
    };
    /// One symbol's segments, in tiers by width, ascending - one or two in practice. The index was
    /// one vector of every segment of every symbol, which every tick's merge compared each new
    /// segment against and sorted whole under the engine's lock, and which every query copied
    /// whole before looking at one symbol: measured on the m9g.xlarge, a merge of 16 new segments
    /// took 7.7 ms at 100 000 segments and a scan that found nothing 3.3 ms.
    struct SymbolIndex {
        std::vector<WidthTier> tiers;
        /// Published and not yet sealed, oldest first: a drain appends, a seal takes from the front.
        std::deque<std::shared_ptr<const RowBlock>> blocks;
    };
    static std::string index_key(std::string_view symbol, std::string_view exchange);

    // The index (rebuilt from meta.json on open_existing). By symbol, keyed with a NUL between
    // symbol and exchange rather than the dot the engine's other maps use, because a symbol can
    // carry a dot: "A.B" on "C" and "A" on "B.C" are one key with a dot and two with a NUL.
    std::unordered_map<std::string, SymbolIndex> by_symbol_;
    std::unordered_set<std::string> indexed_dirs_;   // what the duplicate check asks, in O(1)
    size_t indexed_count_{0};
    size_t unsealed_rows_{0};                        // rows in every symbol's blocks

    /// Index one segment: false, and nothing changed, if its directory is already indexed.
    /// Caller holds `index_mtx_` exclusively.
    bool insert_locked(SegmentMeta meta);
    /// Whether this directory is indexed, taking the lock. A scan asks it about a segment whose
    /// files it could not open: retention may have taken it out between the scan's copy and its
    /// read, and that is not the corruption a missing file otherwise is.
    bool still_indexed(const std::string& dir) const;

    // Protects the index for concurrent scan() (shared) vs merge_segments()/open_existing() (exclusive)
    mutable std::shared_mutex index_mtx_;

    // What the last rebuild read to repair ranges written before #166. Guarded by index_mtx_.
    size_t last_rebuild_ranges_read_{0};

    // Helpers
    /// Rebuild `index_` from the `meta.json` files under `base_dir_`. Caller holds `index_mtx_`
    /// exclusively — `open_existing()` and `replace_from_staging()` both need this and
    /// `std::shared_mutex` is not recursive, so taking it here would deadlock the second one.
    void rebuild_index_locked();

    /// Give every indexed segment whose `meta.json` predates #166 the range of its rows, read from
    /// its `ts.col` - in the index always, and on disk as well where that can be done safely: each
    /// corrected `meta.json` is written beside the old one, one `syncfs()` takes a batch of them to
    /// the device, and only then does a `rename()` publish each one, so a crash anywhere leaves
    /// every segment with the old file or the new one and never with half of either. Caller holds
    /// `index_mtx_` exclusively.
    void repair_ranges_locked(std::vector<SegmentMeta>& found);

    /// Create a segment directory for this span that no other segment is using, and return it.
    ///
    /// A segment's identity **was** its event-time range, so two flushes covering the same span
    /// wrote to one directory and the second destroyed the first (#136). The first segment of a
    /// span still gets `<start>_<end>` character for character — the name only changes where the
    /// engine used to lose data — and a collision appends `_1`, `_2`, and so on.
    ///
    /// `create_directory()` is the arbiter rather than a preceding `exists()`: it reports whether
    /// it created the directory or found one, so two flushers racing for the same free name
    /// cannot both win it. That holds without any claim about which lock the caller holds, which
    /// is worth more here than the claim would be — the guard this replaces asserted a locking
    /// fact about its callers and was wrong about it for a year.
    std::string create_unique_segment_dir(const std::string& symbol, const std::string& exchange,
                                          uint64_t start_ts, uint64_t end_ts) const;

    std::string segment_dir(const std::string& symbol, const std::string& exchange,
                            uint64_t start_ts, uint64_t end_ts) const;
    void ensure_dirs(const std::string& path) const;
    void write_meta_json(const std::string& dir, const SegmentMeta& meta) const;
    std::string meta_json(const SegmentMeta& meta) const;
    bool parse_meta_json(const std::string& path, SegmentMeta& out) const;
};

} // namespace ob
