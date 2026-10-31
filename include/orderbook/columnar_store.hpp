#pragma once

#include "orderbook/best_track.hpp"
#include "orderbook/book_summary.hpp"
#include "orderbook/column_codec.hpp"
#include "orderbook/data_model.hpp"
#include "orderbook/decoded_columns.hpp"
#include "orderbook/query_columns.hpp"

#include <array>
#include <atomic>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <span>
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
///
/// Version 3 (smart-data-engine-workspace/specs/segment-format-v3/) stores them in one file, `columns.v3`, each
/// column in the smallest of its candidate encodings (column_codec.hpp) and checksummed: version 2
/// wrote four of them raw, 25 bytes a row on the comparative dataset where version 3 writes under 2.
///
/// Version 4 (#250, smart-data-engine-workspace/specs/segment-format-v4/) is version 3's file and
/// layout with one more encoding a column may be in, steps/runs, which a reader of version 3 refuses
/// as an unknown transform: the segment says 4 so that such a reader passes it over whole, as it does
/// any version it cannot read, rather than a column at a time.
inline constexpr uint32_t kColumnarFormatV2 = 2;
inline constexpr uint32_t kColumnarFormatV3 = 3;
inline constexpr uint32_t kColumnarFormatV4 = 4;
/// The version a store writes unless told otherwise (`segment-format`). 3 while the version before
/// this one (`scripts/previous_version.txt`) cannot read 4: a downgrade would leave out every format-4
/// segment's rows, which docs/upgrading.md holds to be the failure.
inline constexpr uint32_t kColumnarFormatVersion = kColumnarFormatV3;
/// Whether this build reads a segment of `version`.
inline constexpr bool columnar_format_readable(uint32_t version) {
    return version == kColumnarFormatV2 || version == kColumnarFormatV3 || version == kColumnarFormatV4;
}
/// Whether a segment of `version` keeps its columns in one file, `columns.v3`: 3 and 4 do.
inline constexpr bool columns_in_one_file(uint32_t version) {
    return version == kColumnarFormatV3 || version == kColumnarFormatV4;
}
/// The file a segment of version 3 or 4 holds its columns in.
inline constexpr const char* kColumnsV3File = "columns.v3";
/// Whether `filename` is one of a segment's files, in any format: its `meta.json`, format 2's
/// column files (`*.col`) or format 3's and 4's `columns.v3`, and the book summaries and best tracks beside
/// them - what a snapshot, a backup and a shard migration carry. One definition: a snapshot that
/// matched `*.col` alone shipped format-3 segments without their data, and a replica bootstrapped
/// from it held none of their rows.
inline bool is_segment_file(std::string_view filename) {
    return filename == "meta.json" || filename == kColumnsV3File ||
           (filename.size() > 4 && filename.substr(filename.size() - 4) == ".col") ||
           is_book_summary_file(filename) || is_best_track_file(filename);
}

/// Which (side, level) pairs a segment holds a row of (#47): a bit a level, bids and asks. What lets a
/// scan whose filter names sides or levels skip a segment holding none of them (`cannot_hold()`).
/// It let the book at an instant skip segments too, while the book was the latest row of each
/// level; since #233 the book is read by price, where any row of the range can change it.
///
/// A row of another side, or of a level past the book's capacity, is not representable, and a set
/// that could not hold every row is no set at all: `from_columns()` answers null for it, and a
/// reader then reads the segment, which is what it does for a segment written before this existed.
struct LevelSet {
    static constexpr size_t kLevels = 1000;   ///< SoABuffer::MAX_LEVELS, the book's levels a side
    static constexpr size_t kWords = (kLevels + 63) / 64;
    using Bits = std::array<uint64_t, kWords>;   ///< level i is bit i % 64 of word i / 64
    Bits bid{};
    Bits ask{};

    const Bits& side(size_t s) const { return s == SIDE_BID ? bid : ask; }
    bool has(uint8_t s, uint16_t level) const {
        if (level >= kLevels || (s != SIDE_BID && s != SIDE_ASK)) return false;
        return (side(s)[level / 64] >> (level % 64)) & 1u;
    }
    static size_t count(const Bits& bits);
    bool operator==(const LevelSet&) const = default;

    /// The set of a segment's rows, from its side and level columns; null when a row is of
    /// neither side or past `kLevels`.
    static std::shared_ptr<const LevelSet> from_columns(const std::vector<uint8_t>& sides,
                                                        const std::vector<uint16_t>& levels);
    /// One side as `meta.json` holds it: `kLevels / 4` hex digits, the lowest level the lowest bit
    /// of the last digit - or, `trimmed`, without the leading zeros, as segment format 3 writes it.
    static std::string to_hex(const Bits& bits, bool trimmed = false);
    /// The reverse, of either; false for a string that is neither, or that names a level past
    /// `kLevels`.
    static bool from_hex(const std::string& hex, Bits& bits);
};

/// The conditions a scan applies before it builds a row (#47 step 2): closed ranges, as the query
/// language's are (#199, #200), an end left unset open. What a segment's metadata proves no row of
/// it meets leaves it unread; what a row's columns fail leaves it unbuilt.
struct RowFilter {
    std::optional<int64_t>  price_lo, price_hi;
    std::optional<uint8_t>  side_lo, side_hi;
    std::optional<uint16_t> level_lo, level_hi;

    bool has_price() const { return price_lo.has_value() || price_hi.has_value(); }
    bool has_side() const { return side_lo.has_value() || side_hi.has_value(); }
    bool has_level() const { return level_lo.has_value() || level_hi.has_value(); }
    bool empty() const { return !has_price() && !has_side() && !has_level(); }
    bool keeps(int64_t price, uint8_t side, uint16_t level) const {
        return within(price, price_lo, price_hi) && within(side, side_lo, side_hi) &&
               within(level, level_lo, level_hi);
    }
    /// A closed range with either end open, as each of the three conditions is.
    template <typename T>
    static bool within(T value, const std::optional<T>& lo, const std::optional<T>& hi) {
        return (!lo || value >= *lo) && (!hi || value <= *hi);
    }
    /// The columns it reads, which a scan reads whether or not its caller asked for them.
    ColumnSet columns() const {
        ColumnSet c;
        if (has_price()) c.add(QueryColumn::Price);
        if (has_side()) c.add(QueryColumn::Side);
        if (has_level()) c.add(QueryColumn::Level);
        return c;
    }
};

/// The most rows one batch of a scan chooses: the chunk its selection pass marks at a time (#225),
/// whose masks stay in L1 between the passes over the columns.
inline constexpr size_t kScanBatchRows = 2048;

/// One candidate's rows as a scan hands them over a chunk at a time (OB-12): its columns - each
/// holding the candidate's rows, or empty where the read did not ask for that column - and the
/// indices of the rows chosen from them, increasing, at most `kScanBatchRows` of them.
///
/// The columns are the read's buffers or a segment's held ones (#220), so a batch is valid only
/// inside the call it is handed to. A format-2 column cut short has been padded with zeros to the
/// candidate's rows, which is what reading it a row at a time always gave.
struct RowBatch {
    std::span<const uint64_t> ts;
    std::span<const int64_t>  price;
    std::span<const uint64_t> qty;
    std::span<const uint32_t> cnt;
    std::span<const uint8_t>  side;
    std::span<const uint16_t> level;
    std::span<const int64_t>  seq;
    std::span<const uint32_t> rows;
};

/// What a reader did with a batch: went through all of it, or stopped at `rows[at]` - the row it
/// looked at last, whether or not it took it - after which the scan reads nothing more. The scan's
/// `rows_filtered` then counts the rows the filter failed before that row, as it did row by row.
struct BatchVerdict {
    bool   stop{false};
    size_t at{0};
};
using BatchReader = std::function<BatchVerdict(const RowBatch&)>;

class ScanPool;

/// One candidate's share of a scan (OB-12, stage 2): made and filled on whichever thread reads the
/// candidate, then folded into the answer on the scan's thread, in the candidates' order.
class ScanPart {
public:
    virtual ~ScanPart() = default;
    /// The candidate's rows, a batch at a time and in order, as `scan_batches()` hands them over.
    virtual BatchVerdict take(const RowBatch& batch) = 0;
    /// The rows it took.
    virtual uint64_t rows() const = 0;
};

/// A batch handed to `part` a step of `ScanPool::kStepRows` rows at a time, with `ScanPool::pace()`
/// before each, as a pool thread reads its part (#248): what the part says of a step is said of the
/// batch, a stop at a step's row `at` being a stop at the batch's row `from + at`, and once it stops
/// it is handed nothing more of the batch.
BatchVerdict take_in_steps(ScanPart& part, const RowBatch& batch);

/// How a scan divides its candidates among a pool's threads (OB-12, stage 2).
struct PartsScan {
    /// Where the candidates are read. Null, a pool of no threads, or a scan of one candidate reads
    /// every candidate on the scan's thread, through `direct`.
    ScanPool* pool{nullptr};
    /// The most rows the answer takes: a candidate goes to the pool only while the rows folded and
    /// the rows of those handed out and not folded yet are fewer, so a small LIMIT has nothing read
    /// that it will not use.
    uint64_t limit_rows{UINT64_MAX};
    /// The scan read on its own thread, in order, as `scan_batches()` reads it.
    BatchReader direct;
    /// One candidate's part, made on the thread that reads the candidate.
    std::function<std::unique_ptr<ScanPart>()> make_part;
    /// A part folded into the answer, on the scan's thread and in the candidates' order. False ends
    /// the scan.
    std::function<bool(ScanPart&)> fold;
    /// Divide a scan a pool cannot speed up as well - fewer rows in range than
    /// `kScanDivideMinRows`, or most of them in one candidate - which only tests ask for, so that
    /// a small store reads the parts' path.
    bool divide_small{false};
};

/// The fewest rows in range a scan is divided at: below it a scan is read on its query's thread.
inline constexpr uint64_t kScanDivideMinRows = 32'768;

/// Row `i` of a batch as the row scan hands it over: the fields of the columns the batch holds,
/// zero in every field whose column it does not.
inline SnapshotRow row_of(const RowBatch& b, uint32_t i) {
    SnapshotRow row{};
    row.timestamp_ns = b.ts[i];
    if (!b.seq.empty())   row.sequence_number = static_cast<uint64_t>(b.seq[i]);
    if (!b.side.empty())  row.side            = b.side[i];
    if (!b.level.empty()) row.level_index     = b.level[i];
    if (!b.price.empty()) row.price           = b.price[i];
    if (!b.qty.empty())   row.quantity        = b.qty[i];
    if (!b.cnt.empty())   row.order_count     = b.cnt[i];
    return row;
}

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
    /// The seal epoch of the flush that wrote this segment (#165 part 2a): a checkpoint written
    /// while rows wait in blocks vouches for the segments sealed at or before an epoch, because a
    /// position cannot. 0 in a segment written before epochs existed, and in one no seal wrote.
    uint64_t seal_epoch{0};
    /// How many merges made this segment (#165 part 2b): 0 from a seal, and from a merge one more
    /// than the highest of its inputs'. Merging segments of one level with each other is what keeps a
    /// row from being written again by every merge of the segment it is in.
    uint32_t merge_level{0};
    /// The highest sequence number of `own_origin`'s own rows in this segment (#184, #185): what a
    /// restart continues that origin's counter from and declares its own frontier up to. Not
    /// `max_sequence_number`, which is every origin's - a mesh segment holds its peers' rows too, and
    /// continuing from their numbers left a hole in this node's stream that no receiver's frontier
    /// passes. `has_own_max` is false for a segment written before this was recorded, and for a
    /// merge of such a segment: a start then falls back to `max_sequence_number`, as it always did.
    bool     has_own_max{false};
    uint16_t own_origin{0};           ///< the node the numbers below are of: 0 without a mesh
    uint64_t own_max_sequence{0};     ///< 0: none of this segment's rows is `own_origin`'s
    /// Some of its rows were sealed by another node - it is a snapshot's segment, or a merge of
    /// one - so `own_max_sequence` does not cover what of `own_origin`'s own records they hold. A
    /// start without a vector raises the counter by `max_sequence_number` for such a segment, the
    /// only number that cannot hand one out twice.
    bool     has_received_rows{false};
    /// The (side, level) pairs this segment holds a row of (#47); null when unknown - a segment
    /// written before this was recorded, or one with a row no set can hold. Shared: copies of a
    /// meta are many, and the set does not change once the segment is written.
    std::shared_ptr<const LevelSet> levels;
    /// The segment's decoded columns, held between queries (#220): given by `insert_locked()` to
    /// the index's entry, and shared by every copy a query takes of it, so they live as long as the
    /// entry and the reads that copied it - and a segment that later takes this path starts with its
    /// own. Null outside the index and with holding off. Never written to meta.json.
    std::shared_ptr<DecodedColumns> decoded;
    /// The lowest and the highest price of its rows (#47 step 2), so a scan with a price condition
    /// can leave it unread; `has_price_range` false when unknown - a segment written before this
    /// was recorded - and the segment is then read whatever the condition.
    bool    has_price_range{false};
    int64_t min_price{0};
    int64_t max_price{0};
    std::string symbol;     ///< symbol this segment belongs to
    std::string exchange;   ///< exchange this segment belongs to
    std::string dir_path;   ///< full path to the segment directory
};

/// One segment a merge wrote another from (#165 part 2b), as the merged segment's meta.json names
/// it: its directory under the symbol's, and what its own meta.json said.
///
/// A start that finds a merged segment and one of its inputs both on the disk - a crash after the
/// merge was published and before its inputs were removed - removes the input. The fields past the
/// name are what tells an input from a later segment that took the name once the input was gone: a
/// directory is named after its range, so the name comes back, and a later seal of this WAL has a
/// later epoch.
struct SegmentInput {
    std::string dir_name;
    uint64_t wal_identity{0};
    uint32_t wal_file_index{0};
    uint64_t wal_byte_offset{0};
    uint64_t seal_epoch{0};
    uint64_t row_count{0};
    uint64_t start_ts_ns{0};
    uint64_t end_ts_ns{0};

    static SegmentInput of(const SegmentMeta& meta);
    /// Whether `meta` is this input: its directory's name and everything else recorded.
    bool names(const SegmentMeta& meta) const;
};

namespace detail {

/// A link in the chain of reader generations a store keeps (`ColumnarStore::replace_segments()`,
/// #165 part 2b): each holds the one after it, so the generation current before a replacement is
/// gone exactly when every scan that took it, or an older one, has finished.
///
/// Released without recursion. A scan that outlived many publications holds the start of a chain
/// of every generation since, and a destructor releasing the next recursed once per publication on
/// that scan's thread; peeking at the next link's count instead is a read the count does not order.
/// The destructor that starts a release drains the chain itself, and one it causes only hands its
/// successor over.
struct ReaderGeneration {
    std::shared_ptr<ReaderGeneration> next;
    ReaderGeneration() = default;
    ReaderGeneration(const ReaderGeneration&) = delete;
    ReaderGeneration& operator=(const ReaderGeneration&) = delete;
    ~ReaderGeneration();
};

}  // namespace detail

/// Spare storage for blocks' rows (#165 part 2a): a sealed block's vector, handed back when the
/// block's last reference goes, and taken again by a later drain - which otherwise wrote every row
/// of every tick into memory the kernel had to fault in first. Measured at four pipelining
/// connections on the m9g.xlarge: 68% of the server's page faults were that, in `drain_batch()`.
///
/// Bounded: spares past `max_spare_rows` rows of capacity are freed rather than kept. Thread-safe,
/// because the last reference to a block can be a query's.
class RowBufferPool {
public:
    explicit RowBufferPool(size_t max_spare_rows) : max_spare_rows_(max_spare_rows) {}

    /// An empty vector with room for `rows`: the smallest spare that holds them without being more
    /// than about twice as big - a quiet store should not hold a busy one's buffer until its seal -
    /// or a new one. None for `rows` 0, a store's first block, whose size nothing predicts.
    std::vector<SnapshotRow> take(size_t rows);

    /// Keep `rows`' storage for a later take(), cleared, if the spares stay under the bound;
    /// otherwise leave it to be freed with `rows`.
    void give_back(std::vector<SnapshotRow>&& rows) noexcept;

    /// Rows of capacity held spare, for tests.
    size_t spare_rows() const;

private:
    mutable std::mutex mtx_;
    std::vector<std::vector<SnapshotRow>> spare_;
    size_t spare_rows_{0};
    const size_t max_spare_rows_;
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
    /// The highest sequence number among the rows this node wrote itself (#184); 0 for none. A row
    /// does not say whose it is, so the drain, which knows, says it for the block.
    uint64_t max_own_seq{0};
    /// Every row is this node's own - always without a mesh. Then a block that rolls a segment
    /// over gives each segment its own rows' highest, exactly as appending the rows one by one
    /// does; a block of mixed rows gives both its highest, which a start, taking the highest over a
    /// symbol's segments, reads the same.
    bool all_own{true};

    /// A block of these rows, its range computed from them; null for none, because a symbol that
    /// received no row this drain has no block.
    static std::shared_ptr<const RowBlock> make(std::string symbol, std::string exchange,
                                                std::vector<SnapshotRow> rows);
    /// The same, with the range its caller computed while it collected the rows - the drain does,
    /// so the rows are not walked a second time for it. The range must be the rows': a query skips
    /// a block by it, and a seal gives it to the segment. With a `pool`, the rows' storage goes
    /// back to it when the block's last reference does.
    static std::shared_ptr<const RowBlock> make(std::string symbol, std::string exchange,
                                                std::vector<SnapshotRow> rows,
                                                uint64_t min_ts_ns, uint64_t max_ts_ns,
                                                std::shared_ptr<RowBufferPool> pool,
                                                uint64_t max_own_seq = 0, bool all_own = false);
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

    /// Append a row to the active segment, rolling over if needed. The row counts as this store's
    /// own for the segment's highest own number (#184); a block says its own instead.
    void append(const SnapshotRow& row);

    /// Append a block's rows in their order, writing what append() on each in turn writes - a
    /// property test holds the two to the same bytes on disk. For the seal (#165 part 2a), which
    /// appends every row a second time, after the drain has put it in a block: when no row of the
    /// block is of a later period than its first - a later one rolls the segment over part way -
    /// the rollover is tested once, against the block's latest row, and the segment's range widened
    /// once, by the block's, rather than both row by row.
    void append_block(const RowBlock& block);

    /// Room in the active segment's buffers for `rows` more, so a seal of several blocks grows each
    /// buffer once rather than by doubling on the way. Changes nothing that is written.
    void reserve_rows(size_t rows);

    /// The buffers a segment is accumulated in, one per column.
    struct ColumnBuffers {
        std::vector<int64_t>  price;
        std::vector<uint64_t> qty;
        std::vector<uint64_t> ts;
        std::vector<uint32_t> cnt;
        std::vector<uint8_t>  side;
        std::vector<uint16_t> level;
        std::vector<int64_t>  seq;
    };
    /// Exchange this store's buffers with `other`'s, so the engine lends one set to each seal in
    /// turn (#165 part 2a): sixteen stores had each kept buffers as big as its biggest seal, for the
    /// life of the process. Only with no active segment - what the buffers hold then is a written
    /// segment's, which the next append() clears - and `std::logic_error` otherwise.
    void swap_buffers(ColumnBuffers& other);

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

    /// The seal epoch every segment closed from here on is stamped with, like the position above
    /// (#165 part 2a).
    void set_seal_epoch(uint64_t seal_epoch) { seal_epoch_ = seal_epoch; }

    /// The origin this store's own rows are of (#184): the mesh node's id, 0 without a mesh. Every
    /// segment it closes records the highest number among them - a row appended alone counts as
    /// own, and a block says its own highest.
    void set_own_origin(uint16_t origin) { own_origin_ = origin; }

    /// How this store writes a segment (segment format v3, design §3): the format's version, and
    /// how a column's encoding is chosen - in version 4 with steps/runs among the candidates (#250). What a store is not told is a seal's - LZ4 or
    /// nothing, each column in the encoding its last segment chose and searched for again every
    /// sixteenth - so a store an embedding application makes, which no merge ever rewrites, seals as
    /// the server's do.
    struct SegmentFormat {
        uint32_t version{kColumnarFormatVersion};
        /// The search over the candidates: which compressors it tries, and their margins.
        column_codec::EncodeOptions search{.lz4 = true, .zstd_level = 0, .compressed_margin_pct = 0,
                                           .zstd_margin_pct = 0,
                                           .runs_charge_pct = column_codec::kRunsChargePct,
                                           .hint = nullptr};
        /// A segment reuses the encoding this store's last one chose for each column, and searches
        /// again every this many segments - and always when it has none to reuse. 1: every segment
        /// searches.
        uint32_t search_every{16};

        /// A seal's: what queries read most, so nothing that decodes as slowly as ZSTD, and a runs
        /// candidate charged for its runs (#223).
        static SegmentFormat seal() { return SegmentFormat{}; }
        /// A merge's: what stays, so ZSTD(3) where it saves 10% over LZ4, and every segment searched.
        /// Nothing is charged for runs: a merged column decodes at ZSTD's pace, and the charge moved
        /// its choices among candidates no faster than the ones they replaced - from blocks to ZSTD,
        /// where the runs under LZ4 it took out had kept ZSTD from its margin (#223).
        static SegmentFormat merge() {
            SegmentFormat f;
            f.search.zstd_level = 3;
            f.search.zstd_margin_pct = 10;
            f.search.runs_charge_pct = 0;
            f.search_every = 1;
            return f;
        }
    };
    void set_segment_format(const SegmentFormat& format) {
        segment_format_ = format;
        segments_since_search_ = 0;
        has_last_choice_ = false;
    }
    const SegmentFormat& segment_format() const { return segment_format_; }

    /// What the decoded columns of this store's format-3 segments may hold between queries (#220):
    /// a query reading a column held reads neither the file nor its checksum, and decodes nothing.
    /// 0 holds none, and every read decodes what it reads, as before. Every segment in the index
    /// gets an empty slot under the new budget - or loses its slot, with 0 - and so does every one
    /// indexed later.
    void set_decoded_columns_budget(size_t bytes);
    /// Hits and misses in columns, evictions in segments, and the bytes held against the limit; all
    /// zero with holding off.
    DecodedColumnsBudget::Stats decoded_columns_stats() const;

    /// What the next segment written records as its own highest number instead of what its rows
    /// say (#184) - for a merge, whose rows are its inputs' and whose answer is theirs: the highest
    /// of the inputs', when every input knows it for the same origin, `known = false` otherwise.
    /// The next write consumes it, like the lineage.
    void set_own_max(bool known, uint16_t origin, uint64_t max_own, bool received_rows) {
        has_own_override_      = true;
        own_override_known_    = known;
        own_override_origin_   = origin;
        own_override_max_      = max_own;
        own_override_received_ = received_rows;
    }

    /// Flush the active segment: encode buffers, write column files, write meta.json.
    /// Returns the SegmentMeta of the flushed segment, or std::nullopt if no active segment.
    std::optional<SegmentMeta> flush_segment();

    /// What the next segment written is a merge of (#165 part 2b): its level, its inputs, and the
    /// latest of their `last_row_ts_ns` - replay's fallback compares a record with the highest of a
    /// symbol's, so a merge must not lower it, and the row appended last need not be the latest.
    /// The next write consumes it.
    void set_lineage(uint32_t merge_level, std::vector<SegmentInput> inputs, uint64_t last_row_ts_ns);

    /// Write the active segment into `dir`, which this creates and which must not exist, rather than
    /// into a directory of its own choosing: a merge writes beside the segments it replaces, under a
    /// name no reader takes for a segment (`kCompactingSuffix`), and publishes it by renaming it once
    /// it is on the device. Indexes nothing. nullopt with nothing active; throws, and leaves no
    /// directory, where flush_segment() would.
    std::optional<SegmentMeta> flush_segment_into(const std::string& dir);

    /// The suffix of a merge's working directory (#165 part 2b). A rebuild removes one, and a
    /// snapshot leaves it out.
    static constexpr std::string_view kCompactingSuffix = ".compacting";
    /// Whether `name` is one a merge gives its working directory - `<start>_<end>_<n>.compacting`,
    /// digits all three - and nothing a client names: a symbol or an exchange may end in the suffix
    /// too, and taking one for a working directory removed it, and every row under it, at the next
    /// start. Its callers ask it only of a directory at a segment's depth or deeper.
    static bool is_compacting_dir(std::string_view name) {
        if (name.size() <= kCompactingSuffix.size() ||
            name.substr(name.size() - kCompactingSuffix.size()) != kCompactingSuffix) {
            return false;
        }
        const std::string_view stem = name.substr(0, name.size() - kCompactingSuffix.size());
        int fields = 1;
        bool digit_seen = false;
        for (const char c : stem) {
            if (c == '_') {
                if (!digit_seen) return false;
                ++fields;
                digit_seen = false;
            } else if (c >= '0' && c <= '9') {
                digit_seen = true;
            } else {
                return false;
            }
        }
        return fields == 3 && digit_seen;
    }
    /// How deep below the data directory a segment's directory is: `<symbol>/<exchange>/<segment>`,
    /// the depth a recursive walk from the data directory reports for it.
    static constexpr int kSegmentDepth = 2;

    /// Every row of one segment, all seven columns, in the order it holds them (#165 part 2b: what a
    /// merge reads). False, having handed over no row, when a column is missing or short or the
    /// format is not this build's - the segments scan() skips.
    bool read_segment(const SegmentMeta& meta,
                      const std::function<void(const SnapshotRow&)>& cb) const;

    /// One symbol's segments whose end is in [end_from, end_to) - a merge's partition - with every
    /// segment of the symbol that lies between them, in the order a scan delivers them. `member`
    /// says which are the partition's: a merge may take only segments no other lies between.
    struct PartitionView {
        std::vector<SegmentMeta> segments;
        std::vector<bool> member;
    };
    PartitionView partition_view(std::string_view symbol, std::string_view exchange,
                                 uint64_t end_from, uint64_t end_to) const;

    /// Replace `inputs` with `output` in one step under the index's lock (#165 part 2b), so a query
    /// sees the inputs or the output and never both or neither - if every input is still indexed,
    /// they are still consecutive in their symbol's delivery order, and `output`'s range sorts
    /// strictly between the segments before and after them, so no row is delivered in another order
    /// than before. `publish` runs under the lock once those hold, and moves the output's files to
    /// where it then sets `output.dir_path`; false from it leaves everything as it was.
    ///
    /// On success `readers_before` expires once every scan that may have copied an input before the
    /// swap has finished. The inputs' files have to stay until it does: such a scan reads them after
    /// the swap.
    enum class Replaced { kYes, kInputGone, kNotConsecutive, kWouldMove, kPublishFailed };
    struct ReplaceResult {
        Replaced outcome{Replaced::kInputGone};
        std::weak_ptr<const void> readers_before;
    };
    ReplaceResult replace_segments(const std::vector<SegmentMeta>& inputs, SegmentMeta output,
                                   const std::function<bool(SegmentMeta&)>& publish);

    /// What the last index rebuild removed (#165 part 2b): merges' working directories, and inputs
    /// found beside the merged segment that replaced them.
    struct RebuildRemoved {
        size_t staging{0};
        size_t superseded{0};
        /// Merged segments short of their rows whose inputs were all there to hold them.
        size_t short_merges{0};
    };
    RebuildRemoved last_rebuild_removed() const {
        std::shared_lock<std::shared_mutex> lock(index_mtx_);
        return last_rebuild_removed_;
    }

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
    ///
    /// With a `RowFilter` (#47 step 2) a candidate whose metadata proves no row of it meets the
    /// filter - its price range, its levels - is not read, and a row that fails it is not built:
    /// the rows handed over are those of the scan without it that the filter keeps, in the same
    /// order. `cb` returning false ends the scan; a segment after it is not opened.
    struct ScanCost {
        size_t compared{0};
        size_t candidates{0};
        /// Published blocks whose range met the query, read from memory (#165 part 2a).
        size_t blocks{0};
        /// The candidates opened, and those the filter's proof left unread (#47 step 2).
        size_t segments_read{0};
        size_t skipped_by_price{0};
        size_t skipped_by_levels{0};
        /// Rows in the range the filter failed before they were built.
        uint64_t rows_filtered{0};
        /// Whether `cb` ended it.
        bool stopped{false};
    };
    ScanCost scan(uint64_t start_ns, uint64_t end_ns,
                  std::string_view symbol, std::string_view exchange,
                  ColumnSet columns,
                  std::function<void(const SnapshotRow&)> cb) const;
    ScanCost scan(uint64_t start_ns, uint64_t end_ns,
                  std::string_view symbol, std::string_view exchange,
                  ColumnSet columns, const RowFilter& filter,
                  const std::function<bool(const SnapshotRow&)>& cb) const;

    /// The rows scan() hands over, a batch at a time (OB-12): the same rows, in the same order and
    /// at the same `ScanCost`, with `read` called once a chunk of a candidate rather than once a
    /// row. scan() is this with a reader that builds each row; a reader that takes the columns
    /// themselves - a reply's formatter, a bucket's sums - builds no row it does not need.
    ScanCost scan_batches(uint64_t start_ns, uint64_t end_ns,
                          std::string_view symbol, std::string_view exchange,
                          ColumnSet columns, const RowFilter& filter,
                          const BatchReader& read) const;

    /// The rows `scan_batches()` reads, divided by candidate among `how.pool`'s threads (OB-12,
    /// stage 2): each candidate read into a part of its own on whichever thread takes it, the
    /// scan's thread among them, and the parts folded here in the candidates' order - so what
    /// `how.fold` sees is what one thread reading them in order would have made. With no pool to
    /// use the scan is `scan_batches()` through `how.direct`. The blocks no seal has written yet are
    /// the last part, read here. Every part handed out is finished or dropped before this returns,
    /// when a part throws as well, and the throw then leaves through here.
    ScanCost scan_parts(uint64_t start_ns, uint64_t end_ns,
                        std::string_view symbol, std::string_view exchange,
                        ColumnSet columns, const RowFilter& filter,
                        const PartsScan& how) const;

    /// Whether a segment's metadata proves no row of it meets `filter` (#47 step 2): its price
    /// range disjoint from the filter's, or its levels holding no pair the filter's sides and
    /// levels allow. False whenever what it would need is unknown; `by_price` says which proof it was.
    static bool cannot_hold(const SegmentMeta& meta, const RowFilter& filter, bool* by_price = nullptr);


    /// The rows of one symbol in [start_ns, end_ns] that `keep` accepts, in the order the book at an
    /// instant decides by (#44 step 2): by timestamp, and at one timestamp in the order scan()
    /// delivers them - so the last row of a level at a time is the one the book holds then. `cb`
    /// returning false ends the read.
    ///
    /// Each candidate's kept rows are a run, sorted by time and stable, merged with the others by
    /// a heap. Segments come in the order of their start and hold no row before it (#166), so
    /// before a segment starting at W is read every held row before W is final and is handed over:
    /// what is held at once is the runs of segments overlapping the current time, and the blocks
    /// not yet sealed - read first, like a segment whose range is not its rows', since neither
    /// has a start to wait for.
    struct TimeOrderedCost {
        size_t candidates{0};
        size_t blocks{0};
        uint64_t kept{0};
        uint64_t delivered{0};
        /// The most kept rows held - read and not yet handed over - at any one time.
        uint64_t max_held{0};
        /// Whether `cb` ended it.
        bool stopped{false};
        /// Candidates the filter's proof left unread, and rows it failed before they were built
        /// (#47 step 2).
        size_t skipped{0};
        uint64_t rows_filtered{0};
    };
    /// A segment the filter's proof leaves unread is not read here either (#47 step 2), and a row
    /// that fails it is not built.
    TimeOrderedCost scan_by_time(uint64_t start_ns, uint64_t end_ns, std::string_view symbol,
                                 std::string_view exchange, ColumnSet columns,
                                 const RowFilter& filter,
                                 const std::function<bool(const SnapshotRow&)>& cb) const;

    /// The book of one symbol at `at` by price (#233, decision B'): every row at or before `at`
    /// applied in the order `scan_by_time()` delivers them - by timestamp, a tie in the order
    /// `scan()` delivers - each setting its price's quantity, and a quantity of 0 removing the
    /// price. That is how the live book applies an update, so for writes in time order this is the
    /// book the live one held at `at`.
    ///
    /// `rows` holds the best `depth` prices of each side, bids from the highest and asks from the
    /// lowest; each row is the one that set its price last, its `level_index` its position in the
    /// side. `as_of` is the last row applied, of any price - the update the book holds everything
    /// up to - and is empty when no row was.
    struct PricedBook {
        std::vector<SnapshotRow> rows;
        std::optional<SnapshotRow> as_of;
        /// Prices each side held before `depth` cut it: what a deeper read would have answered.
        size_t bid_prices{0};
        size_t ask_prices{0};
        TimeOrderedCost cost{};
        /// The summary the read started from, by the time it is as of; empty when it composed the
        /// symbol's rows from the first (`book_summary.hpp`).
        std::optional<uint64_t> from_summary;
    };
    PricedBook book_at_by_price(uint64_t at, std::string_view symbol, std::string_view exchange,
                                size_t depth = SIZE_MAX) const;

    /// Write a summary of one symbol's book into `dir`, a directory of one of its indexed segments,
    /// and put it in the index (#233). It is the book by price as of the latest time of a row the
    /// symbol's segments hold, with every row at or before it - in a block too - applied. Not
    /// written when there is nothing newer to say, and not put in the index when rows arrived while
    /// it was written: such a summary could lack one, and its file is removed. Not synced (#247): a
    /// summary that a power cut loses or cuts short is not used, and a read composes the book from
    /// further back.
    struct SummaryWrite {
        bool written{false};
        uint64_t as_of_ts{0};
        size_t prices{0};
        size_t bytes{0};
        uint64_t rows_read{0};          ///< rows applied after the summary it started from
        std::string why_not;            ///< empty when written
        /// The best track written beside it (#243): its entries and bytes, or why there is none.
        size_t track_entries{0};
        size_t track_bytes{0};
        std::string track_why_not;
    };
    SummaryWrite write_book_summary(std::string_view symbol, std::string_view exchange,
                                    const std::string& dir);

    /// The times of a symbol's summaries, oldest first: what a read can start from.
    std::vector<uint64_t> book_summaries(std::string_view symbol, std::string_view exchange) const;
    /// The times of a symbol's best tracks, oldest first (#243): each is as of the summary it ends at.
    std::vector<uint64_t> best_tracks(std::string_view symbol, std::string_view exchange) const;

    /// What tops_by_time() read.
    struct TopsCost {
        size_t before_read{0};        ///< candidates of a book composed before `lo`, when one was
        size_t before_skipped{0};
        TimeOrderedCost rows{};       ///< the rows read, over every stretch read by rows
        size_t row_stretches{0};
        size_t tracks{0};             ///< best tracks read
        uint64_t track_entries{0};    ///< their entries handed over
        bool stopped{false};          ///< whether `on_instant` ended it
    };
    /// The best of each side of a symbol's book through [lo, read_to], for a series (#243):
    /// `on_start` gets the best in force just before `lo`, then `on_instant` the best after each
    /// instant of the range at which it changed, in time order - false from it ends the read. Read
    /// from the best tracks where they cover the range, and from the rows by price elsewhere, which
    /// answer the same: a track is what composing those rows yielded when it was written.
    TopsCost tops_by_time(uint64_t lo, uint64_t read_to, std::string_view symbol,
                          std::string_view exchange,
                          const std::function<void(const BookTop&)>& on_start,
                          const std::function<bool(uint64_t, const BookTop&)>& on_instant) const;
    /// Whether tops_by_time() reads the best tracks. On; a test turns it off to compare the rows'
    /// answer with the tracks'.
    void set_read_best_tracks(bool on) { read_best_tracks_.store(on, std::memory_order_relaxed); }

    /// Every symbol the index holds, with its exchange: the ones holding a segment or a published
    /// block. For a start that rebuilds what is kept per symbol (#235).
    std::vector<std::pair<std::string, std::string>> symbols() const;

    /// The earliest start and the latest end among one symbol's segments and unsealed blocks, or
    /// nothing when it has neither (#44 step 2): the range a series without a time condition
    /// covers.
    std::optional<std::pair<uint64_t, uint64_t>> time_span(std::string_view symbol,
                                                           std::string_view exchange) const;

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

    /// Bytes of segment-read buffers the process keeps between reads, over every thread (#49 step 2):
    /// the columns of the segments read last, as far as the budget below goes.
    static size_t read_buffers_held();

    /// What the process keeps at most: 128 MiB, five reads of the largest segments compaction
    /// writes. A test lowers it to watch a read's buffers freed rather than kept.
    static void set_read_buffers_limit_for_test(size_t bytes);

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

    /// The segments of the symbol the engine's maps key `dotted` - symbol, a dot, exchange - in
    /// `segment_order_less` order, and whether it has any segment or block: what moving a symbol
    /// between shards reads and drops (#196). By the dotted key, so that the rows moved are the ones
    /// the engine and the shard map mean by it - both, where a dot in a name makes it two of this
    /// index's symbols - and without the whole-index copy index() makes.
    std::vector<SegmentMeta> segments_of(std::string_view dotted) const;
    bool holds_dotted(std::string_view dotted) const;

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

    /// Drop every published block, for a path that discards what this node holds - a resync, a
    /// snapshot install. Their rows are in the WAL the discard is also giving up on.
    void drop_blocks();

    /// Give up on the segment being written from a seal that failed (#165 part 2a): the active
    /// rows discarded, and any segment a rollover wrote in the meantime removed from the disk -
    /// a seal is all or nothing, because its rows stay in the blocks it was written from, and a
    /// retry writes them again.
    void abandon_active();

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
    /// append() with the number that counts as own for this row - its own for a row appended
    /// alone, 0 for a block's rows, whose own highest append_block() sets itself.
    void append_row(const SnapshotRow& row, uint64_t own_seq);
    uint16_t    own_origin_{0};          ///< see set_own_origin()
    uint64_t    active_own_max_{0};      ///< own rows' highest number in the active segment
    bool        has_own_override_{false};   ///< see set_own_max()
    bool        own_override_known_{false};
    uint16_t    own_override_origin_{0};
    uint64_t    own_override_max_{0};
    bool        own_override_received_{false};
    uint64_t    seal_epoch_{0};
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

    // What the next segment written is a merge of (set_lineage()).
    bool                      has_lineage_{false};
    uint32_t                  lineage_level_{0};
    std::vector<SegmentInput> lineage_inputs_;
    uint64_t                  lineage_last_row_ts_{0};

    // How segments are written, and what the last version-3 one chose per column (ts, price, qty,
    // cnt, side, level, seq) - see set_segment_format().
    SegmentFormat segment_format_{};
    uint32_t segments_since_search_{0};
    bool has_last_choice_{false};
    // Whether a series reads the best tracks (#243): set_read_best_tracks(). Atomic, because a test
    // may turn it while queries run.
    std::atomic<bool> read_best_tracks_{true};
    std::array<column_codec::Encoding, 7> last_choice_{};

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
        /// The book summaries of this symbol (#233, `book_summary.hpp`): the file each is in, by the
        /// time it is as of. A summary leaves this map before any row it does not hold, at or before
        /// that time, can be read - `publish_blocks()` - and with the segment it is in.
        std::map<uint64_t, std::string> summaries;
        /// The best tracks of this symbol (#243, `best_track.hpp`), by the time each is as of - that
        /// of the summary it ends at - with the time it starts from. A track leaves this map with
        /// its summary: the same late row falsifies both, and the same segment carries both.
        struct TrackRef {
            uint64_t from_ts{0};
            std::string path;
        };
        std::map<uint64_t, TrackRef> tracks;
        /// The time a summary being written is as of, and whether a row at or before it was
        /// published while it was: such a summary could lack that row (`write_book_summary()`).
        std::optional<uint64_t> pending_summary;
        bool pending_falsified{false};
    };
    /// What a book read takes from the index under one lock: the candidates of a time-ordered scan
    /// and the summary it starts from.
    struct Gathered {
        std::vector<SegmentMeta> segments;
        std::vector<std::shared_ptr<const RowBlock>> blocks;
        std::shared_ptr<const void> reading;
        std::optional<std::pair<uint64_t, std::string>> summary;   ///< as of, file
        bool found{false};
    };
    /// The candidates of [start_ns, end_ns] in the order a scan delivers them, and - with
    /// `summary_at` - the newest summary as of at most that, the candidates then from its time on.
    /// Caller holds `index_mtx_`.
    void gather_locked(uint64_t start_ns, uint64_t end_ns, std::string_view symbol,
                       std::string_view exchange, std::optional<uint64_t> summary_at,
                       Gathered& out) const;
    /// book_at_by_price(), and with `track` - when given - the best of each side at the instant the
    /// composition started from and after every instant at which it changed (#243): what
    /// write_book_summary() writes beside the summary. The caller sets the track's `as_of_ts`.
    PricedBook compose_by_price(uint64_t at, std::string_view symbol, std::string_view exchange,
                                size_t depth, BestTrack* track) const;
    /// scan_by_time() over what gather_locked() took.
    TimeOrderedCost scan_gathered(const Gathered& g, uint64_t start_ns, uint64_t end_ns,
                                  std::string_view symbol, std::string_view exchange,
                                  ColumnSet columns, const RowFilter& filter,
                                  const std::function<bool(const SnapshotRow&)>& cb) const;
    /// Remove from `si` the summaries a row at `ts` falsifies - every one as of `ts` or later - and
    /// hand back their files for the caller to remove once the lock is released.
    static void falsify_summaries_locked(SymbolIndex& si, uint64_t ts, std::vector<std::string>& files);
    /// Remove the files of falsified summaries, and sync their directories: a summary that a power
    /// cut brings back after the rows it does not hold were sealed would be believed by the next
    /// start.
    static void remove_summary_files(const std::vector<std::string>& files, std::string_view why);
    /// The summaries in an indexed segment's directory, put in its symbol's map. Caller holds the lock.
    void register_summaries_locked(const SegmentMeta& meta);
    /// The summaries whose files are in `dir`, out of `si`: the segment they are in is leaving the
    /// index, and its directory the disk. Every path that takes a segment out calls this - a merge,
    /// a removal, retention - or a read would start from a file that is gone.
    static void drop_summaries_in_locked(SymbolIndex& si, const std::string& dir);
    static std::string index_key(std::string_view symbol, std::string_view exchange);
    /// Whether this index's `key` is the engine's `dotted` one (segments_of()).
    static bool is_dotted_key(const std::string& key, std::string_view dotted);

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

    /// What every slot `insert_locked()` gives belongs to; null with holding off (#220). Guarded by
    /// `index_mtx_`.
    std::shared_ptr<DecodedColumnsBudget> decoded_budget_;

    // What the last rebuild read to repair ranges written before #166. Guarded by index_mtx_.
    size_t last_rebuild_ranges_read_{0};
    // And what it removed (#165 part 2b). Guarded by index_mtx_.
    RebuildRemoved last_rebuild_removed_{};

    /// What a scan holds while it reads files outside the lock (#165 part 2b). A replacement starts
    /// a new generation, and each generation holds the one after it - an older one keeps every
    /// younger one alive - so the generation current before a replacement is gone exactly when every
    /// scan that took it, or an older one, has finished. Assigned under `index_mtx_` exclusively,
    /// copied under it shared.
    using ReaderGeneration = detail::ReaderGeneration;
    std::shared_ptr<ReaderGeneration> reader_generation_ = std::make_shared<ReaderGeneration>();

    /// Take one segment out of the index; false if it is not there. Caller holds `index_mtx_`
    /// exclusively, and erases the symbol's entry if this leaves it empty.
    bool erase_locked(const SegmentMeta& meta);

    /// Encode the active segment's buffers into `dir`, which exists and is empty, and reset the
    /// active state: what flush_segment() and flush_segment_into() share.
    SegmentMeta write_active_segment(const std::string& dir);
    /// The active segment's seven columns as `dir`/columns.v3 (segment format v3): each column in
    /// the encoding its last segment chose, or in the smallest candidate when this one searches.
    void write_columns_v3(const std::string& dir);

    /// What reading one segment came to: its rows handed over, or none because a column was
    /// missing or short (logged), or none because retention removed it while this read it.
    enum class SegmentRead { kRead, kUnreadable, kRemoved };

    /// What a scan reads, taken under the index's shared lock (#165): this symbol's candidates for
    /// the range in the order rows reach a reader, the blocks no seal has written yet, and the
    /// generation of readers that keeps the candidates' files until the read lets go of it.
    struct ScanSet {
        std::vector<SegmentMeta> segments;
        std::vector<std::shared_ptr<const RowBlock>> blocks;
        std::shared_ptr<const void> reading;
    };
    /// The set for [start_ns, end_ns], `cost.compared` counted; false when the symbol has none.
    bool scan_set(uint64_t start_ns, uint64_t end_ns, std::string_view symbol,
                  std::string_view exchange, ScanSet& set, ScanCost& cost) const;
    /// A set read on this thread, candidate by candidate and then the blocks, through `read`: the
    /// scan `scan_batches()` is, and `scan_parts()` with no pool to use.
    ScanCost read_in_order(const ScanSet& set, std::string_view symbol, std::string_view exchange,
                           ColumnSet columns, uint64_t start_ns, uint64_t end_ns,
                           const RowFilter& filter, const BatchReader& read, ScanCost cost) const;
    /// A query's read may find its segment removed by retention, and pads a short column the way it
    /// always has; a merge's finds nothing removed under it and takes a segment whole or not at all.
    enum class ReadMode { kQuery, kMerge };
    /// `filter` is checked on the decoded columns before a row is chosen (#47 step 2) - the columns
    /// it reads are in `columns`, which the scans make sure of - and its failures are counted into
    /// `filtered`; `read` takes the chosen rows a batch at a time (OB-12), and its stopping ends
    /// the read, which `stopped` then says.
    SegmentRead read_segment_rows(const SegmentMeta& meta, ColumnSet columns, uint64_t start_ns,
                                  uint64_t end_ns, const RowFilter& filter, const BatchReader& read,
                                  ReadMode mode, uint64_t* filtered = nullptr,
                                  bool* stopped = nullptr) const;
    /// A format-3 query read of a segment with a slot: the columns held, the rest read, decoded
    /// and held (#220).
    SegmentRead read_held_columns(const SegmentMeta& meta, ColumnSet columns, uint64_t start_ns,
                                  uint64_t end_ns, const RowFilter& filter, const BatchReader& read,
                                  uint64_t* filtered, bool* stopped) const;

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
    /// `inputs`, when given, is what a merged segment's meta.json names (#165 part 2b).
    void write_meta_json(const std::string& dir, const SegmentMeta& meta,
                         const std::vector<SegmentInput>* inputs = nullptr) const;
    std::string meta_json(const SegmentMeta& meta,
                          const std::vector<SegmentInput>* inputs = nullptr) const;
    /// `inputs`, when given, receives what a merged segment's `compacted_from` names.
    bool parse_meta_json(const std::string& path, SegmentMeta& out,
                         std::vector<SegmentInput>* inputs = nullptr) const;
};

} // namespace ob
