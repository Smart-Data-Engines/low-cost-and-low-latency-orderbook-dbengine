#include "orderbook/columnar_store.hpp"
#include "orderbook/codec.hpp"
#include "orderbook/logger.hpp"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <string>

namespace ob {

namespace fs = std::filesystem;

// ── Constructor ───────────────────────────────────────────────────────────────

ColumnarStore::ColumnarStore(std::string_view base_dir, uint64_t segment_duration_ns)
    : base_dir_(base_dir)
    , segment_duration_ns_(segment_duration_ns)
{}

// ── Private helpers ───────────────────────────────────────────────────────────

std::string ColumnarStore::segment_dir(const std::string& symbol,
                                        const std::string& exchange,
                                        uint64_t start_ts,
                                        uint64_t end_ts) const {
    std::ostringstream oss;
    oss << base_dir_ << "/" << symbol << "/" << exchange << "/"
        << start_ts << "_" << end_ts;
    return oss.str();
}

void ColumnarStore::ensure_dirs(const std::string& path) const {
    fs::create_directories(path);
}

/// How many segments may share one event-time span before this gives up. A backfill re-run
/// produces one collision, not thousands; a number this large being reached means something is
/// rewriting the same span in a loop, and failing the flush is the honest answer to that.
static constexpr uint32_t kMaxSegmentsPerSpan = 10000;

std::string ColumnarStore::create_unique_segment_dir(const std::string& symbol,
                                                     const std::string& exchange,
                                                     uint64_t start_ts,
                                                     uint64_t end_ts) const {
    const std::string base = segment_dir(symbol, exchange, start_ts, end_ts);
    ensure_dirs(fs::path(base).parent_path().string());

    std::error_code ec;
    if (fs::create_directory(base, ec)) return base;
    if (ec) {
        throw std::runtime_error("ColumnarStore: cannot create segment directory '" + base +
                                 "': " + ec.message());
    }

    // The span is taken. Before #136 this is where the second flush wrote over the first.
    for (uint32_t n = 1; n < kMaxSegmentsPerSpan; ++n) {
        const std::string candidate = base + "_" + std::to_string(n);
        if (fs::create_directory(candidate, ec)) {
            OB_LOG_INFO("columnar",
                        "Segment span %llu_%llu for %s.%s is already stored; this flush writes "
                        "'%s' instead of overwriting it",
                        static_cast<unsigned long long>(start_ts),
                        static_cast<unsigned long long>(end_ts),
                        symbol.c_str(), exchange.c_str(), candidate.c_str());
            return candidate;
        }
        if (ec) {
            throw std::runtime_error("ColumnarStore: cannot create segment directory '" +
                                     candidate + "': " + ec.message());
        }
    }
    throw std::runtime_error("ColumnarStore: " + std::to_string(kMaxSegmentsPerSpan) +
                             " segments already share the span " + std::to_string(start_ts) +
                             "_" + std::to_string(end_ts) + " for " + symbol + "." + exchange);
}


namespace {

/// Write `bytes` to a new file at `path`, and throw if any of it does not reach the file (#160).
///
/// The column files and meta.json went through `std::ofstream` and nothing read its state, so a
/// full disk produced a short file, the segment was merged as if whole, and a checkpoint claimed
/// its rows. Now a refused write, a short one, or a close that reports a deferred error (NFS does)
/// is an exception, which the flush turns into a failed flush: the rows stay in memory, no
/// checkpoint claims them, and the next flush writes a new segment. A raw write rather than a
/// stream also makes one system call of a column that a stream would cut into buffer-sized pieces.
void write_file_checked(const std::string& path, const void* data, size_t bytes) {
    // OB_DURABLE: a segment file - Engine::sync_segments() takes it to the device with one syncfs()
    // before any checkpoint claims its rows, and startup removes it if none does (#160).
    const int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    if (fd < 0) {
        throw std::runtime_error("ColumnarStore: cannot create " + path + ": " +
                                 std::strerror(errno));
    }
    const char* p = static_cast<const char*>(data);
    size_t left = bytes;
    while (left > 0) {
        const ssize_t n = ::write(fd, p, left);
        if (n < 0) {
            if (errno == EINTR) continue;
            const int err = errno;
            ::close(fd);
            throw std::runtime_error("ColumnarStore: cannot write " + path + ": " +
                                     std::strerror(err));
        }
        p += n;
        left -= static_cast<size_t>(n);
    }
    if (::close(fd) != 0) {
        throw std::runtime_error("ColumnarStore: cannot close " + path + ": " +
                                 std::strerror(errno));
    }
}

}  // namespace

void ColumnarStore::write_meta_json(const std::string& dir,
                                     const SegmentMeta& meta) const {
    std::string path = dir + "/meta.json";
    std::ostringstream f;
    f << "{\"format_version\":" << meta.format_version
      << ",\"start_ts_ns\":" << meta.start_ts_ns
      << ",\"end_ts_ns\":"   << meta.end_ts_ns
      << ",\"row_count\":"   << meta.row_count
      << ",\"first_price\":" << meta.first_price
      << ",\"has_raw_qty\":"  << (meta.has_raw_qty ? "true" : "false")
      << ",\"max_sequence_number\":" << meta.max_sequence_number
      << ",\"wal_identity\":"        << meta.wal_identity
      << ",\"wal_file_index\":"      << meta.wal_file_index
      << ",\"wal_byte_offset\":"     << meta.wal_byte_offset
      << ",\"symbol\":\""    << meta.symbol   << "\""
      << ",\"exchange\":\""  << meta.exchange << "\""
      << "}";
    const std::string content = f.str();
    write_file_checked(path, content.data(), content.size());
}

bool ColumnarStore::parse_meta_json(const std::string& path,
                                     SegmentMeta& out) const {
    std::ifstream f(path);
    if (!f.is_open()) return false;

    std::string content((std::istreambuf_iterator<char>(f)),
                         std::istreambuf_iterator<char>());

    // Simple hand-written JSON parser for the fixed schema:
    // {"start_ts_ns":N,"end_ts_ns":N,"row_count":N,"first_price":N,
    //  "has_raw_qty":bool,"symbol":"S","exchange":"E"}
    auto extract_uint64 = [&](const std::string& key) -> uint64_t {
        std::string search = "\"" + key + "\":";
        auto pos = content.find(search);
        if (pos == std::string::npos) return 0;
        pos += search.size();
        // skip whitespace
        while (pos < content.size() && content[pos] == ' ') ++pos;
        uint64_t val = 0;
        while (pos < content.size() && content[pos] >= '0' && content[pos] <= '9') {
            val = val * 10 + static_cast<uint64_t>(content[pos] - '0');
            ++pos;
        }
        return val;
    };

    auto extract_bool = [&](const std::string& key) -> bool {
        std::string search = "\"" + key + "\":";
        auto pos = content.find(search);
        if (pos == std::string::npos) return false;
        pos += search.size();
        while (pos < content.size() && content[pos] == ' ') ++pos;
        return content.substr(pos, 4) == "true";
    };

    auto extract_string = [&](const std::string& key) -> std::string {
        std::string search = "\"" + key + "\":\"";
        auto pos = content.find(search);
        if (pos == std::string::npos) return "";
        pos += search.size();
        std::string val;
        while (pos < content.size() && content[pos] != '"') {
            val += content[pos++];
        }
        return val;
    };

    // Absent in version 1 segments, which is why 0 means "too old to read".
    out.format_version = static_cast<uint32_t>(extract_uint64("format_version"));
    out.start_ts_ns  = extract_uint64("start_ts_ns");
    out.end_ts_ns    = extract_uint64("end_ts_ns");
    out.row_count    = extract_uint64("row_count");
    out.first_price  = extract_uint64("first_price");
    out.has_raw_qty  = extract_bool("has_raw_qty");
    // Missing in segments written before sequence numbers were assigned; 0 is then correct,
    // not a fallback.
    out.max_sequence_number = extract_uint64("max_sequence_number");
    // Absent in older segments, and 0 then means "unknown", not "position zero".
    out.wal_identity    = extract_uint64("wal_identity");
    out.wal_file_index  = static_cast<uint32_t>(extract_uint64("wal_file_index"));
    out.wal_byte_offset = extract_uint64("wal_byte_offset");
    out.symbol       = extract_string("symbol");
    out.exchange     = extract_string("exchange");

    // Validate: must have at least start_ts_ns and row_count
    return out.row_count > 0 || out.start_ts_ns > 0;
}

// ── append ────────────────────────────────────────────────────────────────────

void ColumnarStore::append(const SnapshotRow& row) {
    // Determine segment boundary (round down to segment duration)
    uint64_t seg_start = (row.timestamp_ns / segment_duration_ns_) * segment_duration_ns_;

    if (!has_active_segment_) {
        // Start first segment
        active_segment_start_ = seg_start;
        // Only reset symbol_/exchange_ if they haven't been set externally
        // (set_symbol_exchange() allows the C API to inject them).
        if (symbol_.empty() && exchange_.empty()) {
            symbol_   = "";
            exchange_ = "";
        }
        has_active_segment_ = true;
        active_row_count_   = 0;
        active_has_raw_qty_ = false;
        price_buf_.clear();
        qty_buf_.clear();
        ts_buf_.clear();
        cnt_buf_.clear();
        side_buf_.clear();
        level_buf_.clear();
        seq_buf_.clear();
    } else if (row.timestamp_ns >= active_segment_start_ + segment_duration_ns_) {
        // Roll over to a new segment. The meta must be kept: this store has no
        // reference to the index queries read, and discarding it left the segment
        // on disk and invisible to every SELECT until the next open_existing().
        auto rolled = flush_segment();
        if (rolled.has_value()) {
            OB_LOG_DEBUG("columnar",
                         "Segment rolled over: dir=%s rows=%llu, meta queued for merge",
                         rolled->dir_path.c_str(),
                         static_cast<unsigned long long>(rolled->row_count));
            std::unique_lock<std::shared_mutex> lock(index_mtx_);
            rolled_segments_.push_back(std::move(rolled.value()));
        }
        active_segment_start_ = seg_start;
        has_active_segment_   = true;
        active_row_count_     = 0;
        active_has_raw_qty_   = false;
        price_buf_.clear();
        qty_buf_.clear();
        ts_buf_.clear();
        cnt_buf_.clear();
        side_buf_.clear();
        level_buf_.clear();
        seq_buf_.clear();
    }

    // Accumulate into buffers
    price_buf_.push_back(row.price);
    qty_buf_.push_back(row.quantity);
    ts_buf_.push_back(row.timestamp_ns);
    cnt_buf_.push_back(row.order_count);
    side_buf_.push_back(row.side);
    level_buf_.push_back(row.level_index);
    // Sequence numbers go through the zigzag-delta price codec, which is int64.
    // Real sequences stay far below 2^63; the cast is explicit so this does not
    // read as an oversight.
    seq_buf_.push_back(static_cast<int64_t>(row.sequence_number));
    ++active_row_count_;

    // Check if qty needs fallback (> 2^60 - 1)
    static constexpr uint64_t kMaxSimple8b = (1ULL << 60) - 1;
    if (row.quantity > kMaxSimple8b) {
        active_has_raw_qty_ = true;
    }
}


namespace {

/// Total order for the segment index.
///
/// Ordering by start_ts_ns alone is a partial order: two segments can share a start
/// timestamp, and std::sort is not stable, so their relative position was whatever
/// the implementation happened to produce. Scan output order then varied between
/// runs on identical data, and a TTL property test comparing surviving segments
/// element-wise failed roughly one run in three with `304 == 303` — two segments
/// with the same start, tie-broken differently on each side of the comparison.
/// dir_path is unique per segment, so this is a total order.
bool segment_order_less(const SegmentMeta& a, const SegmentMeta& b) {
    if (a.start_ts_ns != b.start_ts_ns) return a.start_ts_ns < b.start_ts_ns;
    if (a.end_ts_ns != b.end_ts_ns) return a.end_ts_ns < b.end_ts_ns;
    return a.dir_path < b.dir_path;
}

} // anonymous namespace

// ── flush_segment ─────────────────────────────────────────────────────────────

std::optional<SegmentMeta> ColumnarStore::flush_segment() {
    if (!has_active_segment_ || active_row_count_ == 0) {
        has_active_segment_ = false;
        return std::nullopt;
    }

    // Compute end timestamp
    uint64_t end_ts = ts_buf_.empty() ? active_segment_start_
                                      : ts_buf_.back();

    // Build segment directory path. Unique per segment rather than per span, so a second flush
    // covering the same event-time range is a second segment instead of a collision (#136).
    std::string dir = create_unique_segment_dir(symbol_, exchange_,
                                                active_segment_start_, end_ts);

    // A write that fails part-way leaves no directory behind (#160): the exception goes to the
    // flush, the rows stay in memory, and the next flush writes a new segment - so a half-written
    // one would only be litter, and litter a later scan could mistake for data.
    struct PartialSegment {
        const std::string& dir;
        bool complete{false};
        ~PartialSegment() {
            if (complete) return;
            std::error_code ec;
            fs::remove_all(dir, ec);
            OB_LOG_DEBUG("columnar", "removed the partly written segment %s%s", dir.c_str(),
                         ec ? " (and could not remove all of it)" : "");
        }
    } partial{dir, false};

    // Encode price column: delta + zigzag
    auto encoded_prices = encode_prices(price_buf_);

    // Encode qty column: Simple8b
    auto qty_result = encode_simple8b(qty_buf_);

    // Write price.col
    {
        write_file_checked(dir + "/price.col", encoded_prices.data(), encoded_prices.size() * sizeof(uint64_t));
    }

    // Write qty.col
    {
        write_file_checked(dir + "/qty.col", qty_result.words.data(), qty_result.words.size() * sizeof(uint64_t));
    }

    // Write ts.col (raw uint64)
    {
        write_file_checked(dir + "/ts.col", ts_buf_.data(), ts_buf_.size() * sizeof(uint64_t));
    }

    // Write cnt.col (raw uint32)
    {
        write_file_checked(dir + "/cnt.col", cnt_buf_.data(), cnt_buf_.size() * sizeof(uint32_t));
    }

    // Write side.col (raw uint8, one byte per row)
    {
        write_file_checked(dir + "/side.col", side_buf_.data(), side_buf_.size() * sizeof(uint8_t));
    }

    // Write level.col (raw uint16)
    {
        write_file_checked(dir + "/level.col", level_buf_.data(), level_buf_.size() * sizeof(uint16_t));
    }

    // Write seq.col (zigzag-delta, then Simple8b bit-packing).
    //
    // Two stages, and both are needed. Zigzag-delta turns "5000000042" into a
    // small non-negative number, and handles a falling sequence, which happens in
    // multi-master mode when records from different nodes land in one segment.
    // Simple8b then packs those small values into shared 64-bit words. Without
    // the second stage the column costs a full 8 bytes per row regardless of how
    // small the deltas are, because encode_prices() returns one uint64 each —
    // measured at 8.00 B/row before this was added, 0.14 B/row after.
    {
        auto zigzag_seq = encode_prices(seq_buf_);
        auto packed_seq = encode_simple8b(zigzag_seq);
        write_file_checked(dir + "/seq.col", packed_seq.words.data(),
                           packed_seq.words.size() * sizeof(uint64_t));
    }

    OB_LOG_DEBUG("columnar",
                 "Segment written: dir=%s rows=%zu columns=7 (side/level/seq included)",
                 dir.c_str(), static_cast<size_t>(active_row_count_));

    // Build SegmentMeta
    SegmentMeta meta{};
    meta.start_ts_ns = active_segment_start_;
    meta.end_ts_ns   = end_ts;
    meta.row_count   = active_row_count_;
    // first_price: store the zigzag-encoded first price as the anchor
    meta.first_price = encoded_prices.empty() ? 0 : encoded_prices[0];
    meta.has_raw_qty = active_has_raw_qty_ || qty_result.has_fallback;
    // Highest, not last: rows are appended in arrival order and a batch can hold numbers
    // from several origins, so the last one is not necessarily the largest.
    meta.max_sequence_number = 0;
    for (int64_t seq : seq_buf_) {
        if (seq > 0 && static_cast<uint64_t>(seq) > meta.max_sequence_number) {
            meta.max_sequence_number = static_cast<uint64_t>(seq);
        }
    }
    meta.symbol      = symbol_;
    meta.exchange    = exchange_;
    meta.dir_path    = dir;
    meta.wal_identity    = wal_identity_;
    meta.wal_file_index  = wal_file_index_;
    meta.wal_byte_offset = wal_byte_offset_;

    // Write meta.json - last, so that a segment with one is a segment with all of its columns.
    write_meta_json(dir, meta);
    partial.complete = true;

    // Add to index (backward compatibility)
    {
        std::unique_lock<std::shared_mutex> lock(index_mtx_);
        index_.push_back(meta);
    }

    // Reset active segment state
    has_active_segment_ = false;
    active_row_count_   = 0;
    active_has_raw_qty_ = false;
    price_buf_.clear();
    qty_buf_.clear();
    ts_buf_.clear();
    cnt_buf_.clear();

    return meta;
}

// ── scan ──────────────────────────────────────────────────────────────────────

namespace {

/// Read a whole column file into `out`; false when the file is not there.
///
/// It replaces seven near-identical copies that each decided for themselves what a missing file
/// meant - four skipped the segment in silence, three logged first. With a read set the answer
/// depends on whether the caller asked for that column, so the decision moves to the caller and
/// this only reports.
template <typename T>
bool read_column_file(const std::string& dir, const char* name, std::vector<T>& out) {
    const std::string path = dir + "/" + name;
    std::ifstream f(path, std::ios::binary);
    if (!f.is_open()) return false;
    f.seekg(0, std::ios::end);
    const auto sz = static_cast<size_t>(f.tellg());
    f.seekg(0, std::ios::beg);
    out.resize(sz / sizeof(T));
    f.read(reinterpret_cast<char*>(out.data()), static_cast<std::streamsize>(sz));
    return true;
}

}  // namespace

void ColumnarStore::scan(uint64_t start_ns, uint64_t end_ns,
                          std::string_view symbol, std::string_view exchange,
                          ColumnSet columns,
                          std::function<void(const SnapshotRow&)> cb) const {
    // Whatever the caller asked to be handed, this filters on the row's timestamp, so it reads
    // that column. Adding it here rather than trusting the caller means a set built by hand
    // cannot produce a scan that compares every row against a zero it never loaded.
    columns.add(QueryColumn::TimestampNs);

    // Take a snapshot of the index under shared lock to avoid data race
    // with merge_segments() which modifies index_ under exclusive lock.
    std::vector<SegmentMeta> index_snapshot;
    {
        std::shared_lock<std::shared_mutex> lock(index_mtx_);
        index_snapshot = index_;
    }

    const bool want_price = columns.has(QueryColumn::Price);
    const bool want_qty   = columns.has(QueryColumn::Quantity);
    const bool want_cnt   = columns.has(QueryColumn::OrderCount);
    const bool want_side  = columns.has(QueryColumn::Side);
    const bool want_level = columns.has(QueryColumn::Level);
    const bool want_seq   = columns.has(QueryColumn::SequenceNumber);

    for (const auto& meta : index_snapshot) {
        // Filter by symbol/exchange
        if (meta.symbol != symbol || meta.exchange != exchange) continue;

        // Time-range pruning: skip segments that don't overlap [start_ns, end_ns]
        if (meta.end_ts_ns < start_ns || meta.start_ts_ns > end_ns) continue;

        const std::string& dir = meta.dir_path;

        // A segment written by an older format lacks side, level_index and
        // sequence_number. Reading it anyway would hand back rows with those
        // fields silently zeroed, which is the defect this version exists to
        // fix. Refuse it loudly instead.
        if (meta.format_version != kColumnarFormatVersion) {
            OB_LOG_ERROR("columnar",
                         "Skipping segment %s: unsupported format_version=%u "
                         "(this build reads %u)",
                         dir.c_str(), meta.format_version, kColumnarFormatVersion);
            continue;
        }

        std::vector<uint64_t> timestamps, enc_prices, enc_qtys, enc_seq;
        std::vector<uint32_t> counts;
        std::vector<uint8_t>  sides;
        std::vector<uint16_t> levels;

        // A missing file is fatal for the segment only when the query needs that column. Before
        // the read set existed every column was needed, so a segment missing any one of the seven
        // was dropped from every query - including queries that would never have looked at it.
        bool missing = false;
        auto need = [&](bool wanted, const char* file, auto& dest) {
            if (!wanted) return;
            if (!read_column_file(dir, file, dest)) {
                OB_LOG_ERROR("columnar", "Skipping segment %s: missing column %s",
                             dir.c_str(), file);
                missing = true;
            }
        };
        // Every column is opened through the set, the timestamp included - the widening at the
        // top of this function is what puts it there. A hardcoded `true` here reads as belt and
        // braces and is worse than that: it makes that widening unobservable, so a mutation
        // deleting it survived the test written to catch exactly that.
        need(columns.has(QueryColumn::TimestampNs), "ts.col", timestamps);
        need(want_price, "price.col", enc_prices);
        need(want_qty,   "qty.col",   enc_qtys);
        need(want_cnt,   "cnt.col",   counts);
        need(want_side,  "side.col",  sides);
        need(want_level, "level.col", levels);
        need(want_seq,   "seq.col",   enc_seq);
        if (missing) continue;

        // Decoding follows the set too, and the sequence number is the expensive one: it is
        // Simple8b **and** zigzag-delta, so a query that does not ask for it skips two of the
        // four decode passes a segment would otherwise cost.
        std::vector<int64_t>  prices;
        std::vector<uint64_t> qtys;
        std::vector<int64_t>  seqs;
        if (want_price) prices = decode_prices(enc_prices);
        if (want_qty)   qtys   = decode_simple8b(enc_qtys, meta.row_count);
        if (want_seq) {
            auto zigzag_seq = decode_simple8b(enc_seq, meta.row_count);
            seqs = decode_prices(zigzag_seq);
        }

        // A short column means a truncated or corrupt segment. Emitting the rows
        // it does have, padded with zeros, is what produced the lost-order-side
        // defect this format version fixes, so refuse the segment instead. Only the columns
        // being read can be short here; one that was never opened is empty by construction.
        const size_t expected = static_cast<size_t>(meta.row_count);
        if ((want_side  && sides.size()  < expected) ||
            (want_level && levels.size() < expected) ||
            (want_seq   && seqs.size()   < expected)) {
            OB_LOG_ERROR("columnar",
                         "Skipping segment %s: short column(s) for row_count=%zu "
                         "(side=%zu level=%zu seq=%zu)",
                         dir.c_str(), expected,
                         sides.size(), levels.size(), seqs.size());
            continue;
        }

        // Emit rows within time range
        size_t n = meta.row_count;
        for (size_t i = 0; i < n; ++i) {
            uint64_t ts = (i < timestamps.size()) ? timestamps[i] : 0;
            if (ts < start_ns || ts > end_ns) continue;

            // Value-initialised, so a field whose column was not read is zero rather than
            // whatever the last row left there.
            SnapshotRow row{};
            row.timestamp_ns = ts;
            if (want_seq)   row.sequence_number = static_cast<uint64_t>(seqs[i]);
            if (want_side)  row.side            = sides[i];
            if (want_level) row.level_index     = levels[i];
            if (want_price) row.price           = (i < prices.size()) ? prices[i] : 0;
            if (want_qty)   row.quantity        = (i < qtys.size())   ? qtys[i]   : 0;
            if (want_cnt)   row.order_count     = (i < counts.size()) ? counts[i] : 0;
            cb(row);
        }
    }
}

// ── open_existing ─────────────────────────────────────────────────────────────

void ColumnarStore::open_existing() {
    std::unique_lock<std::shared_mutex> lock(index_mtx_);
    rebuild_index_locked();
}

void ColumnarStore::rebuild_index_locked() {
    index_.clear();

    if (!fs::exists(base_dir_)) return;

    // Recursively scan for meta.json files
    for (auto& entry : fs::recursive_directory_iterator(base_dir_)) {
        if (!entry.is_regular_file()) continue;
        if (entry.path().filename() != "meta.json") continue;

        SegmentMeta meta;
        if (!parse_meta_json(entry.path().string(), meta)) continue;

        meta.dir_path = entry.path().parent_path().string();
        index_.push_back(meta);
    }

    // Sort by start_ts_ns
    std::sort(index_.begin(), index_.end(), segment_order_less);
}

bool ColumnarStore::replace_from_staging(const std::string& staging_dir,
                                         const std::vector<std::string>& relative_paths) {
    std::unique_lock<std::shared_mutex> lock(index_mtx_);

    // The index goes first, so that nothing below is describing directories it names. A scan
    // waits on this same mutex and then reads the store this function leaves behind.
    index_.clear();
    rolled_segments_.clear();
    has_active_segment_ = false;
    active_row_count_   = 0;

    std::error_code ec;
    const fs::path staging = fs::absolute(fs::path(staging_dir), ec);

    size_t removed = 0;
    for (auto& entry : fs::directory_iterator(base_dir_, ec)) {
        if (!entry.is_directory(ec)) continue;               // repl_state.txt and friends
        const std::string name = entry.path().filename().string();
        if (name.rfind("wal_", 0) == 0) continue;            // WAL files and wal_identity
        const fs::path here = fs::absolute(entry.path(), ec);
        if (!staging.empty() && here == staging) continue;   // the files we are about to install
        OB_LOG_DEBUG("columnar", "replace_from_staging: removing stale segment directory '%s'",
                     entry.path().string().c_str());
        fs::remove_all(entry.path(), ec);
        if (ec) {
            OB_LOG_ERROR("columnar",
                         "replace_from_staging: cannot remove '%s': %s — the store now holds "
                         "part of what was here and part of what is arriving",
                         entry.path().string().c_str(), ec.message().c_str());
            rebuild_index_locked();
            return false;
        }
        ++removed;
    }

    size_t installed = 0;
    for (const auto& rel : relative_paths) {
        const fs::path src = fs::path(staging_dir) / rel;
        const fs::path dst = fs::path(base_dir_) / rel;
        fs::create_directories(dst.parent_path(), ec);
        fs::rename(src, dst, ec);
        if (ec) {
            // Cross-device staging: copy then remove.
            // OB_DURABLE: an installed snapshot file - Engine::install_snapshot() syncs the data
            // directory before anything records the snapshot's position (#162).
            std::error_code copy_ec;
            fs::copy_file(src, dst, fs::copy_options::overwrite_existing, copy_ec);
            if (copy_ec) {
                OB_LOG_ERROR("columnar",
                             "replace_from_staging: cannot install '%s': %s",
                             rel.c_str(), copy_ec.message().c_str());
                rebuild_index_locked();
                return false;
            }
            fs::remove(src, copy_ec);
        }
        ++installed;
    }

    rebuild_index_locked();
    OB_LOG_INFO("columnar",
                "Store replaced from staging: removed %zu stale directory/ies, installed %zu "
                "file(s), index now holds %zu segment(s)",
                removed, installed, index_.size());
    return true;
}

// ── merge_segments ────────────────────────────────────────────────────────────

std::vector<SegmentMeta> ColumnarStore::take_rolled_segments() {
    std::unique_lock<std::shared_mutex> lock(index_mtx_);
    std::vector<SegmentMeta> taken;
    taken.swap(rolled_segments_);
    return taken;
}

size_t ColumnarStore::merge_segments(const std::vector<SegmentMeta>& new_segments) {
    if (new_segments.empty()) return 0;
    std::unique_lock<std::shared_mutex> lock(index_mtx_);

    size_t refused = 0;
    bool added = false;
    for (const auto& meta : new_segments) {
        // Defence in depth, not the fix. Since #136 a directory belongs to one segment rather
        // than to one event-time span, so two flushes covering the same span get two
        // directories and cannot reach here; what remains is the same meta merged twice, which
        // is a caller mistake. The message below used to name a cause — "two flush paths raced"
        // — that stopped being the only way here the moment #105 let a client choose its own
        // event time, and it sent a reader hunting a race that had not happened. It reports
        // the state now and leaves the cause to whoever has the rest of the log.
        const bool already_indexed =
            std::any_of(index_.begin(), index_.end(),
                        [&](const SegmentMeta& m) { return m.dir_path == meta.dir_path; });
        if (already_indexed) {
            OB_LOG_ERROR("columnar",
                         "Refusing to merge a segment already in the index: dir=%s rows=%llu. "
                         "Merging it would make every client read those rows twice",
                         meta.dir_path.c_str(),
                         static_cast<unsigned long long>(meta.row_count));
            ++refused;
            continue;
        }
        index_.push_back(meta);
        added = true;
    }

    if (added) {
        std::sort(index_.begin(), index_.end(), segment_order_less);
    }
    return refused;
}

// ── delete_expired_segments ───────────────────────────────────────────────────

std::pair<size_t, size_t> ColumnarStore::delete_expired_segments(uint64_t cutoff_ns) {
    if (cutoff_ns == 0) {
        return {0, 0};
    }

    std::unique_lock<std::shared_mutex> lock(index_mtx_);

    // Sort index by start_ts_ns (oldest first) for chronological deletion order.
    std::sort(index_.begin(), index_.end(), segment_order_less);

    size_t segments_deleted = 0;
    size_t bytes_reclaimed = 0;

    // Partition: expired segments have end_ts_ns < cutoff_ns.
    // We iterate and collect indices to remove, then erase.
    std::vector<SegmentMeta> remaining;
    remaining.reserve(index_.size());

    for (auto& meta : index_) {
        if (meta.end_ts_ns < cutoff_ns) {
            // Compute directory size before deletion.
            size_t dir_bytes = 0;
            std::error_code ec;
            for (auto& entry : fs::recursive_directory_iterator(meta.dir_path, ec)) {
                if (entry.is_regular_file(ec)) {
                    dir_bytes += static_cast<size_t>(entry.file_size(ec));
                }
            }

            // Attempt to remove the segment directory.
            std::error_code rm_ec;
            fs::remove_all(meta.dir_path, rm_ec);
            if (rm_ec) {
                OB_LOG_ERROR("retention", "failed to delete segment %s: %s",
                    meta.dir_path.c_str(), rm_ec.message().c_str());
                // Keep the segment in the index since deletion failed.
                remaining.push_back(std::move(meta));
                continue;
            }

            ++segments_deleted;
            bytes_reclaimed += dir_bytes;

            // How far past the cutoff the segment's newest row is - which this line used to call
            // the segment's age. It is not: a segment 25 hours old under a 24-hour retention is an
            // hour past it. And while the cutoff was a count from boot (#163) it said
            // `age=4626822.4h` of a segment written a second before.
            const uint64_t past_ns = (cutoff_ns > meta.end_ts_ns) ? (cutoff_ns - meta.end_ts_ns) : 0;
            OB_LOG_INFO("retention",
                        "deleted segment %s: its newest row is %.1f h past the retention, %zu bytes",
                        meta.dir_path.c_str(), static_cast<double>(past_ns) / 3.6e12, dir_bytes);
        } else {
            remaining.push_back(std::move(meta));
        }
    }

    index_ = std::move(remaining);
    return {segments_deleted, bytes_reclaimed};
}

// ── remove_segments ───────────────────────────────────────────────────────────

size_t ColumnarStore::remove_segments(const std::vector<std::string>& dirs) {
    if (dirs.empty()) return 0;
    std::unique_lock<std::shared_mutex> lock(index_mtx_);
    size_t removed = 0;
    std::vector<SegmentMeta> remaining;
    remaining.reserve(index_.size());
    for (auto& meta : index_) {
        if (std::find(dirs.begin(), dirs.end(), meta.dir_path) == dirs.end()) {
            remaining.push_back(std::move(meta));
            continue;
        }
        std::error_code ec;
        fs::remove_all(meta.dir_path, ec);
        if (ec) {
            OB_LOG_ERROR("columnar", "cannot remove segment %s: %s - it stays in the index",
                         meta.dir_path.c_str(), ec.message().c_str());
            remaining.push_back(std::move(meta));
            continue;
        }
        OB_LOG_DEBUG("columnar", "removed segment %s", meta.dir_path.c_str());
        ++removed;
    }
    index_ = std::move(remaining);
    return removed;
}

// ── close ─────────────────────────────────────────────────────────────────────

void ColumnarStore::close() {
    if (has_active_segment_ && active_row_count_ > 0) {
        flush_segment();
    }
    has_active_segment_ = false;
}

} // namespace ob
