#include "orderbook/columnar_store.hpp"
#include "orderbook/codec.hpp"
#include "orderbook/logger.hpp"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <bit>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>

namespace ob {

namespace fs = std::filesystem;

// ── Constructor ───────────────────────────────────────────────────────────────

ColumnarStore::ColumnarStore(std::string_view base_dir, uint64_t segment_duration_ns,
                             OwnIndex own_index)
    : base_dir_(base_dir)
    , segment_duration_ns_(segment_duration_ns)
    , own_index_(own_index)
{}

// ── Blocks: drained rows before a seal (#165 part 2a) ────────────────────────

std::shared_ptr<const RowBlock> RowBlock::make(std::string symbol, std::string exchange,
                                               std::vector<SnapshotRow> rows) {
    if (rows.empty()) return nullptr;
    uint64_t min_ts = rows.front().timestamp_ns;
    uint64_t max_ts = rows.front().timestamp_ns;
    for (const auto& r : rows) {
        min_ts = std::min(min_ts, r.timestamp_ns);
        max_ts = std::max(max_ts, r.timestamp_ns);
    }
    return make(std::move(symbol), std::move(exchange), std::move(rows), min_ts, max_ts);
}

std::shared_ptr<const RowBlock> RowBlock::make(std::string symbol, std::string exchange,
                                               std::vector<SnapshotRow> rows,
                                               uint64_t min_ts_ns, uint64_t max_ts_ns) {
    if (rows.empty()) return nullptr;
    auto block = std::make_shared<RowBlock>();
    block->symbol    = std::move(symbol);
    block->exchange  = std::move(exchange);
    block->min_ts_ns = min_ts_ns;
    block->max_ts_ns = max_ts_ns;
    block->rows      = std::move(rows);
    return block;
}

void ColumnarStore::publish_blocks(const std::vector<std::shared_ptr<const RowBlock>>& blocks) {
    if (blocks.empty()) return;
    size_t rows = 0;
    {
        std::unique_lock<std::shared_mutex> lock(index_mtx_);
        for (const auto& b : blocks) {
            if (!b) continue;
            by_symbol_[index_key(b->symbol, b->exchange)].blocks.push_back(b);
            rows += b->rows.size();
        }
        unsealed_rows_ += rows;
    }
    OB_LOG_DEBUG("columnar", "published %zu block(s) of %zu row(s) for queries to read unsealed",
                 blocks.size(), rows);
}

void ColumnarStore::drop_blocks() {
    size_t blocks = 0;
    size_t rows = 0;
    {
        std::unique_lock<std::shared_mutex> lock(index_mtx_);
        for (auto it = by_symbol_.begin(); it != by_symbol_.end();) {
            blocks += it->second.blocks.size();
            it->second.blocks.clear();
            it = it->second.tiers.empty() ? by_symbol_.erase(it) : std::next(it);
        }
        rows = unsealed_rows_;
        unsealed_rows_ = 0;
    }
    OB_LOG_DEBUG("columnar", "dropped %zu unsealed block(s) of %zu row(s)", blocks, rows);
}

void ColumnarStore::abandon_active() {
    std::vector<SegmentMeta> rolled;
    {
        std::unique_lock<std::shared_mutex> lock(index_mtx_);
        rolled.swap(rolled_segments_);
    }
    for (const auto& meta : rolled) {
        std::error_code ec;
        fs::remove_all(meta.dir_path, ec);
        OB_LOG_WARN("columnar", "removed %s, which a rollover wrote for a seal that then failed; its "
                                "rows are written again by the next one%s",
                    meta.dir_path.c_str(), ec ? " (and it could not be removed)" : "");
    }
    has_active_segment_ = false;
    active_row_count_   = 0;
    active_has_raw_qty_ = false;
    price_buf_.clear();
    qty_buf_.clear();
    ts_buf_.clear();
    cnt_buf_.clear();
    side_buf_.clear();
    level_buf_.clear();
    seq_buf_.clear();
}

size_t ColumnarStore::seal_blocks(const std::string& symbol, const std::string& exchange,
                                  size_t count, const std::vector<SegmentMeta>& segments) {
    size_t refused = 0;
    size_t rows = 0;
    size_t taken = 0;
    {
        std::unique_lock<std::shared_mutex> lock(index_mtx_);
        auto& entry = by_symbol_[index_key(symbol, exchange)];
        for (; taken < count && !entry.blocks.empty(); ++taken) {
            rows += entry.blocks.front()->rows.size();
            entry.blocks.pop_front();
        }
        unsealed_rows_ -= std::min(rows, unsealed_rows_);
        for (const auto& meta : segments) {
            if (!insert_locked(meta)) ++refused;
        }
        if (entry.tiers.empty() && entry.blocks.empty()) by_symbol_.erase(index_key(symbol, exchange));
    }
    if (taken != count) {
        OB_LOG_ERROR("columnar", "sealing %s.%s: asked to replace %zu block(s) and found %zu - the "
                                 "blocks a seal wrote from are not the ones published first",
                     symbol.c_str(), exchange.c_str(), count, taken);
    }
    if (refused > 0) {
        OB_LOG_ERROR("columnar", "sealing %s.%s: %zu segment(s) already in the index were refused",
                     symbol.c_str(), exchange.c_str(), refused);
    }
    OB_LOG_DEBUG("columnar", "sealed %s.%s: %zu block(s), %zu row(s), into %zu segment(s)",
                 symbol.c_str(), exchange.c_str(), taken, rows, segments.size());
    return refused;
}

// ── The index, per symbol (#165) ──────────────────────────────────────────────

std::string ColumnarStore::index_key(std::string_view symbol, std::string_view exchange) {
    std::string key;
    key.reserve(symbol.size() + 1 + exchange.size());
    key.append(symbol);
    key.push_back('\0');
    key.append(exchange);
    return key;
}

bool ColumnarStore::still_indexed(const std::string& dir) const {
    std::shared_lock<std::shared_mutex> lock(index_mtx_);
    return indexed_dirs_.count(dir) != 0;
}

bool ColumnarStore::holds(std::string_view symbol, std::string_view exchange) const {
    std::shared_lock<std::shared_mutex> lock(index_mtx_);
    const auto it = by_symbol_.find(index_key(symbol, exchange));
    if (it == by_symbol_.end()) return false;
    const auto& tiers = it->second.tiers;
    return !it->second.blocks.empty() ||
           std::any_of(tiers.begin(), tiers.end(),
                       [](const WidthTier& t) { return !t.segments.empty(); });
}

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

/// Where a corrected meta.json waits for its sync before a rename publishes it (#166).
constexpr const char* kRangeRepairFile = "meta.json.range";

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
    const std::string content = meta_json(meta);
    write_file_checked(dir + "/meta.json", content.data(), content.size());
}

std::string ColumnarStore::meta_json(const SegmentMeta& meta) const {
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
      << ",\"seal_epoch\":"          << meta.seal_epoch
      << ",\"symbol\":\""    << meta.symbol   << "\""
      << ",\"exchange\":\""  << meta.exchange << "\""
      << ",\"last_row_ts_ns\":" << meta.last_row_ts_ns;
    // Only ever written as "rows": a meta.json without the key is one written before #166, and
    // that absence is what the repair at open looks for.
    if (meta.time_range_is_rows) f << ",\"time_range\":\"rows\"";
    f << "}";
    return f.str();
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
    // Absent and zero are different answers for some keys, so the lookup can say which.
    auto find_uint64 = [&](const std::string& key) -> std::optional<uint64_t> {
        std::string search = "\"" + key + "\":";
        auto pos = content.find(search);
        if (pos == std::string::npos) return std::nullopt;
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
    auto extract_uint64 = [&](const std::string& key) -> uint64_t {
        return find_uint64(key).value_or(0);
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
    // Absent before #165 part 2a, and 0 then: sealed before any epoch a checkpoint can name.
    out.seal_epoch      = extract_uint64("seal_epoch");
    out.symbol       = extract_string("symbol");
    out.exchange     = extract_string("exchange");
    // Both absent before #166. Then the recorded end WAS the last row's time - the number replay's
    // fallback has always compared with - so it is carried over as that, before any repair moves
    // the end to the rows' maximum.
    out.time_range_is_rows = extract_string("time_range") == "rows";
    out.last_row_ts_ns     = find_uint64("last_row_ts_ns").value_or(out.end_ts_ns);

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
        active_min_ts_      = row.timestamp_ns;
        active_max_ts_      = row.timestamp_ns;
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
        active_min_ts_        = row.timestamp_ns;
        active_max_ts_        = row.timestamp_ns;
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

    // What the segment will say it covers (#166). Rows arrive in the order they were written, and
    // that is time order only while the server stamps every one: a client giving its own event
    // times (#105) can send them in any order, and a multi-master node applies a peer's record,
    // stamped by the peer, after a later one of its own. A row that arrives out of order and is
    // still appended here - one from an earlier period is, because only a later period rolls the
    // segment over - has to be inside the range queries prune by and retention deletes by.
    if (row.timestamp_ns < active_min_ts_) active_min_ts_ = row.timestamp_ns;
    if (row.timestamp_ns > active_max_ts_) active_max_ts_ = row.timestamp_ns;

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

void ColumnarStore::append_block(const RowBlock& block) {
    const std::vector<SnapshotRow>& rows = block.rows;
    if (rows.empty()) return;
    // The first row opens the segment, or rolls it over, as it would on its own.
    append(rows.front());
    if (block.max_ts_ns >= active_segment_start_ + segment_duration_ns_) {
        // A row of a later period is in the block and rolls the segment over where it stands, so
        // the rest goes row by row. Rare: a block is one drain of one symbol - 100 ms of it at the
        // default interval - and a period is an hour.
        OB_LOG_DEBUG("columnar", "a block of %zu row(s) for %s.%s reaches past its segment's period; "
                                 "appended row by row",
                     rows.size(), symbol_.c_str(), exchange_.c_str());
        for (size_t i = 1; i < rows.size(); ++i) append(rows[i]);
        return;
    }
    // Every other row stays in this segment - none is of a later period, and one of an earlier
    // period stays, as it does in append() - so the block's range is its rows' range here too.
    //
    // No quantity is tested for Simple8b's width, as append() tests each: flush_segment() records
    // a segment's quantities as raw when that flag says so *or* the encoder fell back, and the
    // encoder falls back for exactly the quantities append() tests - so the flag changes no byte
    // it writes. Found by the mutation table: dropping the test here was the one row it could not
    // kill.
    for (size_t i = 1; i < rows.size(); ++i) {
        const SnapshotRow& row = rows[i];
        price_buf_.push_back(row.price);
        qty_buf_.push_back(row.quantity);
        ts_buf_.push_back(row.timestamp_ns);
        cnt_buf_.push_back(row.order_count);
        side_buf_.push_back(row.side);
        level_buf_.push_back(row.level_index);
        seq_buf_.push_back(static_cast<int64_t>(row.sequence_number));
    }
    active_row_count_ += rows.size() - 1;
    if (block.min_ts_ns < active_min_ts_) active_min_ts_ = block.min_ts_ns;
    if (block.max_ts_ns > active_max_ts_) active_max_ts_ = block.max_ts_ns;
}

void ColumnarStore::reserve_rows(size_t rows) {
    // Without an active segment the buffers still hold the last written one's rows, which the next
    // append() clears - so they are not what the room is added to.
    const size_t base = has_active_segment_ ? price_buf_.size() : 0;
    price_buf_.reserve(base + rows);
    qty_buf_.reserve(base + rows);
    ts_buf_.reserve(base + rows);
    cnt_buf_.reserve(base + rows);
    side_buf_.reserve(base + rows);
    level_buf_.reserve(base + rows);
    seq_buf_.reserve(base + rows);
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

bool ColumnarStore::insert_locked(SegmentMeta meta) {
    if (!indexed_dirs_.insert(meta.dir_path).second) return false;
    SymbolIndex& si = by_symbol_[index_key(meta.symbol, meta.exchange)];
    // A range that ends before it starts - a segment #166's repair could not read - is as wide as
    // nothing, and is found by its start as it always was.
    const uint64_t width =
        meta.end_ts_ns > meta.start_ts_ns ? meta.end_ts_ns - meta.start_ts_ns : 0;
    const auto bits = static_cast<unsigned>(std::bit_width(width));
    auto tier = std::lower_bound(si.tiers.begin(), si.tiers.end(), bits,
                                 [](const WidthTier& t, unsigned b) { return t.width_bits < b; });
    if (tier == si.tiers.end() || tier->width_bits != bits) {
        tier = si.tiers.insert(tier, WidthTier{bits, {}, 0});
    }
    if (width > tier->widest_ns) tier->widest_ns = width;
    // Almost always the end: a tick's segment starts after the ones before it. Out-of-order rows
    // (#166) can start one earlier, and then it goes where the order says.
    auto& v = tier->segments;
    if (v.empty() || !segment_order_less(meta, v.back())) {
        v.push_back(std::move(meta));
    } else {
        v.insert(std::upper_bound(v.begin(), v.end(), meta, segment_order_less), std::move(meta));
    }
    ++indexed_count_;
    return true;
}

std::vector<SegmentMeta> ColumnarStore::index() const {
    std::vector<SegmentMeta> all;
    {
        std::shared_lock<std::shared_mutex> lock(index_mtx_);
        all.reserve(indexed_count_);
        for (const auto& [key, si] : by_symbol_) {
            for (const auto& tier : si.tiers) {
                all.insert(all.end(), tier.segments.begin(), tier.segments.end());
            }
        }
    }
    std::sort(all.begin(), all.end(), segment_order_less);
    return all;
}

// ── flush_segment ─────────────────────────────────────────────────────────────

std::optional<SegmentMeta> ColumnarStore::flush_segment() {
    if (!has_active_segment_ || active_row_count_ == 0) {
        has_active_segment_ = false;
        return std::nullopt;
    }

    // The range this segment records is its rows' - the earliest and the latest timestamp - and
    // not the period it belongs to and its last row, which is what it was until #166. The two
    // agree only while rows arrive in time order; when they did not, a query skipped rows this
    // segment held (measured: `OK` and nothing, for a row a `SELECT` of everything returned) and
    // retention deleted a row one second old with a two-day-old one written after it.
    const uint64_t first_ts = active_min_ts_;
    const uint64_t last_ts  = active_max_ts_;

    // Build segment directory path. Unique per segment rather than per span, so a second flush
    // covering the same event-time range is a second segment instead of a collision (#136).
    std::string dir = create_unique_segment_dir(symbol_, exchange_, first_ts, last_ts);

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
                 "Segment written: dir=%s rows=%zu range=[%llu, %llu] last=%llu",
                 dir.c_str(), static_cast<size_t>(active_row_count_),
                 static_cast<unsigned long long>(first_ts),
                 static_cast<unsigned long long>(last_ts),
                 static_cast<unsigned long long>(ts_buf_.back()));

    // Build SegmentMeta
    SegmentMeta meta{};
    meta.start_ts_ns        = first_ts;
    meta.end_ts_ns          = last_ts;
    meta.last_row_ts_ns     = ts_buf_.back();
    meta.time_range_is_rows = true;
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
    meta.seal_epoch      = seal_epoch_;

    // Write meta.json - last, so that a segment with one is a segment with all of its columns.
    write_meta_json(dir, meta);
    partial.complete = true;

    // A store on its own is its own index; one whose segments are handed to a combined store keeps
    // none (#165), because nothing would read them here.
    if (own_index_ == OwnIndex::kYes) {
        std::unique_lock<std::shared_mutex> lock(index_mtx_);
        insert_locked(meta);
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

ColumnarStore::ScanCost ColumnarStore::scan(uint64_t start_ns, uint64_t end_ns,
                                             std::string_view symbol, std::string_view exchange,
                                             ColumnSet columns,
                                             std::function<void(const SnapshotRow&)> cb) const {
    // Whatever the caller asked to be handed, this filters on the row's timestamp, so it reads
    // that column. Adding it here rather than trusting the caller means a set built by hand
    // cannot produce a scan that compares every row against a zero it never loaded.
    columns.add(QueryColumn::TimestampNs);

    // The segments that can hold a row of this range, copied under the shared lock so the files
    // are read without it. Only this symbol's, and only the window each width tier's sorted starts
    // allow (#165): this used to copy every segment of every symbol - three strings each - and
    // filter the copy.
    ScanCost cost;
    std::vector<SegmentMeta> index_snapshot;
    std::vector<std::shared_ptr<const RowBlock>> block_snapshot;
    {
        std::shared_lock<std::shared_mutex> lock(index_mtx_);
        const auto it = by_symbol_.find(index_key(symbol, exchange));
        if (it == by_symbol_.end()) return cost;
        for (const auto& b : it->second.blocks) {
            if (b->min_ts_ns <= end_ns && b->max_ts_ns >= start_ns) block_snapshot.push_back(b);
        }
        for (const WidthTier& tier : it->second.tiers) {
            const auto& v = tier.segments;
            const size_t before = index_snapshot.size();
            const uint64_t from = start_ns > tier.widest_ns ? start_ns - tier.widest_ns : 0;
            auto first = std::lower_bound(
                v.begin(), v.end(), from,
                [](const SegmentMeta& m, uint64_t at) { return m.start_ts_ns < at; });
            for (auto i = first; i != v.end() && i->start_ts_ns <= end_ns; ++i) {
                ++cost.compared;
                if (i->end_ts_ns >= start_ns) index_snapshot.push_back(*i);
            }
            // In the order the flat index gave them, which is the order rows reach the callback:
            // each tier's are sorted, so this merges two runs rather than sorting every candidate.
            if (before != 0 && before != index_snapshot.size()) {
                std::inplace_merge(index_snapshot.begin(),
                                   index_snapshot.begin() + static_cast<std::ptrdiff_t>(before),
                                   index_snapshot.end(), segment_order_less);
            }
        }
    }
    cost.candidates = index_snapshot.size();
    cost.blocks = block_snapshot.size();
    OB_LOG_DEBUG("columnar", "scan of %.*s.%.*s [%llu, %llu]: %zu segment(s) compared, %zu read, "
                             "%zu unsealed block(s)",
                 static_cast<int>(symbol.size()), symbol.data(),
                 static_cast<int>(exchange.size()), exchange.data(),
                 static_cast<unsigned long long>(start_ns),
                 static_cast<unsigned long long>(end_ns), cost.compared, cost.candidates,
                 cost.blocks);

    const bool want_price = columns.has(QueryColumn::Price);
    const bool want_qty   = columns.has(QueryColumn::Quantity);
    const bool want_cnt   = columns.has(QueryColumn::OrderCount);
    const bool want_side  = columns.has(QueryColumn::Side);
    const bool want_level = columns.has(QueryColumn::Level);
    const bool want_seq   = columns.has(QueryColumn::SequenceNumber);

    for (const auto& meta : index_snapshot) {
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
        bool removed = false;
        auto need = [&](bool wanted, const char* file, auto& dest) {
            if (!wanted || removed) return;
            if (!read_column_file(dir, file, dest)) {
                // Retention takes a segment out of the index and then deletes its directory
                // without the lock (#165), so a scan that copied it first can find it going.
                // Its rows are past the retention, and leaving them out is the right answer.
                if (!still_indexed(dir)) {
                    OB_LOG_DEBUG("columnar", "segment %s was removed while this query read it; "
                                             "its rows are past the retention", dir.c_str());
                    removed = true;
                    return;
                }
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
        if (missing || removed) continue;

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

    // Then the rows no seal has written yet, after every segment and in the order they were
    // drained: they are this symbol's newest writes, so a reader that keeps the last row of a tie
    // keeps the one written later (#168). Field by field as a segment's rows are, so a column the
    // caller did not ask for is zero here too rather than a value it would read by accident.
    for (const auto& block : block_snapshot) {
        for (const SnapshotRow& r : block->rows) {
            if (r.timestamp_ns < start_ns || r.timestamp_ns > end_ns) continue;
            SnapshotRow row{};
            row.timestamp_ns = r.timestamp_ns;
            if (want_seq)   row.sequence_number = r.sequence_number;
            if (want_side)  row.side            = r.side;
            if (want_level) row.level_index     = r.level_index;
            if (want_price) row.price           = r.price;
            if (want_qty)   row.quantity        = r.quantity;
            if (want_cnt)   row.order_count     = r.order_count;
            cb(row);
        }
    }
    return cost;
}

// ── open_existing ─────────────────────────────────────────────────────────────

void ColumnarStore::open_existing() {
    std::unique_lock<std::shared_mutex> lock(index_mtx_);
    rebuild_index_locked();
}

void ColumnarStore::rebuild_index_locked() {
    by_symbol_.clear();
    indexed_dirs_.clear();
    indexed_count_ = 0;
    unsealed_rows_ = 0;
    last_rebuild_ranges_read_ = 0;

    if (!fs::exists(base_dir_)) return;

    // Recursively scan for meta.json files, and for what a range repair that did not finish left
    // beside them. Those are always safe to delete: the repair changes a meta.json only by renaming
    // one of them over it, so the meta.json beside it is the old one or the new one and whole.
    std::vector<fs::path> leftovers;
    std::vector<SegmentMeta> found;
    for (auto& entry : fs::recursive_directory_iterator(base_dir_)) {
        if (!entry.is_regular_file()) continue;
        if (entry.path().filename() == kRangeRepairFile) {
            leftovers.push_back(entry.path());
            continue;
        }
        if (entry.path().filename() != "meta.json") continue;

        SegmentMeta meta;
        if (!parse_meta_json(entry.path().string(), meta)) continue;

        meta.dir_path = entry.path().parent_path().string();
        found.push_back(std::move(meta));
    }
    for (const auto& leftover : leftovers) {
        std::error_code ec;
        fs::remove(leftover, ec);
        OB_LOG_DEBUG("columnar", "removed %s, left by a range repair that did not finish%s",
                     leftover.string().c_str(), ec ? " (and could not remove it)" : "");
    }

    repair_ranges_locked(found);

    // Sorted after the repair, because the repair moves the ranges the order is taken from - and
    // sorted once, before being handed out per symbol, so building the index is not a sorted
    // insertion per segment.
    std::sort(found.begin(), found.end(), segment_order_less);
    for (auto& meta : found) insert_locked(std::move(meta));
}

void ColumnarStore::repair_ranges_locked(std::vector<SegmentMeta>& found) {
    std::vector<SegmentMeta*> legacy;
    for (auto& meta : found) {
        if (!meta.time_range_is_rows) legacy.push_back(&meta);
    }
    if (legacy.empty()) return;

    const auto started = std::chrono::steady_clock::now();
    last_rebuild_ranges_read_ = legacy.size();

    size_t outside = 0;
    size_t unreadable = 0;
    std::string first_outside;
    std::vector<SegmentMeta*> corrected;
    corrected.reserve(legacy.size());
    for (SegmentMeta* meta : legacy) {
        std::vector<uint64_t> ts;
        if (!read_column_file(meta->dir_path, "ts.col", ts) || ts.empty() ||
            ts.size() != meta->row_count) {
            // scan() needs this column for every query, so it skips the segment whatever its
            // range says; there is nothing to correct and nothing a guess would improve.
            ++unreadable;
            OB_LOG_WARN("columnar",
                        "segment %s: its ts.col could not be read as %llu row(s) (%zu read), so "
                        "its recorded range stays [%llu, %llu]",
                        meta->dir_path.c_str(), static_cast<unsigned long long>(meta->row_count),
                        ts.size(), static_cast<unsigned long long>(meta->start_ts_ns),
                        static_cast<unsigned long long>(meta->end_ts_ns));
            continue;
        }
        const auto [lo, hi] = std::minmax_element(ts.begin(), ts.end());
        if (*lo < meta->start_ts_ns || *hi > meta->end_ts_ns) {
            // A row outside the range it recorded: a query asking for that row skipped this
            // segment, and retention judged it by a row that was not its newest.
            ++outside;
            if (first_outside.empty()) first_outside = meta->dir_path;
            OB_LOG_DEBUG("columnar", "segment %s: recorded [%llu, %llu], its rows span [%llu, %llu]",
                         meta->dir_path.c_str(),
                         static_cast<unsigned long long>(meta->start_ts_ns),
                         static_cast<unsigned long long>(meta->end_ts_ns),
                         static_cast<unsigned long long>(*lo),
                         static_cast<unsigned long long>(*hi));
        }
        // In the index always, whatever happens on disk below. `last_row_ts_ns` stays what the
        // file said, which for this format is its recorded end.
        meta->start_ts_ns        = *lo;
        meta->end_ts_ns          = *hi;
        meta->time_range_is_rows = true;
        corrected.push_back(meta);
    }

    // On disk, in batches: every corrected meta.json of a batch is written beside the old one, one
    // syncfs() takes them all to the device, and only then is each renamed over its meta.json. A
    // file each through write_file_atomically() would be the same guarantee at an fsync of the file
    // and one of its directory per segment - and this touches every segment written before #166.
    // The syncfs() of each batch also makes the previous batch's renames durable; the last batch's
    // get one more.
    size_t persisted = 0;
    size_t not_persisted = 0;
    const int dir_fd = ::open(base_dir_.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    constexpr size_t kBatch = 4096;
    for (size_t first = 0; first < corrected.size(); first += kBatch) {
        const size_t last = std::min(corrected.size(), first + kBatch);
        std::vector<SegmentMeta*> written;
        written.reserve(last - first);
        for (size_t i = first; i < last; ++i) {
            SegmentMeta* meta = corrected[i];
            const std::string content = meta_json(*meta);
            try {
                // Through write_file_checked(), whose open is marked for the flush's syncfs(): here
                // the syncfs() is the one below, before the rename publishes the file.
                write_file_checked(meta->dir_path + "/" + kRangeRepairFile, content.data(),
                                   content.size());
                written.push_back(meta);
            } catch (const std::exception& e) {
                ++not_persisted;
                OB_LOG_DEBUG("columnar", "range repair of %s stays in memory: %s",
                             meta->dir_path.c_str(), e.what());
            }
        }
        if (written.empty()) continue;
        const int sync_err = dir_fd < 0 ? EBADF : (::syncfs(dir_fd) == 0 ? 0 : errno);
        if (sync_err != 0) {
            for (SegmentMeta* meta : written) {
                std::error_code ec;
                fs::remove(meta->dir_path + "/" + kRangeRepairFile, ec);
            }
            not_persisted += written.size();
            OB_LOG_WARN("columnar",
                        "the range repair could not sync %zu corrected meta.json file(s) (%s): they "
                        "are corrected in memory and will be repaired again at the next start",
                        written.size(), std::strerror(sync_err));
            continue;
        }
        for (SegmentMeta* meta : written) {
            const std::string tmp = meta->dir_path + "/" + kRangeRepairFile;
            const std::string dst = meta->dir_path + "/meta.json";
            if (::rename(tmp.c_str(), dst.c_str()) == 0) {
                ++persisted;
            } else {
                const int err = errno;
                std::error_code ec;
                fs::remove(tmp, ec);
                ++not_persisted;
                OB_LOG_DEBUG("columnar", "range repair of %s stays in memory: rename: %s",
                             meta->dir_path.c_str(), std::strerror(err));
            }
        }
    }
    if (persisted > 0 && dir_fd >= 0 && ::syncfs(dir_fd) != 0) {
        // The renames may not survive a power cut. Nothing is lost if they do not: each segment
        // then has its old meta.json, and the next start repairs it again.
        OB_LOG_WARN("columnar", "the range repair's last sync failed (%s): %zu repaired meta.json "
                                "file(s) may be repaired again at the next start",
                    std::strerror(errno), persisted);
    }
    if (dir_fd >= 0) ::close(dir_fd);

    const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - started).count();
    const std::string first_note =
        first_outside.empty() ? std::string() : " (the first: " + first_outside + ")";
    OB_LOG_INFO("columnar",
                "%zu segment(s) written before #166 were read for their time range in %lld ms: %zu "
                "held rows outside the range they recorded%s, %zu repaired on disk, %zu only in "
                "memory, %zu unreadable",
                legacy.size(), static_cast<long long>(ms), outside, first_note.c_str(),
                persisted, not_persisted, unreadable);
}

bool ColumnarStore::replace_from_staging(const std::string& staging_dir,
                                         const std::vector<std::string>& relative_paths) {
    std::unique_lock<std::shared_mutex> lock(index_mtx_);

    // The index goes first, so that nothing below is describing directories it names. A scan
    // waits on this same mutex and then reads the store this function leaves behind.
    by_symbol_.clear();
    indexed_dirs_.clear();
    indexed_count_ = 0;
    unsealed_rows_ = 0;
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
                removed, installed, indexed_count_);
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
    for (const auto& meta : new_segments) {
        // Defence in depth, not the fix. Since #136 a directory belongs to one segment rather
        // than to one event-time span, so two flushes covering the same span get two
        // directories and cannot reach here; what remains is the same meta merged twice, which
        // is a caller mistake. The message below used to name a cause — "two flush paths raced"
        // — that stopped being the only way here the moment #105 let a client choose its own
        // event time, and it sent a reader hunting a race that had not happened. It reports
        // the state now and leaves the cause to whoever has the rest of the log.
        //
        // A set lookup and an insertion into this symbol's segments (#165), where it was a
        // comparison with every segment of every symbol and then a sort of all of them - under the
        // engine's lock, once a tick.
        if (!insert_locked(meta)) {
            OB_LOG_ERROR("columnar",
                         "Refusing to merge a segment already in the index: dir=%s rows=%llu. "
                         "Merging it would make every client read those rows twice",
                         meta.dir_path.c_str(),
                         static_cast<unsigned long long>(meta.row_count));
            ++refused;
        }
    }
    return refused;
}

// ── delete_expired_segments ───────────────────────────────────────────────────

std::pair<size_t, size_t> ColumnarStore::delete_expired_segments(uint64_t cutoff_ns) {
    if (cutoff_ns == 0) {
        return {0, 0};
    }

    // Decided and taken out of the index under the lock, measured and deleted without it (#165).
    // The sweep used to hold the index's lock - which every query takes - through the size walk
    // and the remove_all() of every expired directory. A query that copied a segment before it was
    // taken out may still find it being deleted; scan() tells that from a missing file by asking
    // whether the segment is still indexed.
    std::vector<SegmentMeta> expired;
    {
        std::unique_lock<std::shared_mutex> lock(index_mtx_);
        for (auto it = by_symbol_.begin(); it != by_symbol_.end();) {
            auto& tiers = it->second.tiers;
            for (auto tier = tiers.begin(); tier != tiers.end();) {
                auto& v = tier->segments;
                // A segment that ends before the cutoff starts before it too, so every expired one
                // is in the prefix of segments starting before the cutoff.
                const auto limit = std::lower_bound(
                    v.begin(), v.end(), cutoff_ns,
                    [](const SegmentMeta& m, uint64_t at) { return m.start_ts_ns < at; });
                // Kept first and in order, expired after them and in order.
                const auto kept_end = std::stable_partition(
                    v.begin(), limit,
                    [&](const SegmentMeta& m) { return m.end_ts_ns >= cutoff_ns; });
                for (auto i = kept_end; i != limit; ++i) {
                    indexed_dirs_.erase(i->dir_path);
                    expired.push_back(std::move(*i));
                }
                indexed_count_ -= static_cast<size_t>(limit - kept_end);
                v.erase(kept_end, limit);
                tier = v.empty() ? tiers.erase(tier) : std::next(tier);
            }
            it = (tiers.empty() && it->second.blocks.empty()) ? by_symbol_.erase(it) : std::next(it);
        }
    }
    // Oldest first, as this has always deleted.
    std::sort(expired.begin(), expired.end(), segment_order_less);

    size_t segments_deleted = 0;
    size_t bytes_reclaimed = 0;
    for (auto& meta : expired) {
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
            OB_LOG_ERROR("retention", "failed to delete segment %s: %s - it stays in the index",
                meta.dir_path.c_str(), rm_ec.message().c_str());
            // Back in the index, since deletion failed: the next sweep tries again.
            std::unique_lock<std::shared_mutex> lock(index_mtx_);
            insert_locked(std::move(meta));
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
    }

    return {segments_deleted, bytes_reclaimed};
}

// ── remove_segments ───────────────────────────────────────────────────────────

size_t ColumnarStore::remove_segments(const std::vector<std::string>& dirs) {
    if (dirs.empty()) return 0;
    std::unique_lock<std::shared_mutex> lock(index_mtx_);
    // A set, not a search of the list per segment: at a start after a crash the list is every
    // segment the last flush wrote, one per active symbol.
    const std::unordered_set<std::string> wanted(dirs.begin(), dirs.end());
    size_t removed = 0;
    for (auto it = by_symbol_.begin(); it != by_symbol_.end();) {
        auto& tiers = it->second.tiers;
        for (auto tier = tiers.begin(); tier != tiers.end();) {
            auto& v = tier->segments;
            std::vector<SegmentMeta> remaining;
            remaining.reserve(v.size());
            for (auto& meta : v) {
                if (wanted.count(meta.dir_path) == 0) {
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
                indexed_dirs_.erase(meta.dir_path);
                --indexed_count_;
                ++removed;
            }
            v = std::move(remaining);
            tier = v.empty() ? tiers.erase(tier) : std::next(tier);
        }
        it = (tiers.empty() && it->second.blocks.empty()) ? by_symbol_.erase(it) : std::next(it);
    }
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
