#include "orderbook/columnar_store.hpp"
#include "orderbook/codec.hpp"
#include "orderbook/column_codec.hpp"
#include "orderbook/crc32c.hpp"
#include "orderbook/logger.hpp"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <map>
#include <atomic>
#include <bit>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <limits>
#include <filesystem>
#include <fstream>
#include <memory>
#include <mutex>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <type_traits>

namespace ob {

namespace fs = std::filesystem;

// ── Constructor ───────────────────────────────────────────────────────────────

ColumnarStore::ColumnarStore(std::string_view base_dir, uint64_t segment_duration_ns,
                             OwnIndex own_index)
    : base_dir_(base_dir)
    , segment_duration_ns_(segment_duration_ns)
    , own_index_(own_index)
{}

// ── Reader generations (#165 part 2b) ─────────────────────────────────────────

namespace detail {

namespace {
// While a release on this thread lets go of a chain: the one link it still has to let go of. A
// link's release hands over at most its own successor, and the slot is empty whenever a link is
// let go of, so one slot is the whole queue. It is the `next` of the generation whose destructor
// drains the chain - memory that lives until that destructor returns, and nothing to allocate.
thread_local std::shared_ptr<ReaderGeneration>* releasing = nullptr;
}  // namespace

ReaderGeneration::~ReaderGeneration() {
    if (!next) return;
    if (releasing != nullptr) {
        *releasing = std::move(next);
        return;
    }
    releasing = &next;
    while (next) {
        std::shared_ptr<ReaderGeneration> link = std::move(next);
        // The last reference, or not: if it is, its destructor hands its successor to the slot.
        link.reset();
    }
    releasing = nullptr;
}

}  // namespace detail

// ── A merge's inputs, as its segment names them (#165 part 2b) ───────────────

SegmentInput SegmentInput::of(const SegmentMeta& meta) {
    SegmentInput in;
    in.dir_name        = fs::path(meta.dir_path).filename().string();
    in.wal_identity    = meta.wal_identity;
    in.wal_file_index  = meta.wal_file_index;
    in.wal_byte_offset = meta.wal_byte_offset;
    in.seal_epoch      = meta.seal_epoch;
    in.row_count       = meta.row_count;
    in.start_ts_ns     = meta.start_ts_ns;
    in.end_ts_ns       = meta.end_ts_ns;
    return in;
}

bool SegmentInput::names(const SegmentMeta& meta) const {
    return fs::path(meta.dir_path).filename().string() == dir_name &&
           meta.wal_identity == wal_identity && meta.wal_file_index == wal_file_index &&
           meta.wal_byte_offset == wal_byte_offset && meta.seal_epoch == seal_epoch &&
           meta.row_count == row_count && meta.start_ts_ns == start_ts_ns &&
           meta.end_ts_ns == end_ts_ns;
}

// ── Blocks: drained rows before a seal (#165 part 2a) ────────────────────────

std::shared_ptr<const RowBlock> RowBlock::make(std::string symbol, std::string exchange,
                                               std::vector<SnapshotRow> rows) {
    if (rows.empty()) return nullptr;
    uint64_t min_ts = rows.front().timestamp_ns;
    uint64_t max_ts = rows.front().timestamp_ns;
    uint64_t max_seq = 0;
    for (const auto& r : rows) {
        min_ts = std::min(min_ts, r.timestamp_ns);
        max_ts = std::max(max_ts, r.timestamp_ns);
        max_seq = std::max(max_seq, r.sequence_number);
    }
    // Rows whose owner nobody states count as own, as a row appended alone does: a counter
    // continued from too low a number would hand a number out twice, and a peer drops the second
    // record as the first.
    return make(std::move(symbol), std::move(exchange), std::move(rows), min_ts, max_ts, nullptr,
                max_seq, /*all_own=*/true);
}

std::shared_ptr<const RowBlock> RowBlock::make(std::string symbol, std::string exchange,
                                               std::vector<SnapshotRow> rows,
                                               uint64_t min_ts_ns, uint64_t max_ts_ns,
                                               std::shared_ptr<RowBufferPool> pool,
                                               uint64_t max_own_seq, bool all_own) {
    if (rows.empty()) {
        if (pool) pool->give_back(std::move(rows));
        return nullptr;
    }
    auto block = std::make_unique<RowBlock>();
    block->symbol      = std::move(symbol);
    block->exchange    = std::move(exchange);
    block->min_ts_ns   = min_ts_ns;
    block->max_ts_ns   = max_ts_ns;
    block->max_own_seq = max_own_seq;
    block->all_own     = all_own;
    block->rows      = std::move(rows);
    if (!pool) return std::shared_ptr<const RowBlock>(std::move(block));
    // The deleter runs wherever the last reference goes - a seal, a snapshot install, or a query
    // that copied the blocks before the seal took them out of the index.
    return std::shared_ptr<const RowBlock>(block.release(), [pool = std::move(pool)](const RowBlock* b) {
        std::unique_ptr<RowBlock> gone(const_cast<RowBlock*>(b));
        pool->give_back(std::move(gone->rows));
    });
}

std::vector<SnapshotRow> RowBufferPool::take(size_t rows) {
    std::vector<SnapshotRow> out;
    if (rows == 0) return out;
    {
        std::lock_guard<std::mutex> lock(mtx_);
        size_t best = spare_.size();
        for (size_t i = 0; i < spare_.size(); ++i) {
            const size_t cap = spare_[i].capacity();
            if (cap < rows || cap > 2 * rows + 4096) continue;
            if (best == spare_.size() || cap < spare_[best].capacity()) best = i;
        }
        if (best != spare_.size()) {
            std::swap(spare_[best], spare_.back());
            out = std::move(spare_.back());
            spare_.pop_back();
            spare_rows_ -= std::min(spare_rows_, out.capacity());
            return out;
        }
    }
    out.reserve(rows);
    return out;
}

void RowBufferPool::give_back(std::vector<SnapshotRow>&& rows) noexcept {
    rows.clear();
    const size_t cap = rows.capacity();
    if (cap == 0) return;
    try {
        std::lock_guard<std::mutex> lock(mtx_);
        if (spare_rows_ + cap > max_spare_rows_) return;
        spare_.push_back(std::move(rows));
        spare_rows_ += cap;
    } catch (...) {
        // A spare that cannot be kept is freed with `rows`; only the reuse is lost.
    }
}

size_t RowBufferPool::spare_rows() const {
    std::lock_guard<std::mutex> lock(mtx_);
    return spare_rows_;
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
    active_own_max_     = 0;
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

size_t LevelSet::count(const Bits& bits) {
    size_t n = 0;
    for (const uint64_t w : bits) n += static_cast<size_t>(__builtin_popcountll(w));
    return n;
}

std::shared_ptr<const LevelSet> LevelSet::from_columns(const std::vector<uint8_t>& sides,
                                                       const std::vector<uint16_t>& levels) {
    auto set = std::make_shared<LevelSet>();
    const size_t n = std::min(sides.size(), levels.size());
    for (size_t i = 0; i < n; ++i) {
        const uint16_t level = levels[i];
        if (level >= kLevels) return nullptr;
        Bits* bits = sides[i] == SIDE_BID ? &set->bid : sides[i] == SIDE_ASK ? &set->ask : nullptr;
        if (bits == nullptr) return nullptr;
        (*bits)[level / 64] |= uint64_t{1} << (level % 64);
    }
    return set;
}

std::string LevelSet::to_hex(const Bits& bits, bool trimmed) {
    static constexpr char kDigits[] = "0123456789abcdef";
    std::string out(kLevels / 4, '0');
    for (size_t d = 0; d < kLevels / 4; ++d) {
        const unsigned nibble = static_cast<unsigned>((bits[(4 * d) / 64] >> ((4 * d) % 64)) & 0xF);
        out[kLevels / 4 - 1 - d] = kDigits[nibble];
    }
    if (trimmed) {
        const size_t first = out.find_first_not_of('0');
        out.erase(0, first == std::string::npos ? out.size() - 1 : first);
    }
    return out;
}

bool LevelSet::from_hex(const std::string& hex, Bits& bits) {
    static_assert(kLevels % 4 == 0, "a digit is four levels");
    // Every digit, or - as segment format 3 writes it - without the leading zeros: a shorter string
    // that begins with a zero is neither, which is what a string cut short of a digit mostly is.
    if (hex.empty() || hex.size() > kLevels / 4) return false;
    if (hex.size() < kLevels / 4 && hex.size() > 1 && hex[0] == '0') return false;
    bits.fill(0);
    for (size_t d = 0; d < hex.size(); ++d) {
        const char c = hex[hex.size() - 1 - d];
        uint64_t nibble = 0;
        if (c >= '0' && c <= '9') {
            nibble = static_cast<uint64_t>(c - '0');
        } else if (c >= 'a' && c <= 'f') {
            nibble = static_cast<uint64_t>(c - 'a' + 10);
        } else {
            return false;
        }
        bits[(4 * d) / 64] |= nibble << ((4 * d) % 64);
    }
    return true;
}

void ColumnarStore::write_meta_json(const std::string& dir, const SegmentMeta& meta,
                                    const std::vector<SegmentInput>* inputs) const {
    const std::string content = meta_json(meta, inputs);
    write_file_checked(dir + "/meta.json", content.data(), content.size());
}

std::string ColumnarStore::meta_json(const SegmentMeta& meta,
                                     const std::vector<SegmentInput>* inputs) const {
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
    // A merge's (#165 part 2b), last, and in keys no reader of the ones above searches for: the
    // parser finds a key by its first occurrence, and an older build parses this file too.
    if (meta.merge_level > 0) f << ",\"merge_level\":" << meta.merge_level;
    // #184's, likewise last and in keys no older reader searches for. Absent means unknown - a
    // segment written before them, or a merge of one - and a start then does what it always did.
    if (meta.has_own_max) {
        f << ",\"own_origin\":" << meta.own_origin
          << ",\"own_max_sequence\":" << meta.own_max_sequence;
        if (meta.has_received_rows) f << ",\"received_rows\":true";
    }
    // #47's, in keys no older reader searches for either; absent is unknown, and a reader then
    // reads the segment whatever the book already holds.
    if (meta.levels != nullptr) {
        // Without the leading zeros in format 3: all 250 digits a side were over half of a
        // meta.json, and a segment of a few thousand rows paid a tenth of a byte a row for them. A
        // format-2 segment's meta.json keeps every digit, since a build before format 3 reads it.
        const bool trimmed = meta.format_version >= kColumnarFormatV3;
        f << ",\"bid_levels\":\"" << LevelSet::to_hex(meta.levels->bid, trimmed) << "\""
          << ",\"ask_levels\":\"" << LevelSet::to_hex(meta.levels->ask, trimmed) << "\"";
    }
    // #47 step 2's, likewise: absent is unknown, and a scan with a price condition reads it.
    if (meta.has_price_range) {
        f << ",\"min_price\":" << meta.min_price << ",\"max_price\":" << meta.max_price;
    }
    if (inputs != nullptr && !inputs->empty()) {
        f << ",\"compacted_from\":[";
        for (size_t i = 0; i < inputs->size(); ++i) {
            const SegmentInput& in = (*inputs)[i];
            f << (i == 0 ? "" : ",")
              << "{\"dir\":\"" << in.dir_name << "\""
              << ",\"identity\":" << in.wal_identity
              << ",\"file\":" << in.wal_file_index
              << ",\"offset\":" << in.wal_byte_offset
              << ",\"epoch\":" << in.seal_epoch
              << ",\"rows\":" << in.row_count
              << ",\"from\":" << in.start_ts_ns
              << ",\"to\":" << in.end_ts_ns << "}";
        }
        f << "]";
    }
    f << "}";
    return f.str();
}

bool ColumnarStore::parse_meta_json(const std::string& path, SegmentMeta& out,
                                    std::vector<SegmentInput>* inputs) const {
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

    // A signed one, which a price is: nothing when absent or not a number, so that a key cut short
    // reads as unknown rather than as zero (#47 step 2).
    auto find_int64 = [&](const std::string& key) -> std::optional<int64_t> {
        const std::string search = "\"" + key + "\":";
        auto pos = content.find(search);
        if (pos == std::string::npos) return std::nullopt;
        pos += search.size();
        const bool negative = pos < content.size() && content[pos] == '-';
        if (negative) ++pos;
        uint64_t magnitude = 0;
        size_t digits = 0;
        while (pos < content.size() && content[pos] >= '0' && content[pos] <= '9') {
            const uint64_t d = static_cast<uint64_t>(content[pos] - '0');
            if (magnitude > (UINT64_MAX - d) / 10) return std::nullopt;
            magnitude = magnitude * 10 + d;
            ++pos;
            ++digits;
        }
        const uint64_t limit = negative ? static_cast<uint64_t>(INT64_MAX) + 1 : static_cast<uint64_t>(INT64_MAX);
        if (digits == 0 || magnitude > limit) return std::nullopt;
        return negative ? static_cast<int64_t>(0 - magnitude) : static_cast<int64_t>(magnitude);
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
    // Absent before #184, and then unknown: a start falls back to max_sequence_number.
    const auto own_origin = find_uint64("own_origin");
    const auto own_max    = find_uint64("own_max_sequence");
    out.has_own_max      = own_origin.has_value() && own_max.has_value();
    out.own_origin       = static_cast<uint16_t>(own_origin.value_or(0));
    out.own_max_sequence = own_max.value_or(0);
    out.has_received_rows = out.has_own_max && extract_bool("received_rows");
    out.symbol       = extract_string("symbol");
    out.exchange     = extract_string("exchange");
    // Both absent before #166. Then the recorded end WAS the last row's time - the number replay's
    // fallback has always compared with - so it is carried over as that, before any repair moves
    // the end to the rows' maximum.
    out.time_range_is_rows = extract_string("time_range") == "rows";
    out.last_row_ts_ns     = find_uint64("last_row_ts_ns").value_or(out.end_ts_ns);
    // Absent unless a merge wrote the segment (#165 part 2b).
    out.merge_level = static_cast<uint32_t>(extract_uint64("merge_level"));
    // Absent before #47, and then unknown: the book at an instant reads such a segment always. A
    // pair that does not parse is unknown too, never an empty set - an empty set would skip it.
    {
        auto set = std::make_shared<LevelSet>();
        if (LevelSet::from_hex(extract_string("bid_levels"), set->bid) &&
            LevelSet::from_hex(extract_string("ask_levels"), set->ask)) {
            out.levels = std::move(set);
        } else {
            out.levels = nullptr;
        }
    }
    // Absent before #47 step 2, and then unknown: a scan with a price condition reads the segment.
    // A range that does not parse, or whose ends are the wrong way round, is unknown too - never a
    // range that would leave the segment unread.
    {
        const auto lo = find_int64("min_price");
        const auto hi = find_int64("max_price");
        out.has_price_range = lo.has_value() && hi.has_value() && *lo <= *hi;
        out.min_price = out.has_price_range ? *lo : 0;
        out.max_price = out.has_price_range ? *hi : 0;
    }
    if (inputs != nullptr) {
        inputs->clear();
        const std::string list = "\"compacted_from\":[";
        auto pos = content.find(list);
        if (pos != std::string::npos) pos += list.size();
        while (pos != std::string::npos && pos < content.size() && content[pos] == '{') {
            const auto close = content.find('}', pos);
            if (close == std::string::npos) break;
            const std::string_view obj(content.data() + pos, close - pos + 1);
            auto number = [&](std::string_view key) -> uint64_t {
                const std::string search = "\"" + std::string(key) + "\":";
                auto at = obj.find(search);
                if (at == std::string_view::npos) return 0;
                at += search.size();
                uint64_t val = 0;
                while (at < obj.size() && obj[at] >= '0' && obj[at] <= '9') {
                    val = val * 10 + static_cast<uint64_t>(obj[at] - '0');
                    ++at;
                }
                return val;
            };
            SegmentInput in;
            const std::string dir_key = "\"dir\":\"";
            if (auto at = obj.find(dir_key); at != std::string_view::npos) {
                at += dir_key.size();
                const auto end = obj.find('"', at);
                if (end != std::string_view::npos) in.dir_name = std::string(obj.substr(at, end - at));
            }
            in.wal_identity    = number("identity");
            in.wal_file_index  = static_cast<uint32_t>(number("file"));
            in.wal_byte_offset = number("offset");
            in.seal_epoch      = number("epoch");
            in.row_count       = number("rows");
            in.start_ts_ns     = number("from");
            in.end_ts_ns       = number("to");
            if (!in.dir_name.empty()) inputs->push_back(std::move(in));
            pos = close + 1;
            if (pos < content.size() && content[pos] == ',') ++pos;
        }
    }

    // Validate: must have at least start_ts_ns and row_count
    return out.row_count > 0 || out.start_ts_ns > 0;
}

// ── append ────────────────────────────────────────────────────────────────────

void ColumnarStore::append(const SnapshotRow& row) {
    // A row appended alone counts as this store's own (#184) - the C API's, which has no mesh, and a
    // merge's, whose answer set_own_max() gives instead. Counted as someone else's, a start would
    // continue the counter from too low a number and hand one out twice.
    append_row(row, row.sequence_number);
}

void ColumnarStore::append_row(const SnapshotRow& row, uint64_t own_seq) {
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

    // Whether qty takes the codec's raw fallback: at or past 2^60 - 1, the value the marker spells
    // (#198).
    static constexpr uint64_t kFallbackMarker = (1ULL << 60) - 1;
    if (row.quantity >= kFallbackMarker) {
        active_has_raw_qty_ = true;
    }
    // After any rollover above, so the number is the segment's the row is in.
    active_own_max_ = std::max(active_own_max_, own_seq);
}

void ColumnarStore::append_block(const RowBlock& block) {
    const std::vector<SnapshotRow>& rows = block.rows;
    if (rows.empty()) return;
    // The first row opens the segment, or rolls it over, as it would on its own - through the path
    // that counts a row as own, so the block's own highest is what counts instead, set on whichever
    // segment the rows end in (below). A block that rolls a segment over sets it on both: a start
    // takes the highest over all of a symbol's segments, so it is the same number either way.
    const auto own_seq_of = [&block](const SnapshotRow& r) -> uint64_t {
        return block.all_own ? r.sequence_number : 0;
    };
    append_row(rows.front(), own_seq_of(rows.front()));
    if (block.max_ts_ns >= active_segment_start_ + segment_duration_ns_) {
        // A block of mixed rows cannot say which of its own rows went where, so the segment its
        // first row is in and the one its last is in both get its own highest.
        if (!block.all_own) active_own_max_ = std::max(active_own_max_, block.max_own_seq);
        // A row of a later period is in the block and rolls the segment over where it stands, so
        // the rest goes row by row. Rare: a block is one drain of one symbol - 100 ms of it at the
        // default interval - and a period is an hour.
        OB_LOG_DEBUG("columnar", "a block of %zu row(s) for %s.%s reaches past its segment's period; "
                                 "appended row by row",
                     rows.size(), symbol_.c_str(), exchange_.c_str());
        for (size_t i = 1; i < rows.size(); ++i) append_row(rows[i], own_seq_of(rows[i]));
        if (!block.all_own) active_own_max_ = std::max(active_own_max_, block.max_own_seq);
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
    // Every row is in this segment, so the block's own highest is the segment's.
    active_own_max_ = std::max(active_own_max_, block.max_own_seq);
}

void ColumnarStore::swap_buffers(ColumnBuffers& other) {
    if (has_active_segment_) {
        throw std::logic_error("ColumnarStore: buffers swapped under an active segment of " +
                               symbol_ + "." + exchange_);
    }
    price_buf_.swap(other.price);
    qty_buf_.swap(other.qty);
    ts_buf_.swap(other.ts);
    cnt_buf_.swap(other.cnt);
    side_buf_.swap(other.side);
    level_buf_.swap(other.level);
    seq_buf_.swap(other.seq);
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
    // A slot of its own, whatever the meta carried: an entry is a segment as indexed now, and one
    // that comes back - retention returning a segment it could not remove - starts empty (#220).
    meta.decoded = decoded_budget_ ? std::make_shared<DecodedColumns>(decoded_budget_) : nullptr;
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

bool ColumnarStore::erase_locked(const SegmentMeta& meta) {
    const auto it = by_symbol_.find(index_key(meta.symbol, meta.exchange));
    if (it == by_symbol_.end()) return false;
    const uint64_t width =
        meta.end_ts_ns > meta.start_ts_ns ? meta.end_ts_ns - meta.start_ts_ns : 0;
    const auto bits = static_cast<unsigned>(std::bit_width(width));
    auto& tiers = it->second.tiers;
    const auto tier = std::lower_bound(tiers.begin(), tiers.end(), bits,
                                       [](const WidthTier& t, unsigned b) { return t.width_bits < b; });
    if (tier == tiers.end() || tier->width_bits != bits) return false;
    auto& v = tier->segments;
    const auto pos = std::lower_bound(v.begin(), v.end(), meta, segment_order_less);
    if (pos == v.end() || pos->dir_path != meta.dir_path) return false;
    indexed_dirs_.erase(pos->dir_path);
    v.erase(pos);
    --indexed_count_;
    if (v.empty()) tiers.erase(tier);
    return true;
}

ColumnarStore::PartitionView ColumnarStore::partition_view(std::string_view symbol,
                                                           std::string_view exchange,
                                                           uint64_t end_from,
                                                           uint64_t end_to) const {
    PartitionView view;
    std::shared_lock<std::shared_mutex> lock(index_mtx_);
    const auto it = by_symbol_.find(index_key(symbol, exchange));
    if (it == by_symbol_.end()) return view;
    const auto& tiers = it->second.tiers;
    auto by_start = [](const SegmentMeta& m, uint64_t at) { return m.start_ts_ns < at; };

    // The members: a segment that ends in the range starts no earlier than the range's start less
    // the widest segment of its tier, so each tier is searched in that window, as a scan's is.
    bool any = false;
    uint64_t lo = UINT64_MAX;
    uint64_t hi = 0;
    for (const WidthTier& tier : tiers) {
        const auto& v = tier.segments;
        const uint64_t from = end_from > tier.widest_ns ? end_from - tier.widest_ns : 0;
        for (auto i = std::lower_bound(v.begin(), v.end(), from, by_start);
             i != v.end() && i->start_ts_ns < end_to; ++i) {
            if (i->end_ts_ns < end_from || i->end_ts_ns >= end_to) continue;
            any = true;
            lo = std::min(lo, i->start_ts_ns);
            hi = std::max(hi, i->start_ts_ns);
        }
    }
    if (!any) return view;

    // And everything that starts where they do - the stretch of the delivery order they span,
    // which is where a segment of another partition can lie between two of them.
    for (const WidthTier& tier : tiers) {
        const auto& v = tier.segments;
        for (auto i = std::lower_bound(v.begin(), v.end(), lo, by_start);
             i != v.end() && i->start_ts_ns <= hi; ++i) {
            view.segments.push_back(*i);
        }
    }
    std::sort(view.segments.begin(), view.segments.end(), segment_order_less);
    view.member.reserve(view.segments.size());
    for (const SegmentMeta& m : view.segments) {
        view.member.push_back(m.end_ts_ns >= end_from && m.end_ts_ns < end_to);
    }
    return view;
}

ColumnarStore::ReplaceResult ColumnarStore::replace_segments(
        const std::vector<SegmentMeta>& inputs, SegmentMeta output,
        const std::function<bool(SegmentMeta&)>& publish) {
    ReplaceResult result;
    if (inputs.empty()) return result;
    // Kept past the lock, so the generation it names is not destroyed while the lock is held.
    std::shared_ptr<ReaderGeneration> before;
    const char* refused = nullptr;
    {
        std::unique_lock<std::shared_mutex> lock(index_mtx_);
        for (const SegmentMeta& in : inputs) {
            if (indexed_dirs_.count(in.dir_path) == 0) {
                result.outcome = Replaced::kInputGone;
                refused = "an input is no longer indexed";
                break;
            }
        }
        const auto it = refused ? by_symbol_.end()
                                : by_symbol_.find(index_key(output.symbol, output.exchange));
        if (!refused && it == by_symbol_.end()) {
            result.outcome = Replaced::kInputGone;
            refused = "the symbol is no longer indexed";
        }
        if (!refused) {
            // Every segment from the first input to the last, in delivery order, and the segments
            // just before and after them.
            std::vector<const SegmentMeta*> stretch;
            const SegmentMeta* prev = nullptr;
            const SegmentMeta* next = nullptr;
            for (const WidthTier& tier : it->second.tiers) {
                const auto& v = tier.segments;
                const auto first = std::lower_bound(v.begin(), v.end(), inputs.front(),
                                                    segment_order_less);
                const auto past = std::upper_bound(v.begin(), v.end(), inputs.back(),
                                                   segment_order_less);
                if (first != v.begin()) {
                    const SegmentMeta* p = &*std::prev(first);
                    if (prev == nullptr || segment_order_less(*prev, *p)) prev = p;
                }
                if (past != v.end()) {
                    const SegmentMeta* n = &*past;
                    if (next == nullptr || segment_order_less(*n, *next)) next = n;
                }
                for (auto i = first; i < past; ++i) stretch.push_back(&*i);
            }
            std::sort(stretch.begin(), stretch.end(),
                      [](const SegmentMeta* a, const SegmentMeta* b) {
                          return segment_order_less(*a, *b);
                      });
            bool consecutive = stretch.size() == inputs.size();
            for (size_t i = 0; consecutive && i < inputs.size(); ++i) {
                consecutive = stretch[i]->dir_path == inputs[i].dir_path;
            }
            // Strictly between its neighbours by range, so the order does not come down to the
            // directory's name, which the publication below chooses.
            auto range_less = [](const SegmentMeta& a, const SegmentMeta& b) {
                return a.start_ts_ns < b.start_ts_ns ||
                       (a.start_ts_ns == b.start_ts_ns && a.end_ts_ns < b.end_ts_ns);
            };
            if (!consecutive) {
                result.outcome = Replaced::kNotConsecutive;
                refused = "another segment lies between the inputs";
            } else if ((prev != nullptr && !range_less(*prev, output)) ||
                       (next != nullptr && !range_less(output, *next))) {
                result.outcome = Replaced::kWouldMove;
                refused = "the merged range would not sort where the inputs were";
            } else if (!publish(output)) {
                result.outcome = Replaced::kPublishFailed;
                refused = "the merged segment could not be published";
            } else if (indexed_dirs_.count(output.dir_path) != 0) {
                // A rename that does not replace cannot land on an indexed directory, which exists.
                result.outcome = Replaced::kPublishFailed;
                refused = "the merged segment was published over an indexed directory";
                OB_LOG_ERROR("columnar", "merge of %zu segment(s) of %s.%s was published at %s, which "
                                         "is already indexed; the inputs stay",
                             inputs.size(), output.symbol.c_str(), output.exchange.c_str(),
                             output.dir_path.c_str());
            } else {
                for (const SegmentMeta& in : inputs) erase_locked(in);
                insert_locked(output);
                auto fresh = std::make_shared<ReaderGeneration>();
                reader_generation_->next = fresh;
                before = std::move(reader_generation_);
                reader_generation_ = std::move(fresh);
                result.readers_before = before;
                result.outcome = Replaced::kYes;
            }
        }
    }
    if (refused != nullptr) {
        OB_LOG_DEBUG("columnar", "merge of %zu segment(s) of %s.%s into %s not published: %s",
                     inputs.size(), output.symbol.c_str(), output.exchange.c_str(),
                     output.dir_path.c_str(), refused);
    } else {
        OB_LOG_DEBUG("columnar", "published %s, the merge of %zu segment(s) of %s.%s, %llu row(s)",
                     output.dir_path.c_str(), inputs.size(), output.symbol.c_str(),
                     output.exchange.c_str(), static_cast<unsigned long long>(output.row_count));
    }
    return result;
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

bool ColumnarStore::is_dotted_key(const std::string& key, std::string_view dotted) {
    // symbol NUL exchange here, symbol '.' exchange there: the same length, and equal but for that.
    if (key.size() != dotted.size()) return false;
    const size_t nul = key.find('\0');
    if (nul == std::string::npos || dotted[nul] != '.') return false;
    const std::string_view k(key);
    return k.substr(0, nul) == dotted.substr(0, nul) && k.substr(nul + 1) == dotted.substr(nul + 1);
}

std::vector<SegmentMeta> ColumnarStore::segments_of(std::string_view dotted) const {
    std::vector<SegmentMeta> out;
    {
        std::shared_lock<std::shared_mutex> lock(index_mtx_);
        for (const auto& [key, si] : by_symbol_) {
            if (!is_dotted_key(key, dotted)) continue;
            for (const auto& tier : si.tiers) {
                out.insert(out.end(), tier.segments.begin(), tier.segments.end());
            }
        }
    }
    std::sort(out.begin(), out.end(), segment_order_less);
    return out;
}

bool ColumnarStore::holds_dotted(std::string_view dotted) const {
    std::shared_lock<std::shared_mutex> lock(index_mtx_);
    for (const auto& [key, si] : by_symbol_) {
        if (!is_dotted_key(key, dotted)) continue;
        if (!si.blocks.empty() || std::any_of(si.tiers.begin(), si.tiers.end(),
                                              [](const WidthTier& t) { return !t.segments.empty(); })) {
            return true;
        }
    }
    return false;
}

// ── flush_segment ─────────────────────────────────────────────────────────────

std::optional<SegmentMeta> ColumnarStore::flush_segment() {
    if (!has_active_segment_ || active_row_count_ == 0) {
        has_active_segment_ = false;
        return std::nullopt;
    }

    // Build segment directory path. Unique per segment rather than per span, so a second flush
    // covering the same event-time range is a second segment instead of a collision (#136).
    const std::string dir =
        create_unique_segment_dir(symbol_, exchange_, active_min_ts_, active_max_ts_);
    SegmentMeta meta = write_active_segment(dir);

    // A store on its own is its own index; one whose segments are handed to a combined store keeps
    // none (#165), because nothing would read them here.
    if (own_index_ == OwnIndex::kYes) {
        std::unique_lock<std::shared_mutex> lock(index_mtx_);
        insert_locked(meta);
    }
    return meta;
}

void ColumnarStore::set_lineage(uint32_t merge_level, std::vector<SegmentInput> inputs,
                                uint64_t last_row_ts_ns) {
    has_lineage_         = true;
    lineage_level_       = merge_level;
    lineage_inputs_      = std::move(inputs);
    lineage_last_row_ts_ = last_row_ts_ns;
}

std::optional<SegmentMeta> ColumnarStore::flush_segment_into(const std::string& dir) {
    if (!has_active_segment_ || active_row_count_ == 0) {
        has_active_segment_ = false;
        return std::nullopt;
    }
    ensure_dirs(fs::path(dir).parent_path().string());
    std::error_code ec;
    if (!fs::create_directory(dir, ec)) {
        throw std::runtime_error("ColumnarStore: cannot create '" + dir + "' for a merged segment: " +
                                 (ec ? ec.message() : std::string("it already exists")));
    }
    return write_active_segment(dir);
}

// columns.v3 (segment format 3); its layout is described with its reader below.
namespace {

constexpr char kColumnsMagic[4] = {'O', 'B', 'S', '3'};
constexpr size_t kColumns = 7;
constexpr const char* kColumnNames[kColumns] = {"ts", "price", "qty", "cnt", "side", "level", "seq"};
constexpr QueryColumn kColumnIds[kColumns] = {
    QueryColumn::TimestampNs, QueryColumn::Price, QueryColumn::Quantity, QueryColumn::OrderCount,
    QueryColumn::Side,        QueryColumn::Level, QueryColumn::SequenceNumber};
/// No directory is longer: the magic, a 64-bit varint, the count, and seven entries at their widest.
constexpr size_t kColumnsHeaderMax = 4 + 10 + 1 + kColumns * (1 + 10 + 4);
static_assert(kColumns == DecodedColumns::kSlots,
              "a segment's held columns are format 3's, slot for slot (#220)");

}  // namespace

void ColumnarStore::write_columns_v3(const std::string& dir) {
    const size_t n = ts_buf_.size();
    // Every column as uint64: the two signed ones reinterpreted, as column_codec.hpp takes them,
    // and the narrow ones widened.
    thread_local std::vector<uint64_t> cnt, side, level;
    cnt.assign(cnt_buf_.begin(), cnt_buf_.end());
    side.assign(side_buf_.begin(), side_buf_.end());
    level.assign(level_buf_.begin(), level_buf_.end());
    const std::array<std::span<const uint64_t>, 7> columns = {
        std::span<const uint64_t>(ts_buf_),
        std::span<const uint64_t>(reinterpret_cast<const uint64_t*>(price_buf_.data()), price_buf_.size()),
        std::span<const uint64_t>(qty_buf_),
        std::span<const uint64_t>(cnt),
        std::span<const uint64_t>(side),
        std::span<const uint64_t>(level),
        std::span<const uint64_t>(reinterpret_cast<const uint64_t*>(seq_buf_.data()), seq_buf_.size()),
    };
    // The encodings this store's last segment chose, unless it is time to look again (design §3).
    const bool search = !has_last_choice_ || segments_since_search_ + 1 >= segment_format_.search_every;

    thread_local std::array<std::string, 7> blocks;
    std::array<column_codec::Choice, 7> chosen{};
    for (size_t c = 0; c < columns.size(); ++c) {
        if (columns[c].size() != n) {
            throw std::logic_error("ColumnarStore: a column of " + std::to_string(columns[c].size()) +
                                   " values in a segment of " + std::to_string(n) + " rows");
        }
        column_codec::EncodeOptions options = segment_format_.search;
        options.hint = search ? nullptr : &last_choice_[c];
        blocks[c].clear();
        chosen[c] = column_codec::encode(columns[c], options, blocks[c]);
        last_choice_[c] = chosen[c].encoding;
    }
    has_last_choice_ = true;
    segments_since_search_ = search ? 0 : segments_since_search_ + 1;

    std::string file(kColumnsMagic, sizeof kColumnsMagic);
    column_codec::put_varint(file, n);
    file.push_back(static_cast<char>(kColumns));
    for (size_t c = 0; c < kColumns; ++c) {
        file.push_back(static_cast<char>(c));
        column_codec::put_varint(file, blocks[c].size());
        const uint32_t crc = crc32c(blocks[c].data(), blocks[c].size());
        file.append(reinterpret_cast<const char*>(&crc), sizeof crc);   // little-endian hosts only
    }
    for (const std::string& block : blocks) file += block;
    // What this thread keeps for the next seal is bounded as the codec bounds its own: a seal's
    // columns fit, and a segment an embedding application sealed after an hour does not stay.
    const auto keep_small = [](auto& buffer) {
        using Buffer = std::remove_reference_t<decltype(buffer)>;
        if (buffer.capacity() * sizeof(typename Buffer::value_type) > column_codec::kEncodeBufferKept) {
            Buffer().swap(buffer);
        }
    };
    keep_small(cnt);
    keep_small(side);
    keep_small(level);
    for (std::string& block : blocks) keep_small(block);
    write_file_checked(dir + "/" + kColumnsV3File, file.data(), file.size());

    OB_LOG_DEBUG("columnar",
                 "Segment %s: %zu rows in %zu bytes of columns (%s): ts %s %zu, price %s %zu, "
                 "qty %s %zu, cnt %s %zu, side %s %zu, level %s %zu, seq %s %zu",
                 dir.c_str(), n, file.size(), search ? "searched" : "as the last segment",
                 chosen[0].encoding.name().c_str(), chosen[0].bytes, chosen[1].encoding.name().c_str(),
                 chosen[1].bytes, chosen[2].encoding.name().c_str(), chosen[2].bytes,
                 chosen[3].encoding.name().c_str(), chosen[3].bytes, chosen[4].encoding.name().c_str(),
                 chosen[4].bytes, chosen[5].encoding.name().c_str(), chosen[5].bytes,
                 chosen[6].encoding.name().c_str(), chosen[6].bytes);
}

SegmentMeta ColumnarStore::write_active_segment(const std::string& dir) {
    // The range this segment records is its rows' - the earliest and the latest timestamp - and
    // not the period it belongs to and its last row, which is what it was until #166. The two
    // agree only while rows arrive in time order; when they did not, a query skipped rows this
    // segment held (measured: `OK` and nothing, for a row a `SELECT` of everything returned) and
    // retention deleted a row one second old with a two-day-old one written after it.
    const uint64_t first_ts = active_min_ts_;
    const uint64_t last_ts  = active_max_ts_;

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

    uint64_t first_price = 0;
    bool raw_qty = active_has_raw_qty_;
    if (segment_format_.version == kColumnarFormatV3) {
        write_columns_v3(dir);
        // What format 2 records beside its columns, kept in meta.json for what reads it: the first
        // price, zigzagged, and whether a quantity is too wide for a Simple8b word - from the
        // quantities themselves, since append_block() leaves that to the seal (#165 part 2a).
        if (!price_buf_.empty()) {
            first_price = encode_prices(std::span<const int64_t>(price_buf_.data(), 1))[0];
        }
        constexpr uint64_t kSimple8bMarker = (uint64_t{1} << 60) - 1;
        raw_qty = raw_qty || std::any_of(qty_buf_.begin(), qty_buf_.end(),
                                         [](uint64_t q) { return q >= kSimple8bMarker; });
    } else {
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
        first_price = encoded_prices.empty() ? 0 : encoded_prices[0];
        raw_qty = active_has_raw_qty_ || qty_result.has_fallback;
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
    meta.format_version = segment_format_.version;
    // first_price: store the zigzag-encoded first price as the anchor
    meta.first_price = first_price;
    meta.has_raw_qty = raw_qty;
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
    // The levels its rows are of (#47), from the columns just written - a seal's, a rollover's and a
    // merge's alike, since all three write through here.
    meta.levels = LevelSet::from_columns(side_buf_, level_buf_);
    if (meta.levels != nullptr) {
        OB_LOG_DEBUG("columnar", "Segment %s holds %zu bid and %zu ask level(s)", dir.c_str(),
                     LevelSet::count(meta.levels->bid), LevelSet::count(meta.levels->ask));
    } else {
        OB_LOG_DEBUG("columnar", "Segment %s has a row no level set can hold; its levels are "
                                 "unknown, and the book at an instant reads it always",
                     dir.c_str());
    }
    // And the range of its prices (#47 step 2), from the same rows: a seal's, a rollover's and a
    // merge's all come through here.
    if (!price_buf_.empty()) {
        const auto [lo, hi] = std::minmax_element(price_buf_.begin(), price_buf_.end());
        meta.has_price_range = true;
        meta.min_price = *lo;
        meta.max_price = *hi;
        OB_LOG_DEBUG("columnar", "Segment %s holds prices [%lld, %lld]", dir.c_str(),
                     static_cast<long long>(meta.min_price), static_cast<long long>(meta.max_price));
    }
    meta.wal_identity    = wal_identity_;
    meta.wal_file_index  = wal_file_index_;
    meta.wal_byte_offset = wal_byte_offset_;
    meta.seal_epoch      = seal_epoch_;
    // This node's own highest number here (#184), or a merge's inputs' answer for it.
    if (has_own_override_) {
        meta.has_own_max       = own_override_known_;
        meta.own_origin        = own_override_origin_;
        meta.own_max_sequence  = own_override_known_ ? own_override_max_ : 0;
        meta.has_received_rows = own_override_known_ && own_override_received_;
        has_own_override_      = false;
    } else {
        meta.has_own_max       = true;
        meta.own_origin        = own_origin_;
        meta.own_max_sequence  = active_own_max_;
        meta.has_received_rows = false;
    }
    if (has_lineage_) {
        // A merge's (#165 part 2b): the latest of its inputs' last rows, which the row appended
        // last need not be, and its level. The inputs go into meta.json only.
        meta.last_row_ts_ns = lineage_last_row_ts_;
        meta.merge_level    = lineage_level_;
    }

    // Write meta.json - last, so that a segment with one is a segment with all of its columns.
    write_meta_json(dir, meta, has_lineage_ ? &lineage_inputs_ : nullptr);
    partial.complete = true;
    has_lineage_ = false;
    lineage_inputs_.clear();

    // Reset active segment state
    has_active_segment_ = false;
    active_row_count_   = 0;
    active_has_raw_qty_ = false;
    active_own_max_     = 0;
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
    // What was read, not what was asked for: `out` may be a buffer an earlier read filled, and the
    // part a short read left would hold that read's values where a new vector held zeros.
    out.resize(static_cast<size_t>(f.gcount()) / sizeof(T));
    return true;
}

/// The buffers one segment read fills - the column files as read and the columns decoded - kept
/// from one read to the next (#49 step 2). A read allocated all eleven afresh and freed them at the
/// end, and in a process whose heap glibc trims after that free every page came back as a fault on
/// the next read: a store alone in a process scanned a segment of 100 000 rows in 6.3 ms, and in
/// 2.5 ms with the heap left untrimmed.
struct SegmentReadBuffers {
    std::vector<uint64_t> timestamps, enc_prices, enc_qtys, enc_seq, qtys, zigzag_seq;
    std::vector<int64_t>  prices, seqs;
    std::vector<uint32_t> counts;
    std::vector<uint8_t>  sides;
    std::vector<uint16_t> levels;
    std::vector<char>     file;   ///< a format-3 segment's columns.v3, as read
    /// What decoding format 3's blocks works in. Kept here rather than per thread, so that what
    /// the decoder grows to is inside the pool's budget too.
    column_codec::DecodeScratch scratch;

    size_t held_bytes() const {
        return file.capacity() + scratch.held_bytes() +
               (timestamps.capacity() + enc_prices.capacity() + enc_qtys.capacity() +
                enc_seq.capacity() + qtys.capacity() + zigzag_seq.capacity()) * sizeof(uint64_t) +
               (prices.capacity() + seqs.capacity()) * sizeof(int64_t) +
               counts.capacity() * sizeof(uint32_t) + sides.capacity() * sizeof(uint8_t) +
               levels.capacity() * sizeof(uint16_t);
    }
};

/// What the process keeps between reads, over every thread: a segment has at most
/// `compaction::kMaxRows` rows, 262 144, and every column of that many is 23 MB, so this is five
/// reads of the largest segments at once - four io threads and a merge.
constexpr size_t kReadBufferBudget = size_t{128} << 20;

/// The sets of buffers no read is using, shared by every thread rather than kept per thread: an
/// application embedding the engine can query from as many threads as it has, and what those keep
/// has to be bounded by something other than their number. A read takes a set - the one given back
/// last, whose pages are the likeliest to be warm - or a new one, and gives it back; a set that
/// would take the pool past its budget is freed instead. A read inside another's callback simply
/// takes a second set.
class ReadBufferPool {
public:
    /// Never destroyed: a thread still reading while statics are torn down must not find it gone.
    static ReadBufferPool& instance() {
        static ReadBufferPool* pool = new ReadBufferPool();
        return *pool;
    }

    std::unique_ptr<SegmentReadBuffers> take() {
        {
            std::lock_guard<std::mutex> lock(mtx_);
            if (!free_.empty()) {
                Kept kept = std::move(free_.back());
                free_.pop_back();
                held_ -= kept.bytes;
                return std::move(kept.buffers);
            }
        }
        return std::make_unique<SegmentReadBuffers>();
    }

    void give(std::unique_ptr<SegmentReadBuffers> buffers) noexcept {
        const size_t bytes = buffers->held_bytes();
        std::unique_ptr<SegmentReadBuffers> freed;
        size_t held = 0;
        size_t budget = 0;
        {
            std::lock_guard<std::mutex> lock(mtx_);
            budget = budget_;
            if (held_ + bytes > budget_) {
                freed = std::move(buffers);
                held = held_;
            } else {
                try {
                    free_.push_back(Kept{bytes, std::move(buffers)});
                    held_ += bytes;
                } catch (const std::exception&) {
                    freed = std::move(buffers);   // a list that could not grow keeps nothing new
                    held = held_;
                }
            }
        }
        if (freed) {
            try {
                OB_LOG_DEBUG("columnar", "freeing %zu bytes of segment-read buffers: the process "
                                         "keeps %zu of at most %zu between reads", bytes, held, budget);
            } catch (const std::exception&) {
                // A read's end is no place to fail from; the line is all that is lost.
            }
        }
    }   // `freed`, if anything, is given back to the allocator here - outside the lock

    size_t held() {
        std::lock_guard<std::mutex> lock(mtx_);
        return held_;
    }

    /// A new budget, and the sets it no longer holds freed.
    size_t set_budget(size_t bytes) {
        std::vector<Kept> dropped;
        size_t was = 0;
        {
            std::lock_guard<std::mutex> lock(mtx_);
            was = budget_;
            budget_ = bytes;
            while (!free_.empty() && held_ > budget_) {
                held_ -= free_.front().bytes;
                dropped.push_back(std::move(free_.front()));
                free_.erase(free_.begin());
            }
        }
        return was;
    }

private:
    ReadBufferPool() = default;

    struct Kept {
        size_t bytes;
        std::unique_ptr<SegmentReadBuffers> buffers;
    };
    std::mutex        mtx_;
    std::vector<Kept> free_;
    size_t            held_ = 0;
    size_t            budget_ = kReadBufferBudget;
};

/// One read's set of buffers, taken from the pool for the length of the read.
class ReadBuffersLease {
public:
    ReadBuffersLease() : buffers_(ReadBufferPool::instance().take()) {}
    ~ReadBuffersLease() { ReadBufferPool::instance().give(std::move(buffers_)); }
    ReadBuffersLease(const ReadBuffersLease&) = delete;
    ReadBuffersLease& operator=(const ReadBuffersLease&) = delete;

    SegmentReadBuffers& buffers() { return *buffers_; }

private:
    std::unique_ptr<SegmentReadBuffers> buffers_;
};

}  // namespace

// ── Segment format 3: columns.v3 ─────────────────────────────────────────────
//
//   "OBS3"                  magic
//   varint                  the segment's rows
//   u8                      columns (7)
//   7 x (u8, varint, u32)   the directory: a column's id, its block's length, its block's CRC32C
//   ...                     the blocks, in the directory's order
//
// Ids: 0 ts, 1 price, 2 qty, 3 cnt, 4 side, 5 level, 6 seq; column_codec.hpp has the blocks.

namespace {

struct ColumnsDirectory {
    uint64_t rows{0};
    std::array<uint64_t, kColumns> offset{};
    std::array<uint64_t, kColumns> length{};
    std::array<uint32_t, kColumns> crc{};
    uint64_t total_bytes{0};   ///< the directory and every block: what the file's length must be
};

/// The directory at the head of `bytes`; false, and the reason, for bytes that do not begin with one.
bool parse_columns_header(std::span<const char> bytes, ColumnsDirectory& d, std::string& why) {
    if (bytes.size() < sizeof kColumnsMagic || std::memcmp(bytes.data(), kColumnsMagic, sizeof kColumnsMagic) != 0) {
        why = "does not begin with its magic";
        return false;
    }
    size_t at = sizeof kColumnsMagic;
    if (!column_codec::get_varint(bytes, at, d.rows) || at >= bytes.size()) {
        why = "is cut short in its header";
        return false;
    }
    const auto columns = static_cast<uint8_t>(bytes[at++]);
    if (columns != kColumns) {
        why = "has " + std::to_string(columns) + " columns where a segment has 7";
        return false;
    }
    std::array<bool, kColumns> seen{};
    std::array<uint8_t, kColumns> order{};
    std::array<uint64_t, kColumns> length{};
    std::array<uint32_t, kColumns> crc{};
    for (size_t i = 0; i < kColumns; ++i) {
        if (at >= bytes.size()) {
            why = "is cut short in its directory";
            return false;
        }
        const auto id = static_cast<uint8_t>(bytes[at++]);
        if (id >= kColumns || seen[id]) {
            why = "names a column twice, or one a segment does not have";
            return false;
        }
        seen[id] = true;
        order[i] = id;
        if (!column_codec::get_varint(bytes, at, length[i]) || bytes.size() - at < sizeof(uint32_t)) {
            why = "is cut short in its directory";
            return false;
        }
        std::memcpy(&crc[i], bytes.data() + at, sizeof(uint32_t));   // little-endian, as written
        at += sizeof(uint32_t);
    }
    uint64_t offset = at;
    for (size_t i = 0; i < kColumns; ++i) {
        if (length[i] > (uint64_t{1} << 48)) {   // no block of a segment is anywhere near this
            why = "declares a block longer than any segment holds";
            return false;
        }
        d.offset[order[i]] = offset;
        d.length[order[i]] = length[i];
        d.crc[order[i]] = crc[i];
        offset += length[i];
    }
    d.total_bytes = offset;
    return true;
}

enum class ColumnsLoad { kOk, kMissing, kBad };

/// `dir`'s columns.v3, the columns `columns` asks for decoded into `b`: each block checked against
/// its CRC32C and decoded to exactly `rows` values. kBad, and the reason, for a file that is not
/// what the store writes - a reader holds a format-3 segment to what it says of itself, since a
/// Simple8b word decodes into other numbers without a sound.
ColumnsLoad load_columns_v3(const std::string& dir, uint64_t rows, ColumnSet columns,
                            SegmentReadBuffers& b, std::string& why,
                            std::array<DecodedColumns::Column, kColumns>* fresh = nullptr) {
    std::ifstream f(dir + "/" + kColumnsV3File, std::ios::binary);
    if (!f.is_open()) return ColumnsLoad::kMissing;
    f.seekg(0, std::ios::end);
    const auto size = static_cast<size_t>(f.tellg());
    f.seekg(0, std::ios::beg);
    b.file.resize(size);
    f.read(b.file.data(), static_cast<std::streamsize>(size));
    if (static_cast<size_t>(f.gcount()) != size) {
        why = "could not be read whole";
        return ColumnsLoad::kBad;
    }
    const std::span<const char> file(b.file.data(), b.file.size());
    ColumnsDirectory d;
    if (!parse_columns_header(file, d, why)) return ColumnsLoad::kBad;
    if (d.rows != rows) {
        why = "holds " + std::to_string(d.rows) + " rows where its meta.json says " + std::to_string(rows);
        return ColumnsLoad::kBad;
    }
    if (d.total_bytes != file.size()) {
        why = "is " + std::to_string(file.size()) + " bytes where its directory says " +
              std::to_string(d.total_bytes);
        return ColumnsLoad::kBad;
    }
    const auto count = static_cast<size_t>(rows);
    for (size_t c = 0; c < kColumns; ++c) {
        if (!columns.has(kColumnIds[c])) continue;
        const auto block = file.subspan(static_cast<size_t>(d.offset[c]), static_cast<size_t>(d.length[c]));
        const uint32_t crc = crc32c(block.data(), block.size());
        if (crc != d.crc[c]) {
            char line[96];
            std::snprintf(line, sizeof line, "has column %s with checksum %08x where its directory says %08x",
                          kColumnNames[c], crc, d.crc[c]);
            why = line;
            return ColumnsLoad::kBad;
        }
        std::string reason;
        bool ok = false;
        if (fresh != nullptr) {
            // Into a column of its own, which the segment's slot can then hold (#220): the pool's
            // buffers are the next read's.
            auto values = std::make_shared<ColumnValues>();
            const auto into = [&](auto type) {
                using T = decltype(type);
                return column_codec::decode_as(block, count, values->values.emplace<std::vector<T>>(),
                                               b.scratch, &reason);
            };
            switch (c) {
            case 0:  ok = into(uint64_t{}); break;
            case 1:  ok = into(int64_t{}); break;
            case 2:  ok = into(uint64_t{}); break;
            case 3:  ok = into(uint32_t{}); break;
            case 4:  ok = into(uint8_t{}); break;
            case 5:  ok = into(uint16_t{}); break;
            default: ok = into(int64_t{}); break;
            }
            if (ok) (*fresh)[c] = std::move(values);
        } else {
            switch (c) {
            case 0:  ok = column_codec::decode_as(block, count, b.timestamps, b.scratch, &reason); break;
            case 1:  ok = column_codec::decode_as(block, count, b.prices, b.scratch, &reason); break;
            case 2:  ok = column_codec::decode_as(block, count, b.qtys, b.scratch, &reason); break;
            case 3:  ok = column_codec::decode_as(block, count, b.counts, b.scratch, &reason); break;
            case 4:  ok = column_codec::decode_as(block, count, b.sides, b.scratch, &reason); break;
            case 5:  ok = column_codec::decode_as(block, count, b.levels, b.scratch, &reason); break;
            default: ok = column_codec::decode_as(block, count, b.seqs, b.scratch, &reason); break;
            }
        }
        if (!ok) {
            why = std::string("has column ") + kColumnNames[c] + " that cannot be decoded: " + reason;
            return ColumnsLoad::kBad;
        }
    }
    return ColumnsLoad::kOk;
}

/// Whether `dir`'s columns.v3 holds `rows` rows by its directory and is as long as the directory
/// says - from the file's head alone, since the start asks it of every merged segment.
bool columns_v3_hold(const std::string& dir, uint64_t rows, std::string& why) {
    const std::string path = dir + "/" + kColumnsV3File;
    std::error_code ec;
    const auto size = fs::file_size(path, ec);
    if (ec) {
        why = std::string("has no ") + kColumnsV3File;
        return false;
    }
    std::ifstream f(path, std::ios::binary);
    std::array<char, kColumnsHeaderMax> head{};
    f.read(head.data(), static_cast<std::streamsize>(head.size()));
    ColumnsDirectory d;
    if (!parse_columns_header(std::span<const char>(head.data(), static_cast<size_t>(f.gcount())), d, why)) {
        why = std::string("its ") + kColumnsV3File + " " + why;
        return false;
    }
    if (d.rows != rows || d.total_bytes != size) {
        why = std::string("its ") + kColumnsV3File + " holds " + std::to_string(d.rows) + " row(s) in " +
              std::to_string(size) + " byte(s), where its directory says " + std::to_string(d.total_bytes) +
              " and its meta.json " + std::to_string(rows) + " row(s)";
        return false;
    }
    return true;
}

/// Whether a merged segment holds every row it says it does: what the start decides a merge cut
/// short by (#165 part 2b).
bool segment_holds_its_rows(const SegmentMeta& meta, std::string& why) {
    if (meta.format_version == kColumnarFormatV3) return columns_v3_hold(meta.dir_path, meta.row_count, why);
    std::error_code ec;
    const auto ts_bytes = fs::file_size(fs::path(meta.dir_path) / "ts.col", ec);
    if (ec || ts_bytes != meta.row_count * sizeof(uint64_t)) {
        why = "its ts.col holds " + std::to_string(ec ? 0 : ts_bytes) + " byte(s) for " +
              std::to_string(meta.row_count) + " row(s)";
        return false;
    }
    return true;
}

/// A segment's timestamps, whatever its format; false when they cannot be read whole.
bool read_timestamps(const SegmentMeta& meta, std::vector<uint64_t>& ts) {
    if (meta.format_version == kColumnarFormatV3) {
        SegmentReadBuffers b;
        std::string why;
        if (load_columns_v3(meta.dir_path, meta.row_count, ColumnSet{}.add(QueryColumn::TimestampNs), b,
                            why) != ColumnsLoad::kOk) {
            return false;
        }
        ts.swap(b.timestamps);
        return true;
    }
    return read_column_file(meta.dir_path, "ts.col", ts);
}

}  // namespace

size_t ColumnarStore::read_buffers_held() {
    return ReadBufferPool::instance().held();
}

void ColumnarStore::set_read_buffers_limit_for_test(size_t bytes) {
    const size_t was = ReadBufferPool::instance().set_budget(bytes);
    OB_LOG_INFO("columnar", "segment-read buffers: the process keeps at most %zu bytes (was %zu)",
                bytes, was);
}

void ColumnarStore::set_decoded_columns_budget(size_t bytes) {
    std::unique_lock<std::shared_mutex> lock(index_mtx_);
    decoded_budget_ = bytes > 0 ? std::make_shared<DecodedColumnsBudget>(bytes) : nullptr;
    // Every entry a new slot under the new budget, or none: a slot of the old one keeps its columns
    // only for the reads that copied it, and gives its bytes back to the budget it was charged to.
    size_t entries = 0;
    for (auto& [key, si] : by_symbol_) {
        for (auto& tier : si.tiers) {
            for (auto& meta : tier.segments) {
                meta.decoded = decoded_budget_ ? std::make_shared<DecodedColumns>(decoded_budget_)
                                               : nullptr;
                ++entries;
            }
        }
    }
    if (decoded_budget_) {
        OB_LOG_INFO("columnar", "decoded columns: held between queries within %zu bytes (%zu MiB); "
                                "%zu indexed segment(s) given a slot",
                    bytes, bytes >> 20, entries);
    } else {
        OB_LOG_INFO("columnar", "decoded columns: not held between queries (budget 0); every read "
                                "decodes what it reads");
    }
}

DecodedColumnsBudget::Stats ColumnarStore::decoded_columns_stats() const {
    std::shared_lock<std::shared_mutex> lock(index_mtx_);
    return decoded_budget_ ? decoded_budget_->stats() : DecodedColumnsBudget::Stats{0, 0, 0, 0, 0};
}

namespace {

/// The columns one segment's rows are emitted from: spans over the pool's buffers or over held
/// columns (#220), each the segment's rows long or empty when it was not read - a format-2 price,
/// quantity or count cut short is padded with zeros, as it always was.
struct RowColumns {
    std::span<const uint64_t> ts;
    std::span<const int64_t>  price;
    std::span<const uint64_t> qty;
    std::span<const uint32_t> cnt;
    std::span<const uint8_t>  side;
    std::span<const uint16_t> level;
    std::span<const int64_t>  seq;
};

/// Rows the selection pass marks at a time (#225): its masks stay in L1 between the column passes.
constexpr size_t kSelectChunk = 2048;

/// AND into `mask` whether each of `len` values from `v` is within [lo, hi]: one loop with no branch
/// and no bounds check, which the compiler vectorizes.
template <typename T>
void select_within(const T* v, size_t len, T lo, T hi, uint8_t* mask) {
    for (size_t k = 0; k < len; ++k) {
        mask[k] = static_cast<uint8_t>(mask[k] & static_cast<uint8_t>((v[k] >= lo) & (v[k] <= hi)));
    }
}

/// The rows of one segment within [start_ns, end_ns] that `filter` keeps, built field by field from
/// what `columns` names and handed to `cb` in the segment's order; false once `cb` has said to stop
/// (#47 step 2). A row the filter fails is counted into `filtered` and never built. The one loop
/// both reads emit through, so a pooled read and a held one cannot answer differently.
bool emit_rows(size_t n, const RowColumns& c, ColumnSet columns, uint64_t start_ns, uint64_t end_ns,
               const RowFilter& filter, const std::function<bool(const SnapshotRow&)>& cb,
               uint64_t& filtered) {
    const bool want_price = columns.has(QueryColumn::Price);
    const bool want_qty   = columns.has(QueryColumn::Quantity);
    const bool want_cnt   = columns.has(QueryColumn::OrderCount);
    const bool want_side  = columns.has(QueryColumn::Side);
    const bool want_level = columns.has(QueryColumn::Level);
    const bool want_seq   = columns.has(QueryColumn::SequenceNumber);
    const bool check = !filter.empty();
    const auto build = [&](size_t i, uint64_t ts) {
        // Value-initialised, so a field whose column was not read is zero rather than whatever
        // the last row left there.
        SnapshotRow row{};
        row.timestamp_ns = ts;
        if (want_seq)   row.sequence_number = static_cast<uint64_t>(c.seq[i]);
        if (want_side)  row.side            = c.side[i];
        if (want_level) row.level_index     = c.level[i];
        if (want_price) row.price           = (i < c.price.size()) ? c.price[i] : 0;
        if (want_qty)   row.quantity        = (i < c.qty.size())   ? c.qty[i]   : 0;
        if (want_cnt)   row.order_count     = (i < c.cnt.size())   ? c.cnt[i]   : 0;
        return row;
    };

    // The selection pass (#225), when the time and every column the filter reads hold the segment's
    // rows - a format-3 segment always, a format-2 one unless a short file was padded. The rows are
    // chosen first, a chunk at a time, by one loop over each column with no branch and no bounds
    // check, and only the chosen are built; a condition that keeps one row in a hundred costs the
    // passes over its columns rather than a check of every row's optional ends.
    const auto holds = [n](bool read, size_t rows) { return !read || rows == n; };
    const bool whole = c.ts.size() == n && holds(filter.has_price(), c.price.size()) &&
                       holds(filter.has_side(), c.side.size()) &&
                       holds(filter.has_level(), c.level.size());
    if (check && whole) {
        const int64_t  price_lo = filter.price_lo.value_or(std::numeric_limits<int64_t>::min());
        const int64_t  price_hi = filter.price_hi.value_or(std::numeric_limits<int64_t>::max());
        const uint8_t  side_lo  = filter.side_lo.value_or(0);
        const uint8_t  side_hi  = filter.side_hi.value_or(std::numeric_limits<uint8_t>::max());
        const uint16_t level_lo = filter.level_lo.value_or(0);
        const uint16_t level_hi = filter.level_hi.value_or(std::numeric_limits<uint16_t>::max());
        uint8_t in_range[kSelectChunk];
        uint8_t keep[kSelectChunk];
        for (size_t from = 0; from < n; from += kSelectChunk) {
            const size_t len = std::min(kSelectChunk, n - from);
            const uint64_t* ts = c.ts.data() + from;
            size_t ranged = 0;
            for (size_t k = 0; k < len; ++k) {
                in_range[k] = static_cast<uint8_t>((ts[k] >= start_ns) & (ts[k] <= end_ns));
                ranged += in_range[k];
            }
            if (ranged == 0) continue;
            std::memcpy(keep, in_range, len);
            if (filter.has_price()) select_within(c.price.data() + from, len, price_lo, price_hi, keep);
            if (filter.has_side()) select_within(c.side.data() + from, len, side_lo, side_hi, keep);
            if (filter.has_level()) select_within(c.level.data() + from, len, level_lo, level_hi, keep);
            size_t kept = 0;
            for (size_t k = 0; k < len;) {
                // Eight unchosen rows at a time: a selective condition leaves most of the mask zero.
                if (k + 8 <= len) {
                    uint64_t eight = 0;
                    std::memcpy(&eight, keep + k, sizeof eight);
                    if (eight == 0) {
                        k += 8;
                        continue;
                    }
                }
                if (keep[k] == 0) {
                    ++k;
                    continue;
                }
                ++kept;
                if (!cb(build(from + k, ts[k]))) {
                    // Counted as the row-by-row loop counts them: the rows in the range the filter
                    // failed before the one the reader stopped at.
                    size_t ranged_before = 0;
                    for (size_t j = 0; j <= k; ++j) ranged_before += in_range[j];
                    filtered += ranged_before - kept;
                    return false;
                }
                ++k;
            }
            filtered += ranged - kept;
        }
        return true;
    }

    for (size_t i = 0; i < n; ++i) {
        const uint64_t ts = (i < c.ts.size()) ? c.ts[i] : 0;
        if (ts < start_ns || ts > end_ns) continue;
        if (check) {
            // The values the row would carry, so a condition sees what a check of the built row
            // would have seen - a padded price included.
            const int64_t  price = (i < c.price.size()) ? c.price[i] : 0;
            const uint8_t  side  = (i < c.side.size()) ? c.side[i] : 0;
            const uint16_t level = (i < c.level.size()) ? c.level[i] : 0;
            if (!filter.keeps(price, side, level)) {
                ++filtered;
                continue;
            }
        }
        if (!cb(build(i, ts))) return false;
    }
    return true;
}

}  // namespace

ColumnarStore::SegmentRead ColumnarStore::read_segment_rows(
        const SegmentMeta& meta, ColumnSet columns, uint64_t start_ns, uint64_t end_ns,
        const RowFilter& filter, const std::function<bool(const SnapshotRow&)>& cb, ReadMode mode,
        uint64_t* filtered, bool* stopped) const {
    const bool want_price = columns.has(QueryColumn::Price);
    const bool want_qty   = columns.has(QueryColumn::Quantity);
    const bool want_cnt   = columns.has(QueryColumn::OrderCount);
    const bool want_side  = columns.has(QueryColumn::Side);
    const bool want_level = columns.has(QueryColumn::Level);
    const bool want_seq   = columns.has(QueryColumn::SequenceNumber);

    const std::string& dir = meta.dir_path;

    // A segment written by an older format lacks side, level_index and
    // sequence_number. Reading it anyway would hand back rows with those
    // fields silently zeroed, which is the defect version 2 exists to
    // fix. Refuse it loudly instead - and one of a newer format, which this
    // build cannot decode.
    if (!columnar_format_readable(meta.format_version)) {
        OB_LOG_ERROR("columnar",
                     "Skipping segment %s: unsupported format_version=%u "
                     "(this build reads %u and %u)",
                     dir.c_str(), meta.format_version, kColumnarFormatV2, kColumnarFormatV3);
        return SegmentRead::kUnreadable;
    }
    if (meta.format_version == kColumnarFormatV3 && mode == ReadMode::kQuery && meta.decoded) {
        return read_held_columns(meta, columns, start_ns, end_ns, filter, cb, filtered, stopped);
    }

    // A set of buffers from the pool, holding an earlier read's columns. Every column this read uses
    // is read or decoded into its buffer below; one it does not use is emptied here, capacity kept,
    // so that it is empty by construction as a new vector was.
    ReadBuffersLease lease;
    SegmentReadBuffers& b = lease.buffers();
    std::vector<uint64_t>& timestamps = b.timestamps;
    std::vector<uint64_t>& enc_prices = b.enc_prices;
    std::vector<uint64_t>& enc_qtys   = b.enc_qtys;
    std::vector<uint64_t>& enc_seq    = b.enc_seq;
    std::vector<uint32_t>& counts     = b.counts;
    std::vector<uint8_t>&  sides      = b.sides;
    std::vector<uint16_t>& levels     = b.levels;
    if (!columns.has(QueryColumn::TimestampNs)) timestamps.clear();
    if (!want_price) { enc_prices.clear(); b.prices.clear(); }
    if (!want_qty)   { enc_qtys.clear();   b.qtys.clear(); }
    if (!want_cnt)   counts.clear();
    if (!want_side)  sides.clear();
    if (!want_level) levels.clear();
    if (!want_seq)   { enc_seq.clear(); b.zigzag_seq.clear(); b.seqs.clear(); }

    std::vector<int64_t>&  prices = b.prices;
    std::vector<uint64_t>& qtys   = b.qtys;
    std::vector<int64_t>&  seqs   = b.seqs;

    if (meta.format_version == kColumnarFormatV3) {
        // One file, the columns the read asks for decoded from it, each block checked against its
        // checksum and holding exactly the segment's rows - so nothing below is short.
        std::string why;
        const ColumnsLoad loaded = load_columns_v3(dir, meta.row_count, columns, b, why);
        if (loaded == ColumnsLoad::kMissing) {
            if (mode == ReadMode::kQuery && !still_indexed(dir)) {
                OB_LOG_DEBUG("columnar", "segment %s was removed while this query read it; "
                                         "its rows are past the retention", dir.c_str());
                return SegmentRead::kRemoved;
            }
            OB_LOG_ERROR("columnar", "Skipping segment %s: missing %s", dir.c_str(), kColumnsV3File);
            return SegmentRead::kUnreadable;
        }
        if (loaded == ColumnsLoad::kBad) {
            OB_LOG_ERROR("columnar", "Skipping segment %s: its %s %s", dir.c_str(), kColumnsV3File,
                         why.c_str());
            return SegmentRead::kUnreadable;
        }
    } else {
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
                if (mode == ReadMode::kQuery && !still_indexed(dir)) {
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
        // top of scan() is what puts it there. A hardcoded `true` here reads as belt and
        // braces and is worse than that: it makes that widening unobservable, so a mutation
        // deleting it survived the test written to catch exactly that.
        need(columns.has(QueryColumn::TimestampNs), "ts.col", timestamps);
        need(want_price, "price.col", enc_prices);
        need(want_qty,   "qty.col",   enc_qtys);
        need(want_cnt,   "cnt.col",   counts);
        need(want_side,  "side.col",  sides);
        need(want_level, "level.col", levels);
        need(want_seq,   "seq.col",   enc_seq);
        if (removed) return SegmentRead::kRemoved;
        if (missing) return SegmentRead::kUnreadable;

        // Decoding follows the set too, and the sequence number is the expensive one: it is
        // Simple8b **and** zigzag-delta, so a query that does not ask for it skips two of the
        // four decode passes a segment would otherwise cost.
        if (want_price) decode_prices_into(enc_prices, prices);
        if (want_qty)   decode_simple8b_into(enc_qtys, meta.row_count, qtys);
        if (want_seq) {
            decode_simple8b_into(enc_seq, meta.row_count, b.zigzag_seq);
            decode_prices_into(b.zigzag_seq, seqs);
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
            return SegmentRead::kUnreadable;
        }
        // A merge writes every row it reads into a segment that outlives this one, so it reads a
        // segment whole or not at all (#165 part 2b): the padding below is a query's, and a merged row
        // with a zero timestamp would widen the merged segment's range to the epoch.
        if (mode == ReadMode::kMerge &&
            (timestamps.size() < expected || prices.size() < expected ||
             qtys.size() < expected || counts.size() < expected)) {
            OB_LOG_ERROR("columnar",
                         "segment %s cannot be merged: short column(s) for row_count=%zu "
                         "(ts=%zu price=%zu qty=%zu cnt=%zu)",
                         dir.c_str(), expected, timestamps.size(), prices.size(), qtys.size(),
                         counts.size());
            return SegmentRead::kUnreadable;
        }
    }

    uint64_t failed = 0;
    const bool going = emit_rows(static_cast<size_t>(meta.row_count),
                                 RowColumns{timestamps, prices, qtys, counts, sides, levels, seqs},
                                 columns, start_ns, end_ns, filter, cb, failed);
    if (filtered != nullptr) *filtered += failed;
    if (stopped != nullptr) *stopped = !going;
    return SegmentRead::kRead;
}

ColumnarStore::SegmentRead ColumnarStore::read_held_columns(
        const SegmentMeta& meta, ColumnSet columns, uint64_t start_ns, uint64_t end_ns,
        const RowFilter& filter, const std::function<bool(const SnapshotRow&)>& cb,
        uint64_t* filtered, bool* stopped) const {
    DecodedColumns& slot = *meta.decoded;
    const std::string& dir = meta.dir_path;

    // What the slot holds of the columns asked for; the rest is read from the file below.
    std::array<DecodedColumns::Column, kColumns> held{};
    ColumnSet missing;
    for (size_t c = 0; c < kColumns; ++c) {
        if (!columns.has(kColumnIds[c])) continue;
        held[c] = slot.get(c);
        if (!held[c]) missing.add(kColumnIds[c]);
    }
    if (missing.count() > 0) {
        // The file as a read without a slot reads it - every check the same - and the missing
        // columns decoded into columns of their own. Nothing is held from a file that fails a check.
        ReadBuffersLease lease;
        std::array<DecodedColumns::Column, kColumns> fresh{};
        std::string why;
        const ColumnsLoad loaded = load_columns_v3(dir, meta.row_count, missing, lease.buffers(), why,
                                                   &fresh);
        if (loaded == ColumnsLoad::kMissing) {
            if (!still_indexed(dir)) {
                OB_LOG_DEBUG("columnar", "segment %s was removed while this query read it; "
                                         "its rows are past the retention", dir.c_str());
                return SegmentRead::kRemoved;
            }
            OB_LOG_ERROR("columnar", "Skipping segment %s: missing %s", dir.c_str(), kColumnsV3File);
            return SegmentRead::kUnreadable;
        }
        if (loaded == ColumnsLoad::kBad) {
            OB_LOG_ERROR("columnar", "Skipping segment %s: its %s %s", dir.c_str(), kColumnsV3File,
                         why.c_str());
            return SegmentRead::kUnreadable;
        }
        for (size_t c = 0; c < kColumns; ++c) {
            if (fresh[c]) held[c] = slot.put(c, std::move(fresh[c]), dir);
        }
    }

    // The rows within the range, through the loop read_segment_rows() emits through, from the held
    // columns - each exactly `row_count` long, as a format-3 block decodes or not at all.
    const auto column = [&](size_t c, auto type) {
        using T = decltype(type);
        return held[c] ? held[c]->as<T>() : std::span<const T>{};
    };
    uint64_t failed = 0;
    const bool going = emit_rows(
        static_cast<size_t>(meta.row_count),
        RowColumns{column(0, uint64_t{}), column(1, int64_t{}), column(2, uint64_t{}),
                   column(3, uint32_t{}), column(4, uint8_t{}), column(5, uint16_t{}),
                   column(6, int64_t{})},
        columns, start_ns, end_ns, filter, cb, failed);
    if (filtered != nullptr) *filtered += failed;
    if (stopped != nullptr) *stopped = !going;
    return SegmentRead::kRead;
}

bool ColumnarStore::read_segment(const SegmentMeta& meta,
                                 const std::function<void(const SnapshotRow&)>& cb) const {
    // Nothing removes a segment a merge is reading - retention and the merge run on one thread -
    // so a missing file here is the corruption it is everywhere else.
    return read_segment_rows(meta, ColumnSet::all(), 0, UINT64_MAX, RowFilter{},
                             [&](const SnapshotRow& row) {
                                 cb(row);
                                 return true;
                             },
                             ReadMode::kMerge) == SegmentRead::kRead;
}

ColumnarStore::ScanCost ColumnarStore::scan(uint64_t start_ns, uint64_t end_ns,
                                             std::string_view symbol, std::string_view exchange,
                                             ColumnSet columns,
                                             std::function<void(const SnapshotRow&)> cb) const {
    return scan(start_ns, end_ns, symbol, exchange, columns, RowFilter{},
                [&cb](const SnapshotRow& row) {
                    cb(row);
                    return true;
                });
}

bool ColumnarStore::cannot_hold(const SegmentMeta& meta, const RowFilter& filter, bool* by_price) {
    if (by_price != nullptr) *by_price = false;
    if (filter.has_price() && meta.has_price_range &&
        ((filter.price_lo && meta.max_price < *filter.price_lo) ||
         (filter.price_hi && meta.min_price > *filter.price_hi))) {
        if (by_price != nullptr) *by_price = true;
        return true;
    }
    if ((filter.has_side() || filter.has_level()) && meta.levels != nullptr) {
        // A segment has a set only when every row of it is of a side and below `kLevels`
        // (`LevelSet::from_columns()`), so the rectangle needs no more than that.
        const unsigned side_lo = filter.side_lo.value_or(SIDE_BID);
        const unsigned side_hi = std::min<unsigned>(filter.side_hi.value_or(SIDE_ASK), SIDE_ASK);
        const size_t level_lo = filter.level_lo.value_or(0);
        const size_t level_hi =
            std::min<size_t>(filter.level_hi.value_or(LevelSet::kLevels - 1), LevelSet::kLevels - 1);
        for (unsigned side = side_lo; side <= side_hi; ++side) {
            for (size_t level = level_lo; level <= level_hi; ++level) {
                if (meta.levels->has(static_cast<uint8_t>(side), static_cast<uint16_t>(level))) {
                    return false;
                }
            }
        }
        return true;
    }
    return false;
}

ColumnarStore::ScanCost ColumnarStore::scan(uint64_t start_ns, uint64_t end_ns,
                                             std::string_view symbol, std::string_view exchange,
                                             ColumnSet columns, const RowFilter& filter,
                                             const std::function<bool(const SnapshotRow&)>& cb) const {
    // Whatever the caller asked to be handed, this filters on the row's timestamp, so it reads
    // that column. Adding it here rather than trusting the caller means a set built by hand
    // cannot produce a scan that compares every row against a zero it never loaded. The filter's
    // columns likewise (#47 step 2): it is checked on them before a row is built.
    columns.add(QueryColumn::TimestampNs);
    if (filter.has_price()) columns.add(QueryColumn::Price);
    if (filter.has_side()) columns.add(QueryColumn::Side);
    if (filter.has_level()) columns.add(QueryColumn::Level);

    // The segments that can hold a row of this range, copied under the shared lock so the files
    // are read without it. Only this symbol's, and only the window each width tier's sorted starts
    // allow (#165): this used to copy every segment of every symbol - three strings each - and
    // filter the copy.
    ScanCost cost;
    std::vector<SegmentMeta> index_snapshot;
    std::vector<std::shared_ptr<const RowBlock>> block_snapshot;
    // Held until the files are read (#165 part 2b): a merge that replaces a segment this copied
    // removes its files only once every scan that took this generation has let it go.
    std::shared_ptr<const void> reading;
    {
        std::shared_lock<std::shared_mutex> lock(index_mtx_);
        reading = reader_generation_;
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

    const auto summary = [&]() {
        OB_LOG_DEBUG("columnar", "scan of %.*s.%.*s: %zu segment(s) read, %zu left unread by price "
                                 "and %zu by levels, %llu row(s) filtered before they were built%s",
                     static_cast<int>(symbol.size()), symbol.data(),
                     static_cast<int>(exchange.size()), exchange.data(), cost.segments_read,
                     cost.skipped_by_price, cost.skipped_by_levels,
                     static_cast<unsigned long long>(cost.rows_filtered),
                     cost.stopped ? "; ended by its reader" : "");
    };
    const bool filtering = !filter.empty();
    for (const auto& meta : index_snapshot) {
        bool by_price = false;
        if (filtering && cannot_hold(meta, filter, &by_price)) {
            ++(by_price ? cost.skipped_by_price : cost.skipped_by_levels);
            continue;
        }
        ++cost.segments_read;
        bool stopped = false;
        read_segment_rows(meta, columns, start_ns, end_ns, filter, cb, ReadMode::kQuery,
                          &cost.rows_filtered, &stopped);
        if (stopped) {
            cost.stopped = true;
            summary();
            return cost;
        }
    }

    const bool want_price = columns.has(QueryColumn::Price);
    const bool want_qty   = columns.has(QueryColumn::Quantity);
    const bool want_cnt   = columns.has(QueryColumn::OrderCount);
    const bool want_side  = columns.has(QueryColumn::Side);
    const bool want_level = columns.has(QueryColumn::Level);
    const bool want_seq   = columns.has(QueryColumn::SequenceNumber);

    // Then the rows no seal has written yet, after every segment and in the order they were
    // drained: they are this symbol's newest writes, so a reader that keeps the last row of a tie
    // keeps the one written later (#168). Field by field as a segment's rows are, so a column the
    // caller did not ask for is zero here too rather than a value it would read by accident.
    for (const auto& block : block_snapshot) {
        for (const SnapshotRow& r : block->rows) {
            if (r.timestamp_ns < start_ns || r.timestamp_ns > end_ns) continue;
            if (filtering && !filter.keeps(r.price, r.side, r.level_index)) {
                ++cost.rows_filtered;
                continue;
            }
            SnapshotRow row{};
            row.timestamp_ns = r.timestamp_ns;
            if (want_seq)   row.sequence_number = r.sequence_number;
            if (want_side)  row.side            = r.side;
            if (want_level) row.level_index     = r.level_index;
            if (want_price) row.price           = r.price;
            if (want_qty)   row.quantity        = r.quantity;
            if (want_cnt)   row.order_count     = r.order_count;
            if (!cb(row)) {
                cost.stopped = true;
                summary();
                return cost;
            }
        }
    }
    summary();
    return cost;
}

ColumnarStore::BookAt ColumnarStore::latest_per_level(uint64_t at, std::string_view symbol,
                                                      std::string_view exchange,
                                                      const LevelSet* wanted) const {
    BookAt out;
    // What a scan of [0, at] would read, in the order it would deliver it: the segments by
    // `segment_order_less`, then the blocks. That order is each candidate's rank, which decides a
    // tie on the timestamp as the scan's delivery decided it.
    std::vector<SegmentMeta> segments;
    std::vector<std::shared_ptr<const RowBlock>> blocks;
    std::shared_ptr<const void> reading;
    {
        std::shared_lock<std::shared_mutex> lock(index_mtx_);
        reading = reader_generation_;
        const auto it = by_symbol_.find(index_key(symbol, exchange));
        if (it == by_symbol_.end()) return out;
        for (const auto& b : it->second.blocks) {
            if (b->min_ts_ns <= at) blocks.push_back(b);
        }
        for (const WidthTier& tier : it->second.tiers) {
            const size_t before = segments.size();
            for (const SegmentMeta& m : tier.segments) {
                if (m.start_ts_ns > at) break;   // sorted by start
                segments.push_back(m);
            }
            if (before != 0 && before != segments.size()) {
                std::inplace_merge(segments.begin(),
                                   segments.begin() + static_cast<std::ptrdiff_t>(before),
                                   segments.end(), segment_order_less);
            }
        }
    }

    struct Candidate {
        uint64_t latest;   // no row of it is later than this, nor than `at`
        size_t rank;
        const SegmentMeta* segment;
        const RowBlock* block;
    };
    std::vector<Candidate> candidates;
    candidates.reserve(segments.size() + blocks.size());
    for (size_t i = 0; i < segments.size(); ++i) {
        candidates.push_back({std::min(segments[i].end_ts_ns, at), i, &segments[i], nullptr});
    }
    for (size_t i = 0; i < blocks.size(); ++i) {
        candidates.push_back({std::min(blocks[i]->max_ts_ns, at), segments.size() + i, nullptr,
                              blocks[i].get()});
    }
    std::sort(candidates.begin(), candidates.end(), [](const Candidate& a, const Candidate& b) {
        return a.latest != b.latest ? a.latest > b.latest : a.rank > b.rank;
    });

    // The row each level holds so far, and how to compare: later timestamp, then later rank, then
    // later in its candidate. A flat array for the levels a LevelSet can name - the ones a skip
    // asks about - and a map for any other, which no LevelSet holds and so no skip asks about.
    struct Best {
        bool found{false};
        uint64_t ts{0};
        size_t rank{0};
        size_t index{0};
        SnapshotRow row{};
    };
    constexpr size_t kLevels = LevelSet::kLevels;
    std::vector<Best> flat(2 * kLevels);
    std::map<uint32_t, Best> other;
    const auto slot = [&](const SnapshotRow& row) -> Best& {
        if ((row.side == SIDE_BID || row.side == SIDE_ASK) && row.level_index < kLevels) {
            return flat[static_cast<size_t>(row.side) * kLevels + row.level_index];
        }
        return other[(static_cast<uint32_t>(row.side) << 16) | static_cast<uint32_t>(row.level_index)];
    };
    // Whether a row of this candidate at its latest could still win a level it has.
    const auto beats = [](const Best& b, const Candidate& c) {
        return !b.found || b.ts < c.latest || (b.ts == c.latest && b.rank < c.rank);
    };
    // Only the levels asked for, when some are: a candidate none of whose wanted levels can change
    // is not read, whatever else it holds.
    const auto can_change = [&](const Candidate& c) {
        const LevelSet& set = *c.segment->levels;
        for (size_t side = 0; side < 2; ++side) {
            const LevelSet::Bits& bits = set.side(side);
            for (size_t w = 0; w < LevelSet::kWords; ++w) {
                const uint64_t asked = wanted != nullptr ? wanted->side(side)[w] : ~uint64_t{0};
                for (uint64_t x = bits[w] & asked; x != 0; x &= x - 1) {
                    const size_t level = w * 64 + static_cast<size_t>(__builtin_ctzll(x));
                    if (beats(flat[side * kLevels + level], c)) return true;
                }
            }
        }
        return false;
    };

    for (const Candidate& c : candidates) {
        // Skipped only on what its meta.json proves: its levels, and a range that is its rows'
        // (#166) - a segment written before that recorded its last row's time as its end.
        if (c.segment != nullptr && c.segment->levels != nullptr && c.segment->time_range_is_rows &&
            !can_change(c)) {
            ++out.segments_skipped;
            continue;
        }
        size_t index = 0;
        const auto consider = [&](const SnapshotRow& row) {
            const size_t i = index++;
            if (row.timestamp_ns > at) return;
            if (wanted != nullptr && !wanted->has(row.side, row.level_index)) return;
            Best& b = slot(row);
            if (!b.found || row.timestamp_ns > b.ts ||
                (row.timestamp_ns == b.ts && (c.rank > b.rank || (c.rank == b.rank && i > b.index)))) {
                b = Best{true, row.timestamp_ns, c.rank, i, row};
            }
        };
        if (c.segment != nullptr) {
            read_segment_rows(*c.segment, ColumnSet::all(), 0, at, RowFilter{},
                              [&](const SnapshotRow& r) {
                                  consider(r);
                                  return true;
                              },
                              ReadMode::kQuery);
            ++out.segments_read;
        } else {
            for (const SnapshotRow& r : c.block->rows) consider(r);
            ++out.blocks;
        }
    }

    // In the order the book was always answered in: by (side << 16) | level.
    for (size_t side = 0; side < 2; ++side) {
        const uint32_t base = static_cast<uint32_t>(side) << 16;
        // The other levels of this side - past kLevels - come after its flat ones, in order.
        for (size_t level = 0; level < kLevels; ++level) {
            const Best& b = flat[side * kLevels + level];
            if (b.found) out.rows.push_back(b.row);
        }
        for (auto it = other.lower_bound(base); it != other.end() && (it->first >> 16) == side; ++it) {
            out.rows.push_back(it->second.row);
        }
    }
    for (auto it = other.lower_bound(2u << 16); it != other.end(); ++it) out.rows.push_back(it->second.row);

    OB_LOG_DEBUG("columnar", "book of %.*s.%.*s at %llu%s: %zu level(s); %zu segment(s) read, %zu "
                             "skipped, %zu block(s)",
                 static_cast<int>(symbol.size()), symbol.data(),
                 static_cast<int>(exchange.size()), exchange.data(),
                 static_cast<unsigned long long>(at),
                 wanted != nullptr ? ", of the levels asked for" : "", out.rows.size(),
                 out.segments_read, out.segments_skipped, out.blocks);
    return out;
}

ColumnarStore::TimeOrderedCost ColumnarStore::scan_by_time(
        uint64_t start_ns, uint64_t end_ns, std::string_view symbol, std::string_view exchange,
        ColumnSet columns, const RowFilter& filter,
        const std::function<bool(const SnapshotRow&)>& cb) const {
    columns.add(QueryColumn::TimestampNs);
    if (filter.has_price()) columns.add(QueryColumn::Price);
    if (filter.has_side()) columns.add(QueryColumn::Side);
    if (filter.has_level()) columns.add(QueryColumn::Level);
    TimeOrderedCost cost;
    // The candidates scan() would read, in the order it delivers them - that order is each one's
    // rank, which decides a tie on the timestamp - copied under the shared lock, the generation
    // held until the files are read (#165 part 2b).
    std::vector<SegmentMeta> segments;
    std::vector<std::shared_ptr<const RowBlock>> blocks;
    std::shared_ptr<const void> reading;
    {
        std::shared_lock<std::shared_mutex> lock(index_mtx_);
        reading = reader_generation_;
        const auto it = by_symbol_.find(index_key(symbol, exchange));
        if (it == by_symbol_.end()) return cost;
        for (const auto& b : it->second.blocks) {
            if (b->min_ts_ns <= end_ns && b->max_ts_ns >= start_ns) blocks.push_back(b);
        }
        for (const WidthTier& tier : it->second.tiers) {
            const auto& v = tier.segments;
            const size_t before = segments.size();
            const uint64_t from = start_ns > tier.widest_ns ? start_ns - tier.widest_ns : 0;
            auto first = std::lower_bound(
                v.begin(), v.end(), from,
                [](const SegmentMeta& m, uint64_t at) { return m.start_ts_ns < at; });
            for (auto i = first; i != v.end() && i->start_ts_ns <= end_ns; ++i) {
                if (i->end_ts_ns >= start_ns) segments.push_back(*i);
            }
            if (before != 0 && before != segments.size()) {
                std::inplace_merge(segments.begin(),
                                   segments.begin() + static_cast<std::ptrdiff_t>(before),
                                   segments.end(), segment_order_less);
            }
        }
    }
    cost.candidates = segments.size();
    cost.blocks = blocks.size();

    // A run: one candidate's kept rows, by time, a tie in the order it holds them.
    struct Run {
        std::vector<SnapshotRow> rows;
        size_t pos{0};
        size_t rank{0};
    };
    std::vector<std::unique_ptr<Run>> runs;
    // The head of each run with rows left: the earliest time first, and at one time the lower
    // rank - the candidate scan() delivers first.
    struct Head {
        uint64_t ts;
        size_t rank;
        Run* run;
    };
    const auto later = [](const Head& a, const Head& b) {
        return a.ts != b.ts ? a.ts > b.ts : a.rank > b.rank;
    };
    std::vector<Head> heap;
    uint64_t held = 0;
    const auto add_run = [&](std::unique_ptr<Run> run) {
        if (run->rows.empty()) return;
        std::stable_sort(run->rows.begin(), run->rows.end(),
                         [](const SnapshotRow& a, const SnapshotRow& b) {
                             return a.timestamp_ns < b.timestamp_ns;
                         });
        cost.kept += run->rows.size();
        held += run->rows.size();
        cost.max_held = std::max(cost.max_held, held);
        heap.push_back({run->rows.front().timestamp_ns, run->rank, run.get()});
        std::push_heap(heap.begin(), heap.end(), later);
        runs.push_back(std::move(run));
    };
    // Hands over every held row before `until`; false once `cb` has said to stop.
    const auto hand_over = [&](uint64_t until, bool all) {
        while (!heap.empty() && (all || heap.front().ts < until)) {
            std::pop_heap(heap.begin(), heap.end(), later);
            Head head = heap.back();
            heap.pop_back();
            Run& run = *head.run;
            const SnapshotRow& row = run.rows[run.pos++];
            --held;
            ++cost.delivered;
            if (!cb(row)) {
                cost.stopped = true;
                return false;
            }
            if (run.pos < run.rows.size()) {
                heap.push_back({run.rows[run.pos].timestamp_ns, run.rank, &run});
                std::push_heap(heap.begin(), heap.end(), later);
            } else {
                run.rows = {};   // its memory, now - the run itself goes with the read
            }
        }
        return true;
    };
    const auto read_segment_run = [&](size_t i) {
        // A segment the filter's proof leaves unread holds none of the rows asked for (#47 step 2);
        // its rank is still its own, so the others' ties are decided as before.
        if (!filter.empty() && cannot_hold(segments[i], filter)) {
            ++cost.skipped;
            return;
        }
        auto run = std::make_unique<Run>();
        run->rank = i;
        read_segment_rows(segments[i], columns, start_ns, end_ns, filter,
                          [&](const SnapshotRow& row) {
                              run->rows.push_back(row);
                              return true;
                          },
                          ReadMode::kQuery, &cost.rows_filtered);
        add_run(std::move(run));
    };

    // First what has no start to wait for: the blocks, delivered after every segment, and any
    // segment whose recorded range is not its rows' (#166) - none after open_existing() repaired
    // them, but a row of one could be anywhere in time.
    const bool want_price = columns.has(QueryColumn::Price);
    const bool want_qty   = columns.has(QueryColumn::Quantity);
    const bool want_cnt   = columns.has(QueryColumn::OrderCount);
    const bool want_side  = columns.has(QueryColumn::Side);
    const bool want_level = columns.has(QueryColumn::Level);
    const bool want_seq   = columns.has(QueryColumn::SequenceNumber);
    for (size_t b = 0; b < blocks.size(); ++b) {
        auto run = std::make_unique<Run>();
        run->rank = segments.size() + b;
        for (const SnapshotRow& r : blocks[b]->rows) {
            if (r.timestamp_ns < start_ns || r.timestamp_ns > end_ns) continue;
            if (!filter.keeps(r.price, r.side, r.level_index)) {
                ++cost.rows_filtered;
                continue;
            }
            SnapshotRow row{};
            row.timestamp_ns = r.timestamp_ns;
            if (want_seq)   row.sequence_number = r.sequence_number;
            if (want_side)  row.side            = r.side;
            if (want_level) row.level_index     = r.level_index;
            if (want_price) row.price           = r.price;
            if (want_qty)   row.quantity        = r.quantity;
            if (want_cnt)   row.order_count     = r.order_count;
            run->rows.push_back(row);
        }
        add_run(std::move(run));
    }
    size_t unbounded = 0;
    for (size_t i = 0; i < segments.size(); ++i) {
        if (!segments[i].time_range_is_rows) {
            read_segment_run(i);
            ++unbounded;
        }
    }

    // Then the segments by their start: none read later holds a row before it.
    bool going = true;
    for (size_t i = 0; i < segments.size() && going; ++i) {
        if (!segments[i].time_range_is_rows) continue;
        going = hand_over(segments[i].start_ts_ns, false);
        if (going) read_segment_run(i);
    }
    if (going) hand_over(0, true);

    OB_LOG_DEBUG("columnar", "scan by time of %.*s.%.*s [%llu, %llu]: %zu segment(s), %zu read "
                             "first for a range that is not their rows', %zu block(s); %llu row(s) "
                             "kept, %llu handed over, at most %llu held%s",
                 static_cast<int>(symbol.size()), symbol.data(),
                 static_cast<int>(exchange.size()), exchange.data(),
                 static_cast<unsigned long long>(start_ns), static_cast<unsigned long long>(end_ns),
                 cost.candidates, unbounded, cost.blocks, static_cast<unsigned long long>(cost.kept),
                 static_cast<unsigned long long>(cost.delivered),
                 static_cast<unsigned long long>(cost.max_held),
                 cost.stopped ? ", ended by its reader" : "");
    return cost;
}

std::optional<std::pair<uint64_t, uint64_t>> ColumnarStore::time_span(
        std::string_view symbol, std::string_view exchange) const {
    std::shared_lock<std::shared_mutex> lock(index_mtx_);
    const auto it = by_symbol_.find(index_key(symbol, exchange));
    if (it == by_symbol_.end()) return std::nullopt;
    bool any = false;
    uint64_t lo = UINT64_MAX, hi = 0;
    for (const auto& b : it->second.blocks) {
        any = true;
        lo = std::min(lo, b->min_ts_ns);
        hi = std::max(hi, b->max_ts_ns);
    }
    for (const WidthTier& tier : it->second.tiers) {
        for (const SegmentMeta& m : tier.segments) {
            any = true;
            lo = std::min(lo, m.start_ts_ns);
            hi = std::max(hi, m.end_ts_ns);
        }
    }
    if (!any) return std::nullopt;
    return std::make_pair(lo, hi);
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
    last_rebuild_removed_ = {};

    if (!fs::exists(base_dir_)) return;

    // Recursively scan for meta.json files, and for what a range repair that did not finish left
    // beside them. Those are always safe to delete: the repair changes a meta.json only by renaming
    // one of them over it, so the meta.json beside it is the old one or the new one and whole.
    //
    // And for what a merge leaves (#165 part 2b): a working directory, which nothing published and
    // which is not descended into, and the inputs each merged segment names, which a crash between
    // its publication and their removal leaves beside it. Keyed by the path the input had.
    std::vector<fs::path> leftovers;
    std::vector<fs::path> staging;
    // More than one merged segment can name one path: a name comes back once its input is gone,
    // and the segment that took it can be merged in turn.
    std::unordered_multimap<std::string, SegmentInput> replaced;
    std::vector<SegmentMeta> found;
    std::vector<SegmentInput> inputs;
    // Merged segments short of their rows, with what they name: decided once every segment is found.
    std::vector<std::pair<SegmentMeta, std::vector<SegmentInput>>> short_merges;
    for (auto it = fs::recursive_directory_iterator(base_dir_);
         it != fs::recursive_directory_iterator(); ++it) {
        const auto& entry = *it;
        if (entry.is_directory()) {
            if (it.depth() >= kSegmentDepth && is_compacting_dir(entry.path().filename().string())) {
                staging.push_back(entry.path());
                it.disable_recursion_pending();
            }
            continue;
        }
        if (!entry.is_regular_file()) continue;
        if (entry.path().filename() == kRangeRepairFile) {
            leftovers.push_back(entry.path());
            continue;
        }
        if (entry.path().filename() != "meta.json") continue;

        SegmentMeta meta;
        if (!parse_meta_json(entry.path().string(), meta, &inputs)) continue;

        meta.dir_path = entry.path().parent_path().string();
        if (!inputs.empty()) {
            // A merged segment is published only once its files are on the device, so a short one
            // is a storage that did not keep what it acknowledged - and its inputs, where they are
            // still here, are what holds the rows. Decided below, once every segment is found.
            std::string why;
            if (!segment_holds_its_rows(meta, why)) {
                OB_LOG_WARN("columnar", "%s names %zu segment(s) it replaced and %s",
                            meta.dir_path.c_str(), inputs.size(), why.c_str());
                short_merges.emplace_back(std::move(meta), std::move(inputs));
                continue;
            }
        }
        const fs::path parent = entry.path().parent_path().parent_path();
        for (auto& in : inputs) {
            const std::string at = (parent / in.dir_name).string();
            replaced.emplace(at, std::move(in));
        }
        found.push_back(std::move(meta));
    }
    for (const auto& leftover : leftovers) {
        std::error_code ec;
        fs::remove(leftover, ec);
        OB_LOG_DEBUG("columnar", "removed %s, left by a range repair that did not finish%s",
                     leftover.string().c_str(), ec ? " (and could not remove it)" : "");
    }
    for (const auto& dir : staging) {
        std::error_code ec;
        fs::remove_all(dir, ec);
        ++last_rebuild_removed_.staging;
        OB_LOG_DEBUG("columnar", "removed %s, a merge nothing published%s", dir.string().c_str(),
                     ec ? " (and could not remove all of it)" : "");
    }
    // Before the inputs are told apart: a merge took an input with its range repaired, and a repair
    // that could not reach the disk is redone here first, so the input is compared with the range it
    // was merged with.
    repair_ranges_locked(found);

    // A short merged segment goes only when every input it names is here to hold its rows; with one
    // missing it is kept as it is - a scan skips what it cannot read whole, and what can be read of
    // it by hand is still on the disk. Its inputs stay either way.
    if (!short_merges.empty()) {
        std::unordered_map<std::string, const SegmentMeta*> by_dir;
        for (const SegmentMeta& m : found) by_dir.emplace(m.dir_path, &m);
        std::vector<SegmentMeta> kept_short;
        for (auto& [merged, named] : short_merges) {
            const fs::path parent = fs::path(merged.dir_path).parent_path();
            const bool all_here = std::all_of(named.begin(), named.end(), [&](const SegmentInput& in) {
                const auto m = by_dir.find((parent / in.dir_name).string());
                return m != by_dir.end() && in.names(*m->second);
            });
            if (all_here) {
                std::error_code ec;
                fs::remove_all(merged.dir_path, ec);
                ++last_rebuild_removed_.short_merges;
                OB_LOG_WARN("columnar", "removed %s: short of its rows, and every segment it replaced is "
                                        "here to hold them%s",
                            merged.dir_path.c_str(), ec ? " (and it could not all be removed)" : "");
            } else {
                OB_LOG_ERROR("columnar", "%s is short of its rows and not every segment it replaced is "
                                         "here: it is kept as it is, and a scan skips what it cannot "
                                         "read whole", merged.dir_path.c_str());
                kept_short.push_back(std::move(merged));
            }
        }
        for (auto& m : kept_short) found.push_back(std::move(m));
    }

    if (!replaced.empty()) {
        // An input is the directory the merged segment names *and* the segment it recorded: the
        // name alone comes back once an input is gone, with a later seal's rows under it.
        std::vector<SegmentMeta> kept;
        kept.reserve(found.size());
        for (auto& meta : found) {
            const auto [first, last] = replaced.equal_range(meta.dir_path);
            if (std::none_of(first, last, [&](const auto& r) { return r.second.names(meta); })) {
                kept.push_back(std::move(meta));
                continue;
            }
            std::error_code ec;
            fs::remove_all(meta.dir_path, ec);
            ++last_rebuild_removed_.superseded;
            OB_LOG_DEBUG("columnar", "removed %s: a merged segment beside it holds its rows%s",
                         meta.dir_path.c_str(), ec ? " (and could not remove all of it)" : "");
        }
        found.swap(kept);
    }
    if (last_rebuild_removed_.staging > 0 || last_rebuild_removed_.superseded > 0) {
        OB_LOG_INFO("columnar",
                    "a merge was cut short: removed %zu working director(ies) nothing published and "
                    "%zu segment(s) a merged segment beside them had replaced",
                    last_rebuild_removed_.staging, last_rebuild_removed_.superseded);
    }

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
        if (!read_timestamps(*meta, ts) || ts.empty() || ts.size() != meta->row_count) {
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
