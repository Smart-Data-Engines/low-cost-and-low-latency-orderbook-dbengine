#include "orderbook/columnar_store.hpp"
#include "orderbook/codec.hpp"

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

void ColumnarStore::write_meta_json(const std::string& dir,
                                     const SegmentMeta& meta) const {
    std::string path = dir + "/meta.json";
    std::ofstream f(path, std::ios::out | std::ios::trunc);
    if (!f.is_open()) {
        throw std::runtime_error("ColumnarStore: cannot write meta.json: " + path);
    }
    f << "{\"start_ts_ns\":" << meta.start_ts_ns
      << ",\"end_ts_ns\":"   << meta.end_ts_ns
      << ",\"row_count\":"   << meta.row_count
      << ",\"first_price\":" << meta.first_price
      << ",\"has_raw_qty\":"  << (meta.has_raw_qty ? "true" : "false")
      << ",\"symbol\":\""    << meta.symbol   << "\""
      << ",\"exchange\":\""  << meta.exchange << "\""
      << "}";
    f.flush();
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

    out.start_ts_ns  = extract_uint64("start_ts_ns");
    out.end_ts_ns    = extract_uint64("end_ts_ns");
    out.row_count    = extract_uint64("row_count");
    out.first_price  = extract_uint64("first_price");
    out.has_raw_qty  = extract_bool("has_raw_qty");
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
    } else if (row.timestamp_ns >= active_segment_start_ + segment_duration_ns_) {
        // Roll over to a new segment
        flush_segment();
        active_segment_start_ = seg_start;
        has_active_segment_   = true;
        active_row_count_     = 0;
        active_has_raw_qty_   = false;
        price_buf_.clear();
        qty_buf_.clear();
        ts_buf_.clear();
        cnt_buf_.clear();
    }

    // Accumulate into buffers
    price_buf_.push_back(row.price);
    qty_buf_.push_back(row.quantity);
    ts_buf_.push_back(row.timestamp_ns);
    cnt_buf_.push_back(row.order_count);
    ++active_row_count_;

    // Check if qty needs fallback (> 2^60 - 1)
    static constexpr uint64_t kMaxSimple8b = (1ULL << 60) - 1;
    if (row.quantity > kMaxSimple8b) {
        active_has_raw_qty_ = true;
    }
}

// ── flush_segment ─────────────────────────────────────────────────────────────

std::optional<SegmentMeta> ColumnarStore::flush_segment() {
    if (!has_active_segment_ || active_row_count_ == 0) {
        has_active_segment_ = false;
        return std::nullopt;
    }

    // Compute end timestamp
    uint64_t end_ts = ts_buf_.empty() ? active_segment_start_
                                      : ts_buf_.back();

    // Build segment directory path
    std::string dir = segment_dir(symbol_, exchange_,
                                   active_segment_start_, end_ts);
    ensure_dirs(dir);

    // Encode price column: delta + zigzag
    auto encoded_prices = encode_prices(price_buf_);

    // Encode qty column: Simple8b
    auto qty_result = encode_simple8b(qty_buf_);

    // Write price.col
    {
        std::string path = dir + "/price.col";
        std::ofstream f(path, std::ios::out | std::ios::binary | std::ios::trunc);
        if (!f.is_open())
            throw std::runtime_error("ColumnarStore: cannot write price.col");
        f.write(reinterpret_cast<const char*>(encoded_prices.data()),
                static_cast<std::streamsize>(encoded_prices.size() * sizeof(uint64_t)));
    }

    // Write qty.col
    {
        std::string path = dir + "/qty.col";
        std::ofstream f(path, std::ios::out | std::ios::binary | std::ios::trunc);
        if (!f.is_open())
            throw std::runtime_error("ColumnarStore: cannot write qty.col");
        f.write(reinterpret_cast<const char*>(qty_result.words.data()),
                static_cast<std::streamsize>(qty_result.words.size() * sizeof(uint64_t)));
    }

    // Write ts.col (raw uint64)
    {
        std::string path = dir + "/ts.col";
        std::ofstream f(path, std::ios::out | std::ios::binary | std::ios::trunc);
        if (!f.is_open())
            throw std::runtime_error("ColumnarStore: cannot write ts.col");
        f.write(reinterpret_cast<const char*>(ts_buf_.data()),
                static_cast<std::streamsize>(ts_buf_.size() * sizeof(uint64_t)));
    }

    // Write cnt.col (raw uint32)
    {
        std::string path = dir + "/cnt.col";
        std::ofstream f(path, std::ios::out | std::ios::binary | std::ios::trunc);
        if (!f.is_open())
            throw std::runtime_error("ColumnarStore: cannot write cnt.col");
        f.write(reinterpret_cast<const char*>(cnt_buf_.data()),
                static_cast<std::streamsize>(cnt_buf_.size() * sizeof(uint32_t)));
    }

    // Build SegmentMeta
    SegmentMeta meta{};
    meta.start_ts_ns = active_segment_start_;
    meta.end_ts_ns   = end_ts;
    meta.row_count   = active_row_count_;
    // first_price: store the zigzag-encoded first price as the anchor
    meta.first_price = encoded_prices.empty() ? 0 : encoded_prices[0];
    meta.has_raw_qty = active_has_raw_qty_ || qty_result.has_fallback;
    meta.symbol      = symbol_;
    meta.exchange    = exchange_;
    meta.dir_path    = dir;

    // Write meta.json
    write_meta_json(dir, meta);

    // Add to index (backward compatibility)
    index_.push_back(meta);

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

void ColumnarStore::scan(uint64_t start_ns, uint64_t end_ns,
                          std::string_view symbol, std::string_view exchange,
                          std::function<void(const SnapshotRow&)> cb) const {
    for (const auto& meta : index_) {
        // Filter by symbol/exchange
        if (meta.symbol != symbol || meta.exchange != exchange) continue;

        // Time-range pruning: skip segments that don't overlap [start_ns, end_ns]
        if (meta.end_ts_ns < start_ns || meta.start_ts_ns > end_ns) continue;

        // Read column files
        const std::string& dir = meta.dir_path;

        // Read price.col
        std::vector<uint64_t> enc_prices;
        {
            std::string path = dir + "/price.col";
            std::ifstream f(path, std::ios::binary);
            if (!f.is_open()) continue;
            f.seekg(0, std::ios::end);
            auto sz = static_cast<size_t>(f.tellg());
            f.seekg(0, std::ios::beg);
            enc_prices.resize(sz / sizeof(uint64_t));
            f.read(reinterpret_cast<char*>(enc_prices.data()),
                   static_cast<std::streamsize>(sz));
        }

        // Read qty.col
        std::vector<uint64_t> enc_qtys;
        {
            std::string path = dir + "/qty.col";
            std::ifstream f(path, std::ios::binary);
            if (!f.is_open()) continue;
            f.seekg(0, std::ios::end);
            auto sz = static_cast<size_t>(f.tellg());
            f.seekg(0, std::ios::beg);
            enc_qtys.resize(sz / sizeof(uint64_t));
            f.read(reinterpret_cast<char*>(enc_qtys.data()),
                   static_cast<std::streamsize>(sz));
        }

        // Read ts.col
        std::vector<uint64_t> timestamps;
        {
            std::string path = dir + "/ts.col";
            std::ifstream f(path, std::ios::binary);
            if (!f.is_open()) continue;
            f.seekg(0, std::ios::end);
            auto sz = static_cast<size_t>(f.tellg());
            f.seekg(0, std::ios::beg);
            timestamps.resize(sz / sizeof(uint64_t));
            f.read(reinterpret_cast<char*>(timestamps.data()),
                   static_cast<std::streamsize>(sz));
        }

        // Read cnt.col
        std::vector<uint32_t> counts;
        {
            std::string path = dir + "/cnt.col";
            std::ifstream f(path, std::ios::binary);
            if (!f.is_open()) continue;
            f.seekg(0, std::ios::end);
            auto sz = static_cast<size_t>(f.tellg());
            f.seekg(0, std::ios::beg);
            counts.resize(sz / sizeof(uint32_t));
            f.read(reinterpret_cast<char*>(counts.data()),
                   static_cast<std::streamsize>(sz));
        }

        // Decode prices
        auto prices = decode_prices(enc_prices);

        // Decode quantities
        auto qtys = decode_simple8b(enc_qtys, meta.row_count);

        // Emit rows within time range
        size_t n = meta.row_count;
        for (size_t i = 0; i < n; ++i) {
            uint64_t ts = (i < timestamps.size()) ? timestamps[i] : 0;
            if (ts < start_ns || ts > end_ns) continue;

            SnapshotRow row{};
            row.timestamp_ns    = ts;
            row.sequence_number = 0;  // not stored in columnar format
            row.side            = 0;  // not stored in columnar format
            row.level_index     = 0;  // not stored in columnar format
            row.price           = (i < prices.size()) ? prices[i] : 0;
            row.quantity        = (i < qtys.size())   ? qtys[i]   : 0;
            row.order_count     = (i < counts.size()) ? counts[i] : 0;
            cb(row);
        }
    }
}

// ── open_existing ─────────────────────────────────────────────────────────────

void ColumnarStore::open_existing() {
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
    std::sort(index_.begin(), index_.end(),
              [](const SegmentMeta& a, const SegmentMeta& b) {
                  return a.start_ts_ns < b.start_ts_ns;
              });
}

// ── merge_segments ────────────────────────────────────────────────────────────

void ColumnarStore::merge_segments(const std::vector<SegmentMeta>& new_segments) {
    if (new_segments.empty()) return;
    index_.insert(index_.end(), new_segments.begin(), new_segments.end());
    std::sort(index_.begin(), index_.end(),
              [](const SegmentMeta& a, const SegmentMeta& b) {
                  return a.start_ts_ns < b.start_ts_ns;
              });
}

// ── delete_expired_segments ───────────────────────────────────────────────────

std::pair<size_t, size_t> ColumnarStore::delete_expired_segments(uint64_t cutoff_ns) {
    if (cutoff_ns == 0) {
        return {0, 0};
    }

    // Sort index by start_ts_ns (oldest first) for chronological deletion order.
    std::sort(index_.begin(), index_.end(),
              [](const SegmentMeta& a, const SegmentMeta& b) {
                  return a.start_ts_ns < b.start_ts_ns;
              });

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
                std::fprintf(stderr,
                    "[retention] ERROR: failed to delete segment %s: %s\n",
                    meta.dir_path.c_str(), rm_ec.message().c_str());
                // Keep the segment in the index since deletion failed.
                remaining.push_back(std::move(meta));
                continue;
            }

            ++segments_deleted;
            bytes_reclaimed += dir_bytes;

            // Log successful deletion: path, age (cutoff - end_ts), size
            uint64_t age_ns = (cutoff_ns > meta.end_ts_ns) ? (cutoff_ns - meta.end_ts_ns) : 0;
            double age_hours = static_cast<double>(age_ns) / 3.6e12;
            std::fprintf(stderr,
                "[retention] deleted segment %s age=%.1fh size=%zu bytes\n",
                meta.dir_path.c_str(), age_hours, dir_bytes);
        } else {
            remaining.push_back(std::move(meta));
        }
    }

    index_ = std::move(remaining);
    return {segments_deleted, bytes_reclaimed};
}

// ── close ─────────────────────────────────────────────────────────────────────

void ColumnarStore::close() {
    if (has_active_segment_ && active_row_count_ > 0) {
        flush_segment();
    }
    has_active_segment_ = false;
}

} // namespace ob
