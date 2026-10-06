// What segment format 3's column blocks cost to write and to read, against what format 2's columns
// cost to read (kiro-workspace/specs/segment-format-v3/, task 3).
//
// Every segment of a data directory is read as format 2 writes it, each column decoded to its
// values, and then, per column:
//
//   - format 2's read: its file's bytes, in memory, to the typed column the store reads into - a
//     copy for the four raw columns, the decoders for price, quantity and sequence number;
//   - each candidate (design §3): its encode, and its decode into the same type;
//   - the choice itself, `column_codec::encode()`, under the engine's policies - a seal's (LZ4 or
//     nothing), a merge's (ZSTD at 3 where it saves 10% over LZ4) and the smallest: what writing
//     a segment costs, and the decode of what it chose.
//
// Bytes in memory on both sides, so this is the codec and nothing of the disk or the page cache.
// Each timing is the fastest of five passes over every segment, in ns a value.
//
// Usage: column_codec_cost <data dir>
#include "orderbook/codec.hpp"
#include "orderbook/column_codec.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <functional>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

namespace fs = std::filesystem;
namespace cc = ob::column_codec;

namespace {

std::optional<std::string> read_file(const fs::path& p) {
    std::ifstream f(p, std::ios::binary);
    if (!f) return std::nullopt;
    std::ostringstream s;
    s << f.rdbuf();
    return s.str();
}

std::optional<uint64_t> json_uint(const std::string& json, const std::string& key) {
    const std::string k = "\"" + key + "\":";
    const auto at = json.find(k);
    if (at == std::string::npos) return std::nullopt;
    return std::strtoull(json.c_str() + at + k.size(), nullptr, 10);
}

template <typename T>
std::vector<T> as_array(const std::string& bytes) {
    std::vector<T> out(bytes.size() / sizeof(T));
    if (!out.empty()) std::memcpy(out.data(), bytes.data(), out.size() * sizeof(T));
    return out;
}

constexpr std::array<const char*, 7> kNames{"ts", "price", "qty", "cnt", "side", "level", "seq"};

struct Segment {
    size_t rows{0};
    std::array<std::string, 7> v2;                  // the format-2 files
    std::array<std::vector<uint64_t>, 7> values;    // each column's values
};

double fastest_ns(const std::function<void()>& pass, size_t values) {
    double best = 1e300;
    for (int r = 0; r < 5; ++r) {
        const auto t0 = std::chrono::steady_clock::now();
        pass();
        const auto t1 = std::chrono::steady_clock::now();
        best = std::min(best, std::chrono::duration<double, std::nano>(t1 - t0).count());
    }
    return values ? best / static_cast<double>(values) : 0.0;
}

// Format 2's read of column `c` from its file's bytes into the store's type for it.
void v2_read(size_t c, const std::string& file, size_t rows) {
    thread_local std::vector<uint64_t> u64, scratch;
    thread_local std::vector<int64_t> i64;
    thread_local std::vector<uint32_t> u32;
    thread_local std::vector<uint8_t> u8;
    thread_local std::vector<uint16_t> u16;
    switch (c) {
    case 0: u64.resize(file.size() / 8); std::memcpy(u64.data(), file.data(), u64.size() * 8); break;
    case 1: ob::decode_prices_into(as_array<uint64_t>(file), i64); break;
    case 2: ob::decode_simple8b_into(as_array<uint64_t>(file), rows, u64); break;
    case 3: u32.resize(file.size() / 4); std::memcpy(u32.data(), file.data(), u32.size() * 4); break;
    case 4: u8.resize(file.size()); std::memcpy(u8.data(), file.data(), u8.size()); break;
    case 5: u16.resize(file.size() / 2); std::memcpy(u16.data(), file.data(), u16.size() * 2); break;
    case 6: ob::decode_simple8b_into(as_array<uint64_t>(file), rows, scratch);
            ob::decode_prices_into(scratch, i64); break;
    default: break;
    }
}

// Format 3's read of column `c`'s block into the same type.
bool v3_read(size_t c, const std::string& block, size_t rows) {
    thread_local std::vector<uint64_t> u64;
    thread_local std::vector<int64_t> i64;
    thread_local std::vector<uint32_t> u32;
    thread_local std::vector<uint8_t> u8;
    thread_local std::vector<uint16_t> u16;
    const std::span<const char> b(block.data(), block.size());
    switch (c) {
    case 0: case 2: return cc::decode_as(b, rows, u64, nullptr);
    case 1: case 6: return cc::decode_as(b, rows, i64, nullptr);
    case 3: return cc::decode_as(b, rows, u32, nullptr);
    case 4: return cc::decode_as(b, rows, u8, nullptr);
    case 5: return cc::decode_as(b, rows, u16, nullptr);
    default: return false;
    }
}

std::vector<cc::Encoding> candidates() {
    std::vector<cc::Encoding> out = {
        {cc::Transform::kFor, cc::Packing::kSimple8b, cc::Compressor::kNone},
        {cc::Transform::kFor, cc::Packing::kRuns, cc::Compressor::kNone},
        {cc::Transform::kFor, cc::Packing::kBlocks, cc::Compressor::kNone},
        {cc::Transform::kDelta, cc::Packing::kSimple8b, cc::Compressor::kNone},
        {cc::Transform::kDelta, cc::Packing::kRuns, cc::Compressor::kNone},
    };
    for (cc::Compressor z : {cc::Compressor::kLz4, cc::Compressor::kZstd}) {
        for (cc::Transform t : {cc::Transform::kFor, cc::Transform::kDelta}) {
            for (cc::Packing p : {cc::Packing::kRuns, cc::Packing::kNarrow}) out.push_back({t, p, z});
        }
    }
    return out;
}

/// The engine's two policies (engine.hpp): a seal's, LZ4 or nothing, and a merge's, ZSTD at 3
/// where it saves 10% over LZ4.
struct Policy {
    const char* name;
    cc::EncodeOptions options;
};
std::vector<Policy> policies() {
    cc::EncodeOptions seal;
    seal.lz4 = true;
    seal.zstd_level = 0;
    cc::EncodeOptions merge;
    merge.lz4 = true;
    merge.zstd_level = 3;
    merge.zstd_margin_pct = 10;
    cc::EncodeOptions smallest;
    smallest.lz4 = true;
    smallest.zstd_level = 3;
    return {{"seal", seal}, {"merge", merge}, {"smallest", smallest}};
}

}  // namespace

int main(int argc, char** argv) {
    if (argc < 2) {
        std::fprintf(stderr, "usage: %s <data dir>\n", argv[0]);
        return 2;
    }
    std::vector<Segment> segs;
    size_t rows = 0;
    for (const auto& entry : fs::recursive_directory_iterator(argv[1])) {
        if (!entry.is_regular_file() || entry.path().filename() != "meta.json") continue;
        const auto meta = read_file(entry.path());
        if (!meta || json_uint(*meta, "format_version").value_or(0) != 2) continue;
        Segment s;
        s.rows = static_cast<size_t>(json_uint(*meta, "row_count").value_or(0));
        bool ok = s.rows > 0;
        for (size_t c = 0; c < 7 && ok; ++c) {
            const auto f = read_file(entry.path().parent_path() / (std::string(kNames[c]) + ".col"));
            ok = f.has_value();
            if (ok) s.v2[c] = *f;
        }
        if (!ok) continue;
        auto& v = s.values;
        v[0] = as_array<uint64_t>(s.v2[0]);
        for (int64_t p : ob::decode_prices(as_array<uint64_t>(s.v2[1]))) v[1].push_back(static_cast<uint64_t>(p));
        v[2] = ob::decode_simple8b(as_array<uint64_t>(s.v2[2]), s.rows);
        for (uint32_t x : as_array<uint32_t>(s.v2[3])) v[3].push_back(x);
        for (uint8_t x : as_array<uint8_t>(s.v2[4])) v[4].push_back(x);
        for (uint16_t x : as_array<uint16_t>(s.v2[5])) v[5].push_back(x);
        for (int64_t q : ob::decode_prices(ob::decode_simple8b(as_array<uint64_t>(s.v2[6]), s.rows))) {
            v[6].push_back(static_cast<uint64_t>(q));
        }
        if (std::any_of(v.begin(), v.end(), [&](const auto& col) { return col.size() != s.rows; })) continue;
        rows += s.rows;
        segs.push_back(std::move(s));
    }
    if (segs.empty()) {
        std::fprintf(stderr, "no format-2 segments under %s\n", argv[1]);
        return 1;
    }

    std::printf("{\n  \"rows\": %zu, \"segments\": %zu,\n  \"columns\": {\n", rows, segs.size());
    for (size_t c = 0; c < 7; ++c) {
        const double v2_ns = fastest_ns([&] { for (const auto& s : segs) v2_read(c, s.v2[c], s.rows); }, rows);
        size_t v2_bytes = 0;
        for (const auto& s : segs) v2_bytes += s.v2[c].size();
        std::printf("    \"%s\": {\"v2\": {\"bytes_per_row\": %.3f, \"read_ns\": %.2f}, \"candidates\": {",
                    kNames[c], static_cast<double>(v2_bytes) / rows, v2_ns);
        bool first = true;
        for (const cc::Encoding& e : candidates()) {
            for (int level : {1, 3}) {
                if (e.compressor != cc::Compressor::kZstd && level == 3) continue;
                std::vector<std::string> blocks(segs.size());
                const double enc_ns = fastest_ns([&] {
                    for (size_t i = 0; i < segs.size(); ++i) {
                        blocks[i].clear();
                        cc::encode_as(segs[i].values[c], e, level, blocks[i]);
                    }
                }, rows);
                size_t bytes = 0;
                bool ok = true;
                for (size_t i = 0; i < segs.size(); ++i) {
                    bytes += blocks[i].size();
                    ok = ok && v3_read(c, blocks[i], segs[i].rows);
                }
                const double dec_ns = fastest_ns([&] {
                    for (size_t i = 0; i < segs.size(); ++i) v3_read(c, blocks[i], segs[i].rows);
                }, rows);
                const std::string key = e.name() + (e.compressor == cc::Compressor::kZstd ? std::to_string(level) : "");
                std::printf("%s\"%s\": {\"bytes_per_row\": %.3f, \"encode_ns\": %.2f, \"decode_ns\": %.2f%s}",
                            first ? "" : ", ", key.c_str(), static_cast<double>(bytes) / rows, enc_ns, dec_ns,
                            ok ? "" : ", \"decode_failed\": true");
                first = false;
            }
        }
        std::printf("}, \"choice\": {");
        first = true;
        for (const Policy& policy : policies()) {
            std::vector<std::string> blocks(segs.size());
            std::vector<cc::Choice> chosen(segs.size());
            const double enc_ns = fastest_ns([&] {
                for (size_t i = 0; i < segs.size(); ++i) {
                    blocks[i].clear();
                    chosen[i] = cc::encode(segs[i].values[c], policy.options, blocks[i]);
                }
            }, rows);
            size_t bytes = 0;
            for (const auto& b : blocks) bytes += b.size();
            const double dec_ns = fastest_ns([&] {
                for (size_t i = 0; i < segs.size(); ++i) v3_read(c, blocks[i], segs[i].rows);
            }, rows);
            size_t lz4_segments = 0, zstd_segments = 0;
            for (const auto& ch : chosen) {
                lz4_segments += ch.encoding.compressor == cc::Compressor::kLz4;
                zstd_segments += ch.encoding.compressor == cc::Compressor::kZstd;
            }
            std::printf("%s\"%s\": {\"bytes_per_row\": %.3f, \"encode_ns\": %.2f, \"decode_ns\": %.2f, "
                        "\"segments_lz4\": %zu, \"segments_zstd\": %zu, \"first_choice\": \"%s\"}",
                        first ? "" : ", ", policy.name, static_cast<double>(bytes) / rows, enc_ns, dec_ns,
                        lz4_segments, zstd_segments, chosen[0].encoding.name().c_str());
            first = false;
        }
        std::printf("}}%s\n", c + 1 < 7 ? "," : "");
    }
    std::printf("  }\n}\n");
    return 0;
}
