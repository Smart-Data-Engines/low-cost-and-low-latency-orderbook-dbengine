// What each column of a store's segments takes, and what it would take under each candidate
// encoding (segment format v3, kiro-workspace/specs/segment-format-v3/).
//
// Format 2 writes the timestamp, the order count, the side and the level raw, and the price as one
// zigzag delta a word: fifteen and eight of a row's twenty-five bytes on the comparative dataset.
// Which encoding replaces each is a measurement, column by column, and this is it: every segment
// of a data directory is read as it is on the disk, every column decoded to its values, and each
// column encoded again every way below, counting the bytes - including what an encoding has to
// keep beside its words (an anchor, a scale), so that no candidate wins by leaving that out.
//
// A candidate is a transform, a scale and a packing:
//   - transform: none; `for` (minus the column's minimum); `delta` (each value minus the one
//     before, the first kept as the anchor); `dod` (the delta of the deltas);
//   - scale: whether the transformed values are divided by their greatest common divisor - a price
//     on a tick, a quantity in lots;
//   - packing: Simple8b, as format 2 packs quantities; runs (each run of equal values as its value
//     and its length, both in Simple8b); fixed-width blocks of 128 values, each at the width of
//     its largest; or the values as they are, eight bytes each (only under a compressor);
//   - and then, or not, a general-purpose compressor over the packed bytes: LZ4, which the engine
//     links already, and - when built with `OB_PROBE_ZSTD` - ZSTD at levels 1, 3 and 9. What a
//     compressor adds is what requirement 2 asks about if the target is out of reach without one,
//     and what repeats from one update to the next - a book's levels between two snapshots - is
//     what no transform of one column sees.
// Signed results are zigzagged before packing. LZ4 over the format-2 file is beside them, for scale.
//
// Usage: segment_encoding_probe <data dir>
//
// Prints one JSON object: rows, segments, and per column the bytes of its format-2 files, of every
// candidate summed over the segments, the best single candidate for the whole column, and the sum
// of each segment's own best (what a writer choosing per segment would get).
#include "orderbook/codec.hpp"

#include <lz4.h>
#ifdef OB_PROBE_ZSTD
#include <zstd.h>
#endif

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <map>
#include <numeric>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

namespace fs = std::filesystem;

namespace {

// Bytes every candidate is charged per column of a segment: an encoding id, the count, an anchor
// and a scale, whether it uses them or not - so that none wins by what it leaves out.
constexpr size_t kHeaderBytes = 1 + 4 + 8 + 8;

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

uint64_t zigzag(int64_t v) { return (static_cast<uint64_t>(v) << 1) ^ static_cast<uint64_t>(v >> 63); }

unsigned width(uint64_t v) { return v == 0 ? 0 : 64 - static_cast<unsigned>(__builtin_clzll(v)); }

uint64_t magnitude(int64_t v) { return v < 0 ? 0 - static_cast<uint64_t>(v) : static_cast<uint64_t>(v); }

enum class Transform { kNone, kFor, kDelta, kDod };
enum class Packing { kSimple8b, kRuns, kBlocks, kRaw64, kNarrow, kSplit };

constexpr std::array<Transform, 4> kTransforms{Transform::kNone, Transform::kFor, Transform::kDelta,
                                               Transform::kDod};
constexpr std::array<Packing, 6> kPackings{Packing::kSimple8b, Packing::kRuns, Packing::kBlocks,
                                          Packing::kRaw64, Packing::kNarrow, Packing::kSplit};

const char* name(Transform t) {
    switch (t) {
    case Transform::kNone:  return "none";
    case Transform::kFor:   return "for";
    case Transform::kDelta: return "delta";
    case Transform::kDod:   return "dod";
    }
    return "?";
}

const char* name(Packing p) {
    switch (p) {
    case Packing::kSimple8b: return "s8b";
    case Packing::kRuns:     return "runs";
    case Packing::kBlocks:   return "blocks";
    case Packing::kRaw64:    return "raw64";
    case Packing::kNarrow:   return "narrow";
    case Packing::kSplit:    return "split";
    }
    return "?";
}

/// The values to pack: the transform applied, scaled when asked, signed ones zigzagged.
std::vector<uint64_t> transformed(const std::vector<uint64_t>& v, Transform t, bool scale) {
    std::vector<uint64_t> out;
    if (v.empty()) return out;
    if (t == Transform::kNone || t == Transform::kFor) {
        const uint64_t base = t == Transform::kFor ? *std::min_element(v.begin(), v.end()) : 0;
        out.reserve(v.size());
        for (uint64_t x : v) out.push_back(x - base);
        if (scale) {
            uint64_t g = 0;
            for (uint64_t x : out) g = std::gcd(g, x);
            if (g > 1) for (uint64_t& x : out) x /= g;
        }
        return out;
    }
    // Signed differences, in wrapping arithmetic as encode_prices() computes them.
    std::vector<int64_t> d;
    d.reserve(v.size());
    for (size_t i = 1; i < v.size(); ++i) d.push_back(static_cast<int64_t>(v[i] - v[i - 1]));
    if (t == Transform::kDod) {
        for (size_t i = d.size(); i-- > 1;) {
            d[i] = static_cast<int64_t>(static_cast<uint64_t>(d[i]) - static_cast<uint64_t>(d[i - 1]));
        }
    }
    if (scale) {
        uint64_t g = 0;
        for (int64_t x : d) g = std::gcd(g, magnitude(x));
        if (g > 1) for (int64_t& x : d) x /= static_cast<int64_t>(g);
    }
    out.reserve(d.size());
    for (int64_t x : d) out.push_back(zigzag(x));
    return out;   // the first value is the anchor, in the header
}

std::string words_bytes(const std::vector<uint64_t>& w) {
    return std::string(reinterpret_cast<const char*>(w.data()), w.size() * sizeof(uint64_t));
}

/// The packed bytes, or - for blocks, which nothing here compresses further - only their count,
/// with `bytes` empty.
size_t packed(const std::vector<uint64_t>& v, Packing p, std::string& bytes) {
    bytes.clear();
    switch (p) {
    case Packing::kSimple8b:
        bytes = words_bytes(ob::encode_simple8b(v).words);
        return bytes.size();
    case Packing::kRuns: {
        std::vector<uint64_t> values, lengths;
        for (size_t i = 0; i < v.size();) {
            size_t j = i + 1;
            while (j < v.size() && v[j] == v[i]) ++j;
            values.push_back(v[i]);
            lengths.push_back(j - i - 1);
            i = j;
        }
        bytes = words_bytes(ob::encode_simple8b(values).words) + words_bytes(ob::encode_simple8b(lengths).words);
        return bytes.size();
    }
    case Packing::kBlocks: {
        size_t n_bytes = 0;
        for (size_t i = 0; i < v.size(); i += 128) {
            const size_t n = std::min<size_t>(128, v.size() - i);
            unsigned w = 0;
            for (size_t k = 0; k < n; ++k) w = std::max(w, width(v[i + k]));
            n_bytes += 1 + (n * w + 7) / 8;
        }
        return n_bytes;
    }
    case Packing::kRaw64:
        bytes = words_bytes(v);
        return bytes.size();
    case Packing::kNarrow:
    case Packing::kSplit: {
        // Each value in as many bytes as the column's largest needs; `split` writes the lowest
        // byte of every value, then the next byte of every value, and so on (Parquet's
        // BYTE_STREAM_SPLIT), so that a compressor sees each byte position's distribution apart.
        unsigned w = 0;
        for (uint64_t x : v) w = std::max(w, (width(x) + 7) / 8);
        bytes.resize(v.size() * w);
        for (size_t i = 0; i < v.size(); ++i) {
            for (unsigned b = 0; b < w; ++b) {
                const char byte = static_cast<char>((v[i] >> (8 * b)) & 0xff);
                if (p == Packing::kNarrow) bytes[i * w + b] = byte;
                else bytes[b * v.size() + i] = byte;
            }
        }
        return bytes.size();
    }
    }
    return 0;
}

size_t lz4_bytes(const char* data, size_t n) {
    if (n == 0) return 0;
    std::vector<char> out(static_cast<size_t>(LZ4_compressBound(static_cast<int>(n))));
    const int c = LZ4_compress_default(data, out.data(), static_cast<int>(n), static_cast<int>(out.size()));
    return c > 0 ? static_cast<size_t>(c) : n;
}

#ifdef OB_PROBE_ZSTD
size_t zstd_bytes(const std::string& in, int level) {
    if (in.empty()) return 0;
    std::vector<char> out(ZSTD_compressBound(in.size()));
    const size_t c = ZSTD_compress(out.data(), out.size(), in.data(), in.size(), level);
    return ZSTD_isError(c) ? in.size() : c;
}
constexpr std::array<int, 3> kZstdLevels{1, 3, 9};
#endif

struct Column {
    const char* name{nullptr};
    size_t v2_bytes{0};
    size_t lz4_v2_bytes{0};
    size_t lz4_best_bytes{0};
    size_t adaptive_bytes{0};
    std::map<std::string, size_t> candidates;
};

void probe(Column& c, const std::vector<uint64_t>& values, const std::string& v2_file) {
    c.v2_bytes += v2_file.size();
    c.lz4_v2_bytes += lz4_bytes(v2_file.data(), v2_file.size());
    size_t best = SIZE_MAX;
    size_t best_lz4 = SIZE_MAX;
    auto add = [&](const std::string& key, size_t bytes) {
        bytes += kHeaderBytes;
        c.candidates[key] += bytes;
        best = std::min(best, bytes);
    };
    std::string bytes;
    for (Transform t : kTransforms) {
        for (bool scale : {false, true}) {
            const auto x = transformed(values, t, scale);
            for (Packing p : kPackings) {
                const std::string key = std::string(name(t)) + (scale ? "+gcd" : "") + "/" + name(p);
                const size_t n = packed(x, p, bytes);
                if (p == Packing::kSimple8b || p == Packing::kRuns || p == Packing::kBlocks) add(key, n);
                if (p == Packing::kBlocks) continue;
                const size_t l = lz4_bytes(bytes.data(), bytes.size());
                add(key + "+lz4", l);
                best_lz4 = std::min(best_lz4, l + kHeaderBytes);
#ifdef OB_PROBE_ZSTD
                for (int level : kZstdLevels) add(key + "+zstd" + std::to_string(level), zstd_bytes(bytes, level));
#endif
            }
        }
    }
    c.adaptive_bytes += best;
    c.lz4_best_bytes += best_lz4;
}

}  // namespace

int main(int argc, char** argv) {
    if (argc < 2) {
        std::fprintf(stderr, "usage: %s <data dir>\n", argv[0]);
        return 2;
    }
    std::array<Column, 7> cols;
    const std::array<const char*, 7> names{"ts", "price", "qty", "cnt", "side", "level", "seq"};
    for (size_t i = 0; i < cols.size(); ++i) cols[i].name = names[i];
    size_t rows = 0, segments = 0, skipped = 0, meta_bytes = 0;
    for (const auto& entry : fs::recursive_directory_iterator(argv[1])) {
        if (!entry.is_regular_file() || entry.path().filename() != "meta.json") continue;
        const fs::path dir = entry.path().parent_path();
        const auto meta = read_file(entry.path());
        if (!meta) { ++skipped; continue; }
        const auto version = json_uint(*meta, "format_version");
        const auto count = json_uint(*meta, "row_count");
        if (!version || *version != 2 || !count) { ++skipped; continue; }
        const size_t n = static_cast<size_t>(*count);

        std::array<std::string, 7> files;
        bool ok = true;
        for (size_t i = 0; i < cols.size() && ok; ++i) {
            const auto f = read_file(dir / (std::string(cols[i].name) + ".col"));
            ok = f.has_value();
            if (ok) files[i] = *f;
        }
        if (!ok) { ++skipped; continue; }

        std::array<std::vector<uint64_t>, 7> v;
        v[0] = as_array<uint64_t>(files[0]);
        for (int64_t p : ob::decode_prices(as_array<uint64_t>(files[1]))) v[1].push_back(static_cast<uint64_t>(p));
        v[2] = ob::decode_simple8b(as_array<uint64_t>(files[2]), n);
        for (uint32_t x : as_array<uint32_t>(files[3])) v[3].push_back(x);
        for (uint8_t x : as_array<uint8_t>(files[4])) v[4].push_back(x);
        for (uint16_t x : as_array<uint16_t>(files[5])) v[5].push_back(x);
        for (int64_t s : ob::decode_prices(ob::decode_simple8b(as_array<uint64_t>(files[6]), n))) {
            v[6].push_back(static_cast<uint64_t>(s));
        }
        if (std::any_of(v.begin(), v.end(), [&](const auto& c) { return c.size() != n; })) {
            ++skipped;
            continue;
        }
        for (size_t i = 0; i < cols.size(); ++i) probe(cols[i], v[i], files[i]);
        rows += n;
        meta_bytes += meta->size();
        ++segments;
    }

    const double r = rows ? static_cast<double>(rows) : 1.0;
    std::printf("{\n  \"rows\": %zu, \"segments\": %zu, \"skipped\": %zu, \"meta_json_bytes\": %zu,\n",
                rows, segments, skipped, meta_bytes);
    std::printf("  \"header_bytes_charged_per_column_and_segment\": %zu,\n  \"columns\": {\n", kHeaderBytes);
    size_t v2_total = 0, fixed_total = 0, adaptive_total = 0;
    for (size_t i = 0; i < cols.size(); ++i) {
        const Column& c = cols[i];
        const auto best = std::min_element(c.candidates.begin(), c.candidates.end(),
                                           [](const auto& a, const auto& b) { return a.second < b.second; });
        v2_total += c.v2_bytes;
        fixed_total += best->second;
        adaptive_total += c.adaptive_bytes;
        std::printf("    \"%s\": {\"v2\": %.3f, \"lz4_v2\": %.3f, \"best\": \"%s\", \"best_bpr\": %.3f, "
                    "\"adaptive\": %.3f, \"best_lz4\": %.3f, \"candidates\": {",
                    c.name, c.v2_bytes / r, c.lz4_v2_bytes / r, best->first.c_str(), best->second / r,
                    c.adaptive_bytes / r, c.lz4_best_bytes / r);
        bool first = true;
        for (const auto& [k, b] : c.candidates) {
            std::printf("%s\"%s\": %.3f", first ? "" : ", ", k.c_str(), b / r);
            first = false;
        }
        std::printf("}}%s\n", i + 1 < cols.size() ? "," : "");
    }
    std::printf("  },\n  \"bytes_per_row\": {\"v2_columns\": %.3f, \"best_fixed\": %.3f, \"adaptive\": %.3f, "
                "\"meta_json\": %.3f}\n}\n",
                v2_total / r, fixed_total / r, adaptive_total / r, meta_bytes / r);
    return 0;
}
