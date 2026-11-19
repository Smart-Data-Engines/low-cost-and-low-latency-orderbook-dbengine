// What a seal's encoding of its columns costs, as a store pays it (#267).
//
// Every segment of a data directory is read as format 2 writes it, each column decoded to the values
// a seal hands the codec, and the segments are then sealed again and again, in order, as one store
// seals them: each column in the encoding the store's last segment chose for it, and every
// `search_every`-th segment - the first included - searched over the candidates
// (`ColumnarStore::write_columns_v3()`, design §3 of segment format 3), under the engine's own seal
// policy (`ColumnarStore::SegmentFormat::seal()`).
//
// What comes out, as one JSON object: the rows a pass, the passes and the seals; the nanoseconds a
// row of every seal, of the searched ones and of the others, each the fastest of `rounds` runs; the
// bytes a row; how many seals wrote each column in each encoding; and a hash of every block every seal
// wrote, so two builds that should write the same bytes can be compared by one number.
//
// Bytes in memory on both sides, so this is the codec and nothing of the disk.
//
// Usage: seal_encode_cost <data dir> [format: 3 or 4] [passes] [rounds] [search_every]
#include "orderbook/codec.hpp"
#include "orderbook/column_codec.hpp"
#include "orderbook/columnar_store.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <map>
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

/// A segment's columns as a seal hands them to the codec: every one as uint64, the signed ones
/// reinterpreted and the narrow ones widened.
struct Segment {
    size_t rows{0};
    std::array<std::vector<uint64_t>, 7> values;
};

/// A symbol's segments, in the order its store sealed them.
struct Store {
    std::vector<Segment> segments;
};

std::vector<Store> load(const char* dir, size_t& rows) {
    std::vector<std::pair<std::string, Segment>> found;
    for (const auto& entry : fs::recursive_directory_iterator(dir)) {
        if (!entry.is_regular_file() || entry.path().filename() != "meta.json") continue;
        const auto meta = read_file(entry.path());
        if (!meta || json_uint(*meta, "format_version").value_or(0) != 2) continue;
        Segment s;
        s.rows = static_cast<size_t>(json_uint(*meta, "row_count").value_or(0));
        std::array<std::string, 7> files;
        bool ok = s.rows > 0;
        for (size_t c = 0; c < 7 && ok; ++c) {
            const auto f = read_file(entry.path().parent_path() / (std::string(kNames[c]) + ".col"));
            ok = f.has_value();
            if (ok) files[c] = *f;
        }
        if (!ok) continue;
        auto& v = s.values;
        v[0] = as_array<uint64_t>(files[0]);
        for (int64_t p : ob::decode_prices(as_array<uint64_t>(files[1]))) v[1].push_back(static_cast<uint64_t>(p));
        v[2] = ob::decode_simple8b(as_array<uint64_t>(files[2]), s.rows);
        for (uint32_t x : as_array<uint32_t>(files[3])) v[3].push_back(x);
        for (uint8_t x : as_array<uint8_t>(files[4])) v[4].push_back(x);
        for (uint16_t x : as_array<uint16_t>(files[5])) v[5].push_back(x);
        for (int64_t q : ob::decode_prices(ob::decode_simple8b(as_array<uint64_t>(files[6]), s.rows))) {
            v[6].push_back(static_cast<uint64_t>(q));
        }
        if (std::any_of(v.begin(), v.end(), [&](const auto& col) { return col.size() != s.rows; })) continue;
        found.emplace_back(entry.path().parent_path().string(), std::move(s));
    }
    // A directory walk's order is the filesystem's; a store seals its segments in time order, and
    // their directories' names begin with it, under the store's own directory - <symbol>/<exchange>.
    std::sort(found.begin(), found.end(), [](const auto& a, const auto& b) { return a.first < b.first; });
    std::vector<Store> stores;
    rows = 0;
    std::string store_dir;
    for (auto& [path, s] : found) {
        const std::string dir = fs::path(path).parent_path().string();
        if (stores.empty() || dir != store_dir) {
            stores.emplace_back();
            store_dir = dir;
        }
        rows += s.rows;
        stores.back().segments.push_back(std::move(s));
    }
    return stores;
}

/// FNV-1a over a block, folded into `h`.
uint64_t fold(uint64_t h, const std::string& block) {
    for (unsigned char b : block) {
        h ^= b;
        h *= 0x100000001b3ULL;
    }
    // The block's length too, so two blocks cannot trade bytes across their boundary.
    h ^= block.size();
    h *= 0x100000001b3ULL;
    return h;
}

struct Run {
    double all_ns{0}, searched_ns{0}, hinted_ns{0};
    size_t searched_rows{0}, hinted_rows{0}, bytes{0}, seals{0};
    uint64_t hash{0xcbf29ce484222325ULL};
    std::array<std::map<std::string, size_t>, 7> written;   ///< each column's encodings, by seals
};

/// What write_columns_v3() keeps of a store's last seal.
struct Hints {
    std::array<cc::Encoding, 7> last_choice{};
    bool has_last_choice{false};
    uint32_t segments_since_search{0};
};

/// Every store's seals of its segments, `passes` times over, as write_columns_v3() makes them: each
/// store with its own hints.
Run seal_all(const std::vector<Store>& stores, unsigned format, size_t passes, uint32_t search_every) {
    const ob::ColumnarStore::SegmentFormat policy = ob::ColumnarStore::SegmentFormat::seal();
    Run run;
    std::vector<Hints> hints(stores.size());
    std::array<std::string, 7> blocks;
    for (size_t p = 0; p < passes; ++p) {
        for (size_t k = 0; k < stores.size(); ++k) for (const Segment& s : stores[k].segments) {
            auto& [last_choice, has_last_choice, segments_since_search] = hints[k];
            const bool search = !has_last_choice || segments_since_search + 1 >= search_every;
            const auto t0 = std::chrono::steady_clock::now();
            for (size_t c = 0; c < 7; ++c) {
                cc::EncodeOptions options = policy.search;
                options.hint = search ? nullptr : &last_choice[c];
                options.steps = format == 4;
                blocks[c].clear();
                last_choice[c] = cc::encode(s.values[c], options, blocks[c]).encoding;
            }
            const auto t1 = std::chrono::steady_clock::now();
            has_last_choice = true;
            segments_since_search = search ? 0 : segments_since_search + 1;
            const double ns = std::chrono::duration<double, std::nano>(t1 - t0).count();
            run.all_ns += ns;
            (search ? run.searched_ns : run.hinted_ns) += ns;
            (search ? run.searched_rows : run.hinted_rows) += s.rows;
            for (size_t c = 0; c < 7; ++c) ++run.written[c][last_choice[c].name()];
            for (const std::string& b : blocks) {
                run.bytes += b.size();
                run.hash = fold(run.hash, b);
            }
            ++run.seals;
        }
    }
    return run;
}

}  // namespace

int main(int argc, char** argv) {
    if (argc < 2) {
        std::fprintf(stderr, "usage: %s <data dir> [format: 3 or 4] [passes] [rounds] [search_every]\n",
                     argv[0]);
        return 2;
    }
    const unsigned format = argc > 2 ? static_cast<unsigned>(std::strtoul(argv[2], nullptr, 10)) : 3;
    const size_t passes = argc > 3 ? std::strtoull(argv[3], nullptr, 10) : 16;
    const int rounds = argc > 4 ? std::atoi(argv[4]) : 5;
    const uint32_t search_every = argc > 5 ? static_cast<uint32_t>(std::strtoul(argv[5], nullptr, 10))
                                           : ob::ColumnarStore::SegmentFormat::seal().search_every;
    if ((format != 3 && format != 4) || passes == 0 || rounds <= 0 || search_every == 0) {
        std::fprintf(stderr, "format is 3 or 4, and passes, rounds and search_every are at least 1\n");
        return 2;
    }
    size_t rows = 0;
    const std::vector<Store> stores = load(argv[1], rows);
    size_t segments = 0;
    for (const Store& st : stores) segments += st.segments.size();
    if (stores.empty()) {
        std::fprintf(stderr, "no format-2 segments under %s\n", argv[1]);
        return 1;
    }

    // The fastest of the rounds for each figure, and every round's hash, which must agree.
    Run best;
    double best_all = 1e300, best_searched = 1e300, best_hinted = 1e300;
    for (int r = 0; r < rounds; ++r) {
        const Run run = seal_all(stores, format, passes, search_every);
        if (r > 0 && run.hash != best.hash) {
            std::fprintf(stderr, "round %d wrote other bytes than round 0: %016llx against %016llx\n", r,
                         static_cast<unsigned long long>(run.hash), static_cast<unsigned long long>(best.hash));
            return 1;
        }
        if (r == 0) best = run;
        best_all = std::min(best_all, run.all_ns);
        best_searched = std::min(best_searched, run.searched_ns);
        best_hinted = std::min(best_hinted, run.hinted_ns);
    }
    const double all_rows = static_cast<double>(rows * passes);
    std::printf("{\"rows\": %zu, \"stores\": %zu, \"segments\": %zu, \"format\": %u, \"passes\": %zu, \"rounds\": %d, "
                "\"search_every\": %u, \"seals\": %zu, \"ns_per_row\": %.2f, \"searched_ns_per_row\": %.2f, "
                "\"hinted_ns_per_row\": %.2f, \"searched_rows\": %zu, \"hinted_rows\": %zu, "
                "\"bytes_per_row\": %.4f, \"hash\": \"%016llx\", \"written\": {",
                rows, stores.size(), segments, format, passes, rounds, search_every, best.seals, best_all / all_rows,
                best.searched_rows ? best_searched / static_cast<double>(best.searched_rows) : 0.0,
                best.hinted_rows ? best_hinted / static_cast<double>(best.hinted_rows) : 0.0,
                best.searched_rows, best.hinted_rows, static_cast<double>(best.bytes) / all_rows,
                static_cast<unsigned long long>(best.hash));
    for (size_t c = 0; c < 7; ++c) {
        std::printf("%s\"%s\": {", c ? ", " : "", kNames[c]);
        bool first = true;
        for (const auto& [name, seals] : best.written[c]) {
            std::printf("%s\"%s\": %zu", first ? "" : ", ", name.c_str(), seals);
            first = false;
        }
        std::printf("}");
    }
    std::printf("}}\n");
    return 0;
}
