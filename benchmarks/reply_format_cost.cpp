// What a reply's numbers cost to write (#251).
//
// Every segment of a data directory is read as format 2 writes it and each column decoded to its
// values, as `column_codec_cost` reads them. Then, per column, its values are written as decimal
// text by `std::to_chars` - what a reply's rows were written by before #251 - and by
// `decimal_text::write()`, both in this one build, so the comparison carries no code layout of two
// builds (pitfall 634); the two texts are compared byte for byte on the way, and a difference is an
// exit status of 1. And the whole of a `SELECT *` reply over the same rows, as the server builds one:
// `QueryResponseBuilder::add_rows()` a batch of `kScanBatchRows` at a time, then `finish()`; and a
// subscription's push of each row, `format_push()`.
//
// Bytes in memory on both sides: nothing of the disk or the socket. Each timing is the fastest of
// five passes over every segment, in ns a value or a row. One JSON object out.
//
// Usage: reply_format_cost <data dir>
#include "orderbook/codec.hpp"
#include "orderbook/columnar_store.hpp"
#include "orderbook/decimal_text.hpp"
#include "orderbook/query_columns.hpp"
#include "orderbook/query_engine.hpp"
#include "orderbook/response_formatter.hpp"

#include <algorithm>
#include <array>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <functional>
#include <numeric>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

namespace fs = std::filesystem;
namespace dt = ob::decimal_text;

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

/// One segment's columns, typed as a scan hands them to a reply.
struct Segment {
    size_t rows{0};
    std::vector<uint64_t> ts;
    std::vector<int64_t>  price;
    std::vector<uint64_t> qty;
    std::vector<uint32_t> cnt;
    std::vector<uint8_t>  side;
    std::vector<uint16_t> level;
    std::vector<int64_t>  seq;
};

double fastest_ns(const std::function<void()>& pass, size_t count) {
    double best = 1e300;
    for (int r = 0; r < 5; ++r) {
        const auto t0 = std::chrono::steady_clock::now();
        pass();
        const auto t1 = std::chrono::steady_clock::now();
        best = std::min(best, std::chrono::duration<double, std::nano>(t1 - t0).count());
    }
    return count ? best / static_cast<double>(count) : 0.0;
}

/// A column's values, each with a tab after it, into `out` - by to_chars or by write(), into a
/// buffer with the room write() needs past its last value.
template <typename T>
size_t write_column(const std::vector<T>& values, std::vector<char>& out, bool fast) {
    out.resize(values.size() * 21 + dt::kWriteRoom);
    char* p = out.data();
    char* const end = out.data() + out.size();
    for (const T v : values) {
        if (fast) {
            if constexpr (std::is_signed_v<T>) {
                p = dt::write(p, static_cast<int64_t>(v));
            } else {
                p = dt::write(p, static_cast<uint64_t>(v));
            }
        } else {
            if constexpr (std::is_signed_v<T>) {
                p = std::to_chars(p, end, static_cast<long long>(v)).ptr;
            } else {
                p = std::to_chars(p, end, static_cast<unsigned long long>(v)).ptr;
            }
        }
        *p++ = '\t';
    }
    return static_cast<size_t>(p - out.data());
}

struct ColumnCost {
    double to_chars_ns{0};
    double write_ns{0};
    double bytes_per_value{0};
    bool same{true};
};

template <typename T>
ColumnCost column_cost(const std::vector<Segment>& segs, std::vector<T> Segment::*column, size_t rows) {
    ColumnCost c;
    std::vector<char> a;
    std::vector<char> b;
    size_t bytes = 0;
    for (const Segment& s : segs) {
        const size_t na = write_column(s.*column, a, false);
        const size_t nb = write_column(s.*column, b, true);
        c.same = c.same && na == nb && std::memcmp(a.data(), b.data(), na) == 0;
        bytes += na;
    }
    c.bytes_per_value = static_cast<double>(bytes) / static_cast<double>(rows);
    c.to_chars_ns = fastest_ns([&] { for (const Segment& s : segs) write_column(s.*column, a, false); }, rows);
    c.write_ns    = fastest_ns([&] { for (const Segment& s : segs) write_column(s.*column, b, true); }, rows);
    return c;
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
        std::array<std::string, 7> files;
        bool ok = s.rows > 0;
        constexpr std::array<const char*, 7> kNames{"ts", "price", "qty", "cnt", "side", "level", "seq"};
        for (size_t c = 0; c < 7 && ok; ++c) {
            const auto f = read_file(entry.path().parent_path() / (std::string(kNames[c]) + ".col"));
            ok = f.has_value();
            if (ok) files[c] = *f;
        }
        if (!ok) continue;
        s.ts    = as_array<uint64_t>(files[0]);
        s.price = ob::decode_prices(as_array<uint64_t>(files[1]));
        s.qty   = ob::decode_simple8b(as_array<uint64_t>(files[2]), s.rows);
        s.cnt   = as_array<uint32_t>(files[3]);
        s.side  = as_array<uint8_t>(files[4]);
        s.level = as_array<uint16_t>(files[5]);
        s.seq   = ob::decode_prices(ob::decode_simple8b(as_array<uint64_t>(files[6]), s.rows));
        if (s.ts.size() != s.rows || s.price.size() != s.rows || s.qty.size() != s.rows ||
            s.cnt.size() != s.rows || s.side.size() != s.rows || s.level.size() != s.rows ||
            s.seq.size() != s.rows) {
            continue;
        }
        rows += s.rows;
        segs.push_back(std::move(s));
    }
    if (segs.empty()) {
        std::fprintf(stderr, "no format-2 segments under %s\n", argv[1]);
        return 1;
    }

    bool same = true;
    std::printf("{\n  \"rows\": %zu, \"segments\": %zu,\n  \"columns\": {\n", rows, segs.size());
    const auto print = [&](const char* name, const ColumnCost& c, bool last) {
        same = same && c.same;
        std::printf("    \"%s\": {\"bytes_per_value\": %.2f, \"to_chars_ns\": %.2f, \"write_ns\": %.2f, "
                    "\"same_bytes\": %s}%s\n",
                    name, c.bytes_per_value, c.to_chars_ns, c.write_ns, c.same ? "true" : "false",
                    last ? "" : ",");
    };
    print("ts",    column_cost(segs, &Segment::ts, rows),    false);
    print("price", column_cost(segs, &Segment::price, rows), false);
    print("qty",   column_cost(segs, &Segment::qty, rows),   false);
    print("cnt",   column_cost(segs, &Segment::cnt, rows),   false);
    print("side",  column_cost(segs, &Segment::side, rows),  false);
    print("level", column_cost(segs, &Segment::level, rows), false);
    print("seq",   column_cost(segs, &Segment::seq, rows),   true);
    std::printf("  },\n");

    // The whole reply, as the server builds a `SELECT *`'s: the segments' rows a batch at a time.
    ob::QueryShape shape;
    shape.columns = ob::all_query_columns();
    std::vector<uint32_t> index(ob::kScanBatchRows);
    std::iota(index.begin(), index.end(), 0U);
    size_t reply_bytes = 0;
    const double reply_ns = fastest_ns([&] {
        ob::QueryResponseBuilder builder(shape);
        for (const Segment& s : segs) {
            for (size_t from = 0; from < s.rows; from += ob::kScanBatchRows) {
                const size_t n = std::min(ob::kScanBatchRows, s.rows - from);
                ob::RowBatch b{std::span<const uint64_t>(s.ts).subspan(from, n),
                               std::span<const int64_t>(s.price).subspan(from, n),
                               std::span<const uint64_t>(s.qty).subspan(from, n),
                               std::span<const uint32_t>(s.cnt).subspan(from, n),
                               std::span<const uint8_t>(s.side).subspan(from, n),
                               std::span<const uint16_t>(s.level).subspan(from, n),
                               std::span<const int64_t>(s.seq).subspan(from, n),
                               std::span<const uint32_t>(index).first(n)};
                builder.add_rows(b);
            }
        }
        reply_bytes = builder.finish().size();
    }, rows);
    std::printf("  \"reply\": {\"bytes_per_row\": %.2f, \"ns_per_row\": %.2f},\n",
                static_cast<double>(reply_bytes) / static_cast<double>(rows), reply_ns);

    // A subscription's push of each row, as `SubscriptionHub` makes one a row a subscriber.
    size_t push_bytes = 0;
    const double push_ns = fastest_ns([&] {
        push_bytes = 0;
        for (const Segment& s : segs) {
            for (size_t i = 0; i < s.rows; ++i) {
                ob::QueryResult r{};
                r.timestamp_ns    = s.ts[i];
                r.price           = s.price[i];
                r.quantity        = s.qty[i];
                r.order_count     = s.cnt[i];
                r.side            = s.side[i];
                r.level           = s.level[i];
                r.sequence_number = static_cast<uint64_t>(s.seq[i]);
                push_bytes += ob::format_push(7, r).size();
            }
        }
    }, rows);
    std::printf("  \"push\": {\"bytes_per_row\": %.2f, \"ns_per_row\": %.2f},\n",
                static_cast<double>(push_bytes) / static_cast<double>(rows), push_ns);
    std::printf("  \"same_bytes\": %s\n}\n", same ? "true" : "false");
    return same ? 0 : 1;
}
