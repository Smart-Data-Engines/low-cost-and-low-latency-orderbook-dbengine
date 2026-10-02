// What reading one segment costs a store, scan after scan, and what of it is the allocator's (#49).
//
// A query reads every column it needs of a segment whole: seven files read, three columns decoded,
// a row handed over per row. Until #49's step 2 each of those reads allocated its buffers afresh
// and freed them at the end, and what that costs depends on the process, not on the code: a heap
// glibc trims after the free faults every page back in on the next read. So this measures a store
// alone in a process, as the engine's own reads see it, and says how many pages each scan faulted:
//
//   - a scan of the whole segment, every column, into a callback that only counts;
//   - the seven column files read, into vectors kept from one read to the next;
//   - the three decodes - price, quantity, sequence number - into vectors kept the same way;
//   - the rows handed to one std::function, for the cost of the hand-over alone.
//
// Each is the median of `scans` runs, with the process's minor faults per run beside it. Run it
// twice to see the allocator's share - as it is, and with the heap never trimmed:
//
//   segment_read_cost <directory> [rows] [scans]
//   GLIBC_TUNABLES=glibc.malloc.trim_threshold=268435456:glibc.malloc.mmap_threshold=268435456
//       segment_read_cost <directory> [rows] [scans]      (one command line)
//
// The rows are shaped as an order book's are: prices a random walk around 50 000.00, quantities of
// 1 to 5000, sequence numbers rising by one, one row a microsecond.
#include "orderbook/codec.hpp"
#include "orderbook/columnar_store.hpp"
#include "orderbook/query_columns.hpp"

#include <sys/resource.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <functional>
#include <random>
#include <string>
#include <vector>

namespace fs = std::filesystem;

namespace {

using Clock = std::chrono::steady_clock;

long minor_faults() {
    rusage u{};
    getrusage(RUSAGE_SELF, &u);
    return u.ru_minflt;
}

struct Cost {
    double ms;
    double faults;
};

/// The median of `scans` runs and the faults a run, after five not counted: the first run of each
/// allocates what the later ones may keep, and the state between runs is what is being measured.
template <typename F>
Cost measure(int scans, F&& f) {
    for (int i = 0; i < 5; ++i) f();
    std::vector<double> ms;
    ms.reserve(static_cast<size_t>(scans));
    const long f0 = minor_faults();
    for (int i = 0; i < scans; ++i) {
        const auto t0 = Clock::now();
        f();
        ms.push_back(std::chrono::duration<double, std::milli>(Clock::now() - t0).count());
    }
    const long f1 = minor_faults();
    std::sort(ms.begin(), ms.end());
    return {ms[ms.size() / 2], static_cast<double>(f1 - f0) / scans};
}

template <typename T>
void read_file(const std::string& path, std::vector<T>& out) {
    std::ifstream f(path, std::ios::binary);
    f.seekg(0, std::ios::end);
    const auto bytes = static_cast<size_t>(f.tellg());
    f.seekg(0, std::ios::beg);
    out.resize(bytes / sizeof(T));
    f.read(reinterpret_cast<char*>(out.data()), static_cast<std::streamsize>(bytes));
}

}  // namespace

int main(int argc, char** argv) {
    if (argc < 2) {
        std::fprintf(stderr, "usage: %s <directory> [rows] [scans]\n", argv[0]);
        return 2;
    }
    const fs::path dir = fs::path(argv[1]) / "segment_read_cost";
    const size_t rows = argc > 2 ? std::strtoull(argv[2], nullptr, 10) : 100'000;
    const int scans = argc > 3 ? std::atoi(argv[3]) : 201;
    fs::remove_all(dir);
    fs::create_directories(dir);

    ob::ColumnarStore store(dir.string(), ob::ColumnarStore::kDefaultSegmentDurationNs);
    std::mt19937_64 rng(42);
    int64_t price = 5'000'000;
    const uint64_t t0 = 1'790'000'000'000'000'000ULL;
    for (size_t i = 0; i < rows; ++i) {
        price += static_cast<int64_t>(rng() % 21) - 10;
        ob::SnapshotRow r{};
        r.timestamp_ns = t0 + i * 1'000;
        r.sequence_number = i + 1;
        r.side = static_cast<uint8_t>(i % 2);
        r.level_index = static_cast<uint16_t>(i % 20);
        r.price = price;
        r.quantity = 1 + rng() % 5000;
        r.order_count = static_cast<uint32_t>(1 + rng() % 8);
        store.append(r);
    }
    const auto meta = store.flush_segment();
    if (!meta) {
        std::fprintf(stderr, "no segment was written\n");
        return 1;
    }
    const std::string seg = meta->dir_path;

    size_t handed = 0;
    const Cost scan = measure(scans, [&] {
        handed = 0;
        store.scan(0, UINT64_MAX, "", "", ob::ColumnSet::all(), [&](const ob::SnapshotRow&) { ++handed; });
    });
    if (handed != rows) {
        std::fprintf(stderr, "the scan handed over %zu of %zu rows\n", handed, rows);
        return 1;
    }

    std::vector<uint64_t> ts, enc_price, enc_qty, enc_seq;
    std::vector<uint32_t> cnt;
    std::vector<uint8_t> side;
    std::vector<uint16_t> level;
    const Cost files = measure(scans, [&] {
        read_file(seg + "/ts.col", ts);
        read_file(seg + "/price.col", enc_price);
        read_file(seg + "/qty.col", enc_qty);
        read_file(seg + "/cnt.col", cnt);
        read_file(seg + "/side.col", side);
        read_file(seg + "/level.col", level);
        read_file(seg + "/seq.col", enc_seq);
    });

    std::vector<int64_t> prices, seqs;
    std::vector<uint64_t> qtys, zigzag;
    const Cost decode = measure(scans, [&] {
        ob::decode_prices_into(enc_price, prices);
        ob::decode_simple8b_into(enc_qty, rows, qtys);
        ob::decode_simple8b_into(enc_seq, rows, zigzag);
        ob::decode_prices_into(zigzag, seqs);
    });

    std::function<void(const ob::SnapshotRow&)> count = [&](const ob::SnapshotRow&) { ++handed; };
    const Cost hand_over = measure(scans, [&] {
        handed = 0;
        for (size_t i = 0; i < rows; ++i) {
            ob::SnapshotRow r{};
            r.timestamp_ns = ts[i];
            r.sequence_number = static_cast<uint64_t>(seqs[i]);
            r.side = side[i];
            r.level_index = level[i];
            r.price = prices[i];
            r.quantity = qtys[i];
            r.order_count = cnt[i];
            count(r);
        }
    });

    const char* tunables = std::getenv("GLIBC_TUNABLES");
    std::printf("One segment of %zu rows, %d scans each, median; GLIBC_TUNABLES=%s\n", rows, scans,
                tunables ? tunables : "(unset)");
    std::printf("%-44s %10s %16s\n", "", "ms", "faults per scan");
    std::printf("%-44s %10.3f %16.1f\n", "scan(), every column", scan.ms, scan.faults);
    std::printf("%-44s %10.3f %16.1f\n", "the seven files read, into kept vectors", files.ms, files.faults);
    std::printf("%-44s %10.3f %16.1f\n", "price, quantity, sequence decoded, kept", decode.ms,
                decode.faults);
    std::printf("%-44s %10.3f %16.1f\n", "the rows handed to one std::function", hand_over.ms,
                hand_over.faults);
    std::printf("columns: ts %zu B, price %zu B, qty %zu B, seq %zu B\n", ts.size() * 8,
                enc_price.size() * 8, enc_qty.size() * 8, enc_seq.size() * 8);
    fs::remove_all(dir);
    return 0;
}
