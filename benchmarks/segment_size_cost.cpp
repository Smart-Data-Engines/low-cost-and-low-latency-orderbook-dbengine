// What a segment's size costs a query, and what merging small segments into one costs (#165 part 2b).
//
// Compaction merges small segments into bigger ones, and the bigger a segment, the more a query
// reads to find a narrow range in it: a scan reads every column it needs of every segment whose
// range meets its own, whole, because the price and sequence columns are delta-encoded from the
// segment's first row and the quantities are Simple8b words that decode from the first. So the size
// a merge stops at is a trade between the number of files and what a narrow query reads, and this
// measures both sides on a real store, on the disk it is given:
//
//   - a narrow query - one second, at the soak's 400 rows a second per symbol - in the middle of one
//     segment of N rows, warm: every column, and the two a price series reads;
//   - a whole-segment scan of the same, for scale;
//   - a merge: N / 4096 segments of 4096 rows, read back whole and written as one segment of N rows,
//     and the syncfs() that takes the result to the device, on a disk nothing else is writing to.
//
// Usage: segment_size_cost <directory on the disk to measure>
#include "orderbook/columnar_store.hpp"
#include "orderbook/query_columns.hpp"

#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <random>
#include <string>
#include <vector>

namespace fs = std::filesystem;

namespace {

constexpr uint64_t kHour = 3600ULL * 1'000'000'000ULL;
// An hour boundary, so a segment of a million rows (43.7 minutes at 400 rows a second) does not
// reach the next period and roll over.
constexpr uint64_t kT0 = (1'790'000'000'000'000'000ULL / kHour) * kHour;
constexpr uint64_t kUpdateStepNs = 50'000'000;   // 20 levels per update, 20 updates a second
constexpr size_t kLevels = 20;

ob::SnapshotRow row_at(size_t i, std::mt19937_64& rng, int64_t& price) {
    ob::SnapshotRow r{};
    const size_t update = i / kLevels;
    r.timestamp_ns = kT0 + update * kUpdateStepNs;
    r.sequence_number = update + 1;
    r.side = static_cast<uint8_t>(update % 2);
    r.level_index = static_cast<uint16_t>(i % kLevels);
    if (i % kLevels == 0) price += static_cast<int64_t>(rng() % 21) - 10;
    r.price = price + static_cast<int64_t>(r.level_index) * (r.side == 0 ? -1 : 1);
    r.quantity = 1 + rng() % 1000;
    r.order_count = static_cast<uint32_t>(1 + rng() % 20);
    return r;
}

double ms_since(std::chrono::steady_clock::time_point a) {
    return std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - a).count();
}

double percentile(std::vector<double> v, double p) {
    std::sort(v.begin(), v.end());
    const size_t i = std::min(v.size() - 1, static_cast<size_t>(p * static_cast<double>(v.size())));
    return v[i];
}

int sync_dir(const std::string& dir) {
    const int fd = ::open(dir.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (fd < 0) return -1;
    const int rc = ::syncfs(fd);
    ::close(fd);
    return rc;
}

}  // namespace

int main(int argc, char** argv) {
    if (argc < 2) {
        std::fprintf(stderr, "usage: %s <directory on the disk to measure>\n", argv[0]);
        return 2;
    }
    const std::string base = std::string(argv[1]) + "/segment_size_cost";
    fs::remove_all(base);
    fs::create_directories(base);

    const std::vector<size_t> sizes = {4096, 16384, 65536, 262144, 1048576};
    ob::ColumnSet price_series;
    price_series.add(ob::QueryColumn::TimestampNs).add(ob::QueryColumn::Price);

    for (size_t n : sizes) {
        const std::string dir = base + "/one_" + std::to_string(n);
        std::mt19937_64 rng(n);
        int64_t price = 100'000;
        {
            ob::ColumnarStore store(dir);
            store.set_symbol_exchange("S", "EX");
            for (size_t i = 0; i < n; ++i) store.append(row_at(i, rng, price));
            if (!store.flush_segment()) {
                std::fprintf(stderr, "no segment written for %zu rows\n", n);
                return 1;
            }
        }
        ob::ColumnarStore store(dir);
        store.open_existing();
        if (store.segment_count() != 1) {
            std::fprintf(stderr, "%zu segments for %zu rows, not one\n", store.segment_count(), n);
            return 1;
        }
        const uint64_t span = (n / kLevels) * kUpdateStepNs;
        const uint64_t from = kT0 + span / 2;
        const uint64_t to = from + 1'000'000'000ULL - 1;

        auto narrow = [&](ob::ColumnSet columns, size_t& rows_out) {
            std::vector<double> t;
            for (int k = 0; k < 200; ++k) {
                size_t rows = 0;
                const auto a = std::chrono::steady_clock::now();
                store.scan(from, to, "S", "EX", columns, [&](const ob::SnapshotRow&) { ++rows; });
                t.push_back(ms_since(a));
                rows_out = rows;
            }
            return t;
        };
        size_t rows_all = 0, rows_price = 0;
        const auto t_all = narrow(ob::ColumnSet::all(), rows_all);
        const auto t_price = narrow(price_series, rows_price);

        std::vector<double> t_full;
        size_t rows_full = 0;
        for (int k = 0; k < 10; ++k) {
            size_t rows = 0;
            const auto a = std::chrono::steady_clock::now();
            store.scan(0, UINT64_MAX, "S", "EX", ob::ColumnSet::all(),
                       [&](const ob::SnapshotRow&) { ++rows; });
            t_full.push_back(ms_since(a));
            rows_full = rows;
        }
        if (rows_full != n || rows_all == 0 || rows_all != rows_price) {
            std::fprintf(stderr, "read %zu of %zu rows whole, %zu and %zu narrow\n", rows_full, n,
                         rows_all, rows_price);
            return 1;
        }
        uintmax_t bytes = 0;
        for (const auto& e : fs::recursive_directory_iterator(dir)) {
            if (e.is_regular_file()) bytes += e.file_size();
        }
        std::printf("segment %8zu rows %9ju bytes | 1 s narrow (%zu rows): all columns p50 %7.3f "
                    "p90 %7.3f ms, ts+price p50 %7.3f p90 %7.3f ms | whole p50 %8.2f ms\n",
                    n, bytes, rows_all, percentile(t_all, 0.5), percentile(t_all, 0.9),
                    percentile(t_price, 0.5), percentile(t_price, 0.9), percentile(t_full, 0.5));

        if (n < 8192) continue;
        // The merge: n / 4096 segments of 4096 rows read back and written as one, five times.
        const std::string in_dir = base + "/in_" + std::to_string(n);
        {
            std::mt19937_64 rng2(n);
            int64_t p2 = 100'000;
            ob::ColumnarStore in(in_dir);
            in.set_symbol_exchange("S", "EX");
            for (size_t i = 0; i < n; ++i) {
                in.append(row_at(i, rng2, p2));
                if ((i + 1) % 4096 == 0) in.flush_segment();
            }
            in.flush_segment();
        }
        ob::ColumnarStore in(in_dir);
        in.open_existing();
        std::vector<double> t_merge, t_sync;
        for (int k = 0; k < 5; ++k) {
            const std::string out_dir = base + "/out_" + std::to_string(n) + "_" + std::to_string(k);
            sync_dir(base);   // nothing of the setup left for the timed sync
            const auto a = std::chrono::steady_clock::now();
            std::vector<ob::SnapshotRow> rows;
            rows.reserve(n);
            in.scan(0, UINT64_MAX, "S", "EX", ob::ColumnSet::all(),
                    [&](const ob::SnapshotRow& r) { rows.push_back(r); });
            ob::ColumnarStore out(out_dir);
            out.set_symbol_exchange("S", "EX");
            out.reserve_rows(rows.size());
            for (const auto& r : rows) out.append(r);
            const auto meta = out.flush_segment();
            t_merge.push_back(ms_since(a));
            if (!meta || meta->row_count != n) {
                std::fprintf(stderr, "the merge wrote %llu of %zu rows\n",
                             meta ? static_cast<unsigned long long>(meta->row_count) : 0ULL, n);
                return 1;
            }
            const auto b = std::chrono::steady_clock::now();
            if (sync_dir(out_dir) != 0) {
                std::perror("syncfs");
                return 1;
            }
            t_sync.push_back(ms_since(b));
            fs::remove_all(out_dir);
        }
        const double merge_p50 = percentile(t_merge, 0.5);
        std::printf("merge   %8zu rows from %4zu segments: read and written p50 %8.2f ms "
                    "(%6.1f ns a row), then syncfs p50 %7.2f ms\n",
                    n, n / 4096, merge_p50, merge_p50 * 1e6 / static_cast<double>(n),
                    percentile(t_sync, 0.5));
    }
    fs::remove_all(base);
    return 0;
}
