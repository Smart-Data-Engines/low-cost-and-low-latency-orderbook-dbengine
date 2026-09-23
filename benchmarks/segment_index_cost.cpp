// What the segment index costs a flush tick and a query as it grows (#165).
//
// A flush tick closes one segment per symbol that received rows and merges them into the index
// under the engine's lock, and every query looks up its symbol's segments before it reads a byte.
// Until #165 the index was one vector of every segment of every symbol: the merge compared each new
// segment with every indexed one and sorted all of them, and the scan copied all of them. Measured
// with this program on the m9g.xlarge (GCC 14 Release) before that change: a merge of 16 new
// segments took 0.055 ms at 1 000 indexed, 0.61 at 10 000, 3.7 at 50 000 and 7.7 at 100 000, and
// a scan that finds nothing 0.034 / 0.32 / 1.6 / 3.3 ms. After it, both are flat: a merge
// 0.003-0.010 ms and a scan 0.0001 ms at every one of those sizes.
//
// Nothing here touches the disk: the segments are metas merged directly, and the scans ask for
// nothing any segment holds, so what is timed is the index and nothing under it.
//
// The third column is one symbol whose history holds one segment far wider than the rest - what
// a flush writes when it closes a batch that mixes current rows with old ones: a peer's backlog
// applied after a partition, a client's late correction (#105). Its rows are not in the range
// asked for; what is timed is how many of the symbol's other segments the scan walks to learn that.
// With one window per symbol, all of them within its width: 0.0031 / 0.020-0.025 / 0.048-0.052 ms
// at 10 000 / 50 000 / 100 000. With the index's width tiers, 0.0001 ms at each.
//
// Usage: segment_index_cost [symbols]      (default 16, one tick at four pipelining connections)
#include "orderbook/columnar_store.hpp"
#include "orderbook/query_columns.hpp"

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <vector>

namespace {

ob::SegmentMeta meta_for(size_t symbol, uint64_t start, uint64_t width = 6'000'000) {
    ob::SegmentMeta m{};
    m.symbol = "SYM" + std::to_string(symbol);
    m.exchange = "EX";
    m.start_ts_ns = start;
    m.end_ts_ns = start + width;
    m.row_count = 40000;
    m.time_range_is_rows = true;
    m.last_row_ts_ns = m.end_ts_ns;
    m.dir_path = "/data/" + m.symbol + "/EX/" + std::to_string(m.start_ts_ns) + "_" +
                 std::to_string(m.end_ts_ns);
    return m;
}

}  // namespace

int main(int argc, char** argv) {
    const size_t symbols = argc > 1 ? std::strtoull(argv[1], nullptr, 10) : 16;
    const std::vector<size_t> sizes = {1000, 10000, 50000, 100000};
    const uint64_t t0 = 1'790'000'000'000'000'000ULL;
    const uint64_t step = 6'250'000;
    for (size_t n : sizes) {
        ob::ColumnarStore store("/nonexistent-segment-index-cost");
        std::vector<ob::SegmentMeta> seed;
        seed.reserve(n);
        for (size_t i = 0; i < n; ++i) seed.push_back(meta_for(i % symbols, t0 + i * step));
        store.merge_segments(seed);   // the history, in one merge, not timed

        double merge_total = 0, merge_worst = 0;
        const int ticks = 10;
        for (int k = 0; k < ticks; ++k) {
            std::vector<ob::SegmentMeta> tick;
            for (size_t s = 0; s < symbols; ++s) {
                tick.push_back(meta_for(s, t0 + (n + static_cast<size_t>(k) * symbols + s) * step));
            }
            const auto a = std::chrono::steady_clock::now();
            store.merge_segments(tick);
            const double ms =
                std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - a).count();
            merge_total += ms;
            if (ms > merge_worst) merge_worst = ms;
        }

        double scan_total = 0, scan_worst = 0;
        const int scans = 20;
        for (int k = 0; k < scans; ++k) {
            size_t rows = 0;
            const auto a = std::chrono::steady_clock::now();
            store.scan(0, UINT64_MAX, "NOSUCH", "EX", ob::ColumnSet::all(),
                       [&](const ob::SnapshotRow&) { ++rows; });
            const double ms =
                std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - a).count();
            scan_total += ms;
            if (ms > scan_worst) scan_worst = ms;
        }
        // One symbol, n segments 6 ms wide 6.25 ms apart, and one more starting with them and
        // ending halfway through them. The range asked for is one of the 0.25 ms gaps near the
        // end, which no segment holds.
        ob::ColumnarStore one("/nonexistent-segment-index-cost");
        std::vector<ob::SegmentMeta> history;
        history.reserve(n + 1);
        for (size_t i = 0; i < n; ++i) history.push_back(meta_for(0, t0 + i * step));
        history.push_back(meta_for(0, t0, (n / 2) * step));
        one.merge_segments(history);
        const uint64_t gap = t0 + (n - 2) * step + 6'050'000;
        double wide_total = 0, wide_worst = 0;
        for (int k = 0; k < scans; ++k) {
            size_t rows = 0;
            const auto a = std::chrono::steady_clock::now();
            one.scan(gap, gap + 100'000, "SYM0", "EX", ob::ColumnSet::all(),
                     [&](const ob::SnapshotRow&) { ++rows; });
            const double ms =
                std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - a).count();
            wide_total += ms;
            if (ms > wide_worst) wide_worst = ms;
            if (rows != 0) {
                std::fprintf(stderr, "the gap scan found %zu rows - it reads files, not the index\n",
                             rows);
                return 1;
            }
        }
        std::printf("index %7zu segments: merge of %zu new  mean %8.3f ms  worst %8.3f ms | "
                    "scan finding nothing  mean %8.4f ms  worst %8.4f ms | "
                    "one wide segment  mean %8.4f ms  worst %8.4f ms\n",
                    n, symbols, merge_total / ticks, merge_worst, scan_total / scans, scan_worst,
                    wide_total / scans, wide_worst);
    }
    // The store's destructor would flush nothing - no active segment - so it is left to run.
    return 0;
}
