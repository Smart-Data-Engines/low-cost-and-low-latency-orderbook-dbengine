// What composing a book summary costs at a seal (#266).
//
// After a seal of a busy symbol the flush thread writes the book as of the segment's end
// (`ColumnarStore::write_book_summary()`, #233, #243, #247): the newest summary before it and the rows
// since, composed by price. In the node's profile under #190's ingest that was a quarter to a third of
// the flush thread's time (evidence/2026-11-14-ingest-profile/ in the workspace), and the flush thread
// is the one thread every writer's rows go through.
//
// One symbol; a random walk of its book - `prices` prices a side around a fixed mid, the top of the book
// changed more often than the bottom, a tenth of the rows taking a price out - with times a microsecond
// apart. The rows are fed to a store as the engine feeds it: published as blocks of 4096, sealed into
// one segment by a writer store, the blocks replaced by it (`seal_blocks()`). Then write_book_summary(),
// timed, which composes from the summary before it. Every `rows` rows, `summaries` times; the first two
// are not counted, the first composing from the first row.
//
//   book_summary_cost <dir> [rows] [prices] [summaries]
//
// Defaults: 65 536 rows, which is a summary's due at the engine's default; 500 prices a side; 20
// summaries. `dir` is created and left with the store in it. One JSON line: the time a summary took and
// that time over the rows it read, median and range.

#include "orderbook/columnar_store.hpp"
#include "orderbook/data_model.hpp"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <random>
#include <string>
#include <vector>

namespace {

constexpr uint64_t kSec   = 1'000'000'000ULL;
constexpr uint64_t kStart = 1'790'000'000ULL * kSec;
constexpr size_t   kBlock = 4096;
constexpr int64_t  kMid   = 100'000'000;

double median(std::vector<double> v) {
    std::sort(v.begin(), v.end());
    return v.empty() ? 0.0 : v[v.size() / 2];
}

}  // namespace

int main(int argc, char** argv) {
    if (argc < 2) {
        std::fprintf(stderr, "usage: %s <dir> [rows] [prices] [summaries]\n", argv[0]);
        return 2;
    }
    const std::string dir = argv[1];
    const size_t rows      = argc > 2 ? std::strtoull(argv[2], nullptr, 10) : 65'536;
    const size_t prices    = argc > 3 ? std::strtoull(argv[3], nullptr, 10) : 500;
    const size_t summaries = argc > 4 ? std::strtoull(argv[4], nullptr, 10) : 20;
    if (rows == 0 || prices == 0 || summaries < 3) {
        std::fprintf(stderr, "rows and prices above 0, and 3 summaries or more\n");
        return 2;
    }
    std::error_code ec;
    std::filesystem::remove_all(dir, ec);
    std::filesystem::create_directories(dir);

    ob::ColumnarStore store(dir);
    std::mt19937_64 rng(266);
    // The top of a side changes most: a level's index exponentially distributed, a mean of a tenth
    // of the side.
    std::exponential_distribution<double> depth(10.0 / static_cast<double>(prices));
    std::uniform_int_distribution<uint64_t> qty(1, 1000);
    std::uniform_int_distribution<int> tenth(0, 9);
    std::uniform_int_distribution<int> side(0, 1);

    uint64_t ts  = kStart;
    uint64_t seq = 1;
    std::vector<double> ms;
    std::vector<double> ns_row;
    std::vector<double> rows_read;
    ob::ColumnarStore::SummaryWrite last;
    for (size_t k = 0; k < summaries; ++k) {
        std::vector<ob::SnapshotRow> batch;
        batch.reserve(rows);
        for (size_t i = 0; i < rows; ++i) {
            ob::SnapshotRow r{};
            r.timestamp_ns    = ts;
            r.sequence_number = seq++;
            r.side            = side(rng) == 0 ? ob::SIDE_BID : ob::SIDE_ASK;
            const auto level  = std::min<size_t>(prices - 1, static_cast<size_t>(depth(rng)));
            r.price           = r.side == ob::SIDE_BID ? kMid - 1 - static_cast<int64_t>(level)
                                                       : kMid + 1 + static_cast<int64_t>(level);
            r.quantity        = tenth(rng) == 0 ? 0 : qty(rng);
            r.order_count     = static_cast<uint32_t>(r.quantity % 7 + 1);
            r.level_index     = static_cast<uint16_t>(level);
            ts += 1000;
            batch.push_back(r);
        }
        size_t blocks = 0;
        for (size_t at = 0; at < batch.size(); at += kBlock) {
            const size_t end = std::min(batch.size(), at + kBlock);
            store.publish_blocks({ob::RowBlock::make(
                "BENCH", "EX", std::vector<ob::SnapshotRow>(batch.begin() + static_cast<std::ptrdiff_t>(at),
                                                            batch.begin() + static_cast<std::ptrdiff_t>(end)))});
            ++blocks;
        }
        ob::ColumnarStore writer(dir, ob::ColumnarStore::kDefaultSegmentDurationNs,
                                 ob::ColumnarStore::OwnIndex::kNo);
        writer.set_symbol_exchange("BENCH", "EX");
        for (const auto& r : batch) writer.append(r);
        const auto meta = writer.flush_segment();
        if (!meta) {
            std::fprintf(stderr, "summary %zu: the writer sealed no segment\n", k);
            return 1;
        }
        if (store.seal_blocks("BENCH", "EX", blocks, {*meta}) != 0) {
            std::fprintf(stderr, "summary %zu: the segment was refused\n", k);
            return 1;
        }
        const auto started = std::chrono::steady_clock::now();
        last = store.write_book_summary("BENCH", "EX", meta->dir_path);
        const double took = std::chrono::duration<double, std::milli>(
                                std::chrono::steady_clock::now() - started).count();
        if (!last.written) {
            std::fprintf(stderr, "summary %zu not written: %s\n", k, last.why_not.c_str());
            return 1;
        }
        if (k < 2) continue;
        ms.push_back(took);
        rows_read.push_back(static_cast<double>(last.rows_read));
        ns_row.push_back(took * 1e6 / static_cast<double>(std::max<uint64_t>(1, last.rows_read)));
    }
    std::printf("{\"rows\": %zu, \"prices_a_side\": %zu, \"summaries\": %zu, \"book_prices\": %zu, "
                "\"rows_read_p50\": %.0f, \"summary_ms_p50\": %.3f, \"summary_ms_min\": %.3f, "
                "\"summary_ms_max\": %.3f, \"ns_a_row_p50\": %.1f, \"ns_a_row_min\": %.1f, "
                "\"ns_a_row_max\": %.1f, \"summary_bytes\": %zu, \"track_entries\": %zu}\n",
                rows, prices, ms.size(), last.prices, median(rows_read), median(ms),
                *std::min_element(ms.begin(), ms.end()), *std::max_element(ms.begin(), ms.end()),
                median(ns_row), *std::min_element(ns_row.begin(), ns_row.end()),
                *std::max_element(ns_row.begin(), ns_row.end()), last.bytes, last.track_entries);
    return 0;
}
