// A pending row carries its pair's name by pointer since #270, and the drain finds a run of one
// pair by comparing those pointers - across chunks of the queue, which until then each started a
// run again. What a client can see of that is where the rows are stored: each under its own pair,
// whole, whatever pairs its rows were queued between and wherever a chunk ended.

#include "orderbook/chunked_queue.hpp"
#include "orderbook/engine.hpp"
#include "orderbook/data_model.hpp"
#include "orderbook/types.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <cstring>
#include <filesystem>
#include <map>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

namespace fs = std::filesystem;

namespace {

std::atomic<uint64_t> g_dir_counter{0};

struct TempDir {
    std::string path;
    explicit TempDir(const std::string& prefix) {
        auto p = fs::temp_directory_path() /
                 (prefix + std::to_string(g_dir_counter.fetch_add(1, std::memory_order_relaxed)));
        fs::create_directories(p);
        path = p.string();
    }
    ~TempDir() {
        std::error_code ec;
        fs::remove_all(path, ec);
    }
    TempDir(const TempDir&) = delete;
    TempDir& operator=(const TempDir&) = delete;
};

/// One hour, so nothing but the test moves rows out of the queue.
constexpr uint64_t kNoAutoFlush = 3'600'000'000'000ULL;

using Pair = std::pair<std::string, std::string>;
/// A stored row as a client reads it: time, side, level, price, quantity, order count.
using Row = std::tuple<uint64_t, uint8_t, uint16_t, int64_t, uint64_t, uint32_t>;

struct Write {
    ob::DeltaUpdate        update{};
    std::vector<ob::Level> levels;
};

Write make_write(const Pair& pair, uint8_t side, uint16_t width, uint64_t ts) {
    Write w;
    std::strncpy(w.update.symbol, pair.first.c_str(), sizeof(w.update.symbol) - 1);
    std::strncpy(w.update.exchange, pair.second.c_str(), sizeof(w.update.exchange) - 1);
    w.update.timestamp_ns = ts;
    w.update.side         = side;
    w.update.n_levels     = width;
    for (uint16_t i = 0; i < width; ++i) {
        ob::Level level{};
        level.price = static_cast<int64_t>(50'000 + (side == ob::SIDE_BID ? -1 : 1) * (i + 1) * 10 +
                                           static_cast<int64_t>(ts % 11));
        level.qty   = 1 + ts % 97 + i;
        level.cnt   = 1 + i % 4;
        w.levels.push_back(level);
    }
    return w;
}

std::vector<Row> stored(ob::Engine& engine, const Pair& pair) {
    std::vector<Row> rows;
    const std::string err = engine.execute(
        "SELECT * FROM '" + pair.first + "'.'" + pair.second + "'", [&](const ob::QueryResult& r) {
            rows.emplace_back(r.timestamp_ns, r.side, r.level, r.price, r.quantity, r.order_count);
        });
    EXPECT_TRUE(err.empty()) << pair.first << "." << pair.second << ": " << err;
    std::sort(rows.begin(), rows.end());
    return rows;
}

}  // namespace

TEST(PendingRowNames, RowsOfPairsQueuedBetweenEachOtherAndAcrossChunksAreStoredUnderTheirOwnPair) {
    // Three pairs - two symbols on one exchange, and one of those symbols on another - written in
    // turn, an update at a time and a batch at a time, so that consecutive rows change pair at
    // every update and the queue fills more than one chunk of 4096 rows before the flush drains it.
    const std::vector<Pair> pairs = {{"AAA", "EX"}, {"BBB", "EX"}, {"AAA", "EY"}};
    std::map<Pair, std::vector<Row>> written;
    std::vector<Write> writes;
    uint64_t ts = 1'700'000'000'000'000'000ULL;
    for (int round = 0; round < 160; ++round) {
        for (const Pair& pair : pairs) {
            ++ts;
            const uint8_t side = (round % 2 == 0) ? ob::SIDE_BID : ob::SIDE_ASK;
            const uint16_t width = static_cast<uint16_t>(1 + (round * 7 + ts) % 30);
            writes.push_back(make_write(pair, side, width, ts));
            const Write& w = writes.back();
            for (uint16_t i = 0; i < width; ++i) {
                written[pair].emplace_back(ts, side, i, w.levels[i].price, w.levels[i].qty, w.levels[i].cnt);
            }
        }
    }
    size_t total = 0;
    for (const auto& [pair, rows] : written) total += rows.size();
    // The engine queues its rows in chunks of the queue's default size.
    ASSERT_GT(total, ob::ChunkedQueue<int>::chunk_rows()) << "the rows have to cross a chunk";

    TempDir dir("pending_row_names_");
    ob::Engine engine(dir.path, kNoAutoFlush);
    engine.open();
    // The first half one write at a time, the rest in batches of seven: both paths queue rows.
    const size_t half = writes.size() / 2;
    for (size_t i = 0; i < half; ++i) {
        ASSERT_EQ(engine.apply_delta(writes[i].update, writes[i].levels.data()), ob::OB_OK) << "write " << i;
    }
    for (size_t i = half; i < writes.size(); i += 7) {
        std::vector<ob::ClientWrite> batch;
        for (size_t j = i; j < std::min(writes.size(), i + 7); ++j) {
            batch.push_back(ob::ClientWrite{&writes[j].update, writes[j].levels.data()});
        }
        std::vector<ob::WriteOutcome> outcomes(batch.size());
        engine.apply_deltas(batch, outcomes);
        for (size_t j = 0; j < outcomes.size(); ++j) {
            ASSERT_TRUE(outcomes[j].error.empty()) << "write " << i + j << ": " << outcomes[j].error;
            ASSERT_EQ(outcomes[j].status, ob::OB_OK) << "write " << i + j;
        }
    }
    EXPECT_EQ(engine.stats().pending_rows, total);
    engine.flush_incremental();
    EXPECT_EQ(engine.stats().pending_rows, 0u);

    for (const Pair& pair : pairs) {
        std::vector<Row> expected = written[pair];
        std::sort(expected.begin(), expected.end());
        const std::vector<Row> got = stored(engine, pair);
        EXPECT_EQ(got.size(), expected.size()) << pair.first << "." << pair.second;
        EXPECT_TRUE(got == expected) << pair.first << "." << pair.second
                                     << ": the rows stored are not the rows written";
    }
    engine.close();
}
