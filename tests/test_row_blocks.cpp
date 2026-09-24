// #165 part 2a: drained rows wait in published blocks until a seal replaces them with a segment.
//
// A tick used to write a segment for every symbol with a row since the last one, so the flush
// thread spent 84.7% of its CPU in the kernel creating files and a node gained a segment per active
// symbol per tick. What these hold is what makes waiting safe to read: a published block answers
// queries before it is written, a seal replaces blocks with the segment written from them in one
// step - a query sees one or the other, never both and never neither - and nothing that prunes or
// rebuilds the index loses a block's rows or keeps a block past it.

#include <gtest/gtest.h>
#include <rapidcheck.h>
#include <rapidcheck/gtest.h>

#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "orderbook/columnar_store.hpp"
#include "orderbook/data_model.hpp"
#include "orderbook/query_columns.hpp"

namespace fs = std::filesystem;

namespace {

constexpr uint64_t kSec  = 1'000'000'000ULL;
constexpr uint64_t kBase = 1'790'000'000ULL * kSec;

std::atomic<uint64_t> g_counter{0};

struct TempDir {
    fs::path path;
    TempDir()
        : path(fs::temp_directory_path() /
               ("ob_row_blocks_" + std::to_string(::getpid()) + "_" +
                std::to_string(g_counter.fetch_add(1)))) {
        fs::create_directories(path);
    }
    ~TempDir() {
        std::error_code ec;
        fs::remove_all(path, ec);
    }
    std::string str() const { return path.string(); }
};

ob::SnapshotRow row_at(uint64_t ts, int64_t price) {
    ob::SnapshotRow row{};
    row.timestamp_ns    = ts;
    row.sequence_number = static_cast<uint64_t>(price);
    row.side            = ob::SIDE_BID;
    row.level_index     = 1;
    row.price           = price;
    row.quantity        = 7;
    row.order_count     = 3;
    return row;
}

std::shared_ptr<const ob::RowBlock> block(const std::string& symbol,
                                          const std::vector<std::pair<uint64_t, int64_t>>& rows) {
    std::vector<ob::SnapshotRow> out;
    for (const auto& [ts, price] : rows) out.push_back(row_at(ts, price));
    return ob::RowBlock::make(symbol, "EX", std::move(out));
}

/// The segment a seal would write from these rows, by a store whose segments go elsewhere.
ob::SegmentMeta written(const std::string& dir, const std::string& symbol,
                        const std::vector<std::pair<uint64_t, int64_t>>& rows) {
    ob::ColumnarStore writer(dir, ob::ColumnarStore::kDefaultSegmentDurationNs,
                             ob::ColumnarStore::OwnIndex::kNo);
    writer.set_symbol_exchange(symbol, "EX");
    for (const auto& [ts, price] : rows) writer.append(row_at(ts, price));
    auto meta = writer.flush_segment();
    EXPECT_TRUE(meta.has_value());
    return meta.value_or(ob::SegmentMeta{});
}

std::vector<int64_t> delivered(const ob::ColumnarStore& store, const std::string& symbol,
                               uint64_t lo, uint64_t hi, ob::ColumnarStore::ScanCost* cost = nullptr) {
    std::vector<int64_t> out;
    const auto c = store.scan(lo, hi, symbol, "EX", ob::ColumnSet::all(),
                              [&](const ob::SnapshotRow& r) { out.push_back(r.price); });
    if (cost != nullptr) *cost = c;
    return out;
}

}  // namespace

TEST(RowBlocks, APublishedBlockAnswersBeforeItIsSealed) {
    TempDir dir;
    ob::ColumnarStore store(dir.str());
    EXPECT_FALSE(store.holds("A", "EX"));
    store.publish_blocks({block("A", {{kBase + 1 * kSec, 1}, {kBase + 2 * kSec, 2}})});
    ob::ColumnarStore::ScanCost cost;
    EXPECT_EQ(delivered(store, "A", 0, UINT64_MAX, &cost), (std::vector<int64_t>{1, 2}));
    EXPECT_EQ(cost.blocks, 1u);
    EXPECT_EQ(cost.candidates, 0u);
    EXPECT_TRUE(store.holds("A", "EX")) << "a symbol with only unsealed rows answered not-found";
    EXPECT_EQ(store.unsealed_rows(), 2u);
    EXPECT_EQ(store.segment_count(), 0u);
    EXPECT_EQ(store.symbols_indexed(), 1u);
    // The range is the block's rows', so a query past them does not read it.
    EXPECT_TRUE(delivered(store, "A", kBase + 3 * kSec, UINT64_MAX, &cost).empty());
    EXPECT_EQ(cost.blocks, 0u);
}

TEST(RowBlocks, ASealReplacesTheBlocksWithTheirSegmentInOneStep) {
    TempDir dir;
    ob::ColumnarStore store(dir.str());
    const std::vector<std::pair<uint64_t, int64_t>> first = {{kBase + 1 * kSec, 1}};
    const std::vector<std::pair<uint64_t, int64_t>> second = {{kBase + 2 * kSec, 2}};
    store.publish_blocks({block("A", first)});
    store.publish_blocks({block("A", second)});
    const auto meta = written(dir.str(), "A", {first[0], second[0]});
    EXPECT_EQ(store.seal_blocks("A", "EX", 2, {meta}), 0u);
    ob::ColumnarStore::ScanCost cost;
    EXPECT_EQ(delivered(store, "A", 0, UINT64_MAX, &cost), (std::vector<int64_t>{1, 2}))
        << "a sealed row was read twice or not at all";
    EXPECT_EQ(cost.blocks, 0u);
    EXPECT_EQ(cost.candidates, 1u);
    EXPECT_EQ(store.unsealed_rows(), 0u);
    EXPECT_EQ(store.segment_count(), 1u);
}

TEST(RowBlocks, ASealTakesTheBlocksPublishedFirstAndLeavesTheRest) {
    TempDir dir;
    ob::ColumnarStore store(dir.str());
    store.publish_blocks({block("A", {{kBase + 1 * kSec, 1}})});
    store.publish_blocks({block("A", {{kBase + 2 * kSec, 2}})});
    const auto meta = written(dir.str(), "A", {{kBase + 1 * kSec, 1}});
    store.seal_blocks("A", "EX", 1, {meta});
    EXPECT_EQ(delivered(store, "A", 0, UINT64_MAX), (std::vector<int64_t>{1, 2}));
    EXPECT_EQ(store.unsealed_rows(), 1u);
}

TEST(RowBlocks, BlocksAreReadAfterSegmentsInTheOrderTheyWerePublished) {
    // A symbol's blocks are its newest writes, whatever times their rows carry: a reader that keeps
    // the last of a tie keeps the one written later (#168).
    TempDir dir;
    ob::ColumnarStore store(dir.str());
    store.merge_segments({written(dir.str(), "A", {{kBase + 10 * kSec, 1}})});
    store.publish_blocks({block("A", {{kBase + 5 * kSec, 2}})});
    store.publish_blocks({block("A", {{kBase + 1 * kSec, 3}})});
    EXPECT_EQ(delivered(store, "A", 0, UINT64_MAX), (std::vector<int64_t>{1, 2, 3}));
}

TEST(RowBlocks, AQueryThatCopiedTheBlocksBeforeASealReadsThemOnce) {
    // The scan copies what it will read under the index's lock and reads without it. A seal between
    // the two is made deterministic here: the callback runs the seal after the scan's first row.
    TempDir dir;
    ob::ColumnarStore store(dir.str());
    store.publish_blocks({block("A", {{kBase + 1 * kSec, 1}, {kBase + 2 * kSec, 2}})});
    const auto meta = written(dir.str(), "A", {{kBase + 1 * kSec, 1}, {kBase + 2 * kSec, 2}});
    std::vector<int64_t> seen;
    bool sealed = false;
    store.scan(0, UINT64_MAX, "A", "EX", ob::ColumnSet::all(), [&](const ob::SnapshotRow& r) {
        seen.push_back(r.price);
        if (!sealed) {
            sealed = true;
            store.seal_blocks("A", "EX", 1, {meta});
        }
    });
    EXPECT_EQ(seen, (std::vector<int64_t>{1, 2}));
    EXPECT_EQ(delivered(store, "A", 0, UINT64_MAX), (std::vector<int64_t>{1, 2}));
}

TEST(RowBlocks, ABlockAnswersTheColumnsAskedForAndZeroForTheRest) {
    TempDir dir;
    ob::ColumnarStore store(dir.str());
    store.publish_blocks({block("A", {{kBase + 1 * kSec, 5}})});
    std::vector<ob::SnapshotRow> rows;
    ob::ColumnSet only_price;
    only_price.add(ob::QueryColumn::Price);
    store.scan(0, UINT64_MAX, "A", "EX", only_price,
               [&](const ob::SnapshotRow& r) { rows.push_back(r); });
    ASSERT_EQ(rows.size(), 1u);
    EXPECT_EQ(rows[0].price, 5);
    EXPECT_EQ(rows[0].timestamp_ns, kBase + 1 * kSec) << "the timestamp is always read";
    EXPECT_EQ(rows[0].quantity, 0u);
    EXPECT_EQ(rows[0].order_count, 0u);
    EXPECT_EQ(rows[0].sequence_number, 0u);
    EXPECT_EQ(rows[0].level_index, 0u);
}

TEST(RowBlocks, RetentionAndRemovalKeepASymbolThatStillHasBlocks) {
    TempDir dir;
    ob::ColumnarStore store(dir.str());
    store.merge_segments({written(dir.str(), "A", {{kBase + 1 * kSec, 1}})});
    store.publish_blocks({block("A", {{kBase + 100 * kSec, 2}})});
    EXPECT_EQ(store.delete_expired_segments(kBase + 10 * kSec).first, 1u);
    EXPECT_EQ(store.symbols_indexed(), 1u) << "retention erased a symbol whose rows wait in a block";
    EXPECT_EQ(delivered(store, "A", 0, UINT64_MAX), (std::vector<int64_t>{2}));

    const auto b = written(dir.str(), "B", {{kBase + 1 * kSec, 3}});
    store.merge_segments({b});
    store.publish_blocks({block("B", {{kBase + 2 * kSec, 4}})});
    EXPECT_EQ(store.remove_segments({b.dir_path}), 1u);
    EXPECT_TRUE(store.holds("B", "EX"));
    EXPECT_EQ(delivered(store, "B", 0, UINT64_MAX), (std::vector<int64_t>{4}));
}

TEST(RowBlocks, ARebuiltIndexHoldsNoBlocks) {
    // What a block holds is in the WAL until a seal writes it; an index rebuilt from the disk -
    // open_existing(), a snapshot install - has only what is on it.
    TempDir dir;
    ob::ColumnarStore store(dir.str());
    store.publish_blocks({block("A", {{kBase + 1 * kSec, 1}})});
    store.open_existing();
    EXPECT_EQ(store.unsealed_rows(), 0u);
    EXPECT_FALSE(store.holds("A", "EX"));
}

TEST(RowBlocks, NoRowsMakeNoBlock) {
    EXPECT_EQ(ob::RowBlock::make("A", "EX", {}), nullptr);
    const auto b = block("A", {{kBase + 5 * kSec, 1}, {kBase + 2 * kSec, 2}, {kBase + 9 * kSec, 3}});
    ASSERT_NE(b, nullptr);
    EXPECT_EQ(b->min_ts_ns, kBase + 2 * kSec);
    EXPECT_EQ(b->max_ts_ns, kBase + 9 * kSec);
}

// A seal appends a block at once rather than row by row, and has to write the same thing (#165 part
// 2a). Drawn here: rows over three short periods in any order, some quantities too wide for Simple8b,
// cut into blocks anywhere, with segments closed between blocks at random - one store appending the
// blocks, another every row in turn. Every file both write, meta.json included, must be the same
// bytes under the same name.
RC_GTEST_PROP(RowBlocksProperty, AppendingABlockWritesWhatAppendingItsRowsDoes, ()) {
    constexpr uint64_t kPeriod = 10 * kSec;
    const size_t n = *rc::gen::inRange<size_t>(1, 120);
    std::vector<ob::SnapshotRow> rows;
    for (size_t i = 0; i < n; ++i) {
        const uint64_t ts = kBase + *rc::gen::inRange<uint64_t>(0, 3 * kPeriod);
        ob::SnapshotRow r = row_at(ts, static_cast<int64_t>(1000 + i));
        if (*rc::gen::inRange(0, 8) == 0) r.quantity = (1ULL << 61) + i;
        rows.push_back(r);
    }
    TempDir by_block_dir, by_row_dir;
    ob::ColumnarStore by_block(by_block_dir.str(), kPeriod, ob::ColumnarStore::OwnIndex::kNo);
    ob::ColumnarStore by_row(by_row_dir.str(), kPeriod, ob::ColumnarStore::OwnIndex::kNo);
    by_block.set_symbol_exchange("S", "EX");
    by_row.set_symbol_exchange("S", "EX");
    size_t at = 0;
    while (at < n) {
        const size_t len = *rc::gen::inRange<size_t>(1, n - at + 1);
        std::vector<ob::SnapshotRow> part(rows.begin() + static_cast<std::ptrdiff_t>(at),
                                          rows.begin() + static_cast<std::ptrdiff_t>(at + len));
        if (*rc::gen::arbitrary<bool>()) by_block.reserve_rows(len);
        by_block.append_block(*ob::RowBlock::make("S", "EX", part));
        for (const auto& r : part) by_row.append(r);
        at += len;
        if (*rc::gen::inRange(0, 4) == 0) {
            (void)by_block.flush_segment();
            (void)by_row.flush_segment();
        }
    }
    (void)by_block.flush_segment();
    (void)by_row.flush_segment();

    const auto files_of = [](const fs::path& root) {
        std::map<std::string, std::string> out;
        for (const auto& e : fs::recursive_directory_iterator(root)) {
            if (!e.is_regular_file()) continue;
            std::ifstream in(e.path(), std::ios::binary);
            out[fs::relative(e.path(), root).string()] =
                std::string(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
        }
        return out;
    };
    const auto got = files_of(by_block_dir.path);
    const auto want = files_of(by_row_dir.path);
    RC_ASSERT(!want.empty());
    RC_ASSERT(got == want);
}

// A scan answers with every row written for its symbol and range, whether a seal has written it or
// it waits in a block. Drawn here: segments and blocks of three symbols in any order and of any
// width, some blocks then sealed, and queries of any range. The expected answer is a walk over every
// row, as the flat index was.
RC_GTEST_PROP(RowBlocksProperty, AScanReturnsEveryRowWhetherSealedOrNot, ()) {
    TempDir dir;
    ob::ColumnarStore store(dir.str());
    const std::vector<std::string> symbols = {"A", "B", "C.D"};
    struct Written { std::string symbol; uint64_t ts; int64_t price; };
    std::vector<Written> all;
    struct Pending { std::string symbol; std::vector<std::pair<uint64_t, int64_t>> rows; };
    std::vector<Pending> published;
    int64_t price = 1;
    const int pieces = *rc::gen::inRange(1, 12);
    for (int p = 0; p < pieces; ++p) {
        const std::string sym = symbols[static_cast<size_t>(*rc::gen::inRange(0, 3))];
        std::vector<std::pair<uint64_t, int64_t>> rows;
        const int n = *rc::gen::inRange(1, 5);
        for (int r = 0; r < n; ++r) {
            rows.emplace_back(kBase + static_cast<uint64_t>(*rc::gen::inRange(0, 1000)) * kSec,
                              price++);
        }
        for (const auto& [ts, pr] : rows) all.push_back({sym, ts, pr});
        if (*rc::gen::arbitrary<bool>()) {
            store.merge_segments({written(dir.str(), sym, rows)});
        } else {
            store.publish_blocks({block(sym, rows)});
            published.push_back({sym, rows});
        }
    }
    // Seal a prefix of each symbol's blocks, as a tick would.
    for (const auto& sym : symbols) {
        std::vector<std::pair<uint64_t, int64_t>> rows;
        size_t count = 0;
        const size_t upto = static_cast<size_t>(*rc::gen::inRange(0, 3));
        for (const auto& p : published) {
            if (p.symbol != sym || count == upto) continue;
            rows.insert(rows.end(), p.rows.begin(), p.rows.end());
            ++count;
        }
        if (count > 0) store.seal_blocks(sym, "EX", count, {written(dir.str(), sym, rows)});
    }
    for (int q = 0; q < 8; ++q) {
        const std::string sym = symbols[static_cast<size_t>(*rc::gen::inRange(0, 3))];
        uint64_t lo = kBase + static_cast<uint64_t>(*rc::gen::inRange(0, 1001)) * kSec;
        uint64_t hi = kBase + static_cast<uint64_t>(*rc::gen::inRange(0, 1001)) * kSec;
        if (lo > hi) std::swap(lo, hi);
        std::vector<int64_t> expected;
        for (const auto& w : all) {
            if (w.symbol == sym && w.ts >= lo && w.ts <= hi) expected.push_back(w.price);
        }
        std::sort(expected.begin(), expected.end());
        auto got = delivered(store, sym, lo, hi);
        std::sort(got.begin(), got.end());
        RC_ASSERT(got == expected);
    }
}
