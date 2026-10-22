// Book summaries (#233, `book_summary.hpp`): what a book read by price starts from.
//
// A summary is the book of one symbol by price as of one instant, written beside its segments. A read
// at or after it applies only the rows after it, where it applied every row of the symbol from the
// first. What these tests hold is that a read that starts from a summary answers what composing every
// row answers - the oracle below, from a plain scan - through what can make a summary false: a row at
// or before its time arriving after it, a restart that reads it back, a file that does not read back.

#include "orderbook/book_summary.hpp"
#include "orderbook/columnar_store.hpp"
#include "orderbook/data_model.hpp"

#include <gtest/gtest.h>
#include <rapidcheck.h>
#include <rapidcheck/gtest.h>

#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <string>
#include <tuple>
#include <vector>

namespace fs = std::filesystem;

namespace {

constexpr uint64_t kSec  = 1'000'000'000ULL;
constexpr uint64_t kBase = 1'790'000'000ULL * kSec;

std::atomic<uint64_t> g_counter{0};

struct TempDir {
    fs::path path;
    TempDir()
        : path(fs::temp_directory_path() / ("ob_book_summary_" + std::to_string(::getpid()) + "_" +
                                            std::to_string(g_counter.fetch_add(1)))) {
        fs::create_directories(path);
    }
    ~TempDir() {
        std::error_code ec;
        fs::remove_all(path, ec);
    }
};

ob::SnapshotRow row(uint64_t ts, uint8_t side, int64_t price, uint64_t qty, uint64_t seq = 1) {
    ob::SnapshotRow r{};
    r.timestamp_ns    = ts;
    r.sequence_number = seq;
    r.side            = side;
    r.price           = price;
    r.quantity        = qty;
    r.order_count     = static_cast<uint32_t>(qty % 5 + 1);
    return r;
}

/// A level as both reads answer it: side, level, price, quantity, count, time, sequence.
using Level = std::tuple<uint8_t, uint16_t, int64_t, uint64_t, uint32_t, uint64_t, uint64_t>;

std::vector<Level> levels_of(const std::vector<ob::SnapshotRow>& rows) {
    std::vector<Level> out;
    for (const auto& r : rows) {
        out.emplace_back(r.side, r.level_index, r.price, r.quantity, r.order_count, r.timestamp_ns,
                         r.sequence_number);
    }
    return out;
}

/// The oracle: every row a plain scan delivers up to `at`, sorted by time and stable, applied by
/// price.
std::vector<Level> composed(const ob::ColumnarStore& store, uint64_t at) {
    std::vector<ob::SnapshotRow> rows;
    store.scan(0, at, "SUM", "EX", ob::ColumnSet::all(),
               [&](const ob::SnapshotRow& r) { rows.push_back(r); });
    std::stable_sort(rows.begin(), rows.end(), [](const ob::SnapshotRow& a, const ob::SnapshotRow& b) {
        return a.timestamp_ns < b.timestamp_ns;
    });
    std::map<int64_t, ob::SnapshotRow, std::greater<>> bids;
    std::map<int64_t, ob::SnapshotRow> asks;
    for (const auto& r : rows) {
        if (r.side == ob::SIDE_BID) {
            if (r.quantity == 0) bids.erase(r.price); else bids[r.price] = r;
        } else {
            if (r.quantity == 0) asks.erase(r.price); else asks[r.price] = r;
        }
    }
    std::vector<ob::SnapshotRow> out;
    uint16_t level = 0;
    for (auto [p, r] : bids) {
        r.level_index = level++;
        out.push_back(r);
    }
    level = 0;
    for (auto [p, r] : asks) {
        r.level_index = level++;
        out.push_back(r);
    }
    return levels_of(out);
}

/// A store fed the way the engine feeds it: rows are published as a block, which a seal later
/// replaces with a segment written beside the index - so a row reaches the index only through
/// `publish_blocks()`, where a summary it falsifies leaves.
struct Store {
    TempDir dir;
    std::unique_ptr<ob::ColumnarStore> store = std::make_unique<ob::ColumnarStore>(dir.path.string());
    std::vector<std::vector<ob::SnapshotRow>> pending;   ///< published, not yet sealed

    void block(const std::vector<ob::SnapshotRow>& rows) {
        store->publish_blocks({ob::RowBlock::make("SUM", "EX", rows)});
        pending.push_back(rows);
    }
    /// Every block published so far, sealed into one segment: its directory.
    std::string seal() {
        ob::ColumnarStore writer(dir.path.string(), ob::ColumnarStore::kDefaultSegmentDurationNs,
                                 ob::ColumnarStore::OwnIndex::kNo);
        writer.set_symbol_exchange("SUM", "EX");
        for (const auto& rows : pending) {
            for (const auto& r : rows) writer.append(r);
        }
        auto meta = writer.flush_segment();
        EXPECT_TRUE(meta.has_value());
        if (!meta) return {};
        EXPECT_EQ(store->seal_blocks("SUM", "EX", pending.size(), {*meta}), 0u);
        pending.clear();
        return meta->dir_path;
    }
    std::string segment(const std::vector<ob::SnapshotRow>& rows) {
        block(rows);
        return seal();
    }
    ob::ColumnarStore::PricedBook at(uint64_t t) const { return store->book_at_by_price(t, "SUM", "EX"); }
    /// What a restart does: the index read from the disk, and the rows no seal took published again,
    /// as the WAL's replay publishes them.
    void reopen() {
        store->close();
        store = std::make_unique<ob::ColumnarStore>(dir.path.string());
        store->open_existing();
        const auto again = std::move(pending);
        pending.clear();
        for (const auto& rows : again) block(rows);
    }
};

}  // namespace

// ── The file ──────────────────────────────────────────────────────────────────

TEST(BookSummaryFile, ReadsBackWhatWasWritten) {
    ob::BookSummary s;
    s.header.as_of_ts  = kBase + 7;
    s.header.as_of_seq = 42;
    s.rows = {row(kBase + 1, ob::SIDE_BID, 101, 5, 3), row(kBase + 2, ob::SIDE_BID, 99, 7, 4),
              row(kBase + 3, ob::SIDE_ASK, 103, 9, 5)};
    s.header.rows = 3;
    ob::BookSummary back;
    ASSERT_EQ(ob::decode_book_summary(ob::encode_book_summary(s), back), "");
    EXPECT_EQ(back.header.as_of_ts, kBase + 7);
    EXPECT_EQ(back.header.as_of_seq, 42u);
    ASSERT_EQ(back.rows.size(), 3u);
    for (size_t i = 0; i < 3; ++i) {
        EXPECT_EQ(back.rows[i].price, s.rows[i].price);
        EXPECT_EQ(back.rows[i].quantity, s.rows[i].quantity);
        EXPECT_EQ(back.rows[i].order_count, s.rows[i].order_count);
        EXPECT_EQ(back.rows[i].timestamp_ns, s.rows[i].timestamp_ns);
        EXPECT_EQ(back.rows[i].sequence_number, s.rows[i].sequence_number);
        EXPECT_EQ(back.rows[i].side, s.rows[i].side);
    }
    // A row's level is its position in its side.
    EXPECT_EQ(back.rows[1].level_index, 1u);
    EXPECT_EQ(back.rows[2].level_index, 0u);
}

TEST(BookSummaryFile, RefusesWhatIsNotOneAndSaysWhy) {
    ob::BookSummary s;
    s.header.as_of_ts = kBase;
    s.rows = {row(kBase, ob::SIDE_BID, 100, 1)};
    s.header.rows = 1;
    const std::string good = ob::encode_book_summary(s);
    ob::BookSummary back;

    std::string flipped = good;
    flipped.back() ^= 1;   // inside the last column: the prices' CRC
    EXPECT_NE(ob::decode_book_summary(flipped, back).find("CRC"), std::string::npos);
    std::string header = good;
    header[20] ^= 1;       // inside as_of_ts: the header's CRC
    EXPECT_NE(ob::decode_book_summary(header, back).find("header fails its CRC"), std::string::npos);
    EXPECT_NE(ob::decode_book_summary(good.substr(0, 10), back).find("shorter than its header"),
              std::string::npos);
    EXPECT_NE(ob::decode_book_summary(good.substr(0, good.size() - 1), back).find("CRC"),
              std::string::npos);
    std::string magic = good;
    magic[0] = 'X';
    EXPECT_NE(ob::decode_book_summary(magic, back).find("magic"), std::string::npos);
    // The control: the bytes as written read back.
    EXPECT_EQ(ob::decode_book_summary(good, back), "");
}

TEST(BookSummaryFile, ABookOfClosePricesFromOneUpdateIsAFewBytesAPrice) {
    // What the columns are for: 1000 prices a tick apart, all set by one update, cost a few bytes a
    // price - fixed records cost forty, which made a summary of a deep book larger than the rows
    // between two of them.
    ob::BookSummary s;
    s.header.as_of_ts = kBase;
    for (int64_t i = 0; i < 1000; ++i) {
        ob::SnapshotRow r = row(kBase, ob::SIDE_BID, 1'000'000 - i, static_cast<uint64_t>(1 + i % 50), 7);
        s.rows.push_back(r);
    }
    s.header.rows = 1000;
    const std::string bytes = ob::encode_book_summary(s);
    EXPECT_LT(bytes.size(), 1000u * 6) << bytes.size() << " bytes for 1000 prices";
    ob::BookSummary back;
    ASSERT_EQ(ob::decode_book_summary(bytes, back), "");
    ASSERT_EQ(back.rows.size(), 1000u);
    EXPECT_EQ(back.rows[999].price, 1'000'000 - 999);
    EXPECT_EQ(back.rows[999].quantity, 1u + 999 % 50);
}

TEST(BookSummaryFile, ItsNameIsOneOfASegmentsFiles) {
    EXPECT_TRUE(ob::is_segment_file(ob::book_summary_file(kBase)));
    EXPECT_TRUE(ob::is_book_summary_file("book.0.v1"));
    EXPECT_FALSE(ob::is_book_summary_file("book..v1"));
    EXPECT_FALSE(ob::is_book_summary_file("book.12x.v1"));
    EXPECT_FALSE(ob::is_book_summary_file("book.12.v2"));
    EXPECT_FALSE(ob::is_segment_file("book.12.v1.tmp"));
}

// ── A store that writes them ──────────────────────────────────────────────────

TEST(BookSummaryStore, AReadAfterASummaryStartsFromItAndAnswersTheSame) {
    Store s;
    s.segment({row(kBase + 1, ob::SIDE_BID, 100, 1), row(kBase + 2, ob::SIDE_BID, 99, 2),
               row(kBase + 3, ob::SIDE_ASK, 101, 3)});
    const std::string dir = s.segment({row(kBase + 4, ob::SIDE_BID, 100, 0),
                                       row(kBase + 5, ob::SIDE_BID, 98, 4)});
    const auto w = s.store->write_book_summary("SUM", "EX", dir);
    ASSERT_TRUE(w.written) << w.why_not;
    EXPECT_EQ(w.as_of_ts, kBase + 5);
    EXPECT_EQ(s.store->book_summaries("SUM", "EX"), (std::vector<uint64_t>{kBase + 5}));
    EXPECT_TRUE(fs::exists(fs::path(dir) / ob::book_summary_file(kBase + 5)));
    // A second has nothing newer to say until a row after it is sealed.
    const auto again = s.store->write_book_summary("SUM", "EX", dir);
    EXPECT_FALSE(again.written);
    EXPECT_EQ(again.why_not, "a summary as of that time is there");

    s.segment({row(kBase + 6, ob::SIDE_ASK, 102, 6), row(kBase + 7, ob::SIDE_BID, 99, 0)});
    for (uint64_t t = kBase; t <= kBase + 8; ++t) {
        const auto book = s.at(t);
        EXPECT_EQ(levels_of(book.rows), composed(*s.store, t)) << "at +" << t - kBase;
        if (t >= kBase + 5) {
            ASSERT_TRUE(book.from_summary.has_value()) << "at +" << t - kBase;
            EXPECT_EQ(*book.from_summary, kBase + 5);
        } else {
            EXPECT_FALSE(book.from_summary.has_value()) << "a read before the summary started from it";
        }
    }
    // Now it has: a summary as of the last row sealed, which a read at its time starts from.
    const auto later = s.store->write_book_summary("SUM", "EX", dir);
    ASSERT_TRUE(later.written) << later.why_not;
    EXPECT_EQ(later.as_of_ts, kBase + 7);
    EXPECT_EQ(*s.at(kBase + 7).from_summary, kBase + 7);
}

TEST(BookSummaryStore, ARowAtOrBeforeItsTimeArrivingAfterItFalsifiesIt) {
    Store s;
    const std::string dir = s.segment({row(kBase + 10, ob::SIDE_BID, 100, 1),
                                       row(kBase + 20, ob::SIDE_BID, 99, 2)});
    ASSERT_TRUE(s.store->write_book_summary("SUM", "EX", dir).written);
    ASSERT_EQ(s.store->book_summaries("SUM", "EX").size(), 1u);

    // A row after its time leaves it alone.
    s.block({row(kBase + 21, ob::SIDE_BID, 97, 3)});
    EXPECT_EQ(s.store->book_summaries("SUM", "EX").size(), 1u);
    // A client's event time before it does not: the summary does not hold this row.
    s.block({row(kBase + 15, ob::SIDE_BID, 100, 0)});
    EXPECT_TRUE(s.store->book_summaries("SUM", "EX").empty()) << "a falsified summary stayed";
    EXPECT_FALSE(fs::exists(fs::path(dir) / ob::book_summary_file(kBase + 20)))
        << "its file stayed, for the next start to believe";
    for (uint64_t t : {kBase + 15, kBase + 20, kBase + 21}) {
        const auto book = s.at(t);
        EXPECT_FALSE(book.from_summary.has_value());
        EXPECT_EQ(levels_of(book.rows), composed(*s.store, t));
    }
}

TEST(BookSummaryStore, ARowAtExactlyItsTimeFalsifiesIt) {
    Store s;
    const std::string dir = s.segment({row(kBase + 10, ob::SIDE_BID, 100, 1)});
    ASSERT_TRUE(s.store->write_book_summary("SUM", "EX", dir).written);
    s.block({row(kBase + 10, ob::SIDE_BID, 100, 0)});
    EXPECT_TRUE(s.store->book_summaries("SUM", "EX").empty());
    EXPECT_TRUE(s.at(kBase + 10).rows.empty());
}

TEST(BookSummaryStore, AStartReadsThemBackAndAReadStartsFromThem) {
    Store s;
    s.segment({row(kBase + 1, ob::SIDE_BID, 100, 1), row(kBase + 2, ob::SIDE_ASK, 101, 2)});
    const std::string dir = s.segment({row(kBase + 3, ob::SIDE_BID, 100, 5)});
    ASSERT_TRUE(s.store->write_book_summary("SUM", "EX", dir).written);
    const auto before = levels_of(s.at(kBase + 3).rows);
    s.reopen();
    EXPECT_EQ(s.store->book_summaries("SUM", "EX"), (std::vector<uint64_t>{kBase + 3}));
    const auto book = s.at(kBase + 3);
    ASSERT_TRUE(book.from_summary.has_value());
    EXPECT_EQ(levels_of(book.rows), before);
}

TEST(BookSummaryStore, ASummaryThatDoesNotReadBackIsNotStartedFrom) {
    Store s;
    s.segment({row(kBase + 1, ob::SIDE_BID, 100, 1)});
    const std::string dir = s.segment({row(kBase + 2, ob::SIDE_BID, 99, 2)});
    ASSERT_TRUE(s.store->write_book_summary("SUM", "EX", dir).written);
    // A price of it damaged on the disk: the header still reads, the prices do not.
    const fs::path file = fs::path(dir) / ob::book_summary_file(kBase + 2);
    {
        std::fstream f(file, std::ios::in | std::ios::out | std::ios::binary);
        f.seekp(-1, std::ios::end);
        f.put('\x7f');
    }
    const auto book = s.at(kBase + 2);
    EXPECT_FALSE(book.from_summary.has_value());
    EXPECT_EQ(levels_of(book.rows), composed(*s.store, kBase + 2));
}

TEST(BookSummaryStore, ASegmentLeavingTakesItsSummariesWithIt) {
    // Another segment of the symbol stays, so the symbol's entry in the index stays too: what goes
    // is the summary whose file went with its directory, and the read does not start from it.
    Store s;
    s.segment({row(kBase + 1, ob::SIDE_BID, 100, 1)});
    const std::string dir = s.segment({row(kBase + 2, ob::SIDE_BID, 99, 2)});
    ASSERT_TRUE(s.store->write_book_summary("SUM", "EX", dir).written);
    ASSERT_EQ(s.store->remove_segments({dir}), 1u);
    EXPECT_TRUE(s.store->book_summaries("SUM", "EX").empty());
    const auto book = s.at(kBase + 2);
    EXPECT_FALSE(book.from_summary.has_value());
    EXPECT_EQ(levels_of(book.rows), composed(*s.store, kBase + 2));
}

TEST(BookSummaryStore, RetentionTakesTheSummariesOfWhatItRemoves) {
    Store s;
    const std::string old_dir = s.segment({row(kBase + 1, ob::SIDE_BID, 100, 1)});
    ASSERT_TRUE(s.store->write_book_summary("SUM", "EX", old_dir).written);
    const std::string new_dir = s.segment({row(kBase + 50 * kSec, ob::SIDE_BID, 99, 2)});
    ASSERT_TRUE(s.store->write_book_summary("SUM", "EX", new_dir).written);
    ASSERT_EQ(s.store->book_summaries("SUM", "EX").size(), 2u);
    s.store->delete_expired_segments(kBase + 10 * kSec);
    // The newer summary stays, and still holds the price the expired segment set: the book does
    // not lose a price because the row that set it aged out (spec incremental-books).
    EXPECT_EQ(s.store->book_summaries("SUM", "EX"), (std::vector<uint64_t>{kBase + 50 * kSec}));
    const auto book = s.at(kBase + 50 * kSec);
    ASSERT_TRUE(book.from_summary.has_value());
    ASSERT_EQ(book.rows.size(), 2u);
    EXPECT_EQ(book.rows[0].price, 100);
}

// Random rows - in segments and blocks, a quarter of them removals, some with times before what
// was written already - and summaries written between them: every read at every time answers what
// composing every row answers, whether it started from a summary or not.
RC_GTEST_PROP(BookSummaryProperty, AReadFromASummaryIsTheReadFromTheFirstRow, ()) {
    Store s;
    uint64_t latest = kBase;
    const int steps = *rc::gen::inRange(1, 12);
    for (int i = 0; i < steps; ++i) {
        std::vector<ob::SnapshotRow> rows;
        const int n = *rc::gen::inRange(1, 8);
        for (int k = 0; k < n; ++k) {
            // Mostly after what came before, sometimes before it: a client's event time.
            const bool late = *rc::gen::inRange(0, 6) == 0;
            const uint64_t ts = late ? kBase + static_cast<uint64_t>(*rc::gen::inRange(0, 40))
                                     : latest + static_cast<uint64_t>(*rc::gen::inRange(0, 4));
            latest = std::max(latest, ts);
            const uint64_t qty = *rc::gen::inRange(0, 4) == 0 ? 0 : static_cast<uint64_t>(*rc::gen::inRange(1, 50));
            rows.push_back(row(ts, *rc::gen::inRange(0, 2) == 0 ? ob::SIDE_BID : ob::SIDE_ASK,
                               90 + *rc::gen::inRange<int64_t>(0, 15), qty, static_cast<uint64_t>(i * 10 + k)));
        }
        if (*rc::gen::inRange(0, 3) == 0) {
            s.block(rows);
        } else {
            const std::string dir = s.segment(rows);
            if (*rc::gen::inRange(0, 2) == 0) s.store->write_book_summary("SUM", "EX", dir);
        }
    }
    if (*rc::gen::inRange(0, 3) == 0) s.reopen();   // the blocks go with the process, as on a crash
    for (uint64_t t = kBase; t <= latest + 1; ++t) {
        const auto book = s.at(t);
        RC_ASSERT(levels_of(book.rows) == composed(*s.store, t));
    }
}
