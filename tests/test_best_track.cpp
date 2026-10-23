// Best tracks (#243, `best_track.hpp`): what a series of the book reads where it can.
//
// A track is the best price of each side of one symbol's book after every instant at which it
// changed, from the instant a book summary's composition started from to the instant the summary is
// as of, written beside the summary. What these tests hold is that a track says what composing the
// rows says - the oracle below, from a plain scan - and that it leaves the index with its summary,
// through what can make one false: a row at or before its time arriving after it, a restart that
// reads it back, a file that does not read back.

#include "orderbook/aggregation.hpp"
#include "orderbook/best_track.hpp"
#include "orderbook/columnar_store.hpp"
#include "orderbook/data_model.hpp"
#include "orderbook/query_engine.hpp"
#include "orderbook/soa_buffer.hpp"

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
#include <vector>

namespace fs = std::filesystem;

namespace {

constexpr uint64_t kSec  = 1'000'000'000ULL;
constexpr uint64_t kBase = 1'790'000'000ULL * kSec;

std::atomic<uint64_t> g_counter{0};

struct TempDir {
    fs::path path;
    TempDir()
        : path(fs::temp_directory_path() / ("ob_best_track_" + std::to_string(::getpid()) + "_" +
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

ob::BookTop top(bool has_bid, int64_t bid, bool has_ask, int64_t ask) {
    ob::BookTop t;
    t.has_bid = has_bid;
    t.bid = has_bid ? bid : 0;
    t.has_ask = has_ask;
    t.ask = has_ask ? ask : 0;
    return t;
}

/// The oracle: the best of each side after every instant in (from, as_of] at which it changed,
/// from every row a plain scan delivers, sorted by time and stable, applied by price - with the best
/// in force at `from` first.
std::vector<ob::BestTrackEntry> track_of_rows(const ob::ColumnarStore& store, uint64_t from,
                                              uint64_t as_of) {
    std::vector<ob::SnapshotRow> rows;
    store.scan(0, as_of, "SUM", "EX", ob::ColumnSet::all(),
               [&](const ob::SnapshotRow& r) { rows.push_back(r); });
    std::stable_sort(rows.begin(), rows.end(), [](const ob::SnapshotRow& a, const ob::SnapshotRow& b) {
        return a.timestamp_ns < b.timestamp_ns;
    });
    std::map<int64_t, uint64_t, std::greater<>> bids;
    std::map<int64_t, uint64_t> asks;
    const auto now = [&]() {
        return top(!bids.empty(), bids.empty() ? 0 : bids.begin()->first, !asks.empty(),
                   asks.empty() ? 0 : asks.begin()->first);
    };
    std::vector<ob::BestTrackEntry> out;
    size_t i = 0;
    for (; i < rows.size() && rows[i].timestamp_ns <= from; ++i) {
        if (rows[i].side == ob::SIDE_BID) {
            if (rows[i].quantity == 0) bids.erase(rows[i].price); else bids[rows[i].price] = rows[i].quantity;
        } else {
            if (rows[i].quantity == 0) asks.erase(rows[i].price); else asks[rows[i].price] = rows[i].quantity;
        }
    }
    out.push_back(ob::BestTrackEntry{from, now()});
    while (i < rows.size()) {
        const uint64_t t = rows[i].timestamp_ns;
        for (; i < rows.size() && rows[i].timestamp_ns == t; ++i) {
            if (rows[i].side == ob::SIDE_BID) {
                if (rows[i].quantity == 0) bids.erase(rows[i].price); else bids[rows[i].price] = rows[i].quantity;
            } else {
                if (rows[i].quantity == 0) asks.erase(rows[i].price); else asks[rows[i].price] = rows[i].quantity;
            }
        }
        if (!(now() == out.back().top)) out.push_back(ob::BestTrackEntry{t, now()});
    }
    return out;
}

/// A store fed the way the engine feeds it: rows are published as a block, which a seal later
/// replaces with a segment written beside the index - so a row reaches the index only through
/// `publish_blocks()`, where a track it falsifies leaves.
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
    ob::ColumnarStore::SummaryWrite summarise(const std::string& dir_path) {
        return store->write_book_summary("SUM", "EX", dir_path);
    }
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

TEST(BestTrackFile, ReadsBackWhatWasWritten) {
    ob::BestTrack t;
    t.header.from_ts = kBase;
    t.header.as_of_ts = kBase + 9;
    t.entries = {{kBase, top(false, 0, false, 0)},
                 {kBase + 1, top(true, 100, false, 0)},
                 {kBase + 2, top(true, 101, true, 105)},
                 {kBase + 5, top(false, 0, true, 104)},
                 {kBase + 9, top(true, -3, true, 104)}};
    t.header.entries = static_cast<uint32_t>(t.entries.size());
    ob::BestTrack back;
    ASSERT_EQ(ob::decode_best_track(ob::encode_best_track(t), back), "");
    EXPECT_EQ(back.header.from_ts, kBase);
    EXPECT_EQ(back.header.as_of_ts, kBase + 9);
    EXPECT_FALSE(back.header.from_first_row);
    EXPECT_EQ(back.entries, t.entries);
}

TEST(BestTrackFile, RefusesWhatIsNotOneAndSaysWhy) {
    ob::BestTrack t;
    t.header.from_ts = kBase;
    t.header.as_of_ts = kBase + 4;
    t.entries = {{kBase, top(true, 100, true, 101)}, {kBase + 4, top(true, 99, true, 101)}};
    t.header.entries = 2;
    const std::string good = ob::encode_best_track(t);
    ob::BestTrack back;

    std::string flipped = good;
    flipped.back() ^= 1;   // inside the last column: the entries' CRC
    EXPECT_NE(ob::decode_best_track(flipped, back).find("entries fail their CRC"), std::string::npos);
    std::string header = good;
    header[20] ^= 1;       // inside from_ts: the header's CRC
    EXPECT_NE(ob::decode_best_track(header, back).find("header fails its CRC"), std::string::npos);
    EXPECT_NE(ob::decode_best_track(good.substr(0, 10), back).find("shorter than its header"),
              std::string::npos);
    std::string magic = good;
    magic[0] = 'X';
    EXPECT_NE(ob::decode_best_track(magic, back).find("magic"), std::string::npos);

    // Entries a reader could not rely on: one before its predecessor, one past the span, and the
    // first anywhere but the start.
    for (const auto& bad : std::vector<std::vector<ob::BestTrackEntry>>{
             {{kBase, top(true, 1, true, 2)}, {kBase + 3, top(true, 1, true, 3)}, {kBase + 2, top(true, 1, true, 4)}},
             {{kBase, top(true, 1, true, 2)}, {kBase + 5, top(true, 1, true, 3)}},
             {{kBase + 1, top(true, 1, true, 2)}}}) {
        ob::BestTrack b = t;
        b.entries = bad;
        b.header.entries = static_cast<uint32_t>(bad.size());
        EXPECT_NE(ob::decode_best_track(ob::encode_best_track(b), back).find("out of its place"),
                  std::string::npos);
    }
    ob::BestTrack none = t;
    none.entries.clear();
    none.header.entries = 0;
    EXPECT_NE(ob::decode_best_track(ob::encode_best_track(none), back).find("no entry"),
              std::string::npos);
    // The control: the bytes as written read back.
    EXPECT_EQ(ob::decode_best_track(good, back), "");
}

TEST(BestTrackFile, IsOneOfASegmentsFiles) {
    EXPECT_TRUE(ob::is_best_track_file(ob::best_track_file(kBase)));
    EXPECT_TRUE(ob::is_segment_file(ob::best_track_file(kBase)));
    EXPECT_FALSE(ob::is_best_track_file("best.v1"));
    EXPECT_FALSE(ob::is_best_track_file("best.12a.v1"));
    EXPECT_FALSE(ob::is_best_track_file(ob::book_summary_file(kBase)));
}

// ── Written beside a summary ──────────────────────────────────────────────────

TEST(BestTrackWrite, ATrackSaysWhatTheRowsSayBetweenTwoSummaries) {
    Store s;
    // Bids walking up a tick an instant, with the best removed now and then; asks moved by several
    // rows of one instant.
    std::vector<ob::SnapshotRow> first;
    for (uint64_t i = 0; i < 50; ++i) {
        first.push_back(row(kBase + i * 10, ob::SIDE_BID, 1000 + static_cast<int64_t>(i % 7), 1 + i % 3));
        first.push_back(row(kBase + i * 10, ob::SIDE_ASK, 1100 - static_cast<int64_t>(i % 5), 2));
    }
    const std::string d1 = s.segment(first);
    const auto w1 = s.summarise(d1);
    ASSERT_TRUE(w1.written) << w1.why_not;
    ASSERT_GT(w1.track_entries, 1u) << w1.track_why_not;
    std::vector<ob::SnapshotRow> second;
    for (uint64_t i = 50; i < 120; ++i) {
        const int64_t best = 1000 + static_cast<int64_t>(i % 9);
        second.push_back(row(kBase + i * 10, ob::SIDE_BID, best, i % 4));   // a quantity of 0 removes it
        second.push_back(row(kBase + i * 10, ob::SIDE_ASK, 1090 + static_cast<int64_t>(i % 3), 1));
        second.push_back(row(kBase + i * 10, ob::SIDE_ASK, 1089 + static_cast<int64_t>(i % 3), i % 2));
    }
    const std::string d2 = s.segment(second);
    const auto w2 = s.summarise(d2);
    ASSERT_TRUE(w2.written) << w2.why_not;
    ASSERT_EQ(s.store->best_tracks("SUM", "EX"), (std::vector<uint64_t>{w1.as_of_ts, w2.as_of_ts}));

    ob::BestTrack t1;
    ASSERT_EQ(ob::read_best_track((fs::path(d1) / ob::best_track_file(w1.as_of_ts)).string(), t1), "");
    EXPECT_TRUE(t1.header.from_first_row);
    EXPECT_EQ(t1.header.from_ts, 0u);
    EXPECT_EQ(t1.entries, track_of_rows(*s.store, 0, w1.as_of_ts));
    ob::BestTrack t2;
    ASSERT_EQ(ob::read_best_track((fs::path(d2) / ob::best_track_file(w2.as_of_ts)).string(), t2), "");
    EXPECT_FALSE(t2.header.from_first_row);
    EXPECT_EQ(t2.header.from_ts, w1.as_of_ts);
    EXPECT_EQ(t2.header.entries, w2.track_entries);
    EXPECT_EQ(t2.entries, track_of_rows(*s.store, w1.as_of_ts, w2.as_of_ts));
}

TEST(BestTrackWrite, ALateRowRemovesTheTrackWithItsSummary) {
    Store s;
    const std::string d1 = s.segment({row(kBase + 10, ob::SIDE_BID, 100, 1)});
    ASSERT_TRUE(s.summarise(d1).written);
    const std::string d2 = s.segment({row(kBase + 20, ob::SIDE_BID, 101, 1)});
    const auto w2 = s.summarise(d2);
    ASSERT_TRUE(w2.written);
    ASSERT_EQ(s.store->best_tracks("SUM", "EX").size(), 2u);
    const std::string file2 = (fs::path(d2) / ob::best_track_file(w2.as_of_ts)).string();
    ASSERT_TRUE(fs::exists(file2));

    // A row later than both leaves them; one between them takes the second and leaves the first.
    s.block({row(kBase + 30, ob::SIDE_BID, 102, 1)});
    EXPECT_EQ(s.store->best_tracks("SUM", "EX").size(), 2u);
    s.block({row(kBase + 15, ob::SIDE_BID, 103, 1)});
    EXPECT_EQ(s.store->best_tracks("SUM", "EX"), (std::vector<uint64_t>{kBase + 10}));
    EXPECT_FALSE(fs::exists(file2)) << "a falsified track's file stayed";
    EXPECT_EQ(s.store->book_summaries("SUM", "EX"), (std::vector<uint64_t>{kBase + 10}));
}

TEST(BestTrackWrite, AReopenedStoreHasItsTracks) {
    Store s;
    const std::string d1 = s.segment({row(kBase + 10, ob::SIDE_BID, 100, 1), row(kBase + 10, ob::SIDE_ASK, 105, 1)});
    const auto w1 = s.summarise(d1);
    ASSERT_TRUE(w1.written);
    const std::string d2 = s.segment({row(kBase + 20, ob::SIDE_BID, 101, 1)});
    const auto w2 = s.summarise(d2);
    ASSERT_TRUE(w2.written);
    s.reopen();
    EXPECT_EQ(s.store->best_tracks("SUM", "EX"), (std::vector<uint64_t>{w1.as_of_ts, w2.as_of_ts}));
}

TEST(BestTrackWrite, ATrackThatDoesNotReadBackIsNotRegistered) {
    Store s;
    const std::string d1 = s.segment({row(kBase + 10, ob::SIDE_BID, 100, 1)});
    const auto w1 = s.summarise(d1);
    ASSERT_TRUE(w1.written);
    const std::string file = (fs::path(d1) / ob::best_track_file(w1.as_of_ts)).string();
    {
        std::fstream f(file, std::ios::in | std::ios::out | std::ios::binary);
        f.seekp(20);
        f.put('\x7f');   // inside from_ts: the header's CRC
    }
    s.reopen();
    EXPECT_TRUE(s.store->best_tracks("SUM", "EX").empty());
    EXPECT_EQ(s.store->book_summaries("SUM", "EX"), (std::vector<uint64_t>{w1.as_of_ts}));
}

// ── Read by a series ──────────────────────────────────────────────────────────

namespace {

using Tops = std::vector<ob::BestTrackEntry>;

/// What tops_by_time() hands over: the best in force before `lo`, stamped `lo - 1` (or 0), then each
/// instant at which it changed.
Tops tops_read(const ob::ColumnarStore& store, uint64_t lo, uint64_t read_to,
               ob::ColumnarStore::TopsCost* cost = nullptr) {
    Tops out;
    const auto c = store.tops_by_time(
        lo, read_to, "SUM", "EX",
        [&](const ob::BookTop& before) { out.push_back({lo == 0 ? 0 : lo - 1, before}); },
        [&](uint64_t t, const ob::BookTop& after) {
            out.push_back({t, after});
            return true;
        });
    if (cost != nullptr) *cost = c;
    return out;
}

/// The oracle: every row a plain scan delivers, sorted by time and stable, applied by price - the
/// best before `lo`, then the best after each instant of [lo, read_to] at which it changed.
Tops tops_of_rows(const ob::ColumnarStore& store, uint64_t lo, uint64_t read_to) {
    std::vector<ob::SnapshotRow> rows;
    store.scan(0, UINT64_MAX, "SUM", "EX", ob::ColumnSet::all(),
               [&](const ob::SnapshotRow& r) { rows.push_back(r); });
    std::stable_sort(rows.begin(), rows.end(), [](const ob::SnapshotRow& a, const ob::SnapshotRow& b) {
        return a.timestamp_ns < b.timestamp_ns;
    });
    std::map<int64_t, uint64_t, std::greater<>> bids;
    std::map<int64_t, uint64_t> asks;
    const auto apply = [&](const ob::SnapshotRow& r) {
        if (r.side == ob::SIDE_BID) {
            if (r.quantity == 0) bids.erase(r.price); else bids[r.price] = r.quantity;
        } else {
            if (r.quantity == 0) asks.erase(r.price); else asks[r.price] = r.quantity;
        }
    };
    const auto now = [&]() {
        return top(!bids.empty(), bids.empty() ? 0 : bids.begin()->first, !asks.empty(),
                   asks.empty() ? 0 : asks.begin()->first);
    };
    size_t i = 0;
    for (; i < rows.size() && rows[i].timestamp_ns < lo; ++i) apply(rows[i]);
    Tops out{{lo == 0 ? 0 : lo - 1, now()}};
    while (i < rows.size() && rows[i].timestamp_ns <= read_to) {
        const uint64_t t = rows[i].timestamp_ns;
        for (; i < rows.size() && rows[i].timestamp_ns == t; ++i) apply(rows[i]);
        if (!(now() == out.back().top)) out.push_back({t, now()});
    }
    return out;
}

}  // namespace

RC_GTEST_PROP(BestTrackRead, TheBestFromTracksIsTheBestFromTheRows, ()) {
    Store s;
    uint64_t t = kBase;
    // Segments of instants of a few rows each, either side, in a band of prices, a quantity of 0
    // removing one - the best set, moved and taken away - and a summary with its track after some.
    const int segments = *rc::gen::inRange(1, 9);
    for (int seg = 0; seg < segments; ++seg) {
        std::vector<ob::SnapshotRow> rows;
        const int instants = *rc::gen::inRange(1, 25);
        for (int i = 0; i < instants; ++i) {
            t += *rc::gen::inRange<uint64_t>(1, 4);
            const int n = *rc::gen::inRange(1, 5);
            for (int j = 0; j < n; ++j) {
                rows.push_back(row(t, *rc::gen::element(ob::SIDE_BID, ob::SIDE_ASK),
                                   *rc::gen::inRange<int64_t>(95, 106), *rc::gen::inRange<uint64_t>(0, 3)));
            }
        }
        const std::string dir = s.segment(rows);
        if (*rc::gen::arbitrary<bool>()) s.summarise(dir);
    }
    // Rows no seal took yet, and now and then rows earlier than the latest: a late row falsifies the
    // tracks it lands in.
    if (*rc::gen::arbitrary<bool>()) {
        std::vector<ob::SnapshotRow> rows;
        const uint64_t at = *rc::gen::inRange<uint64_t>(kBase, t + 3);
        for (int j = *rc::gen::inRange(1, 4); j > 0; --j) {
            rows.push_back(row(at, *rc::gen::element(ob::SIDE_BID, ob::SIDE_ASK),
                               *rc::gen::inRange<int64_t>(95, 106), *rc::gen::inRange<uint64_t>(0, 3)));
        }
        s.block(rows);
    }
    bool read_a_track = false;
    for (int q = 0; q < 6; ++q) {
        const uint64_t lo = *rc::gen::inRange<uint64_t>(kBase - 2, t + 3);
        const uint64_t read_to = *rc::gen::inRange<uint64_t>(lo, t + 4);
        const Tops expected = tops_of_rows(*s.store, lo, read_to);
        ob::ColumnarStore::TopsCost cost;
        s.store->set_read_best_tracks(true);
        RC_ASSERT(tops_read(*s.store, lo, read_to, &cost) == expected);
        read_a_track = read_a_track || cost.tracks > 0;
        s.store->set_read_best_tracks(false);
        ob::ColumnarStore::TopsCost rows_cost;
        RC_ASSERT(tops_read(*s.store, lo, read_to, &rows_cost) == expected);
        RC_ASSERT(rows_cost.tracks == 0u);
    }
    RC_CLASSIFY(read_a_track, "a track read");
}

TEST(BestTrackRead, ARangeInsideTracksReadsNoRow) {
    // Three summaries with their tracks, and a range inside the second and third: the best comes
    // from their entries alone, and no book before the range is composed.
    Store s;
    std::vector<std::string> dirs;
    for (uint64_t seg = 0; seg < 3; ++seg) {
        std::vector<ob::SnapshotRow> rows;
        for (uint64_t i = 0; i < 40; ++i) {
            // Each instant the whole of each side's band: one price held, the rest removed, so the
            // best walks.
            const uint64_t at = kBase + seg * 1000 + i * 10;
            for (int64_t p = 100; p < 105; ++p) {
                rows.push_back(row(at, ob::SIDE_BID, p, p == 100 + static_cast<int64_t>((seg + i) % 5) ? 1 : 0));
            }
            for (int64_t p = 108; p < 111; ++p) {
                rows.push_back(row(at, ob::SIDE_ASK, p, p == 110 - static_cast<int64_t>((seg + i) % 3) ? 1 : 0));
            }
        }
        dirs.push_back(s.segment(rows));
        ASSERT_TRUE(s.summarise(dirs.back()).written);
    }
    const uint64_t lo = kBase + 1005;
    const uint64_t read_to = kBase + 2200;
    ob::ColumnarStore::TopsCost cost;
    EXPECT_EQ(tops_read(*s.store, lo, read_to, &cost), tops_of_rows(*s.store, lo, read_to));
    EXPECT_EQ(cost.tracks, 2u);
    EXPECT_GT(cost.track_entries, 0u);
    EXPECT_EQ(cost.rows.kept, 0u) << "a row was read where the tracks cover the range";
    EXPECT_EQ(cost.before_read, 0u) << "a book was composed before a range a track covers";
}

TEST(BestTrackRead, RowsAfterTheLastTrackStartFromItsSummary) {
    Store s;
    std::vector<ob::SnapshotRow> rows;
    for (uint64_t i = 0; i < 30; ++i) rows.push_back(row(kBase + i, ob::SIDE_BID, 100 + static_cast<int64_t>(i % 4), 1));
    const std::string dir = s.segment(rows);
    ASSERT_TRUE(s.summarise(dir).written);
    // After the summary: a removal of the best, which only the summary's book can answer.
    s.segment({row(kBase + 40, ob::SIDE_BID, 103, 0), row(kBase + 41, ob::SIDE_BID, 102, 0)});
    s.block({row(kBase + 50, ob::SIDE_BID, 101, 0)});
    ob::ColumnarStore::TopsCost cost;
    EXPECT_EQ(tops_read(*s.store, kBase + 10, kBase + 60, &cost), tops_of_rows(*s.store, kBase + 10, kBase + 60));
    EXPECT_EQ(cost.tracks, 1u);
    EXPECT_EQ(cost.before_read, 0u) << "the book after the track was composed, not read from its summary";
    EXPECT_EQ(cost.rows.kept, 3u);
}

TEST(BestTrackRead, AnUnreadableTrackLeavesTheSeriesToTheRows) {
    Store s;
    std::vector<ob::SnapshotRow> rows;
    for (uint64_t i = 0; i < 30; ++i) rows.push_back(row(kBase + i, ob::SIDE_ASK, 200 - static_cast<int64_t>(i % 6), 1));
    const std::string dir = s.segment(rows);
    const auto w = s.summarise(dir);
    ASSERT_TRUE(w.written);
    {
        std::fstream f((fs::path(dir) / ob::best_track_file(w.as_of_ts)).string(),
                       std::ios::in | std::ios::out | std::ios::binary);
        f.seekp(-1, std::ios::end);
        f.put('\x55');   // inside the last column: the entries' CRC
    }
    ob::ColumnarStore::TopsCost cost;
    EXPECT_EQ(tops_read(*s.store, kBase + 5, kBase + 29, &cost), tops_of_rows(*s.store, kBase + 5, kBase + 29));
    EXPECT_EQ(cost.tracks, 0u);
    EXPECT_GT(cost.rows.kept, 0u);
}

// ── The series an engine answers ──────────────────────────────────────────────

namespace {

/// A bucket answer as (bucket start, values), NULL as INT64_MIN; the error alone when there is one.
struct SeriesAnswer {
    std::string error;
    std::vector<std::pair<uint64_t, std::vector<int64_t>>> buckets;
    bool operator==(const SeriesAnswer&) const = default;
};

SeriesAnswer series(ob::QueryEngine& q, const std::string& sql) {
    SeriesAnswer a;
    ob::QueryShape shape;
    a.error = q.execute(sql, [&](const ob::QueryResult& r) {
        std::vector<int64_t> v;
        for (const auto& x : r.agg_values) v.push_back(x.empty ? INT64_MIN : x.value);
        a.buckets.emplace_back(r.timestamp_ns, std::move(v));
    }, shape);
    return a;
}

}  // namespace

RC_GTEST_PROP(BestTrackRead, ASeriesAnswersTheSameFromTracksAsFromTheRows, ()) {
    Store s;
    uint64_t t = kBase;
    const int segments = *rc::gen::inRange(1, 8);
    for (int seg = 0; seg < segments; ++seg) {
        std::vector<ob::SnapshotRow> rows;
        const int instants = *rc::gen::inRange(1, 20);
        for (int i = 0; i < instants; ++i) {
            t += *rc::gen::inRange<uint64_t>(1, 6);
            for (int j = *rc::gen::inRange(1, 5); j > 0; --j) {
                rows.push_back(row(t, *rc::gen::element(ob::SIDE_BID, ob::SIDE_ASK),
                                   *rc::gen::inRange<int64_t>(95, 106), *rc::gen::inRange<uint64_t>(0, 3)));
            }
        }
        const std::string dir = s.segment(rows);
        if (*rc::gen::arbitrary<bool>()) s.summarise(dir);
    }
    if (*rc::gen::arbitrary<bool>()) {
        std::vector<ob::SnapshotRow> late;
        const uint64_t at = *rc::gen::inRange<uint64_t>(kBase, t + 3);
        late.push_back(row(at, *rc::gen::element(ob::SIDE_BID, ob::SIDE_ASK),
                           *rc::gen::inRange<int64_t>(95, 106), *rc::gen::inRange<uint64_t>(0, 3)));
        s.block(late);
    }
    ob::AggregationEngine agg;
    ob::QueryEngine q(*s.store, [](const std::string&) -> std::shared_ptr<ob::SoABuffer> { return nullptr; },
                      agg);
    for (int n = 0; n < 4; ++n) {
        const uint64_t lo = *rc::gen::inRange<uint64_t>(kBase - 5, t + 3);
        const uint64_t hi = *rc::gen::inRange<uint64_t>(lo, t + 8);
        const uint64_t width = *rc::gen::element<uint64_t>(uint64_t{1}, uint64_t{3}, uint64_t{7}, uint64_t{20});
        std::string sql = "SELECT OPEN(bid), HIGH(bid), LOW(ask), CLOSE(ask), TWAP(mid), HIGH(spread), "
                          "LOW(mid), TWAP(bid) FROM 'SUM'.'EX' WHERE timestamp BETWEEN " +
                          std::to_string(lo) + " AND " + std::to_string(hi) + " GROUP BY TIME_BUCKET(" +
                          std::to_string(width) + "ns)";
        if (*rc::gen::arbitrary<bool>()) {
            sql += " ROLLING " + std::to_string(width * *rc::gen::inRange<uint64_t>(1, 5)) + "ns";
        }
        s.store->set_read_best_tracks(true);
        const SeriesAnswer with_tracks = series(q, sql);
        s.store->set_read_best_tracks(false);
        const SeriesAnswer from_rows = series(q, sql);
        RC_ASSERT(from_rows.error.empty());
        RC_ASSERT(with_tracks == from_rows);
    }
}

TEST(BestTrackRead, ARangeThatBeginsBeforeTheTracksReadsRowsOnlyUntilTheyStart) {
    // Rows of an older build first - no summary, so no track - then two summaries with their
    // tracks. A range from the first rows reads them up to where the first track starts, which
    // holds the best from there, and the tracks after.
    Store s;
    std::vector<ob::SnapshotRow> old_rows;
    for (uint64_t i = 0; i < 20; ++i) old_rows.push_back(row(kBase + i, ob::SIDE_BID, 100 + static_cast<int64_t>(i % 3), 1));
    s.segment(old_rows);
    // The first summary composes from the first row, so its track starts at 0 and covers these;
    // it is the second whose span begins after them.
    std::vector<ob::SnapshotRow> more;
    for (uint64_t i = 20; i < 40; ++i) more.push_back(row(kBase + i, ob::SIDE_BID, 100 + static_cast<int64_t>(i % 5), i % 2));
    const std::string d1 = s.segment(more);
    const auto w1 = s.summarise(d1);
    ASSERT_TRUE(w1.written);
    std::vector<ob::SnapshotRow> last;
    for (uint64_t i = 40; i < 60; ++i) last.push_back(row(kBase + i, ob::SIDE_BID, 99 + static_cast<int64_t>(i % 4), 1));
    const std::string d2 = s.segment(last);
    ASSERT_TRUE(s.summarise(d2).written);
    // Without the first track, the range begins in rows and the second track takes over.
    ASSERT_TRUE(fs::remove(fs::path(d1) / ob::best_track_file(w1.as_of_ts)));
    s.reopen();
    ASSERT_EQ(s.store->best_tracks("SUM", "EX").size(), 1u);
    ob::ColumnarStore::TopsCost cost;
    EXPECT_EQ(tops_read(*s.store, kBase + 5, kBase + 59, &cost), tops_of_rows(*s.store, kBase + 5, kBase + 59));
    EXPECT_EQ(cost.tracks, 1u);
    // The rows of kBase + 5 to the first track's start, kBase + 39, and no further.
    EXPECT_EQ(cost.rows.kept, 35u) << "rows were read where a track covers the range";
}

TEST(BestTrackWrite, ASegmentThatLeavesTakesItsTrackWithIt) {
    Store s;
    const std::string d1 = s.segment({row(kBase + 10, ob::SIDE_BID, 100, 1)});
    ASSERT_TRUE(s.summarise(d1).written);
    const std::string d2 = s.segment({row(kBase + 20, ob::SIDE_ASK, 105, 1)});
    const auto w2 = s.summarise(d2);
    ASSERT_TRUE(w2.written);
    ASSERT_EQ(s.store->best_tracks("SUM", "EX").size(), 2u);
    // The second segment removed, as retention or a drop removes one: its track goes with it, and a
    // series no longer reads a file that is not there.
    ASSERT_EQ(s.store->remove_segments({d2}), 1u);
    EXPECT_EQ(s.store->best_tracks("SUM", "EX"), (std::vector<uint64_t>{kBase + 10}));
    ob::ColumnarStore::TopsCost cost;
    EXPECT_EQ(tops_read(*s.store, kBase, kBase + 30, &cost), tops_of_rows(*s.store, kBase, kBase + 30));
    EXPECT_EQ(cost.tracks, 1u);
}
