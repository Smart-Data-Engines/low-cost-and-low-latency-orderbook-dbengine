// #47 step 1: the book at an instant reads only the segments that can change it.
//
// `SELECT ... WHERE AT <t>` scanned every row of the symbol from time 0 to t and kept the latest of
// each level - 107 ms at 3 M rows on the i3-7100U for an answer of 100 levels, and linear in the
// history (evidence/2026-10-03-at-cost/). A segment now says which (side, level) pairs it holds a
// row of, and the book is read from the newest segment down, a segment none of whose rows can be
// the latest of its level not read at all.
//
// The oracle is the scan it replaced: [0, t], keeping for each level any row whose timestamp is at
// least the one it holds - so the latest, a tie to the later delivered (#168) - in (side << 16 |
// level) order.

#include "orderbook/columnar_store.hpp"
#include "orderbook/data_model.hpp"

#include <gtest/gtest.h>
#include <rapidcheck.h>
#include <rapidcheck/gtest.h>

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <map>
#include <sstream>
#include <string>
#include <unistd.h>
#include <vector>

namespace fs = std::filesystem;

namespace {

constexpr uint64_t kSec  = 1'000'000'000ULL;
constexpr uint64_t kBase = 1'790'000'000ULL * kSec;

std::atomic<uint64_t> g_dir{0};

struct TempDir {
    fs::path path;
    TempDir()
        : path(fs::temp_directory_path() /
               ("ob_book_at_" + std::to_string(::getpid()) + "_" + std::to_string(g_dir.fetch_add(1)))) {
        fs::create_directories(path);
    }
    ~TempDir() {
        std::error_code ec;
        fs::remove_all(path, ec);
    }
    std::string str() const { return path.string(); }
};

ob::SnapshotRow row(uint64_t ts, uint8_t side, uint16_t level, int64_t price) {
    ob::SnapshotRow r{};
    r.timestamp_ns    = ts;
    r.sequence_number = static_cast<uint64_t>(price);
    r.side            = side;
    r.level_index     = level;
    r.price           = price;
    r.quantity        = static_cast<uint64_t>(price % 97 + 1);
    r.order_count     = 1;
    return r;
}

/// One segment of `rows`, written and indexed by `store`.
ob::SegmentMeta segment(ob::ColumnarStore& store, const std::vector<ob::SnapshotRow>& rows) {
    store.set_symbol_exchange("SYM", "EX");
    for (const auto& r : rows) store.append(r);
    auto meta = store.flush_segment();
    EXPECT_TRUE(meta.has_value());
    return meta.value_or(ob::SegmentMeta{});
}

/// What the book at an instant was before #47.
std::vector<ob::SnapshotRow> book_by_scan(const ob::ColumnarStore& store, uint64_t at) {
    std::map<uint32_t, ob::SnapshotRow> state;
    store.scan(0, at, "SYM", "EX", ob::ColumnSet::all(), [&](const ob::SnapshotRow& r) {
        const uint32_t key = (static_cast<uint32_t>(r.side) << 16) | static_cast<uint32_t>(r.level_index);
        auto [it, inserted] = state.try_emplace(key, r);
        if (!inserted && r.timestamp_ns >= it->second.timestamp_ns) it->second = r;
    });
    std::vector<ob::SnapshotRow> out;
    for (const auto& [k, r] : state) out.push_back(r);
    return out;
}

std::string describe(const std::vector<ob::SnapshotRow>& rows) {
    std::ostringstream s;
    for (const auto& r : rows) {
        s << "[side " << +r.side << " level " << r.level_index << " ts " << (r.timestamp_ns - kBase) / kSec
          << " price " << r.price << "] ";
    }
    return s.str();
}

bool same(const std::vector<ob::SnapshotRow>& a, const std::vector<ob::SnapshotRow>& b) {
    if (a.size() != b.size()) return false;
    for (size_t i = 0; i < a.size(); ++i) {
        if (a[i].timestamp_ns != b[i].timestamp_ns || a[i].sequence_number != b[i].sequence_number ||
            a[i].side != b[i].side || a[i].level_index != b[i].level_index ||
            a[i].price != b[i].price || a[i].quantity != b[i].quantity ||
            a[i].order_count != b[i].order_count) {
            return false;
        }
    }
    return true;
}

/// A meta.json as an older build writes it: without the levels.
void strip_levels(const std::string& dir) {
    const fs::path p = fs::path(dir) / "meta.json";
    std::ifstream in(p);
    std::string text((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    in.close();
    for (const char* key : {",\"bid_levels\":\"", ",\"ask_levels\":\""}) {
        const auto at = text.find(key);
        ASSERT_NE(at, std::string::npos) << key << " is not in " << p;
        const auto end = text.find('"', at + std::string(key).size());
        text.erase(at, end + 1 - at);
    }
    std::ofstream(p, std::ios::trunc) << text;
}

}  // namespace

// ── The set ──────────────────────────────────────────────────────────────────

TEST(LevelSet, HoldsTheLevelsOfItsRowsAndNothingElse) {
    const auto set = ob::LevelSet::from_columns({ob::SIDE_BID, ob::SIDE_BID, ob::SIDE_ASK, ob::SIDE_BID},
                                                {0, 3, 999, 3});
    ASSERT_NE(set, nullptr);
    EXPECT_TRUE(set->has(ob::SIDE_BID, 0));
    EXPECT_TRUE(set->has(ob::SIDE_BID, 3));
    EXPECT_TRUE(set->has(ob::SIDE_ASK, 999));
    EXPECT_FALSE(set->has(ob::SIDE_ASK, 0));
    EXPECT_FALSE(set->has(ob::SIDE_BID, 1));
    EXPECT_EQ(ob::LevelSet::count(set->bid), 2u);
    EXPECT_EQ(ob::LevelSet::count(set->ask), 1u);
}

TEST(LevelSet, ARowNoSetCanHoldMakesTheSetUnknown) {
    // Unknown, not partial: a set missing a row's level would let the book skip the segment that
    // holds the row.
    EXPECT_EQ(ob::LevelSet::from_columns({ob::SIDE_BID, ob::SIDE_BID}, {0, 1000}), nullptr);
    EXPECT_EQ(ob::LevelSet::from_columns({ob::SIDE_BID, 2}, {0, 1}), nullptr);
}

TEST(LevelSet, HexReadsBackToTheSameLevels) {
    ob::LevelSet set;
    for (uint16_t level : {0, 1, 63, 64, 500, 959, 960, 999}) {
        set.bid[level / 64] |= uint64_t{1} << (level % 64);
    }
    const std::string hex = ob::LevelSet::to_hex(set.bid);
    EXPECT_EQ(hex.size(), ob::LevelSet::kLevels / 4);
    ob::LevelSet::Bits back{};
    ASSERT_TRUE(ob::LevelSet::from_hex(hex, back));
    EXPECT_EQ(back, set.bid);
    // Without its leading zeros, as segment format 3 writes it: the same levels.
    const std::string trimmed = ob::LevelSet::to_hex(set.bid, true);
    EXPECT_EQ(trimmed.front(), '8') << trimmed;   // level 999 is the top digit's highest bit
    ob::LevelSet::Bits back_trimmed{};
    ASSERT_TRUE(ob::LevelSet::from_hex(trimmed, back_trimmed));
    EXPECT_EQ(back_trimmed, set.bid);
    ob::LevelSet::Bits none{};
    EXPECT_EQ(ob::LevelSet::to_hex(none, true), "0");
    ASSERT_TRUE(ob::LevelSet::from_hex("0", back_trimmed));
    EXPECT_EQ(back_trimmed, none);
    // A digit short of every digit begins with a zero here, which neither form does.
    EXPECT_FALSE(ob::LevelSet::from_hex(hex.substr(1), back)) << "a digit short";
    EXPECT_FALSE(ob::LevelSet::from_hex("0" + trimmed, back)) << "a leading zero in a short form";
    std::string bad = hex;
    bad[7] = 'g';
    EXPECT_FALSE(ob::LevelSet::from_hex(bad, back)) << "not a hex digit";
}

TEST(BookAtAnInstant, ASegmentsLevelsSurviveItsMetaJsonAndAnOlderOneIsReadAlways) {
    TempDir dir;
    std::string stripped;
    {
        ob::ColumnarStore store(dir.str());
        segment(store, {row(kBase + 1 * kSec, ob::SIDE_BID, 2, 10), row(kBase + 2 * kSec, ob::SIDE_ASK, 5, 11)});
        stripped = segment(store, {row(kBase + 3 * kSec, ob::SIDE_BID, 2, 12)}).dir_path;
    }
    strip_levels(stripped);   // as a build before #47 wrote it
    ob::ColumnarStore reopened(dir.str());
    reopened.open_existing();
    const auto metas = reopened.segments_of("SYM.EX");
    ASSERT_EQ(metas.size(), 2u);
    for (const auto& m : metas) {
        if (m.dir_path == stripped) {
            EXPECT_EQ(m.levels, nullptr) << "a meta.json without levels read as a set";
        } else {
            ASSERT_NE(m.levels, nullptr);
            EXPECT_TRUE(m.levels->has(ob::SIDE_BID, 2));
            EXPECT_TRUE(m.levels->has(ob::SIDE_ASK, 5));
            EXPECT_EQ(ob::LevelSet::count(m.levels->bid) + ob::LevelSet::count(m.levels->ask), 2u);
        }
    }
    // The stripped segment is read because nothing says what it holds; the other because only it
    // holds the ask.
    const auto book = reopened.latest_per_level(kBase + 10 * kSec, "SYM", "EX");
    EXPECT_TRUE(same(book.rows, book_by_scan(reopened, kBase + 10 * kSec))) << describe(book.rows);
    EXPECT_EQ(book.segments_read, 2u);
}

// ── The book ─────────────────────────────────────────────────────────────────

TEST(BookAtAnInstant, ASegmentWhoseLevelsAllHaveALaterRowIsNotRead) {
    // Every segment sets the same ten levels, a second apart: the newest holds the book, and the
    // four before it can change none of it.
    TempDir dir;
    ob::ColumnarStore store(dir.str());
    int64_t price = 1;
    for (int s = 0; s < 5; ++s) {
        std::vector<ob::SnapshotRow> rows;
        for (uint16_t level = 0; level < 10; ++level) {
            rows.push_back(row(kBase + static_cast<uint64_t>(s) * kSec, ob::SIDE_BID, level, price++));
        }
        segment(store, rows);
    }
    const auto book = store.latest_per_level(kBase + 100 * kSec, "SYM", "EX");
    EXPECT_TRUE(same(book.rows, book_by_scan(store, kBase + 100 * kSec))) << describe(book.rows);
    ASSERT_EQ(book.rows.size(), 10u);
    EXPECT_EQ(book.segments_read, 1u);
    EXPECT_EQ(book.segments_skipped, 4u);
}

TEST(BookAtAnInstant, ALevelOnlyAnOlderSegmentHoldsIsReadFromIt) {
    TempDir dir;
    ob::ColumnarStore store(dir.str());
    segment(store, {row(kBase + 1 * kSec, ob::SIDE_BID, 0, 1), row(kBase + 1 * kSec, ob::SIDE_ASK, 7, 2)});
    segment(store, {row(kBase + 2 * kSec, ob::SIDE_BID, 0, 3)});
    const auto book = store.latest_per_level(kBase + 100 * kSec, "SYM", "EX");
    EXPECT_TRUE(same(book.rows, book_by_scan(store, kBase + 100 * kSec))) << describe(book.rows);
    ASSERT_EQ(book.rows.size(), 2u);
    EXPECT_EQ(book.rows[0].price, 3) << "the bid is the newer segment's";
    EXPECT_EQ(book.rows[1].price, 2) << "the ask only the older segment holds";
    EXPECT_EQ(book.segments_read, 2u);
}

TEST(BookAtAnInstant, ATieGoesToTheRowDeliveredLaterAcrossSegments) {
    // Two rows of one level at one time, in two segments: the one a scan delivers later is the
    // book's (#168). Delivered by start, so the segment that starts later; read first, it holds the
    // answer, and the earlier one - whose row at that time would lose the tie - is not read.
    TempDir dir;
    ob::ColumnarStore store(dir.str());
    const uint64_t t = kBase + 5 * kSec;
    segment(store, {row(kBase + 1 * kSec, ob::SIDE_BID, 0, 100), row(t, ob::SIDE_BID, 0, 101)});
    segment(store, {row(t, ob::SIDE_BID, 0, 200)});
    const auto book = store.latest_per_level(t, "SYM", "EX");
    EXPECT_TRUE(same(book.rows, book_by_scan(store, t))) << describe(book.rows);
    ASSERT_EQ(book.rows.size(), 1u);
    EXPECT_EQ(book.rows[0].price, 200);
    EXPECT_EQ(book.segments_skipped, 1u);

    // And a block, delivered after every segment, wins the tie over both.
    store.publish_blocks({ob::RowBlock::make("SYM", "EX", {row(t, ob::SIDE_BID, 0, 300)})});
    const auto with_block = store.latest_per_level(t, "SYM", "EX");
    EXPECT_TRUE(same(with_block.rows, book_by_scan(store, t))) << describe(with_block.rows);
    ASSERT_EQ(with_block.rows.size(), 1u);
    EXPECT_EQ(with_block.rows[0].price, 300);
    EXPECT_EQ(with_block.blocks, 1u);
}

TEST(BookAtAnInstant, ASegmentEndingWhenAnEarlierOnesRowIsCanStillWinItsTie) {
    // The wide segment is read first - it ends later - and holds the level at 5 s; the narrow one
    // ends at 5 s, holds the level then, and starts later, so a scan delivers it after the wide
    // one: its row is the book's, and it may not be skipped for ending no later than what the
    // book already holds.
    TempDir dir;
    ob::ColumnarStore store(dir.str());
    segment(store, {row(kBase, ob::SIDE_BID, 9, 1), row(kBase + 5 * kSec, ob::SIDE_BID, 0, 2),
                    row(kBase + 10 * kSec, ob::SIDE_BID, 9, 3)});
    segment(store, {row(kBase + 5 * kSec, ob::SIDE_BID, 0, 4)});
    const auto book = store.latest_per_level(kBase + 20 * kSec, "SYM", "EX");
    EXPECT_TRUE(same(book.rows, book_by_scan(store, kBase + 20 * kSec))) << describe(book.rows);
    ASSERT_EQ(book.rows.size(), 2u);
    EXPECT_EQ(book.rows[0].price, 4) << "level 0 is the narrow segment's, delivered later";
    EXPECT_EQ(book.segments_read, 2u);
}

TEST(BookAtAnInstant, ARowAfterTheInstantIsNotTheBooksAndDoesNotHideAnEarlierOne) {
    TempDir dir;
    ob::ColumnarStore store(dir.str());
    segment(store, {row(kBase + 1 * kSec, ob::SIDE_BID, 0, 1)});
    segment(store, {row(kBase + 9 * kSec, ob::SIDE_BID, 0, 2)});
    const auto book = store.latest_per_level(kBase + 5 * kSec, "SYM", "EX");
    EXPECT_TRUE(same(book.rows, book_by_scan(store, kBase + 5 * kSec))) << describe(book.rows);
    ASSERT_EQ(book.rows.size(), 1u);
    EXPECT_EQ(book.rows[0].price, 1);
}

// The book against the scan it replaced, on what makes the order of reading matter: rows out of time
// order within and across segments, ties on the timestamp, segments an older build wrote (no
// levels), blocks, and instants anywhere - before the first row, inside, after the last.
RC_GTEST_PROP(BookAtAnInstantProperty, TheBookIsTheBookTheScanKept, ()) {
    TempDir dir;
    std::vector<std::string> dirs;
    int64_t price = 1;
    {
        ob::ColumnarStore store(dir.str());
        const int segments = *rc::gen::inRange(1, 8);
        for (int s = 0; s < segments; ++s) {
            std::vector<ob::SnapshotRow> rows;
            const int n = *rc::gen::inRange(1, 12);
            for (int i = 0; i < n; ++i) {
                rows.push_back(row(kBase + static_cast<uint64_t>(*rc::gen::inRange(0, 30)) * kSec,
                                   static_cast<uint8_t>(*rc::gen::inRange(0, 2)),
                                   static_cast<uint16_t>(*rc::gen::inRange(0, 5)), price++));
            }
            dirs.push_back(segment(store, rows).dir_path);
        }
    }
    for (const auto& d : dirs) {
        if (*rc::gen::inRange(0, 4) == 0) strip_levels(d);
    }
    ob::ColumnarStore store(dir.str());
    store.open_existing();
    const int blocks = *rc::gen::inRange(0, 3);
    for (int b = 0; b < blocks; ++b) {
        std::vector<ob::SnapshotRow> rows;
        const int n = *rc::gen::inRange(1, 6);
        for (int i = 0; i < n; ++i) {
            rows.push_back(row(kBase + static_cast<uint64_t>(*rc::gen::inRange(0, 30)) * kSec,
                               static_cast<uint8_t>(*rc::gen::inRange(0, 2)),
                               static_cast<uint16_t>(*rc::gen::inRange(0, 5)), price++));
        }
        store.publish_blocks({ob::RowBlock::make("SYM", "EX", rows)});
    }
    for (int q = 0; q < 8; ++q) {
        const uint64_t at = kBase - kSec + static_cast<uint64_t>(*rc::gen::inRange(0, 33)) * kSec;
        const auto book = store.latest_per_level(at, "SYM", "EX");
        const auto expected = book_by_scan(store, at);
        RC_ASSERT(same(book.rows, expected));
        RC_ASSERT(book.segments_read + book.segments_skipped <= dirs.size());
    }
}
