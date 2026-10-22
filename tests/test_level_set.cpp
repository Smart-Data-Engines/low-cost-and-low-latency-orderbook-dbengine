// #47: a segment says which (side, level) pairs it holds a row of, in its meta.json, and a scan whose
// filter names sides or levels skips a segment holding none of them (`cannot_hold()`).
//
// The book at an instant used the same sets to skip segments while it was the latest row of each
// level; since #233 the book is read by price, where any row can change it, and that read is gone.
// What is left of it here is the set itself and that it survives a segment's meta.json.

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

TEST(LevelSet, ASegmentsLevelsSurviveItsMetaJsonAndAnOlderOnesAreUnknown) {
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
}
