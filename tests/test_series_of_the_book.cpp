// #44 step 2: series of the book through time buckets - OPEN, HIGH, LOW, CLOSE and TWAP of the bid,
// the ask, the mid and the spread - and the two reads under them: a symbol's rows in event-time order
// (`ColumnarStore::scan_by_time()`) and the book of its best levels just before the range
// (`latest_per_level()` with the levels wanted).
//
// A series is a value of the book `AT` answers - level 0 of each side - at every instant: a step
// function that changes only at the instants with a row of level 0, once every row of the instant
// is applied. The oracles here compute it from that definition, from the rows a plain scan delivers
// and the order it delivers them in, and know nothing of runs, heaps or windows.

#include "orderbook/aggregation.hpp"
#include "orderbook/columnar_store.hpp"
#include "orderbook/query_engine.hpp"
#include "orderbook/soa_buffer.hpp"

#include <gtest/gtest.h>
#include <rapidcheck.h>
#include <rapidcheck/gtest.h>

#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <filesystem>
#include <map>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

namespace fs = std::filesystem;

namespace {

constexpr uint64_t kSec  = 1'000'000'000ULL;
constexpr uint64_t kBase = 20'718ULL * 86'400ULL * kSec;   // a whole UTC day, so buckets start on it
constexpr int64_t kNull  = INT64_MIN;                       // how these tests write NULL

std::atomic<uint64_t> g_counter{0};

std::string made(const fs::path& dir) {
    fs::create_directories(dir);
    return dir.string();
}

ob::SnapshotRow row(uint64_t ts, uint8_t side, uint16_t level, int64_t price) {
    ob::SnapshotRow r{};
    r.timestamp_ns    = ts;
    r.sequence_number = static_cast<uint64_t>(price);
    r.side            = side;
    r.level_index     = level;
    r.price           = price;
    r.quantity        = static_cast<uint64_t>(price % 7 + 1);
    r.order_count     = 1;
    return r;
}

struct Fixture {
    fs::path dir;
    ob::ColumnarStore store;
    ob::AggregationEngine agg;
    ob::QueryEngine engine;

    Fixture()
        : dir(fs::temp_directory_path() / ("ob_series_" + std::to_string(::getpid()) + "_" +
                                           std::to_string(g_counter.fetch_add(1))))
        , store(made(dir))
        , engine(store, [](const std::string&) -> std::shared_ptr<ob::SoABuffer> { return nullptr; },
                 agg) {}
    ~Fixture() {
        store.close();
        std::error_code ec;
        fs::remove_all(dir, ec);
    }

    /// One segment of BK.EX holding these rows, in this order.
    ob::SegmentMeta segment(const std::vector<ob::SnapshotRow>& rows) {
        store.set_symbol_exchange("BK", "EX");
        for (const auto& r : rows) store.append(r);
        auto meta = store.flush_segment();
        EXPECT_TRUE(meta.has_value());
        return meta.value_or(ob::SegmentMeta{});
    }
    /// One segment written beside the index, then put in it with a range that is not its rows':
    /// what a build before #166 left - a start `shift` before its first row, or after it, so that
    /// a row of it is earlier than the start it is delivered by.
    void segment_with_a_range_not_its_rows(const std::vector<ob::SnapshotRow>& rows, int64_t shift) {
        ob::ColumnarStore writer(dir.string(), ob::ColumnarStore::kDefaultSegmentDurationNs,
                                 ob::ColumnarStore::OwnIndex::kNo);
        writer.set_symbol_exchange("BK", "EX");
        for (const auto& r : rows) writer.append(r);
        auto meta = writer.flush_segment();
        ASSERT_TRUE(meta.has_value());
        meta->time_range_is_rows = false;
        meta->start_ts_ns = static_cast<uint64_t>(static_cast<int64_t>(meta->start_ts_ns) + shift);
        meta->start_ts_ns = std::min(meta->start_ts_ns, meta->end_ts_ns);
        store.merge_segments({*meta});
    }
    void block(const std::vector<ob::SnapshotRow>& rows) {
        store.publish_blocks({ob::RowBlock::make("BK", "EX", rows)});
    }

    struct Answer {
        std::string error;
        ob::QueryShape shape;
        std::vector<ob::QueryResult> rows;
    };
    Answer run(const std::string& sql) {
        Answer a;
        a.error = engine.execute(sql, [&](const ob::QueryResult& r) { a.rows.push_back(r); }, a.shape);
        return a;
    }
    /// A bucket answer that must succeed, as (bucket start, values) in its order, NULL as kNull.
    std::vector<std::pair<uint64_t, std::vector<int64_t>>> buckets(const std::string& sql) {
        const Answer a = run(sql);
        EXPECT_TRUE(a.error.empty()) << sql << ": " << a.error;
        std::vector<std::pair<uint64_t, std::vector<int64_t>>> out;
        for (const auto& r : a.rows) {
            std::vector<int64_t> v;
            for (const auto& x : r.agg_values) v.push_back(x.empty ? kNull : x.value);
            out.emplace_back(r.timestamp_ns, v);
        }
        return out;
    }
};

/// Every row a scan of the whole symbol delivers, in its order - which is what decides a tie.
std::vector<ob::SnapshotRow> delivered(const ob::ColumnarStore& store) {
    std::vector<ob::SnapshotRow> out;
    store.scan(0, UINT64_MAX, "BK", "EX", ob::ColumnSet::all(),
               [&](const ob::SnapshotRow& r) { out.push_back(r); });
    return out;
}

bool same_row(const ob::SnapshotRow& a, const ob::SnapshotRow& b) {
    return a.timestamp_ns == b.timestamp_ns && a.sequence_number == b.sequence_number &&
           a.side == b.side && a.level_index == b.level_index && a.price == b.price &&
           a.quantity == b.quantity && a.order_count == b.order_count;
}

std::string describe(const std::vector<ob::SnapshotRow>& rows) {
    std::ostringstream s;
    for (const auto& r : rows) {
        s << "[" << (r.timestamp_ns - kBase) / (kSec / 2) << "h s" << +r.side << " l" << r.level_index
          << " p" << r.price << "] ";
    }
    return s.str();
}

// ── The series oracle: the definition, instant by instant ──────────────────────

struct OracleTop {
    std::optional<int64_t> bid, ask;
};

/// Level 0 of each side of the book at `t`: the latest row at or before it, a tie to the row
/// delivered later.
OracleTop top_at(const std::vector<ob::SnapshotRow>& rows, uint64_t t) {
    OracleTop top;
    std::optional<uint64_t> bid_ts, ask_ts;
    for (const auto& r : rows) {
        if (r.level_index != 0 || r.timestamp_ns > t) continue;
        auto& ts = r.side == ob::SIDE_BID ? bid_ts : ask_ts;
        auto& px = r.side == ob::SIDE_BID ? top.bid : top.ask;
        if (r.side != ob::SIDE_BID && r.side != ob::SIDE_ASK) continue;
        if (!ts.has_value() || r.timestamp_ns >= *ts) {
            ts = r.timestamp_ns;
            px = r.price;
        }
    }
    return top;
}

enum class Fn { Open, High, Low, Close, Twap };
enum class S { Bid, Ask, Mid, Spread };

std::optional<__int128> oracle_value(S s, const OracleTop& t) {
    switch (s) {
    case S::Bid: if (!t.bid) return std::nullopt; return static_cast<__int128>(*t.bid);
    case S::Ask: if (!t.ask) return std::nullopt; return static_cast<__int128>(*t.ask);
    case S::Mid:
        if (!t.bid || !t.ask) return std::nullopt;
        return (static_cast<__int128>(*t.bid) + *t.ask) * 500'000;   // MID_PRICE's 10^6 scale
    case S::Spread:
        if (!t.bid || !t.ask) return std::nullopt;
        return static_cast<__int128>(*t.ask) - *t.bid;
    }
    return std::nullopt;
}

const char* fn_name(Fn f) {
    switch (f) {
    case Fn::Open: return "OPEN";
    case Fn::High: return "HIGH";
    case Fn::Low: return "LOW";
    case Fn::Close: return "CLOSE";
    case Fn::Twap: return "TWAP";
    }
    return "?";
}
const char* series_name(S s) {
    switch (s) {
    case S::Bid: return "bid";
    case S::Ask: return "ask";
    case S::Mid: return "mid";
    case S::Spread: return "spread";
    }
    return "?";
}

/// One function of one series over the window [ws, we], from the instants the series is in force.
int64_t oracle_window(Fn fn, S s, const std::vector<ob::SnapshotRow>& rows, uint64_t ws, uint64_t we) {
    // The instants a value begins in the window: its first, and every one with a row of level 0.
    std::vector<uint64_t> points{ws};
    for (const auto& r : rows) {
        if (r.level_index == 0 && (r.side == ob::SIDE_BID || r.side == ob::SIDE_ASK) &&
            r.timestamp_ns > ws && r.timestamp_ns <= we) {
            points.push_back(r.timestamp_ns);
        }
    }
    std::sort(points.begin(), points.end());
    points.erase(std::unique(points.begin(), points.end()), points.end());
    const auto at = [&](uint64_t t) { return oracle_value(s, top_at(rows, t)); };
    const auto out = [](const std::optional<__int128>& v) {
        return v.has_value() ? static_cast<int64_t>(*v) : kNull;
    };
    switch (fn) {
    case Fn::Open: return out(at(ws));
    case Fn::Close: return out(at(we));
    case Fn::High:
    case Fn::Low: {
        std::optional<__int128> best;
        for (uint64_t p : points) {
            const auto v = at(p);
            if (!v) continue;
            if (!best || (fn == Fn::High ? *v > *best : *v < *best)) best = v;
        }
        return out(best);
    }
    case Fn::Twap: {
        __int128 weighted = 0;
        __int128 defined = 0;
        for (size_t i = 0; i < points.size(); ++i) {
            const uint64_t until = i + 1 < points.size() ? points[i + 1] : we + 1;
            const auto v = at(points[i]);
            if (!v) continue;
            // The mid is already at 10^6; the others are scaled here.
            weighted += *v * static_cast<__int128>(until - points[i]) * (s == S::Mid ? 1 : 1'000'000);
            defined += until - points[i];
        }
        if (defined == 0) return kNull;
        return static_cast<int64_t>(weighted / defined);
    }
    }
    return kNull;
}

struct Asked {
    Fn fn;
    S series;
};

/// The whole answer, bucket by bucket, from the definition.
std::vector<std::pair<uint64_t, std::vector<int64_t>>> oracle_answer(
        const std::vector<ob::SnapshotRow>& rows, const std::vector<Asked>& asked, uint64_t width,
        std::optional<uint64_t> from, std::optional<uint64_t> to, std::optional<uint64_t> limit) {
    std::vector<std::pair<uint64_t, std::vector<int64_t>>> out;
    uint64_t lo = 0, hi = 0;
    if (from && to) {
        lo = *from;
        hi = *to;
    } else {
        if (rows.empty()) return out;
        lo = UINT64_MAX;
        for (const auto& r : rows) {
            lo = std::min(lo, r.timestamp_ns);
            hi = std::max(hi, r.timestamp_ns);
        }
        if (from) lo = *from;
        if (to) hi = *to;
    }
    if (hi < lo) return out;
    for (uint64_t b = lo - lo % width; b <= hi; b += width) {
        if (limit && out.size() >= *limit) break;
        const uint64_t ws = std::max(b, lo);
        const uint64_t we = std::min(b + width - 1, hi);
        std::vector<int64_t> values;
        for (const Asked& a : asked) values.push_back(oracle_window(a.fn, a.series, rows, ws, we));
        out.emplace_back(b, values);
    }
    return out;
}

std::string describe(const std::vector<std::pair<uint64_t, std::vector<int64_t>>>& answer) {
    std::ostringstream s;
    for (const auto& [start, values] : answer) {
        s << "{" << (start - kBase) / (kSec / 2) << "h:";
        for (int64_t v : values) {
            if (v == kNull) s << " NULL";
            else s << " " << v;
        }
        s << "} ";
    }
    return s.str();
}

}  // namespace

// ── scan_by_time() ─────────────────────────────────────────────────────────────

// The rows of a scan, sorted by time and stable - so at one time in the order the scan delivers
// them - over segments that overlap in time, blocks, and a segment whose range is not its rows'.
RC_GTEST_PROP(ScanByTimeProperty, TheRowsAreTheScansSortedByTimeStably, ()) {
    Fixture f;
    int64_t price = 1;
    const auto rows_of = [&](int n) {
        std::vector<ob::SnapshotRow> rows;
        for (int i = 0; i < n; ++i) {
            rows.push_back(row(kBase + static_cast<uint64_t>(*rc::gen::inRange(0, 40)) * (kSec / 2),
                               static_cast<uint8_t>(*rc::gen::inRange(0, 2)),
                               static_cast<uint16_t>(*rc::gen::inRange(0, 3)), price++));
        }
        return rows;
    };
    const int segments = *rc::gen::inRange(1, 7);
    for (int s = 0; s < segments; ++s) f.segment(rows_of(*rc::gen::inRange(1, 10)));
    if (*rc::gen::inRange(0, 3) == 0) {
        f.segment_with_a_range_not_its_rows(rows_of(*rc::gen::inRange(2, 8)),
                                            *rc::gen::inRange<int64_t>(-20, 21) * static_cast<int64_t>(kSec));
    }
    const int blocks = *rc::gen::inRange(0, 3);
    for (int b = 0; b < blocks; ++b) f.block(rows_of(*rc::gen::inRange(1, 6)));

    const uint64_t from = kBase + static_cast<uint64_t>(*rc::gen::inRange(0, 25)) * (kSec / 2);
    const uint64_t to = from + static_cast<uint64_t>(*rc::gen::inRange(0, 30)) * (kSec / 2);
    const bool level0 = *rc::gen::inRange(0, 2) == 0;
    // The series' own condition when `level0`, through the filter the scan applies (#47 step 2).
    ob::RowFilter filter;
    if (level0) {
        filter.level_lo = 0;
        filter.level_hi = 0;
    }
    const auto keep = [&](const ob::SnapshotRow& r) { return filter.keeps(r.price, r.side, r.level_index); };

    std::vector<ob::SnapshotRow> expected;
    f.store.scan(from, to, "BK", "EX", ob::ColumnSet::all(), [&](const ob::SnapshotRow& r) {
        if (keep(r)) expected.push_back(r);
    });
    std::stable_sort(expected.begin(), expected.end(),
                     [](const ob::SnapshotRow& a, const ob::SnapshotRow& b) {
                         return a.timestamp_ns < b.timestamp_ns;
                     });

    std::vector<ob::SnapshotRow> got;
    const auto cost = f.store.scan_by_time(from, to, "BK", "EX", ob::ColumnSet::all(), filter,
                                           [&](const ob::SnapshotRow& r) {
                                               got.push_back(r);
                                               return true;
                                           });
    RC_ASSERT(got.size() == expected.size());
    for (size_t i = 0; i < got.size(); ++i) {
        RC_ASSERT(same_row(got[i], expected[i]));
    }
    RC_ASSERT(cost.kept == expected.size());
    RC_ASSERT(cost.delivered == expected.size());
    RC_ASSERT(!cost.stopped);

    // And ended by its reader, it hands over what it had handed over until then, and no more.
    if (!expected.empty()) {
        const size_t stop_after = static_cast<size_t>(*rc::gen::inRange<size_t>(1, expected.size() + 1));
        std::vector<ob::SnapshotRow> part;
        const auto ended = f.store.scan_by_time(from, to, "BK", "EX", ob::ColumnSet::all(), filter,
                                                [&](const ob::SnapshotRow& r) {
                                                    part.push_back(r);
                                                    return part.size() < stop_after;
                                                });
        RC_ASSERT(part.size() == stop_after);
        for (size_t i = 0; i < part.size(); ++i) RC_ASSERT(same_row(part[i], expected[i]));
        RC_ASSERT(ended.stopped == (stop_after <= expected.size()));
    }
}

TEST(ScanByTime, SegmentsApartInTimeAreHeldOneAtATime) {
    // Ten segments of 50 rows each, one after another in time: each is handed over before the
    // next is read, so what is held at once is one segment's rows, not the range's.
    Fixture f;
    int64_t price = 1;
    for (int s = 0; s < 10; ++s) {
        std::vector<ob::SnapshotRow> rows;
        for (int i = 0; i < 50; ++i) {
            rows.push_back(row(kBase + static_cast<uint64_t>(s * 100 + i) * kSec, 0, 0, price++));
        }
        f.segment(rows);
    }
    uint64_t seen = 0;
    const auto cost = f.store.scan_by_time(0, UINT64_MAX, "BK", "EX", ob::ColumnSet::all(),
                                           ob::RowFilter{},
                                           [&](const ob::SnapshotRow&) { ++seen; return true; });
    EXPECT_EQ(seen, 500u);
    EXPECT_EQ(cost.candidates, 10u);
    EXPECT_EQ(cost.max_held, 50u) << "the read held more than one segment's rows at once";
}

TEST(ScanByTime, ARunOfManyTiesKeepsTheOrderItHoldsThemIn) {
    // Two hundred rows at five times, written round-robin: sorted by time, each time's forty must
    // stay in the order the segment holds them - a sort that is not stable keeps that order only
    // for runs short enough to be sorted by insertion.
    Fixture f;
    std::vector<ob::SnapshotRow> rows;
    for (int i = 0; i < 200; ++i) rows.push_back(row(kBase + static_cast<uint64_t>(i % 5) * kSec, 0, 0, i));
    f.segment(rows);
    std::vector<int64_t> prices;
    f.store.scan_by_time(0, UINT64_MAX, "BK", "EX", ob::ColumnSet::all(),
                         ob::RowFilter{},
                         [&](const ob::SnapshotRow& r) { prices.push_back(r.price); return true; });
    ASSERT_EQ(prices.size(), 200u);
    for (size_t i = 0; i < 200; ++i) {
        EXPECT_EQ(prices[i], static_cast<int64_t>((i % 40) * 5 + i / 40)) << "at " << i;
    }
}

TEST(ScanByTime, ABlockIsMergedWithTheSegmentsByTime) {
    // A block - delivered after every segment - holding the earliest row: it is read before the
    // segments are streamed, or the first segment's row would be handed over before it.
    Fixture f;
    f.segment({row(kBase + 5 * kSec, 0, 0, 2)});
    f.segment({row(kBase + 10 * kSec, 0, 0, 3)});
    f.block({row(kBase + 1 * kSec, 0, 0, 1)});
    std::vector<int64_t> prices;
    f.store.scan_by_time(0, UINT64_MAX, "BK", "EX", ob::ColumnSet::all(),
                         ob::RowFilter{},
                         [&](const ob::SnapshotRow& r) { prices.push_back(r.price); return true; });
    EXPECT_EQ(prices, (std::vector<int64_t>{1, 2, 3}));
}

TEST(ScanByTime, ASegmentWhoseRangeIsNotItsRowsIsReadFirst) {
    // Rows at 1 s and 9 s and a recorded start of 8 s, as a build before #166 could leave it:
    // read in the order of that start, its row at 1 s would come after the other segment's at 5 s.
    Fixture f;
    f.segment({row(kBase + 5 * kSec, 0, 0, 2)});
    f.segment_with_a_range_not_its_rows({row(kBase + 1 * kSec, 0, 0, 1), row(kBase + 9 * kSec, 0, 0, 3)},
                                        7 * static_cast<int64_t>(kSec));
    std::vector<int64_t> prices;
    f.store.scan_by_time(0, UINT64_MAX, "BK", "EX", ob::ColumnSet::all(),
                         ob::RowFilter{},
                         [&](const ob::SnapshotRow& r) { prices.push_back(r.price); return true; });
    EXPECT_EQ(prices, (std::vector<int64_t>{1, 2, 3}));
}

TEST(ScanByTime, ABlocksRowAtTheNextSegmentsStartWaitsForIt) {
    // At one time a block's row comes after every segment's - a block is delivered after them - and
    // so after the row of a segment not yet read when the block's was already held: the rows at the
    // next segment's start wait until it is read. A segment's own rows there need not wait - every
    // segment read before it is delivered before it.
    Fixture f;
    const uint64_t t = kBase + 5 * kSec;
    f.segment({row(kBase + 1 * kSec, 0, 0, 1), row(t, 0, 0, 2)});
    f.segment({row(t, 0, 0, 3), row(kBase + 9 * kSec, 0, 0, 5)});
    f.block({row(t, 0, 0, 4)});
    std::vector<int64_t> prices;
    f.store.scan_by_time(0, UINT64_MAX, "BK", "EX", ob::ColumnSet::all(),
                         ob::RowFilter{},
                         [&](const ob::SnapshotRow& r) { prices.push_back(r.price); return true; });
    EXPECT_EQ(prices, (std::vector<int64_t>{1, 2, 3, 4, 5}));
}

// ── latest_per_level() with the levels wanted ──────────────────────────────────

TEST(BookOfTheBestLevels, ASegmentHoldingNoWantedLevelIsNotRead) {
    // The newest segment holds only a deep level; the best levels are in the one before it.
    Fixture f;
    f.segment({row(kBase + 1 * kSec, 0, 0, 100), row(kBase + 1 * kSec, 1, 0, 101)});
    f.segment({row(kBase + 2 * kSec, 0, 5, 90)});
    ob::LevelSet best{};
    best.bid[0] = 1;
    best.ask[0] = 1;
    const auto book = f.store.latest_per_level(kBase + 10 * kSec, "BK", "EX", &best);
    ASSERT_EQ(book.rows.size(), 2u) << describe(book.rows);
    EXPECT_EQ(book.rows[0].price, 100);
    EXPECT_EQ(book.rows[1].price, 101);
    EXPECT_EQ(book.segments_read, 1u);
    EXPECT_EQ(book.segments_skipped, 1u);
    EXPECT_EQ(f.store.latest_per_level(kBase + 10 * kSec, "BK", "EX").rows.size(), 3u);
}

RC_GTEST_PROP(BookOfTheBestLevelsProperty, TheBookOfTheWantedLevelsIsTheBookRestrictedToThem, ()) {
    Fixture f;
    int64_t price = 1;
    const int segments = *rc::gen::inRange(1, 7);
    for (int s = 0; s < segments; ++s) {
        std::vector<ob::SnapshotRow> rows;
        const int n = *rc::gen::inRange(1, 10);
        for (int i = 0; i < n; ++i) {
            rows.push_back(row(kBase + static_cast<uint64_t>(*rc::gen::inRange(0, 30)) * kSec,
                               static_cast<uint8_t>(*rc::gen::inRange(0, 2)),
                               static_cast<uint16_t>(*rc::gen::inRange(0, 4)), price++));
        }
        f.segment(rows);
    }
    ob::LevelSet wanted{};
    for (uint8_t side = 0; side < 2; ++side) {
        for (uint16_t level = 0; level < 4; ++level) {
            if (*rc::gen::inRange(0, 2) == 0) {
                (side == 0 ? wanted.bid : wanted.ask)[0] |= uint64_t{1} << level;
            }
        }
    }
    const uint64_t at = kBase + static_cast<uint64_t>(*rc::gen::inRange(0, 32)) * kSec;
    std::vector<ob::SnapshotRow> expected;
    for (const auto& r : f.store.latest_per_level(at, "BK", "EX").rows) {
        if (wanted.has(r.side, r.level_index)) expected.push_back(r);
    }
    const auto book = f.store.latest_per_level(at, "BK", "EX", &wanted);
    RC_ASSERT(book.rows.size() == expected.size());
    for (size_t i = 0; i < expected.size(); ++i) RC_ASSERT(same_row(book.rows[i], expected[i]));
}

// ── The parser ─────────────────────────────────────────────────────────────────

TEST(SeriesParser, TakesFiveFunctionsOfFourSeriesAndWritesThemBack) {
    Fixture f;
    ob::QueryAST ast;
    const std::string sql = "SELECT open(BID), HIGH(ask), Low(mid), CLOSE(spread), TWAP(mid) FROM "
                            "'BK'.'EX' GROUP BY TIME_BUCKET(1m)";
    ASSERT_EQ(f.engine.parse(sql, ast), "");
    ASSERT_EQ(ast.bucket_aggs.size(), 5u);
    EXPECT_EQ(ast.bucket_aggs[0].fn, ob::BucketFn::Open);
    EXPECT_EQ(ast.bucket_aggs[0].series, ob::Series::Bid);
    EXPECT_EQ(ast.bucket_aggs[1].series, ob::Series::Ask);
    EXPECT_EQ(ast.bucket_aggs[2].fn, ob::BucketFn::Low);
    EXPECT_EQ(ast.bucket_aggs[2].series, ob::Series::Mid);
    EXPECT_EQ(ast.bucket_aggs[3].series, ob::Series::Spread);
    EXPECT_EQ(ast.bucket_aggs[4].fn, ob::BucketFn::Twap);
    EXPECT_EQ(ast.bucket_aggs[0].text, "OPEN(bid)");
    EXPECT_EQ(ast.bucket_aggs[3].text, "CLOSE(spread)");
    const std::string canonical = f.engine.format(ast);
    EXPECT_EQ(canonical, "SELECT OPEN(bid), HIGH(ask), LOW(mid), CLOSE(spread), TWAP(mid) FROM "
                         "'BK'.'EX' GROUP BY TIME_BUCKET(1m)");
    ob::QueryAST again;
    ASSERT_EQ(f.engine.parse(canonical, again), "");
    EXPECT_EQ(again.bucket_aggs, ast.bucket_aggs);
}

TEST(SeriesParser, RefusesWhatIsNotASeriesAndSaysWhatIs) {
    Fixture f;
    for (const char* sql : {"SELECT OPEN(price) FROM 'BK'.'EX' GROUP BY TIME_BUCKET(1m)",
                            "SELECT TWAP(*) FROM 'BK'.'EX' GROUP BY TIME_BUCKET(1m)",
                            "SELECT HIGH(best) FROM 'BK'.'EX' GROUP BY TIME_BUCKET(1m)"}) {
        ob::QueryAST ast;
        const std::string e = f.engine.parse(sql, ast);
        EXPECT_NE(e.find("expected bid, ask, mid or spread"), std::string::npos) << sql << ": " << e;
    }
    ob::QueryAST ast;
    const std::string e = f.engine.parse("SELECT MIN(mid) FROM 'BK'.'EX' GROUP BY TIME_BUCKET(1m)", ast);
    EXPECT_NE(e.find("OPEN, HIGH, LOW, CLOSE and TWAP"), std::string::npos) << e;
    const std::string side = f.engine.parse("SELECT MIN(bid) FROM 'BK'.'EX' GROUP BY TIME_BUCKET(1m)", ast);
    EXPECT_NE(side.find("WHERE side = 0 - or LOW(bid)"), std::string::npos) << side;
}

TEST(SeriesQuery, WithoutGroupByASeriesIsRefused) {
    Fixture f;
    f.segment({row(kBase, 0, 0, 100)});
    const auto a = f.run("SELECT OPEN(bid) FROM 'BK'.'EX'");
    EXPECT_EQ(a.error.rfind("AGG_NEEDS_BUCKET: OPEN reads a series", 0), 0u) << a.error;
}

TEST(SeriesQuery, AConditionOnPriceSideOrLevelBesideASeriesIsRefused) {
    Fixture f;
    f.segment({row(kBase, 0, 0, 100)});
    for (const char* cond : {"price > 5", "side = 0", "level = 0"}) {
        const auto a = f.run(std::string("SELECT CLOSE(bid) FROM 'BK'.'EX' WHERE ") + cond +
                             " GROUP BY TIME_BUCKET(1s)");
        EXPECT_EQ(a.error.rfind("SERIES_FILTER:", 0), 0u) << cond << ": " << a.error;
    }
}

// ── The series ─────────────────────────────────────────────────────────────────

TEST(SeriesQuery, ARowThatLosesItsTieWasNeverInForce) {
    // Two bids at one instant, the higher delivered first: the book at that instant holds the
    // later, so the higher was in force at no instant and is not the bucket's HIGH.
    Fixture f;
    f.segment({row(kBase + 1 * kSec, 0, 0, 100), row(kBase + 2 * kSec, 0, 0, 200),
               row(kBase + 2 * kSec, 0, 0, 150)});
    const auto got = f.buckets("SELECT OPEN(bid), HIGH(bid), LOW(bid), CLOSE(bid) FROM 'BK'.'EX' "
                               "WHERE timestamp BETWEEN " + std::to_string(kBase) + " AND " +
                               std::to_string(kBase + 9 * kSec) + " GROUP BY TIME_BUCKET(10s)");
    ASSERT_EQ(got.size(), 1u);
    EXPECT_EQ(got[0].second, (std::vector<int64_t>{kNull, 150, 100, 150}));
}

TEST(SeriesQuery, ABucketWithoutRowsCarriesWhatWasInForceAndCountsNone) {
    Fixture f;
    f.segment({row(kBase + 1 * kSec, 0, 0, 100), row(kBase + 1 * kSec, 1, 0, 104)});
    f.segment({row(kBase + 25 * kSec, 0, 0, 102)});
    const auto got = f.buckets("SELECT OPEN(mid), CLOSE(mid), TWAP(spread), COUNT(*), FIRST(price) "
                               "FROM 'BK'.'EX' GROUP BY TIME_BUCKET(10s)");
    ASSERT_EQ(got.size(), 3u);
    // [0, 10): the range starts at the first row, 1 s - the window [1, 9] s, mid 102 throughout.
    EXPECT_EQ(got[0].second, (std::vector<int64_t>{102'000'000, 102'000'000, 4'000'000, 2, 100}));
    // [10, 20): no row - the same mid, COUNT 0, FIRST NULL.
    EXPECT_EQ(got[1].first, kBase + 10 * kSec);
    EXPECT_EQ(got[1].second, (std::vector<int64_t>{102'000'000, 102'000'000, 4'000'000, 0, kNull}));
    // [20, 25]: the range ends at the last row, which raises the bid for the window's last
    // nanosecond - a spread of 4 for 5 s and of 2 for 1 ns, truncated: 3 999 999.9996.
    EXPECT_EQ(got[2].second, (std::vector<int64_t>{102'000'000, 103'000'000, 3'999'999, 1, 102}));
}

TEST(SeriesQuery, BeforeASidesFirstRowItsSeriesIsNull) {
    Fixture f;
    f.segment({row(kBase + 0 * kSec, 0, 0, 100), row(kBase + 5 * kSec, 1, 0, 110)});
    const auto got = f.buckets("SELECT OPEN(ask), HIGH(mid), TWAP(bid), TWAP(spread) FROM 'BK'.'EX' "
                               "WHERE timestamp BETWEEN " + std::to_string(kBase) + " AND " +
                               std::to_string(kBase + 9 * kSec) + " GROUP BY TIME_BUCKET(10s)");
    ASSERT_EQ(got.size(), 1u);
    // The spread has a value from 5 s: averaged over the five seconds it had one.
    EXPECT_EQ(got[0].second, (std::vector<int64_t>{kNull, 105'000'000, 100'000'000, 10'000'000}));
}

TEST(SeriesQuery, TheBookBeforeTheRangeIsItsOpen) {
    Fixture f;
    f.segment({row(kBase + 1 * kSec, 0, 0, 100), row(kBase + 1 * kSec, 0, 1, 99)});
    f.segment({row(kBase + 15 * kSec, 0, 3, 50)});   // no best level: skipped by the read before
    const auto got = f.buckets("SELECT OPEN(bid), TWAP(bid) FROM 'BK'.'EX' WHERE timestamp >= " +
                               std::to_string(kBase + 20 * kSec) + " AND timestamp <= " +
                               std::to_string(kBase + 39 * kSec) + " GROUP BY TIME_BUCKET(10s)");
    ASSERT_EQ(got.size(), 2u);
    EXPECT_EQ(got[0].second, (std::vector<int64_t>{100, 100'000'000}));
    EXPECT_EQ(got[1].second, (std::vector<int64_t>{100, 100'000'000}));
}

TEST(SeriesQuery, ARangeToTheLastInstantOfTimeIsAnswered) {
    // The last six nanoseconds there are, the bid 7 for three and 9 for three: the day the bucket
    // starts on would end past them, so its window ends at the last.
    Fixture f;
    f.segment({row(kBase, 0, 0, 7)});
    f.block({row(UINT64_MAX - 2, 0, 0, 9)});
    const auto got = f.buckets("SELECT OPEN(bid), HIGH(bid), CLOSE(bid), TWAP(bid) FROM 'BK'.'EX' WHERE "
                               "timestamp >= " + std::to_string(UINT64_MAX - 5) + " AND timestamp <= " +
                               std::to_string(UINT64_MAX) + " GROUP BY TIME_BUCKET(1d)");
    ASSERT_EQ(got.size(), 1u);
    EXPECT_EQ(got[0].second, (std::vector<int64_t>{7, 9, 9, 8'000'000}));
}

TEST(SeriesQuery, TheMidsTwapIsWeightedByTheTimeEachMidHeld) {
    // A mid of 102 for 4 s and of 103 for 6 s: 102.6, at MID_PRICE's scale.
    Fixture f;
    f.segment({row(kBase, 0, 0, 100), row(kBase, 1, 0, 104), row(kBase + 4 * kSec, 0, 0, 102)});
    const auto got = f.buckets("SELECT TWAP(mid), TWAP(bid) FROM 'BK'.'EX' WHERE timestamp BETWEEN " +
                               std::to_string(kBase) + " AND " + std::to_string(kBase + 10 * kSec - 1) +
                               " GROUP BY TIME_BUCKET(10s)");
    ASSERT_EQ(got.size(), 1u);
    EXPECT_EQ(got[0].second, (std::vector<int64_t>{102'600'000, 101'200'000}));
}

TEST(SeriesQuery, AMidPastSixtyFourBitsIsRefused) {
    Fixture f;
    f.segment({row(kBase, 0, 0, INT64_MAX / 2), row(kBase, 1, 0, INT64_MAX / 2)});
    const auto a = f.run("SELECT CLOSE(mid) FROM 'BK'.'EX' GROUP BY TIME_BUCKET(1s)");
    EXPECT_EQ(a.error.rfind("BUCKET_OVERFLOW: CLOSE(mid) of the bucket at", 0), 0u) << a.error;
    EXPECT_TRUE(a.rows.empty());
    // A spread of the same book fits.
    EXPECT_TRUE(f.run("SELECT CLOSE(spread) FROM 'BK'.'EX' GROUP BY TIME_BUCKET(1s)").error.empty());
}

TEST(SeriesQuery, LimitAndTheCeilingCountTheBucketsAnswered) {
    Fixture f;
    f.segment({row(kBase, 0, 0, 1), row(kBase + 99 * kSec, 0, 0, 2)});
    EXPECT_EQ(f.buckets("SELECT CLOSE(bid) FROM 'BK'.'EX' GROUP BY TIME_BUCKET(1s) LIMIT 3").size(), 3u);
    f.engine.set_max_query_buckets(10);
    const auto a = f.run("SELECT CLOSE(bid) FROM 'BK'.'EX' GROUP BY TIME_BUCKET(1s)");
    EXPECT_EQ(a.error.rfind("BUCKETS_TOO_MANY: more than 10 buckets", 0), 0u) << a.error;
    EXPECT_EQ(f.buckets("SELECT CLOSE(bid) FROM 'BK'.'EX' GROUP BY TIME_BUCKET(1s) LIMIT 10").size(), 10u);
}

TEST(SeriesQuery, TheHeaderCarriesEachColumnsScale) {
    Fixture f;
    f.segment({row(kBase, 0, 0, 1), row(kBase, 1, 0, 3)});
    const auto a = f.run("SELECT OPEN(bid), OPEN(mid), TWAP(bid), CLOSE(spread), COUNT(*) FROM 'BK'.'EX' "
                         "GROUP BY TIME_BUCKET(1s)");
    ASSERT_TRUE(a.error.empty()) << a.error;
    ASSERT_EQ(a.shape.bucket_columns.size(), 5u);
    EXPECT_EQ(a.shape.bucket_columns[0].scale, 1);
    EXPECT_EQ(a.shape.bucket_columns[1].scale, 1'000'000);
    EXPECT_EQ(a.shape.bucket_columns[2].scale, 1'000'000);
    EXPECT_EQ(a.shape.bucket_columns[3].scale, 1);
    EXPECT_EQ(a.shape.bucket_columns[4].scale, 1);
}

// Random rows of both sides at level 0 and below it, in segments and blocks, ties on the time,
// random ranges - open at either end or not - intervals, functions, series and limits: every
// bucket is what the definition says.
RC_GTEST_PROP(SeriesProperty, EveryBucketIsTheDefinitions, ()) {
    Fixture f;
    int64_t price = 0;
    const auto rows_of = [&](int n) {
        std::vector<ob::SnapshotRow> rows;
        for (int i = 0; i < n; ++i) {
            ++price;
            rows.push_back(row(kBase + static_cast<uint64_t>(*rc::gen::inRange(0, 40)) * (kSec / 2),
                               static_cast<uint8_t>(*rc::gen::inRange(0, 2)),
                               static_cast<uint16_t>(*rc::gen::inRange(0, 3)),
                               90 + *rc::gen::inRange<int64_t>(0, 21)));
        }
        return rows;
    };
    const int segments = *rc::gen::inRange(1, 7);
    for (int s = 0; s < segments; ++s) f.segment(rows_of(*rc::gen::inRange(1, 10)));
    const int blocks = *rc::gen::inRange(0, 3);
    for (int b = 0; b < blocks; ++b) f.block(rows_of(*rc::gen::inRange(1, 6)));

    static const uint64_t kWidths[] = {kSec / 2, kSec, 2 * kSec, 3 * kSec, 7 * kSec};
    const uint64_t width = kWidths[*rc::gen::inRange(0, 5)];
    std::optional<uint64_t> from, to, limit;
    if (*rc::gen::inRange(0, 3) != 0) from = kBase + static_cast<uint64_t>(*rc::gen::inRange(0, 45)) * (kSec / 4);
    if (*rc::gen::inRange(0, 3) != 0) to = kBase + static_cast<uint64_t>(*rc::gen::inRange(0, 90)) * (kSec / 4);
    if (*rc::gen::inRange(0, 4) == 0) limit = static_cast<uint64_t>(*rc::gen::inRange(0, 6));
    std::vector<Asked> asked;
    const int n = *rc::gen::inRange(1, 5);
    for (int i = 0; i < n; ++i) {
        asked.push_back({static_cast<Fn>(*rc::gen::inRange(0, 5)), static_cast<S>(*rc::gen::inRange(0, 4))});
    }

    std::string sql = "SELECT ";
    for (size_t i = 0; i < asked.size(); ++i) {
        sql += (i ? ", " : "") + std::string(fn_name(asked[i].fn)) + "(" + series_name(asked[i].series) + ")";
    }
    sql += " FROM 'BK'.'EX'";
    if (from) sql += " WHERE timestamp >= " + std::to_string(*from);
    if (to) sql += std::string(from ? " AND" : " WHERE") + " timestamp <= " + std::to_string(*to);
    sql += " GROUP BY TIME_BUCKET(" + std::to_string(width / 1'000'000) + "ms)";
    if (limit) sql += " LIMIT " + std::to_string(*limit);

    const auto rows = delivered(f.store);
    const auto expected = oracle_answer(rows, asked, width, from, to, limit);
    const auto got = f.buckets(sql);
    RC_LOG() << sql << "\n got      " << describe(got) << "\n expected " << describe(expected)
             << "\n rows     " << describe(rows) << "\n";
    RC_ASSERT(got == expected);
}
