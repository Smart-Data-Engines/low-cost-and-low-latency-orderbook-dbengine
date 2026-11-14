// The bytes of a book summary and of its best track (#233, #243), whatever composes them (#266).
//
// write_book_summary() composes the book of a symbol as of its newest segment's end and writes it, with
// the best of each side after every instant since the summary it started from. How it composes is the
// part #266 changes - the work of it, not the answer - so what these tests hold is the answer's bytes:
// the files a store writes, after any sequence of publishes, seals, late rows, summaries and restarts,
// are the bytes of an oracle that composes from a plain scan of every row and encodes with the files'
// own encoders.

#include "orderbook/best_track.hpp"
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
#include <iterator>
#include <map>
#include <memory>
#include <random>
#include <sstream>
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
        : path(fs::temp_directory_path() / ("ob_summary_bytes_" + std::to_string(::getpid()) + "_" +
                                            std::to_string(g_counter.fetch_add(1)))) {
        fs::create_directories(path);
    }
    ~TempDir() {
        std::error_code ec;
        fs::remove_all(path, ec);
    }
};

ob::SnapshotRow row(uint64_t ts, uint8_t side, int64_t price, uint64_t qty, uint64_t seq) {
    ob::SnapshotRow r{};
    r.timestamp_ns    = ts;
    r.sequence_number = seq;
    r.side            = side;
    r.price           = price;
    r.quantity        = qty;
    r.order_count     = static_cast<uint32_t>(qty % 5 + 1);
    return r;
}

std::string read_file(const fs::path& p) {
    std::ifstream in(p, std::ios::binary);
    return std::string(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
}

/// Every row a plain scan delivers up to `as_of`, by time and stable: the order a composition applies
/// them in.
std::vector<ob::SnapshotRow> rows_to(const ob::ColumnarStore& store, uint64_t as_of) {
    std::vector<ob::SnapshotRow> rows;
    store.scan(0, as_of, "SUM", "EX", ob::ColumnSet::all(),
               [&](const ob::SnapshotRow& r) { rows.push_back(r); });
    std::stable_sort(rows.begin(), rows.end(), [](const ob::SnapshotRow& a, const ob::SnapshotRow& b) {
        return a.timestamp_ns < b.timestamp_ns;
    });
    return rows;
}

/// The oracle's two files, as of `as_of`, composed from `from` - the summary before it, or the first
/// row when there is none.
struct Expected {
    std::string summary;
    std::string track;
};

Expected oracle(const ob::ColumnarStore& store, uint64_t as_of, std::optional<uint64_t> from) {
    const std::vector<ob::SnapshotRow> rows = rows_to(store, as_of);
    std::map<int64_t, ob::SnapshotRow, std::greater<>> bids;
    std::map<int64_t, ob::SnapshotRow> asks;
    const auto apply = [&](const ob::SnapshotRow& r) {
        if (r.side == ob::SIDE_BID) {
            if (r.quantity == 0) bids.erase(r.price); else bids[r.price] = r;
        } else if (r.side == ob::SIDE_ASK) {
            if (r.quantity == 0) asks.erase(r.price); else asks[r.price] = r;
        }
    };
    const auto top_now = [&]() {
        ob::BookTop t;
        if (!bids.empty()) {
            t.has_bid = true;
            t.bid = bids.begin()->first;
        }
        if (!asks.empty()) {
            t.has_ask = true;
            t.ask = asks.begin()->first;
        }
        return t;
    };
    const uint64_t start = from.value_or(0);
    ob::BestTrack track;
    size_t i = 0;
    for (; i < rows.size() && from.has_value() && rows[i].timestamp_ns <= start; ++i) apply(rows[i]);
    track.entries.push_back(ob::BestTrackEntry{start, top_now()});
    while (i < rows.size()) {
        const uint64_t t = rows[i].timestamp_ns;
        for (; i < rows.size() && rows[i].timestamp_ns == t; ++i) apply(rows[i]);
        if (!(top_now() == track.entries.back().top)) track.entries.push_back(ob::BestTrackEntry{t, top_now()});
    }
    track.header.from_ts        = start;
    track.header.as_of_ts       = as_of;
    track.header.from_first_row = !from.has_value();
    track.header.entries        = static_cast<uint32_t>(track.entries.size());

    ob::BookSummary summary;
    summary.header.as_of_ts  = as_of;
    summary.header.as_of_seq = rows.empty() ? 0 : rows.back().sequence_number;
    uint16_t level = 0;
    for (auto [p, r] : bids) {
        r.level_index = level++;
        summary.rows.push_back(r);
    }
    level = 0;
    for (auto [p, r] : asks) {
        r.level_index = level++;
        summary.rows.push_back(r);
    }
    summary.header.rows = static_cast<uint32_t>(summary.rows.size());
    return Expected{ob::encode_book_summary(summary), ob::encode_best_track(track)};
}

/// A store fed the way the engine feeds it: rows published as a block, which a seal later replaces with
/// a segment written beside the index.
struct Store {
    TempDir dir;
    std::unique_ptr<ob::ColumnarStore> store = std::make_unique<ob::ColumnarStore>(dir.path.string());
    std::vector<std::vector<ob::SnapshotRow>> pending;

    void block(const std::vector<ob::SnapshotRow>& rows) {
        store->publish_blocks({ob::RowBlock::make("SUM", "EX", rows)});
        pending.push_back(rows);
    }
    std::string seal() {
        ob::ColumnarStore writer(dir.path.string(), ob::ColumnarStore::kDefaultSegmentDurationNs,
                                 ob::ColumnarStore::OwnIndex::kNo);
        writer.set_symbol_exchange("SUM", "EX");
        for (const auto& rows : pending) {
            for (const auto& r : rows) writer.append(r);
        }
        auto meta = writer.flush_segment();
        if (!meta) return {};
        if (store->seal_blocks("SUM", "EX", pending.size(), {*meta}) != 0) return {};
        pending.clear();
        return meta->dir_path;
    }
    std::string segment(const std::vector<ob::SnapshotRow>& rows) {
        block(rows);
        return seal();
    }
    void reopen() {
        store->close();
        store = std::make_unique<ob::ColumnarStore>(dir.path.string());
        store->open_existing();
        const auto again = std::move(pending);
        pending.clear();
        for (const auto& rows : again) block(rows);
    }
    size_t compared = 0;   ///< summaries written and compared
    std::string last_why_not;

    /// A summary, and whether its two files are the oracle's: "" when they are, or what differs.
    std::string summarise_and_compare(const std::string& seg_dir) {
        // The summary it will start from: the newest one the index holds, before the one written.
        const std::vector<uint64_t> before = store->book_summaries("SUM", "EX");
        const ob::ColumnarStore::SummaryWrite w = store->write_book_summary("SUM", "EX", seg_dir);
        if (!w.written) {
            last_why_not = w.why_not;
            return {};
        }
        ++compared;
        std::optional<uint64_t> from;
        for (uint64_t t : before) {
            if (t < w.as_of_ts && (!from || t > *from)) from = t;
        }
        const Expected want = oracle(*store, w.as_of_ts, from);
        const std::string summary = read_file(fs::path(seg_dir) / ob::book_summary_file(w.as_of_ts));
        const std::string track = read_file(fs::path(seg_dir) / ob::best_track_file(w.as_of_ts));
        std::ostringstream why;
        if (summary != want.summary) {
            why << "the summary as of " << w.as_of_ts << " is " << summary.size() << " bytes, the oracle's "
                << want.summary.size() << (summary.size() == want.summary.size() ? ", and they differ" : "");
        }
        // And the rows it says it read: every row from the summary's time to its own, both included,
        // which the engine's log line reports.
        uint64_t span = 0;
        for (const ob::SnapshotRow& r : rows_to(*store, w.as_of_ts)) {
            if (r.timestamp_ns >= from.value_or(0)) ++span;
        }
        if (w.rows_read != span) {
            why << (why.tellp() > 0 ? "; " : "") << "it says it read " << w.rows_read << " rows, the span holds "
                << span;
        }
        if (track != want.track) {
            why << (why.tellp() > 0 ? "; " : "") << "its track is " << track.size() << " bytes, the oracle's "
                << want.track.size() << (track.size() == want.track.size() ? ", and they differ" : "");
        }
        return why.str();
    }
};

}  // namespace

TEST(BookSummaryBytes, AWalkOfABookSummarisedTwentyTimesIsTheOraclesEveryTime) {
    // A book of 40 prices a side, 2 000 rows between summaries, times a microsecond apart - the shape a
    // busy symbol's seals give, scaled down to run in a test.
    Store s;
    std::mt19937_64 rng(266);
    std::uniform_int_distribution<int> level(0, 39);
    std::uniform_int_distribution<int> tenth(0, 9);
    std::uniform_int_distribution<uint64_t> qty(1, 500);
    uint64_t ts = kBase;
    uint64_t seq = 1;
    for (int k = 0; k < 20; ++k) {
        std::vector<ob::SnapshotRow> rows;
        for (int i = 0; i < 2000; ++i) {
            const bool bid = (rng() & 1) == 0;
            const int64_t price = bid ? 1000 - 1 - level(rng) : 1000 + 1 + level(rng);
            rows.push_back(row(ts, bid ? ob::SIDE_BID : ob::SIDE_ASK, price, tenth(rng) == 0 ? 0 : qty(rng), seq++));
            ts += 1000;
        }
        // Some rows of an instant together, as a book's diff arrives.
        for (size_t i = 1; i < rows.size(); i += 7) rows[i].timestamp_ns = rows[i - 1].timestamp_ns;
        const std::string dir = s.segment(rows);
        ASSERT_FALSE(dir.empty()) << "summary " << k << ": the segment was not sealed";
        EXPECT_EQ(s.summarise_and_compare(dir), "") << "summary " << k;
    }
}

RC_GTEST_PROP(BookSummaryBytes, AnySequenceOfPublishesSealsAndSummariesWritesTheOraclesBytes, ()) {
    Store s;
    uint64_t latest = kBase;
    const int steps = *rc::gen::inRange(1, 16);
    for (int i = 0; i < steps; ++i) {
        std::vector<ob::SnapshotRow> rows;
        const int n = *rc::gen::inRange(1, 12);
        for (int k = 0; k < n; ++k) {
            // Mostly after what came before, sometimes before it: a client's event time (#105).
            const bool late = *rc::gen::inRange(0, 6) == 0;
            const uint64_t ts = late ? kBase + static_cast<uint64_t>(*rc::gen::inRange(0, 40))
                                     : latest + static_cast<uint64_t>(*rc::gen::inRange(0, 4));
            latest = std::max(latest, ts);
            const uint64_t qty = *rc::gen::inRange(0, 4) == 0 ? 0 : static_cast<uint64_t>(*rc::gen::inRange(1, 50));
            rows.push_back(row(ts, *rc::gen::inRange(0, 2) == 0 ? ob::SIDE_BID : ob::SIDE_ASK,
                               90 + *rc::gen::inRange<int64_t>(0, 15), qty, static_cast<uint64_t>(i * 20 + k)));
        }
        const int what = *rc::gen::inRange(0, 4);
        if (what == 0) {
            s.block(rows);
        } else {
            const std::string dir = s.segment(rows);
            RC_ASSERT(!dir.empty());
            if (what != 1) {
                const std::string why = s.summarise_and_compare(dir);
                RC_ASSERT(why.empty());
            }
        }
        if (*rc::gen::inRange(0, 8) == 0) s.reopen();   // the blocks go with the process, as on a crash
    }
    // And one after all of it, so that every case compares: rows after the latest, a segment, a summary.
    std::vector<ob::SnapshotRow> last;
    for (int k = 0; k < 3; ++k) {
        last.push_back(row(latest + 1 + static_cast<uint64_t>(k), k % 2 == 0 ? ob::SIDE_BID : ob::SIDE_ASK,
                           90 + k, static_cast<uint64_t>(k + 1), 1000 + static_cast<uint64_t>(k)));
    }
    const std::string dir = s.segment(last);
    RC_ASSERT(!dir.empty());
    const std::string why = s.summarise_and_compare(dir);
    RC_ASSERT(why.empty());
    RC_CLASSIFY(s.compared == 1, "one summary compared");
    RC_CLASSIFY(s.compared > 1, "more than one compared");
    RC_ASSERT(s.compared > 0u);
}
