// #260: a large read answered off the client loop.
//
// A read is large by the index's count of the rows its scan may read, which the engine's ReadGate asks
// once the query is parsed and before it reads a row: this file holds that count, the gate's decision -
// a row query's LIMIT when it sets no condition on its rows, its range otherwise, time buckets by their
// range, never the live book's functions - and the threads and the mailbox a large read goes through.
// What the loop does with them - a session paused behind its read, its later commands run after the
// answer, the sessions beside it served meanwhile - is held on a live node, in
// tests/integration/test_large_answer_isolation.py.

#include "orderbook/aggregation.hpp"
#include "orderbook/columnar_store.hpp"
#include "orderbook/off_loop_reads.hpp"
#include "orderbook/query_engine.hpp"
#include "orderbook/soa_buffer.hpp"

#include <gtest/gtest.h>

#include <unistd.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace fs = std::filesystem;

namespace {

std::atomic<uint64_t> g_counter{0};

std::string made(const fs::path& dir) {
    fs::create_directories(dir);
    return dir.string();
}

constexpr uint64_t kBase = 1'790'000'000'000'000'000ULL;

/// Four segments of 5000 rows of one book, a millisecond apart.
struct Store {
    fs::path dir;
    ob::ColumnarStore store;
    ob::AggregationEngine agg;
    ob::QueryEngine engine;

    Store()
        : dir(fs::temp_directory_path() / ("ob_off_loop_" + std::to_string(::getpid()) + "_" +
                                           std::to_string(g_counter.fetch_add(1))))
        , store(made(dir))
        , engine(store, [](const std::string&) -> std::shared_ptr<ob::SoABuffer> { return nullptr; },
                 agg) {
        uint64_t seq = 0;
        for (int segment = 0; segment < 4; ++segment) {
            store.set_symbol_exchange("OL", "EX");
            for (int i = 0; i < 5000; ++i, ++seq) {
                ob::SnapshotRow r{};
                r.timestamp_ns    = kBase + seq * 1'000'000;
                r.sequence_number = seq + 1;
                r.side            = static_cast<uint8_t>(seq % 2);
                r.level_index     = static_cast<uint16_t>(seq % 20);
                r.price           = 10'000 + static_cast<int64_t>(seq % 997);
                r.quantity        = 1 + seq;
                r.order_count     = 1;
                store.append(r);
            }
            EXPECT_TRUE(store.flush_segment().has_value());
        }
    }
    ~Store() {
        store.close();
        std::error_code ec;
        fs::remove_all(dir, ec);
    }
};

/// One query through execute() with a gate at `take_from`: what it returned, the gate after it, and
/// the rows and buckets the callback was given.
struct Asked {
    std::string err;
    ob::ReadGate gate;
    uint64_t delivered{0};
};

Asked ask(Store& s, const std::string& sql, uint64_t take_from) {
    Asked a;
    a.gate.take_from = take_from;
    ob::QueryShape shape;
    a.err = s.engine.execute(sql, [&](const ob::QueryResult&) { ++a.delivered; }, shape, nullptr, &a.gate);
    return a;
}

bool taken(const Asked& a) { return a.err == ob::kReadTakenAway; }

}  // namespace

TEST(OffLoopReads, TheIndexCountsTheRowsInRangeAndStopsWhereItIsToldEnough) {
    Store s;
    const uint64_t last = kBase + 19'999ULL * 1'000'000;
    EXPECT_EQ(s.store.rows_in_range_at_most(0, UINT64_MAX, "OL", "EX"), 20'000u);
    // Half of one segment, by the metadata's even spread: within a row or two of 2500.
    EXPECT_NEAR(static_cast<double>(s.store.rows_in_range_at_most(kBase, kBase + 2'499ULL * 1'000'000, "OL", "EX")),
                2500.0, 2.0);
    // Outside every segment, and a book nobody wrote.
    EXPECT_EQ(s.store.rows_in_range_at_most(last + 1, UINT64_MAX, "OL", "EX"), 0u);
    EXPECT_EQ(s.store.rows_in_range_at_most(0, UINT64_MAX, "NONE", "EX"), 0u);
    // Told that 6000 is enough, it stops at the segment that reaches it: the second of four.
    EXPECT_EQ(s.store.rows_in_range_at_most(0, UINT64_MAX, "OL", "EX", 6000), 10'000u);
}

TEST(OffLoopReads, AReadIsTakenAwayFromTheGatesRowsOnAndReadsNothing) {
    Store s;
    const Asked at = ask(s, "SELECT * FROM 'OL'.'EX'", 20'000);
    EXPECT_TRUE(taken(at)) << at.err;
    EXPECT_EQ(at.delivered, 0u) << "a read taken away read rows";
    EXPECT_EQ(at.gate.rows_at_most, 20'000u);

    const Asked below = ask(s, "SELECT * FROM 'OL'.'EX'", 20'001);
    EXPECT_EQ(below.err, "");
    EXPECT_EQ(below.delivered, 20'000u) << "a read below the gate was not answered";
    EXPECT_EQ(below.gate.rows_at_most, 0u);
}

TEST(OffLoopReads, ALimitDecidesARowQueryWithNoConditionAndNotOneWithThem) {
    Store s;
    // No condition on the rows: the scan stops at the LIMIT, and the gate needs nothing more.
    EXPECT_TRUE(taken(ask(s, "SELECT price FROM 'OL'.'EX' LIMIT 10", 10)));
    const Asked small = ask(s, "SELECT price FROM 'OL'.'EX' LIMIT 10", 11);
    EXPECT_EQ(small.err, "");
    EXPECT_EQ(small.delivered, 10u);
    // A condition reads the range to find its rows, whatever the LIMIT.
    for (const char* q : {"SELECT price FROM 'OL'.'EX' WHERE price < 10005 LIMIT 10",
                          "SELECT price FROM 'OL'.'EX' WHERE side = 1 LIMIT 10",
                          "SELECT price FROM 'OL'.'EX' WHERE level BETWEEN 2 AND 3 LIMIT 10"}) {
        const Asked a = ask(s, q, 20'000);
        EXPECT_TRUE(taken(a)) << q << ": " << a.err;
        EXPECT_EQ(a.gate.rows_at_most, 20'000u) << q;
    }
    // LIMIT 0 reads nothing, and is nothing to take.
    const Asked none = ask(s, "SELECT price FROM 'OL'.'EX' LIMIT 0", 1);
    EXPECT_EQ(none.err, "");
    EXPECT_EQ(none.delivered, 0u);
}

TEST(OffLoopReads, BucketsAreTakenByTheirRangeAndTheLiveBooksFunctionsNever) {
    Store s;
    // Time buckets read every row of the range, whatever their LIMIT, which counts buckets.
    const Asked buckets = ask(s, "SELECT COUNT(*) FROM 'OL'.'EX' GROUP BY TIME_BUCKET(1s) LIMIT 1", 20'000);
    EXPECT_TRUE(taken(buckets)) << buckets.err;
    EXPECT_EQ(buckets.delivered, 0u);
    // The live book's functions read the book; a query that does not parse or names no book reads
    // nothing - each answered as it was, none taken.
    for (const char* q : {"SELECT SPREAD(*) FROM 'OL'.'EX'", "SELEKT * FROM 'OL'.'EX'",
                          "SELECT * FROM 'NONE'.'EX'"}) {
        EXPECT_FALSE(taken(ask(s, q, 1))) << q;
    }
}

TEST(OffLoopReads, TheMailboxGivesBackWhatWasPostedAndWakesItsLoop) {
    ob::OffLoopMailbox mailbox;
    EXPECT_TRUE(mailbox.take().empty());
    mailbox.post(ob::OffLoopAnswer{7, 70, "OK\n\n", false, 0, true});
    mailbox.post(ob::OffLoopAnswer{8, 80, "ERR x\n", false, 0, false});
    // The eventfd is readable: the loop's epoll would report it.
    uint64_t count = 0;
    ASSERT_EQ(::read(mailbox.event_fd(), &count, sizeof(count)), static_cast<ssize_t>(sizeof(count)));
    EXPECT_EQ(count, 2u);
    const auto answers = mailbox.take();
    ASSERT_EQ(answers.size(), 2u);
    EXPECT_EQ(answers[0].fd, 7);
    EXPECT_EQ(answers[0].conn_id, 70u);
    EXPECT_EQ(answers[1].answer, "ERR x\n");
    EXPECT_TRUE(mailbox.take().empty()) << "an answer was given back twice";
}

TEST(OffLoopReads, ThePoolRunsReadsInTheOrderTheyCameAndDropsWhatWaitsAtItsEnd) {
    std::vector<int> ran;
    std::mutex mtx;
    {
        ob::OffLoopPool pool(1);
        EXPECT_EQ(pool.threads(), 1u);
        for (int i = 0; i < 5; ++i) {
            pool.submit([&, i] {
                std::lock_guard<std::mutex> lock(mtx);
                ran.push_back(i);
            });
        }
        for (int spins = 0; spins < 2000; ++spins) {
            {
                std::lock_guard<std::mutex> lock(mtx);
                if (ran.size() == 5) break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }
    EXPECT_EQ(ran, (std::vector<int>{0, 1, 2, 3, 4}));

    // A read running at the end finishes; the ones queued behind it are dropped.
    std::atomic<bool> release{false};
    std::atomic<int> finished{0};
    {
        ob::OffLoopPool pool(1);
        pool.submit([&] {
            while (!release.load()) std::this_thread::sleep_for(std::chrono::milliseconds(1));
            finished.fetch_add(1);
        });
        for (int i = 0; i < 3; ++i) pool.submit([&] { finished.fetch_add(100); });
        for (int spins = 0; spins < 2000 && pool.queued() != 3; ++spins) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        EXPECT_EQ(pool.queued(), 3u);
        std::thread releaser([&] {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
            release.store(true);
        });
        releaser.detach();
    }
    EXPECT_EQ(finished.load(), 1) << "the running read did not finish, or the queued ones ran";
}
