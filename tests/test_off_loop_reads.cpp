// #260: a large read answered off the client loop.
//
// A read is large by the index's count of the rows its scan may read, asked before it runs: this file
// holds that count - a row query's range and its LIMIT, time buckets, nothing for the live book's
// functions - and the threads and the mailbox a large read goes through. What the loop does with them
// - a session paused behind its read, its later commands run after the answer, the sessions beside it
// served meanwhile - is held on a live node, in tests/integration/test_large_answer_isolation.py.

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

std::string range(uint64_t from_row, uint64_t to_row) {
    return "WHERE timestamp BETWEEN " + std::to_string(kBase + from_row * 1'000'000) + " AND " +
           std::to_string(kBase + to_row * 1'000'000);
}

}  // namespace

TEST(OffLoopReads, TheEstimateIsTheRowsInRangeCutToTheLimit) {
    Store s;
    const auto all = s.engine.estimated_scan_rows("SELECT * FROM 'OL'.'EX'");
    ASSERT_TRUE(all.has_value());
    EXPECT_EQ(*all, 20'000u);

    // Half of one segment, by the metadata's even spread: within a row or two of 2500.
    const auto half = s.engine.estimated_scan_rows("SELECT * FROM 'OL'.'EX' " + range(0, 2499));
    ASSERT_TRUE(half.has_value());
    EXPECT_NEAR(static_cast<double>(*half), 2500.0, 2.0);

    // A row query reads no more than its LIMIT.
    const auto limited = s.engine.estimated_scan_rows("SELECT price FROM 'OL'.'EX' LIMIT 10");
    ASSERT_TRUE(limited.has_value());
    EXPECT_EQ(*limited, 10u);

    // Outside every segment, and a book nobody wrote.
    EXPECT_EQ(s.engine.estimated_scan_rows("SELECT * FROM 'OL'.'EX' " + range(100'000, 200'000)), 0u);
    EXPECT_EQ(s.engine.estimated_scan_rows("SELECT * FROM 'NONE'.'EX'"), 0u);
}

TEST(OffLoopReads, BucketsScanRowsAndTheLiveBooksFunctionsDoNot) {
    Store s;
    // Time buckets read rows: the range, whatever their LIMIT, which counts buckets.
    const auto buckets = s.engine.estimated_scan_rows(
        "SELECT COUNT(*) FROM 'OL'.'EX' GROUP BY TIME_BUCKET(1s) LIMIT 1");
    ASSERT_TRUE(buckets.has_value());
    EXPECT_EQ(*buckets, 20'000u);
    // A function of the live book reads the book, and a query that does not parse reads nothing.
    EXPECT_FALSE(s.engine.estimated_scan_rows("SELECT SPREAD(*) FROM 'OL'.'EX'").has_value());
    EXPECT_FALSE(s.engine.estimated_scan_rows("SELEKT * FROM 'OL'.'EX'").has_value());
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
