// A writer that runs out of room asks for a flush instead of waiting out the interval (#137).
//
// `apply_delta_impl()` blocks at `MAX_PENDING_ROWS` and waits on `pending_cv_`, which only the
// flush notifies — and nothing asked the flush to run on account of a writer waiting, so the wait
// was for `--flush-interval-ms` to elapse. Measured before the change: at 4,000,000 levels through
// the wire, 1000 ms gave 1,081,417 levels/s, 5000 ms gave 254,691 and 20,000 ms gave 65,930,
// roughly inverse; at 3,600,000 ms the node cannot be told apart from a hung one, and that is
// where it was found. The work being waited for is a flush of a million rows, which takes
// **73 ms** on the reference machine.
//
// The test is written to **fail rather than hang** against the unfixed code. That matters here
// more than usual: the defect's whole shape is a thread that stops, so a test that blocks with it
// would report a stuck runner and CTest's own timeout would be the only thing that noticed.

#include "orderbook/engine.hpp"
#include "orderbook/data_model.hpp"
#include "orderbook/metrics.hpp"
#include "orderbook/types.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <functional>
#include <future>
#include <mutex>
#include <optional>
#include <thread>
#include <string>
#include <vector>

namespace fs = std::filesystem;

namespace {

/// An interval no test can wait out, so "it finished" can only mean the flush was asked for.
constexpr uint64_t kOneHourNs = 3'600'000'000'000ULL;

/// Under `--fsync-policy none`, so that what a writer runs into is the queue's ceiling. Under
/// `interval` it meets the WAL budget first (#190 step 9) - a sync's worth of WAL, 4 MiB before the
/// first sync, where these thousand-level records are about 24 kB each - and waits for a sync long
/// before a million rows are queued: these tests then passed through the budget's path and
/// exercised the ceiling not at all. `none` syncs nothing, so the budget does not bind.
constexpr ob::FsyncPolicy kTheCeilingBinds = ob::FsyncPolicy::NONE;

/// Rows above `MAX_PENDING_ROWS` (1,000,000), written 1000 at a time.
constexpr int kLevelsPerDelta = 1000;
constexpr int kDeltas         = 1100;

/// The value of one counter, out of the Prometheus exposition.
///
/// Written because the obvious lookup is wrong here in a way that passes: the exposition is
/// `ob_flush_ticks_total{node_role="standalone"} 3`, so a search for the name followed by a space
/// finds nothing, and "absent" then reads as zero. That is pitfall 66 of this repository, and the
/// first version of this file committed it twice — once failing loudly, and once **passing** for
/// the wrong reason, because `find("name 0") == npos` is satisfied by a line that never had the
/// shape `name 0` in the first place.
std::optional<uint64_t> counter_value(const std::string& exposition, const std::string& name) {
    for (size_t at = exposition.find("\n" + name); at != std::string::npos;
         at = exposition.find("\n" + name, at + 1)) {
        const size_t after = at + 1 + name.size();
        if (after >= exposition.size()) continue;
        if (exposition[after] != '{' && exposition[after] != ' ') continue;  // a longer name
        const size_t space = exposition.find(' ', after);
        const size_t eol   = exposition.find('\n', after);
        if (space == std::string::npos || space > eol) continue;             // a HELP/TYPE line
        return std::strtoull(exposition.c_str() + space + 1, nullptr, 10);
    }
    return std::nullopt;
}

std::string temp_dir(const std::string& prefix) {
    auto path = fs::temp_directory_path() / (prefix + std::to_string(std::rand()));
    fs::create_directories(path);
    return path.string();
}

/// Write delta number `d`, of `n_levels` rows.
ob::ob_status_t write_delta(ob::Engine& engine, int d, int n_levels) {
    std::vector<ob::Level> levels(static_cast<size_t>(n_levels));
    for (int l = 0; l < n_levels; ++l) {
        levels[static_cast<size_t>(l)] = {10'000'000 - l, 100 + static_cast<uint64_t>(l), 1, 0};
    }
    ob::DeltaUpdate delta{};
    std::strncpy(delta.symbol,   "BACKP", sizeof(delta.symbol)   - 1);
    std::strncpy(delta.exchange, "EX",    sizeof(delta.exchange) - 1);
    delta.timestamp_ns = 1'700'000'000'000'000'000ULL + static_cast<uint64_t>(d);
    delta.side         = ob::SIDE_BID;
    delta.n_levels     = static_cast<uint16_t>(n_levels);
    return engine.apply_delta(delta, levels.data());
}

/// Write deltas `from` to `to`, `kLevelsPerDelta` rows each. Returns OB_OK, or the first refusal.
ob::ob_status_t write_deltas(ob::Engine& engine, int from, int to) {
    for (int d = from; d < to; ++d) {
        if (auto st = write_delta(engine, d, kLevelsPerDelta); st != ob::OB_OK) return st;
    }
    return ob::OB_OK;
}

/// Write `kDeltas * kLevelsPerDelta` rows. Returns OB_OK, or the first refusal.
ob::ob_status_t fill_past_the_ceiling(ob::Engine& engine) { return write_deltas(engine, 0, kDeltas); }

/// Half of `MAX_PENDING_ROWS`, in deltas: the last row of this many brings the queue to half its
/// ceiling, and asks for the flush (#276).
constexpr int kHalfTheCeiling = 500;

uint64_t counter_or_fail(ob::Engine& engine, const std::string& name) {
    const auto v = counter_value(engine.registry().serialize(), name);
    EXPECT_TRUE(v.has_value()) << name << " is not in the exposition at all";
    return v.value_or(0);
}

/// Waits up to `seconds` for a counter to reach `at_least`; returns its value then.
uint64_t counter_once_at_least(ob::Engine& engine, const std::string& name, uint64_t at_least,
                               int seconds) {
    uint64_t v = 0;
    for (int i = 0; i < seconds * 100; ++i) {
        v = counter_or_fail(engine, name);
        if (v >= at_least) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return v;
}

/// Waits up to `seconds` for the rows not yet in a segment to be `rows`; returns their number then.
size_t queued_once_at(ob::Engine& engine, size_t rows, int seconds) {
    size_t queued = 0;
    for (int i = 0; i < seconds * 100; ++i) {
        queued = engine.stats().pending_rows;
        if (queued == rows) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return queued;
}

/// Writes `kDeltas` deltas, past the ceiling, with the flush held where it is about to take the
/// queue: the tick asked for at half has begun - the loop cleared the request as it woke - and waits
/// for the hold, so the writer goes on to the ceiling and waits there, and the hold is let go once it
/// has. Since #276 that is how a writer reaches the ceiling at all: asked for at half, a tick takes
/// the queue while the writers fill the other half.
void write_past_the_ceiling_against_a_held_flush(ob::Engine& engine) {
    std::future<ob::ob_status_t> rest;    // declared before the hold, so it is let go first
    auto hold = engine.hold_flush_for_test();
    ASSERT_EQ(write_deltas(engine, 0, kHalfTheCeiling), ob::OB_OK);
    ASSERT_EQ(counter_once_at_least(engine, "ob_flush_ticks_total", 1, 30), 1u)
        << "no tick began: half the ceiling was queued and no flush was asked for";
    rest = std::async(std::launch::async,
                      [&engine]() { return write_deltas(engine, kHalfTheCeiling, kDeltas); });
    ASSERT_GE(counter_once_at_least(engine, "ob_writer_backpressure_waits_total", 1, 30), 1u)
        << "the writer never waited, so it never reached the ceiling - or its wait is not counted";
    hold.unlock();
    ASSERT_EQ(rest.wait_for(std::chrono::seconds(60)), std::future_status::ready)
        << "the writer did not finish within 60 s of the flush being let go";
    ASSERT_EQ(rest.get(), ob::OB_OK)
        << "a write was refused: the flush that was let go did not free room within the deadline";
}

}  // namespace

TEST(PendingBackpressure, AWriterAtTheCeilingWaitsForAFlushRatherThanForTheInterval) {
    const std::string dir = temp_dir("backp_ask_");
    ob::Engine engine(dir, kOneHourNs, kTheCeilingBinds);
    engine.open();

    // On another thread with a bound, so the unfixed code fails this test in a minute with a
    // sentence rather than wedging the suite for an hour. Since #276 the flush is asked for when half
    // the ceiling is queued, so the writer may not reach the ceiling at all: "it finished" means a
    // flush was asked for, at half or at the ceiling. That one at the ceiling still is, a test below.
    auto written = std::async(std::launch::async, [&engine]() { return fill_past_the_ceiling(engine); });
    const auto verdict = written.wait_for(std::chrono::seconds(60));

    if (verdict != std::future_status::ready) {
        // The writer is still inside apply_delta. Let the engine tear down so the thread can
        // leave — close() sets stop_flush_ and notifies, which is the escape the wait has.
        engine.close();
        written.wait();
        FAIL() << "a writer that ran out of room did not finish within 60 s against a one-hour "
                  "flush interval, so it is waiting for the interval and not for a flush";
    }

    EXPECT_EQ(written.get(), ob::OB_OK)
        << "the writes were refused, which is the deadline firing on a flush that should have "
           "been asked for and should have taken milliseconds";
    // The flag the writer sets has to be cleared by the loop that reads it. Left set, the
    // predicate is permanently true and the loop flushes as fast as it can — a core burned and
    // nothing said, which is #298's shape. Ticks are the observable: at a one-hour interval the
    // only ones that should run are the ones a writer asked for.
    // A window for a spin to show in. A request the loop never clears leaves its predicate
    // permanently true, so the loop stops waiting altogether — and it accumulates nothing at all
    // in the microsecond between the last write and this line, which is how the first version of
    // this assertion measured 1 tick against a mutation that removes the clear. Two hundred
    // milliseconds against a one-hour interval is three orders of magnitude of room: a healthy
    // loop cannot tick in it, and a spinning one cannot help but.
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    const auto ticks = counter_value(engine.registry().serialize(), "ob_flush_ticks_total");
    ASSERT_TRUE(ticks.has_value()) << "the tick counter is not in the exposition at all";
    EXPECT_GT(*ticks, 0u) << "no flush ran at all, so nothing was asked for";
    EXPECT_LT(*ticks, 20u)
        << "the flush loop ran " << *ticks << " times against a one-hour interval, one "
           "request and a 200 ms window, so the request flag is not being cleared and the "
           "loop is spinning";

    engine.close();
    fs::remove_all(dir);
}

TEST(PendingBackpressure, TheWaitIsCountedSoAnOperatorCanSeeItHappened) {
    const std::string dir = temp_dir("backp_count_");
    ob::Engine engine(dir, kOneHourNs, kTheCeilingBinds);
    engine.open();

    ASSERT_NO_FATAL_FAILURE(write_past_the_ceiling_against_a_held_flush(engine));

    // The number is the point: a writer waited, and nothing about that is visible from outside
    // the process without it. Before #137 there was no line and no counter, which is why a node
    // that had stopped looked exactly like a node with nothing to do.
    const std::string exposition = engine.registry().serialize();
    const auto waits = counter_value(exposition, "ob_writer_backpressure_waits_total");
    const auto refusals = counter_value(exposition, "ob_writer_backpressure_refusals_total");
    ASSERT_TRUE(waits.has_value()) << "the counter is not in the exposition at all";
    EXPECT_GT(*waits, 0u)
        << "writing past MAX_PENDING_ROWS did not count a single wait, so either the ceiling was "
           "not reached or the counter is not fed";
    // The pair is what an operator reads: waits without refusals is backpressure working.
    ASSERT_TRUE(refusals.has_value());
    EXPECT_EQ(*refusals, 0u) << "a write was refused, so the flush never freed room";

    engine.close();
    fs::remove_all(dir);
}

TEST(PendingBackpressure, AWriterAtTheCeilingAsksForTheTickAfterTheOneItWaitsFor) {
    // #137's request, after #276: a writer reaches the ceiling while a tick holds the queue, or after
    // one has failed. The tick asked for at half is held here; the writer at the ceiling asks for the
    // next, which takes what it wrote once the held one let it go - with the flush an hour away and no
    // other row reaching half, there is no second tick without that request.
    const std::string dir = temp_dir("backp_next_");
    ob::Engine engine(dir, kOneHourNs, kTheCeilingBinds);
    engine.open();

    ASSERT_NO_FATAL_FAILURE(write_past_the_ceiling_against_a_held_flush(engine));
    // What the second tick takes depends on when it begins - with the writer's last rows or before
    // them - so the ticks are counted, not the rows.
    EXPECT_GE(counter_once_at_least(engine, "ob_flush_ticks_total", 2, 30), 2u)
        << "the writer at the ceiling did not ask for the tick after the held one";
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    EXPECT_EQ(counter_or_fail(engine, "ob_flush_ticks_total"), 2u)
        << "the held tick and the one the writer at the ceiling asked for, and no more";

    engine.close();
    fs::remove_all(dir);
}

TEST(PendingBackpressure, TheRowThatBringsTheQueueToHalfTheCeilingAsksForAFlush) {
    // #276. The queue fills at the write rate and empties a tick at a time, and a writer asked for a
    // tick only once the queue had no room left - so above a million rows an interval, every tick
    // was one a writer had stopped for. The row that brings the queue to half the ceiling asks for
    // it, and the tick runs while the writers fill the other half.
    const std::string dir = temp_dir("backp_half_");
    ob::Engine engine(dir, kOneHourNs, kTheCeilingBinds);
    engine.open();

    // One row short of half. Nothing may tick - the interval is an hour away - and a window is given
    // for a tick to show in.
    ASSERT_EQ(write_deltas(engine, 0, kHalfTheCeiling - 1), ob::OB_OK);
    ASSERT_EQ(write_delta(engine, kHalfTheCeiling - 1, kLevelsPerDelta - 1), ob::OB_OK);
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    EXPECT_EQ(counter_or_fail(engine, "ob_flush_ticks_total"), 0u)
        << "499 999 rows queued were flushed, so something asks for a flush short of half the ceiling";

    // The 500 000th row.
    ASSERT_EQ(write_delta(engine, kHalfTheCeiling, 1), ob::OB_OK);
    EXPECT_GE(counter_once_at_least(engine, "ob_flush_ticks_total", 1, 30), 1u)
        << "half the ceiling was queued and no flush ran, so the writers would go on to the whole "
           "of it and stop there, waiting for a tick nobody had asked for";
    // Asked for once: a request the loop never clears would spin it.
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    EXPECT_EQ(counter_or_fail(engine, "ob_flush_ticks_total"), 1u);
    EXPECT_EQ(counter_or_fail(engine, "ob_writer_backpressure_waits_total"), 0u);

    engine.close();
    fs::remove_all(dir);
}

TEST(PendingBackpressure, RowsWrittenBeforeTheTickTakesTheQueueDoNotAskAgain) {
    // #276. The loop clears the request as it wakes, and the tick takes the queue a moment later,
    // under the engine's lock: a writer in between finds half the ceiling queued and nothing asked
    // for. Asking again there would follow the tick with another, for whatever came in while it ran -
    // which under a steady rate is every tick. The tick is held in between here.
    const std::string dir = temp_dir("backp_window_");
    ob::Engine engine(dir, kOneHourNs, kTheCeilingBinds);
    engine.open();

    {
        auto hold = engine.hold_flush_for_test();
        ASSERT_EQ(write_deltas(engine, 0, kHalfTheCeiling), ob::OB_OK);
        ASSERT_EQ(counter_once_at_least(engine, "ob_flush_ticks_total", 1, 30), 1u)
            << "no tick began: half the ceiling was queued and no flush was asked for";
        ASSERT_EQ(write_deltas(engine, kHalfTheCeiling, kHalfTheCeiling + 10), ob::OB_OK);
    }
    EXPECT_EQ(queued_once_at(engine, 0, 30), 0u) << "the held tick did not flush the queue within 30 s";
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    EXPECT_EQ(counter_or_fail(engine, "ob_flush_ticks_total"), 1u)
        << "rows written while the tick was about to take them asked for another tick";

    engine.close();
    fs::remove_all(dir);
}

TEST(PendingBackpressure, TheRowsATickHoldsDoNotAskForTheNextOne) {
    // #276. A tick's rows count against the ceiling until it has drained them, but they are not in
    // the queue: a writer that asked for a tick on their account would follow every tick of half the
    // ceiling with another, for whatever came in while it synced and drained.
    const std::string dir = temp_dir("backp_held_");
    ob::Engine engine(dir, kOneHourNs, kTheCeilingBinds);
    engine.open();

    // The first tick is held after it has taken the queue, until the test lets it go.
    std::mutex m;
    std::condition_variable cv;
    bool entered = false, released = false;
    std::atomic<int> takes{0};
    engine.while_syncing_wal_for_test([&] {
        if (takes.fetch_add(1) != 0) return;
        std::unique_lock<std::mutex> lock(m);
        entered = true;
        cv.notify_all();
        cv.wait(lock, [&] { return released; });
    });
    const std::function<void()> release = [&] {
        {
            std::lock_guard<std::mutex> lock(m);
            released = true;
        }
        cv.notify_all();
    };
    // Let go and take the hook out on every way out of this test: the hook is set under the lock the
    // held tick holds, and a hook left in would be called by close() after what it refers to is gone.
    struct LetGo {
        const std::function<void()>& let_go;
        ob::Engine& e;
        ~LetGo() {
            let_go();
            e.while_syncing_wal_for_test({});
        }
    } let_go{release, engine};

    ASSERT_EQ(write_deltas(engine, 0, kHalfTheCeiling), ob::OB_OK);
    {
        std::unique_lock<std::mutex> lock(m);
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(30), [&] { return entered; }))
            << "no tick took the queue: half the ceiling was queued and no flush was asked for";
    }
    // 500 000 rows are the held tick's and count against the ceiling; ten thousand more wait for the
    // next tick, far short of half of it.
    ASSERT_EQ(write_deltas(engine, kHalfTheCeiling, kHalfTheCeiling + 10), ob::OB_OK);
    release();

    EXPECT_EQ(queued_once_at(engine, 10'000, 30), 10'000u)
        << "the held tick did not drain its rows within 30 s of being let go";
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    EXPECT_EQ(counter_or_fail(engine, "ob_flush_ticks_total"), 1u)
        << "rows written while a tick held half the ceiling asked for another tick for ten thousand";

    engine.while_syncing_wal_for_test({});
    engine.close();
    fs::remove_all(dir);
}
