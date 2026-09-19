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

#include <chrono>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <future>
#include <optional>
#include <string>
#include <vector>

namespace fs = std::filesystem;

namespace {

/// An interval no test can wait out, so "it finished" can only mean the flush was asked for.
constexpr uint64_t kOneHourNs = 3'600'000'000'000ULL;

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

/// Write `kDeltas * kLevelsPerDelta` rows. Returns OB_OK, or the first refusal.
ob::ob_status_t fill_past_the_ceiling(ob::Engine& engine) {
    std::vector<ob::Level> levels(kLevelsPerDelta);
    for (int d = 0; d < kDeltas; ++d) {
        for (int l = 0; l < kLevelsPerDelta; ++l) {
            levels[static_cast<size_t>(l)] = {10'000'000 - l, 100 + static_cast<uint64_t>(l),
                                              1, 0};
        }
        ob::DeltaUpdate delta{};
        std::strncpy(delta.symbol,   "BACKP", sizeof(delta.symbol)   - 1);
        std::strncpy(delta.exchange, "EX",    sizeof(delta.exchange) - 1);
        delta.timestamp_ns = 1'700'000'000'000'000'000ULL + static_cast<uint64_t>(d);
        delta.side         = ob::SIDE_BID;
        delta.n_levels     = static_cast<uint16_t>(kLevelsPerDelta);
        if (auto st = engine.apply_delta(delta, levels.data()); st != ob::OB_OK) return st;
    }
    return ob::OB_OK;
}

}  // namespace

TEST(PendingBackpressure, AWriterAtTheCeilingWaitsForAFlushRatherThanForTheInterval) {
    const std::string dir = temp_dir("backp_ask_");
    ob::Engine engine(dir, kOneHourNs);
    engine.open();

    // On another thread with a bound, so the unfixed code fails this test in a minute with a
    // sentence rather than wedging the suite for an hour.
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
    const auto ticks = counter_value(engine.registry().serialize(), "ob_flush_ticks_total");
    ASSERT_TRUE(ticks.has_value()) << "the tick counter is not in the exposition at all";
    EXPECT_GT(*ticks, 0u) << "no flush ran at all, so nothing was asked for";
    EXPECT_LT(*ticks, 50u)
        << "the flush loop ran " << *ticks << " times against a one-hour interval and one "
           "request, so the request flag is not being cleared";

    engine.close();
    fs::remove_all(dir);
}

TEST(PendingBackpressure, TheWaitIsCountedSoAnOperatorCanSeeItHappened) {
    const std::string dir = temp_dir("backp_count_");
    ob::Engine engine(dir, kOneHourNs);
    engine.open();

    auto written = std::async(std::launch::async, [&engine]() { return fill_past_the_ceiling(engine); });
    ASSERT_EQ(written.wait_for(std::chrono::seconds(60)), std::future_status::ready)
        << "the writer never finished; the test above says what that means";
    ASSERT_EQ(written.get(), ob::OB_OK);

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
