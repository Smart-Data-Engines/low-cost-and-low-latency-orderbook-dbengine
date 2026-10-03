// #190, step 5: the engine's half of admission - a flush tick shows the controller what the device
// took, and a write waits what the controller says, after it is written and outside the lock.
//
// The controller's own behaviour is `tests/test_admission.cpp`. Here, without a slow device: a
// controller shown a slow tick by hand, and a tick that is slow for real because its interval is a
// millisecond. What admission does to a device slower than the ingest is measured, not tested
// (evidence/2026-10-03-write-ceiling-ec2/).

#include "orderbook/engine.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <optional>
#include <string>
#include <thread>
#include <vector>

namespace fs = std::filesystem;

namespace {

using std::chrono::milliseconds;
using std::chrono::seconds;
using Clock = std::chrono::steady_clock;

constexpr uint64_t kInterval100ms = 100'000'000ULL;

/// The value of one counter, out of the exposition: `name{labels} value` or `name value`, never a
/// longer name and never a HELP or TYPE line (pitfall 66).
std::optional<uint64_t> counter_value(const std::string& exposition, const std::string& name) {
    for (size_t at = exposition.find("\n" + name); at != std::string::npos;
         at = exposition.find("\n" + name, at + 1)) {
        const size_t after = at + 1 + name.size();
        if (after >= exposition.size()) continue;
        if (exposition[after] != '{' && exposition[after] != ' ') continue;
        const size_t space = exposition.find(' ', after);
        const size_t eol   = exposition.find('\n', after);
        if (space == std::string::npos || space > eol) continue;
        return std::strtoull(exposition.c_str() + space + 1, nullptr, 10);
    }
    return std::nullopt;
}

uint64_t delays(ob::Engine& engine) {
    const auto v = counter_value(engine.registry().serialize(), "ob_writer_admission_delays_total");
    EXPECT_TRUE(v.has_value()) << "ob_writer_admission_delays_total is not in the exposition";
    return v.value_or(0);
}

struct TempDir {
    std::string path;
    explicit TempDir(const std::string& prefix)
        : path((fs::temp_directory_path() / (prefix + std::to_string(std::rand()))).string()) {
        fs::create_directories(path);
    }
    ~TempDir() {
        std::error_code ec;
        fs::remove_all(path, ec);
    }
};

/// One delta of `levels` levels on ADM.EX.
ob::ob_status_t write(ob::Engine& engine, int levels, uint64_t ts) {
    std::vector<ob::Level> lv(static_cast<size_t>(levels));
    for (int l = 0; l < levels; ++l) {
        lv[static_cast<size_t>(l)] = {10'000'000 - l, 100 + static_cast<uint64_t>(l), 1, 0};
    }
    ob::DeltaUpdate delta{};
    std::strncpy(delta.symbol,   "ADM", sizeof(delta.symbol)   - 1);
    std::strncpy(delta.exchange, "EX",  sizeof(delta.exchange) - 1);
    delta.timestamp_ns = 1'700'000'000'000'000'000ULL + ts;
    delta.side         = ob::SIDE_BID;
    delta.n_levels     = static_cast<uint16_t>(levels);
    return engine.apply_delta(delta, lv.data());
}

}  // namespace

TEST(EngineAdmission, NothingWaitsWithoutASlowTick) {
    TempDir dir("ob_admission_none_");
    ob::Engine engine(dir.path, kInterval100ms);
    engine.open();
    for (int i = 0; i < 20; ++i) ASSERT_EQ(write(engine, 1000, static_cast<uint64_t>(i)), ob::OB_OK);
    EXPECT_EQ(delays(engine), 0u);
    EXPECT_EQ(engine.admission_for_test().delayed_batches(), 0u);
    engine.close();
}

TEST(EngineAdmission, AWriteWaitsOnceTheControllerHasSeenASlowTick) {
    TempDir dir("ob_admission_slow_");
    ob::Engine engine(dir.path, kInterval100ms);
    engine.open();
    // 1 000 rows a second - the floor - from a tick of 1 000 rows in five seconds.
    ASSERT_EQ(engine.admission_for_test().on_tick(1'000, seconds(5)),
              ob::AdmissionController::Change::Began);
    // 500 rows: half a second at that rate, less the 100 ms of credit the burst allows.
    const auto t0 = Clock::now();
    ASSERT_EQ(write(engine, 500, 1), ob::OB_OK);
    const auto took = Clock::now() - t0;
    EXPECT_GE(took, milliseconds(300)) << "the write was not held for its rows' time";
    EXPECT_GE(engine.admission_for_test().delayed_batches(), 1u);
    EXPECT_GE(delays(engine), 1u);
    engine.close();
}

TEST(EngineAdmission, OffMeansNoWaitAfterASlowTick) {
    TempDir dir("ob_admission_off_");
    ob::Engine engine(dir.path, kInterval100ms);
    engine.set_write_admission_enabled(false);
    engine.open();
    EXPECT_EQ(engine.admission_for_test().on_tick(1'000, seconds(5)),
              ob::AdmissionController::Change::None);
    ASSERT_EQ(write(engine, 500, 1), ob::OB_OK);
    EXPECT_EQ(delays(engine), 0u);
    engine.close();
}

TEST(EngineAdmission, AFlushTickThatTookTwiceItsIntervalIsSeen) {
    // A one-millisecond interval, so that a tick draining a few hundred thousand rows is slow by
    // its own measure - the one way to have the flush thread show the controller a slow tick
    // without a slow device.
    TempDir dir("ob_admission_tick_");
    ob::Engine engine(dir.path, 1'000'000ULL);
    engine.open();
    for (int i = 0; i < 300; ++i) ASSERT_EQ(write(engine, 1000, static_cast<uint64_t>(i)), ob::OB_OK);
    const auto deadline = Clock::now() + seconds(30);
    while (engine.admission_for_test().last_slow_tick().rows == 0 && Clock::now() < deadline) {
        std::this_thread::sleep_for(milliseconds(5));
    }
    const auto m = engine.admission_for_test().last_slow_tick();
    EXPECT_GT(m.rows, 0u) << "no flush tick of 300 000 rows took 2 ms against a 1 ms interval";
    EXPECT_GE(m.took, milliseconds(2));
    engine.close();
}
