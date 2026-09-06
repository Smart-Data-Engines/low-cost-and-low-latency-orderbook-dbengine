// Regression tests for #92: a query holds a resolved `SoABuffer*` across a snapshot install.
//
// `buffers_` owns the `SoABuffer`s, and `load_snapshot()` clears them under `flush_mtx_` + `mtx_`,
// which destroys every one. Since #91 a query resolves its pointer under `mtx_` and then reads
// through it **after releasing the lock** - so an install landing in that window frees memory a
// query is reading. The write path is safe from this by accident of its own locking: `apply_delta`
// holds `mtx_` across the buffer write, so it cannot overlap a clear. The read path does not, and
// must not: holding `mtx_` for a whole scan would put a query's latency on the write path.
//
// Latent, and that is the interesting part: an install happens on bootstrap and on a full resync,
// neither of which overlaps steady-state querying in any test. A raw pointer whose lifetime is
// nested by convention rather than by construction, which is the shape of pitfall 22.
//
// Two tests, failing in different directions. One drives the race and is only loud under
// AddressSanitizer, because a read of freed memory in quarantine is silent otherwise. One refuses
// the shape that makes the race possible, and needs no scheduler luck at all.

#include "orderbook/engine.hpp"
#include "orderbook/query_engine.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <unistd.h>
#include <filesystem>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <type_traits>
#include <vector>

namespace fs = std::filesystem;

namespace {

std::string make_temp_dir(const std::string& prefix) {
    auto p = fs::temp_directory_path() / (prefix + std::to_string(::getpid()) + "_" +
                                          std::to_string(std::rand()));
    fs::create_directories(p);
    return p.string();
}

/// One update of `kLevels` levels for the single symbol these tests use.
///
/// Deep rather than one level, because the window this test drives is the aggregation read: a
/// hundred levels make `read_snapshot()` plus the per-expression scan long enough to be worth
/// racing, and one level makes it a handful of instructions.
constexpr uint32_t kLevels = 100;

void write_one(ob::Engine& engine, uint64_t seq, int32_t price) {
    ob::DeltaUpdate delta{};
    std::strncpy(delta.symbol,   "LIFE", sizeof(delta.symbol)   - 1);
    std::strncpy(delta.exchange, "EX",   sizeof(delta.exchange) - 1);
    delta.sequence_number = seq;
    delta.timestamp_ns    = 1'000'000'000ULL + seq;
    delta.side            = ob::SIDE_BID;
    delta.n_levels        = kLevels;

    std::vector<ob::Level> levels(kLevels);
    for (uint32_t i = 0; i < kLevels; ++i) {
        levels[i].price = price - static_cast<int32_t>(i);
        levels[i].qty   = 10 + static_cast<int64_t>(i);
        levels[i].cnt   = 1;
        levels[i]._pad  = 0;
    }
    engine.apply_delta(delta, levels.data());
}

} // namespace

TEST(QueryBufferLifetime, ASnapshotInstallDuringAQueryDoesNotFreeWhatTheQueryReads) {
    const std::string dir = make_temp_dir("ob_buflife_");
    {
        ob::Engine engine(dir, 60'000'000'000ULL, ob::FsyncPolicy::NONE);
        engine.open();
        write_one(engine, 1, 50'000);

        std::atomic<bool> stop{false};
        std::atomic<uint64_t> queries{0};

        // The reader: resolves the live buffer and scans it, over and over.
        std::thread reader([&] {
            while (!stop.load(std::memory_order_relaxed)) {
                // VWAP, not `SELECT *`. The plain projection resolves the live buffer for an
                // existence check and **never dereferences it**: the first version of this test
                // used it, drove the race for three ASan runs and reported clean, because the
                // pointer it was racing was only ever compared against null. The aggregation
                // branch is the one that reads through it (`read_snapshot(*buf, ...)`), which is
                // also why #91's test picked VWAP.
                engine.execute("SELECT VWAP(price) FROM 'LIFE'.'EX'",
                               [](const ob::QueryResult&) {});
                queries.fetch_add(1, std::memory_order_relaxed);
            }
        });

        // The installer: what a replica does on bootstrap and on a full resync. The manifest is
        // unused by `load_snapshot()`; what matters is that it clears the buffers.
        //
        // The pause matters and was measured, not chosen: without it 400 installs finished in
        // 31 ms and the reader got **one** query in, because `load_snapshot()` holds `mtx_` and
        // the reader needs it to resolve. A driver that starves the thread it is racing is not
        // driving a race.
        const ob::SnapshotManifest manifest{};
        uint64_t installs = 0;
        for (int i = 0; i < 600; ++i) {
            engine.load_snapshot(manifest);
            ++installs;
            // Re-create the buffer, so the next query has something to resolve and the window is
            // entered again rather than short-circuiting on a missing symbol.
            write_one(engine, static_cast<uint64_t>(i) + 2, 50'000 + i);
            std::this_thread::sleep_for(std::chrono::microseconds(300));
        }

        stop.store(true, std::memory_order_relaxed);
        reader.join();

        // The assertion is that this ran at all: under ASan a read through a freed buffer aborts
        // the process. The counts are asserted so that a run in which one side barely moved -
        // a starved reader, or a symbol that had gone for good - cannot pass as a clean one.
        EXPECT_GT(queries.load(), 100u)
            << "the reader barely ran, so this test proves nothing about the window";
        EXPECT_GT(installs, 100u);
        engine.close();
    }
    fs::remove_all(dir);
}

TEST(QueryBufferLifetimeStatic, TheLookupHandsOutAnOwningHandle) {
    // The shape that makes the race above impossible rather than unlikely: what the query holds is
    // an owning handle, so a buffer cleared out of `buffers_` mid-query stays alive until that
    // query drops it. Checked as a type rather than as a source pattern, because the compiler
    // decides this one - and asserted at runtime so a revert reads as a failing test rather than
    // as a build error in a test file.
    const bool owning = std::is_same_v<
        ob::LiveBufferLookup,
        std::function<std::shared_ptr<ob::SoABuffer>(const std::string&)>>;
    EXPECT_TRUE(owning)
        << "the live-buffer lookup hands out a raw pointer, so a snapshot install during a query "
           "frees what the query is reading (#92)";
}
