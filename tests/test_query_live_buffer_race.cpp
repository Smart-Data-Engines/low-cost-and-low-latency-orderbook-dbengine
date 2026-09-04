// Regression tests for #91: a SELECT racing the creation of a symbol's live buffer.
//
// `Engine` owns `live_ptrs_`, a map from "symbol.exchange" to SoABuffer*, and every write path
// inserts into it under `mtx_` - a client write, the replication apply path, the multi-master io
// loop. `QueryEngine` used to hold a **reference** to that map and read it with no lock at all, so
// a query concurrent with the first write for a symbol read an `unordered_map` mid-rehash.
//
// ThreadSanitizer found it on the first integration run that issued a SELECT while a peer's record
// created a buffer on another thread, five reports in one run. The two tests here fail in different
// directions: one drives the race, and one refuses the shape that made it possible - because a
// behavioural test for a rehash race is probabilistic, and a shape test is not.

#include "orderbook/engine.hpp"
#include "orderbook/query_engine.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <string>
#include <thread>
#include <vector>

namespace fs = std::filesystem;

namespace {

std::string make_temp_dir(const std::string& prefix) {
    auto p = fs::temp_directory_path() / (prefix + std::to_string(std::rand()));
    fs::create_directories(p);
    return p.string();
}

void write_symbol(ob::Engine& engine, int index, uint64_t seq) {
    ob::DeltaUpdate delta{};
    std::memset(delta.symbol, 0, sizeof(delta.symbol));
    std::memset(delta.exchange, 0, sizeof(delta.exchange));
    std::snprintf(delta.symbol, sizeof(delta.symbol), "SYM%04d", index);
    std::strncpy(delta.exchange, "EX", sizeof(delta.exchange) - 1);
    delta.sequence_number = seq;
    delta.timestamp_ns    = 1'000'000'000ULL + seq;
    delta.side            = ob::SIDE_BID;
    delta.n_levels        = 1;

    ob::Level lvl{};
    lvl.price = 50000 + index;
    lvl.qty   = 10;
    lvl.cnt   = 1;
    lvl._pad  = 0;
    engine.apply_delta(delta, &lvl);
}

/// The source of the function passed to QueryEngine's constructor in `file`.
///
/// Extracted by brace matching from the `make_unique<...QueryEngine>(` call, so the assertion is
/// about the callable this file actually supplies rather than about the file containing the word
/// "lock" somewhere.
std::string live_buffer_lambda(const std::string& file) {
    std::ifstream in(std::string(OB_SOURCE_DIR) + "/" + file);
    if (!in) return {};
    const std::string src((std::istreambuf_iterator<char>(in)),
                          std::istreambuf_iterator<char>());
    const auto call = src.find("QueryEngine>(");
    if (call == std::string::npos) return {};
    // From the opening parenthesis to its match.
    auto pos = src.find('(', call);
    int depth = 0;
    const auto begin = pos;
    for (; pos < src.size(); ++pos) {
        if (src[pos] == '(') ++depth;
        else if (src[pos] == ')') {
            if (--depth == 0) return src.substr(begin, pos - begin + 1);
        }
    }
    return {};
}

} // namespace

// ── The race itself ───────────────────────────────────────────────────────────

TEST(QueryLiveBufferRace, AQueryConcurrentWithSymbolCreationIsSafe) {
    // What this detects, measured rather than assumed. With the fix reverted - the lookup no
    // longer taking `mtx_` - on i3-7100U:
    //
    //   plain Debug build : 5 of 5 runs pass, nothing detected
    //   ThreadSanitizer   : exit 66, **20 reports**, the first naming `_M_rehash_aux`
    //   TSan with the fix : exit 0, 0 reports
    //
    // So the detector here is TSan and the assertions below are only a liveness check that the
    // loop ran at all. Said outright rather than dressed in an assertion that passes either way,
    // which is pitfall 73 - and the plain-build number is why: a reader who assumed `ctest` covers
    // this would be wrong five times out of five.
    //
    // Running it locally needs `sudo sysctl -w vm.mmap_rnd_bits=28` first, or the TSan binary
    // segfaults before `main` - the same step `sanitizers-integration (tsan)` runs in CI.
    const std::string dir = make_temp_dir("qe_race_");
    {
        ob::Engine engine(dir, 60'000'000'000ULL);
        engine.open();

        std::atomic<bool> stop{false};
        std::atomic<int>  created{0};

        std::thread writer([&] {
            for (int i = 0; i < 400 && !stop.load(std::memory_order_relaxed); ++i) {
                write_symbol(engine, i, static_cast<uint64_t>(i) + 1);
                created.store(i + 1, std::memory_order_relaxed);
            }
        });

        int queries = 0;
        for (int i = 0; i < 400; ++i) {
            char sql[96];
            std::snprintf(sql, sizeof(sql), "SELECT * FROM 'SYM%04d'.'EX'", i);
            // The return value is deliberately not asserted: whether a symbol exists yet is the
            // race, and either answer is legitimate. What must not happen is undefined behaviour.
            engine.execute(sql, [](const ob::QueryResult&) {});
            ++queries;
        }

        stop.store(true, std::memory_order_relaxed);
        writer.join();
        engine.close();

        EXPECT_EQ(queries, 400);
        EXPECT_GT(created.load(), 0) << "the writer never ran, so nothing raced";
    }
    fs::remove_all(dir);
}

TEST(QueryLiveBufferRace, AggregationTakesTheSameResolvedPointer) {
    // The aggregation branch used to do its *own* unsynchronised `find()` on the same map, so a
    // query could see the symbol exist in one lookup and not in the other. One resolution per
    // query removes that as well as the race.
    const std::string dir = make_temp_dir("qe_race_agg_");
    {
        ob::Engine engine(dir, 60'000'000'000ULL);
        engine.open();
        write_symbol(engine, 7, 1);
        const auto err = engine.execute("SELECT VWAP(price) FROM 'SYM0007'.'EX'",
                                        [](const ob::QueryResult&) {});
        EXPECT_TRUE(err.empty()) << err;
        engine.close();
    }
    fs::remove_all(dir);
}

// ── The shape that made it possible ───────────────────────────────────────────

TEST(QueryLiveBufferRaceStatic, QueryEngineDoesNotHoldAReferenceToALiveBufferMap) {
    // A behavioural test for a rehash race is probabilistic; this is not. If the constructor ever
    // takes the map again, the race is back whether or not the other test happens to catch it.
    std::ifstream in(std::string(OB_SOURCE_DIR) + "/include/orderbook/query_engine.hpp");
    ASSERT_TRUE(in);
    const std::string src((std::istreambuf_iterator<char>(in)),
                          std::istreambuf_iterator<char>());
    EXPECT_EQ(src.find("unordered_map<std::string, SoABuffer*>&"), std::string::npos)
        << "QueryEngine holds a reference to a live-buffer map again; every write path inserts "
           "into that map while a query reads it (#91)";
    EXPECT_NE(src.find("LiveBufferLookup"), std::string::npos);
}

TEST(QueryLiveBufferRaceStatic, EverySupplierOfTheLookupTakesALock) {
    // Two suppliers, and both matter: `src/engine.cpp` for the server and `src/c_api.cpp` for the
    // embedded path the Python client uses locally. The C API had the identical race - `ob_insert`
    // creates buffers under its mutex and `ob_query` read the map without it - so fixing only the
    // server would have left the same defect one file away.
    for (const char* file : {"src/engine.cpp", "src/c_api.cpp"}) {
        const auto body = live_buffer_lambda(file);
        ASSERT_FALSE(body.empty()) << "no QueryEngine construction found in " << file;
        EXPECT_NE(body.find("lock_guard"), std::string::npos)
            << file << " supplies a live-buffer lookup that takes no lock:\n" << body;
    }
}
