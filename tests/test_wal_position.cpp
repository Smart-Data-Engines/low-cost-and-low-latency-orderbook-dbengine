// The WAL position is read from four threads and has to be a coherent pair (#85).
//
// Spec: kiro-workspace/specs/wal-position-coherence/.
//
// TSan reported the read of `current_offset()` from `FailoverManager::publish_position_if_due()`
// against the write from the flush thread. The atomicity is the smaller half. The real defect is
// that the file index and the offset were read as a **pair** by two separate loads — in
// `Engine::get_wal_position()` and in `MultiMasterManager::send_handshake()`, two adjacent lines
// each — so a rotation between them yields a position that never existed.
//
// That pair feeds the published WAL position which election deference compares to pick the replica
// furthest ahead (#70, #72). A new file index carrying the previous file's offset reads as a
// candidate that went backwards by a whole file. Stale is survivable; incoherent is not.

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <string>
#include <thread>
#include <vector>

#include "orderbook/data_model.hpp"
#include "orderbook/wal.hpp"

namespace {

std::filesystem::path make_temp_dir(const std::string& suffix) {
    const auto base = std::filesystem::temp_directory_path() /
                      ("ob_wal_pos_" + suffix + "_" +
                       std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    std::filesystem::create_directories(base);
    return base;
}

ob::DeltaUpdate make_delta(uint64_t seq) {
    ob::DeltaUpdate upd{};
    std::strncpy(upd.symbol, "POSSYM", sizeof(upd.symbol) - 1);
    std::strncpy(upd.exchange, "POSEX", sizeof(upd.exchange) - 1);
    upd.sequence_number = seq;
    upd.timestamp_ns    = 1'000'000'000ULL + seq;
    upd.side            = ob::SIDE_BID;
    upd.n_levels        = 1;
    return upd;
}

ob::Level make_level(int64_t price) {
    ob::Level lv{};
    lv.price = price;
    lv.qty   = 100;
    lv.cnt   = 1;
    return lv;
}

} // namespace

// ── The pair, without threads ─────────────────────────────────────────────────────────────────────

TEST(WalPosition, EveryObservedPositionIsOneThatExisted) {
    // The invariant, not a branch: a position with file index N and an offset larger than what file
    // N ever reached is a position that never existed. Written as an invariant on purpose - the
    // implementation this replaces produced exactly that shape, and the next reader of these two
    // accessors would produce it again.
    const auto dir = make_temp_dir("pair");
    // Small threshold so rotation happens inside the loop rather than after it.
    ob::WALWriter writer(dir.string(), 4096);

    uint32_t last_index = writer.current_position().file_index;
    uint32_t high_water = 0;   // largest offset seen for `last_index`

    for (uint64_t seq = 1; seq <= 500; ++seq) {
        ob::Level lv = make_level(10'000 + static_cast<int64_t>(seq));
        writer.append(make_delta(seq), &lv);
        const ob::WalPosition pos = writer.current_position();

        if (pos.file_index == last_index) {
            EXPECT_GE(pos.offset, high_water)
                << "the offset went backwards inside one file, at seq " << seq;
            high_water = pos.offset;
        } else {
            EXPECT_GT(pos.file_index, last_index) << "the file index went backwards at seq " << seq;
            // The moment that used to be wrong: a fresh file must not carry the previous file's
            // offset. A rotation resets it, so it has to be *below* the water mark of the file that
            // was just closed.
            EXPECT_LT(pos.offset, high_water)
                << "file " << pos.file_index << " reports offset " << pos.offset
                << ", which is the previous file's high water mark of " << high_water
                << " - that pair never existed";
            last_index = pos.file_index;
            high_water = pos.offset;
        }
    }

    EXPECT_GT(last_index, 0u) << "no rotation happened, so the interesting half was never exercised";
    std::filesystem::remove_all(dir);
}

// ── The pair, across threads ──────────────────────────────────────────────────────────────────────

TEST(WalPosition, WithinOneFileTheOffsetNeverGoesBackwardsForAReaderOnAnotherThread) {
    // One thread writes and rotates, another samples the position - exactly what
    // `FailoverManager::monitor_loop()` does every 100 ms. On the unfixed tree TSan reports a data
    // race on this read.
    //
    // **The invariant took two attempts and the first one was wrong**, which is worth recording
    // because it looked obviously right: "a fresh file index must not carry an offset larger than
    // the previous file's high water mark". A sampling reader **misses whole files** - with a 4 KiB
    // threshold it can jump from file 5 to file 40 - so the previous index's *sampled* high water is
    // not that file's size, and the comparison is meaningless. It reported 182 118 of 3 010 027
    // observations as incoherent against a correct implementation.
    //
    // What survives missed samples: **within one file index the offset only grows.** That is also
    // precisely what a torn pair produces - the reader loads the index before a rotation and the
    // offset after it, getting `(N, 0)` having already seen `(N, large)`.
    const auto dir = make_temp_dir("threads");
    ob::WALWriter writer(dir.string(), 4096);

    std::atomic<bool> stop{false};
    std::atomic<uint64_t> observations{0};
    std::atomic<uint64_t> went_backwards{0};

    std::thread reader([&] {
        uint32_t last_index  = writer.current_position().file_index;
        uint32_t last_offset = 0;
        while (!stop.load(std::memory_order_relaxed)) {
            const ob::WalPosition pos = writer.current_position();
            observations.fetch_add(1, std::memory_order_relaxed);
            if (pos.file_index == last_index) {
                if (pos.offset < last_offset) {
                    went_backwards.fetch_add(1, std::memory_order_relaxed);
                }
                last_offset = pos.offset;
            } else if (pos.file_index > last_index) {
                last_index  = pos.file_index;
                last_offset = pos.offset;
            } else {
                // The index itself going backwards is never legitimate.
                went_backwards.fetch_add(1, std::memory_order_relaxed);
            }
        }
    });

    for (uint64_t seq = 1; seq <= 3000; ++seq) {
        ob::Level lv = make_level(10'000 + static_cast<int64_t>(seq % 500));
        writer.append(make_delta(seq), &lv);
    }
    stop.store(true, std::memory_order_relaxed);
    reader.join();

    EXPECT_GT(observations.load(), 0u) << "the reader never ran, so this proves nothing";
    EXPECT_GT(writer.current_position().file_index, 0u) << "no rotation happened during the run";
    EXPECT_EQ(went_backwards.load(), 0u)
        << went_backwards.load() << " of " << observations.load()
        << " observed positions moved backwards, which one coherent load cannot produce";
    std::filesystem::remove_all(dir);
}

// ── The bound that makes a 32-bit offset safe ─────────────────────────────────────────────────────

TEST(WalPosition, ARotateThresholdThatCouldOverflowTheOffsetIsRefused) {
    const auto dir = make_temp_dir("threshold");
    // Refused, not clamped: WAL rotation decides how much has to be replayed after a crash, and an
    // operator who asked for 8 GiB files should not silently get 2.
    EXPECT_THROW(ob::WALWriter(dir.string(), 8ULL << 30), std::invalid_argument);
    // And the message has to carry the number, or the operator is left guessing.
    try {
        ob::WALWriter writer(dir.string(), 8ULL << 30);
        FAIL() << "no refusal";
    } catch (const std::invalid_argument& e) {
        const std::string message = e.what();
        EXPECT_NE(message.find("2"), std::string::npos) << message;
    }
    // The default and anything sane still work.
    EXPECT_NO_THROW(ob::WALWriter(dir.string(), 512ULL << 20));
    std::filesystem::remove_all(dir);
}

// ── The rule, not this instance ───────────────────────────────────────────────────────────────────

TEST(WalPosition, NoSourceFileComposesThePairFromTwoSeparateCalls) {
    // Static, over the sources. Every reader that wanted both halves wrote it the same way, because
    // that was how it was written everywhere: the index on one line and the offset on the next.
    // There were five such sites - `Engine::get_wal_position()`, two snapshot manifests, the
    // handshake, and one more in the engine - so the next reader will reach for the same shape.
    //
    // A behavioural test cannot cover this: it would have to hit the two-instruction window, and the
    // measured rate for that is one in 150 million reads. So the guard is the source.
    namespace fs = std::filesystem;
    const fs::path src = fs::path(OB_SOURCE_DIR) / "src";
    ASSERT_TRUE(fs::exists(src)) << "cannot find " << src;

    std::vector<std::string> offenders;
    for (const auto& entry : fs::recursive_directory_iterator(src)) {
        if (!entry.is_regular_file() || entry.path().extension() != ".cpp") continue;
        std::ifstream in(entry.path());
        std::string line;
        int number = 0;
        bool saw_index_on_previous_line = false;
        int index_line = 0;
        while (std::getline(in, line)) {
            ++number;
            const bool has_index  = line.find("current_file_index()") != std::string::npos;
            const bool has_offset = line.find("current_offset()") != std::string::npos;
            // Both on one line is the same defect, more compactly.
            if (has_index && has_offset) {
                offenders.push_back(entry.path().filename().string() + ":" +
                                    std::to_string(number));
            } else if (saw_index_on_previous_line && has_offset) {
                offenders.push_back(entry.path().filename().string() + ":" +
                                    std::to_string(index_line) + "-" + std::to_string(number));
            }
            saw_index_on_previous_line = has_index;
            index_line = number;
        }
    }

    EXPECT_TRUE(offenders.empty())
        << "these sites read current_file_index() and current_offset() together, which assembles a "
           "pair from two moments and can report a position that never existed. Use "
           "current_position(). Offenders: "
        << [&] {
               std::string joined;
               for (const auto& o : offenders) joined += o + " ";
               return joined;
           }();
}
