// AntiEntropyManager: what a reconciliation pass counts, and what it calls repaired.
//
// The pass itself is injected as a function, so these tests need no cluster, no etcd and no
// ports — they hand the manager a fake reconciler and check the arithmetic. That is the point of
// the injection: the previous version of this class could only be exercised by standing up a
// multi-master cluster, and so it never was. detect_gaps() returned an empty list
// unconditionally, repair_gap() returned false, and the run counter ticked away, which read as
// "checked, nothing to repair" for months (roadmap #57, and the crash it hid, #68).

#include "orderbook/anti_entropy.hpp"
#include "orderbook/engine.hpp"
#include "orderbook/peer_registry.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <filesystem>
#include <string>

namespace fs = std::filesystem;

namespace {

static std::atomic<uint64_t> g_dir_counter{0};

struct TempDir {
    std::string path;
    explicit TempDir(const std::string& prefix) {
        auto p = fs::temp_directory_path() /
                 (prefix + std::to_string(g_dir_counter.fetch_add(1, std::memory_order_relaxed)));
        fs::create_directories(p);
        path = p.string();
    }
    ~TempDir() {
        std::error_code ec;
        fs::remove_all(path, ec);
    }
    TempDir(const TempDir&) = delete;
    TempDir& operator=(const TempDir&) = delete;
};

/// A manager with a real engine (it publishes metrics through it) and a registry it never uses,
/// because in this design the peers come from the injected pass, not from etcd.
struct Fixture {
    TempDir dir{"anti_entropy_"};
    ob::Engine engine{dir.path, 3'600'000'000'000ULL, ob::FsyncPolicy::EVERY};
    // The registry is constructed but never contacted: in this design the peers come from the
    // injected pass, not from etcd. It stays in the constructor signature because the manager
    // still owns the reference.
    ob::PeerRegistry registry{ob::CoordinatorConfig{}, /*local_node_id=*/1, "127.0.0.1:1"};
    ob::AntiEntropyManager manager;

    Fixture() : manager(ob::AntiEntropyConfig{3600}, engine, registry) { engine.open(); }
    ~Fixture() { engine.close(); }
};

ob::VectorGap gap(uint16_t peer, const std::string& key, uint16_t origin,
                  uint64_t from_seq, uint64_t to_seq) {
    return ob::VectorGap{peer, key, origin, from_seq, to_seq};
}

}  // namespace

TEST(AntiEntropy, WithoutAReconcilerARunSaysSoInsteadOfReportingNothingToDo) {
    Fixture f;

    const auto result = f.manager.run_now();

    EXPECT_TRUE(result.reconciler_missing)
        << "a run that could not check anything must not look like a run that found nothing";
    EXPECT_EQ(result.gaps_detected, 0u);
    EXPECT_EQ(f.manager.total_runs(), 1u) << "the run still counts, so the gap between runs and "
                                             "checks is visible rather than hidden";
}

TEST(AntiEntropy, CountsGapsInBothDirections) {
    Fixture f;
    f.manager.set_reconciler([] {
        ob::ReconcileReport r{};
        r.peers_contacted = 2;
        r.vectors_sent    = 2;
        r.we_lack.push_back(gap(2, "A.EX", 1, 5, 9));
        r.peer_lacks.push_back(gap(2, "B.EX", 1, 3, 7));
        r.peer_lacks.push_back(gap(3, "C.EX", 2, 1, 4));
        return r;
    });

    const auto result = f.manager.run_now();

    EXPECT_FALSE(result.reconciler_missing);
    EXPECT_EQ(result.peers_checked, 2u);
    EXPECT_EQ(result.vectors_sent, 2u);
    EXPECT_EQ(result.gaps_detected, 3u) << "both directions count as disagreement";
    EXPECT_EQ(result.we_lack, 1u) << "and the direction that costs us data is reported separately";
}

TEST(AntiEntropy, ARepairIsCountedWhenTheGapIsGoneNotWhenItWasRequested) {
    Fixture f;

    int pass = 0;
    f.manager.set_reconciler([&pass] {
        ob::ReconcileReport r{};
        r.peers_contacted = 1;
        r.vectors_sent    = 1;
        if (pass++ == 0) {
            r.we_lack.push_back(gap(2, "A.EX", 1, 5, 9));   // behind on the first pass
        }
        return r;                                            // and caught up on the second
    });

    const auto first = f.manager.run_now();
    EXPECT_EQ(first.we_lack, 1u);
    EXPECT_EQ(first.gaps_closed, 0u)
        << "nothing can be called repaired on the pass that discovers it";

    const auto second = f.manager.run_now();
    EXPECT_EQ(second.we_lack, 0u);
    EXPECT_EQ(second.gaps_closed, 1u)
        << "the gap disappeared between passes, which is the only evidence a repair happened";
    EXPECT_EQ(f.manager.total_repairs(), 1u);
}

TEST(AntiEntropy, AGapThatPersistsIsNotCountedAsRepaired) {
    Fixture f;
    f.manager.set_reconciler([] {
        ob::ReconcileReport r{};
        r.peers_contacted = 1;
        r.vectors_sent    = 1;
        r.we_lack.push_back(gap(2, "A.EX", 1, 5, 9));
        return r;
    });

    f.manager.run_now();
    const auto second = f.manager.run_now();
    const auto third  = f.manager.run_now();

    EXPECT_EQ(second.gaps_closed, 0u);
    EXPECT_EQ(third.gaps_closed, 0u)
        << "sending a vector every pass is not a repair; the difference is still there";
    EXPECT_EQ(f.manager.total_repairs(), 0u);
}

TEST(AntiEntropy, TheSameSymbolFromTwoPeersIsTwoGaps) {
    Fixture f;
    int pass = 0;
    f.manager.set_reconciler([&pass] {
        ob::ReconcileReport r{};
        r.peers_contacted = 2;
        r.we_lack.push_back(gap(2, "A.EX", 1, 5, 9));
        if (pass++ == 0) {
            r.we_lack.push_back(gap(3, "A.EX", 1, 5, 9));   // peer 3 catches up, peer 2 does not
        }
        return r;
    });

    f.manager.run_now();
    const auto second = f.manager.run_now();

    EXPECT_EQ(second.gaps_closed, 1u)
        << "being behind the same symbol on two peers is two facts, and one of them was fixed";
    EXPECT_EQ(second.we_lack, 1u);
}

TEST(AntiEntropy, GapsWeAreAheadOnDoNotCountAsOurRepairs) {
    Fixture f;
    int pass = 0;
    f.manager.set_reconciler([&pass] {
        ob::ReconcileReport r{};
        r.peers_contacted = 1;
        if (pass++ == 0) {
            r.peer_lacks.push_back(gap(2, "A.EX", 1, 5, 9));
        }
        return r;
    });

    f.manager.run_now();
    const auto second = f.manager.run_now();

    // The peer catching up is good news, but this node cannot verify it: it only knows what the
    // peer last told it. Counting it as a repair would inflate the metric with the other side's
    // work seen through a stale vector.
    EXPECT_EQ(second.gaps_closed, 0u);
}
