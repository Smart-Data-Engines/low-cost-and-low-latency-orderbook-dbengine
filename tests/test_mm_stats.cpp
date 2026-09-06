// Engine::stats() on a multi-master node.
//
// This crossing had no test: the multi-master integration modules exercise INSERT, SELECT, ROLE
// and MM_PEERS but never STATUS, and the metrics module runs STATUS and /metrics against the
// plain cluster fixture with multi-master off. Each path was covered on its own, so a null
// dereference lived at their intersection — mm_mgr_->anti_entropy() returns *anti_entropy_, and
// nothing ever constructed it. A single STATUS command killed the node with SIGSEGV.

#include "orderbook/engine.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <cstring>
#include <fstream>
#include <sstream>
#include <string>
#include <thread>

namespace fs = std::filesystem;

namespace {

static std::atomic<uint64_t> g_dir_counter{0};
static std::atomic<uint16_t> g_port{55100};

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

constexpr uint64_t kNoAutoFlush = 3'600'000'000'000ULL;

ob::MultiMasterConfig mm_config(uint16_t node_id) {
    ob::MultiMasterConfig mm{};
    mm.enabled                   = true;
    mm.node_id                   = node_id;
    mm.replication_port          = g_port.fetch_add(1, std::memory_order_relaxed);
    mm.compress                  = false;
    mm.max_catchup_bytes         = 1 << 20;
    mm.anti_entropy_interval_sec = 3600;
    return mm;
}

}  // namespace

TEST(MultiMasterStats, StatsOnAMultiMasterNodeDoesNotCrash) {
    TempDir tmp("mm_stats_");
    ob::Engine engine(tmp.path, kNoAutoFlush, ob::FsyncPolicy::EVERY, {}, {}, {}, {},
                      mm_config(1));
    engine.open();

    // The crash was here, before the anti-entropy manager was ever constructed and while the
    // accessor handed out a reference to it regardless.
    const auto stats = engine.stats();

    EXPECT_EQ(stats.mm_node_id, 1u);
    engine.close();
}

TEST(MultiMasterStats, RepeatedStatsCallsStaySafe) {
    TempDir tmp("mm_stats_repeat_");
    ob::Engine engine(tmp.path, kNoAutoFlush, ob::FsyncPolicy::EVERY, {}, {}, {}, {},
                      mm_config(2));
    engine.open();

    // A monitoring scrape asks repeatedly, and the anti-entropy counters are read every time.
    for (int i = 0; i < 5; ++i) {
        const auto stats = engine.stats();
        EXPECT_EQ(stats.mm_node_id, 2u);
    }
    engine.close();
}

TEST(MultiMasterStats, WithoutAPeerRegistryThereIsNoSchedulerAndThatIsNotACrash) {
    TempDir tmp("mm_stats_ae_");
    auto mm = mm_config(3);
    mm.anti_entropy_interval_sec = 1;

    ob::Engine engine(tmp.path, kNoAutoFlush, ob::FsyncPolicy::EVERY, {}, {}, {}, {}, mm);
    engine.open();

    // No coordinator endpoints here, so there is no peer registry and nothing to reconcile
    // against: the scheduler is not started. What matters is that asking for its counters is
    // answered rather than fatal, and that a zero here means "no scheduler" instead of
    // "checked, nothing to repair" — that ambiguity is what kept roadmap #57 looking finished.
    std::this_thread::sleep_for(std::chrono::milliseconds(1500));
    const auto stats = engine.stats();
    EXPECT_EQ(stats.mm_anti_entropy_runs, 0u);
    EXPECT_EQ(stats.mm_anti_entropy_repairs, 0u);

    engine.close();
}

// That the scheduler does run when there *is* a registry needs etcd, so it is asserted in
// tests/integration/test_mm_stats.py against a real node.

// ── The two mesh peer gauges come from one place ─────────────────────────────

TEST(MultiMasterStatsStatic, BothMeshPeerGaugesArePublishedFromOneFunction) {
    // `ob_mm_peers_connected` was recomputed inline at three sites - connect_to_peer(),
    // disconnect_peer() and the reconnect loop - and none of them is the accept path, so a node
    // that *accepted* a connection never counted the peer. Measured on a three-node TLS mesh:
    // ob_mm_peers_tls_verified 2 against ob_mm_peers_connected 1, which an operator reads as one
    // peer talking plaintext. Three copies of a count is how the fourth site comes to be missing.
    //
    // The behavioural test is the integration one that scrapes both gauges off a live mesh; this
    // one exists to stop the copies coming back, because a fourth inline loop would pass it.
    std::ifstream in(std::string(OB_SOURCE_DIR) + "/src/multi_master.cpp");
    ASSERT_TRUE(in) << "cannot read src/multi_master.cpp";
    std::stringstream ss;
    ss << in.rdbuf();
    const std::string src = ss.str();

    for (const char* gauge : {"set_gauge(\"ob_mm_peers_connected\"",
                              "set_gauge(\"ob_mm_peers_tls_verified\""}) {
        size_t writes = 0;
        for (size_t at = src.find(gauge); at != std::string::npos; at = src.find(gauge, at + 1)) {
            ++writes;
            // Every write has to sit inside publish_peer_gauges(): the member function whose
            // definition is the last one to open before it.
            const size_t owner = src.rfind("MultiMasterManager::", at);
            ASSERT_NE(owner, std::string::npos) << gauge << " is written outside any member";
            EXPECT_EQ(src.compare(owner, std::strlen("MultiMasterManager::publish_peer_gauges"),
                                  "MultiMasterManager::publish_peer_gauges"), 0)
                << gauge << " is written by " << src.substr(owner, 60)
                << ", not by publish_peer_gauges(), so the two gauges can disagree depending on "
                   "which path moved a peer's state";
        }
        EXPECT_EQ(writes, 1u)
            << gauge << " is written at " << writes << " sites; one site per gauge is what makes "
                        "the count independent of which path changed the peer";
    }
}
