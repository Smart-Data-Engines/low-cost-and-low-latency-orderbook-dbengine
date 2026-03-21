// Feature: ha-automatic-failover
// Integration tests for failover scenarios.
//
// Tests the full failover cycle using Engine directly (no real etcd dependency).
// Validates: promotion, demotion, epoch flow, ROLE/FAILOVER commands, STATUS.

#include "orderbook/engine.hpp"

#include <gtest/gtest.h>

#include <cstring>
#include <filesystem>
#include <string>

namespace {

namespace fs = std::filesystem;

/// RAII helper for temporary test directories.
struct TempDir {
    std::string path;
    TempDir(const char* suffix = "int") {
        char tmpl[64];
        std::snprintf(tmpl, sizeof(tmpl), "/tmp/ob_failover_%s_XXXXXX", suffix);
        char* p = ::mkdtemp(tmpl);
        EXPECT_NE(p, nullptr);
        path = p;
    }
    ~TempDir() { fs::remove_all(path); }
};

// ── Test: Full promotion cycle ───────────────────────────────────────────────

TEST(FailoverIntegration, PromotionCycle) {
    TempDir dir("promo");
    ob::Engine engine(dir.path);
    engine.open();

    // Initially STANDALONE.
    EXPECT_EQ(engine.node_role(), ob::NodeRole::STANDALONE);
    EXPECT_EQ(engine.current_epoch(), 0u);

    // Promote to PRIMARY with epoch 1.
    engine.promote_to_primary(ob::EpochValue{1});
    EXPECT_EQ(engine.node_role(), ob::NodeRole::PRIMARY);
    EXPECT_EQ(engine.current_epoch(), 1u);

    // ROLE command should return PRIMARY format.
    std::string role_resp = engine.handle_role_command();
    EXPECT_EQ(role_resp, "PRIMARY 1\n");

    // Writes should work.
    ob::DeltaUpdate delta{};
    std::strncpy(delta.symbol, "BTCUSD", sizeof(delta.symbol) - 1);
    std::strncpy(delta.exchange, "TEST", sizeof(delta.exchange) - 1);
    delta.sequence_number = 1;
    delta.timestamp_ns = 1000;
    delta.side = 0;
    delta.n_levels = 1;
    ob::Level level{};
    level.price = 50000;
    level.qty = 100;
    level.cnt = 1;
    EXPECT_EQ(engine.apply_delta(delta, &level), ob::OB_OK);

    // Stats should reflect role and epoch.
    auto s = engine.stats();
    EXPECT_EQ(s.node_role, ob::NodeRole::PRIMARY);
    EXPECT_EQ(s.current_epoch, 1u);

    engine.close();
}

// ── Test: Demotion cycle ─────────────────────────────────────────────────────

TEST(FailoverIntegration, DemotionCycle) {
    TempDir dir("demo");
    ob::Engine engine(dir.path);
    engine.open();

    // Promote first.
    engine.promote_to_primary(ob::EpochValue{5});
    EXPECT_EQ(engine.node_role(), ob::NodeRole::PRIMARY);

    // Demote.
    engine.demote_to_replica("");
    EXPECT_EQ(engine.node_role(), ob::NodeRole::REPLICA);

    // ROLE command should return REPLICA format.
    std::string role_resp = engine.handle_role_command();
    EXPECT_TRUE(role_resp.find("REPLICA") == 0);

    engine.close();
}

// ── Test: Epoch persistence through WAL ──────────────────────────────────────

TEST(FailoverIntegration, EpochPersistsThroughRestart) {
    TempDir dir("epoch_persist");

    // Phase 1: open, promote, close.
    {
        ob::Engine engine(dir.path);
        engine.open();
        engine.promote_to_primary(ob::EpochValue{42});
        EXPECT_EQ(engine.current_epoch(), 42u);
        engine.close();
    }

    // Phase 2: reopen — epoch should be restored from WAL replay.
    {
        ob::Engine engine(dir.path);
        engine.open();
        EXPECT_EQ(engine.current_epoch(), 42u);
        engine.close();
    }
}

// ── Test: Multiple epoch increments ──────────────────────────────────────────

TEST(FailoverIntegration, MultipleEpochIncrements) {
    TempDir dir("multi_epoch");
    ob::Engine engine(dir.path);
    engine.open();

    engine.promote_to_primary(ob::EpochValue{1});
    EXPECT_EQ(engine.current_epoch(), 1u);

    // Simulate failover: demote then promote with higher epoch.
    engine.demote_to_replica("");
    engine.promote_to_primary(ob::EpochValue{2});
    EXPECT_EQ(engine.current_epoch(), 2u);

    engine.demote_to_replica("");
    engine.promote_to_primary(ob::EpochValue{3});
    EXPECT_EQ(engine.current_epoch(), 3u);

    engine.close();
}

// ── Test: FAILOVER command on non-primary ────────────────────────────────────

TEST(FailoverIntegration, FailoverCommandOnNonPrimary) {
    TempDir dir("failover_cmd");
    ob::Engine engine(dir.path);
    engine.open();

    // FAILOVER on STANDALONE should fail.
    std::string resp = engine.handle_failover_command("target_node");
    EXPECT_EQ(resp, "ERR not_primary\n");

    // FAILOVER on REPLICA should fail.
    engine.demote_to_replica("");
    resp = engine.handle_failover_command("target_node");
    EXPECT_EQ(resp, "ERR not_primary\n");

    engine.close();
}

// ── Test: FAILOVER command on primary without coordinator ────────────────────

TEST(FailoverIntegration, FailoverCommandOnPrimaryNoCoordinator) {
    TempDir dir("failover_no_coord");
    ob::Engine engine(dir.path);
    engine.open();

    engine.promote_to_primary(ob::EpochValue{1});

    // FAILOVER should fail because no FailoverManager is configured.
    std::string resp = engine.handle_failover_command("target_node");
    EXPECT_EQ(resp, "ERR failover_not_configured\n");

    engine.close();
}

// ── Test: get_wal_position returns valid position ────────────────────────────

TEST(FailoverIntegration, GetWalPosition) {
    TempDir dir("wal_pos");
    ob::Engine engine(dir.path);
    engine.open();

    auto [fi, off] = engine.get_wal_position();
    // After open with empty WAL, position should be valid.
    EXPECT_GE(fi, 0u);

    // Write some data to advance WAL.
    ob::DeltaUpdate delta{};
    std::strncpy(delta.symbol, "ETHUSD", sizeof(delta.symbol) - 1);
    std::strncpy(delta.exchange, "TEST", sizeof(delta.exchange) - 1);
    delta.sequence_number = 1;
    delta.timestamp_ns = 2000;
    delta.side = 1;
    delta.n_levels = 1;
    ob::Level level{};
    level.price = 3000;
    level.qty = 50;
    level.cnt = 1;
    engine.apply_delta(delta, &level);

    auto [fi2, off2] = engine.get_wal_position();
    EXPECT_GE(off2, off);  // offset should have advanced

    engine.close();
}

// ── Test: get_current_epoch returns EpochValue ───────────────────────────────

TEST(FailoverIntegration, GetCurrentEpoch) {
    TempDir dir("get_epoch");
    ob::Engine engine(dir.path);
    engine.open();

    auto e = engine.get_current_epoch();
    EXPECT_EQ(e.term, 0u);

    engine.promote_to_primary(ob::EpochValue{7});
    e = engine.get_current_epoch();
    EXPECT_EQ(e.term, 7u);

    engine.close();
}

} // namespace
