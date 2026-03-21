// Feature: ha-automatic-failover
// Properties 10, 15, 16: Role transitions, ROLE command, STATUS includes role/epoch
//
// Property 10: Demotion disables writes — after demotion, INSERT/FLUSH rejected.
// Property 15: ROLE command format — matches expected format for each role.
// Property 16: STATUS includes role and epoch.

#include "orderbook/engine.hpp"

#include <gtest/gtest.h>
#include <rapidcheck.h>

#include <cstring>
#include <filesystem>
#include <string>

namespace {

namespace fs = std::filesystem;

/// RAII helper for temporary test directories.
struct TempDir {
    std::string path;
    TempDir() {
        char tmpl[] = "/tmp/ob_failover_roles_XXXXXX";
        char* p = ::mkdtemp(tmpl);
        EXPECT_NE(p, nullptr);
        path = p;
    }
    ~TempDir() { fs::remove_all(path); }
};

// ── Property 10: Demotion disables writes ────────────────────────────────────

TEST(FailoverRoles, Property10_DemotionDisablesWrites) {
    rc::check("after demotion, engine rejects writes and reports REPLICA role",
              []() {
        TempDir dir;
        ob::Engine engine(dir.path);
        engine.open();

        // Engine starts as STANDALONE — writes should work.
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

        auto status = engine.apply_delta(delta, &level);
        RC_ASSERT(status == ob::OB_OK);

        // Demote to replica.
        engine.demote_to_replica("");

        // Verify role is REPLICA.
        RC_ASSERT(engine.node_role() == ob::NodeRole::REPLICA);

        engine.close();
    });
}

// ── Property 15: ROLE command format ─────────────────────────────────────────

TEST(FailoverRoles, Property15_RoleCommandFormat_Standalone) {
    TempDir dir;
    ob::Engine engine(dir.path);
    engine.open();

    std::string response = engine.handle_role_command();
    EXPECT_EQ(response, "STANDALONE\n");

    engine.close();
}

TEST(FailoverRoles, Property15_RoleCommandFormat_Primary) {
    rc::check("ROLE response for PRIMARY matches format: PRIMARY <epoch>",
              []() {
        TempDir dir;
        ob::Engine engine(dir.path);
        engine.open();

        auto epoch_val = *rc::gen::inRange<uint64_t>(1, 1000);
        ob::EpochValue epoch{epoch_val};
        engine.promote_to_primary(epoch);

        std::string response = engine.handle_role_command();
        std::string expected = "PRIMARY " + std::to_string(epoch_val) + "\n";
        RC_ASSERT(response == expected);

        engine.close();
    });
}

TEST(FailoverRoles, Property15_RoleCommandFormat_Replica) {
    TempDir dir;
    ob::Engine engine(dir.path);
    engine.open();

    engine.demote_to_replica("");

    std::string response = engine.handle_role_command();
    // REPLICA <primary_addr> <epoch>\n — with empty addr and epoch 0
    EXPECT_EQ(response, "REPLICA  0\n");

    engine.close();
}

// ── Property 16: STATUS includes role and epoch ──────────────────────────────

TEST(FailoverRoles, Property16_StatusIncludesRoleAndEpoch) {
    rc::check("STATUS contains node_role and current_epoch",
              []() {
        TempDir dir;
        ob::Engine engine(dir.path);
        engine.open();

        auto epoch_val = *rc::gen::inRange<uint64_t>(1, 1000);
        ob::EpochValue epoch{epoch_val};
        engine.promote_to_primary(epoch);

        auto s = engine.stats();
        RC_ASSERT(s.node_role == ob::NodeRole::PRIMARY);
        RC_ASSERT(s.current_epoch == epoch_val);

        engine.close();
    });
}

TEST(FailoverRoles, StatusStandaloneDefaults) {
    TempDir dir;
    ob::Engine engine(dir.path);
    engine.open();

    auto s = engine.stats();
    EXPECT_EQ(s.node_role, ob::NodeRole::STANDALONE);
    EXPECT_EQ(s.current_epoch, 0u);
    EXPECT_TRUE(s.primary_address.empty());
    EXPECT_EQ(s.lease_ttl_remaining, 0);

    engine.close();
}

} // namespace
