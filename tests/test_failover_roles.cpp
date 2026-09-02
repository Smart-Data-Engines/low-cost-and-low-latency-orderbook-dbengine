// Feature: ha-automatic-failover
// Properties 10, 15, 16: Role transitions, ROLE command, STATUS includes role/epoch
//
// Property 10: Demotion disables writes — after demotion, INSERT/FLUSH rejected.
// Property 15: ROLE command format — matches expected format for each role.
// Property 16: STATUS includes role and epoch.

#include "orderbook/engine.hpp"

#include <gtest/gtest.h>
#include <rapidcheck.h>

#include <atomic>
#include <cstring>
#include <filesystem>
#include <string>
#include <thread>

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

// ═══════════════════════════════════════════════════════════════════════════════
// Feature: replica-read-only-fix
// Property 1: Bug Condition — Write Accepted After Demotion to Replica
//
// Bug condition C(X): command.type IN {INSERT, MINSERT, FLUSH}
//                     AND node_role == REPLICA AND config_read_only == false
// Expected behavior after fix: execute_command() returns ERR containing "read-only"
//
// On UNFIXED code this test MUST FAIL (returns OK instead of ERR).
// ═══════════════════════════════════════════════════════════════════════════════

#include "orderbook/command_parser.hpp"
#include "orderbook/response_formatter.hpp"
#include "orderbook/session.hpp"
#include "orderbook/tcp_server.hpp"

TEST(FailoverRoles, prop_write_rejected_after_demotion) {
    rc::check("write commands rejected after demotion to REPLICA",
              []() {
        TempDir dir;
        ob::Engine engine(dir.path);
        engine.open();

        // Promote to PRIMARY first (so we have a valid state to demote from)
        ob::EpochValue epoch{1};
        engine.promote_to_primary(epoch);
        RC_ASSERT(engine.node_role() == ob::NodeRole::PRIMARY);

        // Demote to REPLICA
        engine.demote_to_replica("");
        RC_ASSERT(engine.node_role() == ob::NodeRole::REPLICA);

        // Create a write command — randomly choose INSERT or FLUSH
        auto cmd_choice = *rc::gen::inRange(0, 2);
        ob::Command cmd{};
        if (cmd_choice == 0) {
            cmd.type = ob::CommandType::INSERT;
            cmd.insert_args.symbol = "TEST";
            cmd.insert_args.exchange = "EX";
            cmd.insert_args.side = 0;
            cmd.insert_args.price = 100;
            cmd.insert_args.qty = 10;
            cmd.insert_args.count = 1;
        } else {
            cmd.type = ob::CommandType::FLUSH;
        }

        // Create session and stats for execute_command
        ob::Session session(/*fd=*/-1);
        ob::ServerStats stats;

        // Call execute_command with read_only=false (simulating static config)
        std::string response = ob::execute_command(
            cmd, engine, session, stats,
            /*read_only=*/false  // BUG: this is the static config flag, not dynamic
        );

        // Expected behavior after fix: response should contain ERR and "read-only"
        RC_ASSERT(response.find("ERR") != std::string::npos);
        RC_ASSERT(response.find("read-only") != std::string::npos);

        engine.close();
    });
}

// ── Concurrent demotion ───────────────────────────────────────────────────────
//
// The other half of #88, and it needed its own test: the fix was in two places and only one of them
// had one. `ReplicationManager::stop()` is covered by
// `ReplicationProtocolTest.ConcurrentStopsJoinTheThreadExactlyOnce`; this covers
// `Engine::demote_to_replica()`, which used to read `repl_mgr_`, release `mtx_` to stop it, relock
// and reset — a window two demotions both entered, the second working on an object the first was
// destroying.
//
// Two demotions is what a graceful handover produces: the outgoing primary revokes its own lease,
// so #82's unconditional lease-lost demotion runs alongside the handover's own.
TEST(FailoverRoles, ConcurrentDemotionsLeaveOneNodeAndNoWreckage) {
    TempDir dir;

    // A replication port, because `promote_to_primary()` only builds a ReplicationManager when one
    // is configured — and without a manager there is nothing for two callers to fight over, so the
    // test would pass against the defect it guards.
    //
    // A counter, not a bind-to-zero-and-close: that idiom hands out a number the caller then races
    // to bind, and it is what made a CI run fail with `bind() failed: Address already in use` (the
    // reason `test_mm_stats.py` uses one shared allocator). A distinct base keeps this out of the
    // range `test_replication.cpp` allocates from.
    static std::atomic<uint16_t> next_port{21987};
    ob::ReplicationConfig repl{};
    repl.port = next_port.fetch_add(1, std::memory_order_relaxed);
    repl.max_replicas = 4;

    ob::Engine engine(dir.path, 100'000'000ULL, ob::FsyncPolicy::INTERVAL, repl);
    engine.open();
    engine.promote_to_primary(ob::EpochValue{7});
    ASSERT_EQ(engine.node_role(), ob::NodeRole::PRIMARY);

    std::atomic<int> ready{0};
    auto demote = [&] {
        ready.fetch_add(1, std::memory_order_release);
        while (ready.load(std::memory_order_acquire) < 2) { /* align the two callers */ }
        engine.demote_to_replica("");
    };

    std::thread first(demote);
    std::thread second(demote);
    first.join();
    second.join();

    // The properties this test is actually about: the node is alive, it settled on one role, and
    // nothing was left half-destroyed for `close()` to trip over. Reaching this line at all is the
    // main assertion — the defect aborted the process.
    //
    // Deliberately not asserting the exact `ROLE` string: after a promotion the epoch is 7 rather
    // than 0, and pinning an incidental detail here would make a failure point at the wrong thing.
    EXPECT_EQ(engine.node_role(), ob::NodeRole::REPLICA);
    EXPECT_EQ(engine.handle_role_command().rfind("REPLICA", 0), 0u);
    engine.close();
}
