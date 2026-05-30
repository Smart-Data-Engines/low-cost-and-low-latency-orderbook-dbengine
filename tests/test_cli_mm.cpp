// Feature: multi-master-replication
// Task 15.4: CLI validation tests for multi-master arguments
//
// Tests that parse_cli_args() correctly validates multi-master CLI flags
// and produces the expected ServerConfig fields.

#include "orderbook/tcp_server.hpp"

#include <gtest/gtest.h>

#include <string>
#include <vector>

namespace {

// Helper: build argv from a vector of strings and call parse_cli_args.
ob::ServerConfig parse(std::vector<std::string>& args_storage) {
    std::vector<char*> argv;
    for (auto& s : args_storage) argv.push_back(s.data());
    return ob::parse_cli_args(static_cast<int>(argv.size()), argv.data());
}

// ── Death tests: invalid multi-master configurations ─────────────────────────

TEST(CliMultiMasterDeathTest, MissingNodeId) {
    EXPECT_EXIT(
        ([]{
            std::vector<std::string> args = {
                "ob_tcp_server",
                "--multi-master",
                "--coordinator-endpoints", "http://localhost:2379",
                "--mm-replication-port", "9100"
            };
            std::vector<char*> argv;
            for (auto& s : args) argv.push_back(s.data());
            ob::parse_cli_args(static_cast<int>(argv.size()), argv.data());
        })(),
        ::testing::ExitedWithCode(1),
        ".*--mm-node-id.*"
    );
}

TEST(CliMultiMasterDeathTest, MissingCoordinatorEndpoints) {
    EXPECT_EXIT(
        ([]{
            std::vector<std::string> args = {
                "ob_tcp_server",
                "--multi-master",
                "--mm-node-id", "1",
                "--mm-replication-port", "9100"
            };
            std::vector<char*> argv;
            for (auto& s : args) argv.push_back(s.data());
            ob::parse_cli_args(static_cast<int>(argv.size()), argv.data());
        })(),
        ::testing::ExitedWithCode(1),
        ".*--coordinator-endpoints.*"
    );
}

TEST(CliMultiMasterDeathTest, MissingReplicationPort) {
    EXPECT_EXIT(
        ([]{
            std::vector<std::string> args = {
                "ob_tcp_server",
                "--multi-master",
                "--mm-node-id", "1",
                "--coordinator-endpoints", "http://localhost:2379"
            };
            std::vector<char*> argv;
            for (auto& s : args) argv.push_back(s.data());
            ob::parse_cli_args(static_cast<int>(argv.size()), argv.data());
        })(),
        ::testing::ExitedWithCode(1),
        ".*--mm-replication-port.*"
    );
}

TEST(CliMultiMasterDeathTest, IncompatibleWithReadOnly) {
    EXPECT_EXIT(
        ([]{
            std::vector<std::string> args = {
                "ob_tcp_server",
                "--multi-master",
                "--mm-node-id", "1",
                "--coordinator-endpoints", "http://localhost:2379",
                "--mm-replication-port", "9100",
                "--read-only"
            };
            std::vector<char*> argv;
            for (auto& s : args) argv.push_back(s.data());
            ob::parse_cli_args(static_cast<int>(argv.size()), argv.data());
        })(),
        ::testing::ExitedWithCode(1),
        ".*incompatible.*read-only.*"
    );
}

TEST(CliMultiMasterDeathTest, IncompatibleWithPrimaryHost) {
    EXPECT_EXIT(
        ([]{
            std::vector<std::string> args = {
                "ob_tcp_server",
                "--multi-master",
                "--mm-node-id", "1",
                "--coordinator-endpoints", "http://localhost:2379",
                "--mm-replication-port", "9100",
                "--primary-host", "10.0.0.1"
            };
            std::vector<char*> argv;
            for (auto& s : args) argv.push_back(s.data());
            ob::parse_cli_args(static_cast<int>(argv.size()), argv.data());
        })(),
        ::testing::ExitedWithCode(1),
        ".*incompatible.*primary.*"
    );
}

// ── Valid configuration ──────────────────────────────────────────────────────

TEST(CliMultiMaster, ValidConfigPopulatesFields) {
    std::vector<std::string> args = {
        "ob_tcp_server",
        "--multi-master",
        "--mm-node-id", "42",
        "--coordinator-endpoints", "http://etcd1:2379,http://etcd2:2379",
        "--mm-replication-port", "9100",
        "--anti-entropy-interval-seconds", "60",
        "--mm-max-catchup-bytes", "1073741824"
    };

    auto config = parse(args);

    EXPECT_TRUE(config.multi_master);
    EXPECT_EQ(config.mm_node_id, 42);
    EXPECT_EQ(config.mm_replication_port, 9100);
    EXPECT_EQ(config.anti_entropy_interval_sec, 60u);
    EXPECT_EQ(config.mm_max_catchup_bytes, 1073741824u);
    ASSERT_EQ(config.coordinator_endpoints.size(), 2u);
    EXPECT_EQ(config.coordinator_endpoints[0], "http://etcd1:2379");
    EXPECT_EQ(config.coordinator_endpoints[1], "http://etcd2:2379");
}

TEST(CliMultiMaster, DefaultsWhenNoMultiMasterArgs) {
    std::vector<std::string> args = {"ob_tcp_server"};
    auto config = parse(args);

    EXPECT_FALSE(config.multi_master);
    EXPECT_EQ(config.mm_node_id, 0);
    EXPECT_EQ(config.mm_replication_port, 0);
    EXPECT_EQ(config.anti_entropy_interval_sec, 30u);
    EXPECT_EQ(config.mm_max_catchup_bytes, 512ULL << 20);
}

// ── MM Port Isolation death tests (Properties 2 & 3) ─────────────────────────
// Feature: mm-port-isolation, Property 2: CLI Port Conflict Detection
// Validates: Requirements 3.3

TEST(CliMultiMasterDeathTest, PortConflictSameMMAndReplicationPort) {
    EXPECT_EXIT(
        ([]{
            std::vector<std::string> args = {
                "ob_tcp_server",
                "--multi-master",
                "--mm-node-id", "1",
                "--coordinator-endpoints", "http://localhost:2379",
                "--mm-replication-port", "9100",
                "--replication-port", "9100"
            };
            std::vector<char*> argv;
            for (auto& s : args) argv.push_back(s.data());
            ob::parse_cli_args(static_cast<int>(argv.size()), argv.data());
        })(),
        ::testing::ExitedWithCode(1),
        ".*must be different ports.*"
    );
}

// Feature: mm-port-isolation, Property 3: Orphaned MM Port Flag Detection
// Validates: Requirements 3.4

TEST(CliMultiMasterDeathTest, OrphanedMmReplicationPortWithoutMultiMaster) {
    EXPECT_EXIT(
        ([]{
            std::vector<std::string> args = {
                "ob_tcp_server",
                "--mm-replication-port", "9100"
            };
            std::vector<char*> argv;
            for (auto& s : args) argv.push_back(s.data());
            ob::parse_cli_args(static_cast<int>(argv.size()), argv.data());
        })(),
        ::testing::ExitedWithCode(1),
        ".*requires --multi-master.*"
    );
}

// Unit test: --multi-master with --replication-port produces warning only (not fatal)
// Validates: Requirements 3.2

TEST(CliMultiMaster, ReplicationPortInMMModeParsesSuccessfully) {
    std::vector<std::string> args = {
        "ob_tcp_server",
        "--multi-master",
        "--mm-node-id", "1",
        "--coordinator-endpoints", "http://localhost:2379",
        "--mm-replication-port", "9100",
        "--replication-port", "8080"
    };

    auto config = parse(args);

    // Config parses successfully (warning only, not fatal)
    EXPECT_TRUE(config.multi_master);
    EXPECT_EQ(config.mm_replication_port, 9100);
    EXPECT_EQ(config.replication_port, 8080);
}

} // namespace
