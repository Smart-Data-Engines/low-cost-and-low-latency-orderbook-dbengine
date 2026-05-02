// Feature: multi-master-replication — Wire protocol multi-master tests
// Tests cover: MM_PEERS/MM_CONFLICTS command parsing, execute_command for
//              multi-master commands, ROLE response format, STATUS [multi_master] section.
//
// Validates: Requirements 7.1, 7.2, 7.3, 7.4, 9.2

#include "orderbook/command_parser.hpp"
#include "orderbook/response_formatter.hpp"
#include "orderbook/session.hpp"
#include "orderbook/tcp_server.hpp"
#include "orderbook/engine.hpp"
#include "orderbook/data_model.hpp"
#include "orderbook/types.hpp"

#include <gtest/gtest.h>
#include <rapidcheck.h>
#include <rapidcheck/gtest.h>

#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <string>

#include <sys/socket.h>
#include <unistd.h>

namespace fs = std::filesystem;

// ═══════════════════════════════════════════════════════════════════════════════
// Helpers
// ═══════════════════════════════════════════════════════════════════════════════

static std::string make_temp_dir(const std::string& prefix) {
    auto tmp = fs::temp_directory_path() / (prefix + std::to_string(std::rand()));
    fs::create_directories(tmp);
    return tmp.string();
}

static std::pair<int, int> make_socketpair() {
    int fds[2];
    int rc_val = ::socketpair(AF_UNIX, SOCK_STREAM, 0, fds);
    if (rc_val != 0) {
        ADD_FAILURE() << "socketpair() failed";
        return {-1, -1};
    }
    return {fds[0], fds[1]};
}

// ═══════════════════════════════════════════════════════════════════════════════
// Task 13.5 — MM_PEERS / MM_CONFLICTS command parsing
// ═══════════════════════════════════════════════════════════════════════════════

TEST(CommandParserMM, ParseMMPeers) {
    ob::Command cmd = ob::parse_command("MM_PEERS");
    EXPECT_EQ(cmd.type, ob::CommandType::MM_PEERS);
}

TEST(CommandParserMM, ParseMMPeersCaseInsensitive) {
    ob::Command cmd = ob::parse_command("mm_peers");
    EXPECT_EQ(cmd.type, ob::CommandType::MM_PEERS);
}

TEST(CommandParserMM, ParseMMConflictsDefault) {
    ob::Command cmd = ob::parse_command("MM_CONFLICTS");
    EXPECT_EQ(cmd.type, ob::CommandType::MM_CONFLICTS);
    EXPECT_EQ(cmd.mm_conflicts_limit, 100u);
}

TEST(CommandParserMM, ParseMMConflictsWithLimit) {
    ob::Command cmd = ob::parse_command("MM_CONFLICTS 50");
    EXPECT_EQ(cmd.type, ob::CommandType::MM_CONFLICTS);
    EXPECT_EQ(cmd.mm_conflicts_limit, 50u);
}

TEST(CommandParserMM, ParseMMConflictsWithLargeLimit) {
    ob::Command cmd = ob::parse_command("MM_CONFLICTS 10000");
    EXPECT_EQ(cmd.type, ob::CommandType::MM_CONFLICTS);
    EXPECT_EQ(cmd.mm_conflicts_limit, 10000u);
}

TEST(CommandParserMM, ParseMMConflictsCaseInsensitive) {
    ob::Command cmd = ob::parse_command("mm_conflicts 25");
    EXPECT_EQ(cmd.type, ob::CommandType::MM_CONFLICTS);
    EXPECT_EQ(cmd.mm_conflicts_limit, 25u);
}

TEST(CommandParserMM, FormatMMPeers) {
    ob::Command cmd{};
    cmd.type = ob::CommandType::MM_PEERS;
    EXPECT_EQ(ob::format_command(cmd), "MM_PEERS\n");
}

TEST(CommandParserMM, FormatMMConflictsDefault) {
    ob::Command cmd{};
    cmd.type = ob::CommandType::MM_CONFLICTS;
    cmd.mm_conflicts_limit = 100;
    EXPECT_EQ(ob::format_command(cmd), "MM_CONFLICTS\n");
}

TEST(CommandParserMM, FormatMMConflictsCustomLimit) {
    ob::Command cmd{};
    cmd.type = ob::CommandType::MM_CONFLICTS;
    cmd.mm_conflicts_limit = 50;
    EXPECT_EQ(ob::format_command(cmd), "MM_CONFLICTS 50\n");
}

// ═══════════════════════════════════════════════════════════════════════════════
// Task 13.5 — MM_PEERS / MM_CONFLICTS on non-multi-master node → ERR
// ═══════════════════════════════════════════════════════════════════════════════

class ExecuteCommandMMTest : public ::testing::Test {
protected:
    std::string temp_dir_;
    std::unique_ptr<ob::Engine> engine_;
    ob::ServerStats stats_;
    int fd_server_ = -1;
    int fd_client_ = -1;

    void SetUp() override {
        temp_dir_ = make_temp_dir("wire_mm_test_");
        // Default engine: standalone mode (not multi-master)
        engine_ = std::make_unique<ob::Engine>(temp_dir_);
        engine_->open();

        auto [s, c] = make_socketpair();
        fd_server_ = s;
        fd_client_ = c;
    }

    void TearDown() override {
        engine_->close();
        if (fd_server_ >= 0) ::close(fd_server_);
        if (fd_client_ >= 0) ::close(fd_client_);
        fs::remove_all(temp_dir_);
    }
};

TEST_F(ExecuteCommandMMTest, MMPeersOnNonMultiMasterReturnsError) {
    ob::Session session(fd_server_);
    ob::Command cmd{};
    cmd.type = ob::CommandType::MM_PEERS;

    std::string response = ob::execute_command(cmd, *engine_, session, stats_);
    EXPECT_EQ(response, "ERR not in multi-master mode\n");
}

TEST_F(ExecuteCommandMMTest, MMConflictsOnNonMultiMasterReturnsError) {
    ob::Session session(fd_server_);
    ob::Command cmd{};
    cmd.type = ob::CommandType::MM_CONFLICTS;
    cmd.mm_conflicts_limit = 100;

    std::string response = ob::execute_command(cmd, *engine_, session, stats_);
    EXPECT_EQ(response, "ERR not in multi-master mode\n");
}

// ═══════════════════════════════════════════════════════════════════════════════
// Task 13.5 — ROLE response format for MULTI_MASTER
// Validates: Requirement 7.1
// ═══════════════════════════════════════════════════════════════════════════════

TEST_F(ExecuteCommandMMTest, RoleStandaloneFormat) {
    ob::Session session(fd_server_);
    ob::Command cmd{};
    cmd.type = ob::CommandType::ROLE;

    std::string response = ob::execute_command(cmd, *engine_, session, stats_);
    EXPECT_EQ(response, "STANDALONE\n");
}

// Test ROLE response format for MULTI_MASTER mode using handle_role_command directly.
// We create an engine with multi-master config to verify the format.
TEST(RoleResponseMM, MultiMasterFormat) {
    auto temp_dir = make_temp_dir("role_mm_test_");

    ob::MultiMasterConfig mm_config{};
    mm_config.node_id = 42;
    mm_config.replication_port = 0;  // don't actually bind
    mm_config.enabled = true;

    // Create engine with multi-master enabled but no coordinator endpoints
    // (so PeerRegistry won't try to connect to etcd)
    ob::Engine engine(temp_dir, 100'000'000ULL, ob::FsyncPolicy::INTERVAL,
                      ob::ReplicationConfig{}, ob::ReplicationClientConfig{},
                      ob::FailoverConfig{}, ob::TTLConfig{}, mm_config);
    engine.open();

    std::string response = engine.handle_role_command();

    // Should start with "MULTI_MASTER 42 "
    EXPECT_EQ(response.substr(0, std::string("MULTI_MASTER 42 ").size()),
              "MULTI_MASTER 42 ");

    // Should end with a peer count and newline
    EXPECT_EQ(response.back(), '\n');

    // Parse: MULTI_MASTER <node_id> <hlc_timestamp> <peer_count>\n
    // The HLC timestamp is in format physical_ns.logical.node_id
    // peer_count should be 0 (no peers connected)
    auto last_space = response.rfind(' ');
    ASSERT_NE(last_space, std::string::npos);
    std::string peer_count_str = response.substr(last_space + 1);
    // Remove trailing newline
    if (!peer_count_str.empty() && peer_count_str.back() == '\n') {
        peer_count_str.pop_back();
    }
    EXPECT_EQ(peer_count_str, "0");

    engine.close();
    fs::remove_all(temp_dir);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Task 13.5 — STATUS response contains [multi_master] section
// Validates: Requirement 7.2, 11.4
// ═══════════════════════════════════════════════════════════════════════════════

TEST(StatusResponseMM, ContainsMultiMasterSection) {
    ob::ServerStats stats{};
    // Simulate multi-master node
    stats.mm_node_role = 3;  // MULTI_MASTER
    stats.mm_node_id = 7;
    stats.mm_peer_count = 3;
    stats.mm_connected_peers = 2;
    stats.mm_conflicts_total = 42;
    stats.mm_anti_entropy_runs = 10;
    stats.mm_anti_entropy_repairs = 5;
    stats.mm_hlc_physical_ns = 1700000000000000000ULL;
    stats.mm_hlc_logical = 17;
    stats.mm_hlc_drift_ns = 500000;
    stats.mm_replication_lag_per_peer = {{2, 1024}, {5, 2048}};

    std::string response = ob::format_status(stats);

    // Should contain [multi_master] section header
    EXPECT_NE(response.find("[multi_master]"), std::string::npos);

    // Should contain all required fields
    EXPECT_NE(response.find("node_id: 7"), std::string::npos);
    EXPECT_NE(response.find("peer_count: 3"), std::string::npos);
    EXPECT_NE(response.find("connected_peers: 2"), std::string::npos);
    EXPECT_NE(response.find("mm_conflicts_total: 42"), std::string::npos);
    EXPECT_NE(response.find("anti_entropy_runs: 10"), std::string::npos);
    EXPECT_NE(response.find("anti_entropy_repairs: 5"), std::string::npos);
    EXPECT_NE(response.find("hlc_physical_ns: 1700000000000000000"), std::string::npos);
    EXPECT_NE(response.find("hlc_logical: 17"), std::string::npos);
    EXPECT_NE(response.find("hlc_drift_ns: 500000"), std::string::npos);
    EXPECT_NE(response.find("replication_lag_peer_2: 1024"), std::string::npos);
    EXPECT_NE(response.find("replication_lag_peer_5: 2048"), std::string::npos);
}

TEST(StatusResponseMM, NoMultiMasterSectionForStandalone) {
    ob::ServerStats stats{};
    stats.mm_node_role = 0;  // STANDALONE

    std::string response = ob::format_status(stats);

    // Should NOT contain [multi_master] section
    EXPECT_EQ(response.find("[multi_master]"), std::string::npos);
}

TEST(StatusResponseMM, NoMultiMasterSectionForPrimary) {
    ob::ServerStats stats{};
    stats.mm_node_role = 1;  // PRIMARY

    std::string response = ob::format_status(stats);

    // Should NOT contain [multi_master] section
    EXPECT_EQ(response.find("[multi_master]"), std::string::npos);
}

TEST(StatusResponseMM, NoMultiMasterSectionForReplica) {
    ob::ServerStats stats{};
    stats.mm_node_role = 2;  // REPLICA

    std::string response = ob::format_status(stats);

    // Should NOT contain [multi_master] section
    EXPECT_EQ(response.find("[multi_master]"), std::string::npos);
}

TEST(StatusResponseMM, EmptyReplicationLagList) {
    ob::ServerStats stats{};
    stats.mm_node_role = 3;  // MULTI_MASTER
    stats.mm_node_id = 1;
    // No peers → empty lag list

    std::string response = ob::format_status(stats);

    EXPECT_NE(response.find("[multi_master]"), std::string::npos);
    EXPECT_NE(response.find("node_id: 1"), std::string::npos);
    // No replication_lag_peer_ lines
    EXPECT_EQ(response.find("replication_lag_peer_"), std::string::npos);
}
