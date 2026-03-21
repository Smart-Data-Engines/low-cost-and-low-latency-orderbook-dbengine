// Feature: ha-automatic-failover
// Property 11: CLI failover argument parsing round-trip
//
// For any valid combination of coordinator endpoints, lease TTL, node ID,
// and failover-enabled flag, parsing CLI arguments should produce a
// ServerConfig with the exact same values.

#include "orderbook/tcp_server.hpp"

#include <gtest/gtest.h>
#include <rapidcheck.h>

#include <string>
#include <vector>

namespace {

// ── Property 11: CLI failover argument parsing round-trip ────────────────────

TEST(CliFailover, Property11_ParseRoundTrip) {
    rc::check("CLI failover args round-trip through parse_cli_args",
              []() {
        // Generate 1..3 endpoints.
        auto n_endpoints = *rc::gen::inRange(1, 4);
        std::vector<std::string> endpoints;
        for (int i = 0; i < n_endpoints; ++i) {
            auto port = *rc::gen::inRange(2379, 2400);
            endpoints.push_back("http://etcd" + std::to_string(i) + ":" + std::to_string(port));
        }

        auto lease_ttl = *rc::gen::inRange<int64_t>(5, 60);
        auto node_id = "node_" + std::to_string(*rc::gen::inRange(0, 1000));
        auto failover_enabled = *rc::gen::element(true, false);

        // Build comma-separated endpoints string.
        std::string endpoints_str;
        for (size_t i = 0; i < endpoints.size(); ++i) {
            if (i > 0) endpoints_str += ',';
            endpoints_str += endpoints[i];
        }

        // Build argv.
        std::vector<std::string> args_storage = {
            "ob_tcp_server",
            "--coordinator-endpoints", endpoints_str,
            "--coordinator-lease-ttl", std::to_string(lease_ttl),
            "--node-id", node_id,
            "--failover-enabled", failover_enabled ? "true" : "false"
        };

        std::vector<char*> argv;
        for (auto& s : args_storage) argv.push_back(s.data());

        auto config = ob::parse_cli_args(static_cast<int>(argv.size()), argv.data());

        RC_ASSERT(config.coordinator_endpoints == endpoints);
        RC_ASSERT(config.coordinator_lease_ttl == lease_ttl);
        RC_ASSERT(config.node_id == node_id);
        RC_ASSERT(config.failover_enabled == failover_enabled);
    });
}

// ── Boundary tests ───────────────────────────────────────────────────────────

TEST(CliFailover, DefaultsWhenNoFailoverArgs) {
    std::vector<std::string> args_storage = {"ob_tcp_server"};
    std::vector<char*> argv;
    for (auto& s : args_storage) argv.push_back(s.data());

    auto config = ob::parse_cli_args(static_cast<int>(argv.size()), argv.data());

    EXPECT_TRUE(config.coordinator_endpoints.empty());
    EXPECT_EQ(config.coordinator_lease_ttl, 10);
    EXPECT_TRUE(config.node_id.empty());
    EXPECT_TRUE(config.failover_enabled);
}

TEST(CliFailover, SingleEndpoint) {
    std::vector<std::string> args_storage = {
        "ob_tcp_server",
        "--coordinator-endpoints", "http://localhost:2379"
    };
    std::vector<char*> argv;
    for (auto& s : args_storage) argv.push_back(s.data());

    auto config = ob::parse_cli_args(static_cast<int>(argv.size()), argv.data());

    ASSERT_EQ(config.coordinator_endpoints.size(), 1u);
    EXPECT_EQ(config.coordinator_endpoints[0], "http://localhost:2379");
}

TEST(CliFailover, MultipleEndpoints) {
    std::vector<std::string> args_storage = {
        "ob_tcp_server",
        "--coordinator-endpoints", "http://a:2379,http://b:2379,http://c:2379"
    };
    std::vector<char*> argv;
    for (auto& s : args_storage) argv.push_back(s.data());

    auto config = ob::parse_cli_args(static_cast<int>(argv.size()), argv.data());

    ASSERT_EQ(config.coordinator_endpoints.size(), 3u);
    EXPECT_EQ(config.coordinator_endpoints[0], "http://a:2379");
    EXPECT_EQ(config.coordinator_endpoints[1], "http://b:2379");
    EXPECT_EQ(config.coordinator_endpoints[2], "http://c:2379");
}

TEST(CliFailover, FailoverDisabled) {
    std::vector<std::string> args_storage = {
        "ob_tcp_server",
        "--failover-enabled", "false"
    };
    std::vector<char*> argv;
    for (auto& s : args_storage) argv.push_back(s.data());

    auto config = ob::parse_cli_args(static_cast<int>(argv.size()), argv.data());
    EXPECT_FALSE(config.failover_enabled);
}

} // namespace
