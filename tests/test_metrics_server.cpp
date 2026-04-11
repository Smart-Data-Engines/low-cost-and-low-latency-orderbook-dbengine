// Tests for MetricsServer: Property 4 (latency instrumentation) + unit/integration tests.
// Feature: observability

#include <gtest/gtest.h>
#include <rapidcheck/gtest.h>

#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include "orderbook/command_parser.hpp"
#include "orderbook/engine.hpp"
#include "orderbook/metrics.hpp"
#include "orderbook/metrics_server.hpp"
#include "orderbook/response_formatter.hpp"
#include "orderbook/session.hpp"
#include "orderbook/tcp_server.hpp"

namespace fs = std::filesystem;

// ── Helpers ──────────────────────────────────────────────────────────────────

static std::string make_temp_dir(const std::string& prefix) {
    auto tmp = fs::temp_directory_path() / (prefix + std::to_string(std::rand()));
    fs::create_directories(tmp);
    return tmp.string();
}

/// Connect a raw TCP socket to localhost:port. Returns fd or -1.
static int connect_to(uint16_t port) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(port);
    ::inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

    if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        ::close(fd);
        return -1;
    }
    return fd;
}

/// Send a string and receive the full response (up to 64KB).
static std::string http_exchange(int fd, const std::string& request) {
    ::send(fd, request.data(), request.size(), 0);

    // Give server time to respond
    std::string result;
    char buf[4096];
    while (true) {
        ssize_t n = ::recv(fd, buf, sizeof(buf), 0);
        if (n <= 0) break;
        result.append(buf, static_cast<size_t>(n));
    }
    return result;
}

/// Find a random available high port by binding to port 0.
static uint16_t find_free_port() {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return 0;

    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = 0;

    if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        ::close(fd);
        return 0;
    }

    socklen_t len = sizeof(addr);
    ::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len);
    uint16_t port = ntohs(addr.sin_port);
    ::close(fd);
    return port;
}


// ═════════════════════════════════════════════════════════════════════════════
// Property 4: Latency instrumentation — histogram count rośnie o 1
// Feature: observability, Property 4: Latency instrumentation
// Validates: Requirements 3.1, 3.2, 3.3, 3.5
//
// For any command of type INSERT, SELECT, or FLUSH executed via
// execute_command(), the corresponding latency histogram count SHALL
// increase by exactly 1.
// ═════════════════════════════════════════════════════════════════════════════

class LatencyInstrumentationFixture : public ::testing::Test {
protected:
    std::string temp_dir_;
    std::unique_ptr<ob::Engine> engine_;
    ob::MetricsRegistry registry_;
    ob::ServerStats stats_;
    int fd_server_ = -1;
    int fd_client_ = -1;

    void SetUp() override {
        temp_dir_ = make_temp_dir("latency_prop4_");
        engine_ = std::make_unique<ob::Engine>(temp_dir_);
        engine_->open();

        int fds[2];
        ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
        fd_server_ = fds[0];
        fd_client_ = fds[1];
    }

    void TearDown() override {
        engine_->close();
        if (fd_server_ >= 0) ::close(fd_server_);
        if (fd_client_ >= 0) ::close(fd_client_);
        fs::remove_all(temp_dir_);
    }
};

RC_GTEST_FIXTURE_PROP(LatencyInstrumentationFixture, prop_insert_histogram_count, ()) {
    // Generate a random INSERT command
    auto symbol = *rc::gen::nonEmpty(rc::gen::container<std::string>(
        rc::gen::oneOf(rc::gen::inRange('A', 'Z'), rc::gen::inRange('0', '9'), rc::gen::just('-'))));
    auto exchange = *rc::gen::nonEmpty(rc::gen::container<std::string>(
        rc::gen::oneOf(rc::gen::inRange('A', 'Z'), rc::gen::inRange('0', '9'))));
    auto price = *rc::gen::inRange<int64_t>(1, 1000000000LL);
    auto qty = *rc::gen::inRange<uint64_t>(1, 1000000ULL);
    auto side = *rc::gen::inRange<uint8_t>(0, 2);

    ob::Command cmd{};
    cmd.type = ob::CommandType::INSERT;
    cmd.insert_args.symbol   = symbol;
    cmd.insert_args.exchange = exchange;
    cmd.insert_args.side     = side;
    cmd.insert_args.price    = price;
    cmd.insert_args.qty      = qty;
    cmd.insert_args.count    = 1;

    const ob::HistogramData* hist = registry_.histogram_data("ob_insert_latency_seconds");
    RC_ASSERT(hist != nullptr);
    uint64_t count_before = hist->count.load(std::memory_order_relaxed);

    ob::Session session(fd_server_);
    ob::execute_command(cmd, *engine_, session, stats_, false, &registry_);

    uint64_t count_after = hist->count.load(std::memory_order_relaxed);
    RC_ASSERT(count_after == count_before + 1);
}

RC_GTEST_FIXTURE_PROP(LatencyInstrumentationFixture, prop_select_histogram_count, ()) {
    // First insert data so the SELECT query can succeed
    {
        ob::Command ins{};
        ins.type = ob::CommandType::INSERT;
        ins.insert_args.symbol   = "BTCUSD";
        ins.insert_args.exchange = "BINANCE";
        ins.insert_args.side     = 0;
        ins.insert_args.price    = 50000;
        ins.insert_args.qty      = 100;
        ins.insert_args.count    = 1;
        ob::Session s(fd_server_);
        ob::execute_command(ins, *engine_, s, stats_, false, &registry_);
    }

    ob::Command cmd{};
    cmd.type = ob::CommandType::SELECT;
    cmd.raw_sql = "SELECT * FROM 'BTCUSD'.'BINANCE' WHERE timestamp BETWEEN 0 AND 9999999999999";

    const ob::HistogramData* hist = registry_.histogram_data("ob_query_latency_seconds");
    RC_ASSERT(hist != nullptr);
    uint64_t count_before = hist->count.load(std::memory_order_relaxed);

    ob::Session session(fd_server_);
    std::string response = ob::execute_command(cmd, *engine_, session, stats_, false, &registry_);

    // The query should succeed now that data exists
    RC_ASSERT(response.find("ERR") == std::string::npos);

    uint64_t count_after = hist->count.load(std::memory_order_relaxed);
    RC_ASSERT(count_after == count_before + 1);
}

RC_GTEST_FIXTURE_PROP(LatencyInstrumentationFixture, prop_flush_histogram_count, ()) {
    ob::Command cmd{};
    cmd.type = ob::CommandType::FLUSH;

    const ob::HistogramData* hist = registry_.histogram_data("ob_flush_latency_seconds");
    RC_ASSERT(hist != nullptr);
    uint64_t count_before = hist->count.load(std::memory_order_relaxed);

    ob::Session session(fd_server_);
    ob::execute_command(cmd, *engine_, session, stats_, false, &registry_);

    uint64_t count_after = hist->count.load(std::memory_order_relaxed);
    RC_ASSERT(count_after == count_before + 1);
}


// ═════════════════════════════════════════════════════════════════════════════
// Unit Tests: CLI flags for observability
// Feature: observability
// ═════════════════════════════════════════════════════════════════════════════

TEST(MetricsServerCli, MetricsPortParsed) {
    char* argv[] = {
        const_cast<char*>("ob_tcp_server"),
        const_cast<char*>("--metrics-port"),
        const_cast<char*>("9090"),
    };
    ob::ServerConfig config = ob::parse_cli_args(3, argv);
    EXPECT_EQ(config.metrics_port, 9090);
}

TEST(MetricsServerCli, MetricsPortDefaultZero) {
    char* argv[] = { const_cast<char*>("ob_tcp_server") };
    ob::ServerConfig config = ob::parse_cli_args(1, argv);
    EXPECT_EQ(config.metrics_port, 0);
}

TEST(MetricsServerCli, LogLevelParsed) {
    char* argv[] = {
        const_cast<char*>("ob_tcp_server"),
        const_cast<char*>("--log-level"),
        const_cast<char*>("INFO"),
    };
    ob::ServerConfig config = ob::parse_cli_args(3, argv);
    EXPECT_EQ(config.log_level, "INFO");
}

// ═════════════════════════════════════════════════════════════════════════════
// Integration Tests: MetricsServer HTTP
// Feature: observability
// ═════════════════════════════════════════════════════════════════════════════

TEST(MetricsServerHttp, GetMetricsReturns200) {
    ob::MetricsRegistry registry;
    registry.increment_counter("ob_total_inserts", 7);

    uint16_t port = find_free_port();
    ASSERT_GT(port, 0);

    ob::MetricsServer server(port, registry);
    server.start();
    ASSERT_TRUE(server.is_running());

    // Give the server thread a moment to bind and listen
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    int fd = connect_to(port);
    ASSERT_GE(fd, 0) << "Failed to connect to MetricsServer on port " << port;

    std::string response = http_exchange(fd,
        "GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n");
    ::close(fd);

    server.stop();

    // Verify HTTP 200
    EXPECT_NE(response.find("HTTP/1.1 200 OK"), std::string::npos);
    // Verify Content-Type
    EXPECT_NE(response.find("text/plain; version=0.0.4; charset=utf-8"), std::string::npos);
    // Verify body contains our counter
    EXPECT_NE(response.find("ob_total_inserts"), std::string::npos);
}

TEST(MetricsServerHttp, GetOtherPathReturns404) {
    ob::MetricsRegistry registry;

    uint16_t port = find_free_port();
    ASSERT_GT(port, 0);

    ob::MetricsServer server(port, registry);
    server.start();
    ASSERT_TRUE(server.is_running());

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    int fd = connect_to(port);
    ASSERT_GE(fd, 0);

    std::string response = http_exchange(fd,
        "GET /other HTTP/1.1\r\nHost: localhost\r\n\r\n");
    ::close(fd);

    server.stop();

    EXPECT_NE(response.find("HTTP/1.1 404 Not Found"), std::string::npos);
}
