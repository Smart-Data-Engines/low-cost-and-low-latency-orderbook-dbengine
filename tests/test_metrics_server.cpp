// Tests for MetricsServer: Property 4 (latency instrumentation) + unit/integration tests.
// Feature: observability

#include <gtest/gtest.h>
#include <rapidcheck/gtest.h>

#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <system_error>
#include <sstream>
#include <fstream>
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

    // A receive deadline, because `http_exchange` below reads until EOF and the server closing its
    // end is the *thing under test* in `FiftyRequestsDoNotLeakDescriptors`. Without this, a server
    // that keeps the socket open makes that test **hang** rather than fail — measured: the leak
    // mutation ran past ten minutes instead of reporting fifty stranded descriptors. A test that
    // loses what it guards has to fail, not wait (#131).
    timeval deadline{};
    deadline.tv_sec = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &deadline, sizeof(deadline));
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

// ── The accepted descriptor, which is where #131 found a leak ────────────────
//
// `handle_request` closed the socket at each of its own two exits and on neither of the two paths
// that throw: `registry_.serialize()` builds a string of every metric and the response
// concatenation builds another, so on a box short of memory either is a `std::bad_alloc` through a
// function holding an open socket. One descriptor per request until EMFILE, and EMFILE on this
// thread is a metrics endpoint that stops answering - the failure #131 is about, arriving by a
// second road.
//
// The two tests below are what can be checked from outside and what cannot. **The throwing path
// cannot be driven**: nothing in this process can make `serialize()` fail on demand, and a knob to
// make it fail would be a knob nothing turns in production. So the behavioural test pins the paths
// a test can reach, and the static one pins the mechanism that extends the guarantee to the paths
// it cannot: a scope guard rather than a `close()` at each exit.

namespace {

/// How many descriptors this process holds. `/proc/self/fd` is the only answer that counts the
/// ones nobody is tracking, which is the point of the question.
size_t open_descriptors() {
    size_t n = 0;
    std::error_code ec;
    for (auto it = std::filesystem::directory_iterator("/proc/self/fd", ec);
         !ec && it != std::filesystem::directory_iterator(); it.increment(ec)) {
        ++n;
    }
    return n;
}

std::string read_file_text(const std::string& path) {
    std::ifstream in(path);
    std::ostringstream ss;
    ss << in.rdbuf();
    return ss.str();
}

}  // namespace

TEST(MetricsServerHttp, FiftyRequestsDoNotLeakDescriptors) {
    ob::MetricsRegistry registry;
    registry.increment_counter("ob_total_inserts", 1);

    uint16_t port = find_free_port();
    ASSERT_GT(port, 0);

    ob::MetricsServer server(port, registry);
    server.start();
    ASSERT_TRUE(server.is_running());
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    // One request first, so the count below is taken after every lazy allocation this path makes:
    // measuring from a cold server would attribute the epoll instance and the listen socket to the
    // requests.
    {
        int fd = connect_to(port);
        ASSERT_GE(fd, 0);
        (void)http_exchange(fd, "GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n");
        ::close(fd);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    const size_t before = open_descriptors();

    // One `recv` per request rather than `http_exchange`'s read-to-EOF, and that is the whole
    // reason this loop is fast. A server that keeps the socket open is exactly the defect here, so
    // waiting for it to close would make this test measure its own patience: with the leak planted
    // and a five-second client deadline it took **250 s to fail** instead of reporting fifty
    // stranded descriptors. The status line is enough to know the request was served, and the
    // descriptor count below is what knows about the leak.
    const std::string request = "GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n";
    for (int i = 0; i < 50; ++i) {
        int fd = connect_to(port);
        ASSERT_GE(fd, 0) << "connect failed on request " << i
                         << ", which is what running out of descriptors looks like from here";
        ASSERT_GT(::send(fd, request.data(), request.size(), 0), 0);
        char head[64] = {};
        const ssize_t n = ::recv(fd, head, sizeof(head) - 1, 0);
        ::close(fd);
        ASSERT_GT(n, 0) << "request " << i << " went unanswered";
        ASSERT_NE(std::string(head, static_cast<size_t>(n)).find("HTTP/1.1 200 OK"),
                  std::string::npos)
            << "request " << i << " was not answered with 200";
    }

    // The server closes its end after answering, so its descriptors have to be back where they
    // were. A leak of one per request would be fifty here.
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    const size_t after = open_descriptors();
    server.stop();

    EXPECT_LE(after, before) << "the process holds " << after << " descriptors after fifty "
                             << "requests against " << before << " before them, so the metrics "
                             << "server is keeping the sockets it answered on";
}

TEST(MetricsServerHttp, TheAcceptedDescriptorIsClosedByScopeNotByEachExit) {
    const std::string src = read_file_text(std::string(OB_SOURCE_DIR) + "/src/metrics_server.cpp");
    ASSERT_FALSE(src.empty()) << "could not read src/metrics_server.cpp; this check would pass by "
                                 "finding nothing";

    // No bare close of the accepted descriptor anywhere: every `return` that used to carry one is
    // a path the author remembered, and the two that throw are the paths nobody writes.
    EXPECT_EQ(src.find("::close(client_fd)"), std::string::npos)
        << "src/metrics_server.cpp closes the accepted descriptor explicitly. That is correct for "
           "every exit somebody wrote and wrong for the two that throw; the leak is one descriptor "
           "per failed request until EMFILE (#131)";

    // And something owns it. Anchored on the destructor rather than on a name or a comment, because
    // a check anchored on prose makes the prose load-bearing (#128).
    const std::size_t at = src.find("void MetricsServer::handle_request(int client_fd) {");
    ASSERT_NE(at, std::string::npos) << "handle_request has moved; fix this row rather than "
                                        "deleting it";
    const std::size_t end = src.find("\n}\n", at);
    ASSERT_NE(end, std::string::npos);
    const std::string body = src.substr(at, end - at);
    EXPECT_NE(body.find("~"), std::string::npos)
        << "nothing in handle_request has a destructor, so the accepted descriptor is owned by "
           "nobody and leaks on any path that throws";
}
