// TLS handshakes of the client port, off the client loops (#265).
//
// The pool is driven here over a socket pair with a real OpenSSL client at the other end: a handshake
// done is handed to its loop with an SSL the session can use; one that fails, one that never starts
// and one under way when the pool stops are closed, each with its place in --max-sessions given back.

#include "orderbook/tls_handshake_pool.hpp"

#include "orderbook/metrics.hpp"
#include "orderbook/tls.hpp"

#include <gtest/gtest.h>
#include <openssl/ssl.h>

#include <fcntl.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <filesystem>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace fs = std::filesystem;
using namespace std::chrono_literals;

namespace {

/// A certificate and its key in a directory of their own, removed with it; both empty when the openssl
/// tool could not make them.
struct KeyPair {
    fs::path    dir;
    std::string cert, key;

    explicit KeyPair(const std::string& tag)
        : dir(fs::temp_directory_path() / ("ob_tlshp_" + tag + "_" + std::to_string(::getpid()))) {
        fs::create_directories(dir);
        const std::string c = (dir / "cert.pem").string();
        const std::string k = (dir / "key.pem").string();
        const std::string cmd = "openssl req -x509 -newkey rsa:2048 -keyout '" + k + "' -out '" + c +
                                "' -days 1 -nodes -subj '/CN=localhost' >/dev/null 2>&1";
        if (std::system(cmd.c_str()) != 0) return;
        ::chmod(k.c_str(), 0600);
        cert = c;
        key  = k;
    }
    ~KeyPair() {
        std::error_code ec;
        fs::remove_all(dir, ec);
    }
    KeyPair(const KeyPair&)            = delete;
    KeyPair& operator=(const KeyPair&) = delete;
};

void set_nonblocking(int fd) { ::fcntl(fd, F_SETFL, ::fcntl(fd, F_GETFL, 0) | O_NONBLOCK); }

/// Whether the client's end reads the server's end closed: the end of the stream, or a reset where the
/// pool closed with bytes the client sent still unread, which a Unix socket pair reports as ECONNRESET.
/// Within ten seconds, so that a server's end left open fails the test rather than hanging it.
bool closed_by_server(int fd) {
    timeval tv{};
    tv.tv_sec = 10;
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    char buf[256];
    ssize_t n = 0;
    while ((n = ::recv(fd, buf, sizeof(buf), 0)) > 0) {}
    return n == 0 || (n < 0 && errno == ECONNRESET);
}

/// What the pool gave back, gathered under a lock for the test's thread to wait on.
struct Outcome {
    std::mutex                                                mtx;
    std::condition_variable                                   cv;
    std::vector<std::pair<size_t, ob::TlsHandshakePool::Done>> done;
    int                                                       given_back = 0;

    ob::TlsHandshakePool::Deliver deliver() {
        return [this](size_t loop, ob::TlsHandshakePool::Done d) {
            std::lock_guard<std::mutex> lock(mtx);
            done.emplace_back(loop, std::move(d));
            cv.notify_all();
        };
    }
    ob::TlsHandshakePool::GiveBack give_back() {
        return [this] {
            std::lock_guard<std::mutex> lock(mtx);
            ++given_back;
            cv.notify_all();
        };
    }
    /// Until `n` connections have come out of the pool, either way, or `wait` has gone.
    bool settled(size_t n, std::chrono::milliseconds wait) {
        std::unique_lock<std::mutex> lock(mtx);
        return cv.wait_for(lock, wait,
                           [&] { return done.size() + static_cast<size_t>(given_back) >= n; });
    }
};

/// The client's end of a pair, and the thread that uses it: shut down and joined however the test
/// leaves, so that a failed assertion does not leave a joinable thread behind (#260's lesson).
struct ClientEnd {
    int         fd = -1;
    std::thread thread;
    ~ClientEnd() {
        if (fd >= 0) ::shutdown(fd, SHUT_RDWR);
        if (thread.joinable()) thread.join();
        if (fd >= 0) ::close(fd);
    }
};

std::shared_ptr<ob::TlsSurface> surface_of(const KeyPair& kp) {
    return std::make_shared<ob::TlsSurface>(
        std::make_shared<const ob::TlsContext>(ob::TlsContext::server(kp.cert, kp.key)));
}

}  // namespace

TEST(TlsHandshakePool, AHandshakeIsDoneOffTheLoopAndTheSessionGivenToTheLoopItWasDealtTo) {
    const KeyPair kp("done");
    ASSERT_FALSE(kp.cert.empty()) << "openssl tool not available";
    ob::MetricsRegistry registry;
    Outcome out;
    int sv[2];
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, sv), 0);
    set_nonblocking(sv[0]);
    ClientEnd client;
    client.fd = sv[1];
    std::string got;
    const ob::TlsContext client_ctx = ob::TlsContext::client("", /*verify=*/false);
    client.thread = std::thread([&] {
        const auto ssl = client_ctx.wrap(sv[1], /*server_side=*/false);
        if (SSL_connect(ssl.get()) != 1) return;
        char buf[64];
        const int n = SSL_read(ssl.get(), buf, sizeof(buf));
        if (n > 0) got.assign(buf, static_cast<size_t>(n));
    });
    {
        ob::TlsHandshakePool pool(1, 10s, surface_of(kp), &registry, out.deliver(), out.give_back());
        pool.submit(sv[0], 42, 3);
        ASSERT_TRUE(out.settled(1, 20s)) << "the handshake never came out of the pool";
    }
    ASSERT_EQ(out.done.size(), 1u);
    EXPECT_EQ(out.given_back, 0);
    EXPECT_EQ(out.done[0].first, 3u) << "the session went to another loop than it was dealt to";
    EXPECT_EQ(out.done[0].second.fd, sv[0]);
    EXPECT_EQ(out.done[0].second.conn_id, 42u);
    // The SSL handed on is the session's: what is written through it, the client reads.
    ASSERT_EQ(SSL_write(out.done[0].second.ssl.get(), "hello", 5), 5);
    client.thread.join();
    EXPECT_EQ(got, "hello");
    EXPECT_EQ(registry.counter_value("ob_tls_handshakes_total"), 1u);
    EXPECT_EQ(registry.gauge_value("ob_tls_handshakes_in_flight"), 0);
    const auto* seconds = registry.histogram_data("ob_tls_handshake_seconds");
    ASSERT_NE(seconds, nullptr);
    EXPECT_EQ(seconds->count.load(), 1u);
    ::close(sv[0]);
}

TEST(TlsHandshakePool, AHandshakeThatFailsIsClosedAndItsPlaceGivenBack) {
    const KeyPair kp("fail");
    ASSERT_FALSE(kp.cert.empty()) << "openssl tool not available";
    ob::MetricsRegistry registry;
    Outcome out;
    int sv[2];
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, sv), 0);
    set_nonblocking(sv[0]);
    ClientEnd client;
    client.fd = sv[1];
    // Not TLS at all: the first bytes are no ClientHello.
    const char not_tls[] = "GET / HTTP/1.0\r\n\r\n";
    ASSERT_EQ(::send(sv[1], not_tls, sizeof(not_tls) - 1, 0), static_cast<ssize_t>(sizeof(not_tls) - 1));
    {
        ob::TlsHandshakePool pool(1, 10s, surface_of(kp), &registry, out.deliver(), out.give_back());
        pool.submit(sv[0], 7, 0);
        ASSERT_TRUE(out.settled(1, 20s));
    }
    EXPECT_TRUE(out.done.empty());
    EXPECT_EQ(out.given_back, 1);
    EXPECT_EQ(registry.counter_value("ob_tls_handshake_failures_total"), 1u);
    EXPECT_EQ(registry.gauge_value("ob_tls_handshakes_in_flight"), 0);
    // Closed by the pool, with the rest of the request unread: an alert, maybe, then the reset.
    EXPECT_TRUE(closed_by_server(sv[1])) << "the server's end was not closed";
}

TEST(TlsHandshakePool, AHandshakeThatNeverStartsIsClosedAtItsDeadline) {
    const KeyPair kp("late");
    ASSERT_FALSE(kp.cert.empty()) << "openssl tool not available";
    ob::MetricsRegistry registry;
    Outcome out;
    int sv[2];
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, sv), 0);
    set_nonblocking(sv[0]);
    ClientEnd client;
    client.fd = sv[1];
    const auto submitted = std::chrono::steady_clock::now();
    {
        ob::TlsHandshakePool pool(1, 200ms, surface_of(kp), &registry, out.deliver(), out.give_back());
        pool.submit(sv[0], 9, 1);
        ASSERT_TRUE(out.settled(1, 20s)) << "a client that never sent a ClientHello was never closed";
    }
    const auto waited = std::chrono::steady_clock::now() - submitted;
    EXPECT_GE(waited, 200ms) << "closed before its deadline";
    EXPECT_TRUE(out.done.empty());
    EXPECT_EQ(out.given_back, 1);
    EXPECT_EQ(registry.counter_value("ob_tls_handshake_timeouts_total"), 1u);
    EXPECT_EQ(registry.counter_value("ob_tls_handshake_failures_total"), 0u);
    EXPECT_EQ(registry.gauge_value("ob_tls_handshakes_in_flight"), 0);
    EXPECT_TRUE(closed_by_server(sv[1])) << "the server's end was not closed";
}

TEST(TlsHandshakePool, AHandshakeUnderWayWhenThePoolStopsIsClosedAndItsPlaceGivenBack) {
    const KeyPair kp("stop");
    ASSERT_FALSE(kp.cert.empty()) << "openssl tool not available";
    ob::MetricsRegistry registry;
    Outcome out;
    int sv[2];
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, sv), 0);
    set_nonblocking(sv[0]);
    ClientEnd client;
    client.fd = sv[1];
    {
        ob::TlsHandshakePool pool(1, 60s, surface_of(kp), &registry, out.deliver(), out.give_back());
        pool.submit(sv[0], 11, 2);
        // Started: the gauge says so once the thread took it.
        const auto until = std::chrono::steady_clock::now() + 10s;
        while (registry.gauge_value("ob_tls_handshakes_in_flight") == 0 &&
               std::chrono::steady_clock::now() < until) {
            std::this_thread::sleep_for(1ms);
        }
        EXPECT_EQ(registry.gauge_value("ob_tls_handshakes_in_flight"), 1);
    }
    EXPECT_TRUE(out.done.empty());
    EXPECT_EQ(out.given_back, 1) << "the pool stopped and the connection's place was not given back";
    EXPECT_EQ(registry.gauge_value("ob_tls_handshakes_in_flight"), 0);
    EXPECT_TRUE(closed_by_server(sv[1])) << "the server's end was not closed";
}

TEST(TlsHandshakePool, ConnectionsOnSeveralThreadsEachReachTheLoopTheyWereDealtTo) {
    const KeyPair kp("many");
    ASSERT_FALSE(kp.cert.empty()) << "openssl tool not available";
    ob::MetricsRegistry registry;
    Outcome out;
    constexpr int kConnections = 6;
    const ob::TlsContext client_ctx = ob::TlsContext::client("", /*verify=*/false);
    std::vector<int> server_fds;
    std::vector<ClientEnd> clients(kConnections);
    for (int i = 0; i < kConnections; ++i) {
        int sv[2];
        ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, sv), 0);
        set_nonblocking(sv[0]);
        server_fds.push_back(sv[0]);
        clients[i].fd     = sv[1];
        clients[i].thread = std::thread([&client_ctx, fd = sv[1]] {
            const auto ssl = client_ctx.wrap(fd, /*server_side=*/false);
            (void)SSL_connect(ssl.get());
        });
    }
    {
        ob::TlsHandshakePool pool(2, 10s, surface_of(kp), &registry, out.deliver(), out.give_back());
        for (int i = 0; i < kConnections; ++i) {
            pool.submit(server_fds[i], 100 + static_cast<uint64_t>(i), static_cast<size_t>(i % 3));
        }
        ASSERT_TRUE(out.settled(kConnections, 30s));
    }
    EXPECT_EQ(out.given_back, 0);
    ASSERT_EQ(out.done.size(), static_cast<size_t>(kConnections));
    for (const auto& [loop, d] : out.done) {
        const auto at = std::find(server_fds.begin(), server_fds.end(), d.fd);
        ASSERT_NE(at, server_fds.end()) << "a descriptor came out of the pool that went into it nowhere";
        const auto i = static_cast<size_t>(at - server_fds.begin());
        EXPECT_EQ(d.conn_id, 100 + i) << "fd=" << d.fd << " came out under another connection's id";
        EXPECT_EQ(loop, i % 3) << "fd=" << d.fd << " went to another loop than it was dealt to";
    }
    EXPECT_EQ(registry.counter_value("ob_tls_handshakes_total"), static_cast<uint64_t>(kConnections));
    for (int fd : server_fds) ::close(fd);
}
