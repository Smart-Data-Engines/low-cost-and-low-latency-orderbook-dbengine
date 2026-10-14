// The server's Unix socket (#52), served by a real `TcpServer` in this process: the protocol through
// it, AUTH, TLS that stays on the port, one session limit for both transports, and the file -
// refused when it is somebody else's, removed when the server stops.
//
// In process rather than through the binary, so a sanitizer build runs the accept path, the hand-off
// to another loop and the drain of both listeners. The connections speak the line protocol by hand:
// the client library's header and the server's define the same names in `ob`, so one file cannot
// include both. `tests/integration/test_unix_socket.py` covers the binary with the clients: Python,
// `ob_backup`, SIGTERM and the metric.

#include "orderbook/auth.hpp"
#include "orderbook/net.hpp"
#include "orderbook/ring.hpp"
#include "orderbook/tcp_server.hpp"
#include "test_ports.hpp"

#include <gtest/gtest.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <csignal>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace fs = std::filesystem;

namespace {

/// As `ob_tcp_server`'s main does (#59): a server in this process writes to peers that may have gone,
/// its TLS writes through OpenSSL without MSG_NOSIGNAL, and the signal's default action would end the
/// test binary instead of failing the one test that met it.
[[maybe_unused]] const bool kSigpipeIgnored = [] {
    std::signal(SIGPIPE, SIG_IGN);
    return true;
}();

/// A directory of its own for each test: the servers' data, and the socket. Under the temp
/// directory like `NetUnix`'s, whose path a socket's address has room for.
struct Dir {
    fs::path path;
    Dir() {
        path = fs::temp_directory_path() / ("ob_uds_" + std::to_string(::getpid()));
        fs::create_directories(path);
    }
    ~Dir() {
        std::error_code ec;
        fs::remove_all(path, ec);
    }
    std::string at(const char* name) const { return (path / name).string(); }
};

/// A port of this file's block. Each test is its own process, so the offsets are given rather than
/// counted.
uint16_t port_at(uint16_t offset) {
    return static_cast<uint16_t>(ob::test::kPortsUnixSocket + offset);
}

/// The rings' tests have a block of their own: with the socket's they would not fit in one.
uint16_t ring_port_at(uint16_t offset) {
    return static_cast<uint16_t>(ob::test::kPortsRings + offset);
}

/// A server listening on `port` and on `dir`'s socket, two loops so connections are handed off as
/// well as adopted (requirement 1.3), and a short drain: every test closes its connections first.
ob::ServerConfig config_in(const Dir& dir, uint16_t port) {
    ob::ServerConfig c;
    c.port             = port;
    c.bind             = "127.0.0.1";
    c.data_dir         = dir.at("data");
    c.unix_socket      = dir.at("ob.sock");
    c.io_threads       = 2;
    c.drain_timeout_ms = 2000;
    return c;
}

/// A `TcpServer` running on a thread of its own, stopped and joined when this goes.
class Running {
public:
    explicit Running(ob::ServerConfig config)
        : server_(std::make_unique<ob::TcpServer>(std::move(config))) {
        thread_ = std::thread([this] {
            try {
                server_->run();
            } catch (const std::exception& e) {
                std::lock_guard<std::mutex> lock(mtx_);
                error_ = e.what();
            }
            returned_.store(true);
        });
    }
    ~Running() { stop(); }
    Running(const Running&) = delete;
    Running& operator=(const Running&) = delete;

    void stop() {
        if (!thread_.joinable()) return;
        server_->shutdown();
        thread_.join();
    }

    /// What run() threw, for a server expected to refuse its start.
    std::string refusal() {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (!returned_.load() && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        if (!returned_.load()) return "(run() is still running)";
        thread_.join();
        std::lock_guard<std::mutex> lock(mtx_);
        return error_;
    }

private:
    std::unique_ptr<ob::TcpServer> server_;
    std::thread                    thread_;
    std::atomic<bool>              returned_{false};
    std::mutex                     mtx_;
    std::string                    error_;
};

/// One connection speaking the line protocol by hand.
class Wire {
public:
    explicit Wire(int fd) : fd_(fd) {
        timeval tv{};
        tv.tv_sec = 5;
        ::setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    }
    ~Wire() { ::close(fd_); }
    Wire(const Wire&) = delete;
    Wire& operator=(const Wire&) = delete;

    int fd() const { return fd_; }

    /// What the server sends first: the banner, or a refusal.
    std::string greeting() { return answer(); }

    /// One command and its whole answer.
    std::string ask(const std::string& line) { return send_raw(line + "\n") ? answer() : "(send failed)"; }

    bool send_raw(const std::string& bytes) {
        return ::send(fd_, bytes.data(), bytes.size(), MSG_NOSIGNAL) ==
               static_cast<ssize_t>(bytes.size());
    }

    /// An answer that begins `OK` ends in a blank line; any other - `PONG`, `ERR ...` - at its first
    /// line. The client library's `recv_response()` frames them the same way.
    std::string answer() {
        for (;;) {
            const bool ok  = buf_.rfind("OK", 0) == 0;
            const auto end = ok ? buf_.find("\n\n") : buf_.find('\n');
            if (end != std::string::npos) {
                const size_t len = end + (ok ? 2 : 1);
                std::string out  = buf_.substr(0, len);
                buf_.erase(0, len);
                return out;
            }
            char tmp[4096];
            const ssize_t n = ::recv(fd_, tmp, sizeof(tmp), 0);
            if (n > 0) {
                buf_.append(tmp, static_cast<size_t>(n));
                continue;
            }
            if (n < 0 && errno == EINTR) continue;
            const std::string why = n == 0 ? "closed" : "recv: " + std::string(std::strerror(errno));
            return buf_.empty() ? "(" + why + ")" : "(" + why + " after " + std::to_string(buf_.size()) +
                                                        " bytes)";
        }
    }

private:
    int         fd_;
    std::string buf_;
};

/// A connection through `path`, once the server listens: the file exists from bind(), and a
/// connection before listen() is refused.
std::unique_ptr<Wire> through(const std::string& path) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    std::string why;
    for (;;) {
        const int fd = ob::net::dial_unix(path, 1000, false, why);
        if (fd >= 0) return std::make_unique<Wire>(fd);
        if (std::chrono::steady_clock::now() >= deadline) {
            ADD_FAILURE() << "no connection through " << path << ": " << why;
            return nullptr;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
}

/// A connection to the port, once the server listens there.
std::unique_ptr<Wire> to_port(uint16_t port) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    std::string why;
    for (;;) {
        const int fd = ob::net::dial("127.0.0.1", port, 1000, false, why, "test");
        if (fd >= 0) return std::make_unique<Wire>(fd);
        if (std::chrono::steady_clock::now() >= deadline) {
            ADD_FAILURE() << "no connection to port " << port << ": " << why;
            return nullptr;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
}

bool greeted(const std::string& greeting) { return greeting.rfind("OK ob_tcp_server v", 0) == 0; }

/// One counter from the node's `/metrics` on `port`, or -1 when it cannot be read.
long long metric(uint16_t port, const std::string& name) {
    const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (fd < 0) return -1;
    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_port        = htons(port);
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    std::string text;
    if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0) {
        const std::string get = "GET /metrics HTTP/1.0\r\n\r\n";
        if (::send(fd, get.data(), get.size(), MSG_NOSIGNAL) == static_cast<ssize_t>(get.size())) {
            char buf[65536];
            for (ssize_t n; (n = ::recv(fd, buf, sizeof(buf), 0)) > 0;) text.append(buf, static_cast<size_t>(n));
        }
    }
    ::close(fd);
    // `name{labels} value`, or `name value`: the value follows the first space after the name.
    for (size_t at = text.find("\n" + name); at != std::string::npos; at = text.find("\n" + name, at + 1)) {
        const char next = text[at + 1 + name.size()];
        if (next != '{' && next != ' ') continue;   // a longer name that starts with this one
        const size_t space = text.find(' ', at + 1);
        return space == std::string::npos ? -1 : std::atoll(text.c_str() + space + 1);
    }
    return -1;
}

/// A session moved to rings (#52, step 2), speaking the protocol through them the way the C++
/// client does: commands into one ring, answers out of the other, and a bell rung only for a side
/// that sleeps.
class RingWire {
public:
    /// Sends `RINGS` on the socket of `w` and takes the answer with its three descriptors.
    RingWire(Wire& w, uint64_t bytes = 0) : socket_fd_(w.fd()) {
        const std::string line = bytes != 0 ? "RINGS " + std::to_string(bytes) + "\n" : "RINGS\n";
        if (!w.send_raw(line)) {
            error_ = "the RINGS line was not sent";
            return;
        }
        std::vector<int> fds;
        if (!ob::net::recv_with_fds(socket_fd_, 4096, 3, 5000, answer_, fds, error_)) return;
        if (answer_.rfind("OK RINGS ", 0) != 0 || fds.size() != 3) {
            for (const int fd : fds) ::close(fd);
            error_ = "answer '" + answer_ + "' with " + std::to_string(fds.size()) + " descriptor(s)";
            return;
        }
        const uint64_t size = std::stoull(answer_.substr(9));
        server_bell_ = fds[1];
        client_bell_ = fds[2];
        mapping_ = ob::ring::Mapping::attach(fds[0], size, error_);
        if (!mapping_.valid()) return;
        commands_ = ob::ring::Writer(mapping_.to_server(), mapping_.to_server_data(), size);
        answers_  = ob::ring::Reader(mapping_.to_client(), mapping_.to_client_data(), size);
        ok_ = true;
    }
    ~RingWire() {
        if (server_bell_ >= 0) ::close(server_bell_);
        if (client_bell_ >= 0) ::close(client_bell_);
    }
    RingWire(const RingWire&) = delete;
    RingWire& operator=(const RingWire&) = delete;

    bool ok() const { return ok_; }
    const std::string& error() const { return error_; }
    const std::string& rings_answer() const { return answer_; }
    ob::ring::Mapping& mapping() { return mapping_; }
    int server_bell() const { return server_bell_; }
    /// How many times a command found the node asleep and rang it.
    uint64_t bells_rung() const { return bells_rung_; }
    /// How many times a full command ring put this client to sleep until the node made room.
    uint64_t room_waits() const { return room_waits_; }

    bool send_raw(const std::string& bytes) {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        size_t done = 0;
        while (done < bytes.size()) {
            const size_t n = commands_.write(bytes.data() + done, bytes.size() - done);
            done += n;
            if (commands_.publish()) {
                ob::ring::ring_bell(server_bell_);
                ++bells_rung_;
            }
            if (n > 0) continue;
            if (std::chrono::steady_clock::now() > deadline) return false;
            if (commands_.prepare_to_wait_for_room()) {
                ++room_waits_;
                // Long enough that a bell the node never rang shows as a failure, not as a pause.
                pollfd p{client_bell_, POLLIN, 0};
                if (::poll(&p, 1, 2000) == 0) return false;
                ob::ring::drain_bell(client_bell_);
                commands_.done_waiting_for_room();
            }
        }
        return true;
    }

    std::string ask(const std::string& line) { return send_raw(line + "\n") ? answer() : "(send failed)"; }

    /// One answer, framed as `Wire::answer()` frames one; sleeps on its bell between bytes.
    std::string answer() {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        for (;;) {
            const bool ok  = buf_.rfind("OK", 0) == 0;
            const auto end = ok ? buf_.find("\n\n") : buf_.find('\n');
            if (end != std::string::npos) {
                const size_t len = end + (ok ? 2 : 1);
                std::string out  = buf_.substr(0, len);
                buf_.erase(0, len);
                return out;
            }
            char tmp[16384];
            const size_t n = answers_.read(tmp, sizeof(tmp));
            if (n > 0) {
                buf_.append(tmp, n);
                if (answers_.producer_waiting()) ob::ring::ring_bell(server_bell_);
                continue;
            }
            if (answers_.broken()) return "(the server left the answer ring broken)";
            if (std::chrono::steady_clock::now() > deadline) {
                return "(timed out with " + std::to_string(buf_.size()) + " bytes)";
            }
            if (answers_.prepare_to_sleep()) {
                // Two seconds asleep with the node's answer due is a bell it never rang: said as
                // such, rather than covered by a poll that wakes anyway.
                pollfd p{client_bell_, POLLIN, 0};
                if (::poll(&p, 1, 2000) == 0) {
                    return "(no bell within 2 s, " + std::to_string(answers_.available()) +
                           " bytes in the ring)";
                }
                ob::ring::drain_bell(client_bell_);
                answers_.woke();
            }
        }
    }

    /// Whether the server has closed the socket the session is kept under, within `ms`.
    bool socket_closed_within(int ms) {
        pollfd p{socket_fd_, POLLIN, 0};
        if (::poll(&p, 1, ms) <= 0) return false;
        char c = 0;
        return ::recv(socket_fd_, &c, 1, MSG_DONTWAIT) == 0;
    }

private:
    int                socket_fd_;
    uint64_t           bells_rung_{0};
    uint64_t           room_waits_{0};
    bool               ok_{false};
    std::string        error_;
    std::string        answer_;
    std::string        buf_;
    ob::ring::Mapping  mapping_;
    ob::ring::Writer   commands_;
    ob::ring::Reader   answers_;
    int                server_bell_{-1};
    int                client_bell_{-1};
};

/// Whether a connection to the port is accepted at all.
bool to_port_answers(uint16_t port) {
    std::string why;
    const int fd = ob::net::dial("127.0.0.1", port, 500, false, why, "test");
    if (fd < 0) return false;
    ::close(fd);
    return true;
}

/// The client's half of `AUTH`; the server's last answer.
std::string authenticate(Wire& w, const std::string& identity, const std::string& secret) {
    const std::string challenge = w.ask("AUTH");
    const std::string prefix    = "OK CHALLENGE ";
    if (challenge.rfind(prefix, 0) != 0) return challenge;
    const std::string nonce = challenge.substr(prefix.size(), challenge.find('\n') - prefix.size());
    return w.ask("AUTH " + identity + " " +
                 ob::auth_response(secret, ob::AuthSurface::Client, ob::AuthRole::Initiator,
                                   identity, nonce));
}

/// Whether nothing listens on `port` of the loopback: a bind there succeeds.
bool port_is_free(uint16_t port) {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    int one = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port        = htons(port);
    const bool free = ::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0;
    ::close(fd);
    return free;
}

bool exists(const std::string& path) {
    struct stat st{};
    return ::lstat(path.c_str(), &st) == 0;
}

}  // namespace

TEST(UnixSocketServer, ThroughTheSocketIsTheSameProtocol) {
    Dir dir;
    const auto config = config_in(dir, port_at(0));
    Running server(config);
    auto w = through(config.unix_socket);
    ASSERT_TRUE(w);
    const std::string banner = w->greeting();
    ASSERT_TRUE(greeted(banner)) << banner;

    EXPECT_EQ(w->ask("PING"), "PONG\n");
    EXPECT_EQ(w->ask("INSERT UDS-T EX bid 10000 5"), "OK\n\n");
    EXPECT_EQ(w->ask("INSERT UDS-T EX ask 10100 7"), "OK\n\n");
    const std::string book = w->ask("BOOK UDS-T EX 5");
    EXPECT_EQ(book.rfind("OK", 0), 0u) << book;
    EXPECT_NE(book.find("10000"), std::string::npos) << book;
    EXPECT_NE(book.find("10100"), std::string::npos) << book;

    // Pipelined: two commands in one write, two answers in order.
    ASSERT_TRUE(w->send_raw("PING\nBOOK UDS-T EX 5\n"));
    EXPECT_EQ(w->answer(), "PONG\n");
    EXPECT_EQ(w->answer(), book);

    // A second connection, dealt to the other loop, reads what the first wrote.
    auto second = through(config.unix_socket);
    ASSERT_TRUE(second);
    ASSERT_TRUE(greeted(second->greeting()));
    EXPECT_EQ(second->ask("BOOK UDS-T EX 5"), book);

    w.reset();
    second.reset();
    server.stop();
}

TEST(UnixSocketServer, TheSocketAsksForAuthLikeThePort) {
    Dir dir;
    const std::string secret(40, 's');
    const std::string secrets = dir.at("clients.secret");
    {
        std::ofstream out(secrets);
        out << "ops " << secret << "\n";
    }
    ASSERT_EQ(::chmod(secrets.c_str(), 0600), 0);
    auto config             = config_in(dir, port_at(10));
    config.auth_secret_file = secrets;
    Running server(config);

    auto anonymous = through(config.unix_socket);
    ASSERT_TRUE(anonymous);
    ASSERT_TRUE(greeted(anonymous->greeting()));
    const std::string refused = anonymous->ask("BOOK UDS-A EX");
    EXPECT_EQ(refused.rfind("ERR unauthenticated", 0), 0u) << refused;

    auto ops = through(config.unix_socket);
    ASSERT_TRUE(ops);
    ASSERT_TRUE(greeted(ops->greeting()));
    const std::string accepted = authenticate(*ops, "ops", secret);
    EXPECT_EQ(accepted.rfind("OK AUTH", 0), 0u) << accepted;
    EXPECT_EQ(ops->ask("INSERT UDS-A EX bid 5000 1"), "OK\n\n");

    auto intruder = through(config.unix_socket);
    ASSERT_TRUE(intruder);
    ASSERT_TRUE(greeted(intruder->greeting()));
    const std::string denied = authenticate(*intruder, "ops", std::string(40, 'x'));
    EXPECT_EQ(denied.rfind("ERR", 0), 0u) << denied;

    anonymous.reset();
    ops.reset();
    intruder.reset();
    server.stop();
}

TEST(UnixSocketServer, TlsOnThePortLeavesTheSocketPlain) {
    Dir dir;
    const std::string cert = dir.at("cert.pem");
    const std::string key  = dir.at("key.pem");
    const std::string cmd  = "openssl req -x509 -newkey rsa:2048 -keyout '" + key + "' -out '" +
                            cert + "' -days 1 -nodes -subj '/CN=uds' "
                            "-addext 'subjectAltName=IP:127.0.0.1' >/dev/null 2>&1";
    ASSERT_EQ(std::system(cmd.c_str()), 0) << "openssl could not make a certificate";
    ASSERT_EQ(::chmod(key.c_str(), 0600), 0);

    auto config          = config_in(dir, port_at(20));
    config.tls_client    = true;
    config.tls_cert_file = cert;
    config.tls_key_file  = key;
    Running server(config);

    // Through the socket: no handshake, the banner and a PING in plain text - on the loop that
    // accepted the connection, and on the one the next is handed to.
    auto local = through(config.unix_socket);
    ASSERT_TRUE(local);
    const std::string banner = local->greeting();
    ASSERT_TRUE(greeted(banner)) << banner;
    EXPECT_EQ(local->ask("PING"), "PONG\n");
    auto handed = through(config.unix_socket);
    ASSERT_TRUE(handed);
    const std::string second = handed->greeting();
    ASSERT_TRUE(greeted(second)) << second;
    EXPECT_EQ(handed->ask("PING"), "PONG\n");

    // Through the port, the same bytes are the start of a handshake that fails: no banner, no PONG.
    auto remote = to_port(config.port);
    ASSERT_TRUE(remote);
    const std::string over_tcp = remote->ask("PING");
    EXPECT_EQ(over_tcp.find("PONG"), std::string::npos) << over_tcp;
    EXPECT_FALSE(greeted(over_tcp)) << over_tcp;

    local.reset();
    handed.reset();
    remote.reset();
    server.stop();
}

TEST(UnixSocketServer, OneSessionLimitForBothTransports) {
    Dir dir;
    auto config         = config_in(dir, port_at(30));
    config.max_sessions = 2;
    Running server(config);

    auto local = through(config.unix_socket);
    ASSERT_TRUE(local);
    ASSERT_TRUE(greeted(local->greeting()));
    auto remote = to_port(config.port);
    ASSERT_TRUE(remote);
    ASSERT_TRUE(greeted(remote->greeting()));

    // The third, through the socket, is over the limit both of them count against.
    {
        auto third = through(config.unix_socket);
        ASSERT_TRUE(third);
        EXPECT_EQ(third->greeting(), "ERR server full\n");
    }

    // One leaves, and the socket admits again once the server has seen it go.
    remote.reset();
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    std::string greeting;
    while (std::chrono::steady_clock::now() < deadline) {
        auto again = through(config.unix_socket);
        ASSERT_TRUE(again);
        greeting = again->greeting();
        if (greeted(greeting)) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    EXPECT_TRUE(greeted(greeting)) << greeting;

    local.reset();
    server.stop();
}

TEST(UnixSocketServer, ADrainClosesTheSocketWithThePort) {
    // Requirement 3.4. A session left open holds the drain, so the socket's refusal is seen while
    // the server is still running: an open listener would answer "server shutting down" instead,
    // and after run() returns the file is gone, which is a different error.
    Dir dir;
    auto config             = config_in(dir, port_at(90));
    config.drain_timeout_ms = 10000;
    Running server(config);
    auto held = through(config.unix_socket);
    ASSERT_TRUE(held);
    ASSERT_TRUE(greeted(held->greeting()));

    std::thread stopper([&] { server.stop(); });
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    std::string why;
    bool refused = false;
    while (std::chrono::steady_clock::now() < deadline) {
        const int fd = ob::net::dial_unix(config.unix_socket, 200, false, why);
        if (fd < 0 && why == std::strerror(ECONNREFUSED)) {
            refused = true;
            break;
        }
        if (fd >= 0) ::close(fd);
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    EXPECT_TRUE(refused) << "the socket took connections while the server drained; last: " << why;
    EXPECT_TRUE(exists(config.unix_socket)) << "the drain was over before the socket refused";
    EXPECT_FALSE(to_port_answers(config.port)) << "the port is still open in the drain";

    held.reset();   // the drain ends with the last session
    stopper.join();
}

TEST(UnixSocketServer, TheFileTakesItsModeAndGoesWithTheServer) {
    Dir dir;
    auto config             = config_in(dir, port_at(40));
    config.unix_socket_mode = 0600;
    {
        Running server(config);
        auto w = through(config.unix_socket);
        ASSERT_TRUE(w);
        ASSERT_TRUE(greeted(w->greeting()));
        struct stat st{};
        ASSERT_EQ(::lstat(config.unix_socket.c_str(), &st), 0);
        EXPECT_TRUE(S_ISSOCK(st.st_mode));
        EXPECT_EQ(st.st_mode & 0777, 0600u);
        w.reset();
        server.stop();
    }
    EXPECT_FALSE(exists(config.unix_socket)) << "the socket file outlived the server";
}

TEST(UnixSocketServer, AFileSomebodyElseBoundIsLeftAlone) {
    // The server removes the file at its path only while it is the file the server bound: one bound
    // there since is somebody else's.
    Dir dir;
    const auto config = config_in(dir, port_at(50));
    Running server(config);
    auto w = through(config.unix_socket);
    ASSERT_TRUE(w);
    ASSERT_TRUE(greeted(w->greeting()));
    w.reset();

    ASSERT_EQ(::unlink(config.unix_socket.c_str()), 0);
    std::string error;
    const int other = ob::net::listen_unix(config.unix_socket, 0600, 4, 0, error);
    ASSERT_GE(other, 0) << error;
    server.stop();
    EXPECT_TRUE(exists(config.unix_socket)) << "the server removed a socket it did not bind";
    ::close(other);
}

TEST(UnixSocketServer, ASecondServerOnTheSameSocketDoesNotStart) {
    Dir dir;
    const auto first_config = config_in(dir, port_at(60));
    Running first(first_config);
    auto w = through(first_config.unix_socket);
    ASSERT_TRUE(w);
    ASSERT_TRUE(greeted(w->greeting()));

    auto second_config     = config_in(dir, port_at(61));
    second_config.data_dir = dir.at("data-2");
    Running second(second_config);
    const std::string refusal = second.refusal();
    EXPECT_NE(refusal.find("another process listens on it"), std::string::npos) << refusal;
    // The port it bound before the socket refused it is given back at once, not at exit.
    EXPECT_TRUE(port_is_free(second_config.port)) << "the refused server still holds its port";

    // The first goes on serving, through the file the second did not touch.
    EXPECT_EQ(w->ask("PING"), "PONG\n");
    w.reset();
    first.stop();
}

TEST(UnixSocketServer, AFileThatIsNotASocketStopsTheStartAndIsKept) {
    Dir dir;
    const auto config = config_in(dir, port_at(70));
    {
        std::ofstream out(config.unix_socket);
        out << "somebody's notes\n";
    }
    Running server(config);
    const std::string refusal = server.refusal();
    EXPECT_NE(refusal.find("the path exists and is not a socket"), std::string::npos) << refusal;
    std::ifstream in(config.unix_socket);
    std::string kept;
    std::getline(in, kept);
    EXPECT_EQ(kept, "somebody's notes");
}

TEST(UnixSocketServer, WhatAKilledServerLeftIsReplaced) {
    Dir dir;
    const auto config = config_in(dir, port_at(80));
    {
        // A socket bound and never listened on: what a process killed between the two leaves, and
        // what refuses a connection as a dead server's socket does.
        const int fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
        ASSERT_GE(fd, 0);
        sockaddr_un addr{};
        addr.sun_family = AF_UNIX;
        std::snprintf(addr.sun_path, sizeof(addr.sun_path), "%s", config.unix_socket.c_str());
        ASSERT_EQ(::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)), 0);
        ::close(fd);
    }
    ASSERT_TRUE(exists(config.unix_socket));
    Running server(config);
    auto w = through(config.unix_socket);
    ASSERT_TRUE(w);
    ASSERT_TRUE(greeted(w->greeting()));
    EXPECT_EQ(w->ask("PING"), "PONG\n");
    w.reset();
    server.stop();
}

// ── Rings in shared memory (#52, step 2) ────────────────────────────────────────────────────────────

namespace {

/// The rings' tests run each twice: with a loop that sleeps between events, so every command rings
/// its bell, and with one that polls, as `boost` does, so none needs to.
ob::ServerConfig ring_config(const Dir& dir, uint16_t port, bool polling) {
    auto c = config_in(dir, port);
    c.io_spin_us = polling ? 50 : 0;
    return c;
}

void the_protocol_goes_through_rings(uint16_t port, bool polling) {
    Dir dir;
    const auto config = ring_config(dir, port, polling);
    Running server(config);
    auto w = through(config.unix_socket);
    ASSERT_TRUE(w);
    ASSERT_TRUE(greeted(w->greeting()));
    RingWire r(*w);
    ASSERT_TRUE(r.ok()) << r.error();
    EXPECT_EQ(r.rings_answer(), "OK RINGS 1048576\n\n");

    EXPECT_EQ(r.ask("PING"), "PONG\n");
    EXPECT_EQ(r.ask("INSERT RING EX bid 10000 5"), "OK\n\n");
    EXPECT_EQ(r.ask("INSERT RING EX ask 10100 7"), "OK\n\n");
    const std::string book = r.ask("BOOK RING EX 5");
    EXPECT_EQ(book.rfind("OK", 0), 0u) << book;
    EXPECT_NE(book.find("10100"), std::string::npos) << book;
    // Pipelined, and again after a pause long enough for the loop to have gone to sleep.
    ASSERT_TRUE(r.send_raw("PING\nBOOK RING EX 5\n"));
    EXPECT_EQ(r.answer(), "PONG\n");
    EXPECT_EQ(r.answer(), book);
    std::this_thread::sleep_for(std::chrono::milliseconds(250));
    // A loop that went to sleep is woken by the command, not by its own 100 ms timeout.
    const auto t0 = std::chrono::steady_clock::now();
    EXPECT_EQ(r.ask("PING"), "PONG\n");
    EXPECT_LT(std::chrono::steady_clock::now() - t0, std::chrono::milliseconds(50))
        << "the PING waited for the loop's timeout rather than waking it";

    // The socket the session is kept under carries nothing more, and the session is still there.
    EXPECT_FALSE(r.socket_closed_within(50));
    w.reset();   // its end is the session's end
    server.stop();
}

}  // namespace

TEST(RingsServer, TheProtocolGoesThroughRingsToALoopThatSleeps) {
    the_protocol_goes_through_rings(ring_port_at(0), false);
}

TEST(RingsServer, TheProtocolGoesThroughRingsToALoopThatPolls) {
    the_protocol_goes_through_rings(ring_port_at(10), true);
}

TEST(RingsServer, ALoopThatPollsFindsACommandNobodyRang) {
    // A client rings only a loop that said it sleeps, so a loop that polls has to find each command
    // itself. The test above cannot tell: its client sleeps until each answer, and waking it takes
    // longer than the loop's spin, so every command there finds the loop asleep and rings. Here the
    // spin outlasts the test, the loop never sleeps, and no command may ring.
    Dir dir;
    auto config       = ring_config(dir, ring_port_at(90), true);
    config.io_spin_us = 30'000'000;
    Running server(config);
    auto w = through(config.unix_socket);
    ASSERT_TRUE(w);
    ASSERT_TRUE(greeted(w->greeting()));
    RingWire r(*w);
    ASSERT_TRUE(r.ok()) << r.error();
    for (int i = 0; i < 100; ++i) {
        ASSERT_EQ(r.ask("PING"), "PONG\n") << "command " << i;
    }
    EXPECT_EQ(r.bells_rung(), 0u) << "the loop slept, so the commands were not found by polling";
    w.reset();
    server.stop();
}

TEST(RingsServer, OnlyASessionThroughTheSocketMovesAndOnlyWhenAsked) {
    Dir dir;
    const std::string secret(40, 's');
    const std::string secrets = dir.at("clients.secret");
    {
        std::ofstream out(secrets);
        out << "ops " << secret << "\n";
    }
    ASSERT_EQ(::chmod(secrets.c_str(), 0600), 0);
    auto config             = config_in(dir, ring_port_at(20));
    config.auth_secret_file = secrets;
    config.ring_bytes       = 65536 * 4;
    Running server(config);

    auto remote = to_port(config.port);
    ASSERT_TRUE(remote);
    ASSERT_TRUE(greeted(remote->greeting()));
    ASSERT_EQ(authenticate(*remote, "ops", secret).rfind("OK AUTH", 0), 0u);
    EXPECT_EQ(remote->ask("RINGS"),
              "ERR rings: only a session through the node's Unix socket can move to rings\n");

    auto local = through(config.unix_socket);
    ASSERT_TRUE(local);
    ASSERT_TRUE(greeted(local->greeting()));
    EXPECT_EQ(local->ask("RINGS"), "ERR unauthenticated\n");
    ASSERT_EQ(authenticate(*local, "ops", secret).rfind("OK AUTH", 0), 0u);
    EXPECT_EQ(local->ask("RINGS 100000").rfind("ERR rings: 100000 bytes a direction", 0), 0u);
    EXPECT_EQ(local->ask("RINGS 524288").rfind("ERR rings: 524288 bytes a direction", 0), 0u)
        << "more than --ring-bytes was granted";
    EXPECT_EQ(local->ask("PING"), "PONG\n") << "a refused RINGS ended the session";

    // Nothing may follow RINGS on the socket before its answer.
    ASSERT_TRUE(local->send_raw("RINGS\nPING\n"));
    EXPECT_EQ(local->answer(), "ERR rings: nothing may follow RINGS before its answer\n");

    auto granted = through(config.unix_socket);
    ASSERT_TRUE(granted);
    ASSERT_TRUE(greeted(granted->greeting()));
    ASSERT_EQ(authenticate(*granted, "ops", secret).rfind("OK AUTH", 0), 0u);
    RingWire r(*granted, 65536);
    ASSERT_TRUE(r.ok()) << r.error();
    EXPECT_EQ(r.rings_answer(), "OK RINGS 65536\n\n");
    EXPECT_EQ(r.ask("RINGS"), "ERR rings: this session is in rings already\n");

    remote.reset();
    local.reset();
    granted.reset();
    server.stop();
}

TEST(RingsServer, ASwitchedOffNodeRefusesToMove) {
    Dir dir;
    auto config  = config_in(dir, ring_port_at(30));
    config.rings = false;
    Running server(config);
    auto w = through(config.unix_socket);
    ASSERT_TRUE(w);
    ASSERT_TRUE(greeted(w->greeting()));
    EXPECT_EQ(w->ask("RINGS"), "ERR rings: switched off on this node (--rings off)\n");
    w.reset();
    server.stop();
}

TEST(RingsServer, PushesComeThroughTheRing) {
    Dir dir;
    const auto config = ring_config(dir, ring_port_at(40), false);
    Running server(config);
    auto w = through(config.unix_socket);
    ASSERT_TRUE(w);
    ASSERT_TRUE(greeted(w->greeting()));
    RingWire r(*w);
    ASSERT_TRUE(r.ok()) << r.error();
    const std::string subscribed = r.ask("SUBSCRIBE * FROM 'PUSHED'.'EX'");
    ASSERT_EQ(subscribed.rfind("OK", 0), 0u) << subscribed;

    auto writer = to_port(config.port);
    ASSERT_TRUE(writer);
    ASSERT_TRUE(greeted(writer->greeting()));
    EXPECT_EQ(writer->ask("INSERT PUSHED EX bid 4200 3"), "OK\n\n");
    const std::string push = r.answer();
    EXPECT_EQ(push.rfind("PUSH ", 0), 0u) << push;
    EXPECT_NE(push.find("4200"), std::string::npos) << push;

    writer.reset();
    w.reset();
    server.stop();
}

TEST(RingsServer, AnAnswerLargerThanTheRingWaitsForTheReader) {
    // 64 KiB a direction and an answer over twice that: the loop fills the ring, says it waits for
    // room, and the client's reads ring it back.
    Dir dir;
    const auto config = ring_config(dir, ring_port_at(50), false);
    Running server(config);
    auto writer = to_port(config.port);
    ASSERT_TRUE(writer);
    ASSERT_TRUE(greeted(writer->greeting()));
    std::string minsert = "MINSERT BIG EX bid 1000\n";
    for (int i = 0; i < 1000; ++i) minsert += std::to_string(100000 - i) + " " + std::to_string(i + 1) + " 1\n";
    for (int round = 0; round < 4; ++round) {
        ASSERT_TRUE(writer->send_raw(minsert));
        ASSERT_EQ(writer->answer(), "OK\n\n");
    }
    ASSERT_EQ(writer->ask("FLUSH"), "OK\n\n");

    auto w = through(config.unix_socket);
    ASSERT_TRUE(w);
    ASSERT_TRUE(greeted(w->greeting()));
    RingWire r(*w, 65536);
    ASSERT_TRUE(r.ok()) << r.error();
    ASSERT_TRUE(r.send_raw("SELECT * FROM 'BIG'.'EX' WHERE timestamp BETWEEN 0 AND 9999999999999999999\n"));
    std::this_thread::sleep_for(std::chrono::milliseconds(100));   // the loop fills the ring first
    const std::string rows = r.answer();
    ASSERT_EQ(rows.rfind("OK", 0), 0u) << rows.substr(0, 200);
    EXPECT_GT(rows.size(), 2u * 65536) << "the answer was not more than twice the ring";
    EXPECT_EQ(static_cast<size_t>(std::count(rows.begin(), rows.end(), '\n')), 4000u + 3)
        << "rows went missing on the way through the ring";

    writer.reset();
    w.reset();
    server.stop();
}

TEST(RingsServer, ACounterTheClientCouldNotHaveLeftEndsTheSession) {
    Dir dir;
    const auto config = ring_config(dir, ring_port_at(60), false);
    Running server(config);
    auto w = through(config.unix_socket);
    ASSERT_TRUE(w);
    ASSERT_TRUE(greeted(w->greeting()));
    RingWire r(*w);
    ASSERT_TRUE(r.ok()) << r.error();
    EXPECT_EQ(r.ask("PING"), "PONG\n");
    // More commands published than the ring holds.
    r.mapping().to_server()->tail.store(r.mapping().capacity() * 4, std::memory_order_release);
    ob::ring::ring_bell(r.server_bell());
    EXPECT_TRUE(r.socket_closed_within(5000)) << "the session went on after its ring was broken";
    w.reset();
    server.stop();
}

TEST(RingsServer, ClosingTheSocketEndsTheSessionInRings) {
    // One session a node: the place the session in rings held is given back when its socket closes.
    Dir dir;
    auto config         = ring_config(dir, ring_port_at(70), false);
    config.max_sessions = 1;
    Running server(config);
    {
        auto w = through(config.unix_socket);
        ASSERT_TRUE(w);
        ASSERT_TRUE(greeted(w->greeting()));
        RingWire r(*w);
        ASSERT_TRUE(r.ok()) << r.error();
        EXPECT_EQ(r.ask("PING"), "PONG\n");
    }   // the socket closes here, and the rings with the session
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    std::string greeting;
    while (std::chrono::steady_clock::now() < deadline) {
        auto again = through(config.unix_socket);
        ASSERT_TRUE(again);
        greeting = again->greeting();
        if (greeted(greeting)) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    EXPECT_TRUE(greeted(greeting)) << "the session in rings outlived its socket: " << greeting;
    server.stop();
}

TEST(RingsServer, ABurstLargerThanTheCommandRingIsTakenAsTheNodeReadsIt) {
    // 60 000 PINGs in one write through a ring of 64 KiB: the client fills it, waits for room on its
    // bell, and the node rings it as it reads. The answers pile up in the node's queue meanwhile, and
    // go out once the client reads them. The node reads a whole ring at a time, so the burst is
    // several rings long, and it starts once the node has gone to sleep: a node still awake from the
    // move could take the first ring before the client finds it full.
    Dir dir;
    auto config         = ring_config(dir, ring_port_at(80), false);
    config.metrics_port = ring_port_at(81);
    Running server(config);
    auto w = through(config.unix_socket);
    ASSERT_TRUE(w);
    ASSERT_TRUE(greeted(w->greeting()));
    RingWire r(*w, 65536);
    ASSERT_TRUE(r.ok()) << r.error();
    // No round trip first: its answer would ring the client, and the count below is of room alone.
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    std::string burst;
    for (int i = 0; i < 60000; ++i) burst += "PING\n";
    ASSERT_TRUE(r.send_raw(burst)) << "the client was never given room";
    ASSERT_GE(r.room_waits(), 1u) << "the client never waited for room, so nothing here tests it";
    // The client has not slept on an answer yet, so every bell so far was for room, and each one
    // counts in `ob_ring_bells_total` as an answer's bell does. The loop publishes once a pass.
    long long bells = 0;
    for (int i = 0; i < 100 && bells < 1; ++i) {
        bells = metric(config.metrics_port, "ob_ring_bells_total");
        if (bells < 1) std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    EXPECT_GE(bells, 1) << "the bells rung for room are not in ob_ring_bells_total";
    for (int i = 0; i < 60000; ++i) {
        const std::string a = r.answer();
        ASSERT_EQ(a, "PONG\n") << "answer " << i;
    }
    w.reset();
    server.stop();
}
