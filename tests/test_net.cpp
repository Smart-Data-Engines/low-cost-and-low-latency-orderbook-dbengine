// ob::net: the one place the engine opens listeners and connections, IPv4 and IPv6.
//
// The address half is pure and checked case by case. The socket half binds real ports on the
// loopback addresses of both families, and connects to them, because what it promises - a listener
// with no --bind takes both families - is a property of the kernel's sockets, not of this code.
// A host without ::1 skips the IPv6 cases by name; CI checks it has one.

#include "orderbook/net.hpp"

#include <gtest/gtest.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <unistd.h>

#include <cstdio>
#include <filesystem>
#include <fstream>
#include <string>

namespace {

using ob::net::address_of;
using ob::net::dial;
using ob::net::dial_unix;
using ob::net::listen_unix;
using ob::net::valid_unix_path;
using ob::net::join_host_port;
using ob::net::listen_on;
using ob::net::literal_family;
using ob::net::peer_address;
using ob::net::split_host_port;
using ob::net::valid_bind;

bool has_ipv6_loopback() {
    const int fd = ::socket(AF_INET6, SOCK_STREAM, 0);
    if (fd < 0) return false;
    sockaddr_in6 a{};
    a.sin6_family = AF_INET6;
    a.sin6_addr   = in6addr_loopback;
    const bool ok = ::bind(fd, reinterpret_cast<sockaddr*>(&a), sizeof(a)) == 0;
    ::close(fd);
    return ok;
}

uint16_t port_of(int fd) {
    sockaddr_storage a{};
    socklen_t len = sizeof(a);
    if (::getsockname(fd, reinterpret_cast<sockaddr*>(&a), &len) != 0) return 0;
    return a.ss_family == AF_INET6 ? ntohs(reinterpret_cast<sockaddr_in6*>(&a)->sin6_port)
                                   : ntohs(reinterpret_cast<sockaddr_in*>(&a)->sin_port);
}

/// The listener's next connection, accepted within a second, or -1.
int accept_one(int listener) {
    pollfd p{listener, POLLIN, 0};
    if (::poll(&p, 1, 1000) != 1) return -1;
    return ::accept(listener, nullptr, nullptr);
}

TEST(NetAddress, SplitsHostAndPortOfEveryFamily) {
    std::string host;
    uint16_t port = 0;
    ASSERT_TRUE(split_host_port("127.0.0.1:9090", host, port));
    EXPECT_EQ(host, "127.0.0.1");
    EXPECT_EQ(port, 9090);
    ASSERT_TRUE(split_host_port("[::1]:5555", host, port));
    EXPECT_EQ(host, "::1") << "the brackets are the text's, not the address's";
    EXPECT_EQ(port, 5555);
    ASSERT_TRUE(split_host_port("[2001:db8::7]:65535", host, port));
    EXPECT_EQ(host, "2001:db8::7");
    EXPECT_EQ(port, 65535);
    ASSERT_TRUE(split_host_port("node-a.internal:21591", host, port));
    EXPECT_EQ(host, "node-a.internal");
    EXPECT_EQ(port, 21591);
}

TEST(NetAddress, RefusesWhatCannotBeSplit) {
    std::string host = "untouched";
    uint16_t port = 7;
    for (const char* bad : {"127.0.0.1", "::1:9090", "2001:db8::7", "[::1]", "[::1]9090", "[::1:9090",
                            "host:", "host:65536", "host:-1", "host:12a", ":"}) {
        EXPECT_FALSE(split_host_port(bad, host, port)) << bad;
    }
    EXPECT_EQ(host, "untouched") << "a refusal leaves the outputs as they were";
    EXPECT_EQ(port, 7);
}

TEST(NetAddress, JoinsWithBracketsForIPv6Only) {
    EXPECT_EQ(join_host_port("127.0.0.1", 1), "127.0.0.1:1");
    EXPECT_EQ(join_host_port("::1", 9090), "[::1]:9090");
    EXPECT_EQ(join_host_port("[::1]", 9090), "[::1]:9090") << "brackets are not doubled";
    EXPECT_EQ(join_host_port("example.org", 443), "example.org:443");
    std::string host;
    uint16_t port = 0;
    ASSERT_TRUE(split_host_port(join_host_port("fe80::1", 21592), host, port));
    EXPECT_EQ(host, "fe80::1");
    EXPECT_EQ(port, 21592);
}

TEST(NetAddress, TellsTheFamilyOfALiteral) {
    EXPECT_EQ(literal_family("10.0.0.1"), AF_INET);
    EXPECT_EQ(literal_family("::"), AF_INET6);
    EXPECT_EQ(literal_family("[::1]"), AF_INET6);
    EXPECT_EQ(literal_family("localhost"), AF_UNSPEC);
    EXPECT_EQ(literal_family("300.1.1.1"), AF_UNSPEC);
    EXPECT_EQ(literal_family(""), AF_UNSPEC);
    EXPECT_TRUE(valid_bind(""));
    EXPECT_TRUE(valid_bind("127.0.0.1"));
    EXPECT_TRUE(valid_bind("::1"));
    EXPECT_TRUE(valid_bind("[::]"));
    EXPECT_FALSE(valid_bind("localhost")) << "a bind address is a literal, as it was for IPv4";
    EXPECT_FALSE(valid_bind("1.2.3"));
}

TEST(NetAddress, WritesAnAddressAsAPeerReadsIt) {
    sockaddr_storage s{};
    auto* a4 = reinterpret_cast<sockaddr_in*>(&s);
    a4->sin_family = AF_INET;
    a4->sin_port   = htons(1234);
    ::inet_pton(AF_INET, "192.0.2.10", &a4->sin_addr);
    EXPECT_EQ(address_of(s, sizeof(sockaddr_in)), "192.0.2.10:1234");

    s = {};
    auto* a6 = reinterpret_cast<sockaddr_in6*>(&s);
    a6->sin6_family = AF_INET6;
    a6->sin6_port   = htons(5678);
    ::inet_pton(AF_INET6, "2001:db8::1", &a6->sin6_addr);
    EXPECT_EQ(address_of(s, sizeof(sockaddr_in6)), "[2001:db8::1]:5678");

    ::inet_pton(AF_INET6, "::ffff:198.51.100.7", &a6->sin6_addr);
    EXPECT_EQ(address_of(s, sizeof(sockaddr_in6)), "198.51.100.7:5678")
        << "what a dual-stack listener hands over for an IPv4 client is shown as that client";

    s = {};
    s.ss_family = AF_UNIX;
    EXPECT_EQ(address_of(s, sizeof(s)), "unknown");
    EXPECT_EQ(address_of(s, 0), "unknown");
}

TEST(NetListen, AnIPv4LiteralListensOnIPv4) {
    std::string error;
    const int fd = listen_on("127.0.0.1", 0, 16, 0, error);
    ASSERT_GE(fd, 0) << error;
    std::string why;
    const int c = dial("127.0.0.1", port_of(fd), 1000, false, why, "test");
    ASSERT_GE(c, 0) << why;
    const int s = accept_one(fd);
    ASSERT_GE(s, 0);
    EXPECT_EQ(peer_address(s).rfind("127.0.0.1:", 0), 0u) << peer_address(s);
    ::close(s);
    ::close(c);
    ::close(fd);
}

TEST(NetListen, NoBindTakesBothFamilies) {
    if (!has_ipv6_loopback()) GTEST_SKIP() << "this host has no ::1";
    std::string error;
    bool fell_back = true;
    const int fd = listen_on("", 0, 16, 0, error, &fell_back);
    ASSERT_GE(fd, 0) << error;
    EXPECT_FALSE(fell_back);
    const uint16_t port = port_of(fd);
    for (const char* host : {"127.0.0.1", "::1", "[::1]"}) {
        std::string why;
        const int c = dial(host, port, 1000, false, why, "test");
        ASSERT_GE(c, 0) << host << ": " << why;
        const int s = accept_one(fd);
        ASSERT_GE(s, 0) << host;
        const std::string seen = peer_address(s);
        EXPECT_EQ(seen.rfind(std::string(host) == "127.0.0.1" ? "127.0.0.1:" : "[::1]:", 0), 0u)
            << host << " was seen as " << seen;
        ::close(s);
        ::close(c);
    }
    ::close(fd);
}

TEST(NetListen, AnIPv6LiteralListensOnIPv6) {
    if (!has_ipv6_loopback()) GTEST_SKIP() << "this host has no ::1";
    std::string error;
    const int fd = listen_on("::1", 0, 16, 0, error);
    ASSERT_GE(fd, 0) << error;
    std::string why;
    EXPECT_LT(dial("127.0.0.1", port_of(fd), 500, false, why, "test"), 0)
        << "a listener on ::1 is not a listener on 127.0.0.1";
    const int c = dial("::1", port_of(fd), 1000, false, why, "test");
    ASSERT_GE(c, 0) << why;
    ::close(c);
    ::close(fd);
}

TEST(NetListen, EveryInterfaceOfOneFamilyIsThatFamilyAlone) {
    // `0.0.0.0` and `::` are how an operator asks for one family on every interface - the first is
    // the way back to the listener a port had before IPv6 (#236). `::` takes IPv4 too on a host
    // whose net.ipv6.bindv6only is 0, most of them, unless the listener says otherwise.
    if (!has_ipv6_loopback()) GTEST_SKIP() << "this host has no ::1";
    std::string error;
    const int v6 = listen_on("::", 0, 16, 0, error);
    ASSERT_GE(v6, 0) << error;
    std::string why;
    EXPECT_LT(dial("127.0.0.1", port_of(v6), 500, false, why, "test"), 0)
        << "a listener on :: took an IPv4 connection";
    const int c6 = dial("::1", port_of(v6), 1000, false, why, "test");
    EXPECT_GE(c6, 0) << why;
    if (c6 >= 0) ::close(c6);
    ::close(v6);

    const int v4 = listen_on("0.0.0.0", 0, 16, 0, error);
    ASSERT_GE(v4, 0) << error;
    EXPECT_LT(dial("::1", port_of(v4), 500, false, why, "test"), 0)
        << "a listener on 0.0.0.0 took an IPv6 connection";
    const int c4 = dial("127.0.0.1", port_of(v4), 1000, false, why, "test");
    EXPECT_GE(c4, 0) << why;
    if (c4 >= 0) ::close(c4);
    ::close(v4);
}

TEST(NetListen, ABadAddressOrABusyPortIsAnError) {
    std::string error;
    EXPECT_LT(listen_on("localhost", 0, 16, 0, error), 0);
    EXPECT_NE(error.find("invalid bind address 'localhost'"), std::string::npos) << error;
    const int first = listen_on("127.0.0.1", 0, 16, 0, error);
    ASSERT_GE(first, 0) << error;
    // SO_REUSEADDR does not let a second listener take a port another is listening on.
    error.clear();
    EXPECT_LT(listen_on("127.0.0.1", port_of(first), 16, 0, error), 0);
    EXPECT_NE(error.find("bind() failed on port " + std::to_string(port_of(first))), std::string::npos)
        << error;
    ::close(first);
}

TEST(NetDial, TriesEveryAddressOfAName) {
    // `localhost` may resolve to ::1 first; a listener on 127.0.0.1 alone must still be reached.
    std::string error;
    const int fd = listen_on("127.0.0.1", 0, 16, 0, error);
    ASSERT_GE(fd, 0) << error;
    std::string why;
    const int c = dial("localhost", port_of(fd), 1000, true, why, "test");
    ASSERT_GE(c, 0) << why;
    const int s = accept_one(fd);
    EXPECT_GE(s, 0);
    if (s >= 0) ::close(s);
    ::close(c);
    ::close(fd);
}

TEST(NetDial, TheConnectionComesBackWithNagleOff) {
    // Every connection the engine dials carries small messages, and dial() is where all of them
    // are dialled (#140). Both modes, because the blocking one goes through one more fcntl().
    std::string error;
    const int fd = listen_on("127.0.0.1", 0, 16, 0, error);
    ASSERT_GE(fd, 0) << error;
    auto nodelay = [](int s) {
        int on = -1;
        socklen_t len = sizeof(on);
        EXPECT_EQ(::getsockopt(s, IPPROTO_TCP, TCP_NODELAY, &on, &len), 0);
        return on;
    };
    for (const bool leave_nonblocking : {false, true}) {
        std::string why;
        const int c = dial("127.0.0.1", port_of(fd), 1000, leave_nonblocking, why, "test");
        ASSERT_GE(c, 0) << why;
        const int s = accept_one(fd);
        ASSERT_GE(s, 0);
        // The control: the accepted end has the kernel's default, so the check below is not
        // reporting a default that happened to be on.
        EXPECT_EQ(nodelay(s), 0) << "an accepted socket starts with Nagle enabled";
        EXPECT_EQ(nodelay(c), 1) << "dial() left Nagle on, leave_nonblocking=" << leave_nonblocking;
        ::close(s);
        ::close(c);
    }
    ::close(fd);
}

TEST(NetDial, SaysWhyNothingAnswered) {
    std::string error;
    const int fd = listen_on("127.0.0.1", 0, 16, 0, error);
    ASSERT_GE(fd, 0) << error;
    const uint16_t port = port_of(fd);
    ::close(fd);   // nothing listens there now
    std::string why;
    EXPECT_LT(dial("127.0.0.1", port, 1000, false, why, "test"), 0);
    EXPECT_NE(why.find("refused"), std::string::npos) << why;
    why.clear();
    EXPECT_LT(dial("no-such-host.invalid", 9, 1000, false, why, "test"), 0);
    EXPECT_NE(why.find("does not resolve"), std::string::npos) << why;
    why.clear();
    EXPECT_LT(dial("127.0.0.1", 0, 1000, false, why, "test"), 0);
    EXPECT_NE(why.find("port 0"), std::string::npos) << why;
}

// ── Unix sockets (#52) ────────────────────────────────────────────────────────

/// A directory of its own under the temporary directory, removed with everything in it at the end.
struct UnixDir {
    std::filesystem::path dir;
    UnixDir() {
        dir = std::filesystem::temp_directory_path() /
              ("ob_unix_" + std::to_string(::getpid()) + "_" + std::to_string(::gettid()));
        std::filesystem::create_directories(dir);
    }
    ~UnixDir() {
        std::error_code ec;
        std::filesystem::remove_all(dir, ec);
    }
    std::string at(const char* name) const { return (dir / name).string(); }
};

TEST(NetUnix, APathMustFitASocketsAddress) {
    std::string why;
    EXPECT_TRUE(valid_unix_path("/run/orderbook/ob.sock", why)) << why;
    EXPECT_FALSE(valid_unix_path("", why));
    EXPECT_NE(why.find("empty"), std::string::npos) << why;
    EXPECT_FALSE(valid_unix_path(std::string("/tmp/a\0b", 8), why));
    EXPECT_NE(why.find("NUL"), std::string::npos) << why;
    const std::string longest = "/" + std::string(sizeof(sockaddr_un{}.sun_path) - 2, 'x');
    EXPECT_TRUE(valid_unix_path(longest, why)) << why;
    EXPECT_FALSE(valid_unix_path(longest + "x", why));
    EXPECT_NE(why.find("at most 107"), std::string::npos) << why;
}

TEST(NetUnix, AListenerTakesAConnectionAndSeesTheProcessThatMadeIt) {
    UnixDir d;
    const std::string path = d.at("ob.sock");
    std::string error;
    const int listener = listen_unix(path, 0660, 16, 0, error);
    ASSERT_GE(listener, 0) << error;
    std::string why;
    const int client = dial_unix(path, 1000, false, why);
    ASSERT_GE(client, 0) << why;
    const int served = accept_one(listener);
    ASSERT_GE(served, 0);
    // The server's end names the client's process, which is this one.
    EXPECT_EQ(peer_address(served), "unix pid=" + std::to_string(::getpid()) + " uid=" +
                                        std::to_string(::getuid()) + " gid=" + std::to_string(::getgid()));
    const char ping[] = "PING\n";
    ASSERT_EQ(::send(client, ping, 5, 0), 5);
    char got[8] = {};
    ASSERT_EQ(::recv(served, got, sizeof(got), 0), 5);
    EXPECT_EQ(std::string(got, 5), "PING\n");
    ::close(served);
    ::close(client);
    ::close(listener);
}

TEST(NetUnix, TheSocketsFileTakesTheModeAsked) {
    UnixDir d;
    for (const uint32_t mode : {0600u, 0660u, 0666u}) {
        const std::string path = d.at(("m" + std::to_string(mode) + ".sock").c_str());
        std::string error;
        const int listener = listen_unix(path, mode, 16, 0, error);
        ASSERT_GE(listener, 0) << error;
        struct stat st{};
        ASSERT_EQ(::lstat(path.c_str(), &st), 0);
        EXPECT_TRUE(S_ISSOCK(st.st_mode));
        EXPECT_EQ(st.st_mode & 0777, mode) << std::oct << (st.st_mode & 0777);
        ::close(listener);
    }
}

TEST(NetUnix, ASocketNobodyListensOnIsReplaced) {
    // What a process killed while it listened leaves: the file, and nothing behind it.
    UnixDir d;
    const std::string path = d.at("stale.sock");
    std::string error;
    const int first = listen_unix(path, 0660, 16, 0, error);
    ASSERT_GE(first, 0) << error;
    ::close(first);   // gone without removing its file
    struct stat st{};
    ASSERT_EQ(::lstat(path.c_str(), &st), 0) << "the stale file is not there, so this tests nothing";
    const int second = listen_unix(path, 0660, 16, 0, error);
    ASSERT_GE(second, 0) << error;
    std::string why;
    const int client = dial_unix(path, 1000, false, why);
    EXPECT_GE(client, 0) << why;
    if (client >= 0) ::close(client);
    ::close(second);
}

TEST(NetUnix, ASocketSomebodyListensOnIsRefusedAndKeepsWorking) {
    UnixDir d;
    const std::string path = d.at("busy.sock");
    std::string error;
    const int first = listen_unix(path, 0660, 16, 0, error);
    ASSERT_GE(first, 0) << error;
    EXPECT_LT(listen_unix(path, 0660, 16, 0, error), 0);
    EXPECT_NE(error.find("another process listens on it"), std::string::npos) << error;
    EXPECT_NE(error.find(path), std::string::npos) << error;
    // The refusal took nothing from the first: its file is there and it still answers.
    std::string why;
    const int client = dial_unix(path, 1000, false, why);
    ASSERT_GE(client, 0) << why;
    const int served = accept_one(first);
    EXPECT_GE(served, 0);
    if (served >= 0) ::close(served);
    ::close(client);
    ::close(first);
}

TEST(NetUnix, AFileThatIsNotASocketIsRefusedAndLeftAsItWas) {
    UnixDir d;
    const std::string path = d.at("notes.txt");
    { std::ofstream(path) << "an operator's file"; }
    std::string error;
    EXPECT_LT(listen_unix(path, 0660, 16, 0, error), 0);
    EXPECT_NE(error.find("is not a socket"), std::string::npos) << error;
    std::ifstream in(path);
    std::string kept((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    EXPECT_EQ(kept, "an operator's file");
}

TEST(NetUnix, ADialSaysWhyNothingAnswered) {
    UnixDir d;
    std::string why;
    EXPECT_LT(dial_unix(d.at("nothing.sock"), 200, false, why), 0);
    EXPECT_NE(why.find("No such file"), std::string::npos) << why;
    std::string error;
    const int listener = listen_unix(d.at("closed.sock"), 0660, 16, 0, error);
    ASSERT_GE(listener, 0) << error;
    ::close(listener);
    EXPECT_LT(dial_unix(d.at("closed.sock"), 200, false, why), 0);
    EXPECT_NE(why.find("refused"), std::string::npos) << why;
}

}  // namespace
