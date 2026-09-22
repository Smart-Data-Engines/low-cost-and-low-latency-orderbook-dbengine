// Every socket this engine keeps open sets TCP_NODELAY, and the ones that do not say why (#140).
//
// The defect this file guards against is not visible in any benchmark that asks one question at a
// time. Nagle holds a small write while an earlier byte is unacknowledged; the peer's delayed-ACK
// timer releases it. With one response in flight there is nothing unacknowledged, so a
// request/response client never meets it — and every published number for this engine came from
// such a client. A client that pipelines meets it on every round trip: measured on one
// m9g.xlarge, **52.75 ms per round trip at batch 8**, and 51.68 and 52.15 at 64 and 512 — the
// same figure at three batch sizes, which is a timer. The same 250 round trips took 12.963 s
// before and 0.021 s once the client acknowledged immediately, with the server unchanged.
//
// Two checks, because neither is sufficient alone. The behavioural one proves the helper does what
// its name says on a real socket, which is what a no-op mutation has to survive. The static one
// proves it is called from the places that need it — and that is the half a behavioural test
// cannot reach, because a socket option cannot be read from the other end of the connection: no
// client can ask whether the server set it. The integration battery observes the *consequence*
// (tests/integration/test_wire_nodelay.py), which is the closest anything gets to reading it.

#include <gtest/gtest.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <filesystem>
#include <fstream>
#include <iterator>
#include <regex>
#include <set>
#include <string>
#include <vector>

#include "orderbook/socket_options.hpp"

namespace {

std::string read_file(const std::filesystem::path& p) {
    std::ifstream in(p);
    if (!in) return {};
    return std::string((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
}

/// Comments removed, so a rule about what the code *does* cannot be satisfied by a sentence about
/// it. `src/multi_master.cpp` discusses a blocking `::connect()` in prose two hundred lines from
/// the call; this repository has paid for that confusion four times, twice inside the checker
/// written to catch it.
std::string without_comments(const std::string& src) {
    std::string out;
    out.reserve(src.size());
    for (size_t i = 0; i < src.size();) {
        if (src.compare(i, 2, "//") == 0) {
            const auto nl = src.find('\n', i);
            i = (nl == std::string::npos) ? src.size() : nl;   // keep the newline
        } else if (src.compare(i, 2, "/*") == 0) {
            const auto end = src.find("*/", i + 2);
            i = (end == std::string::npos) ? src.size() : end + 2;
        } else {
            out.push_back(src[i++]);
        }
    }
    return out;
}

std::vector<std::filesystem::path> engine_sources() {
    std::vector<std::filesystem::path> out;
    for (const auto& e : std::filesystem::directory_iterator(std::filesystem::path(OB_SOURCE_DIR) / "src"))
        if (e.is_regular_file() && e.path().extension() == ".cpp") out.push_back(e.path());
    std::sort(out.begin(), out.end());
    return out;
}

/// A file that ends up holding a TCP connection: it accepts one, or it dials one.
///
/// Derived rather than listed. `::accept` and `::connect` are matched with a leading
/// non-identifier so `CoordinatorClient::connect(` is not one of them — the first version of this
/// rule matched that method and put an exemption comment above a function with no socket in it.
/// It also counted `io_uring_prep_accept`, because on that transport the accept was a submitted
/// request with no syscall spelling at all; the transport is gone (#147).
size_t connection_sites(const std::string& code) {
    static const std::regex syscall(R"([^A-Za-z0-9_:]::(accept4?|connect)\s*\()");
    const auto begin = std::sregex_iterator(code.begin(), code.end(), syscall);
    return static_cast<size_t>(std::distance(begin, std::sregex_iterator()));
}

bool owns_a_connection(const std::string& code) { return connection_sites(code) > 0; }

size_t count_of(const std::string& haystack, const std::string& needle) {
    size_t n = 0;
    for (size_t at = haystack.find(needle); at != std::string::npos;
         at = haystack.find(needle, at + needle.size()))
        ++n;
    return n;
}

std::string rel(const std::filesystem::path& p) {
    return std::filesystem::relative(p, std::filesystem::path(OB_SOURCE_DIR)).string();
}

constexpr const char* kExemption = "OB_NO_TCP_NODELAY:";

}  // namespace

// ── The helper does what it says ─────────────────────────────────────────────

TEST(SocketOptions, TheOptionIsOffBeforeAndOnAfter) {
    // A real AF_INET pair: TCP_NODELAY is meaningless on the AF_UNIX socketpair the session tests
    // use, and `getsockopt` there answers about a different protocol (pitfall 115's neighbourhood).
    int listener = ::socket(AF_INET, SOCK_STREAM, 0);
    ASSERT_GE(listener, 0);
    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port        = 0;   // the kernel picks, so this test cannot collide with another (#109)
    ASSERT_EQ(::bind(listener, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)), 0);
    socklen_t len = sizeof(addr);
    ASSERT_EQ(::getsockname(listener, reinterpret_cast<sockaddr*>(&addr), &len), 0);
    ASSERT_EQ(::listen(listener, 1), 0);

    int dialled = ::socket(AF_INET, SOCK_STREAM, 0);
    ASSERT_GE(dialled, 0);
    ASSERT_EQ(::connect(dialled, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)), 0);
    int accepted = ::accept(listener, nullptr, nullptr);
    ASSERT_GE(accepted, 0);

    auto nodelay = [](int fd) {
        int on = -1;
        socklen_t l = sizeof(on);
        EXPECT_EQ(::getsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &on, &l), 0);
        return on;
    };

    // The control. Without it a helper that does nothing passes the assertion below on a kernel
    // whose default happened to be on, and the test would be reporting the default.
    EXPECT_EQ(nodelay(accepted), 0) << "an accepted socket starts with Nagle enabled";

    ob::set_tcp_nodelay(accepted, "test");
    EXPECT_EQ(nodelay(accepted), 1) << "set_tcp_nodelay left the option off";

    ::close(accepted);
    ::close(dialled);
    ::close(listener);
}

TEST(SocketOptions, ABadDescriptorIsLoggedRatherThanFatal) {
    // A connection that works and is slower beats refusing to serve over a socket option.
    ob::set_tcp_nodelay(-1, "test");
    SUCCEED();
}

// ── and it is called where it is needed ──────────────────────────────────────

TEST(SocketOptionsStatic, EverySocketSetsItOrSaysWhyNot) {
    // Counted per site rather than per file. A per-file rule is satisfied by either of two
    // sockets, so `src/replication.cpp` — which accepts a replica and dials a primary — would
    // keep passing after losing one of its two calls: the rule would cover less than it reads as
    // covering, which is the failure mode this repository files most often.
    size_t files_with_sockets = 0, files_setting = 0, files_exempting = 0;
    std::vector<std::string> short_of;

    for (const auto& path : engine_sources()) {
        const std::string raw  = read_file(path);
        ASSERT_FALSE(raw.empty()) << "cannot read " << rel(path);
        const std::string code = without_comments(raw);
        const size_t sites = connection_sites(code);
        if (sites == 0) continue;

        ++files_with_sockets;
        const size_t calls = count_of(code, "set_tcp_nodelay(");
        // The exemption lives beside the socket it exempts, not in a list here: a list in this
        // file is one more thing to keep in step with the tree, and the reason belongs where the
        // next reader of that accept call is standing.
        const size_t reasons = count_of(raw, kExemption);
        if (calls > 0) ++files_setting;
        if (reasons > 0) ++files_exempting;

        if (calls + reasons < sites)
            short_of.push_back(rel(path) + " (" + std::to_string(sites) + " sockets, " +
                               std::to_string(calls) + " set, " + std::to_string(reasons) +
                               " explained)");
    }

    EXPECT_TRUE(short_of.empty())
        << "these hold more TCP connections than they either set TCP_NODELAY on or carry an "
        << kExemption << " reason for: "
        << [&] { std::string s; for (auto& m : short_of) s += m + "; "; return s; }();

    // A scan that finds nothing satisfies every rule above. These say the sweep reached the tree
    // and that both branches of the rule are live — files that set it, files that explain why
    // they do not — so neither branch can quietly stop being exercised.
    EXPECT_GE(files_with_sockets, 4u) << "the sweep found almost no connection-holding sources";
    EXPECT_GT(files_setting, 0u) << "no file exercises the setting branch any more";
    EXPECT_GT(files_exempting, 0u) << "no file exercises the exemption branch any more";
}

TEST(SocketOptionsStatic, NothingCallsItThatDoesNotHoldAConnection) {
    for (const auto& path : engine_sources()) {
        const std::string code = without_comments(read_file(path));
        if (code.find("set_tcp_nodelay(") == std::string::npos) continue;
        EXPECT_TRUE(owns_a_connection(code))
            << rel(path) << " sets TCP_NODELAY on a socket it neither accepts nor dials";
    }
}

TEST(SocketOptionsStatic, TheOptionIsSetInExactlyOnePlace) {
    // The same move as refusing a bare `SSL_CTX_new` (pitfall 131): a sixth site that open-codes
    // `setsockopt(..., TCP_NODELAY, ...)` passes every rule above while bypassing the definition
    // that carries the measurement and the logging.
    for (const auto& path : engine_sources()) {
        const std::string code = without_comments(read_file(path));
        EXPECT_EQ(code.find("TCP_NODELAY"), std::string::npos)
            << rel(path) << " names TCP_NODELAY directly; call ob::set_tcp_nodelay() instead";
    }
    const std::string header = read_file(std::filesystem::path(OB_SOURCE_DIR) /
                                         "include/orderbook/socket_options.hpp");
    EXPECT_NE(header.find("TCP_NODELAY"), std::string::npos)
        << "the one definition no longer mentions the option — this rule is checking nothing";
}
