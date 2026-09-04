// The C++ client's TLS path, and the name check that makes verification mean something (#30 part
// three).
//
// A real handshake over loopback rather than a mock, because the claim under test is about what
// OpenSSL accepts. The interesting test is the negative one: a certificate this client's CA signed,
// issued for a *different* address. `SSL_CTX_set_verify(SSL_VERIFY_PEER)` accepts it - the chain is
// good - so without `tls_expect_host()` a private CA that signs the cluster makes every node's
// certificate good for every other node, and the verification reads as done.

#include "orderbook/client.hpp"
#include "orderbook/shard_router.hpp"
#include "orderbook/tls.hpp"

#include <gtest/gtest.h>

#include <openssl/ssl.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <cctype>
#include <string>
#include <thread>
#include <vector>

namespace fs = std::filesystem;

namespace {

struct KeyPair { std::string cert, key; };

/// A self-signed certificate. Self-signed means it is also its own CA, so the same file serves as
/// the client's trust anchor - which is what lets these tests separate "chain is good" from "name
/// is right" without building a CA hierarchy.
KeyPair generate(const std::string& tag, const std::string& san) {
    static std::atomic<int> counter{0};
    const auto dir = fs::temp_directory_path() /
                     ("ob_tlsc_" + tag + "_" + std::to_string(::getpid()) + "_" +
                      std::to_string(counter.fetch_add(1)));
    fs::create_directories(dir);
    const std::string cert = (dir / "cert.pem").string();
    const std::string key  = (dir / "key.pem").string();
    const std::string cmd = "openssl req -x509 -newkey rsa:2048 -keyout '" + key + "' -out '" +
                            cert + "' -days 1 -nodes -subj '/CN=" + tag + "' -addext '" + san +
                            "' >/dev/null 2>&1";
    if (std::system(cmd.c_str()) != 0) return {"", ""};
    ::chmod(key.c_str(), 0600);
    return {cert, key};
}

/// One connection, one banner, and PING answered. Enough to prove bytes move both ways.
///
/// It must not assert on a failed handshake: three of the tests below exist precisely to make the
/// handshake fail, and a server thread that fails the test for succeeding at its job would report
/// the defect as being on the wrong side.
class TinyTlsServer {
public:
    TinyTlsServer(const KeyPair& kp, bool speak_tls) : speak_tls_(speak_tls) {
        listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
        int one = 1;
        ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
        sockaddr_in addr{};
        addr.sin_family      = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port        = 0;
        if (::bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) return;
        socklen_t len = sizeof(addr);
        ::getsockname(listen_fd_, reinterpret_cast<sockaddr*>(&addr), &len);
        port_ = ntohs(addr.sin_port);
        ::listen(listen_fd_, 4);

        thread_ = std::thread([this, kp] { run(kp); });
    }

    ~TinyTlsServer() {
        stop_.store(true);
        // Wake the accept by connecting to it, rather than closing the descriptor the thread is
        // sitting in - pitfall 49, and it is wrong every time it is written.
        int poke = ::socket(AF_INET, SOCK_STREAM, 0);
        sockaddr_in addr{};
        addr.sin_family      = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port        = htons(port_);
        ::connect(poke, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
        if (thread_.joinable()) thread_.join();
        ::close(poke);
        if (listen_fd_ != -1) ::close(listen_fd_);
    }

    uint16_t port() const { return port_; }

private:
    void run(const KeyPair& kp) {
        while (!stop_.load()) {
            const int fd = ::accept(listen_fd_, nullptr, nullptr);
            if (fd < 0) return;
            if (stop_.load()) { ::close(fd); return; }
            if (!speak_tls_) {
                // A plaintext banner where a ServerHello is expected. This is the misconfiguration
                // an operator makes most often, so it gets a test rather than a sentence.
                static constexpr char banner[] = "OK ob_tcp_server v0.0.0-test\n\n";
                (void)!::write(fd, banner, sizeof(banner) - 1);
                ::close(fd);
                continue;
            }
            serve_tls(fd, kp);
            ::close(fd);
        }
    }

    void serve_tls(int fd, const KeyPair& kp) {
        ob::TlsContext ctx = ob::TlsContext::server(kp.cert, kp.key);
        auto ssl = ctx.wrap(fd, /*server_side=*/true);
        std::string why;
        if (ob::tls_blocking_handshake(ssl.get(), &why) != 1) return;  // expected in three tests

        static constexpr char banner[] = "OK ob_tcp_server v0.0.0-test\n\n";
        if (ob::tls_blocking_write_all(ssl.get(), banner, sizeof(banner) - 1, &why) != 1) return;

        char buf[512];
        for (;;) {
            const int n = ob::tls_blocking_read(ssl.get(), buf, sizeof(buf), &why);
            if (n <= 0) return;
            const std::string_view cmd(buf, static_cast<size_t>(n));
            const char* reply = cmd.starts_with("PING") ? "PONG\n" : "ERR unsupported\n";
            if (ob::tls_blocking_write_all(ssl.get(), reply, std::strlen(reply), &why) != 1) return;
            if (cmd.starts_with("QUIT")) return;
        }
    }

    bool             speak_tls_;
    int              listen_fd_{-1};
    uint16_t         port_{0};
    std::thread      thread_;
    std::atomic<bool> stop_{false};
};

ob::ClientConfig client_for(uint16_t port) {
    ob::ClientConfig cc;
    cc.host                = "127.0.0.1";
    cc.port                = port;
    cc.connect_timeout_sec = 5.0;
    cc.read_timeout_sec    = 5.0;
    return cc;
}

std::string read_file(const std::string& path) {
    std::ifstream in(path);
    std::ostringstream out;
    out << in.rdbuf();
    return out.str();
}

} // namespace

// ── The byte path ─────────────────────────────────────────────────────────────

TEST(TlsClient, ConnectsAndPingsThroughTls) {
    const auto kp = generate("server", "subjectAltName=IP:127.0.0.1");
    ASSERT_FALSE(kp.cert.empty()) << "openssl tool not available";
    TinyTlsServer server(kp, /*speak_tls=*/true);
    ASSERT_NE(server.port(), 0);

    auto cc = client_for(server.port());
    cc.tls         = true;
    cc.tls_ca_file = kp.cert;   // self-signed: its own trust anchor
    ob::OrderbookClient client(std::move(cc));

    auto conn = client.connect();
    ASSERT_TRUE(conn) << conn.error_message();
    // PING proves both directions through the record layer: the banner arrived, a command went out
    // encrypted, and the answer came back. Asserting only on connect() would leave the write path
    // untested.
    auto pong = client.ping();
    ASSERT_TRUE(pong) << pong.error_message();
    EXPECT_TRUE(pong.value());
    client.disconnect();
}

// ── The test the name check exists for ────────────────────────────────────────

TEST(TlsClient, RefusesACertificateIssuedForAnotherAddress) {
    // Chain: perfect - the client trusts this exact certificate. Name: wrong. Delete the
    // `tls_expect_host()` call in `start_tls()` and this test passes, which is the entire reason it
    // is written as a client-level test rather than as a call to `tls_expect_host()` directly.
    const auto kp = generate("elsewhere", "subjectAltName=IP:10.0.0.2");
    ASSERT_FALSE(kp.cert.empty()) << "openssl tool not available";
    TinyTlsServer server(kp, /*speak_tls=*/true);
    ASSERT_NE(server.port(), 0);

    auto cc = client_for(server.port());
    cc.tls         = true;
    cc.tls_ca_file = kp.cert;
    ob::OrderbookClient client(std::move(cc));

    auto conn = client.connect();
    ASSERT_FALSE(conn) << "a certificate for 10.0.0.2 was accepted for 127.0.0.1";
    EXPECT_NE(conn.error_message().find("handshake"), std::string::npos)
        << "failed for some other reason: " << conn.error_message();
    EXPECT_FALSE(client.connected());
}

TEST(TlsClient, RefusesACertificateItsCaDidNotSign) {
    const auto server_kp = generate("server", "subjectAltName=IP:127.0.0.1");
    const auto other_kp  = generate("stranger", "subjectAltName=IP:127.0.0.1");
    ASSERT_FALSE(server_kp.cert.empty());
    ASSERT_FALSE(other_kp.cert.empty());
    ASSERT_NE(read_file(server_kp.cert), read_file(other_kp.cert));

    TinyTlsServer server(server_kp, /*speak_tls=*/true);
    ASSERT_NE(server.port(), 0);

    auto cc = client_for(server.port());
    cc.tls         = true;
    cc.tls_ca_file = other_kp.cert;   // right name, wrong signer
    ob::OrderbookClient client(std::move(cc));

    auto conn = client.connect();
    ASSERT_FALSE(conn) << "an untrusted certificate was accepted";
    EXPECT_FALSE(client.connected());
}

TEST(TlsClient, WithoutVerificationItConnectsToAnything) {
    // The escape hatch, tested so its cost is on the record rather than in a comment: this same
    // certificate is refused by the two tests above, and here it is accepted.
    const auto kp = generate("elsewhere", "subjectAltName=IP:10.0.0.2");
    ASSERT_FALSE(kp.cert.empty());
    TinyTlsServer server(kp, /*speak_tls=*/true);
    ASSERT_NE(server.port(), 0);

    auto cc = client_for(server.port());
    cc.tls        = true;
    cc.tls_verify = false;
    ob::OrderbookClient client(std::move(cc));

    auto conn = client.connect();
    ASSERT_TRUE(conn) << conn.error_message();
    auto pong = client.ping();
    EXPECT_TRUE(pong) << pong.error_message();
    client.disconnect();
}

TEST(TlsClient, APlaintextServerIsRefusedRatherThanRead) {
    // The commonest misconfiguration: `tls=true` against a port started without `--tls-client`.
    // The failure must be an error, not a banner accepted as a successful connection.
    const auto kp = generate("unused", "subjectAltName=IP:127.0.0.1");
    ASSERT_FALSE(kp.cert.empty());
    TinyTlsServer server(kp, /*speak_tls=*/false);
    ASSERT_NE(server.port(), 0);

    auto cc = client_for(server.port());
    cc.tls        = true;
    cc.tls_verify = false;   // isolate the version failure from any trust question
    ob::OrderbookClient client(std::move(cc));

    auto conn = client.connect();
    ASSERT_FALSE(conn) << "a plaintext banner was accepted as a TLS connection";
    EXPECT_FALSE(client.connected());
}

// ── Configurations that describe a protection the caller does not have ────────

TEST(TlsClient, RefusesOptionsThatContradictEachOther) {
    {
        auto cc = client_for(1);   // no server needed: the refusal precedes the socket
        cc.tls_ca_file = "/etc/hostname";
        ob::OrderbookClient client(std::move(cc));
        auto r = client.connect();
        ASSERT_FALSE(r);
        EXPECT_NE(r.error_message().find("plain text"), std::string::npos) << r.error_message();
    }
    {
        auto cc = client_for(1);
        cc.tls_verify = false;
        ob::OrderbookClient client(std::move(cc));
        auto r = client.connect();
        ASSERT_FALSE(r);
        EXPECT_NE(r.error_message().find("no certificate"), std::string::npos) << r.error_message();
    }
    {
        auto cc = client_for(1);
        cc.tls         = true;
        cc.tls_verify  = false;
        cc.tls_ca_file = "/etc/hostname";
        ob::OrderbookClient client(std::move(cc));
        auto r = client.connect();
        ASSERT_FALSE(r);
        EXPECT_NE(r.error_message().find("nothing consults"), std::string::npos)
            << r.error_message();
    }
}

TEST(TlsClient, RefusesACaBundleThatIsNotOne) {
    auto cc = client_for(1);
    cc.tls         = true;
    cc.tls_ca_file = "/nonexistent/ca.pem";
    ob::OrderbookClient client(std::move(cc));
    auto r = client.connect();
    ASSERT_FALSE(r);
    EXPECT_NE(r.error_message().find("ca.pem"), std::string::npos) << r.error_message();
}

// ── The name check on its own, both branches ─────────────────────────────────

TEST(TlsClient, ExpectHostTakesTheAddressBranchAndTheNameBranch) {
    auto ctx = ob::TlsContext::client("", /*verify=*/false);
    // Both must succeed, and they must succeed by different routes: `set1_ip_asc` parses the first
    // and refuses the second. A single-case test would pass with either branch deleted.
    auto a = ctx.wrap(-1, /*server_side=*/false);
    EXPECT_TRUE(ob::tls_expect_host(a.get(), "127.0.0.1"));
    EXPECT_EQ(SSL_get_servername(a.get(), TLSEXT_NAMETYPE_host_name), nullptr)
        << "SNI was sent for an IP literal, which RFC 6066 forbids";

    auto b = ctx.wrap(-1, /*server_side=*/false);
    EXPECT_TRUE(ob::tls_expect_host(b.get(), "node-1.example.com"));
    ASSERT_NE(SSL_get_servername(b.get(), TLSEXT_NAMETYPE_host_name), nullptr)
        << "no SNI was sent for a host name";
    EXPECT_STREQ(SSL_get_servername(b.get(), TLSEXT_NAMETYPE_host_name), "node-1.example.com");
}

TEST(TlsClient, ExpectHostLeavesNoStaleErrorBehindAfterTheAddressBranchMisses) {
    // The failed `set1_ip_asc` on a host name queues errors. An error queue nobody drains makes the
    // *next* failure report this one instead - the reason tls_last_error() drains, applied to a
    // function that succeeds.
    auto ctx = ob::TlsContext::client("", /*verify=*/false);
    auto ssl = ctx.wrap(-1, /*server_side=*/false);
    ASSERT_TRUE(ob::tls_expect_host(ssl.get(), "node-1.example.com"));
    EXPECT_EQ(ob::tls_last_error(), "");
}

// ── The drift this file's template exists to stop ────────────────────────────

namespace {

std::string slurp(const std::string& rel) {
    std::ifstream in(std::string(OB_SOURCE_DIR) + "/" + rel);
    std::ostringstream out;
    out << in.rdbuf();
    return out.str();
}

/// Member names of a brace block, taken from the source rather than from a list.
std::vector<std::string> struct_fields(const std::string& src, const std::string& decl) {
    std::vector<std::string> names;
    const auto start = src.find(decl);
    if (start == std::string::npos) return names;
    const auto end = src.find("\n};", start);
    if (end == std::string::npos) return names;

    std::istringstream body(src.substr(start + decl.size(), end - start - decl.size()));
    std::string line;
    while (std::getline(body, line)) {
        const auto first = line.find_first_not_of(" \t");
        if (first == std::string::npos) continue;
        line = line.substr(first);
        if (line.starts_with("//")) continue;
        // Cut a trailing comment so `bool tls = false;  // note` still yields `tls`.
        const auto comment = line.find("//");
        if (comment != std::string::npos) line = line.substr(0, comment);
        auto stop = line.find_first_of("=;");
        if (stop == std::string::npos) continue;
        // Also handle brace-init defaults: `bool tls_verify{true};`
        const auto brace = line.find('{');
        if (brace != std::string::npos && brace < stop) stop = brace;
        std::string decl_part = line.substr(0, stop);
        const auto last = decl_part.find_last_not_of(" \t");
        if (last == std::string::npos) continue;
        decl_part = decl_part.substr(0, last + 1);
        const auto sep = decl_part.find_last_of(" \t*&");
        if (sep == std::string::npos) continue;
        const std::string name = decl_part.substr(sep + 1);
        if (!name.empty() && (std::isalpha(static_cast<unsigned char>(name[0])) || name[0] == '_'))
            names.push_back(name);
    }
    return names;
}

} // namespace

TEST(TlsClientStatic, EveryClientConfigFieldReachesEveryPlaceThatBuildsOne) {
    // Three call sites build a `ClientConfig` from a pool-shaped config, and each used to hand-copy
    // the fields it happened to know about. That is how the C++ pool and the shard router reached
    // #30 part one unable to authenticate at all: `auth_identity` existed on `ClientConfig`,
    // nothing carried it, and the symptom is `ERR unauthenticated` from a configuration that reads
    // as complete.
    //
    // The rule is derived from the source on both sides, with no list written by hand - because a
    // list written by hand is not evidence about the code (pitfall 79). Every field must either be
    // carried by `copy_client_access()` or be assigned at every site that constructs one.
    const std::string header  = slurp("include/orderbook/client.hpp");
    const std::string client  = slurp("src/client.cpp");
    const std::string router  = slurp("src/shard_router.cpp");
    ASSERT_FALSE(header.empty());
    ASSERT_FALSE(client.empty());
    ASSERT_FALSE(router.empty());

    const auto fields = struct_fields(header, "struct ClientConfig {");
    ASSERT_GE(fields.size(), 8u) << "ClientConfig field extraction found too little to be trusted";

    const auto tmpl_start = header.find("void copy_client_access(");
    ASSERT_NE(tmpl_start, std::string::npos);
    const auto tmpl_end = header.find("\n}", tmpl_start);
    ASSERT_NE(tmpl_end, std::string::npos);
    const std::string carried = header.substr(tmpl_start, tmpl_end - tmpl_start);

    for (const auto& name : fields) {
        const bool by_template = carried.find("to." + name + " ") != std::string::npos ||
                                 carried.find("to." + name + "=") != std::string::npos;
        const bool at_sites = client.find("cc." + name) != std::string::npos &&
                              router.find("cc." + name) != std::string::npos;
        EXPECT_TRUE(by_template || at_sites)
            << "ClientConfig::" << name << " is carried neither by copy_client_access() nor by "
            << "every site that builds a ClientConfig, so a pool or a shard router silently "
            << "connects without it";
    }
}

TEST(TlsClientStatic, ThePoolShapedConfigsCarryWhatTheTemplateAsksFor) {
    // The other direction, and it is a compile-time guarantee rather than a textual one: the
    // template reads `from.<field>`, so a `PoolConfig` or `ShardRouterConfig` missing one does not
    // build. This test pins that the instantiations exist at all - a template nobody instantiates
    // guarantees nothing, which is how a required job that gates nothing looks like coverage.
    ob::ClientConfig cc;

    ob::PoolConfig pool;
    pool.auth_identity = "alice";
    pool.tls           = true;
    pool.tls_ca_file   = "/ca.pem";
    ob::copy_client_access(pool, cc);
    EXPECT_EQ(cc.auth_identity, "alice");
    EXPECT_TRUE(cc.tls);
    EXPECT_EQ(cc.tls_ca_file, "/ca.pem");
    EXPECT_TRUE(cc.tls_verify);

    ob::ShardRouterConfig shard;
    shard.auth_identity = "bob";
    shard.tls_verify    = false;
    ob::copy_client_access(shard, cc);
    EXPECT_EQ(cc.auth_identity, "bob");
    EXPECT_FALSE(cc.tls_verify);
    EXPECT_FALSE(cc.tls) << "a later copy must not leave a field from the earlier one";
}
