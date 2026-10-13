// The certificate read again without a restart (#238).
//
// What a reload promises is about connections, so the tests that matter here are handshakes over a
// socket pair, as in `test_tls_node_link.cpp`: one opened before the reload finishes on the
// certificate it began with, and one opened after it is shown the new one. The rest is the promise
// a reload makes when it cannot read what it was given - every surface keeps what it had, and the
// refusal is counted - because a renewal that goes wrong must not take down a node that was running.

#include "orderbook/metrics.hpp"
#include "orderbook/tcp_server.hpp"
#include "orderbook/tls.hpp"

#include <gtest/gtest.h>

#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>
#include <thread>

namespace fs = std::filesystem;

namespace {

/// A private CA and the node certificates it signs, the way `docs/operations.md` makes them.
struct Pki {
    std::string dir;
    std::string ca_cert;
    std::string ca_key;

    Pki() {
        static std::atomic<int> counter{0};
        dir = (fs::temp_directory_path() / ("ob_tlsr_" + std::to_string(::getpid()) + "_" +
                                            std::to_string(counter.fetch_add(1)))).string();
        fs::create_directories(dir);
        ca_cert = dir + "/ca.pem";
        ca_key  = dir + "/ca-key.pem";
        const std::string cmd = "openssl req -x509 -newkey rsa:2048 -nodes -keyout '" + ca_key +
                                "' -out '" + ca_cert +
                                "' -days 1 -subj '/CN=ob-reload-ca' >/dev/null 2>&1";
        if (std::system(cmd.c_str()) != 0) ca_cert.clear();
    }
    ~Pki() { fs::remove_all(dir); }

    struct Signed {
        std::string cert;
        std::string key;
    };

    /// A certificate for `cn`, valid for `days`: the two things a renewal changes and a test reads.
    Signed sign(const std::string& cn, int days) const {
        const std::string stem = dir + "/" + cn;
        Signed out{stem + ".pem", stem + "-key.pem"};
        const std::string mk =
            "openssl req -newkey rsa:2048 -nodes -keyout '" + out.key + "' -out '" + stem +
            ".csr' -subj '/CN=" + cn + "' >/dev/null 2>&1 && "
            "openssl x509 -req -in '" + stem + ".csr' -CA '" + ca_cert + "' -CAkey '" + ca_key +
            "' -CAcreateserial -out '" + out.cert + "' -days " + std::to_string(days) +
            " >/dev/null 2>&1";
        if (std::system(mk.c_str()) != 0) return {};
        ::chmod(out.key.c_str(), 0600);
        return out;
    }
};

/// Put `from` where the node reads its certificate and key: what an operator's renewal does.
void install(const Pki::Signed& from, const std::string& cert, const std::string& key) {
    fs::copy_file(from.cert, cert, fs::copy_options::overwrite_existing);
    fs::copy_file(from.key, key, fs::copy_options::overwrite_existing);
    ::chmod(key.c_str(), 0600);
}

/// A node with every TLS surface on, reading its certificate from fixed paths in `pki.dir`.
ob::ServerConfig every_surface(const Pki& pki) {
    ob::ServerConfig c;
    c.tls_client       = true;
    c.tls_replication  = true;
    c.tls_multi_master = true;
    c.tls_cert_file    = pki.dir + "/node.pem";
    c.tls_key_file     = pki.dir + "/node-key.pem";
    c.tls_ca_file      = pki.ca_cert;
    return c;
}

/// Finish the handshake of `server` - an accepting channel opened earlier on `fd` - with a fresh
/// node client on `peer_fd`, and say whose certificate the client was shown ("" if none).
std::string finish(const std::shared_ptr<ob::TlsChannel>& server, int peer_fd,
                   const ob::TlsContext& client_ctx) {
    auto client = client_ctx.open_channel(peer_fd, /*server_side=*/false, "test-server");
    bool server_ok = false;
    std::string server_why;
    std::thread accepting([&] { server_ok = server->blocking_handshake(&server_why); });
    std::string why;
    const bool client_ok = client->blocking_handshake(&why);
    accepting.join();
    EXPECT_TRUE(server_ok) << server_why;
    EXPECT_TRUE(client_ok) << why;
    return client_ok ? client->identity() : std::string();
}

/// A socket pair whose reads give up after three seconds, so a handshake that cannot finish fails
/// the test instead of wedging the suite.
struct Pair {
    int fds[2] = {-1, -1};
    Pair() {
        if (::socketpair(AF_UNIX, SOCK_STREAM, 0, fds) != 0) return;
        struct timeval tv{};
        tv.tv_sec = 3;
        ::setsockopt(fds[0], SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        ::setsockopt(fds[1], SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    }
    ~Pair() {
        if (fds[0] >= 0) ::close(fds[0]);
        if (fds[1] >= 0) ::close(fds[1]);
    }
};

int64_t now_seconds() {
    return std::chrono::duration_cast<std::chrono::seconds>(
               std::chrono::system_clock::now().time_since_epoch()).count();
}

} // namespace

// ── What a context says about its certificate ────────────────────────────────

TEST(TlsReload, AContextSaysWhoItsCertificateNamesAndWhenItExpires) {
    const Pki pki;
    ASSERT_FALSE(pki.ca_cert.empty()) << "openssl is required for these tests";
    const auto a = pki.sign("node-a", 30);
    ASSERT_FALSE(a.cert.empty());

    const auto ctx = ob::TlsContext::server(a.cert, a.key);
    EXPECT_EQ(ctx.certificate_subject(), "/CN=node-a");
    const auto not_after = ctx.certificate_not_after();
    ASSERT_TRUE(not_after.has_value());
    // Thirty days from the moment openssl signed it, which was a moment ago.
    EXPECT_NEAR(static_cast<double>(*not_after - now_seconds()), 30.0 * 86400, 600.0);

    // A context that presents nothing has nothing to say, rather than something made up.
    const auto client = ob::TlsContext::client(pki.ca_cert, /*verify=*/true);
    EXPECT_EQ(client.certificate_subject(), "");
    EXPECT_FALSE(client.certificate_not_after().has_value());
}

TEST(TlsReload, ASurfaceGivesTheNextCallerTheNewContextAndLeavesAHeldOneAlone) {
    const Pki pki;
    ASSERT_FALSE(pki.ca_cert.empty());
    const auto a = pki.sign("node-a", 1);
    const auto b = pki.sign("node-b", 1);
    ASSERT_FALSE(b.cert.empty());

    ob::TlsSurface surface(std::make_shared<const ob::TlsContext>(ob::TlsContext::server(a.cert, a.key)));
    const auto held = surface.current();
    surface.replace(std::make_shared<const ob::TlsContext>(ob::TlsContext::server(b.cert, b.key)));

    EXPECT_EQ(surface.current()->certificate_subject(), "/CN=node-b");
    // The surface let go of A; the connection that took it has not.
    EXPECT_EQ(held.use_count(), 1);
    EXPECT_EQ(held->certificate_subject(), "/CN=node-a");
}

// ── A reload ─────────────────────────────────────────────────────────────────

TEST(TlsReload, ANewConnectionIsShownTheNewCertificateAndAnOpenOneFinishesOnTheOld) {
    const Pki pki;
    ASSERT_FALSE(pki.ca_cert.empty());
    const auto a = pki.sign("node-a", 1);
    const auto b = pki.sign("node-b", 30);
    const auto peer = pki.sign("peer", 1);
    ASSERT_FALSE(peer.cert.empty());
    const ob::ServerConfig config = every_surface(pki);
    install(a, config.tls_cert_file, config.tls_key_file);
    const ob::LoadedTlsContexts tls = ob::build_tls_contexts(config);
    const auto peer_ctx = ob::TlsContext::node_client(peer.cert, peer.key, pki.ca_cert);

    // Accepted before the reload, its handshake not begun: the case a renewal under load is made of.
    Pair before;
    const auto opened = tls.mesh_server->current()->open_channel(before.fds[0], /*server_side=*/true,
                                                                 "before");

    install(b, config.tls_cert_file, config.tls_key_file);
    ob::MetricsRegistry registry;
    ASSERT_EQ(ob::reload_tls_contexts(config, tls, &registry), ob::TlsReload::Reloaded);

    EXPECT_EQ(finish(opened, before.fds[1], peer_ctx), "node-a")
        << "a connection opened before the reload finishes on the certificate it began with";

    Pair after;
    const auto next = tls.mesh_server->current()->open_channel(after.fds[0], /*server_side=*/true,
                                                               "after");
    EXPECT_EQ(finish(next, after.fds[1], peer_ctx), "node-b")
        << "a connection opened after the reload is shown the new certificate";

    // Every surface, not the one the handshake went through.
    for (const auto* surface : {&tls.client_port, &tls.replication_server, &tls.replication_client,
                                &tls.mesh_server, &tls.mesh_client}) {
        EXPECT_EQ((*surface)->current()->certificate_subject(), "/CN=node-b");
    }
    EXPECT_EQ(registry.counter_value("ob_tls_reloads_total"), 1u);
    EXPECT_EQ(registry.counter_value("ob_tls_reload_failures_total"), 0u);
    EXPECT_NEAR(static_cast<double>(registry.gauge_value("ob_tls_certificate_not_after_seconds") -
                                    now_seconds()),
                30.0 * 86400, 600.0)
        << "the gauge follows the certificate now in force";
}

TEST(TlsReload, FilesItRefusesLeaveEverySurfaceWithWhatItHad) {
    const Pki pki;
    ASSERT_FALSE(pki.ca_cert.empty());
    const auto a = pki.sign("node-a", 1);
    const auto b = pki.sign("node-b", 1);
    ASSERT_FALSE(b.cert.empty());
    const ob::ServerConfig config = every_surface(pki);
    install(a, config.tls_cert_file, config.tls_key_file);
    const ob::LoadedTlsContexts tls = ob::build_tls_contexts(config);
    const auto in_force = tls.client_port->current();

    ob::MetricsRegistry registry;
    registry.set_gauge("ob_tls_certificate_not_after_seconds", ob::tls_certificate_not_after(tls));
    const int64_t gauge_before = registry.gauge_value("ob_tls_certificate_not_after_seconds");

    // Half a renewal: B's certificate in place, A's key still beside it.
    fs::copy_file(b.cert, config.tls_cert_file, fs::copy_options::overwrite_existing);
    EXPECT_EQ(ob::reload_tls_contexts(config, tls, &registry), ob::TlsReload::Refused);
    // B's key, readable by anyone: the start refuses it, so the reload does.
    install(b, config.tls_cert_file, config.tls_key_file);
    ::chmod(config.tls_key_file.c_str(), 0644);
    EXPECT_EQ(ob::reload_tls_contexts(config, tls, &registry), ob::TlsReload::Refused);
    // No certificate at all.
    fs::remove(config.tls_cert_file);
    EXPECT_EQ(ob::reload_tls_contexts(config, tls, &registry), ob::TlsReload::Refused);

    EXPECT_EQ(tls.client_port->current(), in_force);
    for (const auto* surface : {&tls.client_port, &tls.replication_server, &tls.replication_client,
                                &tls.mesh_server, &tls.mesh_client}) {
        EXPECT_EQ((*surface)->current()->certificate_subject(), "/CN=node-a");
    }
    EXPECT_EQ(registry.counter_value("ob_tls_reload_failures_total"), 3u);
    EXPECT_EQ(registry.counter_value("ob_tls_reloads_total"), 0u);
    EXPECT_EQ(registry.gauge_value("ob_tls_certificate_not_after_seconds"), gauge_before);
}

TEST(TlsReload, OneSurfaceRefusedIsEverySurfaceKept) {
    // The client port needs no CA and the node links do, so a broken CA bundle is the case where
    // the client port's new context builds and the node links' do not. All or nothing: the client
    // port keeps A too, or the node would present two certificates at once.
    const Pki pki;
    ASSERT_FALSE(pki.ca_cert.empty());
    const auto a = pki.sign("node-a", 1);
    const auto b = pki.sign("node-b", 1);
    ASSERT_FALSE(b.cert.empty());
    ob::ServerConfig config = every_surface(pki);
    config.tls_ca_file = pki.dir + "/bundle.pem";
    fs::copy_file(pki.ca_cert, config.tls_ca_file);
    install(a, config.tls_cert_file, config.tls_key_file);
    const ob::LoadedTlsContexts tls = ob::build_tls_contexts(config);

    install(b, config.tls_cert_file, config.tls_key_file);
    { std::ofstream(config.tls_ca_file, std::ios::trunc) << "not a certificate\n"; }
    // The premise: on its own, the client port's context would build from these files.
    ASSERT_NO_THROW({ auto ctx = ob::TlsContext::server(config.tls_cert_file, config.tls_key_file);
                      (void)ctx; });

    ob::MetricsRegistry registry;
    EXPECT_EQ(ob::reload_tls_contexts(config, tls, &registry), ob::TlsReload::Refused);
    EXPECT_EQ(tls.client_port->current()->certificate_subject(), "/CN=node-a");
    EXPECT_EQ(tls.mesh_server->current()->certificate_subject(), "/CN=node-a");
}

TEST(TlsReload, ANodeWithoutTlsHasNothingToReload) {
    ob::ServerConfig config;
    config.tls_cert_file = "/nonexistent/ob_tls_cert.pem";   // a staged rollout: paths, no surface
    const ob::LoadedTlsContexts tls = ob::build_tls_contexts(config);
    ASSERT_EQ(tls.client_port, nullptr);

    ob::MetricsRegistry registry;
    EXPECT_EQ(ob::reload_tls_contexts(config, tls, &registry), ob::TlsReload::NothingToReload);
    EXPECT_EQ(registry.counter_value("ob_tls_reloads_total"), 0u);
    EXPECT_EQ(registry.counter_value("ob_tls_reload_failures_total"), 0u);
    EXPECT_EQ(ob::tls_certificate_not_after(tls), 0);
}

TEST(TlsReload, ARefusalIsAnExceptionSoOnlyTheStartExits) {
    // build_tls_contexts() is the start's refusal as well as the reload's, and only the start
    // exits: a refusal here has to be an exception, never an exit, or a reload could stop a node.
    ob::ServerConfig config;
    config.tls_client = true;
    EXPECT_THROW(ob::build_tls_contexts(config), std::runtime_error);   // no certificate, no key

    const Pki pki;
    ASSERT_FALSE(pki.ca_cert.empty());
    const auto a = pki.sign("node-a", 1);
    config = ob::ServerConfig{};
    config.tls_replication = true;
    config.tls_cert_file   = a.cert;
    config.tls_key_file    = a.key;
    EXPECT_THROW(ob::build_tls_contexts(config), std::runtime_error);   // a node link without a CA
}
